/*
Copyright 2025 The KubeEdge Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package handlerfactory

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metainternalversionscheme "k8s.io/apimachinery/pkg/apis/meta/internalversion/scheme"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/endpoints/handlers"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/klog/v2"

	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/metaserver/kubernetes/fakers"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/metaserver/kubernetes/scope"
	"github.com/kubeedge/kubeedge/pkg/metaserver"
	"github.com/kubeedge/kubeedge/pkg/metaserver/util"
)

func (f *Factory) Create(req *request.RequestInfo) http.Handler {
	s := scope.NewRequestScope()
	s.Kind = schema.GroupVersionKind{
		Group:   req.APIGroup,
		Version: req.APIVersion,
		Kind:    util.UnsafeResourceToKind(req.Resource),
	}
	h := handlers.CreateResource(f.storage, s, fakers.NewAlwaysAdmit())
	return h
}

func (f *Factory) Update(req *request.RequestInfo) http.Handler {
	if req.Resource == "devices" {
		h := updateEdgeDevice()
		return h
	}
	s := scope.NewRequestScope()
	s.Kind = schema.GroupVersionKind{
		Group:   req.APIGroup,
		Version: req.APIVersion,
		Kind:    util.UnsafeResourceToKind(req.Resource),
	}
	h := handlers.UpdateResource(f.storage, s, fakers.NewAlwaysAdmit())
	return h
}

func (f *Factory) Delete() http.Handler {
	if h, ok := f.getHandler("delete"); ok {
		return h
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	h := handlers.DeleteResource(f.storage, true, f.scope, fakers.NewAlwaysAdmit())
	f.handlers["delete"] = h
	return h
}

func (f *Factory) DeleteCollection(reqInfo *request.RequestInfo) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			klog.Errorf("DeleteCollection: read request error: %v", err)
			return
		}

		listReq := req.Clone(req.Context())
		listReq.Method = http.MethodGet
		listReq.Body = http.NoBody
		listInfo := *reqInfo
		listInfo.Verb = "list"
		listReq = listReq.WithContext(request.WithRequestInfo(listReq.Context(), &listInfo))

		listRec := httptest.NewRecorder()
		f.List().ServeHTTP(listRec, listReq)
		if listRec.Code != http.StatusOK {
			klog.Errorf("DeleteCollection: list record error")
			return
		}

		list_rec_body := listRec.Body.Bytes()
		if listRec.Header().Get("Content-Encoding") == "gzip" {
			zr, err := gzip.NewReader(bytes.NewReader(list_rec_body))
			if err != nil {
				klog.Errorf("DeleteCollection: gzip reader error: %v", err)
				return
			}
			defer zr.Close()
			if list_rec_body, err = io.ReadAll(zr); err != nil {
				klog.Errorf("DeleteCollection: gunzip error: %v", err)
				return
			}
		}
		list := &unstructured.UnstructuredList{}
		if err := list.UnmarshalJSON(list_rec_body); err != nil {
			klog.Errorf("DeleteCollection: unmarshalJSON error: %v", err)
			return
		}

		sel := fields.Everything()
		if fs := req.URL.Query().Get("fieldSelector"); fs != "" {
			if sel, err = fields.ParseSelector(fs); err != nil {
				klog.Errorf("DeleteCollection: parse field selector error: %v", err)
				return
			}
		}

		var errs []error
		for i := range list.Items {
			item := &list.Items[i]
			if !sel.Matches(fieldSetOf(item, sel)) {
				continue
			}

			delPath := strings.TrimSuffix(req.URL.Path, "/") + "/" + item.GetName()
			delReq := req.Clone(req.Context())
			delReq.Method = http.MethodDelete
			delReq.URL.Path = delPath
			delReq.URL.RawQuery = ""
			delReq.Body = io.NopCloser(bytes.NewReader(body))
			delInfo := *reqInfo
			delInfo.Verb = "delete"
			delInfo.Name = item.GetName()
			delInfo.Path = delPath
			delReq = delReq.WithContext(request.WithRequestInfo(delReq.Context(), &delInfo))

			rec := httptest.NewRecorder()
			f.Delete().ServeHTTP(rec, delReq)
			if rec.Code >= 300 && rec.Code != http.StatusNotFound {
				errs = append(errs, fmt.Errorf("delete %s: status %d: %s",
					item.GetName(), rec.Code, rec.Body.String()))
			}
		}

		if len(errs) > 0 {
			klog.Errorf("DeleteCollection: http request error: %v", errs)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(listRec.Body.Bytes())
	})
}

// fieldSetOf builds a fields.Set with exactly the fields the selector asks about,
// read from the object by dotted path (e.g. "spec.nodeName", "spec.driver").
func fieldSetOf(obj *unstructured.Unstructured, sel fields.Selector) fields.Set {
	set := fields.Set{}
	for _, r := range sel.Requirements() {
		if v, found, _ := unstructured.NestedString(obj.Object, strings.Split(r.Field, ".")...); found {
			set[r.Field] = v
		}
	}
	return set
}

func (f *Factory) Patch(reqInfo *request.RequestInfo) http.Handler {
	scope := wrapScope{RequestScope: scope.NewRequestScope()}
	scope.Kind = schema.GroupVersionKind{
		Group:   reqInfo.APIGroup,
		Version: reqInfo.APIVersion,
		Kind:    util.UnsafeResourceToKind(reqInfo.Resource),
	}

	h := func(w http.ResponseWriter, req *http.Request) {
		// Do this first, otherwise name extraction can fail for unrecognized content types
		// TODO: handle this in negotiation
		contentType := req.Header.Get("Content-Type")
		// Remove "; charset=" if included in header.
		if idx := strings.Index(contentType, ";"); idx > 0 {
			contentType = contentType[:idx]
		}
		patchType := types.PatchType(contentType)

		// TODO: we either want to remove timeout or document it (if we
		// document, move timeout out of this function and declare it in
		// api_installer)
		timeout := parseTimeout(req.URL.Query().Get("timeout"))

		namespace, name, err := scope.Namer.Name(req)
		if err != nil {
			scope.err(err, w, req)
			return
		}

		ctx, cancel := context.WithTimeout(req.Context(), timeout)
		defer cancel()
		ctx = request.WithNamespace(ctx, namespace)

		patchBytes, err := limitedReadBody(req, scope.MaxRequestBodyBytes)
		if err != nil {
			scope.err(err, w, req)
			return
		}

		options := &metav1.PatchOptions{}
		if err := metainternalversionscheme.ParameterCodec.DecodeParameters(req.URL.Query(), scope.MetaGroupVersion, options); err != nil {
			err = errors.NewBadRequest(err.Error())
			scope.err(err, w, req)
			return
		}
		if errs := validation.ValidatePatchOptions(options, patchType); len(errs) > 0 {
			err := errors.NewInvalid(schema.GroupKind{Group: metav1.GroupName, Kind: "PatchOptions"}, "", errs)
			scope.err(err, w, req)
			return
		}
		options.TypeMeta.SetGroupVersionKind(metav1.SchemeGroupVersion.WithKind("PatchOptions"))

		reqInfo, _ := request.RequestInfoFrom(req.Context())
		pi := metaserver.PatchInfo{
			Name:         name,
			PatchType:    patchType,
			Data:         patchBytes,
			Options:      *options,
			Subresources: []string{reqInfo.Subresource},
		}

		retObj, err := f.storage.Patch(ctx, pi)
		if err != nil {
			scope.err(err, w, req)
			return
		}

		responsewriters.WriteObjectNegotiated(scope.Serializer, scope, scope.Kind.GroupVersion(), w, req, 200, retObj, false)
	}
	return http.HandlerFunc(h)
}

type wrapScope struct {
	*handlers.RequestScope
}

func (scope *wrapScope) err(err error, w http.ResponseWriter, req *http.Request) {
	responsewriters.ErrorNegotiated(err, scope.Serializer, scope.Kind.GroupVersion(), w, req)
}

func parseTimeout(str string) time.Duration {
	if str != "" {
		timeout, err := time.ParseDuration(str)
		if err == nil {
			return timeout
		}
		klog.Errorf("Failed to parse %q: %v", str, err)
	}
	// 34 chose as a number close to 30 that is likely to be unique enough to jump out at me the next time I see a timeout.  Everyone chooses 30.
	return 34 * time.Second
}

func limitedReadBody(req *http.Request, limit int64) ([]byte, error) {
	defer req.Body.Close()
	if limit <= 0 {
		return io.ReadAll(req.Body)
	}
	lr := &io.LimitedReader{
		R: req.Body,
		N: limit + 1,
	}
	data, err := io.ReadAll(lr)
	if err != nil {
		return nil, err
	}
	if lr.N <= 0 {
		return nil, errors.NewRequestEntityTooLargeError(fmt.Sprintf("limit is %d", limit))
	}
	return data, nil
}
