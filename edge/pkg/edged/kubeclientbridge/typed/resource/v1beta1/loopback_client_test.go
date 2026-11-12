/*
Copyright 2026 The KubeEdge Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Simulates the edge-local half of the DRA request path: kubelet's DRA
manager -> ResourceV1beta1Bridge -> loopback HTTP client -> MetaServer.
This stands in an httptest.Server for MetaServer's REST endpoint (the
real MetaServer -> CloudHub -> cloud -> apiserver hop is not exercised
here - see the accompanying report for why a live cluster is needed to
cover that half, and how to run it).
*/

package v1beta1

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	resourcev1beta1 "k8s.io/api/resource/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestResourceClaimBridge_Get(t *testing.T) {
	const ns, name = "default", "example-claim"
	wantPath := "/apis/resource.k8s.io/v1beta1/namespaces/default/resourceclaims/example-claim"

	claim := &resourcev1beta1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("expected GET, got %s", r.Method)
		}
		if r.URL.Path != wantPath {
			t.Fatalf("expected path %s, got %s", wantPath, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(claim)
	}))
	defer srv.Close()

	bridge := &ResourceClaimBridge{ns: ns, client: &loopbackClient{httpClient: srv.Client(), baseURL: srv.URL}}

	got, err := bridge.Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Name != name || got.Namespace != ns {
		t.Fatalf("Get() = %+v, want name=%s ns=%s", got, name, ns)
	}
}

func TestResourceClaimBridge_Get_CloudError(t *testing.T) {
	// Simulates the RBAC-missing case: cloudcore's dynamicClient.Create/Get
	// against the real apiserver returns 403, which ProcessApplication
	// propagates back through Task/Application to MetaServer's REST layer,
	// which (like any k8s apiserver REST handler) returns a non-2xx status.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"message":"forbidden: RBAC does not allow get on resourceclaims"}`))
	}))
	defer srv.Close()

	bridge := &ResourceClaimBridge{ns: "default", client: &loopbackClient{httpClient: srv.Client(), baseURL: srv.URL}}

	_, err := bridge.Get(context.Background(), "example-claim", metav1.GetOptions{})
	if err == nil {
		t.Fatal("expected error propagated from cloud 403, got nil")
	}
	t.Logf("propagated error (expected): %v", err)
}

func TestResourceSliceBridge_DeleteCollection(t *testing.T) {
	wantPath := "/apis/resource.k8s.io/v1beta1/resourceslices"
	called := false

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		if r.Method != http.MethodDelete {
			t.Fatalf("expected DELETE, got %s", r.Method)
		}
		if r.URL.Path != wantPath {
			t.Fatalf("expected path %s, got %s", wantPath, r.URL.Path)
		}
		if fs := r.URL.Query().Get("fieldSelector"); fs != "nodeName=edge-node-1" {
			t.Fatalf("expected fieldSelector nodeName=edge-node-1, got %q", fs)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	bridge := &ResourceSliceBridge{client: &loopbackClient{httpClient: srv.Client(), baseURL: srv.URL}}
	err := bridge.DeleteCollection(context.Background(), metav1.DeleteOptions{},
		metav1.ListOptions{FieldSelector: "nodeName=edge-node-1"})
	if err != nil {
		t.Fatalf("DeleteCollection() error = %v", err)
	}
	if !called {
		t.Fatal("expected loopback server to be called")
	}
}
