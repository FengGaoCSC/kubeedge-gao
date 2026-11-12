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

Package v1beta1 - loopback_client.go implements a minimal REST client
against edgecore's own local MetaServer HTTP endpoint
(constants.DefaultMetaServerAddr, 127.0.0.1:10550). This is deliberately
NOT going through metaclient.CoreInterface (the Beehive/CloudHub
"mechanism A" path used by LeaseBridge etc.), because that mechanism is
hard-wired per resource type (see edge/pkg/metamanager/client/metaclient.go)
and adding ResourceClaims/ResourceSlices there means new message types
plus CloudHub authorization-whitelist entries for a resource that
MetaServer's generic dynamic REST storage (edge/pkg/metamanager/metaserver/
kubernetes/storage/storage.go) already forwards to the cloud for any GVR.
This client re-enters that same generic REST server that a DRA driver
Pod would use if pointed at http://127.0.0.1:10550 directly (the same
pattern the cilium-kubeedge DaemonSet uses today), just called in-process
from edged instead of from another container.

This is option (a) from the design doc: a real HTTP loopback call,
chosen over reaching into metaserver/kubernetes/storage.REST's Go method
directly (option (b)) because it needs no exported hook into that
package and keeps this bridge fully decoupled from MetaServer's internal
wiring. If profiling later shows the loopback overhead matters for
high-frequency ResourceSlice churn, option (b) is the follow-up.
*/

package v1beta1

import (
	"bytes"
	"context"
	"os"

	"crypto/tls"
	"crypto/x509"

	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	metaserverconstants "github.com/kubeedge/api/apis/common/constants"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/metaserver/certificate"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/metaserver/common"
)

const (
	resourceGroup   = "resource.k8s.io"
	resourceVersion = "v1beta1"
)

// loopbackClient is a tiny helper for calling edgecore's own local
// MetaServer REST endpoint. It intentionally supports only the verbs
// this package's bridges need (Get, DeleteCollection) - it is not a
// general-purpose dynamic client.
type loopbackClient struct {
	httpClient *http.Client
	baseURL    string // e.g. http://127.0.0.1:10550
}

func newLoopbackClient() *loopbackClient {
	return &loopbackClient{
		httpClient: &http.Client{},
		baseURL:    "http://" + metaserverconstants.DefaultMetaServerAddr,
	}
}

func (l *loopbackClient) resourcePath(namespace, resource, name string) string {
	if namespace == "" {
		if name == "" {
			return fmt.Sprintf("%s/apis/%s/%s/%s", l.baseURL, resourceGroup, resourceVersion, resource)
		}
		return fmt.Sprintf("%s/apis/%s/%s/%s/%s", l.baseURL, resourceGroup, resourceVersion, resource, name)
	}
	if name == "" {
		return fmt.Sprintf("%s/apis/%s/%s/namespaces/%s/%s", l.baseURL, resourceGroup, resourceVersion, namespace, resource)
	}
	return fmt.Sprintf("%s/apis/%s/%s/namespaces/%s/%s/%s", l.baseURL, resourceGroup, resourceVersion, namespace, resource, name)
}

func (l *loopbackClient) do(ctx context.Context, method, url string, body io.Reader, out interface{}) error {
	if strings.HasPrefix(url, "http://127.0.0.1:10550/") {
		url = "https://" + strings.TrimPrefix(url, "http://")
		caCert, err := os.ReadFile(certificate.CertificatesDir + "/ca.crt")
		if err == nil {
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			l.httpClient.Transport = &http.Transport{
				TLSClientConfig: &tls.Config{
					RootCAs:    caCertPool,
					MinVersion: tls.VersionTLS12,
				},
			}
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+common.LoopbackToken())
	req.Header.Set("Content-Type", "application/json")
	resp, err := l.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("metaserver loopback request failed: %w", err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("metaserver loopback %s %s returned %d: %s", method, url, resp.StatusCode, string(respBody))
	}
	if out != nil {
		return json.Unmarshal(respBody, out)
	}
	return nil
}

func (l *loopbackClient) get(ctx context.Context, namespace, resource, name string, out interface{}) error {
	return l.do(ctx, http.MethodGet, l.resourcePath(namespace, resource, name), nil, out)
}

func (l *loopbackClient) deleteCollection(ctx context.Context, resource string, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	url := l.resourcePath("", resource, "")
	if q := listOpts.FieldSelector; q != "" {
		url = fmt.Sprintf("%s?fieldSelector=%s", url, q)
	}
	body, err := json.Marshal(opts)
	if err != nil {
		return err
	}
	return l.do(ctx, http.MethodDelete, url, bytes.NewReader(body), nil)
}
