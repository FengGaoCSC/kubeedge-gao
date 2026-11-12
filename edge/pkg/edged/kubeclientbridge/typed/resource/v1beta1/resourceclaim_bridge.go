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
*/

package v1beta1

import (
	"context"

	resourcev1beta1 "k8s.io/api/resource/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	typedresourcev1beta1 "k8s.io/client-go/kubernetes/typed/resource/v1beta1"
)

// ResourceClaimBridge implements ResourceClaimInterface. Only Get is
// overridden because that is the only method the vendored kubelet DRA
// manager (pkg/kubelet/cm/dra/manager.go) calls on this type. Every
// other method (List/Watch/Create/Update/Delete/Patch/Apply) falls
// through to the embedded fake implementation and is a silent no-op
// against a local, in-memory store - do not assume those work until a
// real bridge method is added here.
type ResourceClaimBridge struct {
	typedresourcev1beta1.ResourceClaimInterface
	ns     string
	client *loopbackClient
}

func (c *ResourceClaimBridge) Get(ctx context.Context, name string, _ metav1.GetOptions) (*resourcev1beta1.ResourceClaim, error) {
	claim := &resourcev1beta1.ResourceClaim{}
	if err := c.client.get(ctx, c.ns, "resourceclaims", name, claim); err != nil {
		return nil, err
	}
	return claim, nil
}
