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

@CHANGELOG
KubeEdge Authors: To make a bridge between kubeclient and metaclient,
This file is derived from K8S client-go code with reduced set of methods.
Changes done are
1. Package v1beta1 got some functions from
   "k8s.io/client-go/kubernetes/typed/resource/v1beta1/fake/fake_resource_client.go"
   and made some variant.
*/

package v1beta1

import (
	typedresourcev1beta1 "k8s.io/client-go/kubernetes/typed/resource/v1beta1"
	fakeresourcev1beta1 "k8s.io/client-go/kubernetes/typed/resource/v1beta1/fake"
)

// ResourceV1beta1Bridge is a resource.k8s.io/v1beta1 bridge. Unlike the
// CoreV1/StorageV1/CoordinationV1 bridges it does not take a
// metaclient.CoreInterface, because ResourceClaims/ResourceSlices are
// not (and should not become) hard-wired message types in
// edge/pkg/metamanager/client - they go through MetaServer's already
// generic dynamic-resource REST path via a local HTTP loopback client
// instead (see loopback_client.go).
type ResourceV1beta1Bridge struct {
	fakeresourcev1beta1.FakeResourceV1beta1
	client *loopbackClient
}

// NewResourceV1beta1Bridge constructs the bridge used by
// Clientset.ResourceV1beta1() in clientset_bridge.go.
func NewResourceV1beta1Bridge() *ResourceV1beta1Bridge {
	return &ResourceV1beta1Bridge{client: newLoopbackClient()}
}

func (c *ResourceV1beta1Bridge) ResourceClaims(namespace string) typedresourcev1beta1.ResourceClaimInterface {
	fakeClaims := c.FakeResourceV1beta1.ResourceClaims(namespace)
	return &ResourceClaimBridge{ResourceClaimInterface: fakeClaims, ns: namespace, client: c.client}
}

func (c *ResourceV1beta1Bridge) ResourceSlices() typedresourcev1beta1.ResourceSliceInterface {
	fakeSlices := c.FakeResourceV1beta1.ResourceSlices()
	return &ResourceSliceBridge{ResourceSliceInterface: fakeSlices, client: c.client}
}

// DeviceClasses and ResourceClaimTemplates are not called by any
// vendored kubelet DRA code path today, so they intentionally fall
// through to the embedded fake (no bridge override). Add overrides here
// the same way if a future kubelet version starts calling them.
