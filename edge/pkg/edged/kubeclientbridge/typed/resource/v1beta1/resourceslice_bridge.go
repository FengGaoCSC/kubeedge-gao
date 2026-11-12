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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	typedresourcev1beta1 "k8s.io/client-go/kubernetes/typed/resource/v1beta1"
)

// ResourceSliceBridge implements ResourceSliceInterface. Only
// DeleteCollection is overridden because that is the only call kubelet's
// vendored DRA plugin-registration handler
// (pkg/kubelet/cm/dra/plugin/registration.go) makes on this type - it
// garbage-collects stale ResourceSlices when a DRA plugin re-registers
// or is removed.
//
// kubelet itself does NOT create/update ResourceSlices: the DRA driver
// Pod (e.g. dra-example-driver) publishes its own ResourceSlices from
// its own container using its own client. On KubeEdge that client
// should be pointed at MetaServer's local endpoint
// (http://127.0.0.1:10550), the same mechanism the cilium-kubeedge
// DaemonSet already uses via --k8s-api-server-urls. MetaServer's
// generic dynamic REST storage needs no per-type code to accept
// Create/Update/List for ResourceSlice from that path - only this
// DeleteCollection call needs a bridge because it originates inside
// edged's own kubelet.Dependencies clientset, not the driver Pod.
type ResourceSliceBridge struct {
	typedresourcev1beta1.ResourceSliceInterface
	client *loopbackClient
}

func (c *ResourceSliceBridge) DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	return c.client.deleteCollection(ctx, "resourceslices", opts, listOpts)
}
