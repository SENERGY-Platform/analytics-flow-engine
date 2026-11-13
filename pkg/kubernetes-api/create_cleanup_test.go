/*
 * Copyright 2026 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kubernetes_api

import (
	"testing"

	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	vpafake "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/client/clientset/versioned/fake"
	k8stesting "k8s.io/client-go/testing"
)

// failFirstVPACreate makes the first autoscaler create fail, as a create that got the
// deployment through and then lost the API server would.
func failFirstVPACreate(k *Kubernetes) {
	failed := false
	k.autoscalerClientset.(*vpafake.Clientset).PrependReactor("create", "verticalpodautoscalers", func(k8stesting.Action) (bool, runtime.Object, error) {
		if failed {
			return false, nil, nil
		}
		failed = true
		return true, nil, k8s_errors.NewServiceUnavailable("autoscaler api down")
	})
}

// A create that fails after the deployment exists removes it again; otherwise every
// retry fails on AlreadyExists.
func TestAFailedCreateRemovesWhatItCreated(t *testing.T) {
	k, clientset := fakeDriver()
	failFirstVPACreate(k)

	if err := createFakeOperators(k, true); !k8s_errors.IsServiceUnavailable(err) {
		t.Fatalf("err = %v, want the autoscaler create error", err)
	}
	deploymentGone(t, k)
	if _, err := fakePVC(clientset); !k8s_errors.IsNotFound(err) {
		t.Errorf("claim get = %v, want the claim this call created deleted", err)
	}
	if err := createFakeOperators(k, true); err != nil {
		t.Fatalf("retry after the failed create: %v", err)
	}
}

// A claim the call found and reused holds the operator's state and survives the cleanup.
func TestAFailedCreateKeepsAReusedVolume(t *testing.T) {
	k, clientset := fakeDriver(existingPVC(false))
	failFirstVPACreate(k)

	if err := createFakeOperators(k, true); !k8s_errors.IsServiceUnavailable(err) {
		t.Fatalf("err = %v, want the autoscaler create error", err)
	}
	pvc, err := fakePVC(clientset)
	if err != nil || pvc.Annotations["test/origin"] != "existing" {
		t.Errorf("claim = %v, %v; want the existing claim kept", pvc, err)
	}
	if n := pvcDeletes(clientset); n != 0 {
		t.Errorf("volume deletes = %d, want 0", n)
	}
}
