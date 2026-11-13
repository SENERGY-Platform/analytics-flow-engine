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
	"errors"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"
)

var deploymentResource = appsv1.SchemeGroupVersion.WithResource("deployments")

// terminatingDeployment is the previous deployment while its foreground delete waits
// for the pods.
func terminatingDeployment() *appsv1.Deployment {
	d := fakeDeploymentObject()
	d.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	d.Finalizers = []string{metav1.FinalizerDeleteDependents}
	return d
}

// An update deletes the old deployment in the foreground and creates the new one right
// after; the create waits for the old one instead of failing on AlreadyExists.
func TestCreateOperatorsWaitsForATerminatingDeployment(t *testing.T) {
	k, clientset := fakeDriver(terminatingDeployment())
	gets := 0
	clientset.PrependReactor("get", "deployments", func(k8stesting.Action) (bool, runtime.Object, error) {
		gets++
		if gets == 3 {
			if err := clientset.Tracker().Delete(deploymentResource, fakeNamespace, DeploymentName(fakePipelineId)); err != nil {
				t.Errorf("removing the terminating deployment: %v", err)
			}
		}
		return false, nil, nil
	})

	if err := createFakeOperators(k, false); err != nil {
		t.Fatal(err)
	}
	if gets < 3 {
		t.Errorf("gets = %d, want the deployment polled until it was gone", gets)
	}
	if d := fakeDeployment(t, clientset); d.DeletionTimestamp != nil {
		t.Error("the deployment is still the terminating one")
	}
}

func TestCreateOperatorsGivesUpOnADeploymentThatStaysTerminating(t *testing.T) {
	k, clientset := fakeDriver(terminatingDeployment())
	k.pvcWait = pvcWait{timeout: 50 * time.Millisecond, interval: 5 * time.Millisecond}

	start := time.Now()
	err := createFakeOperators(k, false)
	if !errors.Is(err, ErrDeploymentStillTerminating) {
		t.Fatalf("err = %v, want ErrDeploymentStillTerminating", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("gave up after %s, want about the 50ms timeout", elapsed)
	}
	if d := fakeDeployment(t, clientset); d.DeletionTimestamp == nil {
		t.Error("want the terminating deployment left as it was")
	}
}

// A live deployment of the same name is not this call's to wait for or to delete.
func TestCreateOperatorsLeavesALiveDeploymentAlone(t *testing.T) {
	k, clientset := fakeDriver(fakeDeploymentObject())

	start := time.Now()
	if err := createFakeOperators(k, false); !k8s_errors.IsAlreadyExists(err) {
		t.Fatalf("err = %v, want AlreadyExists", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("returned after %s, want at once", elapsed)
	}
	fakeDeployment(t, clientset)
	for _, action := range clientset.Actions() {
		if action.Matches("delete", "deployments") {
			t.Error("the failed create deleted a deployment it did not create")
		}
	}
}
