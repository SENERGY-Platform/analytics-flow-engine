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
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/config"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	vpafake "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/client/clientset/versioned/fake"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const (
	fakeNamespace  = "pipelines"
	fakePipelineId = "pipe-1"
)

func init() {
	util.InitStructLogger("error")
}

var pvcResource = apiv1.SchemeGroupVersion.WithResource("persistentvolumeclaims")

func fakeDriver(objects ...runtime.Object) (*Kubernetes, *fake.Clientset) {
	clientset := fake.NewClientset(objects...)
	return &Kubernetes{
		clientset:           clientset,
		autoscalerClientset: vpafake.NewSimpleClientset(),
		r2cfg:               &config.Rancher2Config{NamespaceId: fakeNamespace},
		timescaleSecret:     TimescaleSecret{Name: "analytics-timescale", Key: "dsn"},
		pvcWait:             pvcWait{timeout: 2 * time.Second, interval: time.Millisecond},
	}, clientset
}

func fakeOperator(persist bool) pipe.Operator {
	return pipe.Operator{Id: "op-12345678", OperatorId: "adder", ImageId: "adder:1", DeploymentType: "cloud", PersistData: persist}
}

func fakeVolumeName() string {
	return getOperatorName(fakePipelineId, fakeOperator(true))[0]
}

// existingPVC is a claim already in the cluster, marked so a test can tell it from
// one CreateOperators made.
func existingPVC(terminating bool) *apiv1.PersistentVolumeClaim {
	pvc := &apiv1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name:        fakeVolumeName(),
		Namespace:   fakeNamespace,
		Annotations: map[string]string{"test/origin": "existing"},
	}}
	if terminating {
		pvc.DeletionTimestamp = &metav1.Time{Time: time.Now()}
		pvc.Finalizers = []string{"kubernetes.io/pvc-protection"}
	}
	return pvc
}

func createFakeOperators(k *Kubernetes, persist bool) error {
	return k.CreateOperators(context.Background(), fakePipelineId, []pipe.Operator{fakeOperator(persist)}, lib.PipelineConfig{PipelineId: fakePipelineId})
}

func fakeDeployment(t *testing.T, clientset *fake.Clientset) *appsv1.Deployment {
	t.Helper()
	deployment, err := clientset.AppsV1().Deployments(fakeNamespace).Get(context.Background(), "pipeline-"+fakePipelineId, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("deployment: %v", err)
	}
	return deployment
}

func fakePVC(clientset *fake.Clientset) (*apiv1.PersistentVolumeClaim, error) {
	return clientset.CoreV1().PersistentVolumeClaims(fakeNamespace).Get(context.Background(), fakeVolumeName(), metav1.GetOptions{})
}

func pvcDeletes(clientset *fake.Clientset) int {
	n := 0
	for _, action := range clientset.Actions() {
		if action.Matches("delete", "persistentvolumeclaims") {
			n++
		}
	}
	return n
}

// A ReadWriteOnce claim cannot follow a rolling update's second pod to another node.
func TestAnOperatorWithAVolumeIsRolledWithRecreate(t *testing.T) {
	k, clientset := fakeDriver()
	if err := createFakeOperators(k, true); err != nil {
		t.Fatal(err)
	}
	spec := fakeDeployment(t, clientset).Spec
	if spec.Strategy.Type != appsv1.RecreateDeploymentStrategyType || spec.Strategy.RollingUpdate != nil {
		t.Errorf("strategy = %+v, want Recreate without a rollingUpdate block", spec.Strategy)
	}
	if spec.Replicas == nil || *spec.Replicas != 1 {
		t.Errorf("replicas = %v, want 1", spec.Replicas)
	}
	if len(spec.Template.Spec.Volumes) != 1 || spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName != fakeVolumeName() {
		t.Errorf("volumes = %+v, want the operator's claim", spec.Template.Spec.Volumes)
	}
}

func TestAnOperatorWithoutAVolumeKeepsTheDefaultStrategy(t *testing.T) {
	k, clientset := fakeDriver()
	if err := createFakeOperators(k, false); err != nil {
		t.Fatal(err)
	}
	if strategy := fakeDeployment(t, clientset).Spec.Strategy; strategy.Type != "" {
		t.Errorf("strategy = %+v, want it left to the cluster default", strategy)
	}
}

// The rebuild after a restart relies on this: the claim with the operator's state is
// mounted again, not replaced.
func TestCreateOperatorsReusesAnExistingVolume(t *testing.T) {
	k, clientset := fakeDriver(existingPVC(false))
	if err := createFakeOperators(k, true); err != nil {
		t.Fatal(err)
	}
	pvc, err := fakePVC(clientset)
	if err != nil {
		t.Fatal(err)
	}
	if pvc.Annotations["test/origin"] != "existing" {
		t.Error("the existing claim was replaced")
	}
	if n := pvcDeletes(clientset); n != 0 {
		t.Errorf("volume deletes = %d, want 0", n)
	}
	fakeDeployment(t, clientset)
}

// A claim created over a terminating one would vanish with it and leave the pod
// Pending, so CreateOperators waits for it to be gone and creates a fresh one.
func TestCreateOperatorsWaitsForATerminatingVolume(t *testing.T) {
	k, clientset := fakeDriver(existingPVC(true))
	gets := 0
	clientset.PrependReactor("get", "persistentvolumeclaims", func(k8stesting.Action) (bool, runtime.Object, error) {
		gets++
		if gets == 3 {
			// The protection finalizer is released: the claim disappears.
			if err := clientset.Tracker().Delete(pvcResource, fakeNamespace, fakeVolumeName()); err != nil {
				t.Errorf("removing the terminating claim: %v", err)
			}
		}
		return false, nil, nil
	})

	if err := createFakeOperators(k, true); err != nil {
		t.Fatal(err)
	}
	if gets < 3 {
		t.Errorf("gets = %d, want the claim polled until it was gone", gets)
	}
	pvc, err := fakePVC(clientset)
	if err != nil {
		t.Fatalf("the claim was not created after the old one was gone: %v", err)
	}
	if pvc.DeletionTimestamp != nil || pvc.Annotations["test/origin"] == "existing" {
		t.Errorf("claim = %+v, want a fresh one", pvc.ObjectMeta)
	}
	fakeDeployment(t, clientset)
}

func TestCreateOperatorsGivesUpOnAVolumeThatStaysTerminating(t *testing.T) {
	k, clientset := fakeDriver(existingPVC(true))
	k.pvcWait = pvcWait{timeout: 50 * time.Millisecond, interval: 5 * time.Millisecond}

	start := time.Now()
	err := createFakeOperators(k, true)
	if !errors.Is(err, lib.ErrVolumeStillTerminating) || !strings.Contains(err.Error(), fakeVolumeName()) {
		t.Fatalf("err = %v, want ErrVolumeStillTerminating naming the volume", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("gave up after %s, want about the 50ms timeout", elapsed)
	}
	// Nothing is created that would start a pod without its claim.
	if _, err := clientset.AppsV1().Deployments(fakeNamespace).Get(context.Background(), "pipeline-"+fakePipelineId, metav1.GetOptions{}); !k8s_errors.IsNotFound(err) {
		t.Errorf("deployment get = %v, want NotFound", err)
	}
}

// A claim that cannot be created fails the start instead of a pod that stays Pending.
func TestCreateOperatorsReturnsAVolumeCreateError(t *testing.T) {
	k, clientset := fakeDriver()
	clientset.PrependReactor("create", "persistentvolumeclaims", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8s_errors.NewForbidden(pvcResource.GroupResource(), fakeVolumeName(), nil)
	})
	if err := createFakeOperators(k, true); !k8s_errors.IsForbidden(err) {
		t.Fatalf("err = %v, want the Forbidden from the create", err)
	}
	if _, err := clientset.AppsV1().Deployments(fakeNamespace).Get(context.Background(), "pipeline-"+fakePipelineId, metav1.GetOptions{}); !k8s_errors.IsNotFound(err) {
		t.Errorf("deployment get = %v, want NotFound", err)
	}
}

func TestDeleteOperatorsKeepingVolumesLeavesTheClaim(t *testing.T) {
	k, clientset := fakeDriver()
	if err := createFakeOperators(k, true); err != nil {
		t.Fatal(err)
	}
	if err := k.DeleteOperatorsKeepingVolumes(context.Background(), fakePipelineId, []pipe.Operator{fakeOperator(true)}); err != nil {
		t.Fatal(err)
	}
	if _, err := fakePVC(clientset); err != nil {
		t.Errorf("claim: %v, want it kept", err)
	}
	if n := pvcDeletes(clientset); n != 0 {
		t.Errorf("volume deletes = %d, want 0", n)
	}
	if _, err := clientset.AppsV1().Deployments(fakeNamespace).Get(context.Background(), "pipeline-"+fakePipelineId, metav1.GetOptions{}); !k8s_errors.IsNotFound(err) {
		t.Errorf("deployment get = %v, want it deleted", err)
	}
}

func TestDeleteOperatorsDeletesTheClaim(t *testing.T) {
	k, clientset := fakeDriver()
	if err := createFakeOperators(k, true); err != nil {
		t.Fatal(err)
	}
	if err := k.DeleteOperators(context.Background(), fakePipelineId, []pipe.Operator{fakeOperator(true)}); err != nil {
		t.Fatal(err)
	}
	if _, err := fakePVC(clientset); !k8s_errors.IsNotFound(err) {
		t.Errorf("claim get = %v, want it deleted", err)
	}
}

// The wait is one budget for the whole call: a claim that takes most of it to go must
// not leave the next stuck claim a full budget of its own, since the engine runs this
// in a call nobody can cancel.
func TestCreateOperatorsSharesOneWaitAcrossItsVolumes(t *testing.T) {
	second := fakeOperator(true)
	second.Id = "op-87654321"
	secondClaim := existingPVC(true)
	secondClaim.Name = getOperatorName(fakePipelineId, second)[0]
	k, clientset := fakeDriver(existingPVC(true), secondClaim)
	const budget = 600 * time.Millisecond
	k.pvcWait = pvcWait{timeout: budget, interval: 5 * time.Millisecond}

	start := time.Now()
	released := false
	clientset.PrependReactor("get", "persistentvolumeclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
		// The first claim goes after two thirds of the budget; the second never does.
		if !released && action.(k8stesting.GetAction).GetName() == fakeVolumeName() && time.Since(start) > budget*2/3 {
			released = true
			if err := clientset.Tracker().Delete(pvcResource, fakeNamespace, fakeVolumeName()); err != nil {
				t.Errorf("removing the first claim: %v", err)
			}
		}
		return false, nil, nil
	})

	err := k.CreateOperators(context.Background(), fakePipelineId, []pipe.Operator{fakeOperator(true), second}, lib.PipelineConfig{PipelineId: fakePipelineId})
	elapsed := time.Since(start)
	if !errors.Is(err, lib.ErrVolumeStillTerminating) || !strings.Contains(err.Error(), secondClaim.Name) {
		t.Fatalf("err = %v, want ErrVolumeStillTerminating for the second claim", err)
	}
	if !released {
		t.Fatal("the first claim was never released, so the test proves nothing")
	}
	if elapsed >= budget*4/3 {
		t.Errorf("waited %s, want one budget of %s for both claims", elapsed, budget)
	}
}
