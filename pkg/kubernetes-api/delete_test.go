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
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
	appsv1 "k8s.io/api/apps/v1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	vpafake "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/client/clientset/versioned/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
)

func fakeDeploymentObject() *appsv1.Deployment {
	return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: DeploymentName(fakePipelineId), Namespace: fakeNamespace}}
}

func deploymentGone(t *testing.T, k *Kubernetes) {
	t.Helper()
	if _, err := k.clientset.AppsV1().Deployments(fakeNamespace).Get(context.Background(), DeploymentName(fakePipelineId), metav1.GetOptions{}); !k8s_errors.IsNotFound(err) {
		t.Errorf("deployment get = %v, want it deleted", err)
	}
}

// The live incident: a request found the deployment of a concurrent one but not yet
// its autoscaler, and the NotFound of the autoscaler delete failed the update.
func TestDeleteOperatorsSucceedsWhenTheAutoscalerIsMissing(t *testing.T) {
	k, _ := fakeDriver(fakeDeploymentObject())
	if err := k.DeleteOperators(context.Background(), fakePipelineId, []pipe.Operator{fakeOperator(false)}); err != nil {
		t.Fatalf("err = %v, want none for a missing autoscaler", err)
	}
	deploymentGone(t, k)
}

// Deleting what is already gone succeeds, so a failed delete can be retried.
func TestDeleteOperatorsIsIdempotent(t *testing.T) {
	k, _ := fakeDriver()
	if err := createFakeOperators(k, true); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		if err := k.DeleteOperators(context.Background(), fakePipelineId, []pipe.Operator{fakeOperator(true)}); err != nil {
			t.Fatalf("delete %d: %v", i+1, err)
		}
	}
}

// A volume that cannot be deleted fails the call; the remaining steps still run so a
// retry only has the volume left.
func TestDeleteOperatorsReturnsAVolumeDeleteError(t *testing.T) {
	k, clientset := fakeDriver()
	if err := createFakeOperators(k, true); err != nil {
		t.Fatal(err)
	}
	clientset.PrependReactor("delete", "persistentvolumeclaims", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8s_errors.NewForbidden(pvcResource.GroupResource(), fakeVolumeName(), nil)
	})
	err := k.DeleteOperators(context.Background(), fakePipelineId, []pipe.Operator{fakeOperator(true)})
	if !k8s_errors.IsForbidden(err) {
		t.Fatalf("err = %v, want the Forbidden of the volume delete", err)
	}
	deploymentGone(t, k)
}

func TestDeleteOperatorsReturnsAnAutoscalerDeleteError(t *testing.T) {
	k, _ := fakeDriver()
	if err := createFakeOperators(k, false); err != nil {
		t.Fatal(err)
	}
	vpa := k.autoscalerClientset.(*vpafake.Clientset)
	vpa.PrependReactor("delete", "verticalpodautoscalers", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8s_errors.NewServiceUnavailable("autoscaler api down")
	})
	if err := k.DeleteOperators(context.Background(), fakePipelineId, []pipe.Operator{fakeOperator(false)}); !k8s_errors.IsServiceUnavailable(err) {
		t.Fatalf("err = %v, want the autoscaler delete error", err)
	}
}

// An empty name must not reach the API, where it would address no single object.
func TestDeletePrimitivesRefuseAnEmptyName(t *testing.T) {
	k, clientset := fakeDriver()
	for kind, del := range map[string]func(context.Context, string) error{
		"deployment": k.DeleteDeployment, "vpa": k.DeleteVPA, "checkpoint": k.DeleteVPACheckpoint, "pvc": k.DeletePVC,
	} {
		if err := del(context.Background(), ""); err == nil {
			t.Errorf("%s: deleting an empty name succeeded", kind)
		}
	}
	if n := len(clientset.Actions()) + len(k.autoscalerClientset.(*vpafake.Clientset).Actions()); n != 0 {
		t.Errorf("API calls = %d, want 0", n)
	}
}

// A pipeline without deployment is a 404 for the caller, not a 500.
func TestGetPipelineStatusWithoutDeploymentIsNotFound(t *testing.T) {
	k, _ := fakeDriver()
	_, err := k.GetPipelineStatus(context.Background(), fakePipelineId)
	var notFound *lib.NotFoundError
	if !errors.As(err, &notFound) || util.GetStatusCode(err) != http.StatusNotFound {
		t.Fatalf("err = %v, want a lib.NotFoundError", err)
	}
}

func TestGetPipelineStatusReturnsOtherErrorsAsTheyAre(t *testing.T) {
	k, clientset := fakeDriver()
	clientset.PrependReactor("get", "deployments", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8s_errors.NewServiceUnavailable("api down")
	})
	_, err := k.GetPipelineStatus(context.Background(), fakePipelineId)
	if !k8s_errors.IsServiceUnavailable(err) || errors.As(err, new(*lib.NotFoundError)) {
		t.Fatalf("err = %v, want the unavailable error and no NotFoundError", err)
	}
}

// Building the clients registers no flag, so it can run more than once in a process.
func TestNewClientsCanBeCalledTwice(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	const kubeconfig = `apiVersion: v1
kind: Config
clusters: [{name: c, cluster: {server: "https://127.0.0.1:1"}}]
users: [{name: u, user: {}}]
contexts: [{name: x, context: {cluster: c, user: u}}]
current-context: x
`
	if err := os.WriteFile(path, []byte(kubeconfig), 0o600); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		clientset, autoscalerClientset, err := NewClients(path)
		if err != nil || clientset == nil || autoscalerClientset == nil {
			t.Fatalf("call %d: clients %v %v, err %v", i+1, clientset, autoscalerClientset, err)
		}
	}
}

// A config the clients refuse is an error, not a nil clientset used later.
func TestClientsForRefusesAnInvalidConfig(t *testing.T) {
	clientset, autoscalerClientset, err := clientsFor(&rest.Config{Host: "https://127.0.0.1:1", QPS: 1, Burst: 0})
	if err == nil || clientset != nil || autoscalerClientset != nil {
		t.Fatalf("clients %v %v, err %v; want an error and no clients", clientset, autoscalerClientset, err)
	}
}
