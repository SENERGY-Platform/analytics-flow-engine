/*
 * Copyright 2026 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
	"slices"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	vpav1 "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	vpafake "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/client/clientset/versioned/fake"
	k8stesting "k8s.io/client-go/testing"
)

func meta(name, namespace string, created time.Time) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name, Namespace: namespace, CreationTimestamp: metav1.Time{Time: created}}
}

func deploymentWithEnv(name, namespace string, created time.Time, appIds ...string) *appsv1.Deployment {
	var containers []apiv1.Container
	for _, id := range appIds {
		containers = append(containers, apiv1.Container{Env: []apiv1.EnvVar{
			{Name: "PIPELINE_ID", Value: "x"},
			{Name: ApplicationIdEnvVar, Value: id},
		}})
	}
	return &appsv1.Deployment{ObjectMeta: meta(name, namespace, created),
		Spec: appsv1.DeploymentSpec{Template: apiv1.PodTemplateSpec{Spec: apiv1.PodSpec{Containers: containers}}}}
}

// The listing names every object of the pipelines namespace with its creation time,
// and the application ids of a deployment without the prefix the engine adds.
func TestInventoryListsThePipelinesNamespace(t *testing.T) {
	created := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	k, _ := fakeDriver(
		deploymentWithEnv("pipeline-a", fakeNamespace, created, "analytics-11111111-1111-1111-1111-111111111111", "ode-2", "analytics-"),
		deploymentWithEnv("pipeline-elsewhere", "other", created),
		&apiv1.PersistentVolumeClaim{ObjectMeta: meta("operator-a-12345678", fakeNamespace, created.Add(time.Minute))},
	)
	k.autoscalerClientset = vpafake.NewSimpleClientset(
		&vpav1.VerticalPodAutoscaler{ObjectMeta: meta("pipeline-a-vpa", fakeNamespace, created)},
		&vpav1.VerticalPodAutoscalerCheckpoint{ObjectMeta: meta("pipeline-a-vpa-op--a", fakeNamespace, created.Add(time.Hour))},
		&vpav1.VerticalPodAutoscalerCheckpoint{ObjectMeta: meta("pipeline-b-vpa-op--b", "other", created)},
	)

	inv, err := k.Inventory(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(inv.Deployments) != 1 || inv.Deployments[0].Name != "pipeline-a" || !inv.Deployments[0].Created.Equal(created) {
		t.Fatalf("deployments = %+v, want pipeline-a only", inv.Deployments)
	}
	if ids := inv.Deployments[0].ApplicationIds; !slices.Equal(ids, []string{"11111111-1111-1111-1111-111111111111"}) {
		t.Errorf("application ids = %v, want the analytics one without its prefix", ids)
	}
	if len(inv.VPAs) != 1 || inv.VPAs[0].Name != "pipeline-a-vpa" {
		t.Errorf("autoscalers = %+v", inv.VPAs)
	}
	if len(inv.Checkpoints) != 1 || !inv.Checkpoints[0].Created.Equal(created.Add(time.Hour)) {
		t.Errorf("checkpoints = %+v, want the one in the namespace with its creation time", inv.Checkpoints)
	}
	if len(inv.PVCs) != 1 || inv.PVCs[0].Name != "operator-a-12345678" {
		t.Errorf("volumes = %+v", inv.PVCs)
	}
}

// A partial listing would make the objects it misses look absent, so any failed list
// fails the whole call.
func TestInventoryFailsOnAnyFailedList(t *testing.T) {
	k, _ := fakeDriver()
	vpa := vpafake.NewSimpleClientset()
	vpa.PrependReactor("list", "verticalpodautoscalercheckpoints", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("forbidden")
	})
	k.autoscalerClientset = vpa
	if _, err := k.Inventory(context.Background()); err == nil {
		t.Fatal("err = nil, want the failed checkpoint list")
	}
}
