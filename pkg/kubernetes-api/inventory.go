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
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ApplicationIdEnvVar is the env var that carries an operator's Kafka Streams
// application id, and applicationIdPrefix what CreateOperators puts before the id.
const (
	ApplicationIdEnvVar = "CONFIG_APPLICATION_ID"
	applicationIdPrefix = "analytics-"
)

// Resource is one object in the pipelines namespace.
type Resource struct {
	Name    string
	Created time.Time
}

// Deployment is a deployment with the application ids its containers run under,
// without the "analytics-" prefix.
type Deployment struct {
	Resource
	ApplicationIds []string
}

// Inventory is everything in the pipelines namespace the engine creates for a
// pipeline, whether or not its name parses as one of the engine's.
type Inventory struct {
	Deployments []Deployment
	VPAs        []Resource
	Checkpoints []Resource
	PVCs        []Resource
}

// Inventory lists the deployments, autoscalers, autoscaler checkpoints and volumes of
// the pipelines namespace.
func (k *Kubernetes) Inventory(ctx context.Context) (Inventory, error) {
	ns := k.kubeCfg.NamespaceId
	var inv Inventory

	deployments, err := k.clientset.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return Inventory{}, fmt.Errorf("list deployments: %w", err)
	}
	for _, d := range deployments.Items {
		dep := Deployment{Resource: resourceOf(d.ObjectMeta)}
		for _, c := range d.Spec.Template.Spec.Containers {
			for _, env := range c.Env {
				if env.Name != ApplicationIdEnvVar {
					continue
				}
				if id, ok := strings.CutPrefix(env.Value, applicationIdPrefix); ok && id != "" {
					dep.ApplicationIds = append(dep.ApplicationIds, id)
				}
			}
		}
		inv.Deployments = append(inv.Deployments, dep)
	}

	vpas, err := k.autoscalerClientset.AutoscalingV1().VerticalPodAutoscalers(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return Inventory{}, fmt.Errorf("list autoscalers: %w", err)
	}
	for _, v := range vpas.Items {
		inv.VPAs = append(inv.VPAs, resourceOf(v.ObjectMeta))
	}

	checkpoints, err := k.autoscalerClientset.AutoscalingV1().VerticalPodAutoscalerCheckpoints(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return Inventory{}, fmt.Errorf("list autoscaler checkpoints: %w", err)
	}
	for _, c := range checkpoints.Items {
		inv.Checkpoints = append(inv.Checkpoints, resourceOf(c.ObjectMeta))
	}

	pvcs, err := k.clientset.CoreV1().PersistentVolumeClaims(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return Inventory{}, fmt.Errorf("list volumes: %w", err)
	}
	for _, p := range pvcs.Items {
		inv.PVCs = append(inv.PVCs, resourceOf(p.ObjectMeta))
	}
	return inv, nil
}

func resourceOf(meta metav1.ObjectMeta) Resource {
	return Resource{Name: meta.Name, Created: meta.CreationTimestamp.Time}
}
