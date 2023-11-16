/*
 * Copyright 2019 InfAI (CC SES)
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

package rancher2_api

type WorkloadRequest struct {
	Name        string            `json:"name,omitempty"`
	NamespaceId string            `json:"namespaceId,omitempty"`
	Containers  []Container       `json:"containers,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Selector    Selector          `json:"selector,omitempty"`
	Scheduling  Scheduling        `json:"scheduling,omitempty"`
	Volumes     []Volume          `json:"volumes,omitempty"`
}

type Container struct {
	Image           string             `json:"image,omitempty"`
	Name            string             `json:"name,omitempty"`
	Env             []Env              `json:"env,omitempty"`
	ImagePullPolicy string             `json:"imagePullPolicy,omitempty"`
	Command         []string           `json:"command,omitempty"`
	Labels          map[string]string  `json:"labels,omitempty"`
	VolumeMounts    []VolumeMount      `json:"volumeMounts,omitempty"`
	Resources       ContainerResources `json:"resources,omitempty"`
}

type ContainerResources struct {
	Limits   map[string]string `json:"limits,omitempty"`
	Requests map[string]string `json:"requests,omitempty"`
}

type Volume struct {
	Name                  string                `json:"name,omitempty"`
	PersistentVolumeClaim PersistentVolumeClaim `json:"persistentVolumeClaim,omitempty"`
}

type PersistentVolumeClaim struct {
	PersistentVolumeClaimId string `json:"persistentVolumeClaimId,omitempty"`
}

type VolumeMount struct {
	Name      string `json:"name,omitempty"`
	MountPath string `json:"mountPath,omitempty"`
}

type Env struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type Selector struct {
	MatchLabels map[string]string `json:"matchLabels,omitempty"`
}

type Scheduling struct {
	Node      Node   `json:"node,omitempty"`
	Scheduler string `json:"scheduler,omitempty"`
}

type Node struct {
	RequireAll []string `json:"requireAll,omitempty"`
}

type VolumeClaimRequest struct {
	Name           string    `json:"name,omitempty"`
	NamespaceId    string    `json:"namespaceId,omitempty"`
	AccessModes    []string  `json:"accessModes,omitempty"`
	StorageClassId string    `json:"storageClassId,omitempty"`
	Resources      Resources `json:"resources,omitempty"`
}

type Resources struct {
	Requests map[string]string `json:"requests,omitempty"`
}

type AutoscalingRequest struct {
	ApiVersion string                     `json:"apiVersion,omitempty"`
	Kind       string                     `json:"kind,omitempty"`
	Metadata   AutoscalingRequestMetadata `json:"metadata,omitempty"`
	Spec       AutoscalingRequestSpec     `json:"spec,omitempty"`
}

type AutoscalingRequestMetadata struct {
	Name      string `json:"name,omitempty"`
	Namespace string `json:"namespace,omitempty"`
}

type AutoscalingRequestSpec struct {
	TargetRef    AutoscalingRequestTargetRef    `json:"targetRef,omitempty"`
	UpdatePolicy AutoscalingRequestUpdatePolicy `json:"updatePolicy,omitempty"`
}

type AutoscalingRequestTargetRef struct {
	ApiVersion string `json:"apiVersion,omitempty"`
	Kind       string `json:"kind,omitempty"`
	Name       string `json:"name,omitempty"`
}

type AutoscalingRequestUpdatePolicy struct {
	UpdateMode string `json:"updateMode,omitempty"`
}
