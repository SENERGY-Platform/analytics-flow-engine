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

package config

import (
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
)

// The resources every operator container gets unless OperatorResources names its image.
const (
	DefaultOperatorMemoryLimit   = "512Mi"
	DefaultOperatorMemoryRequest = "128Mi"
	DefaultOperatorCPULimit      = "500m"
	DefaultOperatorCPURequest    = "100m"
)

// OperatorResource overrides the default resources of one operator image. An
// empty field keeps the default; the values are Kubernetes quantities.
type OperatorResource struct {
	MemoryLimit   string `json:"memory_limit"`
	MemoryRequest string `json:"memory_request"`
	CPULimit      string `json:"cpu_limit"`
	CPURequest    string `json:"cpu_request"`
}

// ContainerResources is the resolved set of quantities for one operator container.
type ContainerResources struct {
	MemoryLimit   string
	MemoryRequest string
	CPULimit      string
	CPURequest    string
}

// ImageRepository returns the image id without its tag or digest. Only a ':'
// after the last '/' is a tag, so a registry port stays part of the repository.
func ImageRepository(image string) string {
	if i := strings.IndexByte(image, '@'); i >= 0 {
		image = image[:i]
	}
	if i := strings.LastIndexByte(image, ':'); i > strings.LastIndexByte(image, '/') {
		image = image[:i]
	}
	return image
}

// ResourcesFor returns the resources for an operator image: the entry of its
// repository overrides the defaults field by field.
func ResourcesFor(image string, overrides map[string]OperatorResource) ContainerResources {
	res := ContainerResources{
		MemoryLimit:   DefaultOperatorMemoryLimit,
		MemoryRequest: DefaultOperatorMemoryRequest,
		CPULimit:      DefaultOperatorCPULimit,
		CPURequest:    DefaultOperatorCPURequest,
	}
	o, ok := overrides[ImageRepository(image)]
	if !ok {
		return res
	}
	if o.MemoryLimit != "" {
		res.MemoryLimit = o.MemoryLimit
	}
	if o.MemoryRequest != "" {
		res.MemoryRequest = o.MemoryRequest
	}
	if o.CPULimit != "" {
		res.CPULimit = o.CPULimit
	}
	if o.CPURequest != "" {
		res.CPURequest = o.CPURequest
	}
	return res
}

// ValidateOperatorResources rejects what the cluster would refuse at deployment
// time: keys that can never match an image repository, unparsable quantities, and
// a request above its limit once the defaults are filled in.
func ValidateOperatorResources(overrides map[string]OperatorResource) error {
	keys := make([]string, 0, len(overrides))
	for k := range overrides {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if key == "" {
			return fmt.Errorf("operator_resources: empty image key")
		}
		if ImageRepository(key) != key {
			return fmt.Errorf("operator_resources: key %q must be an image repository without tag or digest", key)
		}
		res := ResourcesFor(key, overrides)
		for _, pair := range [][2]string{
			{res.MemoryRequest, res.MemoryLimit},
			{res.CPURequest, res.CPULimit},
		} {
			request, limit, err := parseQuantities(pair[0], pair[1])
			if err != nil {
				return fmt.Errorf("operator_resources: %q: %w", key, err)
			}
			// a zero or negative quantity is never a meaningful operator setting
			if request.Sign() < 0 {
				return fmt.Errorf("operator_resources: %q: request %s must not be negative", key, pair[0])
			}
			if limit.Sign() <= 0 {
				return fmt.Errorf("operator_resources: %q: limit %s must be above zero", key, pair[1])
			}
			if request.Cmp(limit) > 0 {
				return fmt.Errorf("operator_resources: %q: request %s exceeds limit %s", key, pair[0], pair[1])
			}
		}
	}
	return nil
}

func parseQuantities(request, limit string) (r, l resource.Quantity, err error) {
	if r, err = resource.ParseQuantity(request); err != nil {
		return r, l, fmt.Errorf("invalid quantity %q: %w", request, err)
	}
	if l, err = resource.ParseQuantity(limit); err != nil {
		return r, l, fmt.Errorf("invalid quantity %q: %w", limit, err)
	}
	return r, l, nil
}
