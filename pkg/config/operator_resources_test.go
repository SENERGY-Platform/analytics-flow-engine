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
	"strings"
	"testing"
)

const forecastImage = "ghcr.io/senergy-platform/consumption-forecast-operator"

var defaultResources = ContainerResources{
	MemoryLimit:   "512Mi",
	MemoryRequest: "128Mi",
	CPULimit:      "500m",
	CPURequest:    "25m",
}

func TestResourcesFor(t *testing.T) {
	full := OperatorResource{MemoryLimit: "2Gi", MemoryRequest: "1Gi", CPULimit: "2", CPURequest: "250m"}
	overrides := map[string]OperatorResource{
		forecastImage:                       full,
		"registry.example.org:5000/team/op": {MemoryLimit: "1Gi"},
	}
	fullResources := ContainerResources{MemoryLimit: "2Gi", MemoryRequest: "1Gi", CPULimit: "2", CPURequest: "250m"}

	tests := []struct {
		name      string
		image     string
		overrides map[string]OperatorResource
		want      ContainerResources
	}{
		{"no overrides", forecastImage + ":prod", nil, defaultResources},
		{"unknown image", "ghcr.io/senergy-platform/other:prod", overrides, defaultResources},
		{"full entry with tag", forecastImage + ":prod", overrides, fullResources},
		{"full entry without tag", forecastImage, overrides, fullResources},
		{"full entry with digest", forecastImage + "@sha256:0123abcd", overrides, fullResources},
		{"full entry with tag and digest", forecastImage + ":prod@sha256:0123abcd", overrides, fullResources},
		{
			"partial entry keeps the other defaults, registry port kept",
			"registry.example.org:5000/team/op:1.2", overrides,
			ContainerResources{MemoryLimit: "1Gi", MemoryRequest: "128Mi", CPULimit: "500m", CPURequest: "25m"},
		},
		{"registry port is not a tag", "registry.example.org:5000/team/other", overrides, defaultResources},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ResourcesFor(tt.image, tt.overrides); got != tt.want {
				t.Errorf("ResourcesFor(%q) = %+v, want %+v", tt.image, got, tt.want)
			}
		})
	}
}

func TestImageRepository(t *testing.T) {
	tests := map[string]string{
		"nginx":                              "nginx",
		"nginx:1.12":                         "nginx",
		"localhost:5000/op":                  "localhost:5000/op",
		"localhost:5000/op:dev":              "localhost:5000/op",
		"ghcr.io/a/b@sha256:ab:cd":           "ghcr.io/a/b",
		"localhost:5000/a/b:t@sha256:abcdef": "localhost:5000/a/b",
	}
	for in, want := range tests {
		if got := ImageRepository(in); got != want {
			t.Errorf("ImageRepository(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestValidateOperatorResources(t *testing.T) {
	tests := []struct {
		name      string
		overrides map[string]OperatorResource
		wantErr   string
	}{
		{"none", nil, ""},
		{"valid", map[string]OperatorResource{forecastImage: {MemoryLimit: "2Gi", MemoryRequest: "1Gi", CPULimit: "1"}}, ""},
		{"request equal to limit", map[string]OperatorResource{forecastImage: {MemoryLimit: "1Gi", MemoryRequest: "1Gi"}}, ""},
		{"bad memory quantity", map[string]OperatorResource{forecastImage: {MemoryLimit: "lots"}}, "invalid quantity"},
		{"bad cpu quantity", map[string]OperatorResource{forecastImage: {CPURequest: "5 cores"}}, "invalid quantity"},
		{"request above explicit limit", map[string]OperatorResource{forecastImage: {MemoryLimit: "1Gi", MemoryRequest: "2Gi"}}, "exceeds limit"},
		{"request above the default limit", map[string]OperatorResource{forecastImage: {MemoryRequest: "1Gi"}}, "exceeds limit"},
		{"cpu request above the default limit", map[string]OperatorResource{forecastImage: {CPURequest: "1"}}, "exceeds limit"},
		{"limit below the default request", map[string]OperatorResource{forecastImage: {MemoryLimit: "64Mi"}}, "exceeds limit"},
		{"key with tag", map[string]OperatorResource{forecastImage + ":prod": {MemoryLimit: "1Gi"}}, "without tag or digest"},
		{"key with digest", map[string]OperatorResource{forecastImage + "@sha256:abcd": {MemoryLimit: "1Gi"}}, "without tag or digest"},
		{"key with registry port is fine", map[string]OperatorResource{"localhost:5000/op": {MemoryLimit: "1Gi"}}, ""},
		{"empty key", map[string]OperatorResource{"": {MemoryLimit: "1Gi"}}, "empty image key"},
		{"negative request", map[string]OperatorResource{forecastImage: {MemoryRequest: "-1Gi"}}, "must not be negative"},
		{"zero limit", map[string]OperatorResource{forecastImage: {CPULimit: "0", CPURequest: "0"}}, "must be above zero"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateOperatorResources(tt.overrides)
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %v, want one containing %q", err, tt.wantErr)
			}
		})
	}
}
