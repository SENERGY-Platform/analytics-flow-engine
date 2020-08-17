/*
 * Copyright 2018 InfAI (CC SES)
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

package rancher_api

type Request struct {
	Type                   string `json:"type,omitempty"`
	Name                   string `json:"name,omitempty"`
	StackId                string `json:"stackId,omitempty"`
	Scale                  int    `json:"scale,omitempty"`
	StartOnCreate          bool   `json:"startOnCreate,omitempty"`
	LaunchConfig           `json:"launchConfig,omitempty"`
	SecondaryLaunchConfigs []LaunchConfig `json:"secondaryLaunchConfigs,omitempty"`
}

type LaunchConfig struct {
	ImageUuid   string            `json:"imageUuid,omitempty"`
	Environment map[string]string `json:"environment"`
	Labels      map[string]string `json:"labels"`
	Command     []string          `json:"command,omitempty"`
	Name        string            `json:"name,omitempty"`
}

type ServiceCollection struct {
	Data []Service `json:"data"`
}

type Service struct {
	Id           string `json:"id"`
	LaunchConfig `json:"launchConfig,omitempty"`
}
