/*
 * Copyright 2025 InfAI (CC SES)
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
	"flag"
	"path/filepath"

	"k8s.io/client-go/util/homedir"
)

var ConfPath string

// KubeconfigPath is the kubeconfig the Kubernetes driver uses in debug mode.
var KubeconfigPath string

func ParseFlags() {
	flag.StringVar(&ConfPath, "config", "", "path to config JSON file")
	flag.StringVar(&KubeconfigPath, "kubeconfig", DefaultKubeconfigPath(), "kubeconfig used in debug mode")
	flag.Parse()
	return
}

// DefaultKubeconfigPath is ~/.kube/config, or empty without a home directory.
func DefaultKubeconfigPath() string {
	if home := homedir.HomeDir(); home != "" {
		return filepath.Join(home, ".kube", "config")
	}
	return ""
}
