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
	"fmt"
	"strings"

	pipe_lib "github.com/SENERGY-Platform/analytics-pipeline/lib"
	"github.com/google/uuid"
)

// The names below are a contract with the objects already in the cluster: a changed
// name orphans every existing deployment, autoscaler, checkpoint and volume.
const (
	deploymentPrefix = "pipeline-"
	vpaSuffix        = "-vpa"
	pvcPrefix        = "operator-"
	// pvcOperatorIdLength is how much of the operator id a volume name carries.
	pvcOperatorIdLength = 8
	// canonicalUUIDLength is the length of a pipeline id as the registry assigns it.
	canonicalUUIDLength = 36
)

// DeploymentName is the name of the deployment running a pipeline's cloud operators.
func DeploymentName(pipelineId string) string {
	return deploymentPrefix + pipelineId
}

// VPAName is the name of the VerticalPodAutoscaler of a pipeline's deployment.
func VPAName(pipelineId string) string {
	return DeploymentName(pipelineId) + vpaSuffix
}

// ContainerName is the name of an operator's container in the pipeline deployment.
func ContainerName(operator pipe_lib.Operator) string {
	return operator.OperatorId + "--" + operator.Id
}

// CheckpointName is the name the VPA recommender gives the checkpoint of one
// container: the autoscaler name and the container name, joined by a hyphen.
func CheckpointName(pipelineId string, operator pipe_lib.Operator) string {
	return VPAName(pipelineId) + "-" + ContainerName(operator)
}

// PVCName is the name of an operator's volume. It carries only the first bytes of the
// operator id, so an id shorter than that has no volume name.
func PVCName(pipelineId string, operator pipe_lib.Operator) (string, error) {
	if len(operator.Id) < pvcOperatorIdLength {
		return "", fmt.Errorf("operator id %q is shorter than %d characters and names no volume", operator.Id, pvcOperatorIdLength)
	}
	return pvcPrefix + pipelineId + "-" + operator.Id[:pvcOperatorIdLength], nil
}

// PipelineIdFromDeploymentName returns the pipeline id of a deployment name, and false
// for a name the engine does not produce.
func PipelineIdFromDeploymentName(name string) (string, bool) {
	rest, ok := strings.CutPrefix(name, deploymentPrefix)
	if !ok || !isPipelineId(rest) {
		return "", false
	}
	return rest, true
}

// PipelineIdFromVPAName returns the pipeline id of an autoscaler name.
func PipelineIdFromVPAName(name string) (string, bool) {
	rest, ok := strings.CutSuffix(name, vpaSuffix)
	if !ok {
		return "", false
	}
	return PipelineIdFromDeploymentName(rest)
}

// PipelineIdFromCheckpointName returns the pipeline id of a VPA checkpoint name. The
// container part is not checked beyond being present: it comes from the operator.
func PipelineIdFromCheckpointName(name string) (string, bool) {
	rest, ok := strings.CutPrefix(name, deploymentPrefix)
	if !ok || len(rest) < canonicalUUIDLength {
		return "", false
	}
	id, container := rest[:canonicalUUIDLength], rest[canonicalUUIDLength:]
	container, ok = strings.CutPrefix(container, vpaSuffix+"-")
	if !ok || container == "" || !isPipelineId(id) {
		return "", false
	}
	return id, true
}

// PipelineIdFromPVCName returns the pipeline id of an operator volume name.
func PipelineIdFromPVCName(name string) (string, bool) {
	rest, ok := strings.CutPrefix(name, pvcPrefix)
	if !ok || len(rest) != canonicalUUIDLength+1+pvcOperatorIdLength {
		return "", false
	}
	id, sep := rest[:canonicalUUIDLength], rest[canonicalUUIDLength]
	if sep != '-' || !isPipelineId(id) {
		return "", false
	}
	return id, true
}

// isPipelineId accepts only the lower-case hyphenated form, so that parsing a name and
// building it again from the id gives the same bytes.
func isPipelineId(s string) bool {
	if len(s) != canonicalUUIDLength {
		return false
	}
	id, err := uuid.Parse(s)
	return err == nil && id.String() == s
}
