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
	"testing"

	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
)

// Names of objects in the cluster; a builder that drifts from them orphans those objects.
func TestNamesMatchTheObjectsInTheCluster(t *testing.T) {
	const (
		pipelineId    = "06a798d6-cf4c-47ec-a696-0622f0d1aac0"
		pvcPipelineId = "037a1f6e-5910-4550-a843-ebb1eede01c6"
	)
	operator := pipe.Operator{Id: "d9c3f0a2-4b7e-4f1d-8a56-3e2b7c91a0f4", OperatorId: "6af19b044fbf09482b058c3b"}
	pvcOperator := pipe.Operator{Id: "bdeb8c68-1c0e-4b8a-9d1e-2f3a4b5c6d7e"}
	pvc, err := PVCName(pvcPipelineId, pvcOperator)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ kind, got, want string }{
		{"deployment", DeploymentName(pipelineId), "pipeline-06a798d6-cf4c-47ec-a696-0622f0d1aac0"},
		{"vpa", VPAName(pipelineId), "pipeline-06a798d6-cf4c-47ec-a696-0622f0d1aac0-vpa"},
		{"container", ContainerName(operator), "6af19b044fbf09482b058c3b--d9c3f0a2-4b7e-4f1d-8a56-3e2b7c91a0f4"},
		{"checkpoint", CheckpointName(pipelineId, operator), "pipeline-06a798d6-cf4c-47ec-a696-0622f0d1aac0-vpa-6af19b044fbf09482b058c3b--d9c3f0a2-4b7e-4f1d-8a56-3e2b7c91a0f4"},
		{"pvc", pvc, "operator-037a1f6e-5910-4550-a843-ebb1eede01c6-bdeb8c68"},
	} {
		if c.got != c.want {
			t.Errorf("%s name = %q, want %q", c.kind, c.got, c.want)
		}
	}
}

func TestPipelineIdsParseBackFromEveryName(t *testing.T) {
	const id = "06a798d6-cf4c-47ec-a696-0622f0d1aac0"
	operator := pipe.Operator{Id: "d9c3f0a2-4b7e-4f1d-8a56-3e2b7c91a0f4", OperatorId: "6af19b044fbf09482b058c3b"}
	pvc, err := PVCName(id, operator)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		kind  string
		name  string
		parse func(string) (string, bool)
	}{
		{"deployment", DeploymentName(id), PipelineIdFromDeploymentName},
		{"vpa", VPAName(id), PipelineIdFromVPAName},
		{"checkpoint", CheckpointName(id, operator), PipelineIdFromCheckpointName},
		{"pvc", pvc, PipelineIdFromPVCName},
	} {
		if got, ok := c.parse(c.name); !ok || got != id {
			t.Errorf("%s %q parsed to %q, %v; want %q", c.kind, c.name, got, ok, id)
		}
	}
}

// Anything the engine did not name is refused, so the orphan cleanup never takes a
// foreign object for a pipeline's.
func TestNamesTheEngineDidNotProduceAreRefused(t *testing.T) {
	const id = "06a798d6-cf4c-47ec-a696-0622f0d1aac0"
	for _, c := range []struct {
		kind  string
		parse func(string) (string, bool)
		names []string
	}{
		{"deployment", PipelineIdFromDeploymentName, []string{
			"", "pipeline-", "pipeline-x", "pipeline-test-pipe-12345678",
			"pipeline-06A798D6-CF4C-47EC-A696-0622F0D1AAC0",
			"pipeline-06a798d6cf4c47eca6960622f0d1aac0",
			"pipeline-{06a798d6-cf4c-47ec-a696-0622f0d1aac0}",
			"pipeline-" + id + "-vpa",
			"deployment-" + id,
			"xpipeline-" + id,
		}},
		{"vpa", PipelineIdFromVPAName, []string{
			"", "-vpa", "pipeline--vpa", "pipeline-" + id, "pipeline-" + id + "-vpa-x", "pipeline-x-vpa",
		}},
		{"checkpoint", PipelineIdFromCheckpointName, []string{
			"", "pipeline-" + id + "-vpa", "pipeline-" + id + "-vpa-", "pipeline-" + id + "-vpax-c",
			"pipeline-" + id, "pipeline-x-vpa-c", "operator-" + id + "-vpa-c",
		}},
		{"pvc", PipelineIdFromPVCName, []string{
			"", "operator-", "operator-" + id, "operator-" + id + "-", "operator-" + id + "-bdeb8c6",
			"operator-" + id + "-bdeb8c689", "operator-" + id + "xbdeb8c68", "operator-x-bdeb8c68",
			"pipeline-" + id + "-bdeb8c68",
		}},
	} {
		for _, name := range c.names {
			if got, ok := c.parse(name); ok {
				t.Errorf("%s %q parsed to %q, want it refused", c.kind, name, got)
			}
		}
	}
}

// A volume name carries the first eight characters of the operator id; a shorter id
// is an error rather than a slice panic.
func TestPVCNameRefusesAShortOperatorId(t *testing.T) {
	for _, operatorId := range []string{"", "1234567"} {
		if name, err := PVCName("06a798d6-cf4c-47ec-a696-0622f0d1aac0", pipe.Operator{Id: operatorId}); err == nil {
			t.Errorf("operator id %q gave volume %q, want an error", operatorId, name)
		}
	}
}
