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

package service

import (
	"context"
	"testing"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
	"github.com/google/uuid"
)

func persistingCloudOperator() pipe.Operator {
	return pipe.Operator{Id: "op-12345678", DeploymentType: "cloud", PersistData: true}
}

// A pipeline whose deployment is missing at startup is rebuilt without deleting its
// operators' volumes; deleting them lost the stored state and left the new pod
// waiting for a claim that was terminating underneath it.
func TestSyncPipelinesRebuildKeepsVolumes(t *testing.T) {
	driver := &recordingDriver{
		// Another pipeline whose deployment is present, which the sync must leave alone.
		statuses: []lib.PipelineStatus{{Name: "pipeline-present"}},
	}
	pipelines := &recordingPipelines{admin: []pipe.Pipeline{
		{Id: "missing", Operators: []pipe.Operator{persistingCloudOperator()}},
		{Id: "present", Operators: []pipe.Operator{persistingCloudOperator()}},
	}}
	engine := engineWith(driver, pipelines)

	if err := engine.syncPipelines(); err != nil {
		t.Fatal(err)
	}
	if driver.deleteCalls != 0 {
		t.Errorf("the rebuild deleted volumes: DeleteOperators called %d times, want 0", driver.deleteCalls)
	}
	if driver.keepVolumeDeleteCalls != 1 {
		t.Errorf("DeleteOperatorsKeepingVolumes called %d times, want 1 for the leftovers of the missing pipeline", driver.keepVolumeDeleteCalls)
	}
	if len(driver.configs) != 1 || driver.configs[0].PipelineId != "missing" {
		t.Errorf("expected exactly the missing pipeline to be recreated, got %+v", driver.configs)
	}
}

// An explicit delete still removes the volumes: the pipeline is gone for good.
func TestDeletePipelineDeletesVolumes(t *testing.T) {
	driver := &recordingDriver{}
	pipelines := &recordingPipelines{existing: pipe.Pipeline{
		Id:        "3c1f9b42",
		Operators: []pipe.Operator{persistingCloudOperator()},
	}}
	if err := engineWith(driver, pipelines).DeletePipeline(context.Background(), "3c1f9b42", "jonah", "token"); err != nil {
		t.Fatal(err)
	}
	if driver.deleteCalls != 1 || driver.keepVolumeDeleteCalls != 0 {
		t.Errorf("DeleteOperators %d, DeleteOperatorsKeepingVolumes %d, want 1 and 0", driver.deleteCalls, driver.keepVolumeDeleteCalls)
	}
}

// An update replaces the operators and, as before, starts them on fresh volumes.
func TestUpdatePipelineDeletesVolumes(t *testing.T) {
	existingId := uuid.New().String()
	driver := &recordingDriver{}
	pipelines := &recordingPipelines{existing: pipe.Pipeline{
		Id:        existingId,
		Operators: []pipe.Operator{persistingCloudOperator()},
	}}
	if _, err := engineWith(driver, pipelines).UpdatePipeline(context.Background(),
		lib.PipelineRequest{Id: existingId, FlowId: "flow-1"}, "jonah", "token"); err != nil {
		t.Fatal(err)
	}
	if driver.deleteCalls != 1 || driver.keepVolumeDeleteCalls != 0 {
		t.Errorf("DeleteOperators %d, DeleteOperatorsKeepingVolumes %d, want 1 and 0", driver.deleteCalls, driver.keepVolumeDeleteCalls)
	}
}
