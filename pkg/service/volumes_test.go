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
	"errors"
	"fmt"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/deployment"
	"strings"
	"testing"
	"time"

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
	const missing, present = "6b0f3c2e-8d51-4f7a-9c3b-1e2d4a5b6c7d", "a3e9d1f4-2b6c-4e8a-b0d7-5c1f9e3a7b2d"
	driver := &recordingDriver{
		// Another pipeline whose deployment is present, which the sync must leave alone.
		statuses: []lib.PipelineStatus{{Name: "pipeline-" + present}},
	}
	pipelines := &recordingPipelines{admin: []pipe.Pipeline{
		{Id: missing, Operators: []pipe.Operator{persistingCloudOperator()}},
		{Id: present, Operators: []pipe.Operator{persistingCloudOperator()}},
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
	if len(driver.configs) != 1 || driver.configs[0].PipelineId != missing {
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

// A claim still terminating after the driver's whole wait ends the start at once:
// the six retries would each wait that long again in a call nobody can cancel.
func TestStartOperatorsDoesNotRetryAVolumeStillTerminating(t *testing.T) {
	driver := &recordingDriver{createErr: map[string]error{
		"3c1f9b42": fmt.Errorf("%w: operator-3c1f9b42-op-12345 after 1m0s", deployment.ErrVolumeStillTerminating),
	}}
	engine := engineWith(driver, &recordingPipelines{})
	pipeline := pipe.Pipeline{Id: "3c1f9b42", Operators: []pipe.Operator{persistingCloudOperator()}}

	_, err := engine.startOperators(context.Background(), pipeline, engine.createPipelineConfig(pipeline), "")
	if !errors.Is(err, deployment.ErrVolumeStillTerminating) {
		t.Fatalf("err = %v, want ErrVolumeStillTerminating", err)
	}
	if driver.createAttempts != 1 {
		t.Errorf("CreateOperators attempts = %d, want 1", driver.createAttempts)
	}
}

func TestRetryStopsOnAPermanentError(t *testing.T) {
	cause := errors.New("cause")
	calls := 0
	err := retry(context.Background(), 3, time.Millisecond, func() error {
		calls++
		return permanentError{cause}
	})
	if calls != 1 || err != cause {
		t.Errorf("calls = %d, err = %v, want 1 call and the wrapped error itself", calls, err)
	}
	calls = 0
	_ = retry(context.Background(), 3, time.Millisecond, func() error {
		calls++
		return cause
	})
	if calls != 3 {
		t.Errorf("calls = %d for an ordinary error, want all 3 attempts", calls)
	}
}

// One pipeline whose rebuild fails must not keep the later ones down until the next
// restart; the failures still come back together so the caller logs the sync as
// incomplete.
func TestSyncPipelinesContinuesPastAFailedRebuild(t *testing.T) {
	stopFailure := errors.New("stop failed")
	driver := &recordingDriver{
		keepVolumeDeleteErr: map[string]error{"first": stopFailure},
		createErr:           map[string]error{"second": fmt.Errorf("%w: claim", deployment.ErrVolumeStillTerminating)},
	}
	pipelines := &recordingPipelines{admin: []pipe.Pipeline{
		{Id: "first", Operators: []pipe.Operator{persistingCloudOperator()}},
		{Id: "second", Operators: []pipe.Operator{persistingCloudOperator()}},
		{Id: "third", Operators: []pipe.Operator{persistingCloudOperator()}},
	}}

	err := engineWith(driver, pipelines).syncPipelines()
	if len(driver.configs) != 1 || driver.configs[0].PipelineId != "third" {
		t.Errorf("recreated %+v, want the third pipeline despite the two failures before it", driver.configs)
	}
	if !errors.Is(err, stopFailure) || !errors.Is(err, deployment.ErrVolumeStillTerminating) {
		t.Fatalf("err = %v, want both failures joined", err)
	}
	for _, id := range []string{"first", "second"} {
		if !strings.Contains(err.Error(), "pipeline "+id) {
			t.Errorf("err = %q, want it to name pipeline %s", err, id)
		}
	}
}

// The startup sync can run while requests arrive; its rebuild of a pipeline waits for
// a request that holds the pipeline's lock.
func TestSyncPipelinesWaitsForThePipelineLock(t *testing.T) {
	const id = "6b0f3c2e-8d51-4f7a-9c3b-1e2d4a5b6c7d"
	driver := &recordingDriver{}
	pipelines := &recordingPipelines{admin: []pipe.Pipeline{{Id: id, Operators: []pipe.Operator{persistingCloudOperator()}}}}
	engine := engineWith(driver, pipelines)

	unlock := engine.locks.lock(id)
	done := make(chan error)
	go func() { done <- engine.syncPipelines() }()

	select {
	case <-done:
		t.Fatal("the sync rebuilt the pipeline while a request held its lock")
	case <-time.After(100 * time.Millisecond):
	}
	unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(driver.configs) != 1 {
		t.Errorf("expected the pipeline to be rebuilt after the lock was released, got %d creates", len(driver.configs))
	}
}
