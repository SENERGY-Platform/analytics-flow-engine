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

package service

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	kafka2mqtt_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/kafka2mqtt-api"
	parser "github.com/SENERGY-Platform/analytics-parser/lib"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
	"github.com/google/uuid"
)

// scriptedKafka2Mqtt hands out instance-1, instance-2, ... and fails the calls a test
// names. It keeps the ids it removed.
type scriptedKafka2Mqtt struct {
	mu         sync.Mutex
	starts     int
	failStart  map[int]error
	removeErrs []error
	removed    []string
}

func (k *scriptedKafka2Mqtt) StartOperatorInstance(context.Context, string, string, string, string, string) (kafka2mqtt_api.Instance, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.starts++
	if err := k.failStart[k.starts]; err != nil {
		return kafka2mqtt_api.Instance{}, err
	}
	return kafka2mqtt_api.Instance{Id: fmt.Sprintf("instance-%d", k.starts)}, nil
}

func (k *scriptedKafka2Mqtt) RemoveInstance(_ context.Context, id, _, _, _ string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if len(k.removeErrs) > 0 {
		err := k.removeErrs[0]
		k.removeErrs = k.removeErrs[1:]
		if err != nil {
			return err
		}
	}
	k.removed = append(k.removed, id)
	return nil
}

func forwardingOperator(id, instanceId string) pipe.Operator {
	return pipe.Operator{Id: id, Name: "adder", OperatorId: "base-op-1", DeploymentType: "cloud",
		DownstreamConfig: pipe.DownstreamConfig{Enabled: true, InstanceID: instanceId}}
}

func twoForwardingOperatorsFlow() parser.Pipeline {
	flow := downstreamFlow()
	second := flow.Operators["op-1"]
	second.Id = "op-2"
	flow.Operators["op-2"] = second
	return flow
}

// failingUpdates is a registry that refuses every update.
type failingUpdates struct {
	*recordingPipelines
	err error
}

func (p failingUpdates) UpdatePipeline(context.Context, *pipe.Pipeline, string, string) error {
	return p.err
}

// The registry entry goes last, so a delete that failed on the way can be sent again
// and finishes what the first one left.
func TestAFailedDeleteCanBeRetriedToCompletion(t *testing.T) {
	forwarding := &scriptedKafka2Mqtt{removeErrs: []error{errors.New("kafka2mqtt down")}}
	pipelines := &recordingPipelines{existing: pipe.Pipeline{Id: "3c1f9b42", UserId: "jonah",
		Operators: []pipe.Operator{forwardingOperator("op-1", "instance-1")}}}
	engine := engineWith(&recordingDriver{}, pipelines)
	engine.kafak2mqttService = forwarding

	if err := engine.DeletePipeline(context.Background(), "3c1f9b42", "jonah", "token"); err == nil {
		t.Fatal("err = nil, want the forwarding failure")
	}
	if pipelines.deleteCalls != 0 {
		t.Fatal("the registry entry was deleted although the forwarding instance is still there")
	}
	if err := engine.DeletePipeline(context.Background(), "3c1f9b42", "jonah", "token"); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if pipelines.deleteCalls != 1 || !slices.Equal(forwarding.removed, []string{"instance-1"}) {
		t.Errorf("registry deletes %d, removed instances %v; want 1 and instance-1", pipelines.deleteCalls, forwarding.removed)
	}
}

// The ids of the forwarding instances created before the failure are only known to the
// start itself; dropping them left the instances forwarding for a pipeline that never was.
func TestAFailedStartRemovesTheForwardingInstancesItCreated(t *testing.T) {
	forwarding := &scriptedKafka2Mqtt{failStart: map[int]error{2: errors.New("kafka2mqtt down")}}
	driver := &recordingDriver{}
	pipelines := &recordingPipelines{assignedId: uuid.New()}
	engine := engineWith(driver, pipelines)
	engine.parsingService = staticParser{pipeline: twoForwardingOperatorsFlow()}
	engine.kafak2mqttService = forwarding

	if _, err := engine.StartPipeline(context.Background(), lib.PipelineRequest{FlowId: "flow-1"}, "jonah", "token"); err == nil {
		t.Fatal("err = nil, want the forwarding failure")
	}
	if !slices.Equal(forwarding.removed, []string{"instance-1"}) {
		t.Errorf("removed instances %v, want instance-1", forwarding.removed)
	}
	if driver.deleteCalls != 1 || pipelines.deleteCalls != 1 {
		t.Errorf("operator deletes %d, registry deletes %d; want 1 and 1", driver.deleteCalls, pipelines.deleteCalls)
	}
}

// Without the final registry update nothing knows the forwarding instance ids, so the
// start is undone like any other failed start.
func TestAStartWhoseRegistryUpdateFailsIsRolledBack(t *testing.T) {
	updateFailure := errors.New("registry down")
	forwarding := &scriptedKafka2Mqtt{}
	driver := &recordingDriver{}
	pipelines := &recordingPipelines{assignedId: uuid.New()}
	engine := engineWith(driver, pipelines)
	engine.pipelineService = failingUpdates{recordingPipelines: pipelines, err: updateFailure}
	engine.parsingService = staticParser{pipeline: downstreamFlow()}
	engine.kafak2mqttService = forwarding

	if _, err := engine.StartPipeline(context.Background(), lib.PipelineRequest{FlowId: "flow-1"}, "jonah", "token"); !errors.Is(err, updateFailure) {
		t.Fatalf("err = %v, want the registry failure", err)
	}
	if driver.deleteCalls != 1 || pipelines.deleteCalls != 1 || !slices.Equal(forwarding.removed, []string{"instance-1"}) {
		t.Errorf("operator deletes %d, registry deletes %d, removed instances %v; want 1, 1 and instance-1",
			driver.deleteCalls, pipelines.deleteCalls, forwarding.removed)
	}
}

// A failed update removes what it started of the new version, brings the old one back
// for its owner, and stores the old version's new forwarding instance.
func TestAFailedUpdateRestoresTheOldPipeline(t *testing.T) {
	startFailure := errors.New("kafka2mqtt down")
	forwarding := &scriptedKafka2Mqtt{failStart: map[int]error{1: startFailure}}
	driver := &recordingDriver{}
	pipelines := &recordingPipelines{existing: pipe.Pipeline{Id: "3c1f9b42", UserId: "owner",
		Operators: []pipe.Operator{forwardingOperator("op-1", "old-instance")}}}
	engine := engineWith(driver, pipelines)
	engine.parsingService = staticParser{pipeline: downstreamFlow()}
	engine.kafak2mqttService = forwarding

	_, err := engine.UpdatePipeline(context.Background(), lib.PipelineRequest{Id: "3c1f9b42", FlowId: "flow-1"}, "jonah", "token")
	if !errors.Is(err, startFailure) {
		t.Fatalf("err = %v, want the start failure of the new version", err)
	}
	if driver.deleteCalls != 2 {
		t.Errorf("operator deletes = %d, want 2: the old version, then what the new one created", driver.deleteCalls)
	}
	if len(driver.configs) != 2 || driver.configs[1].UserId != "owner" {
		t.Errorf("configs = %+v, want the old version restarted for its owner", driver.configs)
	}
	if !slices.Equal(forwarding.removed, []string{"old-instance"}) {
		t.Errorf("removed instances %v, want old-instance", forwarding.removed)
	}
	if pipelines.stored == nil || len(pipelines.stored.Operators) != 1 || pipelines.stored.Operators[0].DownstreamConfig.InstanceID != "instance-2" {
		t.Errorf("stored = %+v, want the old version with instance-2", pipelines.stored)
	}
}
