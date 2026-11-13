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
	kafka2mqtt_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/kafka2mqtt-api"
	parser "github.com/SENERGY-Platform/analytics-parser/lib"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
	"github.com/google/uuid"
)

// recordingKafka2Mqtt keeps the user id of every forwarding instance it was asked to start.
type recordingKafka2Mqtt struct {
	startedFor []string
}

func (r *recordingKafka2Mqtt) StartOperatorInstance(_ context.Context, _, _ string, _, userID, _ string) (kafka2mqtt_api.Instance, error) {
	r.startedFor = append(r.startedFor, userID)
	return kafka2mqtt_api.Instance{Id: "instance-1"}, nil
}

func (r *recordingKafka2Mqtt) RemoveInstance(context.Context, string, string, string, string) error {
	return nil
}

func downstreamFlow() parser.Pipeline {
	flow := oneCloudOperatorFlow()
	op := flow.Operators["op-1"]
	op.DownstreamConfig.Enabled = true
	flow.Operators["op-1"] = op
	return flow
}

// The cloud-to-fog forwarding is set up for the user's fog; with an empty user id it
// went to nobody's.
func TestStartAndUpdateForwardForTheRequestingUser(t *testing.T) {
	for name, run := range map[string]func(*FlowEngine) error{
		"start": func(f *FlowEngine) error {
			_, err := f.StartPipeline(context.Background(), lib.PipelineRequest{FlowId: "flow-1"}, "jonah", "token")
			return err
		},
		"update": func(f *FlowEngine) error {
			_, err := f.UpdatePipeline(context.Background(), lib.PipelineRequest{Id: "3c1f9b42", FlowId: "flow-1"}, "jonah", "token")
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			pipelines := &recordingPipelines{assignedId: uuid.New(), existing: pipe.Pipeline{Id: "3c1f9b42", UserId: "jonah"}}
			forwarding := &recordingKafka2Mqtt{}
			engine := engineWith(&recordingDriver{}, pipelines)
			engine.parsingService = staticParser{pipeline: downstreamFlow()}
			engine.kafak2mqttService = forwarding
			if err := run(engine); err != nil {
				t.Fatal(err)
			}
			if len(forwarding.startedFor) != 1 || forwarding.startedFor[0] != "jonah" {
				t.Errorf("forwarding started for %q, want once for jonah", forwarding.startedFor)
			}
			if pipelines.stored == nil || pipelines.stored.UserId != "jonah" {
				t.Errorf("stored pipeline = %+v, want user id jonah", pipelines.stored)
			}
		})
	}
}
