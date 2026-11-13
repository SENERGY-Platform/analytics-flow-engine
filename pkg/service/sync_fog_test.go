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
	"testing"

	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
)

// The sync cannot store new forwarding instance ids in the registry, so a rebuild that
// replaced the forwarding left the stored ids pointing at removed instances.
func TestSyncPipelinesRebuildsOnlyTheCloudDeployment(t *testing.T) {
	const id = "6b0f3c2e-8d51-4f7a-9c3b-1e2d4a5b6c7d"
	mqtt := useRecordingMQTT(t)
	forwarding := &scriptedKafka2Mqtt{}
	driver := &recordingDriver{}
	cloud := forwardingOperator("op-cloud-1", "stored-instance")
	fog := pipe.Operator{Id: "op-fog-1", DeploymentType: "local", UpstreamConfig: pipe.UpstreamConfig{Enabled: true}}
	pipelines := &recordingPipelines{admin: []pipe.Pipeline{{Id: id, UserId: "owner", Operators: []pipe.Operator{cloud, fog}}}}
	engine := engineWith(driver, pipelines)
	engine.kafak2mqttService = forwarding

	if err := engine.syncPipelines(); err != nil {
		t.Fatal(err)
	}
	if driver.keepVolumeDeleteCalls != 1 || len(driver.created) != 1 {
		t.Fatalf("stops %d, creates %d; want 1 and 1", driver.keepVolumeDeleteCalls, len(driver.created))
	}
	if ops := driver.created[0]; len(ops) != 1 || ops[0].Id != cloud.Id {
		t.Errorf("created %+v, want only the cloud operator", ops)
	}
	if driver.configs[0].UserId != "owner" {
		t.Errorf("user id = %q, want the owner's", driver.configs[0].UserId)
	}
	if forwarding.starts != 0 || len(forwarding.removed) != 0 || len(mqtt.topics) != 0 {
		t.Errorf("forwarding starts %d, removed %v, fog commands %v; want none", forwarding.starts, forwarding.removed, mqtt.topics)
	}
	if pipelines.stored != nil {
		t.Errorf("registry updated with %+v, want it untouched", pipelines.stored)
	}
}

// A pipeline that runs on fog only has no deployment; counted as missing, every restart
// stopped and restarted its fog operators.
func TestSyncPipelinesLeavesAFogOnlyPipelineAlone(t *testing.T) {
	mqtt := useRecordingMQTT(t)
	driver := &recordingDriver{}
	pipelines := &recordingPipelines{admin: []pipe.Pipeline{{
		Id:        "6b0f3c2e-8d51-4f7a-9c3b-1e2d4a5b6c7d",
		Operators: []pipe.Operator{{Id: "op-1", DeploymentType: "local"}},
	}}}

	if err := engineWith(driver, pipelines).syncPipelines(); err != nil {
		t.Fatal(err)
	}
	if driver.keepVolumeDeleteCalls != 0 || len(driver.configs) != 0 || len(mqtt.topics) != 0 {
		t.Errorf("stops %d, creates %d, fog commands %v; want none", driver.keepVolumeDeleteCalls, len(driver.configs), mqtt.topics)
	}
}
