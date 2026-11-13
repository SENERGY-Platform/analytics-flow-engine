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
	"sync"
	"testing"

	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
)

// recordingMQTT stands in for the broker connection and keeps the topics published to.
type recordingMQTT struct {
	mu     sync.Mutex
	topics []string
}

func (c *recordingMQTT) Publish(topic string, _ string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.topics = append(c.topics, topic)
	return nil
}

// unreadableRegistry fails every read of a user's pipelines.
type unreadableRegistry struct {
	*recordingPipelines
}

func (unreadableRegistry) GetPipelines(context.Context, string, string) ([]pipe.Pipeline, error) {
	return nil, errors.New("registry down")
}

// An empty answer tells the fog agent to stop every operator it runs, so a registry
// failure must not produce one.
func TestAFogSyncAnswersNothingWhenTheRegistryFails(t *testing.T) {
	mqtt := &recordingMQTT{}
	fog := NewFogClient(unreadableRegistry{&recordingPipelines{}}, mqtt)
	fog.sendActiveOperators(context.Background(), "jonah", "")
	fog.sendTopicsWithEnabledForward(context.Background(), "jonah", "")
	if len(mqtt.topics) != 0 {
		t.Errorf("published to %v, want nothing", mqtt.topics)
	}
}

// A user without pipelines still gets the empty answer.
func TestAFogSyncAnswersAnEmptyRegistry(t *testing.T) {
	mqtt := &recordingMQTT{}
	fog := NewFogClient(&recordingPipelines{}, mqtt)
	fog.sendActiveOperators(context.Background(), "jonah", "")
	fog.sendTopicsWithEnabledForward(context.Background(), "jonah", "")
	if len(mqtt.topics) != 2 {
		t.Errorf("published to %v, want both sync answers", mqtt.topics)
	}
}
