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
	"time"

	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
	MQTT "github.com/eclipse/paho.mqtt.golang"
)

// recordingMQTT stands in for the broker connection and keeps the topics published to.
type recordingMQTT struct {
	MQTT.Client
	mu     sync.Mutex
	topics []string
}

func (c *recordingMQTT) Publish(topic string, _ byte, _ bool, _ interface{}) MQTT.Token {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.topics = append(c.topics, topic)
	return doneToken{}
}

type doneToken struct{}

func (doneToken) Wait() bool                     { return true }
func (doneToken) WaitTimeout(time.Duration) bool { return true }
func (doneToken) Done() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
func (doneToken) Error() error { return nil }

// useRecordingMQTT puts a recordingMQTT in place of the package's broker connection
// for the duration of the test.
func useRecordingMQTT(t *testing.T) *recordingMQTT {
	t.Helper()
	prevClient, prevQos, prevRetained := client, qos, retained
	q, r := 0, false
	rec := &recordingMQTT{}
	client, qos, retained = rec, &q, &r
	t.Cleanup(func() { client, qos, retained = prevClient, prevQos, prevRetained })
	return rec
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
	mqtt := useRecordingMQTT(t)
	fog := NewFogClient(unreadableRegistry{&recordingPipelines{}})
	fog.sendActiveOperators(context.Background(), "jonah", "")
	fog.sendTopicsWithEnabledForward(context.Background(), "jonah", "")
	if len(mqtt.topics) != 0 {
		t.Errorf("published to %v, want nothing", mqtt.topics)
	}
}

// A user without pipelines still gets the empty answer.
func TestAFogSyncAnswersAnEmptyRegistry(t *testing.T) {
	mqtt := useRecordingMQTT(t)
	fog := NewFogClient(&recordingPipelines{})
	fog.sendActiveOperators(context.Background(), "jonah", "")
	fog.sendTopicsWithEnabledForward(context.Background(), "jonah", "")
	if len(mqtt.topics) != 2 {
		t.Errorf("published to %v, want both sync answers", mqtt.topics)
	}
}
