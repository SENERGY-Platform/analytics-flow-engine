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

package kafka2mqtt_api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func removeAnswering(t *testing.T, status int) error {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/instances/instance-1" {
			t.Errorf("request %s %s, want DELETE /instances/instance-1", r.Method, r.URL.Path)
		}
		w.WriteHeader(status)
	}))
	defer srv.Close()
	return NewKafka2MqttApi(srv.URL, nil).RemoveInstance(context.Background(), "instance-1", "pipeline-1", "user", "token")
}

// kafka2mqtt-manager answers a missing administrate right with 404, so a 404 must not
// let the caller drop the registry entry of a still running instance.
func TestRemoveInstanceReportsNotFound(t *testing.T) {
	if err := removeAnswering(t, http.StatusNotFound); err == nil {
		t.Fatal("err = nil for 404, want an error")
	}
}

func TestRemoveInstanceReportsOtherFailures(t *testing.T) {
	if err := removeAnswering(t, http.StatusInternalServerError); err == nil {
		t.Fatal("err = nil for 500, want an error")
	}
	if err := removeAnswering(t, http.StatusNoContent); err != nil {
		t.Fatalf("err = %v for 204, want none", err)
	}
}
