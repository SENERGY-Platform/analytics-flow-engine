/*
 * Copyright 2022 InfAI (CC SES)
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

package rancher2_api

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/config"
)

func TestRancher2_createPersistentVolumeClaim(t *testing.T) {
	cfg, err := config.New("../../config.json")
	if err != nil {
		t.Skip(err)
		return
	}
	driver := NewRancher2(
		cfg.Rancher2.Endpoint,
		cfg.Rancher2.AccessKey,
		cfg.Rancher2.SecretKey,
		cfg.Rancher2.StackId,
		&cfg.Rancher2,
		cfg.OperatorResources,
	)
	name := "test"
	err = driver.createPersistentVolumeClaim(context.Background(), name)
	if err != nil {
		t.Error(err.Error())
		return
	}
	time.Sleep(3 * time.Second)

	err = driver.deletePersistentVolumeClaim(context.Background(), name)
	if err != nil {
		t.Error(err.Error())
		return
	}

}

func TestContainerResources(t *testing.T) {
	overrides := map[string]config.OperatorResource{
		"ghcr.io/senergy-platform/consumption-forecast-operator": {MemoryLimit: "2Gi", MemoryRequest: "1Gi"},
	}

	got := containerResources("ghcr.io/senergy-platform/consumption-forecast-operator:prod", overrides)
	want := ContainerResources{
		Limits:   map[string]string{"memory": "2Gi", "cpu": "500m"},
		Requests: map[string]string{"memory": "1Gi", "cpu": "25m"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("containerResources = %+v, want %+v", got, want)
	}

	got = containerResources("nginx:1.12", overrides)
	want = ContainerResources{
		Limits:   map[string]string{"memory": "512Mi", "cpu": "500m"},
		Requests: map[string]string{"memory": "128Mi", "cpu": "25m"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("containerResources of an image without override = %+v, want %+v", got, want)
	}
}

func TestVPARequestLetsTheUpdaterEvictASinglePod(t *testing.T) {
	body, err := json.Marshal(vpaRequest("pipeline-x", "ns"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `"updatePolicy":{"updateMode":"Recreate","minReplicas":1}`) {
		t.Fatalf("vpa request: %s", body)
	}
	if !strings.Contains(string(body), `"name":"pipeline-x-vpa","namespace":"ns"`) {
		t.Fatalf("vpa request: %s", body)
	}
}
