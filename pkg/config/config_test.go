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

package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The DSN carries a password, so it must come from the deployment and never from
// a value compiled into a public repository.
func TestTheTimescaleConnectionHasNoDefault(t *testing.T) {
	cfg, err := New("")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if cfg.TimescaleConnection.Value() != "" {
		t.Error("timescale_connection has a compiled-in default, want it empty")
	}
}

func TestTheTimescaleConnectionIsOverridableFromTheEnvironment(t *testing.T) {
	t.Setenv("TIMESCALE_CONNECTION", "postgresql://ops@timescale.example.org/db")

	cfg, err := New("")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if cfg.TimescaleConnection.Value() != "postgresql://ops@timescale.example.org/db" {
		t.Errorf("timescale_connection = %q, want the environment's value; a deployment "+
			"whose timescale is elsewhere could not be configured otherwise",
			cfg.TimescaleConnection.Value())
	}
}

func TestOperatorResourcesAreEmptyByDefault(t *testing.T) {
	cfg, err := New("")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if len(cfg.OperatorResources) != 0 {
		t.Errorf("operator_resources = %v, want none so every operator keeps the defaults", cfg.OperatorResources)
	}
}

func TestOperatorResourcesAreReadFromTheEnvironment(t *testing.T) {
	t.Setenv("OPERATOR_RESOURCES", `{"ghcr.io/senergy-platform/consumption-forecast-operator":{"memory_limit":"2Gi","memory_request":"1Gi"}}`)

	cfg, err := New("")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	got := cfg.OperatorResources["ghcr.io/senergy-platform/consumption-forecast-operator"]
	if got.MemoryLimit != "2Gi" || got.MemoryRequest != "1Gi" {
		t.Errorf("operator_resources = %+v, want the environment's memory_limit 2Gi and memory_request 1Gi", got)
	}
}

func TestInvalidOperatorResourcesFailTheStart(t *testing.T) {
	// A request alone above the default limit of 512Mi.
	t.Setenv("OPERATOR_RESOURCES", `{"ghcr.io/senergy-platform/consumption-forecast-operator":{"memory_request":"1Gi"}}`)

	if _, err := New(""); err == nil {
		t.Error("New accepted a memory request above the default limit; the deployment would be refused later, per pipeline")
	}
}

// main logs the whole config as JSON, so the credentials must not survive the marshalling.
func TestConfigJsonHidesCredentials(t *testing.T) {
	secrets := map[string]string{
		"BROKER_PASSWORD":      "broker-password-from-env",
		"TIMESCALE_CONNECTION": "postgresql://ops:timescale-password-from-env@timescale.example.org/db",
	}
	for k, v := range secrets {
		t.Setenv(k, v)
	}
	cfg, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"broker-password-from-env", "timescale-password-from-env"} {
		if strings.Contains(string(b), secret) {
			t.Errorf("config JSON contains %q: %s", secret, b)
		}
	}
}

func TestConfigFileSetsCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	file := `{"mqtt": {"broker_password": "broker-password-from-file"},
		"timescale_connection": "postgresql://ops:timescale-password-from-file@timescale.example.org/db"}`
	if err := os.WriteFile(path, []byte(file), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	got := []string{cfg.Mqtt.BrokerPassword.Value(), cfg.TimescaleConnection.Value()}
	want := []string{"broker-password-from-file", "postgresql://ops:timescale-password-from-file@timescale.example.org/db"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("credential %d from file = %q, want %q", i, got[i], want[i])
		}
	}
}
