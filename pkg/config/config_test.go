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

// withOperatorTimescaleSecret sets the one setting New refuses to start without, so
// a test about something else is not failed by it.
func withOperatorTimescaleSecret(t *testing.T) {
	t.Helper()
	t.Setenv("OPERATOR_TIMESCALE_SECRET_NAME", "operator-timescale")
}

// Without the Secret's name every cloud operator would start without a DSN, so the
// engine fails closed at startup instead of per pipeline.
func TestTheEngineRefusesToStartWithoutAnOperatorTimescaleSecretName(t *testing.T) {
	t.Setenv("OPERATOR_TIMESCALE_SECRET_NAME", "")

	_, err := New("")
	if err == nil {
		t.Fatal("New accepted an empty operator_timescale_secret_name")
	}
	if !strings.Contains(err.Error(), "OPERATOR_TIMESCALE_SECRET_NAME") {
		t.Errorf("error = %q, want it to name the setting", err)
	}
}

func TestOperatorTimescaleSecretKeyDefaultsToTsConn(t *testing.T) {
	withOperatorTimescaleSecret(t)

	cfg, err := New("")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if cfg.OperatorTimescaleSecretKey != "TS_CONN" {
		t.Errorf("operator_timescale_secret_key = %q, want TS_CONN", cfg.OperatorTimescaleSecretKey)
	}
}

func TestOperatorTimescaleSecretIsReadFromTheEnvironment(t *testing.T) {
	t.Setenv("OPERATOR_TIMESCALE_SECRET_NAME", "analytics-timescale")
	t.Setenv("OPERATOR_TIMESCALE_SECRET_KEY", "dsn")

	cfg, err := New("")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if cfg.OperatorTimescaleSecretName != "analytics-timescale" || cfg.OperatorTimescaleSecretKey != "dsn" {
		t.Errorf("secret = %q/%q, want analytics-timescale/dsn",
			cfg.OperatorTimescaleSecretName, cfg.OperatorTimescaleSecretKey)
	}
}

// The API server would reject these per pipeline start; the engine rejects them once,
// at startup.
func TestAnInvalidOperatorTimescaleSecretFailsTheStart(t *testing.T) {
	for _, c := range []struct{ name, key string }{
		{"Timescale_DSN", "TS_CONN"},
		{" operator-timescale", "TS_CONN"},
		{"operator-timescale", "TS CONN"},
		{"operator-timescale", ".."},
		{"operator-timescale", ""},
	} {
		path := filepath.Join(t.TempDir(), "config.json")
		file, _ := json.Marshal(map[string]string{
			"operator_timescale_secret_name": c.name,
			"operator_timescale_secret_key":  c.key,
		})
		if err := os.WriteFile(path, file, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := New(path); err == nil {
			t.Errorf("New accepted secret %q key %q", c.name, c.key)
		}
	}
}

// The DSN itself is no longer the engine's to hold: a leftover TIMESCALE_CONNECTION
// in a deployment must not be read, and so cannot reach the logged config.
func TestTheEngineNoLongerReadsTheTimescaleConnection(t *testing.T) {
	withOperatorTimescaleSecret(t)
	t.Setenv("TIMESCALE_CONNECTION", "leftover-dsn-from-env")

	cfg, err := New("")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "leftover-dsn-from-env") {
		t.Errorf("config read TIMESCALE_CONNECTION: %s", b)
	}
}

func TestOperatorResourcesAreEmptyByDefault(t *testing.T) {
	withOperatorTimescaleSecret(t)

	cfg, err := New("")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if len(cfg.OperatorResources) != 0 {
		t.Errorf("operator_resources = %v, want none so every operator keeps the defaults", cfg.OperatorResources)
	}
}

func TestOperatorResourcesAreReadFromTheEnvironment(t *testing.T) {
	withOperatorTimescaleSecret(t)
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
	withOperatorTimescaleSecret(t)
	// A request alone above the default limit of 512Mi.
	t.Setenv("OPERATOR_RESOURCES", `{"ghcr.io/senergy-platform/consumption-forecast-operator":{"memory_request":"1Gi"}}`)

	if _, err := New(""); err == nil {
		t.Error("New accepted a memory request above the default limit; the deployment would be refused later, per pipeline")
	}
}

// main logs the whole config as JSON, so the credentials must not survive the marshalling.
func TestConfigJsonHidesCredentials(t *testing.T) {
	withOperatorTimescaleSecret(t)
	t.Setenv("BROKER_PASSWORD", "broker-password-from-env")

	cfg, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "broker-password-from-env") {
		t.Errorf("config JSON contains the broker password: %s", b)
	}
}

func TestConfigFileSetsCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	file := `{"mqtt": {"broker_password": "broker-password-from-file"},
		"operator_timescale_secret_name": "secret-from-file", "operator_timescale_secret_key": "key-from-file"}`
	if err := os.WriteFile(path, []byte(file), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	got := []string{cfg.Mqtt.BrokerPassword.Value(), cfg.OperatorTimescaleSecretName, cfg.OperatorTimescaleSecretKey}
	want := []string{"broker-password-from-file", "secret-from-file", "key-from-file"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("setting %d from file = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestOnlyTheSenergyPlatformImagesAreTrustedWithTheTimescaleDsnByDefault(t *testing.T) {
	withOperatorTimescaleSecret(t)

	cfg, err := New("")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	got := cfg.OperatorTimescaleTrustedImagePrefixes
	if len(got) != 1 || got[0] != "ghcr.io/senergy-platform/" {
		t.Errorf("operator_timescale_trusted_image_prefixes = %q, want [ghcr.io/senergy-platform/]", got)
	}
}

// The loader reads a list from the environment as a JSON array, as it does OPERATOR_RESOURCES.
func TestTrustedImagePrefixesAreReadFromTheEnvironment(t *testing.T) {
	withOperatorTimescaleSecret(t)
	t.Setenv("OPERATOR_TIMESCALE_TRUSTED_IMAGE_PREFIXES", `["ghcr.io/senergy-platform/","registry.example:5000/analytics/"]`)

	cfg, err := New("")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	got := cfg.OperatorTimescaleTrustedImagePrefixes
	if len(got) != 2 || got[0] != "ghcr.io/senergy-platform/" || got[1] != "registry.example:5000/analytics/" {
		t.Errorf("operator_timescale_trusted_image_prefixes = %q, want the two from the environment", got)
	}
}

// An empty list is a valid setting: no operator gets the DSN.
func TestAnEmptyTrustedImagePrefixListIsAccepted(t *testing.T) {
	withOperatorTimescaleSecret(t)
	t.Setenv("OPERATOR_TIMESCALE_TRUSTED_IMAGE_PREFIXES", `[]`)

	cfg, err := New("")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if len(cfg.OperatorTimescaleTrustedImagePrefixes) != 0 {
		t.Errorf("operator_timescale_trusted_image_prefixes = %q, want none", cfg.OperatorTimescaleTrustedImagePrefixes)
	}
}

// The file's array is decoded into the default in place; a later New must still see the
// default, not the previous file's entries.
func TestTrustedImagePrefixesFromAFileDoNotChangeTheDefault(t *testing.T) {
	withOperatorTimescaleSecret(t)
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"operator_timescale_trusted_image_prefixes": ["registry.example/a/"]}`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := New(path)
	if err != nil {
		t.Fatalf("New(file): %v", err)
	}
	if got := cfg.OperatorTimescaleTrustedImagePrefixes; len(got) != 1 || got[0] != "registry.example/a/" {
		t.Fatalf("prefixes from file = %q, want [registry.example/a/]", got)
	}
	cfg, err = New("")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := cfg.OperatorTimescaleTrustedImagePrefixes; len(got) != 1 || got[0] != "ghcr.io/senergy-platform/" {
		t.Errorf("default after a file = %q, want [ghcr.io/senergy-platform/]", got)
	}
}

// Without the trailing "/" a prefix would also trust a look-alike organisation such as
// ghcr.io/senergy-platform-x/.
func TestAnInvalidTrustedImagePrefixFailsTheStart(t *testing.T) {
	withOperatorTimescaleSecret(t)
	for _, prefixes := range []string{
		`["ghcr.io/senergy-platform"]`,
		`["ghcr.io/senergy-platform/", ""]`,
		`["/"]`,
		`["/senergy-platform/"]`,
		`[" ghcr.io/senergy-platform/"]`,
		`["ghcr.io/senergy-platform/\n"]`,
		`"ghcr.io/senergy-platform/"`,
	} {
		t.Setenv("OPERATOR_TIMESCALE_TRUSTED_IMAGE_PREFIXES", prefixes)
		if _, err := New(""); err == nil {
			t.Errorf("New accepted operator_timescale_trusted_image_prefixes %s", prefixes)
		}
	}
}
