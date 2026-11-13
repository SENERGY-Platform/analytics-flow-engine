/*
 * Copyright 2018 InfAI (CC SES)
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
	"slices"
	"testing"

	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
)

func TestRedactPipelineMasksTsConn(t *testing.T) {
	const dsn = "postgresql://operator:hunter2@timescale.example.org/db"
	original := pipe.Pipeline{Operators: []pipe.Operator{
		{Id: "cloud", Config: map[string]string{OperatorConfigTsConn: dsn, "threshold": "3"}},
		{Id: "local"},
	}}

	redacted := redactPipeline(original)

	got := redacted.Operators[0].Config
	if got[OperatorConfigTsConn] == dsn {
		t.Error("ts_conn survived the redaction")
	}
	if got["threshold"] != "3" {
		t.Errorf("the rest of the config should stay readable, got %v", got)
	}
	if redacted.Operators[1].Config != nil {
		t.Errorf("an operator without config should stay without, got %v", redacted.Operators[1].Config)
	}
	// The engine keeps using the pipeline after logging it, so the redaction must
	// not reach the operator it was handed.
	if original.Operators[0].Config[OperatorConfigTsConn] != dsn {
		t.Error("the redaction changed the original operator config")
	}
}

func TestDiffPipelineIds(t *testing.T) {
	for _, c := range []struct {
		name                      string
		registered, inCluster     []string
		wantMissing, wantOrphaned []string
	}{
		{"both empty", nil, nil, nil, nil},
		{"in sync", []string{"a", "b"}, []string{"b", "a"}, nil, nil},
		{"one each", []string{"a", "b"}, []string{"b", "c"}, []string{"a"}, []string{"c"}},
		{"empty cluster", []string{"a", "b"}, nil, []string{"a", "b"}, nil},
		{"empty registry", nil, []string{"c", "d"}, nil, []string{"c", "d"}},
		{"repeats once, in input order", []string{"b", "a", "b"}, []string{"d", "c", "d"}, []string{"b", "a"}, []string{"d", "c"}},
	} {
		missing, orphaned := diffPipelineIds(c.registered, c.inCluster)
		if !slices.Equal(missing, c.wantMissing) || !slices.Equal(orphaned, c.wantOrphaned) {
			t.Errorf("%s: missing %q orphaned %q, want %q and %q", c.name, missing, orphaned, c.wantMissing, c.wantOrphaned)
		}
	}
}
