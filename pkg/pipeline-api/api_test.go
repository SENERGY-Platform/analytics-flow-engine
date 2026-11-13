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

package pipeline_api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
)

func TestDeletePipelineNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	err := NewPipelineApi(srv.URL).DeletePipeline(context.Background(), "x", "user", "token")
	if !errors.As(err, new(*lib.NotFoundError)) {
		t.Fatalf("DeletePipeline on 404 = %v, want a NotFoundError", err)
	}
}

func TestDeletePipelineFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	err := NewPipelineApi(srv.URL).DeletePipeline(context.Background(), "x", "user", "token")
	if err == nil || errors.As(err, new(*lib.NotFoundError)) {
		t.Fatalf("DeletePipeline on 500 = %v, want an error that is no NotFoundError", err)
	}
}

func adminListAnswering(t *testing.T, body string) ([]pipe.Pipeline, error) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/pipeline" {
			t.Errorf("path %s, want /admin/pipeline", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()
	return NewPipelineApi(srv.URL).GetPipelinesAdmin(context.Background())
}

// A paged answer would make every pipeline beyond the page look orphaned to the cleanup.
func TestGetPipelinesAdminRejectsAPartialAnswer(t *testing.T) {
	if _, err := adminListAnswering(t, `{"data":[{"id":"a"}],"total":2}`); err == nil {
		t.Fatal("err = nil for 1 of 2 pipelines, want an error")
	}
}

func TestGetPipelinesAdminAcceptsAComplete(t *testing.T) {
	pipelines, err := adminListAnswering(t, `{"data":[{"id":"a"},{"id":"b"}],"total":2}`)
	if err != nil || len(pipelines) != 2 {
		t.Fatalf("pipelines %v, err %v; want 2 and none", pipelines, err)
	}
	if pipelines, err = adminListAnswering(t, `{"data":[],"total":0}`); err != nil || len(pipelines) != 0 {
		t.Fatalf("empty registry: pipelines %v, err %v; want none and none", pipelines, err)
	}
}
