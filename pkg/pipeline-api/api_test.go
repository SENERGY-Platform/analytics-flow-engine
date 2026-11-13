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
	"sync/atomic"
	"testing"
	"time"

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

// adminListAnswering serves the bodies in turn, the last one for every further request,
// and returns the result with the number of requests.
func adminListAnswering(t *testing.T, bodies ...string) ([]pipe.Pipeline, int, error) {
	t.Helper()
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/pipeline" {
			t.Errorf("path %s, want /admin/pipeline", r.URL.Path)
		}
		n := int(requests.Add(1))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(bodies[min(n, len(bodies))-1]))
	}))
	defer srv.Close()
	api := NewPipelineApi(srv.URL)
	api.adminListPause = time.Millisecond
	pipelines, err := api.GetPipelinesAdmin(context.Background())
	return pipelines, int(requests.Load()), err
}

// A paged answer would make every pipeline beyond the page look orphaned to the cleanup.
func TestGetPipelinesAdminRejectsAPartialAnswer(t *testing.T) {
	if _, requests, err := adminListAnswering(t, `{"data":[{"id":"a"}],"total":2}`); err == nil || requests != 3 {
		t.Fatalf("err %v after %d requests for 1 of 2 pipelines, want an error after 3", err, requests)
	}
}

// The registry counts and lists in two queries, so a pipeline created in between gives
// one answer whose total does not match.
func TestGetPipelinesAdminAsksAgainAfterAMismatch(t *testing.T) {
	pipelines, requests, err := adminListAnswering(t,
		`{"data":[{"id":"a"}],"total":2}`, `{"data":[{"id":"a"},{"id":"b"}],"total":3}`, `{"data":[{"id":"a"},{"id":"b"}],"total":2}`)
	if err != nil || len(pipelines) != 2 || requests != 3 {
		t.Fatalf("pipelines %v, err %v after %d requests; want 2 and none after 3", pipelines, err, requests)
	}
}

func TestGetPipelinesAdminStopsWaitingWhenCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[],"total":1}`))
	}))
	defer srv.Close()
	api := NewPipelineApi(srv.URL)
	api.adminListPause = time.Hour
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := api.GetPipelinesAdmin(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the deadline", err)
	}
}

func TestGetPipelinesAdminAcceptsAComplete(t *testing.T) {
	pipelines, requests, err := adminListAnswering(t, `{"data":[{"id":"a"},{"id":"b"}],"total":2}`)
	if err != nil || len(pipelines) != 2 || requests != 1 {
		t.Fatalf("pipelines %v, err %v after %d requests; want 2 and none after 1", pipelines, err, requests)
	}
	if pipelines, _, err = adminListAnswering(t, `{"data":[],"total":0}`); err != nil || len(pipelines) != 0 {
		t.Fatalf("empty registry: pipelines %v, err %v; want none and none", pipelines, err)
	}
}
