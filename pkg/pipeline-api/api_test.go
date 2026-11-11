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
