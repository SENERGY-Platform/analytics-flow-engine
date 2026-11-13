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

package permission_api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/SENERGY-Platform/permissions-v2/pkg/client"
)

// The check has to ask under the registry's topic, or it answers for another resource.
func TestUserHasPipelinePermissionAsksForThePipelineTopic(t *testing.T) {
	for _, granted := range []bool{true, false} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/check/analytics-pipelines/3c1f9b42" || r.URL.Query().Get("permissions") != "a" {
				t.Errorf("request %s?%s, want /check/analytics-pipelines/3c1f9b42 with permissions=a", r.URL.Path, r.URL.RawQuery)
			}
			if r.Header.Get("Authorization") != "Bearer token" {
				t.Errorf("authorization %q, want the caller's token", r.Header.Get("Authorization"))
			}
			if granted {
				_, _ = w.Write([]byte("true"))
			} else {
				_, _ = w.Write([]byte("false"))
			}
		}))
		ok, err := NewPermissionApi(srv.URL).UserHasPipelinePermission(context.Background(), "3c1f9b42", client.Administrate, "Bearer token")
		srv.Close()
		if err != nil || ok != granted {
			t.Errorf("granted %v: got %v, %v", granted, ok, err)
		}
	}
}

func TestUserHasPipelinePermissionReportsAFailedCheck(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	if ok, err := NewPermissionApi(srv.URL).UserHasPipelinePermission(context.Background(), "3c1f9b42", client.Write, "Bearer token"); err == nil || ok {
		t.Fatalf("got %v, %v; want an error and no permission", ok, err)
	}
}
