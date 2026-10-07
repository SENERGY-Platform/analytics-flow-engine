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

package analytics_serving_api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestListExportsDecodesThePage(t *testing.T) {
	var gotPath, gotQuery, gotAuth, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath, gotQuery, gotAuth = r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"total":7,"count":1,"instances":[{"ID":"e1","Topic":"t","Filter":"imp","FilterType":"import_id",
			"Database":"db","ExportDatabase":{"Type":"timescaledb","EwFilterTopic":"x"},
			"Values":[{"Name":"col","Type":"float","Path":"value.a","Tag":false}]}]}`))
	}))
	defer srv.Close()

	got, total, err := NewAnalyticsServingApi(srv.URL).ListExports(context.Background(), "Bearer tok", 1000, 20)
	if err != nil {
		t.Fatalf("ListExports: %v", err)
	}
	if gotMethod != http.MethodGet || gotPath != "/instance" {
		t.Errorf("request = %s %s, want GET /instance", gotMethod, gotPath)
	}
	if gotQuery != "limit=1000&offset=20" {
		t.Errorf("query = %q", gotQuery)
	}
	if gotAuth != "Bearer tok" {
		t.Errorf("Authorization = %q, want the caller's token as given", gotAuth)
	}
	if total != 7 || len(got) != 1 {
		t.Fatalf("total=%d len=%d, want 7 and 1", total, len(got))
	}
	e := got[0]
	if e.ID != "e1" || e.ExportDatabase.Type != "timescaledb" || len(e.Values) != 1 || e.Values[0].Path != "value.a" {
		t.Errorf("decoded export = %+v", e)
	}
}

func TestListExportsNon2xxIsAnErrorWithTheStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusForbidden)
	}))
	defer srv.Close()

	_, _, err := NewAnalyticsServingApi(srv.URL).ListExports(context.Background(), "tok", 10, 0)
	if err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("error = %v, want one carrying 403", err)
	}
}

func TestListExportsGarbageBodyIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`not json`))
	}))
	defer srv.Close()

	if _, _, err := NewAnalyticsServingApi(srv.URL).ListExports(context.Background(), "tok", 10, 0); err == nil {
		t.Fatal("no error for an undecodable body")
	}
}

func TestListExportsHonoursTheContext(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"total":0,"count":0,"instances":[]}`))
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := NewAnalyticsServingApi(srv.URL).ListExports(ctx, "tok", 10, 0); err == nil {
		t.Fatal("no error for a cancelled context")
	}
}
