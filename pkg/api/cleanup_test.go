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

package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/service"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"
	gin_mw "github.com/SENERGY-Platform/gin-middleware"
	"github.com/gin-gonic/gin"
)

const cleanupPrefix = "/analytics/flow-engine/v2"

// fakeCleanup answers every call with err, if set, and records the calls.
type fakeCleanup struct {
	err     error
	calls   []string
	orphans []service.OrphanPipeline
	result  service.OrphanPipelinesDeleteResult
	topics  []string
	job     service.TopicCleanupJob
}

func (f *fakeCleanup) record(call string) error {
	f.calls = append(f.calls, call)
	return f.err
}

func (f *fakeCleanup) OrphanPipelines(context.Context) ([]service.OrphanPipeline, error) {
	return f.orphans, f.record("OrphanPipelines")
}
func (f *fakeCleanup) DeleteOrphanPipeline(_ context.Context, id string) error {
	return f.record("DeleteOrphanPipeline " + id)
}
func (f *fakeCleanup) DeleteOrphanPipelines(context.Context) (service.OrphanPipelinesDeleteResult, error) {
	return f.result, f.record("DeleteOrphanPipelines")
}
func (f *fakeCleanup) OrphanTopics(context.Context) ([]string, error) {
	return f.topics, f.record("OrphanTopics")
}
func (f *fakeCleanup) DeleteOrphanTopic(_ context.Context, topic string) error {
	return f.record("DeleteOrphanTopic " + topic)
}
func (f *fakeCleanup) StartTopicJob(context.Context) error { return f.record("StartTopicJob") }
func (f *fakeCleanup) TopicJob() service.TopicCleanupJob {
	_ = f.record("TopicJob")
	return f.job
}
func (f *fakeCleanup) StopTopicJob(context.Context) error { return f.record("StopTopicJob") }

// cleanupRouter mounts the routes the way CreateServer does: behind the prefix, the
// error handler and AuthMiddleware.
func cleanupRouter(t *testing.T, cleanup CleanupService) *gin.Engine {
	t.Helper()
	r := gin.New()
	r.UseRawPath = true
	r.Use(gin_mw.ErrorHandler(util.GetStatusCode, ", "))
	prefix := r.Group(cleanupPrefix)
	prefix.Use(AuthMiddleware())
	if _, err := setCleanupRoutes(prefix, cleanup); err != nil {
		t.Fatal(err)
	}
	return r
}

func tokenWithRoles(t *testing.T, roles ...string) string {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"sub":          "8fbd0e8a-0000-0000-0000-000000000000",
		"realm_access": map[string][]string{"roles": roles},
	})
	if err != nil {
		t.Fatal(err)
	}
	encode := base64.RawURLEncoding.EncodeToString
	return "Bearer " + encode([]byte(`{"alg":"none","typ":"JWT"}`)) + "." + encode(payload) + "."
}

func serve(t *testing.T, r *gin.Engine, method, path string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(method, cleanupPrefix+path, nil)
	for k, v := range headers {
		request.Header.Set(k, v)
	}
	r.ServeHTTP(recorder, request)
	return recorder
}

func asAdmin(t *testing.T) map[string]string {
	return map[string]string{"Authorization": tokenWithRoles(t, "user", "admin")}
}

var cleanupRoutes = []struct{ method, path string }{
	{http.MethodGet, "/admin/cleanup/pipelines"},
	{http.MethodDelete, "/admin/cleanup/pipelines/06a798d6-cf4c-47ec-a696-0622f0d1aac0"},
	{http.MethodDelete, "/admin/cleanup/pipelines"},
	{http.MethodGet, "/admin/cleanup/topics"},
	{http.MethodDelete, "/admin/cleanup/topics/analytics-x-changelog"},
	{http.MethodPost, "/admin/cleanup/topics/job"},
	{http.MethodGet, "/admin/cleanup/topics/job"},
	{http.MethodDelete, "/admin/cleanup/topics/job"},
}

// The role comes from the token alone: a header claiming admin, as the gateway sets it
// for impersonation, does not open these routes.
func TestCleanupRoutesRefuseAllButAdmins(t *testing.T) {
	cases := map[string]struct {
		headers map[string]string
		want    int
	}{
		"user token":            {map[string]string{"Authorization": tokenWithRoles(t, "user")}, http.StatusForbidden},
		"user token with roles": {map[string]string{"Authorization": tokenWithRoles(t, "user"), "X-User-Roles": "admin"}, http.StatusForbidden},
		"user id only":          {map[string]string{"X-UserId": "8fbd0e8a", "X-User-Roles": "admin"}, http.StatusUnauthorized},
		"nothing":               {nil, http.StatusUnauthorized},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			cleanup := &fakeCleanup{}
			r := cleanupRouter(t, cleanup)
			for _, route := range cleanupRoutes {
				if got := serve(t, r, route.method, route.path, c.headers).Code; got != c.want {
					t.Errorf("%s %s = %d, want %d", route.method, route.path, got, c.want)
				}
			}
			if len(cleanup.calls) != 0 {
				t.Errorf("service called: %v", cleanup.calls)
			}
		})
	}
}

// for_user changes nothing on these routes: the caller's own admin role decides.
func TestCleanupRoutesIgnoreForUser(t *testing.T) {
	cleanup := &fakeCleanup{}
	r := cleanupRouter(t, cleanup)
	if got := serve(t, r, http.MethodGet, "/admin/cleanup/pipelines?for_user=someone", asAdmin(t)).Code; got != http.StatusOK {
		t.Errorf("admin with for_user = %d, want 200", got)
	}
	user := map[string]string{"Authorization": tokenWithRoles(t, "user"), "X-User-Roles": "admin"}
	if got := serve(t, r, http.MethodGet, "/admin/cleanup/pipelines?for_user=someone", user).Code; got != http.StatusForbidden {
		t.Errorf("user impersonating with for_user = %d, want 403", got)
	}
}

// The web-ui reads exactly these field names.
func TestCleanupPipelinesContract(t *testing.T) {
	created := time.Date(2026, 11, 12, 17, 22, 17, 0, time.UTC)
	cleanup := &fakeCleanup{
		orphans: []service.OrphanPipeline{{PipelineId: "p1", Created: created, Deployment: "pipeline-p1", VPA: "", Checkpoints: []string{}, PVCs: []string{"operator-p1-12345678"}}},
		result:  service.OrphanPipelinesDeleteResult{Deleted: []string{"p1"}, Failed: []service.OrphanPipelineFailure{{PipelineId: "p2", Error: "e"}}},
	}
	r := cleanupRouter(t, cleanup)

	list := serve(t, r, http.MethodGet, "/admin/cleanup/pipelines", asAdmin(t))
	var listed []map[string]any
	if err := json.Unmarshal(list.Body.Bytes(), &listed); list.Code != http.StatusOK || err != nil {
		t.Fatalf("list = %d %s", list.Code, list.Body)
	}
	want := map[string]any{"pipeline_id": "p1", "created": "2026-11-12T17:22:17Z", "deployment": "pipeline-p1", "vpa": "",
		"checkpoints": []any{}, "pvcs": []any{"operator-p1-12345678"}}
	if !reflect.DeepEqual(listed[0], want) {
		t.Errorf("listed %v, want %v", listed[0], want)
	}

	if got := serve(t, r, http.MethodDelete, "/admin/cleanup/pipelines/p1", asAdmin(t)); got.Code != http.StatusNoContent {
		t.Errorf("delete one = %d", got.Code)
	}
	all := serve(t, r, http.MethodDelete, "/admin/cleanup/pipelines", asAdmin(t))
	var result map[string]any
	if err := json.Unmarshal(all.Body.Bytes(), &result); all.Code != http.StatusOK || err != nil {
		t.Fatalf("delete all = %d %s", all.Code, all.Body)
	}
	wantResult := map[string]any{"deleted": []any{"p1"}, "failed": []any{map[string]any{"pipeline_id": "p2", "error": "e"}}}
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("delete all = %v, want %v", result, wantResult)
	}
	if !slices.Contains(cleanup.calls, "DeleteOrphanPipeline p1") {
		t.Errorf("calls = %v", cleanup.calls)
	}
}

func TestCleanupTopicsContract(t *testing.T) {
	cleanup := &fakeCleanup{topics: []string{"analytics-x-repartition"}, job: service.TopicCleanupJob{Total: 3, Remaining: 2, Running: true, Errors: []string{}}}
	r := cleanupRouter(t, cleanup)

	list := serve(t, r, http.MethodGet, "/admin/cleanup/topics", asAdmin(t))
	if list.Code != http.StatusOK || list.Body.String() != `["analytics-x-repartition"]` {
		t.Errorf("list = %d %s", list.Code, list.Body)
	}
	if got := serve(t, r, http.MethodDelete, "/admin/cleanup/topics/analytics-x-KSTREAM-0000000001-changelog", asAdmin(t)).Code; got != http.StatusNoContent {
		t.Errorf("delete topic = %d", got)
	}
	if got := serve(t, r, http.MethodPost, "/admin/cleanup/topics/job", asAdmin(t)).Code; got != http.StatusAccepted {
		t.Errorf("start job = %d", got)
	}
	job := serve(t, r, http.MethodGet, "/admin/cleanup/topics/job", asAdmin(t))
	if job.Code != http.StatusOK || job.Body.String() != `{"total":3,"remaining":2,"running":true,"errors":[]}` {
		t.Errorf("job = %d %s", job.Code, job.Body)
	}
	if got := serve(t, r, http.MethodDelete, "/admin/cleanup/topics/job", asAdmin(t)).Code; got != http.StatusNoContent {
		t.Errorf("stop job = %d", got)
	}
	// The job route is not taken for a topic named "job".
	want := []string{"OrphanTopics", "DeleteOrphanTopic analytics-x-KSTREAM-0000000001-changelog", "StartTopicJob", "TopicJob", "StopTopicJob"}
	if !slices.Equal(cleanup.calls, want) {
		t.Errorf("calls = %v, want %v", cleanup.calls, want)
	}
}

func TestCleanupErrorStatuses(t *testing.T) {
	unavailable := &service.RegistryUnavailableError{Reason: "the pipeline registry could not be read, nothing is reported"}
	wrapped := fmt.Errorf("wrapped: %w", unavailable)
	cases := []struct {
		name   string
		err    error
		method string
		path   string
		want   int
		body   string
	}{
		{"registry on list", wrapped, http.MethodGet, "/admin/cleanup/pipelines", http.StatusServiceUnavailable, unavailable.Reason},
		{"registry on delete all", unavailable, http.MethodDelete, "/admin/cleanup/pipelines", http.StatusServiceUnavailable, unavailable.Reason},
		{"registry on delete", unavailable, http.MethodDelete, "/admin/cleanup/pipelines/p1", http.StatusServiceUnavailable, unavailable.Reason},
		{"registry on topics", unavailable, http.MethodGet, "/admin/cleanup/topics", http.StatusServiceUnavailable, unavailable.Reason},
		{"registry on topic delete", unavailable, http.MethodDelete, "/admin/cleanup/topics/t", http.StatusServiceUnavailable, unavailable.Reason},
		{"registry on job start", unavailable, http.MethodPost, "/admin/cleanup/topics/job", http.StatusServiceUnavailable, unavailable.Reason},
		{"not orphaned", lib.NewNotFoundError(errors.New("pipeline p1 is not orphaned")), http.MethodDelete, "/admin/cleanup/pipelines/p1", http.StatusNotFound, MessageNotFound},
		{"topic not orphaned", lib.NewNotFoundError(errors.New("x")), http.MethodDelete, "/admin/cleanup/topics/t", http.StatusNotFound, MessageNotFound},
		{"job running", service.ErrTopicJobRunning, http.MethodPost, "/admin/cleanup/topics/job", http.StatusConflict, service.ErrTopicJobRunning.Error()},
		{"no job", service.ErrNoTopicJob, http.MethodDelete, "/admin/cleanup/topics/job", http.StatusConflict, service.ErrNoTopicJob.Error()},
		{"cluster", errors.New("list deployments: forbidden"), http.MethodGet, "/admin/cleanup/pipelines", http.StatusInternalServerError, MessageSomethingWrong},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := serve(t, cleanupRouter(t, &fakeCleanup{err: c.err}), c.method, c.path, asAdmin(t))
			if got.Code != c.want || strings.TrimSpace(got.Body.String()) != c.body {
				t.Errorf("= %d %q, want %d %q", got.Code, got.Body, c.want, c.body)
			}
		})
	}
}
