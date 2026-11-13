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

package service

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	kubernetes_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/kubernetes-api"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
	"github.com/google/uuid"
)

const (
	registeredId = "11111111-1111-4111-8111-111111111111"
	orphanId     = "22222222-2222-4222-8222-222222222222"
	volumeOnlyId = "33333333-3333-4333-8333-333333333333"
)

var cleanupNow = time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)

// callLog records the order in which the fakes were asked.
type callLog struct {
	mu    sync.Mutex
	calls []string
}

func (l *callLog) add(call string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, call)
}

func (l *callLog) get() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.calls)
}

type fakeCluster struct {
	log       *callLog
	inv       kubernetes_api.Inventory
	deleteErr map[string]error
	mu        sync.Mutex
	deleted   []string
}

func (f *fakeCluster) Inventory(ctx context.Context) (kubernetes_api.Inventory, error) {
	if err := ctx.Err(); err != nil {
		return kubernetes_api.Inventory{}, err
	}
	f.log.add("inventory")
	return f.inv, nil
}

func (f *fakeCluster) del(ctx context.Context, kind, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.deleteErr[name]; err != nil {
		return err
	}
	f.deleted = append(f.deleted, kind+"/"+name)
	return nil
}

func (f *fakeCluster) DeleteDeployment(ctx context.Context, name string) error {
	return f.del(ctx, "deployment", name)
}
func (f *fakeCluster) DeleteVPA(ctx context.Context, name string) error {
	return f.del(ctx, "vpa", name)
}
func (f *fakeCluster) DeleteVPACheckpoint(ctx context.Context, name string) error {
	return f.del(ctx, "checkpoint", name)
}
func (f *fakeCluster) DeletePVC(ctx context.Context, name string) error {
	return f.del(ctx, "pvc", name)
}

func (f *fakeCluster) deletedObjects() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.deleted)
}

// adminRegistry answers GetPipelinesAdmin from a fixed list or error.
type adminRegistry struct {
	*recordingPipelines
	log       *callLog
	pipelines []pipe.Pipeline
	err       error
}

func (r adminRegistry) GetPipelinesAdmin(ctx context.Context) ([]pipe.Pipeline, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.log.add("registry")
	return r.pipelines, r.err
}

type fakeTopics struct {
	log       *callLog
	topics    []string
	deleteErr map[string]error
	// entered, if set, gets a value when a delete starts; the delete then waits for gate.
	entered chan struct{}
	gate    chan struct{}
	mu      sync.Mutex
	deleted []string
}

func (f *fakeTopics) ListTopics(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.log.add("topics")
	return f.topics, nil
}

func (f *fakeTopics) DeleteTopic(ctx context.Context, topic string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.entered != nil {
		f.entered <- struct{}{}
		<-f.gate
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.deleteErr[topic]; err != nil {
		return err
	}
	f.deleted = append(f.deleted, topic)
	return nil
}

func (f *fakeTopics) deletedTopics() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.deleted)
}

type cleanupFixture struct {
	log      *callLog
	cluster  *fakeCluster
	registry *adminRegistry
	topics   *fakeTopics
	cleanup  *Cleanup
}

func res(name string, age time.Duration) kubernetes_api.Resource {
	return kubernetes_api.Resource{Name: name, Created: cleanupNow.Add(-age)}
}

func dep(name string, age time.Duration, appIds ...string) kubernetes_api.Deployment {
	return kubernetes_api.Deployment{Resource: res(name, age), ApplicationIds: appIds}
}

// standardInventory holds a registered pipeline, an orphan with every kind of object,
// an orphan with only a volume, and objects whose names are not the engine's.
func standardInventory() kubernetes_api.Inventory {
	return kubernetes_api.Inventory{
		Deployments: []kubernetes_api.Deployment{
			dep("pipeline-"+registeredId, time.Hour),
			dep("pipeline-"+orphanId, 3*time.Hour),
			dep("nginx", 3*time.Hour),
		},
		VPAs: []kubernetes_api.Resource{
			res("pipeline-"+registeredId+"-vpa", time.Hour),
			res("pipeline-"+orphanId+"-vpa", 3*time.Hour),
		},
		Checkpoints: []kubernetes_api.Resource{
			res("pipeline-"+orphanId+"-vpa-op-b--x", 2*time.Hour),
			res("pipeline-"+orphanId+"-vpa-op-a--x", 150*time.Minute),
		},
		PVCs: []kubernetes_api.Resource{
			res("operator-"+orphanId+"-12345678", 3*time.Hour),
			res("operator-"+volumeOnlyId+"-abcdefgh", 30*time.Minute),
			res("data-something-else", 3*time.Hour),
		},
	}
}

func newCleanupFixture(inv kubernetes_api.Inventory, registered ...string) *cleanupFixture {
	log := &callLog{}
	var pipelines []pipe.Pipeline
	for _, id := range registered {
		pipelines = append(pipelines, pipe.Pipeline{Id: id})
	}
	fx := &cleanupFixture{
		log:      log,
		cluster:  &fakeCluster{log: log, inv: inv},
		registry: &adminRegistry{recordingPipelines: &recordingPipelines{}, log: log, pipelines: pipelines},
		topics:   &fakeTopics{log: log},
	}
	engine := engineWith(&recordingDriver{}, &recordingPipelines{})
	engine.pipelineService = fx.registry
	fx.cleanup = NewCleanup(engine, fx.cluster, fx.topics, 15*time.Minute)
	fx.cleanup.now = func() time.Time { return cleanupNow }
	fx.cleanup.topicInterval = time.Millisecond
	return fx
}

func orphanIds(orphans []OrphanPipeline) []string {
	var ids []string
	for _, o := range orphans {
		ids = append(ids, o.PipelineId)
	}
	return ids
}

func TestOrphanPipelinesGroupsObjectsByPipeline(t *testing.T) {
	fx := newCleanupFixture(standardInventory(), registeredId)

	orphans, err := fx.cleanup.OrphanPipelines(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(orphanIds(orphans), []string{orphanId, volumeOnlyId}) {
		t.Fatalf("orphans = %+v, want the orphan and the volume-only one", orphans)
	}
	o := orphans[0]
	if o.Deployment != "pipeline-"+orphanId || o.VPA != "pipeline-"+orphanId+"-vpa" {
		t.Errorf("deployment %q, vpa %q", o.Deployment, o.VPA)
	}
	if !slices.Equal(o.Checkpoints, []string{"pipeline-" + orphanId + "-vpa-op-a--x", "pipeline-" + orphanId + "-vpa-op-b--x"}) {
		t.Errorf("checkpoints = %v", o.Checkpoints)
	}
	if !slices.Equal(o.PVCs, []string{"operator-" + orphanId + "-12345678"}) {
		t.Errorf("pvcs = %v", o.PVCs)
	}
	if want := cleanupNow.Add(-2 * time.Hour); !o.Created.Equal(want) || o.Created.Location() != time.UTC {
		t.Errorf("created = %v, want the newest object's time %v in UTC", o.Created, want)
	}
	if v := orphans[1]; v.Deployment != "" || v.VPA != "" || len(v.Checkpoints) != 0 || len(v.PVCs) != 1 {
		t.Errorf("volume-only orphan = %+v", v)
	}
}

// A pipeline registered after the registry was read cannot be in an earlier cluster
// listing; the other order could see its deployment and miss its entry.
func TestOrphanPipelinesListsTheClusterBeforeTheRegistry(t *testing.T) {
	fx := newCleanupFixture(standardInventory(), registeredId)
	if _, err := fx.cleanup.OrphanPipelines(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls := fx.log.get(); !slices.Equal(calls, []string{"inventory", "registry"}) {
		t.Errorf("calls = %v, want the cluster first", calls)
	}
}

// The age counts from the newest object, so a pipeline whose checkpoint was just
// written is left alone; an object without a creation time is not judged at all.
func TestAnOrphanYoungerThanTheMinimumAgeIsNotReported(t *testing.T) {
	inv := kubernetes_api.Inventory{
		Deployments: []kubernetes_api.Deployment{dep("pipeline-"+orphanId, 3*time.Hour)},
		Checkpoints: []kubernetes_api.Resource{res("pipeline-"+orphanId+"-vpa-op--x", 15*time.Minute-time.Nanosecond)},
		PVCs: []kubernetes_api.Resource{
			res("operator-"+volumeOnlyId+"-abcdefgh", 15*time.Minute),
			{Name: "operator-" + registeredId + "-abcdefgh"},
		},
	}
	orphans, err := newCleanupFixture(inv, "44444444-4444-4444-8444-444444444444").cleanup.OrphanPipelines(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(orphanIds(orphans), []string{volumeOnlyId}) {
		t.Errorf("orphans = %v, want only the one exactly at the minimum age", orphanIds(orphans))
	}
}

func TestARegistryErrorReportsNoOrphan(t *testing.T) {
	fx := newCleanupFixture(standardInventory(), registeredId)
	fx.registry.err = errors.New("registry down")

	orphans, err := fx.cleanup.OrphanPipelines(context.Background())
	var unavailable *RegistryUnavailableError
	if !errors.As(err, &unavailable) || orphans != nil {
		t.Fatalf("orphans %v, err %v; want none and RegistryUnavailableError", orphans, err)
	}
	if strings.Contains(unavailable.Reason, "registry down") {
		t.Errorf("reason %q carries the internal error", unavailable.Reason)
	}
	if _, err := fx.cleanup.DeleteOrphanPipelines(context.Background()); !errors.As(err, &unavailable) {
		t.Errorf("delete all: err = %v, want RegistryUnavailableError", err)
	}
	if err := fx.cleanup.DeleteOrphanPipeline(context.Background(), orphanId); !errors.As(err, &unavailable) {
		t.Errorf("delete one: err = %v, want RegistryUnavailableError", err)
	}
	if d := fx.cluster.deletedObjects(); len(d) != 0 {
		t.Errorf("deleted %v, want nothing", d)
	}
}

// An empty registry next to running deployments is a broken registry, not a cluster of
// orphans.
func TestAnEmptyRegistryBesideDeploymentsReportsNoOrphan(t *testing.T) {
	fx := newCleanupFixture(standardInventory())
	_, err := fx.cleanup.OrphanPipelines(context.Background())
	if !errors.As(err, new(*RegistryUnavailableError)) {
		t.Fatalf("err = %v, want RegistryUnavailableError", err)
	}
	if _, err = fx.cleanup.DeleteOrphanPipelines(context.Background()); !errors.As(err, new(*RegistryUnavailableError)) {
		t.Errorf("delete all: err = %v, want RegistryUnavailableError", err)
	}
	if d := fx.cluster.deletedObjects(); len(d) != 0 {
		t.Errorf("deleted %v, want nothing", d)
	}

	// Without any deployment an empty registry is plausible.
	volumesOnly := kubernetes_api.Inventory{PVCs: []kubernetes_api.Resource{res("operator-"+volumeOnlyId+"-abcdefgh", time.Hour)}}
	orphans, err := newCleanupFixture(volumesOnly).cleanup.OrphanPipelines(context.Background())
	if err != nil || len(orphans) != 1 {
		t.Errorf("orphans %v, err %v; want the volume", orphans, err)
	}
}

func TestDeleteOrphanPipelineDeletesEveryObjectFound(t *testing.T) {
	fx := newCleanupFixture(standardInventory(), registeredId)
	if err := fx.cleanup.DeleteOrphanPipeline(context.Background(), orphanId); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"deployment/pipeline-" + orphanId,
		"vpa/pipeline-" + orphanId + "-vpa",
		"checkpoint/pipeline-" + orphanId + "-vpa-op-a--x",
		"checkpoint/pipeline-" + orphanId + "-vpa-op-b--x",
		"pvc/operator-" + orphanId + "-12345678",
	}
	if d := fx.cluster.deletedObjects(); !slices.Equal(d, want) {
		t.Errorf("deleted %v, want %v", d, want)
	}
}

// The id comes from the client, so the check runs again: a registered, a too young or
// an unknown pipeline is not deleted.
func TestDeleteOrphanPipelineRefusesWhatIsNotOrphaned(t *testing.T) {
	inv := standardInventory()
	inv.Deployments = append(inv.Deployments, dep("pipeline-44444444-4444-4444-8444-444444444444", time.Minute))
	fx := newCleanupFixture(inv, registeredId)
	for _, id := range []string{registeredId, "44444444-4444-4444-8444-444444444444", "55555555-5555-4555-8555-555555555555", "nginx"} {
		if err := fx.cleanup.DeleteOrphanPipeline(context.Background(), id); !errors.As(err, new(*lib.NotFoundError)) {
			t.Errorf("%s: err = %v, want NotFoundError", id, err)
		}
	}
	if d := fx.cluster.deletedObjects(); len(d) != 0 {
		t.Errorf("deleted %v, want nothing", d)
	}
}

// The check runs under the pipeline's lock, after an update or delete of the same id.
func TestDeleteOrphanPipelineChecksUnderThePipelineLock(t *testing.T) {
	fx := newCleanupFixture(standardInventory(), registeredId)
	unlock := fx.cleanup.locks.lock(orphanId)
	done := make(chan error, 1)
	go func() { done <- fx.cleanup.DeleteOrphanPipeline(context.Background(), orphanId) }()

	time.Sleep(50 * time.Millisecond)
	if calls := fx.log.get(); len(calls) != 0 {
		t.Errorf("calls %v while the lock was held, want none", calls)
	}
	unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(fx.cluster.deletedObjects()) == 0 {
		t.Error("nothing deleted after the lock was released")
	}
}

// A delete that has started is finished after the client hung up, so no orphan is left
// half deleted.
func TestDeleteOrphanPipelineFinishesAfterTheClientHangsUp(t *testing.T) {
	fx := newCleanupFixture(standardInventory(), registeredId)
	if err := fx.cleanup.DeleteOrphanPipeline(cancelledContext(), orphanId); err != nil {
		t.Fatal(err)
	}
	if len(fx.cluster.deletedObjects()) != 5 {
		t.Errorf("deleted %v, want all five objects", fx.cluster.deletedObjects())
	}
}

func TestDeleteOrphanPipelinesGoesOnPastAFailure(t *testing.T) {
	fx := newCleanupFixture(standardInventory(), registeredId)
	fx.cluster.deleteErr = map[string]error{"pipeline-" + orphanId + "-vpa": errors.New("vpa refused")}

	result, err := fx.cleanup.DeleteOrphanPipelines(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(result.Deleted, []string{volumeOnlyId}) {
		t.Errorf("deleted = %v, want the volume-only orphan", result.Deleted)
	}
	if len(result.Failed) != 1 || result.Failed[0].PipelineId != orphanId || !strings.Contains(result.Failed[0].Error, "vpa refused") {
		t.Errorf("failed = %+v, want the orphan with the vpa error", result.Failed)
	}
	// The other objects of the failed pipeline are still deleted, so a retry finds less.
	if d := fx.cluster.deletedObjects(); !slices.Contains(d, "deployment/pipeline-"+orphanId) {
		t.Errorf("deleted %v, want the failed pipeline's deployment too", d)
	}
}

func TestDeleteOrphanPipelinesAnswersEmptyListsWhenNothingIsOrphaned(t *testing.T) {
	fx := newCleanupFixture(kubernetes_api.Inventory{}, registeredId)
	result, err := fx.cleanup.DeleteOrphanPipelines(context.Background())
	if err != nil || result.Deleted == nil || result.Failed == nil {
		t.Errorf("result %+v, err %v; want empty, non-nil lists", result, err)
	}
}

const (
	registryAppId   = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	deploymentAppId = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	unusedAppId     = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
)

func topicFixture() *cleanupFixture {
	inv := kubernetes_api.Inventory{Deployments: []kubernetes_api.Deployment{
		dep("pipeline-"+orphanId, time.Hour, deploymentAppId),
	}}
	fx := newCleanupFixture(inv)
	fx.registry.pipelines = []pipe.Pipeline{{Id: registeredId, Operators: []pipe.Operator{{ApplicationId: uuid.MustParse(registryAppId)}}}}
	fx.topics.topics = []string{
		"analytics-" + registryAppId + "-KSTREAM-TOTABLE-STATE-STORE-0000000002-changelog",
		"analytics-" + deploymentAppId + "-KSTREAM-REPARTITION-0000000005-repartition",
		"analytics-" + unusedAppId + "-KSTREAM-TOTABLE-STATE-STORE-0000000002-changelog",
		"analytics-" + unusedAppId + "-KSTREAM-JOINTHIS-0000000004-store-changelog",
		"analytics-" + unusedAppId + "-KSTREAM-REPARTITION-0000000005-repartition",
		// Not internal topics of an engine application id.
		"analytics-" + unusedAppId,
		"analytics-" + unusedAppId + "-changelog",
		"analytics-" + strings.ToUpper(unusedAppId) + "-x-changelog",
		"xanalytics-" + unusedAppId + "-x-changelog",
		"ode-" + unusedAppId + "-x-changelog",
		"analytics-" + unusedAppId + "-x-changelog-old",
		"device-data",
	}
	return fx
}

func TestOrphanTopicsAreInternalTopicsOfUnusedApplicationIds(t *testing.T) {
	fx := topicFixture()
	topics, err := fx.cleanup.OrphanTopics(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"analytics-" + unusedAppId + "-KSTREAM-JOINTHIS-0000000004-store-changelog",
		"analytics-" + unusedAppId + "-KSTREAM-REPARTITION-0000000005-repartition",
		"analytics-" + unusedAppId + "-KSTREAM-TOTABLE-STATE-STORE-0000000002-changelog",
	}
	if !slices.Equal(topics, want) {
		t.Errorf("orphaned topics = %v, want %v", topics, want)
	}
	if calls := fx.log.get(); !slices.Equal(calls, []string{"topics", "inventory", "registry"}) {
		t.Errorf("calls = %v, want the topics first, then the cluster, then the registry", calls)
	}
}

func TestOrphanTopicsFollowTheRegistryRules(t *testing.T) {
	fx := topicFixture()
	fx.registry.err = errors.New("registry down")
	if _, err := fx.cleanup.OrphanTopics(context.Background()); !errors.As(err, new(*RegistryUnavailableError)) {
		t.Errorf("registry error: err = %v, want RegistryUnavailableError", err)
	}
	fx = topicFixture()
	fx.registry.pipelines = nil
	if _, err := fx.cleanup.OrphanTopics(context.Background()); !errors.As(err, new(*RegistryUnavailableError)) {
		t.Errorf("empty registry: err = %v, want RegistryUnavailableError", err)
	}
	if err := fx.cleanup.StartTopicJob(context.Background()); !errors.As(err, new(*RegistryUnavailableError)) {
		t.Errorf("job start: err = %v, want RegistryUnavailableError", err)
	}
	if fx.cleanup.TopicJob().Running {
		t.Error("the job runs after a refused start")
	}
	if d := fx.topics.deletedTopics(); len(d) != 0 {
		t.Errorf("deleted %v, want nothing", d)
	}
}

func TestDeleteOrphanTopicChecksAgain(t *testing.T) {
	fx := topicFixture()
	inUse := "analytics-" + registryAppId + "-KSTREAM-TOTABLE-STATE-STORE-0000000002-changelog"
	for _, topic := range []string{inUse, "device-data", "analytics-" + unusedAppId + "-gone-changelog"} {
		if err := fx.cleanup.DeleteOrphanTopic(context.Background(), topic); !errors.As(err, new(*lib.NotFoundError)) {
			t.Errorf("%s: err = %v, want NotFoundError", topic, err)
		}
	}
	orphan := "analytics-" + unusedAppId + "-KSTREAM-REPARTITION-0000000005-repartition"
	if err := fx.cleanup.DeleteOrphanTopic(context.Background(), orphan); err != nil {
		t.Fatal(err)
	}
	if d := fx.topics.deletedTopics(); !slices.Equal(d, []string{orphan}) {
		t.Errorf("deleted %v, want only %s", d, orphan)
	}
}

func waitForJob(t *testing.T, c *Cleanup) TopicCleanupJob {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if job := c.TopicJob(); !job.Running {
			return job
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("the job did not finish")
	return TopicCleanupJob{}
}

// The job outlives the request that started it.
func TestATopicJobDeletesEveryOrphanedTopic(t *testing.T) {
	fx := topicFixture()
	failing := "analytics-" + unusedAppId + "-KSTREAM-JOINTHIS-0000000004-store-changelog"
	fx.topics.deleteErr = map[string]error{failing: errors.New("not allowed")}

	if err := fx.cleanup.StartTopicJob(cancelledContext()); err != nil {
		t.Fatal(err)
	}
	job := waitForJob(t, fx.cleanup)
	if job.Total != 3 || job.Remaining != 0 || len(job.Errors) != 1 || !strings.Contains(job.Errors[0], "not allowed") {
		t.Errorf("job = %+v, want 3 of 3 done with the one error", job)
	}
	if d := fx.topics.deletedTopics(); len(d) != 2 {
		t.Errorf("deleted %v, want the two that did not fail", d)
	}
}

func TestOnlyOneTopicJobRuns(t *testing.T) {
	fx := topicFixture()
	fx.cleanup.topicInterval = time.Hour
	if err := fx.cleanup.StopTopicJob(context.Background()); !errors.Is(err, ErrNoTopicJob) {
		t.Errorf("stop without job: err = %v, want ErrNoTopicJob", err)
	}
	if err := fx.cleanup.StartTopicJob(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := fx.cleanup.StartTopicJob(context.Background()); !errors.Is(err, ErrTopicJobRunning) {
		t.Errorf("second start: err = %v, want ErrTopicJobRunning", err)
	}
	// The first topic goes at once, the next one only after the pause.
	for deadline := time.Now().Add(5 * time.Second); fx.cleanup.TopicJob().Remaining != 2; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the first topic was not deleted")
		}
	}
	if err := fx.cleanup.StopTopicJob(context.Background()); err != nil {
		t.Fatal(err)
	}
	job := fx.cleanup.TopicJob()
	if job.Running || job.Total != 3 || job.Remaining != 2 {
		t.Errorf("job after stop = %+v, want stopped after the first of 3", job)
	}
	if d := fx.topics.deletedTopics(); len(d) != 1 {
		t.Errorf("deleted %v, want one before the pause", d)
	}
	if err := fx.cleanup.StopTopicJob(context.Background()); !errors.Is(err, ErrNoTopicJob) {
		t.Errorf("stop after stop: err = %v, want ErrNoTopicJob", err)
	}
	// A stopped job can be followed by a new one.
	fx.cleanup.topicInterval = time.Millisecond
	if err := fx.cleanup.StartTopicJob(context.Background()); err != nil {
		t.Fatalf("restart: %v", err)
	}
	waitForJob(t, fx.cleanup)
}

// Concurrent stops close the stop channel once and all return after the job ended;
// the delete in progress is finished.
func TestConcurrentStopsOfATopicJob(t *testing.T) {
	fx := topicFixture()
	fx.topics.entered, fx.topics.gate = make(chan struct{}, 3), make(chan struct{})
	if err := fx.cleanup.StartTopicJob(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-fx.topics.entered
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Go(func() { errs[i] = fx.cleanup.StopTopicJob(context.Background()) })
	}
	// The stops arrive while the first delete is still in progress.
	time.Sleep(50 * time.Millisecond)
	close(fx.topics.gate)
	wg.Wait()
	for i, err := range errs {
		// A stop that arrives only after the end finds no job, which is no failure.
		if err != nil && !errors.Is(err, ErrNoTopicJob) {
			t.Errorf("stop %d: %v", i, err)
		}
	}
	if d := fx.topics.deletedTopics(); len(d) != 1 {
		t.Errorf("deleted %v, want only the delete that was in progress", d)
	}
	if fx.cleanup.TopicJob().Running {
		t.Error("the job still runs")
	}
}

func TestATopicJobKeepsABoundedNumberOfErrors(t *testing.T) {
	fx := topicFixture()
	fx.cleanup.topicInterval = 0
	fx.topics.deleteErr = map[string]error{}
	fx.topics.topics = nil
	for i := range maxJobErrors + 5 {
		topic := fmt.Sprintf("analytics-%s-t%03d-changelog", unusedAppId, i)
		fx.topics.topics = append(fx.topics.topics, topic)
		fx.topics.deleteErr[topic] = errors.New("refused")
	}
	if err := fx.cleanup.StartTopicJob(context.Background()); err != nil {
		t.Fatal(err)
	}
	job := waitForJob(t, fx.cleanup)
	if len(job.Errors) != maxJobErrors+1 || job.Errors[maxJobErrors] != "5 more errors, see the log" {
		t.Errorf("errors = %d, last %q; want %d and a count of the rest", len(job.Errors), job.Errors[len(job.Errors)-1], maxJobErrors+1)
	}
}

func TestATopicJobStatusBeforeAnyJob(t *testing.T) {
	job := topicFixture().cleanup.TopicJob()
	if job.Running || job.Total != 0 || job.Errors == nil {
		t.Errorf("job = %+v, want idle with an empty, non-nil error list", job)
	}
}
