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
	"maps"
	"regexp"
	"slices"
	"sync"
	"time"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	kubernetes_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/kubernetes-api"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"
	operatorLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/operator"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
)

// ClusterInventory lists the pipelines namespace and deletes its objects by name.
type ClusterInventory interface {
	Inventory(ctx context.Context) (kubernetes_api.Inventory, error)
	DeleteDeployment(ctx context.Context, name string) error
	DeleteVPA(ctx context.Context, name string) error
	DeleteVPACheckpoint(ctx context.Context, name string) error
	DeletePVC(ctx context.Context, name string) error
}

// TopicAdmin lists and deletes Kafka topics; deleting a missing topic succeeds.
type TopicAdmin interface {
	ListTopics(ctx context.Context) ([]string, error)
	DeleteTopic(ctx context.Context, topic string) error
}

// OrphanPipeline is a pipeline id with objects in the pipelines namespace and no
// registry entry. Created is the creation time of its newest object.
type OrphanPipeline struct {
	PipelineId  string    `json:"pipeline_id"`
	Created     time.Time `json:"created"`
	Deployment  string    `json:"deployment"`
	VPA         string    `json:"vpa"`
	Checkpoints []string  `json:"checkpoints"`
	PVCs        []string  `json:"pvcs"`
}

type OrphanPipelinesDeleteResult struct {
	Deleted []string                `json:"deleted"`
	Failed  []OrphanPipelineFailure `json:"failed"`
}

type OrphanPipelineFailure struct {
	PipelineId string `json:"pipeline_id"`
	Error      string `json:"error"`
}

type TopicCleanupJob struct {
	Total     int      `json:"total"`
	Remaining int      `json:"remaining"`
	Running   bool     `json:"running"`
	Errors    []string `json:"errors"`
}

// RegistryUnavailableError means no orphan can be told apart because the registry
// could not be read or answered implausibly. Reason is safe to show to the caller.
type RegistryUnavailableError struct {
	Reason string
	cause  error
}

func (e *RegistryUnavailableError) Error() string {
	if e.cause == nil {
		return e.Reason
	}
	return e.Reason + ": " + e.cause.Error()
}

func (e *RegistryUnavailableError) Unwrap() error { return e.cause }

var (
	ErrTopicJobRunning = errors.New("a topic cleanup job is already running")
	ErrNoTopicJob      = errors.New("no topic cleanup job is running")
)

// internalTopic matches the internal topics of Kafka Streams, named
// "<application.id>-<name>-repartition|changelog", for the application ids the engine
// sets ("analytics-<uuid>"). The submatch is the uuid.
var internalTopic = regexp.MustCompile(`^analytics-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})-.+-(?:repartition|changelog)$`)

const (
	defaultTopicInterval = time.Second
	// orphansWithoutShareCheck is how many orphaned pipeline ids are reported without
	// comparing them to maxOrphanShare, so a small cleanup always works.
	orphansWithoutShareCheck = 5
	// defaultListTimeout bounds the listing of the cluster, the registry and the topics.
	// The callers' contexts are not cancelled by the client, and a delete holds the
	// pipeline lock while it lists.
	defaultListTimeout = time.Minute
	// maxJobErrors bounds the errors a job keeps for its status; the log has all.
	maxJobErrors = 100
)

// Cleanup finds and removes what deleted pipelines left behind.
type Cleanup struct {
	cluster  ClusterInventory
	topics   TopicAdmin
	registry PipelineApiService
	locks    *pipelineLocks
	minAge   time.Duration
	// maxOrphanShare is the largest share of the cluster's pipeline ids that may be
	// orphaned before the registry is taken for incomplete.
	maxOrphanShare float64
	now            func() time.Time
	// topicInterval is the pause between two deletes of a topic job.
	topicInterval time.Duration
	// topicRecheckInterval is the longest a topic job deletes on one check of which
	// topics are in use.
	topicRecheckInterval time.Duration
	listTimeout          time.Duration
	job                  topicJob
}

type topicJob struct {
	mu            sync.Mutex
	running       bool
	stopRequested bool
	total         int
	remaining     int
	errors        []string
	droppedErrors int
	stop          chan struct{}
	done          chan struct{}
}

// NewCleanup shares the engine's registry client and per-pipeline locks, so a delete
// here waits for an update or delete of the same pipeline.
func NewCleanup(engine *FlowEngine, cluster ClusterInventory, topics TopicAdmin, minAge time.Duration, maxOrphanShare float64) *Cleanup {
	return &Cleanup{
		cluster:        cluster,
		topics:         topics,
		registry:       engine.pipelineService,
		locks:          engine.locks,
		minAge:         minAge,
		maxOrphanShare: maxOrphanShare,
		now:            time.Now,
		topicInterval:  defaultTopicInterval,
		listTimeout:    defaultListTimeout,
		// One check per topic would cost a registry and a cluster listing every second.
		topicRecheckInterval: 30 * time.Second,
	}
}

// OrphanPipelines lists the orphaned pipelines whose newest object is at least the
// minimum age old.
func (c *Cleanup) OrphanPipelines(ctx context.Context) ([]OrphanPipeline, error) {
	return c.orphanPipelines(ctx)
}

// DeleteOrphanPipeline deletes the objects of one pipeline after checking again, under
// the pipeline's lock, that it is orphaned; a lib.NotFoundError if it is not.
func (c *Cleanup) DeleteOrphanPipeline(ctx context.Context, pipelineId string) error {
	ctx = deploymentContext(ctx)
	defer c.locks.lock(pipelineId)()
	orphans, err := c.orphanPipelines(ctx)
	if err != nil {
		return err
	}
	i := slices.IndexFunc(orphans, func(o OrphanPipeline) bool { return o.PipelineId == pipelineId })
	if i < 0 {
		return lib.NewNotFoundError(fmt.Errorf("pipeline %s is not orphaned", pipelineId))
	}
	return c.deleteOrphan(ctx, orphans[i])
}

// DeleteOrphanPipelines deletes every orphaned pipeline found by one check, each under
// its lock, and goes on past a failed one.
func (c *Cleanup) DeleteOrphanPipelines(ctx context.Context) (OrphanPipelinesDeleteResult, error) {
	ctx = deploymentContext(ctx)
	result := OrphanPipelinesDeleteResult{Deleted: []string{}, Failed: []OrphanPipelineFailure{}}
	orphans, err := c.orphanPipelines(ctx)
	if err != nil {
		return result, err
	}
	for _, orphan := range orphans {
		unlock := c.locks.lock(orphan.PipelineId)
		err := c.deleteOrphan(ctx, orphan)
		unlock()
		if err != nil {
			result.Failed = append(result.Failed, OrphanPipelineFailure{PipelineId: orphan.PipelineId, Error: err.Error()})
			continue
		}
		result.Deleted = append(result.Deleted, orphan.PipelineId)
	}
	return result, nil
}

func (c *Cleanup) deleteOrphan(ctx context.Context, orphan OrphanPipeline) error {
	var errs []error
	if orphan.Deployment != "" {
		errs = append(errs, c.cluster.DeleteDeployment(ctx, orphan.Deployment))
	}
	if orphan.VPA != "" {
		errs = append(errs, c.cluster.DeleteVPA(ctx, orphan.VPA))
	}
	for _, name := range orphan.Checkpoints {
		errs = append(errs, c.cluster.DeleteVPACheckpoint(ctx, name))
	}
	for _, name := range orphan.PVCs {
		errs = append(errs, c.cluster.DeletePVC(ctx, name))
	}
	if err := errors.Join(errs...); err != nil {
		util.Logger.ErrorContext(ctx, "could not delete all objects of an orphaned pipeline", "pipeline_id", orphan.PipelineId, "error", err)
		return err
	}
	util.Logger.InfoContext(ctx, "deleted orphaned pipeline", "pipeline_id", orphan.PipelineId,
		"deployment", orphan.Deployment, "vpa", orphan.VPA, "checkpoints", orphan.Checkpoints, "pvcs", orphan.PVCs)
	return nil
}

// orphanPipelines lists the cluster before the registry: a pipeline registered in
// between is then at worst missing from the cluster listing, never taken for an orphan.
func (c *Cleanup) orphanPipelines(ctx context.Context) ([]OrphanPipeline, error) {
	ctx, cancel := context.WithTimeout(ctx, c.listTimeout)
	defer cancel()
	inv, err := c.cluster.Inventory(ctx)
	if err != nil {
		return nil, fmt.Errorf("list the pipelines namespace: %w", err)
	}
	registered, err := c.registeredPipelines(ctx, inv)
	if err != nil {
		return nil, err
	}
	registeredIds := make([]string, 0, len(registered))
	for _, p := range registered {
		registeredIds = append(registeredIds, p.Id)
	}

	byId := groupByPipeline(inv)
	_, orphaned := diffPipelineIds(registeredIds, slices.Sorted(maps.Keys(byId)))
	// Counted before the age filter: a registry missing entries also misses young ones.
	if n := len(orphaned); n > orphansWithoutShareCheck && float64(n)/float64(len(byId)) > c.maxOrphanShare {
		return nil, &RegistryUnavailableError{Reason: fmt.Sprintf(
			"%d of the %d pipelines in the cluster have no registry entry, more than the share of %v that cleanup_max_orphan_share allows; "+
				"the registry is taken to be incomplete, nothing is reported", n, len(byId), c.maxOrphanShare)}
	}
	now := c.now()
	orphans := []OrphanPipeline{}
	for _, id := range orphaned {
		g := byId[id]
		if g.unknownAge || now.Sub(g.Created) < c.minAge {
			util.Logger.DebugContext(ctx, "orphan candidate too young to report", "pipeline_id", id, "created", g.Created)
			continue
		}
		g.Created = g.Created.UTC()
		orphans = append(orphans, g.OrphanPipeline)
	}
	return orphans, nil
}

// registeredPipelines reads the registry. An empty answer while the cluster holds any
// deployment or pipeline object is taken for a broken registry, not for every pipeline
// being orphaned.
func (c *Cleanup) registeredPipelines(ctx context.Context, inv kubernetes_api.Inventory) ([]pipe.Pipeline, error) {
	pipelines, err := c.registry.GetPipelinesAdmin(ctx)
	if err != nil {
		return nil, &RegistryUnavailableError{Reason: "the pipeline registry could not be read, nothing is reported", cause: err}
	}
	if len(pipelines) == 0 {
		if ids := len(groupByPipeline(inv)); ids > 0 || len(inv.Deployments) > 0 {
			return nil, &RegistryUnavailableError{Reason: fmt.Sprintf(
				"the pipeline registry returned no pipelines while the cluster holds %d deployments and objects of %d pipelines, nothing is reported",
				len(inv.Deployments), ids)}
		}
	}
	return pipelines, nil
}

type pipelineObjects struct {
	OrphanPipeline
	// unknownAge is set when an object carries no creation time.
	unknownAge bool
}

// groupByPipeline collects the objects whose names the engine produces by pipeline id.
// Objects with other names are not the engine's and are left out.
func groupByPipeline(inv kubernetes_api.Inventory) map[string]*pipelineObjects {
	byId := map[string]*pipelineObjects{}
	add := func(id string, r kubernetes_api.Resource) *pipelineObjects {
		g := byId[id]
		if g == nil {
			g = &pipelineObjects{OrphanPipeline: OrphanPipeline{PipelineId: id, Checkpoints: []string{}, PVCs: []string{}}}
			byId[id] = g
		}
		if r.Created.IsZero() {
			g.unknownAge = true
		} else if r.Created.After(g.Created) {
			g.Created = r.Created
		}
		return g
	}
	for _, d := range inv.Deployments {
		if id, ok := kubernetes_api.PipelineIdFromDeploymentName(d.Name); ok {
			add(id, d.Resource).Deployment = d.Name
		}
	}
	for _, r := range inv.VPAs {
		if id, ok := kubernetes_api.PipelineIdFromVPAName(r.Name); ok {
			add(id, r).VPA = r.Name
		}
	}
	for _, r := range inv.Checkpoints {
		if id, ok := kubernetes_api.PipelineIdFromCheckpointName(r.Name); ok {
			g := add(id, r)
			g.Checkpoints = append(g.Checkpoints, r.Name)
		}
	}
	for _, r := range inv.PVCs {
		if id, ok := kubernetes_api.PipelineIdFromPVCName(r.Name); ok {
			g := add(id, r)
			g.PVCs = append(g.PVCs, r.Name)
		}
	}
	for _, g := range byId {
		slices.Sort(g.Checkpoints)
		slices.Sort(g.PVCs)
	}
	return byId
}

// OrphanTopics lists the internal Kafka Streams topics of application ids that
// neither a registered pipeline nor a deployment uses.
func (c *Cleanup) OrphanTopics(ctx context.Context) ([]string, error) {
	return c.orphanTopics(ctx)
}

// DeleteOrphanTopic deletes one topic after checking again that it is orphaned; a
// lib.NotFoundError if it is not.
func (c *Cleanup) DeleteOrphanTopic(ctx context.Context, topic string) error {
	ctx = deploymentContext(ctx)
	orphans, err := c.orphanTopics(ctx)
	if err != nil {
		return err
	}
	if !slices.Contains(orphans, topic) {
		return lib.NewNotFoundError(fmt.Errorf("topic %s is not orphaned", topic))
	}
	if err = c.topics.DeleteTopic(ctx, topic); err != nil {
		return err
	}
	util.Logger.InfoContext(ctx, "deleted orphaned topic", "topic", topic)
	return nil
}

// orphanTopics lists the topics before the cluster and the registry, so a topic that
// appears in between is not in the listing to be judged.
func (c *Cleanup) orphanTopics(ctx context.Context) ([]string, error) {
	orphans, _, err := c.orphanTopicsAndUse(ctx)
	return orphans, err
}

func (c *Cleanup) orphanTopicsAndUse(ctx context.Context) ([]string, topicUse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.listTimeout)
	defer cancel()
	topics, err := c.topics.ListTopics(ctx)
	if err != nil {
		return nil, topicUse{}, fmt.Errorf("list topics: %w", err)
	}
	use, err := c.topicsInUse(ctx)
	if err != nil {
		return nil, topicUse{}, err
	}
	orphans := []string{}
	for _, topic := range topics {
		if use.orphaned(topic) {
			orphans = append(orphans, topic)
		}
	}
	slices.Sort(orphans)
	return orphans, use, nil
}

// topicsInUse lists the cluster before the registry, as orphanPipelines does.
func (c *Cleanup) topicsInUse(ctx context.Context) (topicUse, error) {
	inv, err := c.cluster.Inventory(ctx)
	if err != nil {
		return topicUse{}, fmt.Errorf("list the pipelines namespace: %w", err)
	}
	registered, err := c.registeredPipelines(ctx, inv)
	if err != nil {
		return topicUse{}, err
	}
	return newTopicUse(registered, inv), nil
}

// topicUse is what keeps a topic from being orphaned: the application ids of the
// registered pipelines and of the deployments, and the registered operators' output
// topics, whose names are user-chosen and may look like internal topics.
type topicUse struct {
	applicationIds map[string]bool
	outputTopics   map[string]bool
}

func newTopicUse(registered []pipe.Pipeline, inv kubernetes_api.Inventory) topicUse {
	use := topicUse{applicationIds: map[string]bool{}, outputTopics: map[string]bool{}}
	for _, p := range registered {
		for _, op := range p.Operators {
			use.applicationIds[op.ApplicationId.String()] = true
			use.outputTopics[operatorLib.GenerateCloudOperatorTopic(op.Name)] = true
			if op.OutputTopic != "" {
				use.outputTopics[op.OutputTopic] = true
			}
		}
	}
	for _, d := range inv.Deployments {
		for _, id := range d.ApplicationIds {
			use.applicationIds[id] = true
		}
	}
	return use
}

func (u topicUse) orphaned(topic string) bool {
	if u.outputTopics[topic] {
		return false
	}
	m := internalTopic.FindStringSubmatch(topic)
	return m != nil && !u.applicationIds[m[1]]
}

// StartTopicJob checks for orphaned topics and deletes them in the background, one per
// topicInterval. Which topics are in use is checked again every topicRecheckInterval,
// since an update reuses application ids and a new pipeline may take a listed one.
func (c *Cleanup) StartTopicJob(ctx context.Context) error {
	j := &c.job
	j.mu.Lock()
	if j.running {
		j.mu.Unlock()
		return ErrTopicJobRunning
	}
	stop, done := make(chan struct{}), make(chan struct{})
	j.running, j.stopRequested, j.stop, j.done = true, false, stop, done
	j.total, j.remaining, j.errors, j.droppedErrors = 0, 0, nil, 0
	j.mu.Unlock()

	// The job outlives the request; it keeps the request's trace but not its cancellation.
	ctx = deploymentContext(ctx)
	topics, use, err := c.orphanTopicsAndUse(ctx)
	if err != nil {
		c.finishTopicJob(done)
		return err
	}
	checked := time.Now()
	j.mu.Lock()
	j.total, j.remaining = len(topics), len(topics)
	j.mu.Unlock()
	util.Logger.InfoContext(ctx, "started deleting orphaned topics", "total", len(topics))
	go c.runTopicJob(ctx, topics, use, checked, stop, done)
	return nil
}

// runTopicJob deletes topics as long as the last check of use is younger than
// topicRecheckInterval; checked is a time.Now value, so the age is measured monotonically.
func (c *Cleanup) runTopicJob(ctx context.Context, topics []string, use topicUse, checked time.Time, stop, done chan struct{}) {
	defer c.finishTopicJob(done)
	for i, topic := range topics {
		if i > 0 {
			select {
			case <-stop:
			case <-time.After(c.topicInterval):
			}
		}
		if time.Since(checked) >= c.topicRecheckInterval {
			fresh, err := c.recheckTopicUse(ctx)
			if err != nil {
				util.Logger.ErrorContext(ctx, "stopped deleting orphaned topics, could not check again which are in use",
					"remaining", len(topics)-i, "error", err)
				c.job.record("stopped, could not check again which topics are in use: " + publicReason(err))
				return
			}
			use, checked = fresh, time.Now()
		}
		// Checked on its own, so a stop wins over a pause or a check that has also ended.
		select {
		case <-stop:
			util.Logger.InfoContext(ctx, "stopped deleting orphaned topics", "remaining", len(topics)-i)
			return
		default:
		}
		if !use.orphaned(topic) {
			util.Logger.InfoContext(ctx, "skipped orphaned topic, it is in use now", "topic", topic)
			c.job.handled("skipped, now in use: " + topic)
			continue
		}
		err := c.topics.DeleteTopic(ctx, topic)
		if err != nil {
			util.Logger.ErrorContext(ctx, "could not delete orphaned topic", "topic", topic, "error", err)
			c.job.handled(err.Error())
			continue
		}
		util.Logger.InfoContext(ctx, "deleted orphaned topic", "topic", topic)
		c.job.handled("")
	}
	util.Logger.InfoContext(ctx, "finished deleting orphaned topics", "total", len(topics))
}

func (c *Cleanup) recheckTopicUse(ctx context.Context) (topicUse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.listTimeout)
	defer cancel()
	return c.topicsInUse(ctx)
}

// publicReason is the text of err fit for the job status: a registry error's reason
// without its cause, which stays in the log.
func publicReason(err error) string {
	var unavailable *RegistryUnavailableError
	if errors.As(err, &unavailable) {
		return unavailable.Reason
	}
	return err.Error()
}

// handled counts one topic as done and keeps msg, if any, for the status.
func (j *topicJob) handled(msg string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.remaining--
	j.recordLocked(msg)
}

func (j *topicJob) record(msg string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.recordLocked(msg)
}

// recordLocked keeps up to maxJobErrors messages and counts the rest; the log has all.
func (j *topicJob) recordLocked(msg string) {
	switch {
	case msg == "":
	case len(j.errors) < maxJobErrors:
		j.errors = append(j.errors, msg)
	default:
		j.droppedErrors++
	}
}

func (c *Cleanup) finishTopicJob(done chan struct{}) {
	c.job.mu.Lock()
	c.job.running = false
	c.job.mu.Unlock()
	close(done)
}

// StopTopicJob stops the running job and returns once it has stopped; a delete in
// progress is finished first.
func (c *Cleanup) StopTopicJob(ctx context.Context) error {
	j := &c.job
	j.mu.Lock()
	if !j.running {
		j.mu.Unlock()
		return ErrNoTopicJob
	}
	if !j.stopRequested {
		j.stopRequested = true
		close(j.stop)
	}
	done := j.done
	j.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TopicJob is the state of the running or last job.
func (c *Cleanup) TopicJob() TopicCleanupJob {
	j := &c.job
	j.mu.Lock()
	defer j.mu.Unlock()
	errs := append([]string{}, j.errors...)
	if j.droppedErrors > 0 {
		errs = append(errs, fmt.Sprintf("%d more errors, see the log", j.droppedErrors))
	}
	return TopicCleanupJob{Total: j.total, Remaining: j.remaining, Running: j.running, Errors: errs}
}
