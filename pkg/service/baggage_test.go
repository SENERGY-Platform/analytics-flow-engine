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
	"testing"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	fe_baggage "github.com/SENERGY-Platform/analytics-flow-engine/pkg/baggage"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"
	parser "github.com/SENERGY-Platform/analytics-parser/lib"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
	"github.com/google/uuid"
	otelbaggage "go.opentelemetry.io/otel/baggage"
)

func init() {
	util.InitStructLogger("error")
}

// recordingDriver keeps the PipelineConfig it was handed, which is where the
// baggage has to arrive for either driver to be able to label anything.
type recordingDriver struct {
	configs     []lib.PipelineConfig
	deleteCalls int
}

func (d *recordingDriver) CreateOperators(ctx context.Context, _ string, _ []pipe.Operator, pipelineConfig lib.PipelineConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.configs = append(d.configs, pipelineConfig)
	return nil
}
func (d *recordingDriver) DeleteOperator(context.Context, string, pipe.Operator) error { return nil }
func (d *recordingDriver) DeleteOperators(ctx context.Context, _ string, _ []pipe.Operator) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.deleteCalls++
	return nil
}
func (d *recordingDriver) GetPipelineStatus(context.Context, string) (lib.PipelineStatus, error) {
	return lib.PipelineStatus{}, nil
}
func (d *recordingDriver) GetPipelinesStatus(context.Context) ([]lib.PipelineStatus, error) {
	return nil, nil
}

// recordingPipelines stands in for the registry and keeps what was persisted.
type recordingPipelines struct {
	assignedId  uuid.UUID
	stored      *pipe.Pipeline
	existing    pipe.Pipeline
	deleteCalls int
}

func (p *recordingPipelines) RegisterPipeline(ctx context.Context, _ *pipe.Pipeline, _ string, _ string) (uuid.UUID, error) {
	if err := ctx.Err(); err != nil {
		return uuid.UUID{}, err
	}
	return p.assignedId, nil
}
func (p *recordingPipelines) UpdatePipeline(ctx context.Context, pipeline *pipe.Pipeline, _ string, _ string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stored := *pipeline
	p.stored = &stored
	return nil
}
func (p *recordingPipelines) GetPipeline(ctx context.Context, _ string, _ string, _ string) (pipe.Pipeline, error) {
	if err := ctx.Err(); err != nil {
		return pipe.Pipeline{}, err
	}
	return p.existing, nil
}
func (p *recordingPipelines) GetPipelines(context.Context, string, string) ([]pipe.Pipeline, error) {
	return nil, nil
}
func (p *recordingPipelines) GetPipelinesAdmin(context.Context) ([]pipe.Pipeline, error) {
	return nil, nil
}
func (p *recordingPipelines) DeletePipeline(ctx context.Context, _ string, _ string, _ string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.deleteCalls++
	return nil
}

type staticParser struct {
	pipeline parser.Pipeline
}

func (s staticParser) GetPipeline(context.Context, string, string, string) (parser.Pipeline, error) {
	return s.pipeline, nil
}

type allowAll struct{}

func (allowAll) UserHasExecuteAccess(context.Context, string, []string, string) (bool, error) {
	return true, nil
}

// oneCloudOperatorFlow is the smallest flow that reaches the cloud driver: a
// single operator with no input topics, so nothing has to be authorized and no
// device has to be looked up.
func oneCloudOperatorFlow() parser.Pipeline {
	return parser.Pipeline{
		FlowId: "flow-1",
		Operators: map[string]parser.Operator{
			"op-1": {
				Id:             "op-1",
				Name:           "adder",
				OperatorId:     "base-op-1",
				ImageId:        "adder:latest",
				DeploymentType: "cloud",
			},
		},
	}
}

func engineWith(driver *recordingDriver, pipelines *recordingPipelines) *FlowEngine {
	return &FlowEngine{
		driver:              driver,
		parsingService:      staticParser{pipeline: oneCloudOperatorFlow()},
		permissionService:   allowAll{},
		pipelineService:     pipelines,
		timescaleConnection: "postgresql://example/postgres",
	}
}

func requestContext(t *testing.T, entries map[string]string) context.Context {
	t.Helper()
	ctx := context.Background()
	for key, value := range entries {
		var err error
		ctx, err = fe_baggage.WithValue(ctx, key, value)
		if err != nil {
			t.Fatalf("could not put %q into the baggage: %v", key, err)
		}
	}
	return ctx
}

func TestStartPipelineCarriesTheRequestBaggage(t *testing.T) {
	id := uuid.New()
	driver := &recordingDriver{}
	pipelines := &recordingPipelines{assignedId: id}
	engine := engineWith(driver, pipelines)

	ctx := requestContext(t, map[string]string{
		"smart_service_instance_id": "8fbd0e8a",
		"user_id":                   "jonah",
	})

	pipeline, err := engine.StartPipeline(ctx, lib.PipelineRequest{FlowId: "flow-1"}, "jonah", "token")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("the pipeline id joins the baggage", func(t *testing.T) {
		// It does not exist when the request arrives, so the middleware cannot add it.
		// Without it a log line from an operator names the smart service instance but
		// not which of its pipelines produced the line.
		if got := pipeline.Baggage[fe_baggage.PipelineIdKey]; got != id.String() {
			t.Errorf("expected the pipeline id %q in the baggage, got %q", id.String(), got)
		}
	})

	t.Run("the caller's entries are kept", func(t *testing.T) {
		if got := pipeline.Baggage["smart_service_instance_id"]; got != "8fbd0e8a" {
			t.Errorf("expected the instance id, got %q", got)
		}
	})

	t.Run("the driver is handed the baggage", func(t *testing.T) {
		if len(driver.configs) != 1 {
			t.Fatalf("expected one call to the driver, got %d", len(driver.configs))
		}
		got := driver.configs[0].Baggage
		if got["smart_service_instance_id"] != "8fbd0e8a" || got[fe_baggage.PipelineIdKey] != id.String() {
			t.Errorf("the driver has to see the full baggage to label with it, got %v", got)
		}
	})

	t.Run("it is persisted", func(t *testing.T) {
		// The reason it is stored at all: syncPipelines recreates deployments after a
		// restart and has no request to read the context off.
		if pipelines.stored == nil {
			t.Fatal("the pipeline was never persisted")
		}
		if pipelines.stored.Baggage[fe_baggage.PipelineIdKey] != id.String() {
			t.Errorf("expected the baggage to be persisted, got %v", pipelines.stored.Baggage)
		}
	})

	t.Run("the labels and the environment agree", func(t *testing.T) {
		labels, _ := fe_baggage.Labels(pipeline.Baggage)
		if labels[fe_baggage.LabelPrefix+"smart_service_instance_id"] != "8fbd0e8a" {
			t.Errorf("expected a label for the instance id, got %v", labels)
		}
		parsed, err := otelbaggage.Parse(fe_baggage.Header(pipeline.Baggage))
		if err != nil {
			t.Fatalf("the environment value must be parseable: %v", err)
		}
		if len(parsed.Members()) != 3 {
			t.Errorf("expected all three entries in the environment, got %v", parsed.Members())
		}
	})
}

func TestStartPipelineWithoutBaggage(t *testing.T) {
	// A caller that sends no context must not end up with an empty baggage object on
	// the pipeline, and must not fail.
	driver := &recordingDriver{}
	pipelines := &recordingPipelines{assignedId: uuid.New()}
	engine := engineWith(driver, pipelines)

	pipeline, err := engine.StartPipeline(context.Background(), lib.PipelineRequest{FlowId: "flow-1"}, "jonah", "token")
	if err != nil {
		t.Fatal(err)
	}
	// The pipeline id is added even then: it is the one piece of context this service
	// knows without being told.
	if len(pipeline.Baggage) != 1 || pipeline.Baggage[fe_baggage.PipelineIdKey] == "" {
		t.Errorf("expected only the pipeline id, got %v", pipeline.Baggage)
	}
}

func TestUpdatePipelineKeepsStoredBaggage(t *testing.T) {
	existingId := uuid.New().String()
	driver := &recordingDriver{}
	pipelines := &recordingPipelines{
		existing: pipe.Pipeline{
			Id: existingId,
			Baggage: map[string]string{
				"smart_service_instance_id": "8fbd0e8a",
				fe_baggage.PipelineIdKey:    existingId,
			},
		},
	}
	engine := engineWith(driver, pipelines)

	// An update from a caller that knows nothing about the smart service instance —
	// the web UI, say. otelx still puts user_id on it, so the incoming baggage is not
	// empty and a plain overwrite would lose the instance id.
	ctx := requestContext(t, map[string]string{"user_id": "someone-else"})

	pipeline, err := engine.UpdatePipeline(ctx, lib.PipelineRequest{Id: existingId, FlowId: "flow-1"}, "someone-else", "token")
	if err != nil {
		t.Fatal(err)
	}
	if got := pipeline.Baggage["smart_service_instance_id"]; got != "8fbd0e8a" {
		t.Errorf("the stored instance id has to survive an unrelated update, got %v", pipeline.Baggage)
	}
	if got := pipeline.Baggage["user_id"]; got != "someone-else" {
		t.Errorf("the request should win on its own keys, got %q", got)
	}
	if got := pipeline.Baggage[fe_baggage.PipelineIdKey]; got != existingId {
		t.Errorf("expected the pipeline id, got %q", got)
	}
}

func TestCreatePipelineConfigTakesBaggageFromThePipeline(t *testing.T) {
	// syncPipelines has no request context; it recreates a deployment out of the
	// stored pipeline alone, so the baggage has to travel through the pipeline rather
	// than through the context.
	engine := &FlowEngine{}
	stored := pipe.Pipeline{
		Id:      "3c1f9b42",
		Baggage: map[string]string{"smart_service_instance_id": "8fbd0e8a"},
	}
	if got := engine.createPipelineConfig(stored).Baggage["smart_service_instance_id"]; got != "8fbd0e8a" {
		t.Errorf("expected the stored baggage in the pipeline config, got %q", got)
	}
}

// A request the client gave up on must not leave the cluster and the registry
// disagreeing. These are the claims FlowEngine.deploymentContext makes, and every
// fake above returns ctx.Err() so an inherited cancellation shows up as a missing
// call rather than as nothing at all.

func cancelledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestStartPipelineFinishesAfterTheClientHangsUp(t *testing.T) {
	id := uuid.New()
	driver := &recordingDriver{}
	pipelines := &recordingPipelines{assignedId: id}
	engine := engineWith(driver, pipelines)

	_, err := engine.StartPipeline(cancelledContext(),
		lib.PipelineRequest{FlowId: "flow-1"}, "jonah", "token")
	if err != nil {
		t.Fatalf("a cancelled request must not fail the start: %v", err)
	}
	if len(driver.configs) != 1 {
		t.Errorf("expected the operators to be created, got %d calls", len(driver.configs))
	}
	// The call that persists the fog topics, the downstream instance ids and the
	// baggage. Skipping it leaves running operators the registry knows nothing
	// accurate about, and there is no rollback for that.
	if pipelines.stored == nil {
		t.Error("expected the pipeline to be persisted")
	} else if pipelines.stored.Baggage[fe_baggage.PipelineIdKey] != id.String() {
		t.Errorf("expected the baggage to be persisted, got %v", pipelines.stored.Baggage)
	}
}

func TestDeletePipelineFinishesAfterTheClientHangsUp(t *testing.T) {
	// The worst of the three: a deployment deleted while the registry entry survives
	// is recreated by the startup sync, so a deleted pipeline comes back.
	existingId := uuid.New().String()
	driver := &recordingDriver{}
	pipelines := &recordingPipelines{existing: pipe.Pipeline{
		Id: existingId,
		// A cloud operator, so there is something for the driver to tear down.
		Operators: []pipe.Operator{{Id: "op-1", DeploymentType: "cloud"}},
	}}
	engine := engineWith(driver, pipelines)

	if err := engine.DeletePipeline(cancelledContext(), existingId, "jonah", "token"); err != nil {
		t.Fatalf("a cancelled request must not fail the delete: %v", err)
	}
	if driver.deleteCalls != 1 {
		t.Errorf("expected the operators to be deleted, got %d calls", driver.deleteCalls)
	}
	if pipelines.deleteCalls != 1 {
		t.Errorf("expected the registry entry to be deleted, got %d calls", pipelines.deleteCalls)
	}
}

func TestUpdatePipelineFinishesAfterTheClientHangsUp(t *testing.T) {
	existingId := uuid.New().String()
	driver := &recordingDriver{}
	pipelines := &recordingPipelines{existing: pipe.Pipeline{
		Id:      existingId,
		Baggage: map[string]string{"smart_service_instance_id": "8fbd0e8a"},
	}}
	engine := engineWith(driver, pipelines)

	pipeline, err := engine.UpdatePipeline(cancelledContext(),
		lib.PipelineRequest{Id: existingId, FlowId: "flow-1"}, "jonah", "token")
	if err != nil {
		t.Fatalf("a cancelled request must not fail the update: %v", err)
	}
	if len(driver.configs) != 1 {
		t.Errorf("expected the new operators to be created, got %d calls", len(driver.configs))
	}
	if pipeline.Baggage["smart_service_instance_id"] != "8fbd0e8a" {
		t.Errorf("expected the stored baggage to survive, got %v", pipeline.Baggage)
	}
	if pipelines.stored == nil {
		t.Error("expected the pipeline to be persisted")
	}
}

func TestGetPipelineStatusStaysCancellable(t *testing.T) {
	// The counterpart: an abandoned read costs nothing and leaves nothing behind, so
	// it must not be forced through. If this ever passes, deploymentContext has been
	// applied too widely.
	driver := &recordingDriver{}
	pipelines := &recordingPipelines{existing: pipe.Pipeline{Id: "3c1f9b42"}}
	engine := engineWith(driver, pipelines)

	if _, err := engine.GetPipelineStatus(cancelledContext(), "3c1f9b42", "jonah", "token"); err == nil {
		t.Error("expected a cancelled read to be abandoned")
	}
}
