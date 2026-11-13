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
	"sync"
	"testing"
	"time"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
	"github.com/google/uuid"
)

// overlapDriver records how many requests are between stopping a pipeline's
// operators and starting the new ones at the same time.
type overlapDriver struct {
	mu      sync.Mutex
	active  int
	maxSeen int
}

func (d *overlapDriver) CreateOperators(context.Context, string, []pipe.Operator, lib.PipelineConfig) error {
	time.Sleep(20 * time.Millisecond)
	d.mu.Lock()
	d.active--
	d.mu.Unlock()
	return nil
}
func (d *overlapDriver) DeleteOperator(context.Context, string, pipe.Operator) error { return nil }
func (d *overlapDriver) DeleteOperators(context.Context, string, []pipe.Operator) error {
	d.mu.Lock()
	d.active++
	d.maxSeen = max(d.maxSeen, d.active)
	d.mu.Unlock()
	time.Sleep(20 * time.Millisecond)
	return nil
}
func (d *overlapDriver) DeleteOperatorsKeepingVolumes(ctx context.Context, id string, ops []pipe.Operator) error {
	return d.DeleteOperators(ctx, id, ops)
}
func (d *overlapDriver) GetPipelineStatus(context.Context, string) (lib.PipelineStatus, error) {
	return lib.PipelineStatus{}, nil
}
func (d *overlapDriver) GetPipelinesStatus(context.Context) ([]lib.PipelineStatus, error) {
	return nil, nil
}

type concurrentPipelines struct {
	mu       sync.Mutex
	pipeline pipe.Pipeline
}

func (p *concurrentPipelines) RegisterPipeline(context.Context, *pipe.Pipeline, string, string) (uuid.UUID, error) {
	return uuid.UUID{}, nil
}
func (p *concurrentPipelines) UpdatePipeline(_ context.Context, pipeline *pipe.Pipeline, _ string, _ string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pipeline = *pipeline
	return nil
}
func (p *concurrentPipelines) GetPipeline(context.Context, string, string, string) (pipe.Pipeline, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pipeline, nil
}
func (p *concurrentPipelines) GetPipelines(context.Context, string, string) ([]pipe.Pipeline, error) {
	return nil, nil
}
func (p *concurrentPipelines) GetPipelinesAdmin(context.Context) ([]pipe.Pipeline, error) {
	return nil, nil
}
func (p *concurrentPipelines) DeletePipeline(context.Context, string, string, string) error {
	return nil
}

// Several smart service instances update the same pipeline within seconds. Each update
// has to stop the operators the previous one started, not the ones it read before.
func TestConcurrentUpdatesOfOnePipelineRunOneAfterTheOther(t *testing.T) {
	id := uuid.New().String()
	driver := &overlapDriver{}
	engine := &FlowEngine{
		driver:            driver,
		parsingService:    staticParser{pipeline: oneCloudOperatorFlow()},
		permissionService: allowAll{},
		pipelineService: &concurrentPipelines{pipeline: pipe.Pipeline{
			Id:        id,
			Operators: []pipe.Operator{{Id: "op-1", DeploymentType: "cloud"}},
		}},
		locks: newPipelineLocks(),
	}

	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			if _, err := engine.UpdatePipeline(context.Background(), lib.PipelineRequest{Id: id, FlowId: "flow-1"}, "user", "token"); err != nil {
				t.Errorf("update: %v", err)
			}
		})
	}
	wg.Wait()

	if driver.maxSeen != 1 {
		t.Errorf("%d updates of the same pipeline were between stop and start at once, want 1", driver.maxSeen)
	}
}
