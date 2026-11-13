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

package service

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
	permV2 "github.com/SENERGY-Platform/permissions-v2/pkg/client"
)

// pipelineRights grants every right on a pipeline except the one it denies.
type pipelineRights struct {
	allowAll
	denied permV2.Permission
}

func (p pipelineRights) UserHasPipelinePermission(_ context.Context, _ string, permission permV2.Permission, _ string) (bool, error) {
	return permission != p.denied, nil
}

func runningPipeline() *recordingPipelines {
	return &recordingPipelines{existing: pipe.Pipeline{Id: "3c1f9b42", UserId: "owner",
		Operators: []pipe.Operator{{Id: "op-1", DeploymentType: "cloud"}}}}
}

func forbidden(err error) bool {
	return errors.As(err, new(*lib.ForbiddenError)) && util.GetStatusCode(err) == http.StatusForbidden
}

// A caller who may read a pipeline but not administrate it got its operators stopped,
// and only then the registry's 403; the entry stayed and the startup sync rebuilt it.
func TestADeleteWithoutAdministrateTearsNothingDown(t *testing.T) {
	driver, pipelines := &recordingDriver{}, runningPipeline()
	engine := engineWith(driver, pipelines)
	engine.permissionService = pipelineRights{denied: permV2.Administrate}

	if err := engine.DeletePipeline(context.Background(), "3c1f9b42", "reader", "token"); !forbidden(err) {
		t.Fatalf("err = %v, want 403", err)
	}
	if driver.deleteCalls != 0 || pipelines.deleteCalls != 0 {
		t.Errorf("operator deletes %d, registry deletes %d; want none", driver.deleteCalls, pipelines.deleteCalls)
	}

	engine.permissionService = pipelineRights{denied: permV2.Write}
	if err := engine.DeletePipeline(context.Background(), "3c1f9b42", "admin", "token"); err != nil {
		t.Fatalf("delete with administrate: %v", err)
	}
}

func TestAnUpdateWithoutWriteTearsNothingDown(t *testing.T) {
	driver, pipelines := &recordingDriver{}, runningPipeline()
	engine := engineWith(driver, pipelines)
	engine.permissionService = pipelineRights{denied: permV2.Write}

	request := lib.PipelineRequest{Id: "3c1f9b42", FlowId: "flow-1"}
	if _, err := engine.UpdatePipeline(context.Background(), request, "reader", "token"); !forbidden(err) {
		t.Fatalf("err = %v, want 403", err)
	}
	if driver.deleteCalls != 0 || len(driver.configs) != 0 || pipelines.stored != nil {
		t.Errorf("operator deletes %d, creates %d, stored %v; want nothing", driver.deleteCalls, len(driver.configs), pipelines.stored)
	}

	engine.permissionService = pipelineRights{denied: permV2.Administrate}
	if _, err := engine.UpdatePipeline(context.Background(), request, "writer", "token"); err != nil {
		t.Fatalf("update with write: %v", err)
	}
}
