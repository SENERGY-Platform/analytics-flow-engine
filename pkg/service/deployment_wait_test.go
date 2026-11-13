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
	"fmt"
	"testing"

	kubernetes_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/kubernetes-api"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
)

// The driver already waited its whole budget for the old deployment; five more attempts
// would each wait that long again in a call nobody can cancel.
func TestStartOperatorsDoesNotRetryADeploymentStillTerminating(t *testing.T) {
	driver := &recordingDriver{createErr: map[string]error{
		"3c1f9b42": fmt.Errorf("%w: pipeline-3c1f9b42 after 1m0s", kubernetes_api.ErrDeploymentStillTerminating),
	}}
	engine := engineWith(driver, &recordingPipelines{})
	pipeline := pipe.Pipeline{Id: "3c1f9b42", Operators: []pipe.Operator{{Id: "op-1", DeploymentType: "cloud"}}}

	_, err := engine.startOperators(context.Background(), pipeline, engine.createPipelineConfig(pipeline), "")
	if !errors.Is(err, kubernetes_api.ErrDeploymentStillTerminating) {
		t.Fatalf("err = %v, want ErrDeploymentStillTerminating", err)
	}
	if driver.createAttempts != 1 {
		t.Errorf("CreateOperators attempts = %d, want 1", driver.createAttempts)
	}
}
