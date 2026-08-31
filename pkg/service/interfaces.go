/*
 * Copyright 2018 InfAI (CC SES)
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

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/google/uuid"

	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/kafka2mqtt-api"
	parser "github.com/SENERGY-Platform/analytics-parser/lib"
)

// Every method takes a context so that the trace and the baggage of the caller
// reach the service on the other side. Without it each of these calls starts a
// trace of its own and the log lines it produces there cannot be tied back to the
// request that caused them.
//
// The context the Driver gets is deliberately not cancellable: see the comment on
// FlowEngine.deploymentContext.
type Driver interface {
	CreateOperators(ctx context.Context, pipelineId string, input []pipe.Operator, pipelineConfig lib.PipelineConfig) error
	/*
		DeleteOperator deletes an operator in the given pipeline
		Deprecated: Use DeleteOperators instead.
	*/
	DeleteOperator(ctx context.Context, pipelineId string, input pipe.Operator) error
	DeleteOperators(ctx context.Context, pipelineId string, inputs []pipe.Operator) error
	GetPipelineStatus(ctx context.Context, pipelineId string) (lib.PipelineStatus, error)
	GetPipelinesStatus(ctx context.Context) ([]lib.PipelineStatus, error)
}

type ParsingApiService interface {
	GetPipeline(ctx context.Context, id string, userId string, authorization string) (p parser.Pipeline, err error)
}

type PermissionApiService interface {
	UserHasExecuteAccess(ctx context.Context, resource string, ids []string, authorization string) (bool, error)
}

type Kafka2MqttApiService interface {
	StartOperatorInstance(ctx context.Context, operatorName, operatorID string, pipelineID, userI, token string) (kafka2mqtt_api.Instance, error)
	RemoveInstance(ctx context.Context, id, pipelineID, userID, token string) error
}

type DeviceManagerService interface {
	GetDevice(ctx context.Context, deviceID, userID, token string) (models.Device, error)
	GetDeviceType(ctx context.Context, deviceTypeID, userID, token string) (models.DeviceType, error)
}

type PipelineApiService interface {
	RegisterPipeline(ctx context.Context, pipeline *pipe.Pipeline, userId string, authorization string) (id uuid.UUID, err error)
	UpdatePipeline(ctx context.Context, pipeline *pipe.Pipeline, userId string, authorization string) (err error)
	GetPipeline(ctx context.Context, id string, userId string, authorization string) (pipe pipe.Pipeline, err error)
	GetPipelines(ctx context.Context, userId string, authorization string) (pipelines []pipe.Pipeline, err error)
	GetPipelinesAdmin(ctx context.Context) (pipelines []pipe.Pipeline, err error)
	DeletePipeline(ctx context.Context, id string, userId string, authorization string) (err error)
}
