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

package lib

import (
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/parsing-api"
	"github.com/SENERGY-Platform/models/go/models"

	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/kafka2mqtt-api"
)

type Driver interface {
	CreateOperators(pipelineId string, input []Operator, pipelineConfig PipelineConfig) error
	DeleteOperator(pipelineId string, input Operator) error
	GetPipelineStatus(pipelineId string) (PipelineStatus, error)
	GetPipelinesStatus() ([]PipelineStatus, error)
}

type ParsingApiService interface {
	GetPipeline(id string, userId string, authorization string) (p parsing_api.Pipeline, err error)
}

type PermissionApiService interface {
	UserHasDevicesReadAccess(ids []string, authorization string) (bool, error)
}

type Kafka2MqttApiService interface {
	StartOperatorInstance(operatorName, operatorID string, pipelineID, userI, token string) (kafka2mqtt_api.Instance, error)
	RemoveInstance(id, pipelineID, userID, token string) error
}

type DeviceManagerService interface {
	GetDevice(deviceID, userID, token string) (models.Device, error)
	GetDeviceType(deviceTypeID, userID, token string) (models.DeviceType, error)
}
