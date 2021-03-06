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
	"analytics-flow-engine/internal/metrics-api"
	"analytics-flow-engine/internal/parsing-api"
)

type Driver interface {
	CreateOperator(pipelineId string, input Operator, pipelineConfig PipelineConfig) error
	CreateOperators(pipelineId string, input []Operator, pipelineConfig PipelineConfig) error
	DeleteOperator(id string) error
	GetOperatorName(pipelineId string, operator Operator) []string
}

type ParsingApiService interface {
	GetPipeline(id string, userId string, authorization string) (p parsing_api.Pipeline, err error)
}

type MetricsApiService interface {
	RegisterPipeline(id string) (metrics_api.MetricsConfig, error)
	UnregisterPipeline(id string) error
}

type PermissionApiService interface {
	UserHasDevicesReadAccess(ids []string, authorization string) (bool, error)
}
