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
	"analytics-flow-engine/operator-api"
	"analytics-flow-engine/parsing-api"
)

type Driver interface {
	GetEnvData() map[string] interface{}
	CreateOperator(pipelineId string, operator operator_api.Operator, inputs Operator, outputTopic string, flowId string) string
	DeleteOperator(id string) map[string] interface {}
	DeleteAnalyticsPipeline(flowId string)
	GetAnalyticsPipelineStatus(flowId string) string
}

type OperatorApiService interface {
	GetOperator(id string) (op operator_api.Operator, err error)
}

type ParsingApiService interface {
	GetPipeline(id string, userId string) (p parsing_api.Pipeline, err error)
}

