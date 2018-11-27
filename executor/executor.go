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

package executor

import (
	"analytics-flow-engine/lib"
	"fmt"
	"strconv"
)

type FlowExecutor struct {
	driver         lib.Driver
	operator_api   lib.OperatorApiService
	parsingService lib.ParsingApiService
}

func NewFlowExecutor(driver lib.Driver, operator_api lib.OperatorApiService, parsingService lib.ParsingApiService) *FlowExecutor {
	return &FlowExecutor{driver, operator_api, parsingService}
}

// Starts a pipeline.s
func (f *FlowExecutor) StartPipeline(pipelineRequest lib.PipelineRequest, userId string) (pipeline lib.Pipeline) {
	//Get parsed pipeline
	parsedPipeline, _ := f.parsingService.GetPipeline(pipelineRequest.Id, userId)

	//Convert parsing Schema to internal Schema
	var tmpPipeline lib.Pipeline
	for _, operator := range parsedPipeline {
		op := lib.Operator{Id: operator.Id, Name: operator.Name, ImageId: operator.ImageId}
		for topicName, topic := range operator.InputTopics {
			top := lib.InputTopic{Name: topicName, FilterType: topic.FilterType, FilterValue: topic.FilterValue}
			for _, mapping := range topic.Mappings {
				top.Mappings = append(top.Mappings, lib.Mapping{mapping.Dest, mapping.Source})
			}
			op.InputTopics = append(op.InputTopics, top)
		}
		tmpPipeline.Operators = append(tmpPipeline.Operators, op)
	}

	//Add starting operators
	for _, operator := range tmpPipeline.Operators {
		for _, node := range pipelineRequest.Nodes {
			if operator.Id == node.NodeId {
				for _, input := range node.Inputs {
					t := lib.InputTopic{Name: input.TopicName, FilterType: "DeviceId" ,FilterValue: input.DeviceId}
					for _, value := range input.Values {
						t.Mappings = append(t.Mappings, lib.Mapping{value.Name, value.Path})
					}
					operator.InputTopics = append(operator.InputTopics, t)
				}
			}
		}
		pipeline.Operators = append(pipeline.Operators, operator)
	}
	pipeline.Id, _ = registerPipeline(&pipeline, userId)
	f.startOperators(pipeline, pipelineRequest.Id)
	return pipeline
}

func (f *FlowExecutor) startOperators(pipeline lib.Pipeline, flowId string) {
	for operatorId, operator := range pipeline.Operators {
		operatorData, _ := f.operator_api.GetOperator(operator.ImageId)
		fmt.Println(strconv.Itoa(operatorId) + ": Starting Operator:" + operator.Name)

		var outputTopic = f.getOperatorOutputTopic(operator.Name)

		f.driver.CreateOperator(
			pipeline.Id.String(),
			operatorData,
			operator,
			outputTopic,
			flowId,
		)
	}
}

func (f *FlowExecutor) getOperatorOutputTopic(name string) (op_name string) {
	op_name = "analytics-" + name
	return
}

func (f *FlowExecutor) GetPipelineStatus(id string) string {
	if f.driver.GetAnalyticsPipelineStatus(id) == lib.PIPELINE_RUNNING {
		return lib.PIPELINE_RUNNING
	}
	return lib.PIPELINE_MISSING

}

func (f *FlowExecutor) DeletePipeline(id string, userId string) string {
	println("Deleting Pipeline:" + id)
	f.driver.DeleteAnalyticsPipeline(id)
	deletePipeline(id, userId)
	return "done"
}
