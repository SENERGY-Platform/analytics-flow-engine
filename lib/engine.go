/*
 * Copyright 2019 InfAI (CC SES)
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
	"fmt"
	"strconv"
)

type FlowEngine struct {
	driver         Driver
	parsingService ParsingApiService
}

func NewFlowEngine(driver Driver, parsingService ParsingApiService) *FlowEngine {
	return &FlowEngine{driver, parsingService}
}

// Starts a pipeline.s
func (f *FlowEngine) StartPipeline(pipelineRequest PipelineRequest, userId string) (pipeline Pipeline) {
	//Get parsed pipeline
	parsedPipeline, _ := f.parsingService.GetPipeline(pipelineRequest.Id, userId)

	//Convert parsing Schema to internal Schema
	var tmpPipeline Pipeline
	for _, operator := range parsedPipeline {
		op := Operator{Id: operator.Id, Name: operator.Name, ImageId: operator.ImageId}
		for topicName, topic := range operator.InputTopics {
			top := InputTopic{Name: topicName, FilterType: topic.FilterType, FilterValue: topic.FilterValue}
			for _, mapping := range topic.Mappings {
				top.Mappings = append(top.Mappings, Mapping{mapping.Dest, mapping.Source})
			}
			op.InputTopics = append(op.InputTopics, top)
		}
		tmpPipeline.Operators = append(tmpPipeline.Operators, op)
	}

	//Add starting operators
	for _, operator := range tmpPipeline.Operators {
		for _, node := range pipelineRequest.Nodes {
			if operator.Id == node.NodeId {
				if len(node.Inputs) > 0 {
					for _, input := range node.Inputs {
						t := InputTopic{Name: input.TopicName, FilterType: "DeviceId", FilterValue: input.DeviceId}
						for _, value := range input.Values {
							t.Mappings = append(t.Mappings, Mapping{value.Name, value.Path})
						}
						operator.InputTopics = append(operator.InputTopics, t)
					}
				}

				if len(node.Config) > 0 {
					m := make(map[string]string)
					for _, config := range node.Config {
						m[config.Name] = config.Value
					}
					operator.Config = m
				}
			}
		}
		pipeline.Operators = append(pipeline.Operators, operator)
	}

	pipeline.Id, _ = registerPipeline(&pipeline, userId)
	f.startOperators(pipeline, pipelineRequest.Id)
	return pipeline
}

func (f *FlowEngine) startOperators(pipeline Pipeline, flowId string) {
	for operatorId, operator := range pipeline.Operators {
		fmt.Println(strconv.Itoa(operatorId) + ": Starting Operator:" + operator.Name)

		var outputTopic = f.getOperatorOutputTopic(operator.Name)

		fmt.Println(operator)

		f.driver.CreateOperator(
			pipeline.Id.String(),
			operator,
			operatorId+1,
			outputTopic,
			flowId,
		)
	}
}

func (f *FlowEngine) getOperatorOutputTopic(name string) (op_name string) {
	op_name = "analytics-" + name
	return
}

func (f *FlowEngine) GetPipelineStatus(id string) string {
	if f.driver.GetAnalyticsPipelineStatus(id) == PIPELINE_RUNNING {
		return PIPELINE_RUNNING
	}
	return PIPELINE_MISSING

}

func (f *FlowEngine) DeletePipeline(id string, userId string) string {
	println("Deleting Pipeline:" + id)
	f.driver.DeleteAnalyticsPipeline(id)
	deletePipeline(id, userId)
	return "done"
}
