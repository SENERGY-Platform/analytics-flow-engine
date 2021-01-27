/*
 * Copyright 2020 InfAI (CC SES)
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
	parsingApi "analytics-flow-engine/internal/parsing-api"
	"github.com/google/uuid"
	"strings"
)

func createPipeline(parsedPipeline parsingApi.Pipeline) (pipeline Pipeline) {
	for _, operator := range parsedPipeline.Operators {
		outputTopicName := operator.Name
		if operator.DeploymentType != "local" {
			outputTopicName = getOperatorOutputTopic(operator.Name)
		}
		op := Operator{
			Id:             operator.Id,
			ApplicationId:  uuid.New(),
			Name:           operator.Name,
			ImageId:        operator.ImageId,
			OperatorId:     operator.OperatorId,
			DeploymentType: operator.DeploymentType,
			OutputTopic:    outputTopicName,
		}
		for _, topic := range operator.InputTopics {
			top := InputTopic{Name: topic.TopicName, FilterType: topic.FilterType, FilterValue: topic.FilterValue}
			for _, mapping := range topic.Mappings {
				top.Mappings = append(top.Mappings, Mapping{mapping.Dest, mapping.Source})
			}
			op.InputTopics = append(op.InputTopics, top)
		}
		pipeline.Operators = append(pipeline.Operators, op)
	}
	return pipeline
}

func addStartingOperatorConfigs(pipelineRequest PipelineRequest, tmpPipeline Pipeline) (operators []Operator) {
	for _, operator := range tmpPipeline.Operators {
		for _, node := range pipelineRequest.Nodes {
			if operator.Id == node.NodeId {
				operator.InputSelections = node.InputSelections
				if len(node.Inputs) > 0 {
					for _, input := range node.Inputs {
						deviceId := input.DeviceId
						var deviceIds []string
						if operator.DeploymentType == "local" {
							deviceIds = strings.Split(input.DeviceId, ",")
						}
						for topicKey, topicName := range strings.Split(input.TopicName, ",") {
							if operator.DeploymentType == "local" {
								if len(deviceIds) > 0 {
									deviceId = deviceIds[topicKey]
								}
							}
							t := InputTopic{Name: topicName, FilterType: "DeviceId", FilterValue: deviceId}
							for _, value := range input.Values {
								t.Mappings = append(t.Mappings, Mapping{value.Name, value.Path})
							}
							operator.InputTopics = append(operator.InputTopics, t)
						}
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
		operators = append(operators, operator)
	}
	return
}

func getOperatorOutputTopic(name string) (operatorName string) {
	return "analytics-" + name
}
