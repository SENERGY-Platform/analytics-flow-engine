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
	parsingApi "github.com/SENERGY-Platform/analytics-flow-engine/pkg/parsing-api"
	"github.com/google/uuid"
	operatorLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/operator"
	deploymentLocationLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/location"
	deviceLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/devices"
	"strings"
)

func createPipeline(parsedPipeline parsingApi.Pipeline) (pipeline Pipeline) {
	for _, operator := range parsedPipeline.Operators {
		// TODO error handling
		var outputTopicName string
		if operator.DeploymentType == deploymentLocationLib.Local {
			outputTopicName = operatorLib.GenerateFogOperatorTopic(operator.Name, operator.Id, "")
		} else if operator.DeploymentType == deploymentLocationLib.Cloud {
			outputTopicName = operatorLib.GenerateCloudOperatorTopic(operator.Name)
		}
		
		op := Operator{
			Id:             operator.Id,
			ApplicationId:  uuid.New(),
			Name:           operator.Name,
			ImageId:        operator.ImageId,
			OperatorId:     operator.OperatorId,
			DeploymentType: operator.DeploymentType,
			OutputTopic:    outputTopicName,
			Cost:           operator.Cost,
			UpstreamConfig: UpstreamConfig{
				Enabled: operator.UpstreamConfig.Enabled,
			},
			DownstreamConfig: DownstreamConfig{
				Enabled: operator.DownstreamConfig.Enabled,
			},
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

func addOperatorConfigs(pipelineRequest PipelineRequest, tmpPipeline Pipeline) (operators []Operator) {
	// Add operator configs and input topics of the first operators (input topics of later operators are configured by the parser)
	operatorIds := make([]string, 0)
	for _, operator := range tmpPipeline.Operators {
		operatorIds = append(operatorIds, operator.Id)
	}
	for _, operator := range tmpPipeline.Operators {
		toKeep := make([]int, 0)
		for key, topic := range operator.InputTopics {
			if StringInSlice(topic.FilterValue, operatorIds) {
				toKeep = append(toKeep, key)
			}

		}
		tmpTopics := make([]InputTopic, 0)
		for _, indexToKeep := range toKeep {
			tmpTopics = append(tmpTopics, operator.InputTopics[indexToKeep])
		}
		operator.InputTopics = tmpTopics
		for _, node := range pipelineRequest.Nodes {
			if operator.Id == node.NodeId {
				operator.PersistData = node.PersistData
				operator.InputSelections = node.InputSelections

				// only operators at the beginning have node.Inputs
				if len(node.Inputs) > 0 {
					for _, input := range node.Inputs {
						filterId := input.FilterIds
						var filterIds []string
						if operator.DeploymentType == "local" {
							filterIds = strings.Split(input.FilterIds, ",")
						}
						for topicKey, topicName := range strings.Split(input.TopicName, ",") {
							if operator.DeploymentType == "local" {
								if len(filterIds) > 0 {
									filterId = filterIds[topicKey]
								}
							}
							filterType := "DeviceId"
							if input.FilterType == "operatorId" {
								filterType = "OperatorId"
							} else if input.FilterType == "ImportId" {
								filterType = "ImportId"
							}

							// topic of device inputs must be set according to deployment location
							deviceID := input.FilterIds // TODO send device name and service name in more specific fields
							serviceID := input.TopicName
							topicName, _ = deviceLib.GetDeviceOutputTopic(deviceID, serviceID, operator.DeploymentType)

							t := InputTopic{Name: topicName, FilterType: filterType, FilterValue: filterId}
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

