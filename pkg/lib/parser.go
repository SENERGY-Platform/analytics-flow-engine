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
	"strings"

	parsingApi "github.com/SENERGY-Platform/analytics-flow-engine/pkg/parsing-api"
	deviceLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/devices"
	deploymentLocationLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/location"
	operatorLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/operator"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/google/uuid"
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

func createLocalDeviceTopic(deviceID, serviceID, userID, token string, deviceManagerService DeviceManagerService) (string, models.Service, error) {
	// Load local device id and service name as they are used in local mqtt topics of the device
	device, err := deviceManagerService.GetDevice(deviceID, userID, token)
	localService := models.Service{}
	if err != nil {
		return "", localService, err
	}
	deviceType, err := deviceManagerService.GetDeviceType(device.DeviceTypeId, userID, token)
	if err != nil {
		return "", localService, err
	}
	for _, service := range(deviceType.Services) {
		serviceID = strings.Replace(serviceID, "_", ":",-1)
		if service.Id == serviceID {
			localService = service
			break
		}
	}
	deviceTopic := deviceLib.GetLocalDeviceOutputTopic(device.LocalId, localService.LocalId)
	return deviceTopic, localService, nil
}

func createLocalValuePath(service models.Service, path string) (string) {
	// The path to the selected value is specified by the message structure defined for inside the platform
	// E.g. devices have an envelope of value.root around the actual device message. This needs to be stripped off
	// value.root.OBIS -> OBIS for Landys Device Type
	// one could also use service.Outputs[0].ContentVariable.SubContentVariables to strip all parts until the first "real" device path
	splittedPath := strings.Split(path, ".")
	return strings.Join(splittedPath[2:], ".")
}

func addOperatorConfigs(pipelineRequest PipelineRequest, tmpPipeline Pipeline, deviceManagerService DeviceManagerService, userID, token string) (operators []Operator, err error) {
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
						localService := models.Service{}
						var filterIds []string
						if operator.DeploymentType == "local" {
							filterIds = strings.Split(input.FilterIds, ",")
						}
						for topicKey, topicName := range strings.Split(input.TopicName, ",") {
							if operator.DeploymentType == "local" {
								if len(filterIds) > 0 {
									filterId = filterIds[topicKey]
								}
								topicName, localService, err = createLocalDeviceTopic(filterId, topicName, userID, token, deviceManagerService)
								if err != nil {
									return
								}
							}
							filterType := "DeviceId"
							if input.FilterType == "operatorId" {
								filterType = "OperatorId"
							} else if input.FilterType == "ImportId" {
								filterType = "ImportId"
							}

							t := InputTopic{Name: topicName, FilterType: filterType, FilterValue: filterId}
							for _, value := range input.Values {
								valuePath := value.Path
								if operator.DeploymentType == "local" {
									valuePath = createLocalValuePath(localService, value.Path)
								}
								t.Mappings = append(t.Mappings, Mapping{value.Name, valuePath})
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

