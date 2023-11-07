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
	"encoding/json"

	controlLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/control"
	operatorLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/operator"
	MQTT "github.com/eclipse/paho.mqtt.golang"
)

func processMessage(message MQTT.Message) {

}

func convertInputTopics(inputTopics []InputTopic) []operatorLib.InputTopic {
	// TODO external analytics-lib
	fogInputTopics := []operatorLib.InputTopic{}
	for _, v := range inputTopics {
		fogMappings := []operatorLib.Mapping{}
		for _, mapping := range v.Mappings {
			fogMappings = append(fogMappings, operatorLib.Mapping(mapping))
		}

		fogInputTopics = append(fogInputTopics, operatorLib.InputTopic{
			Name:        v.Name,
			FilterType:  v.FilterType,
			FilterValue: v.FilterValue,
			Mappings:    fogMappings,
		})
	}
	return fogInputTopics
}

func startFogOperator(input Operator, pipelineConfig PipelineConfig, userID string) {
	for key, topic := range input.InputTopics {
		if topic.FilterType == "OperatorId" {
			input.InputTopics[key].Name = topic.Name + "/" + pipelineConfig.PipelineId
		}
	}

	inputTopics := convertInputTopics(input.InputTopics)

	command := &operatorLib.StartOperatorControlCommand{
		ControlMessage: controlLib.ControlMessage{
			Command: operatorLib.StartOperatorCommand,
		},
		Operator: operatorLib.StartOperatorMessage{
			ImageId:        input.ImageId,
			InputTopics:    inputTopics,
			OperatorConfig: input.Config,
			Config: operatorLib.FogConfig{
				OperatorIDs: operatorLib.OperatorIDs{
					OperatorId:     input.Id,
					PipelineId:     pipelineConfig.PipelineId,
					BaseOperatorId: input.OperatorId,
				},
				OutputTopic: input.OutputTopic,
			},
		},
	}

	out, err := json.Marshal(command)
	if err != nil {
		panic(err)
	}
	controlTopic := controlLib.GetConnectorControlTopic(userID)
	publishMessage(controlTopic, string(out))
}

func stopFogOperator(pipelineId string, input Operator, userID string) {
	command := &operatorLib.StopOperatorControlCommand{
		ControlMessage: controlLib.ControlMessage{
			Command: operatorLib.StartOperatorCommand,
		},
		OperatorIDs: operatorLib.OperatorIDs{
			OperatorId:     input.Id,
			PipelineId:     pipelineId,
			BaseOperatorId: input.OperatorId,
		},
	}
	out, err := json.Marshal(command)
	if err != nil {
		panic(err)
	}

	controlTopic := controlLib.GetConnectorControlTopic(userID)
	publishMessage(controlTopic, string(out))
}
