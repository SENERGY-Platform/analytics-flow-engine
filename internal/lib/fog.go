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

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

func processMessage(message MQTT.Message) {

}

func startOperator(input Operator, pipelineConfig PipelineConfig) {
	for key, topic := range input.InputTopics {
		if topic.FilterType == "OperatorId" {
			input.InputTopics[key].Name = topic.Name + "/" + pipelineConfig.PipelineId
		}
	}
	command := &ControlCommand{"startOperator", OperatorJob{ImageId: input.ImageId, InputTopics: input.InputTopics,
		Config: FogConfig{OutputTopic: input.OutputTopic,
			OperatorId:     input.Id,
			PipelineId:     pipelineConfig.PipelineId,
			BaseOperatorId: input.OperatorId,
		}}}
	out, err := json.Marshal(command)
	if err != nil {
		panic(err)
	}
	publishMessage(MQTTControlTopic, string(out))
}

func stopOperator(pipelineId string, input Operator) {
	command := &ControlCommand{"stopOperator", OperatorJob{Config: FogConfig{
		OperatorId:     input.Id,
		PipelineId:     pipelineId,
		BaseOperatorId: input.OperatorId,
	}}}
	out, err := json.Marshal(command)
	if err != nil {
		panic(err)
	}
	publishMessage(MQTTControlTopic, string(out))
}
