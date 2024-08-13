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
	"log"
	"strings"

	operatorLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/operator"
	upstreamLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/upstream"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

func processMessage(message MQTT.Message) {
	topic := message.Topic()
	log.Println("Received message on: " + topic)

	if strings.HasSuffix(topic, "/operator/control/sync/request") {
		userID := operatorLib.GetUserIDFromOperatorControlSyncTopic(topic)
		sendActiveOperators(userID)
	}

	if strings.HasSuffix(topic, "/upstream/sync/request") {
		userID := upstreamLib.GetUserIDFromUpstreamControlSyncTopic(topic)
		sendTopicsWithEnabledForward(userID)
	}
}

func sendActiveOperators(userID string) {
	pipelines, err := getPipelines(userID)
	if err != nil {
		log.Println("Cant get pipelines: " + err.Error())
	}
	startCommands := []operatorLib.StartOperatorControlCommand{}
	for _, pipeline := range(pipelines) {
		for _, operator := range pipeline.Operators {
			if operator.DeploymentType == "local" {
				inputTopics := convertInputTopics(operator.InputTopics)
				command := GenerateFogOperatorStartCommand(operator, pipeline.Id.String(), inputTopics)
				startCommands = append(startCommands, command)
			}
		}
	}

	syncMsgStr, err := json.Marshal(startCommands)
	if err != nil {
		log.Println("Cant marshal operator sync message: " + err.Error())
	}
	topic := operatorLib.GetOperatorControlSyncResponseTopic(userID)
	err = publishMessage(topic, string(syncMsgStr))
	if err != nil {
		log.Println("Cant publish operator sync message: " + err.Error())
	} 
}

func sendTopicsWithEnabledForward(userID string) {
	pipelines, err := getPipelines(userID)
	if err != nil {
		log.Println("Cant get pipelines: " + err.Error())
	}

	topics := []string{}
	for _, pipeline := range(pipelines) {
		for _, operator := range pipeline.Operators {
			if operator.DeploymentType == "local" {
				if operator.UpstreamConfig.Enabled {
					topics = append(topics, operator.OutputTopic)
				}
			}
		}
	}

	log.Printf("Sync %+v\n", topics)

	syncMsg := upstreamLib.UpstreamSyncMessage{OperatorOutputTopics: topics}
	syncMsgStr, err := json.Marshal(syncMsg)
	if err != nil {
		log.Println("Cant marshal upstream sync message: " + err.Error())
	}
	topic := upstreamLib.GetUpstreamControlSyncResponseTopic(userID)
	err = publishMessage(topic, string(syncMsgStr))
	if err != nil {
		log.Println("Cant publish upstream sync message: " + err.Error())
	} 
}

func convertInputTopics(inputTopics []InputTopic) []operatorLib.InputTopic {
	fogInputTopics := []operatorLib.InputTopic{}
	for _, v := range inputTopics {
		fogMappings := []operatorLib.Mapping{}
		for _, mapping := range v.Mappings {
			fogMappings = append(fogMappings, operatorLib.Mapping(mapping))
		}

		fogInputTopics = append(fogInputTopics, operatorLib.InputTopic{
			Name:        v.Name,
			FilterValue: v.FilterValue,
			FilterType: v.FilterType,
			Mappings:    fogMappings,
		})
	}
	return fogInputTopics
}

func GenerateFogOperatorStartCommand(operator Operator, pipelineID string, inputTopics []operatorLib.InputTopic) operatorLib.StartOperatorControlCommand {
	return operatorLib.StartOperatorControlCommand{
		ImageId:        operator.ImageId,
		InputTopics:    inputTopics,
		OperatorConfig: operator.Config,
		OperatorIDs: operatorLib.OperatorIDs{
			OperatorId:     operator.Id,
			PipelineId:     pipelineID,
			BaseOperatorId: operator.OperatorId,
		},
		OutputTopic: operator.OutputTopic,
	}
} 

func startFogOperator(operator Operator, pipelineConfig PipelineConfig, userID string) error {
	inputTopics := convertInputTopics(operator.InputTopics)

	command := GenerateFogOperatorStartCommand(operator, pipelineConfig.PipelineId, inputTopics)
	out, err := json.Marshal(command)
	if err != nil {
		return err
	}
	controlTopic := operatorLib.GetStartOperatorCloudTopic(userID)
	log.Println("publish start command for operator: " + operator.Name + " - " + operator.Id + " to topic: " + controlTopic)
	err = publishMessage(controlTopic, string(out))
	if err != nil {
		log.Println("cant publish start command for operator: " + operator.Name + " - " + operator.Id + ": " + err.Error())
		return err
	}
	return nil
}

func stopFogOperator(pipelineId string, operator Operator, userID string) error {
	command := &operatorLib.StopOperatorControlCommand{
		OperatorIDs: operatorLib.OperatorIDs{
			OperatorId:     operator.Id,
			PipelineId:     pipelineId,
			BaseOperatorId: operator.OperatorId,
		},
	}
	out, err := json.Marshal(command)
	if err != nil {
		log.Println("cant unmarshal stop command for operator: " + operator.Name + " - " + operator.Id + ": " + err.Error())
		return err
	}

	controlTopic := operatorLib.GetStopOperatorCloudTopic(userID)
	log.Println("publish stop command for operator: " + operator.Name + " - " + operator.Id)
	err = publishMessage(controlTopic, string(out))
	if err != nil {
		log.Println("cant publish stop command for operator: " + operator.Name + " - " + operator.Id + ": " + err.Error())
		return err
	}
	return nil
}