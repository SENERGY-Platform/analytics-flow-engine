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

package service

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"

	operatorLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/operator"
	upstreamLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/upstream"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

type FogClient struct {
	pipelineService PipelineApiService
}

func NewFogClient(pipelineService PipelineApiService) *FogClient {
	return &FogClient{pipelineService}
}

func (f *FogClient) processMessage(message MQTT.Message) {
	topic := message.Topic()
	util.Logger.Debug("Received message on: " + topic)

	if strings.HasSuffix(topic, "/operator/control/sync/request") {
		userID := operatorLib.GetUserIDFromOperatorControlSyncTopic(topic)
		f.sendActiveOperators(userID, "")
	}

	if strings.HasSuffix(topic, "/upstream/sync/request") {
		userID := upstreamLib.GetUserIDFromUpstreamControlSyncTopic(topic)
		f.sendTopicsWithEnabledForward(userID, "")
	}
}

func (f *FogClient) sendActiveOperators(userID string, token string) {
	pipelines, err := f.pipelineService.GetPipelines(userID, token)
	if err != nil {
		util.Logger.Error("cannot get pipelines", "error", err)
	}
	var startCommands []operatorLib.StartOperatorControlCommand
	for _, pipeline := range pipelines {
		for _, operator := range pipeline.Operators {
			if operator.DeploymentType == "local" {
				inputTopics := convertInputTopics(operator.InputTopics)
				command := GenerateFogOperatorStartCommand(operator, pipeline.Id, inputTopics)
				startCommands = append(startCommands, command)
			}
		}
	}

	syncMsgStr, err := json.Marshal(startCommands)
	if err != nil {
		util.Logger.Error("cannot marshal operator sync message", "error", err)
	}
	topic := operatorLib.GetOperatorControlSyncResponseTopic(userID)
	err = publishMessage(topic, string(syncMsgStr))
	if err != nil {
		util.Logger.Error("cannot publish operator sync message", "error", err)
	}
}

func (f *FogClient) sendTopicsWithEnabledForward(userID string, token string) {
	pipelines, err := f.pipelineService.GetPipelines(userID, token)
	if err != nil {
		util.Logger.Error("cannot get pipelines", "error", err)
	}

	var topics []string
	for _, pipeline := range pipelines {
		for _, operator := range pipeline.Operators {
			if operator.DeploymentType == "local" {
				if operator.UpstreamConfig.Enabled {
					topics = append(topics, operator.OutputTopic)
				}
			}
		}
	}

	util.Logger.Debug(fmt.Sprintf("sync %+v", topics))

	syncMsg := upstreamLib.UpstreamSyncMessage{OperatorOutputTopics: topics}
	syncMsgStr, err := json.Marshal(syncMsg)
	if err != nil {
		util.Logger.Error("cannot marshal upstream sync message", "error", err)
	}
	topic := upstreamLib.GetUpstreamControlSyncResponseTopic(userID)
	err = publishMessage(topic, string(syncMsgStr))
	if err != nil {
		util.Logger.Error("cannot publish upstream sync message", "error", err)
	}
}

func convertInputTopics(inputTopics []pipe.InputTopic) []operatorLib.InputTopic {
	var fogInputTopics []operatorLib.InputTopic
	for _, v := range inputTopics {
		var fogMappings []operatorLib.Mapping
		for _, mapping := range v.Mappings {
			fogMappings = append(fogMappings, operatorLib.Mapping(mapping))
		}

		fogInputTopics = append(fogInputTopics, operatorLib.InputTopic{
			Name:        v.Name,
			FilterValue: v.FilterValue,
			FilterType:  v.FilterType,
			Mappings:    fogMappings,
		})
	}
	return fogInputTopics
}

func GenerateFogOperatorStartCommand(operator pipe.Operator, pipelineID string, inputTopics []operatorLib.InputTopic) operatorLib.StartOperatorControlCommand {
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

func startFogOperator(operator pipe.Operator, pipelineConfig lib.PipelineConfig, userID string) error {
	inputTopics := convertInputTopics(operator.InputTopics)

	command := GenerateFogOperatorStartCommand(operator, pipelineConfig.PipelineId, inputTopics)
	out, err := json.Marshal(command)
	if err != nil {
		return err
	}
	controlTopic := operatorLib.GetStartOperatorCloudTopic(userID)
	util.Logger.Debug("publish start command for operator", "operator", operator, "topic", controlTopic)
	err = publishMessage(controlTopic, string(out))
	if err != nil {
		util.Logger.Error("cannot publish start command for operator", "error", err, "operator", operator)
		return err
	}
	return nil
}

func stopFogOperator(pipelineId string, operator pipe.Operator, userID string) error {
	command := &operatorLib.StopOperatorControlCommand{
		OperatorIDs: operatorLib.OperatorIDs{
			OperatorId:     operator.Id,
			PipelineId:     pipelineId,
			BaseOperatorId: operator.OperatorId,
		},
	}
	out, err := json.Marshal(command)
	if err != nil {
		util.Logger.Error("cannot unmarshal stop command for operator", "error", err, "operator", operator)
		return err
	}

	controlTopic := operatorLib.GetStopOperatorCloudTopic(userID)
	util.Logger.Debug("publish stop command for operator", "operator", operator, "topic", controlTopic)
	err = publishMessage(controlTopic, string(out))
	if err != nil {
		util.Logger.Error("cannot publish stop command for operator", "error", err, "operator", operator)
		return err
	}
	return nil
}
