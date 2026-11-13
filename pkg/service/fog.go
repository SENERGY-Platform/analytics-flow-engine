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
	"context"
	"encoding/json"
	"fmt"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/deployment"
	"strings"

	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"

	operatorLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/operator"
	upstreamLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/upstream"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

type FogClient struct {
	pipelineService PipelineApiService
	mqtt            Publisher
}

func NewFogClient(pipelineService PipelineApiService, mqtt Publisher) *FogClient {
	return &FogClient{pipelineService: pipelineService, mqtt: mqtt}
}

func (f *FogClient) processMessage(message MQTT.Message) {
	// An MQTT message carries no trace context: the fog agents publish plain
	// payloads, and there is no header to extract one from. A background context, so
	// the calls below still have one to be cancelled by and to log against, but the
	// baggage stays empty here rather than being invented.
	ctx := context.Background()
	topic := message.Topic()
	util.Logger.DebugContext(ctx, "Received message on: "+topic)

	if strings.HasSuffix(topic, "/operator/control/sync/request") {
		userID := operatorLib.GetUserIDFromOperatorControlSyncTopic(topic)
		f.sendActiveOperators(ctx, userID, "")
	}

	if strings.HasSuffix(topic, "/upstream/sync/request") {
		userID := upstreamLib.GetUserIDFromUpstreamControlSyncTopic(topic)
		f.sendTopicsWithEnabledForward(ctx, userID, "")
	}
}

// sendActiveOperators answers a fog agent's sync request. Nothing is sent when the
// registry cannot be read: an empty answer would make the agent stop every operator.
func (f *FogClient) sendActiveOperators(ctx context.Context, userID string, token string) {
	pipelines, err := f.pipelineService.GetPipelines(ctx, userID, token)
	if err != nil {
		util.Logger.ErrorContext(ctx, "cannot get pipelines, no operator sync answer sent", "error", err)
		return
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
		util.Logger.ErrorContext(ctx, "cannot marshal operator sync message", "error", err)
		return
	}
	topic := operatorLib.GetOperatorControlSyncResponseTopic(userID)
	err = f.mqtt.Publish(topic, string(syncMsgStr))
	if err != nil {
		util.Logger.ErrorContext(ctx, "cannot publish operator sync message", "error", err)
	}
}

// sendTopicsWithEnabledForward answers a fog agent's upstream sync request; like
// sendActiveOperators it sends nothing rather than an empty list on a registry error.
func (f *FogClient) sendTopicsWithEnabledForward(ctx context.Context, userID string, token string) {
	pipelines, err := f.pipelineService.GetPipelines(ctx, userID, token)
	if err != nil {
		util.Logger.ErrorContext(ctx, "cannot get pipelines, no upstream sync answer sent", "error", err)
		return
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

	util.Logger.DebugContext(ctx, fmt.Sprintf("sync %+v", topics))

	syncMsg := upstreamLib.UpstreamSyncMessage{OperatorOutputTopics: topics}
	syncMsgStr, err := json.Marshal(syncMsg)
	if err != nil {
		util.Logger.ErrorContext(ctx, "cannot marshal upstream sync message", "error", err)
		return
	}
	topic := upstreamLib.GetUpstreamControlSyncResponseTopic(userID)
	err = f.mqtt.Publish(topic, string(syncMsgStr))
	if err != nil {
		util.Logger.ErrorContext(ctx, "cannot publish upstream sync message", "error", err)
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

func (f *FlowEngine) startFogOperator(ctx context.Context, operator pipe.Operator, pipelineConfig deployment.PipelineConfig, userID string) error {
	inputTopics := convertInputTopics(operator.InputTopics)

	command := GenerateFogOperatorStartCommand(operator, pipelineConfig.PipelineId, inputTopics)
	out, err := json.Marshal(command)
	if err != nil {
		return err
	}
	controlTopic := operatorLib.GetStartOperatorCloudTopic(userID)
	util.Logger.DebugContext(ctx, "publish start command for operator", "operator", operator, "topic", controlTopic)
	err = f.mqtt.Publish(controlTopic, string(out))
	if err != nil {
		util.Logger.ErrorContext(ctx, "cannot publish start command for operator", "error", err, "operator", operator)
		return err
	}
	return nil
}

func (f *FlowEngine) stopFogOperator(ctx context.Context, pipelineId string, operator pipe.Operator, userID string) error {
	command := &operatorLib.StopOperatorControlCommand{
		OperatorIDs: operatorLib.OperatorIDs{
			OperatorId:     operator.Id,
			PipelineId:     pipelineId,
			BaseOperatorId: operator.OperatorId,
		},
	}
	out, err := json.Marshal(command)
	if err != nil {
		util.Logger.ErrorContext(ctx, "cannot unmarshal stop command for operator", "error", err, "operator", operator)
		return err
	}

	controlTopic := operatorLib.GetStopOperatorCloudTopic(userID)
	util.Logger.DebugContext(ctx, "publish stop command for operator", "operator", operator, "topic", controlTopic)
	err = f.mqtt.Publish(controlTopic, string(out))
	if err != nil {
		util.Logger.ErrorContext(ctx, "cannot publish stop command for operator", "error", err, "operator", operator)
		return err
	}
	return nil
}
