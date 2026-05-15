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
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"
	parser "github.com/SENERGY-Platform/analytics-parser/lib"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
	"github.com/google/uuid"
	k8apierrors "k8s.io/apimachinery/pkg/api/errors"

	"encoding/json"

	deploymentLocationLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/location"
	upstreamLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/upstream"
)

type FlowEngine struct {
	driver               Driver
	parsingService       ParsingApiService
	permissionService    PermissionApiService
	kafak2mqttService    Kafka2MqttApiService
	deviceManagerService DeviceManagerService
	pipelineService      PipelineApiService
}

func NewFlowEngine(
	driver Driver,
	parsingService ParsingApiService,
	permissionService PermissionApiService,
	kafak2mqttService Kafka2MqttApiService,
	deviceManagerService DeviceManagerService,
	pipelineService PipelineApiService) *FlowEngine {
	return &FlowEngine{driver, parsingService, permissionService, kafak2mqttService, deviceManagerService, pipelineService}
}

func (f *FlowEngine) StartPipeline(pipelineRequest lib.PipelineRequest, userId string, token string) (pipeline *pipe.Pipeline, err error) {
	util.Logger.Debug("engine - start pipeline: " + pipelineRequest.Id)
	pipeline, err = f.setupPipeline(pipelineRequest, userId, token)
	if err != nil {
		return
	}

	id, err := f.pipelineService.RegisterPipeline(pipeline, userId, token)
	if err != nil {
		return
	}
	pipeline.Id = id.String()

	pipeline.Operators = addPipelineIDToFogTopic(pipeline.Operators, pipeline.Id)
	pipeConfig := f.createPipelineConfig(*pipeline)
	pipeConfig.UserId = userId
	newOperators, err := f.startOperators(*pipeline, pipeConfig, userId, token)
	if err != nil {
		if delErr := f.pipelineService.DeletePipeline(pipeline.Id, userId, token); delErr != nil {
			util.Logger.Error("failed to rollback pipeline registration", "error", delErr)
		}
		return
	}
	pipeline.Operators = newOperators
	err = f.pipelineService.UpdatePipeline(pipeline, userId, token) //update is needed to set correct fog output topics (with pipeline ID) and instance id for downstream config of fog operators
	if err != nil {
		return
	}
	util.Logger.Debug("started pipeline: "+pipeline.Id, "pipeline", pipeline)
	return
}

func (f *FlowEngine) UpdatePipeline(pipelineRequest lib.PipelineRequest, userId string, token string) (pipeline *pipe.Pipeline, err error) {
	util.Logger.Debug("engine - update pipeline: " + pipelineRequest.Id)
	oldPipeline, err := f.pipelineService.GetPipeline(pipelineRequest.Id, userId, token)
	if err != nil {
		return
	}

	pipeline, err = f.setupPipeline(pipelineRequest, userId, token)
	if err != nil {
		return
	}

	// If consume all messages is the same, we can reuse the application IDs
	if pipeline.ConsumeAllMessages == oldPipeline.ConsumeAllMessages {
		oldAppIds := make(map[string]uuid.UUID)
		for _, op := range oldPipeline.Operators {
			oldAppIds[op.Id] = op.ApplicationId
		}
		for i := range pipeline.Operators {
			if appId, exists := oldAppIds[pipeline.Operators[i].Id]; exists {
				pipeline.Operators[i].ApplicationId = appId
			}
		}
	}

	err = f.stopOperators(oldPipeline, token)
	if err != nil {
		util.Logger.Error("cannot stop operators", "error", err)
		return
	}

	pipeline.Id = oldPipeline.Id
	pipeline.Operators = addPipelineIDToFogTopic(pipeline.Operators, pipeline.Id)
	pipeConfig := f.createPipelineConfig(*pipeline)
	pipeConfig.UserId = userId
	newOperators, err := f.startOperators(*pipeline, pipeConfig, userId, token)
	if err != nil {
		util.Logger.Error("failed to start new operators, attempting to restart old pipeline", "error", err)
		if _, err = f.startOperators(oldPipeline, f.createPipelineConfig(oldPipeline), userId, token); err != nil {
			util.Logger.Error("CRITICAL: failed to restart old pipeline", "error", err)
		}
		return nil, fmt.Errorf("failed to start operators: %w", err)
	}
	pipeline.Operators = newOperators
	err = f.pipelineService.UpdatePipeline(pipeline, userId, token)
	util.Logger.Debug("updated pipeline: "+pipeline.Id, "pipeline", pipeline)
	return
}

func (f *FlowEngine) setupPipeline(pipelineRequest lib.PipelineRequest, userId, token string) (*pipe.Pipeline, error) {
	parsedPipeline, err := f.parsingService.GetPipeline(pipelineRequest.FlowId, userId, token)
	if err != nil {
		return nil, err
	}

	if err = f.checkAccess(pipelineRequest, parsedPipeline.Operators, token); err != nil {
		return nil, lib.NewForbiddenError(fmt.Errorf("checkAccess failed: %w", err))
	}

	pipeline := setPipelineModel(pipelineRequest, parsedPipeline)
	tmpPipeline := createOperatorConfig(parsedPipeline)

	configuredOperators, err := addOperatorConfigs(pipelineRequest, tmpPipeline, f.deviceManagerService, userId, token)
	if err != nil {
		return nil, err
	}
	pipeline.Operators = configuredOperators

	return pipeline, nil
}

func (f *FlowEngine) DeletePipeline(id string, userId string, token string) (err error) {
	util.Logger.Debug("engine - delete pipeline: " + id)
	pipeline, err := f.pipelineService.GetPipeline(id, userId, token)
	if err != nil {
		return
	}
	err = f.stopOperators(pipeline, token)
	if err != nil {
		if !k8apierrors.IsNotFound(err) {
			return
		}
	} else {
		util.Logger.Debug("removed all operators for pipeline: " + id)
	}
	err = f.pipelineService.DeletePipeline(id, userId, token)
	if err != nil {
		return
	}
	return
}

func (f *FlowEngine) GetPipelineStatus(id, userId, token string) (status lib.PipelineStatus, err error) {
	_, err = f.pipelineService.GetPipeline(id, userId, token)
	if err != nil {
		return
	}
	status, err = f.driver.GetPipelineStatus(id)
	return
}

func (f *FlowEngine) GetPipelinesStatus(ids []string, userId, token string) (status []lib.PipelineStatus, err error) {
	statusTemp, err := f.driver.GetPipelinesStatus()
	pipes, err := f.pipelineService.GetPipelines(userId, token)
	if err != nil {
		return
	}
	for _, stat := range statusTemp {
		idx := slices.IndexFunc(pipes, func(p pipe.Pipeline) bool { return "pipeline-"+p.Id == stat.Name })
		if idx != -1 {
			stat.Name = strings.Replace(stat.Name, "pipeline-", "", -1)
			status = append(status, stat)
		}
	}
	if len(ids) > 0 {
		statusTemp = status
		status = nil
		for _, id := range ids {
			idx := slices.IndexFunc(statusTemp, func(t lib.PipelineStatus) bool { return t.Name == id })
			if idx != -1 {
				status = append(status, statusTemp[idx])
			}
		}
	}
	return
}

func (f *FlowEngine) checkAccess(pipelineRequest lib.PipelineRequest, operators map[string]parser.Operator, token string) error {
	deviceIds, _, pipelineIds, importIds := getFilterIdsFromPipelineRequest(pipelineRequest)

	checks := []struct {
		resource string
		ids      []string
		msg      string
	}{
		{PermissionResourceFlows, []string{pipelineRequest.FlowId}, "flow: " + pipelineRequest.FlowId},
		{PermissionResourceDevices, deviceIds, "one or more devices"},
		{PermissionResourceAnalyticsPipelines, pipelineIds, "one or more pipelines"},
		{PermissionResourceImports, importIds, "one or more imports"},
	}

	for _, c := range checks {
		if len(c.ids) == 0 {
			continue
		}
		ok, err := f.permissionService.UserHasExecuteAccess(c.resource, c.ids, token)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("engine - user does not have the rights to execute %s", c.msg)
		}
	}

	if len(operators) > 0 {
		operatorIds := make([]string, 0, len(operators))
		for _, op := range operators {
			operatorIds = append(operatorIds, op.OperatorId)
		}
		ok, err := f.permissionService.UserHasExecuteAccess(PermissionResourceOperators, operatorIds, token)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("engine - user does not have the rights to execute one or more operators")
		}
	}

	return nil
}

func addPipelineIDToFogTopic(operators []pipe.Operator, pipelineId string) (newOperators []pipe.Operator) {
	// Input and Output Topics are set during parsing where pipeline ID is not available
	for _, operator := range operators {
		if operator.DeploymentType == deploymentLocationLib.Local {
			operator.OutputTopic = operator.OutputTopic + pipelineId

			var inputTopicsWithID []pipe.InputTopic
			for _, inputTopic := range operator.InputTopics {
				if inputTopic.FilterType == "OperatorId" {
					inputTopic.Name += pipelineId
				}
				inputTopicsWithID = append(inputTopicsWithID, inputTopic)
			}
			operator.InputTopics = inputTopicsWithID
		}
		newOperators = append(newOperators, operator)
	}
	return
}

func seperateOperators(pipeline pipe.Pipeline) (localOperators []pipe.Operator, cloudOperators []pipe.Operator) {
	for _, operator := range pipeline.Operators {
		switch operator.DeploymentType {
		case "local":
			localOperators = append(localOperators, operator)
			break
		default:
			cloudOperators = append(cloudOperators, operator)
			break
		}
	}
	return
}

func setPipelineModel(pipelineRequest lib.PipelineRequest, parsedPipeline parser.Pipeline) *pipe.Pipeline {
	pipeline := &pipe.Pipeline{}
	pipeline.Name = pipelineRequest.Name
	pipeline.Description = pipelineRequest.Description
	pipeline.FlowId = parsedPipeline.FlowId
	pipeline.Image = parsedPipeline.Image
	pipeline.WindowTime = pipelineRequest.WindowTime
	pipeline.MergeStrategy = pipelineRequest.MergeStrategy
	pipeline.ConsumeAllMessages = pipelineRequest.ConsumeAllMessages
	pipeline.Metrics = pipelineRequest.Metrics
	return pipeline
}

func (f *FlowEngine) stopOperators(pipeline pipe.Pipeline, token string) error {
	localOperators, cloudOperators := seperateOperators(pipeline)
	util.Logger.Debug("engine - stop operators for pipeline: "+pipeline.Id, "localOperators", localOperators, "cloudOperators", cloudOperators)

	if len(cloudOperators) > 0 {
		err := f.driver.DeleteOperators(pipeline.Id, cloudOperators)
		if err != nil {
			util.Logger.Error("cannot delete operators", "error", err)
			return err
		}
		err = f.disableCloudToFogForwarding(cloudOperators, pipeline.Id, pipeline.UserId, token)
		if err != nil {
			util.Logger.Error("cannot disable cloud2fog forwarding", "error", err)
			return err
		}
	}

	if len(localOperators) > 0 {
		for _, operator := range localOperators {
			util.Logger.Debug("engine - stop local Operator: " + operator.Name)
			err := stopFogOperator(pipeline.Id,
				operator, pipeline.UserId)
			if err != nil {
				return err
			}
			err = f.disableFogToCloudForwarding(operator, pipeline.Id, pipeline.UserId, token)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *FlowEngine) startOperators(pipeline pipe.Pipeline, pipeConfig lib.PipelineConfig, userID, token string) (newOperators []pipe.Operator, err error) {
	localOperators, cloudOperators := seperateOperators(pipeline)

	if len(cloudOperators) > 0 {
		util.Logger.Debug("try to start cloud operators")
		err = retry(6, 10*time.Second, func() (err error) {
			return f.driver.CreateOperators(
				pipeline.Id,
				cloudOperators,
				pipeConfig,
			)
		})
		if err != nil {
			util.Logger.Error("cannot start cloud operators", "error", err)
			return
		} else {
			util.Logger.Debug("engine - successfully started cloud operators - " + pipeline.Id)
			cloudOperatorsWithDownstreamID, err2 := f.enableCloudToFogForwarding(cloudOperators, pipeline.Id, userID, token)
			if err2 != nil {
				util.Logger.Error("cannot enable cloud2fog forwarding", "error", err2)
				err = err2
				return
			}
			newOperators = append(newOperators, cloudOperatorsWithDownstreamID...)
		}
	}
	if len(localOperators) > 0 {
		for _, operator := range localOperators {
			util.Logger.Debug("try to start local operator: " + operator.Name + " for pipeline: " + pipeline.Id)
			err = startFogOperator(operator, pipeConfig, userID)
			if err != nil {
				util.Logger.Error("cannot start local operator", "error", err, "operator", operator)
				return
			}
			util.Logger.Debug("engine - successfully started local operator: " + operator.Name + " for pipeline: " + pipeline.Id)

			err = f.enableFogToCloudForwarding(operator, pipeline.Id, userID)
			if err != nil {
				return
			}
			newOperators = append(newOperators, operator)
		}
	}
	return
}

func (f *FlowEngine) enableCloudToFogForwarding(operators []pipe.Operator, pipelineID, userID, token string) (newOperators []pipe.Operator, err error) {
	for _, operator := range operators {
		if operator.DownstreamConfig.Enabled {
			util.Logger.Debug("Try to enable Cloud2Fog Forwarding for operator: " + operator.Id)
			createdInstance, err := f.kafak2mqttService.StartOperatorInstance(operator.Name, operator.Id, pipelineID, userID, token)
			if err != nil {
				util.Logger.Error("cannot enable cloud2fog forwarding", "error", err, "operator", operator)
				return []pipe.Operator{}, err
			}
			operator.DownstreamConfig.InstanceID = createdInstance.Id
		}
		newOperators = append(newOperators, operator) // operator needs to be appened so that no operator is lost
	}

	return
}

func (f *FlowEngine) enableFogToCloudForwarding(operator pipe.Operator, _, userID string) error {
	if operator.UpstreamConfig.Enabled {
		util.Logger.Debug("Try to enable Fog2Cloud Forwarding for operator: " + operator.Id)

		command := &upstreamLib.UpstreamControlMessage{
			OperatorOutputTopic: operator.OutputTopic,
		}
		message, err := json.Marshal(command)
		if err != nil {
			util.Logger.Error("cannot unmarshal enable fog2cloud message for operator: "+operator.Name+" - "+operator.Id, "error", err)
			return err
		}
		topic := upstreamLib.GetUpstreamEnableCloudTopic(userID)
		util.Logger.Debug("try to publish enable forwarding command for operator: " + operator.Name + " - " + operator.Id + " to topic: " + topic)
		err = publishMessage(topic, string(message))
		if err != nil {
			util.Logger.Error("cannot publish enable fog2cloud message for operator: "+operator.Name+" - "+operator.Id, "error", err)
			return err
		}
		util.Logger.Debug("published enable forwarding command for operator: " + operator.Name + " - " + operator.Id + " to topic: " + topic)
	}
	return nil
}

func (f *FlowEngine) disableCloudToFogForwarding(operators []pipe.Operator, pipelineID, userID, token string) error {
	for _, operator := range operators {
		downstreamConfig := operator.DownstreamConfig
		if downstreamConfig.Enabled {
			util.Logger.Debug("Try to disable Cloud2Fog Forwarding for operator: " + operator.Id)
			if downstreamConfig.InstanceID == "" {
				util.Logger.Warn("No instance ID set for operator: " + operator.Id)
				continue
			}
			err := f.kafak2mqttService.RemoveInstance(downstreamConfig.InstanceID, pipelineID, userID, token)
			if err != nil {
				util.Logger.Error("cannot disable cloud2fog forwarding", "error", err, "operator", operator)
				return err
			}
			util.Logger.Debug("Disabled Cloud2Fog Forwarding for operator: " + operator.Id)
		} else {
			util.Logger.Debug("Operator " + operator.Id + " has no downstream forwarding enabled")
		}
	}
	return nil
}

func (f *FlowEngine) disableFogToCloudForwarding(operator pipe.Operator, _, userID, _ string) error {
	if operator.UpstreamConfig.Enabled {
		command := &upstreamLib.UpstreamControlMessage{
			OperatorOutputTopic: operator.OutputTopic,
		}
		message, err := json.Marshal(command)
		if err != nil {
			util.Logger.Error("cannot unmarshal disable fog2cloud message for operator: "+operator.Name+" - "+operator.Id, "error", err)
			return err
		}
		util.Logger.Debug("try to publish disable forwarding command for operator: " + operator.Name + " - " + operator.Id)
		err = publishMessage(upstreamLib.GetUpstreamDisableCloudTopic(userID), string(message))
		if err != nil {
			util.Logger.Error("cannot publish disable fog2cloud message for operator: "+operator.Name+" - "+operator.Id, "error", err)
		}
	} else {
		util.Logger.Debug("Operator " + operator.Id + " has no upstream forwarding enabled")
	}
	return nil
}

func (f *FlowEngine) createPipelineConfig(pipeline pipe.Pipeline) lib.PipelineConfig {
	var pipeConfig = lib.PipelineConfig{
		WindowTime:     pipeline.WindowTime,
		MergeStrategy:  pipeline.MergeStrategy,
		FlowId:         pipeline.FlowId,
		ConsumerOffset: "latest",
		Metrics:        true, // always enable metrics SNRGY-3068 pipeline.Metrics,
		PipelineId:     pipeline.Id,
	}
	if pipeline.ConsumeAllMessages {
		pipeConfig.ConsumerOffset = "earliest"
	}
	return pipeConfig
}

func getFilterIdsFromPipelineRequest(pipelineRequest lib.PipelineRequest) (deviceIds []string, operatorIds []string, pipelineIds []string, importIds []string) {
	for _, node := range pipelineRequest.Nodes {
		for _, input := range node.Inputs {
			ids := strings.Split(input.FilterIds, ",")
			switch input.FilterType {
			case RequestDeviceId:
				deviceIds = append(deviceIds, ids...)
			case RequestImportId:
				importIds = append(importIds, ids...)
			case RequestOperatorId:
				for _, v := range ids {
					parts := strings.SplitN(strings.TrimSpace(v), ":", 2)
					if len(parts) != 2 {
						util.Logger.Warn("malformed operator filter ID: " + v)
						continue
					}
					operatorIds = append(operatorIds, parts[0])
					pipelineIds = append(pipelineIds, parts[1])
				}
			}
		}
	}
	return
}
