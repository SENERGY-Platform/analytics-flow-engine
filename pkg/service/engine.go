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
	"slices"
	"strings"
	"time"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"

	"encoding/json"

	deploymentLocationLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/location"
	operatorLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/operator"
	upstreamLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/upstream"
	"github.com/google/uuid"
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

func (f *FlowEngine) StartPipeline(pipelineRequest lib.PipelineRequest, userId string, token string) (pipeline lib.Pipeline, err error) {
	err = f.checkAccess(pipelineRequest, token, userId)
	if err != nil {
		return
	}
	//Get parsed pipeline
	parsedPipeline, err := f.parsingService.GetPipeline(pipelineRequest.FlowId, userId, token)
	if err != nil {
		return
	}
	pipeline.FlowId = parsedPipeline.FlowId
	pipeline.Image = parsedPipeline.Image
	pipeline.WindowTime = pipelineRequest.WindowTime
	pipeline.MergeStrategy = pipelineRequest.MergeStrategy
	pipeline.ConsumeAllMessages = pipelineRequest.ConsumeAllMessages

	tmpPipeline := createPipeline(parsedPipeline)
	pipeline.Name = pipelineRequest.Name
	pipeline.Description = pipelineRequest.Description
	configuredOperators, err := addOperatorConfigs(pipelineRequest, tmpPipeline, f.deviceManagerService, userId, token)
	if err != nil {
		return
	}
	pipeline.Operators = configuredOperators
	pipeline.Id, err = f.pipelineService.RegisterPipeline(&pipeline, userId, token)
	if err != nil {
		return
	}
	pipeline.Operators = addPipelineIDToFogTopic(pipeline.Operators, pipeline.Id.String())
	pipeline.Metrics = pipelineRequest.Metrics
	pipeConfig := f.createPipelineConfig(pipeline)
	pipeConfig.UserId = userId
	newOperators, err := f.startOperators(pipeline, pipeConfig, userId, token)
	if err != nil {
		return
	}
	pipeline.Operators = newOperators
	err = f.pipelineService.UpdatePipeline(&pipeline, userId, token) //update is needed to set correct fog output topics (with pipeline ID) and instance id for downstream config of fog operators
	if err != nil {
		util.Logger.Error("cant update pipeline", "error", err)
	}
	util.Logger.Debug("started pipeline: "+pipeline.Id.String(), "pipeline", pipeline)
	return
}

func addPipelineIDToFogTopic(operators []lib.Operator, pipelineId string) (newOperators []lib.Operator) {
	// Input and Output Topics are set during parsing where pipeline ID is not available
	for _, operator := range operators {
		if operator.DeploymentType == deploymentLocationLib.Local {
			operator.OutputTopic = operator.OutputTopic + pipelineId

			var inputTopicsWithID []lib.InputTopic
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

func (f *FlowEngine) UpdatePipeline(pipelineRequest lib.PipelineRequest, userId string, token string) (pipeline lib.Pipeline, err error) {
	util.Logger.Debug("engine - update pipeline: " + pipelineRequest.Id)
	err = f.checkAccess(pipelineRequest, token, userId)
	if err != nil {
		return
	}
	pipeline, err = f.pipelineService.GetPipeline(pipelineRequest.Id, userId, token)
	if err != nil {
		return
	}
	configuredOperators, err := addOperatorConfigs(pipelineRequest, pipeline, f.deviceManagerService, userId, token)
	if err != nil {
		return
	}
	pipeline.Operators = configuredOperators
	pipeline.Name = pipelineRequest.Name
	pipeline.Description = pipelineRequest.Description
	pipeline.WindowTime = pipelineRequest.WindowTime
	pipeline.MergeStrategy = pipelineRequest.MergeStrategy

	err = f.stopOperators(pipeline, userId, token)
	if err != nil {
		util.Logger.Error("cant stop operators", "error", err)
		return
	}

	// give the backend some time to delete the operators
	time.Sleep(15 * time.Second)

	missingUuid, _ := uuid.FromBytes([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	for index := range pipeline.Operators {
		// if app id is missing, set a new one
		if pipeline.Operators[index].ApplicationId == missingUuid {
			pipeline.Operators[index].ApplicationId = uuid.New()
		}
		// if output topic is missing,set it
		if pipeline.Operators[index].OutputTopic == "" {
			outputTopicName := pipeline.Operators[index].Name
			operator := pipeline.Operators[index]
			if operator.DeploymentType == deploymentLocationLib.Local {
				outputTopicName = operatorLib.GenerateFogOperatorTopic(operator.Name, operator.Id, "")
			} else if operator.DeploymentType == deploymentLocationLib.Cloud {
				outputTopicName = operatorLib.GenerateCloudOperatorTopic(operator.Name)
			}
			pipeline.Operators[index].OutputTopic = outputTopicName
		}
	}

	pipeConfig := f.createPipelineConfig(pipeline)
	pipeConfig.UserId = userId
	if pipelineRequest.ConsumeAllMessages != pipeline.ConsumeAllMessages {
		for index := range pipeline.Operators {
			pipeline.Operators[index].ApplicationId = uuid.New()
		}
	}
	pipeline.ConsumeAllMessages = pipelineRequest.ConsumeAllMessages

	newOperators, err := f.startOperators(pipeline, pipeConfig, userId, token)
	if err != nil {
		return
	}
	pipeline.Operators = newOperators

	err = f.pipelineService.UpdatePipeline(&pipeline, userId, token)

	return
}

func (f *FlowEngine) checkAccess(pipelineRequest lib.PipelineRequest, token string, userId string) (err error) {
	deviceIds, operatorIds := getFilterIdsFromPipelineRequest(pipelineRequest)
	if len(deviceIds) > 0 {
		hasAccess, e := f.permissionService.UserHasDevicesReadAccess(deviceIds, token)
		if e != nil {
			return e
		}
		if !hasAccess {
			e = errors.New("engine - user does not have the rights to access the devices")
			return e
		}
	}
	if len(operatorIds) > 0 {
		for _, operatorId := range operatorIds {
			_, e := f.pipelineService.GetPipeline(strings.Split(operatorId, ":")[1], userId, token)
			if e != nil {
				e = errors.New("engine - user does not have the rights to access the pipeline: " + strings.Split(operatorId, ":")[1])
				return e
			}
		}
	}
	return
}

func (f *FlowEngine) DeletePipeline(id string, userId string, token string) (err error) {
	util.Logger.Debug("engine - delete pipeline: " + id)
	pipeline, err := f.pipelineService.GetPipeline(id, userId, token)
	if err != nil {
		return
	}
	err = f.stopOperators(pipeline, userId, token)
	if err != nil {
		return
	}
	util.Logger.Debug("removed all operators for pipeline: " + id)

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
		idx := slices.IndexFunc(pipes, func(p lib.Pipeline) bool { return "pipeline-"+p.Id.String() == stat.Name })
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

func seperateOperators(pipeline lib.Pipeline) (localOperators []lib.Operator, cloudOperators []lib.Operator) {
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

func (f *FlowEngine) stopOperators(pipeline lib.Pipeline, userID, token string) error {
	localOperators, cloudOperators := seperateOperators(pipeline)
	util.Logger.Debug("engine - stop operators for pipeline: "+pipeline.Id.String(), "localOperators", localOperators, "cloudOperators", cloudOperators)

	if len(cloudOperators) > 0 {
		err := f.driver.DeleteOperators(pipeline.Id.String(), cloudOperators)
		if err != nil {
			util.Logger.Error("cannot delete operators", "error", err)
			return err
		}
		err = f.disableCloudToFogForwarding(cloudOperators, pipeline.Id.String(), userID, token)
		if err != nil {
			util.Logger.Error("cannot disable cloud2fog forwarding", "error", err)
			return err
		}
	}

	if len(localOperators) > 0 {
		for _, operator := range localOperators {
			util.Logger.Debug("engine - stop local Operator: " + operator.Name)
			err := stopFogOperator(pipeline.Id.String(),
				operator, userID)
			if err != nil {
				return err
			}
			err = f.disableFogToCloudForwarding(operator, pipeline.Id.String(), userID, token)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *FlowEngine) startOperators(pipeline lib.Pipeline, pipeConfig lib.PipelineConfig, userID, token string) (newOperators []lib.Operator, err error) {
	localOperators, cloudOperators := seperateOperators(pipeline)

	if len(cloudOperators) > 0 {
		util.Logger.Debug("try to start cloud operators")
		err = retry(6, 10*time.Second, func() (err error) {
			return f.driver.CreateOperators(
				pipeline.Id.String(),
				cloudOperators,
				pipeConfig,
			)
		})
		if err != nil {
			util.Logger.Error("cannot start cloud operators", "error", err)
			return
		} else {
			util.Logger.Debug("engine - successfully started cloud operators - " + pipeline.Id.String())
			cloudOperatorsWithDownstreamID, err2 := f.enableCloudToFogForwarding(cloudOperators, pipeline.Id.String(), userID, token)
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
			util.Logger.Debug("try to start local operator: " + operator.Name + " for pipeline: " + pipeline.Id.String())
			err = startFogOperator(operator, pipeConfig, userID)
			if err != nil {
				util.Logger.Error("cannot start local operator", "error", err, "operator", operator)
				return
			}
			util.Logger.Debug("engine - successfully started local operator: " + operator.Name + " for pipeline: " + pipeline.Id.String())

			err = f.enableFogToCloudForwarding(operator, pipeline.Id.String(), userID)
			if err != nil {
				return
			}
			newOperators = append(newOperators, operator)
		}
	}
	return
}

func (f *FlowEngine) enableCloudToFogForwarding(operators []lib.Operator, pipelineID, userID, token string) (newOperators []lib.Operator, err error) {
	for _, operator := range operators {
		if operator.DownstreamConfig.Enabled {
			util.Logger.Debug("Try to enable Cloud2Fog Forwarding for operator: " + operator.Id)
			createdInstance, err := f.kafak2mqttService.StartOperatorInstance(operator.Name, operator.Id, pipelineID, userID, token)
			if err != nil {
				util.Logger.Error("cannot enable cloud2fog forwarding", "error", err, "operator", operator)
				return []lib.Operator{}, err
			}
			operator.DownstreamConfig.InstanceID = createdInstance.Id
		}
		newOperators = append(newOperators, operator) // operator needs to be appened so that no operator is lost
	}

	return
}

func (f *FlowEngine) enableFogToCloudForwarding(operator lib.Operator, _, userID string) error {
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

func (f *FlowEngine) disableCloudToFogForwarding(operators []lib.Operator, pipelineID, userID, token string) error {
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

func (f *FlowEngine) disableFogToCloudForwarding(operator lib.Operator, _, userID, _ string) error {
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

func (f *FlowEngine) createPipelineConfig(pipeline lib.Pipeline) lib.PipelineConfig {
	var pipeConfig = lib.PipelineConfig{
		WindowTime:     pipeline.WindowTime,
		MergeStrategy:  pipeline.MergeStrategy,
		FlowId:         pipeline.FlowId,
		ConsumerOffset: "latest",
		Metrics:        true, // always enable metrics SNRGY-3068 pipeline.Metrics,
		PipelineId:     pipeline.Id.String(),
	}
	if pipeline.ConsumeAllMessages {
		pipeConfig.ConsumerOffset = "earliest"
	}
	return pipeConfig
}

func getFilterIdsFromPipelineRequest(pipelineRequest lib.PipelineRequest) ([]string, []string) {
	var deviceIds []string
	var operatorIds []string
	for _, node := range pipelineRequest.Nodes {
		for _, input := range node.Inputs {
			if input.FilterType == RequestDeviceId {
				stringSlice := strings.Split(input.FilterIds, ",")
				deviceIds = append(deviceIds, stringSlice...)
			}
			if input.FilterType == RequestOperatorId {
				stringSlice := strings.Split(input.FilterIds, ",")
				operatorIds = append(operatorIds, stringSlice...)
			}

		}

	}
	return deviceIds, operatorIds
}
