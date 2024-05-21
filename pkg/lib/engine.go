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
	"errors"
	"log"
	"strings"
	"time"

	"encoding/json"
	operatorLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/operator"
	upstreamLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/upstream"
	"github.com/google/uuid"
)

type FlowEngine struct {
	driver            Driver
	parsingService    ParsingApiService
	permissionService PermissionApiService
	kafak2mqttService Kafka2MqttApiService
}

func NewFlowEngine(
	driver Driver,
	parsingService ParsingApiService,
	permissionService PermissionApiService,
	kafak2mqttService Kafka2MqttApiService) *FlowEngine {
	return &FlowEngine{driver, parsingService, permissionService, kafak2mqttService}
}

func (f *FlowEngine) StartPipeline(pipelineRequest PipelineRequest, userId string, token string) (pipeline Pipeline, err error) {
	//Check access
	deviceIds, operatorIds := getFilterIdsFromPipelineRequest(pipelineRequest)
	if len(deviceIds) > 0 {
		hasAccess, e := f.permissionService.UserHasDevicesReadAccess(deviceIds, token)
		if e != nil {
			return pipeline, e
		}
		if !hasAccess {
			e = errors.New("engine - user does not have the rights to access the devices")
			return pipeline, e
		}
	}
	if len(operatorIds) > 0 {
		for _, operatorId := range operatorIds {
			_, e := getPipeline(strings.Split(operatorId, ":")[1], userId, token)
			if e != nil {
				e = errors.New("engine - user does not have the rights to access the pipeline: " + strings.Split(operatorId, ":")[1])
				return pipeline, e
			}
		}
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
	pipeline.Operators = addOperatorConfigs(pipelineRequest, tmpPipeline)
	pipeline.Id, err = registerPipeline(&pipeline, userId, token)
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
	err = updatePipeline(&pipeline, userId, token) //update is needed to set correct fog output topics (with pipeline ID) and instance id for downstream config of fog operators
	if err != nil {
		log.Println("Cant update pipeline")
	}
	log.Printf("Started pipeline with config: %+v", pipeline)
	return
}

func addPipelineIDToFogTopic(operators []Operator, pipelineId string) (newOperators []Operator) {
	for _, operator := range operators {
		if operator.DeploymentType == "local" {
			// why pipeline id needed in fog operator topics?
			pipelineIDSuffix := "/" + pipelineId
			operator.OutputTopic = operator.OutputTopic + pipelineIDSuffix
			for _, operatorInputTopic := range operator.InputTopics {
				if operatorInputTopic.FilterType == "OperatorId" {
					operatorInputTopic.Name += pipelineIDSuffix
				}
			}
		}
		newOperators = append(newOperators, operator)
	}
	return
}

func (f *FlowEngine) UpdatePipeline(pipelineRequest PipelineRequest, userId string, token string) (pipeline Pipeline, err error) {
	log.Println("engine - update pipeline: " + pipelineRequest.Id)
	//Check access
	deviceIds, operatorIds := getFilterIdsFromPipelineRequest(pipelineRequest)
	if len(deviceIds) > 0 {
		hasAccess, e := f.permissionService.UserHasDevicesReadAccess(deviceIds, token)
		if e != nil {
			return pipeline, e
		}
		if !hasAccess {
			e = errors.New("engine - user does not have the rights to access the devices")
			return pipeline, e
		}
	}
	if len(operatorIds) > 0 {
		for _, operatorId := range operatorIds {
			_, e := getPipeline(strings.Split(operatorId, ":")[1], userId, token)
			if e != nil {
				e = errors.New("engine - user does not have the rights to access the pipeline: " + strings.Split(operatorId, ":")[1])
				return pipeline, e
			}
		}
	}
	pipeline, err = getPipeline(pipelineRequest.Id, userId, token)
	if err != nil {
		return
	}
	pipeline.Operators = addOperatorConfigs(pipelineRequest, pipeline)

	pipeline.Name = pipelineRequest.Name
	pipeline.Description = pipelineRequest.Description
	pipeline.WindowTime = pipelineRequest.WindowTime
	pipeline.MergeStrategy = pipelineRequest.MergeStrategy

	err = f.stopOperators(pipeline, userId, token)
	if err != nil {
		log.Println("Cant stop operators: " + err.Error())
		return
	}

	// give the backend some time to delete the operators
	time.Sleep(3 * time.Second)

	missingUuid, _ := uuid.FromBytes([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	for index := range pipeline.Operators {
		// if app id is missing, set a new one
		if pipeline.Operators[index].ApplicationId == missingUuid {
			pipeline.Operators[index].ApplicationId = uuid.New()
		}
		// if output topic is missing,set it
		if pipeline.Operators[index].OutputTopic == "" {
			outputTopicName := pipeline.Operators[index].Name
			outputTopicName, _ = operatorLib.GenerateOperatorOutputTopic(pipeline.Operators[index].Name, pipeline.Operators[index].OperatorId, pipeline.Operators[index].Id, pipeline.Operators[index].DeploymentType)
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

	err = updatePipeline(&pipeline, userId, token)

	return
}

func (f *FlowEngine) DeletePipeline(id string, userId string, token string) (err error) {
	log.Println("engine - delete pipeline: " + id)
	pipeline, err := getPipeline(id, userId, token)
	if err != nil {
		return
	}
	err = f.stopOperators(pipeline, userId, token)
	if err != nil {
		return
	}
	log.Println("removed all operators for pipeline: " + id)

	err = deletePipeline(id, userId, token)
	if err != nil {
		return
	}
	return
}

func (f *FlowEngine) GetPipelineStatus(id, userId, token string) (PipelineStatus, error) {
	status, err := f.driver.GetPipelineStatus(id)
	return status, err
}

func seperateOperators(pipeline Pipeline) (localOperators []Operator, cloudOperators []Operator) {
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

func (f *FlowEngine) stopOperators(pipeline Pipeline, userID, token string) error {
	localOperators, cloudOperators := seperateOperators(pipeline)
	// TODO remove localOperators = addPipelineIDToFogTopic(localOperators, pipeline.Id.String())
	log.Printf("%+v\n", localOperators)
	log.Printf("%+v", cloudOperators)

	if len(cloudOperators) > 0 {
		counter := 0
		for _, operator := range cloudOperators {
			err := f.driver.DeleteOperator(pipeline.Id.String(), operator)
			if err != nil {
				log.Println("Cant delete operator: " + err.Error())
				switch {
				// When first operator is deleted -> the whole pod gets removed, so all following operators wont exists anymore
				case errors.Is(err, ErrWorkloadNotFound) && counter > 0:
				default:
					log.Println(err.Error())
					return err
				}
			}
			log.Println("Removed operator: " + operator.Id + " of pipeline: " + pipeline.Id.String())
			counter++
		}
		err := f.disableCloudToFogForwarding(cloudOperators, pipeline.Id.String(), userID, token)
		if err != nil {
			log.Println("Cant disable cloud2fog forwarding: " + err.Error())
			return err
		}
	}

	if len(localOperators) > 0 {
		for _, operator := range localOperators {
			log.Println("engine - stop local Operator: " + operator.Name)
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

func (f *FlowEngine) startOperators(pipeline Pipeline, pipeConfig PipelineConfig, userID, token string) (newOperators []Operator, err error) {
	localOperators, cloudOperators := seperateOperators(pipeline)

	if len(cloudOperators) > 0 {
		log.Println("Try to start cloud operators")
		err = retry(3, 3*time.Second, func() (err error) {
			return f.driver.CreateOperators(
				pipeline.Id.String(),
				cloudOperators,
				pipeConfig,
			)
		})
		if err != nil {
			log.Println(err)
			return
		} else {
			log.Println("engine - successfully started cloud operators - " + pipeline.Id.String())
			cloudOperatorsWithDownstreamID, err2 := f.enableCloudToFogForwarding(cloudOperators, pipeline.Id.String(), userID, token)
			if err2 != nil {
				log.Println("engine - cant enable cloud2fog forwarding - " + err2.Error())
				err = err2
				return
			}
			newOperators = append(newOperators, cloudOperatorsWithDownstreamID...)
		}
	}
	if len(localOperators) > 0 {
		for _, operator := range localOperators {
			log.Println("try to start local operator: " + operator.Name + " for pipeline: " + pipeline.Id.String())
			err = startFogOperator(operator, pipeConfig, userID)
			if err != nil {
				log.Println("Cant start local operator: " + operator.Name + " for pipeline: " + pipeline.Id.String() + ": " + err.Error())
				return
			}
			log.Println("engine - started local operator: " + operator.Name + " for pipeline: " + pipeline.Id.String())

			err = f.enableFogToCloudForwarding(operator, pipeline.Id.String(), userID)
			if err != nil {
				return
			}
			newOperators = append(newOperators, operator)
		}
	}
	return
}

func (f *FlowEngine) enableCloudToFogForwarding(operators []Operator, pipelineID, userID, token string) (newOperators []Operator, err error) {
	for _, operator := range operators {
		if operator.DownstreamConfig.Enabled {
			log.Printf("Try to enable Cloud2Fog Forwarding for operator %s\n", operator.Id)
			createdInstance, err := f.kafak2mqttService.StartOperatorInstance(operator.Name, operator.Id, pipelineID, userID, token)
			if err != nil {
				log.Printf("Cant enable Cloud2Fog Forwarding for operator %s\n: %s", operator.Id, err.Error())
				return []Operator{}, err
			}
			operator.DownstreamConfig.InstanceID = createdInstance.Id
		}
		newOperators = append(newOperators, operator) // operator needs to be appened so that no operator is lost
	}

	return
}

func (f *FlowEngine) enableFogToCloudForwarding(operator Operator, pipelineID, userID string) error {
	if operator.UpstreamConfig.Enabled {
		log.Printf("Try to enable Fog2Cloud Forwarding for operator %s\n", operator.Id)

		command := &upstreamLib.UpstreamControlMessage{
			OperatorOutputTopic: operator.OutputTopic,
		}
		message, err := json.Marshal(command)
		if err != nil {
			log.Println("Cant unmarshal enable fog2cloud message for operator: " + operator.Name + " - " + operator.Id + ": " + err.Error())
			return err
		}
		topic := upstreamLib.GetUpstreamEnableCloudTopic(userID)
		log.Println("try to publish enable forwarding command for operator: " + operator.Name + " - " + operator.Id + " to topic: " + topic)
		err = publishMessage(topic, string(message))
		if err != nil {
			log.Println("Cant publish enable upstream forwarding for " + operator.Id + ": " + err.Error())
			return err
		}
		log.Println("published enable forwarding command for operator: " + operator.Name + " - " + operator.Id + " to topic: " + topic + " was successfull")
	}
	return nil
}

func (f *FlowEngine) disableCloudToFogForwarding(operators []Operator, pipelineID, userID, token string) error {
	for _, operator := range operators {
		if operator.DownstreamConfig.Enabled {
			log.Printf("Try to disable Cloud2Fog Forwarding for operator %s\n", operator.Id)
			err := f.kafak2mqttService.RemoveInstance(operator.DownstreamConfig.InstanceID, pipelineID, userID, token)
			if err != nil {
				log.Printf("Cant disable Cloud2Fog Forwarding for operator %s: %s\n", operator.Id, err.Error())
				return err
			}
			log.Printf("Disabled Cloud2Fog Forwarding for operator %s\n", operator.Id)
		} else {
			log.Println("Operator " + operator.Id + " has no downstream forwarding enabled")
		}
	}
	return nil
}

func (f *FlowEngine) disableFogToCloudForwarding(operator Operator, pipelineID, userID, token string) error {
	if operator.UpstreamConfig.Enabled {
		command := &upstreamLib.UpstreamControlMessage{
			OperatorOutputTopic: operator.OutputTopic,
		}
		message, err := json.Marshal(command)
		if err != nil {
			log.Println("Cant unmarshal fog2cloud forwarding message: " + err.Error())
			return err
		}
		log.Println("publish disable forwarding command for operator: " + operator.Name + " - " + operator.Id)
		err = publishMessage(upstreamLib.GetUpstreamDisableCloudTopic(userID), string(message))
		if err != nil {
			log.Println("cant publish disable Fog2Cloud message for operator: " + operator.Name + " - " + operator.Id + ": " + err.Error())
		}
	} else {
		log.Println("Operator " + operator.Id + " has no upstream forwarding enabled")
	}
	return nil
}

func (f *FlowEngine) createPipelineConfig(pipeline Pipeline) PipelineConfig {
	var pipeConfig = PipelineConfig{
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

func getFilterIdsFromPipelineRequest(pipelineRequest PipelineRequest) ([]string, []string) {
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
