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

	"github.com/google/uuid"
	operatorLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/operator"
	upstreamLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/upstream"
	"encoding/json"

)

type FlowEngine struct {
	driver            Driver
	parsingService    ParsingApiService
	metricsService    MetricsApiService
	permissionService PermissionApiService
	kafak2mqttService Kafka2MqttApiService
}

func NewFlowEngine(
	driver Driver, 
	parsingService ParsingApiService,
	metricsService MetricsApiService,
	permissionService PermissionApiService,
	kafak2mqttService Kafka2MqttApiService) *FlowEngine {
	return &FlowEngine{driver, parsingService, metricsService, permissionService, kafak2mqttService}
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
	if pipeline.Metrics {
		err = f.registerMetrics(&pipeline)
		if err != nil {
			return
		}
	}
	pipeConfig := f.createPipelineConfig(pipeline)
	pipeConfig.UserId = userId
	pipeline.Operators, err = f.startOperators(pipeline, pipeConfig, userId, token)
	if err != nil {
		return
	}
	err = updatePipeline(&pipeline, userId, token) //update is needed to set correct fog output topics (with pipeline ID) and instance id for downstream config of fog operators
	log.Printf("%+v", pipeline)
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

	if pipeline.Metrics != pipelineRequest.Metrics {
		pipeline.Metrics = pipelineRequest.Metrics
		if pipeline.Metrics {
			err = f.registerMetrics(&pipeline)
			if err != nil {
				return
			}
		} else {
			err = f.metricsService.UnregisterPipeline(pipeline.Id.String())
			if err != nil {
				return
			}
		}
	}

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

	pipeline.Operators, err = f.startOperators(pipeline, pipeConfig, userId, token)
	if err != nil {
		return
	}

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
	log.Printf("%+v", pipeline)
	if err != nil {
		return
	}

	err = deletePipeline(id, userId, token)
	if err != nil {
		return
	}
	if pipeline.Metrics == true {
		err = f.metricsService.UnregisterPipeline(pipeline.Id.String())
		if err != nil {
			return
		}
	}
	return
}

func (f *FlowEngine) GetPipelineStatus(id, userId, token string) error {
	//TODO: Implement method
	_, err := getPipeline(id, userId, token)
	if err != nil {
		return errors.New("Pipeline " + id + " not found")
	}
	return nil
}

func (f *FlowEngine) deleteOperators(pipeline Pipeline, userId string) (err error) {
	counter := 0
	for _, operator := range pipeline.Operators {
		switch operator.DeploymentType {
		case "local":
			log.Println("engine - stop local Operator: " + operator.Name)
			err = stopFogOperator(pipeline.Id.String(),
				operator, userId)
			if err != nil {
				return err
			}
			break
		default:
			err = f.driver.DeleteOperator(pipeline.Id.String(), operator)
			if err != nil {
				switch {
				case errors.Is(err, ErrWorkloadNotFound) && counter > 0:
				default:
					log.Println(err.Error())
				}
			}
		}
		counter++
	}
	return
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

	if len(cloudOperators) > 0 {
		counter := 0
		for _, operator := range cloudOperators {
			err := f.driver.DeleteOperator(pipeline.Id.String(), operator)
			if err != nil {
				log.Println("Cant delete operator: " + err.Error())
				switch {
				case errors.Is(err, ErrWorkloadNotFound) && counter > 0:
				default:
					log.Println(err.Error())
					return err
				}
			}
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
			newOperators = cloudOperatorsWithDownstreamID
		}
	}
	if len(localOperators) > 0 {
		for _, operator := range localOperators {
			log.Println("start local Operator: " + operator.Name)
			err = startFogOperator(operator, pipeConfig, userID)
			if err != nil {
				return 
			}

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
			log.Printf("Enable Cloud2Fog Forwarding for operator %s\n", operator.Id)
			createdInstance, err2 := f.kafak2mqttService.StartOperatorInstance(operator.Name, operator.Id, pipelineID, userID, token)
			if err != nil {
				err = err2
				return 
			}
			operator.DownstreamConfig.InstanceID = createdInstance.Id
			newOperators = append(newOperators, operator)
		}
	}
	
	return 
}

func (f *FlowEngine) enableFogToCloudForwarding(operator Operator, pipelineID, userID string) (error) {
	if operator.UpstreamConfig.Enabled {
		log.Printf("Enable Fog2Cloud Forwarding for operator %s\n", operator.Id)

		command := &upstreamLib.UpstreamControlMessage{
			OperatorOutputTopic: operator.OutputTopic,
		}
		message, err := json.Marshal(command)
		if err != nil {
			return err 
		}
		topic := upstreamLib.GetUpstreamEnableCloudTopic(userID)
		log.Println("try to publish enable forwarding command for operator: " + operator.Name + " - " + operator.Id + " to topic: " + topic)
		err = publishMessage(topic, string(message))
		if err != nil {
			log.Println("Cant enable upstream forwarding for " + operator.Id + ": " + err.Error())
			return err 
		} else {
			log.Println("publish enable forwarding command for operator: " + operator.Name + " - " + operator.Id + " to topic: " + topic + " was successfull")
		}
	}
	return nil
}

func (f *FlowEngine) disableCloudToFogForwarding(operators []Operator, pipelineID, userID, token string) error {
	for _, operator := range operators {
		if operator.DownstreamConfig.Enabled {
			err := f.kafak2mqttService.RemoveInstance(operator.DownstreamConfig.InstanceID, pipelineID, userID, token)
			if err != nil {
				return err
			}
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
		publishMessage(upstreamLib.GetUpstreamDisableCloudTopic(userID), string(message))					
	}
	return nil
}


func (f *FlowEngine) createPipelineConfig(pipeline Pipeline) PipelineConfig {
	var pipeConfig = PipelineConfig{
		WindowTime:     pipeline.WindowTime,
		MergeStrategy:  pipeline.MergeStrategy,
		FlowId:         pipeline.FlowId,
		ConsumerOffset: "latest",
		Metrics:        pipeline.Metrics,
		PipelineId:     pipeline.Id.String(),
		MetricsData:    pipeline.MetricsData,
	}
	if pipeline.ConsumeAllMessages {
		pipeConfig.ConsumerOffset = "earliest"
	}
	return pipeConfig
}

func (f *FlowEngine) registerMetrics(pipeline *Pipeline) (err error) {
	metricsConfig, err := f.metricsService.RegisterPipeline(pipeline.Id.String())
	if err != nil {
		return
	}
	pipeline.MetricsData.Database = metricsConfig.Database
	pipeline.MetricsData.Username = metricsConfig.Username
	pipeline.MetricsData.Password = metricsConfig.Password
	pipeline.MetricsData.Url = metricsConfig.Url
	pipeline.MetricsData.Interval = metricsConfig.Interval
	pipeline.MetricsData.XmlUrl = metricsConfig.XmlUrl
	return
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
