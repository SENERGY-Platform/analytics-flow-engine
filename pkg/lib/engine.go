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
)

type FlowEngine struct {
	driver            Driver
	parsingService    ParsingApiService
	metricsService    MetricsApiService
	permissionService PermissionApiService
}

func NewFlowEngine(driver Driver, parsingService ParsingApiService,
	metricsService MetricsApiService,
	permissionService PermissionApiService) *FlowEngine {
	return &FlowEngine{driver, parsingService, metricsService, permissionService}
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
	pipeline.Operators = addOperatorConfigs(pipelineRequest, tmpPipeline)

	pipeline.Name = pipelineRequest.Name
	pipeline.Description = pipelineRequest.Description
	pipeline.Id, err = registerPipeline(&pipeline, userId, token)
	if err != nil {
		return
	}
	pipeline.Metrics = pipelineRequest.Metrics
	if pipeline.Metrics {
		err = f.registerMetrics(&pipeline)
		if err != nil {
			return
		}
	}
	pipeConfig := f.createPipelineConfig(pipeline)
	pipeConfig.UserId = userId
	f.startOperators(pipeline, pipeConfig, userId)
	return
}

func (f *FlowEngine) UpdatePipeline(pipelineRequest PipelineRequest, userId string, token string) (pipeline Pipeline, err error) {
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

	for _, operator := range pipeline.Operators {
		switch operator.DeploymentType {
		case "local":
			log.Println("engine - stop local Operator: " + operator.Name)
			stopFogOperator(pipeline.Id.String(),
				operator, userId)
			break
		default:
			err := f.driver.DeleteOperator(pipeline.Id.String(), operator)
			if err != nil {
				log.Println(err)
			}
		}
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
			if pipeline.Operators[index].DeploymentType != "local" {
				outputTopicName = getOperatorOutputTopic(pipeline.Operators[index].Name)
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

	f.startOperators(pipeline, pipeConfig, userId)

	err = updatePipeline(&pipeline, userId, token)

	return
}

func (f *FlowEngine) DeletePipeline(id string, userId string, token string) (err error) {
	log.Println("engine - delete pipeline: " + id)
	pipeline, err := getPipeline(id, userId, token)
	if err != nil {
		return
	}
	for _, operator := range pipeline.Operators {
		switch operator.DeploymentType {
		case "local":
			log.Println("engine - stop local Operator: " + operator.Name)
			stopFogOperator(pipeline.Id.String(),
				operator, userId)
			break
		default:
			err := f.driver.DeleteOperator(id, operator)
			if err != nil {
				log.Println(err)
			}
		}
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

func (f *FlowEngine) GetPipelineStatus(id string) string {
	//TODO: Implement method
	return PipelineRunning
}

func (f *FlowEngine) startOperators(pipeline Pipeline, pipeConfig PipelineConfig, userID string) {
	var localOperators []Operator
	var cloudOperators []Operator
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
	if len(cloudOperators) > 0 {
		err := retry(3, 3*time.Second, func() (err error) {
			return f.driver.CreateOperators(
				pipeline.Id.String(),
				cloudOperators,
				pipeConfig,
			)
		})
		if err != nil {
			log.Println(err)
		} else {
			log.Println("engine - successfully started cloud operators - " + pipeline.Id.String())
		}
	}
	if len(localOperators) > 0 {
		for _, operator := range localOperators {
			log.Println("start local Operator: " + operator.Name)
			startFogOperator(operator,
				pipeConfig, userID)
		}
	}
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
