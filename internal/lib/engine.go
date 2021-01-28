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
	"log"
	"time"
)

type FlowEngine struct {
	driver         Driver
	parsingService ParsingApiService
	metricsService MetricsApiService
}

func NewFlowEngine(driver Driver, parsingService ParsingApiService, metricsService MetricsApiService) *FlowEngine {
	return &FlowEngine{driver, parsingService, metricsService}
}

// Starts a pipeline
func (f *FlowEngine) StartPipeline(pipelineRequest PipelineRequest, userId string, authorization string) (pipeline Pipeline, err error) {
	//Get parsed pipeline
	parsedPipeline, err := f.parsingService.GetPipeline(pipelineRequest.FlowId, userId, authorization)
	if err != nil {
		return
	}
	pipeline.FlowId = parsedPipeline.FlowId
	pipeline.Image = parsedPipeline.Image
	pipeline.WindowTime = pipelineRequest.WindowTime
	pipeline.ConsumeAllMessages = pipelineRequest.ConsumeAllMessages

	tmpPipeline := createPipeline(parsedPipeline)
	pipeline.Operators = addStartingOperatorConfigs(pipelineRequest, tmpPipeline)

	pipeline.Name = pipelineRequest.Name
	pipeline.Description = pipelineRequest.Description
	pipeline.Metrics = pipelineRequest.Metrics
	if pipeline.Metrics {
		err = f.registerMetrics(&pipeline)
		if err != nil {
			return
		}
	}
	pipeline.Id, err = registerPipeline(&pipeline, userId, authorization)
	if err != nil {
		return
	}
	pipeConfig := f.createPipelineConfig(pipeline)
	f.startOperators(pipeline, pipeConfig)
	return
}

func (f *FlowEngine) UpdatePipeline(pipelineRequest PipelineRequest, userId string, authorization string) (pipeline Pipeline, err error) {
	pipeline, err = getPipeline(pipelineRequest.Id, userId, authorization)
	if err != nil {
		return
	}
	pipeline.Operators = addStartingOperatorConfigs(pipelineRequest, pipeline)

	pipeline.Name = pipelineRequest.Name
	pipeline.Description = pipelineRequest.Description
	pipeline.WindowTime = pipelineRequest.WindowTime
	pipeline.ConsumeAllMessages = pipelineRequest.ConsumeAllMessages

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
			stopOperator(pipeline.Id.String(),
				operator)
			break
		default:
			err := f.driver.DeleteOperator(f.driver.GetOperatorName(pipeline.Id.String(), operator)[0])
			if err != nil {
				log.Println(err)
				err := f.driver.DeleteOperator(f.driver.GetOperatorName(pipeline.Id.String(), operator)[1])
				if err != nil {
					log.Println(err)
				}
			}
		}
	}

	pipeConfig := f.createPipelineConfig(pipeline)

	f.startOperators(pipeline, pipeConfig)

	err = updatePipeline(&pipeline, userId, authorization)

	return
}

func (f *FlowEngine) DeletePipeline(id string, userId string, authorization string) (err error) {
	log.Println("engine - delete pipeline: " + id)
	pipeline, err := getPipeline(id, userId, authorization)
	if err != nil {
		return
	}
	for _, operator := range pipeline.Operators {
		switch operator.DeploymentType {
		case "local":
			log.Println("engine - stop local Operator: " + operator.Name)
			stopOperator(pipeline.Id.String(),
				operator)
			break
		default:
			err := f.driver.DeleteOperator(f.driver.GetOperatorName(id, operator)[0])
			if err != nil {
				log.Println(err)
				err := f.driver.DeleteOperator(f.driver.GetOperatorName(id, operator)[1])
				if err != nil {
					log.Println(err)
				}
			}
		}
	}
	err = deletePipeline(id, userId, authorization)
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

func (f *FlowEngine) startOperators(pipeline Pipeline, pipeConfig PipelineConfig) {
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
			startOperator(operator,
				pipeConfig)
		}
	}
}

func (f *FlowEngine) createPipelineConfig(pipeline Pipeline) PipelineConfig {
	var pipeConfig = PipelineConfig{
		WindowTime:     pipeline.WindowTime,
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
