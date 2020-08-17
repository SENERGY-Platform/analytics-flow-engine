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
	"fmt"
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
func (f *FlowEngine) StartPipeline(pipelineRequest PipelineRequest, userId string, authorization string) (pipeline Pipeline) {
	//Get parsed pipeline
	parsedPipeline, err := f.parsingService.GetPipeline(pipelineRequest.Id, userId, authorization)
	if err != nil {
		fmt.Println(err)
	}
	pipeline.FlowId = parsedPipeline.FlowId
	pipeline.Image = parsedPipeline.Image

	//Convert parsing Schema to internal Schema
	var tmpPipeline Pipeline
	for _, operator := range parsedPipeline.Operators {
		op := Operator{
			Id:             operator.Id,
			Name:           operator.Name,
			ImageId:        operator.ImageId,
			OperatorId:     operator.OperatorId,
			DeploymentType: operator.DeploymentType,
			OutputTopic:    f.getOperatorOutputTopic(operator.Name),
		}
		for _, topic := range operator.InputTopics {
			top := InputTopic{Name: topic.TopicName, FilterType: topic.FilterType, FilterValue: topic.FilterValue}
			for _, mapping := range topic.Mappings {
				top.Mappings = append(top.Mappings, Mapping{mapping.Dest, mapping.Source})
			}
			op.InputTopics = append(op.InputTopics, top)
		}
		tmpPipeline.Operators = append(tmpPipeline.Operators, op)
	}

	//Add starting operators
	for _, operator := range tmpPipeline.Operators {
		for _, node := range pipelineRequest.Nodes {
			if operator.Id == node.NodeId {
				if len(node.Inputs) > 0 {
					for _, input := range node.Inputs {
						t := InputTopic{Name: input.TopicName, FilterType: "DeviceId", FilterValue: input.DeviceId}
						for _, value := range input.Values {
							t.Mappings = append(t.Mappings, Mapping{value.Name, value.Path})
						}
						operator.InputTopics = append(operator.InputTopics, t)
					}
				}

				if len(node.Config) > 0 {
					m := make(map[string]string)
					for _, config := range node.Config {
						m[config.Name] = config.Value
					}
					operator.Config = m
				}
			}
		}
		pipeline.Operators = append(pipeline.Operators, operator)
	}
	pipeline.Name = pipelineRequest.Name
	pipeline.Description = pipelineRequest.Description

	pipeline.Id, _ = registerPipeline(&pipeline, userId, authorization)

	pipeline.Metrics.Enabled = pipelineRequest.Metrics
	if pipeline.Metrics.Enabled {
		metricsConfig, err := f.metricsService.RegisterPipeline(pipeline.Id.String())
		if err != nil {
			fmt.Println(err)
		}
		pipeline.Metrics.Database = metricsConfig.Database
		pipeline.Metrics.Username = metricsConfig.Username
		pipeline.Metrics.Password = metricsConfig.Password
		pipeline.Metrics.Url = metricsConfig.Url
		pipeline.Metrics.Interval = metricsConfig.Interval
		pipeline.Metrics.XmlUrl = metricsConfig.XmlUrl
	}

	var pipeConfig = PipelineConfig{
		WindowTime:     pipelineRequest.WindowTime,
		FlowId:         pipelineRequest.Id,
		ConsumerOffset: "latest",
		PipelineId:     pipeline.Id.String(),
		Metrics:        pipeline.Metrics,
	}
	if pipelineRequest.ConsumeAllMessages {
		pipeConfig.ConsumerOffset = "earliest"
	}

	f.startOperators(pipeline, pipeConfig)

	return pipeline
}

func (f *FlowEngine) DeletePipeline(id string, userId string, authorization string) string {
	println("Deleting Pipeline:" + id)
	var pipeline, err = getPipeline(id, userId, authorization)
	if err != nil {
		fmt.Println(err)
	}
	for _, operator := range pipeline.Operators {
		switch operator.DeploymentType {
		case "local":
			fmt.Println("stop local Operator: " + operator.Name)
			stopOperator(pipeline.Id.String(),
				operator)
			break
		default:
			err := f.driver.DeleteOperator(f.driver.GetOperatorName(id, operator)[0])
			if err != nil {
				fmt.Println(err)
				err := f.driver.DeleteOperator(f.driver.GetOperatorName(id, operator)[1])
				if err != nil {
					fmt.Println(err)
				}
			}
		}
	}
	err = deletePipeline(id, userId, authorization)
	if err != nil {
		fmt.Println(err)
	}
	if pipeline.Metrics.Enabled == true {
		err = f.metricsService.UnregisterPipeline(pipeline.Id.String())
		if err != nil {
			fmt.Println(err)
		}
	}
	return "done"
}

func (f *FlowEngine) GetPipelineStatus(id string) string {
	//TODO: Implement method
	return PIPELINE_RUNNING
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
		err := f.driver.CreateOperators(
			pipeline.Id.String(),
			cloudOperators,
			pipeConfig,
		)
		if err != nil {
			fmt.Println(err)
		}
	}
	if len(localOperators) > 0 {
		for _, operator := range localOperators {
			fmt.Println("start local Operator: " + operator.Name)
			startOperator(operator,
				pipeConfig)
		}
	}
}

func (f *FlowEngine) getOperatorOutputTopic(name string) (op_name string) {
	op_name = "analytics-" + name
	return
}
