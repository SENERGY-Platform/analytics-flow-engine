/*
 * Copyright 2020 InfAI (CC SES)
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
	"fmt"
	parsing_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/parsing-api"
	"github.com/google/uuid"
	"io/ioutil"
	"reflect"
	"testing"
)

func TestParser_createPipeline(t *testing.T) {
	var parsedPipeline parsing_api.Pipeline
	err := json.Unmarshal(parseJsonFile("testdata/pipeline1.json", parsedPipeline), &parsedPipeline)
	if err != nil {
		fmt.Println(err)
	}
	id, _ := uuid.Parse("00000000-0000-0000-0000-000000000000")
	var expected = Pipeline{
		Id:      id,
		Metrics: false,
		Operators: []Operator{
			{
				Id:             "6fc47542-dfee-4d6e-b352-dab9c91e5aed",
				Name:           "adder",
				ApplicationId:  id,
				ImageId:        "repo/analytics-operator-adder:dev",
				DeploymentType: "cloud",
				OperatorId:     "5d2da1c0de2c3100015801f3",
				OutputTopic:    "analytics-adder",
				InputTopics: []InputTopic{
					{
						Name:        "analytics-adder",
						FilterType:  "OperatorId",
						FilterValue: "738dc0c6-91f9-47c0-96d8-3b09c0278837",
						Mappings: []Mapping{
							{
								Dest:   "value",
								Source: "sum",
							},
						},
					},
				},
			},
			{
				Id:             "47ef81cb-fa88-45c2-99fe-ac82d57774ba",
				Name:           "adder",
				ApplicationId:  id,
				ImageId:        "repo/analytics-operator-adder:dev",
				DeploymentType: "cloud",
				OperatorId:     "5d2da1c0de2c3100015801f3",
				OutputTopic:    "analytics-adder",
				InputTopics: []InputTopic{
					{
						Name:        "analytics-adder",
						FilterType:  "OperatorId",
						FilterValue: "738dc0c6-91f9-47c0-96d8-3b09c0278837",
						Mappings: []Mapping{
							{
								Dest:   "timestamp",
								Source: "lastTimestamp",
							},
						},
					},
				},
			},
			{
				Id:             "738dc0c6-91f9-47c0-96d8-3b09c0278837",
				Name:           "adder",
				ApplicationId:  id,
				ImageId:        "repo/analytics-operator-adder:dev",
				DeploymentType: "cloud",
				OperatorId:     "5d2da1c0de2c3100015801f3",
				OutputTopic:    "analytics-adder",
				InputTopics:    nil,
			},
		},
	}
	pipeline := createPipeline(parsedPipeline)
	for key := range pipeline.Operators {
		pipeline.Operators[key].ApplicationId = id
	}
	if !reflect.DeepEqual(expected, pipeline) {
		fmt.Println(expected)
		fmt.Println(pipeline)
		file, _ := json.MarshalIndent(pipeline, "", " ")
		_ = ioutil.WriteFile("./testdata/test.json", file, 0644)
		t.Error("structs do not match")
	}
}

func TestParser_addStartingOperatorConfigs(t *testing.T) {
	id, _ := uuid.Parse("00000000-0000-0000-0000-000000000000")
	var expected = Pipeline{
		Id:      id,
		Metrics: false,
		Operators: []Operator{
			{
				Id:             "6fc47542-dfee-4d6e-b352-dab9c91e5aed",
				Name:           "adder",
				ImageId:        "repo/analytics-operator-adder:dev",
				DeploymentType: "cloud",
				OperatorId:     "5d2da1c0de2c3100015801f3",
				OutputTopic:    "analytics-adder",
				InputTopics: []InputTopic{
					{
						Name:        "analytics-adder",
						FilterType:  "OperatorId",
						FilterValue: "738dc0c6-91f9-47c0-96d8-3b09c0278837",
						Mappings: []Mapping{
							{
								Dest:   "value",
								Source: "sum",
							},
						},
					},
					{
						Name:        "device3",
						FilterType:  "DeviceId",
						FilterValue: "3",
						Mappings: []Mapping{
							{
								Dest:   "timestamp",
								Source: "value.root.time",
							},
						},
					},
				},
			},
			{
				Id:             "47ef81cb-fa88-45c2-99fe-ac82d57774ba",
				Name:           "adder",
				ImageId:        "repo/analytics-operator-adder:dev",
				DeploymentType: "cloud",
				OperatorId:     "5d2da1c0de2c3100015801f3",
				OutputTopic:    "analytics-adder",
				InputTopics: []InputTopic{
					{
						Name:        "analytics-adder",
						FilterType:  "OperatorId",
						FilterValue: "738dc0c6-91f9-47c0-96d8-3b09c0278837",
						Mappings: []Mapping{
							{
								Dest:   "timestamp",
								Source: "lastTimestamp",
							},
						},
					},
					{
						Name:        "device4",
						FilterType:  "DeviceId",
						FilterValue: "4",
						Mappings: []Mapping{
							{
								Dest:   "value",
								Source: "value.root.OBIS_16_7.value",
							},
						},
					},
				},
			},
			{
				Id:             "738dc0c6-91f9-47c0-96d8-3b09c0278837",
				Name:           "adder",
				ImageId:        "repo/analytics-operator-adder:dev",
				DeploymentType: "cloud",
				OperatorId:     "5d2da1c0de2c3100015801f3",
				OutputTopic:    "analytics-adder",
				InputTopics: []InputTopic{
					{
						Name:        "device1",
						FilterType:  "DeviceId",
						FilterValue: "1",
						Mappings: []Mapping{
							{
								Dest:   "timestamp",
								Source: "value.root.time",
							},
						},
					},
					{
						Name:        "device2",
						FilterType:  "DeviceId",
						FilterValue: "2",
						Mappings: []Mapping{
							{
								Dest:   "value",
								Source: "value.root.OBIS_16_7.value",
							},
						},
					},
				},
			},
		},
	}
	var pipelineRequest PipelineRequest
	err := json.Unmarshal(parseJsonFile("testdata/request1.json", pipelineRequest), &pipelineRequest)
	if err != nil {
		fmt.Println(err)
	}
	var parsedPipeline parsing_api.Pipeline
	err = json.Unmarshal(parseJsonFile("testdata/pipeline1.json", parsedPipeline), &parsedPipeline)
	if err != nil {
		fmt.Println(err)
	}
	pipeline := createPipeline(parsedPipeline)
	pipeline.Operators = addOperatorConfigs(pipelineRequest, pipeline)
	for key := range pipeline.Operators {
		pipeline.Operators[key].ApplicationId = id
	}
	if !reflect.DeepEqual(expected, pipeline) {
		fmt.Println(expected)
		fmt.Println(pipeline)
		file, _ := json.MarshalIndent(pipeline, "", " ")
		_ = ioutil.WriteFile("./testdata/test.json", file, 0644)
		t.Error("structs do not match")
	}
}

func TestParser_addStartingOperatorConfigsTwoTimesSimple(t *testing.T) {
	id, _ := uuid.Parse("00000000-0000-0000-0000-000000000000")
	var expected = Pipeline{
		Id:      id,
		Metrics: false,
		Operators: []Operator{
			{
				Id:             "6fc47542-dfee-4d6e-b352-dab9c91e5aed",
				Name:           "adder",
				ImageId:        "repo/analytics-operator-adder:dev",
				DeploymentType: "cloud",
				OperatorId:     "5d2da1c0de2c3100015801f3",
				OutputTopic:    "analytics-adder",
				InputTopics: []InputTopic{
					{
						Name:        "analytics-adder",
						FilterType:  "OperatorId",
						FilterValue: "738dc0c6-91f9-47c0-96d8-3b09c0278837",
						Mappings: []Mapping{
							{
								Dest:   "value",
								Source: "sum",
							},
						},
					},
				},
			},
			{
				Id:             "47ef81cb-fa88-45c2-99fe-ac82d57774ba",
				Name:           "adder",
				ImageId:        "repo/analytics-operator-adder:dev",
				DeploymentType: "cloud",
				OperatorId:     "5d2da1c0de2c3100015801f3",
				OutputTopic:    "analytics-adder",
				InputTopics: []InputTopic{
					{
						Name:        "analytics-adder",
						FilterType:  "OperatorId",
						FilterValue: "738dc0c6-91f9-47c0-96d8-3b09c0278837",
						Mappings: []Mapping{
							{
								Dest:   "timestamp",
								Source: "lastTimestamp",
							},
						},
					},
				},
			},
			{
				Id:             "738dc0c6-91f9-47c0-96d8-3b09c0278837",
				Name:           "adder",
				ImageId:        "repo/analytics-operator-adder:dev",
				DeploymentType: "cloud",
				OperatorId:     "5d2da1c0de2c3100015801f3",
				OutputTopic:    "analytics-adder",
				InputTopics: []InputTopic{
					{
						Name:        "device1",
						FilterType:  "DeviceId",
						FilterValue: "1",
						Mappings: []Mapping{
							{
								Dest:   "timestamp",
								Source: "value.root.time",
							},
						},
					},
					{
						Name:        "device2",
						FilterType:  "DeviceId",
						FilterValue: "2",
						Mappings: []Mapping{
							{
								Dest:   "value",
								Source: "CHANGED",
							},
						},
					},
				},
			},
		},
	}
	var pipelineRequest PipelineRequest
	err := json.Unmarshal(parseJsonFile("testdata/request3.json", pipelineRequest), &pipelineRequest)
	if err != nil {
		fmt.Println(err)
	}
	var pipelineRequest2 PipelineRequest
	err = json.Unmarshal(parseJsonFile("testdata/request3.json", pipelineRequest2), &pipelineRequest2)
	if err != nil {
		fmt.Println(err)
	}
	var parsedPipeline parsing_api.Pipeline
	err = json.Unmarshal(parseJsonFile("testdata/pipeline1.json", parsedPipeline), &parsedPipeline)
	if err != nil {
		fmt.Println(err)
	}
	pipeline := createPipeline(parsedPipeline)
	pipeline.Operators = addOperatorConfigs(pipelineRequest, pipeline)
	pipeline.Operators = addOperatorConfigs(pipelineRequest2, pipeline)
	for key := range pipeline.Operators {
		pipeline.Operators[key].ApplicationId = id
	}
	if !reflect.DeepEqual(expected, pipeline) {
		fmt.Println(expected)
		fmt.Println(pipeline)
		file, _ := json.MarshalIndent(pipeline, "", " ")
		_ = ioutil.WriteFile("./testdata/test.json", file, 0644)
		t.Error("structs do not match")
	}
}

func TestParser_addStartingOperatorConfigsTwoTimes(t *testing.T) {
	id, _ := uuid.Parse("00000000-0000-0000-0000-000000000000")
	var expected = Pipeline{
		Id:      id,
		Metrics: false,
		Operators: []Operator{
			{
				Id:             "6fc47542-dfee-4d6e-b352-dab9c91e5aed",
				Name:           "adder",
				ImageId:        "repo/analytics-operator-adder:dev",
				DeploymentType: "cloud",
				OperatorId:     "5d2da1c0de2c3100015801f3",
				OutputTopic:    "analytics-adder",
				InputTopics: []InputTopic{
					{
						Name:        "analytics-adder",
						FilterType:  "OperatorId",
						FilterValue: "738dc0c6-91f9-47c0-96d8-3b09c0278837",
						Mappings: []Mapping{
							{
								Dest:   "value",
								Source: "sum",
							},
						},
					},
					{
						Name:        "device3",
						FilterType:  "DeviceId",
						FilterValue: "3",
						Mappings: []Mapping{
							{
								Dest:   "timestamp",
								Source: "value.root.time",
							},
						},
					},
				},
			},
			{
				Id:             "47ef81cb-fa88-45c2-99fe-ac82d57774ba",
				Name:           "adder",
				ImageId:        "repo/analytics-operator-adder:dev",
				DeploymentType: "cloud",
				OperatorId:     "5d2da1c0de2c3100015801f3",
				OutputTopic:    "analytics-adder",
				InputTopics: []InputTopic{
					{
						Name:        "analytics-adder",
						FilterType:  "OperatorId",
						FilterValue: "738dc0c6-91f9-47c0-96d8-3b09c0278837",
						Mappings: []Mapping{
							{
								Dest:   "timestamp",
								Source: "lastTimestamp",
							},
						},
					},
					{
						Name:        "device4",
						FilterType:  "DeviceId",
						FilterValue: "4",
						Mappings: []Mapping{
							{
								Dest:   "value",
								Source: "value.root.OBIS_16_7.value",
							},
						},
					},
				},
			},
			{
				Id:             "738dc0c6-91f9-47c0-96d8-3b09c0278837",
				Name:           "adder",
				ImageId:        "repo/analytics-operator-adder:dev",
				DeploymentType: "cloud",
				OperatorId:     "5d2da1c0de2c3100015801f3",
				OutputTopic:    "analytics-adder",
				InputTopics: []InputTopic{
					{
						Name:        "device1",
						FilterType:  "DeviceId",
						FilterValue: "1",
						Mappings: []Mapping{
							{
								Dest:   "timestamp",
								Source: "value.root.time",
							},
						},
					},
					{
						Name:        "device2",
						FilterType:  "DeviceId",
						FilterValue: "2",
						Mappings: []Mapping{
							{
								Dest:   "value",
								Source: "CHANGED",
							},
						},
					},
				},
			},
		},
	}
	var pipelineRequest PipelineRequest
	err := json.Unmarshal(parseJsonFile("testdata/request1.json", pipelineRequest), &pipelineRequest)
	if err != nil {
		fmt.Println(err)
	}
	var pipelineRequest2 PipelineRequest
	err = json.Unmarshal(parseJsonFile("testdata/request2.json", pipelineRequest2), &pipelineRequest2)
	if err != nil {
		fmt.Println(err)
	}
	var parsedPipeline parsing_api.Pipeline
	err = json.Unmarshal(parseJsonFile("testdata/pipeline1.json", parsedPipeline), &parsedPipeline)
	if err != nil {
		fmt.Println(err)
	}
	pipeline := createPipeline(parsedPipeline)
	pipeline.Operators = addOperatorConfigs(pipelineRequest, pipeline)
	pipeline.Operators = addOperatorConfigs(pipelineRequest2, pipeline)
	for key := range pipeline.Operators {
		pipeline.Operators[key].ApplicationId = id
	}
	if !reflect.DeepEqual(expected, pipeline) {
		fmt.Println(expected)
		fmt.Println(pipeline)
		file, _ := json.MarshalIndent(pipeline, "", " ")
		_ = ioutil.WriteFile("./testdata/test.json", file, 0644)
		t.Error("structs do not match")
	}
}
