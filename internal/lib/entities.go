/*
 * Copyright 2018 InfAI (CC SES)
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

import "github.com/google/uuid"

type Response struct {
	Message string `json:"message,omitempty"`
}

type Pipeline struct {
	Id                 uuid.UUID  `json:"id,omitempty"`
	FlowId             string     `json:"flowId,omitempty"`
	Name               string     `json:"name,omitempty"`
	Description        string     `json:"description,omitempty"`
	Image              string     `json:"image,omitempty"`
	WindowTime         int        `json:"windowTime,omitempty"`
	ConsumeAllMessages bool       `json:"consumeAllMessages,omitempty"`
	Metrics            bool       `json:"metrics,omitempty"`
	MetricsData        Metrics    `json:"metricsData,omitempty"`
	Operators          []Operator `json:"operators,omitempty"`
}

type Operator struct {
	Id              string            `json:"id,omitempty"`
	Name            string            `json:"name,omitempty"`
	ApplicationId   uuid.UUID         `json:"applicationId,omitempty"`
	ImageId         string            `json:"imageId,omitempty"`
	DeploymentType  string            `json:"deploymentType,omitempty"`
	OperatorId      string            `json:"operatorId,omitempty"`
	Config          map[string]string `json:"config,omitempty"`
	OutputTopic     string            `json:"outputTopic,omitempty"`
	InputTopics     []InputTopic
	InputSelections []InputSelection `json:"inputSelections,omitempty"`
}

type OperatorRequestConfig struct {
	Config      map[string]string `json:"config,omitempty"`
	InputTopics []InputTopic      `json:"inputTopics,omitempty"`
}

type InputTopic struct {
	Name         string    `json:"name,omitempty"`
	FilterType   string    `json:"filterType,omitempty"`
	FilterValue  string    `json:"filterValue,omitempty"`
	FilterValue2 string    `json:"filterValue2,omitempty"`
	Mappings     []Mapping `json:"mappings,omitempty"`
}

type Mapping struct {
	Dest   string `json:"dest,omitempty"`
	Source string `json:"source,omitempty"`
}

type Metrics struct {
	Database string `json:"database,omitempty"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	Url      string `json:"url,omitempty"`
	Interval string `json:"interval,omitempty"`
	XmlUrl   string `json:"xmlurl,omitempty"`
}

type PipelineRequest struct {
	Id                 string         `json:"id,omitempty"`
	FlowId             string         `json:"flowId,omitempty"`
	Name               string         `json:"name,omitempty"`
	Description        string         `json:"description,omitempty"`
	WindowTime         int            `json:"windowTime,omitempty"`
	ConsumeAllMessages bool           `json:"consumeAllMessages,omitempty"`
	Metrics            bool           `json:"metrics,omitempty"`
	Nodes              []PipelineNode `json:"nodes,omitempty"`
}

type PipelineNode struct {
	NodeId          string           `json:"nodeId, omitempty"`
	Inputs          []NodeInput      `json:"inputs,omitempty"`
	Config          []NodeConfig     `json:"config,omitempty"`
	InputSelections []InputSelection `json:"inputSelections,omitempty"`
}

type NodeConfig struct {
	Name  string `json:"name,omitempty"`
	Value string `json:"value,omitempty"`
}

type InputSelection struct {
	InputName         string   `json:"inputName,omitempty"`
	AspectId          string   `json:"aspectId,omitempty"`
	FunctionId        string   `json:"functionId,omitempty"`
	CharacteristicIds []string `json:"characteristicIds,omitempty"`
	SelectableId      string   `json:"selectableId,omitempty"`
}

type NodeInput struct {
	FilterType string      `json:"filterType,omitempty"`
	FilterIds  string      `json:"filterIds,omitempty"`
	TopicName  string      `json:"topicName,omitempty"`
	Values     []NodeValue `json:"values,omitempty"`
}

type NodeValue struct {
	Name string `json:"name,omitempty"`
	Path string `json:"path,omitempty"`
}

type PipelineConfig struct {
	WindowTime     int
	Metrics        bool
	MetricsData    Metrics
	ConsumerOffset string
	FlowId         string
	PipelineId     string
}

type ControlCommand struct {
	Command string      `json:"command,omitempty"`
	Data    OperatorJob `json:"data,omitempty"`
}

type OperatorJob struct {
	ImageId        string            `json:"imageId,omitempty"`
	InputTopics    []InputTopic      `json:"inputTopics,omitempty"`
	Config         FogConfig         `json:"config,omitempty"`
	OperatorConfig map[string]string `json:"operatorConfig,omitempty"`
}

type FogConfig struct {
	PipelineId     string `json:"pipelineId,omitempty"`
	OutputTopic    string `json:"outputTopic,omitempty"`
	OperatorId     string `json:"operatorId,omitempty"`
	BaseOperatorId string `json:"baseOperatorId,omitempty"`
}

type Claims struct {
	Sub         string              `json:"sub,omitempty"`
	RealmAccess map[string][]string `json:"realm_access,omitempty"`
}

func (c Claims) Valid() error {
	return nil
}
