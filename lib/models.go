/*
 * Copyright 2025 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package lib

import (
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
)

type PipelinesResponse struct {
	Data  []pipe.Pipeline `json:"data,omitempty"`
	Total int             `json:"total,omitempty"`
}

type OperatorRequestConfig struct {
	Config      map[string]string `json:"config,omitempty"`
	InputTopics []pipe.InputTopic `json:"inputTopics,omitempty"`
}

type PipelineRequest struct {
	Id                 string         `json:"id,omitempty"`
	FlowId             string         `json:"flowId,omitempty"`
	Name               string         `json:"name,omitempty"`
	Description        string         `json:"description,omitempty"`
	WindowTime         int            `json:"windowTime,omitempty"`
	MergeStrategy      string         `json:"mergeStrategy,omitempty"`
	ConsumeAllMessages bool           `json:"consumeAllMessages,omitempty"`
	Metrics            bool           `json:"metrics,omitempty"`
	Nodes              []PipelineNode `json:"nodes,omitempty"`
}

type PipelineStatusRequest struct {
	Ids []string `json:"ids,omitempty"`
}

type PipelineNode struct {
	NodeId          string                `json:"nodeId,omitempty"`
	Inputs          []NodeInput           `json:"inputs,omitempty"`
	Config          []NodeConfig          `json:"config,omitempty"`
	InputSelections []pipe.InputSelection `json:"inputSelections,omitempty"`
	PersistData     bool                  `json:"persistData,omitempty"`
}

type NodeConfig struct {
	Name  string `json:"name,omitempty"`
	Value string `json:"value,omitempty"`
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
	MergeStrategy  string
	Metrics        bool
	ConsumerOffset string
	FlowId         string
	PipelineId     string
	UserId         string
}

type PipelineStatus struct {
	Name          string `json:"name,omitempty"`
	Running       bool   `json:"running"`
	Transitioning bool   `json:"transitioning"`
	Message       string `json:"message"`
}
