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

package rancher2_api

import (
	"analytics-flow-engine/internal/lib"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"crypto/tls"

	"encoding/json"

	"github.com/parnurzeal/gorequest"
)

type Rancher2 struct {
	url       string
	accessKey string
	secretKey string
	stackId   string
	zookeeper string
}

func NewRancher2(url string, accessKey string, secretKey string, stackId string, zookeeper string) *Rancher2 {
	return &Rancher2{url, accessKey, secretKey, stackId, zookeeper}
}

func (r *Rancher2) CreateOperator(pipelineId string, operator lib.Operator, pipeConfig lib.PipelineConfig) (err error) {
	fmt.Println("Rancher2 Create " + pipelineId)
	config, _ := json.Marshal(lib.OperatorRequestConfig{Config: operator.Config, InputTopics: operator.InputTopics})
	env := map[string]string{
		"ZK_QUORUM":                         r.zookeeper,
		"CONFIG_APPLICATION_ID":             "analytics-" + pipelineId + "-" + operator.Id,
		"PIPELINE_ID":                       pipelineId,
		"OPERATOR_ID":                       operator.Id,
		"WINDOW_TIME":                       strconv.Itoa(pipeConfig.WindowTime),
		"CONFIG":                            string(config),
		"DEVICE_ID_PATH":                    "device_id",
		"CONSUMER_AUTO_OFFSET_RESET_CONFIG": pipeConfig.ConsumerOffset,
	}

	if operator.OutputTopic != "" {
		env["OUTPUT"] = operator.OutputTopic
	}

	request := gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: true})
	reqBody := &Request{
		Name:        r.GetOperatorName(pipelineId, operator)[0],
		NamespaceId: lib.GetEnv("RANCHER2_NAMESPACE_ID", ""),
		Containers: []Container{{
			Image:           operator.ImageId,
			Name:            r.GetOperatorName(pipelineId, operator)[0],
			Environment:     env,
			ImagePullPolicy: "Always",
		}},
		Scheduling: Scheduling{Scheduler: "default-scheduler", Node: Node{RequireAll: []string{"role=worker"}}},
		Labels:     map[string]string{"op": operator.Id, "flowId": pipeConfig.FlowId, "pipeId": pipelineId},
		Selector:   Selector{MatchLabels: map[string]string{"op": operator.Id}},
	}
	if pipeConfig.Metrics.Enabled {
		env["METRICS_URL"] = pipeConfig.Metrics.Url
		env["METRICS_USER"] = pipeConfig.Metrics.Username
		env["METRICS_PASSWORD"] = pipeConfig.Metrics.Password
		env["METRICS_INTERVAL"] = pipeConfig.Metrics.Interval
		reqBody.Containers = []Container{{
			Image:           operator.ImageId,
			Name:            r.GetOperatorName(pipelineId, operator)[0],
			Environment:     env,
			ImagePullPolicy: "Always",
			Command: []string{
				"java",
				"-javaagent:/opt/jmxtrans-agent.jar=" + pipeConfig.Metrics.XmlUrl,
				"-jar",
				"/opt/operator-" + operator.Name + "-jar-with-dependencies.jar",
			},
		}}
	}
	resp, body, e := request.Post(r.url + "projects/" + lib.GetEnv("RANCHER2_PROJECT_ID", "") + "/workloads").Send(reqBody).End()
	if resp.StatusCode != http.StatusCreated {
		err = errors.New("could not create operator: " + body)
	}
	if len(e) > 0 {
		err = errors.New("could not create operator: an error occurred")
	}
	return
}

func (r *Rancher2) CreateOperators(pipelineId string, inputs []lib.Operator, pipeConfig lib.PipelineConfig) (err error) {
	var containers []Container
	for _, operator := range inputs {
		config, _ := json.Marshal(lib.OperatorRequestConfig{Config: operator.Config, InputTopics: operator.InputTopics})
		labels := map[string]string{"operatorId": operator.Id, "flowId": pipeConfig.FlowId, "pipeId": pipelineId}
		env := map[string]string{
			"ZK_QUORUM":                         r.zookeeper,
			"CONFIG_APPLICATION_ID":             "analytics-" + pipelineId + "-" + operator.Id,
			"PIPELINE_ID":                       pipelineId,
			"OPERATOR_ID":                       operator.Id,
			"WINDOW_TIME":                       strconv.Itoa(pipeConfig.WindowTime),
			"CONFIG":                            string(config),
			"DEVICE_ID_PATH":                    "device_id",
			"CONSUMER_AUTO_OFFSET_RESET_CONFIG": pipeConfig.ConsumerOffset,
		}
		container := Container{
			Image:           operator.ImageId,
			Name:            operator.Id,
			Environment:     env,
			ImagePullPolicy: "Always",
		}
		if operator.OutputTopic != "" {
			env["OUTPUT"] = operator.OutputTopic
		}
		if pipeConfig.Metrics.Enabled {
			env["METRICS_URL"] = pipeConfig.Metrics.Url
			env["METRICS_USER"] = pipeConfig.Metrics.Username
			env["METRICS_PASSWORD"] = pipeConfig.Metrics.Password
			env["METRICS_INTERVAL"] = pipeConfig.Metrics.Interval
			container.Command = []string{
				"java",
				"-javaagent:/opt/jmxtrans-agent.jar=" + pipeConfig.Metrics.XmlUrl,
				"-jar",
				"/opt/operator-" + operator.Name + "-jar-with-dependencies.jar",
			}
		}
		container.Labels = labels
		containers = append(containers, container)
	}
	request := gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: true})
	reqBody := &Request{
		Name:        r.GetOperatorName(pipelineId, lib.Operator{Id: "v3-123456789"})[1],
		NamespaceId: lib.GetEnv("RANCHER2_NAMESPACE_ID", ""),
		Containers:  containers,
		Scheduling:  Scheduling{Scheduler: "default-scheduler", Node: Node{RequireAll: []string{"role=worker"}}},
		Labels:      map[string]string{"flowId": pipeConfig.FlowId, "pipelineId": pipelineId},
		Selector:    Selector{MatchLabels: map[string]string{"pipelineId": pipelineId}},
	}

	resp, body, e := request.Post(r.url + "projects/" + lib.GetEnv("RANCHER2_PROJECT_ID", "") + "/workloads").Send(reqBody).End()
	if resp.StatusCode != http.StatusCreated {
		err = errors.New("could not create operators: " + body)
	}
	if len(e) > 0 {
		err = errors.New("could not create operators: an error occurred")
	}
	return
}

func (r *Rancher2) DeleteOperator(operatorId string) (err error) {
	request := gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: true})
	resp, body, e := request.Delete(r.url + "projects/" + lib.GetEnv("RANCHER2_PROJECT_ID", "") + "/workloads/deployment:" +
		lib.GetEnv("RANCHER2_NAMESPACE_ID", "") + ":" + operatorId).End()
	if resp.StatusCode != http.StatusNoContent {
		err = errors.New("could not delete operator: " + body)
		return
	}
	if len(e) > 0 {
		err = errors.New("something went wrong")
		return
	}
	return
}

func (r *Rancher2) GetOperatorName(pipelineId string, operator lib.Operator) []string {
	return []string{"operator-" + pipelineId + "-" + operator.Id[0:8], "pipeline-" + pipelineId}
}
