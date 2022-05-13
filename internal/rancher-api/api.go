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

package rancher_api

import (
	"analytics-flow-engine/internal/lib"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/parnurzeal/gorequest"
)

type Rancher struct {
	url       string
	accessKey string
	secretKey string
	stackId   string
	zookeeper string
}

func NewRancher(url string, accessKey string, secretKey string, stackId string, zookeeper string) *Rancher {
	return &Rancher{url, accessKey, secretKey, stackId, zookeeper}
}

func (r Rancher) CreateOperator(pipelineId string, input lib.Operator, pipeConfig lib.PipelineConfig) (err error) {
	config, _ := json.Marshal(lib.OperatorRequestConfig{Config: input.Config, InputTopics: input.InputTopics})
	env := map[string]string{
		"ZK_QUORUM":                         r.zookeeper,
		"CONFIG_APPLICATION_ID":             "analytics-" + input.ApplicationId.String(),
		"PIPELINE_ID":                       pipelineId,
		"OPERATOR_ID":                       input.Id,
		"WINDOW_TIME":                       strconv.Itoa(pipeConfig.WindowTime),
		"CONFIG":                            string(config),
		"DEVICE_ID_PATH":                    "device_id",
		"CONSUMER_AUTO_OFFSET_RESET_CONFIG": pipeConfig.ConsumerOffset,
		"USER_ID":                           pipeConfig.UserId,
	}

	if input.OutputTopic != "" {
		env["OUTPUT"] = input.OutputTopic
	}

	labels := map[string]string{
		"flow_id":                         pipeConfig.FlowId,
		"operator_id":                     input.Id,
		"service_type":                    "analytics-service",
		"io.rancher.container.pull_image": "always",
		"io.rancher.scheduler.affinity:host_label": "role=worker",
	}
	request := gorequest.New().SetBasicAuth(r.accessKey, r.secretKey)

	reqBody := &Request{
		Type:          "service",
		Name:          r.GetOperatorName(pipelineId, input)[0],
		StackId:       r.stackId,
		Scale:         1,
		StartOnCreate: true,
		LaunchConfig: LaunchConfig{
			ImageUuid:   "docker:" + input.ImageId,
			Environment: env,
			Labels:      labels,
		},
	}
	if pipeConfig.Metrics {
		env["METRICS_URL"] = pipeConfig.MetricsData.Url
		env["METRICS_USER"] = pipeConfig.MetricsData.Username
		env["METRICS_PASSWORD"] = pipeConfig.MetricsData.Password
		env["METRICS_INTERVAL"] = pipeConfig.MetricsData.Interval
		reqBody.LaunchConfig.Command = []string{
			"java",
			"-javaagent:/opt/jmxtrans-agent.jar=" + pipeConfig.MetricsData.XmlUrl,
			"-jar",
			"/opt/operator.jar",
		}

	}

	resp, body, e := request.Post(r.url + "services").Send(reqBody).End()
	if resp.StatusCode != http.StatusCreated {
		err = errors.New("could not create operator: " + body)
	}
	if len(e) > 0 {
		err = errors.New("could not create operator: an error occurred")
	}
	return
}

func (r Rancher) CreateOperators(pipelineId string, inputs []lib.Operator, pipeConfig lib.PipelineConfig) (err error) {
	var operators []LaunchConfig
	for _, input := range inputs {
		config, _ := json.Marshal(lib.OperatorRequestConfig{Config: input.Config, InputTopics: input.InputTopics})
		env := map[string]string{
			"ZK_QUORUM":                         r.zookeeper,
			"CONFIG_APPLICATION_ID":             "analytics-" + input.ApplicationId.String(),
			"PIPELINE_ID":                       pipelineId,
			"OPERATOR_ID":                       input.Id,
			"WINDOW_TIME":                       strconv.Itoa(pipeConfig.WindowTime),
			"CONFIG":                            string(config),
			"DEVICE_ID_PATH":                    "device_id",
			"CONSUMER_AUTO_OFFSET_RESET_CONFIG": pipeConfig.ConsumerOffset,
			"USER_ID":                           pipeConfig.UserId,
		}

		if input.OutputTopic != "" {
			env["OUTPUT"] = input.OutputTopic
		}

		labels := map[string]string{
			"flow_id":                         pipeConfig.FlowId,
			"operator_id":                     input.Id,
			"service_type":                    "analytics-service",
			"io.rancher.container.pull_image": "always",
			"io.rancher.scheduler.affinity:host_label": "role=worker",
		}

		launchConfig := LaunchConfig{
			Name:        input.Id,
			ImageUuid:   "docker:" + input.ImageId,
			Environment: env,
			Labels:      labels,
		}

		if pipeConfig.Metrics {
			env["METRICS_URL"] = pipeConfig.MetricsData.Url
			env["METRICS_USER"] = pipeConfig.MetricsData.Username
			env["METRICS_PASSWORD"] = pipeConfig.MetricsData.Password
			env["METRICS_INTERVAL"] = pipeConfig.MetricsData.Interval
			launchConfig.Command = []string{
				"java",
				"-javaagent:/opt/jmxtrans-agent.jar=" + pipeConfig.MetricsData.XmlUrl,
				"-jar",
				"/opt/operator.jar",
			}
		}
		operators = append(operators, launchConfig)
	}

	request := gorequest.New().SetBasicAuth(r.accessKey, r.secretKey)

	if len(operators) > 0 {
		reqBody := &Request{
			Type:                   "service",
			Name:                   r.GetOperatorName(pipelineId, lib.Operator{Id: "v3-123456789"})[1],
			StackId:                r.stackId,
			Scale:                  1,
			StartOnCreate:          true,
			LaunchConfig:           operators[0],
			SecondaryLaunchConfigs: operators[1:],
		}

		resp, body, e := request.Post(r.url + "services").Send(reqBody).End()
		if resp.StatusCode != http.StatusCreated {
			err = errors.New("rancher API - could not create operators " + body)
		}
		if len(e) > 0 {
			err = errors.New("rancher API - could not create operators - an error occurred")
		}
	}
	return
}

func (r Rancher) DeleteOperator(operatorName string) (err error) {
	service, err := r.getServiceByName(operatorName)
	if err != nil {
		return
	}
	request := gorequest.New().SetBasicAuth(r.accessKey, r.secretKey)
	resp, body, _ := request.Delete(r.url + "services/" + service.Id).End()
	if resp.StatusCode != http.StatusOK {
		err = errors.New("rancher API - could not delete operator  " + body)
	}
	return
}

func (r Rancher) GetOperatorName(pipelineId string, operator lib.Operator) []string {
	return []string{"v2-" + pipelineId + "-" + operator.Id[0:8], "v3-" + pipelineId}
}

func (r Rancher) getServiceByName(name string) (service Service, err error) {
	request := gorequest.New().SetBasicAuth(r.accessKey, r.secretKey)
	resp, body, errs := request.Get(r.url + "services/?name=" + name).End()
	if len(errs) > 0 || resp.StatusCode != http.StatusOK {
		err = errors.New("rancher API - could not access service name")
		return
	}
	var serviceCollection = ServiceCollection{}
	err = json.Unmarshal([]byte(body), &serviceCollection)
	if len(serviceCollection.Data) < 1 {
		err = errors.New("rancher API - could not find corresponding service")
		return
	}
	service = serviceCollection.Data[0]
	return
}
