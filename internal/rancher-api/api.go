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
	"analytics-flow-engine/internal/metrics-api"
	"encoding/json"
	"errors"
	"fmt"
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

func (r Rancher) CreateOperator(pipelineId string, input lib.Operator, outputTopic string, pipeConfig lib.PipelineConfig,
	useMetrics bool, metricsConfig metrics_api.MetricsConfig) string {
	env := map[string]string{
		"ZK_QUORUM":             r.zookeeper,
		"CONFIG_APPLICATION_ID": "analytics-" + pipelineId + "-" + input.Id,
		"PIPELINE_ID":           pipelineId,
		"OPERATOR_ID":           input.Id,
		"WINDOW_TIME":           strconv.Itoa(pipeConfig.WindowTime),
	}

	config, _ := json.Marshal(lib.OperatorRequestConfig{Config: input.Config, InputTopics: input.InputTopics})
	env["CONFIG"] = string(config)

	env["DEVICE_ID_PATH"] = "device_id"

	if outputTopic != "" {
		env["OUTPUT"] = outputTopic
	}

	labels := map[string]string{
		"flow_id":                         pipeConfig.FlowId,
		"operator_id":                     input.Id,
		"service_type":                    "analytics-service",
		"io.rancher.container.pull_image": "always",
		"io.rancher.scheduler.affinity:host_label": "role=worker",
	}
	request := gorequest.New().SetBasicAuth(r.accessKey, r.secretKey)

	reqBody := &Request{}
	if useMetrics {
		env["METRICS_URL"] = metricsConfig.Url
		env["METRICS_USER"] = metricsConfig.Username
		env["METRICS_PASSWORD"] = metricsConfig.Password
		env["METRICS_INTERVAL"] = metricsConfig.Interval

		reqBody = &Request{
			Type:          "service",
			Name:          r.GetOperatorName(pipelineId, input),
			StackId:       r.stackId,
			Scale:         1,
			StartOnCreate: true,
			LaunchConfig: LaunchConfig{
				ImageUuid:   "docker:" + input.ImageId,
				Environment: env,
				Labels:      labels,
				Command: []string{
					"java",
					"-javaagent:jmxtrans-agent.jar=" + metricsConfig.XmlUrl,
					"-jar",
					"/usr/src/app/target/operator-" + input.Name + "-jar-with-dependencies.jar",
				},
			},
		}
	} else {
		reqBody = &Request{
			Type:          "service",
			Name:          r.GetOperatorName(pipelineId, input),
			StackId:       r.stackId,
			Scale:         1,
			StartOnCreate: true,
			LaunchConfig: LaunchConfig{
				ImageUuid:   "docker:" + input.ImageId,
				Environment: env,
				Labels:      labels,
			},
		}
	}
	resp, body, e := request.Post(r.url + "services").Send(reqBody).End()
	if resp.StatusCode != http.StatusCreated {
		fmt.Println("Could not create Operator", body)
	}
	if len(e) > 0 {
		fmt.Println("Something went wrong", e)
	}
	data := lib.ToJson(body)
	return data["id"].(string)
}

func (r Rancher) DeleteOperator(operatorName string) (err error) {
	service, err := r.getServiceByName(operatorName)
	if err != nil {
		return
	}
	request := gorequest.New().SetBasicAuth(r.accessKey, r.secretKey)
	resp, body, _ := request.Delete(r.url + "services/" + service.Id).End()
	if resp.StatusCode != http.StatusOK {
		err = errors.New("could not delete operator: " + body)
	}
	return
}

func (r Rancher) GetOperatorName(pipelineId string, operator lib.Operator) string {
	return "v2-" + pipelineId + "-" + operator.Id[0:8]
}

func (r Rancher) getServiceByName(name string) (service Service, err error) {
	request := gorequest.New().SetBasicAuth(r.accessKey, r.secretKey)
	resp, body, errs := request.Get(r.url + "services/?name=" + name).End()
	if len(errs) > 0 || resp.StatusCode != http.StatusOK {
		err = errors.New("could not access service name")
		return
	}
	var serviceCollection = ServiceCollection{}
	err = json.Unmarshal([]byte(body), &serviceCollection)
	if len(serviceCollection.Data) < 1 {
		err = errors.New("could not find corresponding service")
		return
	}
	service = serviceCollection.Data[0]
	return
}
