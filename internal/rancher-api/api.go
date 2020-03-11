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
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/parnurzeal/gorequest"
)

type Rancher struct {
	url             string
	accessKey       string
	secretKey       string
	stackId         string
	zookeeper       string
	influxUrl       string
	influxUser      string
	influxPw        string
	metricsInterval string
}

func NewRancher(url string, accessKey string, secretKey string, stackId string, zookeeper string) *Rancher {
	return &Rancher{url, accessKey, secretKey, stackId, zookeeper,
		lib.GetEnv("METRICS_URL", "http://localhost:8086"),
		lib.GetEnv("METRICS_USER", ""),
		lib.GetEnv("METRICS_PASSWORD", ""),
		lib.GetEnv("METRICS_INTERVAL", "")}
}

func (r Rancher) CreateOperator(pipelineId string, input lib.Operator, outputTopic string, pipeConfig lib.PipelineConfig) string {
	env := map[string]string{
		"ZK_QUORUM":             r.zookeeper,
		"CONFIG_APPLICATION_ID": "analytics-" + pipelineId + "-" + input.Id,
		"PIPELINE_ID":           pipelineId,
		"OPERATOR_ID":           input.Id,
		"WINDOW_TIME":           strconv.Itoa(pipeConfig.WindowTime),
		"METRICS_URL":           r.influxUrl,
		"METRICS_USER":          r.influxUser,
		"METRICS_PASSWORD":      r.influxPw,
		"METRICS_INTERVAL":      r.metricsInterval,
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

	reqBody := &Request{
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
	resp, body, e := request.Post(r.url + "services").Send(reqBody).End()
	if resp.StatusCode != http.StatusCreated {
		fmt.Println("Could not create Operator", body)
	}
	if len(e) > 0 {
		fmt.Println("Something went wrong", e)
	}
	data := lib.ToJson(body)

	// Create influx db
	influxRequest := gorequest.New().SetBasicAuth(r.influxUser, r.influxPw).TLSClientConfig(&tls.Config{InsecureSkipVerify: true})
	influxResp, influxBody, e := influxRequest.Post(r.influxUrl + "/query?q=" + url.QueryEscape("CREATE DATABASE \""+input.Id+"\"")).End()
	if influxResp.StatusCode != http.StatusOK {
		fmt.Println("Could not create influx db", influxBody)
	}
	if len(e) > 0 {
		fmt.Println("Something went wrong", e)
	}

	return data["id"].(string)
}

func (r Rancher) DeleteOperator(operatorName string, operator lib.Operator) (err error) {
	service, err := r.getServiceByName(operatorName)
	if err != nil {
		return
	}
	request := gorequest.New().SetBasicAuth(r.accessKey, r.secretKey)
	resp, body, _ := request.Delete(r.url + "services/" + service.Id).End()
	if resp.StatusCode != http.StatusOK {
		err = errors.New("could not delete operator: " + body)
	}

	// Delete influx db
	influxRequest := gorequest.New().SetBasicAuth(r.influxUser, r.influxPw).TLSClientConfig(&tls.Config{InsecureSkipVerify: true})
	influxResp, influxBody, e := influxRequest.Post(r.influxUrl + "/query?q=" + url.QueryEscape("DROP DATABASE \""+operator.Id+"\"")).End()
	if influxResp.StatusCode != http.StatusOK {
		fmt.Println("Could not delete influx db", influxBody)
	}
	if len(e) > 0 {
		err = errors.New("something went wrong")
		return
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
