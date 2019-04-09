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

package rancher2

import (
	"analytics-flow-engine/lib"
	"fmt"
	"net/http"

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

func (r *Rancher2) GetEnvData() map[string]interface{} {
	return nil
}

func (r *Rancher2) CreateOperator(pipelineId string, operator lib.Operator, outputTopic string, flowId string) string {
	fmt.Println("Rancher2 Create " + pipelineId)
	env := map[string]string{
		"ZK_QUORUM":             r.zookeeper,
		"CONFIG_APPLICATION_ID": "analytics-" + pipelineId + "-" + operator.Id,
		"PIPELINE_ID":           pipelineId,
		"OPERATOR_ID":           operator.Id,
		"WINDOW_TIME":           "120",
	}
	config, _ := json.Marshal(lib.OperatorRequestConfig{Config: operator.Config, InputTopics: operator.InputTopics})
	env["CONFIG"] = string(config)

	env["DEVICE_ID_PATH"] = "device_id"

	if outputTopic != "" {
		env["OUTPUT"] = outputTopic
	}
	request := gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: true})
	reqBody := &Request{
		Name:        r.GetOperatorName(pipelineId, operator),
		NamespaceId: lib.GetEnv("RANCHER2_NAMESPACE_ID", ""),
		Containers: []Container{{
			Image:       operator.ImageId,
			Name:        r.GetOperatorName(pipelineId, operator),
			Environment: env,
		}},
		Labels:   map[string]string{"op": operator.Id, "flowId": flowId, "pipeId": pipelineId},
		Selector: Selector{MatchLabels: map[string]string{"op": operator.Id}},
	}
	resp, body, e := request.Post(r.url + "projects/" + lib.GetEnv("RANCHER2_PROJECT_ID", "") + "/workloads").Send(reqBody).End()
	if resp.StatusCode != http.StatusCreated {
		fmt.Println("Could not create Operator", body)
	}
	if len(e) > 0 {
		fmt.Println("Something went wrong", e)
	}
	return ""
}

func (r *Rancher2) DeleteOperator(operatorId string) map[string]interface{} {
	request := gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: true})
	resp, body, e := request.Delete(r.url + "projects/" + lib.GetEnv("RANCHER2_PROJECT_ID", "") + "/workloads/deployment:" +
		lib.GetEnv("RANCHER2_NAMESPACE_ID", "") + ":" + operatorId).End()
	if resp.StatusCode != http.StatusNoContent {
		fmt.Println("Could not delete Operator", body)
	}
	if len(e) > 0 {
		fmt.Println("Something went wrong", e)
	}
	return lib.ToJson(body)
}

func (r *Rancher2) GetAnalyticsPipelineStatus(pipelineId string) string {
	return ""
}

func (r *Rancher2) DeleteAnalyticsPipeline(pipelineId string) {
	fmt.Println(r.DeleteOperator(""))
}

func (r *Rancher2) GetOperatorName(pipelineId string, operator lib.Operator) string {
	return "operator-" + operator.Id
}
