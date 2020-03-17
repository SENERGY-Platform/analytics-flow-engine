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

package metrics_api

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/parnurzeal/gorequest"
	"net/http"
)

type MetricsApi struct {
	url string
}

func NewMetricsApi(url string) *MetricsApi {
	return &MetricsApi{url}
}

func (a MetricsApi) RegisterPipeline(id string) MetricsConfig {
	// Create influx db via metrics api
	metricApiRequest := gorequest.New().TLSClientConfig(&tls.Config{InsecureSkipVerify: true})
	metricsApiResp, metricsApiBody, e := metricApiRequest.Post(a.url + "/pipelines/" + id).End()
	if metricsApiResp.StatusCode != http.StatusCreated {
		fmt.Println("Problem contacting metrics api", metricsApiBody)
	}
	if len(e) > 0 {
		fmt.Println("Problem contacting metrics api", e)
	}
	var metricsConfig MetricsConfig
	err := json.Unmarshal([]byte(metricsApiBody), &metricsConfig)
	if err != nil {
		fmt.Println("Problem unmarshaling metrics api response", err)
	}

	return metricsConfig
}

func (a MetricsApi) UnregisterPipeline(id string) bool {
	metricApiRequest := gorequest.New().TLSClientConfig(&tls.Config{InsecureSkipVerify: true})
	metricsApiResp, metricsApiBody, e := metricApiRequest.Delete(a.url + "/pipelines/" + id).End()
	if metricsApiResp.StatusCode != http.StatusOK {
		fmt.Println("Problem contacting metrics api", metricsApiBody)
		return false
	}
	if len(e) > 0 {
		fmt.Println("Problem contacting metrics api", e)
		return false
	}
	return true
}
