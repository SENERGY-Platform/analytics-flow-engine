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
	"errors"
	"github.com/parnurzeal/gorequest"
	"net/http"
	"strconv"
)

type MetricsApi struct {
	url string
}

func NewMetricsApi(url string) *MetricsApi {
	return &MetricsApi{url}
}

func (a MetricsApi) RegisterPipeline(id string) (metricsConfig MetricsConfig, err error) {
	// Create influx db via metrics api
	metricApiRequest := gorequest.New().TLSClientConfig(&tls.Config{InsecureSkipVerify: true})
	resp, body, e := metricApiRequest.Post(a.url + "/pipelines/" + id).End()
	if resp.StatusCode != http.StatusCreated {
		err = errors.New("metrics API - could not create metrics service: " + strconv.Itoa(resp.StatusCode) + " " + body)
		return
	}
	if len(e) > 0 {
		err = errors.New("metrics API - could not create metrics service: an error occurred")
		return
	}
	err = json.Unmarshal([]byte(body), &metricsConfig)
	if err != nil {
		err = errors.New("metrics API - could not unmarshal metrics api response: " + err.Error())
		return
	}

	return
}

func (a MetricsApi) UnregisterPipeline(id string) (err error) {
	metricApiRequest := gorequest.New().TLSClientConfig(&tls.Config{InsecureSkipVerify: true})
	resp, body, e := metricApiRequest.Delete(a.url + "/pipelines/" + id).End()
	if resp.StatusCode != http.StatusOK {
		err = errors.New("metrics API - could not delete metrics from metrics service: " + strconv.Itoa(resp.StatusCode) + " " + body)
		return
	}
	if len(e) > 0 {
		err = errors.New("metrics API - could not delete metrics from metrics service: an error occurred")
		return
	}
	return
}
