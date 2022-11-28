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

package lib

import (
	"encoding/json"
	"github.com/google/uuid"
	"net/http"

	"strconv"

	"github.com/parnurzeal/gorequest"
	"github.com/pkg/errors"
)

type PipelineResponse struct {
	Id uuid.UUID `json:"id,omitempty"`
}

func registerPipeline(pipeline *Pipeline, userId string, authorization string) (id uuid.UUID, err error) {
	var pipelineServiceUrl = GetEnv("PIPELINE_API_ENDPOINT", "")
	request := gorequest.New()
	request.Post(pipelineServiceUrl+"/pipeline").Set("X-UserId", userId).Set("Authorization", authorization).Send(pipeline)
	resp, body, e := request.End()
	if resp.StatusCode != http.StatusOK {
		err = errors.New("pipeline API - could not register pipeline at pipeline registry: " + strconv.Itoa(resp.StatusCode) + " " + body)
		return
	}
	if len(e) > 0 {
		err = errors.New("pipeline API - could not register pipeline at pipeline registry: an error occurred")
		return
	}
	var res PipelineResponse
	if err = json.Unmarshal([]byte(body), &res); err != nil {
		err = errors.New("pipeline API - could not parse pipeline response: " + err.Error())
		return
	}
	id = res.Id
	return
}

func updatePipeline(pipeline *Pipeline, userId string, authorization string) (err error) {
	var pipelineServiceUrl = GetEnv("PIPELINE_API_ENDPOINT", "")
	request := gorequest.New()
	request.Put(pipelineServiceUrl+"/pipeline").Set("X-UserId", userId).Set("Authorization", authorization).Send(pipeline)
	resp, body, e := request.End()
	if resp.StatusCode != http.StatusOK {
		err = errors.New("pipeline API - could not register pipeline at pipeline registry: " + strconv.Itoa(resp.StatusCode) + " " + body)
	}
	if len(e) > 0 {
		err = errors.New("pipeline API - could not register pipeline at pipeline registry: an error occurred")
	}
	return
}

var ErrNotFound = errors.New("not found")

func getPipeline(id string, userId string, authorization string) (pipe Pipeline, err error) {
	var pipelineServiceUrl = GetEnv("PIPELINE_API_ENDPOINT", "")
	request := gorequest.New()
	request.Get(pipelineServiceUrl+"/pipeline/"+id).Set("X-UserId", userId).Set("Authorization", authorization)
	resp, body, e := request.End()
	if len(e) > 0 {
		return pipe, errors.New("pipeline API - could not get pipeline from pipeline registry: an error occurred")
	}
	if resp.StatusCode == http.StatusNotFound {
		return pipe, ErrNotFound
	}
	if resp.StatusCode != 200 {
		return pipe, errors.New("pipeline API - could not get pipeline from pipeline registry: " + strconv.Itoa(resp.StatusCode) + " " + body)
	}
	err = json.Unmarshal([]byte(body), &pipe)
	if err != nil {
		err = errors.New("pipeline API  - could not parse pipeline: " + err.Error())
		return
	}
	return
}

func deletePipeline(id string, userId string, authorization string) (err error) {
	var pipelineServiceUrl = GetEnv("PIPELINE_API_ENDPOINT", "")
	request := gorequest.New()
	request.Delete(pipelineServiceUrl+"/pipeline/"+id).Set("X-UserId", userId).Set("Authorization", authorization)
	resp, body, e := request.End()
	if resp.StatusCode != 200 {
		err = errors.New("pipeline API - could not delete pipeline from pipeline registry: " + strconv.Itoa(resp.StatusCode) + " " + body)
	}
	if len(e) > 0 {
		err = errors.New("pipeline API - could not delete pipeline from pipeline registry: an error occurred")
		return
	}
	return
}
