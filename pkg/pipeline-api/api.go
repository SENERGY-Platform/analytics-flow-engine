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

package pipeline_api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
	"github.com/google/uuid"
	"github.com/parnurzeal/gorequest"
)

type PipelineResponse struct {
	Id uuid.UUID `json:"id,omitempty"`
}

var ErrNotFound = errors.New("not found")

type PipelineApi struct {
	url string
}

func NewPipelineApi(url string) *PipelineApi {
	return &PipelineApi{url}
}

func (p *PipelineApi) RegisterPipeline(pipeline *pipe.Pipeline, userId string, authorization string) (id uuid.UUID, err error) {
	request := gorequest.New()
	request.Post(p.url+"/pipeline").Set("X-UserId", userId).Set("Authorization", authorization).Send(pipeline)
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

func (p *PipelineApi) UpdatePipeline(pipeline *pipe.Pipeline, userId string, authorization string) (err error) {
	request := gorequest.New()
	request.Put(p.url+"/pipeline").Set("X-UserId", userId).Set("Authorization", authorization).Send(pipeline)
	resp, body, e := request.End()
	if resp.StatusCode != http.StatusOK {
		err = errors.New("pipeline API - could not register pipeline at pipeline registry: " + strconv.Itoa(resp.StatusCode) + " " + body)
	}
	if len(e) > 0 {
		err = errors.New("pipeline API - could not register pipeline at pipeline registry: an error occurred")
	}
	return
}

func (p *PipelineApi) GetPipeline(id string, userId string, authorization string) (pipe pipe.Pipeline, err error) {
	request := gorequest.New()
	request.Get(p.url+"/pipeline/"+id).Set("X-UserId", userId).Set("Authorization", authorization)
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

func (p *PipelineApi) GetPipelines(userId string, authorization string) (pipelines []pipe.Pipeline, err error) {
	request := gorequest.New()
	request.Get(p.url+"/pipeline").Set("X-UserId", userId).Set("Authorization", authorization)
	resp, body, e := request.End()
	if len(e) > 0 {
		err = errors.New("pipeline API - could not get pipelines from pipeline registry: an error occurred")
		return
	}
	if resp.StatusCode == http.StatusNotFound {
		err = ErrNotFound
		return
	}
	if resp.StatusCode != 200 {
		err = errors.New("pipeline API - could not get pipelines from pipeline registry: " + strconv.Itoa(resp.StatusCode) + " " + body)
		return
	}
	var pResponse lib.PipelinesResponse
	err = json.Unmarshal([]byte(body), &pResponse)
	if err != nil {
		err = errors.New("pipeline API  - could not parse pipelines: " + err.Error())
		return
	}
	pipelines = pResponse.Data
	return
}

func (p *PipelineApi) DeletePipeline(id string, userId string, authorization string) (err error) {
	request := gorequest.New()
	request.Delete(p.url+"/pipeline/"+id).Set("X-UserId", userId).Set("Authorization", authorization)
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
