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
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/httpreq"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
	"github.com/google/uuid"
)

type PipelineResponse struct {
	Id uuid.UUID `json:"id,omitempty"`
}

type PipelineApi struct {
	url string
}

func NewPipelineApi(url string) *PipelineApi {
	return &PipelineApi{url}
}

func (p *PipelineApi) RegisterPipeline(ctx context.Context, pipeline *pipe.Pipeline, userId string, authorization string) (id uuid.UUID, err error) {
	response, err := p.do(ctx, http.MethodPost, "/pipeline", pipeline, userId, authorization)
	if err != nil {
		return id, fmt.Errorf("pipeline API - could not register pipeline at pipeline registry: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return id, errors.New("pipeline API - could not register pipeline at pipeline registry: " +
			strconv.Itoa(response.StatusCode) + " " + response.Text())
	}
	var res PipelineResponse
	if err = response.Decode(&res); err != nil {
		return id, errors.New("pipeline API - could not parse pipeline response: " + err.Error())
	}
	return res.Id, nil
}

func (p *PipelineApi) UpdatePipeline(ctx context.Context, pipeline *pipe.Pipeline, userId string, authorization string) (err error) {
	response, err := p.do(ctx, http.MethodPut, "/pipeline", pipeline, userId, authorization)
	if err != nil {
		return fmt.Errorf("pipeline API - could not update pipeline at pipeline registry: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return errors.New("pipeline API - could not update pipeline at pipeline registry: " +
			strconv.Itoa(response.StatusCode) + " " + response.Text())
	}
	return nil
}

func (p *PipelineApi) GetPipeline(ctx context.Context, id string, userId string, authorization string) (result pipe.Pipeline, err error) {
	response, err := p.do(ctx, http.MethodGet, "/pipeline/"+id, nil, userId, authorization)
	if err != nil {
		return result, fmt.Errorf("pipeline API - could not get pipeline from pipeline registry: %w", err)
	}
	switch response.StatusCode {
	case http.StatusNotFound:
		return result, lib.NewNotFoundError(fmt.Errorf("could not find pipeline %s", id))
	case http.StatusForbidden:
		return result, lib.NewForbiddenError(lib.NewNotFoundError(fmt.Errorf("could not access pipeline %s", id)))
	case http.StatusOK:
	default:
		return result, errors.New("pipeline API - could not get pipeline from pipeline registry: " +
			strconv.Itoa(response.StatusCode) + " " + response.Text())
	}
	if err = response.Decode(&result); err != nil {
		return result, errors.New("pipeline API  - could not parse pipeline: " + err.Error())
	}
	return result, nil
}

func (p *PipelineApi) GetPipelines(ctx context.Context, userId string, authorization string) (pipelines []pipe.Pipeline, err error) {
	response, err := p.do(ctx, http.MethodGet, "/pipeline", nil, userId, authorization)
	if err != nil {
		return nil, fmt.Errorf("pipeline API - could not get pipelines from pipeline registry: %w", err)
	}
	return decodePipelines(response, "pipelines")
}

// GetPipelinesAdmin reads every pipeline, for the startup sync. It runs outside any
// request, hence the admin headers rather than a caller's token; ctx carries no
// baggage there and is only what makes the call cancellable.
func (p *PipelineApi) GetPipelinesAdmin(ctx context.Context) (pipelines []pipe.Pipeline, err error) {
	response, err := httpreq.Do(ctx, httpreq.Request{
		Method: http.MethodGet,
		URL:    p.url + "/admin/pipeline",
		Headers: map[string]string{
			"X-UserId":     "admin",
			"X-User-Roles": "admin",
		},
	})
	if err != nil {
		return nil, fmt.Errorf("pipeline API - could not get admin pipelines from pipeline registry: %w", err)
	}
	return decodePipelines(response, "admin pipelines")
}

func (p *PipelineApi) DeletePipeline(ctx context.Context, id string, userId string, authorization string) (err error) {
	response, err := p.do(ctx, http.MethodDelete, "/pipeline/"+id, nil, userId, authorization)
	if err != nil {
		return fmt.Errorf("pipeline API - could not delete pipeline from pipeline registry: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return errors.New("pipeline API - could not delete pipeline from pipeline registry: " +
			strconv.Itoa(response.StatusCode) + " " + response.Text())
	}
	return nil
}

func (p *PipelineApi) do(ctx context.Context, method, path string, body any, userId, authorization string) (httpreq.Response, error) {
	return httpreq.Do(ctx, httpreq.Request{
		Method: method,
		URL:    p.url + path,
		Body:   body,
		Headers: map[string]string{
			"X-UserId":      userId,
			"Authorization": authorization,
		},
	})
}

func decodePipelines(response httpreq.Response, what string) ([]pipe.Pipeline, error) {
	switch response.StatusCode {
	case http.StatusNotFound:
		return nil, lib.NewNotFoundError(fmt.Errorf("could not find %s", what))
	case http.StatusForbidden:
		return nil, lib.NewForbiddenError(lib.NewNotFoundError(fmt.Errorf("could not access %s", what)))
	case http.StatusOK:
	default:
		return nil, errors.New("pipeline API - could not get " + what + " from pipeline registry: " +
			strconv.Itoa(response.StatusCode) + " " + response.Text())
	}
	var pResponse lib.PipelinesResponse
	if err := response.Decode(&pResponse); err != nil {
		return nil, errors.New("pipeline API  - could not parse " + what + ": " + err.Error())
	}
	return pResponse.Data, nil
}
