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
	"fmt"

	"strconv"

	"github.com/parnurzeal/gorequest"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
)

type PipelineResponse struct {
	Id uuid.UUID `json:"id,omitempty"`
}

func registerPipeline(pipeline *Pipeline, userId string) (id uuid.UUID, err error) {
	var pipelineServiceUrl = GetEnv("PIPELINE_API_ENDPOINT", "")
	request := gorequest.New()
	_, body, e := request.Post(pipelineServiceUrl+"/pipeline").Set("X-UserId", userId).Send(pipeline).End()
	//if resp.StatusCode != http.StatusOK{
	//	fmt.Println("Something went wrong", e)
	//	err  = errors.New("Could not get pipeline from service")
	//	return
	//}
	if len(e) > 0 {
		fmt.Println("Something went wrong", e)
		err = errors.New("Could not get pipeline from service")
		return
	}
	var res PipelineResponse
	if err := json.Unmarshal([]byte(body), &res); err != nil {
		panic(err)
	}
	id = res.Id
	return
}

func deletePipeline(id string, userId string) (err error) {
	var pipelineServiceUrl = GetEnv("PIPELINE_API_ENDPOINT", "")
	request := gorequest.New()
	resp, _, e := request.Delete(pipelineServiceUrl+"/pipeline/"+id).Set("X-UserId", userId).End()
	if resp.StatusCode != 200 {
		fmt.Println("Could not access pipeline registry: "+strconv.Itoa(resp.StatusCode), resp.Body)
		err = errors.New("Could not access pipeline registry")
	}
	if len(e) > 0 {
		fmt.Println("Something went wrong", e)
		err = errors.New("Could not get pipeline from service")
		return
	}
	return
}
