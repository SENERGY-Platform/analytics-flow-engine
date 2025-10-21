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

package parsing_api

import (
	"encoding/json"
	"net/http"
	"strconv"

	parser "github.com/SENERGY-Platform/analytics-parser/lib"
	"github.com/parnurzeal/gorequest"
	"github.com/pkg/errors"
)

type ParsingApi struct {
	url string
}

func NewParsingApi(url string) *ParsingApi {
	return &ParsingApi{url}
}

func (a ParsingApi) GetPipeline(id string, userId string, authorization string) (p parser.Pipeline, err error) {
	request := gorequest.New()
	request.Get(a.url+"/flow/"+id).Set("X-UserId", userId).Set("Authorization", authorization)
	resp, body, e := request.End()
	if resp.StatusCode != http.StatusOK {
		err = errors.New("parser API - could not get pipeline from parsing service: " + strconv.Itoa(resp.StatusCode) + " " + body)
		return
	}
	if len(e) > 0 {
		err = errors.New("parser API - could not get pipeline from parsing service: an error occurred")
		return
	}
	err = json.Unmarshal([]byte(body), &p)
	return
}
