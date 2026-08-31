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
	"context"
	"net/http"
	"strconv"

	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/httpreq"
	parser "github.com/SENERGY-Platform/analytics-parser/lib"
	"github.com/pkg/errors"
)

type ParsingApi struct {
	url string
}

func NewParsingApi(url string) *ParsingApi {
	return &ParsingApi{url}
}

func (a ParsingApi) GetPipeline(ctx context.Context, id string, userId string, authorization string) (p parser.Pipeline, err error) {
	response, err := httpreq.Do(ctx, httpreq.Request{
		Method: http.MethodGet,
		URL:    a.url + "/flow/" + id,
		Headers: map[string]string{
			"X-UserId":      userId,
			"Authorization": authorization,
		},
	})
	if err != nil {
		return p, errors.Wrap(err, "parser API - could not get pipeline from parsing service")
	}
	if response.StatusCode != http.StatusOK {
		return p, errors.New("parser API - could not get pipeline from parsing service: " +
			strconv.Itoa(response.StatusCode) + " " + response.Text())
	}
	err = response.Decode(&p)
	return
}
