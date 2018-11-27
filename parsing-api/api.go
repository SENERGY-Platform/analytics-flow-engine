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
	"github.com/parnurzeal/gorequest"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
)

type ParsingApi struct {
	url string
}

func NewParsingApi(url string) *ParsingApi {
	return &ParsingApi{url}
}

func (a ParsingApi) GetPipeline(id string, userId string) (p Pipeline, err error) {
	request := gorequest.New()
	_ , body, e := request.Get(a.url + "pipe/" + id ).Set("X-UserId", userId).End()
	//if resp.StatusCode != http.StatusOK{
	//	fmt.Println("Something went wrong", e)
	//	err  = errors.New("Could not get pipeline from service")
	//	return
	//}
	if len(e) > 0 {
		fmt.Println("Something went wrong", e)
		err  = errors.New("Could not get pipeline from service")
		return
	}
	err = json.Unmarshal([]byte(body), &p)
	return
}