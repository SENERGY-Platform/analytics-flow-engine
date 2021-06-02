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

package permission_api

import (
	"encoding/json"
	"errors"
	"github.com/parnurzeal/gorequest"
	"net/http"
	"strconv"
)

type PermissionApi struct {
	url string
}

func NewPermissionApi(url string) *PermissionApi {
	return &PermissionApi{url}
}

func (a PermissionApi) UserHasDevicesReadAccess(ids []string, authorization string) (result bool, err error) {
	result = false
	var response map[string]bool
	request := gorequest.New()

	request.Post(a.url+"/query").Set("Authorization", authorization).Send(Check{
		Resource: "devices",
		CheckIds: CheckIdsObject{
			Ids:    ids,
			Rights: "r",
		},
	})
	resp, body, e := request.End()
	if resp.StatusCode != http.StatusOK {
		err = errors.New("permission API - could not check access rights: " + strconv.Itoa(resp.StatusCode) + " " + body)
		return
	}
	if len(e) > 0 {
		err = errors.New("permission API - could not check access rights: an error occurred")
		return
	}
	err = json.Unmarshal([]byte(body), &response)
	if len(response) > 0 {
		for _, access := range response {
			if !access {
				return result, nil
			}
		}
		result = true
	}
	return result, nil
}
