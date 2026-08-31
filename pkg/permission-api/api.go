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
	"github.com/SENERGY-Platform/permissions-v2/pkg/client"
)

type PermissionApi struct {
	url string
	c   client.Client
}

func NewPermissionApi(url string) *PermissionApi {
	return &PermissionApi{url: url, c: client.New(url)}
}

// UserHasExecuteAccess reports whether the token holder may execute every one of
// the given ids. All of them, not any: the caller is about to read all of them,
// so a partial answer is a denial.
func (a PermissionApi) UserHasExecuteAccess(resource string, ids []string, authorization string) (result bool, err error) {
	response, err, _ := a.c.CheckMultiplePermissions(authorization, resource, ids, client.Execute)
	if err != nil {
		return false, err
	}
	// Ranged over the ids rather than over the response: an id the service did not
	// answer for is simply absent from the map, and ranging over what came back
	// would take silence for a yes.
	for _, id := range ids {
		if !response[id] {
			return false, nil
		}
	}
	return true, nil
}
