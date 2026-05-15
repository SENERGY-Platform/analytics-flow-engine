/*
 * Copyright 2026 InfAI (CC SES)
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

package util

import (
	"errors"
	"net/http"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
)

func GetStatusCode(err error) int {
	if _, ok := errors.AsType[*lib.NotFoundError](err); ok {
		return http.StatusNotFound
	}
	if _, ok := errors.AsType[*lib.InputError](err); ok {
		return http.StatusBadRequest
	}
	if _, ok := errors.AsType[*lib.InternalError](err); ok {
		return http.StatusInternalServerError
	}
	if _, ok := errors.AsType[*lib.ForbiddenError](err); ok {
		return http.StatusForbidden
	}
	return 0
}
