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
	var nfe *lib.NotFoundError
	if errors.As(err, &nfe) {
		return http.StatusNotFound
	}
	var pe *lib.InputError
	if errors.As(err, &pe) {
		return http.StatusBadRequest
	}
	var ie *lib.InternalError
	if errors.As(err, &ie) {
		return http.StatusInternalServerError
	}
	var fe *lib.ForbiddenError
	if errors.As(err, &fe) {
		return http.StatusForbidden
	}
	return 0
}
