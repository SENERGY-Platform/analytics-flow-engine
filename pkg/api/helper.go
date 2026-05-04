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

package api

import (
	"errors"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
)

func handleError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.As(err, new(*lib.NotFoundError)):
		return lib.NewNotFoundError(errors.New(MessageNotFound))
	case errors.As(err, new(*lib.ForbiddenError)):
		return lib.NewForbiddenError(errors.New(MessageForbidden))
	default:
		return lib.NewInternalError(errors.New(MessageSomethingWrong))
	}
}
