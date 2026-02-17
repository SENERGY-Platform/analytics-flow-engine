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

package lib

const (
	MessageSomethingWrong = "something went wrong"
	MessageNotFound       = "not found"
	MessageParseError     = "failed to parse request"
	MessageForbidden      = "forbidden"
	MessageBadInput       = "bad input"
)

type cError struct {
	err error
}

type InternalError struct {
	cError
}

type InputError struct {
	cError
}

type NotFoundError struct {
	cError
}

type ForbiddenError struct {
	cError
}

func (e *cError) Error() string {
	return e.err.Error()
}

func (e *cError) Unwrap() error {
	return e.err
}

func NewInternalError(err error) error {
	return &InternalError{cError{err: err}}
}

func NewInputError(err error) error {
	return &InputError{cError{err: err}}
}

func NewNotFoundError(err error) error {
	return &NotFoundError{cError{err: err}}
}

func NewForbiddenError(err error) error {
	return &ForbiddenError{cError{err: err}}
}
