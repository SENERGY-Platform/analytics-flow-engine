/*
 * Copyright 2025 InfAI (CC SES)
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

package deployment

import (
	"errors"

	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
)

// ErrVolumeStillTerminating means an operator volume of the same name was still being
// deleted when the time to wait for it ran out. Retrying at once cannot help.
var ErrVolumeStillTerminating = errors.New("volume still terminating")

// OperatorConfigTsConn is the Config key Operator Lib falls back to for its timescale
// DSN when the env var TS_CONN is unset. The platform never sets it and drops it
// from cloud operators, because the DSN reaches them from a Secret.
const OperatorConfigTsConn = "ts_conn"

type OperatorRequestConfig struct {
	Config      map[string]string `json:"config,omitempty"`
	InputTopics []pipe.InputTopic `json:"inputTopics,omitempty"`
}

type PipelineConfig struct {
	WindowTime     int
	MergeStrategy  string
	Metrics        bool
	ConsumerOffset string
	FlowId         string
	PipelineId     string
	UserId         string
	// Baggage is the caller's OpenTelemetry context, which the driver labels the
	// deployment with and hands to the containers.
	Baggage map[string]string
}
