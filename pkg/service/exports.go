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

package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	"github.com/SENERGY-Platform/analytics-flow-engine/lib/exports"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"

	deploymentLocationLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/location"
)

// setImportExports names, in every cloud operator, the export the history of each
// of its import inputs is read from. See package exports for the rule.
//
// Like ts_conn, the value is the platform's: it names a table the operator reads
// over a shared database credential, so a user's node config must not be able to
// carry it. A key already in the flow's config is therefore replaced when an export
// resolves and removed when none does, and removed as well when no analytics-serving
// endpoint is configured.
//
// Fog operators get no export, as in setPlatformOperatorConfig: they have no ts_conn
// to read the table through. A user-supplied key is still removed there, because
// Operator Lib does read it: an import input with an entry and no reader configured
// fails naming ts_conn instead of falling back to Kafka.
//
// Errors refuse the deployment, never fall back to Kafka. An ambiguous import is the
// requester's to resolve (an input error); a failing listing or permission service
// is not a statement about the user, so it is an internal error, not a denial.
func (f *FlowEngine) setImportExports(ctx context.Context, operators []pipe.Operator, token string) error {
	for i := range operators {
		if operators[i].DeploymentType == deploymentLocationLib.Local {
			delete(operators[i].Config, exports.ConfigKey)
			continue
		}
		if f.exportLister == nil {
			delete(operators[i].Config, exports.ConfigKey)
			continue
		}
		entries, err := exports.Resolve(ctx, f.exportLister, f.checker(ctx), token, operators[i].InputTopics)
		if err != nil {
			err = fmt.Errorf("operator %s: %w", operators[i].Id, err)
			if errors.Is(err, exports.ErrAmbiguous) {
				return lib.NewInputError(err)
			}
			return lib.NewInternalError(err)
		}
		if len(entries) == 0 {
			delete(operators[i].Config, exports.ConfigKey)
			continue
		}
		encoded, err := exports.Encode(entries)
		if err != nil {
			return lib.NewInternalError(err)
		}
		if operators[i].Config == nil {
			operators[i].Config = map[string]string{}
		}
		operators[i].Config[exports.ConfigKey] = encoded
	}
	return nil
}
