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

package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
)

func StringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

// permanentError ends retry at once with the error it wraps: further attempts could
// only repeat the wait that produced it.
type permanentError struct{ err error }

func (p permanentError) Error() string { return p.err.Error() }
func (p permanentError) Unwrap() error { return p.err }

func retry(ctx context.Context, attempts int, sleep time.Duration, f func() error) (err error) {
	for i := 0; ; i++ {
		err = f()
		if err == nil {
			return
		}
		var permanent permanentError
		if errors.As(err, &permanent) {
			return permanent.err
		}

		if i >= (attempts - 1) {
			break
		}

		time.Sleep(sleep)

		util.Logger.DebugContext(ctx, "retrying after error", "error", err, "attempt", i+1, "of", attempts)
	}
	return fmt.Errorf("after %d attempts, last error: %s", attempts, err)
}

// diffPipelineIds compares the pipeline ids of the registry with those found in the
// cluster. missing are registered but absent from the cluster, orphaned are in the
// cluster but not registered; both keep input order and name each id once.
func diffPipelineIds(registered, inCluster []string) (missing, orphaned []string) {
	return setDifference(registered, inCluster), setDifference(inCluster, registered)
}

// setDifference is the ids of a, in order and without repeats, that b does not hold.
func setDifference(a, b []string) []string {
	exclude := make(map[string]struct{}, len(b)+len(a))
	for _, id := range b {
		exclude[id] = struct{}{}
	}
	var diff []string
	for _, id := range a {
		if _, found := exclude[id]; found {
			continue
		}
		exclude[id] = struct{}{}
		diff = append(diff, id)
	}
	return diff
}

// redactPipeline returns a copy of pipeline that is safe to log: ts_conn is the
// shared timescale DSN and carries its password.
func redactPipeline(pipeline pipe.Pipeline) pipe.Pipeline {
	pipeline.Operators = redactOperators(pipeline.Operators)
	return pipeline
}

func redactOperators(operators []pipe.Operator) []pipe.Operator {
	redacted := make([]pipe.Operator, len(operators))
	for i, operator := range operators {
		redacted[i] = redactOperator(operator)
	}
	return redacted
}

// redactOperator copies the config before masking, because the map is shared with
// the operator the engine goes on to deploy.
func redactOperator(operator pipe.Operator) pipe.Operator {
	if _, ok := operator.Config[OperatorConfigTsConn]; !ok {
		return operator
	}
	config := make(map[string]string, len(operator.Config))
	for key, value := range operator.Config {
		config[key] = value
	}
	config[OperatorConfigTsConn] = "***"
	operator.Config = config
	return operator
}
