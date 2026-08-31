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

// Package access authorizes the input topics of an operator against the
// platform's permissions, as the user on whose behalf it is being started.
//
// It exists because there are two ways an operator gets a deployment config and
// both must apply the same rule. The flow engine deploys one from a pipeline
// request; the Operator Development Environment launches one from an experiment.
// Operator Lib itself performs no check at all -- it reads whatever series its
// inputTopics name over a shared database credential -- so whichever service
// wrote those topics is the only party in a position to refuse. Having that check
// live here rather than in each service is what keeps the two from drifting.
//
// The unit of authorization is the input topic, not the storage behind it. A
// topic backed by timescale and a topic replayed from Kafka are authorized
// identically, which is the property that makes this cover the Kafka path too.
package access

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
)

// Resource names as permissions-v2 knows them.
const (
	ResourceDevices   = "devices"
	ResourcePipelines = "analytics-pipelines"
	ResourceImports   = "import-instances"
	ResourceOperators = "analytics-operators"
	ResourceFlows     = "analytics-flows"
)

// Filter types as they appear in a *deployed* config, which is what this package
// reads. They are capitalised here and lowercase in a pipeline request; the
// request spelling belongs to the flow engine's parser and deliberately does not
// appear in this package. Operator Lib matches these exact strings in
// gen_identifiers, so they are a wire contract rather than an internal choice.
const (
	FilterTypeDevice   = "DeviceId"
	FilterTypeOperator = "OperatorId"
	FilterTypeImport   = "ImportId"
)

// ErrDenied is returned when the platform refuses one of the checks. Callers
// wrap it in whatever their transport calls a refusal.
var ErrDenied = errors.New("access denied")

// ErrUncheckable is returned for a topic this package cannot authorize at all --
// an unknown filter type, or an operator reference it cannot resolve to a
// pipeline. It is deliberately distinct from ErrDenied: the user may well have
// the rights, and what is wrong is the request.
var ErrUncheckable = errors.New("topic cannot be authorized")

// Checker answers whether a user may execute a set of resources.
//
// An interface rather than a concrete client, and this module deliberately does
// not provide one: the permissions-v2 client pulls gin, a JWT library and some
// four hundred packages behind it, which every consumer of this module would
// then compile in order to read a struct. Each service passes the client it
// already has -- both wrap the same CheckMultiplePermissions call, so what is
// shared here is the part that can actually drift: which ids are extracted, which
// are exempt, and what happens when one cannot be resolved.
type Checker interface {
	UserHasExecuteAccess(resource string, ids []string, authorization string) (bool, error)
}

// Options narrows what CheckTopics considers external.
type Options struct {
	// InternalOperatorIDs are the operator ids of the deployment being checked.
	//
	// A parsed pipeline carries two kinds of operator input topic. One reads
	// another operator *of the same pipeline* -- the pipeline's own wiring, whose
	// FilterValue is a bare operator id because the pipeline it belongs to is
	// being created and has no id yet. The other reads an operator of an already
	// deployed pipeline, whose FilterValue is "operatorId:pipelineId".
	//
	// Only the second is a read of data the user must be entitled to. Without
	// this list the first kind is indistinguishable from a malformed reference to
	// the second, and refusing it would refuse every multi-operator pipeline.
	// An experiment with a single operator passes none.
	InternalOperatorIDs []string
}

// Refs are the ids an operator's inputs name, grouped by the resource that
// governs them.
type Refs struct {
	DeviceIDs   []string
	PipelineIDs []string
	ImportIDs   []string
}

// Empty reports whether there is nothing to check.
func (r Refs) Empty() bool {
	return len(r.DeviceIDs) == 0 && len(r.PipelineIDs) == 0 && len(r.ImportIDs) == 0
}

// FilterIDs extracts the ids the given input topics name.
//
// Returns ErrUncheckable for a topic whose filter type is unknown or whose
// operator reference names neither an internal operator nor a pipeline. Failing
// closed is the point: a topic that is skipped is a topic that is read
// unauthorized.
func FilterIDs(topics []pipe.InputTopic, opts Options) (Refs, error) {
	internal := make(map[string]bool, len(opts.InternalOperatorIDs))
	for _, id := range opts.InternalOperatorIDs {
		internal[strings.TrimSpace(id)] = true
	}

	var refs Refs
	for _, topic := range topics {
		// A single topic may name several ids. Operator Lib splits FilterValue on
		// commas when it builds its filters, so anything not split here is a read
		// that happens without having been checked.
		for _, raw := range strings.Split(topic.FilterValue, ",") {
			id := strings.TrimSpace(raw)
			if id == "" {
				continue
			}
			switch topic.FilterType {
			case FilterTypeDevice:
				refs.DeviceIDs = append(refs.DeviceIDs, id)
			case FilterTypeImport:
				refs.ImportIDs = append(refs.ImportIDs, id)
			case FilterTypeOperator:
				if internal[id] {
					// This pipeline's own wiring. Nothing to authorize: the data never
					// leaves the deployment being created.
					continue
				}
				operatorID, pipelineID, found := strings.Cut(id, ":")
				if !found || strings.TrimSpace(operatorID) == "" || strings.TrimSpace(pipelineID) == "" {
					return Refs{}, fmt.Errorf(
						"%w: operator input %q on topic %q names neither an operator of this "+
							"deployment nor an \"operatorId:pipelineId\" of a deployed one",
						ErrUncheckable, id, topic.Name)
				}
				refs.PipelineIDs = append(refs.PipelineIDs, strings.TrimSpace(pipelineID))
			default:
				return Refs{}, fmt.Errorf(
					"%w: input topic %q has filter type %q, which is none of %s, %s, %s",
					ErrUncheckable, topic.Name, topic.FilterType,
					FilterTypeDevice, FilterTypeOperator, FilterTypeImport)
			}
		}
	}

	refs.DeviceIDs = dedupe(refs.DeviceIDs)
	refs.PipelineIDs = dedupe(refs.PipelineIDs)
	refs.ImportIDs = dedupe(refs.ImportIDs)
	return refs, nil
}

// CheckTopics authorizes every input topic as the holder of the given token.
//
// One call per resource that has ids, none for a resource that has none. Returns
// ErrDenied naming the resource on the first refusal, ErrUncheckable for a topic
// that cannot be authorized, and the checker's own error unchanged when the
// platform could not be asked -- that last case must not be treated as a pass.
func CheckTopics(checker Checker, token string, topics []pipe.InputTopic, opts Options) error {
	if checker == nil {
		return errors.New("access: no permission checker configured, refusing rather than skipping the check")
	}
	refs, err := FilterIDs(topics, opts)
	if err != nil {
		return err
	}
	return CheckRefs(checker, token, refs)
}

// CheckRefs authorizes already-extracted ids. Separate from CheckTopics so a
// caller with ids from somewhere else -- a pipeline request, say -- reaches the
// same checks.
func CheckRefs(checker Checker, token string, refs Refs) error {
	for _, c := range []struct {
		resource string
		ids      []string
	}{
		{ResourceDevices, refs.DeviceIDs},
		{ResourcePipelines, refs.PipelineIDs},
		{ResourceImports, refs.ImportIDs},
	} {
		if err := Check(checker, token, c.resource, c.ids); err != nil {
			return err
		}
	}
	return nil
}

// Check authorizes one resource. An empty id list is not a question worth
// asking, so it makes no call.
func Check(checker Checker, token, resource string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	if checker == nil {
		return errors.New("access: no permission checker configured, refusing rather than skipping the check")
	}
	ok, err := checker.UserHasExecuteAccess(resource, ids, token)
	if err != nil {
		return fmt.Errorf("access: cannot check execute access on %s: %w", resource, err)
	}
	if !ok {
		return fmt.Errorf("%w: no execute access on %s: %s",
			ErrDenied, resource, strings.Join(ids, ", "))
	}
	return nil
}

func dedupe(ids []string) []string {
	if len(ids) < 2 {
		return ids
	}
	seen := make(map[string]bool, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
