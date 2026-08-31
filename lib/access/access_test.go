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

package access

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
)

// fakeChecker records what it was asked and answers from a fixed verdict.
type fakeChecker struct {
	calls  []call
	denied map[string]bool // resource -> denied
	err    error
}

type call struct {
	resource string
	ids      []string
	token    string
}

func (f *fakeChecker) UserHasExecuteAccess(resource string, ids []string, authorization string) (bool, error) {
	f.calls = append(f.calls, call{resource: resource, ids: ids, token: authorization})
	if f.err != nil {
		return false, f.err
	}
	return !f.denied[resource], nil
}

func device(name, value string) pipe.InputTopic {
	return pipe.InputTopic{Name: name, FilterType: FilterTypeDevice, FilterValue: value}
}

func TestACommaSeparatedFilterValueYieldsEveryID(t *testing.T) {
	// A device group input arrives as one topic naming several devices. Operator
	// Lib splits it and subscribes to all of them, so all of them must be checked.
	refs, err := FilterIDs([]pipe.InputTopic{device("t", "dev-a,dev-b , dev-c")}, Options{})
	if err != nil {
		t.Fatalf("FilterIDs: %v", err)
	}
	if want := []string{"dev-a", "dev-b", "dev-c"}; !reflect.DeepEqual(refs.DeviceIDs, want) {
		t.Errorf("device ids = %v, want %v", refs.DeviceIDs, want)
	}
}

func TestAnOperatorInputContributesItsPipeline(t *testing.T) {
	refs, err := FilterIDs([]pipe.InputTopic{
		{Name: "t", FilterType: FilterTypeOperator, FilterValue: "op-1:pipe-9"},
	}, Options{})
	if err != nil {
		t.Fatalf("FilterIDs: %v", err)
	}
	if want := []string{"pipe-9"}; !reflect.DeepEqual(refs.PipelineIDs, want) {
		t.Errorf("pipeline ids = %v, want %v", refs.PipelineIDs, want)
	}
	if len(refs.DeviceIDs) != 0 {
		t.Errorf("device ids = %v, want none", refs.DeviceIDs)
	}
}

func TestThePipelinesOwnWiringIsNotAuthorized(t *testing.T) {
	// A multi-operator pipeline wires operator to operator by bare operator id --
	// the pipeline being created has no id yet. Those topics read nothing the user
	// must be entitled to, and refusing them would refuse every such pipeline.
	refs, err := FilterIDs([]pipe.InputTopic{
		{Name: "internal", FilterType: FilterTypeOperator, FilterValue: "op-2"},
		device("external", "dev-a"),
	}, Options{InternalOperatorIDs: []string{"op-1", "op-2"}})
	if err != nil {
		t.Fatalf("FilterIDs: %v", err)
	}
	if len(refs.PipelineIDs) != 0 {
		t.Errorf("pipeline ids = %v, want none for internal wiring", refs.PipelineIDs)
	}
	if want := []string{"dev-a"}; !reflect.DeepEqual(refs.DeviceIDs, want) {
		t.Errorf("device ids = %v, want %v", refs.DeviceIDs, want)
	}
}

func TestAnUnresolvableOperatorInputIsRefused(t *testing.T) {
	// A bare operator id that is not this deployment's own cannot be resolved to a
	// pipeline, so it cannot be authorized. Skipping it -- which is what the flow
	// engine used to do with a log line -- reads it unchecked.
	_, err := FilterIDs([]pipe.InputTopic{
		{Name: "t", FilterType: FilterTypeOperator, FilterValue: "op-stranger"},
	}, Options{InternalOperatorIDs: []string{"op-1"}})
	if !errors.Is(err, ErrUncheckable) {
		t.Fatalf("error = %v, want ErrUncheckable", err)
	}
}

func TestAnUnknownFilterTypeIsRefused(t *testing.T) {
	for _, filterType := range []string{"", "deviceId", "Device", "nonsense"} {
		_, err := FilterIDs([]pipe.InputTopic{
			{Name: "t", FilterType: filterType, FilterValue: "x"},
		}, Options{})
		if !errors.Is(err, ErrUncheckable) {
			t.Errorf("filter type %q: error = %v, want ErrUncheckable", filterType, err)
		}
	}
}

func TestAResourceWithNoIDsIsNotAsked(t *testing.T) {
	checker := &fakeChecker{}
	err := CheckTopics(checker, "tok", []pipe.InputTopic{device("t", "dev-a")}, Options{})
	if err != nil {
		t.Fatalf("CheckTopics: %v", err)
	}
	if len(checker.calls) != 1 {
		t.Fatalf("calls = %d, want 1: only devices were named", len(checker.calls))
	}
	if checker.calls[0].resource != ResourceDevices {
		t.Errorf("resource = %q, want %q", checker.calls[0].resource, ResourceDevices)
	}
	if checker.calls[0].token != "tok" {
		t.Errorf("token = %q, want the caller's", checker.calls[0].token)
	}
}

func TestEachResourceIsCheckedWithItsOwnIDs(t *testing.T) {
	checker := &fakeChecker{}
	err := CheckTopics(checker, "tok", []pipe.InputTopic{
		device("d", "dev-a"),
		{Name: "i", FilterType: FilterTypeImport, FilterValue: "imp-a"},
		{Name: "o", FilterType: FilterTypeOperator, FilterValue: "op-1:pipe-9"},
	}, Options{})
	if err != nil {
		t.Fatalf("CheckTopics: %v", err)
	}
	got := map[string][]string{}
	for _, c := range checker.calls {
		got[c.resource] = c.ids
	}
	want := map[string][]string{
		ResourceDevices:   {"dev-a"},
		ResourceImports:   {"imp-a"},
		ResourcePipelines: {"pipe-9"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("calls = %v, want %v", got, want)
	}
}

func TestADenialNamesTheResource(t *testing.T) {
	checker := &fakeChecker{denied: map[string]bool{ResourceDevices: true}}
	err := CheckTopics(checker, "tok", []pipe.InputTopic{device("t", "dev-a")}, Options{})
	if !errors.Is(err, ErrDenied) {
		t.Fatalf("error = %v, want ErrDenied", err)
	}
	if !strings.Contains(err.Error(), ResourceDevices) || !strings.Contains(err.Error(), "dev-a") {
		t.Errorf("error = %q, want it to name the resource and the id", err)
	}
}

func TestAnUnreachablePlatformIsNotAPass(t *testing.T) {
	boom := errors.New("connection refused")
	checker := &fakeChecker{err: boom}
	err := CheckTopics(checker, "tok", []pipe.InputTopic{device("t", "dev-a")}, Options{})
	if err == nil {
		t.Fatal("CheckTopics passed while the platform could not be asked")
	}
	if !errors.Is(err, boom) {
		t.Errorf("error = %v, want it to wrap %v", err, boom)
	}
	if errors.Is(err, ErrDenied) {
		t.Error("an unreachable platform must not read as a denial: the remedies differ")
	}
}

func TestNoCheckerIsRefusedRatherThanSkipped(t *testing.T) {
	err := CheckTopics(nil, "tok", []pipe.InputTopic{device("t", "dev-a")}, Options{})
	if err == nil {
		t.Fatal("CheckTopics passed with no checker configured")
	}
}

func TestAnEmptyFilterValueNamesNothing(t *testing.T) {
	// An operator with no external inputs is legitimate; it must not produce an
	// empty-string id that the platform would then be asked about.
	refs, err := FilterIDs([]pipe.InputTopic{device("t", ""), device("t2", " , ")}, Options{})
	if err != nil {
		t.Fatalf("FilterIDs: %v", err)
	}
	if !refs.Empty() {
		t.Errorf("refs = %+v, want empty", refs)
	}
}
