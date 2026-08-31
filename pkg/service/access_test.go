/*
 * Copyright 2026 InfAI (CC SES)
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
	"strings"
	"testing"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib/access"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
)

type recordingPermissions struct {
	asked  map[string][]string
	denied map[string]bool
}

func newRecordingPermissions(denied ...string) *recordingPermissions {
	r := &recordingPermissions{asked: map[string][]string{}, denied: map[string]bool{}}
	for _, d := range denied {
		r.denied[d] = true
	}
	return r
}

func (r *recordingPermissions) UserHasExecuteAccess(_ context.Context, resource string, ids []string, _ string) (bool, error) {
	r.asked[resource] = append(r.asked[resource], ids...)
	return !r.denied[resource], nil
}

func operatorWith(id string, topics ...pipe.InputTopic) pipe.Operator {
	return pipe.Operator{Id: id, InputTopics: topics}
}

// A pipeline whose second operator reads the first must not need permission on a
// pipeline that does not exist yet. This is the case that breaks if the wiring
// topics are treated as references to a deployed pipeline.
func TestATwoOperatorPipelineNeedsNoPipelinePermission(t *testing.T) {
	perms := newRecordingPermissions()
	f := &FlowEngine{permissionService: perms}

	err := f.checkTopicAccess(context.Background(), []pipe.Operator{
		operatorWith("op-1", pipe.InputTopic{
			Name: "urn_infai_ses_service_a", FilterType: access.FilterTypeDevice, FilterValue: "dev-a",
		}),
		operatorWith("op-2", pipe.InputTopic{
			Name: "analytics-op-1", FilterType: access.FilterTypeOperator, FilterValue: "op-1",
		}),
	}, "tok")
	if err != nil {
		t.Fatalf("checkTopicAccess: %v", err)
	}
	if got := perms.asked[access.ResourcePipelines]; len(got) != 0 {
		t.Errorf("asked for pipeline permission on %v, want none for a pipeline's own wiring", got)
	}
	if got := perms.asked[access.ResourceDevices]; len(got) != 1 || got[0] != "dev-a" {
		t.Errorf("device ids = %v, want [dev-a]", got)
	}
}

// The topics an operator ends up with are assembled after the request is parsed,
// so a device that only appears there still has to be authorized.
func TestEveryDeviceOfEveryOperatorIsChecked(t *testing.T) {
	perms := newRecordingPermissions()
	f := &FlowEngine{permissionService: perms}

	err := f.checkTopicAccess(context.Background(), []pipe.Operator{
		operatorWith("op-1", pipe.InputTopic{
			Name: "t1", FilterType: access.FilterTypeDevice, FilterValue: "dev-a,dev-b",
		}),
		operatorWith("op-2", pipe.InputTopic{
			Name: "t2", FilterType: access.FilterTypeImport, FilterValue: "imp-a",
		}),
	}, "tok")
	if err != nil {
		t.Fatalf("checkTopicAccess: %v", err)
	}
	if got := perms.asked[access.ResourceDevices]; len(got) != 2 {
		t.Errorf("device ids = %v, want both members of the group", got)
	}
	if got := perms.asked[access.ResourceImports]; len(got) != 1 {
		t.Errorf("import ids = %v, want one", got)
	}
}

func TestADeniedDeviceRefusesTheDeployment(t *testing.T) {
	perms := newRecordingPermissions(access.ResourceDevices)
	f := &FlowEngine{permissionService: perms}

	err := f.checkTopicAccess(context.Background(), []pipe.Operator{
		operatorWith("op-1", pipe.InputTopic{
			Name: "t1", FilterType: access.FilterTypeDevice, FilterValue: "dev-a",
		}),
	}, "tok")
	if !errors.Is(err, access.ErrDenied) {
		t.Fatalf("error = %v, want ErrDenied", err)
	}
}

// An operator input naming a deployed pipeline is checked against that pipeline.
func TestAnExternalOperatorInputIsCheckedAgainstItsPipeline(t *testing.T) {
	perms := newRecordingPermissions()
	f := &FlowEngine{permissionService: perms}

	err := f.checkTopicAccess(context.Background(), []pipe.Operator{
		operatorWith("op-1", pipe.InputTopic{
			Name: "t1", FilterType: access.FilterTypeOperator, FilterValue: "other-op:pipe-9",
		}),
	}, "tok")
	if err != nil {
		t.Fatalf("checkTopicAccess: %v", err)
	}
	if got := perms.asked[access.ResourcePipelines]; len(got) != 1 || got[0] != "pipe-9" {
		t.Errorf("pipeline ids = %v, want [pipe-9]", got)
	}
}

func TestACloudOperatorGetsTheConfiguredDSN(t *testing.T) {
	f := &FlowEngine{timescaleConnection: "postgresql://ops@timescale/db"}
	operators := []pipe.Operator{{Id: "op-1", DeploymentType: "cloud"}}

	if err := f.setPlatformOperatorConfig(operators); err != nil {
		t.Fatalf("setPlatformOperatorConfig: %v", err)
	}
	if got := operators[0].Config[OperatorConfigTsConn]; got != "postgresql://ops@timescale/db" {
		t.Errorf("ts_conn = %q, want the configured DSN", got)
	}
}

// The flow's own node config is the user's. A ts_conn out of it would point an
// operator at a database the platform never chose, so the platform value wins.
func TestTheFlowsOwnTsConnIsOverwritten(t *testing.T) {
	f := &FlowEngine{timescaleConnection: "postgresql://ops@timescale/db"}
	operators := []pipe.Operator{{
		Id:             "op-1",
		DeploymentType: "cloud",
		Config:         map[string]string{"ts_conn": "postgresql://attacker@elsewhere/db", "window": "5"},
	}}

	if err := f.setPlatformOperatorConfig(operators); err != nil {
		t.Fatalf("setPlatformOperatorConfig: %v", err)
	}
	if got := operators[0].Config[OperatorConfigTsConn]; got != "postgresql://ops@timescale/db" {
		t.Errorf("ts_conn = %q, want the platform's", got)
	}
	if got := operators[0].Config["window"]; got != "5" {
		t.Errorf("window = %q, want the flow's own values left alone", got)
	}
}

// A fog operator runs on hardware the platform does not own. Shipping a platform
// database credential there is worse than the problem this whole change fixes.
func TestAFogOperatorGetsNoDSN(t *testing.T) {
	f := &FlowEngine{timescaleConnection: "postgresql://ops@timescale/db"}
	operators := []pipe.Operator{{Id: "op-1", DeploymentType: "local"}}

	if err := f.setPlatformOperatorConfig(operators); err != nil {
		t.Fatalf("setPlatformOperatorConfig: %v", err)
	}
	if _, present := operators[0].Config[OperatorConfigTsConn]; present {
		t.Error("a fog operator was given the platform DSN")
	}
}

func TestACloudPipelineWithoutAConfiguredDSNIsRefused(t *testing.T) {
	f := &FlowEngine{timescaleConnection: ""}
	operators := []pipe.Operator{{Id: "op-1", DeploymentType: "cloud"}}

	err := f.setPlatformOperatorConfig(operators)
	if err == nil {
		t.Fatal("deployment allowed with no timescale_connection configured")
	}
	if !strings.Contains(err.Error(), "timescale_connection") {
		t.Errorf("error = %q, want it to name the setting", err)
	}
}

// A fog-only pipeline needs no DSN, so it must not be blocked by one being unset.
func TestAFogOnlyPipelineNeedsNoDSN(t *testing.T) {
	f := &FlowEngine{timescaleConnection: ""}
	operators := []pipe.Operator{{Id: "op-1", DeploymentType: "local"}}

	if err := f.setPlatformOperatorConfig(operators); err != nil {
		t.Errorf("setPlatformOperatorConfig: %v, want a fog-only pipeline to deploy", err)
	}
}
