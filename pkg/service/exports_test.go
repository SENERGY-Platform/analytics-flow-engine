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
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	"github.com/SENERGY-Platform/analytics-flow-engine/lib/access"
	"github.com/SENERGY-Platform/analytics-flow-engine/lib/exports"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
)

type fakeLister struct {
	exports []exports.Export
	err     error
	calls   int
	auth    string
}

func (l *fakeLister) ListExports(_ context.Context, authorization string, _, _ int64) ([]exports.Export, int64, error) {
	l.calls++
	l.auth = authorization
	return l.exports, int64(len(l.exports)), l.err
}

// failingPermissions answers every check with an error.
type failingPermissions struct{}

func (failingPermissions) UserHasExecuteAccess(context.Context, string, []string, string) (bool, error) {
	return false, errors.New("permissions down")
}

const (
	testImport  = "urn:infai:ses:import:a50aa583-282e-56c2-b101-7aaf68ebd2b9"
	testTopic   = "urn_infai_ses_import_a50aa583-282e-56c2-b101-7aaf68ebd2b9"
	testExport  = "e31775ba-4bf5-46a5-ba8f-81adb3977104"
	testUser    = "ca4d1149-e3ed-4e0b-9e49-3bda908de436"
	testTable   = "userid:yk0RSePtTgueSTvakI3kNg_export:4xd1ukv1RqW6j4Gts5dxBA"
	testSource  = "value.temp"
	userSuppled = `[{"topic":"evil","table":"userid:x_export:y"}]`
)

func timescaleExport(id string) exports.Export {
	return exports.Export{
		ID: id, Topic: testTopic, Filter: testImport, FilterType: exports.FilterTypeImportExport,
		Database:       testUser,
		ExportDatabase: exports.ExportDatabase{Type: exports.DatabaseTypeTimescale},
		Values:         []exports.ExportValue{{Name: "temp", Path: testSource}},
	}
}

func importOperator(deployment string, config map[string]string) pipe.Operator {
	return pipe.Operator{
		Id: "op-1", DeploymentType: deployment, Config: config,
		InputTopics: []pipe.InputTopic{{
			Name: testTopic, FilterType: access.FilterTypeImport, FilterValue: testImport,
			Mappings: []pipe.Mapping{{Source: testSource, Dest: "x"}},
		}},
	}
}

func TestAResolvedExportLandsInTheConfig(t *testing.T) {
	lister := &fakeLister{exports: []exports.Export{timescaleExport(testExport)}}
	f := &FlowEngine{exportLister: lister, permissionService: allowAll{}}
	operators := []pipe.Operator{importOperator("cloud", map[string]string{"window": "5"})}

	if err := f.setImportExports(context.Background(), operators, "Bearer tok"); err != nil {
		t.Fatalf("setImportExports: %v", err)
	}
	if lister.auth != "Bearer tok" {
		t.Errorf("listing token = %q, want the request token", lister.auth)
	}
	var entries []exports.ImportExport
	if err := json.Unmarshal([]byte(operators[0].Config[exports.ConfigKey]), &entries); err != nil {
		t.Fatalf("config value is not the encoded list: %v", err)
	}
	if len(entries) != 1 || entries[0].ExportID != testExport || entries[0].Table != testTable ||
		entries[0].Topic != testTopic || entries[0].ImportID != testImport ||
		entries[0].Columns[testSource] != "temp" {
		t.Errorf("entries = %+v", entries)
	}
	if operators[0].Config["window"] != "5" {
		t.Error("the flow's own config values were not left alone")
	}
}

func TestAFogOperatorIsSkipped(t *testing.T) {
	lister := &fakeLister{exports: []exports.Export{timescaleExport(testExport)}}
	f := &FlowEngine{exportLister: lister, permissionService: allowAll{}}
	operators := []pipe.Operator{
		importOperator("local", nil),
		importOperator("local", map[string]string{exports.ConfigKey: userSuppled}),
	}

	if err := f.setImportExports(context.Background(), operators, "tok"); err != nil {
		t.Fatalf("setImportExports: %v", err)
	}
	if _, present := operators[0].Config[exports.ConfigKey]; present {
		t.Error("a fog operator was given an import export")
	}
	if _, present := operators[1].Config[exports.ConfigKey]; present {
		t.Error("a user-supplied import_exports survived on a fog operator, where Operator Lib would fail on it")
	}
	if lister.calls != 0 {
		t.Errorf("the listing was read %d times for a fog-only pipeline", lister.calls)
	}
}

func TestAUserSuppliedImportExportsIsOverwritten(t *testing.T) {
	lister := &fakeLister{exports: []exports.Export{timescaleExport(testExport)}}
	f := &FlowEngine{exportLister: lister, permissionService: allowAll{}}
	operators := []pipe.Operator{importOperator("cloud", map[string]string{exports.ConfigKey: userSuppled})}

	if err := f.setImportExports(context.Background(), operators, "tok"); err != nil {
		t.Fatalf("setImportExports: %v", err)
	}
	got := operators[0].Config[exports.ConfigKey]
	if got == userSuppled || strings.Contains(got, "evil") {
		t.Errorf("the user's value survived: %s", got)
	}
}

func TestAUserSuppliedImportExportsIsRemovedWhenNothingResolves(t *testing.T) {
	lister := &fakeLister{} // no exports at all
	f := &FlowEngine{exportLister: lister, permissionService: allowAll{}}
	operators := []pipe.Operator{importOperator("cloud", map[string]string{exports.ConfigKey: userSuppled, "window": "5"})}

	if err := f.setImportExports(context.Background(), operators, "tok"); err != nil {
		t.Fatalf("setImportExports: %v", err)
	}
	if _, present := operators[0].Config[exports.ConfigKey]; present {
		t.Error("a user-supplied import_exports stayed with no export resolved")
	}
	if operators[0].Config["window"] != "5" {
		t.Error("the flow's own config values were not left alone")
	}
}

func TestAnExportTheUserMayNotExecuteIsNotUsed(t *testing.T) {
	lister := &fakeLister{exports: []exports.Export{timescaleExport(testExport)}}
	perms := newRecordingPermissions(exports.ResourceExports)
	f := &FlowEngine{exportLister: lister, permissionService: perms}
	operators := []pipe.Operator{importOperator("cloud", nil)}

	if err := f.setImportExports(context.Background(), operators, "tok"); err != nil {
		t.Fatalf("setImportExports: %v", err)
	}
	if _, present := operators[0].Config[exports.ConfigKey]; present {
		t.Error("an export without execute permission was named")
	}
	if got := perms.asked[exports.ResourceExports]; len(got) != 1 || got[0] != testExport {
		t.Errorf("asked about %v, want [%s]", got, testExport)
	}
}

func TestAmbiguousExportsRefuseTheDeployment(t *testing.T) {
	lister := &fakeLister{exports: []exports.Export{timescaleExport(testExport), timescaleExport("11111111-2222-3333-4444-555555555555")}}
	f := &FlowEngine{exportLister: lister, permissionService: allowAll{}}
	operators := []pipe.Operator{importOperator("cloud", nil)}

	err := f.setImportExports(context.Background(), operators, "tok")
	if !errors.Is(err, exports.ErrAmbiguous) {
		t.Fatalf("error = %v, want ErrAmbiguous", err)
	}
	var inputErr *lib.InputError
	if !errors.As(err, &inputErr) {
		t.Errorf("error %T, want an input error", err)
	}
}

func TestAListingErrorRefusesTheDeployment(t *testing.T) {
	lister := &fakeLister{err: errors.New("serving down")}
	f := &FlowEngine{exportLister: lister, permissionService: allowAll{}}
	operators := []pipe.Operator{importOperator("cloud", nil)}

	err := f.setImportExports(context.Background(), operators, "tok")
	var internal *lib.InternalError
	if !errors.As(err, &internal) {
		t.Fatalf("error = %v (%T), want an internal error, not a fallback to Kafka", err, err)
	}
	if _, present := operators[0].Config[exports.ConfigKey]; present {
		t.Error("a config was written despite the failure")
	}
}

func TestAPermissionErrorRefusesTheDeployment(t *testing.T) {
	lister := &fakeLister{exports: []exports.Export{timescaleExport(testExport)}}
	f := &FlowEngine{exportLister: lister, permissionService: failingPermissions{}}
	operators := []pipe.Operator{importOperator("cloud", nil)}

	err := f.setImportExports(context.Background(), operators, "tok")
	var internal *lib.InternalError
	if !errors.As(err, &internal) {
		t.Fatalf("error = %v (%T), want an internal error", err, err)
	}
}

func TestWithoutAListerOperatorsReadKafkaAndTheUserKeyIsStripped(t *testing.T) {
	f := &FlowEngine{permissionService: allowAll{}}
	operators := []pipe.Operator{
		importOperator("cloud", map[string]string{exports.ConfigKey: userSuppled, "window": "5"}),
		importOperator("cloud", nil),
	}

	if err := f.setImportExports(context.Background(), operators, "tok"); err != nil {
		t.Fatalf("setImportExports: %v", err)
	}
	for i, op := range operators {
		if _, present := op.Config[exports.ConfigKey]; present {
			t.Errorf("operator %d still has import_exports", i)
		}
	}
	if operators[0].Config["window"] != "5" {
		t.Error("the flow's own config values were not left alone")
	}
}

func TestADevicePipelineNeverReadsTheListing(t *testing.T) {
	lister := &fakeLister{err: errors.New("must not be called")}
	f := &FlowEngine{exportLister: lister, permissionService: allowAll{}}
	operators := []pipe.Operator{{Id: "op-1", DeploymentType: "cloud", InputTopics: []pipe.InputTopic{
		{Name: "t", FilterType: access.FilterTypeDevice, FilterValue: "dev-a"},
	}}}

	if err := f.setImportExports(context.Background(), operators, "tok"); err != nil {
		t.Fatalf("setImportExports: %v", err)
	}
	if lister.calls != 0 {
		t.Errorf("listing read %d times for a device-only operator", lister.calls)
	}
}
