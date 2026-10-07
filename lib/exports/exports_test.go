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

package exports

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib/access"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
)

const (
	importID = "urn:infai:ses:import:a50aa583-282e-56c2-b101-7aaf68ebd2b9"
	topicNm  = "urn_infai_ses_import_a50aa583-282e-56c2-b101-7aaf68ebd2b9"
	userID   = "ca4d1149-e3ed-4e0b-9e49-3bda908de436"
	exportA  = "e31775ba-4bf5-46a5-ba8f-81adb3977104"
	exportB  = "11111111-2222-4333-8444-555555555555"
)

type fakeLister struct {
	pages   [][]Export
	total   int64
	err     error
	calls   int
	offsets []int64
	limits  []int64
	tokens  []string
}

func (f *fakeLister) ListExports(_ context.Context, authorization string, limit, offset int64) ([]Export, int64, error) {
	f.calls++
	f.offsets = append(f.offsets, offset)
	f.limits = append(f.limits, limit)
	f.tokens = append(f.tokens, authorization)
	if f.err != nil {
		return nil, 0, f.err
	}
	idx := f.calls - 1
	if idx >= len(f.pages) {
		return nil, f.total, nil
	}
	return f.pages[idx], f.total, nil
}

type fakeChecker struct {
	allowed map[string]bool
	err     error
	calls   []checkCall
}

type checkCall struct {
	resource string
	ids      []string
	token    string
}

func (f *fakeChecker) UserHasExecuteAccess(resource string, ids []string, authorization string) (bool, error) {
	f.calls = append(f.calls, checkCall{resource, ids, authorization})
	if f.err != nil {
		return false, f.err
	}
	return f.allowed[ids[0]], nil
}

func importTopic() pipe.InputTopic {
	return pipe.InputTopic{
		Name: topicNm, FilterType: access.FilterTypeImport, FilterValue: importID,
		Mappings: []pipe.Mapping{
			{Source: "value.forecasted_for", Dest: "a"},
			{Source: "value.instant_air_temperature", Dest: "b"},
		},
	}
}

func goodExport(id string) Export {
	return Export{
		ID: id, Topic: topicNm, Filter: importID, FilterType: FilterTypeImportExport,
		Database:       userID,
		ExportDatabase: ExportDatabase{Type: DatabaseTypeTimescale, EwFilterTopic: "other-filters"},
		Values: []ExportValue{
			{Name: "forecasted_for", Path: "value.forecasted_for"},
			{Name: "instant_air_temperature", Path: "value.instant_air_temperature"},
			{Name: "extra", Path: "value.extra"},
		},
	}
}

func TestTableNameMatchesAnalyticsServing(t *testing.T) {
	got, err := TableName(userID, exportA)
	if err != nil {
		t.Fatalf("TableName: %v", err)
	}
	if want := "userid:yk0RSePtTgueSTvakI3kNg_export:4xd1ukv1RqW6j4Gts5dxBA"; got != want {
		t.Errorf("table = %q, want %q", got, want)
	}
}

func TestTableNameRefusesANonHexID(t *testing.T) {
	if _, err := TableName("not-hex", exportA); err == nil {
		t.Error("TableName accepted a database that is not hex")
	}
	if _, err := TableName(userID, ""); err == nil {
		t.Error("TableName accepted an empty export id")
	}
}

func TestNoImportTopicMeansNoListing(t *testing.T) {
	lister, checker := &fakeLister{}, &fakeChecker{}
	got, err := Resolve(context.Background(), lister, checker, "tok", []pipe.InputTopic{
		{Name: "d", FilterType: access.FilterTypeDevice, FilterValue: "dev"},
		{Name: "o", FilterType: access.FilterTypeOperator, FilterValue: "op:pipe"},
		{Name: "g", FilterType: access.FilterTypeImport, FilterValue: "imp-a,imp-b"},
		{Name: "e", FilterType: access.FilterTypeImport, FilterValue: " "},
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("entries = %v, want none", got)
	}
	if lister.calls != 0 || len(checker.calls) != 0 {
		t.Errorf("listed %d times, checked %d times, want neither", lister.calls, len(checker.calls))
	}
}

func TestOnePermittedCandidateIsResolved(t *testing.T) {
	lister := &fakeLister{pages: [][]Export{{goodExport(exportA)}}, total: 1}
	checker := &fakeChecker{allowed: map[string]bool{exportA: true}}
	got, err := Resolve(context.Background(), lister, checker, "tok", []pipe.InputTopic{
		{Name: "d", FilterType: access.FilterTypeDevice, FilterValue: "dev"},
		importTopic(),
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	want := []ImportExport{{
		Topic: topicNm, ImportID: importID, ExportID: exportA,
		Table: "userid:yk0RSePtTgueSTvakI3kNg_export:4xd1ukv1RqW6j4Gts5dxBA",
		Columns: map[string]string{
			"value.forecasted_for":          "forecasted_for",
			"value.instant_air_temperature": "instant_air_temperature",
		},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("entries = %+v, want %+v", got, want)
	}
	if lister.calls != 1 || lister.tokens[0] != "tok" || lister.limits[0] != 1000 {
		t.Errorf("lister calls = %d tokens = %v limits = %v", lister.calls, lister.tokens, lister.limits)
	}
	if len(checker.calls) != 1 {
		t.Fatalf("checks = %d, want 1", len(checker.calls))
	}
	c := checker.calls[0]
	if c.resource != ResourceExports || !reflect.DeepEqual(c.ids, []string{exportA}) || c.token != "tok" {
		t.Errorf("check = %+v", c)
	}
}

func TestCandidatesAreFilteredByEveryRule(t *testing.T) {
	cases := map[string]func(*Export){
		"wrong filter type":  func(e *Export) { e.FilterType = "device_id" },
		"wrong filter":       func(e *Export) { e.Filter = "urn:infai:ses:import:other" },
		"wrong topic":        func(e *Export) { e.Topic = "other-topic" },
		"influx database":    func(e *Export) { e.ExportDatabase.Type = "influxdb" },
		"sardine":            func(e *Export) { e.ExportDatabase.EwFilterTopic = IgnoredFilterTopic },
		"missing path":       func(e *Export) { e.Values = e.Values[:1] },
		"path without value": func(e *Export) { e.Values[1].Path = "instant_air_temperature" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			e := goodExport(exportA)
			e.Values = append([]ExportValue(nil), e.Values...)
			mutate(&e)
			lister := &fakeLister{pages: [][]Export{{e}}, total: 1}
			checker := &fakeChecker{allowed: map[string]bool{exportA: true}}
			got, err := Resolve(context.Background(), lister, checker, "tok", []pipe.InputTopic{importTopic()})
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if len(got) != 0 {
				t.Errorf("entries = %+v, want none", got)
			}
			if len(checker.calls) != 0 {
				t.Errorf("a filtered candidate was permission-checked: %v", checker.calls)
			}
		})
	}
}

func TestADeniedCandidateIsDroppedAndThePermittedOneWins(t *testing.T) {
	lister := &fakeLister{pages: [][]Export{{goodExport(exportA), goodExport(exportB)}}, total: 2}
	checker := &fakeChecker{allowed: map[string]bool{exportA: false, exportB: true}}
	got, err := Resolve(context.Background(), lister, checker, "tok", []pipe.InputTopic{importTopic()})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(got) != 1 || got[0].ExportID != exportB {
		t.Errorf("entries = %+v, want the permitted export %s", got, exportB)
	}
	if len(checker.calls) != 2 {
		t.Errorf("checks = %d, want one per candidate", len(checker.calls))
	}
}

func TestOnlyDeniedCandidatesMeanNoEntry(t *testing.T) {
	lister := &fakeLister{pages: [][]Export{{goodExport(exportA)}}, total: 1}
	checker := &fakeChecker{allowed: map[string]bool{}}
	got, err := Resolve(context.Background(), lister, checker, "tok", []pipe.InputTopic{importTopic()})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("entries = %+v, want none", got)
	}
}

func TestTwoPermittedCandidatesAreAmbiguous(t *testing.T) {
	lister := &fakeLister{pages: [][]Export{{goodExport(exportA), goodExport(exportB)}}, total: 2}
	checker := &fakeChecker{allowed: map[string]bool{exportA: true, exportB: true}}
	_, err := Resolve(context.Background(), lister, checker, "tok", []pipe.InputTopic{importTopic()})
	if !errors.Is(err, ErrAmbiguous) {
		t.Fatalf("error = %v, want ErrAmbiguous", err)
	}
	for _, want := range []string{topicNm, exportA, exportB} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}
}

func TestAListerErrorIsReturnedNotReadAsNoExport(t *testing.T) {
	boom := errors.New("connection refused")
	got, err := Resolve(context.Background(), &fakeLister{err: boom}, &fakeChecker{}, "tok", []pipe.InputTopic{importTopic()})
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want it to wrap %v", err, boom)
	}
	if got != nil {
		t.Errorf("entries = %v, want none alongside an error", got)
	}
}

func TestACheckerErrorIsReturnedNotReadAsDenied(t *testing.T) {
	boom := errors.New("permissions unreachable")
	lister := &fakeLister{pages: [][]Export{{goodExport(exportA)}}, total: 1}
	_, err := Resolve(context.Background(), lister, &fakeChecker{err: boom}, "tok", []pipe.InputTopic{importTopic()})
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want it to wrap %v", err, boom)
	}
}

func TestMissingCollaboratorsAreRefusedWhenNeeded(t *testing.T) {
	topics := []pipe.InputTopic{importTopic()}
	if _, err := Resolve(context.Background(), nil, &fakeChecker{}, "tok", topics); err == nil {
		t.Error("Resolve passed with no lister")
	}
	if _, err := Resolve(context.Background(), &fakeLister{}, nil, "tok", topics); err == nil {
		t.Error("Resolve passed with no checker")
	}
	// Nothing to resolve: no collaborators needed.
	if _, err := Resolve(context.Background(), nil, nil, "tok", nil); err != nil {
		t.Errorf("Resolve with no topics: %v", err)
	}
}

func TestEveryPageIsRead(t *testing.T) {
	other := goodExport("99999999-9999-4999-8999-999999999999")
	other.Filter = "urn:infai:ses:import:other"
	lister := &fakeLister{pages: [][]Export{{other}, {goodExport(exportA)}}, total: 2}
	checker := &fakeChecker{allowed: map[string]bool{exportA: true}}
	got, err := Resolve(context.Background(), lister, checker, "tok", []pipe.InputTopic{importTopic()})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(got) != 1 || got[0].ExportID != exportA {
		t.Errorf("entries = %+v, want the export on page two", got)
	}
	if want := []int64{0, 1}; !reflect.DeepEqual(lister.offsets, want) {
		t.Errorf("offsets = %v, want %v", lister.offsets, want)
	}
}

func TestAnExportOnTwoPagesIsNotItsOwnRival(t *testing.T) {
	lister := &fakeLister{pages: [][]Export{{goodExport(exportA)}, {goodExport(exportA)}}, total: 2}
	checker := &fakeChecker{allowed: map[string]bool{exportA: true}}
	got, err := Resolve(context.Background(), lister, checker, "tok", []pipe.InputTopic{importTopic()})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(got) != 1 || got[0].ExportID != exportA {
		t.Errorf("entries = %+v, want the one export once", got)
	}
}

func TestAnEmptyPageEndsPaging(t *testing.T) {
	// total claims more than the pages deliver; the loop must not spin.
	lister := &fakeLister{pages: [][]Export{{goodExport(exportA)}}, total: 50}
	checker := &fakeChecker{allowed: map[string]bool{exportA: true}}
	if _, err := Resolve(context.Background(), lister, checker, "tok", []pipe.InputTopic{importTopic()}); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if lister.calls != 2 {
		t.Errorf("list calls = %d, want 2", lister.calls)
	}
}

func TestEntriesFollowTopicOrder(t *testing.T) {
	second := importTopic()
	second.Name = "second-topic"
	second.FilterValue = "urn:infai:ses:import:second"
	e2 := goodExport(exportB)
	e2.Topic, e2.Filter = second.Name, second.FilterValue
	lister := &fakeLister{pages: [][]Export{{goodExport(exportA), e2}}, total: 2}
	checker := &fakeChecker{allowed: map[string]bool{exportA: true, exportB: true}}
	got, err := Resolve(context.Background(), lister, checker, "tok", []pipe.InputTopic{second, importTopic()})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(got) != 2 || got[0].Topic != "second-topic" || got[1].Topic != topicNm {
		t.Errorf("entries = %+v, want the input's topic order", got)
	}
	if lister.calls != 1 {
		t.Errorf("list calls = %d, want one listing for all topics", lister.calls)
	}
}

func TestEncodeRoundTrip(t *testing.T) {
	in := []ImportExport{{
		Topic: topicNm, ImportID: importID, ExportID: exportA, Table: "t",
		Columns: map[string]string{"value.x": "x"},
	}}
	s, err := Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var out []ImportExport
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		t.Fatalf("decode %q: %v", s, err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Errorf("round trip = %+v, want %+v", out, in)
	}
	for _, key := range []string{`"topic"`, `"import_id"`, `"export_id"`, `"table"`, `"columns"`} {
		if !strings.Contains(s, key) {
			t.Errorf("encoded %q lacks key %s", s, key)
		}
	}
}
