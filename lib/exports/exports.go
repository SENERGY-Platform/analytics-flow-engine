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

// Package exports resolves, for the import inputs of an operator, the
// analytics-serving export that holds the import's history in timescale.
//
// It exists because an import topic in Kafka keeps only days, while an export
// writes the same messages to timescale without a retention limit. Operator Lib
// reads whatever its config names and checks nothing, and a deployed operator has
// no user token, so the deployer is the only party that can look the export up and
// ask whether the user may use it. Both deployers -- the flow engine and the
// Operator Development Environment -- must resolve identically, so the rule lives
// here once, next to the access check it depends on.
//
// Resolution fails closed. A listing or permission error is returned, never read
// as "this import has no export": the caller refuses the deployment instead of
// silently falling back to the short Kafka history the user did not ask for. Only a
// definite "no permitted export" yields no entry, and then Operator Lib reads
// Kafka exactly as before.
//
// Like package access, this one defines interfaces instead of clients. A
// permissions client or an analytics-serving HTTP client would be compiled into
// every consumer of this module to read a struct; each service passes the one it
// already has.
package exports

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib/access"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
)

const (
	// ConfigKey is the operator config key under which Encode's result is set.
	ConfigKey = "import_exports"
	// ResourceExports is the permissions-v2 resource of an analytics-serving export.
	ResourceExports = "export-instances"
	// FilterTypeImportExport is analytics-serving's FilterType of an export fed by
	// an import. Lower snake case, where the deployed config says "ImportId"; the
	// two services do not share a vocabulary.
	FilterTypeImportExport = "import_id"
	// DatabaseTypeTimescale is the export database type whose tables
	// timescale-wrapper serves.
	DatabaseTypeTimescale = "timescaledb"
	// IgnoredFilterTopic is the export worker's filter topic that is never used
	// as a history source, by decision of the platform's users.
	IgnoredFilterTopic = "kafka-to-timescaledb-sardine-ew-filters_internal"
)

// pageSize is the listing page size. A user may own more exports than one page;
// stopping at the first would drop the matching export without any error.
const pageSize int64 = 1000

// ErrAmbiguous is returned when more than one export the user may execute fits
// one import input. Picking one would be a guess about which history the operator
// trains on, so the deployment is refused instead.
var ErrAmbiguous = errors.New("more than one export matches the import input")

// Export is one analytics-serving instance in the fields resolution uses.
//
// Declared here because the upstream model is a gorm entity in an internal
// package. It carries no JSON tags, so the tags below are the Go field names it
// happens to marshal; a rename upstream would show up as no export being found.
type Export struct {
	ID               string         `json:"ID"`
	Name             string         `json:"Name"`
	Topic            string         `json:"Topic"`
	Filter           string         `json:"Filter"`
	FilterType       string         `json:"FilterType"`
	TimePath         string         `json:"TimePath"`
	Database         string         `json:"Database"`
	UserId           string         `json:"UserId"`
	ExportDatabaseID string         `json:"ExportDatabaseID"`
	ExportDatabase   ExportDatabase `json:"ExportDatabase"`
	Values           []ExportValue  `json:"Values"`
	CreatedAt        time.Time      `json:"CreatedAt"`
}

// ExportDatabase is the destination an export is written to.
type ExportDatabase struct {
	ID            string `json:"ID"`
	Name          string `json:"Name"`
	Type          string `json:"Type"`
	EwFilterTopic string `json:"EwFilterTopic"`
}

// ExportValue is one column of an export: Path is where the value sits in the
// message, Name is the timescale column it lands in.
type ExportValue struct {
	Name string `json:"Name"`
	Type string `json:"Type"`
	Path string `json:"Path"`
	Tag  bool   `json:"Tag"`
}

// Lister pages through the exports the user may read.
//
// analytics-serving cannot filter by import, so resolution reads the listing and
// filters here. total is the number of exports behind the listing, not the page.
type Lister interface {
	ListExports(ctx context.Context, authorization string, limit, offset int64) ([]Export, int64, error)
}

// ImportExport is one entry of the operator config value: the export to read the
// history of one import input from.
type ImportExport struct {
	// Topic is the input topic's name.
	Topic string `json:"topic"`
	// ImportID is the input topic's filter value.
	ImportID string `json:"import_id"`
	ExportID string `json:"export_id"`
	// Table is the timescale table of the export, see TableName.
	Table string `json:"table"`
	// Columns maps each mapping source of the topic, as the mapping carries it, to
	// the export's timescale column.
	Columns map[string]string `json:"columns"`
}

// Resolve finds the export for every import input that has exactly one the user
// may execute. The result follows the order of topics; inputs without an export
// have no entry.
//
// The listing is read once for all topics, and only if at least one topic is an
// import input naming a single import. Device topics, operator topics and
// comma-separated import groups are left to the Kafka path: a group has no single
// export to name.
func Resolve(ctx context.Context, lister Lister, checker access.Checker, authorization string, topics []pipe.InputTopic) ([]ImportExport, error) {
	var wanted []pipe.InputTopic
	for _, topic := range topics {
		if topic.FilterType != access.FilterTypeImport {
			continue
		}
		id := strings.TrimSpace(topic.FilterValue)
		if id == "" || strings.Contains(id, ",") {
			continue
		}
		wanted = append(wanted, topic)
	}
	if len(wanted) == 0 {
		return nil, nil
	}
	if lister == nil {
		return nil, errors.New("exports: no export lister configured, refusing rather than skipping the lookup")
	}
	if checker == nil {
		return nil, errors.New("exports: no permission checker configured, refusing rather than skipping the check")
	}

	all, err := listAll(ctx, lister, authorization)
	if err != nil {
		return nil, err
	}

	// The checker is all-or-nothing over its ids, so a candidate is asked about on
	// its own. The verdict is kept because two topics may share a candidate.
	verdicts := map[string]bool{}
	var out []ImportExport
	for _, topic := range wanted {
		importID := strings.TrimSpace(topic.FilterValue)
		var permitted []Export
		for _, candidate := range all {
			if !fits(candidate, topic, importID) {
				continue
			}
			ok, known := verdicts[candidate.ID]
			if !known {
				var err error
				ok, err = checker.UserHasExecuteAccess(ResourceExports, []string{candidate.ID}, authorization)
				if err != nil {
					return nil, fmt.Errorf("exports: cannot check execute access on %s %s: %w", ResourceExports, candidate.ID, err)
				}
				verdicts[candidate.ID] = ok
			}
			if ok {
				permitted = append(permitted, candidate)
			}
		}
		switch len(permitted) {
		case 0:
			continue
		case 1:
		default:
			ids := make([]string, len(permitted))
			for i, p := range permitted {
				ids[i] = p.ID
			}
			return nil, fmt.Errorf("%w: input topic %q (import %s) has exports %s",
				ErrAmbiguous, topic.Name, importID, strings.Join(ids, ", "))
		}

		chosen := permitted[0]
		table, err := TableName(chosen.Database, chosen.ID)
		if err != nil {
			return nil, fmt.Errorf("exports: export %s of input topic %q: %w", chosen.ID, topic.Name, err)
		}
		out = append(out, ImportExport{
			Topic:    topic.Name,
			ImportID: importID,
			ExportID: chosen.ID,
			Table:    table,
			Columns:  columnsOf(chosen, topic),
		})
	}
	return out, nil
}

// listAll reads every page. A page that comes back empty ends the loop even if
// total claims more, so a listing that shrinks while it is read cannot spin.
//
// An export already seen is dropped. analytics-serving pages with LIMIT and
// OFFSET over an unordered query, so an export can appear on two pages, and
// counted twice it would be its own rival and refuse the deployment as
// ambiguous.
func listAll(ctx context.Context, lister Lister, authorization string) ([]Export, error) {
	var all []Export
	seen := map[string]bool{}
	var offset int64
	for {
		page, total, err := lister.ListExports(ctx, authorization, pageSize, offset)
		if err != nil {
			return nil, fmt.Errorf("exports: cannot list exports: %w", err)
		}
		if len(page) == 0 {
			return all, nil
		}
		for _, export := range page {
			if seen[export.ID] {
				continue
			}
			seen[export.ID] = true
			all = append(all, export)
		}
		offset += int64(len(page))
		if offset >= total {
			return all, nil
		}
	}
}

// fits applies the matching rule except the permission check.
func fits(e Export, topic pipe.InputTopic, importID string) bool {
	if e.FilterType != FilterTypeImportExport || e.Filter != importID || e.Topic != topic.Name {
		return false
	}
	if e.ExportDatabase.Type != DatabaseTypeTimescale || e.ExportDatabase.EwFilterTopic == IgnoredFilterTopic {
		return false
	}
	// A column is only known by the path it was exported from. An export that
	// lacks one of the mapped paths cannot serve the operator.
	for _, m := range topic.Mappings {
		if _, ok := columnFor(e, m.Source); !ok {
			return false
		}
	}
	return true
}

func columnFor(e Export, source string) (string, bool) {
	for _, v := range e.Values {
		if v.Path == source {
			return v.Name, true
		}
	}
	return "", false
}

func columnsOf(e Export, topic pipe.InputTopic) map[string]string {
	columns := make(map[string]string, len(topic.Mappings))
	for _, m := range topic.Mappings {
		name, _ := columnFor(e, m.Source) // present, fits checked it
		columns[m.Source] = name
	}
	return columns
}

// Encode returns the config value for ConfigKey. The flow engine's config is a
// map of strings, so the list travels as a JSON string. Callers set the key only
// when entries is non-empty: a config without it reads Kafka as before.
func Encode(entries []ImportExport) (string, error) {
	b, err := json.Marshal(entries)
	if err != nil {
		return "", fmt.Errorf("exports: encoding import exports: %w", err)
	}
	return string(b), nil
}

// TableName returns the timescale table of an export, as analytics-serving names
// it: "userid:" + short(database) + "_export:" + short(exportID). database is the
// owner's user id.
func TableName(database, exportID string) (string, error) {
	d, err := shortenID(database)
	if err != nil {
		return "", fmt.Errorf("database %q: %w", database, err)
	}
	e, err := shortenID(exportID)
	if err != nil {
		return "", fmt.Errorf("export id %q: %w", exportID, err)
	}
	return "userid:" + d + "_export:" + e, nil
}

// shortenID mirrors analytics-serving's shortenId: the part after the last ':',
// without '-', hex-decoded, base64 URL-safe without padding.
func shortenID(id string) (string, error) {
	if i := strings.LastIndex(id, ":"); i >= 0 {
		id = id[i+1:]
	}
	raw, err := hex.DecodeString(strings.ReplaceAll(id, "-", ""))
	if err != nil {
		return "", fmt.Errorf("not a hex id: %w", err)
	}
	if len(raw) == 0 {
		return "", errors.New("empty id")
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}
