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

// Package baggage moves the OpenTelemetry baggage of an incoming request onto the
// pipeline it starts, so that everything logged about that pipeline later — by
// this service and by the operator containers — can be traced back to the caller's
// context, for example to the smart service instance a pipeline belongs to.
//
// The context reaches an operator twice, because the two consumers are different:
// as pod labels, which the log aggregation attaches to every container log line
// without the container knowing anything, and as an environment variable, which
// the operator libraries read to put the same fields into their own log records.
package baggage

import (
	"context"
	"sort"
	"strings"

	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"
	"go.opentelemetry.io/otel/attribute"
	otelbaggage "go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/api/validate/content"
)

const (
	// EnvVar is the environment variable every cloud operator receives the baggage
	// in. Its value is a W3C baggage header, not JSON: it is what the propagator
	// produces, and an operator that wants to carry the context into an outgoing
	// request of its own can use it verbatim.
	EnvVar = "BAGGAGE"

	// LabelPrefix prefixes every baggage entry turned into a Kubernetes label. A
	// domain prefix is the convention for labels set by something other than the
	// workload's owner; it also keeps the entries clear of the flowId, pipelineId
	// and user labels the drivers set themselves, which a baggage key could
	// otherwise overwrite.
	LabelPrefix = "baggage.senergy.infai.org/"

	// PipelineIdKey is the baggage key the pipeline's own id is added under, once
	// the registry has assigned one. Spelled like the keys otelx adds (user_id,
	// username) rather than like the pipelineId label, because it ends up in log
	// records next to those.
	PipelineIdKey = "pipeline_id"

	// listDelimiter separates the entries of a W3C baggage header.
	listDelimiter = ","

	// maxMembers and maxHeaderBytes are the limits the W3C specification puts on a
	// baggage header. A header past either is refused by every conforming parser,
	// including the one the operator libraries use, so producing one would silently
	// deliver no context at all rather than too much.
	maxMembers     = 64
	maxHeaderBytes = 8192
)

// FromContext returns the baggage carried by ctx. Nil when there is none, so that
// a pipeline started by a caller that sent no context keeps no baggage field at
// all rather than an empty object.
func FromContext(ctx context.Context) map[string]string {
	members := otelbaggage.FromContext(ctx).Members()
	if len(members) == 0 {
		return nil
	}
	result := make(map[string]string, len(members))
	for _, member := range members {
		result[member.Key()] = member.Value()
	}
	return result
}

// WithStored puts baggage read off a pipeline into ctx, for the paths that have a
// pipeline but no request to read a context off — the startup sync, in practice.
//
// Rendered and parsed back rather than built from members directly, so there is one
// encoding path and not two: NewMember refuses a raw value holding a comma or a
// space, while Parse decodes the header into exactly those values, which is what a
// log field should show.
//
// An unparseable value leaves ctx as it was rather than failing the caller. The
// baggage annotates log lines; losing an annotation is better than losing the
// recreate that was about to happen.
func WithStored(ctx context.Context, entries map[string]string) context.Context {
	if len(entries) == 0 {
		return ctx
	}
	bag, err := otelbaggage.Parse(Header(entries))
	if err != nil {
		util.Logger.DebugContext(ctx, "could not rebuild the stored baggage", "error", err)
		return ctx
	}
	return otelbaggage.ContextWithBaggage(ctx, bag)
}

// WithValue returns ctx with one baggage entry added, and keeps the active span in
// sync the way otelx.AddBaggageToHTTPRequest does for an inbound request. Used for
// the pipeline id, which does not exist yet when the request arrives.
func WithValue(ctx context.Context, key, value string) (context.Context, error) {
	member, err := otelbaggage.NewMember(key, value)
	if err != nil {
		return ctx, err
	}
	bag, err := otelbaggage.FromContext(ctx).SetMember(member)
	if err != nil {
		return ctx, err
	}
	ctx = otelbaggage.ContextWithBaggage(ctx, bag)
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(attribute.String(key, value))
	}
	return ctx, nil
}

// Header renders the baggage as a W3C baggage header value.
//
// Built from sorted keys rather than through otelbaggage.Baggage.String(), which
// ranges over a map and so returns the entries in a different order on every call.
// The value ends up in a container's environment, where an unstable spelling makes
// two identical deployments look different.
func Header(entries map[string]string) string {
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	rendered := make([]string, 0, len(keys))
	size := 0
	for _, key := range keys {
		// The value is percent-encoded before NewMember sees it, and NewMember is left
		// to validate the key and to spell the pair.
		//
		// Not the other way around: NewMember refuses a raw value holding a comma, a
		// space or anything non-ASCII, while such a value is perfectly legitimate — it
		// is what Parse produces from an inbound percent-encoded header, so it is
		// exactly what arrives here. Handing it over raw would silently drop the entry.
		//
		// Encoding first is safe because NewMember percent-decodes what it is given and
		// Member.String encodes it again, so the pair round-trips; without it a genuine
		// value of "a%20b" would be decoded to "a b" behind the caller's back.
		member, err := otelbaggage.NewMember(key, encodeValue(entries[key]))
		if err != nil {
			continue
		}
		spelled := member.String()
		if spelled == "" {
			continue
		}
		// Kept inside the specification's limits by dropping entries, sorted, rather
		// than by emitting a header no parser will accept. Sorted order makes which
		// entries survive deterministic instead of depending on map iteration.
		if len(rendered) >= maxMembers {
			break
		}
		grown := size + len(spelled)
		if len(rendered) > 0 {
			grown += len(listDelimiter)
		}
		if grown > maxHeaderBytes {
			break
		}
		size = grown
		rendered = append(rendered, spelled)
	}
	return strings.Join(rendered, listDelimiter)
}

// encodeValue percent-encodes a baggage value.
//
// The W3C definition of baggage-octet is printable ASCII minus the characters that
// delimit the header itself: %x21, %x23-2B, %x2D-3A, %x3C-5B, %x5D-7E — so no
// space, double quote, comma, semicolon, backslash or DEL. A percent sign is a
// baggage-octet but is encoded all the same, otherwise a literal one in a value
// would read as the start of an escape.
func encodeValue(value string) string {
	var encoded strings.Builder
	// Byte by byte rather than rune by rune: a multi-byte character is encoded as
	// one escape per byte, which is how the encoding is defined and how a parser
	// reassembles it.
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c != '%' && isBaggageOctet(c) {
			encoded.WriteByte(c)
			continue
		}
		encoded.WriteByte('%')
		encoded.WriteByte(hex[c>>4])
		encoded.WriteByte(hex[c&0x0f])
	}
	return encoded.String()
}

const hex = "0123456789ABCDEF"

func isBaggageOctet(c byte) bool {
	switch {
	case c == '!',
		c >= '#' && c <= '+',
		c >= '-' && c <= ':',
		c >= '<' && c <= '[',
		c >= ']' && c <= '~':
		return true
	}
	return false
}

// Labels turns the baggage into Kubernetes labels, prefixed with LabelPrefix.
//
// Entries Kubernetes would refuse are left out, and their keys returned. The
// labels are a best-effort index for log queries, while EnvVar carries the
// complete baggage; dropping an entry loses a query path, whereas letting it
// through fails the whole deployment with a 422 and the pipeline never starts.
// Sanitizing instead of dropping would be worse than either: a label holding a
// mangled value silently fails to match a query for the real one.
func Labels(entries map[string]string) (labels map[string]string, skipped []string) {
	if len(entries) == 0 {
		return nil, nil
	}
	labels = make(map[string]string, len(entries))
	for key, value := range entries {
		label := LabelPrefix + key
		if len(content.IsLabelKey(label)) > 0 || len(content.IsLabelValue(value)) > 0 {
			skipped = append(skipped, key)
			continue
		}
		labels[label] = value
	}
	sort.Strings(skipped)
	if len(labels) == 0 {
		labels = nil
	}
	return labels, skipped
}

// AddLabels merges the baggage labels into an existing label set and logs which
// entries were left out. Kept here so both drivers apply the same rule and log it
// the same way.
//
// The existing labels win on collision. LabelPrefix makes that unreachable today,
// but the drivers' own labels identify the workload and a log annotation must not
// be able to rewrite them.
func AddLabels(ctx context.Context, labels map[string]string, entries map[string]string) map[string]string {
	added, skipped := Labels(entries)
	if len(skipped) > 0 {
		// Keys only. The values are the caller's context, which includes a username,
		// and a diagnostic message is not a reason to copy it into a second log line.
		util.Logger.DebugContext(ctx, "baggage entries left out of the pod labels",
			"keys", strings.Join(skipped, ", "))
	}
	if len(added) == 0 {
		return labels
	}
	if labels == nil {
		labels = make(map[string]string, len(added))
	}
	for key, value := range added {
		if _, exists := labels[key]; exists {
			continue
		}
		labels[key] = value
	}
	return labels
}

// Merge overlays the baggage of the current request on top of the baggage already
// stored on a pipeline.
//
// The stored one is the base because otelx adds user_id and username on every
// request, so an incoming request is never empty: without this, the first update
// from any other caller would drop a smart service instance id set at creation.
// The request wins per key so that a caller which does know a value can correct it.
func Merge(stored, incoming map[string]string) map[string]string {
	if len(stored) == 0 && len(incoming) == 0 {
		return nil
	}
	merged := make(map[string]string, len(stored)+len(incoming))
	for key, value := range stored {
		merged[key] = value
	}
	for key, value := range incoming {
		merged[key] = value
	}
	return merged
}
