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

package baggage

import (
	"context"
	"maps"
	"strconv"
	"strings"
	"testing"

	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"
	otelbaggage "go.opentelemetry.io/otel/baggage"
)

func init() {
	// AddLabels logs the entries it drops.
	util.InitStructLogger("error")
}

func contextWith(t *testing.T, entries map[string]string) context.Context {
	t.Helper()
	members := make([]otelbaggage.Member, 0, len(entries))
	for key, value := range entries {
		member, err := otelbaggage.NewMember(key, value)
		if err != nil {
			t.Fatalf("could not build baggage member %q: %v", key, err)
		}
		members = append(members, member)
	}
	bag, err := otelbaggage.New(members...)
	if err != nil {
		t.Fatalf("could not build baggage: %v", err)
	}
	return otelbaggage.ContextWithBaggage(context.Background(), bag)
}

func TestFromContext(t *testing.T) {
	t.Run("returns the entries", func(t *testing.T) {
		entries := map[string]string{"smart_service_instance_id": "8fbd0e8a", "user_id": "jonah"}
		got := FromContext(contextWith(t, entries))
		if !maps.Equal(entries, got) {
			t.Errorf("expected %v, got %v", entries, got)
		}
	})

	t.Run("returns nil without baggage", func(t *testing.T) {
		// Not an empty map: a pipeline started by a caller that sent no context must
		// end up with no baggage field at all, because the field is omitempty.
		if got := FromContext(context.Background()); got != nil {
			t.Errorf("expected nil, got %v", got)
		}
	})
}

func TestWithValue(t *testing.T) {
	t.Run("adds to existing baggage", func(t *testing.T) {
		ctx := contextWith(t, map[string]string{"smart_service_instance_id": "8fbd0e8a"})
		ctx, err := WithValue(ctx, PipelineIdKey, "3c1f9b42")
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]string{"smart_service_instance_id": "8fbd0e8a", PipelineIdKey: "3c1f9b42"}
		if got := FromContext(ctx); !maps.Equal(want, got) {
			t.Errorf("expected %v, got %v", want, got)
		}
	})

	t.Run("works on a context without baggage", func(t *testing.T) {
		ctx, err := WithValue(context.Background(), PipelineIdKey, "3c1f9b42")
		if err != nil {
			t.Fatal(err)
		}
		if got := FromContext(ctx)[PipelineIdKey]; got != "3c1f9b42" {
			t.Errorf("expected the pipeline id, got %q", got)
		}
	})

	t.Run("returns the original context on a rejected value", func(t *testing.T) {
		ctx := contextWith(t, map[string]string{"user_id": "jonah"})
		got, err := WithValue(ctx, "invalid key with spaces", "x")
		if err == nil {
			t.Fatal("expected an error")
		}
		if v := FromContext(got)["user_id"]; v != "jonah" {
			t.Errorf("the existing baggage should survive, got %v", FromContext(got))
		}
	})
}

func TestHeader(t *testing.T) {
	t.Run("is sorted by key", func(t *testing.T) {
		// Stability is the point: the value ends up in a container's environment, and
		// otelbaggage.Baggage.String ranges over a map, so it cannot be used here.
		entries := map[string]string{"user_id": "jonah", "pipeline_id": "3c1f9b42", "a": "1"}
		want := "a=1,pipeline_id=3c1f9b42,user_id=jonah"
		for i := 0; i < 20; i++ {
			if got := Header(entries); got != want {
				t.Fatalf("expected %q, got %q", want, got)
			}
		}
	})

	t.Run("round-trips values that need encoding", func(t *testing.T) {
		// These are the values otelbaggage.NewMember refuses to take raw, while
		// otelbaggage.Parse produces exactly them from an inbound percent-encoded
		// header — so they are what actually arrives, and rendering them wrongly
		// would drop the entry without a trace. The parser is the judge here rather
		// than a hand-written expected string, because it is what will read the
		// header at the other end.
		entries := map[string]string{
			"plain":     "8fbd0e8a",
			"comma":     "a,b",
			"space":     "a b",
			"semicolon": "a;b",
			"equals":    "a=b",
			"percent":   "50%",
			"quote":     `a"b`,
			"backslash": `a\b`,
			"umlaut":    "grün",
			"email":     "jonah@bitnify.net",
			"empty":     "",
		}
		parsed, err := otelbaggage.Parse(Header(entries))
		if err != nil {
			t.Fatalf("the rendered header must be parseable: %v", err)
		}
		got := make(map[string]string)
		for _, member := range parsed.Members() {
			got[member.Key()] = member.Value()
		}
		if !maps.Equal(entries, got) {
			t.Errorf("expected %v, got %v", entries, got)
		}
	})

	t.Run("encodes only what has to be encoded", func(t *testing.T) {
		if got := Header(map[string]string{"a": "a,b"}); got != "a=a%2Cb" {
			t.Errorf("expected a=a%%2Cb, got %q", got)
		}
		if got := Header(map[string]string{"a": "jonah@bitnify.net"}); got != "a=jonah@bitnify.net" {
			t.Errorf("expected the value unchanged, got %q", got)
		}
	})

	t.Run("empty for no entries", func(t *testing.T) {
		if got := Header(nil); got != "" {
			t.Errorf("expected an empty string, got %q", got)
		}
	})

	t.Run("stays inside the specification's limits", func(t *testing.T) {
		// Merge can grow the baggage past what an inbound header may carry: the
		// persisted entries plus the ones otelx adds on every request. A header over
		// either limit is refused wholesale by the parser, so both operator libraries
		// would read no context at all instead of a truncated one.
		many := make(map[string]string, 100)
		for i := 0; i < 100; i++ {
			many["key"+strconv.Itoa(i)] = "value"
		}
		if _, err := otelbaggage.Parse(Header(many)); err != nil {
			t.Errorf("100 entries must still render a parseable header: %v", err)
		}

		big := make(map[string]string, 40)
		for i := 0; i < 40; i++ {
			big["key"+strconv.Itoa(i)] = strings.Repeat("v", 500)
		}
		header := Header(big)
		if len(header) > 8192 {
			t.Errorf("expected at most 8192 bytes, got %d", len(header))
		}
		if _, err := otelbaggage.Parse(header); err != nil {
			t.Errorf("a 20kB input must still render a parseable header: %v", err)
		}
	})

	t.Run("drops an entry whose key cannot be spelled", func(t *testing.T) {
		got := Header(map[string]string{"ok": "1", "not ok": "2"})
		if got != "ok=1" {
			t.Errorf("expected only the valid entry, got %q", got)
		}
	})
}

func TestWithStored(t *testing.T) {
	t.Run("round-trips stored entries", func(t *testing.T) {
		// Used to annotate the startup sync's log lines, so the values have to come
		// back decoded rather than as the header spelled them.
		entries := map[string]string{"smart_service_instance_id": "8fbd0e8a", "note": "a,b"}
		ctx := WithStored(context.Background(), entries)
		if got := FromContext(ctx); !maps.Equal(entries, got) {
			t.Errorf("expected %v, got %v", entries, got)
		}
	})

	t.Run("leaves the context alone for no entries", func(t *testing.T) {
		if got := FromContext(WithStored(context.Background(), nil)); got != nil {
			t.Errorf("expected nil, got %v", got)
		}
	})
}

func TestLabels(t *testing.T) {
	t.Run("prefixes the keys", func(t *testing.T) {
		labels, skipped := Labels(map[string]string{"smart_service_instance_id": "8fbd0e8a"})
		if len(skipped) > 0 {
			t.Fatalf("nothing should be skipped, got %v", skipped)
		}
		want := map[string]string{LabelPrefix + "smart_service_instance_id": "8fbd0e8a"}
		if !maps.Equal(want, labels) {
			t.Errorf("expected %v, got %v", want, labels)
		}
	})

	t.Run("skips what kubernetes would refuse", func(t *testing.T) {
		// Every one of these fails the deployment with a 422 if it is let through, and
		// a username that is an email address is the realistic case: otelx puts the
		// username into the baggage of every request.
		cases := map[string]map[string]string{
			"value with an at sign": {"username": "jonah@bitnify.net"},
			"value over 63 chars":   {"note": strings.Repeat("a", 64)},
			"value with a slash":    {"path": "a/b"},
			"key with an at sign":   {"we@ird": "1"},
			"key over 63 chars":     {strings.Repeat("k", 64): "1"},
			"value starting with -": {"note": "-leading"},
		}
		for name, entries := range cases {
			t.Run(name, func(t *testing.T) {
				labels, skipped := Labels(entries)
				if len(labels) != 0 {
					t.Errorf("expected no labels, got %v", labels)
				}
				if len(skipped) != 1 {
					t.Errorf("expected one skipped key, got %v", skipped)
				}
			})
		}
	})

	t.Run("keeps the valid entries of a mixed set", func(t *testing.T) {
		labels, skipped := Labels(map[string]string{
			"smart_service_instance_id": "8fbd0e8a",
			"username":                  "jonah@bitnify.net",
		})
		want := map[string]string{LabelPrefix + "smart_service_instance_id": "8fbd0e8a"}
		if !maps.Equal(want, labels) {
			t.Errorf("expected %v, got %v", want, labels)
		}
		if len(skipped) != 1 || skipped[0] != "username" {
			t.Errorf("expected username to be skipped, got %v", skipped)
		}
	})

	t.Run("an empty value is a valid label", func(t *testing.T) {
		labels, skipped := Labels(map[string]string{"note": ""})
		if len(skipped) > 0 {
			t.Errorf("nothing should be skipped, got %v", skipped)
		}
		if labels[LabelPrefix+"note"] != "" {
			t.Errorf("expected the empty label to be present, got %v", labels)
		}
	})

	t.Run("nil for no entries", func(t *testing.T) {
		labels, skipped := Labels(nil)
		if labels != nil || skipped != nil {
			t.Errorf("expected nil, nil, got %v, %v", labels, skipped)
		}
	})
}

func TestAddLabels(t *testing.T) {
	t.Run("merges into the existing labels", func(t *testing.T) {
		got := AddLabels(context.Background(), map[string]string{"pipelineId": "3c1f9b42"},
			map[string]string{"smart_service_instance_id": "8fbd0e8a"})
		want := map[string]string{
			"pipelineId": "3c1f9b42",
			LabelPrefix + "smart_service_instance_id": "8fbd0e8a",
		}
		if !maps.Equal(want, got) {
			t.Errorf("expected %v, got %v", want, got)
		}
	})

	t.Run("the existing labels win", func(t *testing.T) {
		// Unreachable through LabelPrefix today, but the drivers' labels identify the
		// workload and a log annotation must not be able to rewrite them.
		existing := map[string]string{LabelPrefix + "pipelineId": "the real one"}
		got := AddLabels(context.Background(), existing, map[string]string{"pipelineId": "the baggage one"})
		if got[LabelPrefix+"pipelineId"] != "the real one" {
			t.Errorf("expected the existing label to survive, got %v", got)
		}
	})

	t.Run("returns the labels unchanged without baggage", func(t *testing.T) {
		existing := map[string]string{"pipelineId": "3c1f9b42"}
		got := AddLabels(context.Background(), existing, nil)
		if !maps.Equal(existing, got) {
			t.Errorf("expected %v, got %v", existing, got)
		}
	})
}

func TestMerge(t *testing.T) {
	t.Run("the incoming request wins per key", func(t *testing.T) {
		got := Merge(
			map[string]string{"smart_service_instance_id": "8fbd0e8a", "user_id": "old"},
			map[string]string{"user_id": "new"})
		want := map[string]string{"smart_service_instance_id": "8fbd0e8a", "user_id": "new"}
		if !maps.Equal(want, got) {
			t.Errorf("expected %v, got %v", want, got)
		}
	})

	t.Run("keeps stored keys the request does not know", func(t *testing.T) {
		// The reason the rule exists: otelx puts user_id and username on every
		// request, so an update from an unrelated caller would otherwise drop the
		// smart service instance id the pipeline was created with.
		got := Merge(
			map[string]string{"smart_service_instance_id": "8fbd0e8a"},
			map[string]string{"user_id": "jonah", "username": "jonah@bitnify.net"})
		if got["smart_service_instance_id"] != "8fbd0e8a" {
			t.Errorf("expected the stored instance id to survive, got %v", got)
		}
	})

	t.Run("nil when both are empty", func(t *testing.T) {
		if got := Merge(nil, nil); got != nil {
			t.Errorf("expected nil, got %v", got)
		}
	})

	t.Run("does not mutate its inputs", func(t *testing.T) {
		stored := map[string]string{"a": "1"}
		incoming := map[string]string{"b": "2"}
		Merge(stored, incoming)
		if len(stored) != 1 || len(incoming) != 1 {
			t.Errorf("inputs were mutated: %v, %v", stored, incoming)
		}
	})
}
