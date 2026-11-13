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

package kafka_api

import (
	"context"
	"net"
	"slices"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol/deletetopics"
	"github.com/segmentio/kafka-go/protocol/metadata"
)

// fakeTransport answers metadata and delete requests from fixed responses and keeps
// the requests it got.
type fakeTransport struct {
	metadata *metadata.Response
	deleted  *deletetopics.Response
	requests []kafka.Request
}

func (f *fakeTransport) RoundTrip(_ context.Context, _ net.Addr, req kafka.Request) (kafka.Response, error) {
	f.requests = append(f.requests, req)
	switch req.(type) {
	case *metadata.Request:
		return f.metadata, nil
	case *deletetopics.Request:
		return f.deleted, nil
	}
	panic("unexpected request")
}

func withTransport(t *fakeTransport) *Kafka {
	k := New("kafka.kafka:9092")
	k.client.Transport = t
	return k
}

// Asking for no topic by name is what returns all of them; Kafka's own topics and
// one that no longer exists are left out, one without a leader is not.
func TestListTopicsAsksForAllTopics(t *testing.T) {
	transport := &fakeTransport{metadata: &metadata.Response{Topics: []metadata.ResponseTopic{
		{Name: "__consumer_offsets", IsInternal: true},
		{Name: "analytics-x-repartition"},
		{Name: "gone", ErrorCode: int16(kafka.UnknownTopicOrPartition)},
		{Name: "leaderless", ErrorCode: int16(kafka.LeaderNotAvailable)},
	}}}
	topics, err := withTransport(transport).ListTopics(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(topics, []string{"analytics-x-repartition", "leaderless"}) {
		t.Errorf("topics = %v", topics)
	}
	if req := transport.requests[0].(*metadata.Request); req.TopicNames != nil {
		t.Errorf("topic names = %v, want nil for all topics", req.TopicNames)
	}
}

// A topic already gone counts as deleted, so a repeated delete or a job running
// behind a manual one does not report an error.
func TestDeleteTopicTreatsAMissingTopicAsDeleted(t *testing.T) {
	for name, c := range map[string]struct {
		code    kafka.Error
		wantErr bool
	}{
		"deleted": {0, false},
		"missing": {kafka.UnknownTopicOrPartition, false},
		"refused": {kafka.TopicAuthorizationFailed, true},
	} {
		t.Run(name, func(t *testing.T) {
			transport := &fakeTransport{deleted: &deletetopics.Response{Responses: []deletetopics.ResponseTopic{
				{Name: "analytics-x-changelog", ErrorCode: int16(c.code)},
			}}}
			err := withTransport(transport).DeleteTopic(context.Background(), "analytics-x-changelog")
			if (err != nil) != c.wantErr {
				t.Errorf("err = %v, want error %v", err, c.wantErr)
			}
			if names := transport.requests[0].(*deletetopics.Request).TopicNames; !slices.Equal(names, []string{"analytics-x-changelog"}) {
				t.Errorf("deleted %v", names)
			}
		})
	}
}

// An answer that does not name the topic is not a success.
func TestDeleteTopicWithoutAnAnswerFails(t *testing.T) {
	transport := &fakeTransport{deleted: &deletetopics.Response{}}
	if err := withTransport(transport).DeleteTopic(context.Background(), "analytics-x-changelog"); err == nil {
		t.Fatal("err = nil, want an error for an unanswered delete")
	}
}
