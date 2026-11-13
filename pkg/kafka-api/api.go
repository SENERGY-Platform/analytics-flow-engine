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
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// requestTimeout bounds one request to the cluster.
const requestTimeout = 30 * time.Second

// Kafka lists and deletes topics over a plain connection without authentication.
type Kafka struct {
	client *kafka.Client
}

// New connects to the comma-separated bootstrap servers on first use.
func New(bootstrap string) *Kafka {
	var addrs []string
	for _, a := range strings.Split(bootstrap, ",") {
		if a = strings.TrimSpace(a); a != "" {
			addrs = append(addrs, a)
		}
	}
	return &Kafka{client: &kafka.Client{Addr: kafka.TCP(addrs...), Timeout: requestTimeout}}
}

// ListTopics returns the names of all topics except Kafka's own internal ones.
func (k *Kafka) ListTopics(ctx context.Context) ([]string, error) {
	// No topic names asks for all topics.
	resp, err := k.client.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}
	topics := make([]string, 0, len(resp.Topics))
	for _, t := range resp.Topics {
		// A topic with another error, such as a missing leader, still exists.
		if t.Internal || errors.Is(t.Error, kafka.UnknownTopicOrPartition) {
			continue
		}
		topics = append(topics, t.Name)
	}
	return topics, nil
}

// DeleteTopic deletes one topic. A topic that does not exist counts as deleted.
func (k *Kafka) DeleteTopic(ctx context.Context, topic string) error {
	resp, err := k.client.DeleteTopics(ctx, &kafka.DeleteTopicsRequest{Topics: []string{topic}})
	if err != nil {
		return fmt.Errorf("delete topic %s: %w", topic, err)
	}
	topicErr, answered := resp.Errors[topic]
	switch {
	case !answered:
		return fmt.Errorf("delete topic %s: no answer for the topic", topic)
	case topicErr == nil, errors.Is(topicErr, kafka.UnknownTopicOrPartition):
		return nil
	default:
		return fmt.Errorf("delete topic %s: %w", topic, topicErr)
	}
}
