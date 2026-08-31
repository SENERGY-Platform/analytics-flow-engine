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

// Package client talks to the analytics flow engine.
//
// Every call has a Context variant. Use it: it carries the caller's trace and
// baggage onto the wire, which is what lets the flow engine — and the operators it
// deploys — log under the context of whatever caused the call. A smart service
// worker that passes its instance id this way gets it back on every log line the
// resulting pipeline ever writes.
//
// The variants without a context are kept so existing callers still build. They
// pass context.TODO(), so the flow engine starts a trace of its own and the
// connection to the caller is lost.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	pipeApi "github.com/SENERGY-Platform/analytics-pipeline/lib"
	"github.com/SENERGY-Platform/gin-middleware/otelx"
)

type Client struct {
	BaseURL    string
	HTTPClient *http.Client
	AuthToken  string
	UserID     string
}

func NewClient(baseURL, authToken string, userID string) *Client {
	return &Client{
		BaseURL:   baseURL,
		AuthToken: authToken,
		UserID:    userID,
		HTTPClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *Client) SetUserID(userID string) {
	c.UserID = userID
}

func (c *Client) SetToken(authToken string) {
	c.AuthToken = authToken
}

func (c *Client) GetPipelineStatus(id string) (*lib.PipelineStatus, error) {
	return c.GetPipelineStatusContext(context.TODO(), id)
}

func (c *Client) GetPipelineStatusContext(ctx context.Context, id string) (*lib.PipelineStatus, error) {
	var status lib.PipelineStatus
	err := c.do(ctx, http.MethodGet, fmt.Sprintf("%s/pipeline/%s", c.BaseURL, url.PathEscape(id)), nil, &status)
	if err != nil {
		return nil, err
	}
	return &status, nil
}

func (c *Client) GetPipelineStatusForUser(id, forUser string) (*lib.PipelineStatus, error) {
	return c.GetPipelineStatusForUserContext(context.TODO(), id, forUser)
}

func (c *Client) GetPipelineStatusForUserContext(ctx context.Context, id, forUser string) (*lib.PipelineStatus, error) {
	var status lib.PipelineStatus
	target := fmt.Sprintf("%s/pipeline/%s?for_user=%s", c.BaseURL, url.PathEscape(id), url.QueryEscape(forUser))
	err := c.do(ctx, http.MethodGet, target, nil, &status)
	if err != nil {
		return nil, err
	}
	return &status, nil
}

func (c *Client) GetPipelinesStatus(ids []string) ([]lib.PipelineStatus, error) {
	return c.GetPipelinesStatusContext(context.TODO(), ids)
}

func (c *Client) GetPipelinesStatusContext(ctx context.Context, ids []string) ([]lib.PipelineStatus, error) {
	var statuses []lib.PipelineStatus
	err := c.do(ctx, http.MethodPost, fmt.Sprintf("%s/pipelines", c.BaseURL),
		lib.PipelineStatusRequest{Ids: ids}, &statuses)
	if err != nil {
		return nil, err
	}
	return statuses, nil
}

func (c *Client) StartPipeline(request lib.PipelineRequest) (*pipeApi.Pipeline, error) {
	return c.StartPipelineContext(context.TODO(), request)
}

func (c *Client) StartPipelineContext(ctx context.Context, request lib.PipelineRequest) (*pipeApi.Pipeline, error) {
	var pipeline pipeApi.Pipeline
	err := c.do(ctx, http.MethodPost, fmt.Sprintf("%s/pipeline", c.BaseURL), request, &pipeline)
	if err != nil {
		return nil, err
	}
	return &pipeline, nil
}

func (c *Client) UpdatePipeline(request lib.PipelineRequest) (*pipeApi.Pipeline, error) {
	return c.UpdatePipelineContext(context.TODO(), request)
}

func (c *Client) UpdatePipelineContext(ctx context.Context, request lib.PipelineRequest) (*pipeApi.Pipeline, error) {
	var pipeline pipeApi.Pipeline
	err := c.do(ctx, http.MethodPut, fmt.Sprintf("%s/pipeline", c.BaseURL), request, &pipeline)
	if err != nil {
		return nil, err
	}
	return &pipeline, nil
}

func (c *Client) DeletePipeline(id string) error {
	return c.DeletePipelineContext(context.TODO(), id)
}

func (c *Client) DeletePipelineContext(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, fmt.Sprintf("%s/pipeline/%s", c.BaseURL, url.PathEscape(id)), nil, nil)
}

// do sends one request and decodes the response into target, which may be nil for
// a call that returns no body.
func (c *Client) do(ctx context.Context, method, target string, body any, decodeInto any) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("failed to marshal request: %w", err)
		}
		reader = bytes.NewBuffer(encoded)
	}

	req, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	if c.AuthToken != "" {
		req.Header.Set("Authorization", c.AuthToken)
	}
	if c.UserID != "" {
		req.Header.Set("X-UserId", c.UserID)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	// The trace and the baggage of the caller, so the flow engine continues the same
	// trace instead of starting one nothing points at.
	if err = otelx.InjectContextToRequest(ctx, req); err != nil {
		return fmt.Errorf("failed to inject the trace context: %w", err)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if err = checkResponse(resp); err != nil {
		return err
	}
	if decodeInto == nil {
		return nil
	}
	if err = json.NewDecoder(resp.Body).Decode(decodeInto); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}
	return nil
}

func checkResponse(resp *http.Response) error {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	bodyBytes, _ := io.ReadAll(resp.Body)
	bodyString := string(bodyBytes)

	switch resp.StatusCode {
	case http.StatusBadRequest:
		return fmt.Errorf("bad request (400): %s", bodyString)
	case http.StatusUnauthorized:
		return fmt.Errorf("unauthorized (401): %s", bodyString)
	case http.StatusForbidden:
		return fmt.Errorf("forbidden (403): %s", bodyString)
	case http.StatusNotFound:
		return fmt.Errorf("not found (404): %s", bodyString)
	case http.StatusInternalServerError:
		return fmt.Errorf("internal server error (500): %s", bodyString)
	default:
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, bodyString)
	}
}
