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

package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	pipeApi "github.com/SENERGY-Platform/analytics-pipeline/lib"
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

func (c *Client) addHeaders(req *http.Request) {
	if c.AuthToken != "" {
		req.Header.Set("Authorization", c.AuthToken)
	}
	if c.UserID != "" {
		req.Header.Set("X-UserId", c.UserID)
	}
}

func (c *Client) GetPipelineStatus(id string) (*lib.PipelineStatus, error) {
	url := fmt.Sprintf("%s/pipeline/%s", c.BaseURL, id)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	c.addHeaders(req)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {

		}
	}(resp.Body)

	if err := checkResponse(resp); err != nil {
		return nil, err
	}

	var status lib.PipelineStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &status, nil
}

func (c *Client) GetPipelineStatusForUser(id, forUser string) (*lib.PipelineStatus, error) {
	url := fmt.Sprintf("%s/pipeline/%s?for_user=%s", c.BaseURL, id, forUser)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	c.addHeaders(req)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {

		}
	}(resp.Body)

	if err := checkResponse(resp); err != nil {
		return nil, err
	}

	var status lib.PipelineStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &status, nil
}

func (c *Client) GetPipelinesStatus(ids []string) ([]lib.PipelineStatus, error) {
	url := fmt.Sprintf("%s/pipelines", c.BaseURL)

	request := lib.PipelineStatusRequest{Ids: ids}
	body, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	c.addHeaders(req)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {

		}
	}(resp.Body)

	if err := checkResponse(resp); err != nil {
		return nil, err
	}

	var statuses []lib.PipelineStatus
	if err := json.NewDecoder(resp.Body).Decode(&statuses); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return statuses, nil
}

func (c *Client) StartPipeline(request lib.PipelineRequest) (*pipeApi.Pipeline, error) {
	url := fmt.Sprintf("%s/pipeline", c.BaseURL)

	body, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	c.addHeaders(req)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {

		}
	}(resp.Body)

	if err := checkResponse(resp); err != nil {
		return nil, err
	}

	var pipeline pipeApi.Pipeline
	if err := json.NewDecoder(resp.Body).Decode(&pipeline); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &pipeline, nil
}

func (c *Client) UpdatePipeline(request lib.PipelineRequest) (*pipeApi.Pipeline, error) {
	url := fmt.Sprintf("%s/pipeline", c.BaseURL)

	body, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	c.addHeaders(req)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {

		}
	}(resp.Body)

	if err := checkResponse(resp); err != nil {
		return nil, err
	}

	var pipeline pipeApi.Pipeline
	if err := json.NewDecoder(resp.Body).Decode(&pipeline); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &pipeline, nil
}

func (c *Client) DeletePipeline(id string) error {
	url := fmt.Sprintf("%s/pipeline/%s", c.BaseURL, id)

	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	c.addHeaders(req)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {

		}
	}(resp.Body)

	if err := checkResponse(resp); err != nil {
		return err
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
