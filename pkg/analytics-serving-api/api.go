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

// Package analytics_serving_api is the client for the one analytics-serving call
// the flow engine makes: listing the exports a user may read.
package analytics_serving_api

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib/exports"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/httpreq"
)

type AnalyticsServingApi struct {
	url string
}

func NewAnalyticsServingApi(url string) *AnalyticsServingApi {
	return &AnalyticsServingApi{url}
}

// listResponse is analytics-serving's page of instances.
type listResponse struct {
	Total     int64            `json:"total"`
	Count     int64            `json:"count"`
	Instances []exports.Export `json:"instances"`
}

// ListExports implements exports.Lister. analytics-serving restricts the listing to
// the exports the token's user may read, so the token is forwarded as it is.
func (a *AnalyticsServingApi) ListExports(ctx context.Context, authorization string, limit, offset int64) ([]exports.Export, int64, error) {
	query := url.Values{}
	query.Set("limit", strconv.FormatInt(limit, 10))
	query.Set("offset", strconv.FormatInt(offset, 10))
	response, err := httpreq.Do(ctx, httpreq.Request{
		Method:  http.MethodGet,
		URL:     a.url + "/instance?" + query.Encode(),
		Headers: map[string]string{"Authorization": authorization},
	})
	if err != nil {
		return nil, 0, fmt.Errorf("analytics serving API - could not list exports: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return nil, 0, fmt.Errorf("analytics serving API - could not list exports: %d %s",
			response.StatusCode, response.Text())
	}
	var page listResponse
	if err = response.Decode(&page); err != nil {
		return nil, 0, fmt.Errorf("analytics serving API - could not unmarshal exports: %w", err)
	}
	return page.Instances, page.Total, nil
}
