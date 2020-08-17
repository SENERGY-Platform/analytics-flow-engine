/*
 * Copyright 2018 InfAI (CC SES)
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

package api

import (
	"analytics-flow-engine/internal/lib"
	"encoding/json"
	"fmt"
	"github.com/dgrijalva/jwt-go"
	"net/http"

	"github.com/gorilla/mux"
)

type Endpoint struct {
	driver         lib.Driver
	engine         *lib.FlowEngine
	parsingService lib.ParsingApiService
	metricService  lib.MetricsApiService
}

func NewEndpoint(driver lib.Driver, parsingService lib.ParsingApiService, metricsService lib.MetricsApiService) *Endpoint {
	ret := lib.NewFlowEngine(driver, parsingService, metricsService)
	return &Endpoint{driver, ret, parsingService, metricsService}
}

func (e *Endpoint) getRootEndpoint(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(lib.Response{"OK"})
}

func (e *Endpoint) getPipelineStatus(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	ret := e.engine.GetPipelineStatus(vars["id"])
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(lib.Response{ret})
}

func (e *Endpoint) startPipeline(w http.ResponseWriter, req *http.Request) {
	decoder := json.NewDecoder(req.Body)
	var pipe_req lib.PipelineRequest
	err := decoder.Decode(&pipe_req)
	if err != nil {
		fmt.Println(err)
	}
	defer req.Body.Close()
	ret := e.engine.StartPipeline(pipe_req, e.getUserId(req), req.Header.Get("Authorization"))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(ret)
}

func (e *Endpoint) deletePipeline(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	ret := e.engine.DeletePipeline(vars["id"], e.getUserId(req), req.Header.Get("Authorization"))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(lib.Response{ret})
}

func (e *Endpoint) getUserId(req *http.Request) (userId string) {
	userId = req.Header.Get("X-UserId")
	if userId == "" {
		if userId == "" && req.Header.Get("Authorization") != "" {
			_, claims := parseJWTToken(req.Header.Get("Authorization")[7:])
			userId = claims.Sub
			if userId == "" {
				userId = "dummy"
			}
		}
	}
	return
}

func parseJWTToken(encodedToken string) (token *jwt.Token, claims lib.Claims) {
	token, _ = jwt.ParseWithClaims(encodedToken, &claims, nil)
	return
}
