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
	"github.com/dgrijalva/jwt-go"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

type Endpoint struct {
	engine *lib.FlowEngine
}

func NewEndpoint(driver lib.Driver, parsingService lib.ParsingApiService, metricsService lib.MetricsApiService, permissionService lib.PermissionApiService) *Endpoint {
	ret := lib.NewFlowEngine(driver, parsingService, metricsService, permissionService)
	return &Endpoint{ret}
}

func (e *Endpoint) getRootEndpoint(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	_ = json.NewEncoder(w).Encode(lib.Response{Message: "OK"})
}

func (e *Endpoint) getPipelineStatus(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	_ = e.engine.GetPipelineStatus(vars["id"])
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
}

func (e *Endpoint) startPipeline(w http.ResponseWriter, req *http.Request) {
	decoder := json.NewDecoder(req.Body)
	var pipeReq lib.PipelineRequest
	err := decoder.Decode(&pipeReq)
	if err != nil {
		log.Println(err)
	}
	defer func() {
		_ = req.Body.Close()
	}()
	ret, err := e.engine.StartPipeline(pipeReq, e.getUserId(req), req.Header.Get("Authorization"))
	if err != nil {
		log.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ret)
	}
}

func (e *Endpoint) updatePipeline(w http.ResponseWriter, req *http.Request) {
	decoder := json.NewDecoder(req.Body)
	var pipeReq lib.PipelineRequest
	err := decoder.Decode(&pipeReq)
	if err != nil {
		log.Println(err)
	}
	defer func() {
		_ = req.Body.Close()
	}()
	ret, err := e.engine.UpdatePipeline(pipeReq, e.getUserId(req), req.Header.Get("Authorization"))
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		log.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ret)
	}
}

func (e *Endpoint) deletePipeline(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	err := e.engine.DeletePipeline(vars["id"], e.getUserId(req), req.Header.Get("Authorization"))
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		log.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
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
