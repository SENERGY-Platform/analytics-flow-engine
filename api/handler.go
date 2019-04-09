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
	"analytics-flow-engine/lib"
	"analytics-flow-engine/parsing-api"
	"analytics-flow-engine/rancher-api"
	"fmt"
	"log"
	"net/http"

	"analytics-flow-engine/rancher2"

	"github.com/gorilla/mux"
	"github.com/rs/cors"
)

func CreateServer() {
	var driver lib.Driver
	switch selectedDriver := lib.GetEnv("DRIVER", "rancher"); selectedDriver {
	case "rancher":
		driver = rancher_api.NewRancher(
			lib.GetEnv("RANCHER_ENDPOINT", ""),
			lib.GetEnv("RANCHER_ACCESS_KEY", ""),
			lib.GetEnv("RANCHER_SECRET_KEY", ""),
			lib.GetEnv("RANCHER_STACK_ID", ""),
			lib.GetEnv("ZOOKEEPER", ""),
		)
	case "rancher2":
		driver = rancher2.NewRancher2(
			lib.GetEnv("RANCHER2_ENDPOINT", ""),
			lib.GetEnv("RANCHER2_ACCESS_KEY", ""),
			lib.GetEnv("RANCHER2_SECRET_KEY", ""),
			lib.GetEnv("RANCHER2_STACK_ID", ""),
			lib.GetEnv("ZOOKEEPER", ""),
		)
	default:
		fmt.Println("No driver selected")
	}

	parser := parsing_api.NewParsingApi(
		lib.GetEnv("PARSER_API_ENDPOINT", ""),
	)
	port := lib.GetEnv("API_PORT", "8000")
	fmt.Print("Starting Server at port " + port + "\n")
	router := mux.NewRouter()

	e := NewEndpoint(driver, parser)
	router.HandleFunc("/", e.getRootEndpoint).Methods("GET")
	router.HandleFunc("/pipeline/{id}", e.getPipelineStatus).Methods("GET")
	router.HandleFunc("/pipeline", e.startPipeline).Methods("POST")
	router.HandleFunc("/pipeline/{id}", e.deletePipeline).Methods("DELETE")
	c := cors.New(
		cors.Options{
			AllowedHeaders: []string{"Content-Type", "Authorization"},
			AllowedOrigins: []string{"*"},
			AllowedMethods: []string{"GET", "POST", "DELETE", "OPTIONS"},
		})
	handler := c.Handler(router)
	logger := lib.NewLogger(handler, "CALL")
	log.Fatal(http.ListenAndServe(":"+port, logger))
}
