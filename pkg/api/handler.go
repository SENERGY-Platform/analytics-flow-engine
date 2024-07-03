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
	kafka2mqtt_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/kafka2mqtt-api"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/lib"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/parsing-api"
	permission_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/permission-api"
	rancher2_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/rancher2-api"
	devicemanager_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/device-manager-api"

	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/rs/cors"
)

func CreateServer() {
	var driver lib.Driver
	switch selectedDriver := lib.GetEnv("DRIVER", "rancher"); selectedDriver {
	default:
		driver = rancher2_api.NewRancher2(
			lib.GetEnv("RANCHER2_ENDPOINT", ""),
			lib.GetEnv("RANCHER2_ACCESS_KEY", ""),
			lib.GetEnv("RANCHER2_SECRET_KEY", ""),
			lib.GetEnv("RANCHER2_STACK_ID", ""),
			lib.GetEnv("ZOOKEEPER", ""),
		)
	}

	parser := parsing_api.NewParsingApi(lib.GetEnv("PARSER_API_ENDPOINT", ""))
	permission := permission_api.NewPermissionApi(lib.GetEnv("PERMISSION_API_ENDPOINT", ""))
	kafka2mqtt := kafka2mqtt_api.NewKafka2MqttApi(lib.GetEnv("KAFKA2MQTT_API_ENDPOINT", ""))
	deviceManager := devicemanager_api.NewDeviceManagerApi(lib.GetEnv("DEVICE_MANAGER_API_ENDPOINT", ""))

	port := lib.GetEnv("API_PORT", "8000")
	log.Println("Starting Server at port " + port + "\n")
	router := mux.NewRouter()

	e := NewEndpoint(driver, parser, permission, kafka2mqtt, deviceManager)
	router.HandleFunc("/", e.getRootEndpoint).Methods("GET")
	router.HandleFunc("/pipeline/{id}", e.getPipelineStatus).Methods("GET")
	router.HandleFunc("/pipeline", e.startPipeline).Methods("POST")
	router.HandleFunc("/pipeline", e.updatePipeline).Methods("PUT")
	router.HandleFunc("/pipeline/{id}", e.deletePipeline).Methods("DELETE")
	c := cors.New(
		cors.Options{
			AllowedHeaders: []string{"Content-Type", "Authorization"},
			AllowedOrigins: []string{"*"},
			AllowedMethods: []string{"GET", "POST", "DELETE", "OPTIONS", "PUT"},
		})
	handler := c.Handler(router)
	logger := lib.NewLogger(handler, "CALL")
	log.Fatal(http.ListenAndServe(lib.GetEnv("SERVERNAME", "")+":"+port, logger))
}
