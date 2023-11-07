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

package main

import (
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/api"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/lib"
	"github.com/SENERGY-Platform/go-service-base/watchdog"

	"log"
	"os"
	"syscall"

	"github.com/joho/godotenv"
)

func main() {
	ec := 0
	defer func() {
		os.Exit(ec)
	}()

	err := godotenv.Load()
	if err != nil {
		log.Print("Error loading .env file")
	}
	watchdog := watchdog.New(syscall.SIGINT, syscall.SIGTERM)

	lib.ConnectMQTTBroker()

	watchdog.RegisterStopFunc(func() error {
		lib.CloseMQTTConnection()
		return nil
	})

	go api.CreateServer()

	watchdog.Start()

	ec = watchdog.Join()
}
