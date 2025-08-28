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
		lib.GetLogger().Debug("error loading .env file")
	}
	wd := watchdog.New(syscall.SIGINT, syscall.SIGTERM)

	err = lib.ConnectMQTTBroker()
	if err != nil {
		lib.GetLogger().Error("error connecting to mqtt broker", "error", err)
		ec = 1
		return
	}

	wd.RegisterStopFunc(func() error {
		lib.CloseMQTTConnection()
		return nil
	})

	go api.CreateServer()

	wd.Start()

	ec = wd.Join()
}
