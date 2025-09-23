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
	"fmt"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/api"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/config"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/lib"
	pipeline_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/pipeline-api"
	"github.com/SENERGY-Platform/go-service-base/srv-info-hdl"
	"github.com/SENERGY-Platform/go-service-base/watchdog"

	"os"
	"syscall"
)

var version = "dev"

func main() {
	ec := 0
	defer func() {
		os.Exit(ec)
	}()

	_ = srv_info_hdl.New("analytics-flow-engine", version)

	config.ParseFlags()

	cfg, err := config.New(config.ConfPath)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		ec = 1
		return
	}

	wd := watchdog.New(syscall.SIGINT, syscall.SIGTERM)

	pipelineService := pipeline_api.NewPipelineApi(cfg.PipelineApiEndpoint)

	err = lib.ConnectMQTTBroker(cfg.Mqtt, pipelineService)
	if err != nil {
		lib.GetLogger().Error("error connecting to mqtt broker", "error", err)
		ec = 1
		return
	}

	wd.RegisterStopFunc(func() error {
		lib.CloseMQTTConnection()
		return nil
	})

	err = api.CreateServer(cfg, pipelineService)
	if err != nil {
		lib.GetLogger().Error("error starting server", "error", err)
		ec = 1
		return
	}

	wd.Start()

	ec = wd.Join()
}
