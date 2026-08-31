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
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/api"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/config"
	pipeline_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/pipeline-api"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/service"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"
	"github.com/SENERGY-Platform/go-service-base/srv-info-hdl"
	"github.com/SENERGY-Platform/go-service-base/struct-logger/attributes"
	sb_util "github.com/SENERGY-Platform/go-service-base/util"
)

var Version = "{version}"

func main() {
	ec := 0
	defer func() {
		os.Exit(ec)
	}()

	srvInfoHdl := srv_info_hdl.New(api.ServiceName, Version)

	config.ParseFlags()

	cfg, err := config.New(config.ConfPath)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		ec = 1
		return
	}

	util.InitStructLogger(cfg.Logger.Level)

	// The process context. Created here rather than further down because the
	// OpenTelemetry setup is tied to the process lifetime and because everything
	// below logs against it.
	ctx, cf := context.WithCancel(context.Background())

	util.Logger.InfoContext(ctx, srvInfoHdl.Name(), "version", srvInfoHdl.Version())
	util.Logger.InfoContext(ctx, "config: "+sb_util.ToJsonStr(cfg))

	pipelineService := pipeline_api.NewPipelineApi(cfg.PipelineApiEndpoint)

	err = service.ConnectMQTTBroker(ctx, cfg.Mqtt, pipelineService)
	if err != nil {
		util.Logger.ErrorContext(ctx, "error connecting to mqtt broker", "error", err)
		ec = 1
		return
	}

	httpHandler, err := api.CreateServer(ctx, cfg, pipelineService)
	if err != nil {
		util.Logger.ErrorContext(ctx, "error creating http engine", "error", err)
		ec = 1
		return
	}

	bindAddress := ":" + strconv.FormatInt(int64(cfg.ServerPort), 10)

	if cfg.Debug {
		bindAddress = "127.0.0.1:" + strconv.FormatInt(int64(cfg.ServerPort), 10)
	}

	httpServer := &http.Server{
		Addr:    bindAddress,
		Handler: httpHandler}

	go func() {
		util.Wait(ctx, util.Logger, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
		cf()
	}()

	wg := &sync.WaitGroup{}

	wg.Add(1)

	go func() {
		defer wg.Done()
		util.Logger.InfoContext(ctx, "starting http server")
		if err = httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			util.Logger.ErrorContext(ctx, "starting server failed", attributes.ErrorKey, err)
			ec = 1
		}
		cf()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		util.Logger.InfoContext(ctx, "stopping http server")
		ctxWt, cf2 := context.WithTimeout(context.Background(), time.Second*5)
		defer cf2()
		if err := httpServer.Shutdown(ctxWt); err != nil {
			util.Logger.ErrorContext(ctx, "stopping server failed", attributes.ErrorKey, err)
			ec = 1
		} else {
			util.Logger.InfoContext(ctx, "http server stopped")
		}

		util.Logger.InfoContext(ctx, "closing mqtt connection")
		service.CloseMQTTConnection()
		util.Logger.InfoContext(ctx, "mqtt connection closed")
	}()

	wg.Wait()
}
