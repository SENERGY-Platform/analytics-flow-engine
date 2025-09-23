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
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/config"
	devicemanager_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/device-manager-api"
	kafka2mqtt_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/kafka2mqtt-api"
	kubernetes_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/kubernetes-api"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/lib"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/parsing-api"
	permission_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/permission-api"
	rancher2_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/rancher2-api"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"
	gin_mw "github.com/SENERGY-Platform/gin-middleware"
	"github.com/SENERGY-Platform/go-service-base/struct-logger/attributes"
	"github.com/SENERGY-Platform/service-commons/pkg/jwt"
	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/requestid"
	"github.com/gin-gonic/gin"
	"net/http"
	"slices"
	"strconv"
	"strings"
)

func CreateServer(cfg *config.Config, pipelineService lib.PipelineApiService) (r *gin.Engine, err error) {
	var driver lib.Driver
	switch selectedDriver := cfg.Driver; selectedDriver {
	case "rancher":
		driver = rancher2_api.NewRancher2(
			cfg.Rancher2.Endpoint,
			cfg.Rancher2.AccessKey,
			cfg.Rancher2.SecretKey,
			cfg.Rancher2.StackId,
			&cfg.Rancher2,
		)
		break
	default:
		driver, err = kubernetes_api.NewKubernetes(&cfg.Rancher2, cfg.Debug)
		if err != nil {
			util.Logger.Error("Error creating driver", "error", err)
			return
		}
	}

	parser := parsing_api.NewParsingApi(cfg.ParserApiEndpoint)
	permission := permission_api.NewPermissionApi(cfg.PermissionApiEndpoint)
	kafka2mqtt := kafka2mqtt_api.NewKafka2MqttApi(cfg.Kafka2MqttApiEndpoint, &cfg.Mqtt)
	deviceManager := devicemanager_api.NewDeviceManagerApi(cfg.DeviceManagerApiEndpoint)
	flowEngine := lib.NewFlowEngine(driver, parser, permission, kafka2mqtt, deviceManager, pipelineService)

	port := strconv.FormatInt(int64(cfg.ServerPort), 10)
	util.Logger.Info("Starting api server at port " + port)
	if !cfg.Debug {
		gin.SetMode(gin.ReleaseMode)
	}
	r = gin.New()
	r.RedirectTrailingSlash = false
	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "DELETE", "OPTIONS", "PUT"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
	}))
	var middleware []gin.HandlerFunc
	middleware = append(
		middleware,
		gin_mw.StructLoggerHandlerWithDefaultGenerators(
			util.Logger.With(attributes.LogRecordTypeKey, attributes.HttpAccessLogRecordTypeVal),
			attributes.Provider,
			[]string{HealthCheckPath},
			nil,
		),
	)
	middleware = append(middleware,
		requestid.New(requestid.WithCustomHeaderStrKey(HeaderRequestID)),
		gin_mw.StructRecoveryHandler(util.Logger, gin_mw.DefaultRecoveryFunc),
		AuthMiddleware(),
	)
	r.Use(middleware...)
	r.UseRawPath = true
	prefix := r.Group(cfg.URLPrefix)

	prefix.GET(HealthCheckPath, func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	prefix.GET("/pipeline/:id", func(c *gin.Context) {
		id := c.Param("id")
		pipelineStatus, err := flowEngine.GetPipelineStatus(id, c.GetString(UserIdKey), c.GetHeader("Authorization"))
		if err != nil {
			util.Logger.Error("could not get pipeline status", "error", err, "method", "GET", "path", "/pipeline/:id")
			c.JSON(http.StatusBadRequest, gin.H{"error": "something went wrong"})
			return
		}
		c.JSON(http.StatusOK, pipelineStatus)
	})
	prefix.POST("/pipelines", func(c *gin.Context) {
		var request lib.PipelineStatusRequest
		if err := c.ShouldBindJSON(&request); err != nil {
			util.Logger.Error("error parsing request", "error", err, "method", "POST", "path", "/pipelines")
			c.JSON(http.StatusBadRequest, gin.H{"error": "error parsing request"})
			return
		}
		pipelinesStatus, err := flowEngine.GetPipelinesStatus(request.Ids, c.GetString(UserIdKey), c.GetHeader("Authorization"))
		if err != nil {
			util.Logger.Error("could not get pipelines status", "error", err, "method", "POST", "path", "/pipelines")
			c.JSON(http.StatusBadRequest, gin.H{"error": "something went wrong"})
			return
		}
		c.JSON(http.StatusOK, pipelinesStatus)
	})

	prefix.POST("/pipeline", func(c *gin.Context) {
		var request lib.PipelineRequest
		if err := c.ShouldBindJSON(&request); err != nil {
			util.Logger.Error("error parsing request", "error", err, "method", "POST", "path", "/pipeline")
			c.JSON(http.StatusBadRequest, gin.H{"error": "error parsing request"})
			return
		}
		pipe, err := flowEngine.StartPipeline(request, c.GetString(UserIdKey), c.GetHeader("Authorization"))
		if err != nil {
			util.Logger.Error("could not start pipeline", "error", err, "method", "POST", "path", "/pipeline")
			c.JSON(http.StatusBadRequest, gin.H{"error": "something went wrong"})
			return
		}
		c.JSON(http.StatusOK, pipe)
	})

	prefix.PUT("/pipeline", func(c *gin.Context) {
		var request lib.PipelineRequest
		if err := c.ShouldBindJSON(&request); err != nil {
			util.Logger.Error("error parsing request", "error", err, "method", "PUT", "path", "/pipeline")
			c.JSON(http.StatusBadRequest, gin.H{"error": "error parsing request"})
			return
		}
		pipe, err := flowEngine.UpdatePipeline(request, c.GetString(UserIdKey), c.GetHeader("Authorization"))
		if err != nil {
			util.Logger.Error("could not update pipeline", "error", err, "method", "PUT", "path", "/pipeline")
			c.JSON(http.StatusBadRequest, gin.H{"error": "something went wrong"})
			return
		}
		c.JSON(http.StatusOK, pipe)
	})

	prefix.DELETE("/pipeline/:id", func(c *gin.Context) {
		id := c.Param("id")
		err := flowEngine.DeletePipeline(id, c.GetString(UserIdKey), c.GetHeader("Authorization"))
		if err != nil {
			util.Logger.Error("could not delete pipeline", "error", err, "method", "DELETE", "path", "/pipeline/:id")
			c.JSON(http.StatusBadRequest, gin.H{"error": "something went wrong"})
			return
		}
		c.Status(http.StatusNoContent)
	})
	return r, nil
}

func AuthMiddleware() gin.HandlerFunc {
	return func(gc *gin.Context) {
		userId, err := getUserId(gc)
		if err != nil {
			util.Logger.Error("could not get user id", "error", err)
			gc.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}
		gc.Set(UserIdKey, userId)
		gc.Next()
	}
}

func getUserId(c *gin.Context) (userId string, err error) {
	forUser := c.Query("for_user")
	if forUser != "" {
		roles := strings.Split(c.GetHeader("X-User-Roles"), ", ")
		if slices.Contains[[]string](roles, "admin") {
			return forUser, nil
		}
	}

	userId = c.GetHeader("X-UserId")
	if userId == "" {
		if c.GetHeader("Authorization") != "" {
			var claims jwt.Token
			claims, err = jwt.Parse(c.GetHeader("Authorization"))
			if err != nil {
				return
			}
			userId = claims.Sub
		}
	}
	return
}
