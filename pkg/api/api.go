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
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib/exports"
	analytics_serving_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/analytics-serving-api"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/config"
	devicemanager_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/device-manager-api"
	kafka2mqtt_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/kafka2mqtt-api"
	kubernetes_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/kubernetes-api"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/parsing-api"
	permission_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/permission-api"
	rancher2_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/rancher2-api"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/service"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"
	gin_mw "github.com/SENERGY-Platform/gin-middleware"
	"github.com/SENERGY-Platform/gin-middleware/otelx"
	"github.com/SENERGY-Platform/go-service-base/struct-logger/attributes"
	"github.com/SENERGY-Platform/service-commons/pkg/jwt"
	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/requestid"
	"github.com/gin-gonic/gin"
)

// CreateServer godoc
// @title Analytics-Flow-Engine API
// @version {version}
// @description For the administration of analytics pipelines.
// @license.name Apache-2.0
// @license.url http://www.apache.org/licenses/LICENSE-2.0.html
// @BasePath /
func CreateServer(ctx context.Context, cfg *config.Config, pipelineService service.PipelineApiService) (r *gin.Engine, err error) {
	// Before anything else, and in particular before NewFlowEngine: its constructor
	// runs the startup sync, which makes outgoing calls. Until this has run, the
	// global propagator is the no-op one and those calls would carry neither a
	// traceparent nor a baggage header.
	otelHandler, err := otelx.GinOpenTelemetry(ctx, ServiceName, cfg.OtelEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to set up OpenTelemetry: %w", err)
	}

	var driver service.Driver
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
			util.Logger.ErrorContext(ctx, "Error creating driver", "error", err)
			return
		}
	}

	parser := parsing_api.NewParsingApi(cfg.ParserApiEndpoint)
	permission := permission_api.NewPermissionApi(cfg.PermissionApiEndpoint)
	kafka2mqtt := kafka2mqtt_api.NewKafka2MqttApi(cfg.Kafka2MqttApiEndpoint, &cfg.Mqtt)
	deviceManager := devicemanager_api.NewDeviceManagerApi(cfg.DeviceManagerApiEndpoint)
	// A nil interface, not a nil *AnalyticsServingApi: the engine tests the interface
	// against nil to tell the two modes apart.
	var exportLister exports.Lister
	if cfg.AnalyticsServingApiEndpoint != "" {
		exportLister = analytics_serving_api.NewAnalyticsServingApi(cfg.AnalyticsServingApiEndpoint)
		util.Logger.InfoContext(ctx, "import exports: resolving the history of imports from analytics-serving exports")
	} else {
		util.Logger.InfoContext(ctx, "import exports: analytics_serving_api_endpoint is empty, operators read imports from Kafka")
	}
	flowEngine := service.NewFlowEngine(driver, parser, permission, kafka2mqtt, deviceManager, pipelineService, cfg.TimescaleConnection, exportLister)

	port := strconv.FormatInt(int64(cfg.ServerPort), 10)
	util.Logger.InfoContext(ctx, "Starting api server at port "+port)
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
		// First in the chain: it extracts the trace context and the baggage off the
		// request and puts them into the request context, which everything after it —
		// the access log, the handlers, the engine — reads from.
		otelHandler,
		// Directly after it, and it has to stay there: see DiscardBaggageErrors.
		DiscardBaggageErrors(),
		gin_mw.StructLoggerHandlerWithDefaultGenerators(
			util.Logger.With(attributes.LogRecordTypeKey, attributes.HttpAccessLogRecordTypeVal),
			attributes.Provider,
			[]string{HealthCheckPath},
			nil,
		),
	)
	middleware = append(middleware,
		requestid.New(requestid.WithCustomHeaderStrKey(HeaderRequestID)),
		gin_mw.ErrorHandler(util.GetStatusCode, ", "),
		gin_mw.StructRecoveryHandler(util.Logger, gin_mw.DefaultRecoveryFunc),
	)
	r.Use(middleware...)
	r.UseRawPath = true
	prefix := r.Group(cfg.URLPrefix)
	setRoutes, err := routes.Set(*flowEngine, prefix)
	if err != nil {
		return nil, err
	}
	for _, route := range setRoutes {
		util.Logger.DebugContext(ctx, "http route", attributes.MethodKey, route[0], attributes.PathKey, route[1])
	}
	prefix.Use(AuthMiddleware())
	setRoutes, err = routesAuth.Set(*flowEngine, prefix)
	if err != nil {
		return nil, err
	}
	for _, route := range setRoutes {
		util.Logger.DebugContext(ctx, "http route", attributes.MethodKey, route[0], attributes.PathKey, route[1])
	}
	return r, nil
}

// DiscardBaggageErrors takes the errors the OpenTelemetry handler reported off the
// request and logs them instead.
//
// otelx reports a baggage value it cannot carry — one holding a space, a comma or a
// non-ASCII character — with gin's c.Error. gin_mw.ErrorHandler then turns anything
// in c.Errors into a response: it forces a 500 where the status was below 400, and
// appends the error text to the body. A user whose Keycloak username is "Jonah
// Windolph" would therefore get a 500 on every DELETE and a corrupted JSON body on
// every GET, for a log annotation that failed.
//
// **This handler has to sit immediately after the OpenTelemetry handler.** otelx adds
// those errors before it calls c.Next(), so at this point in the chain nothing else
// can have added one, which is what makes clearing the slice safe. Moved further
// down, it would discard a real handler's error.
//
// The proper fix belongs in gin-middleware, which should not use c.Error for
// something that is not a request error.
func DiscardBaggageErrors() gin.HandlerFunc {
	return func(gc *gin.Context) {
		if len(gc.Errors) > 0 {
			for _, reported := range gc.Errors {
				util.Logger.WarnContext(gc.Request.Context(),
					"could not put a value into the request baggage", "error", reported.Err)
			}
			gc.Errors = nil
		}
		gc.Next()
	}
}

func AuthMiddleware() gin.HandlerFunc {
	return func(gc *gin.Context) {
		userId, err := getUserId(gc)
		if err != nil {
			util.Logger.ErrorContext(gc.Request.Context(), "could not get user id", "error", err)
			gc.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		gc.Set(UserIdKey, userId)
		gc.Next()
	}
}

func getUserId(c *gin.Context) (userId string, err error) {
	forUser := c.Query("for_user")
	if forUser != "" {
		if !isValidUserId(forUser) {
			util.Logger.WarnContext(c.Request.Context(), "invalid for_user format",
				"for_user", forUser,
				"requester", userId)
			return "", errors.New("invalid user ID format")
		}

		roles := strings.Split(c.GetHeader("X-User-Roles"), ", ")
		if slices.Contains[[]string](roles, "admin") {
			util.Logger.InfoContext(c.Request.Context(), "user_impersonation",
				"admin_user", userId,
				"target_user", forUser,
				"path", c.Request.URL.Path,
				"method", c.Request.Method,
				"ip", c.ClientIP(),
				"request_id", c.GetHeader(HeaderRequestID))
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
		} else {
			err = errors.New("missing authorization and x-userid header")
		}
	}
	return
}

func isValidUserId(id string) bool {
	if len(id) == 0 || len(id) > 64 {
		return false
	}
	matched, _ := regexp.MatchString(`^[a-zA-Z0-9_-]+$`, id)
	return matched
}
