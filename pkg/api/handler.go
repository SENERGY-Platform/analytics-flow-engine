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
	"errors"
	"fmt"
	devicemanager_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/device-manager-api"
	kafka2mqtt_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/kafka2mqtt-api"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/lib"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/parsing-api"
	permission_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/permission-api"
	rancher2_api "github.com/SENERGY-Platform/analytics-flow-engine/pkg/rancher2-api"
	"github.com/SENERGY-Platform/service-commons/pkg/jwt"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"log"
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
	flowEngine := lib.NewFlowEngine(driver, parser, permission, kafka2mqtt, deviceManager)

	port := lib.GetEnv("API_PORT", "8000")
	log.Println("Starting Server at port " + port + "\n")
	DEBUG, err := strconv.ParseBool(lib.GetEnv("DEBUG", "false"))
	if err != nil {
		log.Print("Error loading debug value")
		DEBUG = false
	}
	if !DEBUG {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.Default()
	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "DELETE", "OPTIONS", "PUT"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
	}))
	prefix := r.Group(lib.GetEnv("ROUTE_PREFIX", ""))
	prefix.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
	})
	prefix.GET("/pipeline/:id", func(c *gin.Context) {
		id := c.Param("id")
		pipelineStatus, err := flowEngine.GetPipelineStatus(id, getUserId(c), c.GetHeader("Authorization"))
		if err != nil {
			log.Println(err)
			return
		}
		c.JSON(http.StatusOK, pipelineStatus)
	})
	prefix.POST("/pipelines", func(c *gin.Context) {
		var request lib.PipelineStatusRequest
		if err := c.ShouldBindJSON(&request); err != nil {
			log.Println(err.Error())
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		pipelinesStatus, err := flowEngine.GetPipelinesStatus(request.Ids, getUserId(c), c.GetHeader("Authorization"))
		if err != nil {
			log.Println(err)
			return
		}
		c.JSON(http.StatusOK, pipelinesStatus)
	})

	prefix.POST("/pipeline", func(c *gin.Context) {
		var request lib.PipelineRequest
		if err := c.ShouldBindJSON(&request); err != nil {
			log.Println(err.Error())
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		pipe, err := flowEngine.StartPipeline(request, getUserId(c), c.GetHeader("Authorization"))
		if err != nil {
			log.Println(err)
			return
		}
		c.JSON(http.StatusOK, pipe)
	})

	prefix.PUT("/pipeline", func(c *gin.Context) {
		var request lib.PipelineRequest
		if err := c.ShouldBindJSON(&request); err != nil {
			log.Println(err.Error())
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		pipe, err := flowEngine.UpdatePipeline(request, getUserId(c), c.GetHeader("Authorization"))
		if err != nil {
			log.Println(err)
			return
		}
		c.JSON(http.StatusOK, pipe)
	})

	prefix.DELETE("/pipeline/:id", func(c *gin.Context) {
		id := c.Param("id")
		err := flowEngine.DeletePipeline(id, getUserId(c), c.GetHeader("Authorization"))
		if err != nil {
			log.Println(err)
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.Status(http.StatusNoContent)
	})

	if !DEBUG {
		err = r.Run(":" + port)
	} else {
		err = r.Run("127.0.0.1:" + port)
	}
	if err == nil {
		fmt.Printf("Starting api server failed: %s \n", err)
	}
}

func getUserId(c *gin.Context) (userId string) {
	forUser := c.Query("for_user")
	if forUser != "" {

		roles := strings.Split(c.GetHeader("X-User-Roles"), ", ")
		if slices.Contains[[]string](roles, "admin") {
			return forUser
		}
	}

	userId = c.GetHeader("X-UserId")
	if userId == "" {
		if c.GetHeader("Authorization") != "" {
			claims, err := jwt.Parse(c.GetHeader("Authorization"))
			if err != nil {
				err = errors.New("Error parsing token: " + err.Error())
				return
			}
			userId = claims.Sub
			if userId == "" {
				userId = "dummy"
			}
		}
	}
	return
}
