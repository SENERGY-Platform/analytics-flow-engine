/*
 * Copyright 2025 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
	"net/http"
	"os"

	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/lib"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"
	"github.com/gin-gonic/gin"
)

// getPipeline godoc
// @Summary Get pipeline status
// @Description	Gets a single pipeline status
// @Tags Pipeline
// @Produce json
// @Param id path string true "Pipeline ID"
// @Success	200 {object} lib.PipelineStatus
// @Failure	500 {string} str
// @Router /pipeline/{id} [get]
func getPipeline(flowEngine lib.FlowEngine) (string, string, gin.HandlerFunc) {
	return http.MethodGet, PipelineIdPath, func(c *gin.Context) {
		id := c.Param("id")
		pipelineStatus, err := flowEngine.GetPipelineStatus(id, c.GetString(UserIdKey), c.GetHeader("Authorization"))
		if err != nil {
			util.Logger.Error("could not get pipeline status", "error", err, "method", "GET", "path", PipelineIdPath)
			_ = c.Error(errors.New(MessageSomethingWrong))
			return
		}
		c.JSON(http.StatusOK, pipelineStatus)
	}
}

// postPipelines godoc
// @Summary Get multiple pipelines status
// @Description	Gets multiple pipelines status
// @Tags Pipeline
// @Accept json
// @Produce json
// @Param request body lib.PipelineStatusRequest true "Pipeline status request"
// @Success	200 {array} lib.PipelineStatus
// @Failure	500 {string} str
// @Router /pipelines [post]
func postPipelines(flowEngine lib.FlowEngine) (string, string, gin.HandlerFunc) {
	return http.MethodPost, PipelinesPath, func(c *gin.Context) {
		var request lib.PipelineStatusRequest
		if err := c.ShouldBindJSON(&request); err != nil {
			util.Logger.Error(MessageParseError, "error", err, "method", "POST", "path", PipelinesPath)
			_ = c.Error(errors.New(MessageSomethingWrong))
			return
		}
		pipelinesStatus, err := flowEngine.GetPipelinesStatus(request.Ids, c.GetString(UserIdKey), c.GetHeader("Authorization"))
		if err != nil {
			util.Logger.Error("could not get pipelines status", "error", err, "method", "POST", "path", PipelinesPath)
			_ = c.Error(errors.New(MessageSomethingWrong))
			return
		}
		c.JSON(http.StatusOK, pipelinesStatus)
	}
}

// postPipeline godoc
// @Summary Start a pipeline
// @Description	Starts a pipeline
// @Tags Pipeline
// @Produce json
// @Success	200 {object} lib.Pipeline
// @Failure	500 {string} str
// @Router /pipeline [post]
func postPipeline(flowEngine lib.FlowEngine) (string, string, gin.HandlerFunc) {
	return http.MethodPost, PipelinePath, func(c *gin.Context) {
		var request lib.PipelineRequest
		if err := c.ShouldBindJSON(&request); err != nil {
			util.Logger.Error(MessageParseError, "error", err, "method", "POST", "path", PipelinePath)
			_ = c.Error(errors.New(MessageSomethingWrong))
			return
		}
		pipe, err := flowEngine.StartPipeline(request, c.GetString(UserIdKey), c.GetHeader("Authorization"))
		if err != nil {
			util.Logger.Error("could not start pipeline", "error", err, "method", "POST", "path", PipelinePath)
			_ = c.Error(errors.New(MessageSomethingWrong))
			return
		}
		c.JSON(http.StatusOK, pipe)
	}
}

// putPipeline godoc
// @Summary Update a pipeline
// @Description	Updates a pipeline
// @Tags Pipeline
// @Produce json
// @Success	200 {object} lib.Pipeline
// @Failure	500 {string} str
// @Router /pipeline [put]
func putPipeline(flowEngine lib.FlowEngine) (string, string, gin.HandlerFunc) {
	return http.MethodPut, PipelinePath, func(c *gin.Context) {
		var request lib.PipelineRequest
		if err := c.ShouldBindJSON(&request); err != nil {
			util.Logger.Error(MessageParseError, "error", err, "method", "PUT", "path", PipelinePath)
			_ = c.Error(errors.New(MessageSomethingWrong))
			return
		}
		pipe, err := flowEngine.UpdatePipeline(request, c.GetString(UserIdKey), c.GetHeader("Authorization"))
		if err != nil {
			util.Logger.Error("could not update pipeline", "error", err, "method", "PUT", "path", PipelinePath)
			_ = c.Error(errors.New(MessageSomethingWrong))
			return
		}
		c.JSON(http.StatusOK, pipe)
	}
}

// deletePipeline godoc
// @Summary Delete pipeline
// @Description	Delete a single pipeline
// @Tags Pipeline
// @Param id path string true "Pipeline ID"
// @Success	204
// @Failure	500 {string} str
// @Router /pipeline/{id} [delete]
func deletePipeline(flowEngine lib.FlowEngine) (string, string, gin.HandlerFunc) {
	return http.MethodDelete, PipelineIdPath, func(c *gin.Context) {
		id := c.Param("id")
		err := flowEngine.DeletePipeline(id, c.GetString(UserIdKey), c.GetHeader("Authorization"))
		if err != nil {
			util.Logger.Error("could not delete pipeline", "error", err, "method", "DELETE", "path", PipelineIdPath)
			_ = c.Error(errors.New(MessageSomethingWrong))
			return
		}
		c.Status(http.StatusNoContent)
	}
}

func getHealthCheckH(_ lib.FlowEngine) (string, string, gin.HandlerFunc) {
	return http.MethodGet, HealthCheckPath, func(c *gin.Context) {
		c.Status(http.StatusOK)
	}
}

func getSwaggerDocH(_ lib.FlowEngine) (string, string, gin.HandlerFunc) {
	return http.MethodGet, "/doc", func(gc *gin.Context) {
		if _, err := os.Stat("docs/swagger.json"); err != nil {
			_ = gc.Error(err)
			return
		}
		gc.Header("Content-Type", gin.MIMEJSON)
		gc.File("docs/swagger.json")
	}
}
