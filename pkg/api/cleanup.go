/*
 * Copyright 2026 InfAI (CC SES)
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
	"context"
	"errors"
	"net/http"

	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/service"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"
	gin_mw "github.com/SENERGY-Platform/gin-middleware"
	"github.com/SENERGY-Platform/service-commons/pkg/jwt"
	"github.com/gin-gonic/gin"
)

// CleanupService is what the admin cleanup routes need of service.Cleanup.
type CleanupService interface {
	OrphanPipelines(ctx context.Context) ([]service.OrphanPipeline, error)
	DeleteOrphanPipeline(ctx context.Context, pipelineId string) error
	DeleteOrphanPipelines(ctx context.Context) (service.OrphanPipelinesDeleteResult, error)
	OrphanTopics(ctx context.Context) ([]string, error)
	DeleteOrphanTopic(ctx context.Context, topic string) error
	StartTopicJob(ctx context.Context) error
	TopicJob() service.TopicCleanupJob
	StopTopicJob(ctx context.Context) error
}

var routesCleanup = gin_mw.Routes[CleanupService]{
	getOrphanPipelines,
	deleteOrphanPipeline,
	deleteOrphanPipelines,
	getOrphanTopics,
	deleteOrphanTopic,
	postTopicJob,
	getTopicJob,
	deleteTopicJob,
}

// setCleanupRoutes mounts the cleanup routes behind RequireAdmin.
func setCleanupRoutes(prefix *gin.RouterGroup, cleanup CleanupService) ([][2]string, error) {
	return routesCleanup.Set(cleanup, prefix.Group(CleanupPath, RequireAdmin()))
}

// RequireAdmin lets only holders of the realm role admin through. It reads the role
// from the token, whose signature the gateway checked, and never from X-User-Roles.
func RequireAdmin() gin.HandlerFunc {
	return func(gc *gin.Context) {
		token, err := jwt.Parse(gc.GetHeader("Authorization"))
		if err != nil {
			util.Logger.WarnContext(gc.Request.Context(), "admin route without a readable token", "error", err, "path", gc.FullPath())
			gc.String(http.StatusUnauthorized, MessageUnauthorized)
			gc.Abort()
			return
		}
		if !token.IsAdmin() {
			util.Logger.WarnContext(gc.Request.Context(), "admin route refused", "user", token.GetUserId(), "path", gc.FullPath())
			gc.String(http.StatusForbidden, MessageForbidden)
			gc.Abort()
			return
		}
		gc.Next()
	}
}

// cleanupError answers a failed cleanup call. A registry that cannot be trusted gives
// 503 with a reason the admin can act on; the cause stays in the log.
func cleanupError(c *gin.Context, msg string, err error) {
	ctx := c.Request.Context()
	var unavailable *service.RegistryUnavailableError
	switch {
	case errors.As(err, &unavailable):
		util.Logger.ErrorContext(ctx, msg, "error", err, "path", c.FullPath())
		c.String(http.StatusServiceUnavailable, unavailable.Reason)
	case errors.Is(err, service.ErrTopicJobRunning), errors.Is(err, service.ErrNoTopicJob):
		util.Logger.WarnContext(ctx, msg, "error", err, "path", c.FullPath())
		c.String(http.StatusConflict, err.Error())
	default:
		util.Logger.Log(ctx, errorLevel(err), msg, "error", err, "path", c.FullPath())
		_ = c.Error(handleError(err))
	}
}

// getOrphanPipelines godoc
// @Summary List orphaned pipelines
// @Description	Lists pipeline ids with objects in the pipelines namespace and no registry entry, whose newest object is at least cleanup_min_age old. Admin only.
// @Tags Cleanup
// @Produce json
// @Success	200 {array} service.OrphanPipeline
// @Failure	401 {string} MessageUnauthorized
// @Failure	403 {string} MessageForbidden
// @Failure	500 {string} MessageSomethingWrong
// @Failure	503 {string} string "registry unavailable or empty"
// @Router /admin/cleanup/pipelines [get]
func getOrphanPipelines(cleanup CleanupService) (string, string, gin.HandlerFunc) {
	return http.MethodGet, CleanupPipelinesPath, func(c *gin.Context) {
		orphans, err := cleanup.OrphanPipelines(c.Request.Context())
		if err != nil {
			cleanupError(c, "could not list orphaned pipelines", err)
			return
		}
		c.JSON(http.StatusOK, orphans)
	}
}

// deleteOrphanPipeline godoc
// @Summary Delete an orphaned pipeline
// @Description	Deletes the objects of one pipeline after checking again that it is orphaned. Admin only.
// @Tags Cleanup
// @Param id path string true "Pipeline ID"
// @Success	204
// @Failure	401 {string} MessageUnauthorized
// @Failure	403 {string} MessageForbidden
// @Failure	404 {string} MessageNotFound
// @Failure	500 {string} MessageSomethingWrong
// @Failure	503 {string} string "registry unavailable or empty"
// @Router /admin/cleanup/pipelines/{id} [delete]
func deleteOrphanPipeline(cleanup CleanupService) (string, string, gin.HandlerFunc) {
	return http.MethodDelete, CleanupPipelinePath, func(c *gin.Context) {
		if err := cleanup.DeleteOrphanPipeline(c.Request.Context(), c.Param("id")); err != nil {
			cleanupError(c, "could not delete orphaned pipeline", err)
			return
		}
		c.Status(http.StatusNoContent)
	}
}

// deleteOrphanPipelines godoc
// @Summary Delete all orphaned pipelines
// @Description	Deletes the objects of every orphaned pipeline and reports which failed. Admin only.
// @Tags Cleanup
// @Produce json
// @Success	200 {object} service.OrphanPipelinesDeleteResult
// @Failure	401 {string} MessageUnauthorized
// @Failure	403 {string} MessageForbidden
// @Failure	500 {string} MessageSomethingWrong
// @Failure	503 {string} string "registry unavailable or empty"
// @Router /admin/cleanup/pipelines [delete]
func deleteOrphanPipelines(cleanup CleanupService) (string, string, gin.HandlerFunc) {
	return http.MethodDelete, CleanupPipelinesPath, func(c *gin.Context) {
		result, err := cleanup.DeleteOrphanPipelines(c.Request.Context())
		if err != nil {
			cleanupError(c, "could not delete orphaned pipelines", err)
			return
		}
		c.JSON(http.StatusOK, result)
	}
}

// getOrphanTopics godoc
// @Summary List orphaned Kafka topics
// @Description	Lists the internal Kafka Streams topics of application ids that no registered pipeline and no deployment uses. Admin only.
// @Tags Cleanup
// @Produce json
// @Success	200 {array} string
// @Failure	401 {string} MessageUnauthorized
// @Failure	403 {string} MessageForbidden
// @Failure	500 {string} MessageSomethingWrong
// @Failure	503 {string} string "registry unavailable or empty"
// @Router /admin/cleanup/topics [get]
func getOrphanTopics(cleanup CleanupService) (string, string, gin.HandlerFunc) {
	return http.MethodGet, CleanupTopicsPath, func(c *gin.Context) {
		topics, err := cleanup.OrphanTopics(c.Request.Context())
		if err != nil {
			cleanupError(c, "could not list orphaned topics", err)
			return
		}
		c.JSON(http.StatusOK, topics)
	}
}

// deleteOrphanTopic godoc
// @Summary Delete an orphaned Kafka topic
// @Description	Deletes one topic after checking again that it is orphaned. Admin only.
// @Tags Cleanup
// @Param topic path string true "Topic name"
// @Success	204
// @Failure	401 {string} MessageUnauthorized
// @Failure	403 {string} MessageForbidden
// @Failure	404 {string} MessageNotFound
// @Failure	500 {string} MessageSomethingWrong
// @Failure	503 {string} string "registry unavailable or empty"
// @Router /admin/cleanup/topics/{topic} [delete]
func deleteOrphanTopic(cleanup CleanupService) (string, string, gin.HandlerFunc) {
	return http.MethodDelete, CleanupTopicPath, func(c *gin.Context) {
		if err := cleanup.DeleteOrphanTopic(c.Request.Context(), c.Param("topic")); err != nil {
			cleanupError(c, "could not delete orphaned topic", err)
			return
		}
		c.Status(http.StatusNoContent)
	}
}

// postTopicJob godoc
// @Summary Start deleting all orphaned Kafka topics
// @Description	Starts a background job that deletes every orphaned topic, one per second. Admin only.
// @Tags Cleanup
// @Success	202
// @Failure	401 {string} MessageUnauthorized
// @Failure	403 {string} MessageForbidden
// @Failure	409 {string} string "a job is already running"
// @Failure	500 {string} MessageSomethingWrong
// @Failure	503 {string} string "registry unavailable or empty"
// @Router /admin/cleanup/topics/job [post]
func postTopicJob(cleanup CleanupService) (string, string, gin.HandlerFunc) {
	return http.MethodPost, CleanupTopicJobPath, func(c *gin.Context) {
		if err := cleanup.StartTopicJob(c.Request.Context()); err != nil {
			cleanupError(c, "could not start the topic cleanup job", err)
			return
		}
		c.Status(http.StatusAccepted)
	}
}

// getTopicJob godoc
// @Summary State of the topic cleanup job
// @Description	The running or the last topic cleanup job. Admin only.
// @Tags Cleanup
// @Produce json
// @Success	200 {object} service.TopicCleanupJob
// @Failure	401 {string} MessageUnauthorized
// @Failure	403 {string} MessageForbidden
// @Router /admin/cleanup/topics/job [get]
func getTopicJob(cleanup CleanupService) (string, string, gin.HandlerFunc) {
	return http.MethodGet, CleanupTopicJobPath, func(c *gin.Context) {
		c.JSON(http.StatusOK, cleanup.TopicJob())
	}
}

// deleteTopicJob godoc
// @Summary Stop the topic cleanup job
// @Description	Stops the running job after the delete in progress. Admin only.
// @Tags Cleanup
// @Success	204
// @Failure	401 {string} MessageUnauthorized
// @Failure	403 {string} MessageForbidden
// @Failure	409 {string} string "no job is running"
// @Router /admin/cleanup/topics/job [delete]
func deleteTopicJob(cleanup CleanupService) (string, string, gin.HandlerFunc) {
	return http.MethodDelete, CleanupTopicJobPath, func(c *gin.Context) {
		if err := cleanup.StopTopicJob(c.Request.Context()); err != nil {
			cleanupError(c, "could not stop the topic cleanup job", err)
			return
		}
		c.Status(http.StatusNoContent)
	}
}
