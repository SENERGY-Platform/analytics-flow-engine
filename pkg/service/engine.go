/*
 * Copyright 2019 InfAI (CC SES)
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

package service

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"encoding/json"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	"github.com/SENERGY-Platform/analytics-flow-engine/lib/access"
	"github.com/SENERGY-Platform/analytics-flow-engine/lib/exports"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/baggage"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"
	parser "github.com/SENERGY-Platform/analytics-parser/lib"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
	"github.com/google/uuid"
	k8apierrors "k8s.io/apimachinery/pkg/api/errors"

	deploymentLocationLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/location"
	upstreamLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/upstream"
)

type FlowEngine struct {
	driver               Driver
	parsingService       ParsingApiService
	permissionService    PermissionApiService
	kafak2mqttService    Kafka2MqttApiService
	deviceManagerService DeviceManagerService
	pipelineService      PipelineApiService
	// timescaleConnection is handed to every cloud operator as ts_conn. Held here
	// rather than in the drivers because both of them build the same operator
	// config, and a fog operator must not receive it at all.
	timescaleConnection string
	// exportLister looks up the exports of imports. Nil when no analytics-serving
	// endpoint is configured; operators then read imports from Kafka.
	exportLister exports.Lister
}

func NewFlowEngine(
	driver Driver,
	parsingService ParsingApiService,
	permissionService PermissionApiService,
	kafak2mqttService Kafka2MqttApiService,
	deviceManagerService DeviceManagerService,
	pipelineService PipelineApiService,
	timescaleConnection string,
	exportLister exports.Lister) *FlowEngine {
	f := &FlowEngine{
		driver:               driver,
		parsingService:       parsingService,
		permissionService:    permissionService,
		kafak2mqttService:    kafak2mqttService,
		deviceManagerService: deviceManagerService,
		pipelineService:      pipelineService,
		timescaleConnection:  timescaleConnection,
		exportLister:         exportLister,
	}
	err := f.syncPipelines()
	if err != nil {
		util.Logger.Error("failed to sync pipelines", "error", err)
	}
	return f
}

// deploymentContext keeps the values of ctx — the trace and the baggage — but drops
// its cancellation.
//
// Applied once, at the top of each method that changes something, and to the whole
// method rather than to individual calls inside it. Starting, updating and deleting
// a pipeline each write to two places that have to agree: the Kubernetes cluster and
// the pipeline registry. A cancellation landing between them leaves them
// disagreeing, and the disagreements are not harmless:
//
//   - Delete: the deployment is gone, the registry entry is not, and the startup
//     sync recreates the deployment. A deleted pipeline comes back.
//   - Start: the operators run, but the registry never gets the fog topics, the
//     downstream instance ids or the baggage. Nothing repairs that.
//   - The rollback of a failed start, and the teardown of a forwarding instance,
//     have to run exactly when the request is already going wrong.
//
// Doing the work nobody is waiting for any more is the cheaper failure. The read
// paths — GetPipelineStatus, GetPipelinesStatus — stay cancellable, because an
// abandoned read costs nothing and leaves nothing behind.
func deploymentContext(ctx context.Context) context.Context {
	return context.WithoutCancel(ctx)
}

// checker binds ctx to the permission service.
//
// lib/access is a separate module, shared with the Operator Development
// Environment, and its Checker interface has no context. Rather than change that
// interface and break the other consumer, the context is bound here, so the calls
// it makes still carry the trace and the baggage.
func (f *FlowEngine) checker(ctx context.Context) access.Checker {
	return boundChecker{ctx: ctx, service: f.permissionService}
}

type boundChecker struct {
	ctx     context.Context
	service PermissionApiService
}

func (b boundChecker) UserHasExecuteAccess(resource string, ids []string, authorization string) (bool, error) {
	return b.service.UserHasExecuteAccess(b.ctx, resource, ids, authorization)
}

// syncPipelines runs at startup, outside any request. It recreates deployments the
// registry knows about but the cluster does not — including their baggage labels,
// which is the reason the baggage is stored on the pipeline rather than only read
// off the request that created it.
func (f *FlowEngine) syncPipelines() (err error) {
	// No request to inherit from, and nothing to cancel this: it runs once while the
	// service starts up, and it recreates deployments, so it is a write path too.
	startupCtx := context.Background()
	util.Logger.InfoContext(startupCtx, "syncing pipelines")
	pipelines, err := f.pipelineService.GetPipelinesAdmin(startupCtx)
	if err != nil {
		return err
	}
	statusTemp, err := f.driver.GetPipelinesStatus(startupCtx)
	if err != nil {
		return err
	}

	missing, extra := CompareSlicesWithKey(
		pipelines,
		statusTemp,
		func(a pipe.Pipeline) string { return a.Id },
		func(b lib.PipelineStatus) string { return strings.Replace(b.Name, "pipeline-", "", -1) },
	)
	if len(missing) > 0 {
		util.Logger.WarnContext(startupCtx, "found missing pipelines")
		for _, item := range missing {
			item.Image = ""
			// The pipeline's own stored baggage, so these lines carry the same context
			// as the ones the original request wrote. This is the case they are read in.
			ctx := baggage.WithStored(startupCtx, item.Baggage)
			util.Logger.WarnContext(ctx, "trying to recreate pipeline", "pipeline", item)
			//first delete every resource that might still be present
			err = f.stopOperators(ctx, item, "")
			if err != nil {
				util.Logger.ErrorContext(ctx, "cannot stop operators", "error", err)
				return
			}

			pipeConfig := f.createPipelineConfig(item)
			pipeConfig.UserId = item.UserId
			_, err := f.startOperators(ctx, item, pipeConfig, "")
			if err != nil {
				return fmt.Errorf("failed to start operators: %w", err)
			}
		}
	}

	if len(extra) > 0 {
		util.Logger.WarnContext(startupCtx, "found extra pipelines")
		for _, item := range extra {
			util.Logger.WarnContext(startupCtx, "extra deployment", "deployment", item)
		}
	}
	return
}

func (f *FlowEngine) StartPipeline(ctx context.Context, pipelineRequest lib.PipelineRequest, userId string, token string) (pipeline *pipe.Pipeline, err error) {
	ctx = deploymentContext(ctx)
	util.Logger.DebugContext(ctx, "engine - start pipeline: "+pipelineRequest.Id)
	pipeline, err = f.setupPipeline(ctx, pipelineRequest, userId, token)
	if err != nil {
		return
	}

	id, err := f.pipelineService.RegisterPipeline(ctx, pipeline, userId, token)
	if err != nil {
		return
	}
	pipeline.Id = id.String()

	// The pipeline id only exists now, so it joins the baggage here rather than in
	// the middleware. From this point on every log line of this request names the
	// pipeline it is about, and the operators are labelled with it too.
	ctx = withPipelineIdInBaggage(ctx, pipeline.Id)
	pipeline.Baggage = baggage.FromContext(ctx)

	pipeline.Operators = addPipelineIDToFogTopic(pipeline.Operators, pipeline.Id)
	pipeConfig := f.createPipelineConfig(*pipeline)
	pipeConfig.UserId = userId
	newOperators, err := f.startOperators(ctx, *pipeline, pipeConfig, token)
	if err != nil {
		// The operators too, not only the registry entry. startOperators can fail after
		// the deployment exists — while enabling the cloud-to-fog forwarding, say — and
		// removing just the registration would leave a deployment in the cluster that
		// nothing points at. The startup sync only logs those as "extra deployment".
		if stopErr := f.stopOperators(ctx, *pipeline, token); stopErr != nil {
			util.Logger.ErrorContext(ctx, "failed to roll back the started operators", "error", stopErr)
		}
		if delErr := f.pipelineService.DeletePipeline(ctx, pipeline.Id, userId, token); delErr != nil {
			util.Logger.ErrorContext(ctx, "failed to rollback pipeline registration", "error", delErr)
		}
		return
	}
	pipeline.Operators = newOperators
	err = f.pipelineService.UpdatePipeline(ctx, pipeline, userId, token) //update is needed to set correct fog output topics (with pipeline ID) and instance id for downstream config of fog operators
	if err != nil {
		return
	}
	util.Logger.DebugContext(ctx, "started pipeline: "+pipeline.Id, "pipeline", pipeline)
	return
}

// withPipelineIdInBaggage adds the pipeline id to the baggage of ctx.
//
// A failure here is logged rather than returned: the id is a uuid and cannot be
// rejected as a baggage value, and a pipeline that starts correctly must not be
// refused over a log annotation.
func withPipelineIdInBaggage(ctx context.Context, pipelineId string) context.Context {
	withId, err := baggage.WithValue(ctx, baggage.PipelineIdKey, pipelineId)
	if err != nil {
		util.Logger.WarnContext(ctx, "could not add the pipeline id to the baggage",
			"error", err, "pipelineId", pipelineId)
		return ctx
	}
	return withId
}

func (f *FlowEngine) UpdatePipeline(ctx context.Context, pipelineRequest lib.PipelineRequest, userId string, token string) (pipeline *pipe.Pipeline, err error) {
	ctx = withPipelineIdInBaggage(deploymentContext(ctx), pipelineRequest.Id)
	util.Logger.DebugContext(ctx, "engine - update pipeline: "+pipelineRequest.Id)
	oldPipeline, err := f.pipelineService.GetPipeline(ctx, pipelineRequest.Id, userId, token)
	if err != nil {
		return
	}

	pipeline, err = f.setupPipeline(ctx, pipelineRequest, userId, token)
	if err != nil {
		return
	}

	// The stored baggage is the base and this request's is laid over it: otelx adds
	// user_id and username to every request, so an update from any other caller
	// would otherwise silently drop a smart service instance id set at creation.
	pipeline.Baggage = baggage.Merge(oldPipeline.Baggage, baggage.FromContext(ctx))

	// If consume all messages is the same, we can reuse the application IDs
	if pipeline.ConsumeAllMessages == oldPipeline.ConsumeAllMessages {
		oldAppIds := make(map[string]uuid.UUID)
		for _, op := range oldPipeline.Operators {
			oldAppIds[op.Id] = op.ApplicationId
		}
		for i := range pipeline.Operators {
			if appId, exists := oldAppIds[pipeline.Operators[i].Id]; exists {
				pipeline.Operators[i].ApplicationId = appId
			}
		}
	}

	err = f.stopOperators(ctx, oldPipeline, token)
	if err != nil {
		util.Logger.ErrorContext(ctx, "cannot stop operators", "error", err)
		return
	}

	pipeline.Id = oldPipeline.Id
	pipeline.Operators = addPipelineIDToFogTopic(pipeline.Operators, pipeline.Id)
	pipeConfig := f.createPipelineConfig(*pipeline)
	pipeConfig.UserId = userId
	newOperators, err := f.startOperators(ctx, *pipeline, pipeConfig, token)
	if err != nil {
		util.Logger.ErrorContext(ctx, "failed to start new operators, attempting to restart old pipeline", "error", err)
		if _, err = f.startOperators(ctx, oldPipeline, f.createPipelineConfig(oldPipeline), token); err != nil {
			util.Logger.ErrorContext(ctx, "CRITICAL: failed to restart old pipeline", "error", err)
		}
		return nil, fmt.Errorf("failed to start operators: %w", err)
	}
	pipeline.Operators = newOperators
	err = f.pipelineService.UpdatePipeline(ctx, pipeline, userId, token)
	util.Logger.DebugContext(ctx, "updated pipeline: "+pipeline.Id, "pipeline", pipeline)
	return
}

func (f *FlowEngine) setupPipeline(ctx context.Context, pipelineRequest lib.PipelineRequest, userId, token string) (*pipe.Pipeline, error) {
	parsedPipeline, err := f.parsingService.GetPipeline(ctx, pipelineRequest.FlowId, userId, token)
	if err != nil {
		return nil, err
	}

	if err = f.checkAccess(ctx, pipelineRequest, parsedPipeline.Operators, token); err != nil {
		return nil, lib.NewForbiddenError(fmt.Errorf("checkAccess failed: %w", err))
	}

	pipeline := setPipelineModel(pipelineRequest, parsedPipeline)
	tmpPipeline := createOperatorConfig(parsedPipeline)

	configuredOperators, err := addOperatorConfigs(ctx, pipelineRequest, tmpPipeline, f.deviceManagerService, userId, token)
	if err != nil {
		return nil, err
	}
	pipeline.Operators = configuredOperators

	// Only here are the input topics final. checkAccess above authorized the flow
	// and the operators, which the request names directly; what a topic reads is
	// decided by the parser and by addOperatorConfigs together, so checking the
	// request would leave whatever those two add unchecked.
	if err = f.checkTopicAccess(ctx, pipeline.Operators, token); err != nil {
		return nil, lib.NewForbiddenError(fmt.Errorf("checkAccess failed: %w", err))
	}

	if err = f.setPlatformOperatorConfig(pipeline.Operators); err != nil {
		return nil, err
	}

	if err = f.setImportExports(ctx, pipeline.Operators, token); err != nil {
		return nil, err
	}

	return pipeline, nil
}

// setPlatformOperatorConfig writes the config values the platform owns into
// every cloud operator, overwriting whatever the flow carried.
//
// After the caller's own config rather than before it: the flow's node config is
// the user's, and a ts_conn out of it would point an operator at a database this
// deployment never chose.
//
// Fog operators are skipped deliberately. They run on hardware the platform does
// not own, and the DSN is a platform credential; an operator there that wants
// history fails naming ts_conn, which is the truthful outcome since it could not
// reach the database anyway.
func (f *FlowEngine) setPlatformOperatorConfig(operators []pipe.Operator) error {
	for i := range operators {
		if operators[i].DeploymentType == deploymentLocationLib.Local {
			continue
		}
		// Only reachable when a deployment blanks the setting deliberately: it has a
		// default. Refused anyway, because an operator started without a ts_conn does
		// not fail until it reads history, by which time the failure is a traceback in
		// a container log rather than a refused deployment.
		if f.timescaleConnection == "" {
			return lib.NewInternalError(errors.New(
				"engine - timescale_connection is set to an empty value, so a cloud operator " +
					"would start without a ts_conn and fail when it reads history"))
		}
		if operators[i].Config == nil {
			operators[i].Config = map[string]string{}
		}
		operators[i].Config[OperatorConfigTsConn] = f.timescaleConnection
	}
	return nil
}

// checkTopicAccess authorizes what the operators are about to read.
//
// Shared with the Operator Development Environment, which builds the same input
// topics from an experiment rather than from a pipeline request. Operator Lib
// reads whatever its topics name over a shared database credential and checks
// nothing itself, so this is the only place the rule is applied for a deployment.
func (f *FlowEngine) checkTopicAccess(ctx context.Context, operators []pipe.Operator, token string) error {
	internal := make([]string, 0, len(operators))
	for _, operator := range operators {
		internal = append(internal, operator.Id)
	}
	for _, operator := range operators {
		err := access.CheckTopics(f.checker(ctx), token, operator.InputTopics,
			access.Options{InternalOperatorIDs: internal})
		if err != nil {
			return fmt.Errorf("operator %s: %w", operator.Id, err)
		}
	}
	return nil
}

func (f *FlowEngine) DeletePipeline(ctx context.Context, id string, userId string, token string) (err error) {
	ctx = withPipelineIdInBaggage(deploymentContext(ctx), id)
	util.Logger.DebugContext(ctx, "engine - delete pipeline: "+id)
	pipeline, err := f.pipelineService.GetPipeline(ctx, id, userId, token)
	if err != nil {
		return
	}
	err = f.stopOperators(ctx, pipeline, token)
	if err != nil {
		if !k8apierrors.IsNotFound(err) {
			return
		}
	} else {
		util.Logger.DebugContext(ctx, "removed all operators for pipeline: "+id)
	}
	err = f.pipelineService.DeletePipeline(ctx, id, userId, token)
	if err != nil {
		return
	}
	return
}

func (f *FlowEngine) GetPipelineStatus(ctx context.Context, id, userId, token string) (status lib.PipelineStatus, err error) {
	ctx = withPipelineIdInBaggage(ctx, id)
	_, err = f.pipelineService.GetPipeline(ctx, id, userId, token)
	if err != nil {
		return
	}
	status, err = f.driver.GetPipelineStatus(ctx, id)
	return
}

func (f *FlowEngine) GetPipelinesStatus(ctx context.Context, ids []string, userId, token string) (status []lib.PipelineStatus, err error) {
	statusTemp, err := f.driver.GetPipelinesStatus(ctx)
	pipes, err := f.pipelineService.GetPipelines(ctx, userId, token)
	if err != nil {
		return
	}
	for _, stat := range statusTemp {
		idx := slices.IndexFunc(pipes, func(p pipe.Pipeline) bool { return "pipeline-"+p.Id == stat.Name })
		if idx != -1 {
			stat.Name = strings.Replace(stat.Name, "pipeline-", "", -1)
			status = append(status, stat)
		}
	}
	if len(ids) > 0 {
		statusTemp = status
		status = nil
		for _, id := range ids {
			idx := slices.IndexFunc(statusTemp, func(t lib.PipelineStatus) bool { return t.Name == id })
			if idx != -1 {
				status = append(status, statusTemp[idx])
			}
		}
	}
	return
}

func (f *FlowEngine) checkAccess(ctx context.Context, pipelineRequest lib.PipelineRequest, operators map[string]parser.Operator, token string) error {
	// What the request names directly. The ids its inputs name are authorized in
	// checkTopicAccess instead, against the parsed topics rather than the request,
	// because those are what the operators actually read.
	if err := access.Check(f.checker(ctx), token,
		access.ResourceFlows, []string{pipelineRequest.FlowId}); err != nil {
		return err
	}

	if len(operators) > 0 {
		operatorIds := make([]string, 0, len(operators))
		for _, op := range operators {
			operatorIds = append(operatorIds, op.OperatorId)
		}
		ok, err := f.permissionService.UserHasExecuteAccess(ctx, access.ResourceOperators, operatorIds, token)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("engine - user does not have the rights to execute one or more operators")
		}
	}

	return nil
}

func addPipelineIDToFogTopic(operators []pipe.Operator, pipelineId string) (newOperators []pipe.Operator) {
	// Input and Output Topics are set during parsing where pipeline ID is not available
	for _, operator := range operators {
		if operator.DeploymentType == deploymentLocationLib.Local {
			operator.OutputTopic = operator.OutputTopic + pipelineId

			var inputTopicsWithID []pipe.InputTopic
			for _, inputTopic := range operator.InputTopics {
				if inputTopic.FilterType == "OperatorId" {
					inputTopic.Name += pipelineId
				}
				inputTopicsWithID = append(inputTopicsWithID, inputTopic)
			}
			operator.InputTopics = inputTopicsWithID
		}
		newOperators = append(newOperators, operator)
	}
	return
}

func seperateOperators(pipeline pipe.Pipeline) (localOperators []pipe.Operator, cloudOperators []pipe.Operator) {
	for _, operator := range pipeline.Operators {
		switch operator.DeploymentType {
		case "local":
			localOperators = append(localOperators, operator)
			break
		default:
			cloudOperators = append(cloudOperators, operator)
			break
		}
	}
	return
}

func setPipelineModel(pipelineRequest lib.PipelineRequest, parsedPipeline parser.Pipeline) *pipe.Pipeline {
	pipeline := &pipe.Pipeline{}
	pipeline.Name = pipelineRequest.Name
	pipeline.Description = pipelineRequest.Description
	pipeline.FlowId = parsedPipeline.FlowId
	pipeline.Image = parsedPipeline.Image
	pipeline.WindowTime = pipelineRequest.WindowTime
	pipeline.MergeStrategy = pipelineRequest.MergeStrategy
	pipeline.ConsumeAllMessages = pipelineRequest.ConsumeAllMessages
	pipeline.Metrics = pipelineRequest.Metrics
	return pipeline
}

func (f *FlowEngine) stopOperators(ctx context.Context, pipeline pipe.Pipeline, token string) error {
	localOperators, cloudOperators := seperateOperators(pipeline)
	util.Logger.DebugContext(ctx, "engine - stop operators for pipeline: "+pipeline.Id, "localOperators", localOperators, "cloudOperators", cloudOperators)

	if len(cloudOperators) > 0 {
		err := f.driver.DeleteOperators(ctx, pipeline.Id, cloudOperators)
		if err != nil {
			//ignore error if operator was not found
			var notFoundErr *lib.NotFoundError
			if ok := errors.As(err, &notFoundErr); !ok {
				return err
			}
		}
		err = f.disableCloudToFogForwarding(ctx, cloudOperators, pipeline.Id, pipeline.UserId, token)
		if err != nil {
			util.Logger.ErrorContext(ctx, "cannot disable cloud2fog forwarding", "error", err)
			return err
		}
	}

	if len(localOperators) > 0 {
		for _, operator := range localOperators {
			util.Logger.DebugContext(ctx, "engine - stop local Operator: "+operator.Name)
			err := stopFogOperator(ctx, pipeline.Id,
				operator, pipeline.UserId)
			if err != nil {
				return err
			}
			err = f.disableFogToCloudForwarding(ctx, operator, pipeline.Id, pipeline.UserId, token)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *FlowEngine) startOperators(ctx context.Context, pipeline pipe.Pipeline, pipeConfig lib.PipelineConfig, token string) (newOperators []pipe.Operator, err error) {
	localOperators, cloudOperators := seperateOperators(pipeline)

	if len(cloudOperators) > 0 {
		util.Logger.DebugContext(ctx, "try to start cloud operators")
		err = retry(ctx, 6, 10*time.Second, func() (err error) {
			return f.driver.CreateOperators(
				ctx,
				pipeline.Id,
				cloudOperators,
				pipeConfig,
			)
		})
		if err != nil {
			util.Logger.ErrorContext(ctx, "cannot start cloud operators", "error", err)
			return
		} else {
			util.Logger.DebugContext(ctx, "engine - successfully started cloud operators - "+pipeline.Id)
			cloudOperatorsWithDownstreamID, err2 := f.enableCloudToFogForwarding(ctx, cloudOperators, pipeline.Id, pipeline.UserId, token)
			if err2 != nil {
				util.Logger.ErrorContext(ctx, "cannot enable cloud2fog forwarding", "error", err2)
				err = err2
				return
			}
			newOperators = append(newOperators, cloudOperatorsWithDownstreamID...)
		}
	}
	if len(localOperators) > 0 {
		for _, operator := range localOperators {
			util.Logger.DebugContext(ctx, "try to start local operator: "+operator.Name+" for pipeline: "+pipeline.Id)
			err = startFogOperator(ctx, operator, pipeConfig, pipeline.UserId)
			if err != nil {
				util.Logger.ErrorContext(ctx, "cannot start local operator", "error", err, "operator", operator)
				return
			}
			util.Logger.DebugContext(ctx, "engine - successfully started local operator: "+operator.Name+" for pipeline: "+pipeline.Id)

			err = f.enableFogToCloudForwarding(ctx, operator, pipeline.Id, pipeline.UserId)
			if err != nil {
				return
			}
			newOperators = append(newOperators, operator)
		}
	}
	return
}

func (f *FlowEngine) enableCloudToFogForwarding(ctx context.Context, operators []pipe.Operator, pipelineID, userID, token string) (newOperators []pipe.Operator, err error) {
	for _, operator := range operators {
		if operator.DownstreamConfig.Enabled {
			util.Logger.DebugContext(ctx, "Try to enable Cloud2Fog Forwarding for operator: "+operator.Id)
			createdInstance, err := f.kafak2mqttService.StartOperatorInstance(ctx, operator.Name, operator.Id, pipelineID, userID, token)
			if err != nil {
				util.Logger.ErrorContext(ctx, "cannot enable cloud2fog forwarding", "error", err, "operator", operator)
				return []pipe.Operator{}, err
			}
			operator.DownstreamConfig.InstanceID = createdInstance.Id
		}
		newOperators = append(newOperators, operator) // operator needs to be appened so that no operator is lost
	}

	return
}

func (f *FlowEngine) enableFogToCloudForwarding(ctx context.Context, operator pipe.Operator, _, userID string) error {
	if operator.UpstreamConfig.Enabled {
		util.Logger.DebugContext(ctx, "Try to enable Fog2Cloud Forwarding for operator: "+operator.Id)

		command := &upstreamLib.UpstreamControlMessage{
			OperatorOutputTopic: operator.OutputTopic,
		}
		message, err := json.Marshal(command)
		if err != nil {
			util.Logger.ErrorContext(ctx, "cannot unmarshal enable fog2cloud message for operator: "+operator.Name+" - "+operator.Id, "error", err)
			return err
		}
		topic := upstreamLib.GetUpstreamEnableCloudTopic(userID)
		util.Logger.DebugContext(ctx, "try to publish enable forwarding command for operator: "+operator.Name+" - "+operator.Id+" to topic: "+topic)
		err = publishMessage(topic, string(message))
		if err != nil {
			util.Logger.ErrorContext(ctx, "cannot publish enable fog2cloud message for operator: "+operator.Name+" - "+operator.Id, "error", err)
			return err
		}
		util.Logger.DebugContext(ctx, "published enable forwarding command for operator: "+operator.Name+" - "+operator.Id+" to topic: "+topic)
	}
	return nil
}

func (f *FlowEngine) disableCloudToFogForwarding(ctx context.Context, operators []pipe.Operator, pipelineID, userID, token string) error {
	for _, operator := range operators {
		downstreamConfig := operator.DownstreamConfig
		if downstreamConfig.Enabled {
			util.Logger.DebugContext(ctx, "Try to disable Cloud2Fog Forwarding for operator: "+operator.Id)
			if downstreamConfig.InstanceID == "" {
				util.Logger.WarnContext(ctx, "No instance ID set for operator: "+operator.Id)
				continue
			}
			err := f.kafak2mqttService.RemoveInstance(ctx, downstreamConfig.InstanceID, pipelineID, userID, token)
			if err != nil {
				util.Logger.ErrorContext(ctx, "cannot disable cloud2fog forwarding", "error", err, "operator", operator)
				return err
			}
			util.Logger.DebugContext(ctx, "Disabled Cloud2Fog Forwarding for operator: "+operator.Id)
		} else {
			util.Logger.DebugContext(ctx, "Operator "+operator.Id+" has no downstream forwarding enabled")
		}
	}
	return nil
}

func (f *FlowEngine) disableFogToCloudForwarding(ctx context.Context, operator pipe.Operator, _, userID, _ string) error {
	if operator.UpstreamConfig.Enabled {
		command := &upstreamLib.UpstreamControlMessage{
			OperatorOutputTopic: operator.OutputTopic,
		}
		message, err := json.Marshal(command)
		if err != nil {
			util.Logger.ErrorContext(ctx, "cannot unmarshal disable fog2cloud message for operator: "+operator.Name+" - "+operator.Id, "error", err)
			return err
		}
		util.Logger.DebugContext(ctx, "try to publish disable forwarding command for operator: "+operator.Name+" - "+operator.Id)
		err = publishMessage(upstreamLib.GetUpstreamDisableCloudTopic(userID), string(message))
		if err != nil {
			util.Logger.ErrorContext(ctx, "cannot publish disable fog2cloud message for operator: "+operator.Name+" - "+operator.Id, "error", err)
		}
	} else {
		util.Logger.DebugContext(ctx, "Operator "+operator.Id+" has no upstream forwarding enabled")
	}
	return nil
}

func (f *FlowEngine) createPipelineConfig(pipeline pipe.Pipeline) lib.PipelineConfig {
	var pipeConfig = lib.PipelineConfig{
		WindowTime:     pipeline.WindowTime,
		MergeStrategy:  pipeline.MergeStrategy,
		FlowId:         pipeline.FlowId,
		ConsumerOffset: "latest",
		Metrics:        true, // always enable metrics SNRGY-3068 pipeline.Metrics,
		PipelineId:     pipeline.Id,
		// Taken off the pipeline rather than off the request context, so that a
		// deployment recreated by syncPipelines gets the same labels as the one the
		// original request produced.
		Baggage: pipeline.Baggage,
	}
	if pipeline.ConsumeAllMessages {
		pipeConfig.ConsumerOffset = "earliest"
	}
	return pipeConfig
}
