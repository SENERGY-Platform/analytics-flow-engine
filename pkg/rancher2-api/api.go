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

package rancher2_api

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/baggage"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/config"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/httpreq"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"

	"encoding/json"
)

// DummyOperatorId stands in where getOperatorName is called for the deployment name
// rather than for an operator's own name: only the first eight characters of the id
// are used, and the deployment name does not depend on them.
const DummyOperatorId = "v3-123456789"

type Rancher2 struct {
	url       string
	kubeUrl   string
	accessKey string
	secretKey string
	stackId   string
	r2cfg     *config.Rancher2Config
}

func NewRancher2(url string, accessKey string, secretKey string, stackId string, r2cfg *config.Rancher2Config) *Rancher2 {
	kubeUrl := strings.TrimSuffix(url, "v3/") + "k8s/clusters/" +
		strings.Split(r2cfg.ProjectId, ":")[0] + "/v1/"
	return &Rancher2{url, kubeUrl, accessKey, secretKey, stackId, r2cfg}
}

// do issues a request against the Rancher API with this driver's credentials.
//
// The status handling stays at the call sites: what a 404 means differs per call —
// a failure when reading a deployment, success when deleting one that is already
// gone.
func (r *Rancher2) do(ctx context.Context, method, url string, body any) (httpreq.Response, error) {
	return httpreq.Do(ctx, httpreq.Request{
		Method:    method,
		URL:       url,
		Body:      body,
		BasicAuth: &httpreq.BasicAuth{User: r.accessKey, Password: r.secretKey},
	})
}

func (r *Rancher2) GetPipelineStatus(ctx context.Context, pipelineId string) (status lib.PipelineStatus, err error) {
	response, err := r.do(ctx, http.MethodGet, r.kubeUrl+"apps.deployments/analytics-pipelines/pipeline-"+pipelineId, nil)
	if err != nil {
		err = errors.New("rancher2 API - could not request deployment - " + err.Error())
		return
	}

	if response.StatusCode != http.StatusOK {
		err = errors.New("rancher2 API - deployment response is not ok - " + strconv.Itoa(response.StatusCode) + " - " + response.Text())
		return
	}

	var deployment DeploymentResponse
	err = response.Decode(&deployment)
	if err != nil {
		util.Logger.ErrorContext(ctx, "rancher2 API - cannot unmarshal deployment response", "error", err)
		return
	}
	status = lib.PipelineStatus{
		Running:       deployment.Metadata.State.Error == false && deployment.Metadata.State.Transitioning == false,
		Transitioning: deployment.Metadata.State.Transitioning,
		Message:       deployment.Metadata.State.Message,
	}
	return
}

func (r *Rancher2) GetPipelinesStatus(ctx context.Context) (status []lib.PipelineStatus, err error) {
	response, err := r.do(ctx, http.MethodGet, r.kubeUrl+"apps.deployments/analytics-pipelines", nil)
	if err != nil {
		err = errors.New("rancher2 API - could not get pipelines status - " + err.Error())
		return
	}

	if response.StatusCode != http.StatusOK {
		err = errors.New("rancher2 API - could not get pipelines status - " + strconv.Itoa(response.StatusCode) + " - " + response.Text())
		return
	}

	var deployments DeploymentsResponse
	err = response.Decode(&deployments)
	if err != nil {
		util.Logger.ErrorContext(ctx, "rancher2 API - cannot unmarshal deployment response", "error", err)
		return
	}
	for _, deployment := range deployments.Data {
		status = append(status, lib.PipelineStatus{
			Running:       deployment.Metadata.State.Error == false && deployment.Metadata.State.Transitioning == false,
			Transitioning: deployment.Metadata.State.Transitioning,
			Message:       deployment.Metadata.State.Message,
			Name:          deployment.Metadata.Name,
		})
	}
	return
}

func (r *Rancher2) CreateOperators(ctx context.Context, pipelineId string, inputs []pipe.Operator, pipeConfig lib.PipelineConfig) (err error) {
	var containers []Container
	var volumes []Volume
	basePort := 8080
	for i, operator := range inputs {
		operatorRequestConfig, _ := json.Marshal(lib.OperatorRequestConfig{Config: operator.Config, InputTopics: operator.InputTopics})
		labels := baggage.AddLabels(ctx,
			map[string]string{"operatorId": operator.Id, "flowId": pipeConfig.FlowId, "pipeId": pipelineId, "user": pipeConfig.UserId},
			pipeConfig.Baggage)
		env := map[string]string{
			"ZK_QUORUM":                         r.r2cfg.Zookeeper,
			"CONFIG_BOOTSTRAP_SERVERS":          r.r2cfg.KafkaBootstrap,
			"CONFIG_APPLICATION_ID":             "analytics-" + operator.ApplicationId.String(),
			"PIPELINE_ID":                       pipelineId,
			"OPERATOR_ID":                       operator.Id,
			"WINDOW_TIME":                       strconv.Itoa(pipeConfig.WindowTime),
			"JOIN_STRATEGY":                     pipeConfig.MergeStrategy,
			"CONFIG":                            string(operatorRequestConfig),
			"DEVICE_ID_PATH":                    "device_id",
			"CONSUMER_AUTO_OFFSET_RESET_CONFIG": pipeConfig.ConsumerOffset,
			"USER_ID":                           pipeConfig.UserId,
		}

		container := Container{
			Image:           operator.ImageId,
			Name:            operator.OperatorId + "--" + operator.Id,
			ImagePullPolicy: "Always",
		}

		if pipeConfig.Metrics {
			metricsPort := basePort + i
			env["METRICS"] = "true"
			env["METRICS_PORT"] = strconv.Itoa(metricsPort)
			container.Ports = []ContainerPort{{
				Name:          "metrics",
				ContainerPort: metricsPort,
			}}
		}
		if operator.OutputTopic != "" {
			env["OUTPUT"] = operator.OutputTopic
		}
		// Read by the operator libraries, which put the entries into every log record
		// they write. The labels above cover the same ground for the log aggregation,
		// which only sees the container from the outside.
		if header := baggage.Header(pipeConfig.Baggage); header != "" {
			env[baggage.EnvVar] = header
		}

		var r2Env []Env
		for k, v := range env {
			r2Env = append(r2Env, Env{
				Name:  k,
				Value: v,
			})
		}
		container.Env = r2Env

		if operator.PersistData {
			err = r.createPersistentVolumeClaim(ctx, r.getOperatorName(pipelineId, operator)[0])
			vm := VolumeMount{
				Name:      r.getOperatorName(pipelineId, operator)[0],
				MountPath: "/opt/data",
			}
			container.VolumeMounts = append(container.VolumeMounts, vm)
			volumes = append(volumes, Volume{
				Name:                  r.getOperatorName(pipelineId, operator)[0],
				PersistentVolumeClaim: PersistentVolumeClaim{PersistentVolumeClaimId: r.getOperatorName(pipelineId, operator)[0]}},
			)
		}
		container.Resources = ContainerResources{
			Requests: map[string]string{
				"memory": "128Mi",
				"cpu":    "100m",
			},
			Limits: map[string]string{
				"memory": "512Mi",
				"cpu":    "500m",
			},
		}
		container.Labels = labels
		containers = append(containers, container)
	}
	time.Sleep(3 * time.Second)
	reqBody := &WorkloadRequest{
		Name:        r.getOperatorName(pipelineId, pipe.Operator{Id: DummyOperatorId})[1],
		NamespaceId: r.r2cfg.NamespaceId,
		Volumes:     volumes,
		Containers:  containers,
		Scheduling:  Scheduling{Scheduler: "default-scheduler", Node: Node{RequireAll: []string{"role=worker"}}},
		Labels: baggage.AddLabels(ctx,
			map[string]string{"flowId": pipeConfig.FlowId, "pipelineId": pipelineId, "user": pipeConfig.UserId},
			pipeConfig.Baggage),
		Selector: Selector{MatchLabels: map[string]string{"pipelineId": pipelineId}},
	}

	response, err := r.do(ctx, http.MethodPost, r.url+"projects/"+r.r2cfg.ProjectId+"/workloads", reqBody)
	if err != nil {
		util.Logger.ErrorContext(ctx, "rancher2 API - could not create operators ", "error", err)
		return errors.New("rancher2 API -  could not create operators - an error occurred")
	}
	if response.StatusCode != http.StatusCreated {
		errBody := ErrorBody{}
		if decodeErr := response.Decode(&errBody); decodeErr != nil {
			return decodeErr
		}
		// AlreadyExists is the ordinary answer to a retry, and the autoscaler below
		// still has to be created for the workload that is already there.
		if errBody.Code != "AlreadyExists" {
			// Returned here rather than remembered across the autoscaler call. err is the
			// named return value, and the autoscaler's own assignment to it would set it
			// back to nil: the caller would be told the operators started while Rancher
			// had refused the workload, and the pipeline would run nothing at all.
			return errors.New("rancher2 API - could not create operators " + errBody.Code)
		}
	}

	autoscaleRequest := AutoscalingRequest{
		ApiVersion: "autoscaling.k8s.io/v1",
		Kind:       "VerticalPodAutoscaler",
		Metadata: AutoscalingRequestMetadata{
			Name:      r.getOperatorName(pipelineId, pipe.Operator{Id: DummyOperatorId})[1] + "-vpa",
			Namespace: r.r2cfg.NamespaceId,
		},
		Spec: AutoscalingRequestSpec{
			TargetRef: AutoscalingRequestTargetRef{
				ApiVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       r.getOperatorName(pipelineId, pipe.Operator{Id: DummyOperatorId})[1],
			},
			UpdatePolicy: AutoscalingRequestUpdatePolicy{UpdateMode: "Auto"},
			ResourcePolicy: ResourcePolicy{
				ContainerPolicies: []ContainerPolicy{
					{
						ContainerName: "*",
						MaxAllowed: MaxAllowed{
							CPU:    1,
							Memory: "4000Mi",
						},
					},
				},
			},
		},
	}
	// Its own variable, so a later edit cannot silently overwrite an error the
	// workload call above wanted to report.
	vpaResponse, err := r.do(ctx, http.MethodPost, r.kubeUrl+"autoscaling.k8s.io.verticalpodautoscalers", autoscaleRequest)
	if err != nil {
		return errors.New("rancher2 API -  could not create operator vpa - an error occurred")
	}
	if vpaResponse.StatusCode != http.StatusCreated && vpaResponse.StatusCode != http.StatusConflict {
		err = errors.New("rancher2 API - could not create vpa " + vpaResponse.Text())
	}
	return
}

func (r *Rancher2) DeleteOperators(ctx context.Context, pipelineId string, operators []pipe.Operator) (err error) {
	deploymentName := r.getOperatorName(pipelineId, pipe.Operator{Id: DummyOperatorId})[1]

	//Delete Workload
	response, err := r.do(ctx, http.MethodDelete, r.url+"projects/"+r.r2cfg.ProjectId+"/workloads/deployment:"+
		r.r2cfg.NamespaceId+":"+deploymentName, nil)
	if err != nil {
		return ErrSomethingWentWrong
	}
	if response.StatusCode != http.StatusNoContent {
		switch {
		case response.StatusCode == http.StatusNotFound:
			util.Logger.ErrorContext(ctx, "cannot delete operator "+deploymentName+" as it does not exist")
			return // dont have to delete whats already deleted
		default:
			err = errors.New("rancher2 API - could not delete operator " + response.Text())
		}
		return
	}

	// Delete Service
	response, err = r.do(ctx, http.MethodDelete, r.url+"projects/"+r.r2cfg.ProjectId+"/services/"+
		r.r2cfg.NamespaceId+":"+deploymentName, nil)
	if err != nil {
		return ErrSomethingWentWrong
	}
	if response.StatusCode != http.StatusNoContent {
		switch {
		case response.StatusCode == http.StatusNotFound:
			util.Logger.DebugContext(ctx, "cannot delete operator service "+deploymentName+" as it does not exist")
			return // dont have to delete whats already deleted
		default:
			err = errors.New("rancher2 API - could not delete operator service " + response.Text())
		}
		return
	}

	// Delete Autoscaler
	response, err = r.do(ctx, http.MethodDelete, r.kubeUrl+"autoscaling.k8s.io.verticalpodautoscalers/"+
		r.r2cfg.NamespaceId+"/"+deploymentName+"-vpa", nil)
	if err != nil {
		return ErrSomethingWentWrong
	}
	if response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusNotFound {
		err = errors.New("rancher2 API - could not delete operator vpa " + response.Text())
		return
	}

	// Collected rather than returned on the first failure: every operator has its own
	// volume and its own autoscaler checkpoint, and stopping at the first one that
	// resists leaves the rest of them behind. The old code overwrote each error with
	// the next call's, so a volume that could not be deleted was reported as a
	// successful delete and the claim stayed for good.
	var cleanupErrs []error
	for _, operator := range operators {
		// Delete Volume
		if operator.PersistData {
			if volumeErr := r.deletePersistentVolumeClaim(ctx, r.getOperatorName(pipelineId, operator)[0]); volumeErr != nil {
				cleanupErrs = append(cleanupErrs, volumeErr)
			}
		}
		// Delete AutoscalerCheckpoint
		if checkpointErr := r.deleteAutoscalerCheckpoint(ctx, pipelineId, operator); checkpointErr != nil {
			cleanupErrs = append(cleanupErrs, checkpointErr)
		}
	}

	return errors.Join(cleanupErrs...)
}

// deleteAutoscalerCheckpoint removes one operator's autoscaler checkpoint. A
// checkpoint that is not there is not a failure: it only exists once the
// autoscaler has produced a recommendation.
func (r *Rancher2) deleteAutoscalerCheckpoint(ctx context.Context, pipelineId string, operator pipe.Operator) error {
	autoscalerCheckpointId := r.getOperatorName(pipelineId, operator)[1] + "-vpa-" + operator.OperatorId + "--" + operator.Id
	util.Logger.DebugContext(ctx, "try to delete autoscaler checkpoint: "+autoscalerCheckpointId)
	response, err := r.do(ctx, http.MethodDelete, r.kubeUrl+"autoscaling.k8s.io.verticalpodautoscalercheckpoints/"+
		r.r2cfg.NamespaceId+"/"+autoscalerCheckpointId, nil)
	if err != nil {
		return ErrSomethingWentWrong
	}
	if response.StatusCode == http.StatusNotFound {
		util.Logger.DebugContext(ctx, "cannot delete autoscaler checkpoint "+autoscalerCheckpointId+" as it does not exist")
		return nil
	}
	if response.StatusCode != http.StatusNoContent {
		return errors.New("rancher2 API - could not delete operator vpa checkpoint " + response.Text())
	}
	return nil
}

func (r *Rancher2) DeleteOperator(ctx context.Context, pipelineId string, operator pipe.Operator) (err error) {
	deploymentName := r.getOperatorName(pipelineId, operator)[1]

	// Delete AutoscalerCheckpoint
	err = r.deleteAutoscalerCheckpoint(ctx, pipelineId, operator)
	if err != nil {
		return
	}

	//Delete Workload
	response, err := r.do(ctx, http.MethodDelete, r.url+"projects/"+r.r2cfg.ProjectId+"/workloads/deployment:"+
		r.r2cfg.NamespaceId+":"+deploymentName, nil)
	if err != nil {
		return ErrSomethingWentWrong
	}
	if response.StatusCode != http.StatusNoContent {
		switch {
		case response.StatusCode == http.StatusNotFound:
			util.Logger.ErrorContext(ctx, "cannot delete operator "+deploymentName+" as it does not exist")
			return // dont have to delete whats already deleted
		default:
			err = errors.New("rancher2 API - could not delete operator " + response.Text())
		}
		return
	}

	// Delete Volume
	if operator.PersistData {
		err = r.deletePersistentVolumeClaim(ctx, r.getOperatorName(pipelineId, operator)[0])
		if err != nil {
			return
		}
	}

	// Delete Service
	response, err = r.do(ctx, http.MethodDelete, r.url+"projects/"+r.r2cfg.ProjectId+"/services/"+
		r.r2cfg.NamespaceId+":"+deploymentName, nil)
	if err != nil {
		return ErrSomethingWentWrong
	}
	if response.StatusCode != http.StatusNoContent {
		switch {
		case response.StatusCode == http.StatusNotFound:
			util.Logger.DebugContext(ctx, "cannot delete operator service "+deploymentName+" as it does not exist")
			return // dont have to delete whats already deleted
		default:
			err = errors.New("rancher2 API - could not delete operator service " + response.Text())
		}
		return
	}

	// Delete Autoscaler
	response, err = r.do(ctx, http.MethodDelete, r.kubeUrl+"autoscaling.k8s.io.verticalpodautoscalers/"+
		r.r2cfg.NamespaceId+"/"+deploymentName+"-vpa", nil)
	if err != nil {
		return ErrSomethingWentWrong
	}
	if response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusNotFound {
		err = errors.New("rancher2 API - could not delete operator vpa " + response.Text())
		return
	}

	return
}

func (r *Rancher2) getOperatorName(pipelineId string, operator pipe.Operator) []string {
	return []string{"operator-" + pipelineId + "-" + operator.Id[0:8], "pipeline-" + pipelineId}
}

func (r *Rancher2) createPersistentVolumeClaim(ctx context.Context, name string) (err error) {
	reqBody := &VolumeClaimRequest{
		Name:           name,
		NamespaceId:    r.r2cfg.NamespaceId,
		AccessModes:    []string{"ReadWriteOnce"},
		Resources:      Resources{Requests: map[string]string{"storage": "50M"}},
		StorageClassId: *r.r2cfg.StorageDriver,
	}
	response, err := r.do(ctx, http.MethodPost, r.url+"projects/"+r.r2cfg.ProjectId+"/persistentvolumeclaims", reqBody)
	if err != nil {
		return errors.New("rancher2 API - could not create PersistentVolumeClaim: an error occurred")
	}
	if response.StatusCode != http.StatusCreated {
		errBody := ErrorBody{}
		err = response.Decode(&errBody)
		if err != nil {
			return err
		}
		return errors.New("rancher2 API - could not create PersistentVolumeClaim: " + errBody.Message)
	}
	return nil
}

func (r *Rancher2) deletePersistentVolumeClaim(ctx context.Context, name string) (err error) {
	claimUrl := r.url + "projects/" + r.r2cfg.ProjectId + "/persistentVolumeClaims/" + r.r2cfg.NamespaceId + ":" + name

	response, err := r.do(ctx, http.MethodDelete, claimUrl, nil)
	if err != nil {
		return ErrSomethingWentWrong
	}
	if response.StatusCode == http.StatusNotFound {
		util.Logger.ErrorContext(ctx, "Cant delete persistent volume claim as it does not exist", "name", name)
		return nil
	}
	if response.StatusCode != http.StatusOK {
		return errors.New("rancher2 API - could not delete PersistentVolumeClaim " + response.Text())
	}
	// Rancher answers the delete before the claim is gone, so wait for it to
	// disappear. Up to 23 polls at 15 seconds, which is where the number comes from.
	for i := 0; i < 24-1; i++ {
		time.Sleep(15 * time.Second)
		response, err = r.do(ctx, http.MethodGet, claimUrl, nil)
		// Checked now, where it used to be ignored: a failing poll left the previous
		// response in place and the loop read a status that was no longer current.
		if err != nil {
			return ErrSomethingWentWrong
		}
		if response.StatusCode == http.StatusNotFound {
			return nil
		}
	}
	return errors.New("rancher2 API - could not delete PersistentVolumeClaim in time")
}
