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
	"crypto/tls"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/config"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"

	"encoding/json"

	"github.com/parnurzeal/gorequest"
)

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

func (r *Rancher2) GetPipelineStatus(pipelineId string) (status lib.PipelineStatus, err error) {
	request := gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: false})
	resp, body, e := request.Get(r.kubeUrl + "apps.deployments/analytics-pipelines/pipeline-" + pipelineId).Send(nil).End()
	if len(e) > 0 {
		err = errors.New("rancher2 API - could not request deployment - " + e[0].Error())
		return
	}

	if resp.StatusCode != http.StatusOK {
		err = errors.New("rancher2 API - deployment response is not ok - " + strconv.Itoa(resp.StatusCode) + " - " + body)
		return
	}

	var deployment DeploymentResponse
	err = json.Unmarshal([]byte(body), &deployment)
	if err != nil {
		util.Logger.Error("rancher2 API - cannot unmarshal deployment response", "error", err)
		return
	}
	status = lib.PipelineStatus{
		Running:       deployment.Metadata.State.Error == false && deployment.Metadata.State.Transitioning == false,
		Transitioning: deployment.Metadata.State.Transitioning,
		Message:       deployment.Metadata.State.Message,
	}
	return
}

func (r *Rancher2) GetPipelinesStatus() (status []lib.PipelineStatus, err error) {
	request := gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: false})
	resp, body, e := request.Get(r.kubeUrl + "apps.deployments/analytics-pipelines").Send(nil).End()
	if len(e) > 0 {
		err = errors.New("rancher2 API - could not get pipelines status - " + e[0].Error())
		return
	}

	if resp.StatusCode != http.StatusOK {
		err = errors.New("rancher2 API - could not get pipelines status - " + strconv.Itoa(resp.StatusCode) + " - " + body)
		return
	}

	var deployments DeploymentsResponse
	err = json.Unmarshal([]byte(body), &deployments)
	if err != nil {
		util.Logger.Error("rancher2 API - cannot unmarshal deployment response", "error", err)
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

func (r *Rancher2) CreateOperators(pipelineId string, inputs []lib.Operator, pipeConfig lib.PipelineConfig) (err error) {
	var containers []Container
	var volumes []Volume
	basePort := 8080
	for i, operator := range inputs {
		operatorRequestConfig, _ := json.Marshal(lib.OperatorRequestConfig{Config: operator.Config, InputTopics: operator.InputTopics})
		labels := map[string]string{"operatorId": operator.Id, "flowId": pipeConfig.FlowId, "pipeId": pipelineId, "user": pipeConfig.UserId}
		env := map[string]string{
			"ZK_QUORUM":                         r.r2cfg.Zookeeper,
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

		var r2Env []Env
		for k, v := range env {
			r2Env = append(r2Env, Env{
				Name:  k,
				Value: v,
			})
		}
		container.Env = r2Env

		if operator.PersistData {
			err = r.createPersistentVolumeClaim(r.getOperatorName(pipelineId, operator)[0])
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
	request := gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: false})
	reqBody := &WorkloadRequest{
		Name:        r.getOperatorName(pipelineId, lib.Operator{Id: "v3-123456789"})[1],
		NamespaceId: r.r2cfg.NamespaceId,
		Volumes:     volumes,
		Containers:  containers,
		Scheduling:  Scheduling{Scheduler: "default-scheduler", Node: Node{RequireAll: []string{"role=worker"}}},
		Labels:      map[string]string{"flowId": pipeConfig.FlowId, "pipelineId": pipelineId, "user": pipeConfig.UserId},
		Selector:    Selector{MatchLabels: map[string]string{"pipelineId": pipelineId}},
	}

	resp, body, e := request.Post(r.url + "projects/" + r.r2cfg.ProjectId + "/workloads").Send(reqBody).End()
	if len(e) > 0 {
		util.Logger.Error("rancher2 API - could not create operators ", "error", e)
		err = errors.New("rancher2 API -  could not create operators - an error occurred")
		return
	}
	if resp.StatusCode != http.StatusCreated {
		errBody := ErrorBody{}
		err = json.Unmarshal([]byte(body), &errBody)
		if err != nil {
			return err
		}
		if errBody.Code != "AlreadyExists" {
			err = errors.New("rancher2 API - could not create operators " + errBody.Code)
		}
	}
	if len(e) > 0 {
		err = errors.New("rancher2 API -  could not create operators - an error occurred")
	}

	autoscaleRequest := AutoscalingRequest{
		ApiVersion: "autoscaling.k8s.io/v1",
		Kind:       "VerticalPodAutoscaler",
		Metadata: AutoscalingRequestMetadata{
			Name:      r.getOperatorName(pipelineId, lib.Operator{Id: "v3-123456789"})[1] + "-vpa",
			Namespace: r.r2cfg.NamespaceId,
		},
		Spec: AutoscalingRequestSpec{
			TargetRef: AutoscalingRequestTargetRef{
				ApiVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       r.getOperatorName(pipelineId, lib.Operator{Id: "v3-123456789"})[1],
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
	request = gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: false})
	resp, body, e = request.Post(r.kubeUrl + "autoscaling.k8s.io.verticalpodautoscalers").
		Send(autoscaleRequest).End()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
		err = errors.New("rancher2 API - could not create vpa " + body)
	}
	if len(e) > 0 {
		err = errors.New("rancher2 API -  could not create operator vpa - an error occurred")
	}
	return
}

func (r *Rancher2) DeleteOperators(pipelineId string, operators []lib.Operator) (err error) {
	//Delete Workload
	request := gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: false})
	resp, body, e := request.Delete(r.url + "projects/" + r.r2cfg.ProjectId + "/workloads/deployment:" +
		r.r2cfg.NamespaceId + ":" + r.getOperatorName(pipelineId, lib.Operator{Id: "v3-123456789"})[1]).End()
	if resp.StatusCode != http.StatusNoContent {
		switch {
		case resp.StatusCode == http.StatusNotFound:
			util.Logger.Error("cannot delete operator " + r.getOperatorName(pipelineId, lib.Operator{Id: "v3-123456789"})[1] + " as it does not exist")
			return // dont have to delete whats already deleted
		default:
			err = errors.New("rancher2 API - could not delete operator " + body)
		}
		return
	}
	if len(e) > 0 {
		err = ErrSomethingWentWrong
		return
	}

	// Delete Service
	request = gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: false})
	resp, body, e = request.Delete(r.url + "projects/" + r.r2cfg.ProjectId + "/services/" +
		r.r2cfg.NamespaceId + ":" + r.getOperatorName(pipelineId, lib.Operator{Id: "v3-123456789"})[1]).End()
	if resp.StatusCode != http.StatusNoContent {
		switch {
		case resp.StatusCode == http.StatusNotFound:
			util.Logger.Debug("cannot delete operator service " + r.getOperatorName(pipelineId, lib.Operator{Id: "v3-123456789"})[1] + " as it does not exist")
			return // dont have to delete whats already deleted
		default:
			err = errors.New("rancher2 API - could not delete operator service " + body)
		}
		return
	}
	if len(e) > 0 {
		err = ErrSomethingWentWrong
		return
	}

	// Delete Autoscaler
	request = gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: false})
	resp, body, e = request.Delete(r.kubeUrl + "autoscaling.k8s.io.verticalpodautoscalers/" +
		r.r2cfg.NamespaceId +
		"/" +
		r.getOperatorName(pipelineId, lib.Operator{Id: "v3-123456789"})[1] + "-vpa").
		End()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		switch {
		case resp.StatusCode == http.StatusNotFound:
			util.Logger.Debug("cannot delete operator vpa " + r.getOperatorName(pipelineId, lib.Operator{Id: "v3-123456789"})[1] + "-vpa" + " as it does not exist")
			return // dont have to delete whats already deleted
		default:
			err = errors.New("rancher2 API - could not delete operator vpa " + body)
		}
		return
	}
	if len(e) > 0 {
		err = ErrSomethingWentWrong
		return
	}

	for _, operator := range operators {
		// Delete Volume
		if operator.PersistData {
			err = r.deletePersistentVolumeClaim(r.getOperatorName(pipelineId, operator)[0])
		}
		// Delete AutoscalerCheckpoint
		autoscalerCheckpointId := r.getOperatorName(pipelineId, operator)[1] + "-vpa-" + operator.OperatorId + "--" + operator.Id
		util.Logger.Debug("try to delete autoscaler checkpoint: " + autoscalerCheckpointId)
		request = gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: false})
		resp, body, e = request.Delete(r.kubeUrl + "autoscaling.k8s.io.verticalpodautoscalercheckpoints/" +
			r.r2cfg.NamespaceId +
			"/" + autoscalerCheckpointId).End()
		if resp.StatusCode != http.StatusNoContent {
			err = errors.New("rancher2 API - could not delete operator vpa checkpoint " + body)
			// There must no checkpoint exists
			if resp.StatusCode == http.StatusNotFound {
				util.Logger.Error("cannot delete autoscaler checkpoint " + autoscalerCheckpointId + " as it does not exist")
				err = nil
			} else {
				return
			}
		}
		if len(e) > 0 {
			err = ErrSomethingWentWrong
			return
		}
	}

	return
}

func (r *Rancher2) DeleteOperator(pipelineId string, operator lib.Operator) (err error) {

	// Delete AutoscalerCheckpoint
	autoscalerCheckpointId := r.getOperatorName(pipelineId, operator)[1] + "-vpa-" + operator.OperatorId + "--" + operator.Id
	util.Logger.Debug("try to delete autoscaler checkpoint: " + autoscalerCheckpointId)
	request := gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: false})
	resp, body, e := request.Delete(r.kubeUrl + "autoscaling.k8s.io.verticalpodautoscalercheckpoints/" +
		r.r2cfg.NamespaceId +
		"/" + autoscalerCheckpointId).End()
	if resp.StatusCode != http.StatusNoContent {
		err = errors.New("rancher2 API - could not delete operator vpa checkpoint " + body)
		// There must no checkpoint exists
		if resp.StatusCode == http.StatusNotFound {
			util.Logger.Error("cannot delete autoscaler checkpoint " + autoscalerCheckpointId + " as it does not exist")
			err = nil
		} else {
			return
		}
	}
	if len(e) > 0 {
		err = ErrSomethingWentWrong
		return
	}

	//Delete Workload
	request = gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: false})
	resp, body, e = request.Delete(r.url + "projects/" + r.r2cfg.ProjectId + "/workloads/deployment:" +
		r.r2cfg.NamespaceId + ":" + r.getOperatorName(pipelineId, operator)[1]).End()
	if resp.StatusCode != http.StatusNoContent {
		switch {
		case resp.StatusCode == http.StatusNotFound:
			util.Logger.Error("cannot delete operator " + r.getOperatorName(pipelineId, operator)[1] + " as it does not exist")
			return // dont have to delete whats already deleted
		default:
			err = errors.New("rancher2 API - could not delete operator " + body)
		}
		return
	}
	if len(e) > 0 {
		err = ErrSomethingWentWrong
		return
	}

	// Delete Volume
	if operator.PersistData {
		err = r.deletePersistentVolumeClaim(r.getOperatorName(pipelineId, operator)[0])
	}

	// Delete Service
	request = gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: false})
	resp, body, e = request.Delete(r.url + "projects/" + r.r2cfg.ProjectId + "/services/" +
		r.r2cfg.NamespaceId + ":" + r.getOperatorName(pipelineId, operator)[1]).End()
	if resp.StatusCode != http.StatusNoContent {
		switch {
		case resp.StatusCode == http.StatusNotFound:
			util.Logger.Debug("cannot delete operator service " + r.getOperatorName(pipelineId, operator)[1] + " as it does not exist")
			return // dont have to delete whats already deleted
		default:
			err = errors.New("rancher2 API - could not delete operator service " + body)
		}
		return
	}
	if len(e) > 0 {
		err = ErrSomethingWentWrong
		return
	}

	// Delete Autoscaler
	request = gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: false})
	resp, body, e = request.Delete(r.kubeUrl + "autoscaling.k8s.io.verticalpodautoscalers/" +
		r.r2cfg.NamespaceId +
		"/" +
		r.getOperatorName(pipelineId, operator)[1] + "-vpa").
		End()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		switch {
		case resp.StatusCode == http.StatusNotFound:
			util.Logger.Debug("cannot delete operator vpa " + r.getOperatorName(pipelineId, operator)[1] + "-vpa" + " as it does not exist")
			return // dont have to delete whats already deleted
		default:
			err = errors.New("rancher2 API - could not delete operator vpa " + body)
		}
		return
	}
	if len(e) > 0 {
		err = ErrSomethingWentWrong
		return
	}

	return
}

func (r *Rancher2) getOperatorName(pipelineId string, operator lib.Operator) []string {
	return []string{"operator-" + pipelineId + "-" + operator.Id[0:8], "pipeline-" + pipelineId}
}

func (r *Rancher2) createPersistentVolumeClaim(name string) (err error) {
	request := gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: false})
	reqBody := &VolumeClaimRequest{
		Name:           name,
		NamespaceId:    r.r2cfg.NamespaceId,
		AccessModes:    []string{"ReadWriteOnce"},
		Resources:      Resources{Requests: map[string]string{"storage": "50M"}},
		StorageClassId: r.r2cfg.StorageDriver,
	}
	resp, body, e := request.Post(r.url + "projects/" + r.r2cfg.ProjectId + "/persistentvolumeclaims").Send(reqBody).End()
	if len(e) > 0 {
		return errors.New("rancher2 API - could not create PersistentVolumeClaim: an error occurred")
	}
	if resp.StatusCode != http.StatusCreated {
		errBody := ErrorBody{}
		err = json.Unmarshal([]byte(body), &errBody)
		if err != nil {
			return err
		}
		return errors.New("rancher2 API - could not create PersistentVolumeClaim: " + errBody.Message)
	}
	return nil
}

func (r *Rancher2) deletePersistentVolumeClaim(name string) (err error) {
	request := gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: false})
	resp, body, e := request.Delete(r.url + "projects/" + r.r2cfg.ProjectId + "/persistentVolumeClaims/" +
		r.r2cfg.NamespaceId + ":" + name).End()
	if len(e) > 0 {
		err = ErrSomethingWentWrong
		return
	}
	if resp.StatusCode == http.StatusNotFound {
		util.Logger.Error("Cant delete persistent volume claim as it does not exist", "name", name)
		err = nil
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = errors.New("rancher2 API - could not delete PersistentVolumeClaim " + body)
		return
	}
	for i := 0; ; i++ {
		if i >= (24 - 1) {
			err = errors.New("rancher2 API - could not delete PersistentVolumeClaim in time")
			break
		}
		time.Sleep(15 * time.Second)
		resp, _, e = request.Get(r.url + "projects/" + r.r2cfg.ProjectId + "/persistentVolumeClaims/" +
			r.r2cfg.NamespaceId + ":" + name).End()
		if resp.StatusCode == http.StatusNotFound {
			return
		}
	}
	return
}
