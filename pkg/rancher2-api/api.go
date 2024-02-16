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
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/lib"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"encoding/json"

	"github.com/parnurzeal/gorequest"
)

type Rancher2 struct {
	url       string
	kubeUrl   string
	accessKey string
	secretKey string
	stackId   string
	zookeeper string
}

func NewRancher2(url string, accessKey string, secretKey string, stackId string, zookeeper string) *Rancher2 {
	kubeUrl := strings.TrimSuffix(url, "v3/") + "k8s/clusters/" +
		strings.Split(lib.GetEnv("RANCHER2_PROJECT_ID", "_:_"), ":")[0] + "/v1/"
	return &Rancher2{url, kubeUrl, accessKey, secretKey, stackId, zookeeper}
}

func (r *Rancher2) GetPipelineStatus(pipelineId string) (status lib.PipelineStatus, err error) {
	request := gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: true})
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
		log.Println("Cant unmarshal deployment response: " + err.Error())
		return
	}
	status = lib.PipelineStatus{
		Running:       deployment.Metadata.State.Error == false && deployment.Metadata.State.Transitioning == false,
		Transitioning: deployment.Metadata.State.Transitioning,
		Message:       deployment.Metadata.State.Message,
	}
	return
}

func (r *Rancher2) CreateOperators(pipelineId string, inputs []lib.Operator, pipeConfig lib.PipelineConfig) (err error) {
	var containers []Container
	var volumes []Volume
	basePort := 8080
	for i, operator := range inputs {
		config, _ := json.Marshal(lib.OperatorRequestConfig{Config: operator.Config, InputTopics: operator.InputTopics})
		labels := map[string]string{"operatorId": operator.Id, "flowId": pipeConfig.FlowId, "pipeId": pipelineId, "user": pipeConfig.UserId}
		env := map[string]string{
			"ZK_QUORUM":                         r.zookeeper,
			"CONFIG_APPLICATION_ID":             "analytics-" + operator.ApplicationId.String(),
			"PIPELINE_ID":                       pipelineId,
			"OPERATOR_ID":                       operator.Id,
			"WINDOW_TIME":                       strconv.Itoa(pipeConfig.WindowTime),
			"JOIN_STRATEGY":                     pipeConfig.MergeStrategy,
			"CONFIG":                            string(config),
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
	request := gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: true})
	reqBody := &WorkloadRequest{
		Name:        r.getOperatorName(pipelineId, lib.Operator{Id: "v3-123456789"})[1],
		NamespaceId: lib.GetEnv("RANCHER2_NAMESPACE_ID", ""),
		Volumes:     volumes,
		Containers:  containers,
		Scheduling:  Scheduling{Scheduler: "default-scheduler", Node: Node{RequireAll: []string{"role=worker"}}},
		Labels:      map[string]string{"flowId": pipeConfig.FlowId, "pipelineId": pipelineId, "user": pipeConfig.UserId},
		Selector:    Selector{MatchLabels: map[string]string{"pipelineId": pipelineId}},
	}

	resp, body, e := request.Post(r.url + "projects/" + lib.GetEnv("RANCHER2_PROJECT_ID", "") + "/workloads").Send(reqBody).End()
	if len(e) > 0 {
		log.Println(e[0])
		err = errors.New("rancher2 API -  could not create operators - an error occurred")
		return
	}
	if resp.StatusCode != http.StatusCreated {
		err = errors.New("rancher2 API - could not create operators " + body)
	}
	if len(e) > 0 {
		err = errors.New("rancher2 API -  could not create operators - an error occurred")
	}

	autoscaleRequest := AutoscalingRequest{
		ApiVersion: "autoscaling.k8s.io/v1",
		Kind:       "VerticalPodAutoscaler",
		Metadata: AutoscalingRequestMetadata{
			Name:      r.getOperatorName(pipelineId, lib.Operator{Id: "v3-123456789"})[1] + "-vpa",
			Namespace: lib.GetEnv("RANCHER2_NAMESPACE_ID", ""),
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
	request = gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: true})
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

func (r *Rancher2) DeleteOperator(pipelineId string, operator lib.Operator) (err error) {

	// Delete AutoscalerCheckpoint
	autoscalerCheckpointId := r.getOperatorName(pipelineId, operator)[1] + "-vpa-" + operator.OperatorId + "--" + operator.Id
	log.Println("Try to delete autoscaler checkpoint: " + autoscalerCheckpointId)
	request := gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: true})
	resp, body, e := request.Delete(r.kubeUrl + "autoscaling.k8s.io.verticalpodautoscalercheckpoints/" +
		lib.GetEnv("RANCHER2_NAMESPACE_ID", "") +
		"/" + autoscalerCheckpointId).End()
	if resp.StatusCode != http.StatusNoContent {
		err = errors.New("rancher2 API - could not delete operator vpa checkpoint " + body)
		// There must no checkpoint exists
		if resp.StatusCode == http.StatusNotFound {
			log.Printf("Cant delete vpa checkpoint %s as it does not exist\n", autoscalerCheckpointId)
			err = nil
		} else {
			return
		}
	}
	if len(e) > 0 {
		err = lib.ErrSomethingWentWrong
		return
	}

	// Delete Volume
	if operator.PersistData {
		err = r.deletePersistentVolumeClaim(r.getOperatorName(pipelineId, operator)[0])
	}

	//Delete Workload
	request = gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: true})
	resp, body, e = request.Delete(r.url + "projects/" + lib.GetEnv("RANCHER2_PROJECT_ID", "") + "/workloads/deployment:" +
		lib.GetEnv("RANCHER2_NAMESPACE_ID", "") + ":" + r.getOperatorName(pipelineId, operator)[1]).End()
	if resp.StatusCode != http.StatusNoContent {
		switch {
		case resp.StatusCode == http.StatusNotFound:
			err = lib.ErrWorkloadNotFound
		default:
			err = errors.New("rancher2 API - could not delete operator " + body)
		}
		return
	}
	if len(e) > 0 {
		err = lib.ErrSomethingWentWrong
		return
	}

	// Delete Service
	request = gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: true})
	resp, body, e = request.Delete(r.url + "projects/" + lib.GetEnv("RANCHER2_PROJECT_ID", "") + "/services/" +
		lib.GetEnv("RANCHER2_NAMESPACE_ID", "") + ":" + r.getOperatorName(pipelineId, operator)[1]).End()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		err = errors.New("rancher2 API - could not delete operator service " + body)
		return
	}
	if len(e) > 0 {
		err = lib.ErrSomethingWentWrong
		return
	}

	// Delete Autoscaler
	request = gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: true})
	resp, body, e = request.Delete(r.kubeUrl + "autoscaling.k8s.io.verticalpodautoscalers/" +
		lib.GetEnv("RANCHER2_NAMESPACE_ID", "") +
		"/" +
		r.getOperatorName(pipelineId, operator)[1] + "-vpa").
		End()
	if resp.StatusCode != http.StatusNoContent {
		err = errors.New("rancher2 API - could not delete operator vpa " + body)
		return
	}
	if len(e) > 0 {
		err = lib.ErrSomethingWentWrong
		return
	}

	return
}

func (r *Rancher2) getOperatorName(pipelineId string, operator lib.Operator) []string {
	return []string{"operator-" + pipelineId + "-" + operator.Id[0:8], "pipeline-" + pipelineId}
}

func (r *Rancher2) createPersistentVolumeClaim(name string) (err error) {
	request := gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: true})
	reqBody := &VolumeClaimRequest{
		Name:           name,
		NamespaceId:    lib.GetEnv("RANCHER2_NAMESPACE_ID", ""),
		AccessModes:    []string{"ReadWriteOnce"},
		Resources:      Resources{Requests: map[string]string{"storage": "50M"}},
		StorageClassId: lib.GetEnv("RANCHER2_STORAGE_DRIVER", "nfs-client"),
	}
	resp, body, e := request.Post(r.url + "projects/" + lib.GetEnv("RANCHER2_PROJECT_ID", "") + "/persistentvolumeclaims").Send(reqBody).End()
	if len(e) > 0 {
		return errors.New("rancher2 API - could not create PersistentVolumeClaim: an error occurred")
	}
	if resp.StatusCode != http.StatusCreated {
		return errors.New("could not create PersistentVolumeClaim: " + body)
	}
	return nil
}

func (r *Rancher2) deletePersistentVolumeClaim(name string) (err error) {
	request := gorequest.New().SetBasicAuth(r.accessKey, r.secretKey).TLSClientConfig(&tls.Config{InsecureSkipVerify: true})
	resp, body, e := request.Delete(r.url + "projects/" + lib.GetEnv("RANCHER2_PROJECT_ID", "") + "/persistentVolumeClaims/" +
		lib.GetEnv("RANCHER2_NAMESPACE_ID", "") + ":" + name).End()
	if len(e) > 0 {
		err = lib.ErrSomethingWentWrong
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = errors.New("rancher2 API - could not delete PersistentVolumeClaim " + body)
		return
	}
	return
}
