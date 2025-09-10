package kubernetes_api

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/lib"
	appsv1 "k8s.io/api/apps/v1"
	autoscaling "k8s.io/api/autoscaling/v1"
	apiv1 "k8s.io/api/core/v1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	autoscaler "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/client/clientset/versioned"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"k8s.io/utils/ptr"
	"path/filepath"
	"strconv"
)

type Kubernetes struct {
	clientset           *kubernetes.Clientset
	autoscalerClientset *autoscaler.Clientset
	zookeeper           string
}

func NewKubernetes() (kube *Kubernetes, err error) {
	var config *rest.Config

	if lib.DebugMode() {
		var kubeconfig *string
		lib.GetLogger().Debug("HomeDir" + homedir.HomeDir())
		if home := homedir.HomeDir(); home != "" {
			kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
		} else {
			kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
		}
		flag.Parse()
		lib.GetLogger().Debug("kube config path: " + *kubeconfig)

		// use the current context in kubeconfig
		config, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
		if err != nil {
			return nil, err
		}
		lib.GetLogger().Debug("loaded kube config", "host", config.Host)
	} else {
		// creates the in-cluster config
		config, err = rest.InClusterConfig()
		if err != nil {
			return nil, err
		}
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(config)
	autoscalerClientSet, err := autoscaler.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	lib.GetLogger().Debug("loaded clientset")

	pods, err := clientset.CoreV1().Pods("").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	lib.GetLogger().Debug("succesfully tested connection", "pods", len(pods.Items))

	return &Kubernetes{clientset: clientset, autoscalerClientset: autoscalerClientSet, zookeeper: lib.GetEnv("ZOOKEEPER", "")}, nil
}

func (k *Kubernetes) CreateOperators(pipelineId string, inputs []lib.Operator, pipeConfig lib.PipelineConfig) (err error) {
	var containers []apiv1.Container
	var volumes []apiv1.Volume
	metricsBasePort := 8080
	deploymentsClient := k.clientset.AppsV1().Deployments(lib.GetEnv("RANCHER2_NAMESPACE_ID", ""))
	pvcClient := k.clientset.CoreV1().PersistentVolumeClaims(lib.GetEnv("RANCHER2_NAMESPACE_ID", ""))

	for i, operator := range inputs {
		var ports []apiv1.ContainerPort
		var volumeMounts []apiv1.VolumeMount
		config, _ := json.Marshal(lib.OperatorRequestConfig{Config: operator.Config, InputTopics: operator.InputTopics})
		envs := []apiv1.EnvVar{
			{
				Name:  "ZK_QUORUM",
				Value: k.zookeeper,
			},
			{
				Name:  "CONFIG_APPLICATION_ID",
				Value: "analytics-" + operator.ApplicationId.String(),
			},
			{
				Name:  "PIPELINE_ID",
				Value: pipelineId,
			},
			{
				Name:  "OPERATOR_ID",
				Value: operator.Id,
			},
			{
				Name:  "WINDOW_TIME",
				Value: strconv.Itoa(pipeConfig.WindowTime),
			},
			{
				Name:  "JOIN_STRATEGY",
				Value: pipeConfig.MergeStrategy,
			},
			{
				Name:  "CONFIG",
				Value: string(config),
			},
			{
				Name:  "DEVICE_ID_PATH",
				Value: "device_id",
			},
			{
				Name:  "CONSUMER_AUTO_OFFSET_RESET_CONFIG",
				Value: pipeConfig.ConsumerOffset,
			},
			{
				Name:  "USER_ID",
				Value: pipeConfig.UserId,
			},
		}

		if pipeConfig.Metrics {
			metricsPort := metricsBasePort + i
			envs = append(envs, apiv1.EnvVar{Name: "METRICS", Value: "true"}, apiv1.EnvVar{Name: "METRICS_PORT", Value: strconv.Itoa(metricsPort)})
			ports = append(ports, apiv1.ContainerPort{
				Name:          "metrics-" + strconv.Itoa(i),
				ContainerPort: int32(metricsPort),
			})
		}
		if operator.OutputTopic != "" {
			envs = append(envs, apiv1.EnvVar{Name: "OUTPUT", Value: operator.OutputTopic})
		}

		if operator.PersistData {
			volumeName := getOperatorName(pipelineId, operator)[0]
			pvc := makePVC(volumeName, "50M")
			_, err = pvcClient.Create(context.TODO(), pvc, metav1.CreateOptions{})
			volumeMounts = append(volumeMounts, apiv1.VolumeMount{
				Name:      volumeName,
				MountPath: "/opt/data",
			})
			volumes = append(volumes, apiv1.Volume{
				Name: volumeName,
				VolumeSource: apiv1.VolumeSource{
					PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{
						ClaimName: volumeName,
						ReadOnly:  false,
					},
				},
			})
		}

		container := apiv1.Container{
			Name:            operator.OperatorId + "--" + operator.Id,
			Image:           operator.ImageId,
			ImagePullPolicy: "Always",
			Env:             envs,
			Ports:           ports,
			VolumeMounts:    volumeMounts,
			Resources: apiv1.ResourceRequirements{
				Limits: apiv1.ResourceList{
					apiv1.ResourceCPU:    resource.MustParse("500m"),
					apiv1.ResourceMemory: resource.MustParse("512Mi"),
				},
				Requests: apiv1.ResourceList{
					apiv1.ResourceCPU:    resource.MustParse("100m"),
					apiv1.ResourceMemory: resource.MustParse("128Mi"),
				},
			},
		}
		containers = append(containers, container)
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: getOperatorName(pipelineId, lib.Operator{Id: "v3-123456789"})[1],
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To[int32](1),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"pipelineId": pipelineId,
				},
			},
			Template: apiv1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"flowId":     pipeConfig.FlowId,
						"pipelineId": pipelineId,
						"user":       pipeConfig.UserId,
					},
				},
				Spec: apiv1.PodSpec{
					Volumes:    volumes,
					Containers: containers,
				},
			},
		},
	}

	// Create Deployment
	lib.GetLogger().Debug("creating deployment")
	result, err := deploymentsClient.Create(context.TODO(), deployment, metav1.CreateOptions{})
	if err != nil {
		return
	}
	lib.GetLogger().Debug(fmt.Sprintf("created deployment %s", result.GetObjectMeta().GetName()))

	// Create Vertical Pod Autoscaler
	updateAutoMode := v1.UpdateModeAuto
	vpa := &v1.VerticalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name: getOperatorName(pipelineId, lib.Operator{Id: "v3-123456789"})[1] + "-vpa",
		},
		Spec: v1.VerticalPodAutoscalerSpec{
			TargetRef:    &autoscaling.CrossVersionObjectReference{Kind: "Deployment", Name: getOperatorName(pipelineId, lib.Operator{Id: "v3-123456789"})[1]},
			UpdatePolicy: &v1.PodUpdatePolicy{UpdateMode: &updateAutoMode},
			ResourcePolicy: &v1.PodResourcePolicy{ContainerPolicies: []v1.ContainerResourcePolicy{{
				ContainerName: "*",
				MaxAllowed: apiv1.ResourceList{
					apiv1.ResourceCPU:    resource.MustParse("1000m"),
					apiv1.ResourceMemory: resource.MustParse("4000Mi"),
				},
			}}},
			Recommenders: nil,
		},
	}

	lib.GetLogger().Debug("creating autoscaler")
	verticalAutoscalerClient := k.autoscalerClientset.AutoscalingV1().VerticalPodAutoscalers(lib.GetEnv("RANCHER2_NAMESPACE_ID", ""))
	vpaResult, err := verticalAutoscalerClient.Create(context.TODO(), vpa, metav1.CreateOptions{})
	if err != nil {
		return
	}
	lib.GetLogger().Debug(fmt.Sprintf("created vpa %s", vpaResult.GetObjectMeta().GetName()))
	return
}

func (k *Kubernetes) DeleteOperator(string, lib.Operator) (err error) {
	return
}

func (k *Kubernetes) DeleteOperators(pipelineId string, operators []lib.Operator) (err error) {
	deploymentsClient := k.clientset.AppsV1().Deployments(lib.GetEnv("RANCHER2_NAMESPACE_ID", ""))
	pvcClient := k.clientset.CoreV1().PersistentVolumeClaims(lib.GetEnv("RANCHER2_NAMESPACE_ID", ""))
	verticalAutoscalerClient := k.autoscalerClientset.AutoscalingV1().VerticalPodAutoscalers(lib.GetEnv("RANCHER2_NAMESPACE_ID", ""))
	verticalAutoscalerCheckpointClient := k.autoscalerClientset.AutoscalingV1().VerticalPodAutoscalerCheckpoints(lib.GetEnv("RANCHER2_NAMESPACE_ID", ""))

	for _, operator := range operators {
		if operator.PersistData {
			volumeName := getOperatorName(pipelineId, operator)[0]
			lib.GetLogger().Debug("deleting volume " + volumeName)
			err = pvcClient.Delete(context.TODO(), volumeName, metav1.DeleteOptions{})
			lib.GetLogger().Debug(fmt.Sprintf("deleted volume %s", volumeName))
		}
		autoscalerCheckpointId := getOperatorName(pipelineId, operator)[1] + "-vpa-" + operator.OperatorId + "--" + operator.Id
		lib.GetLogger().Debug("try to delete autoscaler checkpoint: " + autoscalerCheckpointId)
		err = verticalAutoscalerCheckpointClient.Delete(context.TODO(), autoscalerCheckpointId, metav1.DeleteOptions{})
		if err != nil {
			if k8s_errors.IsNotFound(err) {
				lib.GetLogger().Debug("autoscaler checkpoint not found: " + autoscalerCheckpointId)
			} else {
				return
			}
		} else {
			lib.GetLogger().Debug("deleted autoscaler checkpoint: " + autoscalerCheckpointId)
		}
	}

	lib.GetLogger().Debug("deleting deployment " + pipelineId)
	deletePolicy := metav1.DeletePropagationForeground

	err = deploymentsClient.Delete(context.TODO(), getOperatorName(pipelineId, lib.Operator{Id: "v3-123456789"})[1], metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	})
	if err != nil {
		return
	}
	lib.GetLogger().Debug(fmt.Sprintf("deleted deployment %s", pipelineId))

	lib.GetLogger().Debug("deleting autoscaler " + pipelineId)
	err = verticalAutoscalerClient.Delete(context.TODO(), getOperatorName(pipelineId, lib.Operator{Id: "v3-123456789"})[1]+"-vpa", metav1.DeleteOptions{})
	lib.GetLogger().Debug(fmt.Sprintf("deleted autoscaler %s", pipelineId))
	if err != nil {
		return
	}
	return
}

func (k *Kubernetes) GetPipelineStatus(pipelineId string) (pipeStatus lib.PipelineStatus, err error) {
	deploymentsClient := k.clientset.AppsV1().Deployments(lib.GetEnv("RANCHER2_NAMESPACE_ID", ""))
	pipe, err := deploymentsClient.Get(context.TODO(), getOperatorName(pipelineId, lib.Operator{Id: "v3-123456789"})[1], metav1.GetOptions{})
	if err != nil {
		return
	}
	pipeStatus = lib.PipelineStatus{
		Running:       pipe.Status.AvailableReplicas > 0 && pipe.Status.UnavailableReplicas == 0,
		Transitioning: pipe.Status.UnavailableReplicas > 0,
		Message:       "",
	}
	return pipeStatus, err
}

func (k *Kubernetes) GetPipelinesStatus() (pipeStatus []lib.PipelineStatus, err error) {
	deploymentsClient := k.clientset.AppsV1().Deployments(lib.GetEnv("RANCHER2_NAMESPACE_ID", ""))
	pipes, err := deploymentsClient.List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return
	}

	for _, deployment := range pipes.Items {
		pipeStatus = append(pipeStatus, lib.PipelineStatus{
			Running:       deployment.Status.AvailableReplicas > 0 && deployment.Status.UnavailableReplicas == 0,
			Transitioning: deployment.Status.UnavailableReplicas > 0,
			Message:       "",
			Name:          deployment.Name,
		})
	}
	return
}

func getOperatorName(pipelineId string, operator lib.Operator) []string {
	return []string{"operator-" + pipelineId + "-" + operator.Id[0:8], "pipeline-" + pipelineId}
}

func makePVC(name string, size string) *apiv1.PersistentVolumeClaim {
	fs := apiv1.PersistentVolumeFilesystem
	storageClass := lib.GetEnv("RANCHER2_STORAGE_DRIVER", "nfs-client")
	pvc := apiv1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: lib.GetEnv("RANCHER2_NAMESPACE_ID", ""),
		},
		Spec: apiv1.PersistentVolumeClaimSpec{
			AccessModes: []apiv1.PersistentVolumeAccessMode{apiv1.ReadWriteOnce},
			Resources: apiv1.VolumeResourceRequirements{
				Requests: apiv1.ResourceList{
					apiv1.ResourceStorage: resource.MustParse(size),
				},
			},
			StorageClassName: &storageClass,
			VolumeMode:       &fs,
		},
	}
	return &pvc
}
