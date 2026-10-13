package kubernetes_api

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"maps"
	"path/filepath"
	"strconv"
	"time"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/baggage"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/config"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"
	location "github.com/SENERGY-Platform/analytics-fog-lib/lib/location"
	pipe_lib "github.com/SENERGY-Platform/analytics-pipeline/lib"
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
)

type Kubernetes struct {
	clientset           kubernetes.Interface
	autoscalerClientset autoscaler.Interface
	r2cfg               *config.Rancher2Config
	operatorResources   map[string]config.OperatorResource
	timescaleSecret     TimescaleSecret
	// pvcWait bounds how long one CreateOperators call, across all its operators,
	// waits for terminating PVCs of the same names to disappear.
	pvcWait pvcWait
}

type pvcWait struct {
	timeout  time.Duration
	interval time.Duration
}

var defaultPVCWait = pvcWait{timeout: 60 * time.Second, interval: 2 * time.Second}

// TimescaleSecret names the Secret in the pipelines namespace, and the key in it,
// that cloud operators get their timescale DSN from as TS_CONN.
type TimescaleSecret struct {
	Name string
	Key  string
}

// TimescaleEnvVar is the env var Operator Lib reads its timescale DSN from.
const TimescaleEnvVar = "TS_CONN"

func NewKubernetes(r2cfg *config.Rancher2Config, operatorResources map[string]config.OperatorResource, timescaleSecret TimescaleSecret, debug bool) (kube *Kubernetes, err error) {
	var restConfig *rest.Config

	if debug {
		var kubeconfig *string
		util.Logger.Debug("HomeDir" + homedir.HomeDir())
		if home := homedir.HomeDir(); home != "" {
			kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
		} else {
			kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
		}
		flag.Parse()
		util.Logger.Debug("kube config path: " + *kubeconfig)

		// use the current context in kubeconfig
		restConfig, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
		if err != nil {
			return nil, err
		}
		util.Logger.Debug("loaded kube config", "host", restConfig.Host)
	} else {
		// creates the in-cluster config
		restConfig, err = rest.InClusterConfig()
		if err != nil {
			return nil, err
		}
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(restConfig)
	autoscalerClientSet, err := autoscaler.NewForConfig(restConfig)
	if err != nil {
		return nil, err
	}
	util.Logger.Debug("loaded clientset")

	pods, err := clientset.CoreV1().Pods(r2cfg.NamespaceId).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	util.Logger.Debug("succesfully tested connection", "pods", len(pods.Items))

	return &Kubernetes{clientset: clientset, autoscalerClientset: autoscalerClientSet, r2cfg: r2cfg, operatorResources: operatorResources, timescaleSecret: timescaleSecret, pvcWait: defaultPVCWait}, nil
}

func (k *Kubernetes) CreateOperators(ctx context.Context, pipelineId string, inputs []pipe_lib.Operator, pipeConfig lib.PipelineConfig) (err error) {
	var containers []apiv1.Container
	var volumes []apiv1.Volume
	metricsBasePort := 8080
	deploymentsClient := k.clientset.AppsV1().Deployments(k.r2cfg.NamespaceId)
	vw, cancelWait := k.newVolumeWait(ctx)
	defer cancelWait()

	for i, operator := range inputs {
		var ports []apiv1.ContainerPort
		var volumeMounts []apiv1.VolumeMount
		metricsPort := metricsBasePort + i
		envs, envErr := k.operatorEnv(pipelineId, operator, pipeConfig, metricsPort)
		if envErr != nil {
			return envErr
		}
		if pipeConfig.Metrics {
			ports = append(ports, apiv1.ContainerPort{
				Name:          "metrics-" + strconv.Itoa(i),
				ContainerPort: int32(metricsPort),
			})
		}

		if operator.PersistData {
			volumeName := getOperatorName(pipelineId, operator)[0]
			if err = k.ensurePVC(ctx, vw, k.makePVC(volumeName, "50M")); err != nil {
				return err
			}
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

		resources, resErr := containerResources(operator.ImageId, k.operatorResources)
		if resErr != nil {
			return resErr
		}
		container := apiv1.Container{
			Name:            operator.OperatorId + "--" + operator.Id,
			Image:           operator.ImageId,
			ImagePullPolicy: "Always",
			Env:             envs,
			Ports:           ports,
			VolumeMounts:    volumeMounts,
			Resources:       resources,
		}
		containers = append(containers, container)
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: getOperatorName(pipelineId, pipe_lib.Operator{Id: DummyOperatorId})[1],
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To[int32](1),
			Strategy: deploymentStrategy(volumes),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"pipelineId": pipelineId,
				},
			},
			Template: apiv1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					// On the pod template rather than the deployment: the log aggregation
					// reads pod labels, and the deployment's own labels never reach a log
					// line. Not on the selector either, which must keep matching the pods
					// of a deployment created before the baggage existed.
					Labels: baggage.AddLabels(ctx, map[string]string{
						"flowId":     pipeConfig.FlowId,
						"pipelineId": pipelineId,
						"user":       pipeConfig.UserId,
					}, pipeConfig.Baggage),
				},
				Spec: apiv1.PodSpec{
					Volumes:    volumes,
					Containers: containers,
				},
			},
		},
	}

	// Create Deployment
	util.Logger.DebugContext(ctx, "creating deployment")
	result, err := deploymentsClient.Create(ctx, deployment, metav1.CreateOptions{})
	if err != nil {
		return
	}
	util.Logger.DebugContext(ctx, fmt.Sprintf("created deployment %s", result.GetObjectMeta().GetName()))

	// Create Vertical Pod Autoscaler
	vpa := newVPA(getOperatorName(pipelineId, pipe_lib.Operator{Id: DummyOperatorId})[1])

	util.Logger.DebugContext(ctx, "creating autoscaler")
	verticalAutoscalerClient := k.autoscalerClientset.AutoscalingV1().VerticalPodAutoscalers(k.r2cfg.NamespaceId)
	vpaResult, err := verticalAutoscalerClient.Create(ctx, vpa, metav1.CreateOptions{})
	if err != nil {
		return
	}
	util.Logger.DebugContext(ctx, fmt.Sprintf("created vpa %s", vpaResult.GetObjectMeta().GetName()))
	return
}

// deploymentStrategy is Recreate when the pod mounts a volume: the operator PVCs are
// ReadWriteOnce, and a rolling update's second pod on another node hangs on Multi-Attach.
func deploymentStrategy(volumes []apiv1.Volume) appsv1.DeploymentStrategy {
	if len(volumes) == 0 {
		return appsv1.DeploymentStrategy{}
	}
	return appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType}
}

// volumeWait is the single wait budget of one CreateOperators call, shared by all its
// claims so that several terminating claims cannot multiply the wait.
type volumeWait struct {
	ctx    context.Context
	w      pvcWait
	waited bool
}

func (k *Kubernetes) newVolumeWait(ctx context.Context) (*volumeWait, context.CancelFunc) {
	w := k.pvcWait
	if w.timeout <= 0 || w.interval <= 0 {
		w = defaultPVCWait
	}
	waitCtx, cancel := context.WithTimeout(ctx, w.timeout)
	return &volumeWait{ctx: waitCtx, w: w}, cancel
}

// ensurePVC creates pvc, reuses an existing claim of that name, and waits for a
// terminating one to be gone first, since a claim created over it would vanish with it.
func (k *Kubernetes) ensurePVC(ctx context.Context, vw *volumeWait, pvc *apiv1.PersistentVolumeClaim) error {
	pvcClient := k.clientset.CoreV1().PersistentVolumeClaims(k.r2cfg.NamespaceId)
	logged := false
	for {
		_, err := pvcClient.Create(vw.ctx, pvc, metav1.CreateOptions{})
		if err == nil {
			util.Logger.DebugContext(ctx, "created volume "+pvc.Name)
			return nil
		}
		if !k8s_errors.IsAlreadyExists(err) {
			return vw.error(pvc.Name, err)
		}
		existing, err := pvcClient.Get(vw.ctx, pvc.Name, metav1.GetOptions{})
		if err != nil {
			if k8s_errors.IsNotFound(err) {
				// Gone between Create and Get: try the create again right away.
				continue
			}
			return vw.error(pvc.Name, err)
		}
		if existing.DeletionTimestamp == nil {
			util.Logger.DebugContext(ctx, "reusing existing volume "+pvc.Name)
			return nil
		}
		vw.waited = true
		if !logged {
			util.Logger.InfoContext(ctx, "waiting for the terminating volume to be deleted before creating it again", "volume", pvc.Name, "timeout", vw.w.timeout.String())
			logged = true
		}
		select {
		case <-vw.ctx.Done():
			return vw.error(pvc.Name, vw.ctx.Err())
		case <-time.After(vw.w.interval):
		}
	}
}

// error is ErrVolumeStillTerminating once the budget ran out on a terminating claim of
// this call, and err with the claim name otherwise.
func (vw *volumeWait) error(name string, err error) error {
	if vw.waited && errors.Is(vw.ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%w: %s after %s", lib.ErrVolumeStillTerminating, name, vw.w.timeout)
	}
	return fmt.Errorf("volume %s: %w", name, err)
}

// operatorEnv is the environment of an operator container.
func (k *Kubernetes) operatorEnv(pipelineId string, operator pipe_lib.Operator, pipeConfig lib.PipelineConfig, metricsPort int) ([]apiv1.EnvVar, error) {
	requestConfig, err := operatorRequestConfig(operator)
	if err != nil {
		return nil, fmt.Errorf("config of operator %s: %w", operator.Id, err)
	}
	envs := []apiv1.EnvVar{
		{
			Name:  "ZK_QUORUM",
			Value: k.r2cfg.Zookeeper,
		},
		{
			Name:  "CONFIG_BOOTSTRAP_SERVERS",
			Value: k.r2cfg.KafkaBootstrap,
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
			Value: string(requestConfig),
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

	// By reference, so the DSN shows neither in the Deployment nor in the engine.
	// Not optional: a missing Secret then fails the pod start visibly, naming the
	// Secret, instead of every history read failing later.
	if operator.DeploymentType != location.Local {
		envs = append(envs, apiv1.EnvVar{
			Name: TimescaleEnvVar,
			ValueFrom: &apiv1.EnvVarSource{SecretKeyRef: &apiv1.SecretKeySelector{
				LocalObjectReference: apiv1.LocalObjectReference{Name: k.timescaleSecret.Name},
				Key:                  k.timescaleSecret.Key,
				Optional:             ptr.To(false),
			}},
		})
	}

	// The operator libraries read this and put the entries into every log record
	// they write. The pod labels in CreateOperators cover the same ground for the log
	// aggregation, which sees the container from the outside; this covers the
	// operator process, which is the only place that knows what it is doing.
	if header := baggage.Header(pipeConfig.Baggage); header != "" {
		envs = append(envs, apiv1.EnvVar{Name: baggage.EnvVar, Value: header})
	}

	if pipeConfig.Metrics {
		envs = append(envs, apiv1.EnvVar{Name: "METRICS", Value: "true"}, apiv1.EnvVar{Name: "METRICS_PORT", Value: strconv.Itoa(metricsPort)})
	}
	if operator.OutputTopic != "" {
		envs = append(envs, apiv1.EnvVar{Name: "OUTPUT", Value: operator.OutputTopic})
	}
	return envs, nil
}

// operatorRequestConfig is the CONFIG of an operator container, without ts_conn. The
// engine drops that key too, but a pipeline recreated from the registry (startup
// sync, rollback of an update) carries the config stored before the DSN moved.
func operatorRequestConfig(operator pipe_lib.Operator) ([]byte, error) {
	cfg := operator.Config
	if _, ok := cfg[lib.OperatorConfigTsConn]; ok {
		cfg = maps.Clone(cfg)
		delete(cfg, lib.OperatorConfigTsConn)
	}
	return json.Marshal(lib.OperatorRequestConfig{Config: cfg, InputTopics: operator.InputTopics})
}

func (k *Kubernetes) DeleteOperator(context.Context, string, pipe_lib.Operator) (err error) {
	return
}

// DeleteOperators deletes the deployment of the pipeline, its autoscaler and checkpoints,
// and the operators' volumes.
func (k *Kubernetes) DeleteOperators(ctx context.Context, pipelineId string, operators []pipe_lib.Operator) (err error) {
	return k.deleteOperators(ctx, pipelineId, operators, true)
}

// DeleteOperatorsKeepingVolumes deletes what DeleteOperators does except the volumes,
// so a pipeline rebuilt afterwards keeps its operators' stored state.
func (k *Kubernetes) DeleteOperatorsKeepingVolumes(ctx context.Context, pipelineId string, operators []pipe_lib.Operator) (err error) {
	return k.deleteOperators(ctx, pipelineId, operators, false)
}

func (k *Kubernetes) deleteOperators(ctx context.Context, pipelineId string, operators []pipe_lib.Operator, deleteVolumes bool) (err error) {
	deploymentsClient := k.clientset.AppsV1().Deployments(k.r2cfg.NamespaceId)
	pvcClient := k.clientset.CoreV1().PersistentVolumeClaims(k.r2cfg.NamespaceId)
	verticalAutoscalerClient := k.autoscalerClientset.AutoscalingV1().VerticalPodAutoscalers(k.r2cfg.NamespaceId)
	verticalAutoscalerCheckpointClient := k.autoscalerClientset.AutoscalingV1().VerticalPodAutoscalerCheckpoints(k.r2cfg.NamespaceId)

	for _, operator := range operators {
		if operator.PersistData && deleteVolumes {
			volumeName := getOperatorName(pipelineId, operator)[0]
			util.Logger.DebugContext(ctx, "deleting volume "+volumeName)
			err = pvcClient.Delete(ctx, volumeName, metav1.DeleteOptions{})
			util.Logger.DebugContext(ctx, fmt.Sprintf("deleted volume %s", volumeName))
		}
		autoscalerCheckpointId := getOperatorName(pipelineId, operator)[1] + "-vpa-" + operator.OperatorId + "--" + operator.Id
		util.Logger.DebugContext(ctx, "try to delete autoscaler checkpoint: "+autoscalerCheckpointId)
		err = verticalAutoscalerCheckpointClient.Delete(ctx, autoscalerCheckpointId, metav1.DeleteOptions{})
		if err != nil {
			if k8s_errors.IsNotFound(err) {
				util.Logger.DebugContext(ctx, "autoscaler checkpoint not found: "+autoscalerCheckpointId)
			} else {
				return
			}
		} else {
			util.Logger.DebugContext(ctx, "deleted autoscaler checkpoint: "+autoscalerCheckpointId)
		}
	}

	util.Logger.DebugContext(ctx, "deleting deployment "+pipelineId)
	deletePolicy := metav1.DeletePropagationForeground

	err = deploymentsClient.Delete(ctx, getOperatorName(pipelineId, pipe_lib.Operator{Id: DummyOperatorId})[1], metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	})
	if err != nil {
		if k8s_errors.IsNotFound(err) {
			util.Logger.DebugContext(ctx, "deployment not found: "+getOperatorName(pipelineId, pipe_lib.Operator{Id: DummyOperatorId})[1])
		} else {
			return
		}
	} else {
		util.Logger.DebugContext(ctx, fmt.Sprintf("deleted deployment %s", pipelineId))
	}

	util.Logger.DebugContext(ctx, "deleting autoscaler "+pipelineId)
	err = verticalAutoscalerClient.Delete(ctx, getOperatorName(pipelineId, pipe_lib.Operator{Id: DummyOperatorId})[1]+"-vpa", metav1.DeleteOptions{})
	if err != nil {
		if k8s_errors.IsNotFound(err) {
			util.Logger.DebugContext(ctx, "autoscaler not found: "+pipelineId)
		} else {
			return
		}
	} else {
		util.Logger.DebugContext(ctx, fmt.Sprintf("deleted autoscaler %s", pipelineId))
	}
	return
}

func (k *Kubernetes) GetPipelineStatus(ctx context.Context, pipelineId string) (pipeStatus lib.PipelineStatus, err error) {
	deploymentsClient := k.clientset.AppsV1().Deployments(k.r2cfg.NamespaceId)
	pipe, err := deploymentsClient.Get(ctx, getOperatorName(pipelineId, pipe_lib.Operator{Id: DummyOperatorId})[1], metav1.GetOptions{})
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

func (k *Kubernetes) GetPipelinesStatus(ctx context.Context) (pipeStatus []lib.PipelineStatus, err error) {
	deploymentsClient := k.clientset.AppsV1().Deployments(k.r2cfg.NamespaceId)
	pipes, err := deploymentsClient.List(ctx, metav1.ListOptions{})
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

func getOperatorName(pipelineId string, operator pipe_lib.Operator) []string {
	return []string{"operator-" + pipelineId + "-" + operator.Id[0:8], "pipeline-" + pipelineId}
}

func (k *Kubernetes) makePVC(name string, size string) *apiv1.PersistentVolumeClaim {
	fs := apiv1.PersistentVolumeFilesystem
	pvc := apiv1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: k.r2cfg.NamespaceId,
		},
		Spec: apiv1.PersistentVolumeClaimSpec{
			AccessModes: []apiv1.PersistentVolumeAccessMode{apiv1.ReadWriteOnce},
			Resources: apiv1.VolumeResourceRequirements{
				Requests: apiv1.ResourceList{
					apiv1.ResourceStorage: resource.MustParse(size),
				},
			},
			StorageClassName: func() *string {
				if k.r2cfg.StorageDriver != nil && *k.r2cfg.StorageDriver != "" {
					return k.r2cfg.StorageDriver
				}
				return nil
			}(),
			VolumeMode: &fs,
		},
	}
	return &pvc
}

// containerResources builds the requests and limits of an operator container from
// the quantities config.ResourcesFor resolves for its image.
func containerResources(image string, overrides map[string]config.OperatorResource) (apiv1.ResourceRequirements, error) {
	res := config.ResourcesFor(image, overrides)
	var quantities [4]resource.Quantity
	for i, v := range []string{res.CPULimit, res.MemoryLimit, res.CPURequest, res.MemoryRequest} {
		q, err := resource.ParseQuantity(v)
		if err != nil {
			return apiv1.ResourceRequirements{}, fmt.Errorf("operator resources of image %s: invalid quantity %q: %w", image, v, err)
		}
		quantities[i] = q
	}
	return apiv1.ResourceRequirements{
		Limits: apiv1.ResourceList{
			apiv1.ResourceCPU:    quantities[0],
			apiv1.ResourceMemory: quantities[1],
		},
		Requests: apiv1.ResourceList{
			apiv1.ResourceCPU:    quantities[2],
			apiv1.ResourceMemory: quantities[3],
		},
	}, nil
}

// newVPA is the VerticalPodAutoscaler of the pipeline deployment named deployment.
func newVPA(deployment string) *v1.VerticalPodAutoscaler {
	updateAutoMode := v1.UpdateModeRecreate
	minReplicas := config.OperatorVPAMinReplicas
	return &v1.VerticalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name: deployment + "-vpa",
		},
		Spec: v1.VerticalPodAutoscalerSpec{
			TargetRef:    &autoscaling.CrossVersionObjectReference{Kind: "Deployment", Name: deployment},
			UpdatePolicy: &v1.PodUpdatePolicy{UpdateMode: &updateAutoMode, MinReplicas: &minReplicas},
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
}
