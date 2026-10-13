package kubernetes_api

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/config"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
	"github.com/google/uuid"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

var testPipeId = "test-pipe-12345678"

func getClient() (client *Kubernetes, err error) {
	cfg, err := config.New("../../config.json")
	if err != nil {
		return
	}
	util.InitStructLogger("debug")
	client, err = NewKubernetes(&cfg.Rancher2, cfg.OperatorResources, TimescaleSecret{Name: cfg.OperatorTimescaleSecretName, Key: cfg.OperatorTimescaleSecretKey}, true)
	if err != nil {
		return
	}
	return client, nil
}

func TestKubernetes_createClient(t *testing.T) {
	cfg, err := config.New("../../config.json")
	if err != nil {
		t.Skip(err)
		return
	}
	util.InitStructLogger("debug")
	_, err = NewKubernetes(&cfg.Rancher2, cfg.OperatorResources, TimescaleSecret{Name: cfg.OperatorTimescaleSecretName, Key: cfg.OperatorTimescaleSecretKey}, true)
	if err != nil {
		t.Error(err.Error())
		return
	}
}

func TestKubernetes_CreateOperators(t *testing.T) {
	driver, err := getClient()
	if err != nil {
		t.Skip(err)
		return
	}
	id, _ := uuid.Parse("00000000-0000-0000-0000-000000000000")
	pipelineId := testPipeId
	ops := []pipe.Operator{
		{
			Id:               id.String(),
			Name:             "test-op-1",
			ApplicationId:    id,
			ImageId:          "nginx:1.12",
			DeploymentType:   "cloud",
			OperatorId:       "test-op-1",
			Config:           nil,
			OutputTopic:      "test-output",
			PersistData:      true,
			InputTopics:      nil,
			InputSelections:  nil,
			Cost:             0,
			UpstreamConfig:   pipe.UpstreamConfig{},
			DownstreamConfig: pipe.DownstreamConfig{},
		},
	}
	err = driver.CreateOperators(context.Background(), pipelineId, ops, lib.PipelineConfig{
		WindowTime:     30,
		MergeStrategy:  "inner",
		Metrics:        false,
		ConsumerOffset: "all",
		FlowId:         "65df3289fe696398d26b8772",
		PipelineId:     pipelineId,
		UserId:         "testuser",
	})
	if err != nil {
		t.Error(err.Error())
		return
	}
}

func TestKubernetes_DeleteOperators(t *testing.T) {
	driver, err := getClient()
	if err != nil {
		t.Skip(err)
		return
	}
	pipelineId := testPipeId
	id, _ := uuid.Parse("00000000-0000-0000-0000-000000000000")
	ops := []pipe.Operator{
		{
			Id:               id.String(),
			Name:             "test-op-1",
			ApplicationId:    id,
			ImageId:          "nginx:1.12",
			DeploymentType:   "cloud",
			OperatorId:       "test-op-1",
			Config:           nil,
			OutputTopic:      "test-output",
			PersistData:      true,
			InputTopics:      nil,
			InputSelections:  nil,
			Cost:             0,
			UpstreamConfig:   pipe.UpstreamConfig{},
			DownstreamConfig: pipe.DownstreamConfig{},
		},
	}
	err = driver.DeleteOperators(context.Background(), pipelineId, ops)
	if err != nil {
		t.Error(err.Error())
		return
	}
}

func TestKubernetes_GetPipelineStatus(t *testing.T) {
	driver, err := getClient()
	if err != nil {
		t.Skip(err)
		return
	}
	pipelineId := testPipeId
	_, err = driver.GetPipelineStatus(context.Background(), pipelineId)
	if err != nil {
		t.Error(err.Error())
		return
	}
}

func TestContainerResources(t *testing.T) {
	overrides := map[string]config.OperatorResource{
		"ghcr.io/senergy-platform/consumption-forecast-operator": {MemoryLimit: "2Gi", MemoryRequest: "1Gi"},
	}

	got, err := containerResources("ghcr.io/senergy-platform/consumption-forecast-operator:prod", overrides)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name string
		got  resource.Quantity
		want string
	}{
		{"memory limit", got.Limits[apiv1.ResourceMemory], "2Gi"},
		{"memory request", got.Requests[apiv1.ResourceMemory], "1Gi"},
		{"cpu limit", got.Limits[apiv1.ResourceCPU], "500m"},
		{"cpu request", got.Requests[apiv1.ResourceCPU], "25m"},
	} {
		if want := resource.MustParse(c.want); c.got.Cmp(want) != 0 {
			t.Errorf("%s = %s, want %s", c.name, c.got.String(), c.want)
		}
	}

	got, err = containerResources("nginx:1.12", overrides)
	if err != nil {
		t.Fatal(err)
	}
	if want := resource.MustParse("512Mi"); got.Limits.Memory().Cmp(want) != 0 {
		t.Errorf("memory limit of an image without override = %s, want 512Mi", got.Limits.Memory().String())
	}
}

func TestContainerResourcesRejectsAnUnparsableQuantity(t *testing.T) {
	overrides := map[string]config.OperatorResource{"nginx": {MemoryLimit: "lots"}}
	if _, err := containerResources("nginx:1.12", overrides); err == nil {
		t.Error("expected an error for an unparsable quantity instead of a panic or a silent default")
	}
}

func TestNewVPALetsTheUpdaterEvictASinglePod(t *testing.T) {
	vpa := newVPA("pipeline-x")
	if vpa.Name != "pipeline-x-vpa" || vpa.Spec.TargetRef.Name != "pipeline-x" {
		t.Fatalf("names: %s targets %s", vpa.Name, vpa.Spec.TargetRef.Name)
	}
	if min := vpa.Spec.UpdatePolicy.MinReplicas; min == nil || *min != 1 {
		t.Fatalf("minReplicas = %v, want 1", min)
	}
}

func envNamed(envs []apiv1.EnvVar, name string) []apiv1.EnvVar {
	var found []apiv1.EnvVar
	for _, env := range envs {
		if env.Name == name {
			found = append(found, env)
		}
	}
	return found
}

func timescaleTestDriver() *Kubernetes {
	return &Kubernetes{
		r2cfg:           &config.Rancher2Config{},
		timescaleSecret: TimescaleSecret{Name: "analytics-timescale", Key: "dsn"},
	}
}

// The DSN reaches a cloud operator by reference only, so reading the Deployment does
// not reveal it.
func TestACloudOperatorGetsTsConnFromTheSecret(t *testing.T) {
	envs, err := timescaleTestDriver().operatorEnv(testPipeId, pipe.Operator{Id: "op-1", DeploymentType: "cloud"}, lib.PipelineConfig{}, 8080)
	if err != nil {
		t.Fatal(err)
	}
	found := envNamed(envs, "TS_CONN")
	if len(found) != 1 {
		t.Fatalf("TS_CONN entries = %d, want 1", len(found))
	}
	env := found[0]
	if env.Value != "" || env.ValueFrom == nil || env.ValueFrom.SecretKeyRef == nil {
		t.Fatalf("TS_CONN = %+v, want a secretKeyRef and no literal value", env)
	}
	ref := env.ValueFrom.SecretKeyRef
	if ref.Name != "analytics-timescale" || ref.Key != "dsn" {
		t.Errorf("secretKeyRef = %s/%s, want analytics-timescale/dsn", ref.Name, ref.Key)
	}
	if ref.Optional == nil || *ref.Optional {
		t.Errorf("secretKeyRef optional = %v, want false so a missing Secret fails the pod start", ref.Optional)
	}
}

// A pipeline recreated from the registry carries the config stored while the DSN was
// still put into it; CONFIG must not pass it on.
func TestTheContainerConfigCarriesNoTsConn(t *testing.T) {
	operator := pipe.Operator{
		Id:             "op-1",
		DeploymentType: "cloud",
		Config:         map[string]string{"ts_conn": "stored-dsn", "window": "5"},
	}
	envs, err := timescaleTestDriver().operatorEnv(testPipeId, operator, lib.PipelineConfig{}, 8080)
	if err != nil {
		t.Fatal(err)
	}
	for _, env := range envs {
		if strings.Contains(env.Value, "stored-dsn") {
			t.Errorf("env %s carries the stored DSN: %s", env.Name, env.Value)
		}
	}
	found := envNamed(envs, "CONFIG")
	if len(found) != 1 {
		t.Fatalf("CONFIG entries = %d, want 1", len(found))
	}
	var got lib.OperatorRequestConfig
	if err := json.Unmarshal([]byte(found[0].Value), &got); err != nil {
		t.Fatalf("CONFIG is not JSON: %v", err)
	}
	if _, present := got.Config["ts_conn"]; present || got.Config["window"] != "5" {
		t.Errorf("CONFIG config = %v, want window kept and ts_conn dropped", got.Config)
	}
	// The engine goes on to store the operator, so the driver must not change it.
	if operator.Config["ts_conn"] != "stored-dsn" {
		t.Error("building CONFIG changed the operator's own config")
	}
}

// A fog operator gets no platform DSN, should one ever reach the Kubernetes driver.
func TestAFogOperatorGetsNoTsConn(t *testing.T) {
	envs, err := timescaleTestDriver().operatorEnv(testPipeId, pipe.Operator{Id: "op-1", DeploymentType: "local"}, lib.PipelineConfig{}, 8080)
	if err != nil {
		t.Fatal(err)
	}
	if found := envNamed(envs, "TS_CONN"); len(found) != 0 {
		t.Errorf("TS_CONN = %+v, want none for a fog operator", found)
	}
}
