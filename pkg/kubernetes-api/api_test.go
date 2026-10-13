package kubernetes_api

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
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

const trustedTestImage = "ghcr.io/senergy-platform/adder:1"

func timescaleTestDriver(trustedPrefixes ...string) *Kubernetes {
	if trustedPrefixes == nil {
		trustedPrefixes = []string{config.DefaultOperatorTimescaleTrustedImagePrefix}
	}
	return &Kubernetes{
		r2cfg:           &config.Rancher2Config{},
		timescaleSecret: TimescaleSecret{Name: "analytics-timescale", Key: "dsn", TrustedImagePrefixes: trustedPrefixes},
	}
}

func tsConnFor(t *testing.T, k *Kubernetes, operator pipe.Operator) []apiv1.EnvVar {
	t.Helper()
	envs, err := k.operatorEnv(testPipeId, operator, lib.PipelineConfig{}, 8080)
	if err != nil {
		t.Fatal(err)
	}
	return envNamed(envs, "TS_CONN")
}

// The DSN reaches a cloud operator by reference only, so reading the Deployment does
// not reveal it.
func TestACloudOperatorGetsTsConnFromTheSecret(t *testing.T) {
	found := tsConnFor(t, timescaleTestDriver(), pipe.Operator{Id: "op-1", ImageId: trustedTestImage, DeploymentType: "cloud"})
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

// A fog operator gets no platform DSN, should one ever reach the Kubernetes driver,
// even with a trusted image.
func TestAFogOperatorGetsNoTsConn(t *testing.T) {
	if found := tsConnFor(t, timescaleTestDriver(), pipe.Operator{Id: "op-1", ImageId: trustedTestImage, DeploymentType: "local"}); len(found) != 0 {
		t.Errorf("TS_CONN = %+v, want none for a fog operator", found)
	}
}

// Anyone can register an operator image, so only images under a trusted prefix read
// the database. The match is on the reference as written, without normalisation.
func TestAnUntrustedImageGetsNoTsConn(t *testing.T) {
	for _, image := range []string{
		"docker.io/library/adder:1",
		"ghcr.io/senergy-platform-x/adder:1",
		"ghcr.io/senergy-platformx/adder:1",
		"senergy-platform/adder:1",
		"ghcr.io/senergy-platform",
		"GHCR.IO/senergy-platform/adder:1",
		" ghcr.io/senergy-platform/adder:1",
		"registry.example/ghcr.io/senergy-platform/adder:1",
		"",
	} {
		if found := tsConnFor(t, timescaleTestDriver(), pipe.Operator{Id: "op-1", ImageId: image, DeploymentType: "cloud"}); len(found) != 0 {
			t.Errorf("image %q got TS_CONN %+v, want none", image, found)
		}
	}
}

func TestAnyTrustedPrefixGrantsTsConn(t *testing.T) {
	k := timescaleTestDriver("ghcr.io/senergy-platform/", "registry.example:5000/analytics/")
	for _, image := range []string{trustedTestImage, "registry.example:5000/analytics/forecast@sha256:0123"} {
		if found := tsConnFor(t, k, pipe.Operator{Id: "op-1", ImageId: image, DeploymentType: "cloud"}); len(found) != 1 {
			t.Errorf("image %q: TS_CONN entries = %d, want 1", image, len(found))
		}
	}
}

// No prefixes trust no image; an empty prefix, which the config refuses, must not
// trust every image either.
func TestNoTrustedPrefixGivesNoTsConn(t *testing.T) {
	for _, prefixes := range [][]string{{}, {""}} {
		if found := tsConnFor(t, timescaleTestDriver(prefixes...), pipe.Operator{Id: "op-1", ImageId: trustedTestImage, DeploymentType: "cloud"}); len(found) != 0 {
			t.Errorf("prefixes %q: TS_CONN = %+v, want none", prefixes, found)
		}
	}
}

// The trust rule holds per container of the Deployment, on the image the container runs,
// and each start logs the decision so a missing history can be explained.
func TestCreateOperatorsGivesTsConnOnlyToTheTrustedContainerAndLogsIt(t *testing.T) {
	var logs bytes.Buffer
	previous := util.Logger
	util.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	t.Cleanup(func() { util.Logger = previous })

	k, clientset := fakeDriver()
	k.timescaleSecret.TrustedImagePrefixes = []string{config.DefaultOperatorTimescaleTrustedImagePrefix}
	trusted := pipe.Operator{Id: "op-trusted", OperatorId: "adder", ImageId: trustedTestImage, DeploymentType: "cloud"}
	untrusted := pipe.Operator{Id: "op-untrusted", OperatorId: "custom", ImageId: "ghcr.io/senergy-platform-x/custom:1", DeploymentType: "cloud"}
	if err := k.CreateOperators(context.Background(), fakePipelineId, []pipe.Operator{trusted, untrusted}, lib.PipelineConfig{PipelineId: fakePipelineId}); err != nil {
		t.Fatal(err)
	}

	containers := fakeDeployment(t, clientset).Spec.Template.Spec.Containers
	if len(containers) != 2 {
		t.Fatalf("containers = %d, want 2", len(containers))
	}
	for _, c := range containers {
		want := 0
		if c.Image == trustedTestImage {
			want = 1
		}
		if found := envNamed(c.Env, "TS_CONN"); len(found) != want {
			t.Errorf("container %s with image %s: TS_CONN entries = %d, want %d", c.Name, c.Image, len(found), want)
		}
	}

	access := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var record struct {
			Msg    string `json:"msg"`
			Level  string `json:"level"`
			Id     string `json:"operator_id"`
			Image  string `json:"image"`
			Access *bool  `json:"timescale_access"`
		}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		if record.Msg != "operator timescale access" {
			continue
		}
		if _, seen := access[record.Id]; seen || record.Level != "INFO" || record.Access == nil {
			t.Errorf("log record %+v, want one info record per operator with timescale_access", record)
			continue
		}
		access[record.Id] = *record.Access
		if want := map[string]string{trusted.Id: trusted.ImageId, untrusted.Id: untrusted.ImageId}[record.Id]; record.Image != want {
			t.Errorf("logged image for %s = %q, want %q", record.Id, record.Image, want)
		}
	}
	if len(access) != 2 || !access[trusted.Id] || access[untrusted.Id] {
		t.Errorf("logged access = %v, want %s true and %s false", access, trusted.Id, untrusted.Id)
	}
}
