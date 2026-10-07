package kubernetes_api

import (
	"context"
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
	client, err = NewKubernetes(&cfg.Rancher2, cfg.OperatorResources, true)
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
	_, err = NewKubernetes(&cfg.Rancher2, cfg.OperatorResources, true)
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
		{"cpu request", got.Requests[apiv1.ResourceCPU], "100m"},
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
