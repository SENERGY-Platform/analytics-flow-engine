package kubernetes_api

import (
	"testing"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/config"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"
	pipe "github.com/SENERGY-Platform/analytics-pipeline/lib"
	"github.com/google/uuid"
)

var testPipeId = "test-pipe-12345678"

func getClient() (client *Kubernetes, err error) {
	cfg, err := config.New("../../config.json")
	if err != nil {
		return
	}
	util.InitStructLogger("debug")
	client, err = NewKubernetes(&cfg.Rancher2, true)
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
	_, err = NewKubernetes(&cfg.Rancher2, true)
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
	err = driver.CreateOperators(pipelineId, ops, lib.PipelineConfig{
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
	err = driver.DeleteOperators(pipelineId, ops)
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
	_, err = driver.GetPipelineStatus(pipelineId)
	if err != nil {
		t.Error(err.Error())
		return
	}
}
