package kubernetes_api

import (
	"fmt"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/api"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/lib"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"testing"
)

var testPipeId = "test-pipe-12345678"

func getClient() (client *Kubernetes, err error) {
	err = godotenv.Load("../../.env")
	if err != nil {
		return
	}
	api.DEBUG = true
	client, err = NewKubernetes()
	if err != nil {
		return
	}
	return client, nil
}

func TestKubernetes_createClient(t *testing.T) {
	err := godotenv.Load("../../.env")
	if err != nil {
		t.Skip("missing .env file")
		return
	}
	api.DEBUG = true
	_, err = NewKubernetes()
	if err != nil {
		t.Error(err.Error())
		return
	}
}

func TestKubernetes_CreateOperators(t *testing.T) {
	driver, err := getClient()
	if err != nil {
		t.Error(err.Error())
		return
	}
	id, _ := uuid.Parse("00000000-0000-0000-0000-000000000000")
	pipelineId := testPipeId
	ops := []lib.Operator{
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
			UpstreamConfig:   lib.UpstreamConfig{},
			DownstreamConfig: lib.DownstreamConfig{},
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
		t.Error(err.Error())
		return
	}
	pipelineId := testPipeId
	id, _ := uuid.Parse("00000000-0000-0000-0000-000000000000")
	ops := []lib.Operator{
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
			UpstreamConfig:   lib.UpstreamConfig{},
			DownstreamConfig: lib.DownstreamConfig{},
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
		t.Error(err.Error())
		return
	}
	pipelineId := testPipeId
	status, err := driver.GetPipelineStatus(pipelineId)
	if err != nil {
		t.Error(err.Error())
		return
	}
	fmt.Println(status)
}
