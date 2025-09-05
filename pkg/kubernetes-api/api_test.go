package kubernetes_api

import (
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/api"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/lib"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"testing"
	"time"
)

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
	pipelineId := "test-pipe-123456"
	op1 := lib.Operator{
		Id:               uuid.NewString(),
		Name:             "test-op-1",
		ApplicationId:    uuid.New(),
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
	}
	err = driver.CreateOperators(pipelineId, []lib.Operator{op1}, lib.PipelineConfig{
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
	time.Sleep(10 * time.Second)
	err = driver.DeleteOperator(pipelineId, op1)
	if err != nil {
		t.Error(err.Error())
		return
	}
}
