package kafka2mqtt_api

import (
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/config"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/httpreq"
	downstreamLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/downstream"
	operatorLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/operator"

	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
)

type Kafka2MqttApi struct {
	url     string
	mqttCfg *config.MqttConfig
}

func NewKafka2MqttApi(url string, mqttCfg *config.MqttConfig) *Kafka2MqttApi {
	return &Kafka2MqttApi{url, mqttCfg}
}

func (api *Kafka2MqttApi) StartOperatorInstance(ctx context.Context, operatorName, operatorID string, pipelineId string, userID, token string) (_ Instance, err error) {
	mqttBaseTopic := downstreamLib.GetDownstreamOperatorCloudPubTopicPrefix(userID)
	mqttTopic := operatorLib.GenerateFogOperatorTopic(operatorName, operatorID, pipelineId)
	kafkaTopic := operatorLib.GenerateCloudOperatorTopic(operatorName)

	brokerAddress := api.mqttCfg.BrokerAddress
	username := api.mqttCfg.BrokerUser
	password := api.mqttCfg.BrokerPassword
	instanceConfig := Instance{
		Topic:      kafkaTopic,
		FilterType: "operatorId",
		Filter:     pipelineId + ":" + operatorID,
		UserId:     userID,
		Values: []Value{
			{
				Name: mqttTopic,
				Path: "", // forward the whole message
			},
		},
		CustomMqttBaseTopic: &mqttBaseTopic,
		CustomMqttBroker:    &brokerAddress,
		CustomMqttUser:      &username,
		CustomMqttPassword:  &password,
	}
	return api.startInstance(ctx, instanceConfig, userID, token)
}

func (api *Kafka2MqttApi) startInstance(ctx context.Context, instanceConfig Instance, userID, authorization string) (createdInstance Instance, err error) {
	response, err := httpreq.Do(ctx, httpreq.Request{
		Method: http.MethodPost,
		URL:    api.url + "/instances",
		Body:   instanceConfig,
		Headers: map[string]string{
			"X-UserId":      userID,
			"Authorization": authorization,
		},
	})
	if err != nil {
		return createdInstance, fmt.Errorf("kafka2mqtt API - could not start instance: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return createdInstance, errors.New("kafka2mqtt API - could not start instance: " +
			strconv.Itoa(response.StatusCode) + " " + response.Text())
	}
	err = response.Decode(&createdInstance)
	return
}

func (api *Kafka2MqttApi) RemoveInstance(ctx context.Context, id, _, userID, token string) error {
	response, err := httpreq.Do(ctx, httpreq.Request{
		Method: http.MethodDelete,
		URL:    api.url + "/instances/" + id,
		Headers: map[string]string{
			"X-UserId":      userID,
			"Authorization": token,
		},
	})
	if err != nil {
		return fmt.Errorf("kafka2mqtt API - could not delete instance: %w", err)
	}
	if response.StatusCode != http.StatusNoContent {
		return errors.New("kafka2mqtt API - could not delete instance: " +
			strconv.Itoa(response.StatusCode) + " " + response.Text())
	}
	return nil
}
