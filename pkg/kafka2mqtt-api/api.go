package kafka2mqtt_api

import (
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/config"
	downstreamLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/downstream"
	operatorLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/operator"

	"encoding/json"
	"errors"
	"github.com/parnurzeal/gorequest"
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

func (api *Kafka2MqttApi) StartOperatorInstance(operatorName, operatorID string, pipelineId string, userID, token string) (createdInstance Instance, err error) {
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
	return api.startInstance(instanceConfig, userID, token)
}

func (api *Kafka2MqttApi) startInstance(instanceConfig Instance, userID, authorization string) (createdInstance Instance, err error) {
	request := gorequest.New()
	request.Post(api.url+"/instances").Set("X-UserId", userID).Set("Authorization", authorization)
	payload, err := json.Marshal(instanceConfig)
	if err != nil {
		return
	}

	resp, body, e := request.Send(string(payload)).End()
	if len(e) > 0 {
		err = errors.New("kafka2mqtt API - could not start instance: an error occurred" + e[0].Error())
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = errors.New("kafka2mqtt API - could not start instance: " + strconv.Itoa(resp.StatusCode) + " " + body)
		return
	}
	err = json.Unmarshal([]byte(body), &createdInstance)
	if err != nil {
		return
	}
	return
}

func (api *Kafka2MqttApi) RemoveInstance(id, _, userID, token string) error {
	request := gorequest.New()
	request.Delete(api.url+"/instances/"+id).Set("X-UserId", userID).Set("Authorization", token)
	resp, body, e := request.End()
	if len(e) > 0 {
		err := errors.New("kafka2mqtt API - could not delete instance: an error occurred " + e[0].Error())
		return err
	}
	if resp.StatusCode != http.StatusNoContent {
		err := errors.New("kafka2mqtt API - could not delete instance: " + strconv.Itoa(resp.StatusCode) + " " + body)
		return err
	}
	return nil
}
