package kafka2mqtt_api

import (
	downstreamLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/downstream"
	operatorLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/operator"

	"errors"
	"github.com/parnurzeal/gorequest"
	"strconv"
	"net/http"
	"os"
	"log"
	"encoding/json"
)

type Kafka2MqttApi struct {
	url string
}

func NewKafka2MqttApi(url string) *Kafka2MqttApi {
	return &Kafka2MqttApi{url}
}

func (api *Kafka2MqttApi) StartOperatorInstance(operatorName, operatorID string, pipelineId string, userID, token string) (createdInstance Instance, err error) {
	mqttBaseTopic := downstreamLib.GetDownstreamOperatorCloudPubTopicPrefix(userID)
	mqttTopic, err :=  operatorLib.GenerateOperatorOutputTopic(operatorName, "", operatorID, "local")
	if err != nil {
		return 
	}
	mqttTopic += "/" + pipelineId

	kafkaTopic, err :=  operatorLib.GenerateOperatorOutputTopic(operatorName, "", operatorID, "cloud")
	if err != nil {
		return  
	}

	brokerAddress := os.Getenv("BROKER_ADDRESS")
	username := os.Getenv("BROKER_USER")
	password := os.Getenv("BROKER_PASSWORD")
	instanceConfig := Instance{
		Topic: kafkaTopic,
		FilterType: "operatorId", 
		Filter: pipelineId + ":" + operatorID,
		UserId: userID,
		Values: []Value{
			Value{
				Name: mqttTopic,
				Path: "", // forward the whole message
			},
		},
		CustomMqttBaseTopic: &mqttBaseTopic,
		CustomMqttBroker: &brokerAddress,
		CustomMqttUser: &username,
		CustomMqttPassword: &password,

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
		log.Println("Could not create instance: " + e[0].Error())
		err = errors.New("kafka2mqtt API - could not start instance: an error occurred")
		return 
	}
	if resp.StatusCode != http.StatusOK {
		log.Println("Received wrong response code at created instance: " + strconv.Itoa(resp.StatusCode))
		err = errors.New("kafka2mqtt API - could not start instance: " + strconv.Itoa(resp.StatusCode) + " " + body)
		return 
	}
	err = json.Unmarshal([]byte(body), &createdInstance)
	if err != nil {
		log.Println("Cant unmarshal created instance: " + err.Error())
	}
	return 
}

func (api *Kafka2MqttApi) RemoveInstance(id, pipelineID, userID, token string) error {
	request := gorequest.New()
	request.Delete(api.url+"/instances/"+id).Set("X-UserId", userID).Set("Authorization", token)
	resp, body, e := request.End()
	if len(e) > 0 {
		log.Println("Could not delete instance: " + e[0].Error())
		err := errors.New("kafka2mqtt API - could not delete instance: an error occurred")
		return err 
	}
	if resp.StatusCode != http.StatusNoContent {
		err := errors.New("kafka2mqtt API - could not delete instance: " + strconv.Itoa(resp.StatusCode) + " " + body)
		return err 
	}
	return nil
}