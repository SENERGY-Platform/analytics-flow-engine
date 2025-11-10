/*
 * Copyright 2025 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package config

import (
	sb_config_hdl "github.com/SENERGY-Platform/go-service-base/config-hdl"
)

type MqttConfig struct {
	BrokerAddress  string `json:"broker_address" env_var:"BROKER_ADDRESS"`
	BrokerUser     string `json:"broker_user" env_var:"BROKER_USER"`
	BrokerPassword string `json:"broker_password" env_var:"BROKER_PASSWORD"`
}

type LoggerConfig struct {
	Level string `json:"level" env_var:"LOGGER_LEVEL"`
}

type Rancher2Config struct {
	Endpoint       string `json:"endpoint" env_var:"RANCHER2_ENDPOINT"`
	AccessKey      string `json:"access_key" env_var:"RANCHER2_ACCESS_KEY"`
	SecretKey      string `json:"secret_key" env_var:"RANCHER2_SECRET_KEY"`
	StackId        string `json:"stack_id" env_var:"RANCHER2_STACK_ID"`
	ProjectId      string `json:"project_id" env_var:"RANCHER2_PROJECT_ID"`
	NamespaceId    string `json:"namespace_id" env_var:"RANCHER2_NAMESPACE_ID"`
	StorageDriver  string `json:"storage_driver" env_var:"RANCHER2_STORAGE_DRIVER"`
	Zookeeper      string `json:"zookeeper" env_var:"ZOOKEEPER"`
	KafkaBootstrap string `json:"kafka_bootstrap" env_var:"KAFKA_BOOTSTRAP"`
}

type Config struct {
	Mqtt                     MqttConfig     `json:"mqtt" env_var:"MQTT_CONFIG"`
	Logger                   LoggerConfig   `json:"logger" env_var:"LOGGER_CONFIG"`
	URLPrefix                string         `json:"url_prefix" env_var:"URL_PREFIX"`
	ServerPort               int            `json:"server_port" env_var:"SERVER_PORT"`
	Driver                   string         `json:"driver" env_var:"DRIVER"`
	Rancher2                 Rancher2Config `json:"rancher2" env_var:"RANCHER2_CONFIG"`
	Debug                    bool           `json:"debug" env_var:"DEBUG"`
	ParserApiEndpoint        string         `json:"parser_api_endpoint" env_var:"PARSER_API_ENDPOINT"`
	PermissionApiEndpoint    string         `json:"permission_api_endpoint" env_var:"PERMISSION_API_ENDPOINT"`
	Kafka2MqttApiEndpoint    string         `json:"kafka2mqtt_api_endpoint" env_var:"KAFKA2MQTT_API_ENDPOINT"`
	DeviceManagerApiEndpoint string         `json:"device_manager_api_endpoint" env_var:"DEVICE_MANAGER_API_ENDPOINT"`
	PipelineApiEndpoint      string         `json:"pipeline_api_endpoint" env_var:"PIPELINE_API_ENDPOINT"`
}

func New(path string) (*Config, error) {
	cfg := Config{
		Mqtt: MqttConfig{
			BrokerAddress:  "tcp://127.0.0.1:1883",
			BrokerUser:     "",
			BrokerPassword: "",
		},
		Driver:     "kubernetes",
		ServerPort: 8000,
		Debug:      false,
		Rancher2: Rancher2Config{
			ProjectId:      "_:_",
			StorageDriver:  "nfs-client",
			Zookeeper:      "zookeeper.kafka:2181",
			KafkaBootstrap: "kafka.kafka:9092",
		},
	}
	err := sb_config_hdl.Load(&cfg, nil, envTypeParser, nil, path)
	return &cfg, err
}
