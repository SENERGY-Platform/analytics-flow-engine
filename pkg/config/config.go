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
	sb_config_types "github.com/SENERGY-Platform/go-service-base/config-hdl/types"
)

type MqttConfig struct {
	BrokerAddress  string                 `json:"broker_address" env_var:"BROKER_ADDRESS"`
	BrokerUser     string                 `json:"broker_user" env_var:"BROKER_USER"`
	BrokerPassword sb_config_types.Secret `json:"broker_password" env_var:"BROKER_PASSWORD"`
}

type LoggerConfig struct {
	Level string `json:"level" env_var:"LOGGER_LEVEL"`
}

type Rancher2Config struct {
	NamespaceId    string  `json:"namespace_id" env_var:"RANCHER2_NAMESPACE_ID"`
	StorageDriver  *string `json:"storage_driver" env_var:"RANCHER2_STORAGE_DRIVER"`
	Zookeeper      string  `json:"zookeeper" env_var:"ZOOKEEPER"`
	KafkaBootstrap string  `json:"kafka_bootstrap" env_var:"KAFKA_BOOTSTRAP"`
}

type Config struct {
	Mqtt                     MqttConfig     `json:"mqtt" env_var:"MQTT_CONFIG"`
	Logger                   LoggerConfig   `json:"logger" env_var:"LOGGER_CONFIG"`
	URLPrefix                string         `json:"url_prefix" env_var:"URL_PREFIX"`
	ServerPort               int            `json:"server_port" env_var:"SERVER_PORT"`
	Rancher2                 Rancher2Config `json:"rancher2" env_var:"RANCHER2_CONFIG"`
	Debug                    bool           `json:"debug" env_var:"DEBUG"`
	ParserApiEndpoint        string         `json:"parser_api_endpoint" env_var:"PARSER_API_ENDPOINT"`
	PermissionApiEndpoint    string         `json:"permission_api_endpoint" env_var:"PERMISSION_API_ENDPOINT"`
	Kafka2MqttApiEndpoint    string         `json:"kafka2mqtt_api_endpoint" env_var:"KAFKA2MQTT_API_ENDPOINT"`
	DeviceManagerApiEndpoint string         `json:"device_manager_api_endpoint" env_var:"DEVICE_MANAGER_API_ENDPOINT"`
	PipelineApiEndpoint      string         `json:"pipeline_api_endpoint" env_var:"PIPELINE_API_ENDPOINT"`
	// AnalyticsServingApiEndpoint is where the exports of an import are looked up
	// at deployment, so an operator can read the import's history from timescale
	// instead of its short-lived Kafka topic. Empty, the default, switches the
	// lookup off: operators read imports from Kafka as before.
	AnalyticsServingApiEndpoint string `json:"analytics_serving_api_endpoint" env_var:"ANALYTICS_SERVING_API_ENDPOINT"`
	// TimescaleConnection is the DSN a cloud operator reads history through, set on
	// every one of them as the ts_conn of its operator config.
	//
	// No default: the DSN carries a password, so the deployment sets it from a
	// secret. Unset, starting a cloud operator is refused (setPlatformOperatorConfig).
	//
	// It stays the same shared credential either way, which is the part SNRGY-4637
	// is about and this does not solve: it reaches every series, and which series an
	// operator reads is decided by its input topics. What the move buys is one place
	// to change when that is addressed.
	TimescaleConnection sb_config_types.Secret `json:"timescale_connection" env_var:"TIMESCALE_CONNECTION"`
	// OtelEndpoint is the OTLP collector traces are exported to. Empty means the
	// in-cluster Jaeger the otelx default names, which is what every deployment
	// uses; the knob exists so a local run can point somewhere else instead of
	// exporting into a void.
	OtelEndpoint string `json:"otel_endpoint" env_var:"OTEL_ENDPOINT"`
	// OperatorResources overrides the resources of operator containers per image
	// repository (no tag, no digest); see OperatorResource. Images not named here
	// keep the defaults.
	OperatorResources map[string]OperatorResource `json:"operator_resources" env_var:"OPERATOR_RESOURCES"`
}

func New(path string) (*Config, error) {
	cfg := Config{
		Mqtt: MqttConfig{
			BrokerAddress:  "tcp://127.0.0.1:1883",
			BrokerUser:     "",
			BrokerPassword: "",
		},
		ServerPort: 8000,
		Debug:      false,
		Rancher2: Rancher2Config{
			Zookeeper:      "zookeeper.kafka:2181",
			KafkaBootstrap: "kafka.kafka:9092",
		},
	}
	err := sb_config_hdl.Load(&cfg, nil, envTypeParser, nil, path)
	if err == nil {
		err = ValidateOperatorResources(cfg.OperatorResources)
	}
	return &cfg, err
}
