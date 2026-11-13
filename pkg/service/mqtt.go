/*
 * Copyright 2019 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package service

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/config"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"
	operatorLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/operator"
	upstreamLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/upstream"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"net/url"
	"os"
	"strconv"
	"time"
)

const (
	mqttQos      = 2
	mqttRetained = false
)

// MqttClient is the connection to the broker, on which the engine commands the fog
// agents and which answers their sync requests.
type MqttClient struct {
	client MQTT.Client
	fog    *FogClient
}

// ConnectMQTTBroker takes the process context so the broker callbacks, which paho
// invokes without one of their own, still have something to log against.
func ConnectMQTTBroker(ctx context.Context, config config.MqttConfig, pipelineService PipelineApiService) (*MqttClient, error) {
	//MQTT.DEBUG = log.New(os.Stdout, "", 0)
	//MQTT.ERROR = log.New(os.Stdout, "", 0)

	hostname, _ := os.Hostname()
	m := &MqttClient{}

	topics := map[string]byte{
		upstreamLib.GetUpstreamControlSyncTriggerSubTopic(): byte(0),
		operatorLib.GetOperatorControlSyncTriggerSubTopic(): byte(0),
	}
	util.Logger.InfoContext(ctx, "subscribing to topics: "+fmt.Sprintf("%v", topics))

	clientId := hostname + strconv.Itoa(time.Now().Second())

	connOpts := MQTT.NewClientOptions().
		AddBroker(config.BrokerAddress).
		SetClientID(clientId).
		SetCleanSession(true).
		SetConnectionLostHandler(func(c MQTT.Client, err error) {
			util.Logger.ErrorContext(ctx, "mqtt connection lost: ", "error", err)
		}).
		SetConnectionAttemptHandler(func(broker *url.URL, tlsCfg *tls.Config) *tls.Config {
			util.Logger.InfoContext(ctx, "connecting to broker "+broker.String(), "broker", broker.String())
			return tlsCfg
		}).
		SetReconnectingHandler(func(mqttClient MQTT.Client, opt *MQTT.ClientOptions) {
			util.Logger.InfoContext(ctx, "reconnecting to broker "+opt.Servers[0].String(), "broker", opt.Servers[0].String())
		}).
		SetAutoReconnect(true)

	if config.BrokerUser != "" {
		connOpts.SetUsername(config.BrokerUser)
		if password := config.BrokerPassword.Value(); password != "" {
			connOpts.SetPassword(password)
		}
	}

	tlsConfig := &tls.Config{InsecureSkipVerify: true, ClientAuth: tls.NoClientCert}
	connOpts.SetTLSConfig(tlsConfig)

	connOpts.OnConnect = func(c MQTT.Client) {
		if token := c.SubscribeMultiple(topics, m.onMessageReceived); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}
		util.Logger.InfoContext(ctx, "Subscribed to topics: "+fmt.Sprintf("%v", topics))
	}

	// Set before connecting: OnConnect subscribes, and a message can arrive before
	// Connect returns.
	m.fog = NewFogClient(pipelineService, m)
	m.client = MQTT.NewClient(connOpts)
	if token := m.client.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("Cant connect to broker %s: %s\n", hostname, token.Error())
	}
	util.Logger.InfoContext(ctx, "Connected to broker "+config.BrokerAddress)
	return m, nil
}

func (m *MqttClient) Publish(topic string, message string) error {
	token := m.client.Publish(topic, byte(mqttQos), mqttRetained, message)
	if token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func (m *MqttClient) onMessageReceived(_ MQTT.Client, message MQTT.Message) {
	// paho hands over no context, and an MQTT message carries no trace context to
	// extract one from; processMessage explains why it starts a background one.
	util.Logger.Debug("Received message on topic: "+message.Topic(), "message", message.Payload())
	go m.fog.processMessage(message)
}

func (m *MqttClient) Close() {
	m.client.Disconnect(250)
	time.Sleep(1 * time.Second)
}
