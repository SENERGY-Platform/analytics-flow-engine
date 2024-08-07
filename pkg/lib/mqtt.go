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

package lib

import (
	"crypto/tls"
	"flag"
	"log"
	"os"
	"fmt"
	"strconv"
	"net/url"
	"time"
	operatorLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/operator"
	upstreamLib "github.com/SENERGY-Platform/analytics-fog-lib/lib/upstream"
	MQTT "github.com/eclipse/paho.mqtt.golang"

)

var client MQTT.Client
var qos *int
var retained *bool

func ConnectMQTTBroker() error {
	//MQTT.DEBUG = log.New(os.Stdout, "", 0)
	//MQTT.ERROR = log.New(os.Stdout, "", 0)

	hostname, _ := os.Hostname()

	server := flag.String("server", GetEnv("BROKER_ADDRESS", "tcp://127.0.0.1:1883"), "The full url of the MQTT server to connect to ex: tcp://127.0.0.1:1883")

	topics := map[string]byte{
		upstreamLib.GetUpstreamControlSyncTriggerSubTopic(): byte(0),
		operatorLib.GetOperatorControlSyncTriggerSubTopic(): byte(0),
	} 
	log.Printf("Subscribed to topics: %v\n", topics)

	qos = flag.Int("qos", 2, "The QoS to subscribe to messages at")
	retained = flag.Bool("retained", false, "Are the messages sent with the retained flag")
	clientId := flag.String("clientid", hostname+strconv.Itoa(time.Now().Second()), "A clientid for the connection")
	username := flag.String("username", GetEnv("BROKER_USER", ""), "A username to authenticate to the MQTT server")
	password := flag.String("password", GetEnv("BROKER_PASSWORD", ""), "Password to match username")
	flag.Parse()

	connOpts := MQTT.NewClientOptions().
		AddBroker(*server).
		SetClientID(*clientId).
		SetCleanSession(true).
		SetConnectionLostHandler(func(c MQTT.Client, err error) {
			log.Printf("Connection Lost!")
		}).
		SetConnectionAttemptHandler(func(broker *url.URL, tlsCfg *tls.Config) *tls.Config {
			log.Printf("Attempt to connect!")
			return tlsCfg
		}).
		SetReconnectingHandler(func(mqttClient MQTT.Client, opt *MQTT.ClientOptions) {
			log.Printf("Try to reconnect!")
		}).
		SetAutoReconnect(true)

	if *username != "" {
		connOpts.SetUsername(*username)
		if *password != "" {
			connOpts.SetPassword(*password)
		}
	}

	tlsConfig := &tls.Config{InsecureSkipVerify: true, ClientAuth: tls.NoClientCert}
	connOpts.SetTLSConfig(tlsConfig)

	connOpts.OnConnect = func(c MQTT.Client) {
		if token := c.SubscribeMultiple(topics, onMessageReceived); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}
		log.Println("Connected successfully to MQTT broker")
	}

	client = MQTT.NewClient(connOpts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("Cant connect to broker %s: %s\n", hostname, token.Error())
	} else {
		log.Printf("Connected to %s\n", *server)
	}
	return nil
}

func publishMessage(topic string, message string) error {
	token := client.Publish(topic, byte(*qos), *retained, message)
	if token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func onMessageReceived(client MQTT.Client, message MQTT.Message) {
	log.Printf("Received message on topic: %s\nMessage: %s\n", message.Topic(), message.Payload())
	go processMessage(message)
}

func CloseMQTTConnection() {
	client.Disconnect(250)
	time.Sleep(1 * time.Second)
}
