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
	"fmt"
	"os"
	"strconv"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

var client MQTT.Client
var qos *int
var retained *bool

func ConnectMQTTBroker() {
	//MQTT.DEBUG = log.New(os.Stdout, "", 0)
	//MQTT.ERROR = log.New(os.Stdout, "", 0)

	hostname, _ := os.Hostname()

	server := flag.String("server", GetEnv("BROKER_ADDRESS", "tcp://127.0.0.1:1883"), "The full url of the MQTT server to connect to ex: tcp://127.0.0.1:1883")

	topics := map[string]byte{
		MQTTControlTopic: byte(0),
	}
	qos = flag.Int("qos", 0, "The QoS to subscribe to messages at")
	retained = flag.Bool("retained", false, "Are the messages sent with the retained flag")
	clientId := flag.String("clientid", hostname+strconv.Itoa(time.Now().Second()), "A clientid for the connection")
	username := flag.String("username", "", "A username to authenticate to the MQTT server")
	password := flag.String("password", "", "Password to match username")
	flag.Parse()

	connOpts := MQTT.NewClientOptions().AddBroker(*server).SetClientID(*clientId).SetCleanSession(true)
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
	}

	client = MQTT.NewClient(connOpts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		fmt.Printf("local deployment not working: %s\n", token.Error())
	} else {
		fmt.Printf("Connected to %s\n", *server)
	}
}

func publishMessage(topic string, message string) {
	client.Publish(topic, byte(*qos), *retained, message)
}

func onMessageReceived(client MQTT.Client, message MQTT.Message) {
	fmt.Printf("Received message on topic: %s\nMessage: %s\n", message.Topic(), message.Payload())
	go processMessage(message)
}

func CloseMQTTConnection() {
	client.Disconnect(250)
	time.Sleep(1 * time.Second)
}
