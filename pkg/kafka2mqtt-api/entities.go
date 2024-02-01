package kafka2mqtt_api

import "time"

// TODO external Lib

type Value struct {
	Name string `json:"Name"`
	Path string `json:"Path"`
}

type Instance struct {
	FilterType          string    `json:"FilterType,omitempty" validate:"required"`
	Filter              string    `json:"Filter,omitempty" validate:"required"`
	Name                string    `json:"Name,omitempty" validate:"required"`
	EntityName          string    `json:"EntityName,omitempty" validate:"required"`
	ServiceName         string    `json:"ServiceName,omitempty" validate:"required"`
	Description         string    `json:"Description,omitempty"`
	Topic               string    `json:"Topic,omitempty" validate:"required"`
	Generated           bool      `json:"generated,omitempty"`
	Offset              string    `json:"Offset,omitempty" validate:"required"`
	Values              []Value   `json:"Values,omitempty"`
	UserId              string    `json:"-"`
	ServiceId           string    `json:"-"`
	CustomMqttBroker    *string   `json:"CustomMqttBroker,omitempty"`
	CustomMqttUser      *string   `json:"CustomMqttUser,omitempty"`
	CustomMqttPassword  *string   `json:"CustomMqttPassword,omitempty"`
	CustomMqttBaseTopic *string   `json:"CustomMqttBaseTopic,omitempty"`
	Id                  string    `json:"ID"`
	CreatedAt           time.Time `json:"CreatedAt"`
	UpdatedAt           time.Time `json:"UpdatedAt"`
}