package devicemanagerapi

import (
	"errors"
	"github.com/parnurzeal/gorequest"
	"strconv"
	"net/http"
	"github.com/SENERGY-Platform/models/go/models"
	"encoding/json"
)

type DeviceManagerApi struct {
	url string
}

func NewDeviceManagerApi(url string) *DeviceManagerApi {
	return &DeviceManagerApi{url}
}

func (api *DeviceManagerApi) GetDeviceType(deviceTypeID, userID, authorization string) (deviceType models.DeviceType, err error) {
	request := gorequest.New()
	request.Get(api.url+"/device-types/"+deviceTypeID).Set("X-UserId", userID).Set("Authorization", authorization)

	resp, body, e := request.Send(nil).End()
	if len(e) > 0 {
	}
	if resp.StatusCode != http.StatusOK {
		err = errors.New("device manager API - could not get device type: " + strconv.Itoa(resp.StatusCode) + " " + body)
		return 
	}
	err = json.Unmarshal([]byte(body), &deviceType)
	if err != nil {
		err = errors.New("Cant unmarshal device type: " + err.Error())
		return
	}
	return
}

func (api *DeviceManagerApi) GetDevice(deviceID, userID, authorization string) (device models.Device, err error) {
	request := gorequest.New()
	request.Get(api.url+"/devices/"+deviceID).Set("X-UserId", userID).Set("Authorization", authorization)

	resp, body, e := request.Send(nil).End()
	if len(e) > 0 {
	}
	if resp.StatusCode != http.StatusOK {
		err = errors.New("device manager API - could not get device: " + strconv.Itoa(resp.StatusCode) + " " + body)
		return 
	}
	err = json.Unmarshal([]byte(body), &device)
	if err != nil {
		err = errors.New("Cant unmarshal device: " + err.Error())
		return
	}
	return
}