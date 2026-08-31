package devicemanagerapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/httpreq"
	"github.com/SENERGY-Platform/models/go/models"
)

type DeviceManagerApi struct {
	url string
}

func NewDeviceManagerApi(url string) *DeviceManagerApi {
	return &DeviceManagerApi{url}
}

func (api *DeviceManagerApi) GetDeviceType(ctx context.Context, deviceTypeID, userID, authorization string) (deviceType models.DeviceType, err error) {
	err = api.get(ctx, "/device-types/"+deviceTypeID, userID, authorization, "device type", &deviceType)
	return
}

func (api *DeviceManagerApi) GetDevice(ctx context.Context, deviceID, userID, authorization string) (device models.Device, err error) {
	err = api.get(ctx, "/devices/"+deviceID, userID, authorization, "device", &device)
	return
}

func (api *DeviceManagerApi) get(ctx context.Context, path, userID, authorization, what string, target any) error {
	response, err := httpreq.Do(ctx, httpreq.Request{
		Method: http.MethodGet,
		URL:    api.url + path,
		Headers: map[string]string{
			"X-UserId":      userID,
			"Authorization": authorization,
		},
	})
	// Returned rather than swallowed. This used to be an empty if body, so an
	// unreachable device manager fell through to reading a nil response.
	if err != nil {
		return fmt.Errorf("device manager API - could not get %s: %w", what, err)
	}
	if response.StatusCode != http.StatusOK {
		return errors.New("device manager API - could not get " + what + ": " +
			strconv.Itoa(response.StatusCode) + " " + response.Text())
	}
	if err = response.Decode(target); err != nil {
		return fmt.Errorf("device manager API - could not unmarshal %s: %w", what, err)
	}
	return nil
}
