module github.com/SENERGY-Platform/analytics-flow-engine

go 1.21.3

//replace github.com/SENERGY-Platform/analytics-fog-lib => ../analytics-fog/analytics-fog-lib

require (
	github.com/SENERGY-Platform/analytics-fog-lib v1.1.13
	github.com/SENERGY-Platform/go-service-base/watchdog v0.4.1
	github.com/SENERGY-Platform/models/go v0.0.0-20240627082833-157bd627a94f
	github.com/eclipse/paho.mqtt.golang v1.4.3
	github.com/golang-jwt/jwt v3.2.2+incompatible
	github.com/google/uuid v1.5.0
	github.com/gorilla/mux v1.8.0
	github.com/joho/godotenv v1.5.1
	github.com/parnurzeal/gorequest v0.2.16
	github.com/pkg/errors v0.9.1
	github.com/rs/cors v1.10.1
)

require (
	github.com/elazarl/goproxy v0.0.0-20231117061959-7cc037d33fb5 // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
	github.com/smartystreets/goconvey v1.8.1 // indirect
	golang.org/x/net v0.8.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	moul.io/http2curl v1.0.0 // indirect
)
