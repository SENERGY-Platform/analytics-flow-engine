export GO111MODULE=on
BINARY_NAME=analytics-serving

all: deps build
install:
	go install cmd/flow-engine/flow-engine.go
build:
	go build cmd/flow-engine/flow-engine.go
test:
	go test -v ./...
clean:
	go clean
	rm -f $(BINARY_NAME)
deps:
	go build -v ./...
upgrade:
	go get -u