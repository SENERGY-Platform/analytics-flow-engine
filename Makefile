export GO111MODULE=on
BINARY_NAME=flow-engine

all: deps build
install:
	go install cmd/flow-engine/flow-engine.go
build:
	CGO_ENABLED=0 go build cmd/flow-engine/flow-engine.go
test:
	go test -v ./...
clean:
	go clean
	rm -f $(BINARY_NAME)
deps:
	go build -v ./...
upgrade:
	go get -u