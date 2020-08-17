FROM golang:1.14

COPY . /go/src/flow-engine
WORKDIR /go/src/flow-engine

ENV GO111MODULE=on

RUN make build

EXPOSE 8000

CMD ./flow-engine