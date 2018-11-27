FROM golang

RUN go get -u github.com/golang/dep/cmd/dep

COPY . /go/src/analytics-flow-engine
WORKDIR /go/src/analytics-flow-engine

RUN dep ensure
RUN go build

EXPOSE 8000

CMD ./analytics-flow-engine
