FROM golang:1.24 AS builder

COPY . /go/src/app
WORKDIR /go/src/app

ENV GO111MODULE=on

RUN make build

RUN git log -1 --oneline > version.txt

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /go/src/app/flow-engine .
COPY --from=builder /go/src/app/version.txt .

EXPOSE 8000

LABEL org.opencontainers.image.source=https://github.com/SENERGY-Platform/analytics-flow-engine

ENTRYPOINT ["./flow-engine"]
