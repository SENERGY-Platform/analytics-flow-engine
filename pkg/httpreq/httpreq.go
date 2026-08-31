/*
 * Copyright 2026 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package httpreq issues the service's outgoing HTTP requests with the trace
// context and the baggage of the caller attached.
//
// It exists because every client in this service repeated the same four steps —
// build, send, read, check the status — around a request library that could not
// carry a context. Propagation has to happen on each of those calls, and six
// copies of the same injection is six chances to forget it.
//
// Deliberately thin: it returns the status code and the raw body rather than
// decoding or classifying anything. The clients differ in what a status means to
// them — a 404 from the pipeline registry is a typed NotFoundError, a 404 from
// Rancher while deleting is success — so that judgement stays with them.
package httpreq

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/SENERGY-Platform/gin-middleware/otelx"
)

// BasicAuth is the credential pair the Rancher API is addressed with.
type BasicAuth struct {
	User     string
	Password string
}

type Request struct {
	Method string
	URL    string
	// Body is JSON-encoded when it is not nil. A string or a []byte is sent as it
	// stands, for the callers that marshalled it themselves.
	Body      any
	Headers   map[string]string
	BasicAuth *BasicAuth
}

type Response struct {
	StatusCode int
	Body       []byte
}

// Text returns the body for an error message.
func (r Response) Text() string {
	return string(r.Body)
}

// Decode unmarshals the body into target.
func (r Response) Decode(target any) error {
	return json.Unmarshal(r.Body, target)
}

// client deliberately has no timeout of its own. The Rancher workload calls are the
// slowest thing this service does — one of them waits minutes for a volume to
// disappear — and a fixed number here would cut them off at a value nobody chose.
// The request library this replaced had no timeout either.
//
// On the read paths the deadline therefore comes from the caller's request context.
// On the deployment paths it does not: those run on a context with the cancellation
// deliberately removed, so a peer that never answers holds the goroutine until the
// process ends. That is the same exposure as before this change, and narrowing it is
// a separate decision about how long a deployment may legitimately take.
//
// Proxy is set to nil rather than left at the default. http.DefaultTransport honours
// HTTP_PROXY and HTTPS_PROXY, while the transport the old request library built did
// not; every address this service calls is inside the cluster, so picking up a proxy
// from the environment would route internal traffic outward and change behaviour
// without anybody asking for it.
var client = &http.Client{
	Transport: &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	},
}

// Do sends the request and reads the whole response.
//
// Unlike the request library this replaced, a transport failure is returned as an
// error before the status is looked at. The old call sites read resp.StatusCode
// first and only then checked the error, which dereferences nil whenever the peer
// is unreachable.
func Do(ctx context.Context, request Request) (Response, error) {
	body, err := encodeBody(request.Body)
	if err != nil {
		return Response{}, fmt.Errorf("could not encode the request body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, request.Method, request.URL, body)
	if err != nil {
		return Response{}, fmt.Errorf("could not build the request: %w", err)
	}
	if request.Body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for key, value := range request.Headers {
		if value == "" {
			continue
		}
		req.Header.Set(key, value)
	}
	if request.BasicAuth != nil {
		req.SetBasicAuth(request.BasicAuth.User, request.BasicAuth.Password)
	}

	// The whole point of the package: the trace id and the baggage go onto the wire
	// so the service on the other end logs under the same context.
	if err = otelx.InjectContextToRequest(ctx, req); err != nil {
		return Response{}, fmt.Errorf("could not inject the trace context: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return Response{}, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return Response{StatusCode: resp.StatusCode}, fmt.Errorf("could not read the response body: %w", err)
	}
	return Response{StatusCode: resp.StatusCode, Body: responseBody}, nil
}

func encodeBody(body any) (io.Reader, error) {
	switch value := body.(type) {
	case nil:
		return nil, nil
	case string:
		return bytes.NewBufferString(value), nil
	case []byte:
		return bytes.NewBuffer(value), nil
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(encoded), nil
	}
}
