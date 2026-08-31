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

package httpreq

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	otelbaggage "go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
)

// received keeps what the far side saw, which is the only way to check that a
// header made it onto the wire.
type received struct {
	method string
	path   string
	body   string
	header http.Header
}

func serverRecording(t *testing.T, status int, responseBody string) (*httptest.Server, *received) {
	t.Helper()
	got := &received{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got.method = r.Method
		got.path = r.URL.Path
		got.body = string(body)
		got.header = r.Header.Clone()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(responseBody))
	}))
	t.Cleanup(server.Close)
	return server, got
}

func TestDoSendsTheRequest(t *testing.T) {
	server, got := serverRecording(t, http.StatusOK, `{"id":"3c1f9b42"}`)

	response, err := Do(context.Background(), Request{
		Method:  http.MethodPost,
		URL:     server.URL + "/pipeline",
		Body:    map[string]string{"name": "a pipeline"},
		Headers: map[string]string{"X-UserId": "jonah", "Authorization": "Bearer t"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if got.method != http.MethodPost || got.path != "/pipeline" {
		t.Errorf("expected POST /pipeline, got %s %s", got.method, got.path)
	}
	if got.body != `{"name":"a pipeline"}` {
		t.Errorf("expected the body to be JSON-encoded, got %q", got.body)
	}
	if got.header.Get("Content-Type") != "application/json" {
		t.Errorf("expected a JSON content type, got %q", got.header.Get("Content-Type"))
	}
	if got.header.Get("X-UserId") != "jonah" || got.header.Get("Authorization") != "Bearer t" {
		t.Errorf("headers did not arrive: %v", got.header)
	}
	if response.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", response.StatusCode)
	}
	var decoded struct{ Id string }
	if err = response.Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Id != "3c1f9b42" {
		t.Errorf("expected the response body to decode, got %q", decoded.Id)
	}
}

func TestDoPropagatesBaggage(t *testing.T) {
	// The reason this package exists. The propagator has to be the composite one the
	// otelx setup installs; a plain default would carry the trace but not the baggage.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))

	server, got := serverRecording(t, http.StatusOK, "{}")

	member, err := otelbaggage.NewMember("smart_service_instance_id", "8fbd0e8a")
	if err != nil {
		t.Fatal(err)
	}
	bag, err := otelbaggage.New(member)
	if err != nil {
		t.Fatal(err)
	}
	ctx := otelbaggage.ContextWithBaggage(context.Background(), bag)

	if _, err = Do(ctx, Request{Method: http.MethodGet, URL: server.URL + "/x"}); err != nil {
		t.Fatal(err)
	}

	header := got.header.Get("baggage")
	parsed, err := otelbaggage.Parse(header)
	if err != nil {
		t.Fatalf("the baggage header %q must be parseable: %v", header, err)
	}
	if v := parsed.Member("smart_service_instance_id").Value(); v != "8fbd0e8a" {
		t.Errorf("expected the instance id on the wire, got header %q", header)
	}
}

func TestDoWithoutBodySendsNoContentType(t *testing.T) {
	server, got := serverRecording(t, http.StatusNoContent, "")

	response, err := Do(context.Background(), Request{Method: http.MethodDelete, URL: server.URL + "/x"})
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusNoContent {
		t.Errorf("expected 204, got %d", response.StatusCode)
	}
	if got.header.Get("Content-Type") != "" {
		t.Errorf("a bodyless request should send no content type, got %q", got.header.Get("Content-Type"))
	}
}

func TestDoSendsBasicAuth(t *testing.T) {
	server, got := serverRecording(t, http.StatusOK, "{}")

	_, err := Do(context.Background(), Request{
		Method:    http.MethodGet,
		URL:       server.URL + "/x",
		BasicAuth: &BasicAuth{User: "key", Password: "secret"},
	})
	if err != nil {
		t.Fatal(err)
	}
	user, password, ok := parseBasicAuth(got.header.Get("Authorization"))
	if !ok || user != "key" || password != "secret" {
		t.Errorf("expected basic auth, got %q", got.header.Get("Authorization"))
	}
}

func TestDoSkipsEmptyHeaders(t *testing.T) {
	// The clients pass whatever they were given, and an unauthenticated internal
	// call has no token. An empty Authorization header is worse than none: it makes
	// the far side see the header as present.
	server, got := serverRecording(t, http.StatusOK, "{}")

	_, err := Do(context.Background(), Request{
		Method:  http.MethodGet,
		URL:     server.URL + "/x",
		Headers: map[string]string{"Authorization": "", "X-UserId": "jonah"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.header["Authorization"]; ok {
		t.Errorf("an empty header should not be sent, got %v", got.header["Authorization"])
	}
	if got.header.Get("X-UserId") != "jonah" {
		t.Error("the non-empty header should still arrive")
	}
}

func TestDoReturnsTheErrorBeforeTheStatus(t *testing.T) {
	// The behaviour this replaced read resp.StatusCode first and checked the
	// transport error afterwards, which dereferences nil when the peer is
	// unreachable. Here an unreachable peer is an error and no response.
	response, err := Do(context.Background(), Request{
		Method: http.MethodGet,
		// Reserved by RFC 6761 for exactly this: guaranteed not to resolve.
		URL: "http://something.invalid/x",
	})
	if err == nil {
		t.Fatal("expected an error for an unreachable host")
	}
	if response.StatusCode != 0 {
		t.Errorf("expected no status, got %d", response.StatusCode)
	}
}

func TestDoHonoursACancelledContext(t *testing.T) {
	server, _ := serverRecording(t, http.StatusOK, "{}")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := Do(ctx, Request{Method: http.MethodGet, URL: server.URL + "/x"})
	if err == nil {
		t.Fatal("expected a cancelled context to fail the request")
	}
}

func TestDoPassesPreEncodedBodies(t *testing.T) {
	// Some callers marshalled the payload themselves. A string must go out as it
	// stands rather than being JSON-encoded a second time into a quoted string.
	server, got := serverRecording(t, http.StatusOK, "{}")

	_, err := Do(context.Background(), Request{
		Method: http.MethodPost,
		URL:    server.URL + "/x",
		Body:   `{"already":"json"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.body != `{"already":"json"}` {
		t.Errorf("expected the string to be sent verbatim, got %q", got.body)
	}
}

func TestDoReadsAnErrorBody(t *testing.T) {
	// Every client builds its error message out of the body, so a non-2xx response
	// still has to come back with it.
	server, _ := serverRecording(t, http.StatusUnprocessableEntity, `{"message":"invalid label"}`)

	response, err := Do(context.Background(), Request{Method: http.MethodPost, URL: server.URL + "/x", Body: map[string]string{}})
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusUnprocessableEntity {
		t.Errorf("expected 422, got %d", response.StatusCode)
	}
	if response.Text() != `{"message":"invalid label"}` {
		t.Errorf("expected the error body, got %q", response.Text())
	}
}

// parseBasicAuth reuses the standard library's parser through a throwaway request,
// so the test does not re-implement base64 decoding.
func parseBasicAuth(header string) (string, string, bool) {
	req := &http.Request{Header: http.Header{"Authorization": []string{header}}}
	return req.BasicAuth()
}
