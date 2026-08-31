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

package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
	fe_baggage "github.com/SENERGY-Platform/analytics-flow-engine/pkg/baggage"
	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"
	gin_mw "github.com/SENERGY-Platform/gin-middleware"
	"github.com/SENERGY-Platform/gin-middleware/otelx"
	"github.com/gin-gonic/gin"
)

func init() {
	util.InitStructLogger("error")
	gin.SetMode(gin.TestMode)
}

// tokenWithUsername builds an unsigned JWT the way the platform's own parser reads
// it: the middleware only decodes the claims, it does not verify a signature.
func tokenWithUsername(t *testing.T, username string) string {
	t.Helper()
	claims := map[string]any{
		"sub":                "8fbd0e8a-0000-0000-0000-000000000000",
		"preferred_username": username,
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	encode := base64.RawURLEncoding.EncodeToString
	return "Bearer " + encode([]byte(`{"alg":"none","typ":"JWT"}`)) + "." + encode(payload) + "."
}

// routerLikeCreateServer mirrors the middleware chain CreateServer installs, which
// is the thing under test: the OpenTelemetry handler and the error handler interact,
// and neither is interesting on its own.
func routerLikeCreateServer(t *testing.T) *gin.Engine {
	t.Helper()
	router := gin.New()
	otelHandler, err := otelx.GinOpenTelemetry(context.Background(), ServiceName, "")
	if err != nil {
		t.Fatal(err)
	}
	router.Use(
		otelHandler,
		DiscardBaggageErrors(),
		gin_mw.ErrorHandler(util.GetStatusCode, ", "),
	)
	router.GET("/pipeline/:id", func(c *gin.Context) {
		c.JSON(http.StatusOK, map[string]string{"id": c.Param("id")})
	})
	router.DELETE("/pipeline/:id", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})
	return router
}

// A username the W3C baggage format cannot carry must not change the response.
//
// otelx reports a rejected baggage value with gin's c.Error, and gin_mw.ErrorHandler
// turns anything in c.Errors into a response: it forces a 500 where the status was
// below 400 and appends the error text to the body. Without DiscardBaggageErrors a
// user called "Jonah Windolph" gets a 500 on every DELETE and a JSON body with an
// error message glued to the end of it on every GET.
func TestBaggageThatCannotBeCarriedDoesNotBreakTheResponse(t *testing.T) {
	usernames := map[string]string{
		"a space":     "Jonah Windolph",
		"a comma":     "Windolph,Jonah",
		"non-ascii":   "müller",
		"a semicolon": "a;b",
	}
	for name, username := range usernames {
		t.Run(name, func(t *testing.T) {
			router := routerLikeCreateServer(t)

			t.Run("GET keeps a clean body", func(t *testing.T) {
				recorder := httptest.NewRecorder()
				request := httptest.NewRequest(http.MethodGet, "/pipeline/3c1f9b42", nil)
				request.Header.Set("Authorization", tokenWithUsername(t, username))
				router.ServeHTTP(recorder, request)

				if recorder.Code != http.StatusOK {
					t.Errorf("expected 200, got %d: %s", recorder.Code, recorder.Body.String())
				}
				var decoded map[string]string
				if err := json.Unmarshal(recorder.Body.Bytes(), &decoded); err != nil {
					t.Fatalf("the body must stay valid JSON, got %q: %v", recorder.Body.String(), err)
				}
				if decoded["id"] != "3c1f9b42" {
					t.Errorf("unexpected body: %v", decoded)
				}
			})

			t.Run("DELETE keeps its status", func(t *testing.T) {
				recorder := httptest.NewRecorder()
				request := httptest.NewRequest(http.MethodDelete, "/pipeline/3c1f9b42", nil)
				request.Header.Set("Authorization", tokenWithUsername(t, username))
				router.ServeHTTP(recorder, request)

				if recorder.Code != http.StatusNoContent {
					t.Errorf("expected 204, got %d: %s", recorder.Code, recorder.Body.String())
				}
			})
		})
	}
}

// A username that is carried fine must still reach the baggage, so the guard above
// cannot be a blanket suppression.
func TestAValidUsernameStillReachesTheBaggage(t *testing.T) {
	router := gin.New()
	otelHandler, err := otelx.GinOpenTelemetry(context.Background(), ServiceName, "")
	if err != nil {
		t.Fatal(err)
	}
	var seen map[string]string
	router.Use(otelHandler, DiscardBaggageErrors())
	router.GET("/x", func(c *gin.Context) {
		seen = fe_baggage.FromContext(c.Request.Context())
		c.Status(http.StatusOK)
	})

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/x", nil)
	request.Header.Set("Authorization", tokenWithUsername(t, "jonah"))
	router.ServeHTTP(recorder, request)

	if seen["username"] != "jonah" {
		t.Errorf("expected the username in the baggage, got %v", seen)
	}
	if seen["user_id"] != "8fbd0e8a-0000-0000-0000-000000000000" {
		t.Errorf("expected the user id in the baggage, got %v", seen)
	}
}

// An error a real handler reports must still become a response, so the guard cannot
// be swallowing everything.
func TestAHandlerErrorStillBecomesAResponse(t *testing.T) {
	router := gin.New()
	otelHandler, err := otelx.GinOpenTelemetry(context.Background(), ServiceName, "")
	if err != nil {
		t.Fatal(err)
	}
	router.Use(otelHandler, DiscardBaggageErrors(), gin_mw.ErrorHandler(util.GetStatusCode, ", "))
	handlerError := lib.NewNotFoundError(errors.New("no such pipeline"))
	router.GET("/x", func(c *gin.Context) {
		_ = c.Error(handlerError)
	})

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/x", nil)
	// A username that also fails the baggage, so both kinds of error are present.
	request.Header.Set("Authorization", tokenWithUsername(t, "Jonah Windolph"))
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Errorf("expected the handler's own error to set the status, got %d: %s",
			recorder.Code, recorder.Body.String())
	}
	if recorder.Body.String() != handlerError.Error() {
		t.Errorf("expected only the handler's error in the body, got %q", recorder.Body.String())
	}
}
