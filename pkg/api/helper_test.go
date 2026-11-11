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
	"errors"
	"log/slog"
	"testing"

	"github.com/SENERGY-Platform/analytics-flow-engine/lib"
)

func TestErrorLevel(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want slog.Level
	}{
		{"missing pipeline", lib.NewNotFoundError(errors.New("could not find pipeline x")), slog.LevelWarn},
		{"foreign pipeline", lib.NewForbiddenError(lib.NewNotFoundError(errors.New("could not access pipeline x"))), slog.LevelWarn},
		{"registry failure", errors.New("pipeline API - 500"), slog.LevelError},
	}
	for _, c := range cases {
		if got := errorLevel(c.err); got != c.want {
			t.Errorf("%s: errorLevel = %v, want %v", c.name, got, c.want)
		}
	}
}
