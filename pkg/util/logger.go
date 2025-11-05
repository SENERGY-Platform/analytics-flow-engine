/*
 * Copyright 2025 InfAI (CC SES)
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

package util

import (
	"log/slog"
	"os"
	"runtime/debug"
	"strings"
	"time"

	structlogger "github.com/SENERGY-Platform/go-service-base/struct-logger"
)

var Logger *slog.Logger

func InitStructLogger(level string) {
	if Logger == nil {
		info, ok := debug.ReadBuildInfo()
		project := "github.com/SENERGY-Platform"
		org := "analytics-flow-engine"
		if ok {
			if parts := strings.Split(info.Main.Path, "/"); len(parts) > 2 {
				project = strings.Join(parts[2:], "/")
				org = strings.Join(parts[:2], "/")
			}
		}
		Logger = structlogger.New(
			structlogger.Config{
				Handler:    structlogger.JsonHandlerSelector,
				Level:      level,
				TimeFormat: time.RFC3339Nano,
				TimeUtc:    true,
				AddMeta:    true,
			},
			os.Stdout,
			org,
			project,
		)
	}
}
