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
	"context"
	"log/slog"
	"os"
	"os/signal"
)

func Wait(ctx context.Context, logger *slog.Logger, signals ...os.Signal) {
	ch := make(chan os.Signal, 1)
	for _, sig := range signals {
		signal.Notify(ch, sig)
	}
	select {
	case sig := <-ch:
		logger.Warn("caught os signal", "signal", sig.String())
		break
	case <-ctx.Done():
		break
	}
	signal.Stop(ch)
}
