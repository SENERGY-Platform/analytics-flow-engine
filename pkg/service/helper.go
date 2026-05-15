/*
 * Copyright 2018 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package service

import (
	"fmt"
	"time"

	"github.com/SENERGY-Platform/analytics-flow-engine/pkg/util"
)

func StringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func retry(attempts int, sleep time.Duration, f func() error) (err error) {
	for i := 0; ; i++ {
		err = f()
		if err == nil {
			return
		}

		if i >= (attempts - 1) {
			break
		}

		time.Sleep(sleep)

		util.Logger.Debug("retrying after error", "error", err, "attempt", i+1, "of", attempts)
	}
	return fmt.Errorf("after %d attempts, last error: %s", attempts, err)
}

func CompareSlicesWithKey[T1, T2 any](
	slice1 []T1,
	slice2 []T2,
	keyFunc1 func(T1) string,
	keyFunc2 func(T2) string,
) (missing []T1, extra []T2) {
	lookup := make(map[string]bool)
	for _, item := range slice2 {
		lookup[keyFunc2(item)] = true
	}

	for _, item := range slice1 {
		if !lookup[keyFunc1(item)] {
			missing = append(missing, item)
		}
	}

	for _, item := range slice2 {
		if !containsKeyInSlice(slice1, keyFunc2(item), keyFunc1) {
			extra = append(extra, item)
		}
	}

	return missing, extra
}

// Helper function to check if a key exists in a slice
func containsKeyInSlice[T any](slice []T, key string, keyFunc func(T) string) bool {
	for _, item := range slice {
		if keyFunc(item) == key {
			return true
		}
	}
	return false
}
