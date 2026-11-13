/*
 * Copyright 2026 InfAI (CC SES)
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

import "sync"

// pipelineLocks serialises the requests that stop and rebuild one pipeline. Without it,
// two concurrent updates both read the old pipeline from the registry, and the second
// deletes the deployment the first has just created.
type pipelineLocks struct {
	mu    sync.Mutex
	locks map[string]*pipelineLock
}

type pipelineLock struct {
	sync.Mutex
	holders int
}

func newPipelineLocks() *pipelineLocks {
	return &pipelineLocks{locks: map[string]*pipelineLock{}}
}

// lock blocks until no other request works on the pipeline, and returns the unlock.
func (p *pipelineLocks) lock(pipelineId string) (unlock func()) {
	p.mu.Lock()
	l := p.locks[pipelineId]
	if l == nil {
		l = &pipelineLock{}
		p.locks[pipelineId] = l
	}
	l.holders++
	p.mu.Unlock()

	l.Lock()
	return func() {
		l.Unlock()
		p.mu.Lock()
		l.holders--
		if l.holders == 0 {
			delete(p.locks, pipelineId)
		}
		p.mu.Unlock()
	}
}
