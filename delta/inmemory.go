// Copyright 2023 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package delta

import (
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

type entry[T StorableMetric] struct {
	collected map[uint64]T
	mutex     *sync.RWMutex
}

func NewInMemoryStorage[T StorableMetric](logger log.Logger, ttl time.Duration) Storage[T] {
	storage := &inMemoryStorage[T]{
		data:   &sync.Map{},
		logger: logger,
		ttl:    ttl,
	}

	return storage
}

type inMemoryStorage[T StorableMetric] struct {
	data   *sync.Map
	logger log.Logger
	ttl    time.Duration
}

func (s inMemoryStorage[T]) Get(metricDescriptorName string, key uint64) (value T) {
	tmp, exists := s.data.Load(metricDescriptorName)
	if !exists {
		return
	}
	entry := tmp.(*entry[T])
	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	c, exists := entry.collected[key]
	if !exists {
		return
	}
	return c
}

func (s inMemoryStorage[T]) Set(metricDescriptorName string, key uint64, value T) {
	tmp, _ := s.data.LoadOrStore(metricDescriptorName, &entry[T]{
		collected: map[uint64]T{},
		mutex:     &sync.RWMutex{},
	})
	entry := tmp.(*entry[T])
	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	entry.collected[key] = value
}

func (s inMemoryStorage[T]) List(metricDescriptorName string) []T {
	var output []T
	ttlWindowStart := time.Now().Add(-s.ttl)

	tmp, exists := s.data.Load(metricDescriptorName)
	if !exists {
		return output
	}
	entry := tmp.(*entry[T])
	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	for key, collected := range entry.collected {
		if ttlWindowStart.After(collected.LastCollectedAt()) {
			level.Debug(s.logger).Log("msg", "Deleting counter entry outside of TTL", "key", key)
			delete(entry.collected, key)
			continue
		}
		output = append(output, collected)
	}

	return output
}
