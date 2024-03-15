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
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"google.golang.org/api/monitoring/v3"

	"github.com/prometheus-community/stackdriver_exporter/collectors"
	"github.com/prometheus-community/stackdriver_exporter/hash"
)

type MetricEntry struct {
	Collected map[uint64]*collectors.ConstMetric
	mutex     *sync.RWMutex
}

type InMemoryCounterStore struct {
	store  *sync.Map
	ttl    time.Duration
	logger log.Logger
}

// NewInMemoryCounterStore returns an implementation of CounterStore which is persisted in-memory
func NewInMemoryCounterStore(logger log.Logger, ttl time.Duration) *InMemoryCounterStore {
	store := &InMemoryCounterStore{
		store:  &sync.Map{},
		logger: logger,
		ttl:    ttl,
	}

	return store
}

func (s *InMemoryCounterStore) Increment(metricDescriptor *monitoring.MetricDescriptor, incoming *collectors.ConstMetric) {
	if incoming == nil {
		return
	}

	tmp, _ := s.store.LoadOrStore(metricDescriptor.Name, &MetricEntry{
		Collected: map[uint64]*collectors.ConstMetric{},
		mutex:     &sync.RWMutex{},
	})
	entry := tmp.(*MetricEntry)

	key := toCounterKey(incoming)

	entry.mutex.Lock()
	defer entry.mutex.Unlock()
	existing := entry.Collected[key]

	if existing == nil {
		level.Debug(s.logger).Log("msg", "Tracking new counter", "fqName", incoming.FqName, "key", key, "current_value", incoming.Value, "incoming_time", incoming.ReportTime)
		entry.Collected[key] = incoming
		return
	}

	if existing.ReportTime.Before(incoming.ReportTime) {
		level.Debug(s.logger).Log("msg", "Incrementing existing counter", "fqName", incoming.FqName, "key", key, "current_value", existing.Value, "adding", incoming.Value, "last_reported_time", existing.ReportTime, "incoming_time", incoming.ReportTime)
		incoming.Value = incoming.Value + existing.Value
		entry.Collected[key] = incoming
		return
	}

	level.Debug(s.logger).Log("msg", "Ignoring old sample for counter", "fqName", incoming.FqName, "key", key, "last_reported_time", existing.ReportTime, "incoming_time", incoming.ReportTime)
}

func toCounterKey(c *collectors.ConstMetric) uint64 {
	labels := make(map[string]string)
	keysCopy := append([]string{}, c.LabelKeys...)
	for i := range c.LabelKeys {
		labels[c.LabelKeys[i]] = c.LabelValues[i]
	}
	sort.Strings(keysCopy)

	var keyParts []string
	for _, k := range keysCopy {
		keyParts = append(keyParts, fmt.Sprintf("%s:%s", k, labels[k]))
	}
	hashText := fmt.Sprintf("%s|%s", c.FqName, strings.Join(keyParts, "|"))
	h := hash.New()
	h = hash.Add(h, hashText)

	return h
}

func (s *InMemoryCounterStore) ListMetrics(metricDescriptorName string) []*collectors.ConstMetric {
	var output []*collectors.ConstMetric
	now := time.Now()
	ttlWindowStart := now.Add(-s.ttl)

	tmp, exists := s.store.Load(metricDescriptorName)
	if !exists {
		return output
	}
	entry := tmp.(*MetricEntry)

	entry.mutex.Lock()
	defer entry.mutex.Unlock()
	for key, collected := range entry.Collected {
		// Scan and remove metrics which are outside the TTL
		if ttlWindowStart.After(collected.CollectionTime) {
			level.Debug(s.logger).Log("msg", "Deleting counter entry outside of TTL", "key", key, "fqName", collected.FqName)
			delete(entry.Collected, key)
			continue
		}

		// Dereference to create shallow copy
		metricCopy := *collected
		output = append(output, &metricCopy)
	}

	return output
}
