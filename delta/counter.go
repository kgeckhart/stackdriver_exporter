// Copyright 2022 The Prometheus Authors
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

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"google.golang.org/api/monitoring/v3"

	"github.com/prometheus-community/stackdriver_exporter/collectors"
)

type CounterStore struct {
	Storage Storage[*collectors.ConstMetric]
	Logger  log.Logger
}

func (s *CounterStore) Increment(metricDescriptor *monitoring.MetricDescriptor, currentValue *collectors.ConstMetric) {
	if currentValue == nil {
		return
	}
	key := toCounterKey(currentValue)
	existing := s.Storage.Get(metricDescriptor.Name, key)

	if existing == nil {
		level.Debug(s.Logger).Log("msg", "Tracking new counter", "fqName", currentValue.FqName, "key", key, "current_value", currentValue.Value, "incoming_time", currentValue.ReportTime)
		s.Storage.Set(metricDescriptor.Name, key, currentValue)
		return
	}

	if existing.ReportTime.Before(currentValue.ReportTime) {
		level.Debug(s.Logger).Log("msg", "Incrementing existing counter", "fqName", currentValue.FqName, "key", key, "current_value", existing.Value, "adding", currentValue.Value, "last_reported_time", existing.ReportTime, "incoming_time", currentValue.ReportTime)
		currentValue.Value = currentValue.Value + existing.Value
		s.Storage.Set(metricDescriptor.Name, key, currentValue)
		return
	}

	level.Debug(s.Logger).Log("msg", "Ignoring old sample for counter", "fqName", currentValue.FqName, "key", key, "last_reported_time", existing.ReportTime, "incoming_time", currentValue.ReportTime)
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
	h := collectors.HashNew()
	h = collectors.HashAdd(h, hashText)

	return h
}

func (s *CounterStore) ListMetrics(metricDescriptorName string) []*collectors.ConstMetric {
	return s.Storage.List(metricDescriptorName)
}
