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

type HistogramStore struct {
	Storage Storage[*collectors.HistogramMetric]
	Logger  log.Logger
}

func (s *HistogramStore) Increment(metricDescriptor *monitoring.MetricDescriptor, currentValue *collectors.HistogramMetric) {
	if currentValue == nil {
		return
	}
	key := toHistogramKey(currentValue)
	existing := s.Storage.Get(metricDescriptor.Name, key)

	if existing == nil {
		level.Debug(s.Logger).Log("msg", "Tracking new histogram", "fqName", currentValue.FqName, "key", key, "incoming_time", currentValue.ReportTime)
		s.Storage.Set(metricDescriptor.Name, key, currentValue)
		return
	}

	if existing.ReportTime.Before(currentValue.ReportTime) {
		level.Debug(s.Logger).Log("msg", "Incrementing existing histogram", "fqName", currentValue.FqName, "key", key, "last_reported_time", existing.ReportTime, "incoming_time", currentValue.ReportTime)
		s.Storage.Set(metricDescriptor.Name, key, mergeHistograms(existing, currentValue))
		return
	}

	level.Debug(s.Logger).Log("msg", "Ignoring old sample for histogram", "fqName", currentValue.FqName, "key", key, "last_reported_time", existing.ReportTime, "incoming_time", currentValue.ReportTime)
}

func toHistogramKey(hist *collectors.HistogramMetric) uint64 {
	labels := make(map[string]string)
	keysCopy := append([]string{}, hist.LabelKeys...)
	for i := range hist.LabelKeys {
		labels[hist.LabelKeys[i]] = hist.LabelValues[i]
	}
	sort.Strings(keysCopy)

	var keyParts []string
	for _, k := range keysCopy {
		keyParts = append(keyParts, fmt.Sprintf("%s:%s", k, labels[k]))
	}
	hashText := fmt.Sprintf("%s|%s", hist.FqName, strings.Join(keyParts, "|"))
	h := collectors.HashNew()
	h = collectors.HashAdd(h, hashText)

	return h
}

func mergeHistograms(existing *collectors.HistogramMetric, current *collectors.HistogramMetric) *collectors.HistogramMetric {
	for key, value := range existing.Buckets {
		current.Buckets[key] += value
	}

	// Calculate a new mean and overall count
	mean := existing.Mean
	mean += current.Mean
	mean /= 2

	var count uint64
	for _, v := range current.Buckets {
		count += v
	}

	current.Mean = mean
	current.Count = count

	return current
}

func (s *HistogramStore) ListMetrics(metricDescriptorName string) []*collectors.HistogramMetric {
	return s.Storage.List(metricDescriptorName)
}
