package delta_test

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/prometheus/common/promlog"
	"google.golang.org/api/monitoring/v3"

	"github.com/prometheus-community/stackdriver_exporter/collectors"
	"github.com/prometheus-community/stackdriver_exporter/delta"
)

var _ = Describe("HistogramStore", func() {
	var store delta.HistogramStore
	var histogram *collectors.HistogramMetric
	descriptor := &monitoring.MetricDescriptor{Name: "This is a metric"}
	logger := promlog.New(&promlog.Config{})

	BeforeEach(func() {
		store = delta.HistogramStore{
			Storage: delta.NewInMemoryStorage[*collectors.HistogramMetric](logger, time.Minute),
			Logger:  logger,
		}
		histogram = &collectors.HistogramMetric{
			FqName:         "histogram_name",
			LabelKeys:      []string{"labelKey"},
			Mean:           10,
			Count:          100,
			Buckets:        map[float64]uint64{1.00000000000000000001: 1000},
			LabelValues:    []string{"labelValue"},
			ReportTime:     time.Now().Truncate(time.Second),
			CollectionTime: time.Now().Truncate(time.Second),
			KeysHash:       8765,
		}
	})

	It("can return tracked histograms", func() {
		store.Increment(descriptor, histogram)
		metrics := store.ListMetrics(descriptor.Name)

		Expect(len(metrics)).To(Equal(1))
		Expect(metrics[0]).To(Equal(histogram))
	})

	It("will remove histograms outside of TTL", func() {
		histogram.CollectionTime = histogram.CollectionTime.Add(-time.Hour)

		store.Increment(descriptor, histogram)

		metrics := store.ListMetrics(descriptor.Name)
		Expect(len(metrics)).To(Equal(0))
	})
})
