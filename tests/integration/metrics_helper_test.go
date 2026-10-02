package integration

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Eroniction14/Stream_Processor/internal/metrics"
)

// newIsolatedMetrics creates a Metrics instance registered to its own
// registry so multiple tests don't conflict with the global registry.
func newIsolatedMetrics(namespace string) *metrics.Metrics {
	reg := prometheus.NewRegistry()

	m := &metrics.Metrics{
		MessagesReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_received_total",
		}),
		MessagesProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_processed_total",
		}),
		MessagesEmitted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_emitted_total",
		}),
		ErrorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "errors_total",
		}),
		RetriesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "retries_total",
		}),
		DLQMessages: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "dlq_messages_total",
		}),
		ProcessingLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "processing_latency_seconds",
			Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
		}),
		ActiveWindows: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "active_windows",
		}),
	}

	reg.MustRegister(
		m.MessagesReceived, m.MessagesProcessed, m.MessagesEmitted,
		m.ErrorsTotal, m.RetriesTotal, m.DLQMessages,
		m.ProcessingLatency, m.ActiveWindows,
	)

	return m
}
