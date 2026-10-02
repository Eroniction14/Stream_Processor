package metrics

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// latencyBuckets span 10µs to 1s. Processing a single event usually takes
// well under a millisecond, so buckets must start in microseconds: with a
// smallest bucket of 1ms, every observation fell into it and p99 always
// read as ~0.99ms regardless of real latency.
var latencyBuckets = []float64{
	0.00001, 0.000025, 0.00005, // 10µs, 25µs, 50µs
	0.0001, 0.00025, 0.0005, // 100µs, 250µs, 500µs
	0.001, 0.0025, 0.005, // 1ms, 2.5ms, 5ms
	0.01, 0.025, 0.05, // 10ms, 25ms, 50ms
	0.1, 0.25, 0.5, 1, // 100ms, 250ms, 500ms, 1s
}

type Metrics struct {
	MessagesReceived  prometheus.Counter
	MessagesProcessed prometheus.Counter
	MessagesEmitted   prometheus.Counter
	ErrorsTotal       prometheus.Counter
	RetriesTotal      prometheus.Counter
	DLQMessages       prometheus.Counter
	ProcessingLatency prometheus.Histogram
	ActiveWindows     prometheus.Gauge
}

func New(namespace string) *Metrics {
	m := &Metrics{
		MessagesReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_received_total",
			Help:      "Total messages consumed from Kafka",
		}),
		MessagesProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_processed_total",
			Help:      "Total messages successfully processed",
		}),
		MessagesEmitted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_emitted_total",
			Help:      "Total messages emitted to output topic",
		}),
		ErrorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "errors_total",
			Help:      "Total processing errors",
		}),
		RetriesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "retries_total",
			Help:      "Total retry attempts",
		}),
		DLQMessages: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "dlq_messages_total",
			Help:      "Total messages sent to dead-letter queue",
		}),
		ProcessingLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "processing_latency_seconds",
			Help:      "Message processing latency",
			Buckets:   latencyBuckets,
		}),
		ActiveWindows: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "active_windows",
			Help:      "Number of active aggregation windows",
		}),
	}

	prometheus.MustRegister(
		m.MessagesReceived, m.MessagesProcessed, m.MessagesEmitted,
		m.ErrorsTotal, m.RetriesTotal, m.DLQMessages,
		m.ProcessingLatency, m.ActiveWindows,
	)

	return m
}

func (m *Metrics) ServeHTTP(port int) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(":"+strconv.Itoa(port), mux)
}
