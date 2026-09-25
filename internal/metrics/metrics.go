package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// sinkLatencyBuckets defines histogram buckets for sink delivery latency in seconds.
var sinkLatencyBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// Metrics holds all Prometheus metrics for the goose firehose. Metrics are
// registered with a custom (non-default) registry so tests are isolated and
// metric name collisions with other packages are avoided.
type Metrics struct {
	registry *prometheus.Registry

	MessagesConsumed     *prometheus.CounterVec
	MessagesFiltered     *prometheus.CounterVec
	MessagesDelivered    *prometheus.CounterVec
	MessagesRetried      *prometheus.CounterVec
	MessagesDLQ          *prometheus.CounterVec
	MessagesIgnored      *prometheus.CounterVec
	SinkLatency          *prometheus.HistogramVec
	ConsumerLag          *prometheus.GaugeVec
	WorkerPoolQueueDepth prometheus.Gauge
	WorkerPoolSize       prometheus.Gauge
	CircuitBreakerOpen   *prometheus.GaugeVec
	HTTPResponseCodes    *prometheus.CounterVec
	OffsetCommits        *prometheus.CounterVec
	SchemaUpdates        *prometheus.CounterVec
	ValidationFailed     *prometheus.CounterVec
}

// NewMetrics creates all firehose metrics and registers them with a custom
// prometheus.Registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{registry: reg}

	m.MessagesConsumed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "firehose_messages_consumed_total",
		Help: "Total number of messages consumed from Kafka.",
	}, []string{"topic", "partition"})

	m.MessagesFiltered = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "firehose_messages_filtered_total",
		Help: "Total number of messages filtered out.",
	}, []string{"topic"})

	m.MessagesDelivered = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "firehose_messages_delivered_total",
		Help: "Total number of messages delivered to the sink.",
	}, []string{"sink"})

	m.MessagesRetried = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "firehose_messages_retried_total",
		Help: "Total number of messages retried.",
	}, []string{"error_type"})

	m.MessagesDLQ = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "firehose_messages_dlq_total",
		Help: "Total number of messages sent to the dead-letter queue.",
	}, []string{"error_type"})

	m.MessagesIgnored = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "firehose_messages_ignored_total",
		Help: "Total number of messages ignored.",
	}, []string{"error_type"})

	m.SinkLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "firehose_sink_latency_seconds",
		Help:    "Sink delivery latency in seconds.",
		Buckets: sinkLatencyBuckets,
	}, []string{"sink"})

	m.ConsumerLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "firehose_consumer_lag_messages",
		Help: "Consumer lag in messages per topic/partition.",
	}, []string{"topic", "partition"})

	m.WorkerPoolQueueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "firehose_worker_pool_queue_depth",
		Help: "Current depth of the worker pool queue.",
	})

	m.WorkerPoolSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "firehose_worker_pool_size",
		Help: "Current number of workers in the pool.",
	})

	m.CircuitBreakerOpen = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "firehose_circuit_breaker_open",
		Help: "Circuit breaker open state (1 = open, 0 = closed).",
	}, []string{"sink"})

	m.HTTPResponseCodes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "firehose_http_response_code_total",
		Help: "Total HTTP response codes from sink calls.",
	}, []string{"status_code"})

	m.OffsetCommits = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "firehose_offset_commit_total",
		Help: "Total number of offset commits per topic/partition.",
	}, []string{"topic", "partition"})

	m.SchemaUpdates = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "firehose_schema_updates_total",
		Help: "Total number of schema registry updates.",
	}, []string{"proto_class"})

	m.ValidationFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "firehose_messages_validation_failed_total",
		Help: "Messages that failed schema validation",
	}, []string{"reason"})

	reg.MustRegister(
		m.MessagesConsumed,
		m.MessagesFiltered,
		m.MessagesDelivered,
		m.MessagesRetried,
		m.MessagesDLQ,
		m.MessagesIgnored,
		m.SinkLatency,
		m.ConsumerLag,
		m.WorkerPoolQueueDepth,
		m.WorkerPoolSize,
		m.CircuitBreakerOpen,
		m.HTTPResponseCodes,
		m.OffsetCommits,
		m.SchemaUpdates,
		m.ValidationFailed,
	)

	return m
}

// Handler returns an http.Handler that exposes the metrics registered in the
// custom registry, suitable for mounting at /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
