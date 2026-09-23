package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMessagesConsumedCounter(t *testing.T) {
	m := NewMetrics()
	m.MessagesConsumed.WithLabelValues("test-topic", "0").Inc()
	m.MessagesConsumed.WithLabelValues("test-topic", "0").Inc()

	if got := testutil.ToFloat64(m.MessagesConsumed); got != 2 {
		t.Errorf("MessagesConsumed: expected 2, got %v", got)
	}
}

func TestSinkLatencyHistogram(t *testing.T) {
	m := NewMetrics()
	m.SinkLatency.WithLabelValues("http").Observe(0.050)
	m.SinkLatency.WithLabelValues("http").Observe(0.150)

	mfs, err := m.registry.Gather()
	if err != nil {
		t.Fatalf("unexpected error gathering metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == "firehose_sink_latency_seconds" {
			for _, m2 := range mf.GetMetric() {
				if got := m2.GetHistogram().GetSampleCount(); got != 2 {
					t.Errorf("SinkLatency: expected 2 observations, got %v", got)
				}
				return
			}
		}
	}
	t.Error("SinkLatency: metric not found in registry")
}

func TestCircuitBreakerGauge(t *testing.T) {
	m := NewMetrics()
	m.CircuitBreakerOpen.WithLabelValues("http").Set(1)

	if got := testutil.ToFloat64(m.CircuitBreakerOpen); got != 1 {
		t.Errorf("CircuitBreakerOpen: expected 1, got %v", got)
	}
}

func TestWorkerPoolQueueDepth(t *testing.T) {
	m := NewMetrics()
	m.WorkerPoolQueueDepth.Set(5)

	if got := testutil.ToFloat64(m.WorkerPoolQueueDepth); got != 5 {
		t.Errorf("WorkerPoolQueueDepth: expected 5, got %v", got)
	}
}

func TestMetricsExposition(t *testing.T) {
	m := NewMetrics()
	m.MessagesConsumed.WithLabelValues("test-topic", "0").Inc()

	// Gather from the custom registry and verify the "firehose" prefix
	mfs, err := m.registry.Gather()
	if err != nil {
		t.Fatalf("unexpected error gathering metrics: %v", err)
	}
	var found bool
	for _, mf := range mfs {
		if strings.Contains(mf.GetName(), "firehose") {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected at least one metric name containing 'firehose'")
	}
}
