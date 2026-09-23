package tracing

import (
	"context"
	"testing"
)

func TestNewTracerDisabled(t *testing.T) {
	cfg := Config{Enabled: false}
	tp, err := NewTracerProvider(cfg)
	if err != nil {
		t.Fatalf("NewTracerProvider(disabled) failed: %v", err)
	}

	tr := tp.Tracer("test")
	_, span := tr.Start(context.Background(), "test-span")
	span.End()

	if err := tp.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}
}

func TestNewTracerEnabledNoExporter(t *testing.T) {
	cfg := Config{Enabled: true, OTLPEndpoint: "", ServiceName: "goose"}
	tp, err := NewTracerProvider(cfg)
	if err != nil {
		t.Fatalf("NewTracerProvider(enabled, no exporter) failed: %v", err)
	}

	tr := tp.Tracer("test")
	_, span := tr.Start(context.Background(), "test-span")
	span.End()

	if err := tp.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}
}
