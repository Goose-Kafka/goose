package tracing

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

type Config struct {
	Enabled      bool
	OTLPEndpoint string
	ServiceName  string
}

type TracerProvider struct {
	tp *sdktrace.TracerProvider
}

func NewTracerProvider(cfg Config) (*TracerProvider, error) {
	if !cfg.Enabled {
		tp := sdktrace.NewTracerProvider()
		otel.SetTracerProvider(tp)
		return &TracerProvider{tp: tp}, nil
	}

	res, err := resource.New(context.Background(),
		resource.WithAttributes(semconv.ServiceName(cfg.ServiceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	var exporter sdktrace.SpanExporter
	if cfg.OTLPEndpoint != "" {
		exporter, err = otlptracegrpc.New(context.Background(),
			otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
			otlptracegrpc.WithInsecure(),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
		}
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return &TracerProvider{tp: tp}, nil
}

func (p *TracerProvider) Tracer(name string) trace.Tracer {
	return p.tp.Tracer(name)
}

func (p *TracerProvider) Shutdown(ctx context.Context) error {
	return p.tp.Shutdown(ctx)
}
