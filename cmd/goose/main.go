package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/arelligoutham/goose/internal/config"
	errorpkg "github.com/arelligoutham/goose/internal/error"
	"github.com/arelligoutham/goose/internal/metrics"
	"github.com/arelligoutham/goose/internal/offset/offsetmanager"
	"github.com/arelligoutham/goose/internal/consumer"
	"github.com/arelligoutham/goose/internal/sink"
	"github.com/arelligoutham/goose/internal/tracing"
	"github.com/arelligoutham/goose/internal/worker"
)

func main() {
	// 1. Load configuration from environment variables.
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// 2. Initialize tracing (OpenTelemetry).
	tp, err := tracing.NewTracerProvider(tracing.Config{
		Enabled:      cfg.Tracing.Enabled,
		OTLPEndpoint: cfg.Tracing.OTLPEndpoint,
		ServiceName:  cfg.Tracing.ServiceName,
	})
	if err != nil {
		log.Fatalf("failed to init tracing: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(ctx); err != nil {
			log.Printf("tracing shutdown error: %v", err)
		}
	}()

	// 3. Initialize metrics and start the /metrics HTTP server.
	m := metrics.NewMetrics()
	if cfg.Metrics.PrometheusEnabled {
		go func() {
			metricsMux := http.NewServeMux()
			metricsMux.Handle("/metrics", m.Handler())
			addr := fmt.Sprintf(":%d", cfg.Metrics.PrometheusPort)
			log.Printf("metrics server listening on %s", addr)
			if err := http.ListenAndServe(addr, metricsMux); err != nil {
				log.Printf("metrics server error: %v", err)
			}
		}()
	}

	// 4. Create the offset manager for at-least-once delivery.
	offsetMgr := offsetmanager.New()

	// 5. Create the batch and done channels.
	batchChan := make(chan *worker.Batch, cfg.HTTP.WorkerPoolBuffer)
	doneChan := make(chan string, cfg.HTTP.WorkerPoolBuffer)

	// 6. Create a root context that workers and the consumer share.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 7. Create the HTTP sink.
	httpSink := sink.NewHTTPSink(sink.HTTPSinkConfig{
		ServiceURL:           cfg.HTTP.ServiceURL,
		RequestMethod:        cfg.HTTP.RequestMethod,
		Timeout:              cfg.HTTP.RequestTimeoutMs,
		MaxConnections:       cfg.HTTP.MaxConnections,
		ConnectionTTL:        cfg.HTTP.ConnectionTtlMs,
		ConnectionIdleEvict:  cfg.HTTP.ConnectionIdleEvictMs,
		ValidateInactivityMs: cfg.HTTP.ConnectionValidateInactivityMs,
		Headers:              headersToString(cfg.HTTP.Headers),
		JSONBodyTemplate:     cfg.HTTP.JSONBodyTemplate,
		DataFormat:           cfg.HTTP.DataFormat,
	})
	defer httpSink.Close()

	// 8. Create error handler, exponential backoff, and circuit breaker.
	errorHandler := errorpkg.NewErrorHandler(
		cfg.HTTP.ErrorRetryStatusCodes,
		cfg.HTTP.ErrorDLQStatusCodes,
		cfg.HTTP.ErrorFailStatusCodes,
	)
	backoff := errorpkg.NewExponentialBackoff(
		cfg.HTTP.RetryBackoffInitialMs,
		cfg.HTTP.RetryBackoffMaxMs,
		cfg.HTTP.RetryBackoffMultiplier,
	)
	var circuitBreaker *errorpkg.CircuitBreaker
	if cfg.HTTP.CircuitBreakerEnabled {
		circuitBreaker = errorpkg.NewCircuitBreaker(
			cfg.HTTP.CircuitBreakerFailureThreshold,
			cfg.HTTP.CircuitBreakerWindowSize,
			time.Duration(cfg.HTTP.CircuitBreakerResetTimeoutMs)*time.Millisecond,
		)
	}

	// 9. Start N worker goroutines.
	var wg sync.WaitGroup
	m.WorkerPoolSize.Set(float64(cfg.HTTP.WorkerPoolSize))
	for i := 0; i < cfg.HTTP.WorkerPoolSize; i++ {
		wg.Add(1)
		w := worker.NewWorker(
			i,
			httpSink,
			errorHandler,
			backoff,
			circuitBreaker,
			doneChan,
			cfg.HTTP.RetryMaxAttempts,
			nil, // DLQ writer — wired in a later task
		)
		go w.Run(ctx, batchChan, &wg)
	}
	log.Printf("started %d worker goroutines", cfg.HTTP.WorkerPoolSize)

	// 10. Create the Kafka consumer.
	c, err := consumer.New(cfg, batchChan, doneChan, offsetMgr, m)
	if err != nil {
		log.Fatalf("failed to create consumer: %v", err)
	}

	// 11. Start the offset commit loop goroutine.
	go commitLoop(ctx, offsetMgr, time.Duration(cfg.Kafka.CommitIntervalMs)*time.Millisecond, doneChan)

	// 12. Start the consumer goroutine.
	go func() {
		if err := c.Run(ctx); err != nil {
			log.Printf("consumer stopped: %v", err)
		}
	}()

	// 13. Wait for SIGINT/SIGTERM.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	log.Printf("received signal %v, shutting down gracefully...", sig)

	// 14. Graceful shutdown: cancel context, close channels, wait for workers.
	cancel()
	close(batchChan)
	wg.Wait()
	close(doneChan)
	log.Println("shutdown complete")
}

// commitLoop periodically retrieves committable offsets from the offset
// manager and logs them. When a doneChan message arrives, it marks the
// corresponding batch's offsets as committable. Actual Kafka offset commits
// will be wired in a subsequent task.
func commitLoop(ctx context.Context, offsetMgr *offsetmanager.OffsetManager, interval time.Duration, doneChan <-chan string) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case batchID := <-doneChan:
			offsetMgr.SetCommittable(batchID)
		case <-ticker.C:
			committable := offsetMgr.GetCommittable()
			for tp, offset := range committable {
				log.Printf("offset commit: topic=%s partition=%d offset=%d", tp.Topic, tp.Partition, offset)
			}
		}
	}
}

// headersToString converts a map of HTTP headers to a "Key:Value,Key:Value"
// string that the HTTP sink can parse.
func headersToString(headers map[string]string) string {
	if len(headers) == 0 {
		return ""
	}
	parts := make([]string, 0, len(headers))
	for k, v := range headers {
		parts = append(parts, k+":"+v)
	}
	return strings.Join(parts, ",")
}
