package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Goose-Kafka/goose/internal/config"
	"github.com/Goose-Kafka/goose/internal/consumer"
	errorpkg "github.com/Goose-Kafka/goose/internal/error"
	"github.com/Goose-Kafka/goose/internal/metrics"
	"github.com/Goose-Kafka/goose/internal/offset/offsetmanager"
	"github.com/Goose-Kafka/goose/internal/sink"
	"github.com/Goose-Kafka/goose/internal/tracing"
	"github.com/Goose-Kafka/goose/internal/worker"
	"github.com/segmentio/kafka-go"
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
		BatchMode:            cfg.HTTP.BatchMode,
		BatchMaxSize:         cfg.HTTP.BatchMaxSize,
		BatchSeqField:        cfg.HTTP.BatchSeqField,
		BatchRespPath:        cfg.HTTP.BatchRespPath,
		BatchRespSeqField:    cfg.HTTP.BatchRespSeqField,
		BatchRespStatusField: cfg.HTTP.BatchRespStatusField,
		BatchRespErrorField:  cfg.HTTP.BatchRespErrorField,
		BatchRespRetryFlag:   cfg.HTTP.BatchRespRetryFlag,
		BatchRespSuccessVal:  cfg.HTTP.BatchRespSuccessVal,
	})
	defer httpSink.Close()

	// 7a. Select the process sink based on SinkType. Currently only "http" is
	// implemented; any other value falls back to the HTTP sink.
	var processSink sink.Sink
	switch cfg.SinkType {
	case "http":
		processSink = httpSink
	default:
		processSink = httpSink
	}

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

	// 8b. Create the DLQ writer (if enabled).
	var dlqWriter worker.DLQWriter
	if cfg.HTTP.DLQEnabled && cfg.HTTP.DLQType == "kafka" && cfg.HTTP.DLQKafkaBrokers != "" {
		dw := errorpkg.NewKafkaDLQWriter(cfg.HTTP.DLQKafkaBrokers, cfg.HTTP.DLQKafkaTopic)
		dlqWriter = dw
		defer dw.Close()
		log.Printf("DLQ writer enabled: topic=%s brokers=%s", cfg.HTTP.DLQKafkaTopic, cfg.HTTP.DLQKafkaBrokers)
	}

	// 9. Start N worker goroutines.
	var wg sync.WaitGroup
	m.WorkerPoolSize.Set(float64(cfg.HTTP.WorkerPoolSize))
	for i := 0; i < cfg.HTTP.WorkerPoolSize; i++ {
		wg.Add(1)
		w := worker.NewWorker(
			i,
			processSink,
			errorHandler,
			backoff,
			circuitBreaker,
			doneChan,
			cfg.HTTP.RetryMaxAttempts,
			dlqWriter,
			m,
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
	go commitLoop(ctx, offsetMgr, c.Reader(), time.Duration(cfg.Kafka.CommitIntervalMs)*time.Millisecond, doneChan, batchChan, circuitBreaker, m)

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
// manager, commits them to Kafka, and prunes the committed entries. When a
// doneChan message arrives, it marks the corresponding batch's offsets as
// committable. It also sets gauges for worker-pool queue depth and circuit
// breaker state on each tick.
func commitLoop(ctx context.Context, offsetMgr *offsetmanager.OffsetManager, reader *kafka.Reader, interval time.Duration, doneChan <-chan string, batchChan <-chan *worker.Batch, cb *errorpkg.CircuitBreaker, m *metrics.Metrics) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case batchID := <-doneChan:
			offsetMgr.SetCommittable(batchID)
		case <-ticker.C:
			// Update gauges on each tick.
			if m != nil {
				m.WorkerPoolQueueDepth.Set(float64(len(batchChan)))
				if cb != nil {
					if cb.IsOpen() {
						m.CircuitBreakerOpen.WithLabelValues("http").Set(1)
					} else {
						m.CircuitBreakerOpen.WithLabelValues("http").Set(0)
					}
				}
			}

			committable := offsetMgr.GetCommittable()
			if len(committable) == 0 {
				continue
			}

			// Build kafka messages for CommitMessages — one per partition,
			// carrying the next-offset (highest committable + 1).
			msgs := make([]kafka.Message, 0, len(committable))
			for tp, offset := range committable {
				msgs = append(msgs, kafka.Message{
					Topic:     tp.Topic,
					Partition: tp.Partition,
					Offset:    offset,
				})
			}

			if err := reader.CommitMessages(ctx, msgs...); err != nil {
				log.Printf("offset commit error: %v", err)
				continue
			}

			for tp, offset := range committable {
				log.Printf("offset commit: topic=%s partition=%d offset=%d", tp.Topic, tp.Partition, offset)
				if m != nil {
					m.OffsetCommits.WithLabelValues(tp.Topic, strconv.Itoa(tp.Partition)).Inc()
				}
			}

			offsetMgr.PruneCommitted(committable)
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
