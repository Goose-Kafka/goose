package worker

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"

	errorpkg "github.com/arelligoutham/goose/internal/error"
	"github.com/arelligoutham/goose/internal/metrics"
	"github.com/arelligoutham/goose/internal/sink"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// DLQWriter writes failed messages to a Dead Letter Queue. The implementation
// is provided in a later task.
type DLQWriter interface {
	Write(msgs []errorpkg.FailedMessage) error
}

// Worker is a single goroutine that reads batches from a channel, pushes them
// to a sink, routes failures through the error handler, records circuit
// breaker results, and signals batch completion for offset commit.
type Worker struct {
	id             int
	sink           sink.Sink
	errorHandler   *errorpkg.ErrorHandler
	backoff        *errorpkg.ExponentialBackoff
	circuitBreaker *errorpkg.CircuitBreaker
	doneChan       chan string
	maxRetries     int
	dlqWriter      DLQWriter
	metrics        *metrics.Metrics
}

func NewWorker(
	id int,
	s sink.Sink,
	eh *errorpkg.ErrorHandler,
	bo *errorpkg.ExponentialBackoff,
	cb *errorpkg.CircuitBreaker,
	doneChan chan string,
	maxRetries int,
	dlqWriter DLQWriter,
	m *metrics.Metrics,
) *Worker {
	return &Worker{
		id:             id,
		sink:           s,
		errorHandler:   eh,
		backoff:        bo,
		circuitBreaker: cb,
		doneChan:       doneChan,
		maxRetries:     maxRetries,
		dlqWriter:      dlqWriter,
		metrics:        m,
	}
}

// Run is the worker's main loop. It reads batches from batchChan and processes
// them until the context is cancelled. The WaitGroup is decremented when Run
// returns.
func (w *Worker) Run(ctx context.Context, batchChan <-chan *Batch, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case batch, ok := <-batchChan:
			if !ok {
				return
			}
			w.processBatch(ctx, batch)
		}
	}
}

// processBatch pushes a batch to the sink and handles any failures.
func (w *Worker) processBatch(ctx context.Context, batch *Batch) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[worker %d] recovered from panic processing batch %s: %v", w.id, batch.ID, r)
		}
	}()

	// OTel span: firehose.process_batch
	tracer := otel.Tracer("goose")
	ctx, span := tracer.Start(ctx, "firehose.process_batch",
		trace.WithAttributes(
			attribute.String("batch.id", batch.ID),
			attribute.Int("batch.size", len(batch.Messages)),
			attribute.Int("worker.id", w.id),
		),
	)
	defer span.End()

	// Check circuit breaker before making outbound calls.
	if w.circuitBreaker != nil {
		for !w.circuitBreaker.Allow() {
			log.Printf("[worker %d] circuit breaker open, waiting before retry", w.id)
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
			}
		}
	}

	start := time.Now()

	failed, err := w.sink.Push(batch.Messages)
	sinkDuration := time.Since(start).Seconds()

	if err != nil {
		log.Printf("[worker %d] sink push error: %v", w.id, err)
		failed = allFailed(batch.Messages, err.Error())
	}

	// Record metrics
	if w.metrics != nil {
		w.metrics.SinkLatency.WithLabelValues("http").Observe(sinkDuration)
		delivered := len(batch.Messages) - len(failed)
		if delivered > 0 {
			w.metrics.MessagesDelivered.WithLabelValues("http").Add(float64(delivered))
		}
		// Record HTTP response codes from failed messages
		for _, f := range failed {
			if f.ErrorInfo.StatusCode > 0 {
				w.metrics.HTTPResponseCodes.WithLabelValues(strconv.Itoa(f.ErrorInfo.StatusCode)).Inc()
			}
		}
		// Count successful HTTP responses (2xx) for the batch
		if err == nil && len(failed) == 0 {
			w.metrics.HTTPResponseCodes.WithLabelValues("200").Add(float64(len(batch.Messages)))
		}
	}

	span.SetAttributes(
		attribute.Int("http.delivered", len(batch.Messages)-len(failed)),
		attribute.Int("http.failed", len(failed)),
		attribute.Float64("http.duration_ms", sinkDuration*1000),
	)

	if len(failed) == 0 {
		if w.circuitBreaker != nil {
			w.circuitBreaker.RecordSuccess()
		}
	} else {
		if w.circuitBreaker != nil {
			w.circuitBreaker.RecordFailure()
		}
		w.handleFailures(ctx, failed)
	}

	select {
	case w.doneChan <- batch.ID:
	case <-ctx.Done():
	}
}

// handleFailures routes each failed message through the error handler.
func (w *Worker) handleFailures(ctx context.Context, failed []errorpkg.FailedMessage) {
	for _, msg := range failed {
		action := w.errorHandler.Route(msg)
		switch action {
		case errorpkg.ActionRetry:
			if w.metrics != nil {
				w.metrics.MessagesRetried.WithLabelValues(string(msg.ErrorInfo.ErrorType)).Inc()
			}
			w.retryMessage(ctx, msg)
		case errorpkg.ActionDLQ:
			w.sendToDLQ(ctx, msg)
		case errorpkg.ActionIgnore:
			if w.metrics != nil {
				w.metrics.MessagesIgnored.WithLabelValues(string(msg.ErrorInfo.ErrorType)).Inc()
			}
			log.Printf("[worker %d] ignoring message offset=%d: %s", w.id, msg.Offset, msg.ErrorInfo)
		case errorpkg.ActionFail:
			log.Panicf("[worker %d] FAIL action for message offset=%d: %s — crashing consumer", w.id, msg.Offset, msg.ErrorInfo)
		}
	}
}

// retryMessage retries a single message with exponential backoff up to
// maxRetries times. If all retries are exhausted, the message is routed again
// (typically to the DLQ).
func (w *Worker) retryMessage(ctx context.Context, msg errorpkg.FailedMessage) {
	msg.Retried = true

	for attempt := 1; attempt <= w.maxRetries; attempt++ {
		delay := w.backoff.Backoff(attempt)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}

		failed, err := w.sink.Push([]sink.Message{
			{Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset, Key: msg.Key, Value: msg.Value},
		})
		if err != nil || len(failed) > 0 {
			if w.circuitBreaker != nil {
				w.circuitBreaker.RecordFailure()
			}
			if attempt == w.maxRetries {
				log.Printf("[worker %d] retry exhausted for offset=%d, re-routing", w.id, msg.Offset)
				action := w.errorHandler.Route(msg)
				switch action {
				case errorpkg.ActionDLQ:
					w.sendToDLQ(ctx, msg)
				case errorpkg.ActionIgnore:
					if w.metrics != nil {
						w.metrics.MessagesIgnored.WithLabelValues(string(msg.ErrorInfo.ErrorType)).Inc()
					}
					log.Printf("[worker %d] dropping message offset=%d after retry exhaustion (no DLQ)", w.id, msg.Offset)
				case errorpkg.ActionFail:
					log.Panicf("[worker %d] FAIL after retry exhaustion for offset=%d: %s", w.id, msg.Offset, msg.ErrorInfo)
				default:
					log.Printf("[worker %d] dropping message offset=%d after retry exhaustion", w.id, msg.Offset)
				}
			}
			continue
		}

		if w.circuitBreaker != nil {
			w.circuitBreaker.RecordSuccess()
		}
		return
	}
}

// sendToDLQ writes a failed message to the Dead Letter Queue if a DLQ writer is
// configured; otherwise the message is dropped.
func (w *Worker) sendToDLQ(ctx context.Context, msg errorpkg.FailedMessage) {
	if w.dlqWriter != nil {
		if err := w.dlqWriter.Write([]errorpkg.FailedMessage{msg}); err != nil {
			log.Printf("[worker %d] DLQ write error for offset=%d: %v", w.id, msg.Offset, err)
		} else if w.metrics != nil {
			w.metrics.MessagesDLQ.WithLabelValues(string(msg.ErrorInfo.ErrorType)).Inc()
		}
	} else {
		log.Printf("[worker %d] dropping message to DLQ (no writer configured) offset=%d: %s", w.id, msg.Offset, msg.ErrorInfo)
	}
}

// allFailed converts every message in a batch into a FailedMessage, used when
// the sink returns a transport-level error.
func allFailed(msgs []sink.Message, errMsg string) []errorpkg.FailedMessage {
	failed := make([]errorpkg.FailedMessage, 0, len(msgs))
	for _, msg := range msgs {
		failed = append(failed, errorpkg.FailedMessage{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Key:       msg.Key,
			Value:     msg.Value,
			ErrorInfo: errorpkg.ErrorInfo{
				ErrorType:  errorpkg.ErrorTypeDefault,
				StatusCode: 0,
				Message:    errMsg,
			},
		})
	}
	return failed
}
