package worker

import (
	"context"
	"log"
	"sync"
	"time"

	errorpkg "github.com/arelligoutham/goose/internal/error"
	"github.com/arelligoutham/goose/internal/sink"
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
	id            int
	sink          sink.Sink
	errorHandler  *errorpkg.ErrorHandler
	backoff       *errorpkg.ExponentialBackoff
	circuitBreaker *errorpkg.CircuitBreaker
	doneChan      chan string
	maxRetries    int
	dlqWriter     DLQWriter
}

// NewWorker creates a new worker with the given dependencies.
func NewWorker(
	id int,
	s sink.Sink,
	eh *errorpkg.ErrorHandler,
	bo *errorpkg.ExponentialBackoff,
	cb *errorpkg.CircuitBreaker,
	doneChan chan string,
	maxRetries int,
	dlqWriter DLQWriter,
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

	failed, err := w.sink.Push(batch.Messages)
	if err != nil {
		log.Printf("[worker %d] sink push error: %v", w.id, err)
		failed = allFailed(batch.Messages, err.Error())
	}

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
			w.retryMessage(ctx, msg)
		case errorpkg.ActionDLQ:
			w.sendToDLQ(ctx, msg)
		case errorpkg.ActionIgnore:
			log.Printf("[worker %d] ignoring message offset=%d: %s", w.id, msg.Offset, msg.ErrorInfo)
		case errorpkg.ActionFail:
			log.Printf("[worker %d] FAIL action for message offset=%d: %s", w.id, msg.Offset, msg.ErrorInfo)
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
				retry := w.errorHandler.Route(msg)
				switch retry {
				case errorpkg.ActionDLQ:
					w.sendToDLQ(ctx, msg)
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
