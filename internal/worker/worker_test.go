package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/arelligoutham/goose/internal/config"
	errorpkg "github.com/arelligoutham/goose/internal/error"
	"github.com/arelligoutham/goose/internal/sink"
)

func TestWorkerProcessesBatchSuccessfully(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	s := sink.NewHTTPSink(sink.HTTPSinkConfig{
		ServiceURL:     server.URL,
		RequestMethod:  http.MethodPost,
		Timeout:        5000,
		MaxConnections: 10,
	})
	defer s.Close()

	retryRanges := config.StatusRangeList{config.ParseStatusRange("500-599")}
	dlqRanges := config.StatusRangeList{config.ParseStatusRange("400-499"), config.ParseStatusRange("500-599")}
	failRanges := config.StatusRangeList{}
	errorHandler := errorpkg.NewErrorHandler(retryRanges, dlqRanges, failRanges)

	backoff := errorpkg.NewExponentialBackoff(100, 10000, 2.0)
	circuitBreaker := errorpkg.NewCircuitBreaker(80, 100, 10*time.Second)
	doneChan := make(chan string)

	w := NewWorker(0, s, errorHandler, backoff, circuitBreaker, doneChan, 3, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batchChan := make(chan *Batch, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go w.Run(ctx, batchChan, &wg)

	batch := &Batch{
		ID: "test-batch-1",
		Messages: []sink.Message{
			{Topic: "test", Partition: 0, Offset: 1, Key: []byte("1"), Value: []byte(`{"id":"1"}`)},
			{Topic: "test", Partition: 0, Offset: 2, Key: []byte("2"), Value: []byte(`{"id":"2"}`)},
		},
	}
	batchChan <- batch

	time.Sleep(100 * time.Millisecond)
	cancel()
	wg.Wait()
}

func TestWorkerHandles5xxWithRetry(t *testing.T) {
	var callCount int
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		callCount++
		current := callCount
		mu.Unlock()

		if current <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	s := sink.NewHTTPSink(sink.HTTPSinkConfig{
		ServiceURL:     server.URL,
		RequestMethod:  http.MethodPost,
		Timeout:        5000,
		MaxConnections: 10,
	})
	defer s.Close()

	retryRanges := config.StatusRangeList{config.ParseStatusRange("500-599")}
	dlqRanges := config.StatusRangeList{config.ParseStatusRange("400-499"), config.ParseStatusRange("500-599")}
	failRanges := config.StatusRangeList{}
	errorHandler := errorpkg.NewErrorHandler(retryRanges, dlqRanges, failRanges)

	backoff := errorpkg.NewExponentialBackoff(10, 1000, 2.0)
	circuitBreaker := errorpkg.NewCircuitBreaker(80, 100, 10*time.Second)
	doneChan := make(chan string)

	w := NewWorker(0, s, errorHandler, backoff, circuitBreaker, doneChan, 3, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batchChan := make(chan *Batch, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go w.Run(ctx, batchChan, &wg)

	batchID := "test-batch-retry"
	batch := &Batch{
		ID: batchID,
		Messages: []sink.Message{
			{Topic: "test", Partition: 0, Offset: 1, Key: []byte("1"), Value: []byte(`{"id":"1"}`)},
		},
	}
	batchChan <- batch

	select {
	case id := <-doneChan:
		if id != batchID {
			t.Fatalf("expected batch ID %s, got %s", batchID, id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for doneChan signal")
	}

	cancel()
	wg.Wait()

	mu.Lock()
	cc := callCount
	mu.Unlock()
	if cc < 3 {
		t.Fatalf("expected callCount >= 3 (1 initial + 2 retries), got %d", cc)
	}
}
