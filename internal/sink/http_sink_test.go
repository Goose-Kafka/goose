package sink

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHttpSinkBatchMode(t *testing.T) {
	var receivedBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		receivedBody = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sink := NewHTTPSink(HTTPSinkConfig{
		ServiceURL:    server.URL,
		RequestMethod: http.MethodPost,
		Timeout:       5000,
		MaxConnections: 10,
	})

	msgs := []Message{
		{Topic: "t1", Partition: 0, Offset: 1, Key: []byte("k1"), Value: []byte(`{"id":1}`)},
		{Topic: "t1", Partition: 0, Offset: 2, Key: []byte("k2"), Value: []byte(`{"id":2}`)},
	}

	failed, err := sink.Push(msgs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(failed) != 0 {
		t.Fatalf("expected 0 failed, got %d", len(failed))
	}
	if !strings.Contains(receivedBody, `{"id":1}`) {
		t.Errorf("expected body to contain first message, got %s", receivedBody)
	}
	if !strings.Contains(receivedBody, `{"id":2}`) {
		t.Errorf("expected body to contain second message, got %s", receivedBody)
	}
	sink.Close()
}

func TestHttpSinkIndividualMode(t *testing.T) {
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sink := NewHTTPSink(HTTPSinkConfig{
		ServiceURL:       server.URL,
		RequestMethod:    http.MethodPost,
		Timeout:          5000,
		MaxConnections:   10,
		JSONBodyTemplate: `{"data":{{.Value}}}`,
	})

	msgs := []Message{
		{Topic: "t1", Partition: 0, Offset: 1, Key: []byte("k1"), Value: []byte(`{"id":1}`)},
		{Topic: "t1", Partition: 0, Offset: 2, Key: []byte("k2"), Value: []byte(`{"id":2}`)},
		{Topic: "t1", Partition: 0, Offset: 3, Key: []byte("k3"), Value: []byte(`{"id":3}`)},
	}

	failed, err := sink.Push(msgs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(failed) != 0 {
		t.Fatalf("expected 0 failed, got %d", len(failed))
	}
	if got := atomic.LoadInt32(&requestCount); got != 3 {
		t.Fatalf("expected 3 requests, got %d", got)
	}
	sink.Close()
}

func TestHttpSink5xxReturnsFailed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	sink := NewHTTPSink(HTTPSinkConfig{
		ServiceURL:    server.URL,
		RequestMethod: http.MethodPost,
		Timeout:       5000,
		MaxConnections: 10,
	})

	msgs := []Message{
		{Topic: "t1", Partition: 0, Offset: 1, Key: []byte("k1"), Value: []byte(`{"id":1}`)},
	}

	failed, err := sink.Push(msgs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(failed) != 1 {
		t.Fatalf("expected 1 failed, got %d", len(failed))
	}
	if failed[0].ErrorInfo.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected status code 503, got %d", failed[0].ErrorInfo.StatusCode)
	}
	sink.Close()
}

func TestHttpSink4xxReturnsFailed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	sink := NewHTTPSink(HTTPSinkConfig{
		ServiceURL:    server.URL,
		RequestMethod: http.MethodPost,
		Timeout:       5000,
		MaxConnections: 10,
	})

	msgs := []Message{
		{Topic: "t1", Partition: 0, Offset: 1, Key: []byte("k1"), Value: []byte(`{"id":1}`)},
	}

	failed, err := sink.Push(msgs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(failed) != 1 {
		t.Fatalf("expected 1 failed, got %d", len(failed))
	}
	if failed[0].ErrorInfo.StatusCode != http.StatusNotFound {
		t.Errorf("expected status code 404, got %d", failed[0].ErrorInfo.StatusCode)
	}
	sink.Close()
}

func TestHttpSinkConnectionTTL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sink := NewHTTPSink(HTTPSinkConfig{
		ServiceURL:       server.URL,
		RequestMethod:    http.MethodPost,
		Timeout:          5000,
		MaxConnections:   10,
		ConnectionTTL:    100,
	})

	msgs1 := []Message{
		{Topic: "t1", Partition: 0, Offset: 1, Key: []byte("k1"), Value: []byte(`{"id":1}`)},
	}
	if failed, err := sink.Push(msgs1); err != nil || len(failed) != 0 {
		t.Fatalf("first push failed: err=%v failed=%d", err, len(failed))
	}

	time.Sleep(150 * time.Millisecond)

	msgs2 := []Message{
		{Topic: "t1", Partition: 0, Offset: 2, Key: []byte("k2"), Value: []byte(`{"id":2}`)},
	}
	if failed, err := sink.Push(msgs2); err != nil || len(failed) != 0 {
		t.Fatalf("second push after TTL failed: err=%v failed=%d", err, len(failed))
	}

	sink.Close()
}

func TestHttpSinkHeaders(t *testing.T) {
	var receivedAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sink := NewHTTPSink(HTTPSinkConfig{
		ServiceURL:    server.URL,
		RequestMethod: http.MethodPost,
		Timeout:       5000,
		MaxConnections: 10,
		Headers:        "Authorization:token123",
	})

	msgs := []Message{
		{Topic: "t1", Partition: 0, Offset: 1, Key: []byte("k1"), Value: []byte(`{"id":1}`)},
	}

	failed, err := sink.Push(msgs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(failed) != 0 {
		t.Fatalf("expected 0 failed, got %d", len(failed))
	}
	if receivedAuth != "token123" {
		t.Errorf("expected Authorization header 'token123', got %q", receivedAuth)
	}
	sink.Close()
}
