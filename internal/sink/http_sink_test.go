package sink

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
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
		ServiceURL:     server.URL,
		RequestMethod:  http.MethodPost,
		Timeout:        5000,
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
		ServiceURL:     server.URL,
		RequestMethod:  http.MethodPost,
		Timeout:        5000,
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
		ServiceURL:     server.URL,
		RequestMethod:  http.MethodPost,
		Timeout:        5000,
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
		ServiceURL:     server.URL,
		RequestMethod:  http.MethodPost,
		Timeout:        5000,
		MaxConnections: 10,
		ConnectionTTL:  100,
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
		ServiceURL:     server.URL,
		RequestMethod:  http.MethodPost,
		Timeout:        5000,
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

// --- batch-with-response mode tests ---

func batchWithResponseConfig(url string) HTTPSinkConfig {
	return HTTPSinkConfig{
		ServiceURL:           url,
		RequestMethod:        http.MethodPost,
		Timeout:              5000,
		MaxConnections:       10,
		BatchMode:            "with_response",
		BatchMaxSize:         500,
		BatchSeqField:        "_goose_seq",
		BatchRespPath:        "results",
		BatchRespSeqField:    "seq",
		BatchRespStatusField: "status",
		BatchRespErrorField:  "error",
		BatchRespRetryFlag:   "is_retryable",
		BatchRespSuccessVal:  "success",
	}
}

func TestHttpSinkBatchWithResponse(t *testing.T) {
	// Endpoint echoes back per-message results: seq 0,1 succeed; seq 2 fails
	// with a non-retryable error.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"results":[
			{"seq":0,"status":"success"},
			{"seq":1,"status":"success"},
			{"seq":2,"status":"error","error":"bad payload","is_retryable":false}
		]}`)
	}))
	defer server.Close()

	sink := NewHTTPSink(batchWithResponseConfig(server.URL))
	defer sink.Close()

	msgs := []Message{
		{Topic: "t1", Partition: 0, Offset: 1, Key: []byte("k1"), Value: []byte(`{"id":1}`)},
		{Topic: "t1", Partition: 0, Offset: 2, Key: []byte("k2"), Value: []byte(`{"id":2}`)},
		{Topic: "t1", Partition: 0, Offset: 3, Key: []byte("k3"), Value: []byte(`{"id":3}`)},
	}

	failed, err := sink.Push(msgs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(failed) != 1 {
		t.Fatalf("expected 1 failed message, got %d", len(failed))
	}
	f := failed[0]
	if f.Offset != 3 {
		t.Errorf("expected failed offset 3, got %d", f.Offset)
	}
	if f.ErrorInfo.StatusCode != 422 {
		t.Errorf("expected non-retryable status code 422, got %d", f.ErrorInfo.StatusCode)
	}
	if !strings.Contains(f.ErrorInfo.Message, "bad payload") {
		t.Errorf("expected error message to contain 'bad payload', got %q", f.ErrorInfo.Message)
	}
}

func TestHttpSinkBatchWithResponsePartialRetry(t *testing.T) {
	// seq 0 succeeds; seq 1 fails non-retryable; seq 2 fails retryable.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"results":[
			{"seq":0,"status":"success"},
			{"seq":1,"status":"error","error":"invalid","is_retryable":false},
			{"seq":2,"status":"error","error":"timeout","is_retryable":true}
		]}`)
	}))
	defer server.Close()

	sink := NewHTTPSink(batchWithResponseConfig(server.URL))
	defer sink.Close()

	msgs := []Message{
		{Topic: "t1", Partition: 0, Offset: 1, Key: []byte("k1"), Value: []byte(`{"id":1}`)},
		{Topic: "t1", Partition: 0, Offset: 2, Key: []byte("k2"), Value: []byte(`{"id":2}`)},
		{Topic: "t1", Partition: 0, Offset: 3, Key: []byte("k3"), Value: []byte(`{"id":3}`)},
	}

	failed, err := sink.Push(msgs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(failed) != 2 {
		t.Fatalf("expected 2 failed messages, got %d", len(failed))
	}

	// Both failures present; ensure one retryable (500) and one not (422).
	codes := map[int]int{}
	for _, f := range failed {
		codes[f.ErrorInfo.StatusCode]++
	}
	if codes[422] != 1 {
		t.Errorf("expected one 422 (non-retryable), got %v", codes)
	}
	if codes[500] != 1 {
		t.Errorf("expected one 500 (retryable), got %v", codes)
	}
}

func TestHttpSinkBatchWithResponseAllSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"results":[
			{"seq":0,"status":"success"},
			{"seq":1,"status":"success"},
			{"seq":2,"status":"success"},
			{"seq":3,"status":"success"},
			{"seq":4,"status":"success"}
		]}`)
	}))
	defer server.Close()

	sink := NewHTTPSink(batchWithResponseConfig(server.URL))
	defer sink.Close()

	msgs := make([]Message, 5)
	for i := range msgs {
		msgs[i] = Message{Topic: "t1", Partition: 0, Offset: int64(i + 1), Value: []byte(`{"id":` + strconv.Itoa(i) + `}`)}
	}

	failed, err := sink.Push(msgs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(failed) != 0 {
		t.Fatalf("expected 0 failed messages, got %d: %+v", len(failed), failed)
	}
}

func TestHttpSinkBatchWithResponseSeqInjection(t *testing.T) {
	// Verify goose injects the _goose_seq field into each outgoing message
	// and the endpoint can echo it back.
	var receivedBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		receivedBody = string(b)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"results":[
			{"seq":0,"status":"success"},
			{"seq":1,"status":"success"}
		]}`)
	}))
	defer server.Close()

	sink := NewHTTPSink(batchWithResponseConfig(server.URL))
	defer sink.Close()

	msgs := []Message{
		{Topic: "t1", Partition: 0, Offset: 1, Value: []byte(`{"id":1}`)},
		{Topic: "t1", Partition: 0, Offset: 2, Value: []byte(`{"id":2}`)},
	}

	if _, err := sink.Push(msgs); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(receivedBody, `"_goose_seq":0`) {
		t.Errorf("expected body to contain _goose_seq:0, got %s", receivedBody)
	}
	if !strings.Contains(receivedBody, `"_goose_seq":1`) {
		t.Errorf("expected body to contain _goose_seq:1, got %s", receivedBody)
	}
}

func TestHttpSinkBatchWithResponse5xxRetriesAll(t *testing.T) {
	// Non-2xx response → the whole batch is reported as retryable failures.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	sink := NewHTTPSink(batchWithResponseConfig(server.URL))
	defer sink.Close()

	msgs := []Message{
		{Topic: "t1", Partition: 0, Offset: 1, Value: []byte(`{"id":1}`)},
		{Topic: "t1", Partition: 0, Offset: 2, Value: []byte(`{"id":2}`)},
	}

	failed, err := sink.Push(msgs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(failed) != 2 {
		t.Fatalf("expected 2 failed messages on 5xx, got %d", len(failed))
	}
	for _, f := range failed {
		if f.ErrorInfo.StatusCode != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", f.ErrorInfo.StatusCode)
		}
	}
}

func TestHttpSinkBatchWithResponseMissingResultIsRetryable(t *testing.T) {
	// Endpoint omits seq 1 from the response → goose treats it as a
	// retryable failure.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"results":[
			{"seq":0,"status":"success"}
		]}`)
	}))
	defer server.Close()

	sink := NewHTTPSink(batchWithResponseConfig(server.URL))
	defer sink.Close()

	msgs := []Message{
		{Topic: "t1", Partition: 0, Offset: 1, Value: []byte(`{"id":1}`)},
		{Topic: "t1", Partition: 0, Offset: 2, Value: []byte(`{"id":2}`)},
	}

	failed, err := sink.Push(msgs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(failed) != 1 {
		t.Fatalf("expected 1 failed (missing result), got %d", len(failed))
	}
	if failed[0].ErrorInfo.StatusCode != 500 {
		t.Errorf("expected retryable status 500 for missing result, got %d", failed[0].ErrorInfo.StatusCode)
	}
}

func TestHttpSinkBatchWithResponseBatchMaxSize(t *testing.T) {
	// BatchMaxSize=2 with 4 messages → two POSTs; both succeed per-message.
	var postCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&postCount, 1)
		b, _ := io.ReadAll(r.Body)
		// Count messages in the incoming array to build matching results.
		var arr []map[string]interface{}
		_ = json.Unmarshal(b, &arr)
		results := "["
		for i := range arr {
			if i > 0 {
				results += ","
			}
			results += fmt.Sprintf(`{"seq":%d,"status":"success"}`, i)
		}
		results += "]"
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"results":%s}`, results)
	}))
	defer server.Close()

	cfg := batchWithResponseConfig(server.URL)
	cfg.BatchMaxSize = 2
	sink := NewHTTPSink(cfg)
	defer sink.Close()

	msgs := make([]Message, 4)
	for i := range msgs {
		msgs[i] = Message{Topic: "t1", Partition: 0, Offset: int64(i + 1), Value: []byte(fmt.Sprintf(`{"id":%d}`, i))}
	}

	failed, err := sink.Push(msgs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(failed) != 0 {
		t.Fatalf("expected 0 failed, got %d", len(failed))
	}
	if got := atomic.LoadInt32(&postCount); got != 2 {
		t.Fatalf("expected 2 POSTs (BatchMaxSize=2, 4 msgs), got %d", got)
	}
}

func TestBatchResponseParser(t *testing.T) {
	parser := NewBatchResponseParser(batchWithResponseConfig(""))

	body := []byte(`{"results":[
		{"seq":0,"status":"success"},
		{"seq":1,"status":"error","error":"bad","is_retryable":false},
		{"seq":2,"status":"error","error":"boom","is_retryable":true}
	]}`)

	results, err := parser.Parse(body, 3)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	if !results[0].Success {
		t.Errorf("seq 0 expected success, got failure: %+v", results[0])
	}
	if results[1].Success {
		t.Errorf("seq 1 expected failure, got success")
	}
	if results[1].IsRetryable {
		t.Errorf("seq 1 expected non-retryable")
	}
	if results[1].Error != "bad" {
		t.Errorf("seq 1 error = %q, want %q", results[1].Error, "bad")
	}
	if !results[2].IsRetryable {
		t.Errorf("seq 2 expected retryable")
	}
	if results[2].Error != "boom" {
		t.Errorf("seq 2 error = %q, want %q", results[2].Error, "boom")
	}
}

func TestBatchResponseParserMissingResultDefault(t *testing.T) {
	parser := NewBatchResponseParser(batchWithResponseConfig(""))

	// Response only covers seq 0 of a 2-message batch; seq 1 must default to
	// a retryable failure.
	body := []byte(`{"results":[{"seq":0,"status":"success"}]}`)
	results, err := parser.Parse(body, 2)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if !results[0].Success {
		t.Errorf("seq 0 expected success")
	}
	if results[1].Success {
		t.Errorf("seq 1 expected default failure")
	}
	if !results[1].IsRetryable {
		t.Errorf("seq 1 expected default retryable=true for missing result")
	}
}

func TestBatchResponseParserSeqAsString(t *testing.T) {
	// Some endpoints return seq as a JSON string rather than a number.
	parser := NewBatchResponseParser(batchWithResponseConfig(""))
	body := []byte(`{"results":[{"seq":"0","status":"success"}]}`)
	results, err := parser.Parse(body, 1)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if !results[0].Success {
		t.Errorf("seq as string: expected success")
	}
}

func TestBatchResponseParserMissingResultsField(t *testing.T) {
	parser := NewBatchResponseParser(batchWithResponseConfig(""))
	body := []byte(`{"something_else":[]}`)
	if _, err := parser.Parse(body, 1); err == nil {
		t.Fatalf("expected error for missing %q field", "results")
	}
}

func TestBatchResponseParserInvalidJSON(t *testing.T) {
	parser := NewBatchResponseParser(batchWithResponseConfig(""))
	if _, err := parser.Parse([]byte(`not json`), 1); err == nil {
		t.Fatalf("expected error for invalid JSON")
	}
}
