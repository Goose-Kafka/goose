package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	errorpkg "github.com/Goose-Kafka/goose/internal/error"
)

// HTTPSink delivers messages to an HTTP endpoint. It supports two modes:
//   - Batch mode (no JSONBodyTemplate): all messages are joined into a single
//     JSON array and sent in one POST request — highest throughput.
//   - Individual mode (JSONBodyTemplate set): one POST request per message.
//
// A connectionTracker enforces a TTL on underlying TCP connections so that
// stale connections are closed, forcing new TCP connections that re-resolve
// kube-proxy DNAT and redistribute traffic across backend pods.
type HTTPSink struct {
	client    *http.Client
	config    HTTPSinkConfig
	headers   map[string]string
	connTrack *connectionTracker
}

// NewHTTPSink creates an HTTPSink with a tuned http.Client transport.
// MaxConnections, ConnectionIdleEvict, ValidateInactivityMs, ConnectionTTL
// and Timeout are all applied to the underlying transport and dialer.
func NewHTTPSink(cfg HTTPSinkConfig) *HTTPSink {
	tracker := newConnectionTracker(time.Duration(cfg.ConnectionTTL) * time.Millisecond)

	dialer := &net.Dialer{
		Timeout:   time.Duration(cfg.Timeout) * time.Millisecond,
		KeepAlive: time.Duration(cfg.ConnectionTTL) * time.Millisecond,
	}

	transport := &http.Transport{
		MaxIdleConns:          cfg.MaxConnections,
		MaxIdleConnsPerHost:   cfg.MaxConnections,
		IdleConnTimeout:       time.Duration(cfg.ConnectionIdleEvict) * time.Millisecond,
		ResponseHeaderTimeout: time.Duration(cfg.ValidateInactivityMs) * time.Millisecond,
		DialContext:           dialer.DialContext,
	}

	transport.DialContext = tracker.wrap(transport.DialContext)

	return &HTTPSink{
		client: &http.Client{
			Transport: transport,
			Timeout:   time.Duration(cfg.Timeout) * time.Millisecond,
		},
		config:    cfg,
		headers:   parseHeaders(cfg.Headers),
		connTrack: tracker,
	}
}

// Push delivers the given messages to the sink. Three batch modes are
// supported, selected by config.BatchMode:
//   - "with_response": messages are sent in JSON-array batches injecting a
//     _goose_seq index into each element; the endpoint returns per-message
//     success/failure results that goose parses and maps back to individual
//     messages.
//   - "all_or_nothing": all messages are sent in one JSON-array POST and
//     either all succeed or all are reported as failed.
//   - "none" (default): individual mode — one POST per message. Used when
//     JSONBodyTemplate is set.
func (s *HTTPSink) Push(msgs []Message) ([]errorpkg.FailedMessage, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	switch s.config.BatchMode {
	case "with_response":
		return s.pushBatchWithResponse(msgs)
	case "all_or_nothing":
		return s.pushBatch(msgs)
	default:
		if s.config.JSONBodyTemplate == "" {
			return s.pushBatch(msgs)
		}
		return s.pushIndividual(msgs)
	}
}

// pushBatch joins all message values into a JSON array and sends one POST.
// On 2xx → no failures. On error → all messages are reported as failed.
func (s *HTTPSink) pushBatch(msgs []Message) ([]errorpkg.FailedMessage, error) {
	s.connTrack.evictStale()
	body := buildBatchBody(msgs)
	req, err := http.NewRequest(s.config.RequestMethod, s.config.ServiceURL, bytes.NewReader(body))
	if err != nil {
		return s.toFailedMessages(msgs, 0, err.Error()), nil
	}
	s.setHeaders(req)

	resp, err := s.client.Do(req)
	if err != nil {
		return s.toFailedMessages(msgs, 0, err.Error()), nil
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return s.toFailedMessages(msgs, resp.StatusCode, "sink returned non-2xx status"), nil
	}
	return nil, nil
}

// pushBatchWithResponse sends messages in one or more JSON-array batches and
// parses per-message results from the response. Each outgoing message gets a
// _goose_seq integer index injected (so the caller's original bytes are not
// mutated). On a non-2xx HTTP response or transport error, the whole batch is
// treated as retryable failures. On 2xx, the response body is parsed with
// BatchResponseParser and individual messages are routed: success → omitted
// from failed; non-retryable failure → included with a 4xx status; retryable
// failure → included with a 5xx status. Messages missing from the response are
// treated as retryable failures.
func (s *HTTPSink) pushBatchWithResponse(msgs []Message) ([]errorpkg.FailedMessage, error) {
	s.connTrack.evictStale()

	maxSize := s.config.BatchMaxSize
	if maxSize <= 0 {
		maxSize = len(msgs)
	}

	parser := NewBatchResponseParser(s.config)
	var failed []errorpkg.FailedMessage

	for start := 0; start < len(msgs); start += maxSize {
		end := start + maxSize
		if end > len(msgs) {
			end = len(msgs)
		}
		chunk := msgs[start:end]

		body, err := s.buildBatchWithSeqBody(chunk)
		if err != nil {
			failed = append(failed, s.toFailedMessages(chunk, 0,
				fmt.Sprintf("build batch body: %v", err))...)
			continue
		}

		req, err := http.NewRequest(s.config.RequestMethod, s.config.ServiceURL, bytes.NewReader(body))
		if err != nil {
			failed = append(failed, s.toFailedMessages(chunk, 0, err.Error())...)
			continue
		}
		s.setHeaders(req)

		resp, err := s.client.Do(req)
		if err != nil {
			failed = append(failed, s.toFailedMessages(chunk, 0, err.Error())...)
			continue
		}

		respBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			// Non-2xx → whole batch retries (all-or-nothing fallback).
			failed = append(failed, s.toFailedMessages(chunk, resp.StatusCode,
				"batch endpoint returned non-2xx status")...)
			continue
		}

		if readErr != nil {
			failed = append(failed, s.toFailedMessages(chunk, 0,
				fmt.Sprintf("read batch response: %v", readErr))...)
			continue
		}

		results, parseErr := parser.Parse(respBody, len(chunk))
		if parseErr != nil {
			// Cannot parse per-message results → treat the whole batch as
			// retryable so nothing is silently dropped.
			failed = append(failed, s.toFailedMessages(chunk, resp.StatusCode,
				fmt.Sprintf("parse batch response: %v", parseErr))...)
			continue
		}

		for i, msg := range chunk {
			r := results[i]
			if r.Success {
				continue
			}
			statusCode := 500
			if !r.IsRetryable {
				statusCode = 422
			}
			fm := s.toFailedMessages([]Message{msg}, statusCode, r.Error)[0]
			failed = append(failed, fm)
		}
	}

	return failed, nil
}

// buildBatchWithSeqBody unmarshals each message value, injects a _goose_seq
// index, and marshals the augmented values as a JSON array. The original
// Message.Value bytes are never mutated.
func (s *HTTPSink) buildBatchWithSeqBody(msgs []Message) ([]byte, error) {
	seqField := s.config.BatchSeqField
	if seqField == "" {
		seqField = "_goose_seq"
	}
	items := make([]json.RawMessage, len(msgs))
	for i, msg := range msgs {
		var obj map[string]interface{}
		if err := json.Unmarshal(msg.Value, &obj); err != nil {
			return nil, fmt.Errorf("message %d is not valid JSON: %w", i, err)
		}
		obj[seqField] = i
		augmented, err := json.Marshal(obj)
		if err != nil {
			return nil, fmt.Errorf("marshal message %d: %w", i, err)
		}
		items[i] = augmented
	}
	return json.Marshal(items)
}

// pushIndividual sends one POST per message, using the message Value as body.
// Failures are tracked per message and returned together.
func (s *HTTPSink) pushIndividual(msgs []Message) ([]errorpkg.FailedMessage, error) {
	var failed []errorpkg.FailedMessage
	for _, msg := range msgs {
		req, err := http.NewRequest(s.config.RequestMethod, s.config.ServiceURL, bytes.NewReader(msg.Value))
		if err != nil {
			failed = append(failed, s.toFailedMessages([]Message{msg}, 0, err.Error())[0])
			continue
		}
		s.setHeaders(req)

		resp, err := s.client.Do(req)
		if err != nil {
			failed = append(failed, s.toFailedMessages([]Message{msg}, 0, err.Error())[0])
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			failed = append(failed, s.toFailedMessages([]Message{msg}, resp.StatusCode, "sink returned non-2xx status")[0])
		}
		s.connTrack.evictStale()
	}
	return failed, nil
}

// Close closes idle connections held by the transport.
func (s *HTTPSink) Close() error {
	if tr, ok := s.client.Transport.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
	return nil
}

// setHeaders applies configured headers and a default Content-Type to the request.
func (s *HTTPSink) setHeaders(req *http.Request) {
	for k, v := range s.headers {
		req.Header.Set(k, v)
	}
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
}

// parseHeaders parses a "Key:Value,Key:Value" string into a map.
func parseHeaders(raw string) map[string]string {
	headers := make(map[string]string)
	if raw == "" {
		return headers
	}
	for _, pair := range strings.Split(raw, ",") {
		idx := strings.Index(pair, ":")
		if idx < 0 {
			continue
		}
		key := strings.TrimSpace(pair[:idx])
		val := strings.TrimSpace(pair[idx+1:])
		if key != "" {
			headers[key] = val
		}
	}
	return headers
}

// buildBatchBody joins message values into a JSON array: [msg1,msg2,...].
func buildBatchBody(msgs []Message) []byte {
	var b strings.Builder
	b.WriteByte('[')
	for i, msg := range msgs {
		if i > 0 {
			b.WriteByte(',')
		}
		b.Write(msg.Value)
	}
	b.WriteByte(']')
	return []byte(b.String())
}

// toFailedMessages converts a slice of messages into FailedMessages with the
// given status code and error message, classifying the error type by status.
func (s *HTTPSink) toFailedMessages(msgs []Message, statusCode int, errMsg string) []errorpkg.FailedMessage {
	failed := make([]errorpkg.FailedMessage, 0, len(msgs))
	for _, msg := range msgs {
		failed = append(failed, errorpkg.FailedMessage{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Key:       msg.Key,
			Value:     msg.Value,
			ErrorInfo: errorpkg.ErrorInfo{
				ErrorType:  errorpkg.ErrorTypeFromStatusCode(statusCode),
				StatusCode: statusCode,
				Message:    errMsg,
			},
		})
	}
	return failed
}

// connectionTracker tracks the creation time of each TCP connection so that
// connections older than a TTL can be evicted, forcing new connections to be
// established. This redistributes traffic across backend pods behind a
// kube-proxy DNAT, avoiding the connection-pinning bug.
type connectionTracker struct {
	ttl      time.Duration
	mu       sync.Mutex
	connAges map[net.Conn]time.Time
}

func newConnectionTracker(ttl time.Duration) *connectionTracker {
	return &connectionTracker{
		ttl:      ttl,
		connAges: make(map[net.Conn]time.Time),
	}
}

// wrap wraps a DialContext function so that each newly created connection is
// recorded with its creation time for later TTL checks.
func (c *connectionTracker) wrap(next func(ctx context.Context, network, addr string) (net.Conn, error)) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := next(ctx, network, addr)
		if err != nil || conn == nil {
			return conn, err
		}
		c.mu.Lock()
		c.connAges[conn] = time.Now()
		c.mu.Unlock()
		return conn, nil
	}
}

// isStale returns true if the connection age exceeds the TTL.
func (c *connectionTracker) isStale(conn net.Conn) bool {
	c.mu.Lock()
	createdAt, ok := c.connAges[conn]
	delete(c.connAges, conn)
	c.mu.Unlock()
	if !ok {
		return false
	}
	return time.Since(createdAt) > c.ttl
}

// evictStale closes any tracked connections that have exceeded their TTL.
// This is called between requests in individual mode to proactively refresh
// connections.
func (c *connectionTracker) evictStale() {
	if c.ttl <= 0 {
		return
	}
	c.mu.Lock()
	now := time.Now()
	for conn, createdAt := range c.connAges {
		if now.Sub(createdAt) > c.ttl {
			conn.Close()
			delete(c.connAges, conn)
		}
	}
	c.mu.Unlock()
}
