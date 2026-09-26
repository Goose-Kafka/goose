package sink

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	errorpkg "github.com/Goose-Kafka/goose/internal/error"
)

// Message represents a single message to be delivered to a sink.
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte // JSON bytes for HTTP sink
}

// Sink is the pluggable interface for delivering messages to an external
// destination. Implementations include HTTP, Redis, Elasticsearch, etc.
type Sink interface {
	Push(msgs []Message) ([]errorpkg.FailedMessage, error)
	Close() error
}

// HTTPSinkConfig holds configuration for an HTTP sink.
type HTTPSinkConfig struct {
	ServiceURL           string
	RequestMethod        string
	Timeout              int // milliseconds
	MaxConnections       int
	ConnectionTTL        int // milliseconds
	ConnectionIdleEvict  int // milliseconds
	ValidateInactivityMs int
	Headers              string
	JSONBodyTemplate     string
	DataFormat           string
	BatchMode            string
	BatchMaxSize         int
	BatchSeqField        string
	BatchRespPath        string
	BatchRespSeqField    string
	BatchRespStatusField string
	BatchRespErrorField  string
	BatchRespRetryFlag   string
	BatchRespSuccessVal  string
}

// BatchResult represents the outcome of a single message in a batch response.
type BatchResult struct {
	SeqIndex    int    // index in the batch array (matches _goose_seq)
	Success     bool   // true if the message was processed successfully
	Error       string // error message if failed
	IsRetryable bool   // true if the error is transient and should be retried
}

// BatchResponseParser parses an HTTP response from a batch endpoint and
// extracts per-message results, keyed by the _goose_seq index that goose
// injected into each outgoing message.
type BatchResponseParser struct {
	respPath       string
	seqField       string
	statusField    string
	errorField     string
	retryFlagField string
	successVal     string
}

// NewBatchResponseParser creates a parser that extracts per-message results
// from a batch endpoint response using the provided field-name configuration.
func NewBatchResponseParser(cfg HTTPSinkConfig) *BatchResponseParser {
	return &BatchResponseParser{
		respPath:       cfg.BatchRespPath,
		seqField:       cfg.BatchRespSeqField,
		statusField:    cfg.BatchRespStatusField,
		errorField:     cfg.BatchRespErrorField,
		retryFlagField: cfg.BatchRespRetryFlag,
		successVal:     cfg.BatchRespSuccessVal,
	}
}

// Parse reads the HTTP response body and extracts per-message results indexed
// by the _goose_seq value. The response body must be a JSON object containing
// an array at the configured respPath (e.g. {"results":[...]}). Each array
// element must carry seq + status (+ optional error + is_retryable). Results
// are returned in the order they appear in the response body; the caller uses
// BatchResult.SeqIndex to map them back to the submitted messages.
//
// The returned slice has length batchSize. Elements for seq indices that are
// missing from the response, out of range, or cannot be parsed are filled with
// a default retryable-failure result so the caller treats them as needing
// retry.
func (p *BatchResponseParser) Parse(respBody []byte, batchSize int) ([]BatchResult, error) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(respBody, &root); err != nil {
		return nil, fmt.Errorf("parse batch response root: %w", err)
	}

	arrRaw, ok := root[p.respPath]
	if !ok {
		return nil, fmt.Errorf("batch response missing %q field", p.respPath)
	}

	var items []map[string]json.RawMessage
	if err := json.Unmarshal(arrRaw, &items); err != nil {
		return nil, fmt.Errorf("parse %q array: %w", p.respPath, err)
	}

	results := make([]BatchResult, batchSize)
	for i := range results {
		// Default: treat a missing result as a retryable failure so the
		// caller can re-send those messages instead of silently dropping them.
		results[i] = BatchResult{SeqIndex: i, Success: false, Error: "no result in response", IsRetryable: true}
	}

	for _, item := range items {
		seq, ok := extractInt(item, p.seqField)
		if !ok {
			continue
		}
		if seq < 0 || seq >= batchSize {
			continue
		}

		statusStr, statusOK := item[p.statusField]
		status := ""
		if statusOK {
			_ = json.Unmarshal(statusStr, &status)
		}

		errMsg := ""
		if errRaw, ok := item[p.errorField]; ok {
			_ = json.Unmarshal(errRaw, &errMsg)
		}

		isRetryable := false
		if retryRaw, ok := item[p.retryFlagField]; ok {
			_ = json.Unmarshal(retryRaw, &isRetryable)
		}

		results[seq] = BatchResult{
			SeqIndex:    seq,
			Success:     status == p.successVal,
			Error:       errMsg,
			IsRetryable: isRetryable,
		}
	}

	return results, nil
}

// extractInt reads an integer field (which may be a JSON number or a numeric
// string) from a raw-message map.
func extractInt(m map[string]json.RawMessage, key string) (int, bool) {
	raw, ok := m[key]
	if !ok {
		return 0, false
	}
	var n json.Number
	if err := json.Unmarshal(raw, &n); err == nil {
		if v, err := n.Int64(); err == nil {
			return int(v), true
		}
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if v, err := strconv.Atoi(strings.TrimSpace(s)); err == nil {
			return v, true
		}
	}
	return 0, false
}
