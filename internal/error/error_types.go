package errorpkg

import "fmt"

// ErrorType classifies the kind of error that occurred while processing or
// delivering a message.
type ErrorType string

const (
	// ErrorTypeNone indicates no error — the message was processed successfully.
	ErrorTypeNone ErrorType = "NONE"

	// ErrorTypeSink4xx indicates a 4xx HTTP response from the sink (client error,
	// e.g. 400 Bad Request, 404 Not Found, 429 Too Many Requests).
	ErrorTypeSink4xx ErrorType = "SINK_4XX_ERROR"

	// ErrorTypeSink5xx indicates a 5xx HTTP response from the sink (server error,
	// e.g. 500 Internal Server Error, 503 Service Unavailable).
	ErrorTypeSink5xx ErrorType = "SINK_5XX_ERROR"

	// ErrorTypeDeserialization indicates a failure to deserialize a message.
	ErrorTypeDeserialization ErrorType = "DESERIALIZATION_ERROR"

	// ErrorTypeInvalidMessage indicates a message that failed validation.
	ErrorTypeInvalidMessage ErrorType = "INVALID_MESSAGE_ERROR"

	// ErrorTypeUnknownFields indicates a message containing unknown fields.
	ErrorTypeUnknownFields ErrorType = "UNKNOWN_FIELDS_ERROR"

	// ErrorTypeSinkUnknown indicates a sink error that doesn't fall into a known
	// status code range.
	ErrorTypeSinkUnknown ErrorType = "SINK_UNKNOWN_ERROR"

	// ErrorTypeDefault is the fallback error type when no specific type applies.
	ErrorTypeDefault ErrorType = "DEFAULT_ERROR"
)

// ErrorInfo captures diagnostic information about a failed message.
type ErrorInfo struct {
	ErrorType  ErrorType
	StatusCode int
	Message    string
}

// String returns a human-readable representation of the ErrorInfo.
func (e ErrorInfo) String() string {
	return fmt.Sprintf("type=%s status_code=%d message=%q", e.ErrorType, e.StatusCode, e.Message)
}

// FailedMessage represents a Kafka message that could not be successfully
// delivered to the sink.
type FailedMessage struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	ErrorInfo ErrorInfo
	// Retried is true if this message has already been retried at least once.
	Retried bool
}

// ErrorTypeFromStatusCode maps an HTTP status code to an ErrorType.
//   - 200–299 → ErrorTypeNone (success)
//   - 400–499 → ErrorTypeSink4xx (client error; note 429 is still 4xx)
//   - 500–599 → ErrorTypeSink5xx (server error)
//   - else    → ErrorTypeSinkUnknown
func ErrorTypeFromStatusCode(statusCode int) ErrorType {
	switch {
	case statusCode >= 200 && statusCode <= 299:
		return ErrorTypeNone
	case statusCode >= 400 && statusCode <= 499:
		return ErrorTypeSink4xx
	case statusCode >= 500 && statusCode <= 599:
		return ErrorTypeSink5xx
	default:
		return ErrorTypeSinkUnknown
	}
}
