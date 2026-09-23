package errorpkg

import "github.com/arelligoutham/goose/internal/config"

// Action represents the routing decision the error handler makes for a failed
// message.
type Action string

const (
	// ActionRetry means the message should be retried with exponential backoff.
	ActionRetry Action = "RETRY"

	// ActionDLQ means the message should be sent to the Dead Letter Queue.
	ActionDLQ Action = "DLQ"

	// ActionIgnore means the message should be dropped silently.
	ActionIgnore Action = "IGNORE"

	// ActionFail means the pipeline should crash (infrastructure-level failure).
	ActionFail Action = "FAIL"
)

// ErrorHandler routes failed messages to the correct action — retry, DLQ,
// ignore, or fail — based on configurable HTTP status-code ranges.
type ErrorHandler struct {
	retryRanges config.StatusRangeList
	dlqRanges   config.StatusRangeList
	failRanges  config.StatusRangeList
}

// NewErrorHandler constructs an error handler with the given retry, DLQ, and
// fail status-code ranges.
func NewErrorHandler(retry, dlq, fail config.StatusRangeList) *ErrorHandler {
	return &ErrorHandler{retryRanges: retry, dlqRanges: dlq, failRanges: fail}
}

// Route determines the action for a failed message.
//
// Priority order mirrors the raystack firehose decorator chain:
//  1. Fail  — highest priority; crashes the pipeline for infrastructure
//     failures so an operator can intervene.
//  2. Retry — only if the message has not already been retried (Retried=false)
//     and the status code falls in a retry range.
//  3. DLQ   — messages that were not retried or whose retry is exhausted.
//  4. Ignore — default for any status code not in a configured range.
func (eh *ErrorHandler) Route(msg FailedMessage) Action {
	code := msg.ErrorInfo.StatusCode

	// Check fail first (highest priority).
	if eh.failRanges.Matches(code) {
		return ActionFail
	}

	// Retry only if the message hasn't already been retried.
	if !msg.Retried && eh.retryRanges.Matches(code) {
		return ActionRetry
	}

	// Check DLQ.
	if eh.dlqRanges.Matches(code) {
		return ActionDLQ
	}

	// Default: ignore.
	return ActionIgnore
}
