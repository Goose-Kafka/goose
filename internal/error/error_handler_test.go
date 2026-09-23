package errorpkg

import (
	"testing"

	"github.com/arelligoutham/goose/internal/config"
)

func TestErrorHandlerRoute5xxToRetry(t *testing.T) {
	retryRanges := config.StatusRangeList{config.ParseStatusRange("500-599")}
	dlqRanges := config.StatusRangeList{config.ParseStatusRange("400-499"), config.ParseStatusRange("500-599")}
	failRanges := config.StatusRangeList{}

	eh := NewErrorHandler(retryRanges, dlqRanges, failRanges)

	msg := FailedMessage{
		ErrorInfo: ErrorInfo{StatusCode: 503, ErrorType: ErrorTypeSink5xx},
		Retried:   false,
	}

	action := eh.Route(msg)
	if action != ActionRetry {
		t.Fatalf("expected ActionRetry, got %s", action)
	}
}

func TestErrorHandlerRoute4xxToDLQ(t *testing.T) {
	retryRanges := config.StatusRangeList{config.ParseStatusRange("500-599")}
	dlqRanges := config.StatusRangeList{config.ParseStatusRange("400-499")}
	failRanges := config.StatusRangeList{}

	eh := NewErrorHandler(retryRanges, dlqRanges, failRanges)

	msg := FailedMessage{
		ErrorInfo: ErrorInfo{StatusCode: 404, ErrorType: ErrorTypeSink4xx},
		Retried:   false,
	}

	action := eh.Route(msg)
	if action != ActionDLQ {
		t.Fatalf("expected ActionDLQ, got %s", action)
	}
}

func TestErrorHandlerRouteRetryExhaustedToDLQ(t *testing.T) {
	retryRanges := config.StatusRangeList{config.ParseStatusRange("500-599")}
	dlqRanges := config.StatusRangeList{config.ParseStatusRange("400-499"), config.ParseStatusRange("500-599")}
	failRanges := config.StatusRangeList{}

	eh := NewErrorHandler(retryRanges, dlqRanges, failRanges)

	msg := FailedMessage{
		ErrorInfo: ErrorInfo{StatusCode: 503, ErrorType: ErrorTypeSink5xx},
		Retried:   true,
	}

	action := eh.Route(msg)
	if action != ActionDLQ {
		t.Fatalf("expected ActionDLQ after retry exhausted, got %s", action)
	}
}

func TestErrorHandlerRouteUnconfiguredToIgnore(t *testing.T) {
	retryRanges := config.StatusRangeList{config.ParseStatusRange("500-599")}
	dlqRanges := config.StatusRangeList{config.ParseStatusRange("400-499")}
	failRanges := config.StatusRangeList{}

	eh := NewErrorHandler(retryRanges, dlqRanges, failRanges)

	msg := FailedMessage{
		ErrorInfo: ErrorInfo{StatusCode: 302, ErrorType: ErrorTypeSinkUnknown},
		Retried:   false,
	}

	action := eh.Route(msg)
	if action != ActionIgnore {
		t.Fatalf("expected ActionIgnore for unconfigured status, got %s", action)
	}
}

func TestErrorHandlerRouteFailCrashes(t *testing.T) {
	retryRanges := config.StatusRangeList{}
	dlqRanges := config.StatusRangeList{}
	failRanges := config.StatusRangeList{config.ParseStatusRange("503")}

	eh := NewErrorHandler(retryRanges, dlqRanges, failRanges)

	msg := FailedMessage{
		ErrorInfo: ErrorInfo{StatusCode: 503, ErrorType: ErrorTypeSink5xx},
		Retried:   false,
	}

	action := eh.Route(msg)
	if action != ActionFail {
		t.Fatalf("expected ActionFail, got %s", action)
	}
}
