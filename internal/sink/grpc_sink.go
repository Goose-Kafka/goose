package sink

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	errorpkg "github.com/Goose-Kafka/goose/internal/error"
)

// GrpcSinkConfig holds configuration for the gRPC sink.
type GrpcSinkConfig struct {
	ServiceURL    string // e.g., "grpc-service:9090"
	Method        string // e.g., "events.OrderService/ProcessOrder"
	TimeoutMs     int    // gRPC call timeout
	TLSEnabled    bool
	TLSCACertPath string
	AuthType      string // "bearer" | "none"
	AuthToken     string
}

// GrpcSink sends messages to a gRPC endpoint using raw protobuf bytes.
// Since gRPC uses protobuf natively, no descriptor or JSON conversion is needed —
// the raw Kafka bytes ARE the protobuf message.
type GrpcSink struct {
	conn   *grpc.ClientConn
	config GrpcSinkConfig
}

// NewGrpcSink creates a gRPC sink and connects to the gRPC server.
func NewGrpcSink(cfg GrpcSinkConfig) (*GrpcSink, error) {
	if cfg.ServiceURL == "" {
		return nil, fmt.Errorf("grpc sink: service URL is required")
	}
	if cfg.Method == "" {
		return nil, fmt.Errorf("grpc sink: method is required")
	}

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	if cfg.AuthType == "bearer" && cfg.AuthToken != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(&bearerToken{token: cfg.AuthToken}))
	}

	conn, err := grpc.Dial(cfg.ServiceURL, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("grpc dial %s: %w", cfg.ServiceURL, err)
	}

	log.Printf("grpc sink connected: url=%s method=%s", cfg.ServiceURL, cfg.Method)

	return &GrpcSink{conn: conn, config: cfg}, nil
}

// bearerToken implements credentials.PerRPCCredentials for bearer token auth.
type bearerToken struct {
	token string
}

func (t *bearerToken) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + t.token}, nil
}

func (t *bearerToken) RequireTransportSecurity() bool {
	return false
}

// Push sends each message's raw bytes to the gRPC endpoint as a protobuf message.
// For gRPC, the raw Kafka bytes ARE the protobuf message — no conversion needed.
func (s *GrpcSink) Push(msgs []Message) ([]errorpkg.FailedMessage, error) {
	if len(msgs) == 0 {
		return nil, nil
	}

	var failed []errorpkg.FailedMessage

	for _, msg := range msgs {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(s.config.TimeoutMs)*time.Millisecond)

		// Create a raw protobuf message from the Kafka bytes.
		// gRPC's Invoke method accepts raw bytes as the request.
		var reply []byte
		err := s.conn.Invoke(ctx, s.config.Method, msg.Value, &reply)
		cancel()

		if err != nil {
			st, _ := status.FromError(err)

			// Classify gRPC error codes
			var errorType errorpkg.ErrorType
			var statusCode int

			switch st.Code() {
			case codes.OK:
				continue // success
			case codes.InvalidArgument, codes.NotFound, codes.AlreadyExists, codes.PermissionDenied:
				// Permanent errors → DLQ
				errorType = errorpkg.ErrorTypeSink4xx
				statusCode = 400
			case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
				// Transient errors → retry
				errorType = errorpkg.ErrorTypeSink5xx
				statusCode = 503
			default:
				// Unknown → treat as 5xx (retryable)
				errorType = errorpkg.ErrorTypeSink5xx
				statusCode = 500
			}

			failed = append(failed, errorpkg.FailedMessage{
				Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset,
				Key: msg.Key, Value: msg.Value,
				ErrorInfo: errorpkg.ErrorInfo{
					ErrorType:  errorType,
					StatusCode: statusCode,
					Message:    fmt.Sprintf("grpc: %s", st.Message()),
				},
			})
		}
	}

	return failed, nil
}

// Close closes the gRPC connection.
func (s *GrpcSink) Close() error {
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}
