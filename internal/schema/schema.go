package schema

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Config holds schema-related configuration for the firehose.
type Config struct {
	InputSchemaDataType      string // "json" or "protobuf"
	SchemaRegistryEnabled    bool
	SchemaRegistryURL        string
	SchemaRegistryProtoClass string
	RefreshStrategy          string // "long_polling", "periodic", "none"
	RefreshIntervalMs        int
	FetchTimeoutMs           int
	AuthBearerToken          string
}

// SchemaManager deserializes raw Kafka message bytes into the format
// expected by downstream sinks (JSON for HTTP REST, raw bytes for protobuf).
type SchemaManager interface {
	Parse(rawBytes []byte) ([]byte, error)
	Close()
}

// NewSchemaManager returns the appropriate SchemaManager based on the
// configured input data type. For protobuf, it fetches descriptors from
// the schema registry on startup and starts a background refresh goroutine.
func NewSchemaManager(cfg Config) (SchemaManager, error) {
	switch cfg.InputSchemaDataType {
	case "protobuf":
		return NewProtobufSchemaManager(ProtobufConfig{
			Enabled:           cfg.SchemaRegistryEnabled,
			URL:               cfg.SchemaRegistryURL,
			ProtoClass:        cfg.SchemaRegistryProtoClass,
			RefreshStrategy:   cfg.RefreshStrategy,
			RefreshIntervalMs: cfg.RefreshIntervalMs,
			FetchTimeoutMs:    cfg.FetchTimeoutMs,
			AuthBearerToken:   cfg.AuthBearerToken,
		})
	default:
		return NewJSONSchemaManager(), nil
	}
}

// ---------------------------------------------------------------------------
// JSON schema manager
// ---------------------------------------------------------------------------

type JSONSchemaManager struct{}

func NewJSONSchemaManager() *JSONSchemaManager {
	return &JSONSchemaManager{}
}

func (m *JSONSchemaManager) Parse(rawBytes []byte) ([]byte, error) {
	return rawBytes, nil
}

func (m *JSONSchemaManager) Close() {}

// ---------------------------------------------------------------------------
// Protobuf schema manager — descriptor fetching + DynamicMessage parsing
// ---------------------------------------------------------------------------

type ProtobufConfig struct {
	Enabled           bool
	URL               string
	ProtoClass        string
	RefreshStrategy   string
	RefreshIntervalMs int
	FetchTimeoutMs    int
	AuthBearerToken   string
}

// ProtobufSchemaManager fetches proto descriptors from a schema registry,
// parses raw protobuf bytes into DynamicMessages, and converts them to JSON.
// Supports hot-swapping descriptors via long-polling refresh.
type ProtobufSchemaManager struct {
	config        ProtobufConfig
	client        *RegistryClient
	currentParser atomic.Pointer[DescriptorParser]
	cancelFunc    context.CancelFunc
}

// DescriptorParser parses raw protobuf bytes using a dynamically loaded
// descriptor. It converts proto → JSON for HTTP sinks.
type DescriptorParser struct {
	messageType protoreflect.MessageType
	protoClass  string
}

func NewDescriptorParser(fd *descriptorpb.FileDescriptorSet, protoClass string) (*DescriptorParser, error) {
	files, err := protodesc.NewFiles(fd)
	if err != nil {
		return nil, fmt.Errorf("build file descriptor from descriptor set: %w", err)
	}

	// Find the message descriptor for the configured proto class
	// Proto class format: "package.MessageName" or "package.MessageName"
	fullName := protoreflect.FullName(protoClass)
	desc, err := files.FindDescriptorByName(fullName)
	if err != nil {
		return nil, fmt.Errorf("proto class %q not found in descriptor: %w", protoClass, err)
	}

	msgDesc, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("proto class %q is not a message type", protoClass)
	}

	return &DescriptorParser{
		messageType: dynamicpb.NewMessageType(msgDesc),
		protoClass:  protoClass,
	}, nil
}

// Parse deserializes raw protobuf bytes into a DynamicMessage and converts
// to JSON using protojson.Marshal.
func (p *DescriptorParser) Parse(rawBytes []byte) ([]byte, error) {
	msg := dynamicpb.NewMessage(p.messageType.Descriptor())
	if err := proto.Unmarshal(rawBytes, msg); err != nil {
		return nil, fmt.Errorf("unmarshal protobuf: %w", err)
	}

	jsonBytes, err := protojson.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal to JSON: %w", err)
	}

	return jsonBytes, nil
}

func NewProtobufSchemaManager(cfg ProtobufConfig) (*ProtobufSchemaManager, error) {
	if !cfg.Enabled {
		// Passthrough mode — no registry, just return raw bytes
		mgr := &ProtobufSchemaManager{config: cfg}
		return mgr, nil
	}

	if cfg.URL == "" {
		return nil, fmt.Errorf("SCHEMA_REGISTRY_URL is required when SCHEMA_REGISTRY_ENABLED=true")
	}
	if cfg.ProtoClass == "" {
		return nil, fmt.Errorf("SCHEMA_REGISTRY_PROTO_CLASS is required when SCHEMA_REGISTRY_ENABLED=true")
	}

	timeout := time.Duration(cfg.FetchTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	mgr := &ProtobufSchemaManager{
		config: cfg,
		client: NewRegistryClient(cfg.URL, cfg.FetchTimeoutMs, cfg.AuthBearerToken),
	}

	// Initial fetch — must succeed on startup (fail fast)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := mgr.fetchAndSwap(ctx); err != nil {
		return nil, fmt.Errorf("initial schema fetch: %w", err)
	}

	log.Printf("schema: loaded proto class %q from %s", cfg.ProtoClass, maskURL(cfg.URL))

	// Start background refresh goroutine
	refreshCtx, refreshCancel := context.WithCancel(context.Background())
	mgr.cancelFunc = refreshCancel
	go mgr.refreshLoop(refreshCtx)

	return mgr, nil
}

// Parse uses the current parser (loaded atomically) to deserialize protobuf
// bytes and convert to JSON. If no parser is loaded (registry disabled),
// returns raw bytes as passthrough.
func (m *ProtobufSchemaManager) Parse(rawBytes []byte) ([]byte, error) {
	parser := m.currentParser.Load()
	if parser == nil {
		// Passthrough — no descriptor loaded
		return rawBytes, nil
	}
	return parser.Parse(rawBytes)
}

func (m *ProtobufSchemaManager) Close() {
	if m.cancelFunc != nil {
		m.cancelFunc()
	}
}

// fetchAndSwap fetches the descriptor from the registry, builds a new parser,
// and atomically swaps it in. Called on startup and during refresh.
func (m *ProtobufSchemaManager) fetchAndSwap(ctx context.Context) error {
	descBytes, err := m.client.Fetch(ctx)
	if err != nil {
		return err
	}

	// Parse the descriptor bytes into a FileDescriptorSet
	fd := &descriptorpb.FileDescriptorSet{}
	if err := proto.Unmarshal(descBytes, fd); err != nil {
		return fmt.Errorf("unmarshal FileDescriptorSet: %w", err)
	}

	newParser, err := NewDescriptorParser(fd, m.config.ProtoClass)
	if err != nil {
		return fmt.Errorf("build descriptor parser: %w", err)
	}

	m.currentParser.Store(newParser)
	return nil
}

// refreshLoop runs in a background goroutine and refreshes the descriptor
// based on the configured strategy (long_polling or periodic).
func (m *ProtobufSchemaManager) refreshLoop(ctx context.Context) {
	switch m.config.RefreshStrategy {
	case "none", "":
		return // no refresh
	case "periodic":
		interval := time.Duration(m.config.RefreshIntervalMs) * time.Millisecond
		if interval <= 0 {
			interval = 5 * time.Minute
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fetchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				if err := m.fetchAndSwap(fetchCtx); err != nil {
					log.Printf("schema: periodic refresh error: %v", err)
				} else {
					log.Printf("schema: refreshed proto class %q", m.config.ProtoClass)
				}
				cancel()
			}
		}
	case "long_polling":
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			// Long-poll: use a long timeout so the server holds the connection
			// until a new version is available
			fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
			err := m.fetchAndSwap(fetchCtx)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("schema: long-poll refresh error: %v", err)
				time.Sleep(10 * time.Second) // backoff before retry
				continue
			}
			log.Printf("schema: long-poll detected update for proto class %q", m.config.ProtoClass)
		}
	default:
		return
	}
}

func maskURL(url string) string {
	// Mask auth tokens in URL if present
	if strings.Contains(url, "token=") {
		return strings.Split(url, "?")[0] + "?token=***"
	}
	return url
}

// ---------------------------------------------------------------------------
// Schema registry client
// ---------------------------------------------------------------------------

type RegistryClient struct {
	url        string
	timeoutMs  int
	authToken  string
	httpClient *http.Client
}

func NewRegistryClient(url string, timeoutMs int, authToken string) *RegistryClient {
	timeout := time.Duration(timeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &RegistryClient{
		url:        url,
		timeoutMs:  timeoutMs,
		authToken:  authToken,
		httpClient: &http.Client{Timeout: timeout},
	}
}

func (c *RegistryClient) Fetch(ctx context.Context) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return nil, fmt.Errorf("schema registry: create request: %w", err)
	}
	if c.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.authToken)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("schema registry: fetch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("schema registry: unexpected status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("schema registry: read body: %w", err)
	}
	return body, nil
}
