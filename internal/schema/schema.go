package schema

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Config holds schema-related configuration for the firehose.
type Config struct {
	InputSchemaDataType      string // "json" or "protobuf"
	SchemaRegistryEnabled    bool
	SchemaRegistryURL        string
	SchemaRegistryProtoClass string
	RefreshStrategy          string
	FetchTimeoutMs           int
	AuthBearerToken          string
}

// SchemaManager deserializes raw Kafka message bytes into the format
// expected by downstream sinks.
type SchemaManager interface {
	Parse(rawBytes []byte) ([]byte, error)
}

// NewSchemaManager returns the appropriate SchemaManager based on the
// configured input data type.
func NewSchemaManager(cfg Config) SchemaManager {
	switch cfg.InputSchemaDataType {
	case "protobuf":
		return NewProtobufSchemaManager(ProtobufConfig{
			Enabled:         cfg.SchemaRegistryEnabled,
			URL:             cfg.SchemaRegistryURL,
			ProtoClass:      cfg.SchemaRegistryProtoClass,
			FetchTimeoutMs:  cfg.FetchTimeoutMs,
			AuthBearerToken: cfg.AuthBearerToken,
		})
	default:
		return NewJSONSchemaManager()
	}
}

// ---------------------------------------------------------------------------
// JSON schema manager
// ---------------------------------------------------------------------------

// JSONSchemaManager is a passthrough manager — the raw bytes are already
// JSON and require no deserialization step.
type JSONSchemaManager struct{}

// NewJSONSchemaManager creates a passthrough JSON schema manager.
func NewJSONSchemaManager() *JSONSchemaManager {
	return &JSONSchemaManager{}
}

// Parse returns the input bytes unchanged.
func (m *JSONSchemaManager) Parse(rawBytes []byte) ([]byte, error) {
	return rawBytes, nil
}

// ---------------------------------------------------------------------------
// Protobuf schema manager
// ---------------------------------------------------------------------------

// ProtobufConfig holds configuration specific to protobuf deserialization.
type ProtobufConfig struct {
	Enabled         bool
	URL             string
	ProtoClass      string
	FetchTimeoutMs  int
	AuthBearerToken string
}

// ProtobufSchemaManager handles protobuf-encoded messages. When the schema
// registry is disabled or no parser is available, it acts as a passthrough.
type ProtobufSchemaManager struct {
	config ProtobufConfig
	parser *DescriptorParser
}

// NewProtobufSchemaManager creates a protobuf schema manager from the given
// configuration. If Enabled is true it creates a RegistryClient to fetch
// descriptors; full DynamicMessage parsing is deferred to a later task and
// Parse currently passes through.
func NewProtobufSchemaManager(cfg ProtobufConfig) *ProtobufSchemaManager {
	mgr := &ProtobufSchemaManager{config: cfg}
	if cfg.Enabled {
		_ = NewRegistryClient(cfg.URL, cfg.FetchTimeoutMs, cfg.AuthBearerToken)
		// parser would be populated after fetching + compiling descriptors;
		// full DynamicMessage parsing is a later task.
	}
	return mgr
}

// Parse deserializes protobuf bytes using the descriptor parser. When no
// parser is available (registry disabled or descriptors not yet loaded),
// the raw bytes are returned unchanged.
func (m *ProtobufSchemaManager) Parse(rawBytes []byte) ([]byte, error) {
	if m.parser == nil {
		return rawBytes, nil
	}
	return m.parser.Parse(rawBytes)
}

// DescriptorParser is a placeholder for protobuf descriptor-based parsing.
// Full DynamicMessage parsing will be implemented in a later task.
type DescriptorParser struct{}

// Parse returns the raw bytes unchanged for now.
func (p *DescriptorParser) Parse(rawBytes []byte) ([]byte, error) {
	return rawBytes, nil
}

// ---------------------------------------------------------------------------
// Schema registry client
// ---------------------------------------------------------------------------

// RegistryClient fetches protobuf descriptors from a schema registry.
type RegistryClient struct {
	url        string
	timeoutMs  int
	authToken  string
	httpClient *http.Client
}

// NewRegistryClient creates a client for fetching descriptors from the
// schema registry at the given URL.
func NewRegistryClient(url string, timeoutMs int, authToken string) *RegistryClient {
	timeout := time.Duration(timeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &RegistryClient{
		url:        url,
		timeoutMs:  timeoutMs,
		authToken:  authToken,
		httpClient: &http.Client{Timeout: timeout},
	}
}

// Fetch performs an HTTP GET to the registry URL and returns the response
// body bytes. If an auth token is configured it is sent as a Bearer header.
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
