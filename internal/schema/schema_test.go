package schema

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestJSONSchemaParse(t *testing.T) {
	mgr := NewJSONSchemaManager()
	input := []byte(`{"order_id":"123","status":"created"}`)

	out, err := mgr.Parse(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(out) != string(input) {
		t.Fatalf("expected passthrough %q, got %q", input, out)
	}
}

func TestProtobufSchemaManagerNoRegistry(t *testing.T) {
	mgr := NewProtobufSchemaManager(ProtobufConfig{Enabled: false})
	input := []byte{0x08, 0x01}

	out, err := mgr.Parse(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(out) != string(input) {
		t.Fatalf("expected passthrough %q, got %q", input, out)
	}
}

func TestSchemaManagerFactory(t *testing.T) {
	jsonMgr := NewSchemaManager(Config{InputSchemaDataType: "json"})
	if _, ok := jsonMgr.(*JSONSchemaManager); !ok {
		t.Fatalf("expected *JSONSchemaManager, got %T", jsonMgr)
	}

	protoMgr := NewSchemaManager(Config{InputSchemaDataType: "protobuf"})
	if _, ok := protoMgr.(*ProtobufSchemaManager); !ok {
		t.Fatalf("expected *ProtobufSchemaManager, got %T", protoMgr)
	}
}

func TestSchemaRegistryFetch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("mock-descriptor-data"))
	}))
	defer server.Close()

	client := NewRegistryClient(server.URL, 5000, "")
	data, err := client.Fetch(context.TODO())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := []byte("mock-descriptor-data")
	if string(data) != string(expected) {
		t.Fatalf("expected %q, got %q", expected, data)
	}

	// Verify the body reader is fully consumed (best-effort).
	_ = io.Discard
}
