package schema

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
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
	mgr.Close()
}

func TestProtobufSchemaManagerNoRegistry(t *testing.T) {
	mgr, err := NewProtobufSchemaManager(ProtobufConfig{Enabled: false})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer mgr.Close()
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
	jsonMgr, err := NewSchemaManager(Config{InputSchemaDataType: "json"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := jsonMgr.(*JSONSchemaManager); !ok {
		t.Fatalf("expected *JSONSchemaManager, got %T", jsonMgr)
	}

	// protobuf without registry enabled → passthrough mode
	protoMgr, err := NewSchemaManager(Config{InputSchemaDataType: "protobuf"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := protoMgr.(*ProtobufSchemaManager); !ok {
		t.Fatalf("expected *ProtobufSchemaManager, got %T", protoMgr)
	}
	protoMgr.Close()
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
}

func TestProtobufSchemaManagerWithRegistry(t *testing.T) {
	// Build a minimal FileDescriptorSet for a test proto:
	// message TestMessage { string name = 1; int32 id = 2; }
	fd := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			{
				Name:    proto.String("test.proto"),
				Package: proto.String("test"),
				MessageType: []*descriptorpb.DescriptorProto{
					{
						Name: proto.String("TestMessage"),
						Field: []*descriptorpb.FieldDescriptorProto{
							{
								Name:     proto.String("name"),
								Number:   proto.Int32(1),
								Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
								Type:     descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
								JsonName: proto.String("name"),
							},
							{
								Name:     proto.String("id"),
								Number:   proto.Int32(2),
								Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
								Type:     descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(),
								JsonName: proto.String("id"),
							},
						},
					},
				},
				Syntax: proto.String("proto3"),
			},
		},
	}

	descBytes, err := proto.Marshal(fd)
	if err != nil {
		t.Fatalf("marshal descriptor set: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(descBytes)
	}))
	defer server.Close()

	mgr, err := NewProtobufSchemaManager(ProtobufConfig{
		Enabled:         true,
		URL:             server.URL,
		ProtoClass:      "test.TestMessage",
		FetchTimeoutMs:  5000,
		RefreshStrategy: "none",
	})
	if err != nil {
		t.Fatalf("NewProtobufSchemaManager error: %v", err)
	}
	defer mgr.Close()

	// Build test protobuf message: TestMessage{name: "hello", id: 42}
	// Field 1 (name) = "hello" → tag 0x0a, length 5, "hello"
	// Field 2 (id) = 42 → tag 0x10, value 42 (varint 0x2a)
	rawProto := []byte{
		0x0a, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f, // name = "hello"
		0x10, 0x2a, // id = 42
	}

	jsonOut, err := mgr.Parse(rawProto)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	jsonStr := string(jsonOut)
	t.Logf("Converted JSON: %s", jsonStr)

	if !containsStr(jsonStr, `"name":"hello"`) {
		t.Errorf("JSON should contain name=hello, got: %s", jsonStr)
	}
	if !containsStr(jsonStr, `"id":42`) {
		t.Errorf("JSON should contain id=42, got: %s", jsonStr)
	}
}

func TestProtobufSchemaManagerValidationError(t *testing.T) {
	fd := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			{
				Name:    proto.String("test.proto"),
				Package: proto.String("test"),
				MessageType: []*descriptorpb.DescriptorProto{
					{
						Name: proto.String("TestMessage"),
						Field: []*descriptorpb.FieldDescriptorProto{
							{
								Name:   proto.String("name"),
								Number: proto.Int32(1),
								Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
								Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
							},
						},
					},
				},
				Syntax: proto.String("proto3"),
			},
		},
	}

	descBytes, _ := proto.Marshal(fd)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(descBytes)
	}))
	defer server.Close()

	mgr, err := NewProtobufSchemaManager(ProtobufConfig{
		Enabled:         true,
		URL:             server.URL,
		ProtoClass:      "test.TestMessage",
		FetchTimeoutMs:  5000,
		RefreshStrategy: "none",
	})
	if err != nil {
		t.Fatalf("NewProtobufSchemaManager error: %v", err)
	}
	defer mgr.Close()

	// Valid protobuf should work
	validProto := []byte{0x0a, 0x05, 'h', 'e', 'l', 'l', 'o'}
	_, err = mgr.Parse(validProto)
	if err != nil {
		t.Errorf("valid protobuf should parse without error: %v", err)
	}
}

func TestProtobufSchemaManagerMissingProtoClass(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fd := &descriptorpb.FileDescriptorSet{}
		descBytes, _ := proto.Marshal(fd)
		w.Write(descBytes)
	}))
	defer server.Close()

	_, err := NewProtobufSchemaManager(ProtobufConfig{
		Enabled:         true,
		URL:             server.URL,
		ProtoClass:      "nonexistent.Message",
		FetchTimeoutMs:  5000,
		RefreshStrategy: "none",
	})
	if err == nil {
		t.Fatal("expected error for missing proto class")
	}
}

func TestProtobufSchemaManagerMissingURL(t *testing.T) {
	_, err := NewProtobufSchemaManager(ProtobufConfig{
		Enabled:    true,
		URL:        "",
		ProtoClass: "test.Message",
	})
	if err == nil {
		t.Fatal("expected error for missing URL")
	}
}

func containsStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
