package validation

import "testing"

func TestValidatorSingleExpression(t *testing.T) {
	v, err := NewValidator(`has(message.order_id) && message.order_id != ""`)
	if err != nil {
		t.Fatalf("NewValidator error: %v", err)
	}

	tests := []struct {
		json     string
		expected bool
	}{
		{`{"order_id":"123"}`, true},
		{`{"order_id":""}`, false},
		{`{"other":"value"}`, false},
		{`not json`, false},
	}

	for _, tt := range tests {
		valid, _ := v.Validate([]byte(tt.json))
		if valid != tt.expected {
			t.Errorf("Validate(%q) = %v, want %v", tt.json, valid, tt.expected)
		}
	}
}

func TestValidatorMultipleExpressions(t *testing.T) {
	v, err := NewValidator(`has(message.order_id) && message.order_id != ""; message.amount > 0; message.status in ["created","pending","shipped"]`)
	if err != nil {
		t.Fatalf("NewValidator error: %v", err)
	}

	tests := []struct {
		json     string
		expected bool
		desc     string
	}{
		{`{"order_id":"123","amount":100,"status":"created"}`, true, "all valid"},
		{`{"order_id":"","amount":100,"status":"created"}`, false, "empty order_id"},
		{`{"order_id":"123","amount":0,"status":"created"}`, false, "amount not > 0"},
		{`{"order_id":"123","amount":100,"status":"cancelled"}`, false, "status not in list"},
		{`{"amount":100,"status":"created"}`, false, "missing order_id"},
		{`{"order_id":"123","amount":-5,"status":"created"}`, false, "negative amount"},
	}

	for _, tt := range tests {
		valid, reason := v.Validate([]byte(tt.json))
		if valid != tt.expected {
			t.Errorf("Validate(%q) [%s] = %v, want %v (reason: %s)", tt.json, tt.desc, valid, tt.expected, reason)
		}
		_ = reason
	}
}

func TestValidatorInvalidExpression(t *testing.T) {
	_, err := NewValidator(`message.status ==`)
	if err == nil {
		t.Fatal("expected error for invalid CEL expression")
	}
}

func TestValidatorEmptyExpressions(t *testing.T) {
	_, err := NewValidator("")
	if err == nil {
		t.Fatal("expected error for empty expressions")
	}
}

func TestValidatorNestedFields(t *testing.T) {
	v, err := NewValidator(`message.order.status == "created" && message.order.payment.amount > 500`)
	if err != nil {
		t.Fatalf("NewValidator error: %v", err)
	}

	valid, _ := v.Validate([]byte(`{"order":{"status":"created","payment":{"amount":1000}}}`))
	if !valid {
		t.Error("expected valid for nested fields")
	}

	valid, reason := v.Validate([]byte(`{"order":{"status":"shipped","payment":{"amount":1000}}}`))
	if valid {
		t.Error("expected invalid for wrong status")
	}
	_ = reason
}

func TestValidatorReturnsFailedExpression(t *testing.T) {
	v, _ := NewValidator(`message.x == 1; message.y == 2; message.z == 3`)

	_, reason := v.Validate([]byte(`{"x":1,"y":99,"z":3}`))
	if reason != `message.y == 2` {
		t.Errorf("expected failed expression 'message.y == 2', got %q", reason)
	}
}

func TestDebugValidation(t *testing.T) {
	// Test has() + != "" in a SINGLE expression
	v, err := NewValidator(`has(message.order_id) && message.order_id != ""`)
	if err != nil {
		t.Fatalf("NewValidator error: %v", err)
	}

	// Test with empty string
	valid, reason := v.Validate([]byte(`{"order_id":"","amount":100}`))
	t.Logf("empty string: valid=%v reason=%q", valid, reason)
	if valid {
		t.Errorf("expected invalid for empty order_id")
	}

	// Test with non-empty string
	valid, reason = v.Validate([]byte(`{"order_id":"123","amount":100}`))
	t.Logf("non-empty: valid=%v reason=%q", valid, reason)
	if !valid {
		t.Errorf("expected valid for non-empty order_id")
	}
}
