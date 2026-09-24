package filter

import (
	"testing"
)

func TestCELFilterSimpleEquality(t *testing.T) {
	f, err := NewCELFilter(`message.status == "created"`)
	if err != nil {
		t.Fatalf("NewCELFilter error: %v", err)
	}

	msgs := []Message{
		{Value: []byte(`{"status":"created","id":1}`)},
		{Value: []byte(`{"status":"shipped","id":2}`)},
		{Value: []byte(`{"status":"created","id":3}`)},
	}

	passed, dropped := f.Apply(msgs)
	if len(passed) != 2 {
		t.Errorf("expected 2 passed, got %d", len(passed))
	}
	if len(dropped) != 1 {
		t.Errorf("expected 1 dropped, got %d", len(dropped))
	}
}

func TestCELFilterAndCondition(t *testing.T) {
	f, err := NewCELFilter(`message.status == "created" && message.amount > 100`)
	if err != nil {
		t.Fatalf("NewCELFilter error: %v", err)
	}

	msgs := []Message{
		{Value: []byte(`{"status":"created","amount":50}`)},
		{Value: []byte(`{"status":"created","amount":500}`)},
		{Value: []byte(`{"status":"shipped","amount":500}`)},
	}

	passed, dropped := f.Apply(msgs)
	if len(passed) != 1 {
		t.Errorf("expected 1 passed (status=created AND amount>100), got %d", len(passed))
	}
	if len(dropped) != 2 {
		t.Errorf("expected 2 dropped, got %d", len(dropped))
	}
}

func TestCELFilterOrCondition(t *testing.T) {
	f, err := NewCELFilter(`message.status == "created" || message.status == "pending"`)
	if err != nil {
		t.Fatalf("NewCELFilter error: %v", err)
	}

	msgs := []Message{
		{Value: []byte(`{"status":"created"}`)},
		{Value: []byte(`{"status":"pending"}`)},
		{Value: []byte(`{"status":"shipped"}`)},
		{Value: []byte(`{"status":"cancelled"}`)},
	}

	passed, dropped := f.Apply(msgs)
	if len(passed) != 2 {
		t.Errorf("expected 2 passed (created OR pending), got %d", len(passed))
	}
	if len(dropped) != 2 {
		t.Errorf("expected 2 dropped, got %d", len(dropped))
	}
}

func TestCELFilterInOperator(t *testing.T) {
	f, err := NewCELFilter(`message.city in ["Mumbai", "Delhi", "Bangalore"]`)
	if err != nil {
		t.Fatalf("NewCELFilter error: %v", err)
	}

	msgs := []Message{
		{Value: []byte(`{"city":"Mumbai"}`)},
		{Value: []byte(`{"city":"Chennai"}`)},
		{Value: []byte(`{"city":"Bangalore"}`)},
	}

	passed, dropped := f.Apply(msgs)
	if len(passed) != 2 {
		t.Errorf("expected 2 passed (Mumbai, Bangalore), got %d", len(passed))
	}
	if len(dropped) != 1 {
		t.Errorf("expected 1 dropped (Chennai), got %d", len(dropped))
	}
}

func TestCELFilterNotEqual(t *testing.T) {
	f, err := NewCELFilter(`message.status != "cancelled"`)
	if err != nil {
		t.Fatalf("NewCELFilter error: %v", err)
	}

	msgs := []Message{
		{Value: []byte(`{"status":"created"}`)},
		{Value: []byte(`{"status":"cancelled"}`)},
		{Value: []byte(`{"status":"shipped"}`)},
	}

	passed, dropped := f.Apply(msgs)
	if len(passed) != 2 {
		t.Errorf("expected 2 passed (not cancelled), got %d", len(passed))
	}
	if len(dropped) != 1 {
		t.Errorf("expected 1 dropped (cancelled), got %d", len(dropped))
	}
}

func TestCELFilterHasMacro(t *testing.T) {
	f, err := NewCELFilter(`has(message.customer_id)`)
	if err != nil {
		t.Fatalf("NewCELFilter error: %v", err)
	}

	msgs := []Message{
		{Value: []byte(`{"customer_id":"123"}`)},
		{Value: []byte(`{"other_field":"value"}`)},
	}

	passed, dropped := f.Apply(msgs)
	if len(passed) != 1 {
		t.Errorf("expected 1 passed (has customer_id), got %d", len(passed))
	}
	if len(dropped) != 1 {
		t.Errorf("expected 1 dropped (no customer_id), got %d", len(dropped))
	}
}

func TestCELFilterStringMethods(t *testing.T) {
	f, err := NewCELFilter(`message.name.startsWith("order-")`)
	if err != nil {
		t.Fatalf("NewCELFilter error: %v", err)
	}

	msgs := []Message{
		{Value: []byte(`{"name":"order-123"}`)},
		{Value: []byte(`{"name":"payment-456"}`)},
		{Value: []byte(`{"name":"order-789"}`)},
	}

	passed, dropped := f.Apply(msgs)
	if len(passed) != 2 {
		t.Errorf("expected 2 passed (startsWith order-), got %d", len(passed))
	}
	if len(dropped) != 1 {
		t.Errorf("expected 1 dropped, got %d", len(dropped))
	}
}

func TestCELFilterSizeMethod(t *testing.T) {
	f, err := NewCELFilter(`message.items.size() > 0`)
	if err != nil {
		t.Fatalf("NewCELFilter error: %v", err)
	}

	msgs := []Message{
		{Value: []byte(`{"items":["A","B"]}`)},
		{Value: []byte(`{"items":[]}`)},
		{Value: []byte(`{"items":["C"]}`)},
	}

	passed, dropped := f.Apply(msgs)
	if len(passed) != 2 {
		t.Errorf("expected 2 passed (items.size() > 0), got %d", len(passed))
	}
	if len(dropped) != 1 {
		t.Errorf("expected 1 dropped (empty items), got %d", len(dropped))
	}
}

func TestCELFilterNestedFields(t *testing.T) {
	f, err := NewCELFilter(`message.order.status == "created" && message.order.amount > 100`)
	if err != nil {
		t.Fatalf("NewCELFilter error: %v", err)
	}

	msgs := []Message{
		{Value: []byte(`{"order":{"status":"created","amount":50}}`)},
		{Value: []byte(`{"order":{"status":"created","amount":500}}`)},
		{Value: []byte(`{"order":{"status":"shipped","amount":500}}`)},
	}

	passed, dropped := f.Apply(msgs)
	if len(passed) != 1 {
		t.Errorf("expected 1 passed (nested: status=created AND amount>100), got %d", len(passed))
	}
	if len(dropped) != 2 {
		t.Errorf("expected 2 dropped, got %d", len(dropped))
	}
}

func TestCELFilterInvalidExpression(t *testing.T) {
	_, err := NewCELFilter(`message.status ==`)
	if err == nil {
		t.Fatal("expected error for invalid CEL expression")
	}
}

func TestCELFilterInvalidJSON(t *testing.T) {
	f, _ := NewCELFilter(`message.status == "created"`)

	msgs := []Message{
		{Value: []byte(`not json`)},
	}

	_, dropped := f.Apply(msgs)
	if len(dropped) != 1 {
		t.Errorf("invalid JSON should be dropped, got %d", len(dropped))
	}
}

func TestFilterFactory(t *testing.T) {
	// JSONPath filter
	jsonFilter, err := NewFilter(FilterConfig{
		Engine:     "jsonpath",
		Expression: "$.status",
		MatchValue: "created",
	})
	if err != nil {
		t.Fatalf("NewFilter jsonpath error: %v", err)
	}
	if _, ok := jsonFilter.(*JSONPathFilter); !ok {
		t.Errorf("expected *JSONPathFilter, got %T", jsonFilter)
	}

	// CEL filter
	celFilter, err := NewFilter(FilterConfig{
		Engine:     "cel",
		Expression: `message.status == "created"`,
	})
	if err != nil {
		t.Fatalf("NewFilter cel error: %v", err)
	}
	if _, ok := celFilter.(*CELFilter); !ok {
		t.Errorf("expected *CELFilter, got %T", celFilter)
	}

	// Unknown engine
	_, err = NewFilter(FilterConfig{Engine: "jexl", Expression: "test"})
	if err == nil {
		t.Fatal("expected error for unknown filter engine")
	}
}
