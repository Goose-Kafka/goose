package filter

import (
	"encoding/json"
	"fmt"

	cel "cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types/ref"
)

// CELFilter evaluates a CEL (Common Expression Language) expression against
// each message's JSON content. If the expression evaluates to true, the
// message passes; otherwise it's dropped.
//
// CEL supports:
//   - Comparison: ==, !=, <, >, <=, >=
//   - Logical: &&, ||, !
//   - Arithmetic: +, -, *, /
//   - String: contains(), startsWith(), endsWith(), matches()
//   - Collections: in, size()
//   - Field access: message.order.status, message.order.items[0]
//   - Macros: has(field), all(), exists(), filter()
//
// The entire message JSON is available as the variable "message".
//
// Examples:
//
//	message.order.status == "created"
//	message.order.status == "created" && message.order.amount > 100
//	message.order.city in ["Mumbai", "Delhi"]
//	message.order.status != "cancelled" && has(message.order.customer_id)
//	message.order.items.size() > 0
//	message.order.name.startsWith("order-")
type CELFilter struct {
	env     *cel.Env
	program cel.Program
}

// NewCELFilter compiles a CEL expression that operates on a JSON object
// bound to the variable "message".
func NewCELFilter(expression string) (*CELFilter, error) {
	env, err := cel.NewEnv(
		cel.Variable("message", cel.DynType),
	)
	if err != nil {
		return nil, fmt.Errorf("cel: create environment: %w", err)
	}

	ast, iss := env.Parse(expression)
	if iss.Err() != nil {
		return nil, fmt.Errorf("cel: parse expression %q: %w", expression, iss.Err())
	}

	// Type-check with permissive options (unknown fields are OK since we use DynType)
	checked, iss := env.Check(ast)
	if iss.Err() != nil {
		return nil, fmt.Errorf("cel: type-check expression %q: %w", expression, iss.Err())
	}

	program, err := env.Program(checked)
	if err != nil {
		return nil, fmt.Errorf("cel: build program: %w", err)
	}

	return &CELFilter{env: env, program: program}, nil
}

// Apply evaluates the CEL expression against each message's JSON content.
func (f *CELFilter) Apply(msgs []Message) (passed []Message, dropped []Message) {
	for _, msg := range msgs {
		var data map[string]interface{}
		if err := json.Unmarshal(msg.Value, &data); err != nil {
			dropped = append(dropped, msg)
			continue
		}

		result, _, err := f.program.Eval(map[string]interface{}{
			"message": data,
		})
		if err != nil {
			dropped = append(dropped, msg)
			continue
		}

		if isTruthy(result) {
			passed = append(passed, msg)
		} else {
			dropped = append(dropped, msg)
		}
	}
	return passed, dropped
}

// isTruthy returns true if the CEL evaluation result is boolean true.
func isTruthy(val ref.Val) bool {
	if val == nil {
		return false
	}
	// Try to convert to bool
	boolVal, ok := val.Value().(bool)
	return ok && boolVal
}
