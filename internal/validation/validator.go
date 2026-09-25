package validation

import (
	"encoding/json"
	"fmt"
	"strings"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types/ref"
)

// Validator validates messages against a set of CEL expressions.
// All expressions must evaluate to true for a message to be considered valid.
type Validator struct {
	programs []cel.Program
	exprList []string
}

// NewValidator creates a Validator from a semicolon-separated list of CEL expressions.
// Each expression is compiled independently. The message JSON is available as
// the variable "message" in each expression.
func NewValidator(expressions string) (*Validator, error) {
	exprs := splitExpressions(expressions)
	if len(exprs) == 0 {
		return nil, fmt.Errorf("no validation expressions provided")
	}

	v := &Validator{exprList: exprs}

	for _, expr := range exprs {
		env, err := cel.NewEnv(cel.Variable("message", cel.DynType))
		if err != nil {
			return nil, fmt.Errorf("cel: create env for %q: %w", expr, err)
		}

		ast, iss := env.Parse(expr)
		if iss.Err() != nil {
			return nil, fmt.Errorf("cel: parse %q: %w", expr, iss.Err())
		}

		checked, iss := env.Check(ast)
		if iss.Err() != nil {
			return nil, fmt.Errorf("cel: type-check %q: %w", expr, iss.Err())
		}

		program, err := env.Program(checked)
		if err != nil {
			return nil, fmt.Errorf("cel: build program for %q: %w", expr, err)
		}

		v.programs = append(v.programs, program)
	}

	return v, nil
}

// Validate checks a message's JSON content against all CEL expressions.
// Returns (true, "") if valid, (false, "failed expression") if invalid.
func (v *Validator) Validate(jsonBytes []byte) (bool, string) {
	var data map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &data); err != nil {
		return false, fmt.Sprintf("invalid JSON: %v", err)
	}

	for i, program := range v.programs {
		result, _, err := program.Eval(map[string]interface{}{"message": data})
		if err != nil {
			return false, fmt.Sprintf("expression %q evaluation error: %v", v.exprList[i], err)
		}
		if !isTruthy(result) {
			return false, v.exprList[i]
		}
	}

	return true, ""
}

func splitExpressions(s string) []string {
	var result []string
	for _, expr := range strings.Split(s, ";") {
		expr = strings.TrimSpace(expr)
		if expr != "" {
			result = append(result, expr)
		}
	}
	return result
}

func isTruthy(val ref.Val) bool {
	if val == nil {
		return false
	}
	boolVal, ok := val.Value().(bool)
	return ok && boolVal
}
