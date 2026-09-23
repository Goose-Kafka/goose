package filter

import (
	"encoding/json"
	"fmt"

	"github.com/oliveagle/jsonpath"
)

type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
}

type FilterConfig struct {
	Expression string
	MatchValue string
}

type Filter interface {
	Apply(msgs []Message) (passed []Message, dropped []Message)
}

type NoOpFilter struct{}

func NewNoOpFilter() *NoOpFilter { return &NoOpFilter{} }

func (f *NoOpFilter) Apply(msgs []Message) (passed []Message, dropped []Message) {
	return msgs, nil
}

type JSONPathFilter struct {
	expression string
	matchValue string
	compiled   *jsonpath.Compiled
}

func NewJSONPathFilter(cfg FilterConfig) (*JSONPathFilter, error) {
	compiled, err := jsonpath.Compile(cfg.Expression)
	if err != nil {
		return nil, fmt.Errorf("invalid jsonpath expression %q: %w", cfg.Expression, err)
	}
	return &JSONPathFilter{expression: cfg.Expression, matchValue: cfg.MatchValue, compiled: compiled}, nil
}

func (f *JSONPathFilter) Apply(msgs []Message) (passed []Message, dropped []Message) {
	for _, msg := range msgs {
		var data interface{}
		if err := json.Unmarshal(msg.Value, &data); err != nil {
			dropped = append(dropped, msg)
			continue
		}
		result, err := f.compiled.Lookup(data)
		if err != nil {
			dropped = append(dropped, msg)
			continue
		}
		if matchesValue(result, f.matchValue) {
			passed = append(passed, msg)
		} else {
			dropped = append(dropped, msg)
		}
	}
	return passed, dropped
}

// matchesValue reports whether a jsonpath lookup result equals the configured
// match value. A recursive descent (e.g. "$..status") returns a []interface{}
// even for a single hit, so slices are matched if any element equals the value;
// all other results are compared directly.
func matchesValue(result interface{}, matchValue string) bool {
	if items, ok := result.([]interface{}); ok {
		for _, item := range items {
			if fmt.Sprintf("%v", item) == matchValue {
				return true
			}
		}
		return false
	}
	return fmt.Sprintf("%v", result) == matchValue
}
