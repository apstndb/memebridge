// Package cliparams converts CLI-style query parameter assignments
// ("name:value" arguments or already-split name→value maps) into
// [cloud.google.com/go/spanner.GenericColumnValue] maps, using memefish
// literal parsing via [github.com/apstndb/memebridge].
//
// It extracts the parameter handling shared by spanner-mycli and
// execspansql: each value string is parsed as a GoogleSQL expression
// literal, and — when [WithBareTypeAsNull] is enabled — a value that parses
// as a bare type (for example "ARRAY<STRING>") yields a typed NULL
// parameter instead, which is how PLAN-mode tools declare parameter types
// without values.
package cliparams

import (
	"fmt"
	"strings"

	"cloud.google.com/go/spanner"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/cloudspannerecosystem/memefish"

	"github.com/apstndb/memebridge"
)

// Option configures parsing.
type Option func(*config)

type config struct {
	separator      string
	bareTypeAsNull bool
}

func newConfig(opts []Option) config {
	cfg := config{separator: ":"}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

// WithSeparator sets the name/value separator used by [SplitAssignment] and
// [ParseAssignments]. The default is ":"; flag conventions using "=" can
// pass WithSeparator("="). The assignment splits at the first occurrence,
// so values may contain the separator.
func WithSeparator(separator string) Option {
	return func(cfg *config) { cfg.separator = separator }
}

// WithBareTypeAsNull accepts a bare type as an assignment value, producing
// a typed NULL parameter (for example "p:ARRAY<STRING>" → NULL
// ARRAY<STRING>). This matches how PLAN-mode tools declare parameter types;
// without this option such values fail expression parsing. Note that with
// this option enabled, a value that is both a valid type and a valid
// expression is interpreted as a type.
func WithBareTypeAsNull() Option {
	return func(cfg *config) { cfg.bareTypeAsNull = true }
}

// SplitAssignment splits one "name<separator>value" argument. The name must
// be non-empty; the value may contain further separator occurrences.
func SplitAssignment(arg string, opts ...Option) (name, value string, err error) {
	cfg := newConfig(opts)
	if cfg.separator == "" {
		return "", "", fmt.Errorf("cliparams: separator must not be empty")
	}
	name, value, found := strings.Cut(arg, cfg.separator)
	if !found {
		return "", "", fmt.Errorf("cliparams: assignment %q does not contain separator %q", arg, cfg.separator)
	}
	if name == "" {
		return "", "", fmt.Errorf("cliparams: assignment %q has an empty parameter name", arg)
	}
	return name, value, nil
}

// ParseValue converts one parameter value string into a
// [spanner.GenericColumnValue]: a GoogleSQL expression literal, or — with
// [WithBareTypeAsNull] — a bare type yielding a typed NULL.
func ParseValue(value string, opts ...Option) (spanner.GenericColumnValue, error) {
	cfg := newConfig(opts)
	if cfg.bareTypeAsNull {
		if typ, err := memefish.ParseType("", value); err == nil {
			t, err := memebridge.MemefishTypeToSpannerpbType(typ)
			if err != nil {
				return spanner.GenericColumnValue{}, fmt.Errorf("cliparams: generating typed NULL for %q: %w", value, err)
			}
			return gcvctor.NullOf(t), nil
		}
		// Not a type; fall through to expression parsing.
	}
	expr, err := memefish.ParseExpr("", value)
	if err != nil {
		return spanner.GenericColumnValue{}, fmt.Errorf("cliparams: parsing expression %q: %w", value, err)
	}
	gcv, err := memebridge.MemefishExprToGCV(expr)
	if err != nil {
		return spanner.GenericColumnValue{}, fmt.Errorf("cliparams: generating value for %q: %w", value, err)
	}
	return gcv, nil
}

// ParseAssignments parses raw "name<separator>value" arguments into a
// parameter map. Duplicate names are an error.
func ParseAssignments(args []string, opts ...Option) (map[string]spanner.GenericColumnValue, error) {
	params := make(map[string]spanner.GenericColumnValue, len(args))
	for _, arg := range args {
		name, value, err := SplitAssignment(arg, opts...)
		if err != nil {
			return nil, err
		}
		if _, ok := params[name]; ok {
			return nil, fmt.Errorf("cliparams: duplicate parameter name %q", name)
		}
		gcv, err := ParseValue(value, opts...)
		if err != nil {
			return nil, fmt.Errorf("cliparams: parameter %q: %w", name, err)
		}
		params[name] = gcv
	}
	return params, nil
}

// ParseMap converts an already-split name→value map (the shape produced by
// flag libraries with map values) into a parameter map. The separator
// option is irrelevant here.
func ParseMap(values map[string]string, opts ...Option) (map[string]spanner.GenericColumnValue, error) {
	params := make(map[string]spanner.GenericColumnValue, len(values))
	for name, value := range values {
		if name == "" {
			return nil, fmt.Errorf("cliparams: empty parameter name")
		}
		gcv, err := ParseValue(value, opts...)
		if err != nil {
			return nil, fmt.Errorf("cliparams: parameter %q: %w", name, err)
		}
		params[name] = gcv
	}
	return params, nil
}

// StatementParams widens a parameter map to the map[string]any shape of
// [cloud.google.com/go/spanner.Statement] Params. Values stay
// [spanner.GenericColumnValue] (the client supports it directly).
func StatementParams(params map[string]spanner.GenericColumnValue) map[string]any {
	if params == nil {
		return nil
	}
	out := make(map[string]any, len(params))
	for name, gcv := range params {
		out[name] = gcv
	}
	return out
}
