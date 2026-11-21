// Package filter compiles and evaluates CEL (Common Expression Language)
// expressions against Kafka messages, server-side.
//
// Variables available in an expression:
//
//	key        string               record key ("" when null)
//	value      string               raw value (UTF-8 text, or base64 for binary payloads)
//	json       dyn                  value parsed as JSON (null when the value is not JSON)
//	headers    map(string, string)  record headers
//	partition  int
//	offset     int
//	timestamp  google.protobuf.Timestamp
//	size       int                  value size in bytes
//
// Examples:
//
//	json.status == "ERROR"
//	json.amount > 500 && headers["source"] == "checkout"
//	key.startsWith("order-") && value.contains("timeout")
//	has(json.error) || value.matches("(?i)exception")
//	timestamp > timestamp("2026-11-19T18:00:00Z")
package filter

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/ext"
	"cel.dev/cel-go/interpreter"

	"github.com/stolato/kafpeek/internal/kafka"
)

const (
	// MaxExprLen bounds what users can submit.
	MaxExprLen = 2000
	// costLimit bounds CPU per evaluation (protects the shared consumer
	// from pathological expressions like huge comprehensions).
	costLimit = 100_000
)

var (
	envOnce sync.Once
	env     *cel.Env
	envErr  error
)

func celEnv() (*cel.Env, error) {
	envOnce.Do(func() {
		env, envErr = cel.NewEnv(
			cel.Variable("key", cel.StringType),
			cel.Variable("value", cel.StringType),
			cel.Variable("json", cel.DynType),
			cel.Variable("headers", cel.MapType(cel.StringType, cel.StringType)),
			cel.Variable("partition", cel.IntType),
			cel.Variable("offset", cel.IntType),
			cel.Variable("timestamp", cel.TimestampType),
			cel.Variable("size", cel.IntType),
			// json.amount > 500 works even though JSON numbers are doubles.
			cel.CrossTypeNumericComparisons(true),
			ext.Strings(),  // lowerAscii(), upperAscii(), split(), trim(), ...
			ext.Encoders(), // base64.decode() for binary values
		)
	})
	return env, envErr
}

// CompileError describes an invalid expression, with a 0-based column
// (or -1) so the UI can point at the problem.
type CompileError struct {
	Message string `json:"error"`
	Column  int    `json:"column"`
}

func (e *CompileError) Error() string { return e.Message }

// Program is a compiled, reusable, concurrency-safe filter.
type Program struct {
	expr string
	prg  cel.Program
}

func (p *Program) String() string {
	if p == nil {
		return ""
	}
	return p.expr
}

// Compile parses and type-checks expr. An empty expression returns (nil, nil):
// "no filter".
func Compile(expr string) (*Program, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, nil
	}
	if len(expr) > MaxExprLen {
		return nil, &CompileError{Message: fmt.Sprintf("expression too long (max %d chars)", MaxExprLen), Column: -1}
	}
	e, err := celEnv()
	if err != nil {
		return nil, err
	}
	ast, iss := e.Compile(expr)
	if iss != nil && iss.Err() != nil {
		ce := &CompileError{Message: iss.Err().Error(), Column: -1}
		if errs := iss.Errors(); len(errs) > 0 {
			ce.Message = errs[0].Message
			if errs[0].Location != nil {
				ce.Column = errs[0].Location.Column()
			}
		}
		return nil, ce
	}
	if !ast.OutputType().IsExactType(cel.BoolType) && !ast.OutputType().IsExactType(cel.DynType) {
		return nil, &CompileError{Message: fmt.Sprintf("expression must return a bool, got %s", ast.OutputType()), Column: -1}
	}
	prg, err := e.Program(ast, cel.CostLimit(costLimit), cel.EvalOptions(cel.OptOptimize))
	if err != nil {
		return nil, &CompileError{Message: err.Error(), Column: -1}
	}
	return &Program{expr: expr, prg: prg}, nil
}

// Match reports whether m satisfies the filter. Evaluation errors (e.g. a
// missing JSON field, wrong type) count as "no match" rather than failing
// the stream: messages on a topic are rarely uniform.
func (p *Program) Match(in *Input) bool {
	if p == nil {
		return true
	}
	out, _, err := p.prg.Eval(in)
	if err != nil {
		return false
	}
	b, ok := out.(types.Bool)
	return ok && bool(b)
}

// Input adapts a message to CEL lazily: the JSON body is parsed at most
// once, and only if an expression actually references `json`. One Input can
// be shared by several programs evaluating the same message.
type Input struct {
	m *kafka.Message

	jsonDone bool
	jsonVal  any
	headers  map[string]string
}

var _ interpreter.Activation = (*Input)(nil)

func NewInput(m *kafka.Message) *Input { return &Input{m: m} }

func (in *Input) Parent() interpreter.Activation { return nil }

func (in *Input) ResolveName(name string) (any, bool) {
	m := in.m
	switch name {
	case "key":
		if m.Key == nil {
			return "", true
		}
		return *m.Key, true
	case "value":
		if m.Value == nil {
			return "", true
		}
		return *m.Value, true
	case "json":
		return in.json(), true
	case "headers":
		if in.headers == nil {
			in.headers = make(map[string]string, len(m.Headers))
			for _, h := range m.Headers {
				in.headers[h.Key] = h.Value
			}
		}
		return in.headers, true
	case "partition":
		return int64(m.Partition), true
	case "offset":
		return m.Offset, true
	case "timestamp":
		return m.Timestamp, true
	case "size":
		return int64(m.ValueSize), true
	}
	return nil, false
}

func (in *Input) json() any {
	if !in.jsonDone {
		in.jsonDone = true
		m := in.m
		if m.Value != nil && m.ValueEncoding == "utf8" && !m.Truncated {
			var v any
			if err := json.Unmarshal([]byte(*m.Value), &v); err == nil {
				in.jsonVal = v
			}
		}
	}
	if in.jsonVal == nil {
		return types.NullValue
	}
	return in.jsonVal
}

// AsCompileError unwraps a *CompileError.
func AsCompileError(err error) (*CompileError, bool) {
	var ce *CompileError
	ok := errors.As(err, &ce)
	return ce, ok
}
