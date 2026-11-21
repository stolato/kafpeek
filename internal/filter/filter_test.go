package filter

import (
	"strings"
	"testing"
	"time"

	"github.com/stolato/kafpeek/internal/kafka"
)

func msg(key, value string, headers ...string) *kafka.Message {
	m := &kafka.Message{
		Partition: 2, Offset: 42, Timestamp: time.Date(2026, 11, 19, 18, 0, 0, 0, time.UTC),
		Key: &key, Value: &value, ValueEncoding: "utf8", ValueSize: len(value),
	}
	for i := 0; i+1 < len(headers); i += 2 {
		m.Headers = append(m.Headers, kafka.Header{Key: headers[i], Value: headers[i+1]})
	}
	return m
}

func TestMatch(t *testing.T) {
	order := msg("order-1", `{"status":"ERROR","amount":750.5,"items":[{"sku":"a"},{"sku":"b"}],"customer":{"tier":"gold"}}`, "source", "checkout")
	plain := msg("k", "connection timeout after 30s")

	cases := []struct {
		expr string
		m    *kafka.Message
		want bool
	}{
		{`json.status == "ERROR"`, order, true},
		{`json.status == "OK"`, order, false},
		{`json.amount > 500`, order, true}, // int literal vs JSON double
		{`json.amount > 1000`, order, false},
		{`json.customer.tier == "gold" && headers["source"] == "checkout"`, order, true},
		{`json.items.exists(i, i.sku == "b")`, order, true},
		{`size(json.items) == 2`, order, true},
		{`has(json.error)`, order, false},
		{`json.missing == "x"`, order, false},    // eval error => no match
		{`json.status == "ERROR"`, plain, false}, // not JSON => json is null
		{`json == null`, plain, true},
		{`value.contains("timeout")`, plain, true},
		{`value.matches("(?i)TIMEOUT")`, plain, true},
		{`value.lowerAscii().contains("connection")`, plain, true},
		{`key.startsWith("order-")`, order, true},
		{`partition == 2 && offset >= 40`, order, true},
		{`timestamp > timestamp("2026-11-19T17:00:00Z")`, order, true},
		{`"source" in headers`, order, true},
		{`"source" in headers`, plain, false},
	}
	for _, c := range cases {
		p, err := Compile(c.expr)
		if err != nil {
			t.Fatalf("%s: compile: %v", c.expr, err)
		}
		if got := p.Match(NewInput(c.m)); got != c.want {
			t.Errorf("%s = %v, want %v", c.expr, got, c.want)
		}
	}
}

func TestNilProgramMatchesAll(t *testing.T) {
	p, err := Compile("   ")
	if err != nil || p != nil {
		t.Fatalf("empty expr: got %v, %v", p, err)
	}
	if !p.Match(NewInput(msg("a", "b"))) {
		t.Fatal("nil program must match everything")
	}
}

func TestCompileErrors(t *testing.T) {
	cases := map[string]string{
		`json.status ==`:                  "Syntax error",
		`nope == 1`:                       "undeclared reference",
		`key + "x"`:                       "must return a bool",
		`partition == "two"`:              "no matching overload",
		strings.Repeat("a", MaxExprLen+1): "too long",
	}
	for expr, want := range cases {
		_, err := Compile(expr)
		ce, ok := AsCompileError(err)
		if !ok {
			t.Fatalf("%q: expected CompileError, got %v", expr, err)
		}
		if !strings.Contains(ce.Message, want) {
			t.Errorf("%q: error %q does not contain %q", expr, ce.Message, want)
		}
	}
	_, err := Compile(`key == "a" && nope`)
	ce, _ := AsCompileError(err)
	if ce == nil || ce.Column != 14 {
		t.Errorf("expected column 14 for undeclared 'nope', got %+v", ce)
	}
}

func BenchmarkJSONFilter(b *testing.B) {
	p, _ := Compile(`json.status == "ERROR" && json.amount > 500`)
	m := msg("order-1", `{"status":"ERROR","amount":750.5,"id":"x","at":"2026-11-19T18:00:00Z"}`)
	b.ReportAllocs()
	for b.Loop() {
		p.Match(NewInput(m))
	}
}
