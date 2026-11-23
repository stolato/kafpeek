package schemareg

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
)

func zigzag(vals ...int64) []byte {
	var out []byte
	for _, v := range vals {
		out = binary.AppendVarint(out, v)
	}
	return out
}

func avroDec(t *testing.T, schema string) func([]byte) ([]byte, error) {
	t.Helper()
	dec, err := avroDecoder(resolved{doc: schemaDoc{Schema: schema}})
	if err != nil {
		t.Fatal(err)
	}
	return dec
}

// Each payload used to kill the process (fatal OOM / stack overflow) or burn
// CPU for minutes. They must now fail fast with an error.
func TestAvroHostilePayloadsAreRejected(t *testing.T) {
	const listNode = `{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]}]}`
	deepList := bytes.Repeat([]byte{2}, 100_000) // union index 1 (Node) 100k times
	deepList = append(deepList, 0)

	cases := []struct {
		name, schema string
		payload      []byte
	}{
		{"array of null, 2^40 items in one block", `{"type":"array","items":"null"}`, zigzag(1<<40, 0)},
		{"negative block count", `{"type":"array","items":"null"}`, zigzag(-(1 << 62), 1, 0)},
		{"block count overflow across blocks", `{"type":"array","items":"null"}`, zigzag(1, 1<<62, 1<<62, 0)},
		{"MinInt64 block count", `{"type":"array","items":"null"}`, append(binary.AppendVarint(nil, -1<<63), zigzag(1, 0)...)},
		{"nested arrays amplify the per-slice limit",
			`{"type":"array","items":{"type":"array","items":"null"}}`,
			append(zigzag(1000), append(bytes.Repeat(zigzag(1<<20-1, 0), 1000), 0)...)},
		{"map of arrays amplifies the per-slice limit",
			`{"type":"map","values":{"type":"array","items":"null"}}`,
			append(zigzag(1000), append(bytes.Repeat(append(zigzag(0), zigzag(1<<20-1, 0)...), 1000), 0)...)},
		{"recursive schema nested 100k deep", listNode, deepList},
		{"empty records", `{"type":"array","items":{"type":"record","name":"E","fields":[]}}`, zigzag(1<<40, 0)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dec := avroDec(t, c.schema)
			start := time.Now()
			_, err := dec(c.payload)
			if err == nil {
				t.Fatal("hostile payload was decoded")
			}
			if d := time.Since(start); d > 2*time.Second {
				t.Fatalf("rejecting took %v", d)
			}
		})
	}
}

func TestAvroLimitsErrorIsTyped(t *testing.T) {
	_, err := avroDec(t, `{"type":"array","items":"null"}`)(zigzag(1<<40, 0))
	if !errors.Is(err, errAvroLimits) {
		t.Fatalf("err = %v, want errAvroLimits", err)
	}
}

func TestAvroMalformedPayloads(t *testing.T) {
	cases := []struct{ schema, payload string }{
		{`["null","string"]`, string(zigzag(7))},                              // union index out of range
		{`{"type":"enum","name":"C","symbols":["A","B"]}`, string(zigzag(5))}, // enum index out of range
		{`{"type":"fixed","name":"F","size":16}`, "short"},                    // truncated
		{`"string"`, string(zigzag(1 << 40))},                                 // string longer than payload
		{`{"type":"array","items":"int"}`, string(zigzag(1000))},              // truncated items
	}
	for _, c := range cases {
		if _, err := avroDec(t, c.schema)([]byte(c.payload)); err == nil {
			t.Errorf("schema %s accepted malformed payload %q", c.schema, c.payload)
		}
	}
}

// Legitimate data, including multi-block and negative-count (sized) blocks,
// still decodes the same way.
func TestAvroLegitimatePayloadsStillDecode(t *testing.T) {
	schema := `{"type":"record","name":"Order","fields":[
		{"name":"id","type":"string"},
		{"name":"items","type":{"type":"array","items":"long"}},
		{"name":"tags","type":{"type":"map","values":"string"}},
		{"name":"status","type":{"type":"enum","name":"S","symbols":["NEW","PAID"]}},
		{"name":"note","type":["null","string"]},
		{"name":"hash","type":{"type":"fixed","name":"H","size":2}},
		{"name":"ok","type":"boolean"},{"name":"score","type":"double"}]}`
	sch := avro.MustParse(schema)
	payload, err := avro.Marshal(sch, map[string]any{
		"id": "o-1", "items": []int64{1, 2, 3}, "tags": map[string]any{"a": "b"},
		"status": "PAID", "note": "hi", "hash": [2]byte{1, 2}, "ok": true, "score": 1.5,
	})
	if err != nil {
		t.Fatal(err)
	}
	out, err := avroDec(t, schema)(payload)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	if got["id"] != "o-1" || got["status"] != "PAID" || len(got["items"].([]any)) != 3 {
		t.Fatalf("unexpected decode: %s", out)
	}

	// Sized blocks: count -2 followed by the block's byte size, then a second block.
	multi := zigzag(-2, 2, 1, 2, 1, 3, 0)
	out, err = avroDec(t, `{"type":"array","items":"long"}`)(multi)
	if err != nil || string(out) != "[1,2,3]" {
		t.Fatalf("multi-block array: %s, %v", out, err)
	}

	// Up to the budget is fine.
	out, err = avroDec(t, `{"type":"array","items":"null"}`)(zigzag(1000, 0))
	if err != nil || !strings.HasPrefix(string(out), "[null,") {
		t.Fatalf("1000 nulls: %v", err)
	}
}
