package schemareg

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bufbuild/protocompile"
	"github.com/hamba/avro/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

const addressAvro = `{"type":"record","name":"Address","namespace":"shop","fields":[{"name":"city","type":"string"}]}`

const orderAvro = `{"type":"record","name":"Order","namespace":"shop","fields":[
  {"name":"id","type":"string"},
  {"name":"qty","type":"int"},
  {"name":"note","type":["null","string"],"default":null},
  {"name":"price","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
  {"name":"ship_to","type":"shop.Address"}
]}`

const commonProto = `syntax = "proto3";
package shop;
message Money { string currency = 1; int64 cents = 2; }
`

const orderProto = `syntax = "proto3";
package shop;
import "common.proto";
import "google/protobuf/timestamp.proto";
message Ignored { string x = 1; }
message Order {
  string id = 1;
  Money total = 2;
  google.protobuf.Timestamp created_at = 3;
  message Line { string sku = 1; int32 qty = 2; }
}
`

// fakeRegistry serves a few schemas and counts requests.
func fakeRegistry(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	docs := map[string]any{
		"/schemas/ids/1":               map[string]any{"schema": orderAvro, "references": []any{map[string]any{"name": "shop.Address", "subject": "address", "version": 1}}},
		"/subjects/address/versions/1": map[string]any{"schema": addressAvro},
		"/schemas/ids/2":               map[string]any{"schema": orderProto, "schemaType": "PROTOBUF", "references": []any{map[string]any{"name": "common.proto", "subject": "common", "version": 3}}},
		"/subjects/common/versions/3":  map[string]any{"schema": commonProto, "schemaType": "PROTOBUF"},
		"/schemas/ids/3":               map[string]any{"schema": `{"type":"object"}`, "schemaType": "JSON"},
		"/schemas/ids/4":               map[string]any{"schema": `{"type": nope`},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if u, p, _ := r.BasicAuth(); u != "key" || p != "secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path == "/schemas/ids/500" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		d, ok := docs[r.URL.Path]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(d)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func frame(id int, payload []byte) []byte {
	b := []byte{0, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(b[1:], uint32(id))
	return append(b, payload...)
}

func newTestDecoder(t *testing.T, url string) *Decoder {
	t.Helper()
	d, err := New(Config{URL: url, Username: "key", Password: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func decodeJSON(t *testing.T, d *Decoder, b []byte) map[string]any {
	t.Helper()
	res, ok := d.Decode(b)
	if !ok || res.Err != nil {
		t.Fatalf("Decode: ok=%v err=%v", ok, res.Err)
	}
	var m map[string]any
	if err := json.Unmarshal(res.JSON, &m); err != nil {
		t.Fatalf("decoded value is not JSON: %s", res.JSON)
	}
	return m
}

func TestDecodeAvroWithReference(t *testing.T) {
	srv, hits := fakeRegistry(t)
	d := newTestDecoder(t, srv.URL)

	cache := &avro.SchemaCache{}
	if _, err := avro.ParseWithCache(addressAvro, "", cache); err != nil {
		t.Fatal(err)
	}
	sch, err := avro.ParseWithCache(orderAvro, "", cache)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := avro.Marshal(sch, map[string]any{
		"id": "o-1", "qty": 3, "note": "fragile", "price": mustRat("12.50"),
		"ship_to": map[string]any{"city": "Recife"},
	})
	if err != nil {
		t.Fatal(err)
	}

	m := decodeJSON(t, d, frame(1, payload))
	if m["id"] != "o-1" || m["qty"] != float64(3) || m["price"] != 12.5 {
		t.Fatalf("decoded = %v", m)
	}
	if city := m["ship_to"].(map[string]any)["city"]; city != "Recife" {
		t.Fatalf("ship_to.city = %v", city)
	}
	if !strings.Contains(string(mustJSON(m["note"])), "fragile") {
		t.Fatalf("note = %v", m["note"])
	}

	// The schema is fetched once (id + 1 reference), then cached.
	before := hits.Load()
	decodeJSON(t, d, frame(1, payload))
	if hits.Load() != before {
		t.Fatal("schema fetched again; expected the cache to be used")
	}
}

func TestDecodeProtobufWithIndexesAndImports(t *testing.T) {
	srv, _ := fakeRegistry(t)
	d := newTestDecoder(t, srv.URL)

	comp := protocompile.Compiler{Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
		Accessor: protocompile.SourceAccessorFromMap(map[string]string{"order.proto": orderProto, "common.proto": commonProto}),
	})}
	fds, err := comp.Compile(context.Background(), "order.proto")
	if err != nil {
		t.Fatal(err)
	}
	orderMD := fds[0].Messages().ByName("Order")
	msg := dynamicpb.NewMessage(orderMD)
	msg.Set(orderMD.Fields().ByName("id"), protoValue("o-9"))
	money := dynamicpb.NewMessage(orderMD.Fields().ByName("total").Message())
	money.Set(money.Descriptor().Fields().ByName("currency"), protoValue("BRL"))
	msg.Set(orderMD.Fields().ByName("total"), protoMsgValue(money))
	raw, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	// Order is the 2nd top-level message: indexes [1] → count=1, index=1 (zigzag).
	payload := append([]byte{2, 2}, raw...)
	m := decodeJSON(t, d, frame(2, payload))
	if m["id"] != "o-9" || m["total"].(map[string]any)["currency"] != "BRL" {
		t.Fatalf("decoded = %v", m)
	}

	// Nested message Order.Line: indexes [1, 0].
	lineMD := orderMD.Messages().ByName("Line")
	line := dynamicpb.NewMessage(lineMD)
	line.Set(lineMD.Fields().ByName("sku"), protoValue("ABC"))
	raw, _ = proto.Marshal(line)
	m = decodeJSON(t, d, frame(2, append([]byte{4, 2, 0}, raw...)))
	if m["sku"] != "ABC" {
		t.Fatalf("nested decoded = %v", m)
	}
}

func TestDecodeJSONSchemaAndFailures(t *testing.T) {
	srv, _ := fakeRegistry(t)
	d := newTestDecoder(t, srv.URL)

	if m := decodeJSON(t, d, frame(3, []byte(`{"a":1}`))); m["a"] != float64(1) {
		t.Fatalf("json = %v", m)
	}
	// Not framed / unknown ID: left alone.
	for _, b := range [][]byte{[]byte(`{"plain":true}`), {0, 1}, frame(99, []byte("x"))} {
		if _, ok := d.Decode(b); ok {
			t.Fatalf("Decode(%q) handled, want raw passthrough", b)
		}
	}
	// Registry failure and broken schema: reported, not hidden.
	if res, ok := d.Decode(frame(500, []byte("x"))); !ok || res.Err == nil {
		t.Fatalf("registry 500: ok=%v err=%v", ok, res.Err)
	}
	if res, ok := d.Decode(frame(4, []byte("x"))); !ok || res.Err == nil {
		t.Fatalf("broken schema: ok=%v err=%v", ok, res.Err)
	}
	// Payload that does not match the schema.
	if res, ok := d.Decode(frame(3, []byte("not json"))); !ok || res.Err == nil {
		t.Fatalf("bad payload: ok=%v err=%v", ok, res.Err)
	}
	// Nil decoder (registry not configured) never decodes.
	var none *Decoder
	if _, ok := none.Decode(frame(1, nil)); ok {
		t.Fatal("nil decoder handled a payload")
	}
}

func TestBadCredentials(t *testing.T) {
	srv, _ := fakeRegistry(t)
	d, _ := New(Config{URL: srv.URL, Username: "key", Password: "wrong"})
	res, ok := d.Decode(frame(1, []byte("x")))
	if !ok || res.Err == nil || !strings.Contains(res.Err.Error(), "401") {
		t.Fatalf("bad credentials: ok=%v err=%v", ok, res.Err)
	}
}

func TestConfigValidation(t *testing.T) {
	for _, u := range []string{"", "localhost:8081", "ftp://x"} {
		if _, err := New(Config{URL: u}); err == nil {
			t.Errorf("New(%q) accepted an invalid URL", u)
		}
	}
}

func mustRat(s string) *big.Rat {
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		panic(s)
	}
	return r
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func protoValue(s string) protoreflect.Value { return protoreflect.ValueOfString(s) }

func protoMsgValue(m *dynamicpb.Message) protoreflect.Value { return protoreflect.ValueOfMessage(m) }
