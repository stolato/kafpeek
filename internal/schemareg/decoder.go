package schemareg

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/bufbuild/protocompile"
	"github.com/hamba/avro/v2"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Result describes a decoded (or failed) registry-framed payload.
type Result struct {
	ID   int
	Type string // AVRO, PROTOBUF or JSON
	JSON []byte // the payload as JSON, when Err is nil
	Err  error
}

// How long failures are remembered, so a broken or unreachable registry is
// not hit once per scanned record.
const (
	errorTTL    = 30 * time.Second
	notFoundTTL = 5 * time.Minute
	fetchBudget = 10 * time.Second
)

// Decoder turns registry-framed bytes into JSON. Schemas are fetched once
// per ID and cached; it is safe for concurrent use. A nil *Decoder decodes
// nothing.
type Decoder struct {
	reg *registry

	mu    sync.Mutex
	cache map[int]*entry
}

type entry struct {
	ready   chan struct{}
	kind    string
	decode  func([]byte) ([]byte, error)
	err     error
	missing bool // unknown ID: treat the bytes as not registry-framed
	expires time.Time
}

// New returns a decoder for the registry at cfg.URL.
func New(cfg Config) (*Decoder, error) {
	reg, err := newRegistry(cfg)
	if err != nil {
		return nil, err
	}
	return &Decoder{reg: reg, cache: map[int]*entry{}}, nil
}

// Decode reports ok=false when b is not registry-framed (or the registry does
// not know its schema ID); the caller then shows the raw bytes.
func (d *Decoder) Decode(b []byte) (Result, bool) {
	if d == nil || len(b) < 5 || b[0] != 0 {
		return Result{}, false
	}
	id := int(binary.BigEndian.Uint32(b[1:5]))
	e := d.lookup(id)
	if e.missing {
		return Result{}, false
	}
	res := Result{ID: id, Type: e.kind}
	if e.err != nil {
		res.Err = e.err
		return res, true
	}
	res.JSON, res.Err = e.decode(b[5:])
	return res, true
}

func (d *Decoder) lookup(id int) *entry {
	d.mu.Lock()
	e, ok := d.cache[id]
	if ok && !e.expires.IsZero() && time.Now().After(e.expires) {
		select {
		case <-e.ready: // only drop finished entries
			ok = false
		default:
		}
	}
	if !ok {
		e = &entry{ready: make(chan struct{})}
		d.cache[id] = e
		d.mu.Unlock()
		d.load(id, e)
		close(e.ready)
		return e
	}
	d.mu.Unlock()
	<-e.ready
	return e
}

func (d *Decoder) load(id int, e *entry) {
	ctx, cancel := context.WithTimeout(context.Background(), fetchBudget)
	defer cancel()
	r, err := d.reg.resolve(ctx, id)
	switch {
	case errors.Is(err, errNotFound):
		e.missing, e.expires = true, time.Now().Add(notFoundTTL)
		return
	case err != nil:
		e.err, e.expires = err, time.Now().Add(errorTTL)
		return
	}
	e.kind = r.doc.kind()
	switch e.kind {
	case "AVRO":
		e.decode, err = avroDecoder(r)
	case "PROTOBUF":
		e.decode, err = protoDecoder(ctx, id, r)
	case "JSON":
		e.decode = jsonDecoder
	default:
		err = fmt.Errorf("unsupported schema type %q", e.kind)
	}
	if err != nil {
		// A schema that does not compile will not start compiling later.
		e.err = fmt.Errorf("schema %d (%s): %w", id, e.kind, err)
	}
}

// ---- Avro ----

func avroDecoder(r resolved) (func([]byte) ([]byte, error), error) {
	cache := &avro.SchemaCache{} // per schema ID: versions may reuse type names
	for _, dep := range r.deps {
		if _, err := avro.ParseWithCache(dep.doc.Schema, "", cache); err != nil {
			return nil, fmt.Errorf("reference %s: %w", dep.name, err)
		}
	}
	sch, err := avro.ParseWithCache(r.doc.Schema, "", cache)
	if err != nil {
		return nil, err
	}
	return func(p []byte) ([]byte, error) {
		var v any
		if err := avro.Unmarshal(sch, p, &v); err != nil {
			return nil, err
		}
		return json.Marshal(normalize(v))
	}, nil
}

// normalize makes Avro-decoded values JSON-friendly (decimals as numbers).
func normalize(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, x := range t {
			t[k] = normalize(x)
		}
	case []any:
		for i, x := range t {
			t[i] = normalize(x)
		}
	case *big.Rat:
		s := t.FloatString(18)
		if strings.Contains(s, ".") {
			s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
		}
		return json.Number(s)
	}
	return v
}

// ---- Protobuf ----

// Imports the registry treats as built-in (no reference needed).
var builtinProtos = map[string]string{
	"confluent/meta.proto": `syntax = "proto3";
package confluent;
import "google/protobuf/descriptor.proto";
message Meta { string doc = 1; map<string, string> params = 2; repeated string tags = 3; }
extend google.protobuf.FileOptions { Meta file_meta = 1088; }
extend google.protobuf.MessageOptions { Meta message_meta = 1088; }
extend google.protobuf.FieldOptions { Meta field_meta = 1088; }
extend google.protobuf.EnumOptions { Meta enum_meta = 1088; }
extend google.protobuf.EnumValueOptions { Meta enum_value_meta = 1088; }
`,
	"confluent/type/decimal.proto": `syntax = "proto3";
package confluent.type;
message Decimal { bytes value = 1; uint32 precision = 2; int32 scale = 3; }
`,
}

func protoDecoder(ctx context.Context, id int, r resolved) (func([]byte) ([]byte, error), error) {
	files := make(map[string]string, len(r.deps)+len(builtinProtos)+1)
	for name, src := range builtinProtos {
		files[name] = src
	}
	for _, dep := range r.deps {
		files[dep.name] = dep.doc.Schema
	}
	main := fmt.Sprintf("kafpeek/schema-%d.proto", id)
	files[main] = r.doc.Schema

	comp := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			Accessor: protocompile.SourceAccessorFromMap(files),
		}),
	}
	fds, err := comp.Compile(ctx, main)
	if err != nil {
		return nil, err
	}
	fd := fds[0]
	marshal := protojson.MarshalOptions{UseProtoNames: true}
	return func(p []byte) ([]byte, error) {
		path, rest, err := messageIndexes(p)
		if err != nil {
			return nil, err
		}
		md, err := messageAt(fd, path)
		if err != nil {
			return nil, err
		}
		msg := dynamicpb.NewMessage(md)
		if err := proto.Unmarshal(rest, msg); err != nil {
			return nil, err
		}
		return marshal.Marshal(msg)
	}, nil
}

// messageIndexes reads Confluent's message-index path: a zigzag varint count
// followed by that many zigzag varints; a single 0 byte means [0].
func messageIndexes(p []byte) ([]int, []byte, error) {
	n, read := binary.Varint(p)
	if read <= 0 || n < 0 || n > 64 {
		return nil, nil, fmt.Errorf("invalid protobuf message index header")
	}
	p = p[read:]
	if n == 0 {
		return []int{0}, p, nil
	}
	path := make([]int, n)
	for i := range path {
		v, read := binary.Varint(p)
		if read <= 0 || v < 0 {
			return nil, nil, fmt.Errorf("invalid protobuf message index")
		}
		path[i], p = int(v), p[read:]
	}
	return path, p, nil
}

func messageAt(fd protoreflect.FileDescriptor, path []int) (protoreflect.MessageDescriptor, error) {
	msgs := fd.Messages()
	var md protoreflect.MessageDescriptor
	for _, i := range path {
		if i >= msgs.Len() {
			return nil, fmt.Errorf("message index %v not found in schema", path)
		}
		md = msgs.Get(i)
		msgs = md.Messages()
	}
	if md == nil {
		return nil, fmt.Errorf("schema has no messages")
	}
	return md, nil
}

// ---- JSON Schema ----

func jsonDecoder(p []byte) ([]byte, error) {
	if !json.Valid(p) {
		return nil, fmt.Errorf("payload is not valid JSON")
	}
	return p, nil
}
