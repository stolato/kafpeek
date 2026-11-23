package schemareg

import (
	"errors"
	"fmt"

	"github.com/hamba/avro/v2"
)

// Limits for decoding untrusted Avro payloads. Anyone who can produce to a
// topic controls these bytes, and the Avro decoder (hamba/avro, see
// GO-2026-5046/5047/5048) trusts the block counts they declare:
//
//   - an array of a zero-byte type (null, empty record) can declare 2^40 items
//     in 7 bytes, and the allocation kills the process with a fatal,
//     unrecoverable out-of-memory error;
//   - its per-slice MaxSliceAllocSize does not bound nested arrays or maps of
//     arrays, and the running item count can overflow int;
//   - recursive schemas can nest deeply enough to exhaust the stack.
//
// So every payload is first walked against its schema without allocating
// anything, with a budget on the total number of items and on nesting depth.
// Only payloads that pass are handed to the real decoder.
const (
	maxAvroItems = 1 << 20 // array items + map entries across the whole payload
	maxAvroDepth = 128     // nested records/arrays/maps/unions
)

var errAvroLimits = errors.New("payload exceeds decoding limits")

// avroAPI is the decoder configuration: a second line of defense in case the
// walk below and the library ever disagree.
var avroAPI = avro.Config{
	MaxByteSliceSize:  1 << 20,
	MaxSliceAllocSize: maxAvroItems,
}.Freeze()

// checkAvro reports whether p is a well-formed encoding of sch that stays
// within the item and depth budgets.
func checkAvro(sch avro.Schema, p []byte) error {
	w := avroWalker{r: avro.NewReader(nil, 0).Reset(p), items: maxAvroItems}
	if err := w.walk(sch, 0); err != nil {
		return err
	}
	if w.r.Error != nil {
		return fmt.Errorf("malformed payload: %w", w.r.Error)
	}
	return nil
}

type avroWalker struct {
	r     *avro.Reader
	items int64 // remaining budget
}

func (w *avroWalker) walk(s avro.Schema, depth int) error {
	if depth > maxAvroDepth {
		return fmt.Errorf("%w: nested more than %d levels", errAvroLimits, maxAvroDepth)
	}
	if w.r.Error != nil {
		return nil // reported by checkAvro
	}
	r := w.r
	switch s := s.(type) {
	case *avro.NullSchema:
	case *avro.RefSchema:
		return w.walk(s.Schema(), depth+1)
	case *avro.RecordSchema:
		for _, f := range s.Fields() {
			if err := w.walk(f.Type(), depth+1); err != nil {
				return err
			}
		}
	case *avro.ArraySchema:
		return w.blocks(func() error { return w.walk(s.Items(), depth+1) })
	case *avro.MapSchema:
		return w.blocks(func() error {
			r.SkipString()
			return w.walk(s.Values(), depth+1)
		})
	case *avro.UnionSchema:
		i := r.ReadLong()
		types := s.Types()
		if r.Error == nil && (i < 0 || i >= int64(len(types))) {
			return fmt.Errorf("malformed payload: union index %d out of range", i)
		}
		if r.Error == nil {
			return w.walk(types[i], depth+1)
		}
	case *avro.EnumSchema:
		i := r.ReadInt()
		if r.Error == nil && (i < 0 || int(i) >= len(s.Symbols())) {
			return fmt.Errorf("malformed payload: enum index %d out of range", i)
		}
	case *avro.FixedSchema:
		r.SkipNBytes(s.Size())
	case *avro.PrimitiveSchema:
		switch s.Type() {
		case avro.Null:
		case avro.Boolean:
			r.SkipBool()
		case avro.Int:
			r.SkipInt()
		case avro.Long:
			r.SkipLong()
		case avro.Float:
			r.SkipFloat()
		case avro.Double:
			r.SkipDouble()
		case avro.String:
			r.SkipString()
		case avro.Bytes:
			r.SkipBytes()
		default:
			return fmt.Errorf("unsupported avro type %s", s.Type())
		}
	default:
		return fmt.Errorf("unsupported avro schema %T", s)
	}
	return nil
}

// blocks walks an array/map encoding: blocks of items ending with a zero
// count. A negative count is followed by the block's byte size.
func (w *avroWalker) blocks(item func() error) error {
	r := w.r
	for {
		n := r.ReadLong()
		if r.Error != nil || n == 0 {
			return nil
		}
		if n < 0 {
			r.SkipLong() // block size in bytes
			n = -n
		}
		if n < 0 || n > w.items { // n < 0: -MinInt64 overflowed
			return fmt.Errorf("%w: more than %d array/map items", errAvroLimits, maxAvroItems)
		}
		w.items -= n
		for range n {
			if err := item(); err != nil {
				return err
			}
			if r.Error != nil {
				return nil
			}
		}
	}
}
