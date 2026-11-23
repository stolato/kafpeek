package kafka

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// maxPayload caps how much of a key/value is sent to the browser.
const maxPayload = 64 * 1024

// MaxScan bounds how many records a single filtered browse may read.
const MaxScan = 100_000

// BrowseOptions controls a bounded, one-shot read of recent messages.
type BrowseOptions struct {
	Partition *int32 // nil = all partitions
	Limit     int    // max messages returned (newest first)

	// Match, if set, keeps only matching messages. Scan then sets how many
	// of the newest records (across the selected partitions) are examined.
	Match func(*Message) bool
	Scan  int
}

type BrowseResult struct {
	Messages []Message `json:"messages"`
	Scanned  int       `json:"scanned"`  // records read from Kafka
	Complete bool      `json:"complete"` // false if the time budget ran out
}

// Browse reads the newest messages of a topic without using a consumer
// group, so it never commits offsets or disturbs real consumers.
func (c *Client) Browse(ctx context.Context, topic string, opts BrowseOptions) (BrowseResult, error) {
	if opts.Limit <= 0 || opts.Limit > 1000 {
		opts.Limit = 100
	}
	if opts.Match == nil {
		opts.Scan = opts.Limit
	} else if opts.Scan <= 0 || opts.Scan > MaxScan {
		opts.Scan = min(MaxScan, max(opts.Scan, 10_000))
	}
	res := BrowseResult{Messages: []Message{}, Complete: true}
	start, end, err := c.offsets(ctx, topic)
	if err != nil {
		return res, err
	}
	parts, ok := end[topic]
	if !ok {
		return res, fmt.Errorf("topic %q: %w", topic, ErrNotFound)
	}

	// Spread the scan depth evenly over the partitions that hold records.
	active := livePartitions(topic, start, end, opts.Partition)
	depth := int64(opts.Scan)
	if opts.Match != nil && len(active) > 0 {
		depth = int64((opts.Scan + len(active) - 1) / len(active))
	}

	assign := map[int32]kgo.Offset{}
	remaining := map[int32]int64{} // partition -> last offset we still expect
	for _, p := range active {
		so, _ := start.Lookup(topic, p)
		eo := parts[p]
		from := max(so.Offset, eo.Offset-depth)
		assign[p] = kgo.NewOffset().At(from)
		remaining[p] = eo.Offset - 1
	}
	if len(assign) == 0 {
		return res, nil
	}

	cons, err := c.newConsumer(kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: assign}))
	if err != nil {
		return res, err
	}
	defer cons.Close()

	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	var msgs []Message
	for len(remaining) > 0 && ctx.Err() == nil {
		// Short per-poll timeout: transaction markers / compaction can mean
		// the "last" offset is never delivered as a record.
		pollCtx, pcancel := context.WithTimeout(ctx, 2*time.Second)
		fetches := cons.PollFetches(pollCtx)
		pcancel()

		got := 0
		fetches.EachRecord(func(r *kgo.Record) {
			got++
			res.Scanned++
			m := c.toMessage(r)
			if opts.Match == nil || opts.Match(&m) {
				msgs = append(msgs, m)
				// Keep memory bounded: only the newest Limit can be returned,
				// so trim as we go instead of holding every match (a filter
				// like "true" over 100k records would otherwise keep GBs).
				if len(msgs) >= 2*opts.Limit {
					msgs = newest(msgs, opts.Limit)
				}
			}
			if last, ok := remaining[r.Partition]; ok && r.Offset >= last {
				delete(remaining, r.Partition)
			}
		})
		for _, fe := range fetches.Errors() {
			if !errors.Is(fe.Err, context.DeadlineExceeded) && !errors.Is(fe.Err, context.Canceled) {
				return res, fe.Err
			}
		}
		if got == 0 {
			break // idle: nothing more is coming
		}
	}

	res.Complete = ctx.Err() == nil // false only when the time budget ran out

	msgs = newest(msgs, opts.Limit)
	if msgs != nil {
		res.Messages = msgs
	}
	return res, nil
}

// livePartitions returns the selected partitions that hold at least one
// record. Empty partitions must not take a share of a newest-records budget.
func livePartitions(topic string, start, end kadm.ListedOffsets, only *int32) []int32 {
	var out []int32
	for p, eo := range end[topic] {
		if only != nil && *only != p {
			continue
		}
		so, ok := start.Lookup(topic, p)
		if eo.Err != nil || !ok || so.Err != nil || eo.Offset <= so.Offset {
			continue
		}
		out = append(out, p)
	}
	return out
}

// newest sorts msgs newest-first and keeps at most n of them. The result is
// copied so the dropped tail of the backing array can be garbage collected.
func newest(msgs []Message, n int) []Message {
	sort.Slice(msgs, func(i, j int) bool {
		if !msgs[i].Timestamp.Equal(msgs[j].Timestamp) {
			return msgs[i].Timestamp.After(msgs[j].Timestamp)
		}
		return msgs[i].Offset > msgs[j].Offset
	})
	if len(msgs) <= n {
		return msgs
	}
	out := make([]Message, n, 2*n)
	copy(out, msgs[:n])
	return out
}

// Tail consumes new messages of a topic (starting at the end) and calls fn
// with each polled batch until ctx is cancelled or a fatal error happens.
func (c *Client) Tail(ctx context.Context, topic string, fn func([]Message)) error {
	cons, err := c.newConsumer(
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		return err
	}
	defer cons.Close()

	for {
		fetches := cons.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		for _, fe := range fetches.Errors() {
			if errors.Is(fe.Err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("partition %d: %w", fe.Partition, fe.Err)
		}
		batch := make([]Message, 0, fetches.NumRecords())
		fetches.EachRecord(func(r *kgo.Record) { batch = append(batch, c.toMessage(r)) })
		if len(batch) > 0 {
			fn(batch)
		}
	}
}

func (c *Client) toMessage(r *kgo.Record) Message {
	m := Message{
		Topic:     r.Topic,
		Partition: r.Partition,
		Offset:    r.Offset,
		Timestamp: r.Timestamp,
		ValueSize: len(r.Value),
	}
	if r.Key != nil {
		k, enc, _ := encode(c.decodeSchema(r.Key, &m.KeySchema))
		m.Key, m.KeyEncoding = &k, enc
	}
	if r.Value != nil {
		v, enc, truncated := encode(c.decodeSchema(r.Value, &m.ValueSchema))
		m.Value, m.ValueEncoding, m.Truncated = &v, enc, truncated
	}
	for _, h := range r.Headers {
		v, _, _ := encode(h.Value)
		m.Headers = append(m.Headers, Header{Key: h.Key, Value: v})
	}
	return m
}

// decodeSchema returns the JSON form of a Schema Registry payload (recording
// the schema in *ref), or b unchanged when it is not registry-framed or
// cannot be decoded (the error is then reported in *ref).
func (c *Client) decodeSchema(b []byte, ref **SchemaRef) []byte {
	res, ok := c.schemas.Decode(b)
	if !ok {
		return b
	}
	*ref = &SchemaRef{ID: res.ID, Type: res.Type}
	if res.Err != nil {
		(*ref).Error = res.Err.Error()
		return b
	}
	return res.JSON
}

func encode(b []byte) (s, encoding string, truncated bool) {
	if len(b) > maxPayload {
		b, truncated = b[:maxPayload], true
		// Don't let the cut split a multi-byte UTF-8 rune.
		for i := 0; i < utf8.UTFMax-1 && len(b) > 0 && !utf8.Valid(b); i++ {
			b = b[:len(b)-1]
		}
	}
	if utf8.Valid(b) {
		return string(b), "utf8", truncated
	}
	return base64.StdEncoding.EncodeToString(b), "base64", truncated
}
