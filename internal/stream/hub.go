// Package stream fans out live Kafka messages to many subscribers.
//
// One Kafka consumer is opened per topic, no matter how many browser tabs are
// watching it. Each subscriber has its own bounded buffer; when a subscriber
// is too slow its buffer fills up and messages are dropped (and counted)
// instead of slowing down the consumer or the other subscribers.
package stream

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/stolato/kafpeek/internal/filter"
	"github.com/stolato/kafpeek/internal/kafka"
)

const (
	subscriberBuffer = 1024
	// subscriberMaxBytes bounds the memory a slow subscriber can pin: values
	// are up to 64 KiB, so a full 1024-message buffer could otherwise hold
	// 64 MiB per browser tab.
	subscriberMaxBytes = 4 << 20
)

// Tailer is the subset of kafka.Client the hub needs (eases testing).
type Tailer interface {
	Tail(ctx context.Context, topic string, fn func([]kafka.Message)) error
}

type Hub struct {
	src Tailer
	log *slog.Logger

	mu      sync.Mutex
	streams map[string]*topicStream
	baseCtx context.Context
}

type topicStream struct {
	cancel context.CancelFunc
	subs   map[*Subscriber]struct{}
}

type Subscriber struct {
	// C delivers messages. It is closed if the underlying stream fails;
	// check Err() afterwards.
	C       chan kafka.Message
	filter  *filter.Program // nil = everything
	dropped atomic.Int64
	scanned atomic.Int64
	queued  atomic.Int64 // approximate bytes sitting in C
	err     error
}

// Release must be called for each message received from C: it returns the
// message's bytes to this subscriber's buffer budget.
func (s *Subscriber) Release(m *kafka.Message) { s.queued.Add(-MessageBytes(m)) }

// MessageBytes approximates the memory a message holds.
func MessageBytes(m *kafka.Message) int64 {
	n := 256 + len(m.Topic) // struct, timestamp, encodings...
	if m.Key != nil {
		n += len(*m.Key)
	}
	if m.Value != nil {
		n += len(*m.Value)
	}
	for _, h := range m.Headers {
		n += 32 + len(h.Key) + len(h.Value)
	}
	return int64(n)
}

// Scanned returns and resets how many messages were evaluated against this
// subscriber's filter since the last call (matched or not).
func (s *Subscriber) Scanned() int64 { return s.scanned.Swap(0) }

// Dropped returns and resets the number of messages dropped for this
// subscriber since the last call.
func (s *Subscriber) Dropped() int64 { return s.dropped.Swap(0) }

// Err is the reason the stream ended (valid after C is closed).
func (s *Subscriber) Err() error { return s.err }

func NewHub(ctx context.Context, src Tailer, log *slog.Logger) *Hub {
	return &Hub{src: src, log: log, streams: map[string]*topicStream{}, baseCtx: ctx}
}

// Subscribe attaches to the live stream of a topic, starting the consumer if
// this is the first subscriber. Subscribers with different filters share the
// same consumer; only matching messages reach each one. Call the returned
// func to unsubscribe.
func (h *Hub) Subscribe(topic string, f *filter.Program) (*Subscriber, func()) {
	sub := &Subscriber{C: make(chan kafka.Message, subscriberBuffer), filter: f}

	h.mu.Lock()
	ts, ok := h.streams[topic]
	if !ok {
		ctx, cancel := context.WithCancel(h.baseCtx)
		ts = &topicStream{cancel: cancel, subs: map[*Subscriber]struct{}{}}
		h.streams[topic] = ts
		go h.run(ctx, topic, ts)
	}
	ts.subs[sub] = struct{}{}
	h.mu.Unlock()

	var once sync.Once
	return sub, func() { once.Do(func() { h.unsubscribe(topic, ts, sub) }) }
}

func (h *Hub) unsubscribe(topic string, ts *topicStream, sub *Subscriber) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(ts.subs, sub)
	if len(ts.subs) == 0 && h.streams[topic] == ts {
		delete(h.streams, topic)
		ts.cancel()
		h.log.Info("live stream stopped", "topic", topic)
	}
}

func (h *Hub) run(ctx context.Context, topic string, ts *topicStream) {
	h.log.Info("live stream started", "topic", topic)
	err := h.src.Tail(ctx, topic, func(batch []kafka.Message) {
		// Snapshot under the lock, evaluate and send outside it, so filters
		// never block Subscribe/Unsubscribe. Sending outside the lock is safe:
		// channels are only closed by this goroutine.
		h.mu.Lock()
		subs := make([]*Subscriber, 0, len(ts.subs))
		for sub := range ts.subs {
			subs = append(subs, sub)
		}
		h.mu.Unlock()
		deliver(batch, subs)
	})
	if err == nil || ctx.Err() != nil {
		return
	}

	h.log.Error("live stream failed", "topic", topic, "err", err)
	h.mu.Lock()
	defer h.mu.Unlock()
	for sub := range ts.subs {
		sub.err = err
		close(sub.C)
	}
	ts.subs = map[*Subscriber]struct{}{}
	if h.streams[topic] == ts {
		delete(h.streams, topic)
	}
	ts.cancel()
}

// deliver evaluates each distinct filter once per message (many browser tabs
// usually share the same expression) and fans matches out without blocking.
func deliver(batch []kafka.Message, subs []*Subscriber) {
	if len(subs) == 0 {
		return
	}
	n := int64(len(batch))
	for _, s := range subs {
		s.scanned.Add(n)
	}
	results := map[string]bool{}
	for i := range batch {
		m := &batch[i]
		size := MessageBytes(m)
		in := filter.NewInput(m) // shared: JSON parsed at most once per message
		clear(results)
		for _, s := range subs {
			ok := true
			if s.filter != nil {
				expr := s.filter.String()
				var seen bool
				if ok, seen = results[expr]; !seen {
					ok = s.filter.Match(in)
					results[expr] = ok
				}
			}
			if !ok {
				continue
			}
			// Slow client: drop (and count) instead of blocking the shared
			// consumer, once either the slot or the byte budget is used up.
			if s.queued.Add(size) > subscriberMaxBytes {
				s.queued.Add(-size)
				s.dropped.Add(1)
				continue
			}
			select {
			case s.C <- *m:
			default:
				s.queued.Add(-size)
				s.dropped.Add(1)
			}
		}
	}
}

// Stats reports active topics and subscriber counts.
func (h *Hub) Stats() map[string]int {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string]int, len(h.streams))
	for t, ts := range h.streams {
		out[t] = len(ts.subs)
	}
	return out
}
