package stream

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stolato/kafpeek/internal/filter"
	"github.com/stolato/kafpeek/internal/kafka"
)

// fakeTailer emits whatever is sent on feed; counts how many tails started.
type fakeTailer struct {
	feed    chan []kafka.Message
	started atomic.Int32
	failErr error
}

func (f *fakeTailer) Tail(ctx context.Context, _ string, fn func([]kafka.Message)) error {
	f.started.Add(1)
	for {
		select {
		case <-ctx.Done():
			return nil
		case b, ok := <-f.feed:
			if !ok {
				return f.failErr
			}
			fn(b)
		}
	}
}

func newHub(t *testing.T, src Tailer) *Hub {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return NewHub(ctx, src, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func recv(t *testing.T, s *Subscriber) kafka.Message {
	t.Helper()
	select {
	case m := <-s.C:
		return m
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message")
		return kafka.Message{}
	}
}

func TestSharedConsumerAndCleanup(t *testing.T) {
	src := &fakeTailer{feed: make(chan []kafka.Message)}
	h := newHub(t, src)

	a, unsubA := h.Subscribe("orders", nil)
	b, unsubB := h.Subscribe("orders", nil)
	src.feed <- []kafka.Message{{Offset: 1}}

	if recv(t, a).Offset != 1 || recv(t, b).Offset != 1 {
		t.Fatal("both subscribers should get the message")
	}
	if n := src.started.Load(); n != 1 {
		t.Fatalf("expected 1 consumer, got %d", n)
	}

	unsubA()
	unsubA() // idempotent
	if h.Stats()["orders"] != 1 {
		t.Fatal("stream should stay alive while a subscriber remains")
	}
	unsubB()
	if _, ok := h.Stats()["orders"]; ok {
		t.Fatal("stream should stop after last unsubscribe")
	}
}

func TestSlowSubscriberDropsInsteadOfBlocking(t *testing.T) {
	src := &fakeTailer{feed: make(chan []kafka.Message)}
	h := newHub(t, src)
	slow, unsubSlow := h.Subscribe("t", nil)
	defer unsubSlow()

	big := make([]kafka.Message, subscriberBuffer+100)
	select {
	case src.feed <- big:
	case <-time.After(time.Second):
		t.Fatal("hub blocked on a slow subscriber")
	}
	// Push one more batch to be sure the first one was fully processed.
	src.feed <- []kafka.Message{{}}
	if d := slow.Dropped(); d != 101 {
		t.Fatalf("expected 101 dropped, got %d", d)
	}
	if slow.Dropped() != 0 {
		t.Fatal("Dropped should reset after reading")
	}
}

// A subscriber that stops reading must not pin more than subscriberMaxBytes,
// even when every message is large (pentest: 60 stalled tabs reached 7.7 GB).
func TestSlowSubscriberByteBudget(t *testing.T) {
	src := &fakeTailer{feed: make(chan []kafka.Message)}
	h := newHub(t, src)
	slow, unsub := h.Subscribe("t", nil)
	defer unsub()

	v := string(make([]byte, 64<<10))
	batch := make([]kafka.Message, 200) // ~12.8 MiB, well under the slot count
	for i := range batch {
		batch[i].Value = &v
	}
	src.feed <- batch
	src.feed <- nil // sync: first batch fully delivered

	queued := len(slow.C)
	if got := slow.queued.Load(); got > subscriberMaxBytes {
		t.Fatalf("queued %d bytes, budget is %d", got, subscriberMaxBytes)
	}
	if queued == 0 || queued >= len(batch) {
		t.Fatalf("expected a partial buffer, got %d of %d messages", queued, len(batch))
	}
	if d := slow.Dropped(); d != int64(len(batch)-queued) {
		t.Fatalf("dropped %d, want %d", d, len(batch)-queued)
	}

	// Reading and releasing frees the budget for new messages.
	for range queued {
		m := recv(t, slow)
		slow.Release(&m)
	}
	if got := slow.queued.Load(); got != 0 {
		t.Fatalf("budget not returned after Release: %d bytes", got)
	}
	src.feed <- batch[:1]
	if m := recv(t, slow); m.Value == nil {
		t.Fatal("expected delivery after the budget was freed")
	}
}

func TestFailureClosesSubscribers(t *testing.T) {
	src := &fakeTailer{feed: make(chan []kafka.Message), failErr: errors.New("auth failed")}
	h := newHub(t, src)
	s, unsub := h.Subscribe("t", nil)
	defer unsub()

	close(src.feed)
	select {
	case _, ok := <-s.C:
		if ok {
			t.Fatal("expected closed channel")
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber was not closed on failure")
	}
	if s.Err() == nil || s.Err().Error() != "auth failed" {
		t.Fatalf("unexpected err: %v", s.Err())
	}
}

func TestPerSubscriberFilters(t *testing.T) {
	src := &fakeTailer{feed: make(chan []kafka.Message)}
	h := newHub(t, src)

	errs, _ := filter.Compile(`json.level == "ERROR"`)
	all, unsubAll := h.Subscribe("logs", nil)
	defer unsubAll()
	onlyErr, unsubErr := h.Subscribe("logs", errs)
	defer unsubErr()

	val := func(s string) *string { return &s }
	src.feed <- []kafka.Message{
		{Offset: 1, Value: val(`{"level":"INFO"}`), ValueEncoding: "utf8"},
		{Offset: 2, Value: val(`{"level":"ERROR"}`), ValueEncoding: "utf8"},
		{Offset: 3, Value: val(`not json`), ValueEncoding: "utf8"},
	}
	src.feed <- nil // barrier: first batch fully delivered

	if got := recv(t, onlyErr).Offset; got != 2 {
		t.Fatalf("filtered subscriber got offset %d, want 2", got)
	}
	select {
	case m := <-onlyErr.C:
		t.Fatalf("filtered subscriber got unexpected offset %d", m.Offset)
	default:
	}
	for want := int64(1); want <= 3; want++ {
		if got := recv(t, all).Offset; got != want {
			t.Fatalf("unfiltered subscriber got %d, want %d", got, want)
		}
	}
	if n := onlyErr.Scanned(); n != 3 {
		t.Fatalf("scanned = %d, want 3", n)
	}
	if n := src.started.Load(); n != 1 {
		t.Fatalf("expected 1 shared consumer, got %d", n)
	}
}
