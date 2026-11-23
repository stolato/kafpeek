package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func ptr64(v int64) *int64 { return &v }

func TestResetOffsets(t *testing.T) {
	c, cluster, ctx := newTestClient(t, 2, "orders")
	produceN(t, ctx, cluster, "orders", 0, 10)
	produceN(t, ctx, cluster, "orders", 1, 4)

	// An empty group with commits at 5 (p0) and nothing on p1.
	seed := make(kadm.Offsets)
	seed.Add(kadm.Offset{Topic: "orders", Partition: 0, At: 5, LeaderEpoch: -1})
	if _, err := c.adm.CommitOffsets(ctx, "billing", seed); err != nil {
		t.Fatal(err)
	}

	committedAt := func(p int32) int64 {
		t.Helper()
		os, err := c.adm.FetchOffsets(ctx, "billing")
		if err != nil {
			t.Fatal(err)
		}
		o, ok := os.Lookup("orders", p)
		if !ok {
			return -1
		}
		return o.At
	}

	// Dry run: plan only.
	res, err := c.ResetOffsets(ctx, "billing", ResetOffsetsRequest{Topic: "orders", To: ResetEarliest, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied || len(res.Changes) != 2 || res.Changes[0].Current != 5 || res.Changes[0].Target != 0 || res.Changes[0].LagAfter != 10 {
		t.Fatalf("dry run = %+v", res)
	}
	if res.Changes[1].Current != -1 || res.Changes[1].LagBefore != -1 {
		t.Fatalf("p1 without a commit should report -1: %+v", res.Changes[1])
	}
	if got := committedAt(0); got != 5 {
		t.Fatalf("dry run committed: p0 at %d, want 5", got)
	}

	// Shift fails when an explicitly selected partition has no commit...
	if _, err := c.ResetOffsets(ctx, "billing", ResetOffsetsRequest{Topic: "orders", Partitions: []int32{1}, To: ResetShift, Offset: ptr64(-2)}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("shift on an uncommitted partition: err=%v, want ErrInvalid", err)
	}
	// ...but across all partitions it skips the uncommitted ones.
	res, err = c.ResetOffsets(ctx, "billing", ResetOffsetsRequest{Topic: "orders", To: ResetShift, Offset: ptr64(-2)})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Changes[1].Skipped || res.Changes[0].Skipped {
		t.Fatalf("shift across all partitions = %+v, want p1 skipped", res.Changes)
	}
	if got := committedAt(0); got != 3 {
		t.Fatalf("after shift -2: p0 at %d, want 3", got)
	}
	if got := committedAt(1); got != -1 {
		t.Fatalf("partition filter ignored: p1 committed at %d", got)
	}

	// Absolute offsets are clamped to the partition range.
	if _, err := c.ResetOffsets(ctx, "billing", ResetOffsetsRequest{Topic: "orders", To: ResetOffset, Offset: ptr64(1000)}); err != nil {
		t.Fatal(err)
	}
	if p0, p1 := committedAt(0), committedAt(1); p0 != 10 || p1 != 4 {
		t.Fatalf("offset 1000 → p0=%d p1=%d, want 10 and 4 (clamped to end)", p0, p1)
	}

	// Timestamp in the future: nothing after it, so jump to the end; in the past: start.
	if _, err := c.ResetOffsets(ctx, "billing", ResetOffsetsRequest{Topic: "orders", To: ResetTimestamp, Timestamp: ptr64(0)}); err != nil {
		t.Fatal(err)
	}
	if got := committedAt(0); got != 0 {
		t.Fatalf("timestamp 0 → p0 at %d, want 0", got)
	}
	future := time.Now().Add(time.Hour).UnixMilli()
	if _, err := c.ResetOffsets(ctx, "billing", ResetOffsetsRequest{Topic: "orders", To: ResetTimestamp, Timestamp: &future}); err != nil {
		t.Fatal(err)
	}
	if got := committedAt(0); got != 10 {
		t.Fatalf("future timestamp → p0 at %d, want 10", got)
	}

	if _, err := c.ResetOffsets(ctx, "billing", ResetOffsetsRequest{Topic: "orders", To: "yesterday"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad strategy: err=%v, want ErrInvalid", err)
	}
}

// Kafka rejects commits from outside while a group has members; kafpeek must
// refuse up front with a clear message (dry runs still work).
func TestResetOffsetsRefusedForActiveGroup(t *testing.T) {
	c, cluster, ctx := newTestClient(t, 1, "orders")
	produceN(t, ctx, cluster, "orders", 0, 3)

	consumer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumerGroup("live"), kgo.ConsumeTopics("orders"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	for got := 0; got < 3; {
		fs := consumer.PollFetches(ctx)
		if err := fs.Err0(); err != nil {
			t.Fatal(err)
		}
		got += fs.NumRecords()
	}

	res, err := c.ResetOffsets(ctx, "live", ResetOffsetsRequest{Topic: "orders", To: ResetEarliest, DryRun: true})
	if err != nil || !res.Active {
		t.Fatalf("dry run on an active group = %+v, %v; want Active", res, err)
	}
	if _, err := c.ResetOffsets(ctx, "live", ResetOffsetsRequest{Topic: "orders", To: ResetEarliest}); !errors.Is(err, ErrConflict) {
		t.Fatalf("reset on an active group: err=%v, want ErrConflict", err)
	}
}

func TestResetOffsetsReadOnly(t *testing.T) {
	c := &Client{readOnly: true}
	if _, err := c.ResetOffsets(context.Background(), "g", ResetOffsetsRequest{Topic: "t", To: ResetLatest}); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("err=%v, want ErrReadOnly", err)
	}
}
