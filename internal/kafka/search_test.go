package kafka

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/stolato/kafpeek/internal/config"
)

// A whole-topic search must read every partition to its end even when the
// tail of a partition is a transaction marker (never a deliverable message),
// and must report "finished" only when that is true.
func TestSearchAllReadsWholeTopicWithTransactionTail(t *testing.T) {
	const topic = "orders"
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, topic))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	brokers := cluster.ListenAddrs()

	// Plain records spread over all partitions.
	plain, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(topic))
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	const n = 300
	for i := range n {
		r := &kgo.Record{Value: fmt.Appendf(nil, "msg-%d", i), Partition: int32(i % 3)}
		if err := plain.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	// A committed transaction last on partition 1: its log ends in a marker.
	txn, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID("search-test"), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Close()
	if err := txn.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := txn.ProduceSync(ctx, &kgo.Record{Value: []byte("needle"), Partition: 1}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := txn.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}

	c, err := New(ctx, config.Kafka{ClusterName: "test", Brokers: brokers, Auth: config.AuthNone}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var found []string
	start := time.Now()
	res, err := c.Search(ctx, topic, SearchOptions{
		All:   true,
		Limit: 1000,
		Match: func(m *Message) bool { return m.Value != nil && *m.Value == "needle" },
	}, SearchCallbacks{OnMatch: func(m Message) error { found = append(found, *m.Value); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if res.Reason != ReasonFinished {
		t.Fatalf("reason = %q, want %q (position %d/%d)", res.Reason, ReasonFinished, res.Position, res.Total)
	}
	if res.Position != res.Total {
		t.Fatalf("position %d != total %d", res.Position, res.Total)
	}
	if res.Scanned != n+1 {
		t.Fatalf("scanned = %d, want %d (markers must not count as messages)", res.Scanned, n+1)
	}
	if len(found) != 1 {
		t.Fatalf("found %v, want the needle once", found)
	}
	// Ending on a marker must not cost a stall wait.
	if d := time.Since(start); d > searchStall/2 {
		t.Fatalf("search took %s; the transaction tail was not detected as the end", d)
	}
}

// A broker that goes quiet for longer than a single poll must not make the
// search report "finished" with records left unread.
func TestSearchAllSurvivesSlowBroker(t *testing.T) {
	const topic = "slow"
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, topic))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	brokers := cluster.ListenAddrs()

	prod, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	const n = 100
	for i := range n {
		v := fmt.Sprintf("msg-%d", i)
		if i == n-1 {
			v = "needle"
		}
		if err := prod.ProduceSync(ctx, &kgo.Record{Value: []byte(v), Partition: int32(i % 2)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	// Hold the first fetch well past searchIdle (but within searchStall).
	var once sync.Once
	cluster.ControlKey(kmsg.Fetch.Int16(), func(kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		once.Do(func() { cluster.SleepControl(func() { time.Sleep(searchIdle + 2*time.Second) }) })
		return nil, nil, false
	})

	c, err := New(ctx, config.Kafka{ClusterName: "test", Brokers: brokers, Auth: config.AuthNone}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	found := 0
	res, err := c.Search(ctx, topic, SearchOptions{
		All:   true,
		Limit: 1000,
		Match: func(m *Message) bool { return m.Value != nil && *m.Value == "needle" },
	}, SearchCallbacks{OnMatch: func(Message) error { found++; return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if res.Reason != ReasonFinished || res.Scanned != n || found != 1 {
		t.Fatalf("reason=%q scanned=%d found=%d position=%d/%d; want finished, %d scanned, needle found",
			res.Reason, res.Scanned, found, res.Position, res.Total, n)
	}
}
