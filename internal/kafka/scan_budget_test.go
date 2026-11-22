package kafka

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/stolato/kafpeek/internal/config"
)

// With 3 partitions where only one holds data, a scan of 100 newest records
// must read 100 records, not 100/3 (empty partitions must not use up a share).
func TestNewestScanBudgetIgnoresEmptyPartitions(t *testing.T) {
	const topic = "skewed"
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, topic))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	prod, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.DefaultProduceTopic(topic))
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	for i := 0; i < 300; i++ {
		r := &kgo.Record{Value: []byte("x"), Partition: 0}
		if err := prod.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	c, err := New(ctx, config.Kafka{ClusterName: "test", Brokers: cluster.ListenAddrs(), Auth: config.AuthNone},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	browse, err := c.Browse(ctx, topic, BrowseOptions{
		Limit: 100,
		Scan:  100,
		Match: func(*Message) bool { return true },
	})
	if err != nil {
		t.Fatal(err)
	}
	if browse.Scanned != 100 {
		t.Fatalf("Browse scanned %d records, want 100", browse.Scanned)
	}

	res, err := c.Search(ctx, topic, SearchOptions{Scan: 100, Limit: 1000}, SearchCallbacks{
		OnMatch: func(Message) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Total != 100 {
		t.Fatalf("Search planned %d offsets, want 100", res.Total)
	}
}
