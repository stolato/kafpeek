package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/stolato/kafpeek/internal/config"
)

// newTestClient starts a fake single-broker cluster seeded with topics.
func newTestClient(t *testing.T, partitions int32, topics ...string) (*Client, *kfake.Cluster, context.Context) {
	t.Helper()
	opts := []kfake.Opt{kfake.NumBrokers(1)}
	if len(topics) > 0 {
		opts = append(opts, kfake.SeedTopics(partitions, topics...))
	}
	cluster, err := kfake.NewCluster(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	c, err := New(ctx, config.Kafka{ClusterName: "test", Brokers: cluster.ListenAddrs(), Auth: config.AuthNone},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c, cluster, ctx
}

func produceN(t *testing.T, ctx context.Context, cluster *kfake.Cluster, topic string, partition int32, n int) {
	t.Helper()
	prod, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	for i := 0; i < n; i++ {
		if err := prod.ProduceSync(ctx, &kgo.Record{Value: []byte("x"), Partition: partition}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTopicWritesRefusedInReadOnlyMode(t *testing.T) {
	c := &Client{readOnly: true}
	ctx := context.Background()
	if err := c.DeleteTopic(ctx, "t"); !errors.Is(err, ErrReadOnly) {
		t.Errorf("DeleteTopic: %v", err)
	}
	if err := c.SetPartitions(ctx, "t", 5); !errors.Is(err, ErrReadOnly) {
		t.Errorf("SetPartitions: %v", err)
	}
	if err := c.AlterTopicConfigs(ctx, "t", TopicConfigChange{Reset: []string{"retention.ms"}}); !errors.Is(err, ErrReadOnly) {
		t.Errorf("AlterTopicConfigs: %v", err)
	}
	if _, err := c.PurgeTopic(ctx, "t", nil); !errors.Is(err, ErrReadOnly) {
		t.Errorf("PurgeTopic: %v", err)
	}
}

func TestManageTopic(t *testing.T) {
	c, cluster, ctx := newTestClient(t, 2, "orders", "_schemas")

	// Partitions only grow.
	if err := c.SetPartitions(ctx, "orders", 2); !errors.Is(err, ErrInvalid) {
		t.Fatalf("SetPartitions to the same count: err=%v, want ErrInvalid", err)
	}
	if err := c.SetPartitions(ctx, "orders", 5); err != nil {
		t.Fatal(err)
	}
	if d, err := c.Topic(ctx, "orders"); err != nil || d.Partitions != 5 {
		t.Fatalf("after SetPartitions: partitions=%d err=%v, want 5", d.Partitions, err)
	}

	// Configs: set, then reset.
	if err := c.AlterTopicConfigs(ctx, "orders", TopicConfigChange{Set: map[string]string{"retention.ms": "3600000"}}); err != nil {
		t.Fatal(err)
	}
	if v := configValue(t, c, ctx, "orders", "retention.ms"); v != "3600000" {
		t.Fatalf("retention.ms = %q after set, want 3600000", v)
	}
	if err := c.AlterTopicConfigs(ctx, "orders", TopicConfigChange{Reset: []string{"retention.ms"}}); err != nil {
		t.Fatal(err)
	}
	if v := configValue(t, c, ctx, "orders", "retention.ms"); v == "3600000" {
		t.Fatal("retention.ms still overridden after reset")
	}

	// Purge one partition, then everything.
	produceN(t, ctx, cluster, "orders", 0, 7)
	produceN(t, ctx, cluster, "orders", 1, 3)
	res, err := c.PurgeTopic(ctx, "orders", []int32{1})
	if err != nil || res.Deleted != 3 || res.Partitions != 1 {
		t.Fatalf("PurgeTopic(p1) = %+v, %v; want 3 deleted from 1 partition", res, err)
	}
	if _, err := c.PurgeTopic(ctx, "orders", []int32{9}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("PurgeTopic(missing partition): err=%v, want ErrInvalid", err)
	}
	res, err = c.PurgeTopic(ctx, "orders", nil)
	if err != nil || res.Deleted != 7 {
		t.Fatalf("PurgeTopic(all) = %+v, %v; want 7 deleted", res, err)
	}
	if d, _ := c.Topic(ctx, "orders"); d.Messages != 0 {
		t.Fatalf("topic still has %d messages after purge", d.Messages)
	}

	// System topics are protected.
	if err := c.DeleteTopic(ctx, "_schemas"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("DeleteTopic(_schemas): err=%v, want ErrInvalid", err)
	}

	// Delete.
	if err := c.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteTopic(ctx, "orders"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleting a missing topic: err=%v, want ErrNotFound", err)
	}
}

func configValue(t *testing.T, c *Client, ctx context.Context, topic, name string) string {
	t.Helper()
	d, err := c.Topic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range d.Configs {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}
