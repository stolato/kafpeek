package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/stolato/kafpeek/internal/config"
)

func ptr[T any](v T) *T { return &v }

// Read-only mode must refuse to create topics before touching Kafka at all.
func TestCreateTopicRefusedInReadOnlyMode(t *testing.T) {
	c := &Client{readOnly: true}
	_, err := c.CreateTopic(context.Background(), CreateTopicRequest{Name: "orders"})
	if !errors.Is(err, ErrReadOnly) {
		t.Fatalf("CreateTopic in read-only mode: err=%v, want ErrReadOnly", err)
	}
}

func TestCreateTopicValidation(t *testing.T) {
	bad := map[string]CreateTopicRequest{
		"empty name":      {Name: ""},
		"dot":             {Name: "."},
		"dotdot":          {Name: ".."},
		"space":           {Name: "my topic"},
		"slash":           {Name: "a/b"},
		"too long":        {Name: strings.Repeat("a", MaxTopicNameLen+1)},
		"zero partitions": {Name: "ok", Partitions: ptr[int32](0)},
		"too many parts":  {Name: "ok", Partitions: ptr[int32](MaxCreatePartitions + 1)},
		"zero rf":         {Name: "ok", ReplicationFactor: ptr[int16](0)},
		"empty config":    {Name: "ok", Configs: map[string]string{" ": "x"}},
	}
	c := &Client{} // validation must fail before the (nil) admin client is used
	for name, req := range bad {
		if _, err := c.CreateTopic(context.Background(), req); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err=%v, want ErrInvalid", name, err)
		}
	}
	for _, ok := range []string{"orders", "orders.v2", "my_topic-1", strings.Repeat("a", MaxTopicNameLen)} {
		if err := ValidateTopicName(ok); err != nil {
			t.Errorf("ValidateTopicName(%q) = %v, want nil", ok, err)
		}
	}
}

func TestCreateTopic(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c, err := New(ctx, config.Kafka{ClusterName: "test", Brokers: cluster.ListenAddrs(), Auth: config.AuthNone},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	res, err := c.CreateTopic(ctx, CreateTopicRequest{
		Name:       "created",
		Partitions: ptr[int32](4),
		Configs:    map[string]string{"cleanup.policy": "compact"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Name != "created" || res.Partitions != 4 {
		t.Fatalf("CreateTopic result = %+v, want name=created partitions=4", res)
	}

	detail, err := c.Topic(ctx, "created")
	if err != nil {
		t.Fatal(err)
	}
	if detail.Partitions != 4 {
		t.Fatalf("topic has %d partitions, want 4", detail.Partitions)
	}

	_, err = c.CreateTopic(ctx, CreateTopicRequest{Name: "created"})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("creating an existing topic: err=%v, want ErrConflict", err)
	}
}
