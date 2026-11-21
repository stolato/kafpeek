package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// MaxProduceBytes caps key+value+headers of a message produced from the UI
// (MSK's default max.message.bytes is ~1 MB).
const MaxProduceBytes = 1 << 20

// ErrReadOnly is returned by write operations when KAFKA_READ_ONLY is set.
var ErrReadOnly = errors.New("kafpeek is running in read-only mode (KAFKA_READ_ONLY=true)")

// ErrInvalid wraps user input errors (bad partition, too large...).
var ErrInvalid = errors.New("invalid request")

type ProduceRequest struct {
	Key     *string  `json:"key"`   // nil = null key
	Value   *string  `json:"value"` // nil = null value (tombstone)
	Headers []Header `json:"headers"`
	// Partition, if set, pins the record to that partition. Otherwise the
	// partition is chosen exactly like the Java client: murmur2(key) for
	// keyed records, sticky/random for null keys.
	Partition *int32 `json:"partition"`
}

type ProduceResult struct {
	Topic     string    `json:"topic"`
	Partition int32     `json:"partition"`
	Offset    int64     `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
}

func (c *Client) ReadOnly() bool { return c.readOnly }

// Produce writes a single record synchronously and returns where it landed.
func (c *Client) Produce(ctx context.Context, topic string, req ProduceRequest) (ProduceResult, error) {
	if c.readOnly {
		return ProduceResult{}, ErrReadOnly
	}
	size := 0
	if req.Key != nil {
		size += len(*req.Key)
	}
	if req.Value != nil {
		size += len(*req.Value)
	}
	for _, h := range req.Headers {
		if h.Key == "" {
			return ProduceResult{}, fmt.Errorf("%w: header keys cannot be empty", ErrInvalid)
		}
		size += len(h.Key) + len(h.Value)
	}
	if size > MaxProduceBytes {
		return ProduceResult{}, fmt.Errorf("%w: message is %d bytes, max is %d", ErrInvalid, size, MaxProduceBytes)
	}

	// Validate the topic and the requested partition up front, for a clear error.
	topics, err := c.adm.ListTopicsWithInternal(ctx, topic)
	if err != nil {
		return ProduceResult{}, err
	}
	td, ok := topics[topic]
	if !ok || td.Err != nil {
		return ProduceResult{}, fmt.Errorf("topic %q: %w", topic, ErrNotFound)
	}
	if req.Partition != nil && (*req.Partition < 0 || int(*req.Partition) >= len(td.Partitions)) {
		return ProduceResult{}, fmt.Errorf("%w: partition %d does not exist (topic has %d)", ErrInvalid, *req.Partition, len(td.Partitions))
	}

	prod, err := c.producer()
	if err != nil {
		return ProduceResult{}, err
	}

	r := &kgo.Record{Topic: topic, Context: ctx}
	if req.Key != nil {
		r.Key = []byte(*req.Key)
	}
	if req.Value != nil {
		r.Value = []byte(*req.Value)
	}
	for _, h := range req.Headers {
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: h.Key, Value: []byte(h.Value)})
	}
	if req.Partition != nil {
		r.Partition = *req.Partition
		r.Context = context.WithValue(ctx, manualPartitionKey{}, true)
	}

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	out, err := prod.ProduceSync(ctx, r).First()
	if err != nil {
		return ProduceResult{}, fmt.Errorf("producing to %q: %w", topic, err)
	}
	return ProduceResult{Topic: topic, Partition: out.Partition, Offset: out.Offset, Timestamp: out.Timestamp}, nil
}

// producer lazily creates a dedicated producer client (the shared admin
// client is never used for writes, and read-only mode never creates one).
func (c *Client) producer() (*kgo.Client, error) {
	c.prodOnce.Do(func() {
		opts := append(append([]kgo.Opt{}, c.base...),
			kgo.RecordPartitioner(uiPartitioner{def: kgo.StickyKeyPartitioner(nil)}),
			// Idempotent writes need the extra IAM permission
			// kafka-cluster:WriteDataIdempotently; single UI sends don't need it.
			kgo.DisableIdempotentWrite(),
			kgo.RequiredAcks(kgo.AllISRAcks()),
			kgo.ProducerLinger(0),
		)
		c.prod, c.prodErr = kgo.NewClient(opts...)
	})
	return c.prod, c.prodErr
}

type manualPartitionKey struct{}

// uiPartitioner honors an explicit partition (marked via the record context)
// and otherwise delegates to Kafka's default key hashing.
type uiPartitioner struct{ def kgo.Partitioner }

func (p uiPartitioner) ForTopic(topic string) kgo.TopicPartitioner {
	return uiTopicPartitioner{def: p.def.ForTopic(topic)}
}

type uiTopicPartitioner struct{ def kgo.TopicPartitioner }

func manual(r *kgo.Record) bool {
	return r.Context != nil && r.Context.Value(manualPartitionKey{}) != nil
}

// OnNewBatch keeps the default partitioner's "sticky" behavior for null keys.
func (p uiTopicPartitioner) OnNewBatch() {
	if nb, ok := p.def.(kgo.TopicPartitionerOnNewBatch); ok {
		nb.OnNewBatch()
	}
}

func (p uiTopicPartitioner) RequiresConsistency(r *kgo.Record) bool {
	return manual(r) || p.def.RequiresConsistency(r)
}

func (p uiTopicPartitioner) Partition(r *kgo.Record, n int) int {
	if manual(r) {
		return int(r.Partition)
	}
	return p.def.Partition(r, n)
}
