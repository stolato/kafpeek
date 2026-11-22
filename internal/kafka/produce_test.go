package kafka

import (
	"context"
	"errors"
	"testing"
)

// Read-only mode must refuse writes before touching Kafka at all.
func TestProduceRefusedInReadOnlyMode(t *testing.T) {
	c := &Client{readOnly: true}
	_, err := c.Produce(context.Background(), "orders", ProduceRequest{})
	if !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Produce in read-only mode: err=%v, want ErrReadOnly", err)
	}
}
