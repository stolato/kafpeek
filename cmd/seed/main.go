// Command seed creates demo topics on the LOCAL Kafka and keeps producing
// fake events, so the live tail has something to show. It also runs a
// consumer group that lags behind on purpose.
//
//	go run ./cmd/seed            (defaults to localhost:9092)
//
// Never point this at a real cluster.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	brokers := os.Getenv("SEED_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers), kgo.DefaultProduceTopic("orders"))
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()

	adm := kadm.NewClient(cl)
	for _, t := range []string{"orders", "payments", "audit.logs"} {
		if _, err := adm.CreateTopic(ctx, 3, 1, nil, t); err != nil {
			log.Printf("create %s: %v (ok if it already exists)", t, err)
		}
	}

	go slowConsumer(ctx, brokers)

	statuses := []string{"CREATED", "PAID", "SHIPPED", "ERROR"}
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	log.Printf("producing to %s (Ctrl+C to stop)", brokers)
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		id := fmt.Sprintf("order-%d", rand.IntN(500))
		order, _ := json.Marshal(map[string]any{
			"id": id, "status": statuses[rand.IntN(len(statuses))],
			"amount": rand.Float64() * 1000, "seq": i, "at": time.Now(),
		})
		cl.Produce(ctx, &kgo.Record{
			Key: []byte(id), Value: order,
			Headers: []kgo.RecordHeader{{Key: "source", Value: []byte("seed")}},
		}, nil)
		if i%3 == 0 {
			cl.Produce(ctx, &kgo.Record{Topic: "payments", Key: []byte(id), Value: order}, nil)
		}
		if i%10 == 0 {
			cl.Produce(ctx, &kgo.Record{Topic: "audit.logs", Value: []byte(fmt.Sprintf("tick %d at %s", i, time.Now().Format(time.RFC3339)))}, nil)
		}
	}
}

// slowConsumer commits offsets slowly so the UI shows non-zero lag.
func slowConsumer(ctx context.Context, brokers string) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers), kgo.ConsumerGroup("billing-service"),
		kgo.ConsumeTopics("orders"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		log.Print(err)
		return
	}
	defer cl.Close()
	for ctx.Err() == nil {
		cl.PollRecords(ctx, 1)
		time.Sleep(500 * time.Millisecond)
	}
}
