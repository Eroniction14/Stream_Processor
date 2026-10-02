package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type Event struct {
	ID        string  `json:"id"`
	Type      string  `json:"type"`
	UserID    string  `json:"user_id"`
	Timestamp string  `json:"timestamp"`
	Payload   Payload `json:"payload"`
}

type Payload struct {
	Page     string  `json:"page,omitempty"`
	Action   string  `json:"action,omitempty"`
	Amount   float64 `json:"amount,omitempty"`
	Duration int     `json:"duration_ms,omitempty"`
}

var (
	eventTypes = []string{"page_view", "user_action", "transaction"}
	pages      = []string{"/home", "/products", "/cart", "/checkout", "/profile"}
	actions    = []string{"click", "scroll", "hover", "submit", "search"}
)

func main() {
	brokers := []string{"localhost:9092"}
	if b := os.Getenv("KAFKA_BROKERS"); b != "" {
		brokers = []string{b}
	}

	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    "user-events",
		Balancer: &kafka.Hash{},
		// kafka-go waits up to BatchTimeout (default 1s) to fill a batch before
		// sending. WriteMessages is synchronous, so with the default every call
		// blocked ~1s and the generator produced ~1 event/s instead of ~10.
		BatchTimeout: 10 * time.Millisecond,
	}
	defer writer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(100 * time.Millisecond) // ~10 events/sec
	defer ticker.Stop()

	fmt.Println("Event generator started. Press Ctrl+C to stop.")

	count := 0
	for {
		select {
		case <-sigCh:
			fmt.Printf("\nGenerated %d events\n", count)
			return
		case <-ticker.C:
			event := generateEvent(count)
			data, _ := json.Marshal(event)

			err := writer.WriteMessages(ctx, kafka.Message{
				Key:   []byte(event.UserID),
				Value: data,
			})
			if err != nil {
				fmt.Printf("write error: %v\n", err)
				continue
			}

			count++
			if count%100 == 0 {
				fmt.Printf("Generated %d events\n", count)
			}
		}
	}
}

func generateEvent(seq int) Event {
	t := eventTypes[rand.Intn(len(eventTypes))]
	e := Event{
		ID:        fmt.Sprintf("evt-%d-%d", time.Now().UnixNano(), seq),
		Type:      t,
		UserID:    fmt.Sprintf("user-%d", rand.Intn(100)),
		Timestamp: time.Now().Format(time.RFC3339Nano),
	}

	switch t {
	case "page_view":
		e.Payload.Page = pages[rand.Intn(len(pages))]
		e.Payload.Duration = 500 + rand.Intn(5000)
	case "user_action":
		e.Payload.Action = actions[rand.Intn(len(actions))]
		e.Payload.Page = pages[rand.Intn(len(pages))]
	case "transaction":
		e.Payload.Amount = float64(rand.Intn(10000)) / 100.0
	}

	return e
}
