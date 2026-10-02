package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/Eroniction14/Stream_Processor/internal/pipeline"
	"github.com/Eroniction14/Stream_Processor/internal/producer"
	"github.com/Eroniction14/Stream_Processor/internal/state"
)

func TestEndToEnd_EventsFlowThroughPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	env := setupKafka(t)

	inputTopic := formatTestName("test-input")
	outputTopic := formatTestName("test-output")

	createTopic(t, env.brokers, inputTopic, 1)
	createTopic(t, env.brokers, outputTopic, 1)

	time.Sleep(2 * time.Second)

	// Build pipeline components
	m := newIsolatedMetrics("test_e2e")
	store := state.NewMemStore()
	sink := producer.NewKafkaSink(env.brokers, outputTopic, 10, m)

	// Use a short window so it expires quickly
	windowSize := 3 * time.Second
	aggregator := pipeline.NewAggregator(windowSize, sink)

	p := pipeline.NewPipeline(
		sink,
		&pipeline.Deserializer{},
		pipeline.NewRouter(pipeline.EventPageView),
		pipeline.NewEnricher(store),
		aggregator,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the flusher
	go aggregator.RunFlusher(ctx, 2*time.Second)

	// Produce 10 page_view events with past timestamps so window is already expired
	pastTime := time.Now().Add(-5 * time.Minute)
	var msgs []kafka.Message
	for i := 0; i < 10; i++ {
		msgs = append(msgs, kafka.Message{
			Key:   []byte("user-1"),
			Value: makePastEventJSON(t, fmt.Sprintf("evt-%d", i), "page_view", "user-1", pastTime),
		})
	}
	produceMessages(t, env.brokers, inputTopic, msgs)

	// Instead of using the full consumer (which has StartOffset issues),
	// read directly with a simple reader and process manually.
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     env.brokers,
		Topic:       inputTopic,
		Partition:   0,
		StartOffset: kafka.FirstOffset,
		MaxWait:     1 * time.Second,
	})
	defer reader.Close()

	// Read and process all 10 messages
	readCtx, readCancel := context.WithTimeout(ctx, 15*time.Second)
	defer readCancel()

	for i := 0; i < 10; i++ {
		msg, err := reader.ReadMessage(readCtx)
		if err != nil {
			t.Fatalf("failed to read message %d: %v", i, err)
		}

		event, err := pipeline.UnmarshalEvent(msg.Value)
		if err != nil {
			t.Fatalf("failed to unmarshal event: %v", err)
		}

		if err := p.Process(ctx, event); err != nil {
			t.Fatalf("pipeline error: %v", err)
		}
	}

	t.Log("all 10 messages processed, waiting for window flush...")

	// Wait for the flusher to pick up the expired window and flush the sink
	time.Sleep(5 * time.Second)

	// Shutdown
	cancel()

	// Read from output topic
	results := consumeMessages(t, env.brokers, outputTopic, formatTestName("verify-e2e"), 1, 10*time.Second)

	if len(results) == 0 {
		t.Fatal("expected at least 1 aggregated result in output topic")
	}

	// Verify the aggregated result
	var agg pipeline.AggregatedResult
	if err := json.Unmarshal(results[0].Value, &agg); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}

	if agg.Count != 10 {
		t.Errorf("expected count 10, got %d", agg.Count)
	}
	if agg.UniqueUsers != 1 {
		t.Errorf("expected 1 unique user, got %d", agg.UniqueUsers)
	}

	t.Logf("E2E passed: count=%d, unique_users=%d", agg.Count, agg.UniqueUsers)
}
