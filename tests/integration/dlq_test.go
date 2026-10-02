package integration

import (
	"context"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/Eroniction14/Stream_Processor/internal/consumer"
	"github.com/Eroniction14/Stream_Processor/internal/pipeline"
	"github.com/Eroniction14/Stream_Processor/internal/producer"
	"github.com/Eroniction14/Stream_Processor/internal/state"
)

func TestDLQ_PoisonMessagesRouted(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	env := setupKafka(t)

	inputTopic := formatTestName("dlq-input")
	outputTopic := formatTestName("dlq-output")
	dlqTopic := formatTestName("dlq-dead")

	createTopic(t, env.brokers, inputTopic, 1)
	createTopic(t, env.brokers, outputTopic, 1)
	createTopic(t, env.brokers, dlqTopic, 1)

	time.Sleep(2 * time.Second)

	m := newIsolatedMetrics("test_dlq")
	store := state.NewMemStore()
	sink := producer.NewKafkaSink(env.brokers, outputTopic, 10, m)
	aggregator := pipeline.NewAggregator(5*time.Second, sink)

	p := pipeline.NewPipeline(
		sink,
		&pipeline.Deserializer{},
		pipeline.NewRouter(pipeline.EventPageView),
		pipeline.NewEnricher(store),
		aggregator,
	)

	// Use FirstOffset so the consumer picks up messages we produce before it starts
	c := consumer.New(consumer.ConsumerConfig{
		Brokers:      env.brokers,
		GroupID:      formatTestName("test-group-dlq"),
		Topics:       []string{inputTopic},
		DLQTopic:     dlqTopic,
		MaxRetries:   1,
		RetryBackoff: 10 * time.Millisecond,
	}, p, m)

	// Start the consumer first so it subscribes to the partition
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go c.Run(ctx)

	// Wait for consumer group to stabilize
	time.Sleep(5 * time.Second)

	// Now produce poison messages (consumer will see them)
	poisonMsgs := []kafka.Message{
		{Key: []byte("bad-1"), Value: []byte("not-json-at-all!!!")},
		{Key: []byte("bad-2"), Value: []byte(`{"type":"page_view","user_id":"u1"}`)}, // missing "id"
	}
	produceMessages(t, env.brokers, inputTopic, poisonMsgs)

	// Wait for processing + retries
	time.Sleep(10 * time.Second)
	cancel()

	// Read from DLQ
	dlqMsgs := consumeMessages(t, env.brokers, dlqTopic, formatTestName("verify-dlq"), 2, 10*time.Second)

	if len(dlqMsgs) < 2 {
		t.Fatalf("expected 2 messages in DLQ, got %d", len(dlqMsgs))
	}

	// Verify DLQ messages carry error metadata headers
	for _, msg := range dlqMsgs {
		hasError := false
		for _, h := range msg.Headers {
			if h.Key == "error" {
				hasError = true
				t.Logf("DLQ message key=%s, error=%s", string(msg.Key), string(h.Value))
			}
		}
		if !hasError {
			t.Errorf("DLQ message missing 'error' header: key=%s", string(msg.Key))
		}
	}

	t.Log("DLQ test passed: poison messages routed with error headers")
}
