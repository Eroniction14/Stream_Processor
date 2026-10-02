package consumer

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"

	"github.com/Eroniction14/Stream_Processor/internal/metrics"
	"github.com/Eroniction14/Stream_Processor/internal/pipeline"
)

type Consumer struct {
	reader     *kafka.Reader
	pipeline   *pipeline.Pipeline
	dlq        *kafka.Writer
	maxRetries int
	backoff    time.Duration
	metrics    *metrics.Metrics
}

type ConsumerConfig struct {
	Brokers      []string
	GroupID      string
	Topics       []string
	DLQTopic     string
	MaxRetries   int
	RetryBackoff time.Duration
}

func New(cfg ConsumerConfig, p *pipeline.Pipeline, m *metrics.Metrics) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		GroupID:        cfg.GroupID,
		GroupTopics:    cfg.Topics,
		MinBytes:       1e3,  // 1KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	})

	dlq := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.DLQTopic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  3,
	}

	return &Consumer{
		reader:     reader,
		pipeline:   p,
		dlq:        dlq,
		maxRetries: cfg.MaxRetries,
		backoff:    cfg.RetryBackoff,
		metrics:    m,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	log.Info().Msg("consumer started")

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("consumer shutting down")
			return c.close()
		default:
		}

		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return c.close()
			}
			log.Error().Err(err).Msg("error fetching message")
			c.metrics.ErrorsTotal.Inc()
			continue
		}

		c.metrics.MessagesReceived.Inc()
		start := time.Now()

		if err := c.processWithRetry(ctx, msg); err != nil {
			c.sendToDLQ(ctx, msg, err)
		}

		c.metrics.ProcessingLatency.Observe(time.Since(start).Seconds())

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			log.Error().Err(err).Msg("error committing offset")
		}
	}
}

func (c *Consumer) processWithRetry(ctx context.Context, msg kafka.Message) error {
	var lastErr error
	backoff := c.backoff

	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		event, err := pipeline.UnmarshalEvent(msg.Value)
		if err != nil {
			// Deserialization errors are not retryable
			return err
		}

		if err := c.pipeline.Process(ctx, event); err != nil {
			lastErr = err
			c.metrics.RetriesTotal.Inc()

			// Exponential backoff with jitter
			jitter := time.Duration(rand.Intn(50)) * time.Millisecond
			time.Sleep(backoff + jitter)
			backoff *= 2
			continue
		}

		c.metrics.MessagesProcessed.Inc()
		return nil
	}

	return lastErr
}

func (c *Consumer) sendToDLQ(ctx context.Context, msg kafka.Message, processErr error) {
	dlqMsg := kafka.Message{
		Key:   msg.Key,
		Value: msg.Value,
		Headers: []kafka.Header{
			{Key: "original-topic", Value: []byte(msg.Topic)},
			{Key: "original-partition", Value: []byte(fmt.Sprintf("%d", msg.Partition))},
			{Key: "error", Value: []byte(processErr.Error())},
			{Key: "failed-at", Value: []byte(time.Now().Format(time.RFC3339))},
		},
	}

	if err := c.dlq.WriteMessages(ctx, dlqMsg); err != nil {
		log.Error().Err(err).Msg("failed to write to DLQ")
	} else {
		c.metrics.DLQMessages.Inc()
		log.Warn().
			Str("error", processErr.Error()).
			Str("topic", msg.Topic).
			Msg("message sent to DLQ")
	}
}

func (c *Consumer) close() error {
	if err := c.reader.Close(); err != nil {
		log.Error().Err(err).Msg("error closing reader")
	}
	if err := c.dlq.Close(); err != nil {
		log.Error().Err(err).Msg("error closing DLQ writer")
	}
	return nil
}
