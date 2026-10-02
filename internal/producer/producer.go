package producer

import (
	"context"
	"sync"

	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"

	"github.com/Eroniction14/Stream_Processor/internal/metrics"
	"github.com/Eroniction14/Stream_Processor/internal/pipeline"
)

type KafkaSink struct {
	writer  *kafka.Writer
	metrics *metrics.Metrics
	mu      sync.Mutex
	buffer  []kafka.Message
	maxBuf  int
}

func NewKafkaSink(brokers []string, topic string, batchSize int, m *metrics.Metrics) *KafkaSink {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  5,
		Async:        false,
	}

	return &KafkaSink{
		writer:  writer,
		metrics: m,
		buffer:  make([]kafka.Message, 0, batchSize),
		maxBuf:  batchSize,
	}
}

func (s *KafkaSink) Name() string { return "kafka-sink" }

func (s *KafkaSink) Write(ctx context.Context, result *pipeline.AggregatedResult) error {
	data, err := result.Marshal()
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(string(result.EventType)),
		Value: data,
	}

	s.mu.Lock()
	s.buffer = append(s.buffer, msg)
	shouldFlush := len(s.buffer) >= s.maxBuf
	s.mu.Unlock()

	if shouldFlush {
		return s.Flush(ctx)
	}
	return nil
}

func (s *KafkaSink) Flush(ctx context.Context) error {
	s.mu.Lock()
	if len(s.buffer) == 0 {
		s.mu.Unlock()
		return nil
	}
	batch := make([]kafka.Message, len(s.buffer))
	copy(batch, s.buffer)
	s.buffer = s.buffer[:0]
	s.mu.Unlock()

	if err := s.writer.WriteMessages(ctx, batch...); err != nil {
		log.Error().Err(err).Int("batch_size", len(batch)).Msg("failed to write batch")
		s.metrics.ErrorsTotal.Inc()
		return err
	}

	s.metrics.MessagesEmitted.Add(float64(len(batch)))
	return nil
}

func (s *KafkaSink) Close() error {
	return s.writer.Close()
}
