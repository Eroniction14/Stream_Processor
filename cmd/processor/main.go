package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/Eroniction14/Stream_Processor/config"
	"github.com/Eroniction14/Stream_Processor/internal/consumer"
	"github.com/Eroniction14/Stream_Processor/internal/health"
	"github.com/Eroniction14/Stream_Processor/internal/metrics"
	"github.com/Eroniction14/Stream_Processor/internal/pipeline"
	"github.com/Eroniction14/Stream_Processor/internal/producer"
	"github.com/Eroniction14/Stream_Processor/internal/state"
)

func main() {
	// Structured logging
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	// Load config
	cfg := config.Default()
	if path := os.Getenv("CONFIG_PATH"); path != "" {
		loaded, err := config.Load(path)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to load config")
		}
		cfg = loaded
	}

	// Initialize components
	m := metrics.New("stream_processor")
	m.ServeHTTP(cfg.Metrics.Port)

	hc := health.New()
	hc.ServeHTTP(cfg.Health.Port)

	store := state.NewMemStore()

	// Build pipeline
	sink := producer.NewKafkaSink(
		cfg.Kafka.Brokers,
		cfg.Kafka.OutputTopic,
		cfg.Pipeline.BatchSize,
		m,
	)

	aggregator := pipeline.NewAggregator(cfg.Pipeline.WindowSize, sink)

	p := pipeline.NewPipeline(
		sink,
		&pipeline.Deserializer{},
		pipeline.NewRouter(pipeline.EventPageView, pipeline.EventUserAction, pipeline.EventTransaction),
		pipeline.NewEnricher(store),
		aggregator,
	)

	// Context with graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start aggregator flusher
	go aggregator.RunFlusher(ctx, cfg.Pipeline.FlushInterval)

	// Start consumer
	c := consumer.New(consumer.ConsumerConfig{
		Brokers:      cfg.Kafka.Brokers,
		GroupID:      cfg.Kafka.ConsumerGroup,
		Topics:       cfg.Kafka.InputTopics,
		DLQTopic:     cfg.Kafka.DLQTopic,
		MaxRetries:   cfg.Kafka.MaxRetries,
		RetryBackoff: cfg.Kafka.RetryBackoff,
	}, p, m)

	// Mark as ready
	hc.SetReady(true)
	log.Info().Msg("stream processor started")

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Run consumer in a goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- c.Run(ctx)
	}()

	// Wait for signal or error
	select {
	case sig := <-sigCh:
		log.Info().Str("signal", sig.String()).Msg("received shutdown signal")
	case err := <-errCh:
		log.Error().Err(err).Msg("consumer error")
	}

	// Graceful shutdown with timeout
	hc.SetReady(false)
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Final flush
	if err := sink.Flush(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("error during final flush")
	}
	if err := sink.Close(); err != nil {
		log.Error().Err(err).Msg("error closing sink")
	}

	log.Info().Msg("stream processor stopped gracefully")
}
