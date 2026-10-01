# Production-Ready Stream Processor

A production-grade event stream processing pipeline built with Go and Apache Kafka, demonstrating real-world patterns: windowed aggregation, dead-letter queues, retry with exponential backoff, graceful shutdown, and full Prometheus/Grafana observability.

## Motivation

Modern distributed systems at companies like Amazon, Netflix, and Airbnb rely on event-driven architectures to process millions of events per second. This project implements the core patterns needed to build reliable stream processors, going beyond toy examples to address real production concerns.

## Features

- Multi-stage processing pipeline with composable `Stage` interface
- Windowed aggregation (tumbling windows, configurable size)
- Dead-letter queue for poison messages with error metadata headers
- Retry with exponential backoff + jitter
- Graceful shutdown (SIGINT/SIGTERM, drain in-flight, final flush)
- Prometheus metrics (throughput, latency histograms, error rates)
- Health checks (liveness + readiness HTTP endpoints)
- Dockerized end-to-end deployment with Kafka, Prometheus, and Grafana
- Event generator for demo and load testing
- Integration tests using Testcontainers (real Kafka broker in Docker)

## Architecture

**Components:**
- **Kafka** — Input topic (`user-events`), output topic (`aggregated-stats`), dead-letter topic (`dead-letter`)
- **Consumer Group** — Reads from Kafka with retry + exponential backoff
- **Pipeline** — Deserializer → Router → Enricher → Aggregator → Kafka Sink
- **State Store** — Thread-safe in-memory store for enrichment lookups
- **DLQ Handler** — Routes poison messages to dead-letter topic with error metadata
- **Observability** — Prometheus metrics, Grafana dashboards, health/readiness endpoints

### Processing Pipeline

```
Event --> Deserialize --> Route --> Enrich --> Aggregate --> Emit
                           |                                  |
                      [filtered]                    [output topic]
            |
       [invalid: DLQ]
```

Each stage implements the `Stage` interface, making the pipeline fully composable and testable.

### Production Patterns

| Pattern | Implementation |
|---------|---------------|
| Retry | Exponential backoff with jitter, configurable max attempts |
| DLQ | Failed messages routed to dead-letter topic with error metadata |
| Graceful Shutdown | Context cancellation, drain in-flight messages, final flush |
| Observability | Prometheus counters/histograms, structured logging, health endpoints |
| Windowed Aggregation | Tumbling windows with periodic flush of expired windows |
| State Management | Thread-safe in-memory store with RWMutex |

## Quick Start

```bash
# Clone repository
git clone https://github.com/Eroniction14/stream-processor
cd stream-processor

# Start everything
cd deployments
docker compose up --build

# View Grafana dashboard
# Open http://localhost:3000 (login: admin / admin)

# Check health
curl http://localhost:8081/healthz
curl http://localhost:8081/readyz

# View raw Prometheus metrics
curl http://localhost:9090/metrics
```

## Performance

| Metric | Value |
|--------|-------|
| Throughput | ~10,000 events/sec (single instance) |
| Processing Latency (p50) | < 1ms |
| Processing Latency (p99) | < 5ms |
| Graceful Shutdown | < 5s (drain + flush) |
| Recovery Time | < 10s (consumer group rebalance) |

## Project Structure

```
stream-processor/
├── cmd/
│   ├── processor/          # Main application entrypoint
│   └── generator/          # Event generator for testing
├── internal/
│   ├── pipeline/           # Core pipeline: stages, aggregator, events
│   ├── consumer/           # Kafka consumer with retry + DLQ
│   ├── producer/           # Kafka producer (batched sink)
│   ├── state/              # In-memory state store
│   ├── metrics/            # Prometheus instrumentation
│   └── health/             # Liveness + readiness endpoints
├── config/                 # YAML configuration
├── deployments/            # Docker, Docker Compose, Prometheus config
├── tests/
│   └── integration/        # Integration tests with Testcontainers
├── go.mod
└── README.md
```

## Testing

```bash
# Run all tests (unit + integration)
go test -v ./... -timeout 180s

# Run only unit tests (fast, no Docker needed)
go test -short ./...

# Run only integration tests
go test -v ./tests/integration/ -timeout 120s

# Run with race detector
go test -race ./...
```

### Test Coverage

| Test Suite | Tests | What It Covers |
|------------|-------|---------------|
| Unit (pipeline) | 10 | Aggregator windows, router filtering, deserializer validation, pipeline error propagation |
| Integration (E2E) | 1 | Produce → Kafka → pipeline → aggregated result in output topic |
| Integration (DLQ) | 1 | Poison messages (bad JSON, missing ID) routed to dead-letter queue with error headers |
| Integration (Health) | 1 | Liveness returns 200, readiness transitions from 503 → 200 |

## Key Design Decisions

**segmentio/kafka-go over confluent-kafka-go** — Pure Go, no CGO dependency, simpler cross-compilation and CI, easier debugging. Trade-off: slightly lower raw throughput, but sufficient for most use cases.

**Tumbling windows over sliding** — Simpler state management, no overlap, sufficient for most aggregation use cases. Sliding windows can be added as an alternative Stage implementation.

**Synchronous producer writes** — Trades throughput for delivery guarantees. Async mode available via config for higher throughput use cases.

**Composable Stage interface** — Each processing step is independently testable. New stages (deduplication, rate limiting, schema validation) can be added without modifying existing code.

**In-memory state over RocksDB** — Keeps the project focused. Production systems would use embedded storage with WAL for crash recovery.

## Interview Discussion Points

- **At-least-once vs exactly-once**: Current design is at-least-once. Discuss how to achieve exactly-once with idempotent producers + transactional API.
- **Backpressure**: What happens when processing is slower than consumption? Options: buffered channels, rate limiting, pause/resume consumer.
- **Horizontal scaling**: Consumer group partitioning. Max parallelism = number of partitions.
- **Late arrivals**: Current tumbling windows drop late events. Discuss watermarks and allowed lateness (Flink-style).
- **State recovery**: Current in-memory state is lost on restart. Discuss changelog topics, RocksDB, or external state stores.

## Tech Stack

Go 1.24 | Apache Kafka | Prometheus | Grafana | Docker | Testcontainers

## License

MIT

## Author

Eroniction Presley
