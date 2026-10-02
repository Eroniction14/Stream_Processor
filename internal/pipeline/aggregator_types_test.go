package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"
)

// recordingSink captures results written by the aggregator.
type recordingSink struct {
	mu      sync.Mutex
	results []*AggregatedResult
}

func (s *recordingSink) Write(_ context.Context, r *AggregatedResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results = append(s.results, r)
	return nil
}

func (s *recordingSink) Flush(context.Context) error { return nil }

func (s *recordingSink) Close() error { return nil }

func (s *recordingSink) Name() string { return "recording" }

// Regression test: events of different types in the same window must produce
// one result per type, not one merged result labeled with the first type.
func TestAggregatorSplitsWindowsByEventType(t *testing.T) {
	sink := &recordingSink{}
	agg := NewAggregator(time.Minute, sink)
	ctx := context.Background()

	ts := time.Now().Add(-5 * time.Minute).Truncate(time.Minute).Add(10 * time.Second)
	events := []*Event{
		{ID: "1", Type: EventType("page_view"), UserID: "u1", Timestamp: ts},
		{ID: "2", Type: EventType("page_view"), UserID: "u2", Timestamp: ts},
		{ID: "3", Type: EventType("transaction"), UserID: "u1", Timestamp: ts},
		{ID: "4", Type: EventType("user_action"), UserID: "u3", Timestamp: ts},
	}
	for _, e := range events {
		if _, err := agg.Process(ctx, e); err != nil {
			t.Fatalf("process: %v", err)
		}
	}
	if err := agg.FlushExpired(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	got := map[EventType]int64{}
	for _, r := range sink.results {
		got[r.EventType] = r.Count
	}
	want := map[EventType]int64{"page_view": 2, "transaction": 1, "user_action": 1}
	if len(sink.results) != len(want) {
		t.Fatalf("expected %d results (one per type), got %d: %v", len(want), len(sink.results), got)
	}
	for typ, n := range want {
		if got[typ] != n {
			t.Errorf("%s: expected count %d, got %d", typ, n, got[typ])
		}
	}
}

// FlushAll must emit windows that have not ended yet (used on shutdown).
func TestAggregatorFlushAllEmitsInProgressWindows(t *testing.T) {
	sink := &recordingSink{}
	agg := NewAggregator(time.Minute, sink)
	ctx := context.Background()

	if _, err := agg.Process(ctx, &Event{ID: "1", Type: EventType("page_view"), UserID: "u1", Timestamp: time.Now()}); err != nil {
		t.Fatalf("process: %v", err)
	}
	if err := agg.FlushExpired(ctx); err != nil {
		t.Fatalf("flush expired: %v", err)
	}
	if len(sink.results) != 0 {
		t.Fatalf("in-progress window should not be emitted by FlushExpired, got %d results", len(sink.results))
	}
	if err := agg.FlushAll(ctx); err != nil {
		t.Fatalf("flush all: %v", err)
	}
	if len(sink.results) != 1 {
		t.Fatalf("FlushAll should emit the in-progress window, got %d results", len(sink.results))
	}
}
