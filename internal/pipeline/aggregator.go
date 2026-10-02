package pipeline

import (
	"context"
	"sync"
	"time"
)

// Window represents a time window for aggregation.
type Window struct {
	Start time.Time
	End   time.Time
}

func (w Window) Contains(t time.Time) bool {
	return !t.Before(w.Start) && t.Before(w.End)
}

// windowKey identifies one aggregate: a time window for one event type.
// Keying by window alone (the previous behavior) merged every event type in
// a minute into a single result labeled with whichever type arrived first.
type windowKey struct {
	Window    Window
	EventType EventType
}

// windowState holds the mutable state for a single window and event type.
type windowState struct {
	count       int64
	uniqueUsers map[string]bool
	totalAmount float64
	totalDurMs  int64
}

// Aggregator performs windowed aggregation of events, per event type.
type Aggregator struct {
	mu         sync.Mutex
	windowSize time.Duration
	windows    map[windowKey]*windowState
	sink       Sink
}

func NewAggregator(windowSize time.Duration, sink Sink) *Aggregator {
	return &Aggregator{
		windowSize: windowSize,
		windows:    make(map[windowKey]*windowState),
		sink:       sink,
	}
}

func (a *Aggregator) Name() string { return "aggregator" }

func (a *Aggregator) Process(ctx context.Context, event *Event) (*Event, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	key := windowKey{Window: a.windowFor(event.Timestamp), EventType: event.Type}
	state, ok := a.windows[key]
	if !ok {
		state = &windowState{uniqueUsers: make(map[string]bool)}
		a.windows[key] = state
	}

	state.count++
	state.uniqueUsers[event.UserID] = true
	state.totalAmount += event.Payload.Amount
	state.totalDurMs += int64(event.Payload.Duration)

	return event, nil
}

func (a *Aggregator) windowFor(t time.Time) Window {
	start := t.Truncate(a.windowSize)
	return Window{Start: start, End: start.Add(a.windowSize)}
}

// FlushExpired emits completed windows to the sink and removes them.
func (a *Aggregator) FlushExpired(ctx context.Context) error {
	now := time.Now()
	return a.flush(ctx, func(w Window) bool { return now.After(w.End) })
}

// FlushAll emits every window, including ones still in progress. It is used
// on shutdown: their offsets are already committed, so a window that isn't
// emitted now is lost. The partial result may be followed by another for the
// same window after restart (at-least-once); downstream consumers should
// treat a repeated window as a revision.
func (a *Aggregator) FlushAll(ctx context.Context) error {
	return a.flush(ctx, func(Window) bool { return true })
}

func (a *Aggregator) flush(ctx context.Context, ready func(Window) bool) error {
	a.mu.Lock()
	type item struct {
		k windowKey
		s *windowState
	}
	var toFlush []item
	for k, s := range a.windows {
		if ready(k.Window) {
			toFlush = append(toFlush, item{k, s})
			delete(a.windows, k)
		}
	}
	a.mu.Unlock()

	for _, it := range toFlush {
		avgDur := float64(0)
		if it.s.count > 0 {
			avgDur = float64(it.s.totalDurMs) / float64(it.s.count)
		}

		result := &AggregatedResult{
			WindowStart: it.k.Window.Start,
			WindowEnd:   it.k.Window.End,
			EventType:   it.k.EventType,
			Count:       it.s.count,
			UniqueUsers: int64(len(it.s.uniqueUsers)),
			TotalAmount: it.s.totalAmount,
			AvgDuration: avgDur,
		}

		if err := a.sink.Write(ctx, result); err != nil {
			return err
		}
	}

	return nil
}

// RunFlusher starts a background goroutine that periodically flushes expired windows.
// It also flushes the sink after each cycle to ensure buffered results are written.
func (a *Aggregator) RunFlusher(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Final flush on shutdown: emit in-progress windows too, or they are lost.
			_ = a.FlushAll(context.Background())
			_ = a.sink.Flush(context.Background())
			return
		case <-ticker.C:
			_ = a.FlushExpired(ctx)
			_ = a.sink.Flush(ctx)
		}
	}
}
