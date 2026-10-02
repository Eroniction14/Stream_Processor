// Command dashboard serves a live view of the stream processor.
//
// It is a separate Kafka client: it never touches the processor's code or
// consumer group. It reads three things and serves them to the browser:
//
//   - user-events      (own consumer group) -> animated "flow" of events
//   - aggregated-stats (own consumer group) -> live window log
//   - Prometheus HTTP API                   -> throughput, latency, DLQ, errors
//
// It can also write to user-events, so the page's buttons can inject a burst
// of valid events or a malformed one that should land in the dead-letter queue.
package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

//go:embed static
var staticFS embed.FS

const (
	inputTopic  = "user-events"
	outputTopic = "aggregated-stats"
	maxWindows  = 50
	maxBurst    = 5000

	// The page polls every 2s and keeps 15 minutes; /api/history backfills
	// the same shape from Prometheus so charts are full on first load.
	historyStep    = 2 * time.Second
	historySamples = 450
)

const (
	qThroughput = "sum(rate(stream_processor_messages_received_total[1m]))"
	qP50        = "histogram_quantile(0.50, sum by (le) (rate(stream_processor_processing_latency_seconds_bucket[1m])))"
	qP99        = "histogram_quantile(0.99, sum by (le) (rate(stream_processor_processing_latency_seconds_bucket[1m])))"
	qProcessed  = "sum(stream_processor_messages_processed_total)"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ---------------------------------------------------------------------------
// Server-Sent Events hub: fan out one message to every open browser tab.
// ---------------------------------------------------------------------------

type hub struct {
	mu      sync.Mutex
	clients map[chan []byte]struct{}
}

func newHub() *hub { return &hub{clients: make(map[chan []byte]struct{})} }

func (h *hub) add() chan []byte {
	c := make(chan []byte, 64)
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
	return c
}

func (h *hub) remove(c chan []byte) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
}

// broadcast never blocks: a slow tab just misses a frame.
func (h *hub) broadcast(event string, data []byte) {
	msg := []byte(fmt.Sprintf("event: %s\ndata: %s\n\n", event, data))
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		select {
		case c <- msg:
		default:
		}
	}
}

// ---------------------------------------------------------------------------
// In-memory state
// ---------------------------------------------------------------------------

// windowLog keeps the most recent aggregated windows for newly opened pages.
type windowLog struct {
	mu    sync.Mutex
	items []json.RawMessage
}

func (w *windowLog) add(m json.RawMessage) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.items = append(w.items, m)
	if len(w.items) > maxWindows {
		w.items = w.items[len(w.items)-maxWindows:]
	}
}

func (w *windowLog) snapshot() []json.RawMessage {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]json.RawMessage, len(w.items))
	copy(out, w.items)
	return out
}

// flowCounter tallies events seen on user-events between 100ms ticks, so the
// browser gets ~10 small messages per second instead of one per event.
type flowCounter struct {
	mu        sync.Mutex
	counts    map[string]int
	bad       int
	lastGenAt time.Time // last event produced by the real generator
}

func (f *flowCounter) record(eventType string, bad, fromGenerator bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if bad {
		f.bad++
	} else {
		f.counts[eventType]++
	}
	if fromGenerator {
		f.lastGenAt = time.Now()
	}
}

func (f *flowCounter) drain() (map[string]int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, b := f.counts, f.bad
	f.counts, f.bad = make(map[string]int), 0
	return c, b
}

func (f *flowCounter) generatorAlive() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return time.Since(f.lastGenAt) < 5*time.Second
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

type server struct {
	brokers      []string
	promURL      string
	processorURL string
	hub          *hub
	windows      *windowLog
	flow         *flowCounter
	writer       *kafka.Writer
	httpc        *http.Client
}

// consumeEvents mirrors the processor's validation (valid JSON with an id) so
// the animation can show bad events heading to the dead-letter queue.
func (s *server) consumeEvents(ctx context.Context) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     s.brokers,
		Topic:       inputTopic,
		GroupID:     "dashboard-flow",
		StartOffset: kafka.LastOffset,
		MaxWait:     250 * time.Millisecond,
	})
	defer r.Close()

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("flow reader: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		var e struct {
			ID   string `json:"id"`
			Type string `json:"type"`
		}
		if json.Unmarshal(m.Value, &e) != nil || e.ID == "" {
			s.flow.record("", true, false)
			continue
		}
		s.flow.record(e.Type, false, strings.HasPrefix(e.ID, "evt-"))
	}
}

func (s *server) consumeWindows(ctx context.Context) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     s.brokers,
		Topic:       outputTopic,
		GroupID:     "dashboard-windows",
		StartOffset: kafka.LastOffset,
		MaxWait:     250 * time.Millisecond,
	})
	defer r.Close()

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("window reader: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		if !json.Valid(m.Value) {
			continue
		}
		raw := json.RawMessage(append([]byte(nil), m.Value...))
		s.windows.add(raw)
		s.hub.broadcast("window", raw)
	}
}

func (s *server) pumpFlow(ctx context.Context) {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			counts, bad := s.flow.drain()
			if len(counts) == 0 && bad == 0 {
				continue
			}
			data, _ := json.Marshal(map[string]any{"counts": counts, "bad": bad})
			s.hub.broadcast("flow", data)
		}
	}
}

// ---------------------------------------------------------------------------
// HTTP handlers
// ---------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	c := s.hub.add()
	defer s.hub.remove(c)

	fmt.Fprint(w, ": connected\n\n")
	flusher.Flush()

	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-c:
			if _, err := w.Write(msg); err != nil {
				return
			}
			flusher.Flush()
		case <-ping.C:
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		}
	}
}

func (s *server) handleWindows(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.windows.snapshot())
}

// prom runs one instant query and returns nil when there's no usable value
// (no data yet, NaN from a zero-rate histogram, or Prometheus unreachable).
func (s *server) prom(ctx context.Context, q string) *float64 {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		s.promURL+"/api/v1/query?query="+url.QueryEscape(q), nil)
	if err != nil {
		return nil
	}
	resp, err := s.httpc.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	var pr struct {
		Data struct {
			Result []struct {
				Value []any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if json.NewDecoder(resp.Body).Decode(&pr) != nil ||
		len(pr.Data.Result) == 0 || len(pr.Data.Result[0].Value) < 2 {
		return nil
	}
	str, ok := pr.Data.Result[0].Value[1].(string)
	if !ok {
		return nil
	}
	f, err := strconv.ParseFloat(str, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return nil
	}
	return &f
}

func (s *server) kafkaUp(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	conn, err := kafka.DialContext(ctx, "tcp", s.brokers[0])
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func (s *server) processorUp(ctx context.Context) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.processorURL, nil)
	if err != nil {
		return false
	}
	resp, err := s.httpc.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func (s *server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	writeJSON(w, http.StatusOK, map[string]any{
		"throughput":     s.prom(ctx, qThroughput),
		"p50":            s.prom(ctx, qP50),
		"p99":            s.prom(ctx, qP99),
		"processed":      s.prom(ctx, qProcessed),
		"dlq":            s.prom(ctx, "sum(stream_processor_dlq_messages_total)"),
		"errors":         s.prom(ctx, "sum(stream_processor_errors_total)"),
		"active_windows": s.prom(ctx, "sum(stream_processor_active_windows)"),
		"status": map[string]bool{
			"kafka":     s.kafkaUp(ctx),
			"processor": s.processorUp(ctx),
			"generator": s.flow.generatorAlive(),
		},
	})
}

// promRange runs a range query and lays the result onto a fixed grid of
// historySamples slots, leaving nil where Prometheus has no value.
func (s *server) promRange(ctx context.Context, q string, start time.Time) []*float64 {
	out := make([]*float64, historySamples)
	end := start.Add(time.Duration(historySamples-1) * historyStep)
	v := url.Values{}
	v.Set("query", q)
	v.Set("start", strconv.FormatInt(start.Unix(), 10))
	v.Set("end", strconv.FormatInt(end.Unix(), 10))
	v.Set("step", strconv.Itoa(int(historyStep.Seconds()))+"s")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.promURL+"/api/v1/query_range?"+v.Encode(), nil)
	if err != nil {
		return out
	}
	resp, err := s.httpc.Do(req)
	if err != nil {
		return out
	}
	defer resp.Body.Close()

	var pr struct {
		Data struct {
			Result []struct {
				Values [][]any `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if json.NewDecoder(resp.Body).Decode(&pr) != nil || len(pr.Data.Result) == 0 {
		return out
	}
	for _, pair := range pr.Data.Result[0].Values {
		if len(pair) < 2 {
			continue
		}
		ts, ok1 := pair[0].(float64)
		str, ok2 := pair[1].(string)
		if !ok1 || !ok2 {
			continue
		}
		f, err := strconv.ParseFloat(str, 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			continue
		}
		i := int(math.Round((ts - float64(start.Unix())) / historyStep.Seconds()))
		if i >= 0 && i < historySamples {
			val := f
			out[i] = &val
		}
	}
	return out
}

// handleHistory returns the last 15 minutes of the chart series.
func (s *server) handleHistory(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	end := time.Now().Truncate(historyStep)
	start := end.Add(-time.Duration(historySamples-1) * historyStep)
	writeJSON(w, http.StatusOK, map[string]any{
		"throughput": s.promRange(ctx, qThroughput, start),
		"p99":        s.promRange(ctx, qP99, start),
		"processed":  s.promRange(ctx, qProcessed, start),
	})
}

var (
	eventTypes = []string{"page_view", "user_action", "transaction"}
	pages      = []string{"/home", "/products", "/cart", "/checkout", "/profile"}
	actions    = []string{"click", "scroll", "hover", "submit", "search"}
)

// newEvent matches cmd/generator's format exactly so the processor accepts it.
// IDs start with "dash-" so they're distinguishable from generator traffic.
func newEvent(seq int) ([]byte, string) {
	t := eventTypes[rand.Intn(len(eventTypes))]
	userID := fmt.Sprintf("user-%d", rand.Intn(100))
	payload := map[string]any{}
	switch t {
	case "page_view":
		payload["page"] = pages[rand.Intn(len(pages))]
		payload["duration_ms"] = 500 + rand.Intn(5000)
	case "user_action":
		payload["action"] = actions[rand.Intn(len(actions))]
		payload["page"] = pages[rand.Intn(len(pages))]
	case "transaction":
		payload["amount"] = float64(rand.Intn(10000)) / 100.0
	}
	data, _ := json.Marshal(map[string]any{
		"id":        fmt.Sprintf("dash-%d-%d", time.Now().UnixNano(), seq),
		"type":      t,
		"user_id":   userID,
		"timestamp": time.Now().Format(time.RFC3339Nano),
		"payload":   payload,
	})
	return data, userID
}

func (s *server) handleBurst(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "use POST"})
		return
	}
	n, err := strconv.Atoi(r.URL.Query().Get("n"))
	if err != nil || n < 1 {
		n = 500
	}
	if n > maxBurst {
		n = maxBurst
	}

	msgs := make([]kafka.Message, n)
	for i := range msgs {
		data, key := newEvent(i)
		msgs[i] = kafka.Message{Key: []byte(key), Value: data}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	if err := s.writer.WriteMessages(ctx, msgs...); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "couldn't write to Kafka: " + err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sent": n})
}

func (s *server) handleBad(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "use POST"})
		return
	}
	// Two kinds of poison message, matching the cases the integration tests cover.
	kind, value := "invalid JSON", []byte(`{"type": "page_view", "user_id": oops`)
	if rand.Intn(2) == 0 {
		kind = "missing id"
		value, _ = json.Marshal(map[string]any{
			"type":      "page_view",
			"user_id":   "user-0",
			"timestamp": time.Now().Format(time.RFC3339Nano),
			"payload":   map[string]any{"page": "/home"},
		})
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := s.writer.WriteMessages(ctx, kafka.Message{Key: []byte("dashboard-bad"), Value: value}); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "couldn't write to Kafka: " + err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sent": 1, "kind": kind})
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func main() {
	brokers := strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")
	addr := ":" + env("PORT", "8080")

	s := &server{
		brokers:      brokers,
		promURL:      strings.TrimRight(env("PROMETHEUS_URL", "http://localhost:9091"), "/"),
		processorURL: env("PROCESSOR_HEALTH_URL", "http://localhost:8081/readyz"),
		hub:          newHub(),
		windows:      &windowLog{},
		flow:         &flowCounter{counts: make(map[string]int)},
		httpc:        &http.Client{Timeout: 2 * time.Second},
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        inputTopic,
			Balancer:     &kafka.Hash{},
			BatchTimeout: 10 * time.Millisecond, // default is 1s, which makes single writes feel laggy
			RequiredAcks: kafka.RequireOne,
		},
	}
	defer s.writer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go s.consumeEvents(ctx)
	go s.consumeWindows(ctx)
	go s.pumpFlow(ctx)

	static, err := fs.Sub(staticFS, "static")
	if err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(static)))
	mux.HandleFunc("/events", s.handleEvents)
	mux.HandleFunc("/api/windows", s.handleWindows)
	mux.HandleFunc("/api/metrics", s.handleMetrics)
	mux.HandleFunc("/api/history", s.handleHistory)
	mux.HandleFunc("/api/inject/burst", s.handleBurst)
	mux.HandleFunc("/api/inject/bad", s.handleBad)

	// No WriteTimeout: the /events stream stays open indefinitely.
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}

	go func() {
		log.Printf("dashboard listening on %s (kafka=%v)", addr, brokers)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	log.Print("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}
