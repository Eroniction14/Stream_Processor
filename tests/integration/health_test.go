package integration

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Eroniction14/Stream_Processor/internal/health"
)

func TestHealthEndpoints(t *testing.T) {
	hc := health.New()
	hc.ServeHTTP(18081)

	time.Sleep(200 * time.Millisecond)

	// Liveness should be up immediately
	resp, err := http.Get("http://localhost:18081/healthz")
	if err != nil {
		t.Fatalf("healthz request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var body map[string]string
	json.NewDecoder(resp.Body).Decode(&body)
	if body["status"] != "alive" {
		t.Errorf("expected 'alive', got '%s'", body["status"])
	}

	// Readiness should be not ready initially
	resp2, err := http.Get("http://localhost:18081/readyz")
	if err != nil {
		t.Fatalf("readyz request failed: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 (not ready), got %d", resp2.StatusCode)
	}

	// Set ready, check again
	hc.SetReady(true)
	resp3, err := http.Get("http://localhost:18081/readyz")
	if err != nil {
		t.Fatalf("readyz request failed: %v", err)
	}
	defer resp3.Body.Close()

	if resp3.StatusCode != http.StatusOK {
		t.Errorf("expected 200 (ready), got %d", resp3.StatusCode)
	}

	t.Log("Health endpoints test passed")
}
