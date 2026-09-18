package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// stsStub answers /token, counting exchanges and echoing the assertion
// so the test can see the rotating file take effect.
func stsStub(t *testing.T, exchanges *atomic.Int64, ttl int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		exchanges.Add(1)
		_ = r.ParseForm()
		assertion := r.Form.Get("subject_token")
		// A slow exchange lets the single-flight test overlap callers.
		time.Sleep(40 * time.Millisecond)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "svc-for-" + assertion,
			"expires_in":   ttl,
		})
	}))
}

func writeToken(t *testing.T, path, token string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestProviderCachesRotatesAndSingleFlights(t *testing.T) {
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "coord-node.jwt")
	writeToken(t, tokenFile, "assertion-1")
	var exchanges atomic.Int64
	sts := stsStub(t, &exchanges, 3600)
	defer sts.Close()

	now := time.Unix(1_700_000_000, 0)
	clock := func() time.Time { return now }
	p := NewProvider(ProviderConfig{
		TokenFile:     tokenFile,
		STSURL:        sts.URL,
		Audience:      "tuplesky://cluster-1",
		HTTP:          sts.Client(),
		RefreshMargin: 30 * time.Second,
		Clock:         clock,
	})

	// The first call exchanges; subsequent calls within the cache window
	// do not (no per-operation federation).
	tok, err := p.Token(context.Background())
	if err != nil || tok != "svc-for-assertion-1" {
		t.Fatalf("first token: %q %v", tok, err)
	}
	for i := 0; i < 50; i++ {
		if _, err := p.Token(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if exchanges.Load() != 1 {
		t.Fatalf("expected 1 exchange for 51 calls, got %d", exchanges.Load())
	}

	// The token file rotates; a fresh exchange after the cache expires
	// picks it up.
	writeToken(t, tokenFile, "assertion-2")
	now = now.Add(2 * time.Hour)
	tok, err = p.Token(context.Background())
	if err != nil || tok != "svc-for-assertion-2" {
		t.Fatalf("rotated token: %q %v", tok, err)
	}
	if exchanges.Load() != 2 {
		t.Fatalf("expected 2 exchanges after rotation, got %d", exchanges.Load())
	}

	// Single-flight: many concurrent callers past expiry coalesce into
	// one exchange.
	now = now.Add(2 * time.Hour)
	p.Invalidate()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = p.Token(context.Background())
		}()
	}
	wg.Wait()
	if got := exchanges.Load(); got != 3 {
		t.Fatalf("single-flight: expected 3 exchanges, got %d", got)
	}
}

func TestProviderFailsClosed(t *testing.T) {
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "missing.jwt")
	var exchanges atomic.Int64
	sts := stsStub(t, &exchanges, 3600)
	defer sts.Close()
	p := NewProvider(ProviderConfig{
		TokenFile: tokenFile,
		STSURL:    sts.URL,
		Audience:  "a",
		HTTP:      sts.Client(),
		Clock:     time.Now,
	})
	// A missing token file fails closed, without contacting the STS.
	if _, err := p.Token(context.Background()); err != ErrTokenFileMissing {
		t.Fatalf("expected token file missing, got %v", err)
	}
	if exchanges.Load() != 0 {
		t.Fatal("no exchange should happen without an assertion")
	}
	// A 503 from the STS is an unavailable error, not a bogus token.
	writeToken(t, tokenFile, "a")
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer down.Close()
	p2 := NewProvider(ProviderConfig{TokenFile: tokenFile, STSURL: down.URL, Audience: "a", HTTP: down.Client(), Clock: time.Now})
	if _, err := p2.Token(context.Background()); err != ErrUnavailable {
		t.Fatalf("expected unavailable, got %v", err)
	}
}
