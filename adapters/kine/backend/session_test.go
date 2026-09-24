package backend_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/tuplesky/tuplesky/adapters/kine/backend"
	"github.com/tuplesky/tuplesky/adapters/kine/client"
	"github.com/tuplesky/tuplesky/adapters/kine/internal/fakedomain"
)

// tokenLifetime is what the test STS grants; the provider refreshes
// within refreshMargin of it and the client stops admitting work on a
// binding bindingMargin before its acknowledged expiry, so one advance
// past the refresh point rotates both together.
const (
	tokenLifetime = 60 * time.Second
	refreshMargin = 10 * time.Second
	bindingMargin = 10 * time.Second
)

// testClock is a hand-advanced clock shared by the credential provider,
// the client's binding validity and the domain's acknowledgements, so a
// token lifetime passes without the test sleeping through it.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// stsServer is a workload-identity STS: it exchanges the assertion for a
// service token, and each exchange is a fresh admission naming a fresh
// session, exactly as minting a receipt from fresh entropy does.
type stsServer struct {
	*httptest.Server
	exchanges atomic.Int64
	clock     *testClock
}

func startSts(t *testing.T, clock *testClock) *stsServer {
	t.Helper()
	sts := &stsServer{clock: clock}
	mux := http.NewServeMux()
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil || r.PostForm.Get("subject_token") == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		n := sts.exchanges.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"access_token":%q,"expires_in":%d}`, fmt.Sprintf("svc-%d", n), int(tokenLifetime.Seconds()))
	})
	sts.Server = httptest.NewTLSServer(mux)
	t.Cleanup(sts.Close)
	return sts
}

// sessionOf is the session the domain admits a given service token
// under: one session per exchanged credential.
func sessionOf(token string) ([16]byte, bool) {
	var out [16]byte
	switch token {
	case "svc-1":
		out[0] = 1
	case "svc-2":
		out[0] = 2
	case "svc-3":
		out[0] = 3
	default:
		return out, false
	}
	return out, true
}

// A normal workload-identity credential refresh must not wedge the
// client: the fresh exchange names a new session, the client rolls over
// to it and keeps working, and every invocation allocated before the
// rollover keeps the identity it was allocated under.
func TestACredentialRefreshRollsTheSessionOverWithoutStrandingOldInvocations(t *testing.T) {
	clock := &testClock{now: time.Unix(1_700_000_000, 0)}
	sts := startSts(t, clock)
	cert, pool, err := fakedomain.SelfSigned("frontend.local")
	if err != nil {
		t.Fatal(err)
	}
	domain, err := fakedomain.Start(fakedomain.Config{
		Cert: cert,
		Admit: func(token string) ([16]byte, time.Time, bool) {
			session, ok := sessionOf(token)
			return session, clock.Now().Add(tokenLifetime), ok
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(domain.Close)

	assertion := filepath.Join(t.TempDir(), "assertion")
	if err := os.WriteFile(assertion, []byte("workload-assertion"), 0o600); err != nil {
		t.Fatal(err)
	}
	provider := client.NewProvider(client.ProviderConfig{
		TokenFile:     assertion,
		STSURL:        sts.URL,
		Audience:      "cluster-resource",
		HTTP:          sts.Client(),
		RefreshMargin: refreshMargin,
		Clock:         clock.Now,
	})
	native := client.New(domain.Addr(), client.Config{
		TLS:           &tls.Config{RootCAs: pool, ServerName: "frontend.local", MinVersion: tls.VersionTLS13},
		Tokens:        provider,
		Cluster:       [16]byte{1},
		Domain:        [16]byte{2},
		FrameTimeout:  3 * time.Second,
		BindingMargin: bindingMargin,
		Clock:         clock.Now,
	})
	be, err := backend.New(backend.Config{
		Client:         native,
		Cluster:        [16]byte{1},
		Domain:         [16]byte{2},
		Namespace:      [16]byte{3},
		ClientInstance: [16]byte{4},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := be.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	first, _ := sessionOf("svc-1")
	second, _ := sessionOf("svc-2")
	if session, ok := native.Session(); !ok || session != first {
		t.Fatalf("first binding: %x %v", session, ok)
	}

	// An invocation whose outcome the domain never establishes: the
	// backend resolves it by identity and then refuses it explicitly.
	domain.PendingOnce.Store(true)
	domain.ForgetAll.Store(true)
	if _, err := be.Create(ctx, "/registry/ambiguous", []byte("v"), 0); err == nil {
		t.Fatal("an unestablished outcome was reported as success")
	}
	domain.ForgetAll.Store(false)
	ambiguous := make(map[[32]byte]bool)
	for _, entry := range domain.Log() {
		if entry.Session == first && entry.Sequence == 2 {
			ambiguous[entry.CommandID] = true
		}
	}
	if len(ambiguous) != 1 {
		t.Fatalf("the ambiguous invocation did not keep one identity: %d", len(ambiguous))
	}

	// A dropped connection inside one token lifetime reconnects under the
	// same session: a reconnect is not a rollover.
	// A request lost with the connection is an explicit unknown outcome
	// here, so the caller retries; the retry lands on the warm
	// reconnection.
	domain.DropConnections()
	_, _ = be.Create(ctx, "/registry/a", []byte("v"), 0)
	if _, err := be.Create(ctx, "/registry/a2", []byte("v"), 0); err != nil {
		t.Fatalf("after reconnect: %v", err)
	}
	if native.Reconnects == 0 {
		t.Fatal("the dropped connection was not replaced")
	}
	if session, ok := native.Session(); !ok || session != first {
		t.Fatalf("reconnect changed the session: %x", session)
	}
	if got := sts.exchanges.Load(); got != 1 {
		t.Fatalf("exchanges within one lifetime: %d", got)
	}

	// The token lifetime passes. The provider exchanges the assertion
	// again, the STS admits a fresh session, and the client rolls over.
	clock.advance(tokenLifetime - refreshMargin)
	if _, err := be.Create(ctx, "/registry/b", []byte("v"), 0); err != nil {
		t.Fatalf("after refresh: %v", err)
	}
	if session, ok := native.Session(); !ok || session != second {
		t.Fatalf("session after refresh: %x", session)
	}
	if got := sts.exchanges.Load(); got != 2 {
		t.Fatalf("exchanges across two lifetimes: %d", got)
	}
	if native.Rollovers != 1 {
		t.Fatalf("rollovers: %d", native.Rollovers)
	}

	// New work continues under the new session, and nothing that was
	// allocated under the old one was re-sent under the new identity:
	// rewriting a retry key would make an unresolved write a second one.
	var sawSecond bool
	for _, entry := range domain.Log() {
		if entry.Kind == "bind" {
			continue
		}
		switch entry.Session {
		case first:
		case second:
			sawSecond = true
			if ambiguous[entry.CommandID] {
				t.Fatalf("an old invocation was replayed under the new session: %x", entry.CommandID)
			}
		default:
			t.Fatalf("frame under an unknown session %x", entry.Session)
		}
	}
	if !sawSecond {
		t.Fatal("no work continued under the rolled-over session")
	}

	// A second lifetime passes and the client rolls over again.
	clock.advance(tokenLifetime - refreshMargin)
	if _, err := be.Create(ctx, "/registry/c", []byte("v"), 0); err != nil {
		t.Fatalf("after the second refresh: %v", err)
	}
	third, _ := sessionOf("svc-3")
	if session, ok := native.Session(); !ok || session != third {
		t.Fatalf("session after the second refresh: %x", session)
	}
	if native.Rollovers != 2 {
		t.Fatalf("rollovers after two refreshes: %d", native.Rollovers)
	}
	if _, _, err := be.Get(ctx, server.HealthKey, 0, false); err != nil {
		t.Fatalf("read under the rolled-over session: %v", err)
	}
}

// A configured session pins the identity: an acknowledgement naming
// another session is refused rather than adopted.
func TestAPinnedSessionRefusesAnAcknowledgementNamingAnotherSession(t *testing.T) {
	clock := &testClock{now: time.Unix(1_700_000_000, 0)}
	cert, pool, err := fakedomain.SelfSigned("frontend.local")
	if err != nil {
		t.Fatal(err)
	}
	other := [16]byte{0xaa}
	domain, err := fakedomain.Start(fakedomain.Config{
		Cert: cert,
		Admit: func(string) ([16]byte, time.Time, bool) {
			return other, clock.Now().Add(tokenLifetime), true
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(domain.Close)
	pinned := [16]byte{0xbb}
	native := client.New(domain.Addr(), client.Config{
		TLS:           &tls.Config{RootCAs: pool, ServerName: "frontend.local", MinVersion: tls.VersionTLS13},
		Tokens:        staticTokens("svc-token"),
		Cluster:       [16]byte{1},
		Domain:        [16]byte{2},
		PinnedSession: &pinned,
		FrameTimeout:  3 * time.Second,
		Clock:         clock.Now,
	})
	if err := native.Connect(context.Background()); err == nil {
		t.Fatal("a pinned client adopted another session")
	}
	if _, ok := native.Session(); ok {
		t.Fatal("a refused acknowledgement was remembered")
	}
}
