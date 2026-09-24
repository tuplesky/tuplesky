package driver

import (
	"context"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
	kinetls "github.com/k3s-io/kine/pkg/tls"
)

// The cross-language regression of the native API authentication
// contract. The Rust integration test `coord-session/tests/kine_interop`
// binds a real `coord_transport::Transport` with a real frontend session
// binder, then runs this test against it with the endpoint's address,
// trust root and service tokens in the environment. Nothing here fakes
// either side: the Go module cannot build the Rust endpoint, so without
// that environment the test is skipped.
const (
	envEndpoint   = "COORD_INTEROP_ENDPOINT"
	envCA         = "COORD_INTEROP_CA"
	envServerName = "COORD_INTEROP_SERVER_NAME"
	envCluster    = "COORD_INTEROP_CLUSTER"
	envDomain     = "COORD_INTEROP_DOMAIN"
	envNamespace  = "COORD_INTEROP_NAMESPACE"
	envToken      = "COORD_INTEROP_TOKEN"
	envBadToken   = "COORD_INTEROP_BAD_TOKEN"
)

// startTokenIssuer serves `token` from an RFC 8693 exchange over TLS and
// returns its URL and the PEM file holding the CA that signs it.
func startTokenIssuer(t *testing.T, token string) (string, string) {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil || r.PostForm.Get("subject_token") == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"access_token":%q,"expires_in":3600}`, token)
	})
	sts := httptest.NewTLSServer(mux)
	t.Cleanup(sts.Close)
	// The test server's certificate is self-signed, so it is its own
	// trust anchor for the driver's `sts-ca` option.
	var encoded []byte
	for _, cert := range sts.TLS.Certificates {
		for _, der := range cert.Certificate {
			encoded = append(encoded, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
		}
	}
	if len(encoded) == 0 {
		t.Fatal("no STS certificate to trust")
	}
	path := filepath.Join(t.TempDir(), "sts-ca.pem")
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	return sts.URL, path
}

func interopDSN(t *testing.T, token string) string {
	t.Helper()
	stsURL, stsCA := startTokenIssuer(t, token)
	assertion := filepath.Join(t.TempDir(), "assertion")
	if err := os.WriteFile(assertion, []byte("workload-assertion"), 0o600); err != nil {
		t.Fatal(err)
	}
	return strings.Join([]string{
		"coord://" + os.Getenv(envEndpoint),
		"?cluster=" + os.Getenv(envCluster),
		"&domain=" + os.Getenv(envDomain),
		"&namespace=" + os.Getenv(envNamespace),
		"&server-name=" + os.Getenv(envServerName),
		"&assertion=" + assertion,
		"&sts=" + stsURL,
		"&sts-ca=" + stsCA,
		"&audience=interop",
	}, "")
}

// startDriver builds the backend through Kine's driver registry, exactly
// as a Kine build does for a coord:// DSN.
func startDriver(t *testing.T, token string) (server.Backend, error) {
	t.Helper()
	construct, ok := drivers.Get(Scheme)
	if !ok {
		t.Fatal("coord scheme not registered")
	}
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	leader, backend, err := construct(ctx, &wg, &drivers.Config{
		DataSourceName:   interopDSN(t, token),
		BackendTLSConfig: kinetls.Config{CAFile: os.Getenv(envCA)},
	})
	if err != nil {
		return nil, err
	}
	if leader {
		t.Fatal("the coord driver must never elect a leader")
	}
	return backend, backend.Start(ctx)
}

// The registered Go driver completes the whole native handshake against
// the real Rust endpoint: the TLS profile admits it without a client
// certificate, the Hello is acknowledged, the service token binds a
// session, and a request is answered.
func TestTheRegisteredDriverBindsAndRequestsAgainstTheNativeEndpoint(t *testing.T) {
	if os.Getenv(envEndpoint) == "" {
		t.Skip("no native endpoint; driven by `cargo test -p coord-session --test kine_interop`")
	}
	backend, err := startDriver(t, os.Getenv(envToken))
	if err != nil {
		t.Fatalf("start against the native endpoint: %v", err)
	}
	ctx := context.Background()
	revision, _, err := backend.Get(ctx, server.HealthKey, 0, false)
	if err != nil {
		t.Fatalf("read after binding: %v", err)
	}
	if revision <= 0 {
		t.Fatalf("revision %d", revision)
	}
}

// A service token the frontend does not accept is a refused binding, and
// the backend fails to start rather than serving unauthorized work.
func TestAnInvalidServiceTokenIsRefusedByTheNativeEndpoint(t *testing.T) {
	if os.Getenv(envEndpoint) == "" {
		t.Skip("no native endpoint; driven by `cargo test -p coord-session --test kine_interop`")
	}
	if _, err := startDriver(t, os.Getenv(envBadToken)); err == nil {
		t.Fatal("an invalid service token bound a session")
	}
}
