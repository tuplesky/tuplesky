// Package certify is the Kubernetes storage-profile certification suite
// (task-48; design Sections 3.1, 6.6, 6.8.4, 23 G4).
//
// It runs against a domain that is already up: `coord-harness up` has
// started every committed voter and the credential endpoint, and
// `kine-coord` is serving the etcd edge in front of them. Nothing in
// this package fakes a backend, a session or a listener, and nothing in
// it starts one either -- a suite that started its own would certify its
// own composition rather than the one an operator deploys.
//
// It is skipped unless COORD_CERTIFY_HARNESS names the `harness.json` of
// such a run, so `go test ./...` stays honest on a machine with no
// domain on it.
package certify

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// envHarness names the provisioned domain's description.
const envHarness = "COORD_CERTIFY_HARNESS"

// envPrefix separates one run's keys from another's, so the suite can be
// re-run against a domain that already holds the previous run's state --
// which is exactly what the regional-failover step does.
const envPrefix = "COORD_CERTIFY_PREFIX"

// edge is the Kubernetes storage edge's material, as `coord-harness`
// wrote it.
type edge struct {
	Listen                  string `json:"listen"`
	Endpoint                string `json:"endpoint"`
	ServerName              string `json:"server_name"`
	ServerCertificate       string `json:"server_certificate"`
	ServerKey               string `json:"server_key"`
	ClientCA                string `json:"client_ca"`
	ServerCA                string `json:"server_ca"`
	AllowedClient           string `json:"allowed_client"`
	ClientCertificate       string `json:"client_certificate"`
	ClientKey               string `json:"client_key"`
	UnauthorizedCertificate string `json:"unauthorized_certificate"`
	UnauthorizedKey         string `json:"unauthorized_key"`
	ForeignCertificate      string `json:"foreign_certificate"`
	ForeignKey              string `json:"foreign_key"`
	ForeignCA               string `json:"foreign_ca"`
}

// harness is the part of the provisioned domain this suite reads.
type harness struct {
	Cluster   string `json:"cluster"`
	Domain    string `json:"domain"`
	Namespace string `json:"namespace"`
	Edge      edge   `json:"edge"`
}

// load reads the provisioned domain, or skips.
func load(t *testing.T) harness {
	t.Helper()
	path := os.Getenv(envHarness)
	if path == "" {
		t.Skipf("no domain to certify; set %s to a `coord-harness up` run's harness.json", envHarness)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var h harness
	if err := json.Unmarshal(raw, &h); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	if h.Edge.Endpoint == "" {
		t.Fatalf("%s names no storage edge", path)
	}
	return h
}

// prefix is this run's key space. It is a real Kubernetes registry
// prefix, because the profile being certified is the one an API server
// drives and not a synthetic key layout.
func prefix(t *testing.T) string {
	t.Helper()
	if v := os.Getenv(envPrefix); v != "" {
		return "/registry/" + v + "/default/"
	}
	return fmt.Sprintf("/registry/certify-%d/default/", time.Now().UnixNano())
}

// clientTLS builds the API server's side of the mutual TLS edge: it
// verifies Kine's server identity against the edge CA and presents the
// authorized client identity.
func clientTLS(t *testing.T, e edge, certFile, keyFile, caFile string) *tls.Config {
	t.Helper()
	pool := x509.NewCertPool()
	pem, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatalf("edge CA: %v", err)
	}
	if !pool.AppendCertsFromPEM(pem) {
		t.Fatal("edge CA: no certificate found")
	}
	conf := &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    pool,
		ServerName: e.ServerName,
	}
	if certFile != "" {
		pair, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			t.Fatalf("client identity: %v", err)
		}
		conf.Certificates = []tls.Certificate{pair}
	}
	return conf
}

// dial opens an etcd client over the edge with the given client
// identity. It is the API server's own client library, over the API
// server's own transport.
func dial(t *testing.T, h harness, certFile, keyFile string) (*clientv3.Client, error) {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{h.Edge.Endpoint},
		DialTimeout: 15 * time.Second,
		TLS:         clientTLS(t, h.Edge, certFile, keyFile, h.Edge.ServerCA),
	})
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli, nil
}

// authorized opens the client the edge admits.
func authorized(t *testing.T, h harness) *clientv3.Client {
	t.Helper()
	cli, err := dial(t, h, h.Edge.ClientCertificate, h.Edge.ClientKey)
	if err != nil {
		t.Fatalf("authorized client: %v", err)
	}
	return cli
}

// contextWithTimeout is the bounded context the refusal cases use: they
// must fail within a deadline rather than hang, because a hang in a
// certification run is indistinguishable from a pass that never came.
func contextWithTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}
