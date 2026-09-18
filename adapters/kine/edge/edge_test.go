package edge

import (
	"context"
	"crypto/x509"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tuplesky/tuplesky/adapters/kine/internal/testpki"
)

// Insecure listener configurations are refused at configuration time:
// plaintext TCP, missing server identity, missing client trust, no
// allowlist and a disabled verifier.
func TestInsecureConfigurationsAreRejected(t *testing.T) {
	dir := t.TempDir()
	ca, err := testpki.NewCA("ca")
	if err != nil {
		t.Fatal(err)
	}
	caPath, _ := ca.WriteCA(dir, "ca")
	id, err := ca.Issue("kine.local", true, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	certPath, keyPath, _ := id.WriteFiles(dir, "server")
	full := Config{Listener: "tls://127.0.0.1:0", CertFile: certPath, KeyFile: keyPath, ClientCAFile: caPath, AllowedClients: []string{"apiserver.local"}}
	cases := []struct {
		name string
		cfg  Config
		want error
	}{
		{"empty", Config{}, ErrEmptyListener},
		{"plaintext tcp", Config{Listener: "tcp://127.0.0.1:0"}, ErrInsecureListener},
		{"http", Config{Listener: "http://127.0.0.1:0"}, ErrInsecureListener},
		{"bare address", Config{Listener: "127.0.0.1:0"}, ErrInsecureListener},
		{"skip verify", func() Config { c := full; c.SkipVerify = true; return c }(), ErrInsecureVerifier},
		{"server auth only", func() Config { c := full; c.ClientCAFile = ""; return c }(), ErrNoClientTrust},
		{"no server identity", func() Config { c := full; c.CertFile = ""; return c }(), ErrNoServerIdentity},
		{"broad CA trust without allowlist", func() Config { c := full; c.AllowedClients = nil; return c }(), ErrNoAuthorizedClients},
	}
	for _, tc := range cases {
		ln, err := Listen(context.Background(), tc.cfg)
		if ln != nil {
			_ = ln.Close()
		}
		if !errors.Is(err, tc.want) {
			t.Fatalf("%s: %v, want %v", tc.name, err, tc.want)
		}
	}
	ln, err := Listen(context.Background(), full)
	if err != nil {
		t.Fatalf("mutual TLS edge: %v", err)
	}
	defer func() { _ = ln.Close() }()
	if ln.Scheme != "https" || len(ln.ServerOptions) < 4 {
		t.Fatalf("listener %+v", ln)
	}
}

// A unix edge is created with restricted permissions.
func TestUnixSocketIsRestricted(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "kine.sock")
	ln, err := Listen(context.Background(), Config{Listener: "unix://" + sock})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	info, err := os.Stat(sock)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode %o", info.Mode().Perm())
	}
	if ln.Scheme != "unix" || ln.Endpoint != "unix://"+sock {
		t.Fatalf("listener %+v", ln)
	}
}

// Client authorization matches exact names only: DNS SAN, URI SAN or
// common name, never a prefix or any certificate of the CA.
func TestAuthorizedMatchesExactNames(t *testing.T) {
	u, _ := url.Parse("spiffe://cluster/ns/kube-system/sa/apiserver")
	leaf := &x509.Certificate{DNSNames: []string{"apiserver-a.local"}, URIs: []*url.URL{u}}
	leaf.Subject.CommonName = "apiserver"
	chains := [][]*x509.Certificate{{leaf}}
	for _, ok := range []string{"apiserver-a.local", "spiffe://cluster/ns/kube-system/sa/apiserver", "apiserver"} {
		if err := Authorized([]string{"other", ok}, chains); err != nil {
			t.Fatalf("%s: %v", ok, err)
		}
	}
	for _, bad := range [][]string{nil, {""}, {"apiserver-a"}, {"apiserver-a.local.evil"}, {"spiffe://cluster/ns/kube-system"}} {
		if err := Authorized(bad, chains); !errors.Is(err, ErrClientNotAuthorized) {
			t.Fatalf("%v admitted", bad)
		}
	}
	if err := Authorized([]string{"apiserver"}, nil); !errors.Is(err, ErrClientNotAuthorized) {
		t.Fatal("no chain admitted")
	}
}
