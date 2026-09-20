package certify

import (
	"crypto/tls"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// The API-server-to-Kine edge is a privileged storage boundary
// (design Section 3.1). These are the cases an operator has to be able
// to rely on being refused, tested against the running listener rather
// than against the configuration that built it.

// A plaintext connection to the edge gets no service. There is no
// downgrade, no automatic fallback and no partial answer.
func TestPlaintextIsRefused(t *testing.T) {
	h := load(t)
	conn, err := net.DialTimeout("tcp", h.Edge.Listen, 10*time.Second)
	if err != nil {
		// Refusing the connection outright is also a refusal.
		return
	}
	defer conn.Close()
	// An HTTP/2 client preface is what a plaintext gRPC client would
	// send first. A TLS listener cannot answer it.
	if _, err := conn.Write([]byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")); err != nil {
		return
	}
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	buffer := make([]byte, 1)
	n, err := conn.Read(buffer)
	if err == nil && n > 0 {
		t.Fatalf("the edge answered a plaintext client with %q", buffer[:n])
	}
}

// A client that presents no certificate is refused: the edge requires
// and verifies one.
func TestAnUnauthenticatedClientIsRefused(t *testing.T) {
	h := load(t)
	refuses(t, clientTLS(t, h.Edge, "", "", h.Edge.ServerCA), h.Edge.Listen, "a client with no certificate")
}

// A client whose certificate is issued by some other authority is
// refused, even though its identity string is the authorized one.
// Being called the right thing is not being issued by the right
// authority.
func TestAForeignAuthorityIsRefused(t *testing.T) {
	h := load(t)
	conf := clientTLS(t, h.Edge, h.Edge.ForeignCertificate, h.Edge.ForeignKey, h.Edge.ServerCA)
	refuses(t, conf, h.Edge.Listen, "a client of a foreign authority")
}

// A client the edge's own client CA issued, but that the domain does not
// authorize, is refused. Trusting every certificate under a CA is not
// authorization -- which is the whole point of the allowlist.
func TestAnUnauthorizedClientOfTheSameAuthorityIsRefused(t *testing.T) {
	h := load(t)
	conf := clientTLS(t, h.Edge, h.Edge.UnauthorizedCertificate, h.Edge.UnauthorizedKey, h.Edge.ServerCA)
	refuses(t, conf, h.Edge.Listen, "an unauthorized client of the edge's own authority")
}

// The API server verifies Kine's identity too. A client that expects a
// different server name refuses the edge rather than proceeding.
func TestTheClientVerifiesTheEdgesServerName(t *testing.T) {
	h := load(t)
	conf := clientTLS(t, h.Edge, h.Edge.ClientCertificate, h.Edge.ClientKey, h.Edge.ServerCA)
	conf.ServerName = "some-other-kine.invalid"
	refuses(t, conf, h.Edge.Listen, "a client expecting another server name")
}

// A client that trusts a foreign authority for the *server* side refuses
// the edge: there is no verifier that would accept it.
func TestAClientTrustingTheWrongAuthorityRefusesTheEdge(t *testing.T) {
	h := load(t)
	conf := clientTLS(t, h.Edge, h.Edge.ClientCertificate, h.Edge.ClientKey, h.Edge.ForeignCA)
	refuses(t, conf, h.Edge.Listen, "a client trusting the wrong authority")
}

// And the authorized identity is served -- otherwise the refusals above
// would be satisfied by an edge that refuses everything.
func TestTheAuthorizedClientIsServed(t *testing.T) {
	h := load(t)
	cli := authorized(t, h)
	ctx := ctxFor(t)
	if _, err := cli.Get(ctx, prefix(t)+"probe"); err != nil {
		t.Fatalf("the authorized client was refused: %v", err)
	}
	// Status is what a Kubernetes control plane asks for on startup; the
	// version it reports is the emulated etcd version, and a profile
	// that cannot answer it is not one an API server will use.
	status, err := cli.Status(ctx, h.Edge.Endpoint)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Version == "" {
		t.Fatal("the edge reported no etcd version")
	}
	t.Logf("edge reports etcd version %s", status.Version)
}

// refuses asserts that a TLS client with this configuration gets no
// service from the edge: either the handshake fails, or the connection
// is closed before anything is served on it.
func refuses(t *testing.T, conf *tls.Config, address string, who string) {
	t.Helper()
	dialer := &net.Dialer{Timeout: 15 * time.Second}
	conn, err := tls.DialWithDialer(dialer, "tcp", address, conf)
	if err != nil {
		return
	}
	defer conn.Close()
	// Under TLS 1.3 the server's verdict on a client certificate can
	// arrive after the client believes it has finished, so the refusal
	// is read rather than assumed.
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	if _, err := conn.Write([]byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")); err != nil {
		return
	}
	buffer := make([]byte, 1)
	n, err := conn.Read(buffer)
	if err == nil && n > 0 {
		t.Fatalf("the edge served %s", who)
	}
	if err != nil && !errors.Is(err, io.EOF) {
		t.Logf("%s was refused: %v", who, err)
	}
}

// A refused client must not be able to reach the domain through the
// etcd client library either, which is the shape a misconfigured API
// server would actually take.
func TestAnUnauthorizedEtcdClientCannotReadOrMutate(t *testing.T) {
	h := load(t)
	cli, err := dial(t, h, h.Edge.UnauthorizedCertificate, h.Edge.UnauthorizedKey)
	if err != nil {
		return
	}
	ctx, cancel := contextWithTimeout(20 * time.Second)
	defer cancel()
	key := prefix(t) + "intruder"
	if _, err := cli.Get(ctx, key); err == nil {
		t.Fatal("an unauthorized client read the domain")
	}
	if _, err := cli.Put(ctx, key, "x"); err == nil {
		t.Fatal("an unauthorized client mutated the domain")
	}
	if _, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, "x")).
		Commit(); err == nil {
		t.Fatal("an unauthorized client created a key")
	}
}
