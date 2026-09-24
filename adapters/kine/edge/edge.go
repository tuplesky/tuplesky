// Package edge composes the privileged API-server-to-Kine etcd edge
// (task-46; design Section 3.1): a permission-restricted Unix-domain
// socket inside the trusted deployment boundary, or mutually
// authenticated TLS where the API server verifies Kine's identity and
// Kine admits only the explicitly authorized API-server client identities
// of its domain. A plaintext network listener, a server-authentication-
// only substitute, an insecure verifier or a fallback to any of them is
// refused at configuration time.
package edge

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// Configuration errors.
var (
	// ErrInsecureListener: the listener is neither a Unix socket nor mTLS.
	ErrInsecureListener = errors.New("insecure listener: only unix:// or tls:// (mutual TLS) edges are permitted")
	// ErrInsecureVerifier: verification was disabled.
	ErrInsecureVerifier = errors.New("insecure verifier is not permitted")
	// ErrNoServerIdentity: a TLS edge without certificate and key.
	ErrNoServerIdentity = errors.New("tls edge requires the server certificate and key")
	// ErrNoClientTrust: a TLS edge without the client CA.
	ErrNoClientTrust = errors.New("tls edge requires the client CA that issues API-server identities")
	// ErrNoAuthorizedClients: a TLS edge without an explicit client
	// allowlist (every certificate of a CA is not authorization).
	ErrNoAuthorizedClients = errors.New("tls edge requires explicitly authorized client identities")
	// ErrClientNotAuthorized: a verified client identity outside the
	// allowlist.
	ErrClientNotAuthorized = errors.New("client identity not authorized for this domain")
	// ErrEmptyListener: no listener.
	ErrEmptyListener = errors.New("listener address required")
	// ErrSocketModeTooOpen: a Unix socket mode that lets the group or
	// everyone connect. Filesystem permission is the Unix edge's only
	// client authentication, so a writable socket is an open endpoint.
	ErrSocketModeTooOpen = errors.New("unix socket mode must not be group- or world-writable")
)

// Config configures the edge.
type Config struct {
	// Listener is `unix:///path/kine.sock` or `tls://host:port`.
	Listener string
	// CertFile and KeyFile are Kine's server identity (tls only).
	CertFile string
	KeyFile  string
	// ClientCAFile issues the API-server client identities (tls only).
	ClientCAFile string
	// AllowedClients are the exact DNS SANs, URI SANs or common names of
	// the authorized API-server identities (tls only).
	AllowedClients []string
	// SkipVerify is refused when set; the field exists so a deployment
	// flag maps to an explicit rejection rather than silence.
	SkipVerify bool
	// SocketMode is the Unix socket mode (default 0600). A mode with a
	// group- or world-writable bit is refused: connecting to the socket
	// needs write permission on it and nothing else authenticates the
	// client.
	SocketMode os.FileMode
}

// Listener is the composed edge.
type Listener struct {
	net.Listener
	// Scheme is what Kine's server bridge advertises ("unix" or "https").
	Scheme string
	// Endpoint is the client URL.
	Endpoint string
	// ServerOptions carry the credentials and limits for grpc.NewServer.
	ServerOptions []grpc.ServerOption
}

func schemeAndAddress(s string) (string, string) {
	parts := strings.SplitN(s, "://", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", s
}

// socketMode is the mode a unix:// socket is created with. Connecting to
// a Unix socket needs write permission on it and the edge authenticates
// nothing beyond that, so a mode that lets the group or everyone write
// would open the privileged etcd endpoint to every local workload that
// can reach the path; it is refused at configuration time rather than
// applied.
func socketMode(cfg Config) (os.FileMode, error) {
	mode := cfg.SocketMode
	if mode == 0 {
		return 0o600, nil
	}
	if mode&0o022 != 0 {
		return 0, fmt.Errorf("%w: %#o", ErrSocketModeTooOpen, uint32(mode))
	}
	return mode, nil
}

func baseOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 5 * time.Second}),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 2 * time.Hour, Timeout: 20 * time.Second}),
		grpc.MaxRecvMsgSize(2*1024*1024 + 512*1024),
	}
}

// TLSConfig builds the mutual TLS configuration of a tls:// edge, or an
// error naming what is missing or insecure.
func (c Config) TLSConfig() (*tls.Config, error) {
	if c.SkipVerify {
		return nil, ErrInsecureVerifier
	}
	if c.CertFile == "" || c.KeyFile == "" {
		return nil, ErrNoServerIdentity
	}
	if c.ClientCAFile == "" {
		return nil, ErrNoClientTrust
	}
	if len(c.AllowedClients) == 0 {
		return nil, ErrNoAuthorizedClients
	}
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("server identity: %w", err)
	}
	caPEM, err := os.ReadFile(c.ClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("client CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("client CA: no certificate found")
	}
	allowed := append([]string(nil), c.AllowedClients...)
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		VerifyPeerCertificate: func(_ [][]byte, chains [][]*x509.Certificate) error {
			return Authorized(allowed, chains)
		},
	}, nil
}

// Authorized checks that the verified client leaf presents one of the
// allowed identities. Chain verification against the client CA has
// already happened; this is the per-domain authorization on top of it.
func Authorized(allowed []string, chains [][]*x509.Certificate) error {
	if len(chains) == 0 || len(chains[0]) == 0 {
		return ErrClientNotAuthorized
	}
	leaf := chains[0][0]
	names := make([]string, 0, 1+len(leaf.DNSNames)+len(leaf.URIs))
	if leaf.Subject.CommonName != "" {
		names = append(names, leaf.Subject.CommonName)
	}
	names = append(names, leaf.DNSNames...)
	for _, u := range leaf.URIs {
		names = append(names, u.String())
	}
	for _, name := range names {
		for _, a := range allowed {
			if a != "" && a == name {
				return nil
			}
		}
	}
	return ErrClientNotAuthorized
}

// Listen binds the edge. A `unix://` listener is created with the socket
// mode; a `tls://` listener is a TCP listener whose gRPC credentials
// require and verify client certificates. Every other scheme is refused.
func Listen(ctx context.Context, cfg Config) (*Listener, error) {
	if cfg.Listener == "" {
		return nil, ErrEmptyListener
	}
	if cfg.SkipVerify {
		return nil, ErrInsecureVerifier
	}
	scheme, address := schemeAndAddress(cfg.Listener)
	lc := net.ListenConfig{}
	switch scheme {
	case "unix":
		if address == "" {
			return nil, ErrEmptyListener
		}
		// The mode is validated before anything touches the filesystem, so
		// a refused configuration neither removes a stale socket nor leaves
		// a bound one behind.
		mode, err := socketMode(cfg)
		if err != nil {
			return nil, err
		}
		if err := os.Remove(address); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("stale socket: %w", err)
		}
		ln, err := lc.Listen(ctx, "unix", address)
		if err != nil {
			return nil, err
		}
		if err := os.Chmod(address, mode); err != nil {
			_ = ln.Close()
			return nil, err
		}
		return &Listener{Listener: ln, Scheme: "unix", Endpoint: "unix://" + address, ServerOptions: baseOptions()}, nil
	case "tls":
		tlsConf, err := cfg.TLSConfig()
		if err != nil {
			return nil, err
		}
		ln, err := lc.Listen(ctx, "tcp", address)
		if err != nil {
			return nil, err
		}
		opts := append(baseOptions(), grpc.Creds(credentials.NewTLS(tlsConf)))
		return &Listener{Listener: ln, Scheme: "https", Endpoint: "https://" + ln.Addr().String(), ServerOptions: opts}, nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrInsecureListener, cfg.Listener)
	}
}
