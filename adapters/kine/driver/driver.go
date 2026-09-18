// Package driver registers the `coord://` scheme in Kine's driver
// registry (task-46; design Section 6.6). Importing it into a Kine build
// makes `drivers.New` construct the native backend for a coord:// DSN; a
// DSN alone cannot add the driver to an unmodified binary.
package driver

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/tuplesky/tuplesky/adapters/kine/backend"
	"github.com/tuplesky/tuplesky/adapters/kine/client"
)

// Scheme is the registered DSN scheme.
const Scheme = "coord"

// DSN errors.
var (
	// ErrInvalidDSN: the DSN is not `coord://frontend:port?...`.
	ErrInvalidDSN = errors.New("invalid coord DSN")
	// ErrInsecureOption: a DSN or TLS option that would weaken the edge.
	ErrInsecureOption = errors.New("insecure option is not permitted")
	// ErrNoTrustRoot: no CA to verify the frontend.
	ErrNoTrustRoot = errors.New("coord driver requires the frontend CA (--ca-file)")
)

func init() {
	drivers.Register(Scheme, New)
}

// DSN is the parsed data source: one frontend, one domain, one namespace
// and the workload credential inputs.
type DSN struct {
	// Frontend is host:port of the native frontend.
	Frontend string
	// ServerName is the TLS name expected of the frontend (default host).
	ServerName string
	// Cluster, Domain and Namespace bind the backend.
	Cluster   [16]byte
	Domain    [16]byte
	Namespace [16]byte
	// ClientInstance is the stable instance identity (random per process
	// unless given).
	ClientInstance [16]byte
	// Session pins the expected session (optional).
	Session *[16]byte
	// AssertionFile, STSURL and Audience feed the workload credential
	// provider.
	AssertionFile string
	STSURL        string
	Audience      string
	// DeadlineMs is the per-request deadline.
	DeadlineMs uint32
}

func parseID(q url.Values, name string, required bool) ([16]byte, bool, error) {
	var out [16]byte
	v := q.Get(name)
	if v == "" {
		if required {
			return out, false, fmt.Errorf("%w: %s is required", ErrInvalidDSN, name)
		}
		return out, false, nil
	}
	raw, err := hex.DecodeString(v)
	if err != nil || len(raw) != 16 {
		return out, false, fmt.Errorf("%w: %s must be 32 hex characters", ErrInvalidDSN, name)
	}
	copy(out[:], raw)
	return out, true, nil
}

var knownParams = map[string]bool{
	"cluster": true, "domain": true, "namespace": true, "client-instance": true,
	"session": true, "assertion": true, "sts": true, "audience": true,
	"server-name": true, "deadline-ms": true,
}

// ParseDSN parses `coord://frontend:port?cluster=..&domain=..&namespace=..
// &assertion=/path&sts=https://...&audience=...` (the scheme prefix is
// optional, since Kine strips it). Unknown or insecure options are
// refused.
func ParseDSN(raw string) (DSN, error) {
	raw = strings.TrimPrefix(raw, Scheme+"://")
	u, err := url.Parse("coord://" + raw)
	if err != nil {
		return DSN{}, fmt.Errorf("%w: %v", ErrInvalidDSN, err)
	}
	if u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") {
		return DSN{}, fmt.Errorf("%w: expected frontend host:port with query options", ErrInvalidDSN)
	}
	if _, _, err := splitHostPort(u.Host); err != nil {
		return DSN{}, fmt.Errorf("%w: %v", ErrInvalidDSN, err)
	}
	q := u.Query()
	for name := range q {
		switch name {
		case "insecure", "skip-verify", "plaintext", "insecure-skip-verify":
			return DSN{}, fmt.Errorf("%w: %s", ErrInsecureOption, name)
		}
		if !knownParams[name] {
			return DSN{}, fmt.Errorf("%w: unknown option %s", ErrInvalidDSN, name)
		}
	}
	d := DSN{Frontend: u.Host, ServerName: u.Hostname(), DeadlineMs: 10_000}
	if d.Cluster, _, err = parseID(q, "cluster", true); err != nil {
		return DSN{}, err
	}
	if d.Domain, _, err = parseID(q, "domain", true); err != nil {
		return DSN{}, err
	}
	if d.Namespace, _, err = parseID(q, "namespace", true); err != nil {
		return DSN{}, err
	}
	instance, given, err := parseID(q, "client-instance", false)
	if err != nil {
		return DSN{}, err
	}
	if !given {
		if _, err := rand.Read(instance[:]); err != nil {
			return DSN{}, err
		}
	}
	d.ClientInstance = instance
	session, given, err := parseID(q, "session", false)
	if err != nil {
		return DSN{}, err
	}
	if given {
		d.Session = &session
	}
	d.AssertionFile, d.STSURL, d.Audience = q.Get("assertion"), q.Get("sts"), q.Get("audience")
	if d.AssertionFile == "" || d.STSURL == "" || d.Audience == "" {
		return DSN{}, fmt.Errorf("%w: assertion, sts and audience are required", ErrInvalidDSN)
	}
	if !strings.HasPrefix(d.STSURL, "https://") {
		return DSN{}, fmt.Errorf("%w: sts must be https", ErrInsecureOption)
	}
	if v := q.Get("server-name"); v != "" {
		d.ServerName = v
	}
	if v := q.Get("deadline-ms"); v != "" {
		n, err := strconv.ParseUint(v, 10, 32)
		if err != nil || n == 0 {
			return DSN{}, fmt.Errorf("%w: deadline-ms", ErrInvalidDSN)
		}
		d.DeadlineMs = uint32(n)
	}
	return d, nil
}

func splitHostPort(hostport string) (string, string, error) {
	i := strings.LastIndex(hostport, ":")
	if i <= 0 || i == len(hostport)-1 {
		return "", "", errors.New("frontend must be host:port")
	}
	if _, err := strconv.ParseUint(hostport[i+1:], 10, 16); err != nil {
		return "", "", errors.New("frontend port is not a number")
	}
	return hostport[:i], hostport[i+1:], nil
}

// trustRoot loads the frontend CA; verification cannot be skipped.
func trustRoot(cfg *drivers.Config) (*x509.CertPool, error) {
	if cfg.BackendTLSConfig.SkipVerify {
		return nil, fmt.Errorf("%w: skip-verify", ErrInsecureOption)
	}
	if cfg.BackendTLSConfig.CAFile == "" {
		return nil, ErrNoTrustRoot
	}
	pem, err := os.ReadFile(cfg.BackendTLSConfig.CAFile)
	if err != nil {
		return nil, fmt.Errorf("frontend CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("frontend CA: no certificate found")
	}
	return pool, nil
}

// New is the registered constructor: it never elects a leader (the
// domain's voters do), and returns the native backend.
func New(ctx context.Context, wg *sync.WaitGroup, cfg *drivers.Config) (bool, server.Backend, error) {
	dsn, err := ParseDSN(cfg.DataSourceName)
	if err != nil {
		return false, nil, err
	}
	pool, err := trustRoot(cfg)
	if err != nil {
		return false, nil, err
	}
	provider := client.NewProvider(client.ProviderConfig{
		TokenFile: dsn.AssertionFile,
		STSURL:    dsn.STSURL,
		Audience:  dsn.Audience,
	})
	native := client.New(dsn.Frontend, client.Config{
		TLS: &tls.Config{
			RootCAs:    pool,
			ServerName: dsn.ServerName,
			MinVersion: tls.VersionTLS13,
		},
		Tokens:       provider,
		Cluster:      dsn.Cluster,
		Domain:       dsn.Domain,
		FrameTimeout: time.Duration(dsn.DeadlineMs) * time.Millisecond,
	})
	b, err := backend.New(backend.Config{
		Client:         native,
		Cluster:        dsn.Cluster,
		Domain:         dsn.Domain,
		Namespace:      dsn.Namespace,
		Session:        dsn.Session,
		ClientInstance: dsn.ClientInstance,
		DeadlineMs:     dsn.DeadlineMs,
	})
	if err != nil {
		return false, nil, err
	}
	return false, b, nil
}
