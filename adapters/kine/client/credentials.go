// Package client is the Go QUIC client and workload credentials of the
// Kine adapter (task-45; design Sections 19.4, 19.5). It uses plain
// quic-go streams with the shared wire_v1 framing, binds trust and
// origin at the TLS layer, obtains a service token through workload
// identity federation with a single global exchange rather than per
// operation, and preserves stable invocation identity across reset,
// timeout and warm reconnect. Unknown outcomes stay explicit; there is
// no exactly-once guarantee across a lost upstream identity, and
// epoch-aware collection is task-m02.
//
// The session binding handshake is not on this branch. A connection here
// sends only the Hello and then closes its side of the control stream: it
// reads no HelloAck, sends no Bind, and awaits no BindAck, and the token
// the Provider obtains is checked for availability and then dropped
// rather than presented. The Rust frontend answers every frame on an
// unbound connection with NotBound, so this client is exercised only
// against the in-process test server, which performs no binding check.
// The handshake lands in task-46: "task-46: implement Kine driver
// registration and CRUD/range backend" presents the token once in a Bind
// frame and reads the BindAck, and "task-46: address review findings"
// keeps the control stream open and reads the HelloAck before binding.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"
)

// Credentials errors.
var (
	// ErrTokenFileMissing: the workload token file is absent or empty.
	ErrTokenFileMissing = errors.New("workload token file missing")
	// ErrExchangeFailed: the STS refused the exchange (bounded reason).
	ErrExchangeFailed = errors.New("token exchange failed")
	// ErrUnavailable: the STS could not be reached.
	ErrUnavailable = errors.New("sts unavailable")
)

// serviceToken is a cached service token with its expiry.
type serviceToken struct {
	token     string
	expiresAt time.Time
}

// Clock is the time source (injected for tests).
type Clock func() time.Time

// Provider obtains and caches a cluster service token by exchanging the
// workload's rotating assertion. It never exposes the raw assertion and
// refreshes at most once at a time.
type Provider struct {
	tokenFile string
	stsURL    string
	audience  string
	http      *http.Client
	margin    time.Duration
	clock     Clock

	mu       sync.Mutex
	cached   *serviceToken
	inflight bool
	// refreshErr is why the last refresh failed (nil when it succeeded);
	// waiters that coalesced onto it report this rather than a stale entry.
	refreshErr error
	// Exchanges performed against the STS (for tests).
	Exchanges int
}

// ProviderConfig configures a Provider.
type ProviderConfig struct {
	// TokenFile is the path to the rotating workload assertion.
	TokenFile string
	// STSURL is the base URL of the token-exchange endpoint.
	STSURL string
	// Audience is the target cluster resource.
	Audience string
	// HTTP is the shared client (redirects and env proxies disabled).
	HTTP *http.Client
	// RefreshMargin is how long before expiry to refresh.
	RefreshMargin time.Duration
	// Clock is the time source.
	Clock Clock
}

// NewProvider builds a Provider.
func NewProvider(cfg ProviderConfig) *Provider {
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	margin := cfg.RefreshMargin
	if margin == 0 {
		margin = 30 * time.Second
	}
	httpClient := cfg.HTTP
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 10 * time.Second,
			// A dedicated transport: the default one honours HTTP(S)_PROXY,
			// which would route the assertion through an unintended proxy.
			Transport: &http.Transport{Proxy: nil},
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return errors.New("redirects disabled")
			},
		}
	}
	return &Provider{
		tokenFile: cfg.TokenFile,
		stsURL:    cfg.STSURL,
		audience:  cfg.Audience,
		http:      httpClient,
		margin:    margin,
		clock:     clock,
	}
}

func (p *Provider) readAssertion() (string, error) {
	raw, err := os.ReadFile(p.tokenFile)
	if err != nil {
		return "", ErrTokenFileMissing
	}
	token := string(bytes.TrimSpace(raw))
	if token == "" {
		return "", ErrTokenFileMissing
	}
	return token, nil
}

// Token returns a usable service token, exchanging the current assertion
// only when the cache is empty or within the refresh margin. Concurrent
// callers coalesce: only one exchange runs at a time.
func (p *Provider) Token(ctx context.Context) (string, error) {
	p.mu.Lock()
	if token, ok := p.usableLocked(); ok {
		p.mu.Unlock()
		return token, nil
	}
	if p.inflight {
		// Another goroutine is refreshing; wait for it rather than starting
		// a second exchange (single-flight).
		p.mu.Unlock()
		return p.waitForRefresh(ctx)
	}
	p.inflight = true
	p.mu.Unlock()

	token, err := p.exchange(ctx)
	p.mu.Lock()
	p.inflight = false
	p.refreshErr = err
	if err == nil {
		p.cached = token
	}
	p.mu.Unlock()
	if err != nil {
		return "", err
	}
	return token.token, nil
}

// usableLocked returns the cached token when it is outside the refresh
// margin. The caller holds p.mu.
func (p *Provider) usableLocked() (string, bool) {
	if p.cached != nil && p.clock().Add(p.margin).Before(p.cached.expiresAt) {
		return p.cached.token, true
	}
	return "", false
}

func (p *Provider) waitForRefresh(ctx context.Context) (string, error) {
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
			p.mu.Lock()
			done := !p.inflight
			// The same predicate as the caller that started the refresh: a
			// failed exchange leaves an expired entry in place, and that
			// entry is not a usable token.
			token, ok := p.usableLocked()
			refreshErr := p.refreshErr
			p.mu.Unlock()
			if done {
				if ok {
					return token, nil
				}
				if refreshErr != nil {
					return "", refreshErr
				}
				return "", ErrExchangeFailed
			}
		}
	}
}

func (p *Provider) exchange(ctx context.Context) (*serviceToken, error) {
	assertion, err := p.readAssertion()
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.Exchanges++
	p.mu.Unlock()
	form := url.Values{}
	form.Set("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange")
	form.Set("subject_token", assertion)
	form.Set("subject_token_type", "urn:ietf:params:oauth:token-type:jwt")
	form.Set("requested_token_type", "urn:ietf:params:oauth:token-type:access_token")
	form.Set("resource", p.audience)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.stsURL+"/token", bytes.NewBufferString(form.Encode()))
	if err != nil {
		return nil, ErrUnavailable
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := p.http.Do(req)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return nil, ErrUnavailable
	}
	if resp.StatusCode == http.StatusServiceUnavailable {
		return nil, ErrUnavailable
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: status %d", ErrExchangeFailed, resp.StatusCode)
	}
	var parsed struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil || parsed.AccessToken == "" {
		return nil, ErrExchangeFailed
	}
	return &serviceToken{
		token:     parsed.AccessToken,
		expiresAt: p.clock().Add(time.Duration(parsed.ExpiresIn) * time.Second),
	}, nil
}

// Invalidate drops the cached token (a binding was rejected as expired).
func (p *Provider) Invalidate() {
	p.mu.Lock()
	p.cached = nil
	p.mu.Unlock()
}
