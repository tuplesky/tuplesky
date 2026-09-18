package client

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"sync"
	"time"

	quic "github.com/quic-go/quic-go"
	"github.com/tuplesky/tuplesky/adapters/kine/wire"
)

// ALPNNativeAPI is the native API ALPN the frontend negotiates.
const ALPNNativeAPI = "coord-api/1"

// Client and invocation errors.
var (
	// ErrPayloadConflict: a retry named a sequence bound to another payload.
	ErrPayloadConflict = errors.New("payload conflict")
	// ErrUnknownSequence: a retry named an unallocated sequence.
	ErrUnknownSequence = errors.New("unknown sequence")
	// ErrPoolExhausted: the connection or stream pool is at its cap.
	ErrPoolExhausted = errors.New("pool exhausted")
	// ErrNotConnected: no connection is available.
	ErrNotConnected = errors.New("not connected")
	// ErrUnknownOutcome: the outcome is unknown (timeout, reset, or the
	// endpoint lost the identity); resolve by identity.
	ErrUnknownOutcome = errors.New("unknown outcome")
	// ErrProtocol: a frame the client did not expect.
	ErrProtocol = errors.New("protocol violation")
	// ErrBindRejected: the frontend refused the session binding (the
	// token was not accepted, or the answer was not an acknowledgement).
	ErrBindRejected = errors.New("bind rejected")
	// ErrNegotiationRejected: the frontend did not acknowledge the Hello
	// with the declared lane, so the connection is not usable.
	ErrNegotiationRejected = errors.New("negotiation rejected")
	// ErrSessionConflict: one credential named two sessions, or a
	// configured session was not the one acknowledged. Never a silent
	// identity change.
	ErrSessionConflict = errors.New("session conflict")
)

// LaneUnary is the capability that declares the unary request lane in the
// Hello (spec/wire-v1.md, task-31).
const LaneUnary uint16 = 0x0011

// TokenSource supplies the service token presented at binding. A
// *Provider is the production source.
type TokenSource interface {
	Token(ctx context.Context) (string, error)
}

// invalidator is the optional part of a TokenSource that can be told its
// cached credential was refused, so the next binding exchanges a fresh
// one instead of presenting the same rejected token again.
type invalidator interface {
	Invalidate()
}

// Outcome is a request's result.
type Outcome struct {
	// Response is the decoded response, when established.
	Response *wire.Response
	// Unknown is true when the outcome could not be established (timeout,
	// reset, or an Unknown/Pending answer): resolve by identity.
	Unknown bool
}

// PoolLimits bounds the warm pool.
type PoolLimits struct {
	// MaxStreams is the concurrent request streams in flight.
	MaxStreams int
}

// Config configures a Client.
type Config struct {
	// TLS binds trust and origin: the server certificate is validated
	// against RootCAs and the ServerName; the native API ALPN is offered.
	TLS *tls.Config
	// Tokens supplies the service token presented once per connection in
	// the Bind frame that follows the Hello; nil binds no session (a
	// deployment composition without a frontend session, test only).
	Tokens TokenSource
	// Cluster and Domain identify the origin.
	Cluster [16]byte
	Domain  [16]byte
	// PinnedSession, when set, is the only session this client accepts:
	// an acknowledgement naming another one is refused rather than
	// adopted, so a deployment that pins its session fails closed.
	PinnedSession *[16]byte
	// Limits bounds the pool.
	Limits PoolLimits
	// FrameTimeout bounds one request/response exchange.
	FrameTimeout time.Duration
	// BindingMargin is how long before the acknowledged expiry a binding
	// stops admitting new work, so the client rebinds rather than have
	// the frontend close the connection under it (default 5s).
	BindingMargin time.Duration
	// Clock is the time source (injected for tests).
	Clock func() time.Time
}

// binding is what the frontend acknowledged: the session, how long it
// admits work, the credential that established it (by digest; the token
// itself is never kept) and the rollover epoch.
type binding struct {
	session   [16]byte
	expiresAt time.Time
	// credential identifies the token presented, so the same credential
	// naming two sessions can be told apart from a freshly exchanged one
	// naming a new session.
	credential [32]byte
	// epoch advances once per rollover. Work allocated under an earlier
	// epoch keeps the identity it was allocated under.
	epoch uint64
}

// active reports whether the binding still admits new work at `now`.
func (b *binding) active(now time.Time, margin time.Duration) bool {
	return now.Add(margin).Before(b.expiresAt)
}

// Client is a bounded QUIC client to one frontend.
type Client struct {
	cfg    Config
	dialFn func(ctx context.Context) (*quic.Conn, error)

	mu   sync.Mutex
	conn *quic.Conn
	// control is the negotiation stream of `conn`: it stays open for the
	// connection's life because a Close frame travels on it.
	control *quic.Stream
	sem     chan struct{}
	// bound is what the last acknowledged Bind established.
	bound *binding
	// Reconnects performed (for tests).
	Reconnects int
	// Rollovers to a new session performed (for tests).
	Rollovers int
}

// New builds a client that dials `addr`.
func New(addr string, cfg Config) *Client {
	cfg = withDefaults(cfg)
	tlsConf := cfg.TLS.Clone()
	if len(tlsConf.NextProtos) == 0 {
		tlsConf.NextProtos = []string{ALPNNativeAPI}
	}
	c := &Client{
		cfg: cfg,
		sem: make(chan struct{}, cfg.Limits.MaxStreams),
	}
	c.dialFn = func(ctx context.Context) (*quic.Conn, error) {
		return quic.DialAddr(ctx, addr, tlsConf, &quic.Config{
			MaxIdleTimeout:  30 * time.Second,
			KeepAlivePeriod: 10 * time.Second,
		})
	}
	return c
}

// withDefaults fills the unset bounds.
func withDefaults(cfg Config) Config {
	if cfg.FrameTimeout == 0 {
		cfg.FrameTimeout = 10 * time.Second
	}
	if cfg.Limits.MaxStreams <= 0 {
		cfg.Limits.MaxStreams = 64
	}
	if cfg.BindingMargin == 0 {
		cfg.BindingMargin = 5 * time.Second
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	return cfg
}

// newWithDialer is a test hook injecting a dialer (an in-process server).
func newWithDialer(cfg Config, dial func(ctx context.Context) (*quic.Conn, error)) *Client {
	cfg = withDefaults(cfg)
	return &Client{cfg: cfg, dialFn: dial, sem: make(chan struct{}, cfg.Limits.MaxStreams)}
}

// connect returns a live connection, dialing and negotiating it if
// needed. A connection whose acknowledged binding no longer admits work
// is dropped first: the frontend refuses a rebind that names another
// session on a live connection, so a rolled-over session needs a fresh
// one.
func (c *Client) connect(ctx context.Context) (*quic.Conn, error) {
	c.mu.Lock()
	if c.bound != nil && !c.bound.active(c.cfg.Clock(), c.cfg.BindingMargin) && c.conn != nil {
		c.mu.Unlock()
		c.drop()
		c.mu.Lock()
	}
	if c.conn != nil {
		conn := c.conn
		c.mu.Unlock()
		return conn, nil
	}
	c.mu.Unlock()

	conn, err := c.dialFn(ctx)
	if err != nil {
		return nil, ErrNotConnected
	}
	control, err := c.bind(ctx, conn)
	if err != nil {
		_ = conn.CloseWithError(2, "bind failed")
		return nil, err
	}
	c.mu.Lock()
	c.conn, c.control = conn, control
	c.mu.Unlock()
	return conn, nil
}

// Session is the replicated session the frontend acknowledged at the
// last binding, when a token source is configured and a connection was
// bound.
func (c *Client) Session() ([16]byte, bool) {
	session, _, ok := c.Binding()
	return session, ok
}

// Binding is the session the frontend last acknowledged and the rollover
// epoch it belongs to. The epoch advances whenever a freshly exchanged
// credential established a new session, so a caller can tell whether the
// identity its outstanding work was allocated under is still current.
func (c *Client) Binding() ([16]byte, uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.bound == nil {
		return [16]byte{}, 0, false
	}
	return c.bound.session, c.bound.epoch, true
}

// BindingExpiry is when the acknowledged binding stops admitting work.
func (c *Client) BindingExpiry() (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.bound == nil {
		return time.Time{}, false
	}
	return c.bound.expiresAt, true
}

// Connect establishes and binds the connection now (the backend's Start
// does this so a refused binding fails startup rather than the first
// operation). It is idempotent on a live connection.
func (c *Client) Connect(ctx context.Context) error {
	_, err := c.connect(ctx)
	return err
}

// bind negotiates the connection: the Hello declaring the unary lane
// travels on the control stream and the HelloAck is read back before
// anything else is sent, so a refused negotiation is never mistaken for
// a usable connection. The service token is then presented once in a
// Bind frame. The control stream is returned and stays open, because a
// Close frame travels on it.
func (c *Client) bind(ctx context.Context, conn *quic.Conn) (*quic.Stream, error) {
	var token string
	if c.cfg.Tokens != nil {
		t, err := c.cfg.Tokens.Token(ctx)
		if err != nil {
			return nil, err
		}
		token = t
	}
	hello, err := wire.Encode(wire.Hello{
		Role:         wire.RoleClient,
		ClusterID:    c.cfg.Cluster,
		DomainID:     c.cfg.Domain,
		Capabilities: []uint16{LaneUnary},
	})
	if err != nil {
		return nil, err
	}
	helloCtx, cancel := context.WithTimeout(ctx, c.cfg.FrameTimeout)
	defer cancel()
	stream, err := conn.OpenStreamSync(helloCtx)
	if err != nil {
		return nil, ErrNotConnected
	}
	if err := writeFrame(stream, hello); err != nil {
		return nil, ErrNotConnected
	}
	if err := readHelloAck(helloCtx, stream); err != nil {
		return nil, err
	}
	if c.cfg.Tokens == nil {
		return stream, nil
	}
	if err := c.bindSession(ctx, conn, token); err != nil {
		return nil, err
	}
	return stream, nil
}

// readHelloAck reads the negotiation answer and requires the endpoint to
// have granted the lane the Hello declared. A Close frame carries the
// endpoint's rejection reason; anything else is a protocol violation.
func readHelloAck(ctx context.Context, stream *quic.Stream) error {
	answer, err := readOneFrame(ctx, stream)
	if err != nil {
		return ErrNegotiationRejected
	}
	msg, err := wire.Decode(answer)
	if err != nil {
		return ErrProtocol
	}
	switch m := msg.(type) {
	case wire.HelloAck:
		for _, capability := range m.Capabilities {
			if capability == LaneUnary {
				return nil
			}
		}
		return ErrNegotiationRejected
	case wire.Close:
		return ErrNegotiationRejected
	default:
		return ErrProtocol
	}
}

// bindSession presents the token on its own stream and requires a
// BindAck within the frame timeout; anything else (a Close, a reset, a
// timeout) is a rejected binding and the connection is not used.
func (c *Client) bindSession(ctx context.Context, conn *quic.Conn, token string) error {
	frame, err := wire.EncodeBind(wire.Bind{Token: []byte(token)})
	if err != nil {
		return err
	}
	bindCtx, cancel := context.WithTimeout(ctx, c.cfg.FrameTimeout)
	defer cancel()
	stream, err := conn.OpenStreamSync(bindCtx)
	if err != nil {
		return ErrNotConnected
	}
	if err := writeFrame(stream, frame); err != nil {
		return ErrBindRejected
	}
	_ = stream.Close()
	answer, err := readOneFrame(bindCtx, stream)
	if err != nil {
		c.invalidateToken()
		return ErrBindRejected
	}
	ack, err := wire.DecodeBindAck(answer)
	if err != nil {
		c.invalidateToken()
		return ErrBindRejected
	}
	return c.adopt(ack, sha256.Sum256([]byte(token)))
}

// invalidateToken tells a caching token source that the credential it
// handed out was refused, so the next binding exchanges a fresh one.
func (c *Client) invalidateToken() {
	if source, ok := c.cfg.Tokens.(invalidator); ok {
		source.Invalidate()
	}
}

// adopt records what an acknowledgement established.
//
// A binding is not remembered forever: it carries the validity the
// frontend acknowledged, and a credential refresh legitimately produces
// a new session, because the STS admits a fresh assertion as a fresh
// admission. Refusing every later session would make ordinary credential
// rotation permanently unusable; adopting one silently would let the
// endpoint move this client's identity underneath it. So exactly one
// transition is a rollover: a credential this client has not presented
// before naming a session other than the current one. New work then
// continues under the new session while invocations already allocated
// keep the identity they were allocated under - their retry keys are
// what makes a retry the same invocation, and rewriting them would turn
// an unresolved write into a second write.
func (c *Client) adopt(ack wire.BindAck, credential [32]byte) error {
	expires := time.Unix(int64(ack.ExpiresAt), 0)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cfg.PinnedSession != nil && *c.cfg.PinnedSession != ack.Session {
		return ErrSessionConflict
	}
	switch {
	case c.bound == nil:
		c.bound = &binding{session: ack.Session, expiresAt: expires, credential: credential, epoch: 1}
	case c.bound.session == ack.Session:
		// A rebind of the same session refreshes its validity.
		c.bound.credential = credential
		if expires.After(c.bound.expiresAt) {
			c.bound.expiresAt = expires
		}
	case c.bound.credential == credential:
		// One credential named two sessions: the endpoint is not
		// consistent about this client's identity. Fail closed.
		return ErrSessionConflict
	default:
		c.bound = &binding{session: ack.Session, expiresAt: expires, credential: credential, epoch: c.bound.epoch + 1}
		c.Rollovers++
	}
	return nil
}

// drop closes and forgets the connection so the next call reconnects.
func (c *Client) drop() {
	c.mu.Lock()
	conn, control := c.conn, c.control
	c.conn, c.control = nil, nil
	if conn != nil {
		c.Reconnects++
	}
	c.mu.Unlock()
	if control != nil {
		_ = control.Close()
	}
	if conn != nil {
		_ = conn.CloseWithError(0, "drop")
	}
}

// Close ends the connection and forgets the binding; a later Connect
// negotiates and binds afresh.
func (c *Client) Close() {
	c.mu.Lock()
	conn, control := c.conn, c.control
	c.conn, c.control, c.bound = nil, nil, nil
	c.mu.Unlock()
	if control != nil {
		_ = control.Close()
	}
	if conn != nil {
		_ = conn.CloseWithError(0, "close")
	}
}

// Do sends an invocation's frame on a fresh stream and returns the
// outcome. A stream reset or a timeout is an explicit unknown outcome,
// never a silent success; the connection is dropped so the next call
// warm-reconnects with the same identity.
func (c *Client) Do(ctx context.Context, frame []byte) (Outcome, error) {
	select {
	case c.sem <- struct{}{}:
		defer func() { <-c.sem }()
	default:
		return Outcome{}, ErrPoolExhausted
	}
	conn, err := c.connect(ctx)
	if err != nil {
		return Outcome{}, err
	}
	streamCtx, cancel := context.WithTimeout(ctx, c.cfg.FrameTimeout)
	defer cancel()
	stream, err := conn.OpenStreamSync(streamCtx)
	if err != nil {
		c.drop()
		return Outcome{Unknown: true}, nil
	}
	if err := writeFrame(stream, frame); err != nil {
		c.drop()
		return Outcome{Unknown: true}, nil
	}
	_ = stream.Close()
	resp, err := readOneFrame(streamCtx, stream)
	if err != nil {
		// The request was sent but no response was established: a reset,
		// a timeout or a dropped connection is an explicit unknown
		// outcome, never a silent success. Drop for a warm reconnect.
		c.drop()
		return Outcome{Unknown: true}, nil
	}
	msg, err := wire.Decode(resp)
	if err != nil {
		return Outcome{}, ErrProtocol
	}
	response, ok := msg.(wire.Response)
	if !ok {
		return Outcome{}, ErrProtocol
	}
	if response.Tag == wire.OutcomePending || response.Tag == wire.OutcomeUnknown {
		return Outcome{Unknown: true}, nil
	}
	return Outcome{Response: &response}, nil
}
