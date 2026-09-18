package client

import (
	"context"
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
)

// TokenSource supplies the service token presented at binding. A
// *Provider is the production source.
type TokenSource interface {
	Token(ctx context.Context) (string, error)
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
	// Limits bounds the pool.
	Limits PoolLimits
	// FrameTimeout bounds one request/response exchange.
	FrameTimeout time.Duration
}

// Client is a bounded QUIC client to one frontend.
type Client struct {
	cfg    Config
	dialFn func(ctx context.Context) (*quic.Conn, error)

	mu   sync.Mutex
	conn *quic.Conn
	sem  chan struct{}
	// session bound by the last acknowledged Bind.
	session *[16]byte
	// Reconnects performed (for tests).
	Reconnects int
}

// New builds a client that dials `addr`.
func New(addr string, cfg Config) *Client {
	if cfg.FrameTimeout == 0 {
		cfg.FrameTimeout = 10 * time.Second
	}
	if cfg.Limits.MaxStreams <= 0 {
		cfg.Limits.MaxStreams = 64
	}
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

// newWithDialer is a test hook injecting a dialer (an in-process server).
func newWithDialer(cfg Config, dial func(ctx context.Context) (*quic.Conn, error)) *Client {
	if cfg.FrameTimeout == 0 {
		cfg.FrameTimeout = 10 * time.Second
	}
	if cfg.Limits.MaxStreams <= 0 {
		cfg.Limits.MaxStreams = 64
	}
	return &Client{cfg: cfg, dialFn: dial, sem: make(chan struct{}, cfg.Limits.MaxStreams)}
}

// connect returns a live connection, dialing and binding (Hello) if
// needed. A binding presents the service token once.
func (c *Client) connect(ctx context.Context) (*quic.Conn, error) {
	c.mu.Lock()
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
	if err := c.bind(ctx, conn); err != nil {
		_ = conn.CloseWithError(2, "bind failed")
		return nil, err
	}
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	return conn, nil
}

// Session is the replicated session the frontend acknowledged at the
// last binding, when a token source is configured and a connection was
// bound.
func (c *Client) Session() ([16]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.session == nil {
		return [16]byte{}, false
	}
	return *c.session, true
}

// Connect establishes and binds the connection now (the backend's Start
// does this so a refused binding fails startup rather than the first
// operation). It is idempotent on a live connection.
func (c *Client) Connect(ctx context.Context) error {
	_, err := c.connect(ctx)
	return err
}

// bind opens the control stream and sends the Hello, then presents the
// service token once in a Bind frame and reads the acknowledgement.
func (c *Client) bind(ctx context.Context, conn *quic.Conn) error {
	var token string
	if c.cfg.Tokens != nil {
		t, err := c.cfg.Tokens.Token(ctx)
		if err != nil {
			return err
		}
		token = t
	}
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return ErrNotConnected
	}
	hello, err := wire.Encode(wire.Hello{
		Role:         wire.RoleClient,
		ClusterID:    c.cfg.Cluster,
		DomainID:     c.cfg.Domain,
		Capabilities: []uint16{0x0011},
	})
	if err != nil {
		return err
	}
	if err := writeFrame(stream, hello); err != nil {
		return err
	}
	// The control stream stays open; the client reads no HelloAck body in
	// this preview (negotiation detail is exercised by the Rust adapter).
	_ = stream.Close()
	if c.cfg.Tokens == nil {
		return nil
	}
	return c.bindSession(ctx, conn, token)
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
		return ErrBindRejected
	}
	ack, err := wire.DecodeBindAck(answer)
	if err != nil {
		return ErrBindRejected
	}
	session := ack.Session
	c.mu.Lock()
	c.session = &session
	c.mu.Unlock()
	return nil
}

// drop closes and forgets the connection so the next call reconnects.
func (c *Client) drop() {
	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	if conn != nil {
		c.Reconnects++
	}
	c.mu.Unlock()
	if conn != nil {
		_ = conn.CloseWithError(0, "drop")
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
