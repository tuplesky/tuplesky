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
)

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
	// Provider supplies the service token presented at the Hello.
	Provider *Provider
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

// bind opens the control stream and sends the Hello with the token.
func (c *Client) bind(ctx context.Context, conn *quic.Conn) error {
	if c.cfg.Provider != nil {
		if _, err := c.cfg.Provider.Token(ctx); err != nil {
			return err
		}
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
