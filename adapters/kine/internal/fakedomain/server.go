package fakedomain

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"sync"
	"sync/atomic"
	"time"

	quic "github.com/quic-go/quic-go"
	"github.com/tuplesky/tuplesky/adapters/kine/client"
	"github.com/tuplesky/tuplesky/adapters/kine/wire"
)

// Config configures the test domain.
type Config struct {
	// Token is the service token a Bind must present; empty requires no
	// binding.
	Token string
	// Session is the session acknowledged to a correct Bind.
	Session [16]byte
	// Cert is the frontend's TLS identity.
	Cert tls.Certificate
}

// Logged is one frame the domain handled.
type Logged struct {
	// Kind: "bind", "request" or "resolve".
	Kind string
	// Op names the logical operation of a request.
	Op string
	// Sequence of the retry key.
	Sequence uint64
	// Executed says whether the model applied a command (a retained
	// result answers a retry without executing again).
	Executed bool
}

type retained struct {
	commandID [32]byte
	response  []byte
}

// Server is the in-process domain.
type Server struct {
	cfg    Config
	model  *Model
	udp    *net.UDPConn
	ln     *quic.Listener
	cancel context.CancelFunc

	mu       sync.Mutex
	retained map[wire.RetryKey]retained
	log      []Logged
	conns    map[*quic.Conn]struct{}
	watchers map[*watcher]struct{}
	// HoldProgress suppresses progress frames (a source that never says
	// it processed a revision).
	HoldProgress atomic.Bool

	// PendingOnce answers the next Request with a Pending outcome (the
	// result is still retained for resolution).
	PendingOnce atomic.Bool
	// ForgetOnce answers the next ResolveRequest with Unknown.
	ForgetOnce atomic.Bool
	// ForgetAll answers every ResolveRequest with Unknown (the identity
	// is lost at the endpoint).
	ForgetAll atomic.Bool
	// PendingAlways answers every Request and ResolveRequest with Pending
	// after the first execution (an endpoint that never establishes).
	PendingAlways atomic.Bool
	// Binds handled.
	Binds atomic.Int64
}

// SelfSigned makes a TLS 1.3 server identity for `name` and the pool that
// trusts it.
func SelfSigned(name string) (tls.Certificate, *x509.CertPool, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{name},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	cert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	pool := x509.NewCertPool()
	pool.AddCert(parsed)
	return cert, pool, nil
}

// Start listens on a loopback UDP port.
func Start(cfg Config) (*Server, error) {
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return nil, err
	}
	ln, err := quic.Listen(udp, &tls.Config{
		Certificates: []tls.Certificate{cfg.Cert},
		NextProtos:   []string{client.ALPNNativeAPI},
		MinVersion:   tls.VersionTLS13,
	}, &quic.Config{MaxIdleTimeout: 30 * time.Second})
	if err != nil {
		_ = udp.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{cfg: cfg, model: NewModel(), udp: udp, ln: ln, cancel: cancel, retained: map[wire.RetryKey]retained{}, conns: map[*quic.Conn]struct{}{}, watchers: map[*watcher]struct{}{}}
	s.model.published = s.publish
	s.model.compactWatches = s.compactWatches
	go func() {
		for {
			conn, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conns[conn] = struct{}{}
			s.mu.Unlock()
			go s.serve(ctx, conn)
		}
	}()
	return s, nil
}

// DropConnections closes every accepted connection (a frontend restart
// or network loss); the client must reconnect and resume.
func (s *Server) DropConnections() {
	s.mu.Lock()
	conns := s.conns
	s.conns = map[*quic.Conn]struct{}{}
	s.mu.Unlock()
	for conn := range conns {
		_ = conn.CloseWithError(4, "dropped")
	}
}

// Watchers is the number of open watches.
func (s *Server) Watchers() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.watchers)
}

// Addr is the frontend address.
func (s *Server) Addr() string { return s.udp.LocalAddr().String() }

// Model is the domain state.
func (s *Server) Model() *Model { return s.model }

// Close stops the domain.
func (s *Server) Close() {
	s.cancel()
	_ = s.ln.Close()
	_ = s.udp.Close()
}

// Log returns the frames handled so far.
func (s *Server) Log() []Logged {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Logged(nil), s.log...)
}

// ResetLog clears the log.
func (s *Server) ResetLog() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = nil
}

func (s *Server) record(l Logged) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = append(s.log, l)
}

func (s *Server) serve(ctx context.Context, conn *quic.Conn) {
	first := true
	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		if first {
			first = false
			go func() { _, _ = readFrame(stream) }()
			continue
		}
		go s.handle(stream)
	}
}

func (s *Server) handle(stream *quic.Stream) {
	defer func() { _ = stream.Close() }()
	frame, err := readFrame(stream)
	if err != nil {
		return
	}
	var out []byte
	switch frame.Kind {
	case uint16(wire.KindBind):
		s.Binds.Add(1)
		s.record(Logged{Kind: "bind"})
		bind, err := wire.DecodeBind(frame)
		if err == nil && s.cfg.Token != "" && string(bind.Token) == s.cfg.Token {
			out, _ = wire.EncodeBindAck(wire.BindAck{Session: s.cfg.Session, ExpiresAt: uint64(time.Now().Add(time.Hour).Unix()), Scope: 1, RuleGeneration: 1})
		} else {
			out, _ = wire.Encode(wire.Close{Code: 2, Reason: []byte("bind refused")})
		}
	default:
		msg, err := wire.Decode(frame)
		if err != nil {
			return
		}
		switch m := msg.(type) {
		case wire.Request:
			out = s.request(m)
		case wire.ResolveRequest:
			out = s.resolve(m)
		case wire.WatchOpen:
			s.watch(stream, m)
			return
		default:
			return
		}
	}
	_, _ = stream.Write(out)
}

func opName(op wire.LogicalOp) string {
	switch op.(type) {
	case wire.RangeOp:
		return "Range"
	case wire.KineCreateOp:
		return "KineCreate"
	case wire.KineUpdateOp:
		return "KineUpdate"
	case wire.KineDeleteOp:
		return "KineDelete"
	default:
		return "?"
	}
}

func encodeResponse(r wire.Response) []byte {
	out, _ := wire.Encode(r)
	return out
}

func (s *Server) request(req wire.Request) []byte {
	commandID := client.CommandID(req.RetryKey, req.Logical)
	s.mu.Lock()
	prior, seen := s.retained[req.RetryKey]
	s.mu.Unlock()
	if seen {
		if prior.commandID != commandID {
			s.record(Logged{Kind: "request", Op: "conflict", Sequence: req.RetryKey.RequestSequence})
			return encodeResponse(wire.Response{CommandID: commandID, Tag: wire.OutcomeErr, Code: 0x0001, Detail: []byte("identity conflict")})
		}
		s.record(Logged{Kind: "request", Op: "retained", Sequence: req.RetryKey.RequestSequence})
		if s.PendingAlways.Load() {
			return encodeResponse(wire.Response{CommandID: commandID, Tag: wire.OutcomePending})
		}
		return prior.response
	}
	logical, err := wire.DecodeLogical(req.Logical)
	if err != nil {
		s.record(Logged{Kind: "request", Op: "malformed", Sequence: req.RetryKey.RequestSequence})
		return encodeResponse(wire.Response{CommandID: commandID, Tag: wire.OutcomeErr, Code: 0x0003, Detail: []byte("malformed")})
	}
	result, mutated := s.model.Apply(logical)
	s.record(Logged{Kind: "request", Op: opName(logical.Op), Sequence: req.RetryKey.RequestSequence, Executed: true})
	payload, _ := result.Encode()
	resp := wire.Response{CommandID: commandID, Tag: wire.OutcomeOk, Result: payload}
	if mutated {
		resp.HasRevision, resp.Revision = true, result.Revision
	}
	encoded := encodeResponse(resp)
	s.mu.Lock()
	s.retained[req.RetryKey] = retained{commandID: commandID, response: encoded}
	s.mu.Unlock()
	if s.PendingAlways.Load() || s.PendingOnce.CompareAndSwap(true, false) {
		return encodeResponse(wire.Response{CommandID: commandID, Tag: wire.OutcomePending})
	}
	return encoded
}

func (s *Server) resolve(req wire.ResolveRequest) []byte {
	s.record(Logged{Kind: "resolve", Sequence: req.RetryKey.RequestSequence})
	if s.ForgetAll.Load() || s.ForgetOnce.CompareAndSwap(true, false) {
		return encodeResponse(wire.Response{CommandID: req.CommandID, Tag: wire.OutcomeUnknown})
	}
	if s.PendingAlways.Load() {
		return encodeResponse(wire.Response{CommandID: req.CommandID, Tag: wire.OutcomePending})
	}
	s.mu.Lock()
	prior, seen := s.retained[req.RetryKey]
	s.mu.Unlock()
	if !seen || prior.commandID != req.CommandID {
		return encodeResponse(wire.Response{CommandID: req.CommandID, Tag: wire.OutcomeUnknown})
	}
	return prior.response
}

func readFrame(stream *quic.Stream) (wire.Frame, error) {
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 4096)
	for {
		n, err := stream.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
			if frame, _, ferr := wire.NextFrame(buf); ferr == nil {
				return frame, nil
			}
		}
		if err != nil {
			frame, _, ferr := wire.NextFrame(buf)
			if ferr != nil {
				return wire.Frame{}, ferr
			}
			return frame, nil
		}
	}
}

// watcher is one open watch: its filter and an ordered outbound queue.
type watcher struct {
	open   wire.WatchOpen
	stream *quic.Stream
	queue  chan []byte
	done   chan struct{}
	once   sync.Once
	// final is the close frame the domain chose (nil: cancelled).
	final []byte
}

func (w *watcher) matches(key []byte) bool {
	if w.open.RangeEnd == nil {
		return string(key) == string(w.open.Key)
	}
	return string(key) >= string(w.open.Key) && string(key) < string(*w.open.RangeEnd)
}

func (w *watcher) enqueue(frame []byte) bool {
	select {
	case w.queue <- frame:
		return true
	default:
		return false
	}
}

func (w *watcher) close(final []byte) {
	w.once.Do(func() {
		w.final = final
		close(w.done)
	})
}

func closeFrame(id uint64, reason wire.WatchCloseReason, last *uint64) []byte {
	out, _ := wire.Encode(wire.WatchClose{WatchID: id, Reason: reason, LastCompleteRevision: last})
	return out
}

// watch registers the watch and replays under the model lock (an atomic
// replay/live boundary), then pumps the queue to the stream until the
// client cancels or the watch closes.
func (s *Server) watch(stream *quic.Stream, open wire.WatchOpen) {
	s.record(Logged{Kind: "watch-open", Sequence: open.WatchID})
	w := &watcher{open: open, stream: stream, queue: make(chan []byte, 4096), done: make(chan struct{})}
	m := s.model
	m.mu.Lock()
	if open.StartRevision != nil && *open.StartRevision < m.compactFloor {
		m.mu.Unlock()
		_, _ = stream.Write(closeFrame(open.WatchID, wire.WatchCompacted, nil))
		_ = stream.Close()
		return
	}
	if open.StartRevision != nil {
		for _, r := range m.log {
			if r.Revision >= *open.StartRevision {
				s.deliver(w, r)
			}
		}
	}
	s.mu.Lock()
	s.watchers[w] = struct{}{}
	s.mu.Unlock()
	m.mu.Unlock()
	go s.readCancel(w)
	for {
		select {
		case frame := <-w.queue:
			if _, err := stream.Write(frame); err != nil {
				s.remove(w)
				return
			}
		case <-w.done:
			s.remove(w)
			final := w.final
			if final == nil {
				final = closeFrame(open.WatchID, wire.WatchCancelled, nil)
			}
			_, _ = stream.Write(final)
			_ = stream.Close()
			return
		}
	}
}

func (s *Server) readCancel(w *watcher) {
	frame, err := readFrame(w.stream)
	if err != nil {
		w.close(nil)
		return
	}
	if msg, err := wire.Decode(frame); err == nil {
		if _, ok := msg.(wire.WatchClose); ok {
			s.record(Logged{Kind: "watch-cancel", Sequence: w.open.WatchID})
		}
	}
	w.close(nil)
}

func (s *Server) remove(w *watcher) {
	s.mu.Lock()
	delete(s.watchers, w)
	s.mu.Unlock()
}

// deliver queues one revision to a watch: its matching events as one
// complete batch, or a progress marker when the filter excluded it and
// progress was requested. Called under the model lock, so order equals
// revision order.
func (s *Server) deliver(w *watcher, r Revision) {
	var events []wire.Event
	for _, e := range r.Events {
		if w.matches(e.Key) {
			ev := e
			if !w.open.PrevKV {
				ev.PrevValue = nil
			}
			events = append(events, ev)
		}
	}
	var frame []byte
	switch {
	case len(events) > 0:
		frame, _ = wire.Encode(wire.WatchEvents{WatchID: w.open.WatchID, Revision: r.Revision, Events: events, Complete: true})
	case w.open.ProgressNotify && !s.HoldProgress.Load():
		frame, _ = wire.Encode(wire.WatchProgress{WatchID: w.open.WatchID, Revision: r.Revision})
	default:
		return
	}
	if !w.enqueue(frame) {
		last := r.Revision - 1
		w.close(closeFrame(w.open.WatchID, wire.WatchSlowConsumer, &last))
	}
}

// publish fans one new revision out to every open watch (under the model
// lock).
func (s *Server) publish(r Revision) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for w := range s.watchers {
		s.deliver(w, r)
	}
}

// compactWatches closes the watches whose start revision fell below the
// new floor before they caught up: history they still need is gone.
func (s *Server) compactWatches(floor uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for w := range s.watchers {
		if w.open.StartRevision != nil && *w.open.StartRevision < floor && len(w.queue) > 0 {
			w.close(closeFrame(w.open.WatchID, wire.WatchCompacted, nil))
		}
	}
}
