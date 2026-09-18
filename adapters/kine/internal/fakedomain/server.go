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
	// Admit, when set, decides what a Bind presenting `token` is
	// acknowledged with, so a test can drive real credential rotation:
	// a later credential may name a later session. It takes precedence
	// over Token and Session.
	Admit func(token string) (session [16]byte, expiresAt time.Time, ok bool)
	// Cert is the frontend's TLS identity.
	Cert tls.Certificate
}

// Logged is one frame the domain handled.
type Logged struct {
	// Kind: "bind", "request" or "resolve".
	Kind string
	// Op names the logical operation of a request.
	Op string
	// Session of the retry key (empty for a bind).
	Session [16]byte
	// Sequence of the retry key.
	Sequence uint64
	// CommandID the frame named (requests and resolutions).
	CommandID [32]byte
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

	// PendingOnce answers the next Request with a Pending outcome (the
	// result is still retained for resolution).
	PendingOnce atomic.Bool
	// ForgetOnce answers the next ResolveRequest with Unknown.
	ForgetOnce atomic.Bool
	// ForgetAll answers every ResolveRequest with Unknown (the identity
	// is lost at the endpoint).
	ForgetAll atomic.Bool
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
	s := &Server{cfg: cfg, model: NewModel(), udp: udp, ln: ln, cancel: cancel, retained: map[wire.RetryKey]retained{}, conns: map[*quic.Conn]struct{}{}}
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
			go s.negotiate(stream)
			continue
		}
		go s.handle(stream)
	}
}

// negotiate answers the Hello on the control stream and leaves it open,
// as the native endpoint does: a Close frame travels on it.
func (s *Server) negotiate(stream *quic.Stream) {
	frame, err := readFrame(stream)
	if err != nil {
		return
	}
	msg, err := wire.Decode(frame)
	if err != nil {
		return
	}
	hello, ok := msg.(wire.Hello)
	if !ok {
		out, _ := wire.Encode(wire.Close{Code: 1, Reason: []byte("first frame not hello")})
		_, _ = stream.Write(out)
		return
	}
	// Exactly one lane capability (the 0x0010..0x0013 block) declares the
	// connection's lane, as the native endpoint requires.
	granted := make([]uint16, 0, len(hello.Capabilities))
	for _, capability := range hello.Capabilities {
		if capability >= 0x0010 && capability <= 0x0013 {
			granted = append(granted, capability)
		}
	}
	if len(granted) != 1 {
		out, _ := wire.Encode(wire.Close{Code: 2, Reason: []byte("lane")})
		_, _ = stream.Write(out)
		return
	}
	out, _ := wire.Encode(wire.HelloAck{Capabilities: granted, MaxInflight: 64})
	_, _ = stream.Write(out)
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
		session, expires, ok := s.admit(err, bind)
		if ok {
			out, _ = wire.EncodeBindAck(wire.BindAck{Session: session, ExpiresAt: uint64(expires.Unix()), Scope: 1, RuleGeneration: 1})
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
		default:
			return
		}
	}
	_, _ = stream.Write(out)
}

// admit decides what a binding request establishes.
func (s *Server) admit(decodeErr error, bind wire.Bind) ([16]byte, time.Time, bool) {
	if decodeErr != nil {
		return [16]byte{}, time.Time{}, false
	}
	if s.cfg.Admit != nil {
		return s.cfg.Admit(string(bind.Token))
	}
	if s.cfg.Token != "" && string(bind.Token) == s.cfg.Token {
		return s.cfg.Session, time.Now().Add(time.Hour), true
	}
	return [16]byte{}, time.Time{}, false
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
			s.record(Logged{Kind: "request", Op: "conflict", Session: req.RetryKey.SessionID, Sequence: req.RetryKey.RequestSequence, CommandID: commandID})
			return encodeResponse(wire.Response{CommandID: commandID, Tag: wire.OutcomeErr, Code: 0x0001, Detail: []byte("identity conflict")})
		}
		s.record(Logged{Kind: "request", Op: "retained", Session: req.RetryKey.SessionID, Sequence: req.RetryKey.RequestSequence, CommandID: commandID})
		return prior.response
	}
	logical, err := wire.DecodeLogical(req.Logical)
	if err != nil {
		s.record(Logged{Kind: "request", Op: "malformed", Session: req.RetryKey.SessionID, Sequence: req.RetryKey.RequestSequence, CommandID: commandID})
		return encodeResponse(wire.Response{CommandID: commandID, Tag: wire.OutcomeErr, Code: 0x0003, Detail: []byte("malformed")})
	}
	result, mutated := s.model.Apply(logical)
	s.record(Logged{Kind: "request", Op: opName(logical.Op), Session: req.RetryKey.SessionID, Sequence: req.RetryKey.RequestSequence, CommandID: commandID, Executed: true})
	payload, _ := result.Encode()
	resp := wire.Response{CommandID: commandID, Tag: wire.OutcomeOk, Result: payload}
	if mutated {
		resp.HasRevision, resp.Revision = true, result.Revision
	}
	encoded := encodeResponse(resp)
	s.mu.Lock()
	s.retained[req.RetryKey] = retained{commandID: commandID, response: encoded}
	s.mu.Unlock()
	if s.PendingOnce.CompareAndSwap(true, false) {
		return encodeResponse(wire.Response{CommandID: commandID, Tag: wire.OutcomePending})
	}
	return encoded
}

func (s *Server) resolve(req wire.ResolveRequest) []byte {
	s.record(Logged{Kind: "resolve", Session: req.RetryKey.SessionID, Sequence: req.RetryKey.RequestSequence, CommandID: req.CommandID})
	if s.ForgetAll.Load() || s.ForgetOnce.CompareAndSwap(true, false) {
		return encodeResponse(wire.Response{CommandID: req.CommandID, Tag: wire.OutcomeUnknown})
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
