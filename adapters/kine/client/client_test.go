package client

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/quic-go/quic-go"
	"github.com/tuplesky/tuplesky/adapters/kine/wire"
)

// testCert makes a self-signed TLS 1.3 certificate for the server, and a
// RootCAs pool the client validates against (real trust/origin binding).
func testCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "frontend"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"frontend.local"},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, _ := x509.MarshalPKCS8PrivateKey(key)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	parsed, _ := x509.ParseCertificate(der)
	pool.AddCert(parsed)
	return cert, pool
}

// serverBehavior decides how the server answers each request stream.
type serverBehavior struct {
	// reset: reset the response stream instead of answering.
	reset atomic.Bool
	// hang: never answer (client should time out).
	hang atomic.Bool
	// pending: answer with a Pending outcome (unknown).
	pending atomic.Bool
	// requests seen.
	requests atomic.Int64
	// expectToken: when non-empty, a Bind must present exactly this token
	// to be acknowledged with session; any other Bind is answered with a
	// Close.
	expectToken string
	session     [16]byte
	binds       atomic.Int64
	// refuseHello answers the negotiation with a Close instead of an ack.
	refuseHello atomic.Bool
}

// runServer starts an in-process quic-go frontend. It reads the control
// Hello, then answers each request stream per behavior.
func runServer(t *testing.T, cert tls.Certificate, b *serverBehavior) (string, func()) {
	t.Helper()
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := quic.Listen(udp, &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{ALPNNativeAPI},
		MinVersion:   tls.VersionTLS13,
	}, &quic.Config{MaxIdleTimeout: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			conn, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			go serveConn(ctx, conn, b)
		}
	}()
	return udp.LocalAddr().String(), func() {
		cancel()
		_ = ln.Close()
		_ = udp.Close()
	}
}

func serveConn(ctx context.Context, conn *quic.Conn, b *serverBehavior) {
	first := true
	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		if first {
			first = false
			// Answer the Hello with the lane it declared, as the native
			// endpoint does, and leave the control stream open.
			go func(s *quic.Stream) {
				frame, err := readFramePayload(s)
				if err != nil {
					return
				}
				msg, err := wire.Decode(frame)
				if err != nil {
					return
				}
				hello, ok := msg.(wire.Hello)
				if !ok {
					return
				}
				if b.refuseHello.Load() {
					out, _ := wire.Encode(wire.Close{Code: 2, Reason: []byte("lane")})
					_, _ = s.Write(out)
					return
				}
				out, _ := wire.Encode(wire.HelloAck{Capabilities: hello.Capabilities, MaxInflight: 64})
				_, _ = s.Write(out)
			}(stream)
			continue
		}
		go func(s *quic.Stream) {
			frame, err := readFramePayload(s)
			if err != nil {
				return
			}
			if frame.Kind == uint16(wire.KindBind) {
				b.binds.Add(1)
				bind, err := wire.DecodeBind(frame)
				var out []byte
				if err == nil && b.expectToken != "" && string(bind.Token) == b.expectToken {
					out, _ = wire.EncodeBindAck(wire.BindAck{
						Session:        b.session,
						ExpiresAt:      uint64(time.Now().Add(time.Hour).Unix()),
						Scope:          1,
						RuleGeneration: 1,
					})
				} else {
					out, _ = wire.Encode(wire.Close{Code: 2, Reason: []byte("bind refused")})
				}
				_, _ = s.Write(out)
				_ = s.Close()
				return
			}
			b.requests.Add(1)
			msg, err := wire.Decode(frame)
			if err != nil {
				return
			}
			req, ok := msg.(wire.Request)
			if !ok {
				return
			}
			if b.hang.Load() {
				<-ctx.Done()
				return
			}
			if b.reset.Load() {
				s.CancelWrite(7)
				return
			}
			var resp wire.Response
			resp.CommandID = req.RetryKey.ClusterID16to32()
			if b.pending.Load() {
				resp.Tag = wire.OutcomePending
			} else {
				resp.Tag = wire.OutcomeOk
				resp.HasRevision = true
				resp.Revision = req.RetryKey.RequestSequence
				resp.Result = []byte("ok")
			}
			out, _ := wire.Encode(resp)
			_, _ = s.Write(out)
			_ = s.Close()
		}(stream)
	}
}

func readFramePayload(stream *quic.Stream) (wire.Frame, error) {
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

func clientTLS(pool *x509.CertPool) *tls.Config {
	return &tls.Config{
		RootCAs:    pool,
		ServerName: "frontend.local",
		NextProtos: []string{ALPNNativeAPI},
		MinVersion: tls.VersionTLS13,
	}
}

func instance() *Instance {
	return NewInstance(InstanceConfig{
		Cluster:  [16]byte{1},
		Domain:   [16]byte{2},
		Session:  [16]byte{3},
		Instance: [16]byte{4},
	})
}

func command(seq uint64) [32]byte {
	var c [32]byte
	c[0] = byte(seq)
	return c
}

func TestUnaryRequestOverRealTLS(t *testing.T) {
	cert, pool := testCert(t)
	var b serverBehavior
	addr, stop := runServer(t, cert, &b)
	defer stop()
	c := New(addr, Config{TLS: clientTLS(pool), Cluster: [16]byte{1}, Domain: [16]byte{2}, FrameTimeout: 3 * time.Second})
	inst := instance()
	inv, err := inst.Allocate([]byte("put"), command(1), 0)
	if err != nil {
		t.Fatal(err)
	}
	out, err := c.Do(context.Background(), inv.Frame)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if out.Unknown || out.Response == nil {
		t.Fatalf("expected established response, got %+v", out)
	}
	if out.Response.Revision != 1 || string(out.Response.Result) != "ok" {
		t.Fatalf("unexpected response %+v", out.Response)
	}
	if b.requests.Load() != 1 {
		t.Fatalf("server saw %d requests", b.requests.Load())
	}
}

func TestResetAndTimeoutAreUnknownThenWarmReconnect(t *testing.T) {
	cert, pool := testCert(t)
	var b serverBehavior
	addr, stop := runServer(t, cert, &b)
	defer stop()
	c := New(addr, Config{TLS: clientTLS(pool), Cluster: [16]byte{1}, Domain: [16]byte{2}, FrameTimeout: 500 * time.Millisecond})
	inst := instance()

	// A stream reset is an explicit unknown outcome.
	b.reset.Store(true)
	inv, _ := inst.Allocate([]byte("a"), command(1), 0)
	out, err := c.Do(context.Background(), inv.Frame)
	if err != nil {
		t.Fatalf("reset Do: %v", err)
	}
	if !out.Unknown {
		t.Fatal("reset should be unknown")
	}
	// A timeout is an explicit unknown outcome.
	b.reset.Store(false)
	b.hang.Store(true)
	inv2, _ := inst.Allocate([]byte("b"), command(2), 0)
	out, err = c.Do(context.Background(), inv2.Frame)
	if err != nil {
		t.Fatalf("timeout Do: %v", err)
	}
	if !out.Unknown {
		t.Fatal("timeout should be unknown")
	}
	// Warm reconnect: the same invocation identity resolves after the
	// server recovers.
	b.hang.Store(false)
	reconnectsBefore := c.Reconnects
	retry, err := inst.Retry(inv2.Sequence, []byte("b"), command(2), 0)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if retry.Sequence != inv2.Sequence {
		t.Fatal("retry changed the sequence")
	}
	out, err = c.Do(context.Background(), retry.Frame)
	if err != nil {
		t.Fatalf("warm Do: %v", err)
	}
	if out.Unknown || out.Response == nil {
		t.Fatalf("warm reconnect should establish, got %+v", out)
	}
	if c.Reconnects <= reconnectsBefore-1 {
		t.Fatal("expected a reconnect after the drops")
	}
	// A pending answer is unknown, resolvable by identity.
	b.pending.Store(true)
	inv3, _ := inst.Allocate([]byte("c"), command(3), 0)
	out, _ = c.Do(context.Background(), inv3.Frame)
	if !out.Unknown {
		t.Fatal("pending should be unknown")
	}
	resolveFrame, err := inst.ResolveFrame(inv3)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := wire.NextFrame(resolveFrame); err != nil {
		t.Fatalf("resolve frame: %v", err)
	}
}

func TestPayloadConflictAndUnknownSequence(t *testing.T) {
	inst := instance()
	inv, err := inst.Allocate([]byte("x"), command(1), 0)
	if err != nil {
		t.Fatal(err)
	}
	// The same payload rebuilds identically.
	again, err := inst.Retry(inv.Sequence, []byte("x"), command(1), 0)
	if err != nil || string(again.Frame) != string(inv.Frame) {
		t.Fatal("retry with the same payload must reproduce the frame")
	}
	// A different command id under the sequence is a conflict.
	if _, err := inst.Retry(inv.Sequence, []byte("y"), command(9), 0); err != ErrPayloadConflict {
		t.Fatalf("expected payload conflict, got %v", err)
	}
	// An unallocated sequence.
	if _, err := inst.Retry(99, []byte("z"), command(9), 0); err != ErrUnknownSequence {
		t.Fatalf("expected unknown sequence, got %v", err)
	}
}

func TestStreamPoolIsBounded(t *testing.T) {
	cert, pool := testCert(t)
	var b serverBehavior
	b.hang.Store(true)
	addr, stop := runServer(t, cert, &b)
	defer stop()
	c := New(addr, Config{TLS: clientTLS(pool), Cluster: [16]byte{1}, Domain: [16]byte{2}, Limits: PoolLimits{MaxStreams: 2}, FrameTimeout: 2 * time.Second})
	inst := instance()
	// Fill the two stream slots with hanging requests, then a third is
	// refused rather than opening more.
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		inv, _ := inst.Allocate([]byte("h"), command(uint64(i+1)), 0)
		wg.Add(1)
		go func(f []byte) {
			defer wg.Done()
			_, _ = c.Do(context.Background(), f)
		}(inv.Frame)
	}
	time.Sleep(300 * time.Millisecond)
	inv, _ := inst.Allocate([]byte("h"), command(3), 0)
	if _, err := c.Do(context.Background(), inv.Frame); err != ErrPoolExhausted {
		t.Fatalf("expected pool exhausted, got %v", err)
	}
	stop()
	wg.Wait()
}

type staticTokens string

func (s staticTokens) Token(context.Context) (string, error) { return string(s), nil }

// A configured token source presents the token once per connection in a
// Bind frame and learns the acknowledged session; a refused binding
// never yields a usable connection.
func TestBindPresentsTokenOnceAndLearnsSession(t *testing.T) {
	cert, pool := testCert(t)
	b := &serverBehavior{expectToken: "svc-token", session: [16]byte{7, 7}}
	addr, stop := runServer(t, cert, b)
	defer stop()
	c := New(addr, Config{TLS: clientTLS(pool), Tokens: staticTokens("svc-token"), FrameTimeout: 2 * time.Second})
	if _, ok := c.Session(); ok {
		t.Fatal("session before binding")
	}
	inst := instance()
	for seq := uint64(1); seq <= 2; seq++ {
		inv, _ := inst.Allocate([]byte("op"), command(seq), 1000)
		out, err := c.Do(context.Background(), inv.Frame)
		if err != nil || out.Unknown {
			t.Fatalf("seq %d: %v %+v", seq, err, out)
		}
	}
	if got := b.binds.Load(); got != 1 {
		t.Fatalf("binds %d, want 1 per connection", got)
	}
	if s, ok := c.Session(); !ok || s != b.session {
		t.Fatalf("session %v %v", s, ok)
	}
	c.drop()
	wrong := New(addr, Config{TLS: clientTLS(pool), Tokens: staticTokens("other"), FrameTimeout: 2 * time.Second})
	inv, _ := inst.Allocate([]byte("op"), command(3), 1000)
	if _, err := wrong.Do(context.Background(), inv.Frame); !errors.Is(err, ErrBindRejected) {
		t.Fatalf("refused binding: %v", err)
	}
	if _, ok := wrong.Session(); ok {
		t.Fatal("session after refusal")
	}
}

// AllocateFor derives the payload from the allocated retry key and binds
// the resulting identity; a failing build consumes no sequence.
func TestAllocateForBindsDerivedIdentity(t *testing.T) {
	inst := instance()
	_, err := inst.AllocateFor(1, func(wire.RetryKey) ([]byte, [32]byte, error) {
		return nil, [32]byte{}, errors.New("no")
	})
	if err == nil {
		t.Fatal("build error swallowed")
	}
	var seen wire.RetryKey
	inv, err := inst.AllocateFor(1, func(key wire.RetryKey) ([]byte, [32]byte, error) {
		seen = key
		return []byte("p"), command(9), nil
	})
	if err != nil || inv.Sequence != 1 || seen.RequestSequence != 1 || seen.ClientInstance != [16]byte{4} {
		t.Fatalf("%v %+v %+v", err, inv, seen)
	}
	if _, err := inst.Retry(1, []byte("p"), command(8), 1); !errors.Is(err, ErrPayloadConflict) {
		t.Fatalf("conflict: %v", err)
	}
	if _, err := inst.Retry(1, []byte("p"), command(9), 1); err != nil {
		t.Fatal(err)
	}
}
