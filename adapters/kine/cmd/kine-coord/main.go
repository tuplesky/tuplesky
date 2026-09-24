// kine-coord is the frozen Kine build containing the coord:// adapter: the
// pinned Kine server bridge and driver registry, the native backend, and
// the secured API-server edge of Section 3.1. No SQL driver, TTL worker
// or plaintext listener is linked.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
	kinetls "github.com/k3s-io/kine/pkg/tls"
	"google.golang.org/grpc"

	"github.com/tuplesky/tuplesky/adapters/kine/edge"

	// Register the coord:// driver.
	_ "github.com/tuplesky/tuplesky/adapters/kine/driver"
)

type multi []string

func (m *multi) String() string     { return strings.Join(*m, ",") }
func (m *multi) Set(v string) error { *m = append(*m, v); return nil }

func main() {
	var allowed multi
	endpoint := flag.String("endpoint", "", "coord://frontend:port?cluster=..&domain=..&namespace=..&assertion=..&sts=..&audience=.. (never logged)")
	caFile := flag.String("ca-file", "", "CA that issued the frontend's TLS identity")
	listener := flag.String("listener", "", "unix:///path/kine.sock or tls://host:port")
	certFile := flag.String("server-cert-file", "", "tls edge: Kine's server certificate")
	keyFile := flag.String("server-key-file", "", "tls edge: Kine's server key")
	clientCA := flag.String("client-ca-file", "", "tls edge: CA issuing API-server client identities")
	flag.Var(&allowed, "allowed-client", "tls edge: authorized API-server identity (repeatable)")
	skipVerify := flag.Bool("skip-verify", false, "refused; present so the flag is an explicit error")
	notify := flag.Duration("notify-interval", 5*time.Second, "watch progress interval handed to the bridge")
	version := flag.String("emulated-etcd-version", "3.5.13", "etcd version reported by Status")
	flag.Parse()

	if err := run(*endpoint, *caFile, edge.Config{
		Listener:       *listener,
		CertFile:       *certFile,
		KeyFile:        *keyFile,
		ClientCAFile:   *clientCA,
		AllowedClients: allowed,
		SkipVerify:     *skipVerify,
	}, *notify, *version); err != nil {
		fmt.Fprintln(os.Stderr, "kine-coord:", err)
		os.Exit(1)
	}
}

func run(endpoint, caFile string, edgeCfg edge.Config, notify time.Duration, version string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if endpoint == "" {
		return fmt.Errorf("--endpoint is required")
	}
	wg := &sync.WaitGroup{}
	_, backend, err := drivers.New(ctx, wg, &drivers.Config{
		Endpoint:         endpoint,
		BackendTLSConfig: kinetls.Config{CAFile: caFile},
	})
	if err != nil {
		// The DSN is not echoed: it names the credential file.
		return fmt.Errorf("driver: %w", err)
	}
	ln, err := edge.Listen(ctx, edgeCfg)
	if err != nil {
		return fmt.Errorf("edge: %w", err)
	}
	grpcServer := grpc.NewServer(ln.ServerOptions...)
	server.New(backend, ln.Scheme, notify, version).Register(grpcServer)
	if err := backend.Start(ctx); err != nil {
		_ = ln.Close()
		return fmt.Errorf("start: %w", err)
	}
	fmt.Fprintln(os.Stderr, "kine-coord: serving", ln.Endpoint)
	go func() {
		<-ctx.Done()
		// Closing the backend ends every watch and pending synchronization
		// wait before the bridge's streams are stopped.
		if c, ok := backend.(interface{ Close() }); ok {
			c.Close()
		}
		grpcServer.GracefulStop()
	}()
	if err := grpcServer.Serve(ln); err != nil {
		return err
	}
	wg.Wait()
	return nil
}
