package bench

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// EdgeArm drives what an API server drives: an etcd v3 client over
// mutual TLS into `kine-coord`, through the pinned bridge and the
// coord:// backend behind it.
//
// Every mutation here is the guarded transaction the API server writes.
// That is not a stylistic choice: the bridge accepts the compare shapes
// the API server emits and nothing else, so a benchmark that wrote a
// plain Put would be measuring an error path.
type EdgeArm struct {
	endpoint string
	tlsConf  *tls.Config
	deadline time.Duration

	mu      sync.Mutex
	clients []*clientv3.Client
}

// EdgeConfig is the API server's side of the storage edge, as
// `coord-harness` wrote it.
type EdgeConfig struct {
	// Endpoint is `https://host:port`.
	Endpoint string
	// ServerName the edge's certificate carries.
	ServerName string
	// ClientCertificate and ClientKey are the authorized API-server
	// identity.
	ClientCertificate string
	ClientKey         string
	// ServerCA is the authority the edge is verified against.
	ServerCA string
	// Deadline bounds one operation.
	Deadline time.Duration
}

// NewEdgeArm prepares the arm.
func NewEdgeArm(cfg EdgeConfig) (*EdgeArm, error) {
	certificate, err := tls.LoadX509KeyPair(cfg.ClientCertificate, cfg.ClientKey)
	if err != nil {
		return nil, fmt.Errorf("client identity: %w", err)
	}
	roots := x509.NewCertPool()
	pem, err := os.ReadFile(cfg.ServerCA)
	if err != nil {
		return nil, fmt.Errorf("edge CA: %w", err)
	}
	if !roots.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("edge CA %s holds no certificate", cfg.ServerCA)
	}
	return &EdgeArm{
		endpoint: cfg.Endpoint,
		tlsConf: &tls.Config{
			Certificates: []tls.Certificate{certificate},
			RootCAs:      roots,
			ServerName:   cfg.ServerName,
			MinVersion:   tls.VersionTLS13,
		},
		deadline: cfg.Deadline,
	}, nil
}

// Name is the arm's name in a report.
func (a *EdgeArm) Name() string { return "edge" }

// Caller opens one etcd client. Each gets its own connection, as an API
// server and each of its controllers would.
func (a *EdgeArm) Caller(index int) (Caller, error) {
	cli, err := a.dial()
	if err != nil {
		return nil, err
	}
	return &edgeCaller{cli: cli, seen: map[string]int64{}}, nil
}

func (a *EdgeArm) dial() (*clientv3.Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{a.endpoint},
		TLS:         a.tlsConf,
		DialTimeout: 30 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	a.clients = append(a.clients, cli)
	a.mu.Unlock()
	return cli, nil
}

// Watch observes the workload prefix as an informer would.
func (a *EdgeArm) Watch(ctx context.Context, prefix string) (Watcher, error) {
	cli, err := a.dial()
	if err != nil {
		return nil, err
	}
	w := &edgeWatcher{at: map[int64]time.Time{}}
	channel := cli.Watch(ctx, prefix, clientv3.WithPrefix())
	go func() {
		for response := range channel {
			now := time.Now()
			w.mu.Lock()
			for _, event := range response.Events {
				if event.Kv != nil {
					w.at[event.Kv.ModRevision] = now
				}
			}
			w.mu.Unlock()
		}
	}()
	return w, nil
}

// Stages states what this arm cannot see. The Go codec, the native
// exchange, the credential and the invocation trace all happen inside
// `kine-coord`; an etcd client outside it measures its own round trip
// and nothing finer, and a number invented here would be that round
// trip wearing another stage's name. The backend arm is where those
// stages are measured.
func (a *EdgeArm) Stages(operations uint64) Stages {
	return Stages{
		Codec:       Missing(NotOnThisSide),
		Native:      Missing(NotOnThisSide),
		Credentials: Credentials{Operations: operations, Absent: NotOnThisSide},
		Commands:    Commands{Operations: operations, Absent: NotOnThisSide},
	}
}

// Close ends every client this arm opened.
func (a *EdgeArm) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, cli := range a.clients {
		_ = cli.Close()
	}
	a.clients = nil
}

// edgeCaller is one etcd client with the revisions it has seen, which is
// the cache an API server keeps.
type edgeCaller struct {
	cli  *clientv3.Client
	mu   sync.Mutex
	seen map[string]int64
}

func (c *edgeCaller) revision(key string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seen[key]
}

func (c *edgeCaller) remember(key string, revision int64) {
	c.mu.Lock()
	c.seen[key] = revision
	c.mu.Unlock()
}

func (c *edgeCaller) Do(ctx context.Context, op Operation) Answer {
	switch op.Kind {
	case KindGet:
		got, err := c.cli.Get(ctx, op.Key)
		if err != nil {
			return Answer{Outcome: outcomeOf(err)}
		}
		if len(got.Kvs) == 1 {
			c.remember(op.Key, got.Kvs[0].ModRevision)
		}
		return Answer{Outcome: Established}
	case KindScan:
		_, err := c.cli.Get(ctx, op.From,
			clientv3.WithRange(op.To), clientv3.WithLimit(op.Limit))
		if err != nil {
			return Answer{Outcome: outcomeOf(err)}
		}
		return Answer{Outcome: Established}
	default:
		return c.write(ctx, op)
	}
}

// write is the create-or-guarded-update the API server performs: one
// transaction, with the else-branch returning what is current so a
// failed guard needs no second round trip.
func (c *edgeCaller) write(ctx context.Context, op Operation) Answer {
	guard := clientv3.Compare(clientv3.ModRevision(op.Key), "=", c.revision(op.Key))
	done, err := c.cli.Txn(ctx).
		If(guard).
		Then(clientv3.OpPut(op.Key, string(op.Value))).
		Else(clientv3.OpGet(op.Key)).
		Commit()
	if err != nil {
		return Answer{Outcome: outcomeOf(err)}
	}
	if !done.Succeeded {
		for _, response := range done.Responses {
			if got := response.GetResponseRange(); got != nil && len(got.Kvs) == 1 {
				c.remember(op.Key, got.Kvs[0].ModRevision)
			}
		}
		// A guard that did not hold is the real outcome of a contended
		// key, not a refusal.
		return Answer{Outcome: Established, Revision: done.Header.Revision}
	}
	c.remember(op.Key, done.Header.Revision)
	return Answer{Outcome: Established, Revision: done.Header.Revision, Wrote: true}
}

func (c *edgeCaller) Close() {}

type edgeWatcher struct {
	mu sync.Mutex
	at map[int64]time.Time
}

func (w *edgeWatcher) DeliveredAt(revision int64) (time.Time, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	at, ok := w.at[revision]
	return at, ok
}

func (w *edgeWatcher) Close() {}
