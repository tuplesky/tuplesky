package bench

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
	kinetls "github.com/k3s-io/kine/pkg/tls"

	"github.com/tuplesky/tuplesky/adapters/kine/backend"
	"github.com/tuplesky/tuplesky/adapters/kine/driver"
)

// BackendArm drives the coord:// backend directly: the Go codec, the Go
// QUIC client and the workload credential, with the etcd edge taken
// away. It is the composition the edge arm is measured against, and the
// only one of the two that can see the stages beneath itself.
type BackendArm struct {
	dsn    string
	caFile string

	mu      sync.Mutex
	opened  []*driver.Opened
	codec   Samples
	native  Samples
	invokes uint64
	resolve uint64
	byKind  map[string]uint64
}

// NewBackendArm prepares the arm. Nothing connects until a caller is
// built, because a connection made before the schedule starts is a
// connection the schedule did not pay for.
func NewBackendArm(dsn, caFile string) *BackendArm {
	return &BackendArm{dsn: dsn, caFile: caFile, byKind: map[string]uint64{}}
}

// Name is the arm's name in a report.
func (a *BackendArm) Name() string { return "backend" }

func (a *BackendArm) observe(e backend.Event) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.invokes++
	a.resolve += uint64(e.Resolves)
	a.byKind[e.Kind]++
	if e.CodecNs > 0 {
		a.codec.Add(time.Duration(e.CodecNs))
	}
	if e.NativeNs > 0 {
		a.native.Add(time.Duration(e.NativeNs))
	}
}

// Caller opens one more backend: its own client instance, its own
// session, its own credential. Kine runs one per process; a measurement
// that shared one would report a queue rather than a path.
func (a *BackendArm) Caller(index int) (Caller, error) {
	opened, err := driver.Open(&drivers.Config{
		DataSourceName:   a.dsn,
		BackendTLSConfig: kinetls.Config{CAFile: a.caFile},
	}, a.observe)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := opened.Backend.Start(ctx); err != nil {
		opened.Backend.Close()
		return nil, err
	}
	a.mu.Lock()
	a.opened = append(a.opened, opened)
	a.mu.Unlock()
	return &backendCaller{backend: opened.Backend, seen: map[string]int64{}}, nil
}

// Watch observes the workload prefix through the same backend Kine's
// bridge watches through.
func (a *BackendArm) Watch(ctx context.Context, prefix string) (Watcher, error) {
	a.mu.Lock()
	if len(a.opened) == 0 {
		a.mu.Unlock()
		return nil, fmt.Errorf("a watch needs a caller to watch through")
	}
	b := a.opened[0].Backend
	a.mu.Unlock()
	end := prefixEnd(prefix)
	result := b.Watch(ctx, prefix, end, 0)
	w := &backendWatcher{at: map[int64]time.Time{}}
	go func() {
		for batch := range result.Events {
			now := time.Now()
			w.mu.Lock()
			for _, event := range batch {
				if event.KV != nil {
					w.at[event.KV.ModRevision] = now
				}
			}
			w.mu.Unlock()
		}
	}()
	return w, nil
}

// Stages renders what this arm saw beneath itself.
func (a *BackendArm) Stages(operations uint64) Stages {
	a.mu.Lock()
	defer a.mu.Unlock()
	exchanges := 0
	for _, opened := range a.opened {
		exchanges += opened.Credentials.Exchanges
	}
	byKind := map[string]uint64{}
	for k, v := range a.byKind {
		byKind[k] = v
	}
	return Stages{
		Codec:  a.codec.Percentiles(),
		Native: a.native.Percentiles(),
		Credentials: Credentials{
			Exchanges:  exchanges,
			Operations: operations,
		},
		Commands: Commands{
			Invocations: a.invokes,
			Operations:  operations,
			Resolutions: a.resolve,
			ByKind:      byKind,
		},
	}
}

// Close ends every backend this arm opened.
func (a *BackendArm) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, opened := range a.opened {
		opened.Backend.Close()
	}
	a.opened = nil
}

// backendCaller is one backend, with the revisions it has seen. An API
// server keeps the same memory in its cache: a guarded update names the
// revision the previous answer reported, and a failed guard hands back
// the current one in the same answer, so no read is needed to retry.
type backendCaller struct {
	backend *backend.Backend
	mu      sync.Mutex
	seen    map[string]int64
}

func (c *backendCaller) revision(key string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seen[key]
}

func (c *backendCaller) remember(key string, revision int64) {
	c.mu.Lock()
	c.seen[key] = revision
	c.mu.Unlock()
}

func (c *backendCaller) Do(ctx context.Context, op Operation) Answer {
	switch op.Kind {
	case KindGet:
		_, kv, err := c.backend.Get(ctx, op.Key, 0, false)
		if err != nil {
			return Answer{Outcome: Refused(reasonOf(err))}
		}
		if kv != nil {
			c.remember(op.Key, kv.ModRevision)
		}
		return Answer{Outcome: Established}
	case KindScan:
		_, _, err := c.backend.List(ctx, op.From, op.To, op.Limit, 0, false)
		if err != nil {
			return Answer{Outcome: Refused(reasonOf(err))}
		}
		return Answer{Outcome: Established}
	default:
		return c.write(ctx, op)
	}
}

// write is the API server's mutation: a create when this caller has
// never written the key, a guarded update otherwise, and on a failed
// guard the current revision the same answer carried -- one native
// command either way, and never a read before the write.
func (c *backendCaller) write(ctx context.Context, op Operation) Answer {
	if revision := c.revision(op.Key); revision > 0 {
		rev, kv, updated, err := c.backend.Update(ctx, op.Key, op.Value, revision, 0)
		if err != nil {
			return Answer{Outcome: Refused(reasonOf(err))}
		}
		if !updated {
			if kv != nil {
				c.remember(op.Key, kv.ModRevision)
			}
			// A guard that did not hold is a real outcome of a
			// contended key, not a refusal: the caller learns the
			// current revision and the next attempt is guarded on it.
			return Answer{Outcome: Established, Revision: rev}
		}
		c.remember(op.Key, rev)
		return Answer{Outcome: Established, Revision: rev, Wrote: true}
	}
	rev, err := c.backend.Create(ctx, op.Key, op.Value, 0)
	if err != nil {
		// The key exists already: this caller had not written it, so it
		// learns the revision with one read and then stays guarded.
		_, kv, getErr := c.backend.Get(ctx, op.Key, 0, false)
		if getErr != nil || kv == nil {
			return Answer{Outcome: Refused(reasonOf(err))}
		}
		c.remember(op.Key, kv.ModRevision)
		return Answer{Outcome: Established, Revision: kv.ModRevision}
	}
	c.remember(op.Key, rev)
	return Answer{Outcome: Established, Revision: rev, Wrote: true}
}

func (c *backendCaller) Close() {}

type backendWatcher struct {
	mu sync.Mutex
	at map[int64]time.Time
}

func (w *backendWatcher) DeliveredAt(revision int64) (time.Time, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	at, ok := w.at[revision]
	return at, ok
}

func (w *backendWatcher) Close() {}

// prefixEnd is the exclusive end of a prefix scan.
func prefixEnd(prefix string) string {
	raw := []byte(prefix)
	for i := len(raw) - 1; i >= 0; i-- {
		if raw[i] < 0xff {
			out := append([]byte{}, raw[:i+1]...)
			out[i]++
			return string(out)
		}
	}
	return ""
}

var _ server.Backend = (*backend.Backend)(nil)
