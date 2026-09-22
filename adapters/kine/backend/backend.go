// Package backend implements Kine's `server.Backend` over the native API
// (task-46; design Sections 6.6, 19.5). Every backend method is exactly
// one logical native operation returning the revision and metadata Kine
// needs from one execution point: no remote pre-read, no follow-up
// CurrentRevision call, no SQL or TTL wrapper. A Kine `lease` is a TTL in
// seconds that becomes a hidden per-key binding created atomically with
// the write; the binding identity is derived from the stable invocation
// and never disclosed. Watch, Compact and WaitForSyncTo are task-47 and
// fail explicitly here.
package backend

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/tuplesky/tuplesky/adapters/kine/client"
	"github.com/tuplesky/tuplesky/adapters/kine/wire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Configuration and startup errors.
var (
	// ErrNoClient: no native client.
	ErrNoClient = errors.New("no native client")
	// ErrNoSession: the frontend acknowledged no session and none was
	// configured; the retry key cannot be formed.
	ErrNoSession = errors.New("no session bound")
	// ErrSessionMismatch: the acknowledged session is not the configured
	// one (fail closed: the domain binding would be wrong).
	ErrSessionMismatch = errors.New("bound session differs from the configured session")
	// ErrNotStarted: an operation before Start.
	ErrNotStarted = errors.New("backend not started")
	// ErrWatchUnsupported: watches are task-47.
	ErrWatchUnsupported = status.Error(codes.Unimplemented, "watch is not served by this backend revision (task-47)")
	// ErrCompactUnsupported: compaction conventions are task-47.
	ErrCompactUnsupported = status.Error(codes.Unimplemented, "compaction is not served by this backend revision (task-47)")
)

// Event is one native invocation the backend performed (the trace of an
// operation: exactly one command per backend method).
type Event struct {
	// Op is the backend method ("Get", "Create", ...).
	Op string
	// Kind is the logical operation ("Range", "KineCreate", ...).
	Kind string
	// Sequence of the invocation.
	Sequence uint64
	// Resolves performed for an unknown outcome.
	Resolves int
	// Outcome names the result kind or the error class.
	Outcome string
}

// Config configures a Backend.
type Config struct {
	// Client is the bounded native client to the frontend.
	Client *client.Client
	// Cluster, Domain and Namespace bind this backend to exactly one
	// domain and one tenant namespace.
	Cluster   [16]byte
	Domain    [16]byte
	Namespace [16]byte
	// Session is the replicated session of the retry key. When the client
	// binds a token, the acknowledged session must equal it (nil adopts
	// the acknowledged one); without a token source it is required.
	Session *[16]byte
	// ClientInstance is this process's stable instance identity.
	ClientInstance [16]byte
	// DeadlineMs is the per-request deadline carried on the wire.
	DeadlineMs uint32
	// ResolveAttempts bounds identity resolution after an unknown outcome.
	ResolveAttempts int
	// AccountingPrefix is the keyspace DbSize accounts for (default "/").
	AccountingPrefix string
	// AccountingPageLimit is the page size of the accounting scan.
	AccountingPageLimit uint32
	// AccountingMaxPages bounds the scan; beyond it DbSize fails rather
	// than reporting a partial number.
	AccountingMaxPages int
	// Observer receives one Event per native invocation.
	Observer func(Event)
}

// Backend is the coord:// backend of one domain.
type Backend struct {
	cfg Config

	mu       sync.Mutex
	instance *client.Instance
	// epoch is the client binding epoch `instance` was built for. A later
	// epoch means the frontend acknowledged a new session, so new work is
	// allocated under a new instance.
	epoch uint64
}

// New validates the configuration.
func New(cfg Config) (*Backend, error) {
	if cfg.Client == nil {
		return nil, ErrNoClient
	}
	if cfg.DeadlineMs == 0 {
		cfg.DeadlineMs = 10_000
	}
	if cfg.ResolveAttempts <= 0 {
		cfg.ResolveAttempts = 3
	}
	if cfg.AccountingPrefix == "" {
		cfg.AccountingPrefix = "/"
	}
	if cfg.AccountingPageLimit == 0 {
		cfg.AccountingPageLimit = 256
	}
	if cfg.AccountingPageLimit > wire.MaxPageLimit {
		return nil, fmt.Errorf("accounting page limit above %d", wire.MaxPageLimit)
	}
	if cfg.AccountingMaxPages <= 0 {
		cfg.AccountingMaxPages = 64
	}
	return &Backend{cfg: cfg}, nil
}

var _ server.Backend = (*Backend)(nil)

// Start connects and binds the frontend session, validates it against
// the configuration and creates the health key idempotently. A second
// Start keeps the instance (no new identity) and finds the key present.
func (b *Backend) Start(ctx context.Context) error {
	if err := b.cfg.Client.Connect(ctx); err != nil {
		return mapClientError(err)
	}
	b.mu.Lock()
	err := b.adoptBindingLocked()
	b.mu.Unlock()
	if err != nil {
		return err
	}
	// See kubernetes staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go:
	// the API server's health check reads this key.
	_, err = b.Create(ctx, server.HealthKey, []byte(server.HealthVal), 0)
	if err != nil && !errors.Is(err, server.ErrKeyExists) {
		return err
	}
	return nil
}

// adoptBindingLocked makes the instance match the client's current
// binding. A new epoch means the frontend acknowledged a new session
// (an ordinary credential refresh does this), so later work is allocated
// under a fresh instance from sequence one. Invocations already
// allocated keep the instance they were allocated under: their retry
// keys and frames are already built, so an unresolved write is resolved
// as itself rather than re-sent as a different one.
func (b *Backend) adoptBindingLocked() error {
	session, epoch, bound := b.cfg.Client.Binding()
	switch {
	case bound && b.cfg.Session != nil && *b.cfg.Session != session:
		return ErrSessionMismatch
	case !bound && b.cfg.Session == nil:
		return ErrNoSession
	case !bound:
		session, epoch = *b.cfg.Session, 1
	}
	if b.instance != nil && b.epoch == epoch {
		return nil
	}
	b.instance = client.NewInstance(client.InstanceConfig{
		Cluster:  b.cfg.Cluster,
		Domain:   b.cfg.Domain,
		Session:  session,
		Instance: b.cfg.ClientInstance,
	})
	b.epoch = epoch
	return nil
}

func (b *Backend) instanceOrErr() (*client.Instance, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.instance == nil {
		return nil, ErrNotStarted
	}
	if err := b.adoptBindingLocked(); err != nil {
		return nil, err
	}
	return b.instance, nil
}

func kindName(op wire.LogicalOp) string {
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

// invoke performs exactly one native command: allocate a stable identity
// (the payload may depend on the retry key), send it, resolve an unknown
// outcome by identity a bounded number of times, and decode the result.
func (b *Backend) invoke(ctx context.Context, op string, mk func(key wire.RetryKey) wire.LogicalOp) (wire.Result, error) {
	// The identity an invocation is allocated under has to be the one the
	// connection is bound to, so the binding is made current before the
	// retry key is formed: connecting afterwards could roll the session
	// over underneath a key already built from the previous one.
	if err := b.cfg.Client.Connect(ctx); err != nil {
		return wire.Result{}, mapClientError(err)
	}
	inst, err := b.instanceOrErr()
	if err != nil {
		return wire.Result{}, status.Error(codes.Unavailable, err.Error())
	}
	var kind string
	inv, err := inst.AllocateFor(b.cfg.DeadlineMs, func(key wire.RetryKey) ([]byte, [32]byte, error) {
		logical := wire.LogicalRequest{Namespace: b.cfg.Namespace, Op: mk(key)}
		kind = kindName(logical.Op)
		payload, err := logical.Encode()
		if err != nil {
			return nil, [32]byte{}, err
		}
		return payload, client.CommandID(key, payload), nil
	})
	if err != nil {
		if errors.Is(err, wire.ErrInvalidLogical) {
			return wire.Result{}, status.Error(codes.InvalidArgument, err.Error())
		}
		return wire.Result{}, status.Error(codes.Internal, err.Error())
	}
	event := Event{Op: op, Kind: kind, Sequence: inv.Sequence}
	res, err := b.exchange(ctx, inst, inv, &event)
	// The outcome is final here whichever way it went: established, or
	// given up on and reported as unknown so the API server issues a new
	// invocation. Nothing retries this sequence, so its binding is
	// released rather than retained for every request the process ever
	// made.
	inst.Release(inv.Sequence)
	if err != nil {
		event.Outcome = "error: " + status.Code(err).String()
	}
	if b.cfg.Observer != nil {
		b.cfg.Observer(event)
	}
	return res, err
}

func (b *Backend) exchange(ctx context.Context, inst *client.Instance, inv client.Invocation, event *Event) (wire.Result, error) {
	out, err := b.cfg.Client.Do(ctx, inv.Frame)
	if err != nil {
		return wire.Result{}, mapClientError(err)
	}
	for out.Unknown && event.Resolves < b.cfg.ResolveAttempts {
		if ctx.Err() != nil {
			return wire.Result{}, status.FromContextError(ctx.Err()).Err()
		}
		event.Resolves++
		frame, err := inst.ResolveFrame(inv)
		if err != nil {
			return wire.Result{}, status.Error(codes.Internal, err.Error())
		}
		out, err = b.cfg.Client.Do(ctx, frame)
		if err != nil {
			return wire.Result{}, mapClientError(err)
		}
	}
	if out.Unknown {
		// Never a silent success: the API server retries with a new
		// invocation, and conditional operations keep that safe.
		return wire.Result{}, status.Errorf(codes.Unavailable, "outcome unknown after %d resolutions; retry", event.Resolves)
	}
	resp := out.Response
	if resp.CommandID != inv.CommandID {
		return wire.Result{}, status.Error(codes.Internal, "response names another command identity")
	}
	if resp.Tag == wire.OutcomeErr {
		return wire.Result{}, mapWireError(resp.Code, resp.Detail)
	}
	res, err := wire.DecodeResult(resp.Result)
	if err != nil {
		return wire.Result{}, status.Error(codes.Internal, "undecodable result: "+err.Error())
	}
	if resp.HasRevision && resp.Revision != res.Revision {
		return wire.Result{}, status.Error(codes.Internal, "result and header revisions disagree")
	}
	event.Outcome = outcomeName(res.Kind)
	return res, nil
}

func outcomeName(kind wire.OutcomeKind) string {
	switch kind {
	case wire.OutcomeRange:
		return "Range"
	case wire.OutcomeKineCreated:
		return "KineCreated"
	case wire.OutcomeErrKeyExists:
		return "ErrKeyExists"
	case wire.OutcomeKineUpdated:
		return "KineUpdated"
	case wire.OutcomeKineDeleted:
		return "KineDeleted"
	case wire.OutcomeErrCompacted:
		return "ErrCompacted"
	case wire.OutcomeErrFutureRevision:
		return "ErrFutureRevision"
	case wire.OutcomeErrLeaseExists:
		return "ErrLeaseExists"
	case wire.OutcomeErrSessionInvalid:
		return "ErrSessionInvalid"
	case wire.OutcomeErrPermissionDenied:
		return "ErrPermissionDenied"
	default:
		return fmt.Sprintf("outcome-%d", kind)
	}
}

func toInt64(v uint64) (int64, error) {
	if v > math.MaxInt64 {
		return 0, status.Error(codes.Internal, "revision above int64")
	}
	return int64(v), nil
}

// keyValue converts a Kine-facing entry; the lease is the TTL, never the
// hidden binding identity.
func keyValue(kv *wire.KineKv) *server.KeyValue {
	if kv == nil {
		return nil
	}
	return &server.KeyValue{
		Key:            string(kv.Key),
		Value:          kv.Entry.Value,
		Version:        int64(kv.Entry.Version),
		CreateRevision: int64(kv.Entry.CreateRevision),
		ModRevision:    int64(kv.Entry.ModRevision),
		Lease:          int64(kv.TTLSeconds),
	}
}

// rangeValue converts a read item. A read carries no Kine-facing TTL and
// the hidden binding identity is never disclosed, so Lease is 0.
func rangeValue(item wire.RangeItem) *server.KeyValue {
	return &server.KeyValue{
		Key:            string(item.Key),
		Value:          item.Entry.Value,
		Version:        int64(item.Entry.Version),
		CreateRevision: int64(item.Entry.CreateRevision),
		ModRevision:    int64(item.Entry.ModRevision),
	}
}

func revisionOption(revision int64) (*uint64, error) {
	if revision < 0 {
		return nil, status.Error(codes.InvalidArgument, "negative revision")
	}
	if revision == 0 {
		return nil, nil
	}
	r := uint64(revision)
	return &r, nil
}

func ttlSeconds(lease int64) (uint32, error) {
	if lease < 0 || lease > wire.MaxLeaseTTLSeconds {
		return 0, status.Errorf(codes.InvalidArgument, "lease (TTL seconds) out of range 0..=%d", wire.MaxLeaseTTLSeconds)
	}
	return uint32(lease), nil
}

// bindingFor derives the private binding of a write with a positive TTL.
func bindingFor(key wire.RetryKey, ttl uint32) *[16]byte {
	if ttl == 0 {
		return nil
	}
	b := client.KineBinding(key)
	return &b
}

// readErr maps a read outcome that is not a range to Kine's errors.
func readErr(res wire.Result) error {
	switch res.Kind {
	case wire.OutcomeRange:
		return nil
	case wire.OutcomeErrCompacted:
		return server.ErrCompacted
	case wire.OutcomeErrFutureRevision:
		return server.ErrFutureRev
	default:
		return mapOutcomeError(res.Kind)
	}
}

// Get is one revisioned exact-key read.
func (b *Backend) Get(ctx context.Context, key string, revision int64, keysOnly bool) (int64, *server.KeyValue, error) {
	at, err := revisionOption(revision)
	if err != nil {
		return 0, nil, err
	}
	res, err := b.invoke(ctx, "Get", func(wire.RetryKey) wire.LogicalOp {
		return wire.RangeOp{Range: wire.KeyRange{Key: []byte(key)}, Revision: at, Limit: 1, KeysOnly: keysOnly}
	})
	if err != nil {
		return 0, nil, err
	}
	rev, err := toInt64(res.Revision)
	if err != nil {
		return 0, nil, err
	}
	if err := readErr(res); err != nil {
		return rev, nil, err
	}
	if len(res.Items) == 0 {
		return rev, nil, nil
	}
	return rev, rangeValue(res.Items[0]), nil
}

// Create is one create-if-absent command with an atomic TTL binding.
func (b *Backend) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	ttl, err := ttlSeconds(lease)
	if err != nil {
		return 0, err
	}
	res, err := b.invoke(ctx, "Create", func(rk wire.RetryKey) wire.LogicalOp {
		return wire.KineCreateOp{Key: []byte(key), Value: value, TTLSeconds: ttl, Binding: bindingFor(rk, ttl)}
	})
	if err != nil {
		return 0, err
	}
	rev, err := toInt64(res.Revision)
	if err != nil {
		return 0, err
	}
	switch res.Kind {
	case wire.OutcomeKineCreated:
		return rev, nil
	case wire.OutcomeErrKeyExists:
		return rev, server.ErrKeyExists
	default:
		return rev, mapOutcomeError(res.Kind)
	}
}

// Update is one compare-mod-revision-and-update with TTL replacement;
// success and mismatch metadata come from the same execution point.
func (b *Backend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *server.KeyValue, bool, error) {
	if revision <= 0 {
		return 0, nil, false, status.Error(codes.InvalidArgument, "update requires the expected modification revision")
	}
	ttl, err := ttlSeconds(lease)
	if err != nil {
		return 0, nil, false, err
	}
	res, err := b.invoke(ctx, "Update", func(rk wire.RetryKey) wire.LogicalOp {
		return wire.KineUpdateOp{Key: []byte(key), Value: value, ExpectedModRevision: uint64(revision), TTLSeconds: ttl, Binding: bindingFor(rk, ttl)}
	})
	if err != nil {
		return 0, nil, false, err
	}
	rev, err := toInt64(res.Revision)
	if err != nil {
		return 0, nil, false, err
	}
	if res.Kind != wire.OutcomeKineUpdated {
		return rev, nil, false, mapOutcomeError(res.Kind)
	}
	return rev, keyValue(res.Current), res.Updated, nil
}

// Delete is one conditional deletion keeping the zero-revision, absent
// and mismatch distinctions.
func (b *Backend) Delete(ctx context.Context, key string, revision int64) (int64, *server.KeyValue, bool, error) {
	expected, err := revisionOption(revision)
	if err != nil {
		return 0, nil, false, err
	}
	res, err := b.invoke(ctx, "Delete", func(wire.RetryKey) wire.LogicalOp {
		return wire.KineDeleteOp{Key: []byte(key), ExpectedModRevision: expected}
	})
	if err != nil {
		return 0, nil, false, err
	}
	rev, err := toInt64(res.Revision)
	if err != nil {
		return 0, nil, false, err
	}
	if res.Kind != wire.OutcomeKineDeleted {
		return rev, nil, false, mapOutcomeError(res.Kind)
	}
	return rev, keyValue(res.Prev), res.Deleted, nil
}

func interval(key, end string) (wire.KeyRange, error) {
	if end == "" {
		return wire.KeyRange{}, status.Error(codes.InvalidArgument, "list requires a range end")
	}
	if end == "\x00" {
		return wire.KeyRange{}, status.Error(codes.InvalidArgument, "from-key ranges are not in logical_v1")
	}
	return wire.KeyRange{Key: []byte(key), RangeEnd: []byte(end)}, nil
}

// List is one revisioned interval read with the bridge's limit.
func (b *Backend) List(ctx context.Context, key, end string, limit, revision int64, keysOnly bool) (int64, []*server.KeyValue, error) {
	rng, err := interval(key, end)
	if err != nil {
		return 0, nil, err
	}
	if limit < 0 || limit > wire.MaxPageLimit {
		return 0, nil, status.Errorf(codes.InvalidArgument, "limit out of range 0..=%d", wire.MaxPageLimit)
	}
	at, err := revisionOption(revision)
	if err != nil {
		return 0, nil, err
	}
	res, err := b.invoke(ctx, "List", func(wire.RetryKey) wire.LogicalOp {
		return wire.RangeOp{Range: rng, Revision: at, Limit: uint32(limit), KeysOnly: keysOnly}
	})
	if err != nil {
		return 0, nil, err
	}
	rev, err := toInt64(res.Revision)
	if err != nil {
		return 0, nil, err
	}
	if err := readErr(res); err != nil {
		return rev, nil, err
	}
	kvs := make([]*server.KeyValue, 0, len(res.Items))
	for _, item := range res.Items {
		kvs = append(kvs, rangeValue(item))
	}
	return rev, kvs, nil
}

// Count is one revisioned count-only read.
func (b *Backend) Count(ctx context.Context, key, end string, revision int64) (int64, int64, error) {
	rng, err := interval(key, end)
	if err != nil {
		return 0, 0, err
	}
	at, err := revisionOption(revision)
	if err != nil {
		return 0, 0, err
	}
	res, err := b.invoke(ctx, "Count", func(wire.RetryKey) wire.LogicalOp {
		return wire.RangeOp{Range: rng, Revision: at, CountOnly: true}
	})
	if err != nil {
		return 0, 0, err
	}
	rev, err := toInt64(res.Revision)
	if err != nil {
		return 0, 0, err
	}
	if err := readErr(res); err != nil {
		return rev, 0, err
	}
	count, err := toInt64(res.Count)
	if err != nil {
		return 0, 0, err
	}
	return rev, count, nil
}

// CurrentRevision is the authoritative frontier: one ordered count-only
// read of the health key returns the domain revision in its header.
func (b *Backend) CurrentRevision(ctx context.Context) (int64, error) {
	res, err := b.invoke(ctx, "CurrentRevision", func(wire.RetryKey) wire.LogicalOp {
		return wire.RangeOp{Range: wire.KeyRange{Key: []byte(server.HealthKey)}, CountOnly: true}
	})
	if err != nil {
		return 0, err
	}
	if err := readErr(res); err != nil {
		return 0, err
	}
	return toInt64(res.Revision)
}

// prefixEnd is the exclusive end of a prefix interval (etcd convention).
func prefixEnd(prefix string) (string, bool) {
	end := []byte(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return string(end[:i+1]), true
		}
	}
	return "", false
}

// DbSize is a defined accounting of the bound domain: the key and value
// bytes of the current entries under the accounting prefix at one
// revision, read in bounded pages. Beyond the page bound it fails rather
// than report a partial or invented number.
func (b *Backend) DbSize(ctx context.Context) (int64, error) {
	end, ok := prefixEnd(b.cfg.AccountingPrefix)
	if !ok {
		return 0, status.Error(codes.InvalidArgument, "accounting prefix has no exclusive end")
	}
	var total int64
	var at *uint64
	start := b.cfg.AccountingPrefix
	for page := 0; page < b.cfg.AccountingMaxPages; page++ {
		rng := wire.KeyRange{Key: []byte(start), RangeEnd: []byte(end)}
		snapshot := at
		res, err := b.invoke(ctx, "DbSize", func(wire.RetryKey) wire.LogicalOp {
			return wire.RangeOp{Range: rng, Revision: snapshot, Limit: b.cfg.AccountingPageLimit}
		})
		if err != nil {
			return 0, err
		}
		if err := readErr(res); err != nil {
			return 0, err
		}
		if at == nil {
			// Later pages hold this logical revision (Section 6.3).
			r := res.Revision
			at = &r
		}
		for _, item := range res.Items {
			total += int64(len(item.Key) + len(item.Entry.Value))
		}
		if !res.More || len(res.Items) == 0 {
			return total, nil
		}
		start = string(res.Items[len(res.Items)-1].Key) + "\x00"
	}
	return 0, status.Errorf(codes.ResourceExhausted, "accounting scan exceeded %d pages", b.cfg.AccountingMaxPages)
}

// Watch is task-47: the watch is cancelled with an explicit error rather
// than served from polling or silently.
func (b *Backend) Watch(ctx context.Context, key, end string, revision int64) server.WatchResult {
	events := make(chan []*server.Event)
	close(events)
	errc := make(chan error, 1)
	errc <- ErrWatchUnsupported
	return server.WatchResult{Events: events, Errorc: errc}
}

// Compact is task-47.
func (b *Backend) Compact(ctx context.Context, revision int64) (int64, error) {
	return 0, ErrCompactUnsupported
}

// WaitForSyncTo is part of the watch delivery pipeline (task-47); nothing
// is served that could progress over missing events.
func (b *Backend) WaitForSyncTo(revision int64) {}
