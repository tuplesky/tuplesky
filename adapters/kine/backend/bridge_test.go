package backend_test

import (
	"context"
	"crypto/tls"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/tuplesky/tuplesky/adapters/kine/backend"
	"github.com/tuplesky/tuplesky/adapters/kine/client"
	"github.com/tuplesky/tuplesky/adapters/kine/edge"
	"github.com/tuplesky/tuplesky/adapters/kine/internal/fakedomain"
	"github.com/tuplesky/tuplesky/adapters/kine/internal/testpki"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type staticTokens string

func (s staticTokens) Token(context.Context) (string, error) { return string(s), nil }

const testToken = "svc-token"

var testSession = [16]byte{0x53, 0x53}

// bridge is one composed test: the in-process domain, the backend, the
// pinned Kine server bridge on a secured edge and an etcd client.
type bridge struct {
	domain  *fakedomain.Server
	backend *backend.Backend
	cli     *clientv3.Client
	// events observed from the backend.
	mu     sync.Mutex
	events []backend.Event
}

func (b *bridge) resetTrace() {
	b.mu.Lock()
	b.events = nil
	b.mu.Unlock()
	b.domain.ResetLog()
}

func (b *bridge) trace() []backend.Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]backend.Event(nil), b.events...)
}

type bridgeOptions struct {
	// tokens overrides the token source (nil: the correct token).
	tokens client.TokenSource
	// session pins the expected session.
	session *[16]byte
	// noStart skips Start.
	noStart bool
	// accountingPages bounds DbSize.
	accountingPages int
	accountingPage  uint32
	// syncTimeout bounds WaitForSyncTo.
	syncTimeout time.Duration
	// teardownDelay widens the window in which a terminated watch is
	// cancelled but still live (fault injection).
	teardownDelay time.Duration
	// notifyInterval is the bridge's progress-report interval (default 5s).
	notifyInterval time.Duration
}

func startDomain(t *testing.T) (*fakedomain.Server, *tls.Config) {
	t.Helper()
	cert, pool, err := fakedomain.SelfSigned("frontend.local")
	if err != nil {
		t.Fatal(err)
	}
	domain, err := fakedomain.Start(fakedomain.Config{Token: testToken, Session: testSession, Cert: cert})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(domain.Close)
	return domain, &tls.Config{RootCAs: pool, ServerName: "frontend.local", MinVersion: tls.VersionTLS13}
}

func startBridge(t *testing.T, opts bridgeOptions) *bridge {
	t.Helper()
	domain, tlsConf := startDomain(t)
	var tokens client.TokenSource = staticTokens(testToken)
	if opts.tokens != nil {
		tokens = opts.tokens
	}
	native := client.New(domain.Addr(), client.Config{TLS: tlsConf, Tokens: tokens, Cluster: [16]byte{1}, Domain: [16]byte{2}, FrameTimeout: 3 * time.Second})
	br := &bridge{domain: domain}
	be, err := backend.New(backend.Config{
		Client:              native,
		Cluster:             [16]byte{1},
		Domain:              [16]byte{2},
		Namespace:           [16]byte{3},
		Session:             opts.session,
		ClientInstance:      [16]byte{4},
		AccountingPageLimit: opts.accountingPage,
		AccountingMaxPages:  opts.accountingPages,
		SyncTimeout:         opts.syncTimeout,
		WatchTeardownDelay:  opts.teardownDelay,
		Observer: func(e backend.Event) {
			br.mu.Lock()
			br.events = append(br.events, e)
			br.mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	br.backend = be
	t.Cleanup(be.Close)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if !opts.noStart {
		if err := be.Start(ctx); err != nil {
			t.Fatalf("start: %v", err)
		}
	}
	sock := filepath.Join(t.TempDir(), "kine.sock")
	ln, err := edge.Listen(ctx, edge.Config{Listener: "unix://" + sock})
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer(ln.ServerOptions...)
	notify := opts.notifyInterval
	if notify == 0 {
		notify = 5 * time.Second
	}
	server.New(be, ln.Scheme, notify, "3.5.13").Register(grpcServer)
	go func() { _ = grpcServer.Serve(ln) }()
	t.Cleanup(grpcServer.Stop)
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"unix://" + sock}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	br.cli = cli
	return br
}

func ctxT(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// create is the API server's create transaction.
func create(t *testing.T, cli *clientv3.Client, key, value string, ttl int64) *clientv3.TxnResponse {
	t.Helper()
	put := clientv3.OpPut(key, value)
	if ttl > 0 {
		put = clientv3.OpPut(key, value, clientv3.WithLease(clientv3.LeaseID(ttl)))
	}
	resp, err := cli.Txn(ctxT(t)).If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).Then(put).Commit()
	if err != nil {
		t.Fatalf("create %s: %v", key, err)
	}
	return resp
}

// update is the API server's guaranteed-update transaction.
func update(t *testing.T, cli *clientv3.Client, key, value string, rev, ttl int64) *clientv3.TxnResponse {
	t.Helper()
	put := clientv3.OpPut(key, value)
	if ttl > 0 {
		put = clientv3.OpPut(key, value, clientv3.WithLease(clientv3.LeaseID(ttl)))
	}
	resp, err := cli.Txn(ctxT(t)).If(clientv3.Compare(clientv3.ModRevision(key), "=", rev)).Then(put).Else(clientv3.OpGet(key)).Commit()
	if err != nil {
		t.Fatalf("update %s: %v", key, err)
	}
	return resp
}

// del is the API server's conditional delete (rev 0: unconditional).
func del(t *testing.T, cli *clientv3.Client, key string, rev int64) *clientv3.TxnResponse {
	t.Helper()
	var resp *clientv3.TxnResponse
	var err error
	if rev == 0 {
		resp, err = cli.Txn(ctxT(t)).Then(clientv3.OpGet(key), clientv3.OpDelete(key)).Commit()
	} else {
		resp, err = cli.Txn(ctxT(t)).If(clientv3.Compare(clientv3.ModRevision(key), "=", rev)).Then(clientv3.OpDelete(key)).Else(clientv3.OpGet(key)).Commit()
	}
	if err != nil {
		t.Fatalf("delete %s: %v", key, err)
	}
	return resp
}

func get(t *testing.T, cli *clientv3.Client, key string, opts ...clientv3.OpOption) *clientv3.GetResponse {
	t.Helper()
	resp, err := cli.Get(ctxT(t), key, opts...)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	return resp
}

// Start binds the session, creates the health key once and is
// idempotent: a second Start finds the key present at the same revision
// and mints no new identity.
func TestStartBindsSessionAndCreatesHealthKeyIdempotently(t *testing.T) {
	br := startBridge(t, bridgeOptions{})
	if br.domain.Binds.Load() != 1 {
		t.Fatalf("binds %d", br.domain.Binds.Load())
	}
	health := get(t, br.cli, server.HealthKey)
	if health.Count != 1 || string(health.Kvs[0].Value) != server.HealthVal || health.Kvs[0].CreateRevision != 1 {
		t.Fatalf("health %+v", health.Kvs)
	}
	rev := health.Header.Revision
	if err := br.backend.Start(ctxT(t)); err != nil {
		t.Fatalf("second start: %v", err)
	}
	again := get(t, br.cli, server.HealthKey)
	if again.Header.Revision != rev || again.Count != 1 {
		t.Fatalf("second start changed the domain: %d -> %d", rev, again.Header.Revision)
	}
	if n := len(br.domain.Model().Current()); n != 1 {
		t.Fatalf("%d keys after two starts", n)
	}
	// A revision never regresses: CurrentRevision is the authoritative
	// frontier from one ordered read.
	cur, err := br.backend.CurrentRevision(ctxT(t))
	if err != nil || cur != rev {
		t.Fatalf("current revision %d (%v), want %d", cur, err, rev)
	}
}

// A wrong token, a mismatched session and a missing session all fail
// Start closed.
func TestStartFailsClosedOnIdentityProblems(t *testing.T) {
	br := startBridge(t, bridgeOptions{tokens: staticTokens("wrong"), noStart: true})
	if err := br.backend.Start(ctxT(t)); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("wrong token: %v", err)
	}
	other := [16]byte{9}
	br = startBridge(t, bridgeOptions{session: &other, noStart: true})
	if err := br.backend.Start(ctxT(t)); !errors.Is(err, backend.ErrSessionMismatch) {
		t.Fatalf("session mismatch: %v", err)
	}
	if _, _, err := br.backend.Get(ctxT(t), server.HealthKey, 0, false); status.Code(err) != codes.Unavailable {
		t.Fatalf("operation before start: %v", err)
	}
	br = startBridge(t, bridgeOptions{tokens: staticTokens(""), noStart: true})
	if err := br.backend.Start(ctxT(t)); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("empty token: %v", err)
	}
}

// Create, CAS update and conditional delete through the real server
// handlers: revisions, absent and mismatch metadata and the exact
// duplicate-key result, each from one execution point.
func TestCreateUpdateDeleteRevisionsAndMetadata(t *testing.T) {
	br := startBridge(t, bridgeOptions{})
	key := "/registry/pods/default/p"
	created := create(t, br.cli, key, "v1", 0)
	if !created.Succeeded || created.Header.Revision != 2 {
		t.Fatalf("create %+v", created)
	}
	dup := create(t, br.cli, key, "other", 0)
	if dup.Succeeded || dup.Header.Revision != 2 || len(dup.Responses) != 0 {
		t.Fatalf("duplicate create %+v", dup)
	}
	got := get(t, br.cli, key)
	kv := got.Kvs[0]
	if got.Count != 1 || kv.CreateRevision != 2 || kv.ModRevision != 2 || kv.Version != 1 || string(kv.Value) != "v1" || got.Header.Revision != 2 {
		t.Fatalf("get %+v", got)
	}
	// Mismatch: not applied, the current entry comes back in the failure
	// branch, header is the current revision.
	miss := update(t, br.cli, key, "v2", 7, 0)
	if miss.Succeeded || miss.Header.Revision != 2 {
		t.Fatalf("mismatch %+v", miss)
	}
	cur := miss.Responses[0].GetResponseRange()
	if cur.Count != 1 || cur.Kvs[0].ModRevision != 2 || string(cur.Kvs[0].Value) != "v1" {
		t.Fatalf("mismatch range %+v", cur)
	}
	// Update of an absent key: not applied, nothing current.
	absent := update(t, br.cli, "/registry/none", "v", 1, 0)
	if absent.Succeeded || absent.Responses[0].GetResponseRange().Count != 0 {
		t.Fatalf("absent update %+v", absent)
	}
	// Match with a TTL: applied at revision 3 with the binding created in
	// the same command.
	ok := update(t, br.cli, key, "v2", 2, 60)
	if !ok.Succeeded || ok.Header.Revision != 3 {
		t.Fatalf("update %+v", ok)
	}
	model := br.domain.Model().Current()[key]
	if model.Version != 2 || model.ModRevision != 3 || model.TTLSeconds != 60 || model.Lease == nil {
		t.Fatalf("model %+v", model)
	}
	// Conditional delete mismatch: not deleted, the entry seen returned.
	dm := del(t, br.cli, key, 2)
	if dm.Succeeded || dm.Header.Revision != 3 || dm.Responses[0].GetResponseRange().Kvs[0].ModRevision != 3 {
		t.Fatalf("delete mismatch %+v", dm)
	}
	// The Kine-facing TTL is the lease field of the same-execution-point
	// metadata; the hidden binding identity is never disclosed.
	if l := dm.Responses[0].GetResponseRange().Kvs[0].Lease; l != 60 {
		t.Fatalf("lease %d, want the TTL 60", l)
	}
	// Match: deleted at revision 4 with the previous entry.
	dd := del(t, br.cli, key, 3)
	if !dd.Succeeded || dd.Header.Revision != 4 {
		t.Fatalf("delete %+v", dd)
	}
	prev := dd.Responses[0].GetResponseDeleteRange()
	if prev.Deleted != 1 || string(prev.PrevKvs[0].Value) != "v2" {
		t.Fatalf("delete prev %+v", prev)
	}
	if get(t, br.cli, key).Count != 0 {
		t.Fatal("key survived deletion")
	}
	// Absent key: the reference bridge reports it gone without a revision.
	da := del(t, br.cli, key, 9)
	if !da.Succeeded || da.Header.Revision != 4 || da.Responses[0].GetResponseDeleteRange().Deleted != 0 {
		t.Fatalf("absent delete %+v", da)
	}
	// Unconditional delete of a re-created key.
	create(t, br.cli, key, "v3", 0)
	du := del(t, br.cli, key, 0)
	if !du.Succeeded || du.Header.Revision != 6 || du.Responses[0].GetResponseDeleteRange().Deleted != 1 {
		t.Fatalf("unconditional delete %+v", du)
	}
	if br.domain.Model().Revision() != 6 {
		t.Fatalf("model revision %d", br.domain.Model().Revision())
	}
}

// Byte intervals, count and pagination through the server bridge; a
// historical page, a future revision and a compacted revision are exact.
func TestListCountPaginationAndHistoricalReads(t *testing.T) {
	br := startBridge(t, bridgeOptions{})
	prefix := "/registry/pods/"
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		create(t, br.cli, prefix+k, "v-"+k, 0)
	}
	create(t, br.cli, "/registry/secrets/x", "s", 0)
	end := clientv3.GetPrefixRangeEnd(prefix)
	// Rev after the five pods is 6; the secret makes 7.
	page := get(t, br.cli, prefix, clientv3.WithRange(end), clientv3.WithLimit(2))
	if len(page.Kvs) != 2 || !page.More || page.Count != 5 || page.Header.Revision != 7 {
		t.Fatalf("page %+v", page)
	}
	if string(page.Kvs[0].Key) != prefix+"a" || string(page.Kvs[1].Key) != prefix+"b" {
		t.Fatalf("page keys %s %s", page.Kvs[0].Key, page.Kvs[1].Key)
	}
	// Continue at the list's revision from the next key.
	next := get(t, br.cli, prefix+"b\x00", clientv3.WithRange(end), clientv3.WithLimit(2), clientv3.WithRev(page.Header.Revision))
	if len(next.Kvs) != 2 || !next.More || next.Count != 3 || string(next.Kvs[0].Key) != prefix+"c" {
		t.Fatalf("next %+v", next)
	}
	last := get(t, br.cli, prefix+"d\x00", clientv3.WithRange(end), clientv3.WithLimit(2), clientv3.WithRev(page.Header.Revision))
	if len(last.Kvs) != 1 || last.More || last.Count != 1 {
		t.Fatalf("last %+v", last)
	}
	// Count only, keys only.
	count := get(t, br.cli, prefix, clientv3.WithRange(end), clientv3.WithCountOnly())
	if count.Count != 5 || len(count.Kvs) != 0 {
		t.Fatalf("count %+v", count)
	}
	keys := get(t, br.cli, prefix, clientv3.WithRange(end), clientv3.WithKeysOnly())
	if len(keys.Kvs) != 5 || len(keys.Kvs[0].Value) != 0 || keys.Kvs[0].ModRevision != 2 {
		t.Fatalf("keys only %+v", keys.Kvs[0])
	}
	// A historical read at revision 3 sees only the first two pods.
	hist := get(t, br.cli, prefix, clientv3.WithRange(end), clientv3.WithRev(3))
	if len(hist.Kvs) != 2 || hist.Count != 2 {
		t.Fatalf("historical %+v", hist)
	}
	// An exact historical get of a key updated later returns the old
	// version.
	update(t, br.cli, prefix+"a", "v-a2", 2, 0)
	old := get(t, br.cli, prefix+"a", clientv3.WithRev(5))
	if old.Count != 1 || string(old.Kvs[0].Value) != "v-a" || old.Kvs[0].Version != 1 {
		t.Fatalf("old version %+v", old)
	}
	if _, err := br.cli.Get(ctxT(t), prefix, clientv3.WithRange(end), clientv3.WithRev(1000)); !errors.Is(err, rpctypes.ErrFutureRev) {
		t.Fatalf("future revision: %v", err)
	}
	br.domain.Model().SetCompactFloor(4)
	if _, err := br.cli.Get(ctxT(t), prefix, clientv3.WithRange(end), clientv3.WithRev(2)); !errors.Is(err, rpctypes.ErrCompacted) {
		t.Fatalf("compacted revision: %v", err)
	}
	if _, err := br.cli.Get(ctxT(t), prefix+"a", clientv3.WithRev(2)); !errors.Is(err, rpctypes.ErrCompacted) {
		t.Fatalf("compacted get: %v", err)
	}
}

// One conditional command is exactly one native invocation: no WAN
// pre-read before it and no revision follow-up after it, on both the
// backend's trace and the domain's log.
func TestConditionalUpdateIsOneNativeCommand(t *testing.T) {
	br := startBridge(t, bridgeOptions{})
	key := "/registry/leases/kube-system/leader"
	create(t, br.cli, key, "holder-1", 15)
	br.resetTrace()
	resp := update(t, br.cli, key, "holder-1-renewed", 2, 15)
	if !resp.Succeeded || resp.Header.Revision != 3 {
		t.Fatalf("update %+v", resp)
	}
	events := br.trace()
	if len(events) != 1 || events[0].Op != "Update" || events[0].Kind != "KineUpdate" || events[0].Resolves != 0 || events[0].Outcome != "KineUpdated" {
		t.Fatalf("trace %+v", events)
	}
	log := br.domain.Log()
	if len(log) != 1 || log[0].Kind != "request" || log[0].Op != "KineUpdate" || !log[0].Executed {
		t.Fatalf("domain log %+v", log)
	}
	// The same holds for a conditional delete and a create.
	br.resetTrace()
	del(t, br.cli, key, 3)
	create(t, br.cli, key, "holder-2", 15)
	events = br.trace()
	if len(events) != 2 || events[0].Kind != "KineDelete" || events[1].Kind != "KineCreate" {
		t.Fatalf("trace %+v", events)
	}
	// Every write with a TTL named a fresh binding and none was reused.
	model := br.domain.Model().Current()[key]
	if model.TTLSeconds != 15 || model.Lease == nil {
		t.Fatalf("model %+v", model)
	}
}

// An unknown outcome is resolved by the same identity, never re-executed
// and never reported as success when it stays unknown.
func TestUnknownOutcomeIsResolvedByIdentityOrFailsExplicitly(t *testing.T) {
	br := startBridge(t, bridgeOptions{})
	key := "/registry/configmaps/c"
	br.resetTrace()
	br.domain.PendingOnce.Store(true)
	resp := create(t, br.cli, key, "v", 0)
	if !resp.Succeeded || resp.Header.Revision != 2 {
		t.Fatalf("resolved create %+v", resp)
	}
	events := br.trace()
	if len(events) != 1 || events[0].Resolves != 1 || events[0].Outcome != "KineCreated" {
		t.Fatalf("trace %+v", events)
	}
	log := br.domain.Log()
	if len(log) != 2 || log[0].Kind != "request" || !log[0].Executed || log[1].Kind != "resolve" || log[0].Sequence != log[1].Sequence {
		t.Fatalf("domain log %+v", log)
	}
	// A resolution the endpoint answers Unknown (it does not know the
	// identity) re-sends the identical invocation, which the endpoint
	// that did execute it answers from its retained result: no second
	// execution.
	br.resetTrace()
	br.domain.PendingOnce.Store(true)
	br.domain.ForgetOnce.Store(true)
	again := create(t, br.cli, key+"1", "v", 0)
	if !again.Succeeded || again.Header.Revision != 3 {
		t.Fatalf("resolved after a lost resolution: %+v", again)
	}
	if events := br.trace(); len(events) != 1 || events[0].Resolves != 2 {
		t.Fatalf("trace %+v", events)
	}
	log = br.domain.Log()
	if len(log) != 3 || !log[0].Executed || log[1].Kind != "resolve" || log[2].Op != "retained" {
		t.Fatalf("domain log %+v", log)
	}
	// The endpoint never establishes the outcome: the bridge reports an
	// explicit unavailable error, not a fabricated result, and the
	// command was still executed exactly once.
	br.resetTrace()
	br.domain.PendingAlways.Store(true)
	_, err := br.cli.Txn(ctxT(t)).If(clientv3.Compare(clientv3.ModRevision(key+"2"), "=", 0)).Then(clientv3.OpPut(key+"2", "v")).Commit()
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("unknown outcome: %v", err)
	}
	br.domain.PendingAlways.Store(false)
	log = br.domain.Log()
	executed := 0
	for _, l := range log {
		if l.Executed {
			executed++
		}
	}
	if executed != 1 {
		t.Fatalf("executed %d times: %+v", executed, log)
	}
	events = br.trace()
	if len(events) != 1 || events[0].Resolves != 3 || !strings.HasPrefix(events[0].Outcome, "error") {
		t.Fatalf("trace %+v", events)
	}
	// The key exists (the command applied once) even though the bridge
	// could not establish it: the API server's retry is a new conditional
	// create that finds it present, never a duplicate.
	if get(t, br.cli, key+"2").Count != 1 {
		t.Fatal("applied command lost")
	}
	if dup := create(t, br.cli, key+"2", "v", 0); dup.Succeeded {
		t.Fatal("retry created a duplicate")
	}
}

// The unsupported subset is refused explicitly: from-key ranges and TTLs
// out of range never reach the domain.
func TestUnsupportedRequestsAreExplicit(t *testing.T) {
	br := startBridge(t, bridgeOptions{})
	if _, err := br.cli.Get(ctxT(t), "/registry", clientv3.WithFromKey()); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("from-key: %v", err)
	}
	for _, ttl := range []int64{-1, 7*24*3600 + 1} {
		_, err := br.cli.Txn(ctxT(t)).If(clientv3.Compare(clientv3.ModRevision("/k"), "=", 0)).Then(clientv3.OpPut("/k", "v", clientv3.WithLease(clientv3.LeaseID(ttl)))).Commit()
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("ttl %d: %v", ttl, err)
		}
	}
	if _, err := br.cli.Compact(ctxT(t), 0); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("compact 0: %v", err)
	}
	if len(br.domain.Model().Current()) != 1 {
		t.Fatal("refused requests reached the domain")
	}
}

// DbSize is the defined accounting of the domain's current keys and
// values at one revision, paged; beyond the bound it fails rather than
// report a partial number.
func TestDbSizeIsDefinedDomainAccounting(t *testing.T) {
	br := startBridge(t, bridgeOptions{accountingPage: 2, accountingPages: 4})
	var want int64
	for _, k := range []string{"a", "bb", "ccc", "dddd", "eeeee"} {
		create(t, br.cli, "/registry/x/"+k, strings.Repeat("v", len(k)*3), 0)
	}
	for k, e := range br.domain.Model().Current() {
		want += int64(len(k) + len(e.Value))
	}
	got, err := br.backend.DbSize(ctxT(t))
	if err != nil || got != want {
		t.Fatalf("db size %d (%v), want %d", got, err, want)
	}
	// Every page of the scan held the first page's revision.
	seen := 0
	for _, e := range br.trace() {
		if e.Op == "DbSize" {
			seen++
		}
	}
	if seen != 3 {
		t.Fatalf("%d accounting pages, want 3", seen)
	}
	status_, err := br.cli.Maintenance.Status(ctxT(t), br.cli.Endpoints()[0])
	if err != nil || status_.DbSize != want {
		t.Fatalf("status %+v %v", status_, err)
	}
	create(t, br.cli, "/registry/x/f", "v", 0)
	create(t, br.cli, "/registry/x/g", "v", 0)
	create(t, br.cli, "/registry/x/h", "v", 0)
	if _, err := br.backend.DbSize(ctxT(t)); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("beyond the bound: %v", err)
	}
}

// The tls:// edge admits only the explicitly authorized API-server
// identity: an unauthorized name under the same CA, no client
// certificate, an expired certificate and a wrong server name are
// rejected before any request reaches the bridge.
func TestTLSEdgeAdmitsOnlyAuthorizedAPIServerIdentity(t *testing.T) {
	domain, tlsConf := startDomain(t)
	native := client.New(domain.Addr(), client.Config{TLS: tlsConf, Tokens: staticTokens(testToken), Cluster: [16]byte{1}, Domain: [16]byte{2}, FrameTimeout: 3 * time.Second})
	be, err := backend.New(backend.Config{Client: native, Cluster: [16]byte{1}, Domain: [16]byte{2}, Namespace: [16]byte{3}, ClientInstance: [16]byte{4}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := ctxT(t)
	if err := be.Start(ctx); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	ca, err := testpki.NewCA("edge-ca")
	if err != nil {
		t.Fatal(err)
	}
	caPath, err := ca.WriteCA(dir, "ca")
	if err != nil {
		t.Fatal(err)
	}
	serverID, err := ca.Issue("kine.local", true, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	certPath, keyPath, err := serverID.WriteFiles(dir, "server")
	if err != nil {
		t.Fatal(err)
	}
	ln, err := edge.Listen(ctx, edge.Config{Listener: "tls://127.0.0.1:0", CertFile: certPath, KeyFile: keyPath, ClientCAFile: caPath, AllowedClients: []string{"apiserver-a.local"}})
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer(ln.ServerOptions...)
	server.New(be, ln.Scheme, 5*time.Second, "3.5.13").Register(grpcServer)
	go func() { _ = grpcServer.Serve(ln) }()
	t.Cleanup(grpcServer.Stop)
	endpoint := ln.Endpoint
	issue := func(name string, notAfter time.Time) tls.Certificate {
		id, err := ca.Issue(name, false, notAfter)
		if err != nil {
			t.Fatal(err)
		}
		return id.TLS
	}
	dial := func(conf *tls.Config) error {
		cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 2 * time.Second, TLS: conf})
		if err != nil {
			return err
		}
		defer func() { _ = cli.Close() }()
		// A refused handshake is retried by the client until the deadline.
		c, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		defer cancel()
		resp, err := cli.Get(c, server.HealthKey)
		if err != nil {
			return err
		}
		if resp.Count != 1 {
			return errors.New("health key missing")
		}
		return nil
	}
	otherCA, err := testpki.NewCA("other-ca")
	if err != nil {
		t.Fatal(err)
	}
	authorized := &tls.Config{RootCAs: ca.Pool(), ServerName: "kine.local", Certificates: []tls.Certificate{issue("apiserver-a.local", time.Now().Add(time.Hour))}, MinVersion: tls.VersionTLS13}
	if err := dial(authorized); err != nil {
		t.Fatalf("authorized API server: %v", err)
	}
	cases := map[string]*tls.Config{
		"unauthorized name under the same CA": {RootCAs: ca.Pool(), ServerName: "kine.local", Certificates: []tls.Certificate{issue("apiserver-b.local", time.Now().Add(time.Hour))}, MinVersion: tls.VersionTLS13},
		"no client certificate":               {RootCAs: ca.Pool(), ServerName: "kine.local", MinVersion: tls.VersionTLS13},
		"expired client certificate":          {RootCAs: ca.Pool(), ServerName: "kine.local", Certificates: []tls.Certificate{issue("apiserver-a.local", time.Now().Add(-time.Minute))}, MinVersion: tls.VersionTLS13},
		"untrusted server issuer":             {RootCAs: otherCA.Pool(), ServerName: "kine.local", Certificates: []tls.Certificate{issue("apiserver-a.local", time.Now().Add(time.Hour))}, MinVersion: tls.VersionTLS13},
	}
	for name, conf := range cases {
		if err := dial(conf); err == nil {
			t.Fatalf("%s: admitted", name)
		}
	}
	// A plaintext client cannot use the edge either.
	plain, err := clientv3.New(clientv3.Config{Endpoints: []string{strings.Replace(endpoint, "https://", "http://", 1)}, DialTimeout: 2 * time.Second})
	if err == nil {
		c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err = plain.Get(c, server.HealthKey)
		cancel()
		_ = plain.Close()
		if err == nil {
			t.Fatal("plaintext client admitted")
		}
	}
}
