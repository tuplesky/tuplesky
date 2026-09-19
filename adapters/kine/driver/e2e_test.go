package driver

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/server"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/tuplesky/tuplesky/adapters/kine/edge"
)

// The end-to-end smoke test of the composed stack. `startDriver` gives a
// real `server.Backend` over the real native endpoint; this puts the
// pinned Kine server bridge and a real etcd client on top of it, so the
// path under test is the one Kubernetes uses: etcd gRPC -> Kine ->
// coord:// driver -> QUIC -> the Rust planner and store. The Rust side is
// `coord-session/tests/kine_e2e`, which serves the real domain rather
// than canned outcomes; without it this is skipped.
//
// The assertions are on semantics, not on reachability: a clean boot is
// not compatibility (task-48).
func TestTheKubernetesStorageEdgeRunsAgainstTheRealDomain(t *testing.T) {
	if os.Getenv(envEndpoint) == "" {
		t.Skip("no native endpoint; driven by `cargo test -p coord-session --test kine_e2e`")
	}
	backend, err := startDriver(t, os.Getenv(envToken))
	if err != nil {
		t.Fatalf("start against the real domain: %v", err)
	}
	cli := bridge(t, backend)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	const prefix = "/registry/pods/default/"
	key := prefix + "p"

	// Create. The API server's create is a transaction guarded on the key
	// never having existed.
	created, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, "v1")).
		Commit()
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !created.Succeeded {
		t.Fatalf("create did not apply: %+v", created)
	}
	rev := created.Header.Revision

	// Read it back with its metadata.
	got, err := cli.Get(ctx, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Count != 1 || string(got.Kvs[0].Value) != "v1" {
		t.Fatalf("get: %+v", got)
	}
	if got.Kvs[0].CreateRevision != rev || got.Kvs[0].ModRevision != rev || got.Kvs[0].Version != 1 {
		t.Fatalf("metadata: %+v", got.Kvs[0])
	}
	// The private TTL binding is never disclosed as a lease identity.
	if got.Kvs[0].Lease != 0 {
		t.Fatalf("a Kine caller saw a lease identity: %d", got.Kvs[0].Lease)
	}

	// A create of the same key does not apply twice.
	dup, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, "other")).
		Commit()
	if err != nil {
		t.Fatalf("duplicate create: %v", err)
	}
	if dup.Succeeded {
		t.Fatal("a second create applied")
	}

	// Compare-and-swap on a stale revision does not apply, and returns
	// what is current.
	stale, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", rev+99)).
		Then(clientv3.OpPut(key, "v2")).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		t.Fatalf("stale update: %v", err)
	}
	if stale.Succeeded {
		t.Fatal("a stale compare-and-swap applied")
	}
	if cur := stale.Responses[0].GetResponseRange(); cur.Count != 1 || string(cur.Kvs[0].Value) != "v1" {
		t.Fatalf("stale update current: %+v", cur)
	}

	// Compare-and-swap on the matching revision applies.
	ok, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", rev)).
		Then(clientv3.OpPut(key, "v2")).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if !ok.Succeeded || ok.Header.Revision <= rev {
		t.Fatalf("update: %+v", ok)
	}
	updated := ok.Header.Revision

	got, err = cli.Get(ctx, key)
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	if string(got.Kvs[0].Value) != "v2" || got.Kvs[0].Version != 2 ||
		got.Kvs[0].CreateRevision != rev || got.Kvs[0].ModRevision != updated {
		t.Fatalf("after update: %+v", got.Kvs[0])
	}

	// Range, count and pagination over a prefix.
	for _, suffix := range []string{"a", "b", "c"} {
		if _, err := cli.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(prefix+suffix), "=", 0)).
			Then(clientv3.OpPut(prefix+suffix, "v-"+suffix)).
			Commit(); err != nil {
			t.Fatalf("seed %s: %v", suffix, err)
		}
	}
	count, err := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count.Count != 4 {
		t.Fatalf("count %d, want 4", count.Count)
	}
	page, err := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithLimit(2))
	if err != nil {
		t.Fatalf("page: %v", err)
	}
	if len(page.Kvs) != 2 || !page.More {
		t.Fatalf("first page: %d kvs, more=%v", len(page.Kvs), page.More)
	}
	if string(page.Kvs[0].Key) != prefix+"a" || string(page.Kvs[1].Key) != prefix+"b" {
		t.Fatalf("page order: %s %s", page.Kvs[0].Key, page.Kvs[1].Key)
	}
	rest, err := cli.Get(ctx, prefix+"b\x00", clientv3.WithRange(clientv3.GetPrefixRangeEnd(prefix)))
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if len(rest.Kvs) != 2 || string(rest.Kvs[0].Key) != prefix+"c" {
		t.Fatalf("second page: %+v", rest.Kvs)
	}

	// Conditional delete: mismatch leaves it, match removes it.
	miss, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", rev)).
		Then(clientv3.OpDelete(key)).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		t.Fatalf("delete mismatch: %v", err)
	}
	if miss.Succeeded {
		t.Fatal("a stale conditional delete applied")
	}
	gone, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", updated)).
		Then(clientv3.OpDelete(key)).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !gone.Succeeded {
		t.Fatalf("delete did not apply: %+v", gone)
	}
	after, err := cli.Get(ctx, key)
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if after.Count != 0 {
		t.Fatalf("the key survived deletion: %+v", after.Kvs)
	}
	// The other keys are untouched: a delete is one key, not a range.
	if count, err = cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly()); err != nil {
		t.Fatalf("count after delete: %v", err)
	} else if count.Count != 3 {
		t.Fatalf("count after delete %d, want 3", count.Count)
	}
}

// bridge puts the pinned Kine server on a Unix-socket edge over `be` and
// returns an etcd client for it, exactly as `kine-coord` composes them.
func bridge(t *testing.T, be server.Backend) *clientv3.Client {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sock := filepath.Join(t.TempDir(), "kine.sock")
	ln, err := edge.Listen(ctx, edge.Config{Listener: "unix://" + sock})
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer(ln.ServerOptions...)
	server.New(be, ln.Scheme, 5*time.Second, "3.5.13").Register(grpcServer)
	go func() { _ = grpcServer.Serve(ln) }()
	t.Cleanup(grpcServer.Stop)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"unix://" + sock},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}
