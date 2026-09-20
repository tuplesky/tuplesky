package certify

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// timeout bounds every certification step. A profile that only passes
// when nobody is watching the clock is not the profile.
const timeout = 90 * time.Second

func ctxFor(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	return ctx
}

// The create/read/update/delete profile the API server actually uses:
// every mutation is a transaction guarded on the key's mod revision, and
// what the guard compares is the revision the previous answer reported.
func TestCreateReadCompareAndSwapDelete(t *testing.T) {
	h := load(t)
	cli := authorized(t, h)
	ctx := ctxFor(t)
	key := prefix(t) + "pod-a"

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

	got, err := cli.Get(ctx, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Count != 1 || string(got.Kvs[0].Value) != "v1" {
		t.Fatalf("get: %+v", got)
	}
	if got.Kvs[0].CreateRevision != rev || got.Kvs[0].ModRevision != rev || got.Kvs[0].Version != 1 {
		t.Fatalf("metadata does not describe the create: %+v", got.Kvs[0])
	}

	// A second create does not apply, because the guard no longer holds.
	again, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, "other")).
		Commit()
	if err != nil {
		t.Fatalf("duplicate create: %v", err)
	}
	if again.Succeeded {
		t.Fatal("a second create applied")
	}

	// A stale compare-and-swap does not apply and returns what is
	// current, which is what the API server retries against.
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
	if current := stale.Responses[0].GetResponseRange(); current.Count != 1 ||
		string(current.Kvs[0].Value) != "v1" {
		t.Fatalf("a refused update did not return what is current: %+v", current)
	}

	updated, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", rev)).
		Then(clientv3.OpPut(key, "v2")).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if !updated.Succeeded || updated.Header.Revision <= rev {
		t.Fatalf("update: %+v", updated)
	}

	got, err = cli.Get(ctx, key)
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	if string(got.Kvs[0].Value) != "v2" || got.Kvs[0].Version != 2 ||
		got.Kvs[0].CreateRevision != rev {
		t.Fatalf("update metadata: %+v", got.Kvs[0])
	}

	deleted, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", got.Kvs[0].ModRevision)).
		Then(clientv3.OpDelete(key)).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !deleted.Succeeded {
		t.Fatalf("delete did not apply: %+v", deleted)
	}
	if after, err := cli.Get(ctx, key); err != nil {
		t.Fatalf("get after delete: %v", err)
	} else if after.Count != 0 {
		t.Fatalf("the key survived its delete: %+v", after)
	}
}

// Paging a list the way the API server pages one: a bounded range, then
// the next page from the last key it saw, at one pinned revision.
func TestPaginationIsBoundedOrderedAndResumable(t *testing.T) {
	h := load(t)
	cli := authorized(t, h)
	ctx := ctxFor(t)
	root := prefix(t)

	const total = 12
	var pinned int64
	for i := 0; i < total; i++ {
		put, err := cli.Put(ctx, fmt.Sprintf("%sobj-%02d", root, i), fmt.Sprintf("v%d", i))
		if err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
		pinned = put.Header.Revision
	}

	counted, err := cli.Get(ctx, root, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if counted.Count != total {
		t.Fatalf("count %d, want %d", counted.Count, total)
	}

	const page = 5
	seen := make([]string, 0, total)
	start := root
	for {
		got, err := cli.Get(ctx, start,
			clientv3.WithRange(clientv3.GetPrefixRangeEnd(root)),
			clientv3.WithLimit(page),
			clientv3.WithRev(pinned))
		if err != nil {
			t.Fatalf("page from %q: %v", start, err)
		}
		if int64(len(got.Kvs)) > page {
			t.Fatalf("a page of %d exceeded the limit %d", len(got.Kvs), page)
		}
		for i, kv := range got.Kvs {
			if i > 0 && string(got.Kvs[i-1].Key) >= string(kv.Key) {
				t.Fatalf("a page is not in key order: %q then %q", got.Kvs[i-1].Key, kv.Key)
			}
			seen = append(seen, string(kv.Key))
		}
		if len(got.Kvs) == 0 {
			break
		}
		// `More` is what tells the API server to ask again; the next
		// page starts one key past the last one it read.
		if !got.More {
			break
		}
		start = string(got.Kvs[len(got.Kvs)-1].Key) + "\x00"
	}
	if len(seen) != total {
		t.Fatalf("paging read %d keys, want %d: %v", len(seen), total, seen)
	}
	for i := 0; i < total; i++ {
		want := fmt.Sprintf("%sobj-%02d", root, i)
		if seen[i] != want {
			t.Fatalf("page %d is %q, want %q", i, seen[i], want)
		}
	}
}

// A watch replays from a revision, hands over to live events without a
// gap, and reports progress on an idle prefix. The API server's caches
// are rebuilt from exactly this.
func TestWatchReplaysResumesAndReportsProgress(t *testing.T) {
	h := load(t)
	cli := authorized(t, h)
	ctx := ctxFor(t)
	root := prefix(t)
	key := root + "watched"

	first, err := cli.Put(ctx, key, "one")
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	from := first.Header.Revision

	watched, cancel := context.WithCancel(ctx)
	defer cancel()
	events := cli.Watch(watched, root,
		clientv3.WithPrefix(),
		clientv3.WithRev(from),
		clientv3.WithProgressNotify())

	// The replayed event is the one already written before the watch
	// opened: a cache that missed it would start out wrong.
	replayed := next(t, events, "replay")
	if len(replayed.Events) != 1 || string(replayed.Events[0].Kv.Key) != key ||
		string(replayed.Events[0].Kv.Value) != "one" {
		t.Fatalf("replay: %+v", replayed.Events)
	}

	// Live handover: written after the watch is established.
	if _, err := cli.Put(ctx, key, "two"); err != nil {
		t.Fatalf("live write: %v", err)
	}
	live := next(t, events, "live")
	if len(live.Events) != 1 || string(live.Events[0].Kv.Value) != "two" {
		t.Fatalf("live: %+v", live.Events)
	}
	resume := live.Events[0].Kv.ModRevision

	if _, err := cli.Delete(ctx, key); err != nil {
		t.Fatalf("delete: %v", err)
	}
	removed := next(t, events, "delete")
	if len(removed.Events) != 1 || removed.Events[0].Type != clientv3.EventTypeDelete {
		t.Fatalf("delete event: %+v", removed.Events)
	}

	// Progress on an idle prefix, so a watcher can advance its resume
	// point without a write happening. It is a bound, not a promise
	// about the next write.
	progress := waitForProgress(t, events)
	if progress < removed.Header.Revision {
		t.Fatalf("progress %d went behind the last event %d", progress, removed.Header.Revision)
	}
	cancel()

	// Resuming from the stored revision replays exactly what followed
	// it, and nothing before it.
	resumed, stop := context.WithCancel(ctx)
	defer stop()
	again := cli.Watch(resumed, root, clientv3.WithPrefix(), clientv3.WithRev(resume+1))
	tail := next(t, again, "resume")
	if len(tail.Events) != 1 || tail.Events[0].Type != clientv3.EventTypeDelete {
		t.Fatalf("resume replayed the wrong thing: %+v", tail.Events)
	}
}

// Compaction removes history, and a watch that asks for a compacted
// revision is refused rather than quietly started at the wrong place --
// the refusal is what makes the API server re-list.
func TestCompactionRefusesAWatchBelowTheFloor(t *testing.T) {
	h := load(t)
	cli := authorized(t, h)
	ctx := ctxFor(t)
	root := prefix(t)
	key := root + "compacted"

	first, err := cli.Put(ctx, key, "one")
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	below := first.Header.Revision
	last, err := cli.Put(ctx, key, "two")
	if err != nil {
		t.Fatalf("second: %v", err)
	}

	if _, err := cli.Compact(ctx, last.Header.Revision); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// The current value survives compaction; only history goes.
	if got, err := cli.Get(ctx, key); err != nil {
		t.Fatalf("get after compaction: %v", err)
	} else if got.Count != 1 || string(got.Kvs[0].Value) != "two" {
		t.Fatalf("compaction lost the current value: %+v", got)
	}

	watched, cancel := context.WithCancel(ctx)
	defer cancel()
	events := cli.Watch(watched, root, clientv3.WithPrefix(), clientv3.WithRev(below))
	refused := next(t, events, "compacted watch")
	if refused.Err() == nil {
		t.Fatalf("a watch below the compaction floor was accepted: %+v", refused.Events)
	}
	if refused.Err() != rpctypes.ErrCompacted && refused.CompactRevision == 0 {
		t.Fatalf("a watch below the floor failed without naming the floor: %v", refused.Err())
	}
}

// A key written under a time-to-live disappears on its own, and the
// binding that expires it is never disclosed to the caller as a lease
// identity: Kine's TTL is private to the domain.
func TestTimeToLiveExpiresAndStaysPrivate(t *testing.T) {
	h := load(t)
	cli := authorized(t, h)
	ctx := ctxFor(t)
	key := prefix(t) + "ephemeral"

	lease, err := cli.Grant(ctx, 1)
	if err != nil {
		t.Fatalf("grant: %v", err)
	}
	if _, err := cli.Put(ctx, key, "transient", clientv3.WithLease(lease.ID)); err != nil {
		t.Fatalf("put with a time to live: %v", err)
	}

	got, err := cli.Get(ctx, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Count != 1 {
		t.Fatalf("the key was not written: %+v", got)
	}
	if got.Kvs[0].Lease != 0 {
		t.Fatalf("a caller saw the private time-to-live binding as a lease identity: %d", got.Kvs[0].Lease)
	}

	deadline := time.Now().Add(60 * time.Second)
	for {
		got, err := cli.Get(ctx, key)
		if err != nil {
			t.Fatalf("get while expiring: %v", err)
		}
		if got.Count == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("a key with a one-second time to live was still there after a minute")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// Concurrent writers contend on one key the way several API server
// replicas do. Exactly one compare-and-swap per revision applies, and
// every writer that lost learns the revision that won.
func TestConcurrentWritersSerializeOnOneKey(t *testing.T) {
	h := load(t)
	ctx := ctxFor(t)
	key := prefix(t) + "contended"

	const writers = 4
	const rounds = 5

	clients := make([]*clientv3.Client, writers)
	for i := range clients {
		clients[i] = authorized(t, h)
	}
	if _, err := clients[0].Put(ctx, key, "0"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var applied, refused int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(cli *clientv3.Client, who int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				got, err := cli.Get(ctx, key)
				if err != nil || got.Count != 1 {
					t.Errorf("writer %d read: %v %+v", who, err, got)
					return
				}
				out, err := cli.Txn(ctx).
					If(clientv3.Compare(clientv3.ModRevision(key), "=", got.Kvs[0].ModRevision)).
					Then(clientv3.OpPut(key, fmt.Sprintf("%d-%d", who, r))).
					Else(clientv3.OpGet(key)).
					Commit()
				if err != nil {
					t.Errorf("writer %d commit: %v", who, err)
					return
				}
				mu.Lock()
				if out.Succeeded {
					applied++
				} else {
					refused++
					if current := out.Responses[0].GetResponseRange(); current.Count != 1 {
						t.Errorf("writer %d lost without learning what won: %+v", who, current)
					}
				}
				mu.Unlock()
			}
		}(clients[w], w)
	}
	wg.Wait()
	if applied == 0 {
		t.Fatal("no writer ever applied")
	}
	if applied+refused != writers*rounds {
		t.Fatalf("%d applied and %d refused, want %d outcomes", applied, refused, writers*rounds)
	}
	// Whatever the interleaving, the key holds one value and one
	// history: a lost update would show up as a version count below the
	// number that applied.
	got, err := clients[0].Get(ctx, key)
	if err != nil {
		t.Fatalf("final read: %v", err)
	}
	if got.Count != 1 {
		t.Fatalf("the contended key is not single-valued: %+v", got)
	}
	if got.Kvs[0].Version < applied {
		t.Fatalf("version %d is below the %d writes that applied", got.Kvs[0].Version, applied)
	}
}

func next(t *testing.T, events clientv3.WatchChan, what string) clientv3.WatchResponse {
	t.Helper()
	select {
	case response, ok := <-events:
		if !ok {
			t.Fatalf("the %s watch closed", what)
		}
		return response
	case <-time.After(timeout):
		t.Fatalf("no %s event within %s", what, timeout)
		return clientv3.WatchResponse{}
	}
}

// waitForProgress reads until the watch reports progress with no events,
// which is what a progress notification is.
func waitForProgress(t *testing.T, events clientv3.WatchChan) int64 {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case response, ok := <-events:
			if !ok {
				t.Fatal("the watch closed before it reported progress")
			}
			if response.Err() != nil {
				t.Fatalf("the watch failed before it reported progress: %v", response.Err())
			}
			if len(response.Events) == 0 {
				return response.Header.Revision
			}
		case <-deadline:
			t.Fatalf("no progress notification within %s", timeout)
			return 0
		}
	}
}
