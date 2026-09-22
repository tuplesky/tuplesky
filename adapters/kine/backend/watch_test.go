package backend_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/tuplesky/tuplesky/adapters/kine/backend"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// expectEvents reads watch responses until `n` events were seen, checking
// every response's header is monotonic and never below its events.
func expectEvents(t *testing.T, wch clientv3.WatchChan, n int) []*clientv3.Event {
	t.Helper()
	var out []*clientv3.Event
	var lastHeader int64
	deadline := time.After(10 * time.Second)
	for len(out) < n {
		select {
		case resp, ok := <-wch:
			if !ok {
				t.Fatalf("watch closed after %d of %d events", len(out), n)
			}
			if resp.Canceled {
				t.Fatalf("watch cancelled: %v", resp.Err())
			}
			if resp.Header.Revision < lastHeader {
				t.Fatalf("header regressed %d -> %d", lastHeader, resp.Header.Revision)
			}
			lastHeader = resp.Header.Revision
			for _, e := range resp.Events {
				if e.Kv.ModRevision > resp.Header.Revision {
					t.Fatalf("event %d above header %d", e.Kv.ModRevision, resp.Header.Revision)
				}
				out = append(out, e)
			}
		case <-deadline:
			t.Fatalf("timed out after %d of %d events", len(out), n)
		}
	}
	return out
}

// Replay from a start revision then live events arrive as complete
// revisions with contiguous per-key history and previous values; no
// batch is skipped or duplicated.
func TestWatchReplayToLiveHasNoGap(t *testing.T) {
	br := startBridge(t, bridgeOptions{})
	prefix := "/registry/pods/"
	create(t, br.cli, prefix+"a", "a1", 0) // 2
	create(t, br.cli, prefix+"b", "b1", 0) // 3
	update(t, br.cli, prefix+"a", "a2", 2, 0)
	create(t, br.cli, "/registry/other", "x", 0) // 5: filtered
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wch := br.cli.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(2), clientv3.WithPrevKV())
	del(t, br.cli, prefix+"b", 3)          // 6
	create(t, br.cli, prefix+"c", "c1", 0) // 7
	events := expectEvents(t, wch, 5)
	want := []struct {
		typ  mvccpb.Event_EventType
		key  string
		rev  int64
		prev string
	}{
		{mvccpb.PUT, prefix + "a", 2, ""},
		{mvccpb.PUT, prefix + "b", 3, ""},
		{mvccpb.PUT, prefix + "a", 4, "a1"},
		{mvccpb.DELETE, prefix + "b", 6, "b1"},
		{mvccpb.PUT, prefix + "c", 7, ""},
	}
	for i, w := range want {
		e := events[i]
		if e.Type != w.typ || string(e.Kv.Key) != w.key || e.Kv.ModRevision != w.rev {
			t.Fatalf("event %d: %v %s %d, want %v %s %d", i, e.Type, e.Kv.Key, e.Kv.ModRevision, w.typ, w.key, w.rev)
		}
		if w.prev == "" && e.PrevKv != nil && len(e.PrevKv.Value) != 0 {
			t.Fatalf("event %d: unexpected prev %q", i, e.PrevKv.Value)
		}
		if w.prev != "" && (e.PrevKv == nil || string(e.PrevKv.Value) != w.prev) {
			t.Fatalf("event %d: prev %+v, want %q", i, e.PrevKv, w.prev)
		}
	}
	if events[3].Kv.Version != 0 || events[2].Kv.Version != 2 || !events[0].IsCreate() {
		t.Fatalf("metadata %+v %+v %+v", events[0].Kv, events[2].Kv, events[3].Kv)
	}
	cancel()
	time.Sleep(100 * time.Millisecond)
	if n := br.domain.Watchers(); n != 0 {
		t.Fatalf("%d native watches left after cancel", n)
	}
}

// A future start delivers only revisions at or above it; a live (rev 0)
// waitWatchers blocks until the domain has `n` open native watches, so a
// test can write only once the watches it opened are anchored.
func waitWatchers(t *testing.T, br *bridge, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if br.domain.Watchers() >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("only %d of %d watches opened", br.domain.Watchers(), n)
}

// watch starts right after the authoritative frontier.
func TestWatchFutureAndLiveStart(t *testing.T) {
	br := startBridge(t, bridgeOptions{})
	key := "/registry/cm/x"
	create(t, br.cli, key, "1", 0) // 2
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	future := br.cli.Watch(ctx, key, clientv3.WithRev(5))
	live := br.cli.Watch(ctx, key)
	// A live watch is anchored at the frontier the bridge reads when the
	// watch opens, so the writes below must not race that read.
	waitWatchers(t, br, 2)
	update(t, br.cli, key, "2", 2, 0) // 3
	update(t, br.cli, key, "3", 3, 0) // 4
	update(t, br.cli, key, "4", 4, 0) // 5
	update(t, br.cli, key, "5", 5, 0) // 6
	got := expectEvents(t, future, 2)
	if got[0].Kv.ModRevision != 5 || got[1].Kv.ModRevision != 6 {
		t.Fatalf("future start delivered %d %d", got[0].Kv.ModRevision, got[1].Kv.ModRevision)
	}
	got = expectEvents(t, live, 4)
	if got[0].Kv.ModRevision != 3 || got[3].Kv.ModRevision != 6 {
		t.Fatalf("live start delivered %d..%d", got[0].Kv.ModRevision, got[3].Kv.ModRevision)
	}
}

// Compaction is one ordered command; a watch from a compacted revision
// is cancelled with the compacted error and a historical read below the
// floor is compacted, while history at the floor stays readable.
func TestCompactionEndsCompactedWatchesExplicitly(t *testing.T) {
	br := startBridge(t, bridgeOptions{})
	key := "/registry/cm/y"
	create(t, br.cli, key, "1", 0)    // 2
	update(t, br.cli, key, "2", 2, 0) // 3
	update(t, br.cli, key, "3", 3, 0) // 4
	br.resetTrace()
	if _, err := br.cli.Compact(ctxT(t), 3); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if events := br.trace(); len(events) != 1 || events[0].Kind != "Compact" || events[0].Outcome != "Compacted" {
		t.Fatalf("trace %+v", events)
	}
	if br.domain.Model().CompactFloor() != 3 {
		t.Fatalf("floor %d", br.domain.Model().CompactFloor())
	}
	// Beyond the current revision the floor clamps.
	if _, err := br.cli.Compact(ctxT(t), 1000); err != nil || br.domain.Model().CompactFloor() != 4 {
		t.Fatalf("clamped compact: %v floor %d", err, br.domain.Model().CompactFloor())
	}
	if _, err := br.cli.Get(ctxT(t), key, clientv3.WithRev(3)); err != rpctypes.ErrCompacted {
		t.Fatalf("read below floor: %v", err)
	}
	if resp := get(t, br.cli, key, clientv3.WithRev(4)); resp.Count != 1 || string(resp.Kvs[0].Value) != "3" {
		t.Fatalf("read at floor %+v", resp.Kvs)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wch := br.cli.Watch(ctx, key, clientv3.WithRev(2))
	resp, ok := <-wch
	if !ok || !resp.Canceled || resp.Err() != rpctypes.ErrCompacted {
		t.Fatalf("compacted start: ok=%v %+v err=%v", ok, resp, resp.Err())
	}
	// A watch at the floor is fine.
	fine := br.cli.Watch(ctx, key, clientv3.WithRev(4))
	update(t, br.cli, key, "4", 4, 0) // 5
	got := expectEvents(t, fine, 2)
	if got[0].Kv.ModRevision != 4 || got[1].Kv.ModRevision != 5 {
		t.Fatalf("watch at floor %d %d", got[0].Kv.ModRevision, got[1].Kv.ModRevision)
	}
}

// Progress is queued behind delivery: a requested progress notification
// carries the current revision only after every event through it was
// delivered, and a filtered watch advances only through the domain's
// processed marker, never from mere receipt.
func TestProgressNeverOvertakesEvents(t *testing.T) {
	br := startBridge(t, bridgeOptions{})
	prefix := "/registry/pods/"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wch := br.cli.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithProgressNotify())
	for i := 0; i < 5; i++ {
		create(t, br.cli, prefix+string(rune('a'+i)), "v", 0) // 2..6
	}
	create(t, br.cli, "/registry/other", "x", 0) // 7: filtered
	if err := br.cli.RequestProgress(ctx); err != nil {
		t.Fatal(err)
	}
	var delivered int64
	var progress int64
	deadline := time.After(10 * time.Second)
	for progress < 7 {
		select {
		case resp := <-wch:
			if resp.Canceled {
				t.Fatalf("cancelled: %v", resp.Err())
			}
			if resp.IsProgressNotify() {
				// A marker may precede the writes (the watch is synced at
				// the frontier), but never an event it should cover.
				progress = resp.Header.Revision
				continue
			}
			for _, e := range resp.Events {
				if e.Kv.ModRevision <= progress {
					t.Fatalf("event %d after progress %d", e.Kv.ModRevision, progress)
				}
				if delivered != 0 && e.Kv.ModRevision != delivered+1 {
					t.Fatalf("gap: %d after %d", e.Kv.ModRevision, delivered)
				}
				delivered = e.Kv.ModRevision
			}
		case <-deadline:
			t.Fatalf("no progress through 7; delivered through %d, progress %d", delivered, progress)
		}
	}
	if progress != 7 || delivered != 6 {
		t.Fatalf("progress %d delivered %d", progress, delivered)
	}
	// When the source withholds its processed marker, the adapter does
	// not synchronize the filtered watch on its own: the wait times out
	// and terminates the watch rather than announcing false progress.
	br.domain.HoldProgress.Store(true)
	create(t, br.cli, "/registry/other2", "x", 0) // 8
	start := time.Now()
	waited := make(chan struct{})
	go func() { br.backend.WaitForSyncTo(8); close(waited) }()
	select {
	case <-waited:
		t.Fatal("synchronized without the source's processed marker")
	case <-time.After(300 * time.Millisecond):
	}
	br.domain.HoldProgress.Store(false)
	create(t, br.cli, "/registry/other3", "x", 0) // 9: progress 9 covers 8
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("wait did not end after progress")
	}
	if time.Since(start) > 4*time.Second {
		t.Fatal("wait ended by timeout rather than progress")
	}
}

// A lost frontend connection is resumed from the last complete revision
// without a gap or duplicate; Kine and its client see one continuous
// stream.
func TestWatchResumesAfterConnectionLoss(t *testing.T) {
	br := startBridge(t, bridgeOptions{})
	key := "/registry/leases/l"
	create(t, br.cli, key, "1", 0) // 2
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wch := br.cli.Watch(ctx, key, clientv3.WithRev(2))
	got := expectEvents(t, wch, 1)
	if got[0].Kv.ModRevision != 2 {
		t.Fatalf("first %d", got[0].Kv.ModRevision)
	}
	br.resetTrace()
	br.domain.DropConnections()
	// The unary lane reconnects on the next operation; the watch lane
	// reconnects on its own and resumes from revision 3.
	update(t, br.cli, key, "2", 2, 0) // 3
	update(t, br.cli, key, "3", 3, 0) // 4
	got = expectEvents(t, wch, 2)
	if got[0].Kv.ModRevision != 3 || got[1].Kv.ModRevision != 4 {
		t.Fatalf("resumed %d %d", got[0].Kv.ModRevision, got[1].Kv.ModRevision)
	}
	reopened := 0
	for _, e := range br.trace() {
		if e.Op == "Watch" && e.Outcome == "opened" {
			reopened++
		}
	}
	if reopened != 1 {
		t.Fatalf("reopened %d times: %+v", reopened, br.trace())
	}
	update(t, br.cli, key, "4", 4, 0) // 5
	if got = expectEvents(t, wch, 1); got[0].Kv.ModRevision != 5 {
		t.Fatalf("after resume %d", got[0].Kv.ModRevision)
	}
}

// A peer that accepts every watch stream and resets it before delivering
// a frame is not a reachable source: each such stream counts toward the
// reconnect bound, so the watch fails explicitly after the configured
// attempts instead of reopening forever.
func TestWatchStreamsLostBeforeAnyFrameExhaustTheReconnectBound(t *testing.T) {
	br := startBridge(t, bridgeOptions{reconnectAttempts: 2, reconnectBackoff: 10 * time.Millisecond})
	key := "/registry/x"
	create(t, br.cli, key, "v", 0) // 2
	br.domain.ResetWatchOpens.Store(true)
	br.resetTrace()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wr := br.backend.Watch(ctx, key, "", 1)
	select {
	case err := <-wr.Errorc:
		if !errors.Is(err, backend.ErrWatchUnavailable) {
			t.Fatalf("watch ended with %v, want %v", err, backend.ErrWatchUnavailable)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the watch kept reopening streams that never delivered a frame")
	}
	if _, ok := <-wr.Events; ok {
		t.Fatal("events open after the watch failed")
	}
	opened := 0
	for _, e := range br.trace() {
		if e.Op == "Watch" && e.Outcome == "opened" {
			opened++
		}
	}
	// The first open plus one reopen per allowed attempt, and no more.
	if opened != 3 {
		t.Fatalf("opened %d streams, want 3: %+v", opened, br.trace())
	}
}

// Expiry is the domain's conditional command: it deletes only the key
// whose live binding it names at the bound revision, and the deletion
// reaches the watch as an ordinary delete with the previous value. A
// refreshed key makes the prior binding's expiration stale, and the
// adapter never deletes anything locally.
func TestStaleExpirationDeletesNothing(t *testing.T) {
	br := startBridge(t, bridgeOptions{})
	key := "/registry/events/e"
	create(t, br.cli, key, "v1", 60) // 2
	binding1, mod1, ok := br.domain.Model().Binding(key)
	if !ok || mod1 != 2 {
		t.Fatalf("binding %v %d", ok, mod1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wch := br.cli.Watch(ctx, key, clientv3.WithRev(3), clientv3.WithPrevKV())
	// Refresh: the update replaces the binding.
	update(t, br.cli, key, "v2", 2, 60) // 3
	binding2, mod2, _ := br.domain.Model().Binding(key)
	if binding2 == binding1 || mod2 != 3 {
		t.Fatalf("binding not replaced: %x %d", binding2, mod2)
	}
	if br.domain.Model().Expire(binding1, 2) {
		t.Fatal("stale expiration deleted")
	}
	if got := get(t, br.cli, key); got.Count != 1 || string(got.Kvs[0].Value) != "v2" {
		t.Fatalf("key after stale expiration %+v", got.Kvs)
	}
	if !br.domain.Model().Expire(binding2, 3) {
		t.Fatal("live expiration did not delete")
	}
	events := expectEvents(t, wch, 2)
	if events[0].Type != mvccpb.PUT || events[0].Kv.ModRevision != 3 {
		t.Fatalf("refresh event %+v", events[0])
	}
	if events[1].Type != mvccpb.DELETE || events[1].Kv.ModRevision != 4 || events[1].PrevKv == nil || string(events[1].PrevKv.Value) != "v2" {
		t.Fatalf("expiry event %+v prev %+v", events[1].Kv, events[1].PrevKv)
	}
	if get(t, br.cli, key).Count != 0 {
		t.Fatal("key survived expiration")
	}
	// Nothing in the adapter runs a TTL worker: no delete reached the
	// domain except the domain's own expiration.
	for _, l := range br.domain.Log() {
		if l.Op == "KineDelete" {
			t.Fatalf("adapter issued a delete: %+v", l)
		}
	}
}

// WaitForSyncTo is cancellable: a watch that never processes the awaited
// revision is terminated with a resumption error after the sync timeout
// instead of being reported synced; Close unblocks a pending wait.
func TestSyncWaitTerminatesLaggardsAndCloseUnblocks(t *testing.T) {
	br := startBridge(t, bridgeOptions{syncTimeout: 500 * time.Millisecond})
	key := "/registry/x"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// A raw backend watch whose consumer never reads: its frontier cannot
	// advance past the batch it holds.
	wr := br.backend.Watch(ctx, key, "", 1)
	create(t, br.cli, key, "v", 0) // 2
	time.Sleep(200 * time.Millisecond)
	start := time.Now()
	br.backend.WaitForSyncTo(2)
	if time.Since(start) < 400*time.Millisecond {
		t.Fatal("wait returned while the watch was behind")
	}
	select {
	case err := <-wr.Errorc:
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("laggard error %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("laggard not terminated")
	}
	if _, ok := <-wr.Events; ok {
		t.Fatal("events open after termination")
	}
	// A synced watch does not delay the wait.
	wr2 := br.backend.Watch(ctx, key, "", 0)
	go func() {
		for range wr2.Events {
		}
	}()
	time.Sleep(100 * time.Millisecond)
	start = time.Now()
	br.backend.WaitForSyncTo(2)
	if time.Since(start) > 200*time.Millisecond {
		t.Fatal("synced wait delayed")
	}
	// Close unblocks a pending wait immediately.
	wr3 := br.backend.Watch(ctx, key, "", 1)
	_ = wr3
	time.Sleep(100 * time.Millisecond)
	done := make(chan struct{})
	go func() { br.backend.WaitForSyncTo(2); close(done) }()
	time.Sleep(50 * time.Millisecond)
	br.backend.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not unblock the wait")
	}
	wr4 := br.backend.Watch(ctx, key, "", 0)
	if err := <-wr4.Errorc; status.Code(err) != codes.Unavailable {
		t.Fatalf("watch after close: %v", err)
	}
	_ = server.HealthKey
}

// A watch that cannot synchronize must stay out of progress publication
// until it is actually closed, not merely until its cancellation has
// been requested. The domain publishes no processed marker, the watch's
// key never moves while the domain's frontier advances to R, and the
// terminated watch's teardown is deliberately delayed: neither the
// bridge's per-watch progress timer nor its broadcast path may advertise
// R to a watch that is still live and never delivered through it.
func TestSynchronizationFailureWithholdsProgressUntilTheWatchIsActuallyClosed(t *testing.T) {
	for _, broadcast := range []bool{false, true} {
		name := "individual progress"
		if broadcast {
			name = "broadcast progress"
		}
		t.Run(name, func(t *testing.T) {
			br := startBridge(t, bridgeOptions{
				syncTimeout:    300 * time.Millisecond,
				teardownDelay:  1200 * time.Millisecond,
				notifyInterval: 150 * time.Millisecond,
			})
			// A source that never says it processed a revision: the
			// watch's frontier can only move by delivering events.
			br.domain.HoldProgress.Store(true)
			ctx := ctxT(t)
			wch := br.cli.Watch(ctx, "/registry/w",
				clientv3.WithRev(1),
				clientv3.WithProgressNotify(),
				clientv3.WithCreatedNotify())
			if created, ok := <-wch; !ok || !created.Created {
				t.Fatalf("watch not created: %+v", created)
			}
			// The domain advances outside this watch's key: R is the
			// frontier and this watch has delivered nothing through it.
			started := time.Now()
			resp := create(t, br.cli, "/registry/elsewhere", "v", 0)
			target := resp.Header.Revision
			if target <= 0 {
				t.Fatalf("revision %d", target)
			}
			if broadcast {
				// The API server asks for a broadcast progress report
				// while the caches are syncing; the bridge answers it
				// only when every watch is synced.
				stop := make(chan struct{})
				defer close(stop)
				go func() {
					for {
						select {
						case <-stop:
							return
						case <-time.After(50 * time.Millisecond):
							_ = br.cli.RequestProgress(ctx)
						}
					}
				}()
			}
			// The watch is terminated once it cannot synchronize; the
			// stream ends with it. Nothing on it may advertise the
			// frontier this watch never delivered through.
			for answer := range wch {
				if answer.Canceled {
					continue
				}
				if len(answer.Events) > 0 {
					t.Fatalf("events on a watch whose key never moved: %+v", answer.Events)
				}
				if answer.Header.Revision >= target {
					t.Fatalf("progress advertised revision %d for a live watch that never delivered through %d",
						answer.Header.Revision, target)
				}
			}
			// The barrier is what held progress back: the wait could not
			// return before the terminated watch's teardown finished.
			if held := time.Since(started); held < 300*time.Millisecond+1200*time.Millisecond {
				t.Fatalf("the watch ended after %v, before its teardown could finish", held)
			}
			if br.domain.Watchers() != 0 {
				t.Fatal("the native watch outlived the terminated Kine watch")
			}
		})
	}
}
