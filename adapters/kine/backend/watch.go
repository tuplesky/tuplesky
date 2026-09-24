package backend

import (
	"context"
	"errors"
	"time"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/tuplesky/tuplesky/adapters/kine/client"
	"github.com/tuplesky/tuplesky/adapters/kine/wire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Watch, synchronization and compaction at the reference pin (design
// Sections 6.8, 19.5). The pin's Backend exposes `Watch(ctx, key, end,
// revision) WatchResult` with event slices and `WaitForSyncTo(revision)`
// without context; nothing here mixes in the newer EventBatch interface.
//
// Frontiers are kept distinct: the domain's processed frontier arrives as
// WatchProgress, the adapter's per-watch delivered frontier advances only
// when a complete revision batch has been handed to Kine's watcher
// goroutine (the events channel is unbuffered), and WaitForSyncTo waits
// on the latter, so a progress marker never overtakes an event.

// Watch errors surfaced on Errorc.
var (
	// ErrSyncTimeout: the watch could not process the awaited revision in
	// time; it is terminated so no false progress is reported, and the
	// consumer resumes from its last revision.
	ErrSyncTimeout = status.Error(codes.Unavailable, "watch did not synchronize in time; resume from the last revision")
	// ErrWatchUnavailable: the watch could not be (re)opened.
	ErrWatchUnavailable = status.Error(codes.Unavailable, "watch source unavailable; resume from the last revision")
	// ErrWatchUnauthorized: output authorization denied a batch.
	ErrWatchUnauthorized = status.Error(codes.PermissionDenied, "watch output not authorized by current policy")
)

// watchState is the adapter's view of one Kine watch.
type watchState struct {
	id uint64
	// processed is the revision through which this watch's events have
	// been delivered to Kine (or a domain progress marker beyond them,
	// received after every earlier batch was delivered).
	processed uint64
	// terminal ends the watch from WaitForSyncTo or Close.
	terminal error
	cancel   context.CancelFunc
	// withdrawn records that a synchronization wait gave up on this watch
	// and asked it to end. It stays in `watches` until the pump has
	// stopped and its delivery channel is closed, because a cancellation
	// that has only been *requested* leaves Kine's watcher goroutine
	// parked on its progress channel, ready to advertise a revision this
	// watch never delivered.
	withdrawn bool
	// closed is set once the pump has stopped and the delivery channel is
	// closed, so nothing more can reach Kine's watcher through it.
	closed bool
}

// wake replaces the change channel; called with wmu held.
func (b *Backend) wake() {
	close(b.changed)
	b.changed = make(chan struct{})
}

func (b *Backend) advance(ws *watchState, revision uint64) {
	b.wmu.Lock()
	// A withdrawn watch is on its way out: a marker that arrives after
	// its cancellation was requested is not delivery through the awaited
	// revision, and must not release the wait that gave up on it.
	if !ws.withdrawn && revision > ws.processed {
		ws.processed = revision
		b.wake()
	}
	b.wmu.Unlock()
}

// endWatch publishes the watch's teardown. It runs only after the
// delivery channel is closed, so an observer that sees `closed` knows
// nothing can be delivered through this watch any more.
func (b *Backend) endWatch(ws *watchState) {
	b.wmu.Lock()
	ws.closed = true
	delete(b.watches, ws.id)
	b.wake()
	b.wmu.Unlock()
}

// teardownDelay is the configured fault-injection pause between asking a
// watch to stop and finishing its teardown. Production leaves it zero;
// tests widen the window in which the watch is cancelled but still live.
func (b *Backend) teardownDelay() {
	if b.cfg.WatchTeardownDelay > 0 {
		time.Sleep(b.cfg.WatchTeardownDelay)
	}
}

// Watch opens one native event stream from `revision` (inclusive; 0
// resumes right after the authoritative frontier) and delivers complete
// revisions as Kine event batches, resuming after a lost stream from the
// last complete revision. The start metadata the pin's WatchResult
// requires synchronously (compacted start, current revision) comes from
// one ordered count-only read at the start revision before the stream is
// opened: a compacted start is reported with CompactRevision set so the
// bridge cancels it with the compacted error. Compaction of needed
// history during the watch ends it with ErrCompacted; slow-consumer and
// source loss resume; cancellation of the Kine context or Close ends it.
func (b *Backend) Watch(ctx context.Context, key, end string, revision int64) server.WatchResult {
	events := make(chan []*server.Event)
	errc := make(chan error, 1)
	failed := func(current int64, compact int64, err error) server.WatchResult {
		close(events)
		errc <- err
		return server.WatchResult{CurrentRevision: current, CompactRevision: compact, Events: events, Errorc: errc}
	}
	if revision < 0 {
		return failed(0, 0, server.ErrCompacted)
	}
	if b.root.Err() != nil {
		return failed(0, 0, status.Error(codes.Unavailable, ErrClosed.Error()))
	}
	// One ordered read anchors the start: at `revision` it answers
	// compacted or future; at the frontier it fixes the live boundary.
	at, _ := revisionOption(revision)
	res, err := b.invoke(ctx, "Watch", func(wire.RetryKey) wire.LogicalOp {
		return wire.RangeOp{Range: wire.KeyRange{Key: []byte(server.HealthKey)}, Revision: at, CountOnly: true}
	})
	if err != nil {
		return failed(0, 0, err)
	}
	current, err := toInt64(res.Revision)
	if err != nil {
		return failed(0, 0, err)
	}
	start := uint64(revision)
	switch res.Kind {
	case wire.OutcomeRange:
		if start == 0 {
			start = uint64(current) + 1
		}
	case wire.OutcomeErrFutureRevision:
		// A future start waits for its revision.
	case wire.OutcomeErrCompacted:
		return failed(current, revision, server.ErrCompacted)
	default:
		return failed(current, 0, mapOutcomeError(res))
	}
	wctx, cancel := context.WithCancel(ctx)
	b.wmu.Lock()
	b.nextWatch++
	ws := &watchState{id: b.nextWatch, cancel: cancel, processed: start - 1}
	b.watches[ws.id] = ws
	b.wmu.Unlock()
	go func() {
		select {
		case <-b.root.Done():
			cancel()
		case <-wctx.Done():
		}
	}()
	go b.runWatch(wctx, ws, key, end, events, errc)
	return server.WatchResult{CurrentRevision: current, Events: events, Errorc: errc}
}

func (b *Backend) observeWatch(kind string, ws *watchState, outcome string) {
	if b.cfg.Observer != nil {
		b.cfg.Observer(Event{Op: "Watch", Kind: kind, Sequence: ws.id, Outcome: outcome})
	}
}

func (b *Backend) runWatch(ctx context.Context, ws *watchState, key, end string, events chan<- []*server.Event, errc chan<- error) {
	// Teardown order matters: the delivery channel is closed before the
	// watch is published as ended, so a synchronization wait that is
	// blocked on this watch cannot return while Kine's watcher goroutine
	// is still parked on a live channel.
	defer b.endWatch(ws)
	defer close(events)
	defer b.teardownDelay()
	defer ws.cancel()
	fail := func(err error) {
		errc <- err
	}
	var rangeEnd *[]byte
	if end != "" {
		e := []byte(end)
		rangeEnd = &e
	}
	// The reconnect bound counts consecutive attempts that did not get a
	// stream to the point of carrying a frame: a refused open and a stream
	// the peer accepted but ended before delivering anything are the same
	// failure to this watch. Only a stream that served a frame proves the
	// source reachable again and clears the count, so a peer that accepts
	// every open and resets it at once still exhausts the bound instead of
	// being reopened in a hot loop.
	attempts := 0
	retry := func() bool {
		attempts++
		if attempts > b.cfg.WatchReconnectAttempts {
			fail(ErrWatchUnavailable)
			return false
		}
		select {
		case <-time.After(b.cfg.WatchReconnectBackoff * time.Duration(attempts)):
			return true
		case <-ctx.Done():
			b.finish(ws, fail)
			return false
		}
	}
	for {
		b.wmu.Lock()
		resume := ws.processed + 1
		b.wmu.Unlock()
		w, err := b.cfg.Client.OpenWatch(ctx, wire.WatchOpen{
			WatchID:        ws.id,
			Namespace:      b.cfg.Namespace,
			Key:            []byte(key),
			RangeEnd:       rangeEnd,
			StartRevision:  &resume,
			PrevKV:         true,
			ProgressNotify: true,
		})
		if err != nil {
			if ctx.Err() != nil {
				b.finish(ws, fail)
				return
			}
			b.observeWatch("WatchOpen", ws, "error: "+err.Error())
			if !retry() {
				return
			}
			continue
		}
		b.observeWatch("WatchOpen", ws, "opened")
		served, lost, err := b.pump(ctx, ws, w, events)
		w.Cancel()
		if err != nil {
			fail(err)
			return
		}
		if !lost {
			b.finish(ws, fail)
			return
		}
		b.observeWatch("WatchOpen", ws, "lost")
		if served {
			attempts = 0
			continue
		}
		if !retry() {
			return
		}
	}
}

// finish reports the terminal error a wait or Close set, if any.
func (b *Backend) finish(ws *watchState, fail func(error)) {
	b.wmu.Lock()
	terminal := ws.terminal
	b.wmu.Unlock()
	if terminal != nil {
		fail(terminal)
	}
}

func kineEvent(e wire.Event) *server.Event {
	ev := &server.Event{
		KV: &server.KeyValue{
			Key:            string(e.Key),
			Value:          e.Value,
			CreateRevision: int64(e.CreateRevision),
			ModRevision:    int64(e.ModRevision),
			Version:        int64(e.Version),
		},
		Delete: e.Kind == wire.EventDelete,
		Create: e.Kind == wire.EventPut && e.Version == 1,
	}
	if e.PrevValue != nil {
		ev.PrevKV = &server.KeyValue{Key: string(e.Key), Value: *e.PrevValue}
	}
	return ev
}

// pump delivers one stream. It returns (served, lost, terminal): served
// says the stream carried at least one event or progress frame, so the
// source was reachable through it; lost asks for a resume from the
// processed frontier; terminal ends the watch.
func (b *Backend) pump(ctx context.Context, ws *watchState, w *client.Watch, events chan<- []*server.Event) (served bool, lost bool, terminal error) {
	var pending []*server.Event
	pendingRev := uint64(0)
	for {
		msg, err := w.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return served, false, nil
			}
			if errors.Is(err, client.ErrWatchLost) || errors.Is(err, client.ErrProtocol) {
				return served, true, nil
			}
			return served, false, status.Error(codes.Unavailable, err.Error())
		}
		switch m := msg.(type) {
		case wire.WatchEvents:
			served = true
			b.wmu.Lock()
			processed := ws.processed
			b.wmu.Unlock()
			if m.Revision <= processed {
				// Replay overlap after a resume: already delivered.
				continue
			}
			if pending != nil && pendingRev != m.Revision {
				return served, false, status.Error(codes.Internal, "interleaved revision chunks")
			}
			pendingRev = m.Revision
			for _, e := range m.Events {
				pending = append(pending, kineEvent(e))
			}
			if !m.Complete {
				continue
			}
			batch := pending
			pending = nil
			if len(batch) == 0 {
				b.advance(ws, m.Revision)
				continue
			}
			// The channel is unbuffered: the send completes only when
			// Kine's watcher goroutine has taken the complete revision.
			select {
			case events <- batch:
				b.advance(ws, m.Revision)
			case <-ctx.Done():
				return served, false, nil
			}
		case wire.WatchProgress:
			served = true
			if pending == nil {
				b.advance(ws, m.Revision)
			}
		case wire.WatchClose:
			switch m.Reason {
			case wire.WatchCompacted:
				return served, false, server.ErrCompacted
			case wire.WatchUnauthorized:
				return served, false, ErrWatchUnauthorized
			case wire.WatchSlowConsumer, wire.WatchSourceLost:
				if m.LastCompleteRevision != nil {
					b.advance(ws, *m.LastCompleteRevision)
				}
				return served, true, nil
			default:
				// Cancelled without our cancel: the source replaced or
				// ended the stream; resume.
				return served, ctx.Err() == nil, nil
			}
		}
	}
}

// laggingLocked is the set of live watches that have not processed
// `target` yet; wmu must be held.
func (b *Backend) laggingLocked(target uint64) []*watchState {
	var lagging []*watchState
	for _, ws := range b.watches {
		if !ws.closed && ws.processed < target {
			lagging = append(lagging, ws)
		}
	}
	return lagging
}

// WaitForSyncTo blocks until every open watch has processed `revision`
// (delivered its events through it, or received the domain's progress
// beyond them), so a progress notification Kine sends afterwards never
// precedes an event.
//
// Returning is what lets the bridge publish progress at `revision`, on
// both of its paths: ProgressIfSynced offers the revision to each
// watch's progress channel and ProgressAll broadcasts once every channel
// accepts. A watch whose Kine-side goroutine is parked on that channel
// accepts it whether or not this side has asked the watch to stop, so
// requesting cancellation is not enough to keep the watch out of
// progress publication: a watch that cannot synchronize is terminated
// *and then waited for*, until its pump has stopped and its delivery
// channel is closed. The wait therefore never reports synchronization
// that did not happen; only Close (through the root context) ends it
// otherwise. Blocking the bridge's progress timer is the conservative
// failure: a false progress report would make a consumer skip the events
// it never received.
func (b *Backend) WaitForSyncTo(revision int64) {
	if revision <= 0 {
		return
	}
	target := uint64(revision)
	deadline := time.NewTimer(b.cfg.SyncTimeout)
	defer deadline.Stop()
	withdrawn := false
	for {
		b.wmu.Lock()
		if b.root.Err() != nil {
			b.wmu.Unlock()
			return
		}
		lagging := b.laggingLocked(target)
		if len(lagging) == 0 {
			b.wmu.Unlock()
			return
		}
		changed := b.changed
		b.wmu.Unlock()
		if withdrawn {
			// The laggards were asked to stop; wait for them to be gone.
			select {
			case <-changed:
			case <-b.root.Done():
				return
			}
			continue
		}
		select {
		case <-changed:
		case <-b.root.Done():
			return
		case <-deadline.C:
			b.wmu.Lock()
			for _, ws := range b.laggingLocked(target) {
				if ws.terminal == nil {
					ws.terminal = ErrSyncTimeout
				}
				ws.withdrawn = true
				ws.cancel()
			}
			b.wmu.Unlock()
			withdrawn = true
		}
	}
}

// Compact is one ordered retention command: the floor advances to at
// most the current revision. Watches needing compacted history are ended
// by the domain with a compacted close, never silently.
func (b *Backend) Compact(ctx context.Context, revision int64) (int64, error) {
	if revision <= 0 {
		return 0, status.Error(codes.InvalidArgument, "compaction requires a positive revision")
	}
	res, err := b.invoke(ctx, "Compact", func(wire.RetryKey) wire.LogicalOp {
		return wire.CompactOp{Revision: uint64(revision)}
	})
	if err != nil {
		return 0, err
	}
	if res.Kind != wire.OutcomeCompacted {
		return 0, mapOutcomeError(res)
	}
	header, err := toInt64(res.Revision)
	if err != nil {
		return 0, err
	}
	if revision < header {
		return revision, nil
	}
	return header, nil
}
