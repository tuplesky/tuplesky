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
	closed   bool
}

// wake replaces the change channel; called with wmu held.
func (b *Backend) wake() {
	close(b.changed)
	b.changed = make(chan struct{})
}

func (b *Backend) advance(ws *watchState, revision uint64) {
	b.wmu.Lock()
	if revision > ws.processed {
		ws.processed = revision
		b.wake()
	}
	b.wmu.Unlock()
}

func (b *Backend) endWatch(ws *watchState) {
	b.wmu.Lock()
	ws.closed = true
	delete(b.watches, ws.id)
	b.wake()
	b.wmu.Unlock()
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
		return failed(current, 0, mapOutcomeError(res.Kind))
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
	defer close(events)
	defer b.endWatch(ws)
	defer ws.cancel()
	fail := func(err error) {
		errc <- err
	}
	var rangeEnd *[]byte
	if end != "" {
		e := []byte(end)
		rangeEnd = &e
	}
	attempts := 0
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
			attempts++
			b.observeWatch("WatchOpen", ws, "error: "+err.Error())
			if attempts > b.cfg.WatchReconnectAttempts {
				fail(ErrWatchUnavailable)
				return
			}
			select {
			case <-time.After(b.cfg.WatchReconnectBackoff * time.Duration(attempts)):
			case <-ctx.Done():
				b.finish(ws, fail)
				return
			}
			continue
		}
		attempts = 0
		b.observeWatch("WatchOpen", ws, "opened")
		lost, err := b.pump(ctx, ws, w, events)
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

// pump delivers one stream. It returns (lost, terminal): lost asks for
// a resume from the processed frontier; terminal ends the watch.
func (b *Backend) pump(ctx context.Context, ws *watchState, w *client.Watch, events chan<- []*server.Event) (bool, error) {
	var pending []*server.Event
	pendingRev := uint64(0)
	for {
		msg, err := w.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return false, nil
			}
			if errors.Is(err, client.ErrWatchLost) || errors.Is(err, client.ErrProtocol) {
				return true, nil
			}
			return false, status.Error(codes.Unavailable, err.Error())
		}
		switch m := msg.(type) {
		case wire.WatchEvents:
			b.wmu.Lock()
			processed := ws.processed
			b.wmu.Unlock()
			if m.Revision <= processed {
				// Replay overlap after a resume: already delivered.
				continue
			}
			if pending != nil && pendingRev != m.Revision {
				return false, status.Error(codes.Internal, "interleaved revision chunks")
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
				return false, nil
			}
		case wire.WatchProgress:
			if pending == nil {
				b.advance(ws, m.Revision)
			}
		case wire.WatchClose:
			switch m.Reason {
			case wire.WatchCompacted:
				return false, server.ErrCompacted
			case wire.WatchUnauthorized:
				return false, ErrWatchUnauthorized
			case wire.WatchSlowConsumer, wire.WatchSourceLost:
				if m.LastCompleteRevision != nil {
					b.advance(ws, *m.LastCompleteRevision)
				}
				return true, nil
			default:
				// Cancelled without our cancel: the source replaced or
				// ended the stream; resume.
				return ctx.Err() == nil, nil
			}
		}
	}
}

// WaitForSyncTo blocks until every open watch has processed `revision`
// (delivered its events through it, or received the domain's progress
// beyond them), so a progress notification Kine sends afterwards never
// precedes an event. The wait ends early only by terminating the
// lagging watches (SyncTimeout) or by Close; it never returns while a
// live watch is behind.
func (b *Backend) WaitForSyncTo(revision int64) {
	if revision <= 0 {
		return
	}
	target := uint64(revision)
	deadline := time.NewTimer(b.cfg.SyncTimeout)
	defer deadline.Stop()
	for {
		b.wmu.Lock()
		if b.root.Err() != nil {
			b.wmu.Unlock()
			return
		}
		var lagging []*watchState
		for _, ws := range b.watches {
			if !ws.closed && ws.processed < target {
				lagging = append(lagging, ws)
			}
		}
		if len(lagging) == 0 {
			b.wmu.Unlock()
			return
		}
		changed := b.changed
		select {
		case <-deadline.C:
			for _, ws := range lagging {
				if ws.terminal == nil {
					ws.terminal = ErrSyncTimeout
				}
				ws.cancel()
			}
			b.wmu.Unlock()
			return
		default:
		}
		b.wmu.Unlock()
		select {
		case <-changed:
		case <-deadline.C:
			// Loop once more to terminate the laggards.
			deadline.Reset(0)
		case <-b.root.Done():
			return
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
		return 0, mapOutcomeError(res.Kind)
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
