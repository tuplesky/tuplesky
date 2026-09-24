package client

import (
	"context"
	"sync"

	quic "github.com/quic-go/quic-go"
	"github.com/tuplesky/tuplesky/adapters/kine/wire"
)

// Watch is one long-lived event stream on the watch lane: the WatchOpen
// travels first, then the frontend streams WatchEvents, WatchProgress and
// finally WatchClose. A stream or connection ending without a close is
// ErrWatchLost, and the lane is dropped so the next open reconnects.
type Watch struct {
	c      *Client
	conn   *quic.Conn
	stream *quic.Stream
	id     uint64

	frames chan wire.Message
	done   chan struct{}
	once   sync.Once
	errMu  sync.Mutex
	err    error
}

// OpenWatch opens a watch stream and sends the open frame.
func (c *Client) OpenWatch(ctx context.Context, open wire.WatchOpen) (*Watch, error) {
	conn, err := c.connectLane(ctx, LaneWatch)
	if err != nil {
		return nil, err
	}
	frame, err := wire.Encode(open)
	if err != nil {
		return nil, err
	}
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		c.dropLane(LaneWatch, conn)
		return nil, ErrWatchLost
	}
	// The open is bounded like any other frame write: a peer that stops
	// granting flow control must not hold the caller past its deadline.
	openCtx, cancelOpen := context.WithTimeout(ctx, c.cfg.FrameTimeout)
	defer cancelOpen()
	if err := writeFrameWithin(openCtx, stream, frame); err != nil {
		c.dropLane(LaneWatch, conn)
		return nil, ErrWatchLost
	}
	w := &Watch{c: c, conn: conn, stream: stream, id: open.WatchID, frames: make(chan wire.Message), done: make(chan struct{})}
	go w.read()
	return w, nil
}

func (w *Watch) fail(err error) {
	w.errMu.Lock()
	if w.err == nil {
		w.err = err
	}
	w.errMu.Unlock()
}

// read pumps frames until the stream ends or a close arrives.
func (w *Watch) read() {
	defer close(w.frames)
	for {
		frame, err := readOneFrame(context.Background(), w.stream)
		if err != nil {
			select {
			case <-w.done:
				w.fail(context.Canceled)
			default:
				w.fail(ErrWatchLost)
				w.c.dropLane(LaneWatch, w.conn)
			}
			return
		}
		msg, err := wire.Decode(frame)
		if err != nil {
			w.fail(ErrProtocol)
			w.c.dropLane(LaneWatch, w.conn)
			return
		}
		var closed bool
		switch m := msg.(type) {
		case wire.WatchEvents:
			if m.WatchID != w.id {
				continue
			}
		case wire.WatchProgress:
			if m.WatchID != w.id {
				continue
			}
		case wire.WatchClose:
			if m.WatchID != w.id {
				continue
			}
			closed = true
		default:
			w.fail(ErrProtocol)
			w.c.dropLane(LaneWatch, w.conn)
			return
		}
		select {
		case w.frames <- msg:
		case <-w.done:
			w.fail(context.Canceled)
			return
		}
		if closed {
			return
		}
	}
}

// Next returns the next frame of the watch (WatchEvents, WatchProgress or
// the final WatchClose). After the stream ends it returns the error that
// ended it: ErrWatchLost, ErrProtocol or context.Canceled after Cancel.
func (w *Watch) Next(ctx context.Context) (wire.Message, error) {
	select {
	case msg, ok := <-w.frames:
		if !ok {
			w.errMu.Lock()
			defer w.errMu.Unlock()
			if w.err == nil {
				return nil, ErrWatchLost
			}
			return nil, w.err
		}
		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Cancel asks the frontend to cancel the watch and releases the stream.
// It is idempotent.
func (w *Watch) Cancel() {
	w.once.Do(func() {
		close(w.done)
		if frame, err := wire.Encode(wire.WatchClose{WatchID: w.id, Reason: wire.WatchCancelled}); err == nil {
			// Cancelling must not block on a stalled peer: the watch is
			// being torn down either way.
			cancelCtx, done := context.WithTimeout(context.Background(), w.c.cfg.FrameTimeout)
			_ = writeFrameWithin(cancelCtx, w.stream, frame)
			done()
		}
		_ = w.stream.Close()
		w.stream.CancelRead(0)
	})
}
