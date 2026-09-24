package client

import (
	"bytes"
	"testing"

	"github.com/tuplesky/tuplesky/adapters/kine/wire"
)

func inst(t *testing.T) *Instance {
	t.Helper()
	return NewInstance(InstanceConfig{
		Cluster:  [16]byte{1},
		Domain:   [16]byte{2},
		Session:  [16]byte{3},
		Instance: [16]byte{4},
	})
}

func ackOf(t *testing.T, frame []byte) uint64 {
	t.Helper()
	parsed, rest, err := wire.NextFrame(frame)
	if err != nil || len(rest) != 0 {
		t.Fatalf("frame: %v", err)
	}
	msg, err := wire.Decode(parsed)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	req, ok := msg.(wire.Request)
	if !ok {
		t.Fatalf("not a request: %T", msg)
	}
	return req.AckThrough
}

// A client tells the domain what it has received, and only over the
// contiguous prefix. Nothing else advances the outstanding window's
// floor, so an instance that said nothing would be refused after one
// window of invocations and stay refused for the life of its session.
func TestAcknowledgedFloorFollowsTheContiguousFinishedPrefix(t *testing.T) {
	i := inst(t)
	logical := []byte{0}
	var cmd [32]byte
	// The first invocation has nothing to acknowledge.
	one, err := i.Allocate(logical, cmd, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := ackOf(t, one.Frame); got != 0 {
		t.Fatalf("first acknowledged %d", got)
	}
	two, err := i.Allocate(logical, cmd, 0)
	if err != nil {
		t.Fatal(err)
	}
	three, err := i.Allocate(logical, cmd, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Sequence 2 finishes first. The prefix cannot move over 1, which
	// is still outstanding and may still be retried under its own
	// identity.
	i.Retire(two.Sequence)
	if got := i.Acked(); got != 0 {
		t.Fatalf("a gap advanced the floor to %d", got)
	}
	// 1 finishes: the prefix moves over both at once.
	i.Retire(one.Sequence)
	if got := i.Acked(); got != 2 {
		t.Fatalf("floor %d, want 2", got)
	}
	four, err := i.Allocate(logical, cmd, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := ackOf(t, four.Frame); got != 2 {
		t.Fatalf("fourth acknowledged %d, want 2", got)
	}

	// A retry repeats the floor its first presentation carried, byte
	// for byte: the prefix a command retires is part of what the
	// command is, so a retry acknowledging more would be the same
	// command asking two replicas to retire different prefixes.
	again, err := i.Retry(three.Sequence, logical, cmd, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(again.Frame, three.Frame) {
		t.Fatalf("retry frame differs: acknowledged %d, first carried %d",
			ackOf(t, again.Frame), ackOf(t, three.Frame))
	}
	// And a retired sequence is no longer retryable here at all.
	if _, err := i.Retry(one.Sequence, logical, cmd, 0); err != ErrUnknownSequence {
		t.Fatalf("retired sequence retryable: %v", err)
	}
}
