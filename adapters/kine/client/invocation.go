package client

import (
	"crypto/sha256"
	"sync"

	"github.com/tuplesky/tuplesky/adapters/kine/wire"
)

// Instance is a stable client instance: one identity per process, a
// monotonic request sequence, and retry keys and canonical frames
// allocated once and reused verbatim (design Section 6.5). A retry with
// a different payload under an allocated sequence is refused locally.
type Instance struct {
	cluster  [16]byte
	domain   [16]byte
	session  [16]byte
	instance [16]byte

	mu    sync.Mutex
	next  uint64
	bound map[uint64]seqBinding
	// acked is the contiguous prefix of sequences that reached a
	// result here, and finished holds the ones past a gap that have.
	// A sequence still outstanding holds the prefix where it is: its
	// invocation identity has to survive for a retry, and the domain
	// retires identities on this word alone.
	acked    uint64
	finished map[uint64]bool
}

// seqBinding is what an allocated sequence is fixed to: everything
// besides the payload that its frame is built from, so a retry
// reproduces the frame byte for byte rather than one that merely shares
// its identity, and a digest of the canonical frame, so a retry that
// keeps the command id but changes the logical bytes or the deadline is
// refused as a conflict rather than emitted as a different frame under
// the same retry key. The digest, not the frame, is kept so a bound
// sequence does not retain its payload.
type seqBinding struct {
	commandID  [32]byte
	ackThrough uint64
	frame      [sha256.Size]byte
}

// InstanceConfig configures an Instance.
type InstanceConfig struct {
	Cluster  [16]byte
	Domain   [16]byte
	Session  [16]byte
	Instance [16]byte
}

// NewInstance starts an instance at sequence 1.
func NewInstance(cfg InstanceConfig) *Instance {
	return &Instance{
		cluster:  cfg.Cluster,
		domain:   cfg.Domain,
		session:  cfg.Session,
		instance: cfg.Instance,
		next:     1,
		bound:    make(map[uint64]seqBinding),
		finished: make(map[uint64]bool),
	}
}

// Invocation is one request's stable identity and its exact frame bytes.
type Invocation struct {
	Sequence  uint64
	RetryKey  wire.RetryKey
	CommandID [32]byte
	Frame     []byte
	// ackThrough is the floor this invocation's frame acknowledges,
	// kept so a retry repeats it.
	ackThrough uint64
}

func (i *Instance) retryKey(seq uint64) wire.RetryKey {
	return wire.RetryKey{
		ClusterID:       i.cluster,
		DomainID:        i.domain,
		SessionID:       i.session,
		ClientInstance:  i.instance,
		RequestSequence: seq,
	}
}

// Allocate builds a new invocation for a canonical logical request whose
// command identity is `commandID` (derived from the retry key and the
// canonical payload by the caller, matching the Rust derivation).
func (i *Instance) Allocate(logical []byte, commandID [32]byte, deadlineMs uint32) (Invocation, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	seq := i.next
	inv, err := i.build(seq, logical, commandID, deadlineMs)
	if err != nil {
		return Invocation{}, err
	}
	i.next = seq + 1
	i.bound[seq] = seqBinding{commandID: commandID, ackThrough: inv.ackThrough, frame: sha256.Sum256(inv.Frame)}
	return inv, nil
}

// AllocateFor allocates the next sequence and lets `build` derive the
// canonical payload and command identity from the resulting retry key
// (a Kine write's hidden binding is a function of that key). The
// sequence is consumed only when `build` succeeds.
func (i *Instance) AllocateFor(
	deadlineMs uint32,
	build func(key wire.RetryKey) (logical []byte, commandID [32]byte, err error),
) (Invocation, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	seq := i.next
	logical, commandID, err := build(i.retryKey(seq))
	if err != nil {
		return Invocation{}, err
	}
	inv, err := i.build(seq, logical, commandID, deadlineMs)
	if err != nil {
		return Invocation{}, err
	}
	i.next = seq + 1
	i.bound[seq] = seqBinding{commandID: commandID, ackThrough: inv.ackThrough, frame: sha256.Sum256(inv.Frame)}
	return inv, nil
}

// Retire records that `seq` reached a result, and advances the
// acknowledged floor over the contiguous prefix that has. Every
// invocation allocated afterwards carries the floor, which is the only
// thing that lets the domain retire those identities: without it a
// client instance is refused once its sequence runs a window past the
// floor, and stays refused for the life of its session.
//
// Only a sequence the caller is finished with counts. One whose outcome
// is unknown and may still be retried under its own identity holds the
// prefix where it is, because what a retirement gives up is exactly the
// retained result a retry would have been answered from.
func (i *Instance) Retire(seq uint64) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.finished[seq] = true
	for i.finished[i.acked+1] {
		i.acked++
		delete(i.finished, i.acked)
		delete(i.bound, i.acked)
	}
}

// Acked is the floor this instance has acknowledged.
func (i *Instance) Acked() uint64 {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.acked
}

// Release forgets an allocated sequence once its outcome is final and
// the caller will not retry it. It is [`Instance.Retire`] under the name
// a caller that thinks in bindings rather than floors reaches for: the
// sequence counts towards the acknowledged prefix, its binding goes with
// it, and the sequence is never reused.
func (i *Instance) Release(seq uint64) { i.Retire(seq) }

// Retry rebuilds an allocated sequence's invocation; the command id and
// the canonical frame (logical bytes and deadline) must be the ones bound
// at allocation (no implicit fresh identity, no changed payload under a
// stable identity).
func (i *Instance) Retry(seq uint64, logical []byte, commandID [32]byte, deadlineMs uint32) (Invocation, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	bound, ok := i.bound[seq]
	if !ok {
		return Invocation{}, ErrUnknownSequence
	}
	if bound.commandID != commandID {
		return Invocation{}, ErrPayloadConflict
	}
	// The acknowledged floor is repeated, not recomputed: the prefix a
	// command retires is part of what the command durably is, so a retry
	// acknowledging more would be the same command asking two replicas
	// to retire different prefixes.
	inv, err := i.buildAcking(seq, logical, commandID, deadlineMs, bound.ackThrough)
	if err != nil {
		return Invocation{}, err
	}
	if sha256.Sum256(inv.Frame) != bound.frame {
		return Invocation{}, ErrPayloadConflict
	}
	return inv, nil
}

func (i *Instance) build(seq uint64, logical []byte, commandID [32]byte, deadlineMs uint32) (Invocation, error) {
	return i.buildAcking(seq, logical, commandID, deadlineMs, i.acked)
}

func (i *Instance) buildAcking(
	seq uint64,
	logical []byte,
	commandID [32]byte,
	deadlineMs uint32,
	ackThrough uint64,
) (Invocation, error) {
	key := i.retryKey(seq)
	frame, err := wire.Encode(wire.Request{
		RetryKey:   key,
		Logical:    logical,
		DeadlineMs: deadlineMs,
		AckThrough: ackThrough,
	})
	if err != nil {
		return Invocation{}, err
	}
	return Invocation{
		Sequence:   seq,
		RetryKey:   key,
		CommandID:  commandID,
		Frame:      frame,
		ackThrough: ackThrough,
	}, nil
}

// ResolveFrame builds a ResolveRequest frame for an invocation.
func (i *Instance) ResolveFrame(inv Invocation) ([]byte, error) {
	return wire.Encode(wire.ResolveRequest{RetryKey: inv.RetryKey, CommandID: inv.CommandID})
}
