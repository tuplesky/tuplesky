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
	bound map[uint64]allocation // sequence -> what the sequence was bound to
}

// allocation is what an allocated sequence is bound to: the command id and
// a digest of the canonical frame, so a retry that keeps the command id
// but changes the logical bytes or the deadline is refused as a conflict
// rather than emitted as a different frame under the same retry key. The
// digest, not the frame, is kept so a bound sequence does not retain its
// payload.
type allocation struct {
	commandID [32]byte
	frame     [sha256.Size]byte
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
		bound:    make(map[uint64]allocation),
	}
}

// Invocation is one request's stable identity and its exact frame bytes.
type Invocation struct {
	Sequence  uint64
	RetryKey  wire.RetryKey
	CommandID [32]byte
	Frame     []byte
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
	i.bound[seq] = allocation{commandID: commandID, frame: sha256.Sum256(inv.Frame)}
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
	i.bound[seq] = allocation{commandID: commandID, frame: sha256.Sum256(inv.Frame)}
	return inv, nil
}

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
	inv, err := i.build(seq, logical, commandID, deadlineMs)
	if err != nil {
		return Invocation{}, err
	}
	if sha256.Sum256(inv.Frame) != bound.frame {
		return Invocation{}, ErrPayloadConflict
	}
	return inv, nil
}

// Release forgets an allocated sequence's binding once its outcome is
// final and the caller will not retry it. The binding exists so that a
// retry is the same invocation and never a different payload under the
// same key; a request that is complete (established, or given up on and
// reported as unknown so the caller issues a new invocation) is never
// retried, and keeping its binding would grow the instance by one entry
// per request for the life of the process. The sequence is not reused:
// `next` has already moved past it, so a later Retry of it is refused as
// unknown rather than rebuilt.
func (i *Instance) Release(seq uint64) {
	i.mu.Lock()
	delete(i.bound, seq)
	i.mu.Unlock()
}

func (i *Instance) build(seq uint64, logical []byte, commandID [32]byte, deadlineMs uint32) (Invocation, error) {
	key := i.retryKey(seq)
	frame, err := wire.Encode(wire.Request{RetryKey: key, Logical: logical, DeadlineMs: deadlineMs})
	if err != nil {
		return Invocation{}, err
	}
	return Invocation{Sequence: seq, RetryKey: key, CommandID: commandID, Frame: frame}, nil
}

// ResolveFrame builds a ResolveRequest frame for an invocation.
func (i *Instance) ResolveFrame(inv Invocation) ([]byte, error) {
	return wire.Encode(wire.ResolveRequest{RetryKey: inv.RetryKey, CommandID: inv.CommandID})
}
