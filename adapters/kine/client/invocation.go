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
	bound map[uint64]binding // sequence -> what the sequence was bound to
}

// binding is what an allocated sequence is bound to: the command id and
// a digest of the canonical frame, so a retry that keeps the command id
// but changes the logical bytes or the deadline is refused as a conflict
// rather than emitted as a different frame under the same retry key. The
// digest, not the frame, is kept so a bound sequence does not retain its
// payload.
type binding struct {
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
		bound:    make(map[uint64]binding),
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
	i.bound[seq] = binding{commandID: commandID, frame: sha256.Sum256(inv.Frame)}
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
