package wire

import (
	"errors"
	"fmt"
)

// The Kine subset of the Rust `logical_v1` schema (design Sections 6.1,
// 6.6, 19.5): the canonical operation bytes that participate in command
// identity. Field order, variant discriminants and integer widths are the
// Rust schema's; this package mirrors only the operations the trusted
// Kine collector issues (Range, KineCreate, KineUpdate, KineDelete) and is
// verified against `crates/coord-types/fixtures/command_ids_v1.json`.

// Logical limits owned by the Rust schema (`logical_v1::limits`).
const (
	// LogicalSchemaVersion is the frozen schema version tag.
	LogicalSchemaVersion = 1
	// MaxKeyBytes bounds a key.
	MaxKeyBytes = maxKeyBytes
	// MaxValueBytes bounds a value.
	MaxValueBytes = maxValueBytes
	// MaxRequestBytes bounds one logical request's key and value bytes.
	MaxRequestBytes = 2 * 1024 * 1024
	// MaxLeaseTTLSeconds bounds a Kine TTL (one week).
	MaxLeaseTTLSeconds = 7 * 24 * 3600
	// MaxPageLimit bounds one page.
	MaxPageLimit = 10_000
)

// Logical operation discriminants (frozen `CanonicalOperation` order).
const (
	opRange      = 0
	opCompact    = 8
	opKineCreate = 9
	opKineUpdate = 10
	opKineDelete = 11
)

// ErrInvalidLogical: the request violates a schema rule or limit; the
// detail names the Rust `ValidationError` variant.
var ErrInvalidLogical = errors.New("invalid logical request")

func invalid(variant string) error {
	return fmt.Errorf("%w: %s", ErrInvalidLogical, variant)
}

// KeyRange is an exact key (RangeEnd nil) or a half-open interval.
type KeyRange struct {
	Key      []byte
	RangeEnd []byte
}

func (k KeyRange) validate() (int, error) {
	if err := validateKey(k.Key); err != nil {
		return 0, err
	}
	if k.RangeEnd != nil {
		if len(k.RangeEnd) > MaxKeyBytes {
			return 0, invalid("KeyTooLong")
		}
		if string(k.RangeEnd) <= string(k.Key) {
			return 0, invalid("EmptyRange")
		}
	}
	return len(k.Key) + len(k.RangeEnd), nil
}

func validateKey(key []byte) error {
	if len(key) == 0 {
		return invalid("EmptyKey")
	}
	if len(key) > MaxKeyBytes {
		return invalid("KeyTooLong")
	}
	return nil
}

// LogicalOp is one canonical operation of the Kine subset.
type LogicalOp interface {
	discriminant() uint64
	validate() (int, error)
	encodeOp(w *writer)
}

// RangeOp reads a key or interval. Revision nil is the latest linearizable
// state; Limit 0 is the schema maximum.
type RangeOp struct {
	Range     KeyRange
	Revision  *uint64
	Limit     uint32
	KeysOnly  bool
	CountOnly bool
}

func (RangeOp) discriminant() uint64 { return opRange }
func (r RangeOp) validate() (int, error) {
	cost, err := r.Range.validate()
	if err != nil {
		return 0, err
	}
	if r.Limit > MaxPageLimit {
		return 0, invalid("LimitTooLarge")
	}
	return cost, nil
}
func (r RangeOp) encodeOp(w *writer) {
	w.boundedBytes(r.Range.Key)
	if r.Range.RangeEnd == nil {
		w.byte(0)
	} else {
		w.byte(1)
		w.boundedBytes(r.Range.RangeEnd)
	}
	if r.Revision == nil {
		w.byte(0)
	} else {
		w.byte(1)
		w.varint(*r.Revision)
	}
	w.varint(uint64(r.Limit))
	w.boolean(r.KeysOnly)
	w.boolean(r.CountOnly)
}

// KineCreateOp creates the key if absent with an optional private TTL
// binding, present exactly when TTLSeconds > 0.
type KineCreateOp struct {
	Key        []byte
	Value      []byte
	TTLSeconds uint32
	Binding    *[idBytes]byte
}

func (KineCreateOp) discriminant() uint64 { return opKineCreate }
func (c KineCreateOp) validate() (int, error) {
	return validateKineWrite(c.Key, c.Value, c.TTLSeconds, c.Binding)
}
func (c KineCreateOp) encodeOp(w *writer) {
	w.boundedBytes(c.Key)
	w.boundedBytes(c.Value)
	w.varint(uint64(c.TTLSeconds))
	encodeOptionID(w, c.Binding)
}

// KineUpdateOp compares the key's modification revision and updates it,
// replacing (TTL 0: removing) its private TTL binding.
type KineUpdateOp struct {
	Key                 []byte
	Value               []byte
	ExpectedModRevision uint64
	TTLSeconds          uint32
	Binding             *[idBytes]byte
}

func (KineUpdateOp) discriminant() uint64 { return opKineUpdate }
func (u KineUpdateOp) validate() (int, error) {
	cost, err := validateKineWrite(u.Key, u.Value, u.TTLSeconds, u.Binding)
	if err != nil {
		return 0, err
	}
	if u.ExpectedModRevision == 0 {
		return 0, invalid("ZeroRevision")
	}
	return cost, nil
}
func (u KineUpdateOp) encodeOp(w *writer) {
	w.boundedBytes(u.Key)
	w.boundedBytes(u.Value)
	w.varint(u.ExpectedModRevision)
	w.varint(uint64(u.TTLSeconds))
	encodeOptionID(w, u.Binding)
}

// KineDeleteOp deletes the key when its modification revision matches
// (ExpectedModRevision nil: unconditionally).
type KineDeleteOp struct {
	Key                 []byte
	ExpectedModRevision *uint64
}

func (KineDeleteOp) discriminant() uint64 { return opKineDelete }
func (d KineDeleteOp) validate() (int, error) {
	if err := validateKey(d.Key); err != nil {
		return 0, err
	}
	if d.ExpectedModRevision != nil && *d.ExpectedModRevision == 0 {
		return 0, invalid("ZeroRevision")
	}
	return len(d.Key), nil
}
func (d KineDeleteOp) encodeOp(w *writer) {
	w.boundedBytes(d.Key)
	if d.ExpectedModRevision == nil {
		w.byte(0)
	} else {
		w.byte(1)
		w.varint(*d.ExpectedModRevision)
	}
}

// CompactOp advances the ordered MVCC retention floor to Revision (at
// most the current revision); physical maintenance follows asynchronously.
type CompactOp struct {
	Revision uint64
}

func (CompactOp) discriminant() uint64 { return opCompact }
func (c CompactOp) validate() (int, error) {
	if c.Revision == 0 {
		return 0, invalid("ZeroRevision")
	}
	return 0, nil
}
func (c CompactOp) encodeOp(w *writer) { w.varint(c.Revision) }

func validateKineWrite(key, value []byte, ttl uint32, binding *[idBytes]byte) (int, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	if len(value) > MaxValueBytes {
		return 0, invalid("ValueTooLong")
	}
	if ttl > MaxLeaseTTLSeconds {
		return 0, invalid("TtlTooLong")
	}
	if (binding != nil) != (ttl > 0) {
		return 0, invalid("BindingMismatch")
	}
	return len(key) + len(value), nil
}

func encodeOptionID(w *writer, id *[idBytes]byte) {
	if id == nil {
		w.byte(0)
	} else {
		w.byte(1)
		w.raw(id[:])
	}
}

// LogicalRequest is a domain-scoped tenant namespace plus one operation.
type LogicalRequest struct {
	Namespace [idBytes]byte
	Op        LogicalOp
}

// Encode produces the canonical postcard bytes (the identity payload),
// failing when the request violates a schema rule.
func (l LogicalRequest) Encode() ([]byte, error) {
	if l.Op == nil {
		return nil, invalid("NoOperation")
	}
	cost, err := l.Op.validate()
	if err != nil {
		return nil, err
	}
	if cost > MaxRequestBytes {
		return nil, invalid("RequestTooLarge")
	}
	w := &writer{}
	w.varint(LogicalSchemaVersion)
	w.raw(l.Namespace[:])
	w.varint(l.Op.discriminant())
	l.Op.encodeOp(w)
	return w.buf, nil
}

// DecodeLogical parses canonical bytes of the Kine subset with full
// consumption; other operations of the schema are refused.
func DecodeLogical(payload []byte) (LogicalRequest, error) {
	r := newReader(payload)
	version, err := r.u16()
	if err != nil {
		return LogicalRequest{}, err
	}
	if version != LogicalSchemaVersion {
		return LogicalRequest{}, invalid("SchemaVersion")
	}
	var out LogicalRequest
	if out.Namespace, err = readID(r); err != nil {
		return LogicalRequest{}, err
	}
	disc, err := r.varint(10)
	if err != nil {
		return LogicalRequest{}, err
	}
	switch disc {
	case opRange:
		out.Op, err = decodeRangeOp(r)
	case opKineCreate:
		out.Op, err = decodeKineCreate(r)
	case opKineUpdate:
		out.Op, err = decodeKineUpdate(r)
	case opKineDelete:
		out.Op, err = decodeKineDelete(r)
	case opCompact:
		var rev uint64
		rev, err = r.u64()
		out.Op = CompactOp{Revision: rev}
	default:
		return LogicalRequest{}, invalid("UnsupportedOperation")
	}
	if err != nil {
		return LogicalRequest{}, err
	}
	if !r.done() {
		return LogicalRequest{}, ErrTrailingPayloadBytes
	}
	if _, err := out.Op.validate(); err != nil {
		return LogicalRequest{}, err
	}
	return out, nil
}

func readOptionU64(r *reader) (*uint64, error) {
	some, err := r.option()
	if err != nil || !some {
		return nil, err
	}
	v, err := r.u64()
	if err != nil {
		return nil, err
	}
	return &v, nil
}

func readOptionID(r *reader) (*[idBytes]byte, error) {
	some, err := r.option()
	if err != nil || !some {
		return nil, err
	}
	id, err := readID(r)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

func decodeRangeOp(r *reader) (LogicalOp, error) {
	var op RangeOp
	var err error
	if op.Range.Key, err = r.boundedBytes(maxKeyBytes); err != nil {
		return nil, err
	}
	some, err := r.option()
	if err != nil {
		return nil, err
	}
	if some {
		if op.Range.RangeEnd, err = r.boundedBytes(maxKeyBytes); err != nil {
			return nil, err
		}
	}
	if op.Revision, err = readOptionU64(r); err != nil {
		return nil, err
	}
	if op.Limit, err = r.u32(); err != nil {
		return nil, err
	}
	if op.KeysOnly, err = r.boolean(); err != nil {
		return nil, err
	}
	if op.CountOnly, err = r.boolean(); err != nil {
		return nil, err
	}
	return op, nil
}

func decodeKineCreate(r *reader) (LogicalOp, error) {
	var op KineCreateOp
	var err error
	if op.Key, err = r.boundedBytes(maxKeyBytes); err != nil {
		return nil, err
	}
	if op.Value, err = r.boundedBytes(maxValueBytes); err != nil {
		return nil, err
	}
	if op.TTLSeconds, err = r.u32(); err != nil {
		return nil, err
	}
	if op.Binding, err = readOptionID(r); err != nil {
		return nil, err
	}
	return op, nil
}

func decodeKineUpdate(r *reader) (LogicalOp, error) {
	var op KineUpdateOp
	var err error
	if op.Key, err = r.boundedBytes(maxKeyBytes); err != nil {
		return nil, err
	}
	if op.Value, err = r.boundedBytes(maxValueBytes); err != nil {
		return nil, err
	}
	if op.ExpectedModRevision, err = r.u64(); err != nil {
		return nil, err
	}
	if op.TTLSeconds, err = r.u32(); err != nil {
		return nil, err
	}
	if op.Binding, err = readOptionID(r); err != nil {
		return nil, err
	}
	return op, nil
}

func decodeKineDelete(r *reader) (LogicalOp, error) {
	var op KineDeleteOp
	var err error
	if op.Key, err = r.boundedBytes(maxKeyBytes); err != nil {
		return nil, err
	}
	if op.ExpectedModRevision, err = readOptionU64(r); err != nil {
		return nil, err
	}
	return op, nil
}

// CanonicalBytes is the fixed 72-byte retry-key encoding hashed into
// command identity: four 16-byte ids then the big-endian sequence.
func (k RetryKey) CanonicalBytes() [72]byte {
	var out [72]byte
	copy(out[0:16], k.ClusterID[:])
	copy(out[16:32], k.DomainID[:])
	copy(out[32:48], k.SessionID[:])
	copy(out[48:64], k.ClientInstance[:])
	for i := 0; i < 8; i++ {
		out[64+i] = byte(k.RequestSequence >> (8 * (7 - i)))
	}
	return out
}
