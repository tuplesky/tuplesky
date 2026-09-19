package wire

import "errors"

// The Kine subset of the Rust `coord_state::Response` result schema: the
// exact postcard bytes a released `Ok` result carries. Variant
// discriminants are the frozen `Outcome` order; this package decodes the
// outcomes a Kine operation can produce and refuses every other variant
// as unexpected, verified against
// `crates/coord-state/fixtures/kine_responses_v1.json`.

// OutcomeKind is a `coord_state::Outcome` discriminant.
type OutcomeKind uint8

// The outcomes a Kine request can receive.
const (
	OutcomeRange               OutcomeKind = 2
	OutcomeCompacted           OutcomeKind = 4
	OutcomeErrCompacted        OutcomeKind = 5
	OutcomeErrFutureRevision   OutcomeKind = 6
	OutcomeErrLeaseExists      OutcomeKind = 11
	OutcomeKineCreated         OutcomeKind = 19
	OutcomeErrKeyExists        OutcomeKind = 20
	OutcomeKineUpdated         OutcomeKind = 21
	OutcomeKineDeleted         OutcomeKind = 22
	OutcomeErrSessionInvalid   OutcomeKind = 23
	OutcomeErrPermissionDenied OutcomeKind = 24
)

// ErrUnexpectedOutcome: the result names an outcome no Kine request
// produces.
var ErrUnexpectedOutcome = errors.New("unexpected outcome")

// KvEntry is a stored entry.
type KvEntry struct {
	Value           []byte
	CreateRevision  uint64
	ModRevision     uint64
	Version         uint64
	Lease           *[idBytes]byte
	LeaseGeneration *uint64
}

// RangeItem is one item of a range result.
type RangeItem struct {
	Key   []byte
	Entry KvEntry
}

// KineKv is a Kine-facing entry: the value and revisions of the entry
// plus the TTL of its private binding. It carries no lease identity at
// all: the hidden binding (and any native lease) never reaches a Kine
// caller, so there is no KvEntry to embed here. This mirrors
// coord_state::KineKv field for field, which is what the leader encodes.
type KineKv struct {
	Key            []byte
	Value          []byte
	CreateRevision uint64
	ModRevision    uint64
	Version        uint64
	TTLSeconds     uint32
}

// Result is a decoded response: the header revision plus the outcome.
// Only the fields of Kind are meaningful.
type Result struct {
	Revision uint64
	Kind     OutcomeKind
	// Range:
	Items []RangeItem
	Count uint64
	More  bool
	// KineUpdated:
	Updated bool
	Current *KineKv
	// KineDeleted:
	Deleted bool
	Prev    *KineKv
}

// DecodeResult parses the postcard bytes of a response with full
// consumption.
func DecodeResult(payload []byte) (Result, error) {
	r := newReader(payload)
	var out Result
	var err error
	if out.Revision, err = r.u64(); err != nil {
		return Result{}, err
	}
	disc, err := r.varint(10)
	if err != nil {
		return Result{}, err
	}
	out.Kind = OutcomeKind(disc)
	switch out.Kind {
	case OutcomeRange:
		n, err := r.length(MaxPageLimit)
		if err != nil {
			return Result{}, err
		}
		out.Items = make([]RangeItem, 0, n)
		for i := 0; i < n; i++ {
			var item RangeItem
			if item.Key, err = r.boundedBytes(maxKeyBytes); err != nil {
				return Result{}, err
			}
			if item.Entry, err = readEntry(r); err != nil {
				return Result{}, err
			}
			out.Items = append(out.Items, item)
		}
		if out.Count, err = r.u64(); err != nil {
			return Result{}, err
		}
		if out.More, err = r.boolean(); err != nil {
			return Result{}, err
		}
	case OutcomeKineUpdated:
		if out.Updated, err = r.boolean(); err != nil {
			return Result{}, err
		}
		if out.Current, err = readOptionKineKv(r); err != nil {
			return Result{}, err
		}
	case OutcomeKineDeleted:
		if out.Deleted, err = r.boolean(); err != nil {
			return Result{}, err
		}
		if out.Prev, err = readOptionKineKv(r); err != nil {
			return Result{}, err
		}
	case OutcomeCompacted, OutcomeErrCompacted, OutcomeErrFutureRevision, OutcomeErrLeaseExists,
		OutcomeKineCreated, OutcomeErrKeyExists, OutcomeErrSessionInvalid,
		OutcomeErrPermissionDenied:
	default:
		return Result{}, ErrUnexpectedOutcome
	}
	if !r.done() {
		return Result{}, ErrTrailingPayloadBytes
	}
	return out, nil
}

func readEntry(r *reader) (KvEntry, error) {
	var e KvEntry
	var err error
	if e.Value, err = r.boundedBytes(maxValueBytes); err != nil {
		return e, err
	}
	if e.CreateRevision, err = r.u64(); err != nil {
		return e, err
	}
	if e.ModRevision, err = r.u64(); err != nil {
		return e, err
	}
	if e.Version, err = r.u64(); err != nil {
		return e, err
	}
	if e.Lease, err = readOptionID(r); err != nil {
		return e, err
	}
	if e.LeaseGeneration, err = readOptionU64(r); err != nil {
		return e, err
	}
	return e, nil
}

func readOptionKineKv(r *reader) (*KineKv, error) {
	some, err := r.option()
	if err != nil || !some {
		return nil, err
	}
	var kv KineKv
	if kv.Key, err = r.boundedBytes(maxKeyBytes); err != nil {
		return nil, err
	}
	if kv.Value, err = r.boundedBytes(maxValueBytes); err != nil {
		return nil, err
	}
	if kv.CreateRevision, err = r.u64(); err != nil {
		return nil, err
	}
	if kv.ModRevision, err = r.u64(); err != nil {
		return nil, err
	}
	if kv.Version, err = r.u64(); err != nil {
		return nil, err
	}
	if kv.TTLSeconds, err = r.u32(); err != nil {
		return nil, err
	}
	return &kv, nil
}

// Encode produces the postcard bytes of a result (the test domain and
// fixture round trips use it; the production adapter only decodes).
func (res Result) Encode() ([]byte, error) {
	w := &writer{}
	w.varint(res.Revision)
	w.varint(uint64(res.Kind))
	switch res.Kind {
	case OutcomeRange:
		w.varint(uint64(len(res.Items)))
		for _, item := range res.Items {
			w.boundedBytes(item.Key)
			writeEntry(w, item.Entry)
		}
		w.varint(res.Count)
		w.boolean(res.More)
	case OutcomeKineUpdated:
		w.boolean(res.Updated)
		writeOptionKineKv(w, res.Current)
	case OutcomeKineDeleted:
		w.boolean(res.Deleted)
		writeOptionKineKv(w, res.Prev)
	case OutcomeCompacted, OutcomeErrCompacted, OutcomeErrFutureRevision, OutcomeErrLeaseExists,
		OutcomeKineCreated, OutcomeErrKeyExists, OutcomeErrSessionInvalid,
		OutcomeErrPermissionDenied:
	default:
		return nil, ErrUnexpectedOutcome
	}
	return w.buf, nil
}

func writeEntry(w *writer, e KvEntry) {
	w.boundedBytes(e.Value)
	w.varint(e.CreateRevision)
	w.varint(e.ModRevision)
	w.varint(e.Version)
	encodeOptionID(w, e.Lease)
	if e.LeaseGeneration == nil {
		w.byte(0)
	} else {
		w.byte(1)
		w.varint(*e.LeaseGeneration)
	}
}

func writeOptionKineKv(w *writer, kv *KineKv) {
	if kv == nil {
		w.byte(0)
		return
	}
	w.byte(1)
	w.boundedBytes(kv.Key)
	w.boundedBytes(kv.Value)
	w.varint(kv.CreateRevision)
	w.varint(kv.ModRevision)
	w.varint(kv.Version)
	w.varint(uint64(kv.TTLSeconds))
}
