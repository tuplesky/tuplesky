package bench

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
)

// Kind is an operation kind, reported as its own distribution. Mixing a
// read and a replicated write into one number would hide the thing this
// measurement is for.
type Kind string

// The kinds. They are named as `coord-wan-bench` names them, because the
// two programs' reports are read side by side.
const (
	// KindPut is an unconditional-looking write: a create when this
	// caller has never written the key, and a guarded update on the
	// revision it last saw otherwise. That is what an API server does,
	// and there is no unguarded write in the profile.
	KindPut Kind = "put"
	// KindGet is a point read.
	KindGet Kind = "get"
	// KindCompareAndSwap is a guarded write onto a contended key.
	KindCompareAndSwap Kind = "compare-and-swap"
	// KindScan is a bounded range read.
	KindScan Kind = "scan"
)

// Kinds in report order.
var Kinds = []Kind{KindPut, KindGet, KindCompareAndSwap, KindScan}

// Mix is the relative weight of each kind. A zero weight removes it.
type Mix struct {
	Put            uint32
	Get            uint32
	CompareAndSwap uint32
	Scan           uint32
}

// ControlPlane is the mix a Kubernetes control plane produces: mostly
// reads, a steady stream of guarded writes, the occasional list.
//
// It is `coord-wan-bench`'s CONTROL_PLANE with the transaction weight
// folded into the guarded writes, because the Kine bridge accepts only
// the single-key compare the API server emits: a multi-key transaction
// is a native-only shape and is reported as one rather than simulated
// here.
var ControlPlane = Mix{Put: 15, Get: 55, CompareAndSwap: 25, Scan: 5}

// ParseMix reads `put=15,get=55,cas=25,scan=5`.
func ParseMix(text string) (Mix, error) {
	var mix Mix
	for _, part := range strings.Split(text, ",") {
		name, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return Mix{}, fmt.Errorf("`%s` is not name=weight", part)
		}
		weight, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return Mix{}, fmt.Errorf("`%s` is not a weight", value)
		}
		switch name {
		case "put":
			mix.Put = uint32(weight)
		case "get":
			mix.Get = uint32(weight)
		case "cas", "compare-and-swap":
			mix.CompareAndSwap = uint32(weight)
		case "scan":
			mix.Scan = uint32(weight)
		default:
			return Mix{}, fmt.Errorf("unknown operation `%s`", name)
		}
	}
	if mix.Total() == 0 {
		return Mix{}, errors.New("a mix with no weight offers no work")
	}
	return mix, nil
}

// Total is the sum of the weights.
func (m Mix) Total() uint32 {
	return m.Put + m.Get + m.CompareAndSwap + m.Scan
}

func (m Mix) pick(draw uint32) Kind {
	at := draw % m.Total()
	for _, pair := range []struct {
		kind   Kind
		weight uint32
	}{
		{KindPut, m.Put},
		{KindGet, m.Get},
		{KindCompareAndSwap, m.CompareAndSwap},
		{KindScan, m.Scan},
	} {
		if at < pair.weight {
			return pair.kind
		}
		at -= pair.weight
	}
	return KindGet
}

// Workload shapes the offered work. The parameters are
// `coord-wan-bench`'s, with the same defaults, so that the two programs
// offer the same work to the same domain.
type Workload struct {
	// Prefix every key sits under. A real registry prefix, because the
	// profile being measured is the one an API server drives.
	Prefix string
	// Keyspace is how many distinct keys the reads and writes touch.
	Keyspace uint32
	// HotKeys is how many keys the guarded writes contend on.
	HotKeys uint32
	// ValueBytes is the value size.
	ValueBytes int
	// ScanLimit is how many rows a scan asks for.
	ScanLimit int64
	// Mix is the relative weight of the kinds.
	Mix Mix
}

// Operation is one offered unit of work, in terms both arms understand.
// It names a key and a shape; how that shape reaches the domain is the
// arm's business.
type Operation struct {
	Kind Kind
	// Key the operation names. Empty for a scan, which names an
	// interval instead.
	Key string
	// From and To bound a scan.
	From, To string
	// Value a write carries.
	Value []byte
	// Limit a scan asks for.
	Limit int64
}

// Key is the object key at `index`.
func (w Workload) Key(index uint32) string {
	return fmt.Sprintf("%sobjects/%08d", w.Prefix, index)
}

// Hot is the contended key at `index`.
func (w Workload) Hot(index uint32) string {
	return fmt.Sprintf("%shot/%04d", w.Prefix, index)
}

// Next draws the next operation.
func (w Workload) Next(rng *rand.Rand) Operation {
	kind := w.Mix.pick(rng.Uint32())
	switch kind {
	case KindPut:
		return Operation{Kind: kind, Key: w.Key(rng.Uint32() % max32(w.Keyspace, 1)), Value: w.value(rng)}
	case KindGet:
		return Operation{Kind: kind, Key: w.Key(rng.Uint32() % max32(w.Keyspace, 1))}
	case KindCompareAndSwap:
		return Operation{Kind: kind, Key: w.Hot(rng.Uint32() % max32(w.HotKeys, 1)), Value: w.value(rng)}
	default:
		from := rng.Uint32() % max32(w.Keyspace, 1)
		start := w.Key(from)
		return Operation{Kind: KindScan, From: start, To: start + "\xff", Limit: w.ScanLimit}
	}
}

// value is incompressible enough not to be free and comes from the same
// stream, so one seed offers one sequence of bytes.
func (w Workload) value(rng *rand.Rand) []byte {
	out := make([]byte, w.ValueBytes)
	for i := range out {
		out[i] = byte(rng.Uint32())
	}
	return out
}

func max32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}
