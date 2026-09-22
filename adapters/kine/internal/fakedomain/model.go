// Package fakedomain is the in-process test domain of the bridge tests
// (task-46; design Section 19.5): a quic-go frontend that speaks the
// native API frames and applies the Kine subset of `logical_v1` to an
// in-memory model with the semantics of the Rust planner (task-17). It is
// an early proxy composition for tests, not a voter, a second state
// machine or a production artifact; `cmd/kine-coord` never links it.
package fakedomain

import (
	"sort"
	"sync"

	"github.com/tuplesky/tuplesky/adapters/kine/wire"
)

// Entry is a current or historical entry of the model.
type Entry struct {
	Value          []byte
	CreateRevision uint64
	ModRevision    uint64
	Version        uint64
	Lease          *[16]byte
	TTLSeconds     uint32
}

type version struct {
	revision uint64
	entry    *Entry // nil: tombstone
}

// Model is the domain's KV state: per-key version history, the current
// revision, the spent binding identities and the compaction floor.
type Model struct {
	mu           sync.Mutex
	revision     uint64
	keys         map[string][]version
	usedBindings map[[16]byte]bool
	compactFloor uint64
}

// NewModel starts at revision 0.
func NewModel() *Model {
	return &Model{keys: map[string][]version{}, usedBindings: map[[16]byte]bool{}}
}

// Revision is the current revision.
func (m *Model) Revision() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.revision
}

// SetCompactFloor sets the MVCC retention floor (reads below it are
// compacted).
func (m *Model) SetCompactFloor(rev uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.compactFloor = rev
}

// Current returns the live entries.
func (m *Model) Current() map[string]Entry {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]Entry{}
	for k, hist := range m.keys {
		if e := hist[len(hist)-1].entry; e != nil {
			out[k] = *e
		}
	}
	return out
}

func (m *Model) latest(key string) *Entry {
	hist := m.keys[key]
	if len(hist) == 0 {
		return nil
	}
	return hist[len(hist)-1].entry
}

func (m *Model) at(key string, rev uint64) *Entry {
	var found *Entry
	for _, v := range m.keys[key] {
		if v.revision <= rev {
			found = v.entry
		} else {
			break
		}
	}
	return found
}

func (m *Model) append(key string, e *Entry) {
	m.keys[key] = append(m.keys[key], version{revision: m.revision, entry: e})
}

func toWire(e *Entry) wire.KvEntry {
	out := wire.KvEntry{
		Value:          append([]byte(nil), e.Value...),
		CreateRevision: e.CreateRevision,
		ModRevision:    e.ModRevision,
		Version:        e.Version,
	}
	if e.Lease != nil {
		id := *e.Lease
		gen := uint64(1)
		out.Lease, out.LeaseGeneration = &id, &gen
	}
	return out
}

func kineKv(key string, e *Entry) *wire.KineKv {
	if e == nil {
		return nil
	}
	// A Kine-facing entry carries no lease identity: the private binding
	// that backs the TTL never reaches a Kine caller.
	return &wire.KineKv{
		Key:            []byte(key),
		Value:          e.Value,
		CreateRevision: e.CreateRevision,
		ModRevision:    e.ModRevision,
		Version:        e.Version,
		TTLSeconds:     e.TTLSeconds,
	}
}

// Apply executes one logical request at one execution point and returns
// the result with the header revision the planner would report. The
// second value says whether KV changed.
func (m *Model) Apply(req wire.LogicalRequest) (wire.Result, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch op := req.Op.(type) {
	case wire.RangeOp:
		return m.read(op), false
	case wire.KineCreateOp:
		key := string(op.Key)
		if m.latest(key) != nil {
			return wire.Result{Revision: m.revision, Kind: wire.OutcomeErrKeyExists}, false
		}
		if op.Binding != nil && m.usedBindings[*op.Binding] {
			return wire.Result{Revision: m.revision, Kind: wire.OutcomeErrLeaseExists}, false
		}
		m.revision++
		e := &Entry{Value: op.Value, CreateRevision: m.revision, ModRevision: m.revision, Version: 1, TTLSeconds: op.TTLSeconds}
		if op.Binding != nil {
			b := *op.Binding
			e.Lease = &b
			m.usedBindings[b] = true
		}
		m.append(key, e)
		return wire.Result{Revision: m.revision, Kind: wire.OutcomeKineCreated}, true
	case wire.KineUpdateOp:
		key := string(op.Key)
		cur := m.latest(key)
		if cur == nil {
			return wire.Result{Revision: m.revision, Kind: wire.OutcomeKineUpdated}, false
		}
		if cur.ModRevision != op.ExpectedModRevision {
			return wire.Result{Revision: m.revision, Kind: wire.OutcomeKineUpdated, Current: kineKv(key, cur)}, false
		}
		if op.Binding != nil && m.usedBindings[*op.Binding] {
			return wire.Result{Revision: m.revision, Kind: wire.OutcomeErrLeaseExists}, false
		}
		m.revision++
		e := &Entry{Value: op.Value, CreateRevision: cur.CreateRevision, ModRevision: m.revision, Version: cur.Version + 1, TTLSeconds: op.TTLSeconds}
		if op.Binding != nil {
			b := *op.Binding
			e.Lease = &b
			m.usedBindings[b] = true
		}
		m.append(key, e)
		return wire.Result{Revision: m.revision, Kind: wire.OutcomeKineUpdated, Updated: true, Current: kineKv(key, e)}, true
	case wire.KineDeleteOp:
		key := string(op.Key)
		cur := m.latest(key)
		if cur == nil {
			// Absent keys report gone, as the reference bridge does.
			return wire.Result{Revision: m.revision, Kind: wire.OutcomeKineDeleted, Deleted: true}, false
		}
		if op.ExpectedModRevision != nil && cur.ModRevision != *op.ExpectedModRevision {
			return wire.Result{Revision: m.revision, Kind: wire.OutcomeKineDeleted, Prev: kineKv(key, cur)}, false
		}
		m.revision++
		m.append(key, nil)
		return wire.Result{Revision: m.revision, Kind: wire.OutcomeKineDeleted, Deleted: true, Prev: kineKv(key, cur)}, true
	default:
		return wire.Result{Revision: m.revision, Kind: wire.OutcomeErrPermissionDenied}, false
	}
}

func (m *Model) read(op wire.RangeOp) wire.Result {
	header := m.revision
	at := uint64(0)
	if op.Revision != nil {
		at = *op.Revision
		if at > m.revision {
			return wire.Result{Revision: header, Kind: wire.OutcomeErrFutureRevision}
		}
		if at < m.compactFloor {
			return wire.Result{Revision: header, Kind: wire.OutcomeErrCompacted}
		}
	}
	keys := make([]string, 0, len(m.keys))
	for k := range m.keys {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	start := string(op.Range.Key)
	var items []wire.RangeItem
	for _, k := range keys {
		if op.Range.RangeEnd == nil {
			if k != start {
				continue
			}
		} else if k < start || k >= string(op.Range.RangeEnd) {
			continue
		}
		var e *Entry
		if op.Revision != nil {
			e = m.at(k, at)
		} else {
			e = m.latest(k)
		}
		if e == nil {
			continue
		}
		items = append(items, wire.RangeItem{Key: []byte(k), Entry: toWire(e)})
	}
	res := wire.Result{Revision: header, Kind: wire.OutcomeRange, Count: uint64(len(items))}
	if op.CountOnly {
		return res
	}
	limit := len(items)
	if op.Limit != 0 && int(op.Limit) < limit {
		limit = int(op.Limit)
		res.More = true
	}
	items = items[:limit]
	if op.KeysOnly {
		for i := range items {
			items[i].Entry.Value = nil
		}
	}
	res.Items = items
	return res
}
