package wire

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

const responseFixture = "../../../crates/coord-state/fixtures/kine_responses_v1.json"

type jsonEntry struct {
	Value           []byte  `json:"value"`
	CreateRevision  uint64  `json:"create_revision"`
	ModRevision     uint64  `json:"mod_revision"`
	Version         uint64  `json:"version"`
	Lease           []byte  `json:"lease"`
	LeaseGeneration *uint64 `json:"lease_generation"`
}

type jsonKineKv struct {
	Key            []byte `json:"key"`
	Value          []byte `json:"value"`
	CreateRevision uint64 `json:"create_revision"`
	ModRevision    uint64 `json:"mod_revision"`
	Version        uint64 `json:"version"`
	TTLSeconds     uint32 `json:"ttl_seconds"`
}

type responseVector struct {
	Name     string `json:"name"`
	Response struct {
		Revision uint64          `json:"revision"`
		Outcome  json.RawMessage `json:"outcome"`
	} `json:"response"`
	PayloadHex string `json:"payload_hex"`
}

func (e jsonEntry) entry(t *testing.T) KvEntry {
	out := KvEntry{Value: e.Value, CreateRevision: e.CreateRevision, ModRevision: e.ModRevision, Version: e.Version, LeaseGeneration: e.LeaseGeneration}
	if e.Lease != nil {
		id := id16(t, e.Lease)
		out.Lease = &id
	}
	return out
}

func (k *jsonKineKv) kv(*testing.T) *KineKv {
	if k == nil {
		return nil
	}
	return &KineKv{
		Key:            k.Key,
		Value:          k.Value,
		CreateRevision: k.CreateRevision,
		ModRevision:    k.ModRevision,
		Version:        k.Version,
		TTLSeconds:     k.TTLSeconds,
	}
}

// rejectionReasons maps the Rust variant name to this package's
// discriminant. It is spelled out rather than derived so that a reason
// renamed or reordered upstream fails the fixture comparison instead of
// decoding to a neighbouring reason and a different gRPC status.
var rejectionReasons = map[string]RejectionReason{
	"Invalid":           RejectedInvalid,
	"NamespaceMismatch": RejectedNamespaceMismatch,
	"ResponseTooLarge":  RejectedResponseTooLarge,
	"TooManyEvents":     RejectedTooManyEvents,
	"TooManyDeletes":    RejectedTooManyDeletes,
	"CounterOverflow":   RejectedCounterOverflow,
	"Unsupported":       RejectedUnsupported,
	"ViewTooLarge":      RejectedViewTooLarge,
	"RetryConflict":     RejectedRetryConflict,
	"RetryTooOld":       RejectedRetryTooOld,
	"RetryOutOfWindow":  RejectedRetryOutOfWindow,
	"SessionInvalid":    RejectedSessionInvalid,
	"RetryUnauthorized": RejectedRetryUnauthorized,
	"AdmissionMismatch": RejectedAdmissionMismatch,
}

var unitOutcomes = map[string]OutcomeKind{
	"Compacted":           OutcomeCompacted,
	"KineCreated":         OutcomeKineCreated,
	"ErrKeyExists":        OutcomeErrKeyExists,
	"ErrCompacted":        OutcomeErrCompacted,
	"ErrFutureRevision":   OutcomeErrFutureRevision,
	"ErrPermissionDenied": OutcomeErrPermissionDenied,
	"ErrSessionInvalid":   OutcomeErrSessionInvalid,
	"ErrLeaseExists":      OutcomeErrLeaseExists,
}

// expected builds the Go result the vector's serde JSON describes.
func expected(t *testing.T, v responseVector) Result {
	t.Helper()
	res := Result{Revision: v.Response.Revision}
	var unit string
	if err := json.Unmarshal(v.Response.Outcome, &unit); err == nil {
		kind, ok := unitOutcomes[unit]
		if !ok {
			t.Fatalf("%s: unit outcome %q not in the Kine subset", v.Name, unit)
		}
		res.Kind = kind
		return res
	}
	var variants map[string]json.RawMessage
	if err := json.Unmarshal(v.Response.Outcome, &variants); err != nil {
		t.Fatal(err)
	}
	if raw, ok := variants["Range"]; ok {
		var r struct {
			Items []struct {
				Key   []byte    `json:"key"`
				Entry jsonEntry `json:"entry"`
			} `json:"items"`
			Count uint64 `json:"count"`
			More  bool   `json:"more"`
		}
		if err := json.Unmarshal(raw, &r); err != nil {
			t.Fatal(err)
		}
		res.Kind = OutcomeRange
		res.Items = make([]RangeItem, 0, len(r.Items))
		for _, it := range r.Items {
			res.Items = append(res.Items, RangeItem{Key: it.Key, Entry: it.Entry.entry(t)})
		}
		res.Count, res.More = r.Count, r.More
		return res
	}
	if raw, ok := variants["KineUpdated"]; ok {
		var u struct {
			Updated bool        `json:"updated"`
			Current *jsonKineKv `json:"current"`
		}
		if err := json.Unmarshal(raw, &u); err != nil {
			t.Fatal(err)
		}
		res.Kind, res.Updated, res.Current = OutcomeKineUpdated, u.Updated, u.Current.kv(t)
		return res
	}
	if raw, ok := variants["KineDeleted"]; ok {
		var d struct {
			Deleted bool        `json:"deleted"`
			Prev    *jsonKineKv `json:"prev"`
		}
		if err := json.Unmarshal(raw, &d); err != nil {
			t.Fatal(err)
		}
		res.Kind, res.Deleted, res.Prev = OutcomeKineDeleted, d.Deleted, d.Prev.kv(t)
		return res
	}
	if raw, ok := variants["ErrRejected"]; ok {
		var e struct {
			Reason string `json:"reason"`
		}
		if err := json.Unmarshal(raw, &e); err != nil {
			t.Fatal(err)
		}
		reason, ok := rejectionReasons[e.Reason]
		if !ok {
			t.Fatalf("%s: rejection reason %q is not named here", v.Name, e.Reason)
		}
		res.Kind, res.Reason = OutcomeErrRejected, reason
		return res
	}
	t.Fatalf("%s: outcome not in the Kine subset", v.Name)
	return res
}

func sameResult(a, b Result) bool {
	x, err1 := a.Encode()
	y, err2 := b.Encode()
	return err1 == nil && err2 == nil && bytes.Equal(x, y)
}

// Every Rust response vector decodes to the described result and
// re-encodes to identical bytes.
func TestResultDecodingMatchesRustVectors(t *testing.T) {
	raw, err := os.ReadFile(filepath.Clean(responseFixture))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var f struct {
		Schema  string           `json:"schema"`
		Vectors []responseVector `json:"vectors"`
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	if f.Schema != "kine_responses_v1" || len(f.Vectors) < 10 {
		t.Fatalf("unexpected fixture %q with %d vectors", f.Schema, len(f.Vectors))
	}
	for _, v := range f.Vectors {
		payload, _ := hex.DecodeString(v.PayloadHex)
		got, err := DecodeResult(payload)
		if err != nil {
			t.Fatalf("%s: decode: %v", v.Name, err)
		}
		want := expected(t, v)
		if !sameResult(got, want) {
			t.Fatalf("%s: decoded %+v, want %+v", v.Name, got, want)
		}
		again, err := got.Encode()
		if err != nil || !bytes.Equal(again, payload) {
			t.Fatalf("%s: re-encode %x (%v), want %x", v.Name, again, err, payload)
		}
		if _, err := DecodeResult(append(payload, 0)); !errors.Is(err, ErrTrailingPayloadBytes) {
			t.Fatalf("%s: trailing byte accepted: %v", v.Name, err)
		}
	}
}

// An outcome outside the Kine subset (a lease grant, a plain put) is
// refused, never mapped to a Kine result.
func TestResultRefusesOutcomesOutsideTheSubset(t *testing.T) {
	for _, disc := range []byte{0, 1, 3, 7, 8, 34, 100} {
		if _, err := DecodeResult([]byte{5, disc}); !errors.Is(err, ErrUnexpectedOutcome) {
			t.Fatalf("discriminant %d: %v", disc, err)
		}
	}
	if _, err := (Result{Kind: 99}).Encode(); !errors.Is(err, ErrUnexpectedOutcome) {
		t.Fatalf("encode of unknown kind: %v", err)
	}
}

// A rejection decodes to its reason, and every reason the Rust enum has
// is covered by a vector. The planner's refusal is an ordinary executed
// outcome; before this it decoded as "unexpected outcome", which reached
// an API server as an internal error and told an operator nothing.
func TestRejectionReasonsAreCovered(t *testing.T) {
	raw, err := os.ReadFile(filepath.Clean(responseFixture))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var f struct {
		Vectors []responseVector `json:"vectors"`
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	seen := map[RejectionReason]bool{}
	for _, v := range f.Vectors {
		var variants map[string]json.RawMessage
		if json.Unmarshal(v.Response.Outcome, &variants) != nil {
			continue
		}
		if _, ok := variants["ErrRejected"]; !ok {
			continue
		}
		payload, _ := hex.DecodeString(v.PayloadHex)
		got, err := DecodeResult(payload)
		if err != nil {
			t.Fatalf("%s: decode: %v", v.Name, err)
		}
		if got.Kind != OutcomeErrRejected {
			t.Fatalf("%s: decoded kind %d", v.Name, got.Kind)
		}
		want := expected(t, v)
		if got.Reason != want.Reason {
			t.Fatalf("%s: reason %d, want %d", v.Name, got.Reason, want.Reason)
		}
		if got.Reason.String() == "unknown reason" {
			t.Fatalf("%s: reason %d is not named", v.Name, got.Reason)
		}
		seen[got.Reason] = true
	}
	for r := RejectionReason(0); r < rejectedBeyond; r++ {
		if !seen[r] {
			t.Fatalf("no vector covers reason %d (%s)", r, r)
		}
	}
	if len(seen) != int(rejectedBeyond) {
		t.Fatalf("covered %d reasons, this package names %d", len(seen), rejectedBeyond)
	}
}

// A reason beyond the table is refused rather than kept as an integer:
// decoding it would hand the backend a reason it cannot map.
func TestRejectionReasonBeyondTheTableIsRefused(t *testing.T) {
	// revision 20, discriminant 35, reason one past the last.
	payload := []byte{20, 35, byte(rejectedBeyond)}
	if _, err := DecodeResult(payload); !errors.Is(err, ErrUnexpectedOutcome) {
		t.Fatalf("out-of-range reason accepted: %v", err)
	}
	if _, err := (Result{Kind: OutcomeErrRejected, Reason: rejectedBeyond}).Encode(); !errors.Is(err, ErrUnexpectedOutcome) {
		t.Fatalf("out-of-range reason encoded: %v", err)
	}
}

func TestBindFramesRoundTrip(t *testing.T) {
	frame, err := EncodeBind(Bind{Token: []byte("tok")})
	if err != nil {
		t.Fatal(err)
	}
	parsed, rest, err := NextFrame(frame)
	if err != nil || len(rest) != 0 {
		t.Fatal(err)
	}
	b, err := DecodeBind(parsed)
	if err != nil || string(b.Token) != "tok" {
		t.Fatalf("%v %q", err, b.Token)
	}
	if _, err := DecodeBindAck(parsed); !errors.Is(err, ErrUnexpectedFrame) {
		t.Fatalf("bind accepted as ack: %v", err)
	}
	ack := BindAck{Session: [idBytes]byte{9}, ExpiresAt: 1_700_000_000, Scope: 3, RuleGeneration: 2}
	frame, err = EncodeBindAck(ack)
	if err != nil {
		t.Fatal(err)
	}
	parsed, _, _ = NextFrame(frame)
	got, err := DecodeBindAck(parsed)
	if err != nil || got != ack {
		t.Fatalf("%v %+v", err, got)
	}
	if _, err := EncodeBind(Bind{Token: make([]byte, MaxTokenBytes+1)}); err == nil {
		t.Fatal("oversized token framed")
	}
	// The typed registry does not know the binding kinds: they are raw.
	if _, err := Decode(parsed); !errors.Is(err, ErrUnsupportedKind) {
		t.Fatalf("typed decode of a binding frame: %v", err)
	}
}
