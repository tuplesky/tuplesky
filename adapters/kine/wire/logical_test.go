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

const commandIDFixture = "../../../crates/coord-types/fixtures/command_ids_v1.json"

// The command-id fixture's request shape (serde JSON of `LogicalRequest`).
type commandIDVector struct {
	Name            string `json:"name"`
	ClusterID       string `json:"cluster_id"`
	DomainID        string `json:"domain_id"`
	SessionID       string `json:"session_id"`
	ClientInstance  string `json:"client_instance_id"`
	RequestSequence uint64 `json:"request_sequence"`
	Request         struct {
		SchemaVersion uint16                     `json:"schema_version"`
		Namespace     []byte                     `json:"namespace"`
		Operation     map[string]json.RawMessage `json:"operation"`
	} `json:"request"`
	PayloadHex string `json:"payload_hex"`
	CommandID  string `json:"command_id"`
}

type commandIDFile struct {
	Schema      string            `json:"schema"`
	HashContext string            `json:"hash_context"`
	Vectors     []commandIDVector `json:"vectors"`
}

// LoadCommandIDVectors reads the Rust-owned command-id fixture (shared with
// the client package's identity test).
func loadCommandIDVectors(t *testing.T) commandIDFile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(commandIDFixture))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var f commandIDFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	if f.Schema != "command_ids_v1" {
		t.Fatalf("unexpected fixture schema %q", f.Schema)
	}
	return f
}

func id16(t *testing.T, b []byte) [idBytes]byte {
	t.Helper()
	if len(b) != idBytes {
		t.Fatalf("id of %d bytes", len(b))
	}
	var out [idBytes]byte
	copy(out[:], b)
	return out
}

// opFromVector builds the Go operation of a fixture vector when it is in
// the Kine subset.
func opFromVector(t *testing.T, ops map[string]json.RawMessage) (LogicalOp, bool) {
	t.Helper()
	if raw, ok := ops["Range"]; ok {
		var v struct {
			Range struct {
				Key      []byte `json:"key"`
				RangeEnd []byte `json:"range_end"`
			} `json:"range"`
			Revision  *uint64 `json:"revision"`
			Limit     uint32  `json:"limit"`
			KeysOnly  bool    `json:"keys_only"`
			CountOnly bool    `json:"count_only"`
		}
		if err := json.Unmarshal(raw, &v); err != nil {
			t.Fatal(err)
		}
		return RangeOp{
			Range:     KeyRange{Key: v.Range.Key, RangeEnd: v.Range.RangeEnd},
			Revision:  v.Revision,
			Limit:     v.Limit,
			KeysOnly:  v.KeysOnly,
			CountOnly: v.CountOnly,
		}, true
	}
	if raw, ok := ops["KineCreate"]; ok {
		var v struct {
			Key        []byte `json:"key"`
			Value      []byte `json:"value"`
			TTLSeconds uint32 `json:"ttl_seconds"`
			Binding    []byte `json:"binding"`
		}
		if err := json.Unmarshal(raw, &v); err != nil {
			t.Fatal(err)
		}
		op := KineCreateOp{Key: v.Key, Value: v.Value, TTLSeconds: v.TTLSeconds}
		if v.Binding != nil {
			b := id16(t, v.Binding)
			op.Binding = &b
		}
		return op, true
	}
	if raw, ok := ops["KineUpdate"]; ok {
		var v struct {
			Key                 []byte `json:"key"`
			Value               []byte `json:"value"`
			ExpectedModRevision uint64 `json:"expected_mod_revision"`
			TTLSeconds          uint32 `json:"ttl_seconds"`
			Binding             []byte `json:"binding"`
		}
		if err := json.Unmarshal(raw, &v); err != nil {
			t.Fatal(err)
		}
		op := KineUpdateOp{Key: v.Key, Value: v.Value, ExpectedModRevision: v.ExpectedModRevision, TTLSeconds: v.TTLSeconds}
		if v.Binding != nil {
			b := id16(t, v.Binding)
			op.Binding = &b
		}
		return op, true
	}
	if raw, ok := ops["KineDelete"]; ok {
		var v struct {
			Key                 []byte  `json:"key"`
			ExpectedModRevision *uint64 `json:"expected_mod_revision"`
		}
		if err := json.Unmarshal(raw, &v); err != nil {
			t.Fatal(err)
		}
		return KineDeleteOp{Key: v.Key, ExpectedModRevision: v.ExpectedModRevision}, true
	}
	return nil, false
}

// Every Kine-subset vector of the Rust command-id fixture encodes to the
// exact Rust payload bytes and decodes back to the same operation.
func TestLogicalEncodingMatchesRustVectors(t *testing.T) {
	f := loadCommandIDVectors(t)
	seen := 0
	for _, v := range f.Vectors {
		op, ok := opFromVector(t, v.Request.Operation)
		if !ok {
			continue
		}
		seen++
		req := LogicalRequest{Namespace: id16(t, v.Request.Namespace), Op: op}
		got, err := req.Encode()
		if err != nil {
			t.Fatalf("%s: encode: %v", v.Name, err)
		}
		want, _ := hex.DecodeString(v.PayloadHex)
		if !bytes.Equal(got, want) {
			t.Fatalf("%s: payload %x, want %x", v.Name, got, want)
		}
		decoded, err := DecodeLogical(want)
		if err != nil {
			t.Fatalf("%s: decode: %v", v.Name, err)
		}
		again, err := decoded.Encode()
		if err != nil || !bytes.Equal(again, want) {
			t.Fatalf("%s: re-encode %x (%v), want %x", v.Name, again, err, want)
		}
	}
	if seen < 4 {
		t.Fatalf("only %d Kine-subset vectors found", seen)
	}
}

// Operations outside the Kine subset, trailing bytes and rule violations
// are refused rather than reinterpreted.
func TestLogicalRejectsOutsideSubsetAndRuleViolations(t *testing.T) {
	f := loadCommandIDVectors(t)
	for _, v := range f.Vectors {
		if _, ok := opFromVector(t, v.Request.Operation); ok {
			continue
		}
		payload, _ := hex.DecodeString(v.PayloadHex)
		if _, err := DecodeLogical(payload); !errors.Is(err, ErrInvalidLogical) {
			t.Fatalf("%s: decoded outside the subset: %v", v.Name, err)
		}
	}
	ns := [idBytes]byte{1}
	valid := LogicalRequest{Namespace: ns, Op: KineDeleteOp{Key: []byte("/k")}}
	payload, err := valid.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeLogical(append(payload, 0)); !errors.Is(err, ErrTrailingPayloadBytes) {
		t.Fatalf("trailing byte accepted: %v", err)
	}
	seven := uint64(7)
	zero := uint64(0)
	b := [idBytes]byte{2}
	bad := []LogicalOp{
		RangeOp{Range: KeyRange{Key: nil}},
		RangeOp{Range: KeyRange{Key: []byte("b"), RangeEnd: []byte("a")}},
		RangeOp{Range: KeyRange{Key: []byte("a")}, Limit: MaxPageLimit + 1},
		KineCreateOp{Key: []byte("k"), TTLSeconds: 5},
		KineCreateOp{Key: []byte("k"), Binding: &b},
		KineCreateOp{Key: []byte("k"), TTLSeconds: MaxLeaseTTLSeconds + 1, Binding: &b},
		KineCreateOp{Key: []byte("k"), Value: make([]byte, MaxValueBytes+1)},
		KineUpdateOp{Key: []byte("k"), ExpectedModRevision: 0},
		KineDeleteOp{Key: []byte("k"), ExpectedModRevision: &zero},
		KineDeleteOp{Key: make([]byte, MaxKeyBytes+1), ExpectedModRevision: &seven},
	}
	for i, op := range bad {
		if _, err := (LogicalRequest{Namespace: ns, Op: op}).Encode(); !errors.Is(err, ErrInvalidLogical) {
			t.Fatalf("bad op %d accepted: %v", i, err)
		}
	}
}

func TestRetryKeyCanonicalBytes(t *testing.T) {
	k := RetryKey{RequestSequence: 0x0102030405060708}
	k.ClusterID[0] = 0xaa
	out := k.CanonicalBytes()
	if out[0] != 0xaa || !bytes.Equal(out[64:], []byte{1, 2, 3, 4, 5, 6, 7, 8}) {
		t.Fatalf("canonical bytes %x", out)
	}
}
