package client

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/tuplesky/tuplesky/adapters/kine/wire"
)

const (
	commandIDFixture   = "../../../crates/coord-types/fixtures/command_ids_v1.json"
	kineBindingFixture = "../../../crates/coord-types/fixtures/kine_bindings_v1.json"
)

type identityVector struct {
	Name            string `json:"name"`
	ClusterID       string `json:"cluster_id"`
	DomainID        string `json:"domain_id"`
	SessionID       string `json:"session_id"`
	ClientInstance  string `json:"client_instance_id"`
	RequestSequence uint64 `json:"request_sequence"`
	PayloadHex      string `json:"payload_hex"`
	CommandID       string `json:"command_id"`
	BindingID       string `json:"binding_id"`
}

type identityFile struct {
	Schema      string           `json:"schema"`
	HashContext string           `json:"hash_context"`
	Vectors     []identityVector `json:"vectors"`
}

func loadIdentityFixture(t *testing.T, path, schema string) identityFile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var f identityFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	if f.Schema != schema {
		t.Fatalf("unexpected fixture schema %q", f.Schema)
	}
	return f
}

func hexID(t *testing.T, s string) [16]byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 16 {
		t.Fatalf("id %q", s)
	}
	var out [16]byte
	copy(out[:], b)
	return out
}

func (v identityVector) key(t *testing.T) wire.RetryKey {
	return wire.RetryKey{
		ClusterID:       hexID(t, v.ClusterID),
		DomainID:        hexID(t, v.DomainID),
		SessionID:       hexID(t, v.SessionID),
		ClientInstance:  hexID(t, v.ClientInstance),
		RequestSequence: v.RequestSequence,
	}
}

// Every command id of the Rust fixture is reproduced from the retry key
// and the canonical payload bytes.
func TestCommandIDMatchesRustVectors(t *testing.T) {
	f := loadIdentityFixture(t, commandIDFixture, "command_ids_v1")
	if f.HashContext != CommandIDContext {
		t.Fatalf("hash context %q", f.HashContext)
	}
	for _, v := range f.Vectors {
		payload, _ := hex.DecodeString(v.PayloadHex)
		got := CommandID(v.key(t), payload)
		if hex.EncodeToString(got[:]) != v.CommandID {
			t.Fatalf("%s: command id %x, want %s", v.Name, got, v.CommandID)
		}
	}
}

// Every binding id of the Rust fixture is reproduced from the retry key.
func TestKineBindingMatchesRustVectors(t *testing.T) {
	f := loadIdentityFixture(t, kineBindingFixture, "kine_bindings_v1")
	if f.HashContext != KineBindingContext {
		t.Fatalf("hash context %q", f.HashContext)
	}
	if len(f.Vectors) < 3 {
		t.Fatal("too few vectors")
	}
	for _, v := range f.Vectors {
		got := KineBinding(v.key(t))
		if hex.EncodeToString(got[:]) != v.BindingID {
			t.Fatalf("%s: binding %x, want %s", v.Name, got, v.BindingID)
		}
	}
}

// Length prefixes keep part boundaries distinct.
func TestDomainDigestIsLengthPrefixed(t *testing.T) {
	a := domainDigest(CommandIDContext, []byte("ab"), []byte("c"))
	b := domainDigest(CommandIDContext, []byte("a"), []byte("bc"))
	if a == b {
		t.Fatal("part boundaries collapsed")
	}
}
