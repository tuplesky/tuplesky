package client

import (
	"encoding/binary"

	"github.com/tuplesky/tuplesky/adapters/kine/wire"
	"lukechampine.com/blake3"
)

// The frozen BLAKE3 derive-key contexts of `coord_types::HashDomain`.
const (
	// CommandIDContext derives command identity from the retry key and the
	// canonical logical bytes.
	CommandIDContext = "tuplesky coord.v1 2026-09 command-id"
	// KineBindingContext derives the hidden private TTL binding identity of
	// a Kine write from the retry key alone.
	KineBindingContext = "tuplesky coord.v1 2026-09 kine-binding"
)

// domainDigest hashes length-prefixed parts under a derive-key context,
// exactly as `HashDomain::digest` does: an eight-byte big-endian length
// precedes every part, so ["ab","c"] and ["a","bc"] stay distinct.
func domainDigest(context string, parts ...[]byte) [32]byte {
	material := make([]byte, 0, 128)
	var length [8]byte
	for _, part := range parts {
		binary.BigEndian.PutUint64(length[:], uint64(len(part)))
		material = append(material, length[:]...)
		material = append(material, part...)
	}
	var out [32]byte
	blake3.DeriveKey(out[:], context, material)
	return out
}

// CommandID derives the command identity of `payload` (canonical logical
// bytes) under `key`, matching `CommandId::derive`.
func CommandID(key wire.RetryKey, payload []byte) [32]byte {
	canonical := key.CanonicalBytes()
	return domainDigest(CommandIDContext, canonical[:], payload)
}

// KineBinding derives the hidden binding identity of a Kine write from the
// retry key, matching `coord_types::kine_binding_id`: a retry reproduces
// it and the next sequence names a fresh one.
func KineBinding(key wire.RetryKey) [16]byte {
	canonical := key.CanonicalBytes()
	digest := domainDigest(KineBindingContext, canonical[:])
	var out [16]byte
	copy(out[:], digest[:16])
	return out
}
