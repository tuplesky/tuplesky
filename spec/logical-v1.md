# logical_v1: canonical commands, identities and ordered keys (task-02)

Normative companion to `crates/coord-types`. Design references: Sections 2.3,
4.4, 10.5.1, 17.2, 18.3 and 19.3. Frozen vectors live in
`crates/coord-types/fixtures/` and are checked by `crates/coord-types/tests`.

## Identifiers and counters

| Type | Encoding | Rule |
|---|---|---|
| `ClusterId`, `DomainId`, `NamespaceId`, `SessionId`, `ClientInstanceId`, `ReplicaId`, `LeaseId`, `ReadFenceId` | 16 opaque bytes | Allocated at a trusted boundary or derived; exact length on decode |
| `KvRevision` | u64 big-endian, max `i64::MAX` | Advances once per KV-mutating command; overflow stops |
| `ExecutionPosition`, `ConfigurationEpoch`, `EndpointGeneration`, `CatalogGeneration`, `LocalJournalSeq`, `FinalizedFrameSeq`, `RequestSequence`, `LeaseGeneration`, `ReplicaIncarnation` | u64 big-endian | Distinct types, no conversions, never wrap or recycle |
| `Ballot` | `(epoch, number, leader)` | Comparable only within one epoch |

## Stable invocation identity

```text
retry_key  = cluster_id || domain_id || session_id || client_instance_id || request_sequence_be64   (72 bytes)
payload    = postcard(LogicalRequest { schema_version: 1, namespace, operation })
command_id = BLAKE3.derive_key("tuplesky coord.v1 2026-09 command-id")(len(retry_key) || retry_key || len(payload) || payload)
```

Lengths are 8-byte big-endian prefixes. Membership epoch, ballot, endpoint
generation, credential handle, connection and stream identifiers are
`AdmissionContext` and never enter the hash. A different payload under one
retry key is `RequestIdentityConflict`; the state machine accepts only the
first. Sequences at or below the retired floor are `SequenceTooOld`; a jump
beyond the outstanding window is `SequenceOutOfWindow`.

Every other digest uses its own derive-key context (`HashDomain`), so
command, result, journal batch, checkpoint root, finalized frame and grant
commitment digests cannot collide across purposes.

## Canonical operation schema

`LogicalRequest` and `CanonicalOperation` are serde/postcard types whose
field order and variant order are frozen. Allowed evolution: append variants
at the end of an enum. Anything else is `logical_v2`.

Variant order of `CanonicalOperation`: `Range`, `Put`, `DeleteRange`, `Txn`,
`LeaseGrant`, `LeaseKeepAlive`, `LeaseRevoke`, `LeaseTimeToLive`, `Compact`.

Normalization: transaction comparisons form a conjunction, so they are
sorted and de-duplicated before encoding; a non-canonical transaction is
rejected by `validate`. Branch operations keep their order because results
depend on it.

Limits (rejected before encoding): key 1..=8 KiB, value <= 1 MiB, request
<= 2 MiB of key/value bytes, transaction work (comparisons plus branch
operations) <= 128, one write per key per branch, no nested transactions,
half-open ranges with `range_end > key`, lease TTL 1..=604800 seconds,
positive compaction revision, page limit <= 10000.

## Ordered keys

```text
current  = namespace(16) || escape(key) || 00 00
history  = namespace(16) || escape(key) || 00 00 || revision_be64
escape   = copy bytes; 00 -> 00 ff
```

Unsigned lexicographic order of the encoding equals the order of
`(namespace, key, revision)`. Decoding rejects `00 xx` with `xx` not in
`{00, ff}`, truncation, trailing bytes and revisions above `i64::MAX`, so
each row has exactly one encoding. `history_bounds(ns, key)` returns the
half-open interval containing exactly the history rows of `key`.

## Fixtures

* `ordered_keys_v1.json`: `(namespace, key, revision?) -> hex encoding`,
  plus the sorted order of the vectors.
* `command_ids_v1.json`: `(retry key components, request JSON) -> payload hex
  and command id hex`, including a pair that differs only in admission
  context (same id) and a pair that differs in payload (different id).

Regenerate with `COORD_TYPES_WRITE_FIXTURES=1 cargo test -p coord-types`
only in a reviewed schema change.
