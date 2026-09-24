# wire_v1: bounded frames and stable transport DTOs (task-03)

Normative companion to `crates/coord-types/src/wire_v1.rs`. Design
references: Sections 11.2, 19.1, 6.7.1 and 10.5. Frozen vectors:
`crates/coord-types/fixtures/wire_frames_v1.json`.

## Frame

```text
u32_be frame_length     excludes these 4 bytes, includes kind and version
u16_be message_kind
u16_be schema_version
postcard payload        frame_length - 4 bytes
```

Rules checked before any payload allocation:

* `frame_length < 4` is `LengthBelowMinimum`.
* `frame_length` above the class limit of the kind's range is
  `LengthAboveClassLimit`; unregistered kinds use the smallest limit.
* A stream ending inside a header or payload is `IncompleteFrame`; bytes
  after the last complete frame are an error at `finish`, never ignored.
* Socket reads are not messages: `FrameReader` assembles exact frames.

Class limits (`frame_length`):

| Range | Kinds | Limit |
|---|---|---|
| Negotiation | `0x00xx` | 64 KiB |
| API | `0x01xx` | 3 MiB |
| Watch | `0x02xx` | 8 MiB + 64 KiB |
| Protocol evidence | `0x03xx` (reserved, task-19+) | 4 MiB |
| Configuration | `0x04xx` (reserved, task-m01) | 256 KiB |
| Observer replication | `0x05xx` (reserved, task-o01) | 8 MiB + 64 KiB |
| Snapshot | `0x06xx` (reserved, task-49/task-50) | 1 MiB + 64 KiB |
| Collector evidence | `0x07xx` (reserved, task-33/task-m02) | 1 MiB |
| Read fence | `0x08xx` (reserved, task-o05) | 64 KiB |

## Registered kinds (version 1)

| Kind | Value | DTO |
|---|---|---|
| Hello | `0x0001` | `HelloV1 { role, cluster_id, domain_id, incarnation?, capabilities[<=64] }` |
| HelloAck | `0x0002` | `HelloAckV1 { capabilities[<=64], max_inflight }` |
| Close | `0x0003` | `CloseV1 { code, reason[<=256] }` |
| Request | `0x0100` | `RequestV1 { retry_key, logical[<=2 MiB+64 KiB], deadline_ms }` |
| Response | `0x0101` | `ResponseV1 { command_id, outcome }` |
| ResolveRequest | `0x0102` | `ResolveRequestV1 { retry_key, command_id }` |
| WatchOpen | `0x0200` | `WatchOpenV1 { watch_id, namespace, key, range_end?, start_revision?, prev_kv, progress_notify }` |
| WatchEvents | `0x0201` | `WatchEventsV1 { watch_id, revision, events[<=4096], complete }` |
| WatchProgress | `0x0202` | `WatchProgressV1 { watch_id, revision }` |
| WatchClose | `0x0203` | `WatchCloseV1 { watch_id, reason, last_complete_revision? }` |

Unknown kinds are `UnsupportedKind`; a registered kind with a version other
than 1 is `UnsupportedVersion`. Both are decided before the payload is
inspected. Payload decoding rejects varint overflow, bad discriminants and
truncated fields (`MalformedPayload`), bytes after the DTO
(`TrailingPayloadBytes`) and bounded collections whose declared length
exceeds the bound (checked from the length prefix before elements are read).

DTO field and variant order are frozen. Evolution appends enum variants or
registers a new kind/version; changing an existing DTO is a new
`schema_version` with a supported old-decoder window. Integers use explicit
widths, byte strings are opaque and bounded, and there are no floats, maps or
platform-sized values.

## Identity-bearing payloads

`RequestV1.logical` carries the `logical_v1` canonical postcard bytes. A
receiver decodes them, validates, re-encodes and requires byte equality
(`NonCanonicalPayload` otherwise), so the command identity computed from the
wire form equals the one computed from the logical form and an upgraded
transport cannot change retry identity.

## Separation from other formats

The durable journal record (`JournalRecordV1`, task-j01), store envelopes
(`StoreEnvelopeV1`, task-s01), shared/local checkpoints and finalized frames
are versioned independently of this transport schema. Only the kind ranges
above are reserved for them here.

## Fuzzing

`fuzz/fuzz_targets/wire_frames.rs` drives `wire_v1::fuzz_entry` under
libFuzzer (nightly). The regular test suite replays the same harness over
bit flips, truncations and length edits of every fixture frame so a decoder
regression is caught without the nightly toolchain.

## Transport negotiation (task-30)

Two ALPNs, neither a registered standard, separate the planes:

| ALPN | Class | Roles admitted in `Hello` |
|---|---|---|
| `coord-api/1` | native API | `Client`, `Frontend`, `KineCollector` |
| `coord-peer/1` | internal peer plane | `Voter`, `Observer`, `Learner` (incarnation required) |

The first frame on the first bidirectional stream (the control stream) is
`Hello`; the acceptor answers `HelloAck` or closes, and the dialer reads
that answer before it sends anything else. `Hello` must name the
acceptor's cluster and domain and a role of the connection's class. The
control stream then carries only `Close`. Every other stream carries
exactly one frame and ends: peer evidence on unidirectional streams, unary
requests and their responses on bidirectional streams. TLS 1.3 with the
explicit AWS-LC provider, no application early data, no server-side
migration.

Client authentication differs per plane, and the acceptor enforces the
difference after the ALPN and `Hello` are known:

* `coord-peer/1` is mutually authenticated. A peer-class connection
  without a client certificate is rejected, and the presented chain is
  bound to the declared role, cluster, domain and incarnation by the
  runtime's identity binder.
* `coord-api/1` authenticates the acceptor, and a client certificate is
  optional. `Frontend` and `KineCollector` act for other principals and
  are still rejected without one; `Client` speaks only for itself and its
  authority is the session binding below, so it may negotiate without a
  certificate and can do nothing until a `Bind` is acknowledged.

A certificate that is presented is always validated against the same
trust anchors on both planes: only its presence is optional, never its
validity.

| Kind | Value | Payload |
|---|---|---|
| PeerEvidence | `0x0300` | Opaque `coord-consensus` protocol message (postcard); protocol-evidence class limit |

Close codes (in `CloseV1.code` and the QUIC application close): `0`
orderly, `1` protocol violation (framing, unexpected frame), `2`
negotiation rejected (origin, role class, version, identity), `3`
deadline, `4` shutdown.

Lanes (task-31): a `Hello` declares exactly one lane through a frozen
capability identifier; the acceptor admits it only for the dialing role.

| Capability | Lane | Roles |
|---|---|---|
| `0x0010` | control (negotiation, evidence, recovery pages) | Voter, Observer, Learner, Frontend, KineCollector |
| `0x0011` | unary (requests and responses) | Frontend, KineCollector, Client |
| `0x0012` | watch (long-lived event streams) | Frontend, KineCollector, Client |
| `0x0013` | bulk (snapshots, replication) | Voter, Observer, Learner |

Each lane is a separate connection with its own stream limits, windows and
queues; a peer pair therefore holds at most one connection per admitted
lane and direction.

## Collector frames (task-33)

The trusted collector (`spec/collector-v1.md`) uses three raw kinds. Two
are in the API range because they carry or answer a client request and
take its class limit; the evidence frame is in the collector-evidence
range. None is decodable by the typed decoder: they are dispatched by raw
kind at the collector boundary, like peer evidence.

| Kind | Value | Direction | Payload |
|---|---|---|---|
| Submit | `0x0103` | collector to every voter | `SubmitV1 { receipt: AdmissionReceipt, request: RequestV1 }` (postcard); API class limit |
| Release | `0x0701` | leader to collector | `ReleasedResult` (postcard); collector-evidence class limit |
| Evidence | `0x0700` | voter to collector | Opaque `coord-consensus` protocol message (`LeaderReply`, `FastAck`, `SlowAck`); collector-evidence class limit |

A voter admits `Submit` only from a connection bound to a collector role
(`Frontend`, `KineCollector`); `Evidence` and `Release` are only ever
sent by voters, and nothing received on an API-class connection is a
vote. Frozen error codes of `ResponseV1::Err` are `wire_v1::codes` in
`coord-types` (`0x0001` request identity conflict, `0x0002`
backpressure, `0x0003` malformed request, `0x0004` not admitted, `0x0005`
result too large; append-only); pending and unknown outcomes use
the `Pending` and `Unknown` outcomes, not error codes.

## API session binding (task-37)

An API-class connection carries no session until its first frame after
`Hello` binds one. Two raw kinds in the API range:

| Kind | Value | Direction | Payload |
|---|---|---|---|
| Bind | `0x0105` | client to frontend | `BindV1 { token }`: the STS service token (at most 8 KiB) |
| BindAck | `0x0106` | frontend to client | `BindAckV1 { session, expires_at, scope, rule_generation }` |

The frontend verifies the token locally against the STS's published keys
under its clock health; a rejected binding closes the connection with
code `2`. A later `Bind` on the same connection (rebind) must carry the
same session: it refreshes validity, never identity or ceiling. Nothing
is admitted before a binding, or after its validity ends; watches and
results of already admitted work are released only through the fresh
authorization barrier of `coord-session`.

## Configuration frames (task-m01)

Configuration discovery uses six raw kinds of the configuration range
(class limit 256 KiB). Their DTOs are `coord_types::config_v1`; like the
collector and binding frames they are dispatched by raw kind at the
configuration boundary, not by the typed decoder, and the Go mirror does
not carry them until task-m02.

| Kind | Value | Direction | Payload |
|---|---|---|---|
| BootstrapRequest | `0x0400` | client to any node | `BootstrapRequestV1 { cluster, domain, known_epoch?, known_certificate? }` |
| BootstrapResponse | `0x0401` | node to client | `BootstrapResponseV1 { records[<=64] of GroupConfigurationV1, complete, ballot?: BallotConfigurationV1, endpoints?: EndpointCatalogV1 }` |
| Subscribe | `0x0402` | client to node | `SubscribeV1 { cluster, domain, epoch, endpoint_generation, catalog_generation }` |
| Notice | `0x0403` | node to subscriber | `NoticeV1 { hint: ConfigurationHintV1 }` |
| ObserverDiscoveryRequest | `0x0404` | client to node | `ObserverDiscoveryRequestV1 { cluster, domain, after?, limit (1..=64) }` |
| ObserverDiscoveryPage | `0x0405` | node to client | `ObserverDiscoveryPageV1 { epoch, generation, entries[<=64], next?, complete }` |

`GroupConfigurationV1` binds cluster, domain, epoch, the exact voters
(node, committed incarnation, uncompressed P-256 public key), the quorum
policy identifier (`1` C2 fixed majority, `2` C1), the previous epoch's
certificate hash and the activation evidence: the genesis admin's ES256
signature over the activation message for epoch one, or the terminal
certificate plus approvals of a majority of the previous epoch's voters
for a handoff, ascending by node. A record has at most five voters, the
design's ceiling for an active configuration. The activation message is
the domain-separated digest (`configuration-activation`) of every field
except the signatures; the certificate hash (`configuration-record`) is
the domain-separated digest of the activation message. The certificate
therefore names the signed content, not the evidence: reordering,
trimming to another majority or re-signing the approvals leaves it
unchanged, and a verifier compares held epochs by certificate. Only the
ascending approval order is well formed, so each copy has one encoding.
`BallotConfigurationV1 { cluster, domain, epoch,
configuration_certificate, ballot, quorum_policy, fast_set[<=5],
promises[<=5] }` binds a ballot's leader and sorted fast set to one
cluster, one domain and one configuration record under an epoch, with the
voters' promises over the `configuration-ballot` message. That message is
the domain-separated digest of cluster, domain, epoch, the configuration
certificate hash, the ballot number, the leader, the policy and the fast
set, in that order; a verifier accepts the certificate only for its own
cluster and domain and only when the named configuration certificate is
the one its chain holds for that epoch. Without the three context fields
the same promises are valid evidence in any domain sharing those voter
identities, incarnations and keys. Catalogs carry one voter attestation
over the `configuration-catalog` message. Frozen digests and frames:
`crates/coord-types/fixtures/config_frames_v1.json`.

A response, a hint or a larger epoch number authorizes nothing: the
receiver verifies the chain from its trusted genesis (`coord-membership`),
installs epochs monotonically, treats a hint as a refresh trigger only,
and lets endpoint and observer catalogs change addresses, certificate
routing and serving topology but never the voter set.

## Snapshot frames (task-49)

`SharedCheckpointV1` (`crates/coord-checkpoint`) is carried by two raw
kinds of the snapshot range (class limit 1 MiB + 64 KiB), dispatched at the
snapshot boundary like the collector, binding and configuration frames.

| Kind | Value | Direction | Payload |
|---|---|---|---|
| Manifest | `0x0600` | donor to learner | `SharedManifestV1 { format, cluster, domain, configuration, boundary { execution_position, kv_revision, retention_floor, lease_authority }, collections[] { collection, rows, bytes }, chunks[] { ordinal, rows, bytes, first, last, digest }, root }`, encoded at most the class limit less the frame header's kind and version |
| Chunk | `0x0601` | donor to learner | `ChunkV1 { ordinal, rows[] { collection, key, value } }`, at most 1 MiB plus one maximal row |

Rows are the common collections of the registry (never `meta_v1`,
`protocol_v1` or `checkpoint_v1`) in identifier order and unsigned key
order; a chunk's digest is the `shared-checkpoint-chunk` BLAKE3 of its
encoded bytes and the manifest root is the `shared-checkpoint-root` digest
of format, identity, boundary, collection summaries and chunk descriptors.
The receiver verifies root, digests, order, uniqueness, counts and bounds
(`coord_checkpoint::verify_shared`) before any install (task-50). The
artifact carries no node identity, incarnation, boot, stamp or journal
sequence and is not the local recovery checkpoint (task-j04).

The manifest is not paginated: one Manifest frame carries the whole
descriptor list, so the encoded manifest is itself a supported-size bound
of the artifact rather than a property of the transport. Export refuses a
checkpoint whose manifest exceeds it (`ManifestTooLarge`) and structural
verification refuses to accept one, so an artifact that passes either can
always be framed. Chunk count alone does not decide it, since every
descriptor also carries its chunk's first and last key; the derived count
ceiling `MAX_CHUNKS` is only what the frame could describe if every
boundary key were empty.

## Go mirror (task-44)

`adapters/kine/wire` mirrors the frame codec and the postcard subset of
the client-facing DTOs (Hello, HelloAck, Close, Request, Response,
ResolveRequest and the four Watch frames) for the trusted Go collector.
It owns no schema: the numeric kinds, versions and byte layout are this
document's, and both sides are verified against the shared fixtures in
`crates/coord-types/fixtures/wire_frames_v1.json` (every valid vector
decodes and re-encodes to identical bytes; the malformed corpus is
rejected by class within a bounded read). A schema change requires
reviewed fixtures on both sides. No Serde reflection, cgo, protobuf or Go
voting state machine is introduced; the collector and configuration
evidence schemas are reserved for later client integration (task-m02).

task-46 extends the mirror with the Kine subset of two schemas the
adapter must produce and consume, and the raw binding kinds: the
`logical_v1` operations `Range`, `KineCreate`, `KineUpdate` and
`KineDelete` (encoder and decoder, verified against
`crates/coord-types/fixtures/command_ids_v1.json`), the
`coord_state::Response` outcomes a Kine request can receive (decoder,
verified against `crates/coord-state/fixtures/kine_responses_v1.json`;
every other `Outcome` variant is refused as unexpected), the command-id
and Kine-binding derivations (BLAKE3 derive-key contexts of
`HashDomain`, verified against `command_ids_v1.json` and
`kine_bindings_v1.json`), and `Bind`/`BindAck` (raw kinds, not part of
the typed registry). A schema change on either side requires reviewed
fixtures on both.

task-47 adds the `Compact` operation and the `Compacted` outcome to the
mirrored subsets, and uses the watch lane (capability `0x0012`, a second
bound connection of the same session): each watch is one bidirectional
stream carrying the `WatchOpen` first, then `WatchEvents` (chunks of one
revision until `complete`), `WatchProgress` and the final `WatchClose`;
the client cancels by writing a `WatchClose` (`Cancelled`) on that stream.
`start_revision` is inclusive on both sides; the adapter resumes a lost
stream from its last complete revision plus one. On the unary lane a
`Pending` answer means the endpoint knows the identity (resolve again),
while an `Unknown` answer to a `ResolveRequest` means it never saw it, so
the identical `Request` is re-sent under the same retry key.
