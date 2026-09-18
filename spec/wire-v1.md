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
