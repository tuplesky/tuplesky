//! Record sealing, verification and exact decoding; group completion
//! mapping; frozen record-format fixture.

use std::path::PathBuf;

use coord_core::effect::{ApplyBase, BarrierId, BootId, StoreUpdate};
use coord_core::event::StorageEvent;
use coord_journal_api::frontier::{
    CheckpointPointerV1, LOCAL_CHECKPOINT_FORMAT_V1, OLDEST_POINTER_FORMAT,
};
use coord_journal_api::group::{GroupEntry, GroupError, GroupLimits, GroupWrite};
use coord_journal_api::head::WrittenBytes;
use coord_journal_api::record::{
    GENESIS_PREDECESSOR, GroupOutcomeV1, JOURNAL_RECORD_FORMAT_GROUPS, JOURNAL_RECORD_FORMAT_V1,
    JournalRecordV1, LifecycleRecordV1, MAX_RECORD_BYTES, MAX_RECORD_KEY_BYTES, MAX_RECORD_UPDATES,
    MAX_RECORD_VALUE_BYTES, RecordBody, RecordDraft, RecordError, RecordExpectation, RecordOrigin,
    TransitionContext,
};
use coord_journal_api::stream::StorageStreamId;
use coord_store_api::registry::Collection;
use coord_types::formats::Format;
use coord_types::identity::Digest32;
use coord_types::ids::{
    Ballot, ClusterId, ConfigurationEpoch, DomainId, ExecutionPosition, KvRevision,
    LocalJournalSeq, ReplicaId, ReplicaIncarnation,
};
use serde::{Deserialize, Serialize};

fn origin() -> RecordOrigin {
    RecordOrigin {
        cluster: ClusterId([1; 16]),
        domain: DomainId([2; 16]),
        replica: ReplicaId([3; 16]),
        incarnation: ReplicaIncarnation::new(4).unwrap(),
        stream: StorageStreamId::from_durable(5).unwrap(),
    }
}

fn seq(n: u64) -> LocalJournalSeq {
    LocalJournalSeq::new(n).unwrap()
}

fn context() -> TransitionContext {
    let epoch = ConfigurationEpoch::new(2).unwrap();
    TransitionContext {
        boot: BootId([0xb0; 16]),
        configuration: epoch,
        ballot: Ballot {
            epoch,
            number: 3,
            leader: ReplicaId([3; 16]),
        },
    }
}

fn update(tag: u8) -> StoreUpdate {
    StoreUpdate {
        collection: Collection::ProtocolV1.id(),
        key: vec![tag, 1, 2],
        value: Some(vec![tag; 4]),
    }
}

fn protocol() -> RecordBody {
    RecordBody::ProtocolTransition {
        context: context(),
        updates: vec![update(1), update(2)],
    }
}

fn application() -> RecordBody {
    RecordBody::ApplicationOutcome {
        context: context(),
        base: ApplyBase {
            configuration: ConfigurationEpoch::new(2).unwrap(),
            execution_position: ExecutionPosition::new(9).unwrap(),
        },
        position: ExecutionPosition::new(10).unwrap(),
        revision: Some(KvRevision::new(77).unwrap()),
        result_digest: Digest32([0xaa; 32]),
        updates: vec![StoreUpdate {
            collection: Collection::KvCurrentV1.id(),
            key: vec![9],
            value: None,
        }],
    }
}

fn pointer(represented: u64) -> CheckpointPointerV1 {
    CheckpointPointerV1 {
        origin: origin(),
        represented: seq(represented),
        format: LOCAL_CHECKPOINT_FORMAT_V1,
        manifest_digest: Digest32([0x11; 32]),
        checkpoint_id: Digest32([0x22; 32]),
    }
}

fn genesis() -> JournalRecordV1 {
    genesis_at(JOURNAL_RECORD_FORMAT_V1)
}

/// A genesis record of `format`, naming it.
fn genesis_at(format: u16) -> JournalRecordV1 {
    JournalRecordV1::seal_as(
        format,
        RecordDraft {
            origin: origin(),
            seq: seq(1),
            predecessor: GENESIS_PREDECESSOR,
            body: RecordBody::Lifecycle(LifecycleRecordV1::Genesis { format }),
        },
    )
    .unwrap()
}

/// The format this build reads and doesn't write: 1 since step 1b, the
/// one every journal before it holds.
const FORMAT_BEFORE_GROUPS: u16 = 1;

/// Genesis, boot, protocol, application and pointer records chained, at
/// the format this build writes.
fn stream() -> Vec<JournalRecordV1> {
    stream_at(JOURNAL_RECORD_FORMAT_V1)
}

/// [`stream`], every record sealed at `format`.
fn stream_at(format: u16) -> Vec<JournalRecordV1> {
    let mut out = vec![genesis_at(format)];
    let bodies = [
        RecordBody::Lifecycle(LifecycleRecordV1::Boot {
            boot: BootId([0xb0; 16]),
        }),
        protocol(),
        application(),
        RecordBody::PublishLocalCheckpoint(pointer(3)),
    ];
    for body in bodies {
        let last = out.last().unwrap();
        out.push(
            JournalRecordV1::seal_as(
                format,
                RecordDraft {
                    origin: origin(),
                    seq: last.seq().checked_next().unwrap(),
                    predecessor: last.digest(),
                    body,
                },
            )
            .unwrap(),
        );
    }
    out
}

fn seal(
    seq: LocalJournalSeq,
    predecessor: Digest32,
    body: RecordBody,
) -> Result<JournalRecordV1, RecordError> {
    JournalRecordV1::seal(RecordDraft {
        origin: origin(),
        seq,
        predecessor,
        body,
    })
}

/// A pointer published before the image's format was raised seals and
/// decodes: the record's layout is the same, so a journal that retains one
/// still opens. What refuses it is loading the image (#247, Codex).
#[test]
fn a_pointer_naming_an_older_image_format_still_decodes() {
    const { assert!(OLDEST_POINTER_FORMAT < LOCAL_CHECKPOINT_FORMAT_V1) };
    let g = genesis();
    for format in OLDEST_POINTER_FORMAT..=LOCAL_CHECKPOINT_FORMAT_V1 {
        let record = seal(
            seq(2),
            g.digest(),
            RecordBody::PublishLocalCheckpoint(CheckpointPointerV1 {
                format,
                ..pointer(1)
            }),
        )
        .expect("seals");
        let decoded = JournalRecordV1::decode(&record.encode().unwrap()).expect("decodes");
        assert_eq!(decoded, record);
        decoded
            .verify(&RecordExpectation::genesis(origin()).after(&g).unwrap())
            .expect("verifies");
    }
}

#[test]
fn chained_records_verify_and_round_trip_exactly() {
    let records = stream();
    let mut expect = RecordExpectation::genesis(origin());
    for r in &records {
        r.verify(&expect).unwrap();
        let bytes = r.encode().unwrap();
        let decoded = JournalRecordV1::decode(&bytes).unwrap();
        assert_eq!(&decoded, r);
        assert_eq!(decoded.encode().unwrap(), bytes);
        assert_eq!(r.format(), JOURNAL_RECORD_FORMAT_V1);
        expect = expect.after(r).unwrap();
    }
    assert_eq!(expect.seq, seq(6));
    assert_eq!(expect.predecessor, records[4].digest());
    // Digests chain: changing any earlier record changes every later one.
    let digests: Vec<Digest32> = records.iter().map(JournalRecordV1::digest).collect();
    assert_eq!(
        digests
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        5
    );
    assert_eq!(records[2].body().updates().len(), 2);
    assert!(records[4].body().updates().is_empty());
}

/// The three ways to encode a record agree (task-d60): `encoded_len`
/// is the length counted when the record was sealed or decoded,
/// `encode_into` appends after what a buffer already holds, and both give
/// exactly what `encode` gives, which the frozen fixture pins. A record
/// decoded from those bytes carries the same length and encodes to the
/// same bytes.
#[test]
fn encoded_len_and_encode_into_agree_with_encode() {
    let mut buf = vec![0xee; 3];
    let mut expect = buf.clone();
    for r in &stream() {
        let bytes = r.encode().unwrap();
        assert_eq!(r.encoded_len().unwrap(), bytes.len());
        r.encode_into(&mut buf).unwrap();
        expect.extend_from_slice(&bytes);
        assert_eq!(buf, expect);
        let decoded = JournalRecordV1::decode(&bytes).unwrap();
        assert_eq!(decoded.encoded_len().unwrap(), bytes.len());
        assert_eq!(decoded.encode().unwrap(), bytes);
        assert_eq!(&decoded, r);
        // A clone shares the body rather than copying its updates.
        let clone = r.clone();
        assert!(std::ptr::eq(clone.body(), r.body()));
        assert_eq!(&clone, r);
    }
}

type VerifyCase = (fn(&mut RecordExpectation), RecordError);

#[test]
fn verify_rejects_mismatched_origin_index_and_predecessor() {
    let records = stream();
    let r = &records[2];
    let good = RecordExpectation {
        origin: origin(),
        seq: seq(3),
        predecessor: records[1].digest(),
    };
    r.verify(&good).unwrap();
    let cases: [VerifyCase; 7] = [
        (
            |e| e.origin.cluster = ClusterId([9; 16]),
            RecordError::ClusterMismatch,
        ),
        (
            |e| e.origin.domain = DomainId([9; 16]),
            RecordError::DomainMismatch,
        ),
        (
            |e| e.origin.replica = ReplicaId([9; 16]),
            RecordError::ReplicaMismatch,
        ),
        (
            |e| e.origin.incarnation = ReplicaIncarnation::new(5).unwrap(),
            RecordError::IncarnationMismatch,
        ),
        (
            |e| e.origin.stream = StorageStreamId::from_durable(6).unwrap(),
            RecordError::StreamMismatch,
        ),
        (
            |e| e.seq = LocalJournalSeq::new(4).unwrap(),
            RecordError::IndexMismatch {
                expected: LocalJournalSeq::new(4).unwrap(),
                found: LocalJournalSeq::new(3).unwrap(),
            },
        ),
        (
            |e| e.predecessor = Digest32([1; 32]),
            RecordError::PredecessorMismatch,
        ),
    ];
    for (mutate, expected) in cases {
        let mut e = good;
        mutate(&mut e);
        assert_eq!(r.verify(&e), Err(expected));
    }
}

#[test]
fn decode_rejects_tampering_truncation_trailing_bytes_and_formats() {
    let records = stream();
    let r = &records[2];
    let bytes = r.encode().unwrap();
    // Every single-byte change is detected (digest, guard or framing).
    for i in 0..bytes.len() {
        let mut t = bytes.clone();
        t[i] ^= 0x01;
        let result = JournalRecordV1::decode(&t);
        assert!(result.is_err(), "byte {i} flipped went undetected");
    }
    // Digest mismatch specifically: flip the last body byte (the stored
    // digest is the trailing 32 bytes).
    let mut body_changed = bytes.clone();
    body_changed[bytes.len() - 33] ^= 0x01;
    assert_eq!(
        JournalRecordV1::decode(&body_changed),
        Err(RecordError::DigestMismatch)
    );
    for cut in 0..bytes.len() {
        assert!(JournalRecordV1::decode(&bytes[..cut]).is_err());
    }
    assert_eq!(
        JournalRecordV1::decode(&bytes[..bytes.len() - 1]),
        Err(RecordError::Truncated)
    );
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert_eq!(
        JournalRecordV1::decode(&trailing),
        Err(RecordError::TrailingBytes)
    );
    // The leading field is the record format; a format past the newest
    // this build reads is refused without inspecting the rest.
    assert_eq!(bytes[0], JOURNAL_RECORD_FORMAT_V1 as u8);
    let past = Format::JournalRecord.window().newest + 1;
    let mut other = bytes.clone();
    other[0] = past as u8;
    assert_eq!(
        JournalRecordV1::decode(&other),
        Err(RecordError::UnsupportedFormat { found: past as u16 })
    );
    // One it reads is taken as written: the format is in the digest, so
    // a record's bytes relabelled with another format don't decode.
    other[0] = FORMAT_BEFORE_GROUPS as u8;
    assert_eq!(
        JournalRecordV1::decode(&other),
        Err(RecordError::DigestMismatch)
    );
    assert_eq!(
        JournalRecordV1::decode(&vec![1u8; MAX_RECORD_BYTES + 1]),
        Err(RecordError::TooLarge)
    );
    assert_eq!(JournalRecordV1::decode(&[]), Err(RecordError::Truncated));
}

/// Only a record's own encoding decodes (#284, Codex). A varint written
/// longer than it needs decodes to the same value and passes the digest,
/// which is derived from the decoded fields and the body encoded again;
/// taken, the record would carry the longer length and encode to other
/// bytes than it was read from. Refused, wherever the varint is: the
/// record's format in its header, and the genesis format in its body.
#[test]
fn decode_takes_only_the_canonical_encoding() {
    let genesis = genesis();
    let bytes = genesis.encode().unwrap();
    // The record's format leads; the genesis record's own format is the
    // body's last byte, before the 32 bytes of the digest.
    assert_eq!(bytes[0], JOURNAL_RECORD_FORMAT_V1 as u8);
    let tail = bytes.len() - 33;
    assert_eq!(bytes[tail], JOURNAL_RECORD_FORMAT_V1 as u8);
    for at in [0, tail] {
        // The same value, as a two-byte varint: the low seven bits with
        // the continuation bit, then a zero.
        let mut longer = bytes[..at].to_vec();
        longer.extend_from_slice(&[0x80 | bytes[at], 0x00]);
        longer.extend_from_slice(&bytes[at + 1..]);
        assert_eq!(
            JournalRecordV1::decode(&longer),
            Err(RecordError::NonCanonical),
            "an overlong varint at byte {at} was taken"
        );
    }
    let decoded = JournalRecordV1::decode(&bytes).unwrap();
    assert_eq!(decoded.encoded_len().unwrap(), bytes.len());
    assert_eq!(decoded.encode().unwrap(), bytes);
}

type SealCase = (LocalJournalSeq, Digest32, RecordBody, RecordError);

#[test]
fn seal_rejects_bounds_and_guards() {
    let g = genesis();
    let ctx = context();
    let mut wrong_epoch = ctx;
    wrong_epoch.ballot.epoch = ConfigurationEpoch::new(3).unwrap();
    let base = ApplyBase {
        configuration: ctx.configuration,
        execution_position: ExecutionPosition::new(9).unwrap(),
    };
    let outcome = |position: u64, base: ApplyBase, context: TransitionContext| {
        RecordBody::ApplicationOutcome {
            context,
            base,
            position: ExecutionPosition::new(position).unwrap(),
            revision: None,
            result_digest: Digest32([0; 32]),
            updates: vec![update(1)],
        }
    };
    let cases: Vec<SealCase> = vec![
        (
            LocalJournalSeq::ZERO,
            g.digest(),
            protocol(),
            RecordError::ZeroSequence,
        ),
        (
            seq(2),
            g.digest(),
            RecordBody::ProtocolTransition {
                context: ctx,
                updates: vec![],
            },
            RecordError::EmptyUpdates,
        ),
        (
            seq(2),
            g.digest(),
            RecordBody::ProtocolTransition {
                context: ctx,
                updates: vec![update(1); MAX_RECORD_UPDATES + 1],
            },
            RecordError::TooManyUpdates,
        ),
        (
            seq(2),
            g.digest(),
            RecordBody::ProtocolTransition {
                context: ctx,
                updates: vec![StoreUpdate {
                    key: vec![0; MAX_RECORD_KEY_BYTES + 1],
                    ..update(1)
                }],
            },
            RecordError::KeyTooLong,
        ),
        (
            seq(2),
            g.digest(),
            RecordBody::ProtocolTransition {
                context: ctx,
                updates: vec![StoreUpdate {
                    value: Some(vec![0; MAX_RECORD_VALUE_BYTES + 1]),
                    ..update(1)
                }],
            },
            RecordError::ValueTooLong,
        ),
        (
            seq(2),
            g.digest(),
            RecordBody::ProtocolTransition {
                context: wrong_epoch,
                updates: vec![update(1)],
            },
            RecordError::EpochMismatch,
        ),
        (
            seq(2),
            g.digest(),
            outcome(
                10,
                ApplyBase {
                    configuration: ConfigurationEpoch::new(3).unwrap(),
                    ..base
                },
                ctx,
            ),
            RecordError::EpochMismatch,
        ),
        (
            seq(2),
            g.digest(),
            outcome(0, base, ctx),
            RecordError::ZeroPosition,
        ),
        (
            seq(2),
            g.digest(),
            outcome(9, base, ctx),
            RecordError::PositionNotAfterBase,
        ),
        // Established history is contiguous: a position beyond the next
        // one encodes a gap replay could never reconcile with the base.
        (
            seq(2),
            g.digest(),
            outcome(11, base, ctx),
            RecordError::PositionNotAfterBase,
        ),
        (
            seq(2),
            g.digest(),
            RecordBody::PublishLocalCheckpoint(CheckpointPointerV1 {
                origin: RecordOrigin {
                    domain: DomainId([9; 16]),
                    ..origin()
                },
                ..pointer(1)
            }),
            RecordError::PointerOriginMismatch,
        ),
        (
            seq(2),
            g.digest(),
            RecordBody::PublishLocalCheckpoint(pointer(2)),
            RecordError::PointerNotBelowRecord,
        ),
        (
            seq(2),
            g.digest(),
            RecordBody::PublishLocalCheckpoint(CheckpointPointerV1 {
                format: 7,
                ..pointer(1)
            }),
            RecordError::UnsupportedFormat { found: 7 },
        ),
        (
            seq(2),
            g.digest(),
            RecordBody::PublishLocalCheckpoint(CheckpointPointerV1 {
                format: 0,
                ..pointer(1)
            }),
            RecordError::UnsupportedFormat { found: 0 },
        ),
        (
            seq(1),
            GENESIS_PREDECESSOR,
            protocol(),
            RecordError::FirstNotGenesis,
        ),
        (
            seq(2),
            g.digest(),
            RecordBody::Lifecycle(LifecycleRecordV1::Genesis {
                format: JOURNAL_RECORD_FORMAT_V1,
            }),
            RecordError::GenesisNotFirst,
        ),
        (
            seq(1),
            Digest32([1; 32]),
            RecordBody::Lifecycle(LifecycleRecordV1::Genesis {
                format: JOURNAL_RECORD_FORMAT_V1,
            }),
            RecordError::GenesisNotFirst,
        ),
        (
            seq(1),
            GENESIS_PREDECESSOR,
            // The format past the newest this build reads (it reads a
            // group's, 2: `this_build_writes_one_format_and_reads_the_next`).
            RecordBody::Lifecycle(LifecycleRecordV1::Genesis {
                format: JOURNAL_RECORD_FORMAT_GROUPS + 1,
            }),
            RecordError::UnsupportedFormat {
                found: JOURNAL_RECORD_FORMAT_GROUPS + 1,
            },
        ),
    ];
    for (s, pred, body, expected) in cases {
        assert_eq!(seal(s, pred, body).err(), Some(expected));
    }
    // The large-record bound.
    let huge = RecordBody::ProtocolTransition {
        context: ctx,
        updates: (0..5u8)
            .map(|i| StoreUpdate {
                value: Some(vec![i; MAX_RECORD_VALUE_BYTES]),
                ..update(i)
            })
            .collect(),
    };
    assert_eq!(
        seal(seq(2), g.digest(), huge).err(),
        Some(RecordError::TooLarge)
    );
}

fn barrier(n: u64) -> BarrierId {
    BarrierId {
        node_generation: ReplicaIncarnation::new(4).unwrap(),
        boot_id: BootId([0xb0; 16]),
        sequence: n,
    }
}

#[test]
fn group_maps_success_to_exact_completions() {
    let records = stream();
    let s5 = origin().stream;
    let entry = GroupEntry::new(barrier(1), s5, records[..3].to_vec()).unwrap();
    assert_eq!((entry.first(), entry.last()), (seq(1), seq(3)));
    assert!(entry.bytes() > 0);
    let mut g = GroupWrite::new(GroupLimits::DEFAULT);
    assert_eq!(g.receipt(WrittenBytes(1)), Err(GroupError::EmptyGroup));
    g.push(entry.clone()).unwrap();
    assert_eq!(
        g.push(GroupEntry::new(barrier(2), s5, records[3..].to_vec()).unwrap()),
        Err(GroupError::StreamAlreadyInGroup)
    );
    // Another stream under the same barrier is refused too.
    let other_origin = RecordOrigin {
        stream: StorageStreamId::from_durable(6).unwrap(),
        ..origin()
    };
    let other_genesis = JournalRecordV1::seal(RecordDraft {
        origin: other_origin,
        seq: seq(1),
        predecessor: GENESIS_PREDECESSOR,
        body: RecordBody::Lifecycle(LifecycleRecordV1::Genesis {
            format: JOURNAL_RECORD_FORMAT_V1,
        }),
    })
    .unwrap();
    assert_eq!(
        g.push(
            GroupEntry::new(barrier(1), other_origin.stream, vec![other_genesis.clone()]).unwrap()
        ),
        Err(GroupError::DuplicateBarrier)
    );
    g.push(GroupEntry::new(barrier(2), other_origin.stream, vec![other_genesis]).unwrap())
        .unwrap();
    assert_eq!(g.record_count(), 4);
    let receipt = g.receipt(WrittenBytes(987_654)).unwrap();
    assert_eq!(receipt.written, WrittenBytes(987_654));
    assert_eq!(receipt.completions.len(), 2);
    assert_eq!(receipt.completions[0].barrier, barrier(1));
    assert_eq!(
        (receipt.completions[0].first, receipt.completions[0].last),
        (seq(1), seq(3))
    );
    assert_eq!(receipt.completions[1].stream, other_origin.stream);
    assert_eq!(
        receipt.completions[1].storage_event(),
        StorageEvent::JournalDurable {
            barrier_id: barrier(2),
            journal_seq: seq(1),
        }
    );
}

#[test]
fn group_entries_and_limits_are_checked() {
    let records = stream();
    let s5 = origin().stream;
    assert_eq!(
        GroupEntry::new(barrier(1), s5, vec![]).err(),
        Some(GroupError::EmptyEntry)
    );
    assert_eq!(
        GroupEntry::new(barrier(1), StorageStreamId::FIRST, records[..1].to_vec()).err(),
        Some(GroupError::StreamMismatch)
    );
    assert_eq!(
        GroupEntry::new(barrier(1), s5, vec![records[0].clone(), records[2].clone()]).err(),
        Some(GroupError::NotContiguous)
    );
    let rechained = JournalRecordV1::seal(RecordDraft {
        origin: origin(),
        seq: seq(2),
        predecessor: Digest32([7; 32]),
        body: protocol(),
    })
    .unwrap();
    assert_eq!(
        GroupEntry::new(barrier(1), s5, vec![records[0].clone(), rechained]).err(),
        Some(GroupError::ChainBroken)
    );
    let mut small = GroupWrite::new(GroupLimits {
        max_records: 2,
        max_bytes: 1 << 20,
    });
    assert_eq!(
        small.push(GroupEntry::new(barrier(1), s5, records[..3].to_vec()).unwrap()),
        Err(GroupError::TooManyRecords)
    );
    assert!(small.is_empty());
    let mut tiny = GroupWrite::new(GroupLimits {
        max_records: 64,
        max_bytes: 8,
    });
    assert_eq!(
        tiny.push(GroupEntry::new(barrier(1), s5, records[..1].to_vec()).unwrap()),
        Err(GroupError::TooManyBytes)
    );
    let mut large = GroupWrite::new(GroupLimits::LARGE_RECORD);
    large
        .push(GroupEntry::new(barrier(1), s5, records[..1].to_vec()).unwrap())
        .unwrap();
    assert_eq!(large.limits().max_bytes, MAX_RECORD_BYTES);
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct RecordFixture {
    schema: String,
    format: u16,
    max_record_bytes: usize,
    max_record_updates: usize,
    max_record_key_bytes: usize,
    max_record_value_bytes: usize,
    records: Vec<(String, u64, String, String)>,
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[test]
fn record_format_fixture_is_frozen() {
    let kinds = ["genesis", "boot", "protocol", "application", "pointer"];
    // Format 1's bytes, frozen: this build no longer writes it, and reads
    // every journal written before it (step 1b).
    let fixture = RecordFixture {
        schema: "journal_record_v1".to_owned(),
        format: FORMAT_BEFORE_GROUPS,
        max_record_bytes: MAX_RECORD_BYTES,
        max_record_updates: MAX_RECORD_UPDATES,
        max_record_key_bytes: MAX_RECORD_KEY_BYTES,
        max_record_value_bytes: MAX_RECORD_VALUE_BYTES,
        records: stream_at(FORMAT_BEFORE_GROUPS)
            .iter()
            .zip(kinds)
            .map(|(r, kind)| {
                (
                    kind.to_owned(),
                    r.seq().get(),
                    hex(&r.encode().unwrap()),
                    hex(&r.digest().0),
                )
            })
            .collect(),
    };
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/journal_record_v1.json");
    if std::env::var_os("COORD_JOURNAL_WRITE_FIXTURES").is_some() {
        let mut json = serde_json::to_string_pretty(&fixture).unwrap();
        json.push('\n');
        std::fs::write(&path, json).unwrap();
        return;
    }
    let stored: RecordFixture =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(
        stored, fixture,
        "journal record fixture drifted; the durable format is frozen"
    );
    // And this build reads it: each stored record decodes to the one
    // sealed here, at format 1.
    for ((_, _, bytes, _), record) in stored.records.iter().zip(stream_at(FORMAT_BEFORE_GROUPS)) {
        assert_eq!(JournalRecordV1::decode(&unhex(bytes)).unwrap(), record);
    }
}

// ---------------------------------------------------------------------
// A group's application outcomes in one record (the batching-rule doc's
// 10.7, step 1a). This build reads them; its successor writes them.
// ---------------------------------------------------------------------

fn base(at: u64) -> ApplyBase {
    ApplyBase {
        configuration: ConfigurationEpoch::new(2).unwrap(),
        execution_position: ExecutionPosition::new(at).unwrap(),
    }
}

fn outcome(position: u64, updates: u32) -> GroupOutcomeV1 {
    GroupOutcomeV1 {
        position: ExecutionPosition::new(position).unwrap(),
        revision: KvRevision::new(position * 10).ok(),
        result_digest: Digest32([position as u8; 32]),
        updates,
    }
}

fn kv(tag: u8) -> StoreUpdate {
    StoreUpdate {
        collection: Collection::KvCurrentV1.id(),
        key: vec![tag],
        value: Some(vec![tag; 3]),
    }
}

/// Three commands after position 9: the first writes two rows, the
/// second none (a rejection still takes its position), the third one.
fn group() -> RecordBody {
    RecordBody::ApplicationGroup {
        context: context(),
        base: base(9),
        outcomes: vec![outcome(10, 2), outcome(11, 0), outcome(12, 1)],
        updates: vec![kv(1), kv(2), kv(3)],
    }
}

fn seal_at(
    format: u16,
    seq: LocalJournalSeq,
    predecessor: Digest32,
    body: RecordBody,
) -> Result<JournalRecordV1, RecordError> {
    JournalRecordV1::seal_as(
        format,
        RecordDraft {
            origin: origin(),
            seq,
            predecessor,
            body,
        },
    )
}

#[test]
fn a_group_of_outcomes_seals_decodes_and_verifies_at_its_format() {
    let genesis = genesis();
    let record = seal_at(
        JOURNAL_RECORD_FORMAT_GROUPS,
        seq(2),
        genesis.digest(),
        group(),
    )
    .unwrap();
    assert_eq!(record.format(), JOURNAL_RECORD_FORMAT_GROUPS);
    let bytes = record.encode().unwrap();
    let decoded = JournalRecordV1::decode(&bytes).unwrap();
    assert_eq!(decoded, record);
    assert_eq!(decoded.encoded_len().unwrap(), bytes.len());
    decoded
        .verify(
            &RecordExpectation::genesis(origin())
                .after(&genesis)
                .unwrap(),
        )
        .unwrap();
    // It reads as the three outcomes, one after another: the frontier
    // moves from the group's base to the last one's position, and the
    // updates are every outcome's in order.
    let body = decoded.body();
    assert_eq!(body.application_outcomes(), 3);
    assert_eq!(
        body.execution(),
        Some((base(9), ExecutionPosition::new(12).unwrap()))
    );
    assert_eq!(body.updates(), &[kv(1), kv(2), kv(3)][..]);
    // A per-command outcome reads the same way, as a group of one.
    let single = application();
    assert_eq!(single.application_outcomes(), 1);
    assert_eq!(
        single.execution(),
        Some((base(9), ExecutionPosition::new(10).unwrap()))
    );
    assert_eq!(protocol().execution(), None);
    assert_eq!(protocol().application_outcomes(), 0);
}

/// Format 1 never holds a group: a build that reads only format 1 would
/// refuse it as malformed, so a group in a record labelled 1 is refused
/// at sealing and at decoding alike.
#[test]
fn a_group_is_refused_in_a_record_of_the_format_before_it() {
    assert_eq!(
        seal_at(FORMAT_BEFORE_GROUPS, seq(2), Digest32([7; 32]), group()),
        Err(RecordError::GroupBeforeItsFormat)
    );
    let record = seal_at(
        JOURNAL_RECORD_FORMAT_GROUPS,
        seq(2),
        Digest32([7; 32]),
        group(),
    )
    .unwrap();
    let mut bytes = record.encode().unwrap();
    assert_eq!(bytes[0], JOURNAL_RECORD_FORMAT_GROUPS as u8);
    bytes[0] = FORMAT_BEFORE_GROUPS as u8;
    assert_eq!(
        JournalRecordV1::decode(&bytes),
        Err(RecordError::GroupBeforeItsFormat)
    );
}

#[test]
fn a_group_is_held_to_its_outcomes_positions_and_counts() {
    let at = |body| {
        seal_at(
            JOURNAL_RECORD_FORMAT_GROUPS,
            seq(2),
            Digest32([7; 32]),
            body,
        )
    };
    let with = |b: ApplyBase, outcomes: Vec<GroupOutcomeV1>, updates: Vec<StoreUpdate>| {
        RecordBody::ApplicationGroup {
            context: context(),
            base: b,
            outcomes,
            updates,
        }
    };
    assert_eq!(
        at(with(base(9), vec![], vec![])),
        Err(RecordError::EmptyGroup)
    );
    // The first outcome takes the position after the base, and each later
    // one the position after the one before it.
    assert_eq!(
        at(with(base(9), vec![outcome(11, 0)], vec![])),
        Err(RecordError::PositionNotAfterBase)
    );
    assert_eq!(
        at(with(base(9), vec![outcome(10, 0), outcome(12, 0)], vec![])),
        Err(RecordError::PositionNotAfterBase)
    );
    assert_eq!(
        at(with(base(9), vec![outcome(10, 0), outcome(10, 0)], vec![])),
        Err(RecordError::PositionNotAfterBase)
    );
    // The outcomes' counts are exactly the group's updates.
    assert_eq!(
        at(with(
            base(9),
            vec![outcome(10, 1), outcome(11, 1)],
            vec![kv(1)]
        )),
        Err(RecordError::GroupUpdatesMismatch)
    );
    assert_eq!(
        at(with(base(9), vec![outcome(10, 0)], vec![kv(1)])),
        Err(RecordError::GroupUpdatesMismatch)
    );
    assert_eq!(
        at(with(
            base(9),
            vec![outcome(10, u32::MAX), outcome(11, u32::MAX)],
            vec![]
        )),
        Err(RecordError::GroupUpdatesMismatch)
    );
    // A group of commands that changed no rows still takes positions.
    at(with(base(9), vec![outcome(10, 0), outcome(11, 0)], vec![])).unwrap();
    // The record's bounds hold of the group's updates together.
    let many = MAX_RECORD_UPDATES + 1;
    let half = (many / 2) as u32;
    assert_eq!(
        at(with(
            base(9),
            vec![outcome(10, half), outcome(11, many as u32 - half)],
            (0..many).map(|i| kv(i as u8)).collect(),
        )),
        Err(RecordError::TooManyUpdates)
    );
    // Its context, base and epoch agree, as a per-command outcome's do.
    let other = ApplyBase {
        configuration: ConfigurationEpoch::new(3).unwrap(),
        execution_position: ExecutionPosition::new(9).unwrap(),
    };
    assert_eq!(
        at(with(other, vec![outcome(10, 0)], vec![])),
        Err(RecordError::EpochMismatch)
    );
}

/// This build writes format 2, the first whose records hold a group, and
/// reads 1, which every journal before it holds; nothing past 2 (the
/// batching-rule doc's 10.7, step 1b).
#[test]
fn this_build_writes_the_format_of_groups_and_reads_the_one_before() {
    let window = Format::JournalRecord.window();
    assert_eq!(window.current, u32::from(JOURNAL_RECORD_FORMAT_V1));
    assert_eq!(window.current, u32::from(JOURNAL_RECORD_FORMAT_GROUPS));
    assert_eq!(window.newest, window.current);
    assert_eq!(window.oldest, u32::from(FORMAT_BEFORE_GROUPS));
    assert_eq!(genesis().format(), JOURNAL_RECORD_FORMAT_GROUPS);
    assert_eq!(
        seal(seq(2), Digest32([7; 32]), group()).unwrap().format(),
        JOURNAL_RECORD_FORMAT_GROUPS,
        "this build seals a group at the format it writes"
    );
    // A stream the release before it started names format 1, and opens
    // here.
    let genesis = genesis_at(FORMAT_BEFORE_GROUPS);
    assert_eq!(genesis.format(), FORMAT_BEFORE_GROUPS);
    assert_eq!(
        JournalRecordV1::decode(&genesis.encode().unwrap()).unwrap(),
        genesis
    );
    // Past the window, at sealing and in a genesis record alike.
    let past = JOURNAL_RECORD_FORMAT_GROUPS + 1;
    assert_eq!(
        seal_at(past, seq(2), Digest32([7; 32]), protocol()),
        Err(RecordError::UnsupportedFormat { found: past })
    );
    assert_eq!(
        seal_at(
            JOURNAL_RECORD_FORMAT_GROUPS,
            seq(1),
            GENESIS_PREDECESSOR,
            RecordBody::Lifecycle(LifecycleRecordV1::Genesis { format: past }),
        ),
        Err(RecordError::UnsupportedFormat { found: past })
    );
}

/// What this build writes, frozen by the release before it (step 1a): a
/// stream begun at format 2, with a group of three outcomes, then a lone
/// outcome and a protocol transition. This build seals these bytes with
/// its own `seal`, at the format it writes, and decodes and chains the
/// stored ones. 1a froze this file and reads it, so 1a's release opens
/// the journal this build writes (the batching-rule doc's 10.7, step 1b).
#[test]
fn record_format_groups_fixture_is_frozen() {
    assert_eq!(
        JOURNAL_RECORD_FORMAT_V1, JOURNAL_RECORD_FORMAT_GROUPS,
        "this build writes the format of groups"
    );
    let mut records: Vec<JournalRecordV1> = Vec::new();
    let bodies = [
        RecordBody::Lifecycle(LifecycleRecordV1::Genesis {
            format: JOURNAL_RECORD_FORMAT_GROUPS,
        }),
        RecordBody::Lifecycle(LifecycleRecordV1::Boot {
            boot: BootId([0xb0; 16]),
        }),
        group(),
        RecordBody::ApplicationOutcome {
            context: context(),
            base: base(12),
            position: ExecutionPosition::new(13).unwrap(),
            revision: None,
            result_digest: Digest32([0xcc; 32]),
            updates: vec![],
        },
        protocol(),
    ];
    for body in bodies {
        let (next, predecessor) = match records.last() {
            Some(last) => (last.seq().checked_next().unwrap(), last.digest()),
            None => (seq(1), GENESIS_PREDECESSOR),
        };
        records.push(
            JournalRecordV1::seal(RecordDraft {
                origin: origin(),
                seq: next,
                predecessor,
                body,
            })
            .unwrap(),
        );
    }
    let kinds = ["genesis", "boot", "group", "application", "protocol"];
    let fixture = RecordFixture {
        schema: "journal_record_v1".to_owned(),
        format: JOURNAL_RECORD_FORMAT_GROUPS,
        max_record_bytes: MAX_RECORD_BYTES,
        max_record_updates: MAX_RECORD_UPDATES,
        max_record_key_bytes: MAX_RECORD_KEY_BYTES,
        max_record_value_bytes: MAX_RECORD_VALUE_BYTES,
        records: records
            .iter()
            .zip(kinds)
            .map(|(r, kind)| {
                (
                    kind.to_owned(),
                    r.seq().get(),
                    hex(&r.encode().unwrap()),
                    hex(&r.digest().0),
                )
            })
            .collect(),
    };
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/journal_record_groups.json");
    if std::env::var_os("COORD_JOURNAL_WRITE_FIXTURES").is_some() {
        let mut json = serde_json::to_string_pretty(&fixture).unwrap();
        json.push('\n');
        std::fs::write(&path, json).unwrap();
        return;
    }
    let stored: RecordFixture =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(
        stored, fixture,
        "the groups fixture drifted; it is what 1a's release reads of this build's journal"
    );
    // The stored bytes decode and chain, without this build sealing them.
    let mut expect = RecordExpectation::genesis(origin());
    for (_, _, bytes, digest) in &stored.records {
        let record = JournalRecordV1::decode(&unhex(bytes)).unwrap();
        record.verify(&expect).unwrap();
        assert_eq!(hex(&record.digest().0), *digest);
        expect = expect.after(&record).unwrap();
    }
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}
