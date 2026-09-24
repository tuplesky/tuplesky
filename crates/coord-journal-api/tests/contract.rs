//! Record sealing, verification and exact decoding; group completion
//! mapping; frozen record-format fixture.

use std::path::PathBuf;

use coord_core::effect::{ApplyBase, BarrierId, BootId, StoreUpdate};
use coord_core::event::StorageEvent;
use coord_journal_api::frontier::{CheckpointPointerV1, LOCAL_CHECKPOINT_FORMAT_V1};
use coord_journal_api::group::{GroupEntry, GroupError, GroupLimits, GroupWrite};
use coord_journal_api::head::WrittenBytes;
use coord_journal_api::record::{
    GENESIS_PREDECESSOR, JOURNAL_RECORD_FORMAT_V1, JournalRecordV1, LifecycleRecordV1,
    MAX_RECORD_BYTES, MAX_RECORD_KEY_BYTES, MAX_RECORD_UPDATES, MAX_RECORD_VALUE_BYTES, RecordBody,
    RecordDraft, RecordError, RecordExpectation, RecordOrigin, TransitionContext,
};
use coord_journal_api::stream::StorageStreamId;
use coord_store_api::registry::Collection;
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
    JournalRecordV1::seal(RecordDraft {
        origin: origin(),
        seq: seq(1),
        predecessor: GENESIS_PREDECESSOR,
        body: RecordBody::Lifecycle(LifecycleRecordV1::Genesis {
            format: JOURNAL_RECORD_FORMAT_V1,
        }),
    })
    .unwrap()
}

/// Genesis, boot, protocol, application and pointer records chained.
fn stream() -> Vec<JournalRecordV1> {
    let mut out = vec![genesis()];
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
            JournalRecordV1::seal(RecordDraft {
                origin: origin(),
                seq: last.seq().checked_next().unwrap(),
                predecessor: last.digest(),
                body,
            })
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
    // The leading field is the record format; another format is refused
    // without inspecting the rest.
    assert_eq!(bytes[0], JOURNAL_RECORD_FORMAT_V1 as u8);
    let mut other = bytes.clone();
    other[0] = 2;
    assert_eq!(
        JournalRecordV1::decode(&other),
        Err(RecordError::UnsupportedFormat { found: 2 })
    );
    assert_eq!(
        JournalRecordV1::decode(&vec![1u8; MAX_RECORD_BYTES + 1]),
        Err(RecordError::TooLarge)
    );
    assert_eq!(JournalRecordV1::decode(&[]), Err(RecordError::Truncated));
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
            RecordBody::Lifecycle(LifecycleRecordV1::Genesis { format: 2 }),
            RecordError::UnsupportedFormat { found: 2 },
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
    let fixture = RecordFixture {
        schema: "journal_record_v1".to_owned(),
        format: JOURNAL_RECORD_FORMAT_V1,
        max_record_bytes: MAX_RECORD_BYTES,
        max_record_updates: MAX_RECORD_UPDATES,
        max_record_key_bytes: MAX_RECORD_KEY_BYTES,
        max_record_value_bytes: MAX_RECORD_VALUE_BYTES,
        records: stream()
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
}
