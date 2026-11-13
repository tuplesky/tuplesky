//! SCRATCH, TEST-ONLY, MUST NEVER MERGE (structural floors, #185 09:13
//! item 4): where the appender's time a command goes, and its floor when a
//! frame is one write. Run in release, by hand:
//! `cargo test --release -p coord-journal-raft-engine --test xfloor -- --ignored --nocapture`
//! (with `COORDD_X_NOCOMP=1` for no batch compression). Writes under
//! `XFLOOR_DIR` (default `/dev/shm/xfloor-app`).
#![allow(missing_docs)]

use std::path::Path;
use std::time::Instant;

use coord_core::effect::{BarrierId, BootId, StoreUpdate};
use coord_journal_api::engine::JournalEngine;
use coord_journal_api::group::{GroupEntry, GroupLimits, GroupWrite};
use coord_journal_api::record::{
    GENESIS_PREDECESSOR, JOURNAL_RECORD_FORMAT_V1, JournalRecordV1, LifecycleRecordV1, RecordBody,
    RecordDraft, RecordOrigin, TransitionContext,
};
use coord_journal_api::stream::{ShardId, StreamAllocator, StreamKey};
use coord_journal_raft_engine::{JournalIdentity, JournalOptions, RaftEngineJournal, RecordCodec};
use coord_store_api::registry::Collection;
use coord_types::identity::Digest32;
use coord_types::ids::{
    Ballot, ClusterId, ConfigurationEpoch, DomainId, LocalJournalSeq, ReplicaId, ReplicaIncarnation,
};
use raft_engine::ValueCodec;

fn context() -> TransitionContext {
    let epoch = ConfigurationEpoch::new(1).unwrap();
    TransitionContext {
        boot: BootId([0xb0; 16]),
        configuration: epoch,
        ballot: Ballot { epoch, number: 1, leader: ReplicaId([7; 16]) },
    }
}

/// One command's adoption rows, about the bench's sizes: a dependency row
/// and a payload row (the leader's frames carry ~630 B of payload each).
fn command_updates(n: u64) -> Vec<StoreUpdate> {
    let mut dep_key = vec![1u8; 9];
    dep_key.extend_from_slice(&n.to_be_bytes());
    let mut pay_key = vec![2u8; 9];
    pay_key.extend_from_slice(&n.to_be_bytes());
    vec![
        StoreUpdate { collection: Collection::ProtocolV1.id(), key: dep_key, value: Some(vec![(n % 251) as u8; 150]) },
        StoreUpdate { collection: Collection::ProtocolV1.id(), key: pay_key, value: Some(vec![(n % 241) as u8; 630]) },
    ]
}

struct Stream {
    origin: RecordOrigin,
    id: coord_journal_api::stream::StorageStreamId,
    seq: LocalJournalSeq,
    digest: Digest32,
}

impl Stream {
    fn seal(&mut self, body: RecordBody) -> JournalRecordV1 {
        self.seq = self.seq.checked_next().unwrap();
        let r = JournalRecordV1::seal(RecordDraft { origin: self.origin, seq: self.seq, predecessor: self.digest, body }).unwrap();
        self.digest = r.digest();
        r
    }
}

fn open(dir: &Path) -> (RaftEngineJournal, Stream) {
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    let options = JournalOptions { target_file_size_bytes: 64 * 1024 * 1024, purge_threshold_bytes: 1 << 30, ..JournalOptions::default() };
    let mut journal = RaftEngineJournal::create(dir, JournalIdentity { cluster: ClusterId([1; 16]), replica: ReplicaId([7; 16]) }, &options).unwrap();
    let key = StreamKey { cluster: ClusterId([1; 16]), domain: DomainId([3; 16]), incarnation: ReplicaIncarnation::new(1).unwrap() };
    let mut allocator = StreamAllocator::new();
    let mapping = allocator.allocate(key, ShardId::new(0).unwrap()).unwrap();
    journal.persist_mapping(allocator.high_water(), &mapping).unwrap();
    allocator.mapping_durable(mapping.stream).unwrap();
    let origin = RecordOrigin { cluster: key.cluster, domain: key.domain, replica: ReplicaId([7; 16]), incarnation: key.incarnation, stream: mapping.stream };
    let genesis = JournalRecordV1::seal(RecordDraft { origin, seq: LocalJournalSeq::new(1).unwrap(), predecessor: GENESIS_PREDECESSOR, body: RecordBody::Lifecycle(LifecycleRecordV1::Genesis { format: JOURNAL_RECORD_FORMAT_V1 }) }).unwrap();
    let mut g = GroupWrite::new(GroupLimits::DEFAULT);
    g.push(GroupEntry::new(barrier(0), mapping.stream, vec![genesis.clone()]).unwrap()).unwrap();
    journal.append_group(&g).unwrap();
    (journal, Stream { origin, id: mapping.stream, seq: genesis.seq(), digest: genesis.digest() })
}

fn barrier(n: u64) -> BarrierId {
    BarrierId { node_generation: ReplicaIncarnation::new(1).unwrap(), boot_id: BootId([0xb0; 16]), sequence: n }
}

/// How a frame of `depth` commands becomes records.
#[derive(Clone, Copy, Debug)]
enum Shape {
    /// Today: one record a command (its two rows), all in one group.
    RecordACommand,
    /// One record a frame holding every command's rows.
    RecordAFrame,
    /// The floor: one record a frame of one row, the frame's bytes.
    FrameBytes,
}

fn run(shape: Shape, depth: u64, frames: u64) -> (f64, f64, f64, f64) {
    let base = std::env::var("XFLOOR_DIR").unwrap_or_else(|_| "/dev/shm/xfloor-app".into());
    let dir = Path::new(&base).join(format!("{shape:?}-{depth}"));
    let (mut journal, mut stream) = open(&dir);
    let (mut seal_ns, mut codec_ns, mut append_ns) = (0u128, 0u128, 0u128);
    let mut bytes = 0usize;
    let mut cmd = 0u64;
    for f in 0..frames {
        let mut records = Vec::new();
        let t = Instant::now();
        match shape {
            Shape::RecordACommand => {
                for _ in 0..depth {
                    cmd += 1;
                    records.push(stream.seal(RecordBody::ProtocolTransition { context: context(), updates: command_updates(cmd) }));
                }
            }
            Shape::RecordAFrame => {
                let mut updates = Vec::new();
                for _ in 0..depth {
                    cmd += 1;
                    updates.extend(command_updates(cmd));
                }
                records.push(stream.seal(RecordBody::ProtocolTransition { context: context(), updates }));
            }
            Shape::FrameBytes => {
                let mut frame = Vec::with_capacity(depth as usize * 800);
                for _ in 0..depth {
                    cmd += 1;
                    for u in command_updates(cmd) {
                        frame.extend_from_slice(&u.key);
                        frame.extend_from_slice(u.value.as_deref().unwrap());
                    }
                }
                records.push(stream.seal(RecordBody::ProtocolTransition {
                    context: context(),
                    updates: vec![StoreUpdate { collection: Collection::ProtocolV1.id(), key: f.to_be_bytes().to_vec(), value: Some(frame) }],
                }));
            }
        }
        seal_ns += t.elapsed().as_nanos();
        let t = Instant::now();
        let mut buf = Vec::new();
        for r in &records {
            RecordCodec::encode_to(r, &mut buf).unwrap();
        }
        codec_ns += t.elapsed().as_nanos();
        bytes += buf.len();
        let mut g = GroupWrite::new(GroupLimits::DEFAULT);
        g.push(GroupEntry::new(barrier(f + 1), stream.id, records).unwrap()).unwrap();
        let t = Instant::now();
        journal.append_group(&g).unwrap();
        append_ns += t.elapsed().as_nanos();
    }
    drop(journal);
    let _ = std::fs::remove_dir_all(&dir);
    let per = |ns: u128| ns as f64 / 1000.0 / cmd as f64;
    (per(seal_ns), per(codec_ns), per(append_ns), bytes as f64 / cmd as f64)
}

#[test]
#[ignore]
fn appender_time_a_command_by_shape_and_depth() {
    let nocomp = std::env::var("COORDD_X_NOCOMP").is_ok_and(|v| v == "1");
    println!("XFLOOR compression={} (µs a command: seal = the domain thread's, append = the appender's, codec = one encode, as verify and the engine each do again)", !nocomp);
    for depth in [1u64, 4, 8, 18, 32, 64] {
        for shape in [Shape::RecordACommand, Shape::RecordAFrame, Shape::FrameBytes] {
            let frames = 40_000 / depth;
            let _ = run(shape, depth, frames.min(2_000)); // warm
            let (seal, codec, append, bytes) = run(shape, depth, frames);
            println!("XFLOOR depth={depth:2} shape={shape:?} seal={seal:6.2} codec={codec:6.2} append={append:6.2} bytes_a_cmd={bytes:6.0}");
        }
    }
}
