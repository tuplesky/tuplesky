//! Ownership examples, non-Send worker-local transactions, envelope and
//! registry fixtures.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::ops::Bound;
use std::path::PathBuf;
use std::rc::Rc;

use coord_core::effect::CollectionId;
use coord_store_api::engine::{
    CommitFailure, Direction, EngineError, ErrorClass, LocalEngine, OrderedRead, Row, RowPage,
    ScanRequest, SnapshotSource, WriteTxn,
};
use coord_store_api::envelope::{
    AppliedStamp, STAMP_RECORD_KIND, STAMP_SCHEMA_VERSION, StoreEnvelopeV1,
};
use coord_store_api::registry::{Collection, meta_fields};
use coord_store_api::seq::StoreSeq;
use coord_types::identity::Digest32;
use coord_types::ids::LocalJournalSeq;
use serde::{Deserialize, Serialize};

type Rows = BTreeMap<(u16, Vec<u8>), Vec<u8>>;
type SharedRows = std::sync::Arc<std::sync::RwLock<Rows>>;
type PendingRows = Rc<RefCell<BTreeMap<(u16, Vec<u8>), Option<Vec<u8>>>>>;
type RowIter<'a> = Box<dyn Iterator<Item = (&'a (u16, Vec<u8>), &'a Vec<u8>)> + 'a>;

/// A toy engine used only to prove the contract compiles with a worker-local
/// (`!Send`) transaction and `Send + Sync` readers.
#[derive(Default)]
struct ToyEngine {
    data: SharedRows,
}

#[derive(Clone)]
struct ToyReader(SharedRows);

struct ToyView(Rows);

impl OrderedRead for ToyView {
    fn get(&self, c: CollectionId, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        Ok(self.0.get(&(c.0, key.to_vec())).cloned())
    }
    fn scan_page(&self, c: CollectionId, r: &ScanRequest) -> Result<RowPage, EngineError> {
        let mut rows = Vec::new();
        let mut bytes = 0usize;
        let mut it: RowIter<'_> = match r.direction {
            Direction::Forward => Box::new(self.0.iter()),
            Direction::Reverse => Box::new(self.0.iter().rev()),
        };
        for ((cc, k), v) in it.by_ref() {
            if *cc != c.0 || !r.contains(k) || !r.past_cursor(k) {
                continue;
            }
            if rows.len() as u32 >= r.max_rows.get() {
                return Ok(RowPage {
                    rows,
                    exhausted: false,
                });
            }
            if bytes + k.len() + v.len() > r.max_bytes.get() as usize {
                if rows.is_empty() {
                    return Err(EngineError::new(
                        ErrorClass::Limit,
                        "row exceeds page budget",
                    ));
                }
                return Ok(RowPage {
                    rows,
                    exhausted: false,
                });
            }
            bytes += k.len() + v.len();
            rows.push(Row {
                key: k.clone(),
                value: v.clone(),
            });
        }
        Ok(RowPage {
            rows,
            exhausted: true,
        })
    }
}

impl SnapshotSource for ToyReader {
    type View = ToyView;
    fn snapshot(&self) -> Result<ToyView, EngineError> {
        Ok(ToyView(self.0.read().unwrap().clone()))
    }
}

/// Worker-local transaction: holds an `Rc`, so it is deliberately `!Send`.
struct ToyWrite<'a> {
    engine: &'a ToyEngine,
    pending: PendingRows,
}

impl OrderedRead for ToyWrite<'_> {
    fn get(&self, c: CollectionId, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        if let Some(v) = self.pending.borrow().get(&(c.0, key.to_vec())) {
            return Ok(v.clone());
        }
        Ok(self
            .engine
            .data
            .read()
            .unwrap()
            .get(&(c.0, key.to_vec()))
            .cloned())
    }
    fn scan_page(&self, c: CollectionId, r: &ScanRequest) -> Result<RowPage, EngineError> {
        let mut merged = self.engine.data.read().unwrap().clone();
        for (k, v) in self.pending.borrow().iter() {
            match v {
                Some(v) => {
                    merged.insert(k.clone(), v.clone());
                }
                None => {
                    merged.remove(k);
                }
            }
        }
        ToyView(merged).scan_page(c, r)
    }
}

impl WriteTxn for ToyWrite<'_> {
    fn put(&mut self, c: CollectionId, key: &[u8], value: &[u8]) -> Result<(), EngineError> {
        self.pending
            .borrow_mut()
            .insert((c.0, key.to_vec()), Some(value.to_vec()));
        Ok(())
    }
    fn delete(&mut self, c: CollectionId, key: &[u8]) -> Result<(), EngineError> {
        self.pending.borrow_mut().insert((c.0, key.to_vec()), None);
        Ok(())
    }
    fn abort(self) -> Result<(), EngineError> {
        Ok(())
    }
    fn commit_durable(self) -> Result<(), CommitFailure> {
        let mut data = self.engine.data.write().unwrap();
        for (k, v) in self.pending.borrow().iter() {
            match v {
                Some(v) => {
                    data.insert(k.clone(), v.clone());
                }
                None => {
                    data.remove(k);
                }
            }
        }
        Ok(())
    }
}

impl LocalEngine for ToyEngine {
    type Reader = ToyReader;
    type Write<'a> = ToyWrite<'a>;
    fn reader(&self) -> ToyReader {
        ToyReader(self.data.clone())
    }
    fn begin_write(&mut self) -> Result<ToyWrite<'_>, EngineError> {
        Ok(ToyWrite {
            engine: self,
            pending: Rc::new(RefCell::new(BTreeMap::new())),
        })
    }
}

fn assert_send_sync<T: Send + Sync>() {}
fn assert_send<T: Send>() {}

/// A worker job owns the engine, runs one transaction to completion and
/// hands snapshots to other threads.
fn worker_job<E: LocalEngine>(engine: &mut E) -> Result<(), CommitFailure> {
    let mut tx = engine
        .begin_write()
        .map_err(CommitFailure::DefinitelyNotCommitted)?;
    tx.put(Collection::KvCurrentV1.id(), b"k", b"v")
        .map_err(CommitFailure::DefinitelyNotCommitted)?;
    // Read-your-writes inside the transaction.
    assert_eq!(
        tx.get(Collection::KvCurrentV1.id(), b"k").unwrap(),
        Some(b"v".to_vec())
    );
    tx.commit_durable()
}

#[test]
fn ownership_example_compiles_and_runs() {
    assert_send_sync::<ToyReader>();
    assert_send::<ToyEngine>();
    let mut engine = ToyEngine::default();
    let reader = engine.reader();
    let before = reader.snapshot().unwrap();
    worker_job(&mut engine).unwrap();
    // A snapshot pinned before the commit does not see it; a new one does.
    assert_eq!(
        before.get(Collection::KvCurrentV1.id(), b"k").unwrap(),
        None
    );
    let after = std::thread::spawn(move || reader.snapshot().unwrap())
        .join()
        .unwrap();
    assert_eq!(
        after.get(Collection::KvCurrentV1.id(), b"k").unwrap(),
        Some(b"v".to_vec())
    );
    // Abort discards.
    let mut tx = engine.begin_write().unwrap();
    tx.delete(Collection::KvCurrentV1.id(), b"k").unwrap();
    tx.abort().unwrap();
    assert_eq!(
        engine
            .reader()
            .snapshot()
            .unwrap()
            .get(Collection::KvCurrentV1.id(), b"k")
            .unwrap(),
        Some(b"v".to_vec())
    );
}

#[test]
fn scan_request_bounds_and_cursors() {
    let r = ScanRequest {
        lower: Bound::Included(b"b".to_vec()),
        upper: Bound::Excluded(b"d".to_vec()),
        direction: Direction::Forward,
        resume_after: Some(b"b".to_vec()),
        max_rows: NonZeroU32::new(10).unwrap(),
        max_bytes: NonZeroU32::new(100).unwrap(),
    };
    assert!(r.contains(b"b") && r.contains(b"c") && !r.contains(b"d") && !r.contains(b"a"));
    assert!(!r.past_cursor(b"b") && r.past_cursor(b"c"));
    let rev = ScanRequest {
        direction: Direction::Reverse,
        resume_after: Some(b"c".to_vec()),
        ..r.clone()
    };
    assert!(rev.past_cursor(b"b") && !rev.past_cursor(b"c"));
    let all = ScanRequest::all(0, 0);
    assert_eq!(all.max_rows.get(), 1);
    assert!(all.contains(b"") && all.past_cursor(b""));
}

#[test]
fn oversized_row_is_a_limit_error_not_an_empty_page() {
    let mut engine = ToyEngine::default();
    let mut tx = engine.begin_write().unwrap();
    tx.put(Collection::KvCurrentV1.id(), b"big", &[0u8; 200])
        .unwrap();
    tx.commit_durable().unwrap();
    let view = engine.reader().snapshot().unwrap();
    let small = ScanRequest::all(10, 50);
    let err = view
        .scan_page(Collection::KvCurrentV1.id(), &small)
        .unwrap_err();
    assert_eq!(err.class, ErrorClass::Limit);
    let big = ScanRequest::all(10, 1_000);
    let page = view.scan_page(Collection::KvCurrentV1.id(), &big).unwrap();
    assert_eq!(page.rows.len(), 1);
    assert!(page.exhausted);
}

#[test]
fn store_seq_is_not_a_public_counter() {
    let seq = LocalJournalSeq::new(41).unwrap();
    let stamp = StoreSeq::from_journal(seq);
    assert_eq!(stamp.journal_seq(), seq);
    assert_eq!(StoreSeq::INITIAL.journal_seq(), LocalJournalSeq::ZERO);
    assert_eq!(format!("{stamp:?}"), "StoreSeq(41)");
}

#[test]
fn envelope_and_stamp_round_trip_and_reject_corruption() {
    let env = StoreEnvelopeV1 {
        record_kind: 7,
        schema_version: 1,
        payload: vec![1, 2, 3],
    };
    let bytes = env.encode().unwrap();
    assert_eq!(StoreEnvelopeV1::decode(&bytes).unwrap(), env);
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert_eq!(
        StoreEnvelopeV1::decode(&trailing).unwrap_err().class,
        ErrorClass::Corrupt
    );
    assert_eq!(
        StoreEnvelopeV1::decode(&bytes[..2]).unwrap_err().class,
        ErrorClass::Corrupt
    );
    let huge = StoreEnvelopeV1 {
        record_kind: 7,
        schema_version: 1,
        payload: vec![0; coord_store_api::envelope::MAX_ENVELOPE_PAYLOAD + 1],
    };
    assert_eq!(huge.encode().unwrap_err().class, ErrorClass::Limit);

    let seq = LocalJournalSeq::new(9).unwrap();
    let stamp = AppliedStamp {
        store_seq: StoreSeq::from_journal(seq),
        journal_seq: seq,
        last_batch_digest: Digest32([3; 32]),
    };
    let bytes = stamp.to_envelope().unwrap();
    assert_eq!(AppliedStamp::from_envelope(&bytes).unwrap(), stamp);
    let wrong_kind = StoreEnvelopeV1 {
        record_kind: 2,
        schema_version: STAMP_SCHEMA_VERSION,
        payload: postcard::to_allocvec(&stamp).unwrap(),
    }
    .encode()
    .unwrap();
    assert_eq!(
        AppliedStamp::from_envelope(&wrong_kind).unwrap_err().class,
        ErrorClass::Corrupt
    );
    let mismatched = AppliedStamp {
        store_seq: StoreSeq::from_journal(LocalJournalSeq::new(8).unwrap()),
        ..stamp
    };
    let bytes = StoreEnvelopeV1 {
        record_kind: STAMP_RECORD_KIND,
        schema_version: STAMP_SCHEMA_VERSION,
        payload: postcard::to_allocvec(&mismatched).unwrap(),
    }
    .encode()
    .unwrap();
    assert_eq!(
        AppliedStamp::from_envelope(&bytes).unwrap_err().class,
        ErrorClass::Corrupt
    );
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct RegistryFixture {
    schema: String,
    collections: Vec<(String, u16, bool)>,
    meta_fields: Vec<String>,
    stamp_example_hex: String,
    envelope_example_hex: String,
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[test]
fn registry_and_stamp_fixtures_are_frozen() {
    let seq = LocalJournalSeq::new(0x0102).unwrap();
    let stamp = AppliedStamp {
        store_seq: StoreSeq::from_journal(seq),
        journal_seq: seq,
        last_batch_digest: Digest32([0xab; 32]),
    };
    let fixture = RegistryFixture {
        schema: "store_registry_v1".to_owned(),
        collections: Collection::ALL
            .iter()
            .map(|c| (c.name().to_owned(), c.id().0, c.in_common_hash()))
            .collect(),
        meta_fields: [
            meta_fields::CLUSTER_ID,
            meta_fields::DOMAIN_ID,
            meta_fields::REPLICA_ID,
            meta_fields::INCARNATION,
            meta_fields::FORMAT_VERSION,
            meta_fields::ENGINE,
            meta_fields::PROFILE,
            meta_fields::APPLIED_STAMP,
            meta_fields::EXECUTION_FRONTIER,
            meta_fields::KV_REVISION,
            meta_fields::RETENTION_FLOOR,
            meta_fields::LEASE_AUTHORITY,
        ]
        .iter()
        .map(|f| String::from_utf8(f.to_vec()).unwrap())
        .collect(),
        stamp_example_hex: hex(&stamp.to_envelope().unwrap()),
        envelope_example_hex: hex(&StoreEnvelopeV1 {
            record_kind: 0x0102,
            schema_version: 3,
            payload: vec![0, 0xff, 7],
        }
        .encode()
        .unwrap()),
    };
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/store_registry_v1.json");
    if std::env::var_os("COORD_STORE_WRITE_FIXTURES").is_some() {
        let mut json = serde_json::to_string_pretty(&fixture).unwrap();
        json.push('\n');
        std::fs::write(&path, json).unwrap();
        return;
    }
    let stored: RegistryFixture =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(
        stored, fixture,
        "store registry/stamp fixture drifted; identifiers are frozen"
    );
}
