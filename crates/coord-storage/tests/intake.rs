//! task-d113, step 1: the dependency rows, and the leader's proposal rows,
//! keyed by intake order (design Section 17.1, as amended by task-d113's
//! design step).
//!
//! The machine and the journal name these rows by command; the store's
//! lowering keys them by the applied stamp's sequence of the record that
//! brings them and the row's ordinal in it, through an index by command
//! kept in memory. These tests hold the lowering to what the rows meant
//! when they were keyed by command: boot reads the same machine, a
//! rewrite stays in place, the recovery cut lays its rows where
//! materialization will (rule (e)), the index follows a commit whose
//! outcome is not known (rule (d)), replay writes the same keys, and a
//! store of the other layout is refused.

use std::collections::{BTreeMap, BTreeSet};

use coord_consensus::rows::{
    DEPENDENCY_TAG, PROPOSAL_TAG, ProposalRecordV1, dependency_delete, dependency_key,
    dependency_update, payload_delete, payload_update, proposal_update,
};
use coord_consensus::{CommandRecord, PayloadRecordV1, Phase};
use coord_core::effect::{BarrierId, BootId, PersistBatch, StoreUpdate};
use coord_core::outbox::BarrierAllocator;
use coord_journal_api::stream::ShardId;
use coord_storage::intake::{self, IntakeIndex, IntakeOrder, ProtocolKey};
use coord_storage::journaled::{
    DomainStatus, JournalLimits, JournaledStore, Submission, TransitionKind,
};
use coord_storage::protocol::{RecoveredProtocol, catch_up_entry, read_protocol};
use coord_storage::views::ViewBudget;
use coord_storage::{GroupLimits, StoreWorker};
use coord_store_api::engine::{LocalEngine, OrderedRead, ScanRequest, SnapshotSource, WriteTxn};
use coord_store_api::registry::{Collection, meta_fields};
use coord_store_testkit::journal::ModelJournal;
use coord_store_testkit::model::{CommitScript, ModelEngine};
use coord_types::identity::Digest32;
use coord_types::ids::*;
use coord_types::{CommandId, RetryKey};

const CLUSTER: ClusterId = ClusterId([0x11; 16]);
const REPLICA: ReplicaId = ReplicaId([0x22; 16]);
const A: DomainId = DomainId([0xa1; 16]);

fn inc() -> ReplicaIncarnation {
    ReplicaIncarnation::new(1).unwrap()
}

fn epoch() -> ConfigurationEpoch {
    ConfigurationEpoch::new(1).unwrap()
}

fn ballot(number: u64) -> Ballot {
    Ballot {
        epoch: epoch(),
        number,
        leader: REPLICA,
    }
}

fn shard() -> ShardId {
    ShardId::new(0).unwrap()
}

fn boot(n: u8) -> BootId {
    BootId([n; 16])
}

fn retry_key(sequence: u64) -> RetryKey {
    RetryKey {
        cluster_id: CLUSTER,
        domain_id: A,
        session_id: SessionId([0x5; 16]),
        client_instance_id: ClientInstanceId([0x6; 16]),
        request_sequence: RequestSequence::new(sequence).unwrap(),
    }
}

fn request_of(sequence: u64) -> coord_types::logical_v1::LogicalRequest {
    use coord_types::logical_v1::{CanonicalOperation, LogicalRequest, PutOp};
    let mut r = LogicalRequest::new(
        NamespaceId([5; 16]),
        CanonicalOperation::Put(PutOp {
            key: sequence.to_be_bytes().to_vec(),
            value: vec![7],
            lease: None,
            prev_kv: false,
        }),
    );
    r.canonicalize();
    r
}

fn payload_of(sequence: u64) -> PayloadRecordV1 {
    PayloadRecordV1 {
        ack_through: 0,
        retry_key: retry_key(sequence),
        logical: postcard::to_allocvec(&request_of(sequence)).unwrap(),
        admission: None,
    }
}

/// The identity a recovering reader derives for [`payload_of`].
fn command_of(sequence: u64) -> CommandId {
    CommandId::derive(&retry_key(sequence), &request_of(sequence)).unwrap()
}

fn record(phase: Phase, deps: &[CommandId]) -> CommandRecord {
    CommandRecord {
        phase,
        deps: deps.to_vec(),
        keys: vec![b"k".to_vec()],
        payload: Some(Digest32([0x5; 32])),
        paths: Vec::new(),
        path: Digest32([0x6; 32]),
        synced_seq: None,
    }
}

fn executed(command: &CommandId, position: u64) -> StoreUpdate {
    coord_storage::retry::executed_update(
        command,
        ExecutionPosition::new(position).unwrap(),
        None,
        Digest32([position as u8; 32]),
    )
    .unwrap()
}

/// Every row of `collection` in `view`, in key order.
fn rows<V: OrderedRead>(view: &V, collection: Collection) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    let mut resume = None;
    loop {
        let mut request = ScanRequest::all(1024, 1 << 20);
        request.resume_after = resume.clone();
        let page = view.scan_page(collection.id(), &request).unwrap();
        for row in &page.rows {
            out.push((row.key.clone(), row.value.clone()));
        }
        match page.rows.last() {
            Some(last) if !page.exhausted => resume = Some(last.key.clone()),
            _ => break,
        }
    }
    out
}

/// The dependency and proposal rows a projection holds, as (key, value).
fn intake_rows<R: SnapshotSource>(raw: &R) -> Vec<(Vec<u8>, Vec<u8>)> {
    let view = raw.snapshot().unwrap();
    rows(&view, Collection::ProtocolV1)
        .into_iter()
        .filter(|(k, _)| matches!(intake::parse(k), ProtocolKey::Intake { .. }))
        .collect()
}

/// The store, journaled or not, under one interface for the scenarios.
enum Store {
    Journaled {
        store: Option<JournaledStore<ModelJournal, ModelEngine>>,
        boots: u8,
    },
    Worker {
        worker: Option<StoreWorker<ModelEngine>>,
        boots: u8,
        /// The worker holds its engine; a model engine's reader shares its
        /// state.
        raw: Raw,
    },
}

type Raw = <ModelEngine as LocalEngine>::Reader;

/// A row as (key, value).
type RawRow = (Vec<u8>, Vec<u8>);

impl Store {
    fn journaled() -> Self {
        let mut store = JournaledStore::open(
            ModelJournal::new(),
            CLUSTER,
            REPLICA,
            inc(),
            boot(1),
            JournalLimits::default(),
        )
        .unwrap();
        store.attach(A, shard(), ModelEngine::new()).unwrap();
        Store::Journaled {
            store: Some(store),
            boots: 1,
        }
    }

    fn worker() -> Self {
        let engine = ModelEngine::new();
        let raw = engine.reader();
        Store::Worker {
            worker: Some(
                StoreWorker::open(engine, boot(1), inc(), GroupLimits::default()).unwrap(),
            ),
            boots: 1,
            raw,
        }
    }

    fn barrier(&self) -> BarrierId {
        // A barrier is unique within a boot; the allocator is per boot.
        thread_local! {
            static ALLOC: std::cell::RefCell<BTreeMap<u8, BarrierAllocator>> =
                const { std::cell::RefCell::new(BTreeMap::new()) };
        }
        let boots = match self {
            Store::Journaled { boots, .. } | Store::Worker { boots, .. } => *boots,
        };
        ALLOC.with(|a| {
            a.borrow_mut()
                .entry(boots)
                .or_insert_with(|| BarrierAllocator::new(inc(), boot(boots)))
                .allocate()
        })
    }

    /// Submit a protocol record, appended to the journal (not
    /// materialized) under the journaled store, and lowered under the
    /// worker.
    fn protocol(&mut self, updates: Vec<StoreUpdate>) {
        let barrier = self.barrier();
        match self {
            Store::Journaled { store, .. } => {
                let store = store.as_mut().unwrap();
                store
                    .submit(Submission {
                        domain: A,
                        ballot: ballot(1),
                        kind: TransitionKind::Protocol,
                        batch: PersistBatch {
                            barrier,
                            base: None,
                            updates,
                        },
                    })
                    .unwrap();
                store.append_pending().unwrap();
            }
            Store::Worker { worker, .. } => {
                let worker = worker.as_mut().unwrap();
                worker
                    .submit(PersistBatch {
                        barrier,
                        base: None,
                        updates,
                    })
                    .unwrap();
                worker.flush().unwrap();
            }
        }
    }

    /// Submit an application record executing at `position`.
    fn application(&mut self, position: u64, updates: Vec<StoreUpdate>) {
        let barrier = self.barrier();
        match self {
            Store::Journaled { store, .. } => {
                let store = store.as_mut().unwrap();
                let base = store.application_base(A).unwrap();
                store
                    .submit(Submission {
                        domain: A,
                        ballot: Ballot {
                            epoch: base.configuration,
                            number: 1,
                            leader: REPLICA,
                        },
                        kind: TransitionKind::Application {
                            position: ExecutionPosition::new(position).unwrap(),
                            revision: None,
                            result_digest: Digest32([position as u8; 32]),
                        },
                        batch: PersistBatch {
                            barrier,
                            base: Some(base),
                            updates,
                        },
                    })
                    .unwrap();
                store.append_pending().unwrap();
            }
            Store::Worker { worker, .. } => {
                let worker = worker.as_mut().unwrap();
                let base = worker.application_base();
                assert_eq!(base.execution_position.get() + 1, position);
                worker
                    .submit(PersistBatch {
                        barrier,
                        base: Some(base),
                        updates,
                    })
                    .unwrap();
                worker.flush().unwrap();
            }
        }
    }

    fn materialize(&mut self) {
        if let Store::Journaled { store, .. } = self {
            store.as_mut().unwrap().materialize().unwrap();
        }
    }

    /// Stop and open again: the index is rebuilt from the rows, and the
    /// journaled store replays what it had not materialized.
    fn restart(&mut self) {
        match self {
            Store::Journaled { store, boots } => {
                let (journal, mut engines) = store.take().unwrap().into_parts();
                *boots += 1;
                let mut reopened = JournaledStore::open(
                    journal,
                    CLUSTER,
                    REPLICA,
                    inc(),
                    boot(*boots),
                    JournalLimits::default(),
                )
                .unwrap();
                let (_, engine) = engines.pop().unwrap();
                reopened.attach(A, shard(), engine).unwrap();
                *store = Some(reopened);
            }
            Store::Worker { worker, boots, .. } => {
                let engine = worker.take().unwrap().into_engine();
                *boots += 1;
                *worker = Some(
                    StoreWorker::open(engine, boot(*boots), inc(), GroupLimits::default()).unwrap(),
                );
            }
        }
    }

    /// What boot reads: the recovery cut under the journaled store, the
    /// snapshot under the worker.
    fn recovered(&self) -> RecoveredProtocol {
        match self {
            Store::Journaled { store, .. } => {
                let cut = store.as_ref().unwrap().recovery_cut(A).unwrap();
                read_protocol(&cut, epoch(), ViewBudget::default()).unwrap()
            }
            Store::Worker { worker, .. } => {
                let gated = worker.as_ref().unwrap().reader().snapshot().unwrap();
                read_protocol(gated.view(), epoch(), ViewBudget::default()).unwrap()
            }
        }
    }

    /// A raw reader of the projection, under no gate.
    fn raw(&self) -> Raw {
        match self {
            Store::Journaled { store, .. } => {
                store.as_ref().unwrap().projection(A).unwrap().reader()
            }
            Store::Worker { raw, .. } => raw.clone(),
        }
    }

    fn reader(&self) -> coord_storage::GatedReader<<ModelEngine as LocalEngine>::Reader> {
        match self {
            Store::Journaled { store, .. } => store.as_ref().unwrap().reader(A).unwrap(),
            Store::Worker { worker, .. } => worker.as_ref().unwrap().reader(),
        }
    }

    /// The index as published equals the one an open would build from the
    /// rows the projection holds.
    fn assert_index_is_the_rows(&self, what: &str) {
        let reader = self.reader();
        let published = reader.intake_index();
        let snapshot = self.raw().snapshot().unwrap();
        let rebuilt = IntakeIndex::build(&snapshot, published.stamp()).unwrap();
        assert_eq!(published, rebuilt, "{what}: the index is not the rows'");
    }
}

/// What the rows mean, by command: the model a store keyed by command
/// would hold.
#[derive(Default)]
struct Model {
    deps: BTreeMap<CommandId, CommandRecord>,
    payloads: BTreeMap<CommandId, PayloadRecordV1>,
    executed: BTreeMap<CommandId, u64>,
    position: u64,
}

impl Model {
    fn expected(&self) -> RecoveredProtocol {
        let records: Vec<(CommandId, CommandRecord)> =
            self.deps.iter().map(|(c, r)| (*c, r.clone())).collect();
        let payloads = records
            .iter()
            .filter(|(_, r)| r.payload.is_some())
            .map(|(c, _)| (*c, self.payloads[c].clone()))
            .collect();
        let mut executed: Vec<(CommandId, ExecutionPosition)> = self
            .deps
            .keys()
            .filter_map(|c| {
                self.executed
                    .get(c)
                    .map(|p| (*c, ExecutionPosition::new(*p).unwrap()))
            })
            .collect();
        executed.sort_by_key(|(_, p)| *p);
        let mut history: Vec<(u64, CommandId)> = self
            .executed
            .iter()
            .filter(|(c, _)| !self.deps.contains_key(c))
            .map(|(c, p)| (*p, *c))
            .collect();
        history.sort();
        RecoveredProtocol {
            records,
            payloads,
            executed,
            history: history.into_iter().map(|(_, c)| c).collect(),
            frontier: ExecutionPosition::new(self.position).unwrap(),
            ..RecoveredProtocol::default()
        }
    }
}

/// A small deterministic generator, so a failure names its step.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, below: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((self.0 >> 33) as usize) % below
    }
}

/// Drive `store` through the rows a voter writes under load, and check
/// after every step that what boot reads is what the rows meant by
/// command.
fn under_load(mut store: Store, seed: u64, steps: usize) {
    let mut model = Model::default();
    let mut rng = Lcg(seed);
    let mut next_command = 1u64;
    // Commands the machine holds: pending (a row, not executed), released
    // by a Sync (no rows), executed.
    let mut pending: Vec<u64> = Vec::new();
    let mut released: Vec<u64> = Vec::new();
    let mut done: Vec<u64> = Vec::new();
    let mut seen = BTreeSet::new();
    for step in 0..steps {
        let what = format!("seed {seed}, step {step}");
        match rng.next(10) {
            // Intake: a vote, an adoption or the proposal batch brings the
            // dependency row and the payload, several commands a record.
            0 | 1 => {
                let mut updates = Vec::new();
                for _ in 0..1 + rng.next(4) {
                    let n = next_command;
                    next_command += 1;
                    let c = command_of(n);
                    let r = record(Phase::PreAccept, &[]);
                    updates.push(dependency_update(epoch(), &c, &r).unwrap());
                    updates.push(payload_update(&c, &payload_of(n)).unwrap());
                    model.deps.insert(c, r);
                    model.payloads.insert(c, payload_of(n));
                    pending.push(n);
                }
                store.protocol(updates);
            }
            // ACCEPT in a later batch rewrites the row, of a pending or an
            // executed command.
            2 | 3 => {
                let held: Vec<u64> = pending
                    .iter()
                    .chain(done.iter())
                    .copied()
                    .filter(|n| model.deps.contains_key(&command_of(*n)))
                    .collect();
                if held.is_empty() {
                    continue;
                }
                let n = held[rng.next(held.len())];
                let c = command_of(n);
                let deps: Vec<CommandId> = (0..rng.next(3))
                    .map(|_| command_of(1 + rng.next(next_command as usize) as u64))
                    .collect();
                let r = record(Phase::Accept, &deps);
                model.deps.insert(c, r.clone());
                store.protocol(vec![dependency_update(epoch(), &c, &r).unwrap()]);
            }
            // Execution of a pending command.
            4 => {
                if pending.is_empty() {
                    continue;
                }
                let n = pending.remove(rng.next(pending.len()));
                let c = command_of(n);
                model.position += 1;
                model.executed.insert(c, model.position);
                done.push(n);
                store.application(model.position, vec![executed(&c, model.position)]);
            }
            // A Sync releases a pending command (task-d24): its rows go.
            5 => {
                if pending.is_empty() {
                    continue;
                }
                let n = pending.remove(rng.next(pending.len()));
                let c = command_of(n);
                model.deps.remove(&c);
                model.payloads.remove(&c);
                released.push(n);
                store.protocol(vec![dependency_delete(epoch(), &c), payload_delete(&c)]);
            }
            // The next leader proposes a released command again: its row
            // comes back under a new intake key.
            6 => {
                if released.is_empty() {
                    continue;
                }
                let n = released.remove(rng.next(released.len()));
                let c = command_of(n);
                let r = record(Phase::PreAccept, &[]);
                model.deps.insert(c, r.clone());
                model.payloads.insert(c, payload_of(n));
                pending.push(n);
                store.protocol(vec![
                    payload_update(&c, &payload_of(n)).unwrap(),
                    dependency_update(epoch(), &c, &r).unwrap(),
                ]);
            }
            // Catch-up pulls a command (task-d08): its payload, and either
            // the donor's decided record or the deletion of the row this
            // replica had; it executes in the same turn.
            7 => {
                let (n, fresh) = match (rng.next(3), pending.is_empty()) {
                    (0, false) => (pending.remove(rng.next(pending.len())), false),
                    _ => {
                        let n = next_command;
                        next_command += 1;
                        (n, true)
                    }
                };
                let c = command_of(n);
                let mut updates = vec![payload_update(&c, &payload_of(n)).unwrap()];
                model.payloads.insert(c, payload_of(n));
                if rng.next(2) == 0 {
                    let r = record(Phase::Commit, &[]);
                    updates.push(dependency_update(epoch(), &c, &r).unwrap());
                    model.deps.insert(c, r);
                } else {
                    updates.push(dependency_delete(epoch(), &c));
                    model.deps.remove(&c);
                }
                let _ = fresh;
                store.protocol(updates);
                model.position += 1;
                model.executed.insert(c, model.position);
                done.push(n);
                store.application(model.position, vec![executed(&c, model.position)]);
            }
            8 => store.materialize(),
            _ => {
                if rng.next(3) == 0 {
                    store.restart();
                } else {
                    store.materialize();
                }
            }
        }
        let recovered = store.recovered();
        // The cut reads the frontier the projection has materialized; the
        // last restart below reads the whole of it.
        let mut expected = model.expected();
        expected.frontier = recovered.frontier;
        assert_eq!(recovered, expected, "{what}: boot reads another machine");
        seen.insert(recovered.records.len());
    }
    store.materialize();
    store.restart();
    assert_eq!(
        store.recovered(),
        model.expected(),
        "after the last restart"
    );
    // One row a command: a rewrite stays in place, and a released
    // command's row is gone before its new one comes. An executed
    // command's moved to `decided_v1` at its execution (task-d113, step 2,
    // the bound), so the intake part holds only the others.
    let dependency_rows = intake_rows(&store.raw())
        .into_iter()
        .filter(|(k, _)| k.get(8) == Some(&DEPENDENCY_TAG))
        .count();
    let executed_records = model
        .deps
        .keys()
        .filter(|c| model.executed.contains_key(c))
        .count();
    assert_eq!(dependency_rows, model.deps.len() - executed_records);
    let decided = rows(&store.raw().snapshot().unwrap(), Collection::DecidedV1).len();
    assert_eq!(decided, executed_records, "each executed record moved once");
    assert!(
        executed_records > 0,
        "the run executed commands with records"
    );
    store.assert_index_is_the_rows("after the run");
    assert!(seen.len() > 3, "the run held rows");
}

#[test]
fn boot_restores_the_same_machine_from_a_store_written_under_load() {
    for seed in 1..=6 {
        under_load(Store::journaled(), seed, 250);
    }
}

#[test]
fn boot_restores_the_same_machine_from_a_worker_store_written_under_load() {
    for seed in 1..=3 {
        under_load(Store::worker(), seed, 200);
    }
}

/// A journaled store with two commands taken in and materialized, one
/// record each, after the attach's two lifecycle records.
fn two_taken_in() -> (Store, CommandId, CommandId) {
    let mut store = Store::journaled();
    let (c1, c2) = (command_of(1), command_of(2));
    for (n, c) in [(1, c1), (2, c2)] {
        store.protocol(vec![
            dependency_update(epoch(), &c, &record(Phase::PreAccept, &[])).unwrap(),
            payload_update(&c, &payload_of(n)).unwrap(),
        ]);
    }
    store.materialize();
    (store, c1, c2)
}

#[test]
fn an_accept_rewrite_stays_in_place_under_its_intake_key() {
    let (mut store, c1, c2) = two_taken_in();
    let before = intake_rows(&store.raw());
    // Records 3 and 4 took the two commands in, each row first in its
    // record.
    let at = |seq| IntakeOrder { seq, ordinal: 0 };
    let e = epoch().to_be_bytes();
    assert_eq!(
        before.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
        vec![
            intake::intake_key(e, DEPENDENCY_TAG, at(3), &c1),
            intake::intake_key(e, DEPENDENCY_TAG, at(4), &c2),
        ]
    );
    // ACCEPT arrives in a later batch, behind another command's intake.
    let accepted = record(Phase::Accept, &[c2]);
    store.protocol(vec![
        dependency_update(epoch(), &command_of(3), &record(Phase::PreAccept, &[])).unwrap(),
        payload_update(&command_of(3), &payload_of(3)).unwrap(),
        dependency_update(epoch(), &c1, &accepted).unwrap(),
    ]);
    store.materialize();
    let after = intake_rows(&store.raw());
    let c1_rows: Vec<_> = after
        .iter()
        .filter(|(k, _)| matches!(intake::parse(k), ProtocolKey::Intake { command, .. } if command == c1))
        .collect();
    assert_eq!(c1_rows.len(), 1, "one row, no second under a new key");
    assert_eq!(
        c1_rows[0].0,
        intake::intake_key(e, DEPENDENCY_TAG, at(3), &c1)
    );
    assert_eq!(
        coord_consensus::rows::decode_dependency(&c1_rows[0].1).unwrap(),
        accepted,
        "with the rewrite"
    );
    // The new command took the next intake key, the right-hand end.
    assert_eq!(
        after.last().unwrap().0,
        intake::intake_key(e, DEPENDENCY_TAG, at(5), &command_of(3))
    );
    store.assert_index_is_the_rows("after the rewrite");
}

#[test]
fn the_leaders_proposal_row_is_keyed_by_intake_order_too() {
    let mut store = Store::journaled();
    let c = command_of(1);
    let proposal = ProposalRecordV1 {
        ballot: ballot(1),
        seqnum: 4,
        deps: Vec::new(),
        path: Digest32([0x7; 32]),
    };
    store.protocol(vec![
        payload_update(&c, &payload_of(1)).unwrap(),
        dependency_update(epoch(), &c, &record(Phase::PreAccept, &[])).unwrap(),
        proposal_update(epoch(), &c, &proposal).unwrap(),
    ]);
    store.materialize();
    let e = epoch().to_be_bytes();
    let keys: Vec<_> = intake_rows(&store.raw())
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert_eq!(
        keys,
        vec![
            intake::intake_key(e, DEPENDENCY_TAG, IntakeOrder { seq: 3, ordinal: 1 }, &c),
            intake::intake_key(e, PROPOSAL_TAG, IntakeOrder { seq: 3, ordinal: 2 }, &c),
        ]
    );
    // Nothing is left under the key the machine names it by.
    let raw = store.raw().snapshot().unwrap();
    assert!(
        raw.get(Collection::ProtocolV1.id(), &dependency_key(epoch(), &c))
            .unwrap()
            .is_none()
    );
}

#[test]
fn a_recovery_cut_over_an_unmaterialized_rewrite_reads_the_row_once_with_the_rewrite() {
    let (mut store, c1, c2) = two_taken_in();
    let accepted = record(Phase::Accept, &[c2]);
    // Durable in the journal, not materialized.
    store.protocol(vec![dependency_update(epoch(), &c1, &accepted).unwrap()]);
    let Store::Journaled { store: s, .. } = &store else {
        unreachable!()
    };
    let s = s.as_ref().unwrap();
    assert!(s.frontiers(A).unwrap().materialized() < s.frontiers(A).unwrap().durable());
    let summary = store.recovered();
    let rows_of_c1: Vec<_> = summary.records.iter().filter(|(c, _)| *c == c1).collect();
    assert_eq!(rows_of_c1.len(), 1, "read once: {:?}", summary.records);
    assert_eq!(rows_of_c1[0].1, accepted, "with the rewrite");
    assert_eq!(summary.records.len(), 2);
    // Materialized, the projection reads what the cut anticipated.
    store.materialize();
    assert_eq!(store.recovered(), summary);
}

#[test]
fn a_recovery_cut_over_an_unmaterialized_dependency_delete_does_not_read_the_row() {
    let (mut store, c1, c2) = two_taken_in();
    // A command pulled by catch-up without the donor's decided record
    // (task-d08): the row this replica had is deleted.
    store.protocol(vec![
        payload_update(&c1, &payload_of(1)).unwrap(),
        dependency_delete(epoch(), &c1),
    ]);
    let summary = store.recovered();
    assert_eq!(
        summary.records.iter().map(|(c, _)| *c).collect::<Vec<_>>(),
        vec![c2],
        "the deleted row is not read"
    );
    store.materialize();
    assert_eq!(store.recovered(), summary);
    assert_eq!(intake_rows(&store.raw()).len(), 1);
}

#[test]
fn a_commit_whose_outcome_is_not_known_settled_either_way_leaves_the_index_as_the_store_has_it() {
    for applied in [true, false] {
        let what = format!("applied {applied}");
        let (mut store, c1, c2) = two_taken_in();
        // One record that rewrites c1, deletes c2 and takes c3 in.
        store.protocol(vec![
            dependency_update(epoch(), &c1, &record(Phase::Accept, &[])).unwrap(),
            dependency_delete(epoch(), &c2),
            payload_delete(&c2),
            dependency_update(epoch(), &command_of(3), &record(Phase::PreAccept, &[])).unwrap(),
            payload_update(&command_of(3), &payload_of(3)).unwrap(),
        ]);
        {
            let Store::Journaled { store: s, .. } = &mut store else {
                unreachable!()
            };
            let s = s.as_mut().unwrap();
            s.projection(A)
                .unwrap()
                .script_commit(CommitScript::Indeterminate { applied });
            let report = s.materialize().unwrap();
            assert!(report.indeterminate, "{what}");
            assert_eq!(
                s.status(A),
                Some(DomainStatus::MaterializationUncertain),
                "{what}"
            );
            // Not yet known: the index is the one before the commit.
            assert_eq!(
                s.reader(A).unwrap().intake_index().len(),
                4,
                "{what}: nothing published while the outcome is not known \
                 (two dependency rows and two pending payloads)"
            );
            s.reconcile(A).unwrap();
            assert_eq!(s.status(A), Some(DomainStatus::Ready), "{what}");
        }
        store.assert_index_is_the_rows(&what);
        // And the store's own next writes find their rows through it.
        store.protocol(vec![
            dependency_update(epoch(), &command_of(3), &record(Phase::Accept, &[])).unwrap(),
        ]);
        store.materialize();
        store.assert_index_is_the_rows(&what);
        let rows = intake_rows(&store.raw());
        assert_eq!(rows.len(), 2, "{what}: {rows:?}");
    }
}

#[test]
fn a_worker_group_whose_outcome_is_not_known_settled_either_way_leaves_the_index_as_the_store_has_it()
 {
    for applied in [true, false] {
        let what = format!("applied {applied}");
        let engine = ModelEngine::new();
        let raw = engine.reader();
        let mut worker = StoreWorker::open(engine, boot(1), inc(), GroupLimits::default()).unwrap();
        let mut alloc = BarrierAllocator::new(inc(), boot(1));
        let c1 = command_of(1);
        worker
            .submit(PersistBatch {
                barrier: alloc.allocate(),
                base: None,
                updates: vec![
                    dependency_update(epoch(), &c1, &record(Phase::PreAccept, &[])).unwrap(),
                ],
            })
            .unwrap();
        worker.flush().unwrap();
        worker
            .submit(PersistBatch {
                barrier: alloc.allocate(),
                base: None,
                updates: vec![
                    dependency_delete(epoch(), &c1),
                    dependency_update(epoch(), &command_of(2), &record(Phase::PreAccept, &[]))
                        .unwrap(),
                ],
            })
            .unwrap();
        worker
            .engine_mut()
            .script_commit(CommitScript::Indeterminate { applied });
        assert!(worker.flush().unwrap().indeterminate, "{what}");
        worker.reconcile().unwrap();
        let published = worker.reader().intake_index();
        let rebuilt = IntakeIndex::build(&raw.snapshot().unwrap(), published.stamp()).unwrap();
        assert_eq!(published, rebuilt, "{what}");
        assert_eq!(published.len(), 1, "{what}");
    }
}

/// The intake rows of a journaled store that took the load below in, all
/// of it materialized.
fn loaded(materialize_every: usize) -> (Store, Vec<RawRow>) {
    let mut store = Store::journaled();
    let mut rng = Lcg(7);
    for i in 0..60u64 {
        let c = command_of(1 + i);
        let mut updates = vec![
            payload_update(&c, &payload_of(1 + i)).unwrap(),
            dependency_update(epoch(), &c, &record(Phase::PreAccept, &[])).unwrap(),
        ];
        if i > 0 {
            let earlier = command_of(1 + rng.next(i as usize) as u64);
            match rng.next(3) {
                0 => updates.push(
                    dependency_update(epoch(), &earlier, &record(Phase::Accept, &[c])).unwrap(),
                ),
                1 => updates.push(dependency_delete(epoch(), &earlier)),
                _ => updates.push(
                    dependency_update(epoch(), &earlier, &record(Phase::Commit, &[])).unwrap(),
                ),
            }
        }
        store.protocol(updates);
        if materialize_every != 0 && (i as usize).is_multiple_of(materialize_every) {
            store.materialize();
        }
    }
    store.materialize();
    let rows = intake_rows(&store.raw());
    (store, rows)
}

#[test]
fn a_replay_of_the_journal_suffix_writes_the_same_keys_as_the_first_lowering() {
    let (_, first) = loaded(1);
    // The same records, the projection behind by a suffix that the attach
    // replays: materialized in other commits, and after a restart.
    for every in [0, 7] {
        let mut store = Store::journaled();
        let mut rng = Lcg(7);
        for i in 0..60u64 {
            let c = command_of(1 + i);
            let mut updates = vec![
                payload_update(&c, &payload_of(1 + i)).unwrap(),
                dependency_update(epoch(), &c, &record(Phase::PreAccept, &[])).unwrap(),
            ];
            if i > 0 {
                let earlier = command_of(1 + rng.next(i as usize) as u64);
                match rng.next(3) {
                    0 => updates.push(
                        dependency_update(epoch(), &earlier, &record(Phase::Accept, &[c])).unwrap(),
                    ),
                    1 => updates.push(dependency_delete(epoch(), &earlier)),
                    _ => updates.push(
                        dependency_update(epoch(), &earlier, &record(Phase::Commit, &[])).unwrap(),
                    ),
                }
            }
            store.protocol(updates);
            if every != 0 && (i as usize).is_multiple_of(every) {
                store.materialize();
            }
        }
        // The suffix is replayed by the attach.
        store.restart();
        let replayed = intake_rows(&store.raw());
        assert_eq!(replayed, first, "materialized every {every}: other keys");
        store.assert_index_is_the_rows("after the replay");
    }
    // And the whole journal replayed into an empty projection.
    let (store, _) = loaded(1);
    let Store::Journaled { store: s, .. } = store else {
        unreachable!()
    };
    let (journal, _) = s.unwrap().into_parts();
    let mut fresh = JournaledStore::open(
        journal,
        CLUSTER,
        REPLICA,
        inc(),
        boot(9),
        JournalLimits::default(),
    )
    .unwrap();
    fresh.attach(A, shard(), ModelEngine::new()).unwrap();
    assert_eq!(intake_rows(&fresh.projection(A).unwrap().reader()), first);
}

#[test]
fn a_store_of_the_other_layout_is_refused_with_the_reason() {
    // Recorded as another format: 1 (dependency rows by command), 2
    // (executed rows by command, before task-d113's step 2) or 3 (versions
    // in history rows, before task-d105).
    for format in [1u32, 2, 3] {
        let mut engine = ModelEngine::new();
        let mut tx = engine.begin_write().unwrap();
        tx.put(
            Collection::MetaV1.id(),
            meta_fields::FORMAT_VERSION,
            &format.to_be_bytes(),
        )
        .unwrap();
        tx.commit_durable().unwrap();
        let refused = StoreWorker::open(engine, boot(1), inc(), GroupLimits::default())
            .err()
            .expect("a store of another format is refused");
        assert!(
            refused
                .diagnostic
                .contains(&format!("store format {format} is not this build's (4)")),
            "{refused:?}"
        );
    }
    // Recording nothing, with a dependency row keyed by command: the
    // format before this one, under either store.
    let old = || {
        let mut engine = ModelEngine::new();
        let mut tx = engine.begin_write().unwrap();
        tx.put(
            Collection::ProtocolV1.id(),
            &dependency_key(epoch(), &command_of(1)),
            &coord_consensus::rows::encode_dependency(&record(Phase::Accept, &[])).unwrap(),
        )
        .unwrap();
        tx.commit_durable().unwrap();
        engine
    };
    let refused = StoreWorker::open(old(), boot(1), inc(), GroupLimits::default())
        .err()
        .expect("a store of the layout before is refused");
    assert!(
        refused
            .diagnostic
            .contains("dependency rows keyed by command"),
        "{refused:?}"
    );
    let mut store = JournaledStore::open(
        ModelJournal::new(),
        CLUSTER,
        REPLICA,
        inc(),
        boot(1),
        JournalLimits::default(),
    )
    .unwrap();
    let refused = store
        .attach(A, shard(), old())
        .expect_err("refused at attach");
    assert!(
        refused
            .to_string()
            .contains("dependency rows keyed by command"),
        "{refused}"
    );
    // Recording nothing, with an executed row keyed by command: the format
    // before task-d113's step 2.
    let executed = || {
        let mut engine = ModelEngine::new();
        let mut tx = engine.begin_write().unwrap();
        tx.put(
            Collection::ExecutedV1.id(),
            &coord_storage::codecs::executed_key(&command_of(1)),
            &coord_storage::codecs::encode_executed(&coord_storage::codecs::ExecutedRecordV1 {
                position: coord_types::ids::ExecutionPosition::new(1).unwrap(),
                revision: None,
                result_digest: coord_types::identity::Digest32([1; 32]),
            })
            .unwrap(),
        )
        .unwrap();
        tx.commit_durable().unwrap();
        engine
    };
    let refused = StoreWorker::open(executed(), boot(1), inc(), GroupLimits::default())
        .err()
        .expect("a store of format 2 is refused");
    assert!(refused.diagnostic.contains("store format 2"), "{refused:?}");
    // This build's format, recorded, opens.
    let mut engine = ModelEngine::new();
    let mut tx = engine.begin_write().unwrap();
    tx.put(
        Collection::MetaV1.id(),
        meta_fields::FORMAT_VERSION,
        &coord_store_api::registry::STORE_FORMAT_VERSION.to_be_bytes(),
    )
    .unwrap();
    tx.commit_durable().unwrap();
    StoreWorker::open(engine, boot(1), inc(), GroupLimits::default()).unwrap();
}

#[test]
fn a_redb_generation_records_the_format_it_was_initialized_under() {
    let dir = tempfile::tempdir().unwrap();
    let identity = coord_storage_redb::lifecycle::StoreIdentity {
        cluster_id: CLUSTER,
        domain_id: A,
        replica_id: REPLICA,
        incarnation: inc(),
    };
    let mut generation = coord_storage_redb::lifecycle::Generation::create(
        dir.path(),
        identity,
        coord_storage_redb::lifecycle::OpenOptions {
            cache_bytes: 4 << 20,
        },
    )
    .unwrap();
    let engine = generation.engine();
    let view = engine.reader().snapshot().unwrap();
    assert_eq!(
        view.get(Collection::MetaV1.id(), meta_fields::FORMAT_VERSION)
            .unwrap(),
        Some(
            coord_store_api::registry::STORE_FORMAT_VERSION
                .to_be_bytes()
                .to_vec()
        )
    );
}

#[test]
fn catch_up_finds_an_executed_commands_dependency_row_through_the_index() {
    let (mut store, c1, c2) = two_taken_in();
    let accepted = record(Phase::Accept, &[c2]);
    store.protocol(vec![dependency_update(epoch(), &c1, &accepted).unwrap()]);
    store.application(1, vec![executed(&c1, 1)]);
    store.materialize();
    let gated = store.reader().snapshot().unwrap();
    let entry = catch_up_entry(&gated, epoch(), &c1)
        .unwrap()
        .expect("executed and held");
    assert_eq!(entry.decided, Some(accepted), "the row, rewritten in place");
    // A command not executed is not looked up (rule (c)).
    assert_eq!(catch_up_entry(&gated, epoch(), &c2).unwrap(), None);
    // Nor is a row under another epoch.
    let other = ConfigurationEpoch::new(2).unwrap();
    assert_eq!(
        catch_up_entry(&gated, other, &c1).unwrap().unwrap().decided,
        None
    );
}

/// Two executed commands whose identities share the index's 8-byte prefix
/// (the lead on #237): catch-up finds each one's own dependency row,
/// whichever of them is first under the prefix. The index holds both
/// entries, and the full key, which carries the command, settles whose
/// each is. Tried with only the first candidate, the second command's
/// lookup reads a key no row is under and serves it with no decided
/// record.
#[test]
fn catch_up_finds_each_of_two_executed_commands_that_share_a_prefix() {
    let mut store = Store::journaled();
    let sharing = |tail: u8| {
        let mut bytes = [0x3c; 32];
        bytes[8..].fill(tail);
        CommandId(Digest32(bytes))
    };
    let (first, second) = (sharing(1), sharing(2));
    let first_row = record(Phase::Commit, &[]);
    let second_row = record(Phase::Commit, &[first]);
    for (n, command, row) in [(1, first, &first_row), (2, second, &second_row)] {
        store.protocol(vec![
            dependency_update(epoch(), &command, row).unwrap(),
            payload_update(&command, &payload_of(n)).unwrap(),
        ]);
    }
    store.application(1, vec![executed(&first, 1)]);
    store.application(2, vec![executed(&second, 2)]);
    store.materialize();
    let gated = store.reader().snapshot().unwrap();
    for (command, row) in [(first, first_row), (second, second_row)] {
        let entry = catch_up_entry(&gated, epoch(), &command)
            .unwrap()
            .expect("executed and held");
        assert_eq!(entry.command, command);
        assert_eq!(entry.decided, Some(row), "{command:?}'s own row");
    }
}

/// The first of two executed commands that share the index's prefix is
/// deleted (review on #282): the executed part moves the second's position
/// up, so the second still reads as executed. Had the prefix gone with the
/// first, catch-up would not find the second, and a later payload put of
/// it would be written pending, against rule (a).
#[test]
fn deleting_the_first_of_two_executed_commands_that_share_a_prefix_keeps_the_second() {
    for mut store in [Store::journaled(), Store::worker()] {
        let sharing = |tail: u8| {
            let mut bytes = [0x3c; 32];
            bytes[8..].fill(tail);
            CommandId(Digest32(bytes))
        };
        let (first, second) = (sharing(1), sharing(2));
        let first_row = record(Phase::Commit, &[]);
        let second_row = record(Phase::Commit, &[first]);
        for (n, command, row) in [(1, first, &first_row), (2, second, &second_row)] {
            store.protocol(vec![
                dependency_update(epoch(), &command, row).unwrap(),
                payload_update(&command, &payload_of(n)).unwrap(),
            ]);
        }
        store.application(1, vec![executed(&first, 1)]);
        store.application(2, vec![executed(&second, 2)]);
        // Published before the delete, so the delete meets both entries in
        // the index, the first's under the prefix's own.
        store.materialize();
        store.application(
            3,
            vec![StoreUpdate {
                value: None,
                ..executed(&first, 1)
            }],
        );
        store.materialize();
        store.assert_index_is_the_rows("after the delete");
        let gated = store.reader().snapshot().unwrap();
        assert_eq!(
            catch_up_entry(&gated, epoch(), &first).unwrap(),
            None,
            "the first no longer executed"
        );
        let entry = catch_up_entry(&gated, epoch(), &second)
            .unwrap()
            .expect("the second still executed");
        assert_eq!(entry.command, second);
        assert_eq!(entry.decided, Some(second_row));
        drop(gated);
        // Rule (a): a payload put of an executed command writes nothing.
        store.protocol(vec![payload_update(&second, &payload_of(2)).unwrap()]);
        store.materialize();
        let raw = store.raw().snapshot().unwrap();
        assert!(
            rows(&raw, Collection::PendingPayloadV1).is_empty(),
            "rule (a)"
        );
        store.assert_index_is_the_rows("after the payload put");
    }
}

/// A trim step names a row by its intake key (review on #282), and the
/// index follows the delete: a command touched again after its row was
/// trimmed is lowered as having none. A put by command is a new row under
/// a later intake key, and a delete by command then takes that one. Had
/// the index kept the trimmed row, the put would be written under the
/// trimmed key (debug builds' check refuses it first: the row is not
/// there).
#[test]
fn a_command_touched_again_after_its_row_was_trimmed_by_intake_key_has_none() {
    fn keys_of(rows: &[RawRow], c: CommandId) -> Vec<Vec<u8>> {
        rows.iter()
            .filter(|(k, _)| matches!(intake::parse(k), ProtocolKey::Intake { command, .. } if command == c))
            .map(|(k, _)| k.clone())
            .collect()
    }
    for mut store in [Store::journaled(), Store::worker()] {
        let (c1, c2) = (command_of(1), command_of(2));
        for c in [c1, c2] {
            store.protocol(vec![
                dependency_update(epoch(), &c, &record(Phase::PreAccept, &[])).unwrap(),
            ]);
        }
        store.materialize();
        let [trimmed] = keys_of(&intake_rows(&store.raw()), c1)
            .try_into()
            .expect("one row");
        // The trim step: c1's row, named by its intake key.
        store.protocol(vec![StoreUpdate {
            collection: Collection::ProtocolV1.id(),
            key: trimmed.clone(),
            value: None,
        }]);
        store.materialize();
        store.assert_index_is_the_rows("after the trim");
        let accepted = record(Phase::Accept, &[c2]);
        store.protocol(vec![dependency_update(epoch(), &c1, &accepted).unwrap()]);
        store.materialize();
        let [now] = keys_of(&intake_rows(&store.raw()), c1)
            .try_into()
            .expect("one row");
        assert!(now > trimmed, "a later intake key, not the trimmed one");
        store.assert_index_is_the_rows("after the put");
        store.protocol(vec![dependency_delete(epoch(), &c1)]);
        store.materialize();
        assert!(keys_of(&intake_rows(&store.raw()), c1).is_empty());
        assert_eq!(keys_of(&intake_rows(&store.raw()), c2).len(), 1);
        store.assert_index_is_the_rows("after the delete");
    }
}

/// Rule (e)'s timing (review on #233, 6813935632): the materializer moves
/// the gate as its commit returns, before the store takes the commit back,
/// so a cut's snapshot can be one commit ahead of the take-back. The
/// overlay's index is the one at the snapshot's stamp: it moved with the
/// gate, not at the take-back.
#[test]
fn a_cut_whose_snapshot_is_one_commit_ahead_of_the_take_back_lays_its_rows_where_materialization_will()
 {
    let (materializer, handle) = coord_storage::ManualMaterializer::new();
    let mut store = JournaledStore::open(
        ModelJournal::new(),
        CLUSTER,
        REPLICA,
        inc(),
        boot(1),
        JournalLimits::default(),
    )
    .unwrap();
    store.attach(A, shard(), ModelEngine::new()).unwrap();
    store.pipeline(Box::new(materializer)).unwrap();
    let mut barriers = BarrierAllocator::new(inc(), boot(1));
    let mut protocol = |store: &mut JournaledStore<ModelJournal, ModelEngine>, updates| {
        store
            .submit(Submission {
                domain: A,
                ballot: ballot(1),
                kind: TransitionKind::Protocol,
                batch: PersistBatch {
                    barrier: barriers.allocate(),
                    base: None,
                    updates,
                },
            })
            .unwrap();
        store.append().unwrap();
    };
    let (c1, c3) = (command_of(1), command_of(3));
    // Record 3: c1 taken in, materialized and taken back.
    protocol(
        &mut store,
        vec![
            dependency_update(epoch(), &c1, &record(Phase::PreAccept, &[])).unwrap(),
            payload_update(&c1, &payload_of(1)).unwrap(),
        ],
    );
    store.hand_off().unwrap();
    assert!(handle.run());
    store.hand_off().unwrap();
    // Record 4: c3 taken in, its commit lent and run, not taken back.
    protocol(
        &mut store,
        vec![
            dependency_update(epoch(), &c3, &record(Phase::PreAccept, &[])).unwrap(),
            payload_update(&c3, &payload_of(3)).unwrap(),
        ],
    );
    store.hand_off().unwrap();
    assert!(store.lent(A));
    assert!(handle.run());
    assert_eq!(
        store.frontiers(A).unwrap().materialized().get(),
        3,
        "the store has not taken record 4's commit back"
    );
    // Record 5, journaled only: c3 rewritten at ACCEPT, c1 deleted.
    let accepted = record(Phase::Accept, &[c1]);
    protocol(
        &mut store,
        vec![
            dependency_update(epoch(), &c3, &accepted).unwrap(),
            dependency_delete(epoch(), &c1),
        ],
    );
    let cut = store.recovery_cut(A).unwrap();
    assert_eq!(
        cut.materialized().get(),
        4,
        "the snapshot is ahead of the take-back"
    );
    let summary = read_protocol(&cut, epoch(), ViewBudget::default()).unwrap();
    assert_eq!(
        summary.records,
        vec![(c3, accepted)],
        "c3 once, rewritten in place; c1 hidden"
    );
    drop(cut);
    // Materialized, the projection reads what the cut did.
    store.drain().unwrap();
    store.hand_off().unwrap();
    assert!(handle.run());
    store.drain().unwrap();
    let gated = store.reader(A).unwrap().snapshot().unwrap();
    assert_eq!(
        read_protocol(gated.view(), epoch(), ViewBudget::default())
            .unwrap()
            .records,
        summary.records
    );
}

/// Every collection's rows, in [`Collection::ALL`]'s order.
type AllRows = Vec<Vec<(Vec<u8>, Vec<u8>)>>;

/// One commit's records lowered by `batched` (each record's rows kept in a
/// [`Lowered`] and applied as one batch, task-d86) or a row at a time, over
/// a store that already holds `before`. Returns every collection's rows and
/// the index's delta.
fn lower_commit(
    before: &[StoreUpdate],
    commit: &[Vec<StoreUpdate>],
    batched: bool,
) -> (AllRows, intake::IndexDelta) {
    use coord_storage::lowering::{Lowered, lower_record};
    let mut engine = ModelEngine::new();
    let index = IntakeIndex::build(&engine.reader().snapshot().unwrap(), 0).unwrap();
    let mut assigner = intake::Assigner::new(&index);
    let mut tx = engine.begin_write().unwrap();
    lower_record(&mut tx, &mut assigner, 1, before).unwrap();
    tx.commit_durable().unwrap();
    let index = IntakeIndex::build(&engine.reader().snapshot().unwrap(), 1).unwrap();
    let mut assigner = intake::Assigner::new(&index);
    let mut tx = engine.begin_write().unwrap();
    if batched {
        let mut lowered = Lowered::new();
        for (i, updates) in commit.iter().enumerate() {
            lowered
                .lower_record(&tx, &mut assigner, 2 + i as u64, updates)
                .unwrap();
        }
        tx.apply_updates(lowered.updates()).unwrap();
    } else {
        for (i, updates) in commit.iter().enumerate() {
            lower_record(&mut tx, &mut assigner, 2 + i as u64, updates).unwrap();
        }
    }
    let delta = assigner.into_delta();
    tx.commit_durable().unwrap();
    let view = engine.reader().snapshot().unwrap();
    let all = Collection::ALL.iter().map(|c| rows(&view, *c)).collect();
    (all, delta)
}

#[test]
fn a_commit_lowered_as_one_batch_writes_what_lowering_a_row_at_a_time_writes() {
    let (c1, c2) = (command_of(1), command_of(2));
    let proposal = ProposalRecordV1 {
        ballot: ballot(1),
        seqnum: 4,
        deps: Vec::new(),
        path: Digest32([0x7; 32]),
    };
    let before = vec![
        dependency_update(epoch(), &c1, &record(Phase::PreAccept, &[])).unwrap(),
        payload_update(&c1, &payload_of(1)).unwrap(),
    ];
    let committed = record(Phase::Commit, &[c1]);
    let commit = vec![
        // c2 taken in with its proposal row; c1, a row the store holds,
        // rewritten in place.
        vec![
            payload_update(&c2, &payload_of(2)).unwrap(),
            dependency_update(epoch(), &c2, &record(Phase::PreAccept, &[])).unwrap(),
            proposal_update(epoch(), &c2, &proposal).unwrap(),
            dependency_update(epoch(), &c1, &record(Phase::Accept, &[c2])).unwrap(),
        ],
        // c2's row, which only this commit holds, rewritten and then
        // deleted.
        vec![
            dependency_update(epoch(), &c2, &record(Phase::Accept, &[c1])).unwrap(),
            dependency_delete(epoch(), &c2),
        ],
        // A delete of a row that is no longer there; c2 taken in again;
        // c1 deleted and executed.
        vec![
            dependency_delete(epoch(), &c2),
            dependency_update(epoch(), &c2, &committed).unwrap(),
            dependency_delete(epoch(), &c1),
            executed(&c1, 1),
        ],
    ];
    let one_at_a_time = lower_commit(&before, &commit, false);
    let batched = lower_commit(&before, &commit, true);
    assert_eq!(batched, one_at_a_time);

    // What both wrote: c2's one row, under the key its last intake took
    // (the third record's second update), with the last value; c1's row
    // gone; its proposal row from the first record.
    let e = epoch().to_be_bytes();
    let protocol = &batched.0[Collection::ALL
        .iter()
        .position(|c| *c == Collection::ProtocolV1)
        .unwrap()];
    let intake: Vec<_> = protocol
        .iter()
        .filter(|(k, _)| matches!(intake::parse(k), ProtocolKey::Intake { .. }))
        .collect();
    assert_eq!(
        intake.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
        vec![
            intake::intake_key(e, DEPENDENCY_TAG, IntakeOrder { seq: 4, ordinal: 1 }, &c2),
            intake::intake_key(e, PROPOSAL_TAG, IntakeOrder { seq: 2, ordinal: 2 }, &c2),
        ]
    );
    assert_eq!(
        coord_consensus::rows::decode_dependency(&intake[0].1).unwrap(),
        committed
    );
}

/// How many rows `all` holds in `collection`.
fn rows_in(all: &AllRows, collection: Collection) -> usize {
    all[Collection::ALL
        .iter()
        .position(|c| *c == collection)
        .unwrap()]
    .len()
}

/// Lower `commit` over `before` both ways, check that one batch writes
/// what a row at a time writes, and return the rows (#244 review).
fn lower_both(before: &[StoreUpdate], commit: &[Vec<StoreUpdate>]) -> AllRows {
    let one_at_a_time = lower_commit(before, commit, false);
    let batched = lower_commit(before, commit, true);
    assert_eq!(batched, one_at_a_time);
    batched.0
}

/// Step 2's cases for the batch (#244 review), each in one commit. The
/// payload an execution moves is a row only this commit holds: read past
/// the batch's own rows, the execution finds no pending payload and rule
/// (b) refuses it.
#[test]
fn a_payload_and_its_execution_in_one_commit_lower_as_one_batch_as_a_row_at_a_time() {
    let c1 = command_of(1);
    let before = vec![dependency_update(epoch(), &c1, &record(Phase::Commit, &[])).unwrap()];
    let commit = vec![
        vec![payload_update(&c1, &payload_of(1)).unwrap()],
        vec![executed(&c1, 1)],
    ];
    let rows = lower_both(&before, &commit);
    assert_eq!(
        [
            rows_in(&rows, Collection::ExecutionV1),
            rows_in(&rows, Collection::PayloadV1),
            rows_in(&rows, Collection::PendingPayloadV1),
            rows_in(&rows, Collection::DecidedV1),
            rows_in(&rows, Collection::ProtocolV1),
        ],
        [1, 1, 0, 1, 0],
        "executed rows, payloads moved, none pending, decided records moved, none left by command"
    );
}

/// A dependency row and a payload for a command written after it executed
/// in the same commit (#244 review): one batch writes what a row at a
/// time writes.
#[test]
fn a_late_dependency_row_after_an_execution_in_one_commit_lowers_as_one_batch_as_a_row_at_a_time() {
    let c1 = command_of(1);
    let before = vec![
        dependency_update(epoch(), &c1, &record(Phase::Commit, &[])).unwrap(),
        payload_update(&c1, &payload_of(1)).unwrap(),
    ];
    let commit = vec![
        vec![executed(&c1, 1)],
        vec![
            dependency_update(epoch(), &c1, &record(Phase::Commit, &[])).unwrap(),
            payload_update(&c1, &payload_of(1)).unwrap(),
        ],
    ];
    let rows = lower_both(&before, &commit);
    assert_eq!(
        [
            rows_in(&rows, Collection::ExecutionV1),
            rows_in(&rows, Collection::PayloadV1),
            rows_in(&rows, Collection::PendingPayloadV1),
            rows_in(&rows, Collection::DecidedV1),
            rows_in(&rows, Collection::ProtocolV1),
        ],
        [1, 1, 0, 1, 0],
        "executed rows, payloads moved, none pending, decided records moved, none left by command"
    );
}

/// A decided record written and moved by its command's execution in the
/// same commit (#244 review): the record the execution moves to
/// `decided_v1` is a row only this commit holds.
#[test]
fn a_decided_record_moved_in_the_commit_that_wrote_it_lowers_as_one_batch_as_a_row_at_a_time() {
    let (c1, c2) = (command_of(1), command_of(2));
    let before = vec![
        dependency_update(epoch(), &c1, &record(Phase::Commit, &[])).unwrap(),
        payload_update(&c1, &payload_of(1)).unwrap(),
    ];
    let commit = vec![
        vec![
            payload_update(&c2, &payload_of(2)).unwrap(),
            dependency_update(epoch(), &c2, &record(Phase::Commit, &[c1])).unwrap(),
            executed(&c1, 1),
        ],
        vec![executed(&c2, 2)],
    ];
    let rows = lower_both(&before, &commit);
    assert_eq!(
        [
            rows_in(&rows, Collection::ExecutionV1),
            rows_in(&rows, Collection::PayloadV1),
            rows_in(&rows, Collection::PendingPayloadV1),
            rows_in(&rows, Collection::DecidedV1),
            rows_in(&rows, Collection::ProtocolV1),
        ],
        [2, 2, 0, 2, 0],
        "executed rows, payloads moved, none pending, decided records moved, none left by command"
    );
}

/// The event row of a put of `k<revision>` (task-d105).
fn event_put(revision: u64) -> StoreUpdate {
    let at = KvRevision::new(revision).unwrap();
    StoreUpdate {
        collection: Collection::EventsV1.id(),
        key: coord_storage::codecs::event_key(at, 0),
        value: Some(
            coord_storage::codecs::encode_event(&coord_storage::codecs::EventRecordV1 {
                kind: coord_state::KvEventKind::Put,
                namespace: NamespaceId([0; 16]),
                key: format!("k{revision}").into_bytes(),
                entry: Some(coord_state::KvEntry {
                    value: vec![revision as u8; 4],
                    create_revision: at,
                    mod_revision: at,
                    version: 1,
                    lease: None,
                    lease_generation: None,
                }),
                prev: None,
            })
            .unwrap(),
        ),
    }
}

/// The delete of the event row at `revision`, as compaction writes it.
fn event_delete(revision: u64) -> StoreUpdate {
    StoreUpdate {
        collection: Collection::EventsV1.id(),
        key: coord_storage::codecs::event_key(KvRevision::new(revision).unwrap(), 0),
        value: None,
    }
}

/// A commit lowered as one batch leaves the key index's delta as lowering
/// a row at a time does (#244 review, after #245): each event row is
/// observed as it is lowered, and a deleted one, which names its key only
/// in its old value, is read through the batch's own rows. Here an event
/// row only this commit holds is deleted, beside one the store held, and
/// the store's row is deleted again (#244 review): the second delete must
/// read the first's absence from the batch, not the store's row beneath.
/// Without the observation the batch's delta holds none of it; read past
/// the batch's rows, the row this commit wrote stays added.
#[test]
fn a_commit_lowered_as_one_batch_leaves_the_key_index_as_a_row_at_a_time_does() {
    let before = vec![event_put(1)];
    let revision = |n: u64| StoreUpdate {
        collection: Collection::MetaV1.id(),
        key: meta_fields::KV_REVISION.to_vec(),
        value: Some(coord_storage::codecs::encode_counter(n).unwrap()),
    };
    let commit = vec![
        vec![event_put(2), event_put(3), revision(3)],
        vec![event_delete(2), event_delete(1)],
        vec![event_delete(1)],
    ];
    let one_at_a_time = lower_commit(&before, &commit, false);
    let batched = lower_commit(&before, &commit, true);
    assert!(
        !one_at_a_time.1.is_empty(),
        "the commit changes the key index: the test shows nothing"
    );
    assert_eq!(batched, one_at_a_time);
}
