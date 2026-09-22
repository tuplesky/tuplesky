//! Conservative all-voter checkpoint trimming: what establishes a floor,
//! what a floor authorizes, and what it never authorizes.
//!
//! The store under test is one voter's: common state at the boundary plus
//! its own `protocol_v1` rows for commands that are settled, for commands
//! that are not, and for one command executed beyond the boundary. Every
//! test asserts the same shape of invariant: an obligation is never evicted,
//! a delayed message never revives what was forgotten, and a deletion never
//! reaches outside `protocol_v1`.

use std::collections::{BTreeMap, BTreeSet};

use coord_checkpoint::export::{CheckpointOrigin, ExportLimits, export_shared};
use coord_checkpoint::floor::{
    ACTIVATION_KEY, CheckpointReadinessV1, READINESS_KEY_PREFIX, RecoveryObligation,
    RecoveryReport, activate_floor, publish_activation, published_activation, read_readiness,
    readiness_key, record_readiness, recovery_obligation,
};
use coord_checkpoint::manifest::{CheckpointBoundary, SharedManifestV1};
use coord_checkpoint::trim::{
    ACK_KEY_PREFIX, CheckpointAckV1, FLOOR_KEY, FenceDecision, TrimError, TrimFence, TrimFloor,
    TrimLimits, TrimPlan, TrimmedFloorV1, ack_key, ack_update, establish_floor, plan_trim,
    publish_floor, publish_floor_in, published_floor, read_acks, trim_backpressure,
};
use coord_consensus::quorum::EpochVoters;
use coord_consensus::recovery::SyncDecision;
use coord_consensus::rows::{
    PayloadRecordV1, PromiseRecordV1, ProposalRecordV1, SyncRecordV1, dependency_key,
    encode_dependency, encode_payload, encode_promise, encode_proposal, encode_sync, payload_key,
    promise_key, proposal_key, sync_key,
};
use coord_consensus::{CommandRecord, Phase};
use coord_core::effect::StoreUpdate;
use coord_state::view::KvEntry;
use coord_storage::codecs::{self, ExecutedRecordV1, HistoryRecordV1};
use coord_storage::compaction::{GcBudget, RetentionHolds, plan_gc};
use coord_storage::lowering::{DurableMeta, ExecutionFrontier};
use coord_storage::protocol::read_protocol;
use coord_storage::views::ViewBudget;
use coord_storage_redb::{Generation, OpenOptions, StoreIdentity};
use coord_store_api::engine::{LocalEngine, OrderedRead, ScanRequest, SnapshotSource, WriteTxn};
use coord_store_api::envelope::AppliedStamp;
use coord_store_api::registry::{Collection, meta_fields};
use coord_store_api::seq::StoreSeq;
use coord_store_testkit::model::ModelEngine;
use coord_types::identity::{CommandId, Digest32};
use coord_types::ids::*;

const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);
const NS: NamespaceId = NamespaceId([5; 16]);
const SELF: ReplicaId = ReplicaId([0xa1; 16]);
const PEER: ReplicaId = ReplicaId([0xb2; 16]);
const LAGGARD: ReplicaId = ReplicaId([0xc3; 16]);
const OBSERVER: ReplicaId = ReplicaId([0xd4; 16]);
/// Configuration epoch the acknowledged boundary was reached under.
const EPOCH: u64 = 7;
/// Execution position the acknowledged checkpoint closes at.
const FLOOR_POSITION: u64 = 3;

fn rev(n: u64) -> KvRevision {
    KvRevision::new(n).unwrap()
}

fn pos(n: u64) -> ExecutionPosition {
    ExecutionPosition::new(n).unwrap()
}

fn epoch(n: u64) -> ConfigurationEpoch {
    ConfigurationEpoch::new(n).unwrap()
}

/// A canonical request. Recovery rederives a payload row's identity from
/// its bytes, so a command in these fixtures is the identity of a real
/// request, not a tag.
fn request_of(n: u8) -> coord_types::logical_v1::LogicalRequest {
    use coord_types::logical_v1::{CanonicalOperation, LogicalRequest, PutOp};
    let mut r = LogicalRequest::new(
        NS,
        CanonicalOperation::Put(PutOp {
            key: vec![n],
            value: vec![7],
            lease: None,
            prev_kv: false,
        }),
    );
    r.canonicalize();
    r
}

fn payload_of(n: u8) -> PayloadRecordV1 {
    PayloadRecordV1 {
        retry_key: retry_key(u64::from(n)),
        logical: postcard::to_allocvec(&request_of(n)).unwrap(),
        admission: None,
    }
}

fn command(n: u8) -> CommandId {
    CommandId::derive(&retry_key(u64::from(n)), &request_of(n)).unwrap()
}

fn voters() -> BTreeSet<ReplicaId> {
    BTreeSet::from([SELF, PEER, LAGGARD])
}

fn ballot(number: u64) -> Ballot {
    Ballot {
        epoch: epoch(EPOCH),
        number,
        leader: SELF,
    }
}

fn boundary() -> CheckpointBoundary {
    CheckpointBoundary {
        execution_position: pos(FLOOR_POSITION),
        kv_revision: rev(4),
        retention_floor: rev(2),
        lease_authority: LeaseAuthorityEpoch::new(1).unwrap(),
    }
}

fn root() -> Digest32 {
    Digest32([0x5a; 32])
}

fn ack(voter: ReplicaId) -> CheckpointAckV1 {
    CheckpointAckV1 {
        voter,
        cluster: CLUSTER,
        domain: DOMAIN,
        configuration: epoch(EPOCH),
        boundary: boundary(),
        root: root(),
    }
}

fn floor() -> TrimFloor {
    TrimFloor {
        cluster: CLUSTER,
        domain: DOMAIN,
        configuration: epoch(EPOCH),
        boundary: boundary(),
        root: root(),
        voters: voters(),
    }
}

fn published() -> TrimmedFloorV1 {
    floor().published()
}

fn entry(value: &[u8], create: u64, modified: u64, version: u64) -> KvEntry {
    KvEntry {
        value: value.to_vec(),
        create_revision: rev(create),
        mod_revision: rev(modified),
        version,
        lease: None,
        lease_generation: None,
    }
}

fn retry_key(sequence: u64) -> coord_types::RetryKey {
    coord_types::RetryKey {
        cluster_id: CLUSTER,
        domain_id: DOMAIN,
        session_id: coord_types::ids::SessionId([9; 16]),
        client_instance_id: coord_types::ids::ClientInstanceId([8; 16]),
        request_sequence: coord_types::ids::RequestSequence::new(sequence).unwrap(),
    }
}

fn record(phase: Phase, deps: &[u8]) -> CommandRecord {
    CommandRecord {
        phase,
        deps: deps.iter().copied().map(command).collect(),
        keys: vec![b"k".to_vec()],
        payload: Some(Digest32([0x11; 32])),
        paths: vec![(b"k".to_vec(), Digest32([0x22; 32]))],
        path: Digest32([0x33; 32]),
        synced_seq: None,
    }
}

fn proposal(deps: &[u8]) -> ProposalRecordV1 {
    ProposalRecordV1 {
        ballot: ballot(2),
        seqnum: 9,
        deps: deps.iter().copied().map(command).collect(),
        path: Digest32([0x44; 32]),
    }
}

type Row = (Collection, Vec<u8>, Vec<u8>);

/// Common state at and around the boundary. None of it is protocol state,
/// and trimming must leave every row of it alone.
fn common_rows() -> Vec<Row> {
    let mut rows: Vec<Row> = Vec::new();
    let mut put = |c: Collection, k: Vec<u8>, v: Vec<u8>| rows.push((c, k, v));
    put(
        Collection::MetaV1,
        meta_fields::KV_REVISION.to_vec(),
        codecs::encode_counter(4).unwrap(),
    );
    put(
        Collection::MetaV1,
        meta_fields::RETENTION_FLOOR.to_vec(),
        codecs::encode_counter(2).unwrap(),
    );
    put(
        Collection::MetaV1,
        meta_fields::LEASE_AUTHORITY.to_vec(),
        codecs::encode_counter(1).unwrap(),
    );
    // Commands 1..=3 are executed at or below the boundary; command 5 is
    // executed beyond it; commands 4 and 6 are not executed at all.
    // Commands with a dependency record but no execution still need a
    // payload row: recovery treats a recovered command without one as
    // corruption.
    for n in [4u8, 6] {
        put(
            Collection::PayloadV1,
            payload_key(&command(n)),
            encode_payload(&payload_of(n)).unwrap(),
        );
    }
    for (n, position) in [(1u8, 1u64), (2, 2), (3, 3), (5, 4), (7, 1), (8, 2)] {
        put(
            Collection::PayloadV1,
            payload_key(&command(n)),
            encode_payload(&payload_of(n)).unwrap(),
        );
        put(
            Collection::ExecutedV1,
            codecs::executed_key(&command(n)),
            codecs::encode_executed(&ExecutedRecordV1 {
                position: pos(position),
                revision: Some(rev(position)),
                result_digest: Digest32([n; 32]),
            })
            .unwrap(),
        );
    }
    put(
        Collection::KvCurrentV1,
        codecs::current_key(&NS, b"a"),
        codecs::encode_current(&entry(b"a4", 1, 4, 3)).unwrap(),
    );
    for revision in [1u64, 2, 3, 4] {
        put(
            Collection::KvHistoryV1,
            codecs::history_key(&NS, b"a", rev(revision)),
            codecs::encode_history(&HistoryRecordV1 {
                entry: Some(entry(b"a", 1, revision, revision)),
            })
            .unwrap(),
        );
    }
    rows
}

/// This voter's own protocol state: a promise, a bound Sync, dependency and
/// proposal rows for settled and unsettled commands, and one row of an epoch
/// above the floor.
fn protocol_rows() -> Vec<Row> {
    let e = epoch(EPOCH);
    let mut rows: Vec<Row> = Vec::new();
    let mut put = |k: Vec<u8>, v: Vec<u8>| rows.push((Collection::ProtocolV1, k, v));
    put(
        promise_key(e),
        encode_promise(&PromiseRecordV1 {
            promised: ballot(3),
            synced: ballot(2),
        })
        .unwrap(),
    );
    put(
        sync_key(e, &ballot(3)),
        encode_sync(&SyncRecordV1 {
            decision: SyncDecision {
                ballot: ballot(3),
                source_ballot: ballot(2),
                entries: BTreeMap::new(),
                reproposed: BTreeSet::new(),
            },
        })
        .unwrap(),
    );
    // Executed commands keep the phase their durable row had before
    // execution, as in production: execution is recorded only in
    // `executed_v1`, never written back into the dependency row.
    //
    // Settled below the boundary, depending on each other in a chain.
    for (n, deps) in [(1u8, &[][..]), (2, &[1][..]), (3, &[2][..])] {
        put(
            dependency_key(e, &command(n)),
            encode_dependency(&record(Phase::Accept, deps)).unwrap(),
        );
        put(
            proposal_key(e, &command(n)),
            encode_proposal(&proposal(deps)).unwrap(),
        );
    }
    // Settled below the boundary and named by nothing: these are what a
    // trim can actually reclaim once the chain above is pinned.
    for (n, phase) in [(7u8, Phase::Accept), (8, Phase::Commit)] {
        put(
            dependency_key(e, &command(n)),
            encode_dependency(&record(phase, &[])).unwrap(),
        );
        put(
            proposal_key(e, &command(n)),
            encode_proposal(&proposal(&[])).unwrap(),
        );
    }
    // Unresolved: accepted, never executed, and it names command 3.
    put(
        dependency_key(e, &command(4)),
        encode_dependency(&record(Phase::Accept, &[3])).unwrap(),
    );
    put(
        proposal_key(e, &command(4)),
        encode_proposal(&proposal(&[3])).unwrap(),
    );
    // Executed beyond the boundary.
    put(
        dependency_key(e, &command(5)),
        encode_dependency(&record(Phase::Commit, &[])).unwrap(),
    );
    // Committed but not executed.
    put(
        dependency_key(e, &command(6)),
        encode_dependency(&record(Phase::Commit, &[])).unwrap(),
    );
    // A settled command of a configuration above the floor's: outside the
    // acknowledged history entirely.
    put(
        dependency_key(epoch(EPOCH + 1), &command(1)),
        encode_dependency(&record(Phase::Accept, &[])).unwrap(),
    );
    rows
}

fn identity_rows() -> Vec<Row> {
    vec![
        (
            Collection::MetaV1,
            meta_fields::CLUSTER_ID.to_vec(),
            CLUSTER.as_bytes().to_vec(),
        ),
        (
            Collection::MetaV1,
            meta_fields::DOMAIN_ID.to_vec(),
            DOMAIN.as_bytes().to_vec(),
        ),
        (
            Collection::MetaV1,
            meta_fields::REPLICA_ID.to_vec(),
            SELF.as_bytes().to_vec(),
        ),
    ]
}

fn seed<E: LocalEngine>(engine: &mut E, rows: Vec<Row>) {
    let mut tx = engine.begin_write().unwrap();
    for (c, k, v) in rows {
        tx.put(c.id(), &k, &v).unwrap();
    }
    let seq = LocalJournalSeq::new(17).unwrap();
    DurableMeta {
        stamp: AppliedStamp::new(StoreSeq::from_journal(seq), Digest32([0xcd; 32])),
        frontier: ExecutionFrontier {
            configuration: epoch(EPOCH),
            execution_position: pos(4),
        },
    }
    .write(&mut tx)
    .unwrap();
    tx.commit_durable().unwrap();
}

/// A voter's store: common state, its protocol state and its identity.
fn voter_store() -> ModelEngine {
    let mut engine = ModelEngine::new();
    let rows = common_rows()
        .into_iter()
        .chain(protocol_rows())
        .chain(identity_rows())
        .collect();
    seed(&mut engine, rows);
    engine
}

/// Publish the fixture's floor durably, as trimming requires before it
/// plans any deletion.
fn publish<E: LocalEngine>(engine: &mut E) {
    let mut tx = engine.begin_write().unwrap();
    publish_floor_in(&mut tx, &floor()).unwrap();
    tx.commit_durable().unwrap();
}

/// A voter's store with the unanimous floor already published.
fn trimmable_store() -> ModelEngine {
    let mut engine = voter_store();
    publish(&mut engine);
    engine
}

fn apply<E: LocalEngine>(engine: &mut E, updates: &[StoreUpdate]) {
    let mut tx = engine.begin_write().unwrap();
    for update in updates {
        match &update.value {
            Some(value) => tx.put(update.collection, &update.key, value).unwrap(),
            None => tx.delete(update.collection, &update.key).unwrap(),
        }
    }
    tx.commit_durable().unwrap();
}

fn rows_of<E: LocalEngine>(engine: &E, collection: Collection) -> Vec<(Vec<u8>, Vec<u8>)> {
    let view = engine.reader().snapshot().unwrap();
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

fn protocol_keys<E: LocalEngine>(engine: &E) -> BTreeSet<Vec<u8>> {
    rows_of(engine, Collection::ProtocolV1)
        .into_iter()
        .map(|(k, _)| k)
        .collect()
}

fn step<E: LocalEngine>(engine: &E, limits: &TrimLimits) -> TrimPlan {
    let view = engine.reader().snapshot().unwrap();
    plan_trim(&view, &published(), limits).unwrap()
}

/// Trim to convergence, returning the number of steps it took.
fn trim_to_completion<E: LocalEngine>(engine: &mut E, limits: &TrimLimits) -> u32 {
    let mut steps = 0;
    loop {
        let plan = step(engine, limits);
        steps += 1;
        apply(engine, &plan.updates);
        if plan.done {
            return steps;
        }
        assert!(steps < 100, "trimming must converge");
    }
}

#[test]
fn a_floor_exists_only_when_every_configured_voter_acknowledged_the_identical_checkpoint() {
    let all: Vec<CheckpointAckV1> = voters().into_iter().map(ack).collect();
    let established = establish_floor(&all, &voters(), CLUSTER, DOMAIN).unwrap();
    assert_eq!(established, floor());
    assert_eq!(established.execution_position(), pos(FLOOR_POSITION));
    assert_eq!(established.published().voters, 3);

    // One voter short: no floor, and the missing voter is named so the
    // caller can apply backpressure rather than guess.
    let short: Vec<CheckpointAckV1> = vec![ack(SELF), ack(PEER)];
    assert_eq!(
        establish_floor(&short, &voters(), CLUSTER, DOMAIN),
        Err(TrimError::MissingAcknowledgements {
            missing: vec![LAGGARD]
        })
    );
    // A majority is not unanimity, however large the configuration.
    assert!(matches!(
        establish_floor(&[ack(SELF)], &voters(), CLUSTER, DOMAIN),
        Err(TrimError::MissingAcknowledgements { .. })
    ));
    assert_eq!(
        establish_floor(&all, &BTreeSet::new(), CLUSTER, DOMAIN),
        Err(TrimError::NoVoters)
    );
}

#[test]
fn an_observer_or_foreign_acknowledgement_is_refused_rather_than_counted() {
    // An observer holds catch-up state, not obligations: its
    // acknowledgement is not a trim vote, and its presence in the ledger
    // does not quietly stand in for the voter that is still owed.
    let mut acks: Vec<CheckpointAckV1> = voters().into_iter().map(ack).collect();
    acks.retain(|a| a.voter != LAGGARD);
    acks.push(ack(OBSERVER));
    assert_eq!(
        establish_floor(&acks, &voters(), CLUSTER, DOMAIN),
        Err(TrimError::NonVoterAcknowledgement { replica: OBSERVER })
    );

    let mut foreign: Vec<CheckpointAckV1> = voters().into_iter().map(ack).collect();
    foreign[0].cluster = ClusterId([9; 16]);
    let voter = foreign[0].voter;
    assert_eq!(
        establish_floor(&foreign, &voters(), CLUSTER, DOMAIN),
        Err(TrimError::OriginMismatch {
            voter,
            field: "cluster"
        })
    );
    let mut other_domain: Vec<CheckpointAckV1> = voters().into_iter().map(ack).collect();
    other_domain[1].domain = DomainId([9; 16]);
    let voter = other_domain[1].voter;
    assert_eq!(
        establish_floor(&other_domain, &voters(), CLUSTER, DOMAIN),
        Err(TrimError::OriginMismatch {
            voter,
            field: "domain"
        })
    );
}

#[test]
fn voters_acknowledging_different_checkpoints_establish_no_floor() {
    for mutate in [
        (|a: &mut CheckpointAckV1| a.root = Digest32([0xff; 32])) as fn(&mut CheckpointAckV1),
        |a: &mut CheckpointAckV1| a.boundary.execution_position = pos(FLOOR_POSITION + 1),
        |a: &mut CheckpointAckV1| a.boundary.retention_floor = rev(1),
        |a: &mut CheckpointAckV1| a.boundary.kv_revision = rev(9),
        |a: &mut CheckpointAckV1| a.boundary.lease_authority = LeaseAuthorityEpoch::new(2).unwrap(),
        |a: &mut CheckpointAckV1| a.configuration = epoch(EPOCH + 1),
    ] {
        let mut acks: Vec<CheckpointAckV1> = voters().into_iter().map(ack).collect();
        // The highest voter in voter order is the one that disagrees.
        let last = acks.len() - 1;
        mutate(&mut acks[last]);
        let voter = acks[last].voter;
        assert_eq!(
            establish_floor(&acks, &voters(), CLUSTER, DOMAIN),
            Err(TrimError::DivergentAcknowledgement { voter }),
            "divergence must block the floor"
        );
    }
}

#[test]
fn acknowledgements_are_read_back_from_durable_rows_and_not_confused_with_other_records() {
    let dir = tempfile::tempdir().unwrap();
    let mut generation = Generation::create(
        dir.path(),
        StoreIdentity {
            cluster_id: CLUSTER,
            domain_id: DOMAIN,
            replica_id: SELF,
            incarnation: ReplicaIncarnation::new(1).unwrap(),
        },
        OpenOptions {
            cache_bytes: 4 << 20,
        },
    )
    .unwrap();
    let engine = generation.engine();
    let limits = TrimLimits::default();

    let mut tx = engine.begin_write().unwrap();
    for voter in voters() {
        let update = ack_update(&ack(voter)).unwrap();
        tx.put(
            update.collection,
            &update.key,
            update.value.as_ref().unwrap(),
        )
        .unwrap();
    }
    // Neighbouring `checkpoint_v1` records must not be read as
    // acknowledgements: the published floor sorts after the ack prefix.
    let floor_update = publish_floor(&floor(), None).unwrap();
    tx.put(
        floor_update.collection,
        &floor_update.key,
        floor_update.value.as_ref().unwrap(),
    )
    .unwrap();
    tx.commit_durable().unwrap();

    let view = engine.reader().snapshot().unwrap();
    let acks = read_acks(&view, &limits).unwrap();
    assert_eq!(acks.len(), 3);
    assert_eq!(
        acks.iter().map(|a| a.voter).collect::<Vec<_>>(),
        voters().into_iter().collect::<Vec<_>>()
    );
    assert_eq!(
        establish_floor(&acks, &voters(), CLUSTER, DOMAIN).unwrap(),
        floor()
    );
    assert_eq!(published_floor(&view).unwrap(), Some(published()));
    assert_eq!(FLOOR_KEY, b"trimmed_floor_v1");
    assert!(ack_key(&SELF).starts_with(ACK_KEY_PREFIX));

    // A row whose value claims another voter than its key is corrupt, not a
    // vote: unanimity is never computed from a ledger this build cannot
    // fully account for.
    let mut tx = engine.begin_write().unwrap();
    tx.put(
        Collection::CheckpointV1.id(),
        &ack_key(&PEER),
        &ack(LAGGARD).encode().unwrap(),
    )
    .unwrap();
    tx.commit_durable().unwrap();
    let view = engine.reader().snapshot().unwrap();
    assert!(matches!(
        read_acks(&view, &limits),
        Err(TrimError::Engine(_))
    ));
}

#[test]
fn a_missing_voter_stops_trimming_with_bounded_backpressure_instead_of_evicting_obligations() {
    let engine = voter_store();
    let view = engine.reader().snapshot().unwrap();
    let before = protocol_keys(&engine);

    let acks: Vec<CheckpointAckV1> = vec![ack(SELF), ack(PEER)];
    let missing = match establish_floor(&acks, &voters(), CLUSTER, DOMAIN) {
        Err(TrimError::MissingAcknowledgements { missing }) => missing,
        other => panic!("a missing voter must block the floor, got {other:?}"),
    };
    assert_eq!(missing, vec![LAGGARD]);

    // Below the bound new work is still admitted and nothing is deleted.
    let limits = TrimLimits::default();
    let pressure = trim_backpressure(&view, &missing, &limits).unwrap();
    assert_eq!(pressure.missing, vec![LAGGARD]);
    assert_eq!(pressure.protocol_rows, before.len() as u32);
    assert!(pressure.admits_new_work());

    // At the bound the caller refuses new work; the accounting itself stays
    // bounded and no obligation is touched.
    let tight = TrimLimits {
        max_protocol_rows: 4,
        max_examined_rows: 5,
        ..TrimLimits::default()
    };
    let pressure = trim_backpressure(&view, &missing, &tight).unwrap();
    assert_eq!(pressure.protocol_rows, 4);
    assert_eq!(pressure.limit, 4);
    assert!(!pressure.admits_new_work());
    assert_eq!(protocol_keys(&engine), before, "nothing may be evicted");
}

#[test]
fn trimming_deletes_only_settled_below_floor_protocol_rows_and_keeps_every_obligation() {
    let mut engine = trimmable_store();
    let before = protocol_keys(&engine);
    let limits = TrimLimits::default();

    let plan = step(&engine, &limits);
    assert!(plan.done);
    assert!(plan.examined > 0);
    assert!(
        plan.updates
            .iter()
            .all(|u| u.collection == Collection::ProtocolV1.id() && u.value.is_none()),
        "a trim emits only protocol deletions"
    );
    apply(&mut engine, &plan.updates);
    let after = protocol_keys(&engine);
    let e = epoch(EPOCH);

    // Commands 7 and 8 are executed at or below the boundary and nothing
    // retained can reach them, so their rows go.
    for n in [7u8, 8] {
        assert!(
            !after.contains(&dependency_key(e, &command(n))),
            "command {n} is settled below the floor and reachable from nothing"
        );
        assert!(!after.contains(&proposal_key(e, &command(n))));
    }

    // Everything else stays: the promise, the bound Sync, the accepted but
    // unexecuted command, the settled commands it can reach through the
    // chain, the command committed but not executed, the command executed
    // beyond the boundary, and a command of an epoch above the floor's.
    for retained in [
        promise_key(e),
        sync_key(e, &ballot(3)),
        dependency_key(e, &command(1)),
        proposal_key(e, &command(1)),
        dependency_key(e, &command(2)),
        proposal_key(e, &command(2)),
        dependency_key(e, &command(3)),
        proposal_key(e, &command(3)),
        dependency_key(e, &command(4)),
        proposal_key(e, &command(4)),
        dependency_key(e, &command(5)),
        dependency_key(e, &command(6)),
        dependency_key(epoch(EPOCH + 1), &command(1)),
    ] {
        assert!(
            after.contains(&retained),
            "an obligation, a promise, a Sync or an out-of-scope row was trimmed"
        );
    }
    assert!(after.is_subset(&before));
    assert_eq!(before.len() - after.len(), plan.updates.len());

    // A second step over the trimmed store deletes nothing more.
    let again = step(&engine, &limits);
    assert!(again.done);
    assert!(again.updates.is_empty());
}

#[test]
fn a_retained_command_pins_the_protocol_rows_of_the_dependencies_it_names() {
    let engine = trimmable_store();
    let plan = step(&engine, &TrimLimits::default());
    let e = epoch(EPOCH);

    // Command 4 is accepted and unexecuted and names command 3, so
    // command 3's rows are held back although command 3 is settled below
    // the floor - and so are command 2's and command 1's. The pin runs
    // the whole chain: closure traversal after a restart walks 4 -> 3 ->
    // 2 -> 1, and a deleted link stops it at a command it cannot resolve.
    // Being settled below the floor is not on its own a reason to drop a
    // row something unresolved still has to reach.
    assert_eq!(
        plan.pinned, 6,
        "the dependency and proposal rows of commands 1, 2 and 3"
    );
    let deleted: BTreeSet<Vec<u8>> = plan.updates.iter().map(|u| u.key.clone()).collect();
    for n in [1u8, 2, 3] {
        assert!(
            !deleted.contains(&dependency_key(e, &command(n))),
            "command {n} is reachable from the unresolved command 4"
        );
        assert!(!deleted.contains(&proposal_key(e, &command(n))));
    }
    // What nothing unresolved can reach is still reclaimed.
    assert!(deleted.contains(&dependency_key(e, &command(7))));
    assert!(deleted.contains(&dependency_key(e, &command(8))));

    // Once the unresolved command executes at or below a later boundary,
    // the pin is gone and the rows become eligible.
    let mut settled = ModelEngine::new();
    let mut rows: Vec<Row> = common_rows()
        .into_iter()
        .chain(protocol_rows())
        .chain(identity_rows())
        .collect();
    rows.retain(|(c, k, _)| *c != Collection::ProtocolV1 || *k != dependency_key(e, &command(4)));
    rows.push((
        Collection::ProtocolV1,
        dependency_key(e, &command(4)),
        encode_dependency(&record(Phase::Accept, &[3])).unwrap(),
    ));
    rows.push((
        Collection::ExecutedV1,
        codecs::executed_key(&command(4)),
        codecs::encode_executed(&ExecutedRecordV1 {
            position: pos(FLOOR_POSITION),
            revision: Some(rev(3)),
            result_digest: Digest32([4; 32]),
        })
        .unwrap(),
    ));
    seed(&mut settled, rows);
    publish(&mut settled);
    let plan = step(&settled, &TrimLimits::default());
    let deleted: BTreeSet<Vec<u8>> = plan.updates.iter().map(|u| u.key.clone()).collect();
    assert_eq!(plan.pinned, 0);
    assert!(deleted.contains(&dependency_key(e, &command(3))));
    assert!(deleted.contains(&proposal_key(e, &command(3))));
    assert!(deleted.contains(&dependency_key(e, &command(4))));
}

#[test]
fn a_trim_step_is_bounded_and_repeating_it_converges_on_the_same_retained_state() {
    let mut whole = trimmable_store();
    let mut piecemeal = trimmable_store();
    let limits = TrimLimits::default();

    assert_eq!(trim_to_completion(&mut whole, &limits), 1);
    let bounded = TrimLimits {
        max_deletions: 1,
        ..limits
    };
    let steps = trim_to_completion(&mut piecemeal, &bounded);
    assert!(steps > 1, "a one-row budget must take several steps");
    assert_eq!(protocol_keys(&piecemeal), protocol_keys(&whole));

    // A survey that cannot complete deletes nothing at all: no row can be
    // shown to be unpinned from a partial view of the epoch.
    let engine = trimmable_store();
    let view = engine.reader().snapshot().unwrap();
    let cramped = TrimLimits {
        max_protocol_rows: 2,
        max_examined_rows: 3,
        ..limits
    };
    assert_eq!(
        plan_trim(&view, &published(), &cramped),
        Err(TrimError::ExaminationBudget { limit: 3 })
    );
    // Limits that would let the store outgrow one survey are refused up
    // front, so that state is unreachable while the bounds hold.
    let unusable = TrimLimits {
        max_protocol_rows: 8,
        max_examined_rows: 8,
        ..limits
    };
    assert_eq!(
        plan_trim(&view, &published(), &unusable),
        Err(TrimError::InvalidLimits)
    );
}

#[test]
fn a_delayed_message_cannot_revive_state_below_the_published_floor() {
    let mut engine = trimmable_store();
    trim_to_completion(&mut engine, &TrimLimits::default());
    let view = engine.reader().snapshot().unwrap();
    // The fence is built from the floor the store holds, as a restarted
    // node builds it, not from a value that was never persisted.
    let fence = TrimFence::new(published_floor(&view).unwrap().unwrap());

    // A command executed at or below the floor is settled: its protocol
    // rows are gone and the retained execution record answers for it.
    for n in [1u8, 2, 3] {
        assert_eq!(
            fence.command(&view, &command(n)).unwrap(),
            FenceDecision::BelowFloor,
            "command {n} is below the floor"
        );
    }
    // A command executed beyond the floor, and one this replica has never
    // seen, are ordinary live work.
    assert_eq!(
        fence.command(&view, &command(5)).unwrap(),
        FenceDecision::Admit
    );
    assert_eq!(
        fence.command(&view, &command(9)).unwrap(),
        FenceDecision::Admit
    );
    // Traffic of a retired configuration is below the floor whatever it
    // claims about commands.
    assert_eq!(
        fence.configuration(epoch(EPOCH - 1)),
        FenceDecision::BelowFloor
    );
    assert_eq!(fence.configuration(epoch(EPOCH)), FenceDecision::Admit);
    assert_eq!(
        fence.ballot(&Ballot {
            epoch: epoch(EPOCH - 1),
            number: 99,
            leader: PEER
        }),
        FenceDecision::BelowFloor
    );
    assert_eq!(fence.ballot(&ballot(4)), FenceDecision::Admit);
    assert_eq!(fence.floor(), &published());
}

#[test]
fn the_published_floor_never_moves_backwards_under_a_late_acknowledgement() {
    let current = published();
    // Republishing the same floor is idempotent.
    let update = publish_floor(&floor(), Some(&current)).unwrap();
    assert_eq!(update.key, FLOOR_KEY.to_vec());
    assert_eq!(
        TrimmedFloorV1::decode(update.value.as_ref().unwrap()).unwrap(),
        current
    );

    // A unanimous but older checkpoint, arriving late, cannot lower it.
    let mut older = floor();
    older.boundary.execution_position = pos(FLOOR_POSITION - 1);
    assert_eq!(
        publish_floor(&older, Some(&current)),
        Err(TrimError::FloorRegressed {
            published: pos(FLOOR_POSITION),
            offered: pos(FLOOR_POSITION - 1)
        })
    );
    let mut older_configuration = floor();
    older_configuration.configuration = epoch(EPOCH - 1);
    assert!(matches!(
        publish_floor(&older_configuration, Some(&current)),
        Err(TrimError::FloorRegressed { .. })
    ));

    // The same boundary with another root is a disagreement about history.
    let mut conflicting = floor();
    conflicting.root = Digest32([0xee; 32]);
    assert_eq!(
        publish_floor(&conflicting, Some(&current)),
        Err(TrimError::FloorConflict)
    );

    // A later boundary moves it forward.
    let mut newer = floor();
    newer.boundary.execution_position = pos(FLOOR_POSITION + 1);
    newer.root = Digest32([0x77; 32]);
    assert!(publish_floor(&newer, Some(&current)).is_ok());

    // A floor of another cluster or domain authorizes nothing here.
    let mut foreign = floor();
    foreign.cluster = ClusterId([9; 16]);
    assert_eq!(
        publish_floor(&foreign, Some(&current)),
        Err(TrimError::FloorOriginMismatch { field: "cluster" })
    );
    let engine = trimmable_store();
    let view = engine.reader().snapshot().unwrap();
    let mut elsewhere = published();
    elsewhere.domain = DomainId([9; 16]);
    assert_eq!(
        plan_trim(&view, &elsewhere, &TrimLimits::default()),
        Err(TrimError::FloorOriginMismatch { field: "domain" })
    );
}

#[test]
fn trimming_leaves_public_mvcc_common_state_and_the_local_journal_stamp_untouched() {
    let mut engine = trimmable_store();
    let common: Vec<Vec<(Vec<u8>, Vec<u8>)>> = Collection::ALL
        .iter()
        .filter(|c| **c != Collection::ProtocolV1)
        .map(|c| rows_of(&engine, *c))
        .collect();
    let exported = export_shared(
        &engine.reader().snapshot().unwrap(),
        CheckpointOrigin {
            cluster: CLUSTER,
            domain: DOMAIN,
        },
        &ExportLimits::default(),
    )
    .unwrap();

    trim_to_completion(&mut engine, &TrimLimits::default());

    let after: Vec<Vec<(Vec<u8>, Vec<u8>)>> = Collection::ALL
        .iter()
        .filter(|c| **c != Collection::ProtocolV1)
        .map(|c| rows_of(&engine, *c))
        .collect();
    assert_eq!(
        common, after,
        "history, events, executed identities, payloads, retries, the MVCC \
         retention floor and the applied stamp are not this floor's business"
    );

    // `protocol_v1` is node-private, so semantic forgetting cannot change
    // what the domain agrees its common state is.
    let reexported = export_shared(
        &engine.reader().snapshot().unwrap(),
        CheckpointOrigin {
            cluster: CLUSTER,
            domain: DOMAIN,
        },
        &ExportLimits::default(),
    )
    .unwrap();
    assert_eq!(reexported.manifest.root, exported.manifest.root);
    assert_eq!(reexported.manifest, exported.manifest);
}

#[test]
fn mvcc_collection_and_protocol_trimming_decide_different_retention() {
    // Section 17.16.5: one floor cannot control them all. MVCC garbage
    // collection deletes history under the replicated retention floor and
    // never a protocol row; trimming deletes protocol rows under the
    // all-voter floor and never a history row.
    let mut engine = trimmable_store();
    let protocol_before = protocol_keys(&engine);

    let gc = plan_gc(
        &engine.reader().snapshot().unwrap(),
        &RetentionHolds::default(),
        GcBudget::default(),
    )
    .unwrap();
    assert!(
        gc.updates
            .iter()
            .any(|u| u.collection == Collection::KvHistoryV1.id() && u.value.is_none()),
        "the replicated retention floor is above the oldest history version"
    );
    assert!(
        gc.updates
            .iter()
            .all(|u| u.collection != Collection::ProtocolV1.id()),
        "MVCC collection never forgets protocol state"
    );
    apply(&mut engine, &gc.updates);
    assert_eq!(protocol_keys(&engine), protocol_before);

    let history_before = rows_of(&engine, Collection::KvHistoryV1);
    let plan = step(&engine, &TrimLimits::default());
    assert!(
        plan.updates
            .iter()
            .all(|u| u.collection == Collection::ProtocolV1.id())
    );
    apply(&mut engine, &plan.updates);
    assert_eq!(rows_of(&engine, Collection::KvHistoryV1), history_before);
}

#[test]
fn an_unrecognized_protocol_row_is_retained_rather_than_deleted() {
    let mut engine = ModelEngine::new();
    let e = epoch(EPOCH);
    let mut rows: Vec<Row> = common_rows()
        .into_iter()
        .chain(protocol_rows())
        .chain(identity_rows())
        .collect();
    // A row of a tag this build does not know, and a dependency-tagged row
    // whose key is the wrong shape: neither may be deleted on the strength
    // of being near a trimmable one.
    let mut unknown_tag = e.to_be_bytes().to_vec();
    unknown_tag.push(0x02 + 0x40);
    unknown_tag.extend_from_slice(command(1).as_bytes());
    let mut short_key = e.to_be_bytes().to_vec();
    short_key.push(0x01);
    short_key.extend_from_slice(&command(1).as_bytes()[..8]);
    rows.push((
        Collection::ProtocolV1,
        unknown_tag.clone(),
        b"future".to_vec(),
    ));
    rows.push((
        Collection::ProtocolV1,
        short_key.clone(),
        b"future".to_vec(),
    ));
    seed(&mut engine, rows);
    publish(&mut engine);

    let plan = step(&engine, &TrimLimits::default());
    let deleted: BTreeSet<Vec<u8>> = plan.updates.iter().map(|u| u.key.clone()).collect();
    assert!(!deleted.contains(&unknown_tag));
    assert!(!deleted.contains(&short_key));
    assert!(plan.retained >= 2);
    apply(&mut engine, &plan.updates);
    let after = protocol_keys(&engine);
    assert!(after.contains(&unknown_tag) && after.contains(&short_key));
}

#[test]
fn a_corrupt_protocol_or_execution_record_stops_the_trim_without_deleting_anything() {
    let e = epoch(EPOCH);
    for (collection, key, value) in [
        (
            Collection::ProtocolV1,
            dependency_key(e, &command(1)),
            b"not a dependency record".to_vec(),
        ),
        (
            Collection::ExecutedV1,
            codecs::executed_key(&command(1)),
            b"not an execution record".to_vec(),
        ),
    ] {
        let mut engine = ModelEngine::new();
        let mut rows: Vec<Row> = common_rows()
            .into_iter()
            .chain(protocol_rows())
            .chain(identity_rows())
            .collect();
        rows.retain(|(c, k, _)| !(*c == collection && *k == key));
        rows.push((collection, key.clone(), value));
        seed(&mut engine, rows);
        publish(&mut engine);
        let before = protocol_keys(&engine);
        let view = engine.reader().snapshot().unwrap();
        assert!(
            matches!(
                plan_trim(&view, &published(), &TrimLimits::default()),
                Err(TrimError::Engine(_))
            ),
            "a record this build cannot read is never trimmed around"
        );
        assert_eq!(protocol_keys(&engine), before);
    }
}

#[test]
fn an_acknowledgement_is_built_from_what_a_voter_verified_and_round_trips() {
    let manifest = SharedManifestV1 {
        format: coord_checkpoint::manifest::SHARED_CHECKPOINT_FORMAT_V1,
        cluster: CLUSTER,
        domain: DOMAIN,
        configuration: epoch(EPOCH),
        boundary: boundary(),
        collections: Vec::new(),
        chunks: Vec::new(),
        root: root(),
    };
    let from_manifest = CheckpointAckV1::for_manifest(SELF, &manifest);
    assert_eq!(from_manifest, ack(SELF));

    let receipt = coord_checkpoint::install::InstalledCheckpointV1 {
        format: manifest.format,
        cluster: CLUSTER,
        domain: DOMAIN,
        configuration: epoch(EPOCH),
        boundary: boundary(),
        root: root(),
        chunks: 0,
        rows: 0,
    };
    assert_eq!(CheckpointAckV1::for_install(PEER, &receipt), ack(PEER));

    let encoded = from_manifest.encode().unwrap();
    assert_eq!(CheckpointAckV1::decode(&encoded).unwrap(), from_manifest);
    // An acknowledgement is not a floor and a floor is not an
    // acknowledgement, whatever their bytes look like.
    assert!(TrimmedFloorV1::decode(&encoded).is_err());
    assert!(CheckpointAckV1::decode(&published().encode().unwrap()).is_err());
    assert!(CheckpointAckV1::decode(&encoded[..encoded.len() - 1]).is_err());
}

#[test]
fn recovery_after_a_trim_knows_how_far_the_store_executed() {
    // The execution frontier was derived from the surviving dependency
    // rows. Trimming removes those for executed commands while keeping
    // the durable applied position, so a store whose executed prefix has
    // been fully trimmed reported zero: the next command would be
    // planned at a position the store had already used.
    let mut engine = trimmable_store();
    let before = {
        let view = engine.reader().snapshot().unwrap();
        read_protocol(&view, epoch(EPOCH), ViewBudget::default())
            .unwrap()
            .execution_frontier()
    };
    assert!(before.get() > 0, "the fixture has executed commands");
    trim_to_completion(&mut engine, &TrimLimits::default());
    {
        let view = engine.reader().snapshot().unwrap();
        assert_eq!(
            read_protocol(&view, epoch(EPOCH), ViewBudget::default())
                .unwrap()
                .execution_frontier(),
            before,
            "trimming rows does not move the execution frontier back"
        );
    }

    // The case the derivation could not survive at all: every dependency
    // row of the epoch gone, the durable position untouched.
    let mut stripped = ModelEngine::new();
    let rows: Vec<Row> = common_rows()
        .into_iter()
        .chain(identity_rows())
        .chain(core::iter::once((
            Collection::ProtocolV1,
            promise_key(epoch(EPOCH)),
            encode_promise(&PromiseRecordV1 {
                promised: ballot(3),
                synced: ballot(2),
            })
            .unwrap(),
        )))
        .collect();
    seed(&mut stripped, rows);
    let view = stripped.reader().snapshot().unwrap();
    let recovered = read_protocol(&view, epoch(EPOCH), ViewBudget::default()).unwrap();
    assert!(recovered.records.is_empty(), "no dependency rows survive");
    assert_eq!(
        recovered.executed_through(),
        ExecutionPosition::ZERO,
        "the surviving rows show nothing, and say so"
    );
    assert_eq!(
        recovered.execution_frontier(),
        pos(4),
        "the durable baseline says how far the store executed"
    );
}

#[test]
fn a_trim_keeps_every_dependency_a_retained_command_can_reach() {
    // Pinning only direct dependencies left a retained command able to
    // reach an executed command whose own dependency had been deleted,
    // so closure traversal after a restart stopped at a command it could
    // not resolve. The chain in the fixture is 4 -> 3 -> 2 -> 1, with 4
    // unresolved.
    let mut engine = trimmable_store();
    trim_to_completion(&mut engine, &TrimLimits::default());
    let remaining = protocol_keys(&engine);
    let e = epoch(EPOCH);
    for n in [1u8, 2, 3, 4] {
        assert!(
            remaining.contains(&dependency_key(e, &command(n))),
            "command {n} is reachable from the unresolved command 4"
        );
    }
    // Every dependency named by a surviving record has a record of its
    // own: the graph a restart walks is closed.
    let view = engine.reader().snapshot().unwrap();
    let recovered = read_protocol(&view, epoch(EPOCH), ViewBudget::default()).unwrap();
    let known: BTreeSet<_> = recovered.records.iter().map(|(c, _)| *c).collect();
    for (command, record) in &recovered.records {
        for dep in &record.deps {
            assert!(
                known.contains(dep),
                "{command:?} names {dep:?}, which trimming removed"
            );
        }
    }
}

#[test]
fn a_plan_is_refused_until_its_floor_is_durable() {
    // Deletions committed ahead of their floor, or in the same batch, leave
    // a crash with rows gone and no fence to reject delayed traffic for
    // them. Planning therefore reads the floor from the store it plans
    // against, never from the caller.
    let mut engine = voter_store();
    let before = protocol_keys(&engine);
    let limits = TrimLimits::default();
    {
        let view = engine.reader().snapshot().unwrap();
        assert_eq!(
            plan_trim(&view, &published(), &limits),
            Err(TrimError::FloorNotDurable),
            "no floor is durable yet"
        );
    }

    // A durable floor below the offered one does not cover it; planning
    // under the durable floor itself is fine.
    let mut lower = floor();
    lower.boundary.execution_position = pos(FLOOR_POSITION - 1);
    lower.root = Digest32([0x77; 32]);
    let mut tx = engine.begin_write().unwrap();
    publish_floor_in(&mut tx, &lower).unwrap();
    tx.commit_durable().unwrap();
    {
        let view = engine.reader().snapshot().unwrap();
        assert_eq!(
            plan_trim(&view, &published(), &limits),
            Err(TrimError::FloorNotDurable)
        );
        assert!(plan_trim(&view, &lower.published(), &limits).is_ok());
    }
    assert_eq!(protocol_keys(&engine), before, "nothing was deleted");

    // Once the offered floor is durable the plan proceeds, and a floor that
    // names the same boundary with another root is still refused.
    publish(&mut engine);
    let view = engine.reader().snapshot().unwrap();
    assert!(
        !plan_trim(&view, &published(), &limits)
            .unwrap()
            .updates
            .is_empty()
    );
    let mut conflicting = published();
    conflicting.root = Digest32([0xee; 32]);
    assert_eq!(
        plan_trim(&view, &conflicting, &limits),
        Err(TrimError::FloorConflict)
    );
}

#[test]
fn an_executed_command_is_trimmed_whatever_phase_its_durable_row_records() {
    // Execution is recorded in `executed_v1` only; the durable dependency
    // row keeps the phase it had before, normally `Accept` or `Commit`.
    // Requiring `Executed` in the row retained every real command forever.
    let e = epoch(EPOCH);
    for phase in [Phase::Accept, Phase::Commit, Phase::Executed] {
        let mut engine = ModelEngine::new();
        let rows: Vec<Row> = common_rows()
            .into_iter()
            .chain(identity_rows())
            .chain([
                (
                    Collection::ProtocolV1,
                    dependency_key(e, &command(7)),
                    encode_dependency(&record(phase, &[])).unwrap(),
                ),
                (
                    Collection::ProtocolV1,
                    proposal_key(e, &command(7)),
                    encode_proposal(&proposal(&[])).unwrap(),
                ),
                // Committed but not executed: an obligation in any phase.
                (
                    Collection::ProtocolV1,
                    dependency_key(e, &command(6)),
                    encode_dependency(&record(Phase::Commit, &[])).unwrap(),
                ),
            ])
            .collect();
        seed(&mut engine, rows);
        publish(&mut engine);
        trim_to_completion(&mut engine, &TrimLimits::default());
        let after = protocol_keys(&engine);
        assert!(
            !after.contains(&dependency_key(e, &command(7))),
            "an executed command's {phase:?} row is trimmed"
        );
        assert!(!after.contains(&proposal_key(e, &command(7))));
        assert!(after.contains(&dependency_key(e, &command(6))));
    }
}

#[test]
fn a_floor_publication_planned_from_a_stale_snapshot_cannot_lower_the_floor() {
    let mut engine = voter_store();
    // A maintenance attempt reads the floor (none yet) and is delayed.
    let stale = {
        let view = engine.reader().snapshot().unwrap();
        published_floor(&view).unwrap()
    };
    assert_eq!(stale, None);

    // Meanwhile a newer floor lands.
    let mut newer = floor();
    newer.boundary.execution_position = pos(FLOOR_POSITION + 1);
    newer.root = Digest32([0x77; 32]);
    let mut tx = engine.begin_write().unwrap();
    publish_floor_in(&mut tx, &newer).unwrap();
    tx.commit_durable().unwrap();

    // Checked against its stale read, the older floor looks publishable;
    // checked inside the write, it is a regression and nothing is written.
    assert!(publish_floor(&floor(), stale.as_ref()).is_ok());
    let mut tx = engine.begin_write().unwrap();
    assert_eq!(
        publish_floor_in(&mut tx, &floor()),
        Err(TrimError::FloorRegressed {
            published: pos(FLOOR_POSITION + 1),
            offered: pos(FLOOR_POSITION)
        })
    );
    tx.abort().unwrap();
    let view = engine.reader().snapshot().unwrap();
    assert_eq!(published_floor(&view).unwrap(), Some(newer.published()));
}

// ---------------------------------------------------------------------
// task-53: the quorum-certified floor, over the same store and the same
// trimming. Everything below the floor is unchanged; what changes is how
// the floor comes to exist and what a recovery owes because of it.
// ---------------------------------------------------------------------

fn floor_voters() -> EpochVoters {
    EpochVoters::new(epoch(EPOCH), voters()).unwrap()
}

fn readiness(voter: ReplicaId) -> CheckpointReadinessV1 {
    CheckpointReadinessV1 {
        voter,
        cluster: CLUSTER,
        domain: DOMAIN,
        configuration: epoch(EPOCH),
        boundary: boundary(),
        root: root(),
    }
}

/// The store with `who` having durably promised.
fn store_with_readiness(who: &[ReplicaId]) -> ModelEngine {
    let mut engine = voter_store();
    for voter in who {
        let view = engine.reader().snapshot().unwrap();
        let update = record_readiness(&view, &floor_voters(), &readiness(*voter)).unwrap();
        drop(view);
        apply(&mut engine, &[update]);
    }
    engine
}

/// A permanently absent voter no longer stops semantic forgetting.
///
/// This is the whole of what task-53 buys over task-51, and the shape of
/// the test says why it is safe rather than merely convenient: the same
/// store, the same protocol rows, the same deletions -- only the floor
/// arrives with two promises out of three instead of three out of three.
#[test]
fn a_majority_of_promises_certifies_the_floor_a_missing_voter_would_have_blocked() {
    let mut engine = store_with_readiness(&[SELF, PEER]);
    let view = engine.reader().snapshot().unwrap();

    // Task-51's rule still says no, and says exactly who is missing.
    let acks: Vec<CheckpointAckV1> = [SELF, PEER].into_iter().map(ack).collect();
    assert_eq!(
        establish_floor(&acks, &voters(), CLUSTER, DOMAIN),
        Err(TrimError::MissingAcknowledgements {
            missing: vec![LAGGARD]
        })
    );

    let promises = read_readiness(&view, &TrimLimits::default()).unwrap();
    let certified = activate_floor(&promises, &floor_voters(), CLUSTER, DOMAIN).unwrap();
    assert_eq!(certified.signers, BTreeSet::from([SELF, PEER]));
    assert_eq!(certified.boundary, boundary());
    // The certificate yields the floor the all-voter path would have,
    // so everything after it is the same code. The one difference is
    // the one that matters: it is published on two promises, not three.
    let yielded = certified.trim_floor().published();
    assert_eq!(yielded.boundary, published().boundary);
    assert_eq!(yielded.root, published().root);
    assert_eq!(yielded.configuration, published().configuration);
    assert_eq!(yielded.voters, 2);
    drop(view);

    // Publish the certificate and the floor in one durable batch, then
    // trim. What survives is what task-51's own test asserts survives.
    let view = engine.reader().snapshot().unwrap();
    let batch = vec![
        publish_activation(&certified, published_activation(&view).unwrap().as_ref()).unwrap(),
        publish_floor(
            &certified.trim_floor(),
            published_floor(&view).unwrap().as_ref(),
        )
        .unwrap(),
    ];
    drop(view);
    apply(&mut engine, &batch);
    let before = protocol_keys(&engine);
    trim_to_completion(&mut engine, &TrimLimits::default());
    let after = protocol_keys(&engine);
    assert!(
        after.len() < before.len(),
        "a certified floor authorized no deletion at all"
    );
    // The promise row itself is not protocol state and is never trimmed:
    // it is what a recovery reads.
    let view = engine.reader().snapshot().unwrap();
    assert_eq!(
        read_readiness(&view, &TrimLimits::default()).unwrap().len(),
        2
    );
}

/// A minority certifies nothing, and possession is not a promise.
#[test]
fn possession_without_a_promise_certifies_nothing_and_neither_does_a_minority() {
    // One promise of three voters.
    let engine = store_with_readiness(&[SELF]);
    let view = engine.reader().snapshot().unwrap();
    let promises = read_readiness(&view, &TrimLimits::default()).unwrap();
    assert_eq!(
        activate_floor(&promises, &floor_voters(), CLUSTER, DOMAIN),
        Err(TrimError::NoQuorum { have: 1, need: 2 })
    );

    // Two voters hold the checkpoint -- their acknowledgements are
    // durable -- and neither promised. Possession is visible and
    // certifies nothing: there is no readiness row to certify from.
    let mut holders = voter_store();
    let updates: Vec<StoreUpdate> = [SELF, PEER]
        .into_iter()
        .map(|v| ack_update(&ack(v)).unwrap())
        .collect();
    apply(&mut holders, &updates);
    let view = holders.reader().snapshot().unwrap();
    assert_eq!(read_acks(&view, &TrimLimits::default()).unwrap().len(), 2);
    assert!(
        read_readiness(&view, &TrimLimits::default())
            .unwrap()
            .is_empty(),
        "an acknowledgement was read back as a promise"
    );
}

/// A promise moves up and never down, and never holds two checkpoints at
/// one boundary.
///
/// The second rule is the one at most one subject per boundary rests on:
/// two certificates would need two majorities, and those intersect in a
/// voter that would have had to promise twice at one position.
#[test]
fn a_promise_moves_up_never_down_and_never_holds_two_checkpoints_at_one_boundary() {
    let mut engine = store_with_readiness(&[SELF]);

    // The same promise again is the same promise.
    let view = engine.reader().snapshot().unwrap();
    record_readiness(&view, &floor_voters(), &readiness(SELF)).expect("idempotent");

    // Another checkpoint at the same boundary is refused.
    let mut competing = readiness(SELF);
    competing.root = Digest32([0xff; 32]);
    assert_eq!(
        record_readiness(&view, &floor_voters(), &competing),
        Err(TrimError::ReadinessCompeting {
            position: pos(FLOOR_POSITION)
        })
    );
    drop(view);

    // A later boundary is accepted and becomes what is held.
    let mut later = readiness(SELF);
    later.boundary.execution_position = pos(FLOOR_POSITION + 1);
    later.root = Digest32([0x7b; 32]);
    let view = engine.reader().snapshot().unwrap();
    let update = record_readiness(&view, &floor_voters(), &later).unwrap();
    drop(view);
    apply(&mut engine, &[update]);

    // And the earlier one can never come back.
    let view = engine.reader().snapshot().unwrap();
    assert_eq!(
        record_readiness(&view, &floor_voters(), &readiness(SELF)),
        Err(TrimError::ReadinessRegressed {
            held: pos(FLOOR_POSITION + 1),
            offered: pos(FLOOR_POSITION)
        })
    );
    let held = read_readiness(&view, &TrimLimits::default()).unwrap();
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].boundary.execution_position, pos(FLOOR_POSITION + 1));
}

/// An observer or a foreign checkpoint supplies no signature.
#[test]
fn an_observer_or_a_foreign_promise_is_refused_rather_than_counted() {
    let voters = floor_voters();
    let mut observer = readiness(OBSERVER);
    observer.voter = OBSERVER;
    assert_eq!(
        activate_floor(
            &[readiness(SELF), readiness(PEER), observer],
            &voters,
            CLUSTER,
            DOMAIN
        ),
        Err(TrimError::NonVoterReadiness { replica: OBSERVER })
    );
    let engine = voter_store();
    let view = engine.reader().snapshot().unwrap();
    assert_eq!(
        record_readiness(&view, &voters, &observer),
        Err(TrimError::NonVoterReadiness { replica: OBSERVER })
    );

    for (field, mutate) in [
        (
            "cluster",
            (|r: &mut CheckpointReadinessV1| r.cluster = ClusterId([0xee; 16]))
                as fn(&mut CheckpointReadinessV1),
        ),
        ("domain", |r: &mut CheckpointReadinessV1| {
            r.domain = DomainId([0xee; 16])
        }),
    ] {
        let mut stranger = readiness(PEER);
        mutate(&mut stranger);
        assert_eq!(
            activate_floor(&[readiness(SELF), stranger], &voters, CLUSTER, DOMAIN),
            Err(TrimError::OriginMismatch { voter: PEER, field })
        );
    }
}

/// A recovery reads a majority, honours the highest floor it finds, and
/// refuses to answer from a narrower read.
#[test]
fn a_recovery_honours_the_highest_floor_a_majority_reports_and_refuses_a_narrower_read() {
    let voters = floor_voters();
    let promises = vec![readiness(SELF), readiness(PEER)];
    let reports: Vec<RecoveryReport> = promises
        .iter()
        .copied()
        .map(RecoveryReport::promised)
        .collect();

    // A lagging replica must obtain the checkpoint before voting.
    let behind = pos(FLOOR_POSITION - 1);
    let obligation = recovery_obligation(&voters, &reports, behind).unwrap();
    assert_eq!(
        obligation,
        RecoveryObligation::Install {
            position: pos(FLOOR_POSITION),
            subject: readiness(SELF).subject(),
        }
    );

    // One that is already at or beyond the floor owes nothing.
    assert_eq!(
        recovery_obligation(&voters, &reports, pos(FLOOR_POSITION)).unwrap(),
        RecoveryObligation::None
    );

    // The narrower read is refused rather than answered. This is the
    // counterexample task-52 froze: a recovery that answered from one
    // report could miss the highest floor entirely, and the replica
    // that missed it would vote from a baseline the cluster forgot
    // below. One report is one report whatever it says.
    assert_eq!(
        recovery_obligation(
            &voters,
            &[RecoveryReport::promised(readiness(LAGGARD))],
            behind
        ),
        Err(TrimError::NoQuorum { have: 1, need: 2 })
    );
    assert_eq!(
        recovery_obligation(&voters, &[], behind),
        Err(TrimError::NoQuorum { have: 0, need: 2 })
    );
    assert_eq!(
        recovery_obligation(
            &voters,
            &[RecoveryReport::promised(readiness(SELF))],
            behind
        ),
        Err(TrimError::NoQuorum { have: 1, need: 2 })
    );
    assert_eq!(
        recovery_obligation(&voters, &[RecoveryReport::unpromised(LAGGARD)], behind),
        Err(TrimError::NoQuorum { have: 1, need: 2 })
    );

    // Every majority of the voters intersects the signers, so every one
    // of them discovers the floor.
    let certified = activate_floor(&promises, &voters, CLUSTER, DOMAIN).unwrap();
    for majority in [
        BTreeSet::from([SELF, PEER]),
        BTreeSet::from([SELF, LAGGARD]),
        BTreeSet::from([PEER, LAGGARD]),
        voters.voters().clone(),
    ] {
        // The laggard promised nothing and answers so. A majority
        // containing it reports one promise -- but it is a majority of
        // voters answering, which is what the rule requires: the
        // signers this replica can reach may be fewer than a majority
        // while the voters it can reach are not, and it is the voters
        // that are counted.
        let reporting: Vec<RecoveryReport> = majority
            .iter()
            .map(|v| {
                promises
                    .iter()
                    .find(|p| &p.voter == v)
                    .copied()
                    .map_or_else(|| RecoveryReport::unpromised(*v), RecoveryReport::promised)
            })
            .collect();
        let discovered = recovery_obligation(&voters, &reporting, behind).unwrap();
        assert_eq!(
            discovered,
            RecoveryObligation::Install {
                position: certified.boundary.execution_position,
                subject: readiness(SELF).subject(),
            },
            "a majority containing a signer discovered less than the floor"
        );
    }

    // A majority that has promised nothing is a complete answer: nobody
    // has promised, so nothing is owed.
    assert_eq!(
        recovery_obligation(
            &voters,
            &[
                RecoveryReport::unpromised(PEER),
                RecoveryReport::unpromised(LAGGARD)
            ],
            behind
        )
        .unwrap(),
        RecoveryObligation::None
    );
}

/// A promise from another configuration is refused before it is
/// counted, and a report that carries somebody else's promise is not
/// a report.
///
/// A delayed answer from a prior epoch would otherwise satisfy the
/// majority read and could replace the intersecting signer's current
/// promise with an older, lower one -- and the voter set it is evidence
/// about need not intersect this one at all.
#[test]
fn a_recovery_refuses_a_promise_from_another_configuration() {
    let voters = floor_voters();
    let behind = pos(FLOOR_POSITION - 1);

    // The peer's stale answer from the prior epoch names a lower floor.
    let mut stale = readiness(PEER);
    stale.configuration = epoch(EPOCH - 1);
    stale.boundary.execution_position = pos(FLOOR_POSITION - 1);
    assert_eq!(
        recovery_obligation(
            &voters,
            &[
                RecoveryReport::promised(readiness(SELF)),
                RecoveryReport::promised(stale)
            ],
            behind
        ),
        Err(TrimError::FloorOriginMismatch {
            field: "configuration"
        })
    );

    // It is refused even when it would not change the answer: it is
    // rejected as evidence, not weighed.
    assert_eq!(
        recovery_obligation(
            &voters,
            &[
                RecoveryReport::promised(readiness(SELF)),
                RecoveryReport::promised(readiness(LAGGARD)),
                RecoveryReport::promised(stale)
            ],
            behind
        ),
        Err(TrimError::FloorOriginMismatch {
            field: "configuration"
        })
    );

    // A report naming one voter and carrying another's promise is
    // malformed rather than counted for either.
    let relayed = RecoveryReport {
        voter: LAGGARD,
        readiness: Some(readiness(SELF)),
    };
    assert!(matches!(
        recovery_obligation(
            &voters,
            &[RecoveryReport::promised(readiness(SELF)), relayed],
            behind
        ),
        Err(TrimError::Engine(_))
    ));
}

/// The published certificate never moves backwards, and a disagreement
/// at one boundary stops rather than choosing.
#[test]
fn the_published_certificate_never_moves_backwards() {
    let voters = floor_voters();
    let certified = activate_floor(
        &[readiness(SELF), readiness(PEER)],
        &voters,
        CLUSTER,
        DOMAIN,
    )
    .unwrap();

    let mut earlier = certified.clone();
    earlier.boundary.execution_position = pos(FLOOR_POSITION - 1);
    assert_eq!(
        publish_activation(&earlier, Some(&certified)),
        Err(TrimError::FloorRegressed {
            published: pos(FLOOR_POSITION),
            offered: pos(FLOOR_POSITION - 1)
        })
    );

    let mut other_root = certified.clone();
    other_root.root = Digest32([0xff; 32]);
    assert_eq!(
        publish_activation(&other_root, Some(&certified)),
        Err(TrimError::FloorConflict)
    );

    let mut elsewhere = certified.clone();
    elsewhere.domain = DomainId([0xee; 16]);
    assert_eq!(
        publish_activation(&elsewhere, Some(&certified)),
        Err(TrimError::FloorOriginMismatch { field: "domain" })
    );

    // Republishing the same certificate is not a regression.
    publish_activation(&certified, Some(&certified)).expect("idempotent");
}

/// A crash between the certificate and the floor deletes nothing, and
/// the next attempt is the same attempt.
///
/// The order is the safety: the certificate is evidence, the floor is
/// the authorization, and the deletions come last. Stopping anywhere in
/// between leaves a store that still holds everything it held.
#[test]
fn a_crash_between_the_certificate_and_the_floor_deletes_nothing() {
    let mut engine = store_with_readiness(&[SELF, PEER]);
    let before = protocol_keys(&engine);

    let view = engine.reader().snapshot().unwrap();
    let promises = read_readiness(&view, &TrimLimits::default()).unwrap();
    let certified = activate_floor(&promises, &floor_voters(), CLUSTER, DOMAIN).unwrap();
    let cert_update =
        publish_activation(&certified, published_activation(&view).unwrap().as_ref()).unwrap();
    drop(view);
    apply(&mut engine, &[cert_update]);

    // The certificate is durable and nothing is authorized yet.
    let view = engine.reader().snapshot().unwrap();
    assert_eq!(
        published_activation(&view).unwrap(),
        Some(certified.clone())
    );
    assert_eq!(published_floor(&view).unwrap(), None);
    drop(view);
    assert_eq!(protocol_keys(&engine), before);

    // The next attempt recomputes the same certificate from the same
    // promises and publishes the floor.
    let view = engine.reader().snapshot().unwrap();
    let again = activate_floor(
        &read_readiness(&view, &TrimLimits::default()).unwrap(),
        &floor_voters(),
        CLUSTER,
        DOMAIN,
    )
    .unwrap();
    assert_eq!(again, certified);
    let floor_update = publish_floor(
        &again.trim_floor(),
        published_floor(&view).unwrap().as_ref(),
    )
    .unwrap();
    drop(view);
    apply(&mut engine, &[floor_update]);
    trim_to_completion(&mut engine, &TrimLimits::default());
    assert!(protocol_keys(&engine).len() < before.len());
}

/// Promise rows are read back from durable state and never confused with
/// the other `checkpoint_v1` records.
#[test]
fn promises_are_read_back_from_durable_rows_and_not_confused_with_other_records() {
    let mut engine = store_with_readiness(&[SELF, PEER, LAGGARD]);
    // An acknowledgement, a published floor and a certificate all live
    // in the same collection and none of them is a promise.
    let view = engine.reader().snapshot().unwrap();
    let certified = activate_floor(
        &read_readiness(&view, &TrimLimits::default()).unwrap(),
        &floor_voters(),
        CLUSTER,
        DOMAIN,
    )
    .unwrap();
    let batch = vec![
        ack_update(&ack(SELF)).unwrap(),
        publish_activation(&certified, None).unwrap(),
        publish_floor(&certified.trim_floor(), None).unwrap(),
    ];
    drop(view);
    apply(&mut engine, &batch);

    let view = engine.reader().snapshot().unwrap();
    let promises = read_readiness(&view, &TrimLimits::default()).unwrap();
    assert_eq!(
        promises.iter().map(|p| p.voter).collect::<BTreeSet<_>>(),
        voters()
    );
    // Keys of the four record families are distinct and ordered, which
    // is what lets one prefix scan end at the next family.
    assert!(readiness_key(&SELF).starts_with(READINESS_KEY_PREFIX));
    assert!(ack_key(&SELF).starts_with(ACK_KEY_PREFIX));
    assert!(ACK_KEY_PREFIX < ACTIVATION_KEY);
    assert!(ACTIVATION_KEY < READINESS_KEY_PREFIX);
    assert!(READINESS_KEY_PREFIX < FLOOR_KEY);

    // A promise row whose value is another record is corruption, not an
    // absent promise.
    apply(
        &mut engine,
        &[StoreUpdate {
            collection: Collection::CheckpointV1.id(),
            key: readiness_key(&PEER),
            value: Some(ack(PEER).encode().unwrap()),
        }],
    );
    let view = engine.reader().snapshot().unwrap();
    assert!(read_readiness(&view, &TrimLimits::default()).is_err());
}

/// A promise binds the boundary, not the moment: it round-trips through
/// the store and through the manifest a voter verified.
#[test]
fn a_promise_is_built_from_what_a_voter_verified_and_round_trips() {
    let engine = voter_store();
    let view = engine.reader().snapshot().unwrap();
    let manifest = export_shared(
        &view,
        CheckpointOrigin {
            cluster: CLUSTER,
            domain: DOMAIN,
        },
        &ExportLimits::default(),
    )
    .unwrap()
    .manifest;
    let promise = CheckpointReadinessV1::for_manifest(SELF, &manifest);
    assert_eq!(promise.root, manifest.root);
    assert_eq!(promise.boundary, manifest.boundary);
    let encoded = promise.encode().unwrap();
    assert_eq!(CheckpointReadinessV1::decode(&encoded).unwrap(), promise);
    // A promise and an acknowledgement about the same checkpoint are
    // different records, so neither decodes as the other.
    assert!(CheckpointAckV1::decode(&encoded).is_err());
    assert!(CheckpointReadinessV1::decode(&ack(SELF).encode().unwrap()).is_err());
    // The subject is the whole checkpoint identity: changing any part of
    // it changes the subject.
    let base = promise.subject();
    for mutate in [
        (|p: &mut CheckpointReadinessV1| p.cluster = ClusterId([0xee; 16]))
            as fn(&mut CheckpointReadinessV1),
        |p: &mut CheckpointReadinessV1| p.domain = DomainId([0xee; 16]),
        |p: &mut CheckpointReadinessV1| p.configuration = epoch(EPOCH + 1),
        |p: &mut CheckpointReadinessV1| p.boundary.execution_position = pos(99),
        |p: &mut CheckpointReadinessV1| p.boundary.kv_revision = rev(99),
        |p: &mut CheckpointReadinessV1| p.boundary.retention_floor = rev(1),
        |p: &mut CheckpointReadinessV1| {
            p.boundary.lease_authority = LeaseAuthorityEpoch::new(9).unwrap()
        },
        |p: &mut CheckpointReadinessV1| p.root = Digest32([0xff; 32]),
    ] {
        let mut changed = promise;
        mutate(&mut changed);
        assert_ne!(changed.subject(), base);
    }
    // The voter is not part of the subject: signers agree on a
    // checkpoint, not on each other.
    let mut other_voter = promise;
    other_voter.voter = PEER;
    assert_eq!(other_voter.subject(), base);
}
