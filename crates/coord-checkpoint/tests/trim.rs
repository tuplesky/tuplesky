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
use coord_checkpoint::manifest::{CheckpointBoundary, SharedManifestV1};
use coord_checkpoint::trim::{
    ACK_KEY_PREFIX, CheckpointAckV1, FLOOR_KEY, FenceDecision, TrimError, TrimFence, TrimFloor,
    TrimLimits, TrimPlan, TrimmedFloorV1, ack_key, ack_update, establish_floor, plan_trim,
    publish_floor, published_floor, read_acks, trim_backpressure,
};
use coord_consensus::recovery::SyncDecision;
use coord_consensus::rows::{
    PromiseRecordV1, ProposalRecordV1, SyncRecordV1, dependency_key, encode_dependency,
    encode_promise, encode_proposal, encode_sync, promise_key, proposal_key, sync_key,
};
use coord_consensus::{CommandRecord, Phase};
use coord_core::effect::StoreUpdate;
use coord_state::view::KvEntry;
use coord_storage::codecs::{self, ExecutedRecordV1, HistoryRecordV1};
use coord_storage::compaction::{GcBudget, RetentionHolds, plan_gc};
use coord_storage::lowering::{DurableMeta, ExecutionFrontier};
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

fn command(n: u8) -> CommandId {
    CommandId(Digest32([n; 32]))
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

fn record(phase: Phase, deps: &[u8]) -> CommandRecord {
    CommandRecord {
        phase,
        deps: deps.iter().copied().map(command).collect(),
        keys: vec![b"k".to_vec()],
        payload: Some(Digest32([0x11; 32])),
        paths: vec![(b"k".to_vec(), Digest32([0x22; 32]))],
        path: Digest32([0x33; 32]),
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
    for (n, position) in [(1u8, 1u64), (2, 2), (3, 3), (5, 4)] {
        put(
            Collection::PayloadV1,
            command(n).as_bytes().to_vec(),
            format!("payload-{n}").into_bytes(),
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
    // Settled below the boundary, depending on each other in a chain.
    for (n, deps) in [(1u8, &[][..]), (2, &[1][..]), (3, &[2][..])] {
        put(
            dependency_key(e, &command(n)),
            encode_dependency(&record(Phase::Executed, deps)).unwrap(),
        );
        put(
            proposal_key(e, &command(n)),
            encode_proposal(&proposal(deps)).unwrap(),
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
        encode_dependency(&record(Phase::Executed, &[])).unwrap(),
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
        encode_dependency(&record(Phase::Executed, &[])).unwrap(),
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
        stamp: AppliedStamp {
            store_seq: StoreSeq::from_journal(seq),
            journal_seq: seq,
            last_batch_digest: Digest32([0xcd; 32]),
        },
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
    let mut engine = voter_store();
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

    // Commands 1 and 2 are executed at or below the boundary and nothing
    // retained names them, so their dependency and proposal rows go.
    for n in [1u8, 2] {
        assert!(
            !after.contains(&dependency_key(e, &command(n))),
            "command {n} is settled below the floor"
        );
        assert!(!after.contains(&proposal_key(e, &command(n))));
    }

    // Everything else stays: the promise, the bound Sync, the accepted but
    // unexecuted command, the settled command it still names, the command
    // committed but not executed, the command executed beyond the boundary,
    // and a command of an epoch above the floor's.
    for retained in [
        promise_key(e),
        sync_key(e, &ballot(3)),
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
    let engine = voter_store();
    let plan = step(&engine, &TrimLimits::default());
    let e = epoch(EPOCH);

    // Command 4 is accepted and unexecuted and names command 3, so command
    // 3's rows are held back although command 3 is settled below the floor.
    assert_eq!(
        plan.pinned, 2,
        "the dependency and proposal rows of command 3"
    );
    let deleted: BTreeSet<Vec<u8>> = plan.updates.iter().map(|u| u.key.clone()).collect();
    assert!(!deleted.contains(&dependency_key(e, &command(3))));
    assert!(!deleted.contains(&proposal_key(e, &command(3))));
    // The pin does not spread further than the closure needs: command 2 is
    // named only by command 3, which is itself settled below the floor.
    assert!(deleted.contains(&dependency_key(e, &command(2))));

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
        encode_dependency(&record(Phase::Executed, &[3])).unwrap(),
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
    let plan = step(&settled, &TrimLimits::default());
    let deleted: BTreeSet<Vec<u8>> = plan.updates.iter().map(|u| u.key.clone()).collect();
    assert_eq!(plan.pinned, 0);
    assert!(deleted.contains(&dependency_key(e, &command(3))));
    assert!(deleted.contains(&proposal_key(e, &command(3))));
    assert!(deleted.contains(&dependency_key(e, &command(4))));
}

#[test]
fn a_trim_step_is_bounded_and_repeating_it_converges_on_the_same_retained_state() {
    let mut whole = voter_store();
    let mut piecemeal = voter_store();
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
    let engine = voter_store();
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
    let mut engine = voter_store();
    trim_to_completion(&mut engine, &TrimLimits::default());
    let fence = TrimFence::new(published());
    let view = engine.reader().snapshot().unwrap();

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
    let engine = voter_store();
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
    let mut engine = voter_store();
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
    let mut engine = voter_store();
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
