//! The unique handoff certificate: what a successor inherits, how the
//! old quorum concludes it, and why a restart cannot conclude it twice
//! (task-56; design Sections 4.8-4.9, 10.3, 17.6).
//!
//! Task-54's model says what may conclude a terminal state; this is the
//! durable form of that conclusion, so the tests are about the two
//! things the model cannot check: what the root is actually derived
//! from, and what a row does when a second coordinator writes to it.

use std::collections::{BTreeMap, BTreeSet};

use coord_checkpoint::handoff::{
    TERMINAL_KEY, TerminalCertificateV1, TerminalStateV1, closure_root, publish_certificate,
    published_certificate, select_certificate,
};
use coord_checkpoint::manifest::CheckpointBoundary;
use coord_checkpoint::trim::TrimError;
use coord_consensus::Phase;
use coord_consensus::handoff::{HandoffError, Stance, StanceRecord, Transition, seal};
use coord_consensus::quorum::EpochVoters;
use coord_consensus::recovery::{SyncDecision, SyncEntry};
use coord_core::effect::StoreUpdate;
use coord_store_api::engine::{LocalEngine, SnapshotSource, WriteTxn};
use coord_store_api::registry::Collection;
use coord_store_testkit::model::ModelEngine;
use coord_types::CommandId;
use coord_types::identity::Digest32;
use coord_types::ids::*;

const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);
const OLD: u64 = 4;
const NEW: u64 = 5;

fn r(i: u8) -> ReplicaId {
    ReplicaId([i; 16])
}

fn epoch(n: u64) -> ConfigurationEpoch {
    ConfigurationEpoch::new(n).unwrap()
}

fn inc(n: u64) -> ReplicaIncarnation {
    ReplicaIncarnation::new(n).unwrap()
}

fn old_voters() -> EpochVoters {
    EpochVoters::new(epoch(OLD), (0..3).map(r).collect()).unwrap()
}

fn transition() -> Transition {
    Transition {
        from: epoch(OLD),
        to: epoch(NEW),
        subject: Digest32([0xa1; 32]),
    }
}

fn boundary() -> CheckpointBoundary {
    CheckpointBoundary {
        execution_position: ExecutionPosition::new(12).unwrap(),
        kv_revision: KvRevision::new(30).unwrap(),
        retention_floor: KvRevision::new(8).unwrap(),
        lease_authority: LeaseAuthorityEpoch::new(2).unwrap(),
    }
}

fn successors() -> BTreeMap<ReplicaId, ReplicaIncarnation> {
    [(r(2), inc(1)), (r(3), inc(1)), (r(4), inc(7))]
        .into_iter()
        .collect()
}

fn command(i: u8) -> CommandId {
    CommandId(Digest32([i; 32]))
}

fn ballot(number: u64) -> Ballot {
    Ballot {
        epoch: epoch(OLD),
        number,
        leader: r(0),
    }
}

/// A selection over the reports at the seal cut, with `commands`
/// adopted.
fn selection(commands: &[u8]) -> SyncDecision {
    SyncDecision {
        ballot: ballot(9),
        source_ballot: ballot(3),
        entries: commands
            .iter()
            .map(|i| {
                (
                    command(*i),
                    SyncEntry {
                        command: command(*i),
                        phase: Phase::Commit,
                        deps: Vec::new(),
                        path: coord_consensus::graph::empty_path(),
                        paths: Vec::new(),
                        seqnum: u64::from(*i),
                    },
                )
            })
            .collect(),
        reproposed: BTreeSet::new(),
    }
}

fn state(commands: &[u8]) -> TerminalStateV1 {
    TerminalStateV1 {
        cluster: CLUSTER,
        domain: DOMAIN,
        transition: transition(),
        boundary: boundary(),
        state_root: Digest32([0x5a; 32]),
        closure_root: closure_root(&selection(commands)),
        floor: Digest32([0x7b; 32]),
        successors: successors(),
    }
}

fn sealed() -> coord_consensus::handoff::SealCertificate {
    let stances: Vec<StanceRecord> = (0..2)
        .map(|i| StanceRecord {
            voter: r(i),
            transition: transition(),
            stance: Stance::Sealed,
        })
        .collect();
    seal(&old_voters(), transition(), &stances).expect("a majority sealed")
}

fn store() -> ModelEngine {
    ModelEngine::new()
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

/// Everything the successor inherits is inside the root.
///
/// The point of binding all of it is that "mixed evidence" stops being
/// a judgement call: two old voters either produce the same 32 bytes or
/// they do not, and every difference that matters produces different
/// bytes.
#[test]
fn the_terminal_root_binds_everything_the_successor_inherits() {
    let base = state(&[1, 2]);
    let root = base.terminal_root();
    for (what, mutate) in [
        (
            "cluster",
            (|s: &mut TerminalStateV1| s.cluster = ClusterId([0xee; 16]))
                as fn(&mut TerminalStateV1),
        ),
        ("domain", |s: &mut TerminalStateV1| {
            s.domain = DomainId([0xee; 16])
        }),
        ("the transition", |s: &mut TerminalStateV1| {
            s.transition.subject = Digest32([0xee; 32])
        }),
        ("the successor epoch", |s: &mut TerminalStateV1| {
            s.transition.to = epoch(NEW + 1)
        }),
        ("the execution boundary", |s: &mut TerminalStateV1| {
            s.boundary.execution_position = ExecutionPosition::new(13).unwrap()
        }),
        ("the revision boundary", |s: &mut TerminalStateV1| {
            s.boundary.kv_revision = KvRevision::new(31).unwrap()
        }),
        ("the retention floor", |s: &mut TerminalStateV1| {
            s.boundary.retention_floor = KvRevision::new(9).unwrap()
        }),
        ("the lease authority", |s: &mut TerminalStateV1| {
            s.boundary.lease_authority = LeaseAuthorityEpoch::new(3).unwrap()
        }),
        ("the common state", |s: &mut TerminalStateV1| {
            s.state_root = Digest32([0xee; 32])
        }),
        ("the closure", |s: &mut TerminalStateV1| {
            s.closure_root = Digest32([0xee; 32])
        }),
        ("the floor lineage", |s: &mut TerminalStateV1| {
            s.floor = Digest32([0xee; 32])
        }),
        ("a successor identity", |s: &mut TerminalStateV1| {
            s.successors.remove(&r(4));
            s.successors.insert(r(5), inc(1));
        }),
        ("a successor incarnation", |s: &mut TerminalStateV1| {
            s.successors.insert(r(4), inc(8));
        }),
    ] {
        let mut changed = base.clone();
        mutate(&mut changed);
        assert_ne!(
            changed.terminal_root(),
            root,
            "changing {what} left the terminal root alone"
        );
    }

    // A latent old completion stays represented: a command chosen
    // immediately before the fence is in the selection, and a terminal
    // state that omitted it is a different terminal state.
    assert_ne!(
        state(&[1, 2]).terminal_root(),
        state(&[1]).terminal_root(),
        "dropping a potentially chosen command did not change the terminal state"
    );
    assert_eq!(
        state(&[1, 2]).terminal_root(),
        state(&[2, 1]).terminal_root(),
        "the selection's order changed its digest"
    );
}

/// A majority of the sealed old voters reporting one state is the
/// certificate; anything less, or anything mixed, is not.
#[test]
fn a_majority_of_sealed_voters_agreeing_is_the_certificate() {
    let voters = old_voters();
    let seal = sealed();
    let agreed = state(&[1, 2]);

    let certificate = select_certificate(
        &voters,
        &seal,
        &[(r(0), agreed.clone()), (r(1), agreed.clone())],
    )
    .expect("two of three");
    assert_eq!(certificate.signers, BTreeSet::from([r(0), r(1)]));
    assert_eq!(certificate.terminal_root(), agreed.terminal_root());
    assert_eq!(certificate.state, agreed);
    assert_eq!(
        certificate.successor_voters().unwrap().voters(),
        &successors().keys().copied().collect::<BTreeSet<_>>()
    );

    // One report is not a majority.
    assert_eq!(
        select_certificate(&voters, &seal, &[(r(0), agreed.clone())]),
        Err(TrimError::Handoff(HandoffError::NoQuorum {
            have: 1,
            need: 2
        }))
    );

    // Mixed roots are refused, never merged and never picked between.
    assert_eq!(
        select_certificate(
            &voters,
            &seal,
            &[(r(0), agreed.clone()), (r(1), state(&[1]))]
        ),
        Err(TrimError::Handoff(HandoffError::MixedTerminal))
    );

    // Racing successor sets differ in the root, so they cannot both be
    // certified -- and offered together they are simply mixed evidence.
    let mut other_successor = agreed.clone();
    other_successor.successors.insert(r(6), inc(1));
    assert_ne!(other_successor.terminal_root(), agreed.terminal_root());
    assert_eq!(
        select_certificate(&voters, &seal, &[(r(0), agreed), (r(1), other_successor)]),
        Err(TrimError::Handoff(HandoffError::MixedTerminal))
    );

    // A report about another transition is not about this one.
    let elsewhere = TerminalStateV1 {
        transition: Transition {
            subject: Digest32([0xb2; 32]),
            ..transition()
        },
        ..state(&[1, 2])
    };
    assert_eq!(
        select_certificate(&voters, &seal, &[(r(0), elsewhere)]),
        Err(TrimError::Handoff(HandoffError::WrongTransition))
    );

    // An observer's report supplies no signature.
    assert_eq!(
        select_certificate(&voters, &seal, &[(r(9), state(&[1, 2]))]),
        Err(TrimError::Handoff(HandoffError::NotAVoter {
            replica: r(9)
        }))
    );
}

/// The published certificate is the selection: it republishes and it
/// never changes.
///
/// This is what "selection stable across restart" is: the replacement
/// coordinator reuses the decision because the row refuses to become a
/// different one, so a restart in the middle of a handoff cannot
/// produce a second destination.
#[test]
fn a_published_certificate_republishes_and_never_becomes_another_one() {
    let voters = old_voters();
    let seal = sealed();
    let agreed = state(&[1, 2]);
    let certificate = select_certificate(
        &voters,
        &seal,
        &[(r(0), agreed.clone()), (r(1), agreed.clone())],
    )
    .unwrap();

    let mut engine = store();
    let view = engine.reader().snapshot().unwrap();
    assert_eq!(published_certificate(&view).unwrap(), None);
    let update = publish_certificate(&certificate, None).unwrap();
    assert_eq!(update.key, TERMINAL_KEY);
    assert_eq!(update.collection, Collection::CheckpointV1.id());
    drop(view);
    apply(&mut engine, &[update]);

    let view = engine.reader().snapshot().unwrap();
    let published = published_certificate(&view).unwrap().expect("published");
    assert_eq!(published, certificate);

    // The same certificate again is the same certificate.
    publish_certificate(&certificate, Some(&published)).expect("idempotent");

    // Any other one is refused, including one that differs only in
    // which commands the closure resolved.
    let other =
        select_certificate(&voters, &seal, &[(r(0), state(&[1])), (r(1), state(&[1]))]).unwrap();
    assert_ne!(other.terminal_root(), published.terminal_root());
    assert_eq!(
        publish_certificate(&other, Some(&published)),
        Err(TrimError::Handoff(HandoffError::MixedTerminal))
    );

    // And a certificate of another transition is not a republication of
    // this one.
    let elsewhere = TerminalCertificateV1 {
        state: TerminalStateV1 {
            transition: Transition {
                subject: Digest32([0xb2; 32]),
                ..transition()
            },
            ..published.state.clone()
        },
        signers: published.signers.clone(),
    };
    assert_eq!(
        publish_certificate(&elsewhere, Some(&published)),
        Err(TrimError::Handoff(HandoffError::WrongTransition))
    );
}

/// The certificate round-trips through the store and is not confused
/// with the other `checkpoint_v1` records.
#[test]
fn the_certificate_round_trips_and_is_its_own_record() {
    let agreed = state(&[1, 2]);
    let certificate = TerminalCertificateV1 {
        state: agreed,
        signers: BTreeSet::from([r(0), r(1)]),
    };
    let encoded = certificate.encode().unwrap();
    assert_eq!(
        TerminalCertificateV1::decode(&encoded).unwrap(),
        certificate
    );
    // A checkpoint acknowledgement is not a terminal certificate.
    assert!(coord_checkpoint::trim::CheckpointAckV1::decode(&encoded).is_err());
    assert!(
        TerminalCertificateV1::decode(
            &coord_checkpoint::trim::CheckpointAckV1 {
                voter: r(0),
                cluster: CLUSTER,
                domain: DOMAIN,
                configuration: epoch(OLD),
                boundary: boundary(),
                root: Digest32([0x5a; 32]),
            }
            .encode()
            .unwrap()
        )
        .is_err()
    );
}

/// Only voters that sealed prove the certificate.
///
/// The failing sequence, with old voters A, B and C: A and B seal, so
/// the seal certificate's signers are {A, B}. C has not sealed, reports
/// a terminal state selected over {B, C} without a command X that A
/// accepted, and can then still accept X, which {A, C} chose. Counting
/// C's report would certify a terminal state that omits a chosen
/// command. C counts only once its own seal is in evidence -- certified
/// again with its record among the stances.
#[test]
fn a_certificate_is_proved_only_by_voters_that_sealed() {
    let voters = old_voters();
    let seal_ab = sealed();
    assert_eq!(seal_ab.signers(), &BTreeSet::from([r(0), r(1)]));
    let without_x = state(&[1]);
    assert_eq!(
        select_certificate(
            &voters,
            &seal_ab,
            &[(r(1), without_x.clone()), (r(2), without_x.clone())]
        ),
        Err(TrimError::Handoff(HandoffError::ReporterNotSealed {
            replica: r(2)
        }))
    );

    // C seals late: the seal certified over its record too makes it a
    // signer, and its report then counts.
    let stances: Vec<StanceRecord> = (0..3)
        .map(|i| StanceRecord {
            voter: r(i),
            transition: transition(),
            stance: Stance::Sealed,
        })
        .collect();
    let seal_abc = seal(&voters, transition(), &stances).unwrap();
    let certificate = select_certificate(
        &voters,
        &seal_abc,
        &[(r(1), without_x.clone()), (r(2), without_x)],
    )
    .expect("both reporters are fenced");
    assert_eq!(certificate.signers, BTreeSet::from([r(1), r(2)]));
}

/// The closure binds what was selected, not the ballot the selection ran
/// under.
///
/// A replacement coordinator repeating the same terminal recovery after
/// its predecessor died runs it under a higher ballot. The commands,
/// their phases, dependencies, paths and the source they were selected
/// from are the same, so the old voters must agree on one root rather
/// than be refused as mixed evidence.
#[test]
fn the_closure_binds_the_selection_and_not_the_recovery_ballot() {
    let first = selection(&[1, 2]);
    let mut replacement = first.clone();
    replacement.ballot = ballot(12);
    assert_eq!(closure_root(&first), closure_root(&replacement));

    let mut state_first = state(&[1, 2]);
    state_first.closure_root = closure_root(&first);
    let mut state_replacement = state(&[1, 2]);
    state_replacement.closure_root = closure_root(&replacement);
    select_certificate(
        &old_voters(),
        &sealed(),
        &[(r(0), state_first), (r(1), state_replacement)],
    )
    .expect("one terminal state, whoever asked for it");

    // What the selection decided still moves it.
    let mut source = first.clone();
    source.source_ballot = ballot(4);
    assert_ne!(closure_root(&first), closure_root(&source));
    let mut reproposed = first.clone();
    reproposed.reproposed.insert(command(7));
    assert_ne!(closure_root(&first), closure_root(&reproposed));
}
