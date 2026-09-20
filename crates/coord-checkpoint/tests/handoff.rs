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

// ---------------------------------------------------------------------
// task-57: installing the terminal state and activating the successor,
// with the coordinator dying at every point it could.
// ---------------------------------------------------------------------

use coord_checkpoint::handoff::{
    HANDOFF_ACTIVATION_KEY, HandoffActivationV1, INSTALL_KEY_PREFIX, LocalEvidence,
    TerminalInstallV1, activate_successor, install_key, publish_handoff_activation,
    published_activation, read_installs, record_install,
};
use coord_checkpoint::install::InstalledCheckpointV1;
use coord_checkpoint::trim::TrimLimits;
use coord_consensus::handoff::{Evidence, Stage, resume};

fn successor_voters() -> EpochVoters {
    EpochVoters::new(epoch(NEW), successors().keys().copied().collect()).unwrap()
}

/// The receipt a successor replica has after installing the terminal
/// state.
fn receipt() -> InstalledCheckpointV1 {
    InstalledCheckpointV1 {
        format: 1,
        cluster: CLUSTER,
        domain: DOMAIN,
        configuration: epoch(OLD),
        boundary: boundary(),
        root: Digest32([0x5a; 32]),
        chunks: 2,
        rows: 40,
    }
}

fn certificate() -> TerminalCertificateV1 {
    let agreed = state(&[1, 2]);
    select_certificate(
        &old_voters(),
        &sealed(),
        &[(r(0), agreed.clone()), (r(1), agreed)],
    )
    .expect("a majority")
}

/// The stances a majority of the old voters durably hold.
fn stances() -> Vec<StanceRecord> {
    (0..2)
        .map(|i| StanceRecord {
            voter: r(i),
            transition: transition(),
            stance: Stance::Sealed,
        })
        .collect()
}

/// Where a coordinator reading `engine` would resume.
fn stage_of<E: LocalEngine>(engine: &E) -> Stage {
    let view = engine.reader().snapshot().unwrap();
    let local = LocalEvidence::read(&view, &TrimLimits::default()).unwrap();
    let (certificate, installs, activation) = local.records();
    let stances = stances();
    resume(
        &old_voters(),
        &successor_voters(),
        transition(),
        Evidence {
            authorized: true,
            stances: &stances,
            reports: &[],
            terminal: certificate.as_ref(),
            installs: &installs,
            activation: activation.as_ref(),
        },
    )
    .expect("the evidence is consistent")
}

/// An installation record is evidence of holding the state, not of
/// having been told to say so.
#[test]
fn an_installation_record_comes_from_a_receipt_and_only_from_the_successor() {
    let certificate = certificate();

    // A replica outside the successor set is not part of the quorum
    // that activates, and counting it would let a bystander stand in
    // for a member holding nothing.
    assert_eq!(
        record_install(&certificate, r(0), &receipt()),
        Err(TrimError::Handoff(HandoffError::NotAVoter {
            replica: r(0)
        }))
    );

    // A receipt for other bytes is not an install of this state.
    for mutate in [
        (|rec: &mut InstalledCheckpointV1| rec.root = Digest32([0xee; 32]))
            as fn(&mut InstalledCheckpointV1),
        |rec: &mut InstalledCheckpointV1| {
            rec.boundary.execution_position = ExecutionPosition::new(13).unwrap()
        },
        |rec: &mut InstalledCheckpointV1| {
            rec.boundary.retention_floor = KvRevision::new(9).unwrap()
        },
    ] {
        let mut wrong = receipt();
        mutate(&mut wrong);
        assert_eq!(
            record_install(&certificate, r(3), &wrong),
            Err(TrimError::Handoff(HandoffError::WrongTerminalRoot))
        );
    }

    // The right receipt writes the record, under the replica's own key.
    let update = record_install(&certificate, r(3), &receipt()).unwrap();
    assert_eq!(update.key, install_key(&r(3)));
    assert!(update.key.starts_with(INSTALL_KEY_PREFIX));
    let record = TerminalInstallV1::decode(update.value.as_ref().unwrap()).unwrap();
    assert_eq!(record.replica, r(3));
    assert_eq!(record.terminal_root, certificate.terminal_root());
}

/// A majority of the successor holding the state is the activation, and
/// a minority is not.
#[test]
fn a_majority_of_the_successor_activates_and_a_minority_does_not() {
    let certificate = certificate();
    let installs: Vec<TerminalInstallV1> = successors()
        .keys()
        .map(|replica| TerminalInstallV1 {
            replica: *replica,
            transition: transition(),
            terminal_root: certificate.terminal_root(),
        })
        .collect();

    assert_eq!(
        activate_successor(&certificate, &installs[..1]),
        Err(TrimError::Handoff(HandoffError::NoQuorum {
            have: 1,
            need: 2
        }))
    );
    let activation = activate_successor(&certificate, &installs[..2]).expect("two of three");
    assert_eq!(activation.terminal_root, certificate.terminal_root());
    assert_eq!(activation.transition, transition());
    assert_eq!(activation.installers.len(), 2);

    // An installation of another history is refused rather than
    // counted: the successor set is the certificate's, and so is the
    // root.
    let mut other = installs.clone();
    other[0].terminal_root = Digest32([0xee; 32]);
    assert_eq!(
        activate_successor(&certificate, &other),
        Err(TrimError::Handoff(HandoffError::WrongTerminalRoot))
    );
    let mut stranger = installs.clone();
    stranger[0].replica = r(9);
    assert_eq!(
        activate_successor(&certificate, &stranger),
        Err(TrimError::Handoff(HandoffError::NotAVoter {
            replica: r(9)
        }))
    );
}

/// The whole handoff, with the coordinator dying after every durable
/// step, resuming from the rows each time.
///
/// This is the acceptance criterion that matters: "recover every phase".
/// Nothing in the loop remembers which step it is on -- the stage comes
/// out of the store each time -- and the decisions already made are
/// reused rather than recomputed.
#[test]
fn every_phase_of_the_handoff_resumes_from_the_rows_and_reuses_its_decisions() {
    let mut engine = store();
    let certificate = certificate();

    // Sealed, nothing else: the coordinator resumes at terminal
    // recovery, because the fence is durable and no certificate is.
    assert_eq!(stage_of(&engine), Stage::TerminalRecovery);

    // The certificate is published, and the coordinator dies.
    let view = engine.reader().snapshot().unwrap();
    let update =
        publish_certificate(&certificate, published_certificate(&view).unwrap().as_ref()).unwrap();
    drop(view);
    apply(&mut engine, &[update]);
    assert_eq!(stage_of(&engine), Stage::Installing);

    // A replacement recomputes the selection from the same reports and
    // gets the same certificate; publishing it again is a no-op.
    let view = engine.reader().snapshot().unwrap();
    let published = published_certificate(&view).unwrap().expect("published");
    assert_eq!(published, certificate);
    let agreed = state(&[1, 2]);
    let again = select_certificate(
        &old_voters(),
        &sealed(),
        &[(r(1), agreed.clone()), (r(0), agreed)],
    )
    .unwrap();
    assert_eq!(again.terminal_root(), published.terminal_root());
    publish_certificate(&again, Some(&published)).expect("the same certificate");
    drop(view);

    // One successor replica installs, and the coordinator dies. Still
    // installing: one of three is not a majority.
    let update = record_install(&published, r(3), &receipt()).unwrap();
    apply(&mut engine, &[update]);
    assert_eq!(stage_of(&engine), Stage::Installing);

    // The second installs. Now a majority holds the state and the
    // activation is owed, but it is not granted until it is durable.
    let update = record_install(&published, r(4), &receipt()).unwrap();
    apply(&mut engine, &[update]);
    assert_eq!(stage_of(&engine), Stage::Activating);
    let view = engine.reader().snapshot().unwrap();
    assert_eq!(published_activation(&view).unwrap(), None);
    drop(view);

    // A new node restarting changes nothing: the records are the same
    // records.
    let view = engine.reader().snapshot().unwrap();
    let installs = read_installs(&view, &TrimLimits::default()).unwrap();
    assert_eq!(
        installs.iter().map(|i| i.replica).collect::<BTreeSet<_>>(),
        BTreeSet::from([r(3), r(4)])
    );
    let activation = activate_successor(&published, &installs).unwrap();
    let update = publish_handoff_activation(&activation, None).unwrap();
    assert_eq!(update.key, HANDOFF_ACTIVATION_KEY);
    drop(view);
    apply(&mut engine, &[update]);
    assert_eq!(stage_of(&engine), Stage::Served);

    // A duplicate activation is a no-op, not a second grant of
    // authority: the coordinator that lost its reply retries safely.
    let view = engine.reader().snapshot().unwrap();
    let published_activation = published_activation(&view).unwrap().expect("activated");
    publish_handoff_activation(&activation, Some(&published_activation)).expect("idempotent");
    assert_eq!(published_activation, activation);

    // And an activation of another history never replaces it.
    let other = HandoffActivationV1 {
        terminal_root: Digest32([0xee; 32]),
        ..activation.clone()
    };
    assert_eq!(
        publish_handoff_activation(&other, Some(&published_activation)),
        Err(TrimError::Handoff(HandoffError::WrongTerminalRoot))
    );
    let elsewhere = HandoffActivationV1 {
        transition: Transition {
            subject: Digest32([0xb2; 32]),
            ..transition()
        },
        ..activation
    };
    assert_eq!(
        publish_handoff_activation(&elsewhere, Some(&published_activation)),
        Err(TrimError::Handoff(HandoffError::WrongTransition))
    );

    // Once served, it stays served. Delayed old traffic -- another old
    // voter's seal arriving late -- adds evidence that was always
    // implied and changes nothing.
    drop(view);
    assert_eq!(stage_of(&engine), Stage::Served);
}

/// One absent old voter does not stop the handoff, and the records are
/// read back as themselves.
#[test]
fn one_absent_old_voter_does_not_stop_the_handoff() {
    // Two of three sealed and two of three reported: replica 2 is gone
    // for good, and the certificate exists anyway.
    let certificate = certificate();
    assert_eq!(certificate.signers, BTreeSet::from([r(0), r(1)]));
    assert!(!certificate.signers.contains(&r(2)));

    let mut engine = store();
    apply(
        &mut engine,
        &[
            publish_certificate(&certificate, None).unwrap(),
            record_install(&certificate, r(2), &receipt()).unwrap(),
            record_install(&certificate, r(3), &receipt()).unwrap(),
        ],
    );
    // Replica 2 is in the successor, which is the point of the overlap:
    // a replica can be gone from the old configuration's quorum and
    // still be part of the new one.
    let view = engine.reader().snapshot().unwrap();
    let local = LocalEvidence::read(&view, &TrimLimits::default()).unwrap();
    assert_eq!(local.certificate.as_ref(), Some(&certificate));
    assert_eq!(local.installs.len(), 2);
    assert_eq!(local.activation, None);
    drop(view);
    assert_eq!(stage_of(&engine), Stage::Activating);

    // A row whose value is another record is corruption, not an absent
    // installation.
    apply(
        &mut engine,
        &[StoreUpdate {
            collection: Collection::CheckpointV1.id(),
            key: install_key(&r(3)),
            value: Some(certificate.encode().unwrap()),
        }],
    );
    let view = engine.reader().snapshot().unwrap();
    assert!(read_installs(&view, &TrimLimits::default()).is_err());
}
