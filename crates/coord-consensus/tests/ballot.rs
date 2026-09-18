//! task-20 acceptance: promise replies wait for complete durable state;
//! old messages cannot lower a recovered promise; wrong configuration
//! identity never votes; a same-boot election fences obsolete vote
//! callbacks while their bookkeeping still completes; initialization and
//! index publication are atomic and placeholders are invisible.

use std::collections::BTreeSet;

use coord_consensus::{
    BallotState, CommandTable, ConfigurationIdentity, GuardViolation, InitError, Phase,
    PromiseOutcome, PromiseRecordV1, PromiseRejection, ProtocolMessage, ReplicaRole, SyncRejection,
    decode_promise, promise_key,
};
use coord_core::effect::{BarrierId, BootId, Effect, PeerId, PersistBatch, StoreUpdate};
use coord_core::event::{StorageError, StorageEvent};
use coord_core::outbox::{BarrierAllocator, Outbox, PendingSend, ReleaseError};
use coord_sim::storage::StorageModel;
use coord_store_api::registry::Collection;
use coord_types::identity::Digest32;
use coord_types::ids::*;
use coord_types::logical_v1::{CanonicalOperation, LogicalRequest, PutOp};
use coord_types::{CommandId, RetryKey};

fn peer(i: u8) -> PeerId {
    PeerId {
        replica: r(i),
        incarnation: ReplicaIncarnation::new(3).unwrap(),
    }
}

fn r(i: u8) -> ReplicaId {
    ReplicaId([i; 16])
}

fn epoch(n: u64) -> ConfigurationEpoch {
    ConfigurationEpoch::new(n).unwrap()
}

fn ballot(e: u64, number: u64, leader: u8) -> Ballot {
    Ballot {
        epoch: epoch(e),
        number,
        leader: r(leader),
    }
}

fn identity(role: ReplicaRole) -> ConfigurationIdentity {
    ConfigurationIdentity {
        cluster: ClusterId([1; 16]),
        domain: DomainId([2; 16]),
        epoch: epoch(1),
        voters: (0..3).map(r).collect(),
        replica: r(1),
        incarnation: ReplicaIncarnation::new(1).unwrap(),
        role,
    }
}

fn boot(i: u8) -> BootId {
    BootId([i; 16])
}

fn alloc(b: BootId) -> BarrierAllocator {
    BarrierAllocator::new(ReplicaIncarnation::new(1).unwrap(), b)
}

fn durable(barrier: BarrierId, seq: u64) -> StorageEvent {
    StorageEvent::JournalDurable {
        barrier_id: barrier,
        journal_seq: LocalJournalSeq::new(seq).unwrap(),
    }
}

/// A voter in epoch 1 with genesis ballot (1, 0, leader r0).
fn voter() -> BallotState {
    BallotState::recover(identity(ReplicaRole::Voter), ballot(1, 0, 0), None)
}

#[test]
fn promise_reply_waits_for_the_row_and_every_batch_before_the_cut() {
    let b = boot(1);
    let mut a = alloc(b);
    let mut outbox = Outbox::new(b);
    let mut state = voter();
    // Two batches submitted before the election, not yet durable.
    let pending = [a.allocate(), a.allocate()];
    let effects = state
        .on_new_leader(peer(2), ballot(1, 1, 2), b, &mut a, &pending)
        .unwrap();
    let Effect::Persist(batch) = &effects.persist else {
        panic!("promise must persist first")
    };
    assert_eq!(
        batch.base, None,
        "a protocol row carries no application base"
    );
    assert_eq!(batch.updates.len(), 1);
    assert_eq!(batch.updates[0].collection, Collection::ProtocolV1.id());
    assert_eq!(batch.updates[0].key, promise_key(epoch(1)));
    let promise = batch.barrier;
    assert_eq!(
        effects.reply.requires,
        vec![pending[0], pending[1], promise]
    );
    assert_eq!(effects.reply.context.ballot, ballot(1, 1, 2));
    assert_eq!(effects.reply.context.boot_id, b);
    assert_eq!(effects.reply.context.configuration, epoch(1));
    assert_eq!(
        ProtocolMessage::decode(&effects.reply.frame).unwrap(),
        ProtocolMessage::Promise {
            ballot: ballot(1, 1, 2),
            synced: ballot(1, 0, 0),
            replica: r(1),
        }
    );
    outbox.publish(effects.reply.clone());
    // The row alone is not enough; the earlier batches must complete too.
    outbox.observe(&durable(promise, 3));
    assert_eq!(
        state.on_storage(&durable(promise, 3)),
        Some(PromiseOutcome::Promised(ballot(1, 1, 2)))
    );
    assert_eq!(state.promised(), ballot(1, 1, 2));
    assert!(outbox.release(&state.promised()).is_empty());
    outbox.observe(&durable(pending[0], 1));
    assert!(outbox.release(&state.promised()).is_empty());
    outbox.observe(&durable(pending[1], 2));
    let released = outbox.release(&state.promised());
    assert_eq!(released.len(), 1);
    assert!(matches!(
        &released[0],
        Effect::SendWhenDurable { to, .. } if to.replica == r(2)
    ));
    // While a promise is in flight no lower or equal ballot is admitted,
    // and after it is durable the promise only grows.
    let mut state = voter();
    let e = state
        .on_new_leader(peer(2), ballot(1, 5, 2), b, &mut a, &[])
        .unwrap();
    assert_eq!(
        state.on_new_leader(peer(0), ballot(1, 4, 0), b, &mut a, &[]),
        Err(PromiseRejection::NotHigher {
            promised: ballot(1, 5, 2)
        })
    );
    assert_eq!(
        state.on_new_leader(peer(2), ballot(1, 5, 2), b, &mut a, &[]),
        Err(PromiseRejection::NotHigher {
            promised: ballot(1, 5, 2)
        })
    );
    let Effect::Persist(batch) = &e.persist else {
        panic!()
    };
    // A failed row leaves the earlier promise in force.
    assert_eq!(
        state.on_storage(&StorageEvent::Failed {
            barrier_id: batch.barrier,
            error: StorageError::NoSpace
        }),
        Some(PromiseOutcome::Failed(ballot(1, 5, 2)))
    );
    assert_eq!(state.promised(), ballot(1, 0, 0));
    assert_eq!(state.in_flight(), None);
}

#[test]
fn old_messages_cannot_lower_a_recovered_promise() {
    let b1 = boot(1);
    let mut a = alloc(b1);
    let mut storage = StorageModel::default();
    let mut state = voter();
    let e = state
        .on_new_leader(peer(2), ballot(1, 7, 2), b1, &mut a, &[])
        .unwrap();
    let Effect::Persist(batch) = e.persist else {
        panic!()
    };
    let barrier = batch.barrier;
    storage.submit(batch);
    storage.complete(barrier).unwrap();
    state.on_storage(&durable(barrier, 1));
    assert_eq!(state.promised(), ballot(1, 7, 2));
    // Crash and reboot: the promise is recovered from the row.
    storage.crash();
    let row = storage
        .durable_rows()
        .into_iter()
        .find(|(c, k, _)| *c == Collection::ProtocolV1.id().0 && *k == promise_key(epoch(1)))
        .map(|(_, _, v)| decode_promise(&v).unwrap())
        .expect("promise row survives");
    assert_eq!(
        row,
        PromiseRecordV1 {
            promised: ballot(1, 7, 2),
            synced: ballot(1, 0, 0)
        }
    );
    let b2 = boot(2);
    let mut a2 = alloc(b2);
    let mut recovered =
        BallotState::recover(identity(ReplicaRole::Voter), ballot(1, 0, 0), Some(row));
    assert_eq!(recovered.promised(), ballot(1, 7, 2));
    assert_eq!(recovered.elections(), 0);
    // An old NewLeader arriving after the restart cannot lower it; the same
    // ballot is not higher either; a higher one is accepted.
    assert_eq!(
        recovered.on_new_leader(peer(0), ballot(1, 3, 0), b2, &mut a2, &[]),
        Err(PromiseRejection::NotHigher {
            promised: ballot(1, 7, 2)
        })
    );
    assert_eq!(
        recovered.on_new_leader(peer(2), ballot(1, 7, 2), b2, &mut a2, &[]),
        Err(PromiseRejection::NotHigher {
            promised: ballot(1, 7, 2)
        })
    );
    assert!(
        recovered
            .on_new_leader(peer(0), ballot(1, 8, 0), b2, &mut a2, &[])
            .is_ok()
    );
    // A promise that was in flight at the crash never became durable: the
    // recovered state does not know it, and the outbox of the new boot
    // never replays its reply.
    let mut state = voter();
    let mut storage = StorageModel::default();
    let e = state
        .on_new_leader(peer(2), ballot(1, 9, 2), b1, &mut a, &[])
        .unwrap();
    let Effect::Persist(batch) = e.persist else {
        panic!()
    };
    storage.submit(batch);
    assert_eq!(storage.crash(), 1);
    assert!(storage.durable_rows().is_empty());
    let mut outbox = Outbox::new(b2);
    outbox.publish(e.reply);
    assert_eq!(
        outbox.pending().len(),
        0,
        "a send from another boot is dropped"
    );
    assert!(matches!(
        outbox.take_dropped().as_slice(),
        [(_, ReleaseError::WrongBoot)]
    ));
}

#[test]
fn wrong_configuration_identity_never_votes() {
    let b = boot(1);
    let mut a = alloc(b);
    // Another epoch.
    let mut state = voter();
    assert_eq!(
        state.on_new_leader(peer(2), ballot(2, 1, 2), b, &mut a, &[]),
        Err(PromiseRejection::WrongEpoch {
            expected: epoch(1),
            got: epoch(2)
        })
    );
    // A non-voter candidate.
    assert_eq!(
        state.on_new_leader(peer(9), ballot(1, 1, 9), b, &mut a, &[]),
        Err(PromiseRejection::NotAVoter { from: r(9) })
    );
    // A ballot naming a leader other than the sender.
    assert_eq!(
        state.on_new_leader(peer(2), ballot(1, 1, 0), b, &mut a, &[]),
        Err(PromiseRejection::LeaderMismatch {
            leader: r(0),
            from: r(2)
        })
    );
    assert_eq!(state.in_flight(), None);
    assert_eq!(state.promised(), ballot(1, 0, 0));
    // Observers and learners never promise, whatever the ballot.
    for role in [ReplicaRole::Observer, ReplicaRole::Learner] {
        let mut s = BallotState::recover(identity(role), ballot(1, 0, 0), None);
        assert_eq!(
            s.on_new_leader(peer(2), ballot(1, 1, 2), b, &mut a, &[]),
            Err(PromiseRejection::NotVoting { role })
        );
    }
}

#[test]
fn a_same_boot_election_fences_obsolete_vote_callbacks() {
    let b = boot(1);
    let mut a = alloc(b);
    let mut outbox = Outbox::new(b);
    let mut state = voter();
    // A vote produced under the genesis ballot, persisted but not yet
    // durable: its send waits on barrier `vote`.
    let vote = a.allocate();
    let send = PendingSend {
        context: state.context(b, ballot(1, 0, 0), LocalJournalSeq::ZERO),
        requires: vec![vote],
        to: PeerId {
            replica: r(0),
            incarnation: ReplicaIncarnation::ZERO,
        },
        frame: b"fast-ack".to_vec(),
    };
    outbox.publish(send);
    // An election for ballot (1, 1, r2) completes in the same boot.
    let e = state
        .on_new_leader(peer(2), ballot(1, 1, 2), b, &mut a, &[vote])
        .unwrap();
    let Effect::Persist(batch) = &e.persist else {
        panic!()
    };
    outbox.publish(e.reply.clone());
    outbox.observe(&durable(batch.barrier, 2));
    state.on_storage(&durable(batch.barrier, 2));
    assert_eq!(state.elections(), 1);
    // The old vote's storage completes late: bookkeeping is updated, but
    // the vote is dropped as obsolete instead of being sent; the promise
    // reply, which required that batch too, is now released.
    assert!(outbox.observe(&durable(vote, 1)));
    assert!(outbox.is_durable(&vote));
    let released = outbox.release(&state.promised());
    assert_eq!(released.len(), 1);
    assert!(matches!(&released[0], Effect::SendWhenDurable { frame, .. } if frame != b"fast-ack"));
    let dropped = outbox.take_dropped();
    assert_eq!(dropped.len(), 1);
    assert_eq!(dropped[0].1, ReleaseError::ObsoleteBallot);
    assert_eq!(dropped[0].0.frame, b"fast-ack".to_vec());
}

fn cmd(i: u8) -> CommandId {
    let key = RetryKey {
        cluster_id: ClusterId([1; 16]),
        domain_id: DomainId([2; 16]),
        session_id: SessionId([3; 16]),
        client_instance_id: ClientInstanceId([4; 16]),
        request_sequence: RequestSequence::new(u64::from(i) + 1).unwrap(),
    };
    let request = LogicalRequest::new(
        NamespaceId([5; 16]),
        CanonicalOperation::Put(PutOp {
            key: vec![i],
            value: vec![],
            lease: None,
            prev_kv: false,
        }),
    );
    CommandId::derive(&key, &request).unwrap()
}

#[test]
fn initialization_publishes_atomically_and_placeholders_are_invisible() {
    let mut table = CommandTable::new();
    let (c1, c2, c3) = (cmd(1), cmd(2), cmd(3));
    let k = vec![b"k".to_vec()];
    // Leader evidence for c1 arrives first: a placeholder, invisible to
    // conflict lookups and to the guards.
    table.expect(c1).unwrap();
    assert_eq!(table.phase_of(&c1), None);
    assert_eq!(table.conflicts(&k), vec![]);
    assert_eq!(
        table.accept(c2, vec![c1]),
        Err(GuardViolation::DependencyUnknown { dep: c1 })
    );
    // c2 initializes while c1 is still a placeholder: it cannot see c1.
    assert_eq!(
        table
            .initialize(c2, Digest32([2; 32]), k.clone())
            .unwrap()
            .deps,
        vec![]
    );
    assert_eq!(table.phase_of(&c2), Some(Phase::PreAccept));
    // c1's payload arrives: one transition binds it, computes deps from the
    // index (now c2) and publishes it.
    assert_eq!(
        table
            .initialize(c1, Digest32([1; 32]), k.clone())
            .unwrap()
            .deps,
        vec![c2]
    );
    assert_eq!(table.conflicts(&k), vec![c1]);
    // Duplicate / reordered initialization converges without change; a
    // different payload under the same identity is a conflict.
    assert_eq!(
        table.initialize(c1, Digest32([1; 32]), k.clone()),
        Err(InitError::AlreadyInitialized)
    );
    assert_eq!(
        table.initialize(c1, Digest32([9; 32]), k.clone()),
        Err(InitError::PayloadConflict)
    );
    assert_eq!(table.record(&c1).unwrap().deps, vec![c2]);
    // Dependency-phase prerequisites: c1 cannot be accepted with dep c2
    // until c2 is accepted; nor committed until c2 is committed.
    assert_eq!(
        table.accept(c1, vec![c2]),
        Err(GuardViolation::DependencyNotAccepted { dep: c2 })
    );
    table.accept(c2, vec![]).unwrap();
    table.accept(c1, vec![c2]).unwrap();
    assert_eq!(
        table.commit(c1),
        Err(GuardViolation::DependencyNotCommitted { dep: c2 })
    );
    table.commit(c2).unwrap();
    table.commit(c1).unwrap();
    assert_eq!(
        table.execute(c1),
        Err(GuardViolation::DependencyNotExecuted { dep: c2 })
    );
    table.execute(c2).unwrap();
    table.execute(c1).unwrap();
    assert_eq!(table.phase_of(&c1), Some(Phase::Executed));
    // Phases only advance: a delayed or duplicate ACCEPT or COMMIT for an
    // executed command is idempotent and keeps the dependencies the later
    // phase was reached with.
    table.accept(c1, vec![]).unwrap();
    table.commit(c1).unwrap();
    table.execute(c1).unwrap();
    assert_eq!(table.phase_of(&c1), Some(Phase::Executed));
    assert_eq!(table.record(&c1).unwrap().deps, vec![c2]);
    // A third command initialized now depends on the last in the index.
    assert_eq!(
        table.initialize(c3, Digest32([3; 32]), k).unwrap().deps,
        vec![c1]
    );
    assert_eq!(table.len(), 3);
    let _ = BTreeSet::<u8>::new();
    let _ = StoreUpdate {
        collection: Collection::ProtocolV1.id(),
        key: vec![],
        value: None,
    };
    let _ = PersistBatch {
        barrier: BarrierId {
            node_generation: ReplicaIncarnation::ZERO,
            boot_id: boot(0),
            sequence: 0,
        },
        base: None,
        updates: vec![],
    };
}

#[test]
fn every_in_flight_promise_completes_independently() {
    let b = boot(1);
    let mut a = alloc(b);
    let mut state = voter();
    // Ballot 5 is promised and still in flight when ballot 10 arrives: the
    // higher ballot is admitted (10 > 5) and both promises stay tracked.
    let e5 = state
        .on_new_leader(peer(2), ballot(1, 5, 2), b, &mut a, &[])
        .unwrap();
    let e10 = state
        .on_new_leader(peer(0), ballot(1, 10, 0), b, &mut a, &[])
        .unwrap();
    let barrier = |e: &coord_consensus::PromiseEffects| match &e.persist {
        Effect::Persist(batch) => batch.barrier,
        other => panic!("{other:?}"),
    };
    assert_eq!(state.promises_in_flight().len(), 2);
    assert_eq!(state.in_flight().unwrap().ballot, ballot(1, 10, 0));
    // Replies are addressed to the candidate's authenticated incarnation.
    assert_eq!(e5.reply.to, peer(2));
    assert_eq!(e10.reply.to, peer(0));
    assert_eq!(state.promises_in_flight()[0].to, peer(2));
    // Ballot 5's row becomes durable first: the promise advances to 5 and
    // ballot 10 stays in flight.
    assert_eq!(
        state.on_storage(&durable(barrier(&e5), 1)),
        Some(PromiseOutcome::Promised(ballot(1, 5, 2)))
    );
    assert_eq!(state.promised(), ballot(1, 5, 2));
    assert_eq!(state.promises_in_flight().len(), 1);
    // Ballot 10's row fails: the durable promise is 5, not the genesis
    // ballot, so a later ballot below 5 is still refused.
    assert_eq!(
        state.on_storage(&StorageEvent::Failed {
            barrier_id: barrier(&e10),
            error: StorageError::NoSpace
        }),
        Some(PromiseOutcome::Failed(ballot(1, 10, 0)))
    );
    assert_eq!(state.promised(), ballot(1, 5, 2));
    assert_eq!(state.in_flight(), None);
    assert_eq!(
        state.on_new_leader(peer(0), ballot(1, 4, 0), b, &mut a, &[]),
        Err(PromiseRejection::NotHigher {
            promised: ballot(1, 5, 2)
        })
    );
    // Rows completing out of order never move the promise backward.
    let e6 = state
        .on_new_leader(peer(0), ballot(1, 6, 0), b, &mut a, &[])
        .unwrap();
    let e7 = state
        .on_new_leader(peer(2), ballot(1, 7, 2), b, &mut a, &[])
        .unwrap();
    state.on_storage(&durable(barrier(&e7), 2));
    assert_eq!(state.promised(), ballot(1, 7, 2));
    state.on_storage(&durable(barrier(&e6), 3));
    assert_eq!(state.promised(), ballot(1, 7, 2));
    assert_eq!(state.elections(), 3);
    assert_eq!(state.in_flight(), None);
}

#[test]
fn the_synchronized_ballot_never_regresses_or_leaves_the_epoch() {
    let mut state = voter();
    let record = state.mark_synced(ballot(1, 4, 0)).unwrap();
    assert_eq!(record.synced, ballot(1, 4, 0));
    // A delayed Sync of an older ballot, and one from another epoch, are
    // rejected without touching the record; the same ballot is idempotent.
    assert_eq!(
        state.mark_synced(ballot(1, 2, 2)),
        Err(SyncRejection::Regression {
            synced: ballot(1, 4, 0),
            got: ballot(1, 2, 2)
        })
    );
    assert_eq!(
        state.mark_synced(ballot(2, 9, 0)),
        Err(SyncRejection::WrongEpoch {
            expected: epoch(1),
            got: epoch(2)
        })
    );
    assert_eq!(state.synced(), ballot(1, 4, 0));
    assert_eq!(
        state.mark_synced(ballot(1, 4, 0)).unwrap().synced,
        ballot(1, 4, 0)
    );
    assert_eq!(
        state.mark_synced(ballot(1, 6, 2)).unwrap().synced,
        ballot(1, 6, 2)
    );
}
