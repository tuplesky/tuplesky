//! task-21 acceptance: direct-set equality differs from full path
//! evidence; leader synchronization aligns follower paths; duplicate and
//! reordered messages converge; closure traversal is exact and
//! incremental; backpressure refuses new work without deleting unresolved
//! acceptance; dependency rows round-trip.

use std::collections::BTreeSet;

use coord_consensus::{
    ClosureProgress, CommandTable, GuardViolation, InitError, Phase, RetireError, chain,
    decode_dependency, dependency_key, dependency_update, empty_path,
};
use coord_store_api::registry::Collection;
use coord_types::identity::Digest32;
use coord_types::ids::*;
use coord_types::logical_v1::{CanonicalOperation, LogicalRequest, PutOp};
use coord_types::{CommandId, RetryKey};

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

fn payload(i: u8) -> Digest32 {
    Digest32([i; 32])
}

fn k(s: &str) -> Vec<Vec<u8>> {
    vec![s.as_bytes().to_vec()]
}

#[test]
fn direct_set_equality_differs_from_full_path_evidence() {
    // Two replicas process x and a in different orders, then b, then c.
    // c's direct dependency set is {b} on both; its path differs.
    let (x, a, b, c) = (cmd(1), cmd(2), cmd(3), cmd(4));
    let mut one = CommandTable::new();
    let mut two = CommandTable::new();
    for (t, order) in [(&mut one, [x, a]), (&mut two, [a, x])] {
        for c in order {
            t.initialize(c, payload(c.0.0[0]), k("key")).unwrap();
        }
    }
    let b1 = one.initialize(b, payload(3), k("key")).unwrap();
    let b2 = two.initialize(b, payload(3), k("key")).unwrap();
    assert_ne!(b1.deps, b2.deps, "b's direct deps already differ");
    let c1 = one.initialize(c, payload(4), k("key")).unwrap();
    let c2 = two.initialize(c, payload(4), k("key")).unwrap();
    assert_eq!(c1.deps, vec![b]);
    assert_eq!(c2.deps, vec![b], "equal direct dependency sets");
    assert_ne!(c1.path, c2.path, "different full paths");
    // The path is the hash chain over the ordered prefix.
    let mut expected = empty_path();
    for step in [x, a, b, c] {
        expected = chain(&expected, &step);
    }
    assert_eq!(c1.paths, vec![(b"key".to_vec(), expected)]);
    // Same order on a third replica: identical path evidence.
    let mut three = CommandTable::new();
    for step in [x, a, b, c] {
        three
            .initialize(step, payload(step.0.0[0]), k("key"))
            .unwrap();
    }
    assert_eq!(three.record(&c).unwrap().path, c1.path);
    // Multi-key evidence is independent of key listing order.
    let mut m1 = CommandTable::new();
    let mut m2 = CommandTable::new();
    let d = cmd(5);
    let i1 = m1
        .initialize(d, payload(5), vec![b"p".to_vec(), b"q".to_vec()])
        .unwrap();
    let i2 = m2
        .initialize(d, payload(5), vec![b"q".to_vec(), b"p".to_vec()])
        .unwrap();
    assert_eq!(i1.path, i2.path);
}

#[test]
fn leader_synchronization_aligns_follower_paths() {
    // Leader order: a (seq 0), b (seq 1). The follower saw b before a.
    let (a, b, c) = (cmd(1), cmd(2), cmd(3));
    let mut leader = CommandTable::new();
    let la = leader.initialize(a, payload(1), k("key")).unwrap();
    let lb = leader.initialize(b, payload(2), k("key")).unwrap();
    let mut follower = CommandTable::new();
    follower.initialize(b, payload(2), k("key")).unwrap();
    follower.initialize(a, payload(1), k("key")).unwrap();
    assert_ne!(follower.path_head(b"key"), leader.path_head(b"key"));
    // The leader's fast acknowledgements (seqnum + path) synchronize the
    // follower's log: its head now equals the leader's.
    follower.record_leader_path(a, 0, &la.paths);
    follower.record_leader_path(b, 1, &lb.paths);
    assert_eq!(follower.path_head(b"key"), leader.path_head(b"key"));
    // A next command carries equal evidence on both.
    let lc = leader.initialize(c, payload(3), k("key")).unwrap();
    let fc = follower.initialize(c, payload(3), k("key")).unwrap();
    assert_eq!(lc.path, fc.path);
    // Leader evidence arriving before the follower appended the command
    // is applied at the append (no lost synchronization).
    let d = cmd(4);
    let ld = leader.initialize(d, payload(4), k("key")).unwrap();
    let mut late = CommandTable::new();
    late.record_leader_path(d, 3, &ld.paths);
    let fd = late.initialize(d, payload(4), k("key")).unwrap();
    assert_eq!(fd.paths, ld.paths);
}

#[test]
fn duplicate_and_reordered_messages_converge() {
    let (a, b) = (cmd(1), cmd(2));
    let mut t = CommandTable::new();
    // Leader evidence for b first (placeholder), then a, then b's payload,
    // then b's payload again, then a's again.
    t.expect(b).unwrap();
    t.expect(b).unwrap();
    assert_eq!(t.conflicts(&k("key")), vec![], "placeholder invisible");
    t.initialize(a, payload(1), k("key")).unwrap();
    let first = t.initialize(b, payload(2), k("key")).unwrap();
    assert_eq!(first.deps, vec![a]);
    assert_eq!(
        t.initialize(b, payload(2), k("key")),
        Err(InitError::AlreadyInitialized)
    );
    assert_eq!(
        t.initialize(a, payload(1), k("key")),
        Err(InitError::AlreadyInitialized)
    );
    assert_eq!(t.record(&b).unwrap().deps, vec![a]);
    assert_eq!(t.record(&b).unwrap().path, first.path);
    assert_eq!(t.len(), 2);
    // An identical table built in the converged order agrees.
    let mut u = CommandTable::new();
    u.initialize(a, payload(1), k("key")).unwrap();
    u.initialize(b, payload(2), k("key")).unwrap();
    assert_eq!(u.record(&b), t.record(&b));
}

#[test]
fn closure_traversal_is_exact_and_incremental() {
    // Chain c0 <- c1 <- ... <- c9 plus a diamond: c10 depends on c9 and c3.
    let mut t = CommandTable::new();
    for i in 0..10u8 {
        t.initialize(cmd(i), payload(i), k("chain")).unwrap();
    }
    t.initialize(cmd(10), payload(10), k("chain")).unwrap();
    t.accept(cmd(10), vec![cmd(9), cmd(3)]).unwrap_err(); // c9 not accepted yet
    for i in 0..10u8 {
        t.accept(cmd(i), t.record(&cmd(i)).unwrap().deps.clone())
            .unwrap();
    }
    t.accept(cmd(10), vec![cmd(9), cmd(3)]).unwrap();
    // One-shot closure.
    let cursor = t.closure_start(cmd(10)).unwrap();
    let ClosureProgress::Complete(full) = t.closure_step(cursor, usize::MAX).unwrap() else {
        panic!("unbounded budget completes")
    };
    let expected: BTreeSet<CommandId> = (0..10u8).map(cmd).collect();
    assert_eq!(full.members, expected);
    assert_eq!(full.visits, 10, "each dependency visited exactly once");
    // Incremental: budget 3 per step, identical result.
    let mut cursor = t.closure_start(cmd(10)).unwrap();
    let mut steps = 0;
    let stepped = loop {
        steps += 1;
        match t.closure_step(cursor, 3).unwrap() {
            ClosureProgress::Continue(c) => {
                assert!(!c.frontier().is_empty());
                cursor = c;
            }
            ClosureProgress::Complete(done) => break done,
        }
    };
    assert_eq!(stepped.members, full.members);
    assert_eq!(stepped.visits, full.visits);
    assert!(steps >= 4, "bounded work: {steps} steps");
    // A placeholder or unknown dependency stops the traversal with its
    // identity; nothing is guessed.
    let mut u = CommandTable::new();
    u.initialize(cmd(1), payload(1), k("chain")).unwrap();
    u.expect(cmd(0)).unwrap();
    u.accept(cmd(1), vec![]).unwrap();
    u.record(&cmd(1)).unwrap();
    let mut v = u.clone();
    // Force a dependency on the placeholder through the leader's order:
    // the guard already refuses it.
    assert_eq!(
        v.accept(cmd(1), vec![cmd(0)]),
        Err(GuardViolation::DependencyUnknown { dep: cmd(0) })
    );
    assert_eq!(
        u.closure_start(cmd(0)),
        Err(GuardViolation::DependencyUnknown { dep: cmd(0) })
    );
}

#[test]
fn backpressure_refuses_new_work_without_deleting_unresolved_acceptance() {
    let mut t = CommandTable::with_capacity(3);
    for i in 0..3u8 {
        t.initialize(cmd(i), payload(i), k("key")).unwrap();
        t.accept(cmd(i), t.record(&cmd(i)).unwrap().deps.clone())
            .unwrap();
    }
    // Full: new work is refused, placeholders included; nothing accepted
    // is evicted.
    assert_eq!(
        t.initialize(cmd(3), payload(3), k("key")),
        Err(InitError::Backpressure)
    );
    assert_eq!(t.expect(cmd(4)), Err(InitError::Backpressure));
    assert_eq!(t.len(), 3);
    for i in 0..3u8 {
        assert_eq!(t.phase_of(&cmd(i)), Some(Phase::Accept));
        assert_eq!(
            t.retire(&cmd(i)),
            Err(RetireError::NotExecuted(Phase::Accept))
        );
    }
    assert_eq!(t.retire(&cmd(9)), Err(RetireError::Unknown));
    // Only executed commands free capacity; the index still names them so
    // later commands depend on them.
    t.commit(cmd(0)).unwrap();
    t.execute(cmd(0)).unwrap();
    t.retire(&cmd(0)).unwrap();
    assert_eq!(t.len(), 2);
    let i3 = t.initialize(cmd(3), payload(3), k("key")).unwrap();
    assert_eq!(i3.deps, vec![cmd(2)]);
    assert_eq!(t.len(), 3);
    // Dependency-phase prerequisites still hold after retirement: c1 is
    // accepted but not committed, so c2 cannot commit.
    assert_eq!(
        t.commit(cmd(2)),
        Err(GuardViolation::DependencyNotCommitted { dep: cmd(1) })
    );
}

#[test]
fn dependency_rows_round_trip() {
    let mut t = CommandTable::new();
    t.initialize(cmd(1), payload(1), k("key")).unwrap();
    t.initialize(cmd(2), payload(2), vec![b"key".to_vec(), b"other".to_vec()])
        .unwrap();
    t.accept(cmd(1), vec![]).unwrap();
    t.accept(cmd(2), vec![cmd(1)]).unwrap();
    let epoch = ConfigurationEpoch::new(1).unwrap();
    let record = t.record(&cmd(2)).unwrap();
    let update = dependency_update(epoch, &cmd(2), record).unwrap();
    assert_eq!(update.collection, Collection::ProtocolV1.id());
    assert_eq!(update.key, dependency_key(epoch, &cmd(2)));
    assert_eq!(update.key.len(), 41);
    assert_eq!(&update.key[..8], &epoch.to_be_bytes());
    let decoded = decode_dependency(update.value.as_ref().unwrap()).unwrap();
    assert_eq!(&decoded, record);
    assert_eq!(decoded.phase, Phase::Accept);
    assert_eq!(decoded.deps, vec![cmd(1)]);
    assert_eq!(decoded.paths.len(), 2);
    // Keys of an epoch are contiguous and distinct from the promise row.
    assert_ne!(
        dependency_key(epoch, &cmd(2)),
        coord_consensus::promise_key(epoch)
    );
    assert!(dependency_key(epoch, &cmd(1)) < dependency_key(epoch, &cmd(2)) || cmd(1) > cmd(2));
    let _ = ReplicaId([0; 16]);
}
