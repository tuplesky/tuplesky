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
                assert!(c.remaining() > 0);
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

#[test]
fn closure_steps_charge_every_edge_and_frontier_entry() {
    // c100 depends on c0..c99 directly (and each of those on nothing):
    // with a budget of one unit per step, no step may examine more than
    // one edge or frontier entry, so a wide dependency list takes as many
    // steps as it has edges instead of being expanded in one visit.
    let mut t = CommandTable::new();
    for i in 0..100u8 {
        t.initialize(cmd(i), payload(i), vec![vec![i]]).unwrap();
        t.accept(cmd(i), vec![]).unwrap();
    }
    t.initialize(cmd(100), payload(100), vec![vec![100]])
        .unwrap();
    t.accept(cmd(100), (0..100u8).map(cmd).collect()).unwrap();
    let mut cursor = t.closure_start(cmd(100)).unwrap();
    let mut steps = 0usize;
    let done = loop {
        steps += 1;
        match t.closure_step(cursor, 1).unwrap() {
            ClosureProgress::Continue(c) => cursor = c,
            ClosureProgress::Complete(done) => break done,
        }
    };
    assert_eq!(done.members.len(), 100);
    assert_eq!(done.visits, 100);
    assert!(steps >= 100, "one edge or entry per unit: {steps} steps");
    // The same traversal with a large budget spends the same total work
    // and reaches the same closure.
    let cursor = t.closure_start(cmd(100)).unwrap();
    let ClosureProgress::Complete(whole) = t.closure_step(cursor, usize::MAX).unwrap() else {
        panic!()
    };
    assert_eq!(whole.members, done.members);
    // A diamond: both branches name c0, and the second edge to it costs
    // a unit even though nothing is pushed for it.
    let mut d = CommandTable::new();
    d.initialize(cmd(0), payload(0), vec![vec![0]]).unwrap();
    d.accept(cmd(0), vec![]).unwrap();
    for i in 1..=2u8 {
        d.initialize(cmd(i), payload(i), vec![vec![i]]).unwrap();
        d.accept(cmd(i), vec![cmd(0)]).unwrap();
    }
    d.initialize(cmd(3), payload(3), vec![vec![3]]).unwrap();
    d.accept(cmd(3), vec![cmd(1), cmd(2)]).unwrap();
    let mut cursor = d.closure_start(cmd(3)).unwrap();
    let mut steps = 0usize;
    let done = loop {
        steps += 1;
        match d.closure_step(cursor, 1).unwrap() {
            ClosureProgress::Continue(c) => cursor = c,
            ClosureProgress::Complete(done) => break done,
        }
    };
    assert_eq!(done.members, BTreeSet::from([cmd(0), cmd(1), cmd(2)]));
    assert_eq!(done.visits, 3);
    // Three frontier entries taken (c1, c2, c0) and two edges examined
    // (c1 -> c0, c2 -> c0): five units, so five single-unit steps.
    assert_eq!(steps, 5, "{steps}");
}

#[test]
fn a_hot_key_keeps_working_after_its_executed_predecessor_is_retired() {
    let mut t = CommandTable::with_capacity(2);
    t.initialize(cmd(0), payload(0), k("hot")).unwrap();
    t.accept(cmd(0), vec![]).unwrap();
    t.initialize(cmd(1), payload(1), k("hot")).unwrap();
    assert_eq!(t.record(&cmd(1)).unwrap().deps, vec![cmd(0)]);
    t.accept(cmd(1), vec![cmd(0)]).unwrap();
    t.commit(cmd(0)).unwrap();
    t.execute(cmd(0)).unwrap();
    // c0 is retired while c1 still depends on it: c1 keeps seeing an
    // executed dependency and can commit, execute and be traversed.
    t.retire(&cmd(0)).unwrap();
    assert_eq!(t.phase_of(&cmd(0)), Some(Phase::Executed));
    assert!(t.tombstones().contains(&cmd(0)));
    t.commit(cmd(1)).unwrap();
    let cursor = t.closure_start(cmd(1)).unwrap();
    let ClosureProgress::Complete(closure) = t.closure_step(cursor, usize::MAX).unwrap() else {
        panic!()
    };
    assert_eq!(closure.members, BTreeSet::from([cmd(0)]));
    // A new command on the hot key no longer depends on the retired,
    // executed predecessor (its effects are complete); it depends on c1.
    let i2 = t.initialize(cmd(2), payload(2), k("hot")).unwrap();
    assert_eq!(i2.deps, vec![cmd(1)]);
    t.accept(cmd(2), vec![cmd(1)]).unwrap();
    t.execute(cmd(1)).unwrap();
    // Retiring c1 does not drop c0's tombstone. What a tombstone
    // answers for is a dependency, and a dependency is named by
    // whoever holds the evidence -- which need not be a record in this
    // table. The memory is bounded by recency instead: the table has
    // room for two records, so it remembers its last two retirements.
    t.retire(&cmd(1)).unwrap();
    assert!(t.tombstones().contains(&cmd(1)));
    assert!(t.tombstones().contains(&cmd(0)));
    assert_eq!(t.phase_of(&cmd(0)), Some(Phase::Executed));
    t.commit(cmd(2)).unwrap();
    t.execute(cmd(2)).unwrap();
    t.retire(&cmd(2)).unwrap();
    // The third retirement pushes the oldest out, and only the oldest.
    assert_eq!(
        t.tombstones().len(),
        2,
        "the table remembers as many retirements as it has room for records"
    );
    assert!(!t.tombstones().contains(&cmd(0)));
    assert!(t.tombstones().contains(&cmd(1)));
    assert!(t.tombstones().contains(&cmd(2)));
    // The key's next command still depends on the latest one, retired
    // as it is (task-d06): a replica that has not executed c2 yet must not
    // execute c3 first, and one that has reads c2's tombstone as EXECUTED.
    let i3 = t.initialize(cmd(3), payload(3), k("hot")).unwrap();
    assert_eq!(i3.deps, vec![cmd(2)]);
    t.accept(cmd(3), i3.deps)
        .expect("the tombstone answers for c2");
}

/// A command retired before the evidence that names it arrives is still
/// executed as far as that evidence is concerned.
///
/// This is the shape a follower is in under concurrent callers. Its
/// table holds the leader's proposals as well as its own records, so it
/// fills -- and reclaims -- ahead of the leader, and the proposal that
/// arrives next names the command it just retired. Nothing in the table
/// referred to that command when it went, because the dependency the
/// proposal names lives in the proposal.
///
/// Before the tombstone was made unconditional, the guard read "unknown"
/// here for a command this replica had executed itself. The proposal
/// could then never be adopted, every later command queued behind it,
/// and the table stayed full: a replica that had answered two hundred
/// operations stopped answering any, permanently.
#[test]
fn a_dependency_retired_before_its_proposal_arrives_is_still_executed() {
    let mut t = CommandTable::with_capacity(2);
    t.initialize(cmd(0), payload(0), k("key")).unwrap();
    t.accept(cmd(0), vec![]).unwrap();
    t.commit(cmd(0)).unwrap();
    t.execute(cmd(0)).unwrap();
    t.retire(&cmd(0)).unwrap();
    assert!(
        !t.records().any(|(_, r)| r.deps.contains(&cmd(0))),
        "nothing in the table depends on the retired command"
    );

    // Now the leader's order for a later command arrives, naming it.
    t.initialize(cmd(1), payload(1), k("key")).unwrap();
    assert_eq!(
        t.adopt(cmd(1), vec![cmd(0)], None, Digest32([0; 32])),
        Ok(()),
        "the leader's order names a command this replica executed"
    );
}

#[test]
fn applied_synchronizations_are_not_taken_for_early_evidence() {
    let (a, b) = (cmd(1), cmd(2));
    let mut t = CommandTable::new();
    let ia = t.initialize(a, payload(1), k("key")).unwrap();
    t.record_leader_path(a, 1, &ia.paths);
    let before = t.log(b"key").unwrap().clone();
    assert_eq!(before.pending(), &[]);
    assert!(before.applied().contains(&a));
    // The same synchronization again, and an older reordered copy: no
    // change, and no early entry is recorded.
    t.record_leader_path(a, 1, &ia.paths);
    t.record_leader_path(a, 0, &[(b"key".to_vec(), Digest32([7; 32]))]);
    assert_eq!(t.log(b"key").unwrap(), &before);
    // A genuinely early synchronization is still remembered and applied
    // on append, and forgotten with the command.
    t.record_leader_path(b, 2, &[(b"key".to_vec(), Digest32([9; 32]))]);
    let ib = t.initialize(b, payload(2), k("key")).unwrap();
    assert_eq!(ib.paths[0].1, Digest32([9; 32]));
    assert_eq!(t.log(b"key").unwrap().pending(), &[]);
    t.accept(a, vec![]).unwrap();
    t.commit(a).unwrap();
    t.execute(a).unwrap();
    t.retire(&a).unwrap();
    assert!(!t.log(b"key").unwrap().applied().contains(&a));
    assert!(t.log(b"key").unwrap().applied().contains(&b));
}

/// A full table makes room from what it has finished, and only from
/// that.
///
/// The rule the module states -- only executed records may be retired --
/// says what may be forgotten. It does not say to forget nothing: a
/// table that waited for someone to call `retire` would turn its
/// capacity into a bound on how many commands a replica may execute for
/// as long as it runs, and the caller of the next one would wait out its
/// deadline against an idle cluster.
#[test]
fn a_full_table_reclaims_what_it_executed_and_nothing_else() {
    let mut t = CommandTable::with_capacity(2);
    t.initialize(cmd(0), payload(0), k("key")).unwrap();
    t.initialize(cmd(1), payload(1), k("key")).unwrap();

    // Nothing has finished, so nothing is reclaimable and the refusal
    // stands: a record in PRE-ACCEPT is work this replica still owes.
    assert_eq!(
        t.initialize(cmd(2), payload(2), k("key")),
        Err(InitError::Backpressure)
    );
    assert_eq!(t.len(), 2);
    assert_eq!(t.reclaim(), 0);

    // The first one executes. The next initialization needs no explicit
    // retirement: the table makes room from it, and the new command
    // still depends on the one that has not executed.
    t.accept(cmd(0), t.record(&cmd(0)).unwrap().deps.clone())
        .unwrap();
    t.commit(cmd(0)).unwrap();
    t.execute(cmd(0)).unwrap();
    let third = t.initialize(cmd(2), payload(2), k("key")).unwrap();
    assert_eq!(t.len(), 2, "the executed record made room for this one");
    assert_eq!(third.deps, vec![cmd(1)]);

    // And the retired command is still executed as far as anything that
    // depends on it is concerned.
    t.accept(cmd(1), t.record(&cmd(1)).unwrap().deps.clone())
        .unwrap();
    assert_eq!(t.phase_of(&cmd(0)), Some(Phase::Executed));
    assert!(t.tombstones().contains(&cmd(0)));

    // A table that never fills reclaims nothing: retirement is what a
    // full table does, not a policy of forgetting as soon as possible.
    let mut roomy = CommandTable::with_capacity(8);
    roomy.initialize(cmd(0), payload(0), k("key")).unwrap();
    roomy
        .accept(cmd(0), roomy.record(&cmd(0)).unwrap().deps.clone())
        .unwrap();
    roomy.commit(cmd(0)).unwrap();
    roomy.execute(cmd(0)).unwrap();
    roomy.initialize(cmd(1), payload(1), k("key")).unwrap();
    assert_eq!(roomy.phase_of(&cmd(0)), Some(Phase::Executed));
    assert_eq!(roomy.len(), 2);
}

/// Run `command` through to EXECUTED under its own dependencies.
fn run_through(
    t: &mut CommandTable,
    command: CommandId,
    i: u8,
    keys: Vec<Vec<u8>>,
) -> Vec<CommandId> {
    let deps = t.initialize(command, payload(i), keys).unwrap().deps;
    t.accept(command, deps.clone()).unwrap();
    t.commit(command).unwrap();
    t.execute(command).unwrap();
    deps
}

/// The command after a reclaim still depends on the key's latest command,
/// retired or not (task-d06).
///
/// The table reclaims exactly when it is full, and before it computes the
/// next command's dependencies. Retirement used to clear the key's latest,
/// so the first command a full leader proposed named no dependency at all;
/// a follower still behind the command it should have named found it
/// ready and executed it first. On the leader nothing looked wrong: what
/// it retired had executed there.
#[test]
fn the_command_after_a_reclaim_still_depends_on_the_last_one() {
    let capacity = 4u8;
    let mut t = CommandTable::with_capacity(usize::from(capacity));
    let mut previous = None;
    for i in 1..=capacity {
        let deps = run_through(&mut t, cmd(i), i, k("conservative"));
        assert_eq!(deps, previous.into_iter().collect::<Vec<_>>());
        previous = Some(cmd(i));
    }
    // Full, so this reclaims every executed record first.
    let next = t
        .initialize(cmd(100), payload(100), k("conservative"))
        .unwrap();
    assert_eq!(
        next.deps,
        vec![cmd(capacity)],
        "the chain broke at the reclaim"
    );
    assert!(t.record(&cmd(capacity)).is_none(), "it was retired");
    assert_eq!(t.phase_of(&cmd(capacity)), Some(Phase::Executed));
    assert_eq!(t.conflicts(&k("conservative")), vec![cmd(100)]);
}

/// A key's latest command keeps its tombstone however many retirements
/// follow on other keys, because the next command on that key will name
/// it and the guards must answer for it (task-d06). Every other tombstone
/// still goes by age.
#[test]
fn a_keys_latest_tombstone_outlives_the_bound() {
    let capacity = 2u8;
    let mut t = CommandTable::with_capacity(usize::from(capacity));
    run_through(&mut t, cmd(1), 1, k("quiet"));
    t.retire(&cmd(1)).unwrap();
    for i in 2..=8u8 {
        run_through(&mut t, cmd(i), i, k("busy"));
        t.retire(&cmd(i)).unwrap();
    }
    assert!(
        t.tombstones().contains(&cmd(1)),
        "the quiet key's latest lost its tombstone"
    );
    assert!(!t.tombstones().contains(&cmd(2)), "an old tombstone stayed");
    let next = t.initialize(cmd(50), payload(50), k("quiet")).unwrap();
    assert_eq!(next.deps, vec![cmd(1)]);
    t.accept(cmd(50), next.deps)
        .expect("the guard answers for it");
    // Superseded, it goes in its turn.
    t.commit(cmd(50)).unwrap();
    t.execute(cmd(50)).unwrap();
    t.retire(&cmd(50)).unwrap();
    for i in 9..=12u8 {
        run_through(&mut t, cmd(i), i, k("busy"));
        t.retire(&cmd(i)).unwrap();
    }
    assert!(
        !t.tombstones().contains(&cmd(1)),
        "a superseded tombstone stayed"
    );
    assert!(
        t.tombstones().contains(&cmd(50)),
        "the quiet key's new latest"
    );
}
