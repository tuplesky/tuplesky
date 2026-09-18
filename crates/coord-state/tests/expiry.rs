//! task-16 acceptance: renewal/expiry permutations, delayed old leader,
//! restart, no quorum and fast-clock bounds checked against true
//! simulator time; stale timers cannot delete renewed or rebound keys; a
//! delayed reply creates no new TTL anchor.

mod common;

use std::collections::{BTreeMap, VecDeque};

use common::*;
use coord_state::{
    ClockAssumptions, ExpiryOutcome, InternalCommand, LeaseObservation, LeaseScheduler,
    LeaseSnapshot, LeaseStatus, Outcome, TimerRequest,
};
use coord_types::ids::*;
use proptest::prelude::*;

/// Nominal local ticks per real second.
const TPS: u64 = 1_000;
/// Documented maximum fast clock-rate error: 5%.
const RHO_PPM: u32 = 50_000;

fn assumptions() -> ClockAssumptions {
    ClockAssumptions {
        ticks_per_second: TPS,
        max_fast_rate_ppm: RHO_PPM,
    }
}

fn snapshot(f: &Fixture, id: LeaseId) -> LeaseSnapshot {
    let r = f.record(id);
    LeaseSnapshot {
        namespace: NS,
        lease_id: id,
        generation: r.generation,
        renewal_sequence: r.renewal_sequence,
        ttl_seconds: r.ttl_seconds,
    }
}

fn expire_fields(c: &InternalCommand) -> (LeaseId, u64, u64) {
    match c {
        InternalCommand::ExpireLease {
            lease_id,
            expected_renewal_sequence,
            authority_epoch,
            ..
        } => (*lease_id, *expected_renewal_sequence, authority_epoch.get()),
        other => panic!("{other:?}"),
    }
}

#[test]
fn renewal_and_expiry_permutations_at_the_state_machine() {
    // Renewal ordered first makes the old expiry a no-op.
    let mut f = Fixture::new();
    f.run_internal(&establish(1));
    f.run(&grant(L1, 10));
    f.run(&put(b"a", b"1", Some(L1)));
    assert_eq!(
        f.run(&keep_alive(L1)).response.outcome,
        Outcome::LeaseKeptAlive {
            lease_id: L1,
            generation: LeaseGeneration::new(1).unwrap(),
            renewal_sequence: 1,
            ttl_seconds: 10
        }
    );
    let p = f.run_internal(&expire(L1, 1, 0, 1));
    assert_eq!(p.response.outcome, Outcome::ExpireStale);
    assert!(p.mutations.is_empty() && p.revision.is_none());
    assert!(f.current.contains_key(b"a".as_slice()));
    // Matching expiration deletes the attachments in one revision.
    let p = f.run_internal(&expire(L1, 1, 1, 1));
    assert_eq!(p.response.outcome, Outcome::LeaseExpired { deleted: 1 });
    assert_eq!(p.revision, Some(rev(2)));
    assert_eq!(f.record(L1).status, LeaseStatus::Expired);
    assert!(!f.current.contains_key(b"a".as_slice()));
    // Expiry first makes a later renewal LeaseNotFound; a second matching
    // expiration is stale, not a double delete.
    assert_eq!(
        f.run(&keep_alive(L1)).response.outcome,
        Outcome::ErrLeaseNotFound
    );
    assert_eq!(
        f.run_internal(&expire(L1, 1, 1, 1)).response.outcome,
        Outcome::ExpireStale
    );
    assert_eq!(f.revision, 2);
    // Wrong generation, unknown lease and a revoked lease are all stale.
    f.run(&grant(L2, 10));
    assert_eq!(
        f.run_internal(&expire(L2, 2, 0, 1)).response.outcome,
        Outcome::ExpireStale
    );
    assert_eq!(
        f.run_internal(&expire(LeaseId([9; 16]), 1, 0, 1))
            .response
            .outcome,
        Outcome::ExpireStale
    );
    f.run(&revoke(L2));
    assert_eq!(
        f.run_internal(&expire(L2, 1, 0, 1)).response.outcome,
        Outcome::ExpireStale
    );
    // Renewal is owner-only and a retry (same view) yields the same sequence
    // without a second increment: the retained result is what a retry gets.
    f.run(&grant(LeaseId([3; 16]), 10));
    assert_eq!(
        f.run_as(BOB, &keep_alive(LeaseId([3; 16])))
            .response
            .outcome,
        Outcome::ErrLeasePermission
    );
    assert_eq!(f.record(LeaseId([3; 16])).renewal_sequence, 0);
}

#[test]
fn authority_epochs_fence_former_leaders_and_only_advance() {
    let mut f = Fixture::new();
    f.run(&grant(L1, 10));
    f.run(&put(b"a", b"1", Some(L1)));
    // No authority established: an expiration under epoch 1 is rejected.
    assert_eq!(
        f.run_internal(&expire(L1, 1, 0, 1)).response.outcome,
        Outcome::ErrStaleAuthority
    );
    assert_eq!(
        f.run_internal(&establish(1)).response.outcome,
        Outcome::LeaseAuthorityEstablished {
            epoch: LeaseAuthorityEpoch::new(1).unwrap()
        }
    );
    assert_eq!(
        f.run_internal(&establish(1)).response.outcome,
        Outcome::ErrStaleAuthority,
        "epochs only advance"
    );
    // A former leader (epoch 1) after a successor (epoch 2) established.
    let mut old = LeaseScheduler::new(assumptions());
    old.establish(LeaseAuthorityEpoch::new(1).unwrap(), 0, [snapshot(&f, L1)]);
    f.run_internal(&establish(2));
    let due = old.poll(old.next_deadline().unwrap());
    assert_eq!(due.len(), 1);
    let p = f.run_internal(&due[0]);
    assert_eq!(p.response.outcome, Outcome::ErrStaleAuthority);
    assert!(
        f.current.contains_key(b"a".as_slice()),
        "delayed old leader deletes nothing"
    );
    old.outcome(L1, 0, ExpiryOutcome::Stale);
    assert!(old.is_empty());
    // The successor's expiration applies.
    let mut new = LeaseScheduler::new(assumptions());
    new.establish(LeaseAuthorityEpoch::new(2).unwrap(), 0, [snapshot(&f, L1)]);
    let due = new.poll(new.next_deadline().unwrap());
    assert_eq!(
        f.run_internal(&due[0]).response.outcome,
        Outcome::LeaseExpired { deleted: 1 }
    );
}

#[test]
fn stale_timers_and_duplicate_observations_cannot_delete_renewed_or_rebound_keys() {
    let mut f = Fixture::new();
    f.run_internal(&establish(1));
    f.run(&grant(L1, 2));
    f.run(&grant(L2, 100));
    f.run(&put(b"a", b"1", Some(L1)));
    let mut s = LeaseScheduler::new(assumptions());
    let timers = s.establish(
        LeaseAuthorityEpoch::new(1).unwrap(),
        1_000,
        [snapshot(&f, L1)],
    );
    assert_eq!(timers.len(), 1);
    let t1 = timers[0];
    assert_eq!(t1.at_tick, 1_000 + assumptions().wait_ticks(2));
    assert_eq!(
        assumptions().wait_ticks(2),
        2_100,
        "(1 + rho) * TTL local units"
    );
    // A renewal commits; its observation rearms from the observation tick.
    f.run(&keep_alive(L1));
    let t2 = s
        .observe(1_500, LeaseObservation::Alive(snapshot(&f, L1)))
        .unwrap();
    assert_eq!(t2.at_tick, 1_500 + 2_100);
    assert!(t2.generation > t1.generation);
    // The old timer firing late is a no-op even past its deadline.
    assert_eq!(s.fire(5_000, t1), None);
    // A delayed duplicate of the same renewal creates no new anchor.
    assert_eq!(
        s.observe(3_000, LeaseObservation::Alive(snapshot(&f, L1))),
        None
    );
    assert_eq!(s.remaining(&L1, 3_000).unwrap().deadline_tick, 3_600);
    // An older observation (delayed reply of the grant) creates none either.
    let mut older = snapshot(&f, L1);
    older.renewal_sequence = 0;
    assert_eq!(s.observe(3_100, LeaseObservation::Alive(older)), None);
    // Even an expiration built from the stale state cannot delete: the
    // renewal sequence no longer matches.
    assert_eq!(
        f.run_internal(&expire(L1, 1, 0, 1)).response.outcome,
        Outcome::ExpireStale
    );
    // The key is rebound to L2 before L1's deadline; L1's expiration then
    // deletes nothing because the key is no longer its attachment.
    f.run(&put(b"a", b"2", Some(L2)));
    let due = s.fire(3_600, t2).unwrap();
    assert_eq!(expire_fields(&due), (L1, 1, 1));
    let p = f.run_internal(&due);
    assert_eq!(p.response.outcome, Outcome::LeaseExpired { deleted: 0 });
    assert!(
        f.current.contains_key(b"a".as_slice()),
        "rebound key survives"
    );
    assert_eq!(f.current[b"a".as_slice()].lease, Some(L2));
    s.outcome(L1, 1, ExpiryOutcome::Applied);
    assert!(s.is_empty());
    assert_eq!(s.poll(10_000), vec![]);
}

#[test]
fn no_quorum_anomalies_and_recovery_are_conservative() {
    let mut f = Fixture::new();
    f.run_internal(&establish(1));
    f.run(&grant(L1, 1));
    let mut s = LeaseScheduler::new(assumptions());
    assert_eq!(s.poll(u64::MAX), vec![], "nothing before authority");
    s.establish(LeaseAuthorityEpoch::new(1).unwrap(), 0, [snapshot(&f, L1)]);
    let due = s.poll(1_050);
    assert_eq!(due.len(), 1);
    assert_eq!(s.poll(1_050), vec![], "in flight: not emitted twice");
    // No quorum: the expiration could not be ordered; it is retried later,
    // and a renewal cannot commit either (the fixture simply does not run it).
    s.outcome(L1, 0, ExpiryOutcome::Unavailable);
    assert_eq!(s.poll(1_060).len(), 1);
    s.outcome(L1, 0, ExpiryOutcome::Unavailable);
    // A detected clock anomaly suspends expiration; resuming rearms the full
    // TTL from the resumption point.
    s.suspend();
    assert!(s.is_suspended());
    assert_eq!(s.poll(9_000), vec![]);
    let rearmed = s.resume(9_000);
    assert_eq!(rearmed.len(), 1);
    assert_eq!(rearmed[0].at_tick, 9_000 + 1_050);
    assert_eq!(s.poll(9_000 + 1_049), vec![]);
    let stale = s.poll(9_000 + 1_050);
    assert_eq!(stale.len(), 1);
    // Restart / failover: a fresh scheduler under a new epoch arms every
    // surviving lease for its full TTL from the recovery observation.
    f.run_internal(&establish(2));
    let mut recovered = LeaseScheduler::new(assumptions());
    let timers = recovered.establish(
        LeaseAuthorityEpoch::new(2).unwrap(),
        20_000,
        [snapshot(&f, L1)],
    );
    assert_eq!(timers[0].at_tick, 21_050);
    let est = recovered.remaining(&L1, 20_500).unwrap();
    assert_eq!(est.anchor_tick, 20_000);
    assert_eq!(est.remaining_ticks, 550);
    assert_eq!(est.authority_epoch.get(), 2);
    assert_eq!(recovered.remaining(&L2, 20_500), None);
    // The old scheduler's late expiration is fenced by the state machine.
    assert_eq!(expire_fields(&stale[0]).2, 1);
    assert_eq!(
        f.run_internal(&stale[0]).response.outcome,
        Outcome::ErrStaleAuthority
    );
}

// ---------------------------------------------------------------------------
// Randomised world against true time.

#[derive(Clone, Debug)]
enum Op {
    Grant { lease: u8, ttl: u32 },
    Renew(u8),
    Revoke(u8),
    Advance(u64),
    Failover { node: u8 },
    Restart,
    QuorumLoss(u64),
    ClockAnomaly,
    Duplicate(u8),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (0u8..4, 1u32..6).prop_map(|(lease, ttl)| Op::Grant { lease, ttl }),
        6 => (0u8..4).prop_map(Op::Renew),
        1 => (0u8..4).prop_map(Op::Revoke),
        8 => (1u64..4_000).prop_map(Op::Advance),
        1 => (0u8..3).prop_map(|node| Op::Failover { node }),
        1 => Just(Op::Restart),
        1 => (500u64..3_000).prop_map(Op::QuorumLoss),
        1 => Just(Op::ClockAnomaly),
        2 => (0u8..4).prop_map(Op::Duplicate),
    ]
}

fn lease(i: u8) -> LeaseId {
    LeaseId([i + 1; 16])
}

struct Node {
    scheduler: LeaseScheduler,
    /// Actual rate error of this node's clock in ppm (fast when positive;
    /// never faster than the documented `RHO_PPM`).
    rate_ppm: i64,
    pending: VecDeque<(u64, LeaseObservation)>,
    timers: Vec<TimerRequest>,
}

impl Node {
    fn local(&self, true_tick: u64) -> u64 {
        let scaled = i128::from(true_tick) * (1_000_000 + i128::from(self.rate_ppm));
        u64::try_from(scaled / 1_000_000).unwrap()
    }
}

struct World {
    f: Fixture,
    now: u64,
    nodes: Vec<Node>,
    leader: Option<usize>,
    epoch: u64,
    quorum_until: u64,
    anomaly: Option<usize>,
    /// True tick of the last committed grant/renewal per lease.
    anchor: BTreeMap<LeaseId, u64>,
    last_snapshot: BTreeMap<LeaseId, LeaseSnapshot>,
    expired: u64,
}

impl World {
    fn new(rates: [i64; 3]) -> Self {
        World {
            f: Fixture::new(),
            now: 0,
            nodes: rates
                .into_iter()
                .map(|rate_ppm| Node {
                    scheduler: LeaseScheduler::new(assumptions()),
                    rate_ppm,
                    pending: VecDeque::new(),
                    timers: Vec::new(),
                })
                .collect(),
            leader: None,
            epoch: 0,
            quorum_until: 0,
            anomaly: None,
            anchor: BTreeMap::new(),
            last_snapshot: BTreeMap::new(),
            expired: 0,
        }
    }

    fn quorum(&self) -> bool {
        self.now >= self.quorum_until
    }

    fn broadcast(&mut self, obs: LeaseObservation, delay: u64) {
        let at = self.now + delay;
        for n in &mut self.nodes {
            n.pending.push_back((at, obs));
        }
    }

    fn commit_alive(&mut self, id: LeaseId, delay: u64) {
        self.anchor.insert(id, self.now);
        let snap = snapshot(&self.f, id);
        self.last_snapshot.insert(id, snap);
        self.broadcast(LeaseObservation::Alive(snap), delay);
    }

    fn establish_on(&mut self, node: usize) {
        self.epoch += 1;
        assert!(matches!(
            self.f.run_internal(&establish(self.epoch)).response.outcome,
            Outcome::LeaseAuthorityEstablished { .. }
        ));
        self.leader = Some(node);
        let active: Vec<LeaseSnapshot> = self
            .f
            .leases
            .iter()
            .filter(|(_, r)| r.status == LeaseStatus::Active)
            .map(|(id, _)| snapshot(&self.f, *id))
            .collect();
        let local = self.nodes[node].local(self.now);
        let epoch = LeaseAuthorityEpoch::new(self.epoch).unwrap();
        let n = &mut self.nodes[node];
        n.pending.clear();
        n.timers = n.scheduler.establish(epoch, local, active);
    }

    fn apply(&mut self, op: Op, delay: u64) {
        match op {
            Op::Grant { lease: i, ttl } if self.quorum() => {
                let id = lease(i);
                if matches!(
                    self.f.run(&grant(id, ttl)).response.outcome,
                    Outcome::LeaseGranted { .. }
                ) {
                    self.f.run(&put(&[b'k', i], b"v", Some(id)));
                    self.commit_alive(id, delay);
                }
            }
            Op::Renew(i) if self.quorum() => {
                let id = lease(i);
                if matches!(
                    self.f.run(&keep_alive(id)).response.outcome,
                    Outcome::LeaseKeptAlive { .. }
                ) {
                    self.commit_alive(id, delay);
                }
            }
            Op::Revoke(i) if self.quorum() => {
                let id = lease(i);
                if matches!(
                    self.f.run(&revoke(id)).response.outcome,
                    Outcome::LeaseRevoked { .. }
                ) {
                    self.broadcast(LeaseObservation::Ended(id), delay);
                }
            }
            Op::Grant { .. } | Op::Renew(_) | Op::Revoke(_) => {}
            Op::Advance(ticks) => self.advance(ticks),
            Op::Failover { node } if self.quorum() => self.establish_on(node as usize),
            Op::Restart if self.quorum() => {
                if let Some(l) = self.leader {
                    self.nodes[l].scheduler = LeaseScheduler::new(assumptions());
                    self.nodes[l].timers.clear();
                    self.establish_on(l);
                }
            }
            Op::Failover { .. } | Op::Restart => {}
            Op::QuorumLoss(ticks) => self.quorum_until = self.now + ticks,
            Op::ClockAnomaly => {
                if let Some(l) = self.leader
                    && self.anomaly.is_none()
                {
                    self.nodes[l].scheduler.suspend();
                    self.anomaly = Some(l);
                }
            }
            Op::Duplicate(i) => {
                if let Some(snap) = self.last_snapshot.get(&lease(i)).copied() {
                    // A delayed reply / redelivery of an older observation.
                    self.broadcast(LeaseObservation::Alive(snap), delay);
                }
            }
        }
    }

    /// Advance true time one tick at a time, delivering observations,
    /// firing timers and polling every scheduler (old leaders included).
    fn advance(&mut self, ticks: u64) {
        for _ in 0..ticks {
            self.now += 1;
            // Anomalies clear after a while; resume rearms conservatively.
            if let Some(l) = self.anomaly
                && self.now.is_multiple_of(700)
            {
                let local = self.nodes[l].local(self.now);
                let t = self.nodes[l].scheduler.resume(local);
                self.nodes[l].timers.extend(t);
                self.anomaly = None;
            }
            for n in 0..self.nodes.len() {
                let local = self.nodes[n].local(self.now);
                while let Some((at, _)) = self.nodes[n].pending.front()
                    && *at <= self.now
                {
                    let (_, obs) = self.nodes[n].pending.pop_front().unwrap();
                    if let Some(t) = self.nodes[n].scheduler.observe(local, obs) {
                        self.nodes[n].timers.push(t);
                    }
                }
                let fired: Vec<TimerRequest> = self.nodes[n]
                    .timers
                    .iter()
                    .filter(|t| t.at_tick <= local)
                    .copied()
                    .collect();
                self.nodes[n].timers.retain(|t| t.at_tick > local);
                let mut commands = Vec::new();
                for t in fired {
                    commands.extend(self.nodes[n].scheduler.fire(local, t));
                }
                if self.now.is_multiple_of(97) {
                    commands.extend(self.nodes[n].scheduler.poll(local));
                }
                for c in commands {
                    let (id, seq, _) = expire_fields(&c);
                    if !self.quorum() {
                        self.nodes[n]
                            .scheduler
                            .outcome(id, seq, ExpiryOutcome::Unavailable);
                        continue;
                    }
                    let outcome = self.f.run_internal(&c).response.outcome;
                    let result = match outcome {
                        Outcome::LeaseExpired { .. } => {
                            let ttl = u64::from(self.f.record(id).ttl_seconds) * TPS;
                            let anchor = self.anchor[&id];
                            assert!(
                                self.now >= anchor + ttl,
                                "lease {id:?} expired at true tick {} but was anchored at {anchor} with ttl {ttl} ticks (node {n}, rate {} ppm)",
                                self.now,
                                self.nodes[n].rate_ppm
                            );
                            self.expired += 1;
                            self.broadcast(LeaseObservation::Ended(id), 0);
                            ExpiryOutcome::Applied
                        }
                        Outcome::ExpireStale | Outcome::ErrStaleAuthority => ExpiryOutcome::Stale,
                        other => panic!("{other:?}"),
                    };
                    self.nodes[n].scheduler.outcome(id, seq, result);
                }
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]

    /// Under the documented clock assumptions no lease ever expires before
    /// its TTL of true time has passed since its last committed grant or
    /// renewal, through renewals, delayed and duplicated observations,
    /// failovers, restarts, quorum loss and clock anomalies; and once the
    /// world settles every lease that is not renewed does expire.
    #[test]
    fn leases_never_expire_early_and_eventually_expire(
        ops in prop::collection::vec(op(), 1..60),
        delays in prop::collection::vec(0u64..1_500, 60),
        rates in prop::array::uniform3(-100_000i64..=i64::from(RHO_PPM)),
    ) {
        let mut w = World::new(rates);
        w.establish_on(0);
        for (i, op) in ops.into_iter().enumerate() {
            w.apply(op, delays[i]);
        }
        // Settle: quorum back, anomalies cleared, a live leader, and enough
        // true time for the slowest allowed clock to reach every deadline.
        w.quorum_until = 0;
        if w.anomaly.is_some() || w.leader.is_none() {
            w.establish_on(1);
        }
        let worst = 3 * assumptions().wait_ticks(5) + 1_500;
        w.advance(worst);
        for (id, r) in &w.f.leases {
            prop_assert_ne!(r.status, LeaseStatus::Active, "lease {:?} never expired", id);
        }
        // Attachments of ended leases are gone.
        prop_assert!(w.f.current.is_empty());
    }
}
