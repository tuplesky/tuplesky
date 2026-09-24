//! Driving lease and private-TTL expiry from the leader (design Sections
//! 6.6, 7.2-7.3).
//!
//! The deterministic half of this is `coord_state::expiry::LeaseScheduler`:
//! it arms deadlines under a replicated authority epoch, rearms
//! conservatively, and turns a passed deadline into a *conditional*
//! command. What it never does is mutate anything, and what it cannot do
//! is see the domain. This is the other half -- the part that reads
//! committed state, counts local time, and puts the candidates the
//! scheduler produces through the ordinary replicated path.
//!
//! Three rules it exists to keep.
//!
//! * **A timer is a scheduling hint, never permission.** Every candidate
//!   is a command whose every field the state machine rechecks: a
//!   renewal ordered first makes it a no-op, a rebinding of the key
//!   invalidates it, and a former leader's epoch is refused. So a clock
//!   this node got wrong costs a wasted command, not a deleted key.
//! * **Late is allowed; early is not.** The deadline waits
//!   `(1 + rho) * TTL` local ticks for a documented maximum fast
//!   clock-rate error, anchored to the observation of *committed* state
//!   rather than to a reply's arrival. A restart or a failover rearms
//!   every surviving lease for its full TTL from the new epoch's
//!   observation.
//! * **Nobody expires without a quorum.** The candidate is an ordinary
//!   replicated command. A node that cannot reach a quorum proposes one
//!   and it does not execute, which is the same thing as not expiring.
//!
//! Two things make that hold over time rather than only on a quiet,
//! loss-free path. The driver is woken when its next deadline passes
//! (it implements [`crate::serve::Deadline`], which the domain loop
//! sleeps on), so expiry does not wait for unrelated traffic. And a
//! proposal that is not seen to resolve within [`RETRY_TICKS`] is
//! presented again, which makes the leader publish it to the voters
//! again: a proposal lost on its way to a quorum is retried, not
//! abandoned.

use coord_state::expiry::{
    ClockAssumptions, ExpiryOutcome, LeaseObservation, LeaseScheduler, LeaseSnapshot,
};
use coord_state::lease::LeaseStatus;
use coord_storage::Persistence;
use coord_storage::views::ViewBudget;
use coord_types::ids::{
    ClientInstanceId, ClusterId, DomainId, LeaseAuthorityEpoch, LeaseGeneration, LeaseId,
    RequestSequence, SessionId,
};
use coord_types::logical_v1::{CanonicalOperation, LogicalRequest};
use coord_types::wire_v1::{MessageV1, RequestV1};
use coord_types::{NamespaceId, RetryKey};

/// Local ticks per second the scheduler counts in.
const TICKS_PER_SECOND: u64 = 1_000;

/// Documented maximum fast clock-rate error, in parts per million.
///
/// Generous on purpose: the cost of assuming a faster clock than the host
/// has is that expiry is late, and late is the side of this that is
/// allowed to be wrong.
const MAX_FAST_RATE_PPM: u32 = 5_000;

/// How often committed lease state is read back.
///
/// A scan is a bounded read of the `lease_v1` rows, and it is what turns
/// a grant, a renewal or a revocation somebody else ordered into an
/// observation this scheduler can anchor to. Between scans the scheduler
/// is working from what it last saw, which is exactly the conservatism
/// the design asks for: an anchor is never later than the observation it
/// came from.
const SCAN_INTERVAL_TICKS: u64 = 250;

/// How long a proposal this driver made may go unresolved before it is
/// presented again.
///
/// Resolution is read from committed state -- the authority installed,
/// the lease gone or renewed -- so this is how long the driver waits for
/// that before concluding the proposal may never have reached a quorum
/// (a send that failed, a peer that was not connected yet). Presenting
/// it again costs one republished proposal and is harmless if the first
/// one did arrive: the leader publishes an unlearned proposal again and
/// treats a learned one as the duplicate it is.
pub const RETRY_TICKS: u64 = 1_000;

/// The expiry driver of one domain on this node.
pub struct Expiry {
    scheduler: LeaseScheduler,
    started: std::time::Instant,
    cluster: ClusterId,
    domain: DomainId,
    /// The epoch this node has asked the cluster to install, and the
    /// tick it last presented it at, while it is still in flight. Cleared
    /// once committed state shows it.
    proposing: Option<(LeaseAuthorityEpoch, u64)>,
    /// Expiry candidates proposed and not yet seen to resolve: the
    /// renewal sequence each was proposed for, and the tick it was.
    in_flight: std::collections::BTreeMap<LeaseId, (u64, u64)>,
    /// Local tick of the last observation scan; `None` before the first.
    scanned: Option<u64>,
    /// Commands this replica had applied when committed state was last
    /// read, and whether it has applied more since. A grant, a renewal
    /// or a revocation is an applied command, so this is what says a
    /// scan has something new to see.
    applied: u64,
    unobserved: bool,
    /// Authority epochs proposed, and expiry candidates proposed.
    pub done: (u64, u64),
}

impl Expiry {
    /// A driver for `domain`, with no authority yet.
    pub fn new(cluster: ClusterId, domain: DomainId) -> Self {
        Expiry {
            scheduler: LeaseScheduler::new(ClockAssumptions {
                ticks_per_second: TICKS_PER_SECOND,
                max_fast_rate_ppm: MAX_FAST_RATE_PPM,
            }),
            started: std::time::Instant::now(),
            cluster,
            domain,
            proposing: None,
            in_flight: std::collections::BTreeMap::new(),
            scanned: None,
            applied: 0,
            unobserved: false,
            done: (0, 0),
        }
    }

    /// Leases currently armed (diagnostic).
    pub fn armed(&self) -> usize {
        self.scheduler.len()
    }

    /// The local tick now.
    fn now(&self) -> u64 {
        let elapsed = self.started.elapsed();
        u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
    }

    /// What this node should propose now: the authority epoch it needs
    /// before it may schedule anything, then the expirations that have
    /// come due, then any of either that has gone unresolved too long.
    ///
    /// `applied` is how many commands this replica has applied so far;
    /// a change since the last read is what makes the next scan worth
    /// waking for.
    ///
    /// Returns encoded request frames, because what a voter proposes is
    /// a frame; building them here keeps the serving loop from knowing
    /// how a service command is spelled.
    pub fn due<P: Persistence>(&mut self, store: &P, applied: u64) -> Vec<Vec<u8>> {
        let now = self.now();
        if applied != self.applied {
            self.applied = applied;
            self.unobserved = true;
        }
        // Reading committed lease state is a scan, so it happens on an
        // interval rather than every turn. Between scans the scheduler
        // works from what it last saw, which is the conservative
        // direction: an anchor is never later than the observation it
        // came from, so a renewal this node has not seen yet can only
        // make an expiry candidate stale -- and a stale candidate is a
        // no-op, not a deletion.
        let state = if self.scan_due(now) {
            // Counted as a scan whether or not the read succeeds: a read
            // that fails changes nothing, and the next interval tries
            // again, so a scheduler holding its old anchors expires late
            // rather than early -- and a failing read is not retried in
            // a loop.
            self.scanned = Some(now);
            self.unobserved = false;
            match self.read(store) {
                Some(state) => Some(state),
                None => return Vec::new(),
            }
        } else {
            None
        };
        self.decide(now, state.as_ref())
    }

    fn scan_due(&self, now: u64) -> bool {
        self.scanned
            .is_none_or(|last| now.saturating_sub(last) >= SCAN_INTERVAL_TICKS)
    }

    /// The decision half of [`Expiry::due`], at tick `now`, given what a
    /// scan read if one was due.
    fn decide(&mut self, now: u64, state: Option<&Committed>) -> Vec<Vec<u8>> {
        if let Some(state) = state {
            if self.scheduler.epoch() != Some(state.authority) {
                return self.take_authority(now, state);
            }
            self.observe(now, state);
            self.retry_unresolved(now);
        }
        let candidates = self.scheduler.poll(now);
        self.done.1 += candidates.len() as u64;
        candidates
            .iter()
            .filter_map(|c| match c {
                coord_state::InternalCommand::ExpireLease {
                    namespace,
                    lease_id,
                    generation,
                    expected_renewal_sequence,
                    authority_epoch,
                } => {
                    self.in_flight
                        .insert(*lease_id, (*expected_renewal_sequence, now));
                    self.frame(
                        *namespace,
                        CanonicalOperation::ExpireLease {
                            lease_id: *lease_id,
                            generation: *generation,
                            expected_renewal_sequence: *expected_renewal_sequence,
                            authority_epoch: *authority_epoch,
                        },
                        expiry_key(
                            self.cluster,
                            self.domain,
                            *lease_id,
                            *generation,
                            *expected_renewal_sequence,
                            *authority_epoch,
                        ),
                    )
                }
                // The scheduler produces nothing else.
                _ => None,
            })
            .collect()
    }

    /// Hand back to the scheduler every expiry candidate that has gone
    /// [`RETRY_TICKS`] without committed state showing how it ended.
    ///
    /// Run only straight after a scan, so "unresolved" means "not
    /// resolved as of a read just taken": an expiry that applied has
    /// ended its lease, and one that was stale has been superseded by a
    /// renewal the same scan rearmed, and both are gone from the
    /// scheduler's in-flight set before this looks. What is left may
    /// never have reached a quorum, and reporting it `Unavailable` lets
    /// the next poll produce it again -- the same candidate, under the
    /// same invocation, which the leader publishes to the voters again.
    fn retry_unresolved(&mut self, now: u64) {
        let armed: std::collections::BTreeSet<LeaseId> =
            self.scheduler.armed_ids().into_iter().collect();
        let scheduler = &mut self.scheduler;
        self.in_flight.retain(|lease, (sequence, at)| {
            if !armed.contains(lease) {
                return false;
            }
            if now.saturating_sub(*at) < RETRY_TICKS {
                return true;
            }
            // A no-op if a newer renewal already rearmed the lease: the
            // scheduler only takes an outcome for the candidate it has.
            scheduler.outcome(*lease, *sequence, ExpiryOutcome::Unavailable);
            false
        });
    }

    /// Take authority for scheduling, asking the cluster to install a
    /// new epoch and arming every surviving lease from this observation.
    ///
    /// A new epoch per boot, not per domain. Design Section 7.2 has a
    /// recovered leader establish one before it schedules anything,
    /// precisely so that a predecessor's timers cannot delete a key
    /// after this node has taken over: an expiry carries the epoch it
    /// was scheduled under, and the state machine refuses one from an
    /// older epoch outright.
    ///
    /// Arming happens when the epoch commits, from the state observed
    /// then, so every surviving lease gets its full TTL from that
    /// observation rather than from whatever the previous authority had
    /// counted. Late, never early.
    fn take_authority(&mut self, now: u64, state: &Committed) -> Vec<Vec<u8>> {
        // Whatever was in flight was scheduled under an authority that is
        // not the one committed state names; the scheduler is about to be
        // rebuilt from this observation, and so is what it has proposed.
        self.in_flight.clear();
        if let Some((mine, presented)) = self.proposing {
            if state.authority == mine {
                // Installed. Arm from this observation and start.
                self.scheduler
                    .establish(mine, now, state.snapshots.iter().copied());
                self.proposing = None;
                return Vec::new();
            }
            if state.authority < mine {
                // Still in flight. Once it has gone unresolved for
                // `RETRY_TICKS` it is presented again, under the same
                // invocation: the leader publishes a bound but unlearned
                // service command to the voters again, so a first
                // proposal a quorum never heard is proposed again rather
                // than refused as a duplicate. Sooner than that it is
                // left alone -- it is most likely simply on its way.
                if now.saturating_sub(presented) < RETRY_TICKS {
                    return Vec::new();
                }
                self.proposing = Some((mine, now));
                return self
                    .frame(
                        state.namespace,
                        CanonicalOperation::EstablishLeaseAuthority { epoch: mine },
                        authority_key(self.cluster, self.domain, mine),
                    )
                    .into_iter()
                    .collect();
            }
            // Somebody established a later epoch. This node's is spent.
            self.proposing = None;
        }
        let Some(next) = state.authority.get().checked_add(1) else {
            return Vec::new();
        };
        let next = LeaseAuthorityEpoch::new(next).expect("one more than an epoch is positive");
        self.proposing = Some((next, now));
        self.done.0 += 1;
        self.frame(
            state.namespace,
            CanonicalOperation::EstablishLeaseAuthority { epoch: next },
            authority_key(self.cluster, self.domain, next),
        )
        .into_iter()
        .collect()
    }

    /// Feed the scheduler what committed state now says: a grant or
    /// renewal newer than what is armed rearms from this observation,
    /// and a lease that is gone is forgotten.
    fn observe(&mut self, now: u64, state: &Committed) {
        let mut seen = alloc_set(&state.snapshots);
        for snapshot in &state.snapshots {
            self.scheduler
                .observe(now, LeaseObservation::Alive(*snapshot));
        }
        let armed: Vec<LeaseId> = self.scheduler.armed_ids();
        for lease in armed {
            if !seen.remove(&lease) {
                self.scheduler.observe(now, LeaseObservation::Ended(lease));
            }
        }
    }

    /// One service command as a request frame.
    fn frame(
        &self,
        namespace: NamespaceId,
        operation: CanonicalOperation,
        retry_key: RetryKey,
    ) -> Option<Vec<u8>> {
        let mut logical = LogicalRequest::new(namespace, operation);
        logical.canonicalize();
        let request = RequestV1::new(retry_key, &logical, 0).ok()?;
        MessageV1::Request(request).encode().ok()
    }

    /// Committed lease state, read once per call.
    fn read<P: Persistence>(&self, store: &P) -> Option<Committed> {
        let gated = store.reader().snapshot().ok()?;
        let authority = coord_storage::codecs::read_lease_authority(gated.view()).ok()?;
        let leases =
            coord_storage::views::active_leases(gated.view(), ViewBudget::default()).ok()?;
        let mut namespace = None;
        let snapshots: Vec<LeaseSnapshot> = leases
            .into_iter()
            .filter(|(_, r)| r.status == LeaseStatus::Active)
            .map(|(lease_id, r)| {
                namespace.get_or_insert(r.namespace);
                LeaseSnapshot {
                    namespace: r.namespace,
                    lease_id,
                    generation: r.generation,
                    renewal_sequence: r.renewal_sequence,
                    ttl_seconds: r.ttl_seconds,
                }
            })
            .collect();
        Some(Committed {
            authority,
            // The authority row is domain-wide, so any namespace of this
            // domain plans it; a domain with no lease yet uses the one
            // its own configuration names.
            namespace: namespace.unwrap_or(self.default_namespace()),
            snapshots,
        })
    }

    /// The namespace a domain-wide command is planned in.
    fn default_namespace(&self) -> NamespaceId {
        NamespaceId(self.domain.0)
    }
}

impl crate::serve::Deadline for Expiry {
    /// The earliest tick this driver has work at, as an instant.
    ///
    /// The next armed deadline, when one is not already in flight; and
    /// the next scan, when a scan has something to find -- an authority
    /// proposal to see installed, commands applied since the last read,
    /// or candidates to see resolved (no earlier than their retry is
    /// due). Each of those is cleared or moved forward by the `due` call
    /// the wake-up leads to, so a deadline that has passed is never
    /// reported twice.
    fn next_deadline(&self) -> Option<std::time::Instant> {
        if self.scheduler.is_suspended() {
            return None;
        }
        let next_scan = self
            .scanned
            .map_or(0, |last| last.saturating_add(SCAN_INTERVAL_TICKS));
        let mut earliest = self.scheduler.next_deadline();
        let mut consider = |tick: u64| {
            earliest = Some(earliest.map_or(tick, |e| e.min(tick)));
        };
        if self.scheduler.epoch().is_none() || self.proposing.is_some() || self.unobserved {
            consider(next_scan);
        }
        if let Some(retry) = self
            .in_flight
            .values()
            .map(|(_, at)| at.saturating_add(RETRY_TICKS))
            .min()
        {
            consider(retry.max(next_scan));
        }
        earliest.map(|tick| {
            self.started
                + std::time::Duration::from_millis(tick.saturating_mul(1_000) / TICKS_PER_SECOND)
        })
    }
}

/// What one read of committed lease state says.
struct Committed {
    authority: LeaseAuthorityEpoch,
    namespace: NamespaceId,
    snapshots: Vec<LeaseSnapshot>,
}

fn alloc_set(snapshots: &[LeaseSnapshot]) -> std::collections::BTreeSet<LeaseId> {
    snapshots.iter().map(|s| s.lease_id).collect()
}

/// The invocation identity of one expiry candidate.
///
/// A service command has no session and no client, so the fields that
/// name those name the candidate instead: which lease, at which
/// ownership generation and renewal sequence, under which authority
/// epoch -- every field of the command it identifies. That is what makes
/// a re-proposal of the same candidate the same command -- resolved from
/// its retry record rather than executed twice -- while a candidate for a
/// later generation, a later renewal or a later epoch is a different one.
///
/// The renewal sequence has to be in it. A renewal ordered ahead of a
/// due expiry makes that expiry a no-op and rearms the same generation;
/// the next candidate for it differs only in the renewal sequence, and
/// under a key without it would be a second payload for an invocation
/// already bound, refused as an identity conflict every time -- a lease
/// that could never expire under this authority again.
fn expiry_key(
    cluster: ClusterId,
    domain: DomainId,
    lease: LeaseId,
    generation: LeaseGeneration,
    renewal_sequence: u64,
    epoch: LeaseAuthorityEpoch,
) -> RetryKey {
    let mut instance = [0u8; 16];
    instance[..8].copy_from_slice(&generation.get().to_be_bytes());
    instance[8..].copy_from_slice(&renewal_sequence.to_be_bytes());
    RetryKey {
        cluster_id: cluster,
        domain_id: domain,
        session_id: SessionId(lease.0),
        client_instance_id: ClientInstanceId(instance),
        request_sequence: RequestSequence::new(epoch.get()).expect("an epoch is positive"),
    }
}

/// The invocation identity of one authority installation.
fn authority_key(cluster: ClusterId, domain: DomainId, epoch: LeaseAuthorityEpoch) -> RetryKey {
    RetryKey {
        cluster_id: cluster,
        domain_id: domain,
        session_id: SessionId([0; 16]),
        client_instance_id: ClientInstanceId([0; 16]),
        request_sequence: RequestSequence::new(epoch.get()).expect("an epoch is positive"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::Deadline;

    const CLUSTER: ClusterId = ClusterId([1; 16]);
    const DOMAIN: DomainId = DomainId([2; 16]);
    const NAMESPACE: NamespaceId = NamespaceId([3; 16]);
    const LEASE: LeaseId = LeaseId([7; 16]);

    fn epoch(n: u64) -> LeaseAuthorityEpoch {
        LeaseAuthorityEpoch::new(n).unwrap()
    }

    fn committed(authority: u64, renewal_sequence: Option<u64>) -> Committed {
        Committed {
            authority: if authority == 0 {
                LeaseAuthorityEpoch::ZERO
            } else {
                epoch(authority)
            },
            namespace: NAMESPACE,
            snapshots: renewal_sequence
                .map(|renewal_sequence| LeaseSnapshot {
                    namespace: NAMESPACE,
                    lease_id: LEASE,
                    generation: LeaseGeneration::new(1).unwrap(),
                    renewal_sequence,
                    ttl_seconds: 1,
                })
                .into_iter()
                .collect(),
        }
    }

    /// The request a frame carries.
    fn request(frame: &[u8]) -> RequestV1 {
        match coord_types::wire_v1::decode_stream(frame)
            .unwrap()
            .as_slice()
        {
            [MessageV1::Request(r)] => r.clone(),
            other => panic!("not one request: {other:?}"),
        }
    }

    /// A driver whose authority epoch 1 is installed at tick 10, with
    /// one lease of a one-second TTL at `renewal_sequence` 0.
    fn established() -> Expiry {
        let mut expiry = Expiry::new(CLUSTER, DOMAIN);
        let asked = expiry.decide(0, Some(&committed(0, Some(0))));
        assert_eq!(asked.len(), 1, "no authority was asked for");
        assert!(expiry.decide(10, Some(&committed(1, Some(0)))).is_empty());
        assert_eq!(expiry.scheduler.epoch(), Some(epoch(1)));
        expiry
    }

    /// An expiry candidate that committed state never shows resolving --
    /// its proposal's sends were lost, say -- is presented again after
    /// `RETRY_TICKS`, as the same command, and not sooner.
    #[test]
    fn an_unresolved_expiry_is_presented_again() {
        let mut expiry = established();
        let wait = expiry.scheduler.next_deadline().expect("armed");
        let first = expiry.decide(wait, None);
        assert_eq!(first.len(), 1, "the due lease produced no candidate");

        // Still the same lease, same renewal: nothing new yet.
        assert!(
            expiry
                .decide(wait + 300, Some(&committed(1, Some(0))))
                .is_empty()
        );
        // Unresolved for longer than the retry interval: the same frame
        // again, which the leader proposes to the voters again.
        let again = expiry.decide(wait + RETRY_TICKS + 1, Some(&committed(1, Some(0))));
        assert_eq!(
            again, first,
            "the unresolved candidate was not presented again"
        );

        // And once the lease is gone, nothing more is presented.
        assert!(
            expiry
                .decide(wait + 3 * RETRY_TICKS, Some(&committed(1, None)))
                .is_empty()
        );
        assert!(expiry.in_flight.is_empty());
    }

    /// The authority proposal is presented again on the same interval,
    /// not on every scan, and never once it is installed.
    #[test]
    fn an_unresolved_authority_is_presented_again_on_the_retry_interval() {
        let mut expiry = Expiry::new(CLUSTER, DOMAIN);
        let first = expiry.decide(0, Some(&committed(0, None)));
        assert_eq!(first.len(), 1);
        assert!(expiry.decide(300, Some(&committed(0, None))).is_empty());
        let again = expiry.decide(RETRY_TICKS + 1, Some(&committed(0, None)));
        assert_eq!(again, first, "the authority was not presented again");
        assert!(
            expiry
                .decide(3 * RETRY_TICKS, Some(&committed(1, None)))
                .is_empty()
        );
        assert_eq!(expiry.scheduler.epoch(), Some(epoch(1)));
    }

    /// A renewal ordered between a candidate's proposal and its
    /// execution makes that candidate a no-op; the lease's next
    /// candidate, for the new renewal sequence, is a different
    /// invocation. Under one key it would be a second payload for a
    /// bound invocation, refused every time, and the lease could never
    /// expire under this authority.
    #[test]
    fn a_renewal_between_candidate_and_execution_gets_its_own_invocation() {
        let mut expiry = established();
        let wait = expiry.scheduler.next_deadline().expect("armed");
        let before = expiry.decide(wait, None);
        assert_eq!(before.len(), 1);

        // The renewal is ordered first; the next scan rearms from it.
        let renewed_at = wait + 300;
        assert!(
            expiry
                .decide(renewed_at, Some(&committed(1, Some(1))))
                .is_empty()
        );
        let next = expiry.scheduler.next_deadline().expect("rearmed");
        assert!(next > renewed_at);
        let after = expiry.decide(next, None);
        assert_eq!(after.len(), 1, "the renewed lease produced no candidate");

        let (before, after) = (request(&before[0]), request(&after[0]));
        assert_ne!(
            before.retry_key, after.retry_key,
            "two different expirations share one invocation"
        );
    }

    /// The driver registers the deadline the domain loop has to wake for:
    /// the armed lease's, then, once its candidate is in flight, the retry.
    #[test]
    fn the_driver_registers_its_next_deadline() {
        let mut expiry = established();
        let started = expiry.started;
        let at = |tick: u64| started + std::time::Duration::from_millis(tick);
        let deadline = expiry.scheduler.next_deadline().expect("armed");
        assert_eq!(expiry.next_deadline(), Some(at(deadline)));
        assert_eq!(expiry.decide(deadline, None).len(), 1);
        assert_eq!(
            expiry.next_deadline(),
            Some(at(deadline + RETRY_TICKS)),
            "an in-flight candidate registers no retry"
        );
    }
}
