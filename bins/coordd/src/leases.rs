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

use coord_state::expiry::{ClockAssumptions, LeaseObservation, LeaseScheduler, LeaseSnapshot};
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

/// The expiry driver of one domain on this node.
pub struct Expiry {
    scheduler: LeaseScheduler,
    started: std::time::Instant,
    cluster: ClusterId,
    domain: DomainId,
    /// The epoch this node has asked the cluster to install, while it is
    /// still in flight. Cleared once committed state shows it.
    proposing: Option<LeaseAuthorityEpoch>,
    /// Local tick of the last observation scan; `None` before the first.
    scanned: Option<u64>,
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
            scanned: None,
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
    /// come due.
    ///
    /// Returns encoded request frames, because what a voter proposes is
    /// a frame; building them here keeps the serving loop from knowing
    /// how a service command is spelled.
    pub fn due<P: Persistence>(&mut self, store: &P) -> Vec<Vec<u8>> {
        let now = self.now();
        // Reading committed lease state is a scan, so it happens on an
        // interval rather than every turn. Between scans the scheduler
        // works from what it last saw, which is the conservative
        // direction: an anchor is never later than the observation it
        // came from, so a renewal this node has not seen yet can only
        // make an expiry candidate stale -- and a stale candidate is a
        // no-op, not a deletion.
        if self
            .scanned
            .is_none_or(|last| now.saturating_sub(last) >= SCAN_INTERVAL_TICKS)
        {
            // A read that fails changes nothing: the next turn tries
            // again, and a scheduler holding its old anchors expires
            // late rather than early.
            let Some(state) = self.read(store) else {
                return Vec::new();
            };
            self.scanned = Some(now);
            if self.scheduler.epoch() != Some(state.authority) {
                return self.take_authority(now, &state);
            }
            self.observe(now, &state);
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
                } => self.frame(
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
                        *authority_epoch,
                    ),
                ),
                // The scheduler produces nothing else.
                _ => None,
            })
            .collect()
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
        if let Some(mine) = self.proposing {
            if state.authority == mine {
                // Installed. Arm from this observation and start.
                self.scheduler
                    .establish(mine, now, state.snapshots.iter().copied());
                self.proposing = None;
                return Vec::new();
            }
            if state.authority < mine {
                // Still in flight. The command is idempotent under its
                // own invocation identity, so it is presented again on
                // the next scan rather than left to a quorum that may
                // never have heard it.
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
        self.proposing = Some(next);
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
/// ownership generation, under which authority epoch. That is what makes
/// a re-proposal of the same candidate the same command -- resolved from
/// its retry record rather than executed twice -- while a candidate for a
/// later generation or a later epoch is a different one.
fn expiry_key(
    cluster: ClusterId,
    domain: DomainId,
    lease: LeaseId,
    generation: LeaseGeneration,
    epoch: LeaseAuthorityEpoch,
) -> RetryKey {
    let mut instance = [0u8; 16];
    instance[..8].copy_from_slice(&generation.get().to_be_bytes());
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
