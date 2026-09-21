//! Lease expiry scheduling under a replicated authority epoch (design
//! Sections 7.2-7.3).
//!
//! The scheduler is leader-local and deterministic: it consumes observed
//! committed lease state stamped with the local monotonic tick of the
//! observation, arms deadlines, and emits conditional
//! [`InternalCommand::ExpireLease`] candidates. It never mutates replicated
//! state itself: a candidate applies only if the state machine finds every
//! field unchanged, so a stale timer, a duplicate observation or a former
//! leader can never delete renewed or rebound keys.
//!
//! Conservatism rules:
//! * Anchors are observation ticks of committed grants/renewals, never
//!   earlier, and never a reply's arrival; a duplicate or delayed
//!   observation of an already armed renewal creates no new anchor.
//! * A deadline waits `(1 + rho) * TTL` local units for a documented
//!   maximum fast clock-rate error `rho`, so at least `TTL` real seconds
//!   pass after the anchor.
//! * Establishing an authority epoch (recovery, restart, failover) rearms
//!   every surviving lease for its full TTL from that observation; a
//!   suspended scheduler (detected clock anomaly) emits nothing until
//!   resumed, which rearms conservatively again.
//! * Timer generations make an obsolete fire a no-op.

use alloc::collections::BTreeMap;
use alloc::vec::Vec;

use coord_types::ids::{LeaseAuthorityEpoch, LeaseGeneration, LeaseId, NamespaceId};
use serde::{Deserialize, Serialize};

use crate::internal::InternalCommand;

/// Documented clock assumptions the scheduler waits under.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClockAssumptions {
    /// Local monotonic ticks per real second (nominal).
    pub ticks_per_second: u64,
    /// Maximum fast rate error of the local clock in parts per million.
    pub max_fast_rate_ppm: u32,
}

impl ClockAssumptions {
    /// Local ticks to wait so that at least `ttl_seconds` real seconds pass:
    /// `ceil((1 + rho) * ttl * ticks_per_second)`.
    pub fn wait_ticks(&self, ttl_seconds: u32) -> u64 {
        let nominal = u128::from(ttl_seconds) * u128::from(self.ticks_per_second);
        let scaled = nominal * (1_000_000 + u128::from(self.max_fast_rate_ppm));
        let ticks = scaled.div_ceil(1_000_000);
        u64::try_from(ticks).unwrap_or(u64::MAX)
    }
}

/// What the scheduler knows about one committed, active lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseSnapshot {
    /// Namespace.
    pub namespace: NamespaceId,
    /// Lease.
    pub lease_id: LeaseId,
    /// Ownership generation.
    pub generation: LeaseGeneration,
    /// Committed renewal sequence.
    pub renewal_sequence: u64,
    /// Granted TTL in seconds.
    pub ttl_seconds: u32,
}

/// An observation of committed lease state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LeaseObservation {
    /// A grant or renewal committed with this state.
    Alive(LeaseSnapshot),
    /// The lease ended (revoked or expired).
    Ended(LeaseId),
}

/// A request to be woken for a lease at a local tick. An older generation
/// for the same lease is obsolete.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerRequest {
    /// Lease.
    pub lease_id: LeaseId,
    /// Timer generation.
    pub generation: u64,
    /// Local tick to fire at.
    pub at_tick: u64,
}

/// How a submitted expiration ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpiryOutcome {
    /// It matched and the lease ended.
    Applied,
    /// It did not match (renewed, revoked, gone or stale epoch).
    Stale,
    /// It could not be ordered (no quorum, lost leadership); retry later.
    Unavailable,
}

/// A labelled remaining-time estimate for `LeaseTimeToLive` responses:
/// scheduler context, never replicated ownership proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemainingEstimate {
    /// Authority epoch the estimate was made under.
    pub authority_epoch: LeaseAuthorityEpoch,
    /// Local tick of the observation the deadline is anchored to.
    pub anchor_tick: u64,
    /// Local tick the expiration becomes eligible.
    pub deadline_tick: u64,
    /// Ticks left at the time of the estimate (zero when eligible).
    pub remaining_ticks: u64,
    /// Timer generation currently armed for the lease.
    pub timer_generation: u64,
}

#[derive(Clone, Debug)]
struct Armed {
    snapshot: LeaseSnapshot,
    anchor: u64,
    deadline: u64,
    timer: u64,
    /// Renewal sequence of an expiration in flight, if any.
    candidate: Option<u64>,
}

/// The expiry scheduler of one domain on one node.
#[derive(Debug)]
pub struct LeaseScheduler {
    assumptions: ClockAssumptions,
    epoch: Option<LeaseAuthorityEpoch>,
    armed: BTreeMap<LeaseId, Armed>,
    timers: BTreeMap<LeaseId, u64>,
    suspended: bool,
}

impl LeaseScheduler {
    /// A scheduler with no authority; it emits nothing until established.
    pub fn new(assumptions: ClockAssumptions) -> Self {
        LeaseScheduler {
            assumptions,
            epoch: None,
            armed: BTreeMap::new(),
            timers: BTreeMap::new(),
            suspended: false,
        }
    }

    /// The epoch this scheduler runs under, once established.
    pub const fn epoch(&self) -> Option<LeaseAuthorityEpoch> {
        self.epoch
    }

    /// Whether expiration is suspended by a detected clock anomaly.
    pub const fn is_suspended(&self) -> bool {
        self.suspended
    }

    /// Number of armed leases.
    pub fn len(&self) -> usize {
        self.armed.len()
    }

    /// The leases currently armed, in lease id order.
    ///
    /// A driver reading committed state needs this to tell a lease that
    /// ended from one it simply did not see: what is armed and absent
    /// from an observation is what has gone.
    pub fn armed_ids(&self) -> Vec<LeaseId> {
        self.armed.keys().copied().collect()
    }

    /// Whether nothing is armed.
    pub fn is_empty(&self) -> bool {
        self.armed.is_empty()
    }

    fn arm(&mut self, now: u64, snapshot: LeaseSnapshot) -> TimerRequest {
        let timer = self.timers.entry(snapshot.lease_id).or_insert(0);
        *timer += 1;
        let deadline = now.saturating_add(self.assumptions.wait_ticks(snapshot.ttl_seconds));
        self.armed.insert(
            snapshot.lease_id,
            Armed {
                snapshot,
                anchor: now,
                deadline,
                timer: *timer,
                candidate: None,
            },
        );
        TimerRequest {
            lease_id: snapshot.lease_id,
            generation: *timer,
            at_tick: deadline,
        }
    }

    /// Take authority under `epoch` (it must have been established in the
    /// replicated history first) at local tick `now`, over the recovered
    /// active leases. Every lease is armed for its full TTL from `now`;
    /// anything armed before is discarded.
    pub fn establish(
        &mut self,
        epoch: LeaseAuthorityEpoch,
        now: u64,
        recovered: impl IntoIterator<Item = LeaseSnapshot>,
    ) -> Vec<TimerRequest> {
        self.epoch = Some(epoch);
        self.armed.clear();
        self.suspended = false;
        recovered.into_iter().map(|s| self.arm(now, s)).collect()
    }

    /// Observe committed lease state at local tick `now`. A grant or a
    /// renewal newer than the armed one rearms from `now`; an observation
    /// no newer than the armed state (a duplicate or a delayed reply)
    /// creates no new anchor. Returns the timer to arm, if any.
    pub fn observe(&mut self, now: u64, observation: LeaseObservation) -> Option<TimerRequest> {
        self.epoch?;
        match observation {
            LeaseObservation::Alive(snapshot) => {
                if let Some(armed) = self.armed.get(&snapshot.lease_id)
                    && armed.snapshot.generation == snapshot.generation
                    && armed.snapshot.renewal_sequence >= snapshot.renewal_sequence
                {
                    return None;
                }
                Some(self.arm(now, snapshot))
            }
            LeaseObservation::Ended(lease_id) => {
                self.armed.remove(&lease_id);
                if let Some(t) = self.timers.get_mut(&lease_id) {
                    *t += 1;
                }
                None
            }
        }
    }

    fn candidate(&mut self, lease_id: LeaseId, now: u64) -> Option<InternalCommand> {
        let epoch = self.epoch?;
        if self.suspended {
            return None;
        }
        let armed = self.armed.get_mut(&lease_id)?;
        if armed.candidate.is_some() || now < armed.deadline {
            return None;
        }
        armed.candidate = Some(armed.snapshot.renewal_sequence);
        Some(InternalCommand::ExpireLease {
            namespace: armed.snapshot.namespace,
            lease_id,
            generation: armed.snapshot.generation,
            expected_renewal_sequence: armed.snapshot.renewal_sequence,
            authority_epoch: epoch,
        })
    }

    /// A timer fired at local tick `now`. An obsolete generation is a
    /// no-op; a current one whose deadline has passed yields the
    /// conditional expiration to submit.
    pub fn fire(&mut self, now: u64, timer: TimerRequest) -> Option<InternalCommand> {
        if self.timers.get(&timer.lease_id) != Some(&timer.generation) {
            return None;
        }
        self.candidate(timer.lease_id, now)
    }

    /// Every eligible expiration at local tick `now` not already in flight.
    pub fn poll(&mut self, now: u64) -> Vec<InternalCommand> {
        let due: Vec<LeaseId> = self
            .armed
            .iter()
            .filter(|(_, a)| a.candidate.is_none() && now >= a.deadline)
            .map(|(id, _)| *id)
            .collect();
        due.into_iter()
            .filter_map(|id| self.candidate(id, now))
            .collect()
    }

    /// Report how a submitted expiration for `lease_id` at
    /// `expected_renewal_sequence` ended.
    pub fn outcome(
        &mut self,
        lease_id: LeaseId,
        expected_renewal_sequence: u64,
        outcome: ExpiryOutcome,
    ) {
        let Some(armed) = self.armed.get_mut(&lease_id) else {
            return;
        };
        if armed.candidate != Some(expected_renewal_sequence) {
            // A newer renewal already rearmed the lease; this outcome is old.
            return;
        }
        match outcome {
            ExpiryOutcome::Applied | ExpiryOutcome::Stale => {
                // Ended, or superseded by state whose observation (re)arms it.
                self.armed.remove(&lease_id);
                if let Some(t) = self.timers.get_mut(&lease_id) {
                    *t += 1;
                }
            }
            ExpiryOutcome::Unavailable => armed.candidate = None,
        }
    }

    /// Suspend expiration after a detected clock anomaly.
    pub fn suspend(&mut self) {
        self.suspended = true;
    }

    /// Resume after an anomaly: every lease is rearmed for its full TTL
    /// from `now`, because elapsed local time is no longer trusted.
    pub fn resume(&mut self, now: u64) -> Vec<TimerRequest> {
        self.suspended = false;
        let snapshots: Vec<LeaseSnapshot> = self.armed.values().map(|a| a.snapshot).collect();
        snapshots.into_iter().map(|s| self.arm(now, s)).collect()
    }

    /// The scheduler's remaining-time estimate for a lease at `now`.
    pub fn remaining(&self, lease_id: &LeaseId, now: u64) -> Option<RemainingEstimate> {
        let epoch = self.epoch?;
        let armed = self.armed.get(lease_id)?;
        Some(RemainingEstimate {
            authority_epoch: epoch,
            anchor_tick: armed.anchor,
            deadline_tick: armed.deadline,
            remaining_ticks: armed.deadline.saturating_sub(now),
            timer_generation: armed.timer,
        })
    }

    /// Earliest deadline among armed leases without an expiration in flight.
    pub fn next_deadline(&self) -> Option<u64> {
        self.armed
            .values()
            .filter(|a| a.candidate.is_none())
            .map(|a| a.deadline)
            .min()
    }
}
