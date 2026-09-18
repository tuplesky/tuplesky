//! Durable promises under a configuration identity (task-20; design
//! Sections 4.1, 4.7-4.8, 5.1, 18.1; prototype `handleNewLeader`,
//! `MNewLeaderAckN.Cballot`).
//!
//! A promise is a vote-producing transition: the replica persists the new
//! promise row first and publishes its reply through the logical outbox
//! requiring that row *and* every batch submitted before the cut, so the
//! reply summarizes complete durable state. While a promise is in flight
//! no lower or equal ballot is admitted, and once durable the promised
//! ballot only grows. After a restart the promise is recovered from the row
//! and an old `NewLeader` cannot lower it.
//!
//! Guards that never vote: an epoch other than the configuration's, a
//! sender that is not a voter of the epoch, a ballot whose leader is not
//! the sender, and a replica whose role is not `Voter`.

use alloc::collections::BTreeSet;
use alloc::vec::Vec;

use coord_core::effect::{BarrierId, BootId, Effect, EffectContext, PeerId, PersistBatch};
use coord_core::event::StorageEvent;
use coord_core::outbox::{BarrierAllocator, PendingSend};
use coord_store_api::engine::EngineError;
use coord_types::ids::{
    Ballot, ClusterId, ConfigurationEpoch, DomainId, LocalJournalSeq, ReplicaId, ReplicaIncarnation,
};
use serde::{Deserialize, Serialize};

use crate::messages::ProtocolMessage;
use crate::rows::{PromiseRecordV1, promise_update};

/// The role a replica holds in a configuration epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ReplicaRole {
    /// Votes and may lead.
    Voter,
    /// Replicates established history; never votes.
    Observer,
    /// Catching up toward a role; never votes.
    Learner,
}

/// What this replica is in this epoch: the identity every vote binds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigurationIdentity {
    /// Cluster.
    pub cluster: ClusterId,
    /// Domain.
    pub domain: DomainId,
    /// Configuration epoch (exact voters).
    pub epoch: ConfigurationEpoch,
    /// Voters of the epoch.
    pub voters: BTreeSet<ReplicaId>,
    /// This replica.
    pub replica: ReplicaId,
    /// This replica's incarnation.
    pub incarnation: ReplicaIncarnation,
    /// This replica's role.
    pub role: ReplicaRole,
}

/// Why a `NewLeader` produced no vote.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PromiseRejection {
    /// The ballot's epoch is not this configuration.
    WrongEpoch {
        /// Configured epoch.
        expected: ConfigurationEpoch,
        /// Epoch received.
        got: ConfigurationEpoch,
    },
    /// The sender is not a voter of the epoch.
    NotAVoter {
        /// Sender.
        from: ReplicaId,
    },
    /// The ballot names a leader other than the sender.
    LeaderMismatch {
        /// Ballot leader.
        leader: ReplicaId,
        /// Sender.
        from: ReplicaId,
    },
    /// This replica does not vote in this epoch.
    NotVoting {
        /// Role.
        role: ReplicaRole,
    },
    /// The ballot is not higher than the promised (or in-flight) ballot.
    NotHigher {
        /// The ballot that bounds it.
        promised: Ballot,
    },
}

/// A promise persisted but not yet durable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromiseInFlight {
    /// Ballot being promised.
    pub ballot: Ballot,
    /// Barrier of the promise row.
    pub barrier: BarrierId,
    /// Candidate the reply goes to.
    pub to: ReplicaId,
}

/// The effects of accepting a `NewLeader`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromiseEffects {
    /// Persist the promise row.
    pub persist: Effect,
    /// The reply, to publish on the logical outbox; it requires the promise
    /// row and every batch submitted before the cut.
    pub reply: PendingSend,
}

/// What a storage event did to the promise state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PromiseOutcome {
    /// The promise is durable: the promised ballot advanced.
    Promised(Ballot),
    /// The promise row failed; the earlier promise stands.
    Failed(Ballot),
}

/// The promise state of one replica in one epoch.
#[derive(Clone, Debug)]
pub struct BallotState {
    identity: ConfigurationIdentity,
    promised: Ballot,
    synced: Ballot,
    in_flight: Option<PromiseInFlight>,
    elections: u64,
}

impl BallotState {
    /// Recover from the durable promise row (`None` before any promise:
    /// the genesis ballot of the configuration is promised implicitly).
    pub fn recover(
        identity: ConfigurationIdentity,
        genesis: Ballot,
        durable: Option<PromiseRecordV1>,
    ) -> Self {
        let (promised, synced) = match durable {
            Some(r) => (r.promised, r.synced),
            None => (genesis, genesis),
        };
        BallotState {
            identity,
            promised,
            synced,
            in_flight: None,
            elections: 0,
        }
    }

    /// Configuration identity.
    pub const fn identity(&self) -> &ConfigurationIdentity {
        &self.identity
    }

    /// Highest durably promised ballot: the ballot voting transitions are
    /// admitted under, and the bound the outbox fences against.
    pub const fn promised(&self) -> Ballot {
        self.promised
    }

    /// Highest synchronized ballot.
    pub const fn synced(&self) -> Ballot {
        self.synced
    }

    /// The promise in flight, if any.
    pub const fn in_flight(&self) -> Option<&PromiseInFlight> {
        self.in_flight.as_ref()
    }

    /// Number of durable promise advances since recovery (same-boot
    /// elections).
    pub const fn elections(&self) -> u64 {
        self.elections
    }

    /// The context every effect of this replica binds: domain, incarnation,
    /// boot, epoch and the ballot it was produced under.
    pub fn context(
        &self,
        boot: BootId,
        ballot: Ballot,
        required_journal_seq: LocalJournalSeq,
    ) -> EffectContext {
        EffectContext {
            domain: self.identity.domain,
            replica_incarnation: self.identity.incarnation,
            boot_id: boot,
            configuration: self.identity.epoch,
            ballot,
            required_journal_seq,
        }
    }

    fn bound(&self) -> Ballot {
        match &self.in_flight {
            Some(p) => p.ballot,
            None => self.promised,
        }
    }

    /// Handle `NewLeader { ballot }` from `from`. On acceptance the promise
    /// row is persisted and the reply is published requiring it and
    /// `outstanding` (every barrier submitted before this cut and not yet
    /// complete).
    pub fn on_new_leader(
        &mut self,
        from: ReplicaId,
        ballot: Ballot,
        boot: BootId,
        alloc: &mut BarrierAllocator,
        outstanding: &[BarrierId],
    ) -> Result<PromiseEffects, PromiseRejection> {
        if ballot.epoch != self.identity.epoch {
            return Err(PromiseRejection::WrongEpoch {
                expected: self.identity.epoch,
                got: ballot.epoch,
            });
        }
        if !self.identity.voters.contains(&from) {
            return Err(PromiseRejection::NotAVoter { from });
        }
        if ballot.leader != from {
            return Err(PromiseRejection::LeaderMismatch {
                leader: ballot.leader,
                from,
            });
        }
        if self.identity.role != ReplicaRole::Voter {
            return Err(PromiseRejection::NotVoting {
                role: self.identity.role,
            });
        }
        let bound = self.bound();
        if ballot.compare_same_epoch(&bound) != Some(core::cmp::Ordering::Greater) {
            return Err(PromiseRejection::NotHigher { promised: bound });
        }
        let barrier = alloc.allocate();
        let record = PromiseRecordV1 {
            promised: ballot,
            synced: self.synced,
        };
        let update = promise_update(self.identity.epoch, &record)
            .map_err(|_: EngineError| PromiseRejection::NotHigher { promised: bound })?;
        let persist = Effect::Persist(PersistBatch {
            barrier,
            base: None,
            updates: alloc::vec![update],
        });
        let mut requires: Vec<BarrierId> = outstanding.to_vec();
        requires.push(barrier);
        let reply = PendingSend {
            context: self.context(boot, ballot, LocalJournalSeq::ZERO),
            requires,
            to: PeerId {
                replica: from,
                incarnation: ReplicaIncarnation::ZERO,
            },
            frame: ProtocolMessage::Promise {
                ballot,
                synced: self.synced,
                replica: self.identity.replica,
            }
            .encode(),
        };
        self.in_flight = Some(PromiseInFlight {
            ballot,
            barrier,
            to: from,
        });
        Ok(PromiseEffects { persist, reply })
    }

    /// Observe a storage fact; a durable promise row advances the promised
    /// ballot, a failed one leaves the earlier promise in force.
    pub fn on_storage(&mut self, event: &StorageEvent) -> Option<PromiseOutcome> {
        let pending = self.in_flight.as_ref()?;
        if event.barrier() != Some(pending.barrier) {
            return None;
        }
        match event {
            StorageEvent::JournalDurable { .. } => {
                let ballot = pending.ballot;
                self.promised = ballot;
                self.in_flight = None;
                self.elections += 1;
                Some(PromiseOutcome::Promised(ballot))
            }
            StorageEvent::Failed { .. } => {
                let ballot = pending.ballot;
                self.in_flight = None;
                Some(PromiseOutcome::Failed(ballot))
            }
            StorageEvent::Materialized { .. } | StorageEvent::LocalCheckpointPublished { .. } => {
                None
            }
        }
    }

    /// Record that this replica synchronized to `ballot` (adopted a Sync);
    /// the caller persists the row.
    pub fn mark_synced(&mut self, ballot: Ballot) -> PromiseRecordV1 {
        self.synced = ballot;
        PromiseRecordV1 {
            promised: self.promised,
            synced: self.synced,
        }
    }
}
