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
    Ballot, ClusterId, ConfigurationEpoch, DomainId, ExecutionPosition, LocalJournalSeq, ReplicaId,
    ReplicaIncarnation,
};
use serde::{Deserialize, Serialize};

use crate::handoff::Transition;
use crate::messages::ProtocolMessage;
use crate::rows::{PromiseRecordV1, SealRecordV1, promise_update, seal_update};

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
    /// This configuration is sealed. Ordinary voting is over for every
    /// ballot of it, including ones nobody has proposed yet, and the
    /// recorded transition is what has to be finished. A higher ballot
    /// is not an exception to a seal -- it is exactly what a seal is
    /// for (task-55).
    Sealed {
        /// The transition this replica sealed for.
        transition: Transition,
    },
    /// The candidate is more than a table behind what this replica
    /// executed (task-d10). A leader that far behind wins and then cannot
    /// serve: what it lacks was executed and retired by the voters that
    /// would have to send it, and a leader asks nobody for payloads. The
    /// refusal is sent back naming `own`, and the candidate does not
    /// campaign again until it has executed past it.
    CandidateBehind {
        /// What the candidate said it executed.
        candidate: ExecutionPosition,
        /// What this replica executed.
        own: ExecutionPosition,
    },
}

/// Why a seal was not written.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SealRejection {
    /// The transition leaves another configuration than this one.
    WrongEpoch {
        /// Configured epoch.
        expected: ConfigurationEpoch,
        /// The transition's.
        got: ConfigurationEpoch,
    },
    /// This replica does not vote in this epoch, so it fences nothing.
    NotVoting {
        /// Role.
        role: ReplicaRole,
    },
    /// This replica is already sealed for another transition. A seal is
    /// irreversible and the recorded transition is the one that must be
    /// finished; a competing initiator does not get a second one.
    AlreadySealed {
        /// The transition it sealed for.
        held: Transition,
    },
}

/// A promise persisted but not yet durable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromiseInFlight {
    /// Ballot being promised.
    pub ballot: Ballot,
    /// Barrier of the promise row.
    pub barrier: BarrierId,
    /// Candidate (at its authenticated incarnation) the reply goes to.
    pub to: PeerId,
}

/// Why a synchronization was not recorded.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncRejection {
    /// The ballot's epoch is not this configuration.
    WrongEpoch {
        /// Configured epoch.
        expected: ConfigurationEpoch,
        /// Epoch received.
        got: ConfigurationEpoch,
    },
    /// The ballot precedes the highest synchronized ballot (a delayed
    /// Sync of an older ballot): the record never regresses.
    Regression {
        /// Highest synchronized ballot.
        synced: Ballot,
        /// Ballot received.
        got: Ballot,
    },
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
    /// The seal row is durable: ordinary voting is over here.
    Sealed(Transition),
    /// The seal row failed. This replica is *not* sealed and says so;
    /// the initiator may ask again. It is not evidence that the
    /// transition was cancelled, and nothing here turns it into any
    /// (task-55).
    SealFailed(Transition),
}

/// The effects of sealing.
#[derive(Clone, Debug)]
pub struct SealEffects {
    /// Persist the seal row.
    pub persist: Effect,
    /// The seal report, to publish on the logical outbox. Like a
    /// promise reply it requires the row *and* every batch submitted
    /// before the cut, so what it reports is complete durable state:
    /// work this replica learned immediately before sealing is inside
    /// it even if the response is delayed.
    pub report: PendingSend,
}

/// The promise state of one replica in one epoch.
#[derive(Clone, Debug)]
pub struct BallotState {
    identity: ConfigurationIdentity,
    promised: Ballot,
    synced: Ballot,
    /// Every promise persisted and not yet durable, in ascending ballot
    /// order. Each completes independently: a durable row advances the
    /// promise to its ballot (if higher), a failed row is forgotten while
    /// the others stay tracked, so no accepted promise is ever lost.
    in_flight: Vec<PromiseInFlight>,
    elections: u64,
    /// The durable seal, once it exists. Nothing in this type clears it.
    sealed: Option<SealRecordV1>,
    /// Every seal row persisted and not yet durable, with its barrier.
    /// Retries of one transition are kept side by side like promises in
    /// flight rather than replacing each other: each row is the same
    /// record, whichever lands first seals the replica, and a later
    /// failure of another copy can neither hide that nor undo it.
    sealing: Vec<(BarrierId, SealRecordV1)>,
    /// The highest ballot this replica refused as behind (task-d10).
    /// Its candidate promised itself that ballot and hears nothing below
    /// it, so the next ballot this replica campaigns for has to be above
    /// it, or the refused candidate could never follow the domain it has
    /// to catch up from. Not durable: after a restart only the ballots
    /// seen since count.
    outranked: Option<Ballot>,
}

impl BallotState {
    /// Recover from the durable promise row (`None` before any promise:
    /// the genesis ballot of the configuration is promised implicitly).
    pub fn recover(
        identity: ConfigurationIdentity,
        genesis: Ballot,
        durable: Option<PromiseRecordV1>,
    ) -> Self {
        Self::recover_sealed(identity, genesis, durable, None)
    }

    /// Recover from the durable promise row and the durable seal row.
    ///
    /// A replica whose seal row is there comes back sealed, and that is
    /// the whole of "restart cannot resume old service": it is not a
    /// decision this type makes on recovery, it is a row it reads. The
    /// absence of the row means this replica has no seal -- never that
    /// the transition was cancelled, which is a quorum's fact and not
    /// one replica's (task-55).
    pub fn recover_sealed(
        identity: ConfigurationIdentity,
        genesis: Ballot,
        durable: Option<PromiseRecordV1>,
        seal: Option<SealRecordV1>,
    ) -> Self {
        let (promised, synced) = match durable {
            Some(r) => (r.promised, r.synced),
            None => (genesis, genesis),
        };
        BallotState {
            identity,
            promised,
            synced,
            in_flight: Vec::new(),
            elections: 0,
            sealed: seal,
            sealing: Vec::new(),
            outranked: None,
        }
    }

    /// The durable seal, if this replica has one.
    pub const fn sealed(&self) -> Option<&SealRecordV1> {
        self.sealed.as_ref()
    }

    /// Whether the seal row is durable here.
    pub const fn is_sealed(&self) -> bool {
        self.sealed.is_some()
    }

    /// The transition this replica is sealed for or sealing for: the
    /// durable seal, or else a seal row still in flight.
    pub fn seal_held(&self) -> Option<Transition> {
        self.sealed
            .iter()
            .chain(self.sealing.iter().map(|(_, r)| r))
            .map(|r| r.transition)
            .next()
    }

    /// Whether ordinary voting is over here: the seal is durable, or a
    /// seal row is in flight.
    ///
    /// The fence starts at the cut, not at durability. The seal report
    /// was built over the batches outstanding when the seal was asked
    /// for, so a vote cast after that and before the row lands is one
    /// the report does not cover, and an initiator counting the report
    /// would count a fence this replica had already voted past. If the
    /// row fails nothing was fenced, and voting resumes with it.
    pub fn is_fenced(&self) -> bool {
        self.sealed.is_some() || !self.sealing.is_empty()
    }

    /// Seal this configuration for `transition`.
    ///
    /// The row is persisted and the report published through the
    /// logical outbox requiring it and `outstanding`, exactly as a
    /// promise reply is: a seal that was reported before its own row
    /// was durable would let an initiator count a fence that a crash
    /// then removed, and a report built before the outstanding batches
    /// resolved would omit work this replica had already learned.
    ///
    /// Sealing again for the same transition is the same seal, which is
    /// what makes a retry after a lost report safe. Sealing for another
    /// one is refused: the recorded transition is the one that must be
    /// finished.
    ///
    /// `to` is the coordinator that asked, at its authenticated
    /// incarnation, so a stale incarnation of it never receives the
    /// report -- the same rule a promise reply follows.
    pub fn seal(
        &mut self,
        transition: Transition,
        to: PeerId,
        boot: BootId,
        alloc: &mut BarrierAllocator,
        outstanding: &[BarrierId],
    ) -> Result<SealEffects, SealRejection> {
        if transition.from != self.identity.epoch {
            return Err(SealRejection::WrongEpoch {
                expected: self.identity.epoch,
                got: transition.from,
            });
        }
        if self.identity.role != ReplicaRole::Voter {
            return Err(SealRejection::NotVoting {
                role: self.identity.role,
            });
        }
        for held in self
            .sealed
            .iter()
            .chain(self.sealing.iter().map(|(_, r)| r))
        {
            if held.transition != transition {
                return Err(SealRejection::AlreadySealed {
                    held: held.transition,
                });
            }
        }
        // A retry of the recorded transition writes the recorded row
        // again, not a new one: a promise that advanced in between must
        // not give the same seal two different cuts on disk.
        let record = self
            .sealed
            .iter()
            .chain(self.sealing.iter().map(|(_, r)| r))
            .next()
            .copied()
            .unwrap_or(SealRecordV1 {
                transition,
                at: self.promised,
            });
        let barrier = alloc.allocate();
        let update = seal_update(self.identity.epoch, &record).map_err(|_: EngineError| {
            SealRejection::WrongEpoch {
                expected: self.identity.epoch,
                got: transition.from,
            }
        })?;
        let persist = Effect::Persist(PersistBatch {
            barrier,
            base: None,
            updates: alloc::vec![update],
        });
        let mut requires: Vec<BarrierId> = outstanding.to_vec();
        requires.push(barrier);
        let report = PendingSend {
            context: self.context(boot, self.promised, LocalJournalSeq::ZERO),
            requires,
            to,
            frame: ProtocolMessage::Sealed {
                transition,
                at: record.at,
                replica: self.identity.replica,
            }
            .encode(),
        };
        self.sealing.push((barrier, record));
        Ok(SealEffects { persist, report })
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

    /// The highest promise in flight, if any (the bound new ballots must
    /// exceed).
    pub fn in_flight(&self) -> Option<&PromiseInFlight> {
        self.in_flight.last()
    }

    /// Every promise in flight, in ascending ballot order.
    pub fn promises_in_flight(&self) -> &[PromiseInFlight] {
        &self.in_flight
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
        match self.in_flight.last() {
            Some(p) => p.ballot,
            None => self.promised,
        }
    }

    /// The checks every `NewLeader` must pass before this replica
    /// promises or refuses anything: no seal, this epoch, a voter naming
    /// itself as the ballot's leader, this replica a voter, and a ballot
    /// above everything promised or in flight and above the highest ballot
    /// refused as behind.
    ///
    /// The last is what makes a refusal stick (task-d10). The refused
    /// candidate promised itself that ballot and hears nothing below it;
    /// a lower ballot promised here could win without it, and then it
    /// could never follow the leader it has to catch up from. Refusing
    /// those as `NotHigher` makes the next winning ballot go above it.
    fn check_candidate(&self, from: ReplicaId, ballot: Ballot) -> Result<(), PromiseRejection> {
        // The seal comes first. A higher ballot is not an exception to
        // a fence; it is the thing a fence exists to stop. A seal row in
        // flight already binds: the report's cut is taken, and a promise
        // made after it is one the report does not show.
        if let Some(transition) = self.seal_held() {
            return Err(PromiseRejection::Sealed { transition });
        }
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
        let mut bound = self.bound();
        if let Some(refused) = self.outranked
            && refused.compare_same_epoch(&bound) == Some(core::cmp::Ordering::Greater)
        {
            bound = refused;
        }
        if ballot.compare_same_epoch(&bound) != Some(core::cmp::Ordering::Greater) {
            return Err(PromiseRejection::NotHigher { promised: bound });
        }
        Ok(())
    }

    /// Whether to refuse `NewLeader { ballot }` from `from` because the
    /// candidate, which executed through `candidate`, is more than
    /// `window` commands behind this replica's `own` (task-d10).
    ///
    /// Only a `NewLeader` that would otherwise be promised is refused
    /// this way; any other rejection is [`BallotState::on_new_leader`]'s
    /// to give. A refused ballot is remembered as outranked: the next
    /// campaign here goes above it.
    pub fn refuses_behind(
        &mut self,
        from: ReplicaId,
        ballot: Ballot,
        candidate: ExecutionPosition,
        own: ExecutionPosition,
        window: u64,
    ) -> Option<PromiseRejection> {
        self.check_candidate(from, ballot).ok()?;
        if own.get() <= candidate.get().saturating_add(window) {
            return None;
        }
        let higher = self
            .outranked
            .is_none_or(|o| ballot.compare_same_epoch(&o) == Some(core::cmp::Ordering::Greater));
        if higher {
            self.outranked = Some(ballot);
        }
        Some(PromiseRejection::CandidateBehind { candidate, own })
    }

    /// The reply refusing `ballot` to the candidate `to` as behind,
    /// naming what this replica executed through (task-d10). It needs no
    /// barrier: nothing was promised or written.
    pub fn refusal(
        &self,
        to: PeerId,
        ballot: Ballot,
        boot: BootId,
        executed: ExecutionPosition,
    ) -> PendingSend {
        PendingSend {
            context: self.context(boot, ballot, LocalJournalSeq::ZERO),
            requires: Vec::new(),
            to,
            frame: ProtocolMessage::PromiseRefused {
                ballot,
                replica: self.identity.replica,
                executed,
            }
            .encode(),
        }
    }

    /// The promise of `ballot` again, for the candidate `to` that asks for
    /// it once more (task-d33): this replica promised it and has not
    /// synchronized to it, so the Sync that answered the promise may have
    /// been lost, and the leader asks until this replica follows. The row
    /// is durable already, so the reply requires nothing. Not while a
    /// higher promise is in flight: that promise voids this one.
    pub fn promise_again(&self, to: PeerId, ballot: Ballot, boot: BootId) -> Option<PendingSend> {
        (self.bound() == ballot
            && self.promised() == ballot
            && self.synced != ballot
            && ballot.leader == to.replica)
            .then(|| PendingSend {
                context: self.context(boot, ballot, LocalJournalSeq::ZERO),
                requires: Vec::new(),
                to,
                frame: ProtocolMessage::Promise {
                    ballot,
                    synced: self.synced,
                    replica: self.identity.replica,
                }
                .encode(),
            })
    }

    /// The highest ballot refused as behind, which this replica's next
    /// campaign has to go above.
    pub const fn outranked(&self) -> Option<Ballot> {
        self.outranked
    }

    /// Handle `NewLeader { ballot }` from the candidate `from` (its
    /// authenticated replica and incarnation, which the reply is addressed
    /// to so a stale incarnation of the candidate never receives it). On
    /// acceptance the promise row is persisted and the reply is published
    /// requiring it and `outstanding` (every barrier submitted before this
    /// cut and not yet complete).
    pub fn on_new_leader(
        &mut self,
        from: PeerId,
        ballot: Ballot,
        boot: BootId,
        alloc: &mut BarrierAllocator,
        outstanding: &[BarrierId],
    ) -> Result<PromiseEffects, PromiseRejection> {
        self.check_candidate(from.replica, ballot)?;
        let candidate = from;
        let bound = self.bound();
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
            to: candidate,
            frame: ProtocolMessage::Promise {
                ballot,
                synced: self.synced,
                replica: self.identity.replica,
            }
            .encode(),
        };
        self.in_flight.push(PromiseInFlight {
            ballot,
            barrier,
            to: candidate,
        });
        Ok(PromiseEffects { persist, reply })
    }

    /// Observe a storage fact about one in-flight promise: a durable row
    /// advances the promised ballot to its ballot (rows of several promises
    /// may complete in any order; the promise never moves backward), a
    /// failed one is dropped while the earlier promise, and every other
    /// promise in flight, stay in force.
    pub fn on_storage(&mut self, event: &StorageEvent) -> Option<PromiseOutcome> {
        if let Some(index) = self
            .sealing
            .iter()
            .position(|(b, _)| Some(*b) == event.barrier())
        {
            return match event {
                StorageEvent::JournalDurable { .. } => {
                    let (_, record) = self.sealing.remove(index);
                    // The first durable copy seals; a later one is the
                    // same record and changes nothing.
                    let sealed = *self.sealed.get_or_insert(record);
                    Some(PromiseOutcome::Sealed(sealed.transition))
                }
                StorageEvent::Failed { .. } => {
                    let (_, record) = self.sealing.remove(index);
                    // One failed copy is a failed seal only when no other
                    // copy is durable or still in flight: a retry's row
                    // failing says nothing about a row that already
                    // landed, and must not report this replica unfenced.
                    (self.sealed.is_none() && self.sealing.is_empty())
                        .then_some(PromiseOutcome::SealFailed(record.transition))
                }
                StorageEvent::Materialized { .. }
                | StorageEvent::LocalCheckpointPublished { .. } => None,
            };
        }
        let index = self
            .in_flight
            .iter()
            .position(|p| Some(p.barrier) == event.barrier())?;
        match event {
            StorageEvent::JournalDurable { .. } => {
                let ballot = self.in_flight.remove(index).ballot;
                if ballot.compare_same_epoch(&self.promised) == Some(core::cmp::Ordering::Greater) {
                    self.promised = ballot;
                }
                self.elections += 1;
                Some(PromiseOutcome::Promised(ballot))
            }
            StorageEvent::Failed { .. } => {
                let ballot = self.in_flight.remove(index).ballot;
                Some(PromiseOutcome::Failed(ballot))
            }
            StorageEvent::Materialized { .. } | StorageEvent::LocalCheckpointPublished { .. } => {
                None
            }
        }
    }

    /// Record that this replica synchronized to `ballot` (adopted a Sync);
    /// the caller persists the row. The ballot must belong to this
    /// configuration and not precede the highest synchronized ballot: a
    /// delayed Sync of an older ballot changes nothing.
    pub fn mark_synced(&mut self, ballot: Ballot) -> Result<PromiseRecordV1, SyncRejection> {
        if ballot.epoch != self.identity.epoch {
            return Err(SyncRejection::WrongEpoch {
                expected: self.identity.epoch,
                got: ballot.epoch,
            });
        }
        if ballot.compare_same_epoch(&self.synced) == Some(core::cmp::Ordering::Less) {
            return Err(SyncRejection::Regression {
                synced: self.synced,
                got: ballot,
            });
        }
        self.synced = ballot;
        Ok(PromiseRecordV1 {
            promised: self.highest_queued(),
            synced: self.synced,
        })
    }

    /// The highest promise already queued for disk: the durable promise or
    /// the highest promise in flight, whichever is higher.
    ///
    /// Every write of the promise row carries it (task-d18). The journal
    /// applies rows in the order they were queued, so a row carrying less
    /// than a row queued before it would lower the durable promise below
    /// one this replica may already have published. A promise row from
    /// [`BallotState::on_new_leader`] carries a ballot above this bound by
    /// construction; a Sync's row carries this bound, so it can never fall
    /// below a promise in flight. (The seal writes its own row, not this
    /// one.)
    pub fn highest_queued(&self) -> Ballot {
        let bound = self.bound();
        if bound.compare_same_epoch(&self.promised) == Some(core::cmp::Ordering::Greater) {
            bound
        } else {
            self.promised
        }
    }
}
