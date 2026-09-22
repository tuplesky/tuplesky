//! Repairing delivery of evidence a replica already produced (task-c02).
//!
//! A replica acknowledges a command once and publishes that
//! acknowledgement to its peers and to the frontend. The peers count it
//! and are done with it. The frontend may not be: it routes evidence to
//! the collector that submitted the command, and a command a replica
//! learned from a peer -- a proposal that outran the submission, a
//! transferred payload -- is acknowledged before any submission has told
//! the frontend which collector that is. The frontend holds such evidence
//! for a while and then lets it go, and a submission arriving afterwards
//! used to be refused as a duplicate with no effects: the collector was
//! one voter short for ever, on a command the domain had long since
//! executed.
//!
//! So a replica keeps what it published, and an *exact* duplicate
//! submission -- same identity, same admission facts, same acknowledged
//! floor -- publishes it again, to the frontend only, through the same
//! outbox and under the same gates as the first time. Nothing is
//! recomputed, nothing is voted again, nothing is executed again. The
//! peers are not sent anything: they were never subject to the hold, and
//! each of them counted the acknowledgement once already.
//!
//! What is kept is bounded by what the command table remembers -- its
//! live records and the tombstones of the records it retired -- and not
//! by any count of unrelated commands. Past that the replica has
//! forgotten the command entirely, and the durable record of its
//! execution is the answer, exactly as it is for any caller that retries
//! after a restart. Nothing here survives a boot: the outbox the evidence
//! went through is this boot's, and so are the barriers it rests on.

use alloc::collections::BTreeMap;
use alloc::vec::Vec;

use coord_core::effect::{BarrierId, EffectContext, PeerId};
use coord_core::outbox::{Outbox, PendingSend};
use coord_types::CommandId;
use coord_types::ids::Ballot;

use crate::commands::CommandTable;

/// Why an exact duplicate submission did not repair delivery of the
/// evidence this replica already produced for the command.
///
/// None of these is a fault of the command: it stays wherever it was,
/// ordered, executed or waiting. They say only that the shortcut --
/// handing the submitter the same acknowledgement again -- is not
/// available here, and the command's outcome is reached the way it
/// always was: through consensus, and through the durable record its
/// execution leaves behind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ReplayRefusal {
    /// This replica may not act in the configured ballot now: a higher
    /// promise is durable or in flight, the role is not one that
    /// produces evidence, or the replica is not booted.
    Fenced,
    /// The retained evidence was produced under another ballot. Old
    /// evidence is never relabelled; the new ballot collects its own.
    BallotMoved,
    /// The batch behind the evidence is not durable yet, so the original
    /// publication is still queued and will go by itself when it is.
    NotYetDurable,
    /// The batch behind the evidence failed. That evidence was never a
    /// fact, and the original publication was dropped for the same
    /// reason; a second copy would be the one thing that must not exist.
    BatchFailed,
    /// This boot has published nothing for the command. Either the
    /// replica has not acknowledged it yet -- it is not in the fast set
    /// and has not adopted an order, and the acknowledgement goes out
    /// when it exists -- or it acknowledged it in an earlier boot, whose
    /// sends are not this boot's to repeat. In the second case the
    /// durable record of the execution is the way back.
    NothingToReplay,
    /// This replica no longer remembers the command at all: it executed
    /// and was retired long enough ago that its tombstone is gone too.
    /// What this replica produced for it is in the durable record of the
    /// execution, which is where a caller's retry is answered from.
    Forgotten,
    /// This command's evidence has been repaired as often as one boot
    /// will repair it. A submitter that keeps presenting the same
    /// command is not being told anything new.
    TooMany,
}

/// How many times one boot repairs one command's evidence.
///
/// A bound on rate, not on obligation: the collector re-offers on a
/// schedule with a floor and a ceiling, and this is more repeats than
/// that schedule produces before the command is either settled or
/// under recovery. Past it the command is still whatever it was.
pub const MAX_EVIDENCE_REPAIRS: u32 = 8;

/// One publication to the frontend, kept exactly as it was made.
///
/// Everything the outbox judged the first time is here so it judges the
/// same thing again: the context (boot and ballot), the barriers the
/// publication required, and the bytes. A repeat is not a new message
/// that happens to say the same thing; it is the same message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RetainedEvidence {
    /// The batch the evidence rests on.
    pub barrier: BarrierId,
    /// The ballot it was produced under.
    pub ballot: Ballot,
    /// The context it was published under.
    pub context: EffectContext,
    /// The barriers the publication required.
    pub requires: Vec<BarrierId>,
    /// The frame, exactly as published.
    pub frame: Vec<u8>,
}

/// What one replica has published to the frontend for the commands it
/// still remembers, and how often each has been published again.
#[derive(Debug)]
pub(crate) struct EvidenceStore {
    own: BTreeMap<CommandId, Vec<RetainedEvidence>>,
    repairs: BTreeMap<CommandId, u32>,
    /// The command table's capacity, which bounds this twice over.
    capacity: usize,
}

impl EvidenceStore {
    /// An empty store for a table of `capacity` records.
    pub fn new(capacity: usize) -> Self {
        EvidenceStore {
            own: BTreeMap::new(),
            repairs: BTreeMap::new(),
            capacity,
        }
    }

    /// Keep `evidence` for `command`.
    ///
    /// A command has at most one fast and one adoption acknowledgement
    /// from one replica, so at most two are kept; the same bytes twice
    /// are one publication. Bounded by what the table remembers, which
    /// is bounded by its capacity twice over -- the live records and the
    /// tombstones of retired ones -- and swept when it has outgrown that
    /// rather than on every insert, so the sweep is amortized over the
    /// growth that made it necessary.
    pub fn retain(&mut self, command: CommandId, evidence: RetainedEvidence, table: &CommandTable) {
        let kept = self.own.entry(command).or_default();
        if !kept.iter().any(|e| e.frame == evidence.frame) {
            kept.push(evidence);
            if kept.len() > 2 {
                kept.remove(0);
            }
        }
        if self.own.len() > self.capacity.saturating_mul(2) {
            self.sweep(table);
        }
    }

    /// Forget every command the table no longer remembers either.
    fn sweep(&mut self, table: &CommandTable) {
        self.own
            .retain(|c, _| table.record(c).is_some() || table.tombstones().contains(c));
        let own = &self.own;
        self.repairs.retain(|c, _| own.contains_key(c));
    }

    /// The sends that publish `command`'s evidence again, or why there
    /// are none.
    ///
    /// Each retained publication is judged on its own: one whose batch
    /// is not durable yet still has its original send queued, one whose
    /// batch failed had that send dropped, and one from another ballot
    /// is not this ballot's evidence. What passes is offered to
    /// `frontend` under the context and barriers it was first published
    /// with, so the outbox applies the boot fence, the durable
    /// prerequisites and the promise at release exactly as it did the
    /// first time. If nothing passes, the first refusal says why.
    ///
    /// The caller has already decided this replica may act at all --
    /// that is the machine's rule (voting eligibility for a follower,
    /// leadership for a leader) and is not repeated here.
    pub fn plan(
        &mut self,
        command: CommandId,
        outbox: &Outbox,
        ballot: Ballot,
        frontend: PeerId,
        table: &CommandTable,
    ) -> Result<Vec<PendingSend>, ReplayRefusal> {
        let Some(kept) = self.own.get(&command) else {
            return Err(if table.record(&command).is_some() {
                ReplayRefusal::NothingToReplay
            } else {
                ReplayRefusal::Forgotten
            });
        };
        if self.repairs.get(&command).copied().unwrap_or(0) >= MAX_EVIDENCE_REPAIRS {
            return Err(ReplayRefusal::TooMany);
        }
        let mut sends = Vec::new();
        let mut refused = None;
        for e in kept {
            let why = if e.ballot != ballot {
                ReplayRefusal::BallotMoved
            } else if outbox.is_failed(&e.barrier) {
                ReplayRefusal::BatchFailed
            } else if !outbox.is_durable(&e.barrier) {
                ReplayRefusal::NotYetDurable
            } else {
                sends.push(PendingSend {
                    context: e.context,
                    requires: e.requires.clone(),
                    to: frontend,
                    frame: e.frame.clone(),
                });
                continue;
            };
            refused.get_or_insert(why);
        }
        if sends.is_empty() {
            return Err(refused.expect("a kept command has at least one publication"));
        }
        *self.repairs.entry(command).or_insert(0) += 1;
        Ok(sends)
    }

    /// How often `command`'s evidence has been published again this boot.
    pub fn repairs_of(&self, command: &CommandId) -> u32 {
        self.repairs.get(command).copied().unwrap_or(0)
    }
}
