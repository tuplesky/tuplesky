//! Sealed membership handoff (task-54; design Sections 4.8, 10.3.2,
//! 23 G5).
//!
//! Membership is a stop-and-transfer extension, deliberately not an
//! ordinary KV write and deliberately not joint consensus. The
//! lifecycle is `Stable -> Preparing -> Sealing -> TerminalRecovery ->
//! Installing -> Activating -> Stable`, with a cancellation back to
//! `Stable` that is only available before sealing -- and this module is
//! the vocabulary, the certificates and, above all, the rule for
//! choosing where a recovering coordinator resumes.
//!
//! That last part is the whole of it. A coordinator dies at some point
//! in the transition and another one takes over; what it must *not* do
//! is believe a label. The lifecycle name the dead coordinator was
//! holding is not evidence, a missing local record is not evidence, and
//! a timeout is not evidence. [`resume`] takes only durable records
//! that some replica made -- stances, certificates, installations --
//! and every stage it returns is justified by one of them.
//!
//! The certificates and what each requires:
//!
//! * [`SealCertificate`]: a majority of the *old* voters have durably
//!   fenced ordinary voting for the whole old configuration, across
//!   ballots. Handoff-only recovery stays possible; nothing else does.
//! * [`CancellationCertificate`]: a majority of the old voters have
//!   durably refused the transition instead, and none of them sealed.
//!   A voter records one stance per transition and never reverses it
//!   ([`StanceLedger`]), so two majorities cannot form and a seal and a
//!   cancellation can never both authorize a continuation; and a single
//!   fence refuses the certificate outright, because cancellation is
//!   what happens before the seal and nothing else.
//! * [`TerminalCertificate`]: after the seal, a majority of the old
//!   voters report the same terminal root. Each sealed voter reports
//!   once and never reports another root ([`StanceLedger::report_terminal`]),
//!   so two majorities cannot certify two roots. Mixed roots are refused
//!   rather than merged, and there is no selection without a seal -- an
//!   applied KV view or a closed frontend defines no terminal state.
//! * [`ActivationCertificate`]: a majority of the *successor* set have
//!   durably installed that exact terminal root. A new majority formed
//!   without old authority is disaster recovery, not handoff, and this
//!   module has no way to express it.
//!
//! Three things are structurally impossible here rather than checked
//! for: a fence cleared by a retry (a stance never reverses, and a
//! cancelled transition stays cancelled after another one replaces it),
//! a second terminal root (one report per sealed voter, selected by an
//! old majority after the seal), and a return to `Stable` once any
//! voter has sealed ([`resume`] has no path to it).
//!
//! The successor is not in that list. The transition's subject binds
//! it, but as a digest this module cannot open, and a terminal report
//! carries only the root; so [`select_terminal`] names whatever
//! successor its caller passes. Binding the successor to what the old
//! majority reported is the caller's obligation: the caller derives it
//! from the reported terminal state and makes the root cover it
//! (task-56 hashes the successor set into the terminal root), so racing
//! successor sets become mixed roots and are refused here.
//!
//! The module is pure: no I/O, no clock, no row encoding. Where the
//! stances live, what the terminal root is computed over and how a
//! successor is staged are the implementing tasks' (task-55, task-56,
//! task-57).

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::vec::Vec;

use coord_types::identity::Digest32;
use coord_types::ids::{ConfigurationEpoch, ReplicaId};
use serde::{Deserialize, Serialize};

use crate::quorum::EpochVoters;

/// One membership transition's identity.
///
/// `subject` binds the domain, both epochs and the exact successor
/// incarnations. Carried as a digest because this module decides
/// nothing about the bytes -- only that everyone who records anything
/// about a transition recorded it about the same one. Two transitions
/// differing in any of those are different transitions, and a stale
/// attempt is therefore a different subject rather than an older
/// version of this one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Transition {
    /// The configuration being left.
    pub from: ConfigurationEpoch,
    /// The configuration being entered.
    pub to: ConfigurationEpoch,
    /// Digest binding the domain, both epochs and the successor.
    pub subject: Digest32,
}

/// What one old voter durably decided about one transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Stance {
    /// Ordinary voting is fenced for the whole old configuration,
    /// across every ballot of it. Irreversible.
    Sealed,
    /// This transition is refused. Available only while nothing has
    /// sealed.
    Cancelled,
}

/// One old voter's durable record about one transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StanceRecord {
    /// The recording voter.
    pub voter: ReplicaId,
    /// The transition it is about.
    pub transition: Transition,
    /// What it decided.
    pub stance: Stance,
}

/// Why a voter could not record a stance.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum StanceError {
    /// This replica does not vote in the old configuration.
    NotAVoter,
    /// The transition leaves another configuration than this voter's.
    EpochMismatch,
    /// The opposite stance for the transition this voter already
    /// decided. A voter that could reverse itself would let a seal and
    /// a cancellation both certify, and a retry would clear a fence.
    Reversal {
        /// What it already holds.
        held: Stance,
    },
    /// This voter has sealed, so the old configuration is fenced and
    /// the recorded transition is the one that must be finished.
    /// Another transition is not an alternative to it.
    Sealed {
        /// The transition it sealed.
        held: Transition,
    },
}

/// Why a voter could not report a terminal root.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum TerminalReportError {
    /// This voter has not durably sealed the transition. Before its own
    /// fence it can still accept work, so what it would report is not
    /// terminal.
    NotSealed,
    /// This voter already reported another root. One voter in two
    /// majorities for two roots is exactly how two terminal certificates
    /// would form, so the first report is the only one.
    Conflicting {
        /// The root it already reported.
        held: Digest32,
    },
}

/// One old voter's durable stance row.
///
/// Besides the current stance it keeps every transition this voter
/// cancelled in the epoch, and once it has sealed, the one terminal root
/// it reported. A cancellation is not released by being replaced: a
/// delayed seal request for a cancelled transition names the same
/// subject, and a voter that had forgotten its refusal would record both
/// stances for it. The tombstones grow by one per cancelled transition
/// and are dropped with the epoch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StanceLedger {
    voter: ReplicaId,
    epoch: ConfigurationEpoch,
    held: Option<StanceRecord>,
    cancelled: BTreeSet<Transition>,
    terminal: Option<TerminalReport>,
}

impl StanceLedger {
    /// An empty ledger for `voter` of `epoch`.
    pub const fn new(voter: ReplicaId, epoch: ConfigurationEpoch) -> Self {
        StanceLedger {
            voter,
            epoch,
            held: None,
            cancelled: BTreeSet::new(),
            terminal: None,
        }
    }

    /// Rebuild from a durable row: the current stance, the transitions
    /// cancelled earlier in the epoch, and the terminal report if one
    /// was made.
    pub const fn recovered(
        voter: ReplicaId,
        epoch: ConfigurationEpoch,
        held: Option<StanceRecord>,
        cancelled: BTreeSet<Transition>,
        terminal: Option<TerminalReport>,
    ) -> Self {
        StanceLedger {
            voter,
            epoch,
            held,
            cancelled,
            terminal,
        }
    }

    /// What this voter holds, if anything.
    pub const fn held(&self) -> Option<StanceRecord> {
        self.held
    }

    /// Transitions this voter cancelled before the one it holds.
    pub const fn cancelled(&self) -> &BTreeSet<Transition> {
        &self.cancelled
    }

    /// The terminal report this voter made, if any.
    pub const fn terminal(&self) -> Option<TerminalReport> {
        self.terminal
    }

    /// Every stance this voter has recorded in the epoch: the retained
    /// cancellations and the current stance.
    pub fn records(&self) -> Vec<StanceRecord> {
        self.cancelled
            .iter()
            .filter(|t| self.held.is_none_or(|h| h.transition != **t))
            .map(|t| StanceRecord {
                voter: self.voter,
                transition: *t,
                stance: Stance::Cancelled,
            })
            .chain(self.held)
            .collect()
    }

    /// Whether this voter's ordinary voting is fenced.
    pub fn fenced(&self) -> bool {
        matches!(
            self.held,
            Some(StanceRecord {
                stance: Stance::Sealed,
                ..
            })
        )
    }

    /// Record `stance` about `transition`.
    ///
    /// Repeating what is already recorded succeeds and changes nothing,
    /// which is what makes a retry after a lost reply safe. Everything
    /// else that would change a decision is refused: the fence is the
    /// one thing in the lifecycle that nothing may undo, and the reason
    /// a seal and a cancellation cannot both certify is that this
    /// refusal happens in one voter, once -- including for a transition
    /// it cancelled before the one it holds now.
    pub fn record(
        &mut self,
        voters: &EpochVoters,
        transition: Transition,
        stance: Stance,
    ) -> Result<StanceRecord, StanceError> {
        if !voters.is_voter(&self.voter) {
            return Err(StanceError::NotAVoter);
        }
        if transition.from != self.epoch || voters.epoch() != self.epoch {
            return Err(StanceError::EpochMismatch);
        }
        let record = StanceRecord {
            voter: self.voter,
            transition,
            stance,
        };
        match self.held {
            Some(held) if held == record => return Ok(record),
            Some(held) if held.transition == transition => {
                return Err(StanceError::Reversal { held: held.stance });
            }
            _ => {}
        }
        // A transition cancelled earlier stays cancelled. Its tombstone
        // answers a repeated refusal and refuses a late seal, which is
        // the same subject arriving after another transition replaced
        // it, not a new attempt.
        if self.cancelled.contains(&transition) {
            return match stance {
                Stance::Cancelled => Ok(record),
                Stance::Sealed => Err(StanceError::Reversal {
                    held: Stance::Cancelled,
                }),
            };
        }
        match self.held {
            Some(held) if held.stance == Stance::Sealed => {
                return Err(StanceError::Sealed {
                    held: held.transition,
                });
            }
            // A cancelled transition released the domain, so another one
            // may start. Its cancellation moves to the tombstones rather
            // than being overwritten.
            Some(held) => {
                self.cancelled.insert(held.transition);
            }
            None => {}
        }
        self.held = Some(record);
        Ok(record)
    }

    /// Report `terminal_root` as the terminal state of `transition`,
    /// which this voter must have durably sealed.
    ///
    /// Once only. Repeating the same root succeeds and changes nothing,
    /// for the same retry reason as [`StanceLedger::record`]; any other
    /// root is refused. That refusal is what [`select_terminal`]'s
    /// intersection argument rests on: a voter that could report two
    /// roots would sit in one majority for each, and two coordinators
    /// could select two terminal certificates for one transition.
    pub fn report_terminal(
        &mut self,
        transition: Transition,
        terminal_root: Digest32,
    ) -> Result<TerminalReport, TerminalReportError> {
        let sealed = StanceRecord {
            voter: self.voter,
            transition,
            stance: Stance::Sealed,
        };
        if self.held != Some(sealed) {
            return Err(TerminalReportError::NotSealed);
        }
        let report = TerminalReport {
            voter: self.voter,
            transition,
            terminal_root,
        };
        match self.terminal {
            Some(held) if held == report => Ok(report),
            Some(held) => Err(TerminalReportError::Conflicting {
                held: held.terminal_root,
            }),
            None => {
                self.terminal = Some(report);
                Ok(report)
            }
        }
    }
}

/// Why a certificate could not be formed, or a stage could not be
/// chosen.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum HandoffError {
    /// A record came from a replica that does not vote where it must.
    NotAVoter {
        /// The sender.
        replica: ReplicaId,
    },
    /// A record is about another transition.
    WrongTransition,
    /// The voter set offered is not the configuration the transition
    /// leaves, or the successor offered is not the one it enters. A
    /// majority is a majority of that exact configuration; counted over
    /// some other set it is not a weaker certificate, it is none.
    EpochMismatch,
    /// The successor configuration offered for activation is not the
    /// one the terminal certificate names. The certificate was selected
    /// by the old majority for that successor and no other.
    WrongSuccessor,
    /// A cancellation was asked for after some voter sealed the
    /// transition. Cancellation is a pre-seal outcome: one fence is
    /// already irreversible, so the transition continues or blocks, and
    /// the refusals arrived too late to be a certificate.
    Fenced {
        /// A voter that sealed.
        by: ReplicaId,
    },
    /// Fewer than a majority.
    NoQuorum {
        /// Distinct replicas offered.
        have: usize,
        /// A majority of the configuration.
        need: usize,
    },
    /// Terminal reports of one transition disagree about the state or
    /// the successor. Merged they would be a history nobody agreed on,
    /// so they are refused.
    MixedTerminal,
    /// A terminal certificate was asked for without a durable seal. An
    /// applied KV view, a closed frontend or a vanished client is not a
    /// fence and defines no terminal state.
    NotSealed,
    /// An installation names a terminal root other than the
    /// certificate's.
    WrongTerminalRoot,
    /// A terminal report came from a voter that is not among the seal's
    /// signers. Its own fence is not in evidence, so it may still be
    /// accepting work and what it calls terminal is not; counted, it
    /// could complete a majority that omits a command chosen by it and a
    /// sealed voter. A voter that sealed after the certificate was
    /// formed counts once the seal is certified again over its record.
    ReporterNotSealed {
        /// The reporter.
        replica: ReplicaId,
    },
    /// A seal and a cancellation both certified one transition. The
    /// stance rules make this unreachable, so finding it is a reason to
    /// stop rather than to choose.
    SealedAndCancelled,
    /// Some voter is fenced by a different transition. The domain
    /// permits one at a time and a fence belongs to the configuration,
    /// not to whoever started it, so the recorded transition is the one
    /// that must be finished -- this one has nothing to resume.
    FencedByAnother {
        /// The transition that fenced it.
        held: Transition,
    },
}

/// A majority of the old voters have durably fenced the old
/// configuration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SealCertificate {
    transition: Transition,
    signers: BTreeSet<ReplicaId>,
}

impl SealCertificate {
    /// The transition it seals.
    pub const fn transition(&self) -> Transition {
        self.transition
    }

    /// The voters that fenced.
    pub const fn signers(&self) -> &BTreeSet<ReplicaId> {
        &self.signers
    }
}

/// A majority of the old voters have durably refused the transition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CancellationCertificate {
    transition: Transition,
    signers: BTreeSet<ReplicaId>,
}

impl CancellationCertificate {
    /// The transition it cancels.
    pub const fn transition(&self) -> Transition {
        self.transition
    }

    /// The voters that refused.
    pub const fn signers(&self) -> &BTreeSet<ReplicaId> {
        &self.signers
    }
}

fn certify(
    voters: &EpochVoters,
    transition: Transition,
    stances: &[StanceRecord],
    wanted: Stance,
) -> Result<BTreeSet<ReplicaId>, HandoffError> {
    // The majority is of the configuration being left. Counted over
    // another epoch's voters the signers could be a majority of a set
    // that never fenced anything.
    if voters.epoch() != transition.from {
        return Err(HandoffError::EpochMismatch);
    }
    let mut signers = BTreeSet::new();
    for record in stances {
        if !voters.is_voter(&record.voter) {
            return Err(HandoffError::NotAVoter {
                replica: record.voter,
            });
        }
        if record.transition != transition {
            return Err(HandoffError::WrongTransition);
        }
        if record.stance == wanted {
            signers.insert(record.voter);
        }
    }
    let need = voters.majority();
    if signers.len() < need {
        return Err(HandoffError::NoQuorum {
            have: signers.len(),
            need,
        });
    }
    Ok(signers)
}

/// Certify that the old configuration is fenced.
pub fn seal(
    voters: &EpochVoters,
    transition: Transition,
    stances: &[StanceRecord],
) -> Result<SealCertificate, HandoffError> {
    Ok(SealCertificate {
        transition,
        signers: certify(voters, transition, stances, Stance::Sealed)?,
    })
}

/// Certify that the transition was refused before it sealed.
///
/// A single fence forbids it. A voter that sealed will not vote in the
/// old configuration again whatever the others decided, so a
/// cancellation formed beside it would authorize a return to `Stable`
/// that the fence has already made impossible.
pub fn cancel(
    voters: &EpochVoters,
    transition: Transition,
    stances: &[StanceRecord],
) -> Result<CancellationCertificate, HandoffError> {
    if let Some(fence) = stances
        .iter()
        .find(|s| s.transition == transition && s.stance == Stance::Sealed)
    {
        return Err(HandoffError::Fenced { by: fence.voter });
    }
    Ok(CancellationCertificate {
        transition,
        signers: certify(voters, transition, stances, Stance::Cancelled)?,
    })
}

/// One old voter's report of the terminal state, made after its own
/// seal is durable and at most once per voter
/// ([`StanceLedger::report_terminal`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalReport {
    /// The reporting voter.
    pub voter: ReplicaId,
    /// The transition it is about.
    pub transition: Transition,
    /// Root binding the final common state, history, execution and
    /// revision boundaries, retries, floors, leases, sessions, policy
    /// and checkpoint lineage.
    pub terminal_root: Digest32,
}

/// The one terminal state and successor of a sealed transition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TerminalCertificate {
    transition: Transition,
    terminal_root: Digest32,
    successor: BTreeSet<ReplicaId>,
    signers: BTreeSet<ReplicaId>,
}

impl TerminalCertificate {
    /// The transition it settles.
    pub const fn transition(&self) -> Transition {
        self.transition
    }

    /// The terminal state the successor must install.
    pub const fn terminal_root(&self) -> Digest32 {
        self.terminal_root
    }

    /// The exact successor.
    pub const fn successor(&self) -> &BTreeSet<ReplicaId> {
        &self.successor
    }

    /// The old voters that reported it.
    pub const fn signers(&self) -> &BTreeSet<ReplicaId> {
        &self.signers
    }
}

/// Select the terminal certificate of a sealed transition.
///
/// There is no selection without `seal`, and the reason is the whole
/// reason sealing exists: before the fence, an old voter can still
/// accept work, so what it reports as terminal is not terminal. After
/// it, a majority reporting the same root is the terminal state, and
/// any majority a later attempt reads intersects this one in a voter
/// that reported once and will report nothing else -- so a coordinator
/// that dies here is replaced by one that selects the same certificate
/// rather than a competing destination. Reports are counted as given;
/// that each came from its voter's [`StanceLedger::report_terminal`] is
/// what makes them one per voter.
///
/// "After the fence" is about each reporter, not only about the
/// configuration: every report must come from one of `seal`'s signers.
/// A majority sealing does not fence the rest, and a voter outside the
/// signers can still accept a command after reporting, so a certificate
/// it helped prove could omit a command chosen by it together with a
/// sealed voter -- the latent old completion the certificate exists to
/// keep. A voter that sealed late is counted by certifying the seal
/// again with its record among the stances.
///
/// `successor` is copied into the certificate unchecked, because
/// nothing here can compare it with the transition's opaque subject.
/// The caller derives it from the reported terminal state, whose root
/// must cover it, so a different successor is a different root.
pub fn select_terminal(
    voters: &EpochVoters,
    seal: &SealCertificate,
    successor: &BTreeSet<ReplicaId>,
    reports: &[TerminalReport],
) -> Result<TerminalCertificate, HandoffError> {
    let transition = seal.transition();
    if voters.epoch() != transition.from {
        return Err(HandoffError::EpochMismatch);
    }
    let mut roots: BTreeMap<Digest32, BTreeSet<ReplicaId>> = BTreeMap::new();
    for report in reports {
        if !voters.is_voter(&report.voter) {
            return Err(HandoffError::NotAVoter {
                replica: report.voter,
            });
        }
        if report.transition != transition {
            return Err(HandoffError::WrongTransition);
        }
        if !seal.signers().contains(&report.voter) {
            return Err(HandoffError::ReporterNotSealed {
                replica: report.voter,
            });
        }
        roots
            .entry(report.terminal_root)
            .or_default()
            .insert(report.voter);
    }
    if roots.len() > 1 {
        return Err(HandoffError::MixedTerminal);
    }
    let Some((terminal_root, signers)) = roots.into_iter().next() else {
        return Err(HandoffError::NoQuorum {
            have: 0,
            need: voters.majority(),
        });
    };
    let need = voters.majority();
    if signers.len() < need {
        return Err(HandoffError::NoQuorum {
            have: signers.len(),
            need,
        });
    }
    Ok(TerminalCertificate {
        transition,
        terminal_root,
        successor: successor.clone(),
        signers,
    })
}

/// One successor replica's durable installation of the terminal state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallRecord {
    /// The installing replica.
    pub replica: ReplicaId,
    /// The transition it is about.
    pub transition: Transition,
    /// The terminal root it installed.
    pub terminal_root: Digest32,
}

/// The successor is authorized to serve.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ActivationCertificate {
    transition: Transition,
    terminal_root: Digest32,
    installers: BTreeSet<ReplicaId>,
}

impl ActivationCertificate {
    /// The transition it completes.
    pub const fn transition(&self) -> Transition {
        self.transition
    }

    /// The terminal state the successor installed.
    pub const fn terminal_root(&self) -> Digest32 {
        self.terminal_root
    }

    /// The successor replicas that installed it.
    pub const fn installers(&self) -> &BTreeSet<ReplicaId> {
        &self.installers
    }
}

/// Activate the successor of `certificate`.
///
/// A majority of the successor set must have durably installed that
/// exact terminal root. Installing something else is not a weaker
/// installation, it is another history, and it is refused rather than
/// counted. The successor set is the certificate's, not the caller's:
/// a majority of some other set holding the root is a set the old
/// majority never selected.
pub fn activate(
    successor: &EpochVoters,
    certificate: &TerminalCertificate,
    installs: &[InstallRecord],
) -> Result<ActivationCertificate, HandoffError> {
    if successor.epoch() != certificate.transition().to {
        return Err(HandoffError::EpochMismatch);
    }
    if successor.voters() != certificate.successor() {
        return Err(HandoffError::WrongSuccessor);
    }
    let mut installers = BTreeSet::new();
    for record in installs {
        if !successor.is_voter(&record.replica) {
            return Err(HandoffError::NotAVoter {
                replica: record.replica,
            });
        }
        if record.transition != certificate.transition() {
            return Err(HandoffError::WrongTransition);
        }
        if record.terminal_root != certificate.terminal_root() {
            return Err(HandoffError::WrongTerminalRoot);
        }
        installers.insert(record.replica);
    }
    let need = successor.majority();
    if installers.len() < need {
        return Err(HandoffError::NoQuorum {
            have: installers.len(),
            need,
        });
    }
    Ok(ActivationCertificate {
        transition: certificate.transition(),
        terminal_root: certificate.terminal_root(),
        installers,
    })
}

/// Everything durable about one transition that a recovering
/// coordinator can read.
///
/// Every field is a record some replica made durable. There is no
/// lifecycle label here, and deliberately no place to put one: the last
/// stage a dead coordinator believed it was in is exactly the thing
/// [`resume`] must not consult.
#[derive(Clone, Copy, Debug)]
pub struct Evidence<'a> {
    /// Whether the transition was authorized at all. Durable, because
    /// an unauthorized transition has no records and an authorized one
    /// that produced none is still not `Stable` by accident.
    pub authorized: bool,
    /// Every old-voter stance that was recovered, about any
    /// transition. Passing only this transition's would hide a fence
    /// another one left, and the fence is the configuration's, not the
    /// transition's.
    pub stances: &'a [StanceRecord],
    /// Terminal reports recovered from the old voters.
    pub reports: &'a [TerminalReport],
    /// The selected terminal certificate, if one was made durable.
    pub terminal: Option<&'a TerminalCertificate>,
    /// Installations recovered from the successor.
    pub installs: &'a [InstallRecord],
    /// The activation, if one was made durable.
    pub activation: Option<&'a ActivationCertificate>,
}

/// Where a recovering coordinator resumes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum Stage {
    /// Nothing is in flight, or the transition was certifiably
    /// cancelled before anything sealed.
    Stable,
    /// Authorized and staging; sealing has not started and cancellation
    /// is still available.
    Preparing,
    /// Some voter has fenced. The seal is the point of no return, so
    /// there is no path from here back to `Stable`: the authorized
    /// sealing procedure continues, or it blocks.
    Sealing,
    /// The old configuration is fenced; the terminal certificate is not
    /// selected yet.
    TerminalRecovery,
    /// The terminal certificate exists; the successor has not installed
    /// it.
    Installing,
    /// A majority of the successor has installed; activation is owed.
    Activating,
    /// The successor is activated and serving. Reused, never
    /// recomputed.
    Served,
}

/// Choose where to resume from durable evidence alone.
///
/// The order is by how far the transition demonstrably got, highest
/// first, because every later record implies the earlier ones happened
/// and the opposite is never true. A stage is returned only when a
/// record justifies it.
pub fn resume(
    old: &EpochVoters,
    successor: &EpochVoters,
    transition: Transition,
    evidence: Evidence<'_>,
) -> Result<Stage, HandoffError> {
    // The configurations are the transition's. Certifying over any
    // other pair would count majorities of sets the records are not
    // about, and a stage justified that way is justified by nothing.
    if old.epoch() != transition.from || successor.epoch() != transition.to {
        return Err(HandoffError::EpochMismatch);
    }
    // An activation that exists is the answer. Reuse it rather than
    // recomputing an outcome that could differ from the one the
    // successor is already serving under.
    if let Some(activation) = evidence.activation {
        if activation.transition() != transition {
            return Err(HandoffError::WrongTransition);
        }
        return Ok(Stage::Served);
    }
    // A fence another transition left is still a fence. The domain
    // permits one transition at a time, so finding one means this one
    // is not what the configuration is in the middle of.
    if let Some(elsewhere) = evidence
        .stances
        .iter()
        .find(|s| s.transition != transition && s.stance == Stance::Sealed)
    {
        return Err(HandoffError::FencedByAnother {
            held: elsewhere.transition,
        });
    }
    let mine: Vec<StanceRecord> = evidence
        .stances
        .iter()
        .filter(|s| s.transition == transition)
        .copied()
        .collect();
    let sealed = seal(old, transition, &mine);
    let cancelled = cancel(old, transition, &mine);
    if sealed.is_ok() && cancelled.is_ok() {
        return Err(HandoffError::SealedAndCancelled);
    }
    if let Some(certificate) = evidence.terminal {
        if certificate.transition() != transition {
            return Err(HandoffError::WrongTransition);
        }
        return match activate(successor, certificate, evidence.installs) {
            Ok(_) => Ok(Stage::Activating),
            Err(HandoffError::NoQuorum { .. }) => Ok(Stage::Installing),
            Err(e) => Err(e),
        };
    }
    if let Ok(certificate) = sealed {
        // Sealed, and the certificate may already be selectable from
        // what was recovered -- but it is not durable, so the stage is
        // still the one that selects it.
        let _ = certificate;
        return Ok(Stage::TerminalRecovery);
    }
    // Any fence at all, even one voter's, forbids `Stable`. A partial
    // seal is reconciled by continuing, never by clearing a fence: the
    // voters that sealed will not vote in the old configuration again
    // whatever a coordinator decides.
    let fenced = mine.iter().any(|s| s.stance == Stance::Sealed);
    if fenced {
        return Ok(Stage::Sealing);
    }
    if cancelled.is_ok() {
        return Ok(Stage::Stable);
    }
    if evidence.authorized {
        return Ok(Stage::Preparing);
    }
    Ok(Stage::Stable)
}

/// Every stance a set of ledgers holds, retained cancellations
/// included, for a coordinator assembling evidence.
///
/// Deliberately unfiltered. A coordinator that collected only its own
/// transition's stances would hand [`resume`] evidence that cannot show
/// it a fence another transition left, and that fence is the whole
/// reason its transition has nothing to resume.
pub fn stances_of(ledgers: &[StanceLedger]) -> Vec<StanceRecord> {
    ledgers.iter().flat_map(StanceLedger::records).collect()
}
