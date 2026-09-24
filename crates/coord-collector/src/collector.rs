//! The collector contract (design Sections 3.2, 4.3, 4.4; frozen in
//! `spec/collector-v1.md`).
//!
//! * **Fan-out** is parallel and direct: the canonical command goes to
//!   every voter of the configuration at once. The leader is one target
//!   among them, never a relay; nothing waits for a leader reply before
//!   the followers hear the command.
//! * **Evidence** is counted by voter identity through
//!   [`coord_consensus::VoteSet`]: the sender bound at negotiation must be
//!   the replica the acknowledgement claims, a second connection of the
//!   same identity is a duplicate, non-voters and out-of-fast-set fast
//!   acknowledgements are rejected, and only the ballot's leader may
//!   publish a release.
//! * **Release** needs both halves: the collector's own learning
//!   predicate over counted votes (fast: leader proposal plus path-equal
//!   fast-set members; slow: adoption by a majority including the
//!   leader) *and* the leader's release-gate result for the same epoch,
//!   ballot and command. A lone leader reply, a lone release, or a
//!   majority without the leader never releases tentative data.
//! * **Cancellation** detaches the caller and nothing else: the entry
//!   keeps collecting, its identity stays bound and the outcome, once
//!   established, is resolvable by `ResolveRequest`.
//! * **Dissemination** is the collector's obligation, not the caller's.
//!   Every voter is offered the submission, independently and without
//!   blocking; a destination whose queue is full or whose link is down
//!   refuses *delivery*, which is neither a rejection of the command nor
//!   a change of membership. While the command is unresolved this
//!   collector re-offers what could not be queued. All-voter targeting
//!   is required; all-voter acceptance is not, and nothing waits for it:
//!   release is the learning predicate plus the leader's release gate,
//!   exactly as before.
//! * **Bounds**: pending commands per domain are capped and unresolved
//!   entries are never evicted to make room; the retained submission
//!   envelopes are capped in bytes; resolved outcomes are kept in a
//!   bounded window. Capacity for both is reserved *before* anything is
//!   offered, so a refusal means nothing was sent by this attempt --
//!   and a destination refusing its queue afterwards never turns into
//!   "not executed".

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use coord_consensus::{
    BallotConfiguration, FastAck, Learned, ProtocolMessage, Vote, VoteError, VoteSet,
};
use coord_core::capability::{ReleasedResult, admission_digest};
use coord_core::event::{AdmittedRequest, PeerProvenance};
use coord_types::ids::{KvRevision, ReplicaId, SessionId};
use coord_types::wire_v1::{
    BoundedBytes, MessageV1, OutcomeV1, ResolveRequestV1, ResponseV1, decode_stream,
};
use coord_types::{CommandId, RetryKey};

use crate::clock::MonotonicMillis;
use crate::codes;

use crate::trace::{CollectorEvent, command_hex, replica_hex};
use crate::wire::{SubmitV1, submit_frame};

/// The largest result a client can actually be handed: an API-class
/// frame, less room for the response's own fields and the frame header.
const MAX_DELIVERABLE_RESULT_BYTES: usize =
    (coord_types::wire_v1::KindRange::Api.max_frame_length() as usize) - 64 * 1024;

/// What a submission envelope may carry beyond its request's own bytes.
///
/// The undelivered-bytes budget is sized from the request limit, but
/// what the collector holds is the whole `Submit` frame: the request's
/// canonical encoding -- which the wire lets run up to 64 KiB past the
/// request limit, for the encoding's own tags and lengths -- inside a
/// `SubmitV1` beside the admission facts, with the retry key, deadline
/// and acknowledged floor, and the frame header. Those come to a few
/// hundred bytes; the allowance is the wire's 64 KiB twice over, so a
/// request at the limit always fits the slot it was admitted into.
pub const SUBMIT_ENVELOPE_ALLOWANCE: usize = 128 * 1024;

/// The undelivered-bytes budget of a collector that may hold
/// `max_pending` commands of requests up to `max_request_bytes`: one
/// whole submission envelope for each, request and allowance together.
///
/// Budgeting the request alone refused a legal request near the limit
/// when only one command may be pending -- the envelope around it is
/// what is held, and it is larger than the request by a constant.
pub const fn undelivered_budget(max_pending: usize, max_request_bytes: usize) -> usize {
    max_pending.saturating_mul(max_request_bytes.saturating_add(SUBMIT_ENVELOPE_ALLOWANCE))
}

/// Configuration of one domain's collector.
#[derive(Clone, Debug)]
pub struct CollectorConfig {
    /// The current ballot configuration (voters, leader, fast set).
    pub quorum: BallotConfiguration,
    /// Commands pending at once (per domain).
    pub max_pending: usize,
    /// Resolved outcomes retained for retries and resolution.
    pub max_resolved: usize,
    /// Submission envelope bytes this collector will hold at once for
    /// commands it still owes a destination.
    ///
    /// The second half of the admission reservation, and the one a
    /// count alone cannot express: what re-offering costs is the
    /// envelopes, and one large command is not one small one. Reserved
    /// before any destination is offered and released the moment the
    /// command owes nobody -- either because every voter took it or
    /// because it settled.
    pub max_undelivered_bytes: usize,
}

/// The parallel fan-out of one submission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FanOut {
    /// Command.
    pub command: CommandId,
    /// Retry key.
    pub retry_key: RetryKey,
    /// The voters this offer is for.
    ///
    /// Every voter of the configuration on the first offer. A re-offer
    /// names only the destinations that still owe an enqueue, which is
    /// what keeps a repeat from costing the voters that already took it.
    pub targets: Vec<ReplicaId>,
    /// The `Submit` frame.
    ///
    /// Shared rather than owned, because the collector keeps this exact
    /// envelope for as long as it may have to offer it again. A
    /// re-offer is another delivery attempt for the original
    /// submission: the same command identity, retry key, canonical
    /// request, admission facts and acknowledged sequence floor. It is
    /// never rebuilt from current session state, which would mint fresh
    /// admission facts for a command already submitted under others.
    pub frame: Arc<[u8]>,
}

/// What one destination did with one offer of a submission.
///
/// This is delivery, not consensus. `Queued` says an ingress accepted
/// responsibility for the frame; it is not a vote, not durability and
/// not application, and a destination that later fails or makes no
/// progress is still the same-identity retry and recovery problem it
/// always was. The reasons are kept apart because they need different
/// treatment, which is the whole point of reporting them separately.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OfferOutcome {
    /// The destination's ingress took the frame.
    Queued,
    /// The destination is a live voter with no room right now. Delivery
    /// backpressure: offer it again.
    Saturated,
    /// There is no route to the destination at the moment. A network
    /// fact, and also transient: offer it again.
    Unreachable,
    /// The committed configuration does not name this replica as a
    /// voter of this domain. A disagreement between the plan and the
    /// configuration, which repeating cannot fix: it is not retried on
    /// the congestion schedule, and only a reconfiguration revisits it.
    NotACommittedVoter,
    /// The frame can never reach this destination as it stands -- it
    /// does not fit what the route may carry. Permanent for this
    /// envelope, so retrying it forever would be a busy loop with a
    /// known answer.
    Undeliverable,
}

impl OfferOutcome {
    /// Whether offering this destination again could plausibly differ.
    const fn worth_repeating(self) -> bool {
        matches!(self, OfferOutcome::Saturated | OfferOutcome::Unreachable)
    }
}

/// What an offer of one command's submission actually achieved.
///
/// Handed back to the collector by whoever holds the sockets, so the
/// obligation to re-offer lives beside the pending command rather than
/// in the runtime that happened to make the attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Offered {
    /// The command the offer was for.
    pub command: CommandId,
    /// One outcome per destination the offer named.
    pub outcomes: Vec<(ReplicaId, OfferOutcome)>,
}

/// What one destination still owes this command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Owed {
    /// The ingress took it. Nothing further is owed by delivery.
    Queued,
    /// Not taken, and worth offering again at `next` with `attempts`
    /// behind it.
    Missed {
        attempts: u32,
        next: MonotonicMillis,
        why: OfferOutcome,
    },
    /// Not taken, and not on the congestion schedule: a configuration
    /// disagreement or an envelope this route can never carry. Held so
    /// it can be reported at settlement rather than forgotten, and
    /// revisited only by a reconfiguration.
    Stalled(OfferOutcome),
}

/// The delivery state of one pending command's submission.
struct Dissemination {
    /// The exact envelope that was submitted, kept while anything is
    /// owed. `None` once every destination has taken it, which is what
    /// releases the bytes reserved for it.
    envelope: Option<Arc<[u8]>>,
    /// Bytes this command has reserved against the byte budget. Held
    /// separately from `envelope` so releasing is a single subtraction
    /// that cannot drift from what was added.
    reserved: usize,
    /// What each planned destination owes, in replica order.
    owed: BTreeMap<ReplicaId, Owed>,
}

/// Reported by its shape, never its contents: the envelope is a
/// caller's request, and a debug format that printed it would put one
/// in every log line that ever formats a collector.
impl core::fmt::Debug for Dissemination {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Dissemination")
            .field("envelope_bytes", &self.envelope.as_ref().map(|e| e.len()))
            .field("reserved", &self.reserved)
            .field("owed", &self.owed)
            .finish()
    }
}

impl Dissemination {
    /// Destinations that have not taken the frame, for any reason.
    fn missing(&self) -> Vec<ReplicaId> {
        self.owed
            .iter()
            .filter(|(_, owed)| !matches!(owed, Owed::Queued))
            .map(|(r, _)| *r)
            .collect()
    }

    /// Whether anything is still owed and worth another offer.
    fn outstanding(&self) -> bool {
        self.owed
            .values()
            .any(|owed| matches!(owed, Owed::Missed { .. }))
    }
}

/// How long a destination waits before the first repeat, in
/// milliseconds.
///
/// Milliseconds of a **monotonic** reading ([`MonotonicMillis`]), the
/// same reading the deadline path takes, and deliberately not the wall
/// clock that authenticates a token: a retry schedule built on a clock
/// that can step is a retry schedule that can stall or stampede. The
/// caller supplies it; this crate holds no clock of its own.
///
/// A floor, not a rate. A queue that was full a moment ago is often
/// free a moment later, so the first repeat is soon; what must not
/// happen is a repeat per turn, which would spend the lane on the
/// retries instead of on the drain that ends them.
const OFFER_FLOOR_MILLIS: u64 = 25;

/// The longest a destination waits between repeats, in milliseconds.
///
/// Doubling stops here. A destination that has refused many times in a
/// row is congested or gone, and the cost of finding out is one frame a
/// second rather than a frame per floor for as long as it lasts.
///
/// Public because a voter's side has to outlast it. A voter that
/// acknowledged a command before the submission naming its submitter
/// arrived holds that evidence for the submission to place it, and a
/// hold shorter than this schedule would expire between two repeats --
/// so the frame would land on a voter with nothing left to hand over,
/// and the acknowledgement would be lost for good.
pub const OFFER_CEILING_MILLIS: u64 = 1_000;

/// What a submission produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Submitted {
    /// New work: fan out.
    FanOut(FanOut),
    /// A retry of a pending command: the caller is re-attached, nothing
    /// is sent again.
    Attached {
        /// Command.
        command: CommandId,
    },
    /// A retry of a resolved command: the retained outcome.
    Resolved(ResponseV1),
}

/// Why a submission was refused. Nothing was sent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubmitRefusal {
    /// The admitted frame is not a canonical request.
    Malformed,
    /// The retry key is bound to another payload.
    RequestIdentityConflict {
        /// Identity bound first.
        bound: CommandId,
    },
    /// The domain is at its pending bound.
    Backpressure {
        /// Commands pending.
        pending: usize,
    },
}

/// Why evidence was not counted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvidenceError {
    /// The acknowledgement claims a replica other than the bound sender.
    SenderMismatch {
        /// Bound sender.
        sender: ReplicaId,
        /// Claimed replica.
        claimed: ReplicaId,
    },
    /// No pending command of that identity.
    UnknownCommand,
    /// A release from someone other than the ballot's leader.
    NotLeader {
        /// Sender.
        sender: ReplicaId,
    },
    /// A release for another epoch.
    WrongEpoch,
    /// A release for another ballot.
    WrongBallot,
    /// The vote set rejected the acknowledgement.
    Vote(VoteError),
    /// Not an evidence message (a recovery or transfer message).
    NotEvidence,
    /// A second, different release for the command.
    ReleaseMismatch,
}

/// What a release still waits for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HoldReason {
    /// The learning predicate does not hold over counted votes.
    AwaitingVotes,
    /// The leader's release-gate result has not arrived.
    AwaitingRelease,
}

/// A released result for a caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Release {
    /// Retry key.
    pub retry_key: RetryKey,
    /// Command.
    pub command: CommandId,
    /// Session that submitted it.
    pub session: SessionId,
    /// The response frame payload.
    pub response: ResponseV1,
    /// Whether it preceded materialization.
    pub speculative: bool,
    /// Whether the collector learned on the fast path.
    pub fast: bool,
    /// Whether a caller is still attached.
    pub attached: bool,
}

/// Progress after evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Progress {
    /// Still pending.
    Held(HoldReason),
    /// Released now.
    Released(Release),
    /// Already released earlier; the evidence changed nothing.
    Settled,
}

/// Resolution of an identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Resolution {
    /// The established outcome.
    Outcome(ResponseV1),
    /// Bound and still collecting.
    Pending,
    /// The retry key is bound to another payload.
    Conflict {
        /// Identity bound.
        bound: CommandId,
    },
    /// Never seen here, or beyond the retained window.
    Unknown,
}

/// A command whose client deadline passed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Expired {
    /// Retry key.
    pub retry_key: RetryKey,
    /// Command.
    pub command: CommandId,
    /// Session.
    pub session: SessionId,
}

/// Why the durable record of a command's execution did not settle it
/// here (task-c02).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SettleError {
    /// Not pending here: never submitted through this collector, or
    /// already resolved and beyond the retained window.
    NotPending,
    /// Nothing of this collector's own corroborates the record -- neither
    /// the learning predicate nor the leader's release. The record alone
    /// is deliberately not enough here; a caller's retry is answered from
    /// it directly, before anything is submitted, and that is the path
    /// for a command this collector holds nothing for.
    Uncorroborated,
    /// The release this collector holds and the record disagree on what
    /// the command produced. Neither is believed over the other.
    Mismatch,
}

#[derive(Debug)]
struct Pending {
    retry_key: RetryKey,
    session: SessionId,
    /// Digest of the admission this collector submitted the command
    /// with. Evidence counted for the command must agree with it: a
    /// voter that acknowledged the same identity under other attested
    /// facts did not acknowledge this request.
    admission: coord_types::identity::Digest32,
    votes: VoteSet,
    released: Option<ReleasedResult>,
    attached: bool,
    deadline: Option<MonotonicMillis>,
    timed_out: bool,
    /// What each voter still owes this command by way of taking the
    /// submission. Independent of `votes`: a destination that took the
    /// frame has not voted, and a destination that never took it may
    /// still have learned the command from the leader.
    ///
    /// Bound to the *command*, not to the caller: a caller that times
    /// out or disconnects is detached and this obligation stays, which
    /// is the rule that keeps a client deadline from silently becoming
    /// the lifetime of work the domain has accepted.
    dissemination: Dissemination,
}

/// The collector of one domain.
#[derive(Debug)]
pub struct Collector {
    config: CollectorConfig,
    pending: BTreeMap<CommandId, Pending>,
    bindings: BTreeMap<RetryKey, CommandId>,
    resolved: BTreeMap<RetryKey, (CommandId, ResponseV1)>,
    resolved_order: VecDeque<RetryKey>,
    trace: Vec<CollectorEvent>,
    /// Envelope bytes reserved by commands that still owe a
    /// destination. The sum of every pending entry's `reserved`, kept
    /// incrementally so admission is a comparison rather than a walk.
    undelivered_bytes: usize,
    /// Where the next round of repeats starts looking, so one
    /// destination's congestion cannot spend the whole budget on the
    /// same command every turn.
    offer_cursor: usize,
}

impl Collector {
    /// A collector under `config`.
    pub const fn new(config: CollectorConfig) -> Self {
        Collector {
            config,
            pending: BTreeMap::new(),
            bindings: BTreeMap::new(),
            resolved: BTreeMap::new(),
            resolved_order: VecDeque::new(),
            trace: Vec::new(),
            undelivered_bytes: 0,
            offer_cursor: 0,
        }
    }

    /// The current ballot configuration.
    pub const fn quorum(&self) -> &BallotConfiguration {
        &self.config.quorum
    }

    /// Commands pending.
    pub fn pending(&self) -> usize {
        self.pending.len()
    }

    /// The admission digest `command` was submitted under, while it is
    /// outstanding: what evidence for it has to agree with.
    pub fn admission(&self, command: &CommandId) -> Option<coord_types::identity::Digest32> {
        self.pending.get(command).map(|p| p.admission)
    }

    /// Whether `command` is outstanding here.
    pub fn is_pending(&self, command: &CommandId) -> bool {
        self.pending.contains_key(command)
    }

    /// The transitions so far.
    pub fn trace(&self) -> &[CollectorEvent] {
        &self.trace
    }

    /// Take the transitions so far.
    pub fn take_trace(&mut self) -> Vec<CollectorEvent> {
        std::mem::take(&mut self.trace)
    }

    /// Submit an admitted request at `now`, the monotonic reading its
    /// deadline is measured from.
    pub fn submit(
        &mut self,
        now: MonotonicMillis,
        admitted: &AdmittedRequest,
    ) -> Result<Submitted, SubmitRefusal> {
        let request = match decode_stream(&admitted.frame).as_deref() {
            Ok([MessageV1::Request(r)]) => r.clone(),
            _ => return Err(SubmitRefusal::Malformed),
        };
        let logical = request.logical().map_err(|_| SubmitRefusal::Malformed)?;
        let command = CommandId::derive(&request.retry_key, &logical)
            .map_err(|_| SubmitRefusal::Malformed)?;
        let key = request.retry_key;
        let sequence = key.request_sequence.get();
        if let Some(bound) = self.bindings.get(&key).copied() {
            if bound != command {
                self.trace.push(CollectorEvent::Refused {
                    sequence,
                    reason: "request-identity-conflict".into(),
                });
                return Err(SubmitRefusal::RequestIdentityConflict { bound });
            }
            if let Some((_, response)) = self.resolved.get(&key) {
                self.trace.push(CollectorEvent::Retained {
                    command: command_hex(&command),
                    sequence,
                });
                return Ok(Submitted::Resolved(response.clone()));
            }
            if let Some(entry) = self.pending.get_mut(&command) {
                entry.attached = true;
                entry.timed_out = false;
                entry.deadline = deadline(now, request.deadline_ms);
                self.trace.push(CollectorEvent::Attached {
                    command: command_hex(&command),
                    sequence,
                });
                return Ok(Submitted::Attached { command });
            }
            // Bound but neither pending nor resolved: the retained window
            // moved past it; it is new work again under the same identity.
        }
        if self.pending.len() >= self.config.max_pending {
            self.trace.push(CollectorEvent::Refused {
                sequence,
                reason: "backpressure".into(),
            });
            return Err(SubmitRefusal::Backpressure {
                pending: self.pending.len(),
            });
        }
        let frame = submit_frame(&SubmitV1 {
            receipt: admitted.receipt.facts(),
            request: request.clone(),
        })
        .map_err(|_| SubmitRefusal::Malformed)?;
        // The second half of the reservation, and the last point at
        // which refusing is honest. Accepting a command means owing its
        // dissemination until it settles, and that obligation is the
        // envelope: reserve it here, before a single destination has
        // been offered anything, so a refusal is a statement that
        // nothing was sent by this attempt. Past this line no
        // destination's answer may turn into a refusal of the command,
        // because by then it may be anywhere.
        let bytes = frame.len();
        if self.undelivered_bytes.saturating_add(bytes) > self.config.max_undelivered_bytes {
            self.trace.push(CollectorEvent::Refused {
                sequence,
                reason: "undelivered-bytes".into(),
            });
            return Err(SubmitRefusal::Backpressure {
                pending: self.pending.len(),
            });
        }
        let frame: Arc<[u8]> = Arc::from(frame.into_boxed_slice());
        let targets: Vec<ReplicaId> = self.config.quorum.voters().iter().copied().collect();
        self.undelivered_bytes += bytes;
        self.bindings.insert(key, command);
        self.pending.insert(
            command,
            Pending {
                retry_key: key,
                session: admitted.receipt.session(),
                admission: admission_digest(Some(&admitted.receipt.facts()), request.ack_through),
                votes: VoteSet::new(self.config.quorum.clone(), command),
                released: None,
                attached: true,
                deadline: deadline(now, request.deadline_ms),
                timed_out: false,
                dissemination: Dissemination {
                    envelope: Some(Arc::clone(&frame)),
                    reserved: bytes,
                    // Every voter is a planned destination from the
                    // start, and stays one until it takes the frame.
                    // Nothing here is scheduled yet: the offer about to
                    // be made is the first attempt, and its answer is
                    // what puts a destination on the repeat schedule.
                    owed: targets
                        .iter()
                        .map(|t| (*t, Owed::Stalled(OfferOutcome::Unreachable)))
                        .collect(),
                },
            },
        );
        self.trace.push(CollectorEvent::Submitted {
            command: command_hex(&command),
            sequence,
            targets: targets.iter().map(replica_hex).collect(),
        });
        Ok(Submitted::FanOut(FanOut {
            command,
            retry_key: key,
            targets,
            frame,
        }))
    }

    /// Record what an offer of `report.command` achieved, destination by
    /// destination.
    ///
    /// The one way delivery state moves. Whoever holds the sockets makes
    /// the attempt and says what happened; the obligation that follows
    /// from it lives here, beside the pending command, because that is
    /// what survives the caller going away and the runtime moving on.
    ///
    /// Nothing here can fail a command. A destination that refused its
    /// queue has said something about itself and nothing about whether
    /// the command will be established: the learning predicate and the
    /// leader's release gate decide that, over however many voters did
    /// take it.
    pub fn offered(&mut self, now: MonotonicMillis, report: &Offered) {
        let Some(entry) = self.pending.get_mut(&report.command) else {
            // Settled, or never ours. Either way there is nothing left
            // to owe, and a late report about it is not an error.
            return;
        };
        for (replica, outcome) in &report.outcomes {
            // Only planned destinations. A report naming a replica this
            // command never targeted is not permission to start owing
            // it one.
            let Some(owed) = entry.dissemination.owed.get_mut(replica) else {
                continue;
            };
            *owed = match outcome {
                OfferOutcome::Queued => Owed::Queued,
                why if why.worth_repeating() => {
                    let attempts = match owed {
                        Owed::Missed { attempts, .. } => attempts.saturating_add(1),
                        _ => 1,
                    };
                    Owed::Missed {
                        attempts,
                        next: now.plus(backoff_millis(attempts)),
                        why: *why,
                    }
                }
                // A configuration disagreement or an envelope this
                // route can never carry. Repeating it on the congestion
                // schedule would be a busy loop with a known answer, so
                // it waits for the only thing that could change it.
                why => Owed::Stalled(*why),
            };
        }
        // Owing nobody is the other way the reservation ends, and the
        // common one: every voter took it, so the envelope is not
        // needed again and its bytes go back to the budget now rather
        // than when the command settles.
        if entry.dissemination.missing().is_empty() {
            self.undelivered_bytes = self
                .undelivered_bytes
                .saturating_sub(entry.dissemination.reserved);
            entry.dissemination.reserved = 0;
            entry.dissemination.envelope = None;
        }
    }

    /// The re-offers that are due, newest obligations last, bounded by
    /// `budget` destinations in total.
    ///
    /// Fair across commands rather than in command order: the cursor
    /// advances past whatever was served, so one congested destination's
    /// command cannot take the whole budget on every turn while the
    /// commands behind it wait.
    ///
    /// Bounded in what it *does*, never in what it *owes*. There is no
    /// attempt limit after which an accepted, unresolved command is
    /// forgotten: what the bounds cap is memory and retry pressure, and
    /// a command stops being re-offered when it settles or when the
    /// destination stops being one, not when it has been difficult
    /// enough times.
    pub fn due_offers(&mut self, now: MonotonicMillis, budget: usize) -> Vec<FanOut> {
        if budget == 0 || self.pending.is_empty() {
            return Vec::new();
        }
        let commands: Vec<CommandId> = self.pending.keys().copied().collect();
        let start = self.offer_cursor % commands.len();
        let mut out = Vec::new();
        let mut spent = 0usize;
        let mut examined = 0usize;
        for i in 0..commands.len() {
            if spent >= budget {
                break;
            }
            examined = i + 1;
            let command = commands[(start + i) % commands.len()];
            let Some(entry) = self.pending.get_mut(&command) else {
                continue;
            };
            let Some(envelope) = entry.dissemination.envelope.clone() else {
                continue;
            };
            let mut targets = Vec::new();
            for (replica, owed) in &mut entry.dissemination.owed {
                if spent >= budget {
                    break;
                }
                if let Owed::Missed { next, .. } = owed
                    && *next <= now
                {
                    targets.push(*replica);
                    spent += 1;
                    // Marked as offered before the offer is made. The
                    // answer will overwrite this, and until it does the
                    // destination must not be picked again -- an
                    // in-flight attempt is not a free slot.
                    *next = now.plus(OFFER_CEILING_MILLIS);
                }
            }
            if targets.is_empty() {
                continue;
            }
            self.trace.push(CollectorEvent::Reoffered {
                command: command_hex(&command),
                targets: targets.iter().map(replica_hex).collect(),
            });
            out.push(FanOut {
                command,
                retry_key: entry.retry_key,
                targets,
                frame: envelope,
            });
        }
        self.offer_cursor = start.wrapping_add(examined);
        out
    }

    /// When the next re-offer falls due, on the clock `due_offers` is
    /// given, or `None` when nothing is owed on the congestion schedule.
    ///
    /// For the runtime to wake on. The schedule lives here, but nothing
    /// here runs on its own: a runtime that waits only on its sockets
    /// would carry out a due re-offer only when unrelated traffic woke
    /// it, and on a quiet domain -- exactly when the destination that
    /// was busy is free again -- that is never. A destination stalled
    /// for a reason repeating cannot change is not on the schedule and
    /// is not counted.
    pub fn next_due(&self) -> Option<MonotonicMillis> {
        self.pending
            .values()
            .filter(|entry| entry.dissemination.envelope.is_some())
            .flat_map(|entry| entry.dissemination.owed.values())
            .filter_map(|owed| match owed {
                Owed::Missed { next, .. } => Some(*next),
                Owed::Queued | Owed::Stalled(_) => None,
            })
            .min()
    }

    /// Commands that still owe a destination an enqueue (diagnostic).
    pub fn undelivered(&self) -> usize {
        self.pending
            .values()
            .filter(|p| p.dissemination.outstanding())
            .count()
    }

    /// Submission envelope bytes held for commands that owe a
    /// destination (diagnostic).
    pub const fn undelivered_bytes(&self) -> usize {
        self.undelivered_bytes
    }

    /// Count protocol evidence from a bound voter identity.
    pub fn on_evidence(
        &mut self,
        from: PeerProvenance,
        message: ProtocolMessage,
    ) -> Result<Progress, EvidenceError> {
        let sender = from.from();
        let (kind, vote) = match message {
            ProtocolMessage::LeaderReply {
                ballot,
                command,
                seqnum,
                deps,
                path,
            } => (
                "leader-reply",
                Vote::Fast(FastAck {
                    replica: sender,
                    ballot,
                    command,
                    deps,
                    paths: Vec::new(),
                    path,
                    // A compact leader reply carries no evidence of the
                    // admission, so what this vote is counted under is
                    // what this collector submitted. That is the
                    // stricter reading: every voter acknowledgement
                    // afterwards is compared against the facts the
                    // frontend actually sent, not against whatever the
                    // first arrival happened to claim.
                    admission: self
                        .pending
                        .get(&command)
                        .map_or_else(|| admission_digest(None, 0), |p| p.admission),
                    seqnum: Some(seqnum),
                }),
            ),
            ProtocolMessage::FastAck(ack) => ("fast-ack", Vote::Fast(ack)),
            ProtocolMessage::SlowAck(ack) => ("slow-ack", Vote::Slow(ack)),
            _ => return Err(EvidenceError::NotEvidence),
        };
        let command = vote.command();
        let claimed = vote.replica();
        let result = if claimed != sender {
            Err(EvidenceError::SenderMismatch { sender, claimed })
        } else if let Some(entry) = self.pending.get_mut(&command) {
            entry.votes.add(vote).map_err(EvidenceError::Vote)
        } else if self.is_resolved(&command) {
            self.trace
                .push(evidence_event(&command, &sender, kind, true, None));
            return Ok(Progress::Settled);
        } else {
            Err(EvidenceError::UnknownCommand)
        };
        match result {
            Ok(()) => {
                self.trace
                    .push(evidence_event(&command, &sender, kind, true, None));
                Ok(self.try_release(command))
            }
            Err(e) => {
                self.trace.push(evidence_event(
                    &command,
                    &sender,
                    kind,
                    false,
                    Some(format!("{e:?}")),
                ));
                Err(e)
            }
        }
    }

    /// Take the leader's release-gate result.
    pub fn on_release(
        &mut self,
        from: PeerProvenance,
        released: ReleasedResult,
    ) -> Result<Progress, EvidenceError> {
        let sender = from.from();
        let command = released.established().command();
        let result = self.check_release(sender, &released);
        match result {
            Ok(settled) => {
                self.trace
                    .push(evidence_event(&command, &sender, "release", true, None));
                if settled {
                    return Ok(Progress::Settled);
                }
                Ok(self.try_release(command))
            }
            Err(e) => {
                self.trace.push(evidence_event(
                    &command,
                    &sender,
                    "release",
                    false,
                    Some(format!("{e:?}")),
                ));
                Err(e)
            }
        }
    }

    /// `Ok(true)` when the command was already released.
    fn check_release(
        &mut self,
        sender: ReplicaId,
        released: &ReleasedResult,
    ) -> Result<bool, EvidenceError> {
        let quorum = &self.config.quorum;
        if sender != quorum.leader() {
            return Err(EvidenceError::NotLeader { sender });
        }
        let established = released.established();
        if established.epoch() != quorum.epoch() {
            return Err(EvidenceError::WrongEpoch);
        }
        if established.ballot() != quorum.ballot() {
            return Err(EvidenceError::WrongBallot);
        }
        let command = established.command();
        let Some(entry) = self.pending.get_mut(&command) else {
            return if self.is_resolved(&command) {
                Ok(true)
            } else {
                Err(EvidenceError::UnknownCommand)
            };
        };
        if let Some(previous) = &entry.released {
            // A final release may follow a speculative one; anything else
            // that differs is a mismatch.
            if previous.established().result_digest() != established.result_digest()
                || previous.established().position() != established.position()
                || previous.response() != released.response()
            {
                return Err(EvidenceError::ReleaseMismatch);
            }
        }
        entry.released = Some(released.clone());
        Ok(false)
    }

    fn is_resolved(&self, command: &CommandId) -> bool {
        self.resolved.values().any(|(c, _)| c == command)
    }

    /// The release rule: both the collector's learning predicate and the
    /// leader's release must hold.
    fn try_release(&mut self, command: CommandId) -> Progress {
        let Some(entry) = self.pending.get(&command) else {
            return Progress::Settled;
        };
        let Some(learned) = entry.votes.learned() else {
            self.trace.push(CollectorEvent::Held {
                command: command_hex(&command),
                reason: "awaiting-votes".into(),
            });
            return Progress::Held(HoldReason::AwaitingVotes);
        };
        let Some(released) = entry.released.clone() else {
            self.trace.push(CollectorEvent::Held {
                command: command_hex(&command),
                reason: "awaiting-release".into(),
            });
            return Progress::Held(HoldReason::AwaitingRelease);
        };
        let entry = self.pending.remove(&command).expect("present");
        // Settled, so the dissemination obligation ends here. A command
        // may legitimately be established by the quorum it needed
        // before a saturated voter ever took its submission, and
        // holding this collector's capacity open for an unreachable
        // minority after the answer exists would be paying for a
        // delivery nobody is waiting on. What that minority missed is
        // the replication and recovery path's to close, which is what
        // it is for; what is owed here is to say so rather than let it
        // pass unrecorded.
        self.undelivered_bytes = self
            .undelivered_bytes
            .saturating_sub(entry.dissemination.reserved);
        let missed = entry.dissemination.missing();
        if !missed.is_empty() {
            self.trace.push(CollectorEvent::Undisseminated {
                command: command_hex(&command),
                missed: missed.iter().map(replica_hex).collect(),
            });
        }
        let fast = matches!(learned, Learned::Fast { .. });
        let established = released.established();
        let response = self.answer_of(command, established.revision(), released.response());
        self.finish(command, entry, response, released.speculative(), fast)
    }

    /// The response a caller is handed for a result, bounded by what
    /// can be delivered.
    ///
    /// Bounded by the API frame class, not by the storage result bound:
    /// a result between the two is real but undeliverable, and saying so
    /// is an answer while silence is not.
    fn answer_of(
        &self,
        command: CommandId,
        revision: Option<KvRevision>,
        result: &[u8],
    ) -> ResponseV1 {
        let deliverable = result.len() <= MAX_DELIVERABLE_RESULT_BYTES;
        match BoundedBytes::new(result.to_vec()) {
            Ok(result) if deliverable => ResponseV1 {
                command_id: command,
                outcome: OutcomeV1::Ok { revision, result },
            },
            _ => codes::error_response(command, codes::RESULT_TOO_LARGE, "result bound"),
        }
    }

    /// Retire a pending entry with its answer: retain the outcome for
    /// resolution, record the release, hand back what the caller gets.
    fn finish(
        &mut self,
        command: CommandId,
        entry: Pending,
        response: ResponseV1,
        speculative: bool,
        fast: bool,
    ) -> Progress {
        let voters: Vec<String> = entry.votes.voted().iter().map(replica_hex).collect();
        let revision = match &response.outcome {
            OutcomeV1::Ok { revision, .. } => revision.map(|r| r.get()),
            _ => None,
        };
        self.retain(entry.retry_key, command, response.clone());
        self.trace.push(CollectorEvent::Released {
            command: command_hex(&command),
            speculative,
            fast,
            voters,
            revision,
            delivered: entry.attached,
        });
        Progress::Released(Release {
            retry_key: entry.retry_key,
            command,
            session: entry.session,
            response,
            speculative,
            fast,
            attached: entry.attached,
        })
    }

    /// Pending commands that hold half of what a release needs: the
    /// learning predicate without the leader's release, or the release
    /// without the predicate (task-c02).
    ///
    /// A fresh command has neither and a releasable one has both. These
    /// are the ones something arrived for and something else did not,
    /// which is the shape a lost frontend delivery leaves behind, and the
    /// only shape the durable record is consulted for.
    pub fn half_established(&self) -> Vec<(CommandId, RetryKey)> {
        self.pending
            .iter()
            .filter(|(_, e)| e.votes.learned().is_some() != e.released.is_some())
            .map(|(c, e)| (*c, e.retry_key))
            .collect()
    }

    /// Settle `command` from the durable record its execution left
    /// behind on this node (task-c02).
    ///
    /// The record is not evidence and is not a release. It is this
    /// node's own committed state -- the same thing that answers a
    /// caller's retry before anything is submitted -- and it is trusted
    /// here only to complete what the collector already half holds. When
    /// the release is held, the record must agree with it on the digest
    /// and the bytes, and then it confirms what the missing votes would
    /// have: the command executed. When the learning predicate holds, the
    /// record supplies the response the missing release would have
    /// carried. With neither, the record is not used, deliberately: a
    /// collector that answered from local state alone would be a
    /// different component under a different contract.
    ///
    /// Never counted as a vote, never `speculative`, and never fast: what
    /// this collector learned is recorded as whatever it actually
    /// counted, and the trace says the record completed it.
    pub fn settle_from_record(
        &mut self,
        command: CommandId,
        result_digest: coord_types::identity::Digest32,
        revision: Option<KvRevision>,
        response: &[u8],
    ) -> Result<Progress, SettleError> {
        let Some(entry) = self.pending.get(&command) else {
            return if self.is_resolved(&command) {
                Ok(Progress::Settled)
            } else {
                Err(SettleError::NotPending)
            };
        };
        let learned = entry.votes.learned();
        let fast = matches!(learned, Some(Learned::Fast { .. }));
        let corroborated = match (&entry.released, learned.is_some()) {
            (Some(released), _) => {
                if released.established().result_digest() != result_digest
                    || released.response() != response
                {
                    return Err(SettleError::Mismatch);
                }
                "release"
            }
            (None, true) => "votes",
            (None, false) => return Err(SettleError::Uncorroborated),
        };
        let entry = self.pending.remove(&command).expect("present");
        self.trace.push(CollectorEvent::SettledFromRecord {
            command: command_hex(&command),
            corroborated: corroborated.into(),
        });
        let response = self.answer_of(command, revision, response);
        Ok(self.finish(command, entry, response, false, fast))
    }

    fn retain(&mut self, key: RetryKey, command: CommandId, response: ResponseV1) {
        self.resolved.insert(key, (command, response));
        self.resolved_order.push_back(key);
        while self.resolved_order.len() > self.config.max_resolved {
            let old = self.resolved_order.pop_front().expect("non-empty");
            self.resolved.remove(&old);
            self.bindings.remove(&old);
        }
    }

    /// The caller of `retry_key` went away. The command keeps collecting;
    /// returns it when it was pending.
    pub fn cancel(&mut self, retry_key: &RetryKey) -> Option<CommandId> {
        let command = *self.bindings.get(retry_key)?;
        let entry = self.pending.get_mut(&command)?;
        entry.attached = false;
        self.trace.push(CollectorEvent::Cancelled {
            command: command_hex(&command),
        });
        Some(command)
    }

    /// Resolve an identity.
    pub fn resolve(&mut self, request: &ResolveRequestV1) -> Resolution {
        let sequence = request.retry_key.request_sequence.get();
        let resolution = match self.bindings.get(&request.retry_key) {
            None => Resolution::Unknown,
            Some(bound) if *bound != request.command_id => Resolution::Conflict { bound: *bound },
            Some(bound) => match self.resolved.get(&request.retry_key) {
                Some((_, response)) => Resolution::Outcome(response.clone()),
                None if self.pending.contains_key(bound) => Resolution::Pending,
                None => Resolution::Unknown,
            },
        };
        self.trace.push(CollectorEvent::Resolved {
            sequence,
            result: match &resolution {
                Resolution::Outcome(_) => "outcome",
                Resolution::Pending => "pending",
                Resolution::Conflict { .. } => "conflict",
                Resolution::Unknown => "unknown",
            }
            .into(),
        });
        resolution
    }

    /// Commands whose client deadline passed at `now`; each is reported
    /// once and stays pending for resolution.
    pub fn expire(&mut self, now: MonotonicMillis) -> Vec<Expired> {
        let mut out = Vec::new();
        for (command, entry) in &mut self.pending {
            if entry.timed_out {
                continue;
            }
            if let Some(deadline) = entry.deadline
                && now >= deadline
            {
                entry.timed_out = true;
                out.push(Expired {
                    retry_key: entry.retry_key,
                    command: *command,
                    session: entry.session,
                });
            }
        }
        for e in &out {
            self.trace.push(CollectorEvent::TimedOut {
                command: command_hex(&e.command),
            });
        }
        out
    }

    /// The ballot changed (recovery): evidence and releases of the old
    /// ballot are void; pending commands collect afresh under the new
    /// configuration. Full epoch integration is task-m02.
    pub fn reconfigure(&mut self, quorum: BallotConfiguration) {
        let reset = self.pending.len();
        let voters: Vec<ReplicaId> = quorum.voters().iter().copied().collect();
        let mut released = 0usize;
        for (command, entry) in &mut self.pending {
            entry.votes = VoteSet::new(quorum.clone(), *command);
            entry.released = None;
            // Delivery follows the committed configuration, the same as
            // evidence does. A replica that is no longer a voter is no
            // longer a destination and stops being owed anything; one
            // that has become a voter is owed the submission and is due
            // it now. Saturation never got a say in either: this is the
            // configuration changing, not a busy queue being
            // reinterpreted as a membership change.
            entry.dissemination.owed.retain(|r, _| voters.contains(r));
            let held = entry.dissemination.envelope.is_some();
            for voter in &voters {
                let owed = entry
                    .dissemination
                    .owed
                    .entry(*voter)
                    .or_insert(Owed::Stalled(OfferOutcome::Unreachable));
                // A destination held off the congestion schedule was
                // waiting for exactly this. One that has taken the
                // frame keeps that; re-offering a voter that already
                // has it would be work with no question behind it.
                //
                // And only while there is something to offer. A command
                // every voter had taken released its envelope, so a
                // voter that joins afterwards has genuinely missed this
                // submission and cannot be handed it: saying it is due
                // would leave it owed for ever with nothing to send.
                // It stays recorded, and settlement reports it.
                if matches!(owed, Owed::Stalled(_)) && held {
                    *owed = Owed::Missed {
                        attempts: 1,
                        next: MonotonicMillis::ZERO,
                        why: OfferOutcome::Unreachable,
                    };
                }
            }
            // A command that owes nothing again gives its bytes back,
            // and one that owes something after the reconciliation
            // keeps holding them: the reservation tracks the
            // obligation, never the other way round.
            if entry.dissemination.missing().is_empty() {
                released += entry.dissemination.reserved;
                entry.dissemination.reserved = 0;
                entry.dissemination.envelope = None;
            }
        }
        self.undelivered_bytes = self.undelivered_bytes.saturating_sub(released);
        self.trace.push(CollectorEvent::Reconfigured {
            ballot: quorum.ballot().number,
            reset,
        });
        self.config.quorum = quorum;
    }
}

/// How long the `attempts`-th repeat waits, in milliseconds.
///
/// Exponential from the floor to the ceiling. The point is not to find
/// the fastest schedule -- it is that a destination which keeps
/// refusing costs less and less, so sustained congestion cannot be
/// turned into sustained retry traffic by the retries themselves.
const fn backoff_millis(attempts: u32) -> u64 {
    let shift = if attempts > 8 { 8 } else { attempts - 1 };
    let wait = OFFER_FLOOR_MILLIS << shift;
    if wait > OFFER_CEILING_MILLIS {
        OFFER_CEILING_MILLIS
    } else {
        wait
    }
}

/// The reading at which a request presented at `now` with `deadline_ms`
/// expires; none for a request that named no deadline.
fn deadline(now: MonotonicMillis, deadline_ms: u32) -> Option<MonotonicMillis> {
    (deadline_ms > 0).then(|| now.plus(u64::from(deadline_ms)))
}

fn evidence_event(
    command: &CommandId,
    from: &ReplicaId,
    kind: &str,
    accepted: bool,
    reason: Option<String>,
) -> CollectorEvent {
    CollectorEvent::Evidence {
        command: command_hex(command),
        from: replica_hex(from),
        kind: kind.into(),
        accepted,
        reason,
    }
}
