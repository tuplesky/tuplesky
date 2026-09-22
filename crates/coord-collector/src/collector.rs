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
//! * **Bounds**: pending commands per domain are capped and unresolved
//!   entries are never evicted to make room; resolved outcomes are kept
//!   in a bounded window.

use std::collections::{BTreeMap, VecDeque};

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

use crate::codes;

use crate::trace::{CollectorEvent, command_hex, replica_hex};
use crate::wire::{SubmitV1, submit_frame};

/// The largest result a client can actually be handed: an API-class
/// frame, less room for the response's own fields and the frame header.
const MAX_DELIVERABLE_RESULT_BYTES: usize =
    (coord_types::wire_v1::KindRange::Api.max_frame_length() as usize) - 64 * 1024;

/// Configuration of one domain's collector.
#[derive(Clone, Debug)]
pub struct CollectorConfig {
    /// The current ballot configuration (voters, leader, fast set).
    pub quorum: BallotConfiguration,
    /// Commands pending at once (per domain).
    pub max_pending: usize,
    /// Resolved outcomes retained for retries and resolution.
    pub max_resolved: usize,
}

/// The parallel fan-out of one submission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FanOut {
    /// Command.
    pub command: CommandId,
    /// Retry key.
    pub retry_key: RetryKey,
    /// Every voter, all at once.
    pub targets: Vec<ReplicaId>,
    /// The `Submit` frame.
    pub frame: Vec<u8>,
}

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
    deadline: Option<u64>,
    timed_out: bool,
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

    /// Submit an admitted request at `now_ticks`.
    pub fn submit(
        &mut self,
        now_ticks: u64,
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
                entry.deadline = deadline(now_ticks, request.deadline_ms);
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
        let targets: Vec<ReplicaId> = self.config.quorum.voters().iter().copied().collect();
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
                deadline: deadline(now_ticks, request.deadline_ms),
                timed_out: false,
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

    /// Commands whose client deadline passed at `now_ticks`; each is
    /// reported once and stays pending for resolution.
    pub fn expire(&mut self, now_ticks: u64) -> Vec<Expired> {
        let mut out = Vec::new();
        for (command, entry) in &mut self.pending {
            if entry.timed_out {
                continue;
            }
            if let Some(deadline) = entry.deadline
                && now_ticks >= deadline
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
        for (command, entry) in &mut self.pending {
            entry.votes = VoteSet::new(quorum.clone(), *command);
            entry.released = None;
        }
        self.trace.push(CollectorEvent::Reconfigured {
            ballot: quorum.ballot().number,
            reset,
        });
        self.config.quorum = quorum;
    }
}

fn deadline(now_ticks: u64, deadline_ms: u32) -> Option<u64> {
    (deadline_ms > 0).then(|| now_ticks.saturating_add(u64::from(deadline_ms)))
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
