//! The leader read barrier, on the leader (task-d50; design Section 6.3).
//!
//! A frontend sends a current read to the leader of the ballot it
//! follows instead of ordering it. The leader takes the read's index
//! (its ballot's next sequence number) when the read arrives, confirms
//! its ballot with a round started after that, waits until everything
//! it proposed below the index has executed and its projection has
//! materialized through what it had executed then, and plans the read
//! over one snapshot of that state. Anything else is a refusal, and a
//! refused read is ordered by its frontend instead: the barrier never
//! answers with anything weaker.
//!
//! [`ReadBarrier`] is the bookkeeping, with no machine and no store in
//! it, so every rule it applies is tested here on its own. The voter
//! drives it ([`crate::voter::Voter::on_read`] and
//! [`crate::voter::Voter::pump_reads`]), and [`evaluate`] is the plan.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use coord_collector::{MonotonicMillis, ReadAnswerV1, ReadOutcomeV1, ReadRefusal, ReadV1};
use coord_state::{PlanLimits, plan};
use coord_storage::retry::{self, Admission, RetryBinding};
use coord_storage::{GatedView, ViewBudget, build_authorized_view};
use coord_store_api::engine::OrderedRead;
use coord_types::CommandId;
use coord_types::ids::{Ballot, ExecutionPosition, ReplicaId};
use coord_types::logical_v1::LogicalRequest;

use crate::voter::Origin;

/// How many reads a leader holds at once. Past it a read is refused and
/// ordered instead, which costs it latency and nothing else.
pub const READS_HELD: usize = 4096;

/// How long a read waits at the leader, in milliseconds, before it is
/// refused and ordered instead: a round that has not confirmed by then
/// is not going to, or the leader is too far behind its own proposals
/// to answer sooner than the ordered path would.
pub const READ_WAIT_MILLIS: u64 = 1_000;

/// How long a confirmation round in flight keeps the next one from
/// starting, in milliseconds (task-d58). Past it the round is presumed
/// lost for that purpose only: it still confirms if its answers come.
/// Well under [`READ_WAIT_MILLIS`], so one lost answer costs a read this
/// much, not its whole wait.
pub const ROUND_IN_FLIGHT_MILLIS: u64 = 100;

pub use coord_collector::servable;

/// What the leader knows when a read arrives or is pumped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Leading {
    /// The ballot it leads.
    pub ballot: Ballot,
    /// Its read index now ([`coord_consensus::Leader::read_index`]).
    pub index: Option<u64>,
    /// How many confirmations, its own included, a round needs.
    pub slow_size: usize,
}

/// A read held at the leader.
#[derive(Clone, Debug)]
pub struct Held {
    /// Where its answer goes.
    pub origin: Origin,
    /// The read as the frontend sent it.
    pub read: ReadV1,
    /// The request it carries, decoded.
    pub logical: LogicalRequest,
    /// Its read index.
    index: u64,
    /// The first round that started after it arrived.
    after_round: u64,
    /// When it arrived.
    arrived: MonotonicMillis,
    /// When a round started after it arrived was first seen confirmed.
    confirmed_at: Option<MonotonicMillis>,
    /// When everything below its index had also executed.
    due_at: Option<MonotonicMillis>,
    /// The position the snapshot it is planned over must reach, once
    /// everything below its index has executed.
    pub required: Option<ExecutionPosition>,
}

/// A confirmation round in flight.
#[derive(Clone, Debug)]
struct Round {
    ballot: Ballot,
    started: MonotonicMillis,
    confirmed_by: BTreeSet<ReplicaId>,
}

/// What [`ReadBarrier`] has done since boot (diagnostic).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReadCounts {
    /// Reads answered by the barrier.
    pub served: u64,
    /// Reads refused, to be ordered by their frontends instead.
    pub refused: u64,
    /// Confirmation rounds started.
    pub rounds: u64,
    /// Confirmation rounds that confirmed.
    pub confirmed: u64,
    /// Over the reads served: milliseconds from arrival until a round
    /// started after it was seen confirmed, summed.
    pub waited_confirm_ms: u64,
    /// Over the reads served: milliseconds from arrival until, confirmed,
    /// everything below its index had executed, summed.
    pub waited_index_ms: u64,
    /// Over the reads served: milliseconds from arrival until served,
    /// summed.
    pub waited_ms: u64,
    /// Snapshots pinned to answer reads (task-d58).
    pub snapshots: u64,
    /// Due reads held again because their snapshot was behind them.
    pub behind: u64,
}

/// The reads a leader holds, and the rounds that confirm them.
#[derive(Debug, Default)]
pub struct ReadBarrier {
    held: VecDeque<Held>,
    /// The number the next round takes. Rounds are numbered across
    /// ballots, so an answer to a round of another ballot never matches.
    next_round: u64,
    rounds: BTreeMap<u64, Round>,
    /// The highest round confirmed, and its ballot.
    confirmed: Option<(Ballot, u64)>,
    /// What came of it.
    pub counts: ReadCounts,
}

impl ReadBarrier {
    /// An empty barrier.
    pub fn new() -> Self {
        ReadBarrier::default()
    }

    /// Reads held.
    pub fn held(&self) -> usize {
        self.held.len()
    }

    /// Take `read` from `origin`, or refuse it at once (boxed: the
    /// refusal is the rare case).
    ///
    /// `leading` is `None` when this voter does not lead a ballot.
    pub fn admit(
        &mut self,
        origin: Origin,
        read: ReadV1,
        leading: Option<Leading>,
        now: MonotonicMillis,
    ) -> Result<(), Box<(Origin, ReadAnswerV1)>> {
        let retry_key = read.request.retry_key;
        let refuse = |this: &mut Self, ballot: Ballot, reason| {
            this.counts.refused += 1;
            Err(Box::new((origin, refusal(retry_key, ballot, reason))))
        };
        let Some(leading) = leading.filter(|l| l.ballot == read.ballot) else {
            let ballot = leading.map_or(read.ballot, |l| l.ballot);
            return refuse(self, ballot, ReadRefusal::NotLeading);
        };
        let Some(logical) = read.request.logical().ok().filter(servable) else {
            return refuse(self, leading.ballot, ReadRefusal::NotServable);
        };
        let Some(index) = leading.index else {
            return refuse(self, leading.ballot, ReadRefusal::NothingProposed);
        };
        if self.held.len() >= READS_HELD {
            return refuse(self, leading.ballot, ReadRefusal::Busy);
        }
        self.held.push_back(Held {
            origin,
            read,
            logical,
            index,
            after_round: self.next_round,
            arrived: now,
            confirmed_at: None,
            due_at: None,
            required: None,
        });
        Ok(())
    }

    /// The round to start now, if a held read arrived after the last one
    /// started and no round of `ballot` is in flight. Its number is what
    /// the caller sends in `ReadConfirm`.
    ///
    /// One round at a time (task-d58). A read covered only by a round
    /// started after it arrived, a round per arrival was a round per
    /// read: four requests and four answers at five voters, and a third
    /// of them superseded before they confirmed. Waiting for the round in
    /// flight lets every read that arrives meanwhile share the next one,
    /// and the wait overlaps the read's wait for its index to execute,
    /// which is the longer. A round in flight past
    /// [`ROUND_IN_FLIGHT_MILLIS`] does not hold the next one back.
    ///
    /// Where the leader alone is a slow quorum (`slow_size` one, a
    /// single voter), the round confirms as it starts: nothing else's
    /// promise is needed, and none will come.
    pub fn round_to_start(
        &mut self,
        ballot: Ballot,
        slow_size: usize,
        now: MonotonicMillis,
    ) -> Option<u64> {
        let uncovered = self
            .held
            .iter()
            .any(|h| h.read.ballot == ballot && h.after_round >= self.next_round);
        if !uncovered {
            return None;
        }
        let in_flight = self.rounds.values().any(|r| {
            r.ballot == ballot && now.get().saturating_sub(r.started.get()) < ROUND_IN_FLIGHT_MILLIS
        });
        if in_flight {
            return None;
        }
        let round = self.next_round;
        self.next_round += 1;
        self.rounds.insert(
            round,
            Round {
                ballot,
                started: now,
                confirmed_by: BTreeSet::new(),
            },
        );
        self.counts.rounds += 1;
        // The negative control (`skip-read-confirmation`, never in a
        // shipped build) confirms every round as it starts, so that the
        // register check can show what the round is for.
        if slow_size <= 1 || cfg!(feature = "skip-read-confirmation") {
            self.confirmed = Some((ballot, round));
            self.counts.confirmed += 1;
            self.rounds.remove(&round);
        }
        Some(round)
    }

    /// `from` confirmed `round` at `ballot`. The round confirms once
    /// `slow_size` voters have, the leader counted as one of them.
    pub fn on_confirmed(&mut self, from: ReplicaId, ballot: Ballot, round: u64, slow_size: usize) {
        let Some(r) = self.rounds.get_mut(&round) else {
            return;
        };
        if r.ballot != ballot || from == ballot.leader {
            return;
        }
        r.confirmed_by.insert(from);
        if r.confirmed_by.len() + 1 < slow_size {
            return;
        }
        if self
            .confirmed
            .is_none_or(|(b, at)| b != ballot || at < round)
        {
            self.confirmed = Some((ballot, round));
            self.counts.confirmed += 1;
        }
        // A later round confirming covers every read an earlier one
        // would have.
        self.rounds.retain(|n, _| *n > round);
    }

    /// Whether `held` may be answered from `ballot`'s confirmation.
    fn covered(&self, held: &Held, ballot: Ballot) -> bool {
        self.confirmed
            .is_some_and(|(b, round)| b == ballot && round >= held.after_round)
    }

    /// The reads that may be planned now, and the refusals due.
    ///
    /// A read is due once a round started after it arrived has
    /// confirmed and everything the leader proposed below its index has
    /// executed (`executed_below`). The position it must be planned at
    /// is fixed the first time that holds: what the leader had executed
    /// then. A read past [`READ_WAIT_MILLIS`] is refused, and so is
    /// every read once the leader no longer leads its ballot.
    pub fn take_due(
        &mut self,
        leading: Option<Leading>,
        now: MonotonicMillis,
        executed_below: impl Fn(u64) -> bool,
        executed_through: ExecutionPosition,
    ) -> (Vec<Held>, Vec<(Origin, ReadAnswerV1)>) {
        let mut due = Vec::new();
        let mut refused = Vec::new();
        let mut kept = VecDeque::with_capacity(self.held.len());
        for mut held in core::mem::take(&mut self.held) {
            let ballot = held.read.ballot;
            let reason = match leading {
                Some(l) if l.ballot == ballot => None,
                _ => Some(ReadRefusal::NotLeading),
            }
            .or_else(|| {
                (now.get().saturating_sub(held.arrived.get()) > READ_WAIT_MILLIS)
                    .then_some(ReadRefusal::Expired)
            });
            if let Some(reason) = reason {
                self.counts.refused += 1;
                refused.push((
                    held.origin,
                    refusal(held.read.request.retry_key, ballot, reason),
                ));
                continue;
            }
            if held.confirmed_at.is_none() && self.covered(&held, ballot) {
                held.confirmed_at = Some(now);
            }
            if held.required.is_none() && held.confirmed_at.is_some() && executed_below(held.index)
            {
                held.required = Some(executed_through);
                held.due_at = Some(now);
            }
            if held.required.is_some() {
                due.push(held);
            } else {
                kept.push_back(held);
            }
        }
        self.held = kept;
        // A round older than any read's wait is not going to answer one.
        self.rounds.retain(|_, r| {
            now.get().saturating_sub(r.started.get()) <= READ_WAIT_MILLIS
                && leading.is_some_and(|l| l.ballot == r.ballot)
        });
        (due, refused)
    }

    /// Hold `held` again: its snapshot has not reached its position yet.
    pub fn hold_again(&mut self, held: Held) {
        self.held.push_front(held);
    }

    /// Count a snapshot pinned to answer the reads due in one pump, and
    /// how many of them it was behind.
    pub fn pinned(&mut self, behind: usize) {
        self.counts.snapshots += 1;
        self.counts.behind += behind as u64;
    }

    /// Count `held` served at `now`, and how long it waited.
    pub fn served(&mut self, held: &Held, now: MonotonicMillis) {
        let since = |at: Option<MonotonicMillis>| {
            at.map_or(0, |at| at.get().saturating_sub(held.arrived.get()))
        };
        self.counts.served += 1;
        self.counts.waited_confirm_ms += since(held.confirmed_at);
        self.counts.waited_index_ms += since(held.due_at);
        self.counts.waited_ms += since(Some(now));
    }

    /// Count a read refused after it was due.
    pub fn refused(&mut self) {
        self.counts.refused += 1;
    }
}

/// A refusal answer.
pub fn refusal(
    retry_key: coord_types::identity::RetryKey,
    ballot: Ballot,
    reason: ReadRefusal,
) -> ReadAnswerV1 {
    ReadAnswerV1 {
        retry_key,
        ballot,
        outcome: ReadOutcomeV1::Refused { reason },
    }
}

/// What planning a held read came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Evaluated {
    /// The encoded response.
    Served(Vec<u8>),
    /// The store's readable state has not reached the read's position.
    NotYet,
    /// The ordered path decides this one: the request is not new work
    /// inside its window, its view cannot be built, or its plan is not a
    /// plain range result.
    NotServable,
}

/// Plan `held` over `gated`, a snapshot at or past its required
/// position.
///
/// The snapshot is the caller's, so that every read due in one pump is
/// planned over one (task-d58): each is due because everything below its
/// index executed, and a snapshot taken after that covers all of them.
///
/// The same view, planner and encoding the ordered path executes a read
/// with (`Applier::apply_bound`), so a served answer carries the bytes
/// the ordered path would have recorded at that state. What the ordered
/// path would not execute as new work -- a retry, a request outside its
/// session's window, a session that is unknown or retired -- is left to
/// it, and so is a request past half its session's window, which the
/// ordered path must retire behind, and so is any plan that is not a range result: a refusal is an
/// outcome the ordered path records, and recording it is not this
/// path's to skip.
pub fn evaluate<V: OrderedRead>(gated: &GatedView<V>, held: &Held) -> Evaluated {
    let Some(required) = held.required else {
        return Evaluated::NotYet;
    };
    if gated.meta().frontier.execution_position < required {
        return Evaluated::NotYet;
    }
    let request = &held.read.request;
    let logical = &held.logical;
    let Ok(command_id) = CommandId::derive(&request.retry_key, logical) else {
        return Evaluated::NotServable;
    };
    let Ok(retires) = retry::retirement(gated.view(), &request.retry_key, request.ack_through)
    else {
        return Evaluated::NotServable;
    };
    let binding = RetryBinding {
        retry_key: request.retry_key,
        command_id,
        retires,
    };
    if !matches!(
        retry::admit(gated.view(), &binding, |_| false),
        Ok(Admission::New)
    ) {
        return Evaluated::NotServable;
    }
    // A read served here records nothing, so it retires nothing either:
    // only an ordered command moves its client's floor, by at most
    // `MAX_RETIRE_PER_COMMAND` sequences. A session that read only
    // through this path would walk its sequence out of its window, and
    // past it even its ordered requests are refused without retiring
    // anything. So past half the window from the floor the read is
    // ordered instead, and pulls the floor up behind it.
    let sequence = request.retry_key.request_sequence.get();
    match retry::session_floor(gated.view(), &request.retry_key) {
        Ok(Some(floor)) if sequence - floor.floor.get() <= u64::from(floor.width / 2) => {}
        _ => return Evaluated::NotServable,
    }
    let session = request.retry_key.session_id;
    let Ok(view) = build_authorized_view(
        gated,
        logical.namespace,
        &session,
        logical,
        ViewBudget::SCHEMA,
    ) else {
        return Evaluated::NotServable;
    };
    let Ok(planned) = plan(logical, &view, &PlanLimits::default()) else {
        return Evaluated::NotServable;
    };
    if !matches!(planned.response.outcome, coord_state::Outcome::Range { .. }) {
        return Evaluated::NotServable;
    }
    match postcard::to_allocvec(&planned.response) {
        Ok(bytes) => Evaluated::Served(bytes),
        Err(_) => Evaluated::NotServable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coord_types::RetryKey;
    use coord_types::ids::{
        ClientInstanceId, ClusterId, ConfigurationEpoch, DomainId, NamespaceId, RequestSequence,
        SessionId,
    };
    use coord_types::logical_v1::{CanonicalOperation, KeyRange, PutOp, RangeOp};
    use coord_types::wire_v1::RequestV1;

    fn ballot(number: u64) -> Ballot {
        Ballot {
            epoch: ConfigurationEpoch::new(1).unwrap(),
            number,
            leader: ReplicaId([1; 16]),
        }
    }

    fn read(seq: u64, b: Ballot, operation: CanonicalOperation) -> ReadV1 {
        let key = RetryKey {
            cluster_id: ClusterId([1; 16]),
            domain_id: DomainId([2; 16]),
            session_id: SessionId([3; 16]),
            client_instance_id: ClientInstanceId([4; 16]),
            request_sequence: RequestSequence::new(seq).unwrap(),
        };
        let logical = LogicalRequest::new(NamespaceId([5; 16]), operation);
        ReadV1 {
            ballot: b,
            request: RequestV1::new(key, &logical, 0, 0).unwrap(),
        }
    }

    fn range() -> CanonicalOperation {
        CanonicalOperation::Range(RangeOp {
            range: KeyRange {
                key: b"k".to_vec(),
                range_end: None,
            },
            revision: None,
            limit: 1,
            keys_only: false,
            count_only: false,
        })
    }

    fn leading(b: Ballot, index: u64) -> Option<Leading> {
        Some(Leading {
            ballot: b,
            index: Some(index),
            slow_size: 2,
        })
    }

    const T0: MonotonicMillis = MonotonicMillis::ZERO;
    fn at(n: u64) -> ExecutionPosition {
        ExecutionPosition::new(n).unwrap()
    }

    fn refusal_of(answer: &ReadAnswerV1) -> Option<ReadRefusal> {
        match answer.outcome {
            ReadOutcomeV1::Refused { reason } => Some(reason),
            ReadOutcomeV1::Served { .. } => None,
        }
    }

    #[test]
    fn only_a_current_range_is_served() {
        assert!(servable(&LogicalRequest::new(
            NamespaceId([5; 16]),
            range()
        )));
        let CanonicalOperation::Range(mut historical) = range() else {
            unreachable!()
        };
        historical.revision = Some(coord_types::ids::KvRevision::new(3).unwrap());
        assert!(!servable(&LogicalRequest::new(
            NamespaceId([5; 16]),
            CanonicalOperation::Range(historical)
        )));
        let put = CanonicalOperation::Put(PutOp {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            lease: None,
            prev_kv: false,
        });
        assert!(!servable(&LogicalRequest::new(NamespaceId([5; 16]), put)));
    }

    #[test]
    fn a_read_is_refused_by_a_voter_that_does_not_lead_its_ballot() {
        let mut barrier = ReadBarrier::new();
        let (_, answer) = *barrier
            .admit(Origin::Local, read(1, ballot(1), range()), None, T0)
            .unwrap_err();
        assert_eq!(refusal_of(&answer), Some(ReadRefusal::NotLeading));
        let (_, answer) = *barrier
            .admit(
                Origin::Local,
                read(1, ballot(1), range()),
                leading(ballot(2), 3),
                T0,
            )
            .unwrap_err();
        assert_eq!(refusal_of(&answer), Some(ReadRefusal::NotLeading));
    }

    #[test]
    fn a_leader_that_proposed_nothing_in_its_ballot_refuses() {
        let mut barrier = ReadBarrier::new();
        let mut l = leading(ballot(1), 0).unwrap();
        l.index = None;
        let (_, answer) = *barrier
            .admit(Origin::Local, read(1, ballot(1), range()), Some(l), T0)
            .unwrap_err();
        assert_eq!(refusal_of(&answer), Some(ReadRefusal::NothingProposed));
    }

    /// The round that answers a read is one started after it arrived: a
    /// confirmation of a round already in flight when it came does not.
    #[test]
    fn a_read_waits_for_a_round_started_after_it_arrived() {
        let b = ballot(1);
        let mut barrier = ReadBarrier::new();
        barrier
            .admit(Origin::Local, read(1, b, range()), leading(b, 3), T0)
            .unwrap();
        let first = barrier.round_to_start(b, 2, T0).unwrap();
        // Nothing new arrived: no second round.
        assert_eq!(barrier.round_to_start(b, 2, T0), None);
        barrier
            .admit(Origin::Local, read(2, b, range()), leading(b, 4), T0)
            .unwrap();
        barrier.on_confirmed(ReplicaId([2; 16]), b, first, 2);
        let (due, refused) = barrier.take_due(leading(b, 4), T0, |_| true, at(5));
        assert!(refused.is_empty());
        assert_eq!(due.len(), 1, "the first read is covered, the second is not");
        assert_eq!(due[0].read.request.retry_key.request_sequence.get(), 1);
        let second = barrier.round_to_start(b, 2, T0).unwrap();
        assert!(second > first);
        barrier.on_confirmed(ReplicaId([3; 16]), b, second, 2);
        let (due, _) = barrier.take_due(leading(b, 4), T0, |_| true, at(5));
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].read.request.retry_key.request_sequence.get(), 2);
    }

    /// A single voter is its own slow quorum: its round confirms as it
    /// starts.
    #[test]
    fn a_single_voter_confirms_its_own_round() {
        let b = ballot(1);
        let mut barrier = ReadBarrier::new();
        barrier
            .admit(Origin::Local, read(1, b, range()), leading(b, 3), T0)
            .unwrap();
        barrier.round_to_start(b, 1, T0).unwrap();
        let (due, _) = barrier.take_due(leading(b, 3), T0, |_| true, at(5));
        assert_eq!(due.len(), 1);
    }

    #[test]
    fn a_round_confirms_only_with_a_slow_quorum_at_its_ballot() {
        let b = ballot(1);
        let mut barrier = ReadBarrier::new();
        barrier
            .admit(Origin::Local, read(1, b, range()), leading(b, 3), T0)
            .unwrap();
        let round = barrier.round_to_start(b, 2, T0).unwrap();
        // Five voters: the leader and two others.
        barrier.on_confirmed(ReplicaId([2; 16]), b, round, 3);
        // An answer for another ballot, and the leader's own, count for
        // nothing.
        barrier.on_confirmed(ReplicaId([3; 16]), ballot(2), round, 3);
        barrier.on_confirmed(b.leader, b, round, 3);
        let (due, _) = barrier.take_due(leading(b, 3), T0, |_| true, at(5));
        assert!(due.is_empty());
        barrier.on_confirmed(ReplicaId([3; 16]), b, round, 3);
        let (due, _) = barrier.take_due(leading(b, 3), T0, |_| true, at(5));
        assert_eq!(due.len(), 1);
    }

    /// A read is planned at what the leader had executed when everything
    /// below its index had: not before, and fixed from then on.
    #[test]
    fn a_read_waits_for_its_index_to_execute() {
        let b = ballot(1);
        let mut barrier = ReadBarrier::new();
        barrier
            .admit(Origin::Local, read(1, b, range()), leading(b, 7), T0)
            .unwrap();
        let round = barrier.round_to_start(b, 2, T0).unwrap();
        barrier.on_confirmed(ReplicaId([2; 16]), b, round, 2);
        let (due, _) = barrier.take_due(leading(b, 9), T0, |index| index <= 6, at(5));
        assert!(
            due.is_empty(),
            "a proposal below the index has not executed"
        );
        let (due, _) = barrier.take_due(leading(b, 9), T0, |index| index <= 7, at(8));
        assert_eq!(due[0].required, Some(at(8)));
        barrier.hold_again(due.into_iter().next().unwrap());
        let (due, _) = barrier.take_due(leading(b, 9), T0, |_| true, at(12));
        assert_eq!(due[0].required, Some(at(8)));
    }

    /// A served read's waits are counted from its arrival: until its
    /// round confirmed, until its index executed, and until it was served.
    #[test]
    fn a_served_read_counts_what_it_waited_for() {
        let b = ballot(1);
        let ms = |n: u64| T0.plus(n);
        let mut barrier = ReadBarrier::new();
        barrier
            .admit(Origin::Local, read(1, b, range()), leading(b, 7), T0)
            .unwrap();
        let round = barrier.round_to_start(b, 2, T0).unwrap();
        barrier.on_confirmed(ReplicaId([2; 16]), b, round, 2);
        let (due, _) = barrier.take_due(leading(b, 9), ms(2), |index| index <= 6, at(5));
        assert!(due.is_empty());
        let (due, _) = barrier.take_due(leading(b, 9), ms(7), |_| true, at(8));
        barrier.served(&due[0], ms(9));
        let counts = barrier.counts;
        assert_eq!(counts.served, 1);
        assert_eq!(counts.waited_confirm_ms, 2);
        assert_eq!(counts.waited_index_ms, 7);
        assert_eq!(counts.waited_ms, 9);
    }

    #[test]
    fn a_leader_that_stops_leading_refuses_what_it_holds() {
        let b = ballot(1);
        let mut barrier = ReadBarrier::new();
        barrier
            .admit(
                Origin::Connection(9),
                read(1, b, range()),
                leading(b, 3),
                T0,
            )
            .unwrap();
        let (due, refused) = barrier.take_due(leading(ballot(2), 1), T0, |_| true, at(5));
        assert!(due.is_empty());
        assert_eq!(refused.len(), 1);
        assert_eq!(refused[0].0, Origin::Connection(9));
        assert_eq!(refusal_of(&refused[0].1), Some(ReadRefusal::NotLeading));
        assert_eq!(barrier.held(), 0);
    }

    #[test]
    fn a_read_that_waits_too_long_is_refused() {
        let b = ballot(1);
        let mut barrier = ReadBarrier::new();
        barrier
            .admit(Origin::Local, read(1, b, range()), leading(b, 3), T0)
            .unwrap();
        let later = T0.plus(READ_WAIT_MILLIS + 1);
        let (due, refused) = barrier.take_due(leading(b, 3), later, |_| true, at(5));
        assert!(due.is_empty());
        assert_eq!(refusal_of(&refused[0].1), Some(ReadRefusal::Expired));
    }

    /// Reads that arrive while a round is in flight start none of their
    /// own; the next round, started once that one confirms, covers them
    /// all, and the round under way answers none of them (task-d58).
    #[test]
    fn reads_that_arrive_during_a_round_share_the_next() {
        let b = ballot(1);
        let mut barrier = ReadBarrier::new();
        barrier
            .admit(Origin::Local, read(1, b, range()), leading(b, 3), T0)
            .unwrap();
        let first = barrier.round_to_start(b, 2, T0).unwrap();
        for seq in 2..=4 {
            barrier
                .admit(Origin::Local, read(seq, b, range()), leading(b, 4), T0)
                .unwrap();
            assert_eq!(
                barrier.round_to_start(b, 2, T0),
                None,
                "one round in flight at a time"
            );
        }
        barrier.on_confirmed(ReplicaId([2; 16]), b, first, 2);
        let (due, _) = barrier.take_due(leading(b, 4), T0, |_| true, at(5));
        assert_eq!(
            due.len(),
            1,
            "the round under way answers only what preceded it"
        );
        assert_eq!(due[0].read.request.retry_key.request_sequence.get(), 1);
        let second = barrier.round_to_start(b, 2, T0).unwrap();
        barrier.on_confirmed(ReplicaId([3; 16]), b, second, 2);
        let (due, _) = barrier.take_due(leading(b, 4), T0, |_| true, at(5));
        assert_eq!(due.len(), 3, "the next round covers every read that waited");
        assert_eq!(barrier.counts.rounds, 2);
    }

    /// A round that has not confirmed in [`ROUND_IN_FLIGHT_MILLIS`] does
    /// not hold the next one back: a lost answer costs a read that long,
    /// not its whole wait.
    #[test]
    fn a_round_in_flight_too_long_does_not_hold_the_next() {
        let b = ballot(1);
        let mut barrier = ReadBarrier::new();
        barrier
            .admit(Origin::Local, read(1, b, range()), leading(b, 3), T0)
            .unwrap();
        let first = barrier.round_to_start(b, 2, T0).unwrap();
        let soon = T0.plus(ROUND_IN_FLIGHT_MILLIS - 1);
        barrier
            .admit(Origin::Local, read(2, b, range()), leading(b, 4), soon)
            .unwrap();
        assert_eq!(barrier.round_to_start(b, 2, soon), None);
        let late = T0.plus(ROUND_IN_FLIGHT_MILLIS);
        let second = barrier.round_to_start(b, 2, late).unwrap();
        assert!(second > first);
        // The first still confirms if its answer comes, and covers only
        // the read before it.
        barrier.on_confirmed(ReplicaId([2; 16]), b, first, 2);
        let (due, _) = barrier.take_due(leading(b, 4), late, |_| true, at(5));
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].read.request.retry_key.request_sequence.get(), 1);
    }
}
