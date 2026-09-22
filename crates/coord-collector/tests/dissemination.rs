//! task-c01: the collector owns re-offering a submission a destination
//! could not take, under a bounded budget.
//!
//! The rule this pins down is one sentence with two halves that pull in
//! opposite directions, and both have to hold:
//!
//! **All-voter targeting is required; all-voter acceptance is not.**
//!
//! The first half is why unresolved work keeps its delivery obligation:
//! a voter whose lane was full when the submission went past is still a
//! voter, and dropping its copy silently leaves the command running on
//! whatever subset happened to be free. The second half is why settled
//! work does not wait for an unavailable minority: a command that has
//! the quorum it needs is answered, and holding this collector's
//! capacity open for a destination that may never come back would be
//! paying for a delivery nobody is waiting on.
//!
//! What sits between them is the admission boundary. The collector
//! reserves what tracking a command costs -- a slot *and* the envelope's
//! bytes -- before a single destination has been offered anything, so a
//! refusal is a statement that nothing was sent by this attempt. Past
//! that point no destination's answer may become a refusal of the
//! command.

use std::collections::BTreeSet;

use coord_collector::{
    Collector, CollectorConfig, FanOut, OfferOutcome, Offered, SubmitRefusal, Submitted,
};
use coord_consensus::{BallotConfiguration, ProtocolMessage, SlowAck};
use coord_core::capability::{AdmissionReceipt, AttestedAdmission, VerifierToken};
use coord_core::event::{AdmittedRequest, PeerProvenance};
use coord_types::identity::Digest32;
use coord_types::ids::*;
use coord_types::logical_v1::*;
use coord_types::wire_v1::{MessageV1, RequestV1};
use coord_types::{CommandId, RetryKey};

const NS: NamespaceId = NamespaceId([5; 16]);
const SESSION: SessionId = SessionId([3; 16]);
const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);
const CLIENT: ClientInstanceId = ClientInstanceId([4; 16]);

fn r(i: u8) -> ReplicaId {
    ReplicaId([i; 16])
}

fn epoch() -> ConfigurationEpoch {
    ConfigurationEpoch::new(1).unwrap()
}

fn ballot() -> Ballot {
    Ballot {
        epoch: epoch(),
        number: 0,
        leader: r(0),
    }
}

fn quorum(n: u8) -> BallotConfiguration {
    let fast: BTreeSet<ReplicaId> = (0..(n / 2 + 1)).map(r).collect();
    BallotConfiguration::c2(epoch(), ballot(), (0..n).map(r).collect(), fast).unwrap()
}

fn retry_key(seq: u64) -> RetryKey {
    RetryKey {
        cluster_id: CLUSTER,
        domain_id: DOMAIN,
        session_id: SESSION,
        client_instance_id: CLIENT,
        request_sequence: RequestSequence::new(seq).unwrap(),
    }
}

fn put(k: &[u8], v: &[u8]) -> CanonicalOperation {
    CanonicalOperation::Put(PutOp {
        key: k.to_vec(),
        value: v.to_vec(),
        lease: None,
        prev_kv: true,
    })
}

fn admitted(seq: u64, op: CanonicalOperation) -> (CommandId, AdmittedRequest) {
    let mut logical = LogicalRequest::new(NS, op);
    logical.canonicalize();
    let key = retry_key(seq);
    let command = CommandId::derive(&key, &logical).unwrap();
    let request = RequestV1::new(key, &logical, 0, 0).unwrap();
    (
        command,
        AdmittedRequest {
            receipt: AdmissionReceipt::submitting(
                VerifierToken::for_boundary(),
                AttestedAdmission {
                    cluster: CLUSTER,
                    domain: DOMAIN,
                    session: SESSION,
                    rule_generation: 1,
                    scope_ceiling: u32::MAX,
                    receipt_id: Digest32([7; 32]),
                    admitted_at_ticks: 0,
                },
            ),
            frame: MessageV1::Request(request).encode().unwrap(),
        },
    )
}

fn collector(max_pending: usize, max_undelivered_bytes: usize) -> Collector {
    Collector::new(CollectorConfig {
        quorum: quorum(3),
        max_pending,
        max_resolved: 16,
        max_undelivered_bytes,
    })
}

fn fan_out(c: &mut Collector, seq: u64) -> (CommandId, FanOut) {
    let (command, request) = admitted(seq, put(b"k", b"v"));
    let Submitted::FanOut(plan) = c.submit(0, &request).expect("admitted") else {
        panic!("a first presentation fans out");
    };
    (command, plan)
}

/// Report one outcome for every target of `plan`.
fn report(c: &mut Collector, now: u64, plan: &FanOut, outcome: OfferOutcome) {
    let outcomes = plan.targets.iter().map(|t| (*t, outcome)).collect();
    c.offered(
        now,
        &Offered {
            command: plan.command,
            outcomes,
        },
    );
}

/// Report `outcome` for one destination and `Queued` for the rest.
fn report_one(c: &mut Collector, now: u64, plan: &FanOut, who: ReplicaId, outcome: OfferOutcome) {
    let outcomes = plan
        .targets
        .iter()
        .map(|t| {
            (
                *t,
                if *t == who {
                    outcome
                } else {
                    OfferOutcome::Queued
                },
            )
        })
        .collect();
    c.offered(
        now,
        &Offered {
            command: plan.command,
            outcomes,
        },
    );
}

// --- saturation and recovery -------------------------------------------

/// One destination's full queue costs that destination and nothing
/// else, and the collector offers it again by itself once the floor has
/// passed -- with no second presentation from the caller.
///
/// The negative control is the whole of the old behaviour: with the
/// rejected target simply counted and forgotten, `due_offers` returns
/// nothing here for ever and the command runs on two voters of three
/// without anyone being told.
#[test]
fn a_full_queue_costs_that_destination_and_is_offered_again_by_itself() {
    let mut c = collector(8, usize::MAX);
    let (command, plan) = fan_out(&mut c, 1);
    assert_eq!(plan.targets.len(), 3, "every voter is a target");

    // Two took it; one was full. The other two are not re-offered:
    // repeating a submission to a voter that has it is work with no
    // question behind it.
    report_one(&mut c, 0, &plan, r(2), OfferOutcome::Saturated);
    assert_eq!(c.undelivered(), 1);

    // Not immediately: a repeat per turn would spend the lane on the
    // retries instead of on the drain that ends them.
    assert!(c.due_offers(0, 16).is_empty(), "the floor has not passed");

    let due = c.due_offers(1_000, 16);
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].command, command);
    assert_eq!(due[0].targets, vec![r(2)], "only what is still owed");

    // The queue drained, so the obligation ends -- and with it the
    // capacity the envelope was holding.
    report_one(&mut c, 1_000, &due[0], r(2), OfferOutcome::Queued);
    assert_eq!(c.undelivered(), 0);
    assert_eq!(c.undelivered_bytes(), 0);
    assert!(c.due_offers(u64::MAX, 16).is_empty());
}

/// A destination that is unreachable rather than busy is on the same
/// schedule: both are delivery backpressure, and both pass.
#[test]
fn an_unreachable_destination_is_offered_again_like_a_busy_one() {
    let mut c = collector(8, usize::MAX);
    let (_, plan) = fan_out(&mut c, 1);
    report_one(&mut c, 0, &plan, r(1), OfferOutcome::Unreachable);
    let due = c.due_offers(1_000, 16);
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].targets, vec![r(1)]);
}

/// A repeat carries the original envelope, byte for byte.
///
/// Not a re-derivation from current session state: that would mint
/// fresh admission facts for a command already submitted under others,
/// and a voter comparing the two would be right to call them different
/// requests.
#[test]
fn a_repeat_is_the_same_envelope_and_the_same_identity() {
    let mut c = collector(8, usize::MAX);
    let (command, plan) = fan_out(&mut c, 1);
    report_one(&mut c, 0, &plan, r(2), OfferOutcome::Saturated);
    let due = c.due_offers(1_000, 16);
    assert_eq!(due[0].command, command);
    assert_eq!(due[0].retry_key, plan.retry_key);
    assert_eq!(
        due[0].frame.as_ref(),
        plan.frame.as_ref(),
        "the repeat is the submission, not a new one"
    );
}

/// A destination whose offer is outstanding is not offered again.
///
/// An attempt in flight is not a free slot. Without this the budget
/// goes on the same destination every turn while it is still deciding.
#[test]
fn an_outstanding_offer_is_not_a_free_slot() {
    let mut c = collector(8, usize::MAX);
    let (_, plan) = fan_out(&mut c, 1);
    report_one(&mut c, 0, &plan, r(2), OfferOutcome::Saturated);
    assert_eq!(c.due_offers(1_000, 16).len(), 1);
    assert!(
        c.due_offers(1_001, 16).is_empty(),
        "the answer to the last offer has not come back"
    );
}

// --- admission versus uncertain outcome --------------------------------

/// The collector's own capacity is the last point at which refusing is
/// honest, and it is checked before anything is offered.
#[test]
fn capacity_is_reserved_before_a_single_destination_is_offered() {
    // One command's worth of bytes, and the second command wants its
    // own: the slot bound is nowhere near reached, so this is the byte
    // budget alone doing the refusing.
    let (_, probe) = admitted(1, put(b"k", b"v"));
    let mut sizer = collector(64, usize::MAX);
    let Submitted::FanOut(plan) = sizer.submit(0, &probe).unwrap() else {
        panic!()
    };
    let one = plan.frame.len();

    let mut c = collector(64, one);
    let (_, first) = fan_out(&mut c, 1);
    assert_eq!(c.undelivered_bytes(), one);
    // Nothing has been offered yet, so the first command is still
    // owing every destination and still holding its envelope.
    let (_, second) = admitted(2, put(b"j", b"w"));
    assert!(
        matches!(
            c.submit(0, &second),
            Err(SubmitRefusal::Backpressure { .. })
        ),
        "the envelope budget refuses before dispatch"
    );

    // And the refusal is a statement about *this* attempt: once the
    // first command owes nobody, the capacity is back.
    report(&mut c, 0, &first, OfferOutcome::Queued);
    assert_eq!(c.undelivered_bytes(), 0);
    assert!(matches!(c.submit(0, &second), Ok(Submitted::FanOut(_))));
}

/// A destination refusing its queue is never a refusal of the command.
///
/// This is the rule that makes accepting work mean something: once a
/// command has been dispatched it may be anywhere, so "not executed" is
/// no longer this collector's to say.
#[test]
fn saturation_after_dispatch_never_becomes_a_refusal() {
    let mut c = collector(8, usize::MAX);
    let (command, plan) = fan_out(&mut c, 1);
    // Every destination refused. The worst case, and still not a
    // failure: the command keeps its identity, keeps collecting, and
    // keeps its delivery obligation.
    report(&mut c, 0, &plan, OfferOutcome::Saturated);
    assert!(c.is_pending(&command));
    assert_eq!(c.undelivered(), 1);

    // A retry of the same request attaches; it does not re-fan-out and
    // it is certainly not refused.
    let (_, same) = admitted(1, put(b"k", b"v"));
    assert!(matches!(c.submit(0, &same), Ok(Submitted::Attached { .. })));
}

/// The caller's deadline is not the lifetime of accepted work.
///
/// A timed-out caller is detached and answered `Pending`; the command
/// it left behind still owes every destination it never reached, and
/// still gets them.
#[test]
fn a_callers_deadline_does_not_discard_the_delivery_obligation() {
    let mut c = collector(8, usize::MAX);
    let (command, request) = admitted(1, put(b"k", b"v"));
    let Submitted::FanOut(plan) = c.submit(0, &request).unwrap() else {
        panic!()
    };
    report_one(&mut c, 0, &plan, r(2), OfferOutcome::Saturated);

    // The caller goes away.
    c.cancel(&retry_key(1));
    assert!(c.is_pending(&command), "the command keeps collecting");
    assert_eq!(c.undelivered(), 1, "and keeps owing the destination");
    let due = c.due_offers(1_000, 16);
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].targets, vec![r(2)]);
}

// --- quorum availability and resource bounds ---------------------------

/// A permanently unreachable minority does not hold up the answer, and
/// what it missed is recorded rather than forgotten.
#[test]
fn a_settled_command_does_not_wait_for_a_minority_that_never_took_it() {
    let mut c = collector(8, usize::MAX);
    let (command, plan) = fan_out(&mut c, 1);
    // Two voters took it; the third never will.
    report_one(&mut c, 0, &plan, r(2), OfferOutcome::Unreachable);

    // The two that did take it establish the command. Release needs
    // both halves, exactly as before -- this changes nothing about what
    // a result costs, only about who was offered the submission.
    settle(&mut c, command, &[r(0), r(1)]);

    assert!(!c.is_pending(&command), "the quorum answered");
    assert_eq!(c.undelivered(), 0, "the obligation retired with it");
    assert_eq!(c.undelivered_bytes(), 0, "and so did its capacity");
    assert!(
        c.due_offers(u64::MAX, 16).is_empty(),
        "a settled command is not offered again"
    );
    // Said, not silently dropped: an operator can see that this command
    // was established without one of its voters ever being handed it.
    let missed: Vec<&coord_collector::CollectorEvent> = c
        .trace()
        .iter()
        .filter(|e| matches!(e, coord_collector::CollectorEvent::Undisseminated { .. }))
        .collect();
    assert_eq!(missed.len(), 1, "settlement records what was missed");
}

/// One congested command cannot spend the whole budget every turn.
///
/// The budget here is smaller than a single command's destinations, and
/// every destination refuses again the moment it is offered, so on each
/// round every command is due and they compete. Without a cursor the
/// scan starts at the same command every time and the two commands at
/// the back are never reached: they hold their capacity and wait for a
/// quiet moment that a busy domain does not have.
#[test]
fn the_offer_budget_is_spread_across_commands() {
    let mut c = collector(8, usize::MAX);
    for seq in 1..=4 {
        let (_, plan) = fan_out(&mut c, seq);
        report(&mut c, 0, &plan, OfferOutcome::Saturated);
    }
    let mut served: Vec<CommandId> = Vec::new();
    let mut now = 0u64;
    for _ in 0..4 {
        // Far enough forward that every destination is due again, so
        // what decides who is served is the order alone.
        now += 10_000;
        let due = c.due_offers(now, 2);
        for plan in &due {
            served.push(plan.command);
            report(&mut c, now, plan, OfferOutcome::Saturated);
        }
    }
    let distinct: BTreeSet<CommandId> = served.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        4,
        "every command got a turn, not just the ones at the front: {served:?}"
    );
}

/// The per-turn budget is a hard cap on destinations, not a suggestion.
#[test]
fn the_offer_budget_bounds_destinations_and_not_commands() {
    let mut c = collector(8, usize::MAX);
    for seq in 1..=4 {
        let (_, plan) = fan_out(&mut c, seq);
        report(&mut c, 0, &plan, OfferOutcome::Saturated);
    }
    for budget in [0usize, 1, 2, 5, 7] {
        let due = c.due_offers(1_000, budget);
        let destinations: usize = due.iter().map(|d| d.targets.len()).sum();
        assert!(
            destinations <= budget,
            "budget {budget} produced {destinations} destinations"
        );
        // Put them back on the schedule for the next round.
        for plan in &due {
            report(&mut c, 1_000, plan, OfferOutcome::Saturated);
        }
    }
}

/// Sustained refusal costs less and less, so retries cannot become the
/// load.
#[test]
fn a_destination_that_keeps_refusing_is_asked_less_often() {
    let mut c = collector(8, usize::MAX);
    let (_, plan) = fan_out(&mut c, 1);
    report_one(&mut c, 0, &plan, r(2), OfferOutcome::Saturated);

    let mut now = 0u64;
    let mut waits = Vec::new();
    for _ in 0..6 {
        // Walk time forward to whenever the next offer is due.
        let mut waited = 0u64;
        loop {
            let due = c.due_offers(now, 16);
            if let Some(plan) = due.first() {
                report_one(&mut c, now, plan, r(2), OfferOutcome::Saturated);
                break;
            }
            now += 1;
            waited += 1;
            assert!(waited < 10_000, "the destination stopped being offered");
        }
        waits.push(waited);
    }
    assert!(
        waits.windows(2).all(|w| w[1] >= w[0]),
        "the wait never shortens while it keeps refusing: {waits:?}"
    );
    assert!(
        waits.last().unwrap() > waits.first().unwrap(),
        "and it does grow: {waits:?}"
    );
    // Bounded: it backs off, it does not give up.
    assert!(
        *waits.last().unwrap() <= 1_001,
        "the wait is capped: {waits:?}"
    );
}

/// A destination held off the congestion schedule is not a busy one.
///
/// A plan and a configuration disagreeing is not something repeating
/// settles, and an envelope the route can never carry is not either.
/// Both are remembered -- they are reported at settlement -- and
/// neither is retried on the floor.
#[test]
fn a_configuration_disagreement_is_not_retried_as_congestion() {
    for permanent in [
        OfferOutcome::NotACommittedVoter,
        OfferOutcome::Undeliverable,
    ] {
        let mut c = collector(8, usize::MAX);
        let (_, plan) = fan_out(&mut c, 1);
        report_one(&mut c, 0, &plan, r(2), permanent);
        assert!(
            c.due_offers(u64::MAX, 16).is_empty(),
            "{permanent:?} must not enter the congestion loop"
        );
        assert_eq!(
            c.undelivered(),
            0,
            "{permanent:?} is not an outstanding repeat"
        );
    }
}

// --- reconfiguration ---------------------------------------------------

/// Delivery follows the committed configuration, the same as evidence
/// does -- and saturation never gets a say in it.
#[test]
fn a_reconfiguration_reconciles_the_destinations_it_still_owes() {
    let mut c = collector(8, usize::MAX);
    let (_, plan) = fan_out(&mut c, 1);
    // A voter the configuration will drop is stalled; one that will
    // stay is merely busy.
    report_one(&mut c, 0, &plan, r(2), OfferOutcome::NotACommittedVoter);

    // Five voters now: r(2) is one again, and r(3) and r(4) are new.
    c.reconfigure(quorum(5));

    let due = c.due_offers(0, 16);
    assert_eq!(due.len(), 1);
    let targets: BTreeSet<ReplicaId> = due[0].targets.iter().copied().collect();
    assert_eq!(
        targets,
        [r(2), r(3), r(4)].into_iter().collect::<BTreeSet<_>>(),
        "the voters that have not taken it, old and new"
    );
}

/// A voter that leaves the configuration stops being owed anything,
/// and a command that then owes nobody gives its capacity back.
#[test]
fn a_voter_that_leaves_the_configuration_is_no_longer_a_destination() {
    let mut c = collector(8, usize::MAX);
    let (_, plan) = fan_out(&mut c, 1);
    // r(0) and r(1) took it; r(2) did not, and is about to stop being
    // a voter at all.
    report_one(&mut c, 0, &plan, r(2), OfferOutcome::Saturated);
    assert_eq!(c.undelivered(), 1);
    assert!(c.undelivered_bytes() > 0);

    c.reconfigure(
        BallotConfiguration::c2(
            epoch(),
            ballot(),
            [r(0), r(1)].into_iter().collect(),
            [r(0), r(1)].into_iter().collect(),
        )
        .unwrap(),
    );

    assert_eq!(c.undelivered(), 0, "nobody is owed the submission now");
    assert_eq!(c.undelivered_bytes(), 0, "so nothing holds its bytes");
    assert!(c.due_offers(u64::MAX, 16).is_empty());
}

/// Establish `command` over `voters` and release it, so the test can
/// reach settlement without rebuilding the consensus stack.
fn settle(c: &mut Collector, command: CommandId, voters: &[ReplicaId]) {
    use coord_core::capability::{EstablishedResult, EstablishmentEvidence, ReleasedResult};
    for v in voters {
        // The leader publishes its order as a `LeaderReply`; a follower
        // outside the fast set adopts it and answers `SlowAck`. Each
        // identity votes once, which is the whole of what the learning
        // predicate counts.
        let message = if *v == ballot().leader {
            ProtocolMessage::LeaderReply {
                ballot: ballot(),
                command,
                seqnum: 1,
                deps: Vec::new(),
                path: Digest32([9; 32]),
            }
        } else {
            ProtocolMessage::SlowAck(SlowAck {
                replica: *v,
                ballot: ballot(),
                command,
                admission: c.admission(&command).expect("pending"),
            })
        };
        let _ = c.on_evidence(
            PeerProvenance::from_transport(*v, ReplicaIncarnation::new(1).unwrap(), 1),
            message,
        );
    }
    let released = ReleasedResult::from_gate(
        EstablishedResult::establish(EstablishmentEvidence {
            command,
            epoch: epoch(),
            ballot: ballot(),
            position: ExecutionPosition::new(1).unwrap(),
            closed_predecessors: vec![],
            result_digest: Digest32([2; 32]),
            revision: Some(KvRevision::new(1).unwrap()),
            fast_path: false,
        })
        .unwrap(),
        vec![0xaa],
        true,
    );
    let _ = c.on_release(
        PeerProvenance::from_transport(ballot().leader, ReplicaIncarnation::new(1).unwrap(), 1),
        released,
    );
}

// --- late submission and duplicate safety ------------------------------

/// A submission that arrives late is the same submission, so counting
/// its evidence twice is not possible.
///
/// The repeat carries the original command identity, and evidence is
/// counted by voter identity: a voter that answers the late copy as
/// well as the proposal it had already seen contributes one vote, not
/// two. This is what makes re-offering safe to do at all -- if a repeat
/// could be counted separately, a congested destination would be worth
/// more to the quorum than a healthy one.
#[test]
fn a_late_submission_cannot_be_counted_twice() {
    let mut c = collector(8, usize::MAX);
    let (command, plan) = fan_out(&mut c, 1);
    report_one(&mut c, 0, &plan, r(2), OfferOutcome::Saturated);

    let admission = c.admission(&command).expect("pending");
    let vote = |replica: ReplicaId| {
        ProtocolMessage::SlowAck(SlowAck {
            replica,
            ballot: ballot(),
            command,
            admission,
        })
    };
    let from = |replica: ReplicaId, connection: u64| {
        PeerProvenance::from_transport(replica, ReplicaIncarnation::new(1).unwrap(), connection)
    };

    // r(1) voted off the leader's proposal, before the submission ever
    // reached it.
    assert!(c.on_evidence(from(r(1), 1), vote(r(1))).is_ok());

    // The repeat lands, and r(1) answers it too -- on another
    // connection, which is the shape this arrives in.
    assert!(
        c.on_evidence(from(r(1), 2), vote(r(1))).is_err(),
        "the same voter's second answer is a duplicate, not a second vote"
    );

    // And the command is still one command: no second identity, no
    // second execution.
    assert!(c.is_pending(&command));
    let (_, same) = admitted(1, put(b"k", b"v"));
    assert!(matches!(c.submit(0, &same), Ok(Submitted::Attached { .. })));
}

/// A voter holding evidence for a submitter it does not know yet must
/// outlast the schedule the repeat arrives on.
///
/// The two live in different crates and neither is free to move on its
/// own: a hold shorter than the gap between the slowest two repeats
/// expires between them, the repeat lands on a voter with nothing to
/// hand over, and a duplicate submission produces no effects -- so that
/// acknowledgement is gone. `coordd` derives its hold from this
/// ceiling; this is the end of the rope they are tied with.
#[test]
fn the_repeat_schedule_is_something_a_voters_hold_must_outlast() {
    // A ceiling a hold can be a multiple of, and short enough that
    // several of them are a sane thing to hold evidence for.
    assert!(
        (100..=5_000).contains(&coord_collector::OFFER_CEILING_MILLIS),
        "a ceiling outside this range makes the derived hold wrong at one end or the other"
    );
}

/// A voter that joins after every other voter took the submission is
/// recorded as having missed it, never left owed something that cannot
/// be sent.
///
/// The envelope is released the moment a command owes nobody -- that is
/// what keeps the byte budget honest -- so there is nothing left to
/// offer a latecomer. Marking it due anyway would leave the command
/// outstanding for ever against an offer that can never be made.
#[test]
fn a_voter_joining_after_the_envelope_was_released_is_not_left_owed() {
    let mut c = collector(8, usize::MAX);
    let (command, plan) = fan_out(&mut c, 1);
    report(&mut c, 0, &plan, OfferOutcome::Queued);
    assert_eq!(
        c.undelivered_bytes(),
        0,
        "nobody is owed, so nothing is held"
    );

    c.reconfigure(quorum(5));

    assert!(
        c.due_offers(u64::MAX, 16).is_empty(),
        "there is no envelope to offer the new voters"
    );
    assert_eq!(
        c.undelivered(),
        0,
        "and they are not counted as an outstanding repeat"
    );
    // Recorded rather than forgotten: settlement says who missed it.
    settle(&mut c, command, &[r(0), r(1), r(2)]);
    let missed = c
        .trace()
        .iter()
        .filter(|e| matches!(e, coord_collector::CollectorEvent::Undisseminated { .. }))
        .count();
    assert_eq!(missed, 1, "the voters that joined too late are reported");
}
