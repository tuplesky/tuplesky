# Collector contract, revision 3 (task-33, revised by task-c02 and task-c01)

The collector is the trusted component that turns an admitted client
request into a SwiftPaxos command, fans it out to the voters and
establishes the result from source-exact evidence (design Sections 3,
3.2, 3.3, 4.3, 4.4). Two implementations share this contract: the Rust
frontend (`crates/coord-collector`) and the authorized Go collector inside
Kine (task-m02). Go implements *collection*, never a second voting state
machine. This revision is frozen; changes are a new revision.

**What revision 2 changed.** Revision 1 assumed that what a voter said
about a command reached the collector that submitted it. It need not: a
voter that learns a command from a peer acknowledges it before any
submission has told the voter's frontend which collector asked, and a
frontend that holds such evidence for a while and then lets it go left
the collector short of that voter's evidence for ever, on a command the
domain had executed. Two things close that. The voters repair it
(task-c02, `coord_consensus::replay`): an exact duplicate submission --
same identity, same admission facts, same acknowledged floor --
publishes the same acknowledgement again, to the frontend only, under
the same gates as the first time. And the collector completes what it
half holds from the durable record of the command's execution on its
own node; see *Release*. Nothing about evidence or learning moved: a
command is established on exactly the evidence it was before, and the
record is never counted as a vote.

**What revision 3 changed.** Revisions 1 and 2 said fan-out was to every
voter at once and said nothing about a destination that could not take it. In
practice the transport dropped that copy and nothing re-offered it, so
a command ran on whatever subset happened to be free while the caller
was told nothing. Delivery is now a stated obligation with a stated
end: see *Dissemination*, and the admission rule it forced in
*Submission and fan-out*. Nothing about evidence, learning or release
moved -- a command is established on exactly the evidence it was
before.

## Roles and authority

| Role (from `Hello`) | May submit `Submit` to voters | Receives evidence | Votes |
|---|---|---|---|
| `Frontend` | yes, for admitted native clients | `Evidence`, `Release` | never |
| `KineCollector` | yes, for its bound domain | `Evidence`, `Release` for that domain | never |
| `Client` | no (`RoleNotAuthorized`) | no | never |
| `Voter`, `Observer`, `Learner` | no | n/a (peer plane) | voters only, on the peer plane |

Role-scoped collector access never becomes general request or voting
access: a voter admits a `Submit` only from a collector role, and nothing
that arrives on an API-class connection is ever counted as a vote. The
native SDK talks to a frontend because it is not a trusted collector.

## Frames

See `spec/wire-v1.md`, "Collector frames": `Submit` (`0x0103`, collector
to every voter), `Release` (`0x0701`, leader to collector), `Evidence`
(`0x0700`, voter to collector). Evidence is the postcard `ProtocolMessage`
the voter published to its frontend peer identity: `LeaderReply`,
`FastAck` or `SlowAck`. Anything else in an `Evidence` frame is
`NotEvidence` and is not counted.

## Admission

A request enters through the admission interface with the connection's
verified caller (role, session, rule generation, scope ceiling). The
request must decode as a canonical `RequestV1` whose logical bytes
re-encode identically; its retry key must name the collector's cluster and
domain and the caller's own session; the session must be below its
pending bound. Admission mints the sealed `AdmissionReceipt` (receipt
identity under the `admission-receipt` hash domain). Raw tokens never
appear on the wire or in traces.

## Submission and fan-out

```text
command_id = H(command-id domain, retry_key, canonical logical bytes)
```

* The first presentation of a retry key binds it to its command; the same
  key with another payload is `RequestIdentityConflict` and is never
  submitted.
* A retry of a pending command re-attaches the caller and sends nothing.
* A retry of a resolved command returns the retained outcome.
* The domain has two admission bounds and **both are reserved before any
  destination is offered anything**: a pending-command bound, and a bound
  on the submission envelope bytes held for commands that still owe a
  destination. The byte bound is sized in whole envelopes -- a request
  at the size limit plus what the envelope carries around it -- so a
  valid request always fits a free pending slot. At either bound a new
  submission is refused with backpressure. Unresolved entries are never evicted, and a cancelled
  entry frees no slot.
* A refusal therefore means *nothing was offered by this attempt*. It is
  not evidence that the same identity was never submitted from
  elsewhere. **Past the point of dispatch no destination's answer may
  become a refusal of the command**: a queue that was full says
  something about that destination and nothing about whether the command
  will be established.
* Fan-out targets are **every voter of the configuration, at once**. The
  leader is one target; no voter relays the submission and no follower
  waits for a leader hop before voting (the fast-set follower's `FastAck`
  leaves on arrival of the `Submit`; a follower outside the fixed fast
  set adopts the leader's order and answers with `SlowAck`, which is the
  source rule, not a relay).

## Dissemination

Delivery, and nothing here is consensus. An enqueue accepted says a
destination's ingress took responsibility for the frame; it is not a
vote, not durability, not application, and a destination that later
fails or makes no progress remains the same-identity retry and recovery
problem it always was.

**All-voter targeting is required; all-voter acceptance is not.** Every
voter is offered the submission, independently and without blocking.
One destination's refusal never delays another's offer and never blocks
the command.

* While a command is unresolved, the collector that accepted it **owns
  re-offering** what could not be queued. A caller does not repair this
  by presenting the request again: a retry of a pending command attaches
  and sends nothing, deliberately, and is not the delivery-retry
  mechanism.
* A repeat is **another delivery attempt for the original submission**:
  the same command identity, retry key, canonical request, admission
  facts and acknowledged sequence floor, from one retained envelope. It
  is never rebuilt from current session state, which would mint fresh
  admission facts for a command already submitted under others.
* A repeat names **only the destinations that still owe an enqueue**.
* Refusals are classified, because they need different treatment. A full
  queue and an absent route are delivery backpressure and are offered
  again on a floor with exponential backoff to a ceiling. A target the
  committed configuration does not name as a voter, and an envelope a
  route can never carry, are **not** on that schedule: repeating them is
  a busy loop with a known answer. They are remembered and reported at
  settlement, and only a reconfiguration revisits them.
* Re-offering is bounded in **memory and rate, never in obligation**.
  There is no attempt limit after which an accepted, unresolved command
  is forgotten. Scheduling is fair across commands and capped per turn,
  so one congested destination starves neither the other destinations
  nor new work, protocol traffic or recovery.
* A due re-offer is carried out **without any other traffic**. The
  collector says when its next re-offer falls due, and the runtime wakes
  for it; a runtime that waited only on its sockets would leave a quiet
  domain's refused submission unoffered until something unrelated
  arrived or the caller retried.
* A caller that times out or disconnects is detached; **the command
  keeps its delivery obligation**. A client deadline is not the lifetime
  of work the domain has accepted.
* **The obligation ends at settlement.** A command may legitimately be
  established by the quorum it needed before a saturated voter ever took
  its submission. At settlement the retry state is retired, its reserved
  capacity is released, and destinations that never took the submission
  are recorded (`Undisseminated`). Closing that gap is the replication
  and recovery path's, not a reason to hold a settled command's capacity
  open for an unreachable minority.
* Release is unchanged and does **not** wait for delivery: the
  collector's learning predicate over counted votes plus the leader's
  release-gate result, exactly as in *Release* below.

A voter that acknowledged a command before the submission naming its
submitter arrived holds that evidence until a submission places it.
That hold must outlast the repeat schedule's ceiling: a duplicate
submission produces no effects, so a repeat landing after the hold has
expired finds a voter with nothing to hand over and the acknowledgement
is lost. The runtime derives its hold from the ceiling for that reason.

## Evidence

Evidence is counted per **voter identity**, never per connection:

* the replica an acknowledgement claims must be the identity bound to the
  connection it arrived on (`SenderMismatch` otherwise);
* one identity contributes at most one fast and one adoption
  acknowledgement per command and ballot; the same acknowledgement over
  another connection is a duplicate and counts nothing;
* non-voters (observers, strangers) and fast acknowledgements from outside
  the ballot's fixed fast set are rejected, as are wrong-ballot,
  wrong-command and forged-proposal acknowledgements;
* `LeaderReply` is the leader's proposal (`FastAck` with a sequence
  number) and is accepted only from the ballot's leader identity.

The learning predicate over counted votes is the source one
(`coord_consensus::VoteSet::learned`): fast when the leader proposal and
path-equal fast-set members reach the fast size (leader included); slow
when adopters (slow acknowledgements, or fast acknowledgements whose
dependency set equals the leader's) plus the leader reach the slow
majority.

## Release

A result is released to the caller only when **both** hold for the same
epoch, ballot and command:

1. the collector's own learning predicate over counted voter identities;
2. the leader's release-gate result (`Release`), which carries the exact
   response and whether it preceded materialization. Only the ballot's
   leader may send it; a wrong epoch or ballot is refused; a second
   release must carry the same position, digest and response (a final
   release may follow a speculative one).

A lone leader reply, a lone release, or a loosely counted majority never
releases tentative data. The response is `ResponseV1 { command_id, Ok {
revision, result } }` with the exact response bytes, or `Err` with the
frozen `RESULT_TOO_LARGE` code when the result does not fit the response
bound.

**Settlement from the durable record** (revision 2). A collector that
holds one of the two -- the predicate without the release, or the
release without the predicate -- may complete the command from the
durable retry record of its execution on the collector's own node,
which is the same committed state that answers a caller's retry before
anything is submitted. The record must name this exact command; when the
release is held, the record must also agree with it on the result digest
and the response bytes, and a disagreement settles nothing and is
reported. With neither half held the record is not consulted: a
collector that answered from local state alone would be a different
component under a different contract. A settlement of this kind is never
counted as a vote and is never speculative. It is recorded in the trace
as `SettledFromRecord` followed by the `Released` it produced, whose
`voters` are the identities actually counted.

## Cancellation, deadlines, resolution

* A caller that goes away (connection closed) is detached; the command
  keeps collecting under the same identity and its outcome is retained.
* A client deadline that passes before establishment answers the caller
  with the `Pending` outcome (unknown, never failed) and detaches it; the
  command keeps collecting.
* `ResolveRequest { retry_key, command_id }` answers: the retained
  outcome; `Pending` while bound and collecting; `Err
  RequestIdentityConflict` when the key is bound to another command;
  `Unknown` for an identity never seen here or beyond the retained window.

## Ballot changes

When the collector learns a new ballot configuration, evidence and
releases of the old ballot are void: pending commands collect afresh under
the new configuration. Delivery is reconciled with it in the same step: a
replica that is no longer a voter stops being a destination, a replica
that has become one is owed the submission and is due it at once, and a
destination held off the congestion schedule is revisited here. Saturation
never gets a say in either direction -- a busy queue is not a membership
change. Epoch (membership) integration and configuration
refresh are task-m02.

## Golden event trace

Every transition is recorded as a `CollectorEvent` (`Submitted`,
`Attached`, `Retained`, `Refused`, `Evidence`, `Held`, `Released`,
`Cancelled`, `Resolved`, `TimedOut`, `Reconfigured`, `SettledFromRecord`,
`Reoffered`, `Undisseminated`) with identities in lowercase hex. The three
added in revisions 2 and 3 appear only when a delivery was lost or did not go
straight through, so the frozen revision-1 scenario produces the same trace it
always did. The Rust reference trace of the frozen scenario is
`crates/coord-collector/fixtures/collector_trace_v1.json` (regenerate
deliberately with `COORD_COLLECTOR_WRITE_FIXTURES=1`). The Go collector
must produce the same trace for the same scenario; differential tests of
loss, duplication, reordering, recovery and configuration change are
task-m02.

## Not claimed

Re-offering is delivery, not a guarantee of it: a destination that
accepts an enqueue may still fail before processing the frame, and
that remains the same-identity retry and recovery problem. This
revision does not claim reliable end-to-end delivery, only that a
submission a destination could not take is offered to it again while
the command is unresolved.

The collector re-derives command-level learning from voter evidence; the
predecessor order, authorization and exact result of a speculative release
are determined by the leader's release gate (task-29), whose output the
collector binds to its own evidence but does not recompute. No production
listener, session binding (task-37) or SDK (task-34) is part of this
revision.

Repair is delivery, not a guarantee of it. A voter repairs what it
retained, and what it retained is bounded by what its command table
remembers -- its live records and the tombstones of the ones it retired
-- and not by any count of unrelated commands. A voter that restarted
repairs nothing, because the sends were the previous boot's. Past both,
the durable record is the path, for the collector as above and for a
caller's retry as before, and this revision claims no more than that.
