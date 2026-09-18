# Collector contract, revision 1 (task-33)

The collector is the trusted component that turns an admitted client
request into a SwiftPaxos command, fans it out to the voters and
establishes the result from source-exact evidence (design Sections 3,
3.2, 3.3, 4.3, 4.4). Two implementations share this contract: the Rust
frontend (`crates/coord-collector`) and the authorized Go collector inside
Kine (task-m02). Go implements *collection*, never a second voting state
machine. This revision is frozen; changes are a new revision.

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
to every voter), `Release` (`0x0104`, leader to collector), `Evidence`
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
* The domain has a pending bound; at the bound a new submission is
  refused with backpressure. Unresolved entries are never evicted, and a
  cancelled entry frees no slot.
* Fan-out targets are **every voter of the configuration, at once**. The
  leader is one target; no voter relays the submission and no follower
  waits for a leader hop before voting (the fast-set follower's `FastAck`
  leaves on arrival of the `Submit`; a follower outside the fixed fast
  set adopts the leader's order and answers with `SlowAck`, which is the
  source rule, not a relay).

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
the new configuration. Epoch (membership) integration and configuration
refresh are task-m02.

## Golden event trace

Every transition is recorded as a `CollectorEvent` (`Submitted`,
`Attached`, `Retained`, `Refused`, `Evidence`, `Held`, `Released`,
`Cancelled`, `Resolved`, `TimedOut`, `Reconfigured`) with identities in
lowercase hex. The Rust reference trace of the frozen scenario is
`crates/coord-collector/fixtures/collector_trace_v1.json` (regenerate
deliberately with `COORD_COLLECTOR_WRITE_FIXTURES=1`). The Go collector
must produce the same trace for the same scenario; differential tests of
loss, duplication, reordering, recovery and configuration change are
task-m02.

## Not claimed

The collector re-derives command-level learning from voter evidence; the
predecessor order, authorization and exact result of a speculative release
are determined by the leader's release gate (task-29), whose output the
collector binds to its own evidence but does not recompute. No production
listener, session binding (task-37) or SDK (task-34) is part of this
revision.
