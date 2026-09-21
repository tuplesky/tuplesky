# Implementation notes

Things the plan did not anticipate, and what was done about them. Each
entry records what was expected, what turned out to be true, what we did
instead, and what would let us undo it. This is not a design document and
does not override one: the design and the PR plan stay authoritative, and
an entry here exists so a decision taken under surprise can be revisited
deliberately rather than rediscovered.

Add an entry when a workaround outlives the change that introduced it, or
when a future reader would otherwise reasonably ask "why is it done this
strange way?".

## etcd client panics on a progress notify after a watch cancel

**Where:** `adapters/kine/backend/watch_test.go`, the synchronization
barrier test. Affects anything driving a cancellable watch through
`clientv3` while progress notifications are enabled.

**Expected:** the Kine-facing watch tests could assert every property
through a real etcd client, the way an API server sees them.

**Actually:** `go.etcd.io/etcd/client/v3@v3.7.1` panics with
`send on closed channel` when a progress notify is dispatched to a watch
the server has cancelled. Its run loop closes the cancelled substream's
`recvc` and hands the substream to `closingc`, but the entry stays in
`w.substreams` until that handoff completes (`watch.go:610-626`,
`685-688`); `broadcastResponse` meanwhile sends to every entry in the map
(`watch.go:738-747`). A send on a closed channel panics rather than
blocking, so the `select` on `donec` cannot skip it. `unicastResponse`
has the same exposure through the same stale entry.

It failed the gate roughly one run in four (6 failures in 25 full Go-suite
runs). Two things make a downgrade useless: `v3.6.14` carries the
identical code, diffed function for function, and pinned Kine itself
requires `v3.7.1`, so any pin below it needs a `replace` override anyway.

It is known upstream, and independently diagnosed the same way:
[etcd-io/etcd#21969](https://github.com/etcd-io/etcd/issues/21969)
reports it against `main`, `v3.6.12`, `v3.7.0-rc.0` and `v3.8.0-alpha.0`,
and says it is likelier "under high watch churn with progress notify
enabled". [PR #22191](https://github.com/etcd-io/etcd/pull/22191) proposes
the one-line fix -- delete the substream from `w.substreams` at cancel
rather than leaving it for deferred cleanup. At the time of writing the
PR is open, unmerged, waiting on a code-owner review, and `main` still
carries the defect.

**Did:** asserted the property on our own code instead. `WaitForSyncTo` is
the barrier both of the bridge's progress paths rest on -- `ProgressIfSynced`
per watch and `ProgressAll` broadcast each publish a revision only once it
returns -- so the test opens the watch through `Backend.Watch`, drains it
as a real consumer would, and holds the contract directly. The negative
control (returning from the wait as soon as cancellation is requested)
still fails it. 30 consecutive full-suite runs are clean.

This dropped two subtests that differed only in which bridge call reached
the barrier; both reached the same one. The bridge's progress path keeps
its coverage in `TestProgressNeverOvertakesEvents`, whose watch is never
cancelled and so never meets the defect.

**Revisit when:** #22191 (or its successor) merges and reaches a release
we can pin. Then this test could go back through `clientv3` if that ever
seems worth it, and the note here can be closed out. Until then the
defect is still real in production -- a watch cancel racing a progress
notify, with a far smaller window than the test's injected teardown
delay -- and if that ever matters the fix here is a `replace` directive
onto a patched client, not a downgrade.

## `Bind` is a raw kind the transport admits without decoding

**Where:** `coord_types::wire_v1::KIND_SESSION_BIND`,
`coord-transport/src/endpoint.rs` (`read_request`), `coord-session/src/wire.rs`.

**Expected:** two independent statements that did not know about each
other. `spec/wire-v1.md` says `Bind`/`BindAck` are raw kinds dispatched by
number at the session boundary, "not part of the typed registry"; task-43
hardened API streams to decode at the boundary and accept only frames that
decode to a client request.

**Actually:** composing both means every binding frame is closed as
`UnsupportedKind { kind: 261 }`, so no client can connect at all. Neither
statement is wrong; they had simply never met, because the session binding
and the transport hardening were built on separate branches.

**Did:** the transport admits exactly this kind at exactly its version,
before decoding, and the kind and version constants moved to `coord-types`
so the boundary that admits the frame and the module that answers it
cannot drift apart. The exemption is enumerated, not open: a neighbouring
unregistered kind, the acknowledgement kind (which only a frontend may
send) and a wrong version are each still refused, which
`the_session_binding_is_the_only_undecodable_kind_an_api_stream_may_carry`
pins.

**Revisit when:** another raw kind needs the same treatment (the
configuration and collector ranges are already specified as raw). The
pattern generalises, but each one should be admitted explicitly rather
than by widening the rule to "anything in the API range".

## A frozen fixture can go stale across a rebase without conflicting

**Where:** `crates/coord-checkpoint/fixtures/shared_checkpoint_v1.json`,
`crates/coord-journal-api/fixtures/journal_record_v1.json`,
`crates/coord-state/fixtures/kine_responses_v1.json`.

**Expected:** a frozen fixture and the type it encodes change together,
and a rebase that breaks the pairing conflicts.

**Actually:** the fixture and the type live in different files, so a
cherry-pick onto a base that changed the type applies cleanly and leaves
the fixture describing a shape that no longer exists. Three fixtures
drifted this way when the branch tracks were rebased into one lineage, and
each looked like a corruption until traced:

* the checkpoint fixture was frozen before task-40's review fix added
  `expires_at` to `SessionRecord` -- exactly ten bytes on one session row;
* the journal record fixture moved only its declared
  `max_record_value_bytes`, because the envelope payload bound was raised;
  every record's bytes and digest were unchanged;
* the Kine response fixture was frozen against a nested `KineKv` that put
  the private lease identity on the wire, which the Rust type has never
  had.

**Did:** regenerated each one only after accounting for the drift, and
fixed the Go mirror rather than the Rust type in the third case, because
the Rust type was right.

**Revisit when:** never, as such -- but the lesson is procedural. Before
regenerating a frozen fixture, find the commit that changed the encoding
and say which field moved. A fixture that "just drifted" is either a real
regression or a rebase artifact, and the two are indistinguishable until
the byte difference is named.

## Gate invocations that differ from the obvious one

**Where:** `xtask/src/main.rs`.

* `cargo clippy --workspace --all-features` does not build. The
  `secret-service` crate fails with `compile_error!` unless one of its
  crypto features is selected, and nothing in this workspace selects one.
  The gate runs `--all-targets --locked` without `--all-features`, and so
  should you.
* `cargo xtask check-deps` on its own tries to fetch the RustSec advisory
  database over the network and fails where that is unavailable. `cargo
  xtask ci` calls it with `offline = true`, which skips only the advisory
  fetch; the bans, licences, sources and role/isolation checks all still
  run. Use `cargo xtask check-deps --offline` to reproduce what the gate
  does.

## Evidence is a send, and takes the durability gate with the rest

**Where:** `coord-daemon/src/node.rs` (`Node::one_round`),
`coord_collector::frontend_frame`.

**Expected:** the daemon's node driver could route a voter's effects the
way the existing composition test does: anything `frontend_frame` claims
goes to the collector, everything else to the peer plane through the
outbox.

**Actually:** `frontend_frame` claims two different things. A
`Released` result is the leader's release-gate output and rests on no
barrier of its own -- the release rule has already decided it may be
disclosed. Evidence is an `Effect::SendWhenDurable` addressed to the
frontend, and it is a *vote*: "I have this and I will not forget it".
Routing both the same way publishes the vote before the record behind it
is durable, which is the one promise a replica may never break. The
machines happen to gate most sends themselves, which is why the hole is
invisible in a protocol test; the follower's evidence is the case where
they do not, and it is described in the same round as its own batch.

**Did:** every `SendWhenDurable` goes through the outbox, the frontend's
included, and becomes an evidence frame only when that send is released.
Only `Released` is published immediately.

The negative control needed care. Reverting the fix still passed a test
that asserted "some send was held", because the same round holds two
peer sends. `Node::held_at_least_once` therefore counts evidence
separately, which is also the count an operator wants: a voter whose
disk is slow holds its votes, and from the collector's side that is
indistinguishable from a voter that is partitioned or gone.

**Revisit when:** a second kind of effect is addressed to the collector.
The rule to keep is the distinction, not the list: an effect that names
barriers is gated on them wherever it is addressed, and an effect that
names none has already been gated somewhere else.

## Two coordinators, one application path, and one completion rule

**Where:** `coord-storage/src/persistence.rs`, `materialize.rs`
(`prepare`/`submit`/`complete`), `coord-storage/tests/composed.rs`.
Recorded against task-j08, which the plan gained for this work.

**Expected:** task-j03 integrated the journal-first coordinator, so a
serving daemon could be composed on it.

**Actually:** `JournaledStore` and `StoreWorker` were parallel
coordinators that nothing joined. `Applier` concretely owned a
`StoreWorker`, so the only ways to serve were to bypass the journal or
to write a second applier -- and a second applier is a second copy of
planning, admission and retry resolution, one of which would fall
behind. The plan named no task for the join.

**Did:** one seam (`Persistence`) at exactly the width the application
path needs, with the planner and `plan_to_batch` shared above it. The
application path became prepare -> submit -> complete, so the runtime
can hold a pending application rather than block inside a helper.

The completion rule is the part worth remembering. Three weaker rules
each look right and each publish a revision the next reader will not
find:

* a successful submission says only that the batch was taken;
* a flush that did not fail says only that *something* was lowered --
  the pipeline is shared, so a flush routinely carries other work's
  events;
* any event naming the barrier includes `JournalDurable`, which on the
  journal-first path means the record is safe and the state is not yet
  readable.

Only the matching barrier's own `Materialized` completes an application.
`JournalDurable` still satisfies a protocol send's durability
prerequisite, which is why a vote does not wait for materialization.

Two consequences were not obvious. A projection that refuses a
materialization produces **no event at all** -- a durable journal record
is never reported as a failed batch -- so "no event for my barrier and
nothing queued" had to stop meaning "corrupt"; `Persistence` gained
`unmaterialized()` so an owed materialization is distinguishable from
nothing coming. And completion is bounded: a projection that keeps
refusing yields to reconciliation rather than spinning, because the
record is durable either way and the caller is owed an answer.

The negative control needed care twice. Reverting the rule to "any event
for the barrier" first passed, because the test had consumed the
`JournalDurable` itself before calling `complete`; the test now lets
`complete` do the lowering that journals the record. And the
"unknown-outcome failure" branch is unreachable through either
coordinator today -- both emit only `DefinitelyNotCommitted` -- so it is
held against a stand-in implementation of the seam rather than left as
an untested claim.

**Revisit when:** task-j05 qualifies this under real filesystem and
power-loss faults, which is where the composition's durability claims
are actually established. This is integration evidence and not that.

## The packet simulator is sensitive to machine load, not to its input

**Where:** `coord-transport-sim/tests/packets.rs`,
`loss_reorder_duplication_and_mtu_schedules_reproduce_and_keep_the_visible_outcome`
(task-32).

**Expected:** a deterministic packet-level simulation reproduces, so it
either passes or fails on its input.

**Actually:** it failed 3 times in about 8 full-workspace `nextest` runs
while a `cargo xtask ci` build was running concurrently, and passed 6/6
in isolation and 4/4 on an otherwise idle machine. The schedule is
deterministic; the surrounding quinn endpoints are not, because they use
real timers, and under enough concurrent CPU load a handshake or
idle deadline elapses before the scheduled packets get there.

**Did:** characterised it and left the test alone. It is not skipped,
disabled or quarantined: it is a real test of real code and it passes
when the machine is not saturated.

**Revisit when:** it fails on CI. The fix is to give that test's
endpoints deadlines proportional to the simulated schedule rather than
to wall-clock defaults, so a slow machine slows the test instead of
failing it. Re-running it is not the fix, and neither is a retry
annotation: both hide the case where the sensitivity is a real
regression in the transport's own timing.

## The Rust transport had no client-side unary request

**Where:** `coord_transport::Transport::send` (the `Class::Api` arm of
the sender loop), `Responder::respond`, `TransportEvent::ApiDelivery`.

**Expected:** a smoke test could drive `coordd` with the Rust
`Transport` as a client -- connect, send a request, read the answer.

**Actually:** they do not compose. The sender opens a bidirectional
stream and drops the receiving half
(`peer.conn.open_bi().await.map(|(send, _recv)| send)`), while
`Responder::respond` writes the answer on that same stream. The client
therefore never sees it. `ApiDelivery` is not the missing half: it is
read from streams the *peer* opens, which is how a frontend receives
output on a connection it dialed -- server-to-server, not a caller
asking a question.

This is not a defect in either piece. `Transport` is the node's
endpoint, and the caller's shape -- one stream, request written, answer
read on it -- is implemented by the Go client
(`adapters/kine/client`), which is the client surface the Kine edge
needs. Nothing in Rust asks a node a question today: `coord-sdk` is
lifecycle and pooling and does not dial.

**Did:** the daemon's bind smoke test dials with quinn directly and
speaks the caller's shape, which is what the daemon answers on. The test
says so where it does it.

**Resolved (task-j10):** `Transport::request` is that method. It keeps
both halves of the stream it opens, writes the question, and reads the
answer back on it. `send` and `ApiDelivery` are untouched and keep their
meanings, because they are a different thing and not a broken version of
this one: output addressed *to* a node this side dialed.

Three properties are the reason it is a method rather than four lines at
each call site:

* The request is admitted under the destination and node budgets before
  a stream is opened, exactly as a reply is. A caller with many
  questions in flight would otherwise hand window after window to QUIC
  outside the caps the lane exists to enforce.
* The answer is read by the same bounded frame reader as everything
  else, so a response above its kind's class limit is refused as a bound
  rather than truncated and handed to a decoder.
* A question may only be asked on an API-class connection this side
  dialed. On an accepted connection the streams this side opens are
  output, read as a delivery at the far end, so a request written there
  would be answered by nobody.

There is no group to name: a group is what the lane's fair queue shares
capacity between, and a question owns the stream it is asked on.

**What an error means** is the part worth keeping straight. Only that
this caller did not hear back *here*. A timeout says nothing about
whether the node acted, so the invocation keeps its identity and stays
resolvable by it; dropping the future releases this caller's stream and
budget and asserts nothing about the command. The SDK says the same
thing in its own vocabulary -- `Outcome::Unknown`, then a
`ResolveRequest` by identity -- and `coordd`'s tests drive both halves
together against a real daemon.

## Genesis commits to a voter's key, and only a peer was checking

**Where:** `bins/coordd/src/membership.rs` (`place`),
`coord_membership::binder::PeerBinder`.

**Expected:** a node that holds the right name and incarnation is the
voter the configuration names.

**Actually:** genesis commits to the *key* as well, and `PeerBinder`
enforces it -- `is_current_voter_key` compares the presented
SubjectPublicKeyInfo against the manifest. A node holding some other key
starts perfectly well, because nothing it does alone checks it, and is
then refused by every peer it meets. That reads as a network problem and
is not one.

It surfaced as a test fixture whose manifest committed to placeholder
bytes. Every earlier test passed, because none of them connected
anything to the daemon.

**Did:** the daemon checks its own certificate against the committed key
at startup, before storage is opened, and says which is wrong. The
fixture commits to the real key, and a separate case holds the refusal.

**Revisit when:** a key rotation makes the committed key change while a
node runs. The check is at startup because that is where the answer is
knowable and cheap; a rotation that outlives the process is the
membership handoff's business (task-57), not this.

## The peer loop's open question: how a co-located voter receives a submission

**Where:** `coord-daemon/src/fanout.rs`, `bins/coordd/src/serve.rs`.
Not yet implemented; recorded so the decision is made deliberately.

A frontend fans a submission out to every committed voter. In the
reference preview the frontend and a voter are the same process, so one
of those targets is this node itself, and there are two ways to reach
it:

* **Over the wire.** The process dials its own peer listener. Its own
  certificate is a current committed voter, so the binder admits it, and
  nothing else in the stack needs to know. It costs a connection, a
  handshake and a copy of every submission, and it makes a node's
  ability to vote depend on its own network stack.
* **Locally.** `dispatch` recognises this node's replica and hands the
  submission to the local `Node` as `Event::Admitted`, skipping the
  wire.

The design anticipates the co-location (Section 6: "Never count an
identity twice, including when frontend and voter are colocated") and
requires that "role readiness and budgets remain separate even when
colocated" (Section 22). Neither settles the delivery path. What they do
settle is the safety property either way: evidence is counted by voter
identity, so a submission that reached a voter twice -- once locally and
once over the wire -- still yields one vote.

Local delivery looks right and is what the second sentence of Section 6
implies is normal. It is written here rather than done because it
changes what `Dispatched` reports (a target that was reached without
being sent to) and because "the frontend can vote for itself without a
network" is the kind of shortcut that should be a decision rather than
an implementation detail someone finds later.

**Resolved:** local in-process delivery, through the voter's normal
bounded ingress (task-j08). The frontend cannot vote; a co-located voter
can contribute its ordinary vote without a network hop, and requiring a
QUIC connection to itself would make a node's ability to vote depend on
its own network stack for no gain.

The constraints that make it a transport optimization rather than a
consensus shortcut are in the plan's task-j08 and hold in the code:

* The local destination is a capability the voter runtime owns, bound to
  the voter instance and its *committed* identity. `Ingress::new` exists
  only for a replica the configuration names as a voter and takes its
  incarnation from the configuration, so a process running no voter
  never has one, and a request naming a replica id cannot conjure one.
  `dispatch` re-checks on every submission, which is what makes a
  superseded runtime fall back to the wire.
* Both routes converge at `Voter::on_submission`, before anything
  protocol-shaped happens: the same bounded `FrameReader` with the same
  class limits, then the same `admitted_from_submit`, which mints the
  receipt only after checking the submitting role.
* The voter's evidence carries `PeerProvenance::from_local_voter`, built
  from its committed identity, and enters the collector's ordinary
  `on_evidence` path. There is no `self_vote` flag and no pre-counted
  acknowledgement, which is why one co-located voter is not a quorum of
  three.
* Delivery is a bounded mailbox the voter drains on its own turn, not an
  inline call. The remote fan-out happens before the local offer, so a
  full local ingress cannot hold up the voters that could have taken the
  frame.
* `Dispatched` says `queued_local`, `queued_remote` and rejections by
  reason. Nothing it reports implies a vote, durability or an applied
  command.

## Nothing writes the session row a command authorizes against

`coordd` can now carry a request from a caller through consensus,
materialization and back. The answer it gives is a *rejection*:
`ErrRejected { reason: SessionInvalid }`.

That is the correct fail-closed answer, and it is not a bug in the path.
The session row execution authorizes against is replicated state, and
the only things that write one are `coord-storage`'s test fixtures and
the bench driver (`bootstrap_session`). No production path establishes a
session, so no cluster this binary starts can execute a mutating
command.

It is separated out as **task-j09** rather than folded into task-j08,
because it is a different question -- how a verified binding becomes a
replicated session -- and because task-j08's own gate does not depend on
the answer: a request that is carried all the way to a replicated
refusal has been through every stage that matters here.

Two things in task-j08's acceptance tests were limited by it, and said
so where they were: the served-request test asserted the shape of the
answer rather than a mutation, and the restart test held that the same
invocation is answered the same way without holding retry resolution
from the durable record. Both now hold what the plan asks for --
task-j09 landed, and the sections below are what it turned out to need.

### What task-j09 needed, and what was decided

It was not a wiring job. `InternalCommand::ConsumeAdmission` existed and
wrote the row (task-18), but nothing replicated an internal command:
`plan_internal` and `build_internal_view` were reached only from tests
that applied straight to a store, and every production path from a
submission to execution was typed to a client request.

The task's own constraint decided most of the shape:

> the claims are verified outside replicated execution, the receipt is
> minted there, and **nothing about the session is asserted by a
> command's payload**.

That rules out the obvious implementation -- a `CanonicalOperation`
variant carrying an `AdmissionReceiptV1` -- because then the payload
would assert the session, the principal and the scope, and a payload is
exactly what a client controls. So `CanonicalOperation::ConsumeAdmission`
carries **nothing**: it names one action, and everything about the
session comes from the admission travelling beside the payload.

**The open question was what the receipt may carry**, and it was put to
the plan's author. The answer, which the implementation follows
literally:

> The trusted authentication/admission boundary may attest the
> principal, trust-rule ID and generation, scope ceiling, and
> credential-validity bound associated with a session, provided these
> are derived from verified credentials and configured trust -- not
> from the command payload. [...] The approved boundary is: the
> verifier attests who was authenticated, under which trust rule, and
> within what limits; the replicated state machine decides what session
> and permissions actually exist.

Six things follow from it, and each is a place the code says so.

**1. Attesting is not identity authority.** `AttestedEstablishment`
(principal, trust rule, credential deadline) is a *separate* shape from
`AttestedAdmission`, not three optional fields on it. A submission
receipt must not be *able* to carry a principal: if it could, every
principal that may submit would be one field away from being an
identity issuer, and the check that stops it would be a runtime
condition rather than a type. `coord-session` joins the
`VerifierToken::for_boundary` allow list because it is the
authentication boundary; consensus and storage do not, and do not need
to.

**2. Establishment is not ordinary admission.** `AdmissionPurpose` is
part of what the verifier attests, it is checked at the ingress against
the role's authority (`may_establish_sessions`, which is `Frontend`
only, versus `may_submit_for_clients`), and `Applier::apply` chooses
the planner by the *accepted admission's purpose* rather than by the
operation. A domain-scoped Kine collector can send a `Submit` and still
cannot originate an identity.

**3. The admission is part of what the command is.** `PayloadRecordV1`
carries it, so a replica that executed the command, one that recovered
its payload from a peer and one that replayed it from the journal all
execute the same command. `FastAck` and `SlowAck` carry its digest and
the command table binds it beside the identity, so two voters cannot
accept one command as different session-creation facts. The command
*identity* is untouched -- a retry under a rotated credential is still
the same command -- which is exactly why the facts had to be bound to
the acknowledgement instead.

**4. Expiry is an admission constraint.** `CredentialDeadline` is whole
seconds of the issuer's clock, checked once at the boundary. No replica
reads a clock while applying and recovery does not revalidate; a
command admitted before the deadline may finish after it, and what
denies execution later is an ordered retirement or revocation at its
own position.

**5. Current replicated policy decides.** `plan_internal` rechecks the
trust rule and its generation, refuses a consumed receipt, refuses a
session identity that already exists, and consumes any grant
commitments -- all in the one batch that also writes the session row and
the retry binding.

**6. The replication path was built, not bypassed.** The bind holds its
stream until the command is established, so no credential is usable
before the session durably exists.

### A domain has to trust something before it can trust anyone

Session establishment made a circularity visible that nothing had run
into before. A session exists only under a trust rule replicated policy
holds enabled; permission is allow-only and a fresh domain allows
nothing. So the first command that would write a trust rule or a
permission needs a session to be authorized, and that session needs the
rule.

`coordd init` therefore writes the domain's **genesis policy**: the
trust rule its configured issuer signs under (`[sts] trust_rule`) and
the permissions its genesis grants (`[[grant]]`). Every replica writes
the same rows from the same configuration, as it does the genesis
membership, and the batch carries no application base and takes no
execution position -- it is initial state, not an execution. Everything
after it is ordinary replicated administration.

The identities in that configuration are row keys, so they are parsed
strictly: exactly 32 lowercase hex characters, or the node refuses to
start. A spelling that differed between nodes would be a different row
on each, and the divergence would surface as a session that exists on
some replicas and not others rather than as a bad configuration.

### A retry after a restart was new work to the collector

The last defect task-j09 found, and the one that was invisible until a
command actually executed.

The collector's retained results live in one process's memory. After a
restart, a client's ordinary retry of a command the cluster had already
executed was new work to it: it was proposed a second time, executed,
and the applier handed back the outcome **at the position the command
already had** -- which is not the position the restarted replica's
learner was waiting to fill. The replica halted with a position
mismatch and the caller got no answer at all.

Before task-j09 this could not happen, because every command was refused
at execution with `SessionInvalid` and a semantic refusal deliberately
records nothing under the retry key: there was no retained record to
hand back, so the second trip re-derived the same refusal at the next
position.

The fix is at the frontend, not in the learner: a request whose
invocation this domain already holds a durable record for is answered
from that record *before* anything is submitted, gated on the way out
exactly as a fresh result is. Loosening the learner instead was tried
and reverted -- accepting any outcome at or below the execution
frontier would have made a replica whose durable records were lost
accept re-proposed commands as already executed, which is precisely
what `a_deliberately_omitted_durable_record_is_detected` exists to
catch.

## A command that was never speculated was never released

Driving a real request through `coordd` found two places it could not
get to an answer. Both were invisible to every test that stopped short
of a whole request, and both are worth remembering for the shape of the
mistake rather than the fix.

**The release gate only released speculated outcomes.**
`Leader::applied` emitted `Effect::Established`, which the driver
deliberately does not publish (publishing it would disclose an outcome
before the release rule had admitted it), plus `release_ready()`, which
releases *tentative* outcomes from the speculation overlay. A comment in
the driver said that without speculation "a result is released on the
final path rather than the early one -- slower, never wrong". There was
no final path. A command the speculation companion declines -- one that
is not speculable, one over the overlay's budget, one whose authorized
view cannot be built -- executed, became durable, and left its caller
waiting for a disclosure nothing was going to assemble.

The fix is a release from `applied` when the speculative one was not
made, marked `speculative: false`. It rests on *more* evidence than the
early release: the command is committed, its whole prefix has executed
before it, and its result is materialized and durable. `AppliedOutcome`
carries the response bytes for it -- the digest sealed the result, but
the bytes are what a caller gets.

The existing test asserted that a declined command was "never released
early", which was true and also true of a command that was never
released at all. It now asserts released exactly once, not speculative,
not before execution, equal to what was established.

**An application outcome that changed no rows could not be recorded.**
The journal's `check_updates` refused an empty update set for every
record body. A protocol transition with no rows really is a record of
nothing, and is still refused. An application outcome is not: it carries
the position the command took, and a command that legitimately changes
nothing -- a rejection, a comparison that did not match -- still took
one. Refusing it failed the whole replica with `EmptyUpdates`; accepting
it without recording it would free the position for a successor and let
two replicas disagree about which command holds it.

The two faults compounded: the first request `coordd` ever served was a
policy rejection, which is exactly the command that changes no rows and
exactly the command the speculation companion cannot plan.

## A `Hello` declares the lane it opens, not the lanes the endpoint has

The first code in this repository to dial a peer from a real node found
that it could not: every dial was refused with `lane NotExactlyOne`.

`negotiate_outgoing` built its `Hello` from the endpoint's own
capabilities plus the lane being opened. That is right for an endpoint
whose granted capabilities are not lanes, and wrong for every real node:
a voter's endpoint grants control *and* bulk, a collector's grants
three, and a `Hello` carrying two lane capabilities is refused outright,
because a peer that saw two would have no way to tell which stream is
which.

It had never shown. The transport's own tests grant `[1, 2]`, which are
not lane capabilities, and the daemon's api endpoint only ever accepts.
Dialling is what exposed it.

The endpoint's other lanes are now filtered out of the outgoing `Hello`.
The regression test grants a voter's real lane set and requires the dial
to be accepted.

## Two planes, two connection-identity spaces

A `ConnectionId` is allocated per `Transport`. A node that serves an api
plane and a peer plane has two of them, so an api connection and a peer
connection can share a number.

One handler for both looked reasonable and was not: a peer link closing
at id 1 made the frontend forget a caller's binding at id 1, and a
caller's close was swallowed as a peer's. The planes have separate
handlers now, and which plane an event arrived on is carried from the
`select!` arm that produced it rather than inferred.

## A process that serves clients and votes is two principals

**Where:** `coord-transport/src/config.rs` (`ClientIdentity`),
`coord-daemon/src/identity.rs`, `bins/coordd/src/serve.rs`.

A node certificate binds exactly one role: `PeerBinder` refuses a
`Hello` that declares any role other than the certificate's own
node-identity URI. So a `coordd` running `role = "voter-frontend-..."`
holds a voter certificate and, with only that, cannot present itself as
a collector to anybody.

That matters because a submission is the *collector's*, not the
voter's. It carries admission claims minted for a client's session, and
`admitted_from_submit` admits it only from a role that may act for other
principals. Two ways out were available and only one of them is a
design:

* Let a voter submit on a client's behalf. That would give every voter
  the authority to mint admission claims for any session -- a second,
  weaker way into the protocol, which is exactly what the co-located
  route was written to avoid.
* Let the process present the credential of the principal it is acting
  as. A node certificate when it votes; a collector certificate when it
  submits. What it *serves* as is the node either way, because a caller
  validating this node's server certificate is talking to the node.

The second. `LocalIdentity::api_client` carries the credential the
endpoint presents when it *dials* an API-class peer, which the endpoint
already had the shape for: `client_tls[0]` and `client_tls[1]` were
always separate, one per class. The configuration names it as
`identity.collector_certificate` and `identity.collector_key`; naming
one of the two, or neither where the process serves clients and has
other voters to submit to, is refused at startup rather than at the
first request it could admit and then not deliver.

Nothing commits the collector certificate the way genesis commits a
voter's key. It does not need to: what it proves is that this domain's
issuer said this process may act as a collector, and the receipt is
still minted at the collector boundary on the role the certificate
carries.

## One listener, one plane, one ALPN

**Where:** `coord-transport/src/endpoint.rs`, `LocalIdentity::serves`.

Every endpoint advertised both ALPNs. The design says an address is a
hint and the planes negotiate different protocols, so a dialler that
guessed the wrong one of a node's two addresses fails to negotiate and
tries the next -- but with both advertised everywhere, it *succeeds* on
the wrong listener instead.

That is not a routing inconvenience. The two planes' events are read in
different places: `Domain::on_transport` serves the api plane and
`Domain::on_peer_plane` the peer plane, and each counts the other's
events as unserved. A submission that negotiated on the peer listener is
accepted, framed, delivered -- and then dropped, while its sender waits
out a deadline for an answer nobody will give.

A listener now offers only its own plane's ALPN, which is what lets
`EndpointV1::addresses` stay one unlabelled list. That answers the
smaller question left open by the previous note: the catalog does not
have to say which address is which plane, because dialling settles it.

## A collector's submission is a raw kind whose admission depends on who is asking

**Where:** `coord-types/src/wire_v1.rs`, `coord-transport/src/endpoint.rs`.

`Submit` is `0x0103`, in the API kind range, and its payload belongs to
`coord-collector` -- so `wire_v1::decode` does not know it, and
`read_request` closed the connection as malformed. The daemon's
`frame.kind == KIND_SUBMIT` branch could never be reached; the three
voters' links were up and every submission over them killed the link it
arrived on.

The session binding had the same shape and was already handled: an
enumerated raw kind, admitted by kind and version. A submission is the
second, with one difference. A binding may be opened by anyone -- it is
how a connection becomes authorized at all. A submission carries claims
minted for somebody else's session, so the stream is open only to a role
that may act for other principals, checked against the *bound*
certificate before the frame is read.

That rule now lives once, as `PeerRole::may_submit_for_clients`, with
`coord_collector::ingress::is_collector` delegating to it. Two copies of
"who may submit" is how the transport and the collector boundary come to
disagree, and the disagreement would be in the permissive direction at
whichever of them was updated last.

## A sender never asserts a receiver's incarnation

**Where:** `bins/coordd/src/serve.rs` (`addressed`).

`PendingSend.to` is a `PeerId`, and the machines fill its incarnation
with `ReplicaIncarnation::ZERO`. That is deliberate and documented on
the field: a sender does not know which generation of another node is
current, and what binds an incarnation is the receiver's own
certificate, checked when the link was bound.

`Transport::send` keys links by `(replica, incarnation)`, so passing the
open value through addressed a generation that exists nowhere and every
protocol frame came back `NotConnected`. The three voters held their
links, admitted submissions, produced evidence -- and no proposal, vote
or adoption ever left any of them.

The runtime resolves it against the committed configuration on the way
out, which is the same rule `fanout::dispatch` follows for a
submission's targets, and refuses to send to a generation the
configuration has replaced.

## A peer frame is framed by the transport, not by the machine

**Where:** `bins/coordd/src/serve.rs`.

`PendingSend.frame` is the consensus message's own encoding;
`KIND_PEER_EVIDENCE` is the transport's wrapper around it, and
`read_uni` refuses anything else. The daemon sent the bare message
(refused at the far end) and, on receipt, re-framed the payload before
handing it to the machine (`ProtocolMessage::decode` then failed on the
header). Two mirror-image mistakes that cancelled out in every test that
did not actually run two processes.

## Both ends of a peer pair dial, and both have to keep the same connection

**Where:** `coord-transport/src/endpoint.rs` (`Shared::survivor`).

A lane holds one connection. When a second arrives for the same link,
`register` displaced the incumbent and closed it -- "a redial replaces",
which is right for a client that reconnects and wrong for a mesh.

In a full mesh both ends dial, so each pair produces two connections and
each end sees one of them as an incumbent and the other as a newcomer --
*opposite* ones. Newcomer-wins is symmetric only in appearance: each end
closes the connection the other kept, both connections die, and both
sides go on believing they are connected because their own dial
succeeded.

It presented as a flaky three-voter test: whichever pairs happened to
collide lost their link, so a request sometimes found a quorum and
sometimes did not. The transport's own tests never saw it because they
dial one way, or one after the other -- with a gap, the second end has
no incumbent to choose against and there is no choice to get wrong.

The surviving connection is now the one whose *dialler* has the lower
replica identity. Both ends compute it from the same two identities, so
exactly one connection is closed. `LocalIdentity::replica` exists for
this and for nothing else: it is not an authority over anything, and an
endpoint without one keeps the replace-on-redial behaviour, which is
what an API endpoint (whose links are per-connection and never collide)
wants.

A consequence worth knowing: the dial that loses is closed, so its
caller sees an error for a link it now holds. "Reachable" therefore has
to mean *a connection is holding the lane* (`Transport::linked`), not
*my dial succeeded* -- otherwise a node reports a link it no longer has
and misses one it has only because the peer dialled.

## What the three-voter served request needed

Three processes, three committed certificates, real QUIC, and a request
the caller sends to one of them established by all three. It took two
pieces of routing and five defects, each of which is its own note above.

The routing:

* **A collector reaches a remote voter over the *api* plane**, as an
  API-class client of it -- the peer plane is voter-to-voter protocol
  traffic, and a `Submit` is not that. `CollectorLinks` dials each
  voter's api listener as `PeerRole::Frontend` on the unary lane, which
  is what gives `fanout::dispatch` a link to find. It shares the api
  endpoint rather than having its own, because a connection's direction
  already says which kind of peer is on it: a stream a caller opens is a
  request to serve, and a stream a voter opens on a link this process
  dialled is that voter's evidence.
* **A remote voter's evidence returns to the collector that
  submitted.** `Voter::origin_of` remembers which connection each
  command was admitted from -- bounded, oldest forgotten first, because
  a collector that submitted and vanished must not cost a replica memory
  for ever -- and `Domain::hand_to_collector` reads the command out of
  the evidence frame and sends it back there. Falling out of the bound
  costs a caller its evidence and nothing else: the command is
  established and durable by the quorum rule either way.

Not a shortcut for the co-located voter: it is reached through its
bounded ingress instead of a socket, and contributes exactly one voter's
evidence through the same collector validation. The three-voter test is
what says so -- one process holding one vote of three cannot answer a
caller by itself.

**Still missing:** there is no timer loop, so nothing retransmits and
nothing re-dials a link that dropped. Every message here is carried by
QUIC and arrives, and every link is established before the request, so
the composition holds; a cluster that loses a link mid-request does not
recover until something re-dials it. `Outbound::arm` and `cancel` are
counted as unserved, which is where that shows.

## A node's journal grows until something local decides it should not

The journal is the record, so nothing in the protocol ever tells a
replica it may forget a prefix. `C <= M <= J` says what *may* be
retired; what makes it happen is a node deciding to image its own
storage, and that decision is not a replicated one. Two replicas of the
same domain may publish at completely different rates, or one may never
publish at all, and every replicated result is the same either way.

So the trigger is a local setting -- `limits.checkpoint_after_records`,
the number of durable records a domain may hold past its baseline --
and the work happens between serving turns:

* `Domain::maintain` runs every turn and is almost always one
  comparison. `J - C` is a number the store already has; only crossing
  the bound spends any I/O.
* It is deliberately not counted as progress. A publication is not work
  a caller is waiting for, and a loop that treated it as progress would
  keep itself awake to do housekeeping.
* Nothing in the cycle can fail the node. A failed export, a refused
  pointer or a failed compaction leaves the previous baseline selected
  and the journal holding more history than it needs, which is the safe
  direction: the cost of not publishing is disk, and the cost of
  publishing something unloadable would be the prefix that proved it.
  Failures are counted and printed rather than swallowed -- a node whose
  checkpoints have been failing for a week is a node whose disk is
  filling.
* A failure raises the gap the next attempt needs. Without that, a node
  that cannot publish -- a full disk being the obvious way -- would
  attempt a complete export on every turn for as long as the condition
  lasted, which is the one shape of housekeeping that can make an
  incident worse.

`LocalBaseline` is the seam. Publishing needs the filesystem and the
image format, and the storage coordinator is deliberately ignorant of
both; the trait is implemented for `JournaledDomain` in
`coord-checkpoint`, which is the one crate that can see all five steps.
What it images is `M` and never `J`: a durable record the projection
still owes is an obligation this node has taken on and does not yet
hold, and an image claiming it would be a baseline missing what its own
pointer promised.

### The baseline is read before the projection is attached

`open_storage` reads `recovery_baseline` from the journal and *loads*
the image it names before attaching the domain, and refuses to start if
that fails. Two reasons, and the second is the one that matters:

* What the baseline answers is *which projection to attach* -- the live
  one, or a fresh generation installed from the image. Asking after
  attaching would be asking too late.
* An image this node published and can no longer load is the loss of a
  durable prefix. Discovering that at the moment a recovery needs it is
  discovering it at the worst possible time, so it is a startup refusal
  instead.

A `.pending-` directory is not an image. Stopping a node mid-write
leaves one, nothing selects it, and the next publication reclaims it
along with the superseded images.

**Still missing:** the export runs inline in the serving turn that
triggered it, so a very large domain pays its whole image in one turn.
Bounded, resumable export is task-j06's, and the trigger is a local
setting precisely so an operator can keep the bound low until it exists.
Installing a selected image into a fresh generation is written and
tested (`install_local`), but nothing on the serving path chooses to do
it yet: a node whose projection is intact attaches the live one, and
recovery *from* the image is what task-j05 qualifies under real faults.

## A promise is not a copy, and that is the whole floor protocol

Task-51 trims when every configured voter has acknowledged the same
checkpoint. Task-53 trims when a majority has *promised* about it, and
the difference between those two verbs is where all the difficulty is.

`CheckpointAckV1` says "I have these bytes". It was always documented as
possession and never as authority, and that turned out to be exactly
right: a certificate built from possession binds nobody. The holder
crashes, comes back with its old baseline, and votes from history the
cluster has already agreed to forget -- and every other voter's
acknowledgement was correct throughout. `CheckpointReadinessV1` says "I
have these bytes, and I will never again vote from below this boundary",
and it is durable before it is told to anybody, because a promise that is
not durable is not a promise.

Two rules make the rest work, and neither is obvious from the outside.

**A voter never promises about two checkpoints at one boundary.** That
single refusal, in one voter's own ledger, is what makes at most one
subject per boundary certifiable anywhere: two certificates would need
two majorities, and two majorities of one voter set intersect in a voter
that would have had to promise twice at one position. Without it the
bounded model finds two majorities certifying different state at the
same executed prefix in a three-voter configuration.

**A recovery reads promises, not certificates.** A signer promised before
any certificate existed and keeps the promise whether or not it ever saw
one, so the promises are the evidence that is actually guaranteed to be
there. `recovery_obligation` therefore refuses to answer from fewer than
a majority of distinct voters: that refusal is the protocol, not caution.
A narrower read can miss the highest floor entirely, and the replica that
missed it would vote from a discarded baseline while believing itself
current.

### What is deliberately *not* part of a floor

A ballot, and its leader. A floor belongs to a configuration and outlives
every term in it; requiring the leader would bind a durable cross-ballot
fact to a leadership that changes underneath it, and it would buy
nothing, because the intersection that makes discovery work is between
two majorities of the same voter set and those intersect whoever leads.

### What task-51 keeps

Everything after the floor exists. `ActivatedFloorV1::trim_floor` yields
the same `TrimmedFloorV1` the all-voter path publishes, so the fence, the
bounded survey, the pinning rules and the deletions are one
implementation with two ways of authorizing it. The all-voter path is
untouched and still available; what a deployment chooses between them is
an availability decision, not a correctness one.

**Still missing:** nothing proposes candidates or collects promises over
the wire yet, so a floor is certified from readiness a caller already
has. Wiring the prepare round into the peer plane, and fetching the
checkpoint a `RecoveryObligation::Install` names, are the parts task-57
and the membership workstream need and are not yet built.

## The dangerous part of a membership change is the coordinator dying

A sealed handoff is a long operation with a single driver, and the
failure that matters is not the driver crashing -- that is expected --
but the replacement believing the wrong thing about how far the last one
got. Everything in `coord_consensus::handoff` is arranged around that
one sentence.

`Evidence` has no lifecycle label and deliberately no place to put one.
Every field is a record some replica made durable: stances, terminal
reports, the certificates, installations. `resume` returns a stage only
when one of those justifies it, and the order it checks them in is by
how far the transition demonstrably got -- highest first, because a
later record implies the earlier ones happened and the reverse is never
true.

Three things are structurally impossible rather than checked for:

* **A fence cleared by a retry.** A voter records one stance per
  transition and never reverses it, so a seal and a cancellation cannot
  both certify. `resume` has no path from any fence back to `Stable`:
  not a partial one, not one whose transition was later cancelled
  elsewhere. The voters that sealed will not vote in the old
  configuration again whatever a coordinator decides, so a coordinator
  that returned to `Stable` would be describing a cluster that cannot
  serve.
* **A second successor.** The terminal certificate is selected after the
  seal by a majority of the old voters agreeing on one root. Two
  selections need two majorities, which intersect, and the intersecting
  voter reported one root.
* **Terminal state without a fence.** `select_terminal` takes a
  `SealCertificate`, so there is no selection before the fence. This is
  the subtle one: before sealing, an old voter can still accept work, so
  what it reports as terminal is a state the old configuration may
  already have moved past. An applied KV view, a closed frontend or a
  vanished client is not a fence.

### A fence belongs to the configuration, not to the transition

The model found this. A voter that sealed for transition B is fenced,
and a coordinator driving transition A that only collected A's stances
would see nothing and resume as if the domain were idle. So
`stances_of` is deliberately unfiltered and `resume` refuses with
`FencedByAnother`: the domain permits one transition at a time, and the
recorded one is the one that must be finished. A cancelled transition is
different -- it releases the domain, and a voter that cancelled may
record a stance for the next one.

### What the model does and does not say

Every assignment of a stance script to each of three old voters, crossed
with five points at which the coordinator can die: 1715 worlds, all
eight stages reached. The invariants are checked against everything that
was ever recorded, not against whatever the rows hold at the end -- a
certificate formed from an earlier row does not stop existing when the
row changes, and a model that only looked at final state could not tell
a rule that forbids clearing a fence from one that clears it quietly.

**Still missing:** this is the model, not the implementation. Where
stances live, what the terminal root is computed over, how a successor
is staged and how any of it travels between nodes are task-55, task-56
and task-57. Nothing here is a proof; it is a regression of the rules
and of the five counterexamples they exist for.

## A seal is a promise rule applied to a fence

Task-55 turned out to need almost no new machinery, which is the useful
finding: sealing a configuration is the *same shape* as promising a
ballot, and the places it differs are exactly the places it must.

The same: the row is persisted first, and the report is published
through the logical outbox requiring that row **and** every batch
submitted before the cut. That is the Section 4.8 rule, and it is what
"work learned immediately before sealing survives even with a delayed
response" means operationally -- a report built before the outstanding
batches resolved would omit an obligation this replica had already taken
on, and an initiator counting a seal whose row was not yet durable would
be counting a fence a crash could remove.

The differences, and why each one is the way round it is:

* **A promise is about one ballot; a seal is about all of them.** Once
  the row is there, `on_new_leader` refuses ballot 1, ballot 2 and
  `u64::MAX` alike. A higher ballot is not an exception to a fence; it
  is the thing a fence exists to stop.
* **A promise moves up; a seal does not move.** There is no method that
  clears one. Not on a timeout, not on a missing reply, not on a retry
  from a competing initiator, and not by recovering without it --
  `recover_sealed` takes the row, and a replica that reads no row simply
  has no seal, which is a different fact from the transition having been
  cancelled. A cancellation is a quorum's fact (task-54); one replica's
  absent row is not evidence of one, and nothing here turns it into any.
* **A failed row is an answer, not a cancellation.**
  `PromiseOutcome::SealFailed` says this replica did not fence and may
  be asked again. Silence would have been the dangerous shape: an
  initiator that could not tell "refused" from "no reply" would be
  tempted to infer one from a timeout.

### Where the row lives, and why that tag

`protocol_v1` keys are `epoch || tag || rest`, and the seal takes tag
`0x04` -- immediately after the Sync tag. That is not arbitrary: a trim
step surveys an epoch's rows up to and *excluding* `SYNC_TAG + 1`, so
the floor epoch's own seal is outside every step's range by
construction rather than by a rule someone has to remember. An earlier
epoch's seal is inside the range and is retained deliberately, for the
same reason a promise row is: a forgotten seal is an old configuration
serving again.

**Still missing:** the coordinator side. `SealRequest` is handled by
both machines and `Sealed` is carried, but nothing yet drives the
request to a quorum, counts the reports into a `SealCertificate`, or
does anything with the terminal state afterwards. Selecting the terminal
certificate is task-56 and activating the successor is task-57; task-54
already says what each of those may conclude.

## Make the disagreement impossible rather than detectable

The terminal certificate binds everything the successor inherits into
one root: the closed boundary, the shared checkpoint root of the common
state, a digest of the selection at the seal cut, the floor lineage, and
the exact successor incarnations. That last one is the interesting
choice, and it is what the acceptance criterion "racing successor sets
cannot both obtain authority" turns into.

The alternative design is a certificate that names the successor beside
the state and a rule that checks the two candidates agree. That rule has
to live somewhere, be called on every path, and be right about what
"agree" means. Binding the successor into the root instead means two
coordinators proposing different successors produce different 32-byte
roots, so the old voters' reports simply do not form a majority for
either -- `MixedTerminal`, from the same code that rejects a disagreement
about the KV boundary. There is no successor-comparison rule to forget
to call.

The same reasoning covers the rest of the acceptance list. "Partial or
mixed-root evidence rejected" is not a check on partiality; it is what
happens when a majority cannot be found for one root. "Latent old
completion remains represented" is not a scan for stragglers; the
closure digest covers the selection, so a terminal state that dropped a
command chosen immediately before the fence is a different terminal
state and no majority reports it.

### Stability across a restart is a property of the row

`publish_certificate` accepts the identical certificate and refuses any
other for the same transition. That is the whole of "selection stable
across restart": a replacement coordinator that recomputes -- from a
different subset of reports, or at a different moment -- writes the same
bytes or is refused. It cannot produce a second destination, and it does
not need to know whether it is the first coordinator or the fifth.

**Still missing:** nothing asks the old voters for their
`TerminalStateV1`s and nothing gives the successor the state the root
names. The certificate is the decision; carrying it out is task-57.

## Recovering every phase means never remembering which one you are in

The handoff test that matters runs the whole transition over a store and
kills the coordinator after every durable step. What makes it pass is
that the loop never tracks where it is: the stage comes out of the rows
each time, through `LocalEvidence::read` and task-54's `resume`. Sealed
alone resumes at terminal recovery; a published certificate at
installing; one installation of three still at installing; two at
activating; the published activation at served.

That shape is worth the trouble because the alternative -- a coordinator
that knows its own phase -- is correct exactly until it is replaced, and
being replaced is the situation the whole protocol exists for.

Two rules keep the phases from being forgeable:

* **An installation record comes from a receipt.** `record_install`
  takes the `InstalledCheckpointV1` of the install that produced it and
  requires its verified root and boundary to be the certificate's. A
  coordinator cannot write one on a replica's behalf, and a replica
  cannot write one for state it does not have. "The new quorum installs
  identical terminal state" is then a fact about what is on the disks,
  not a message anybody sent.
* **The successor set comes from the certificate.**
  `activate_successor` reads it out of the certificate rather than
  taking it as an argument, so there is no call site that could activate
  against a different successor than the old quorum certified.

`publish_handoff_activation` accepts the identical activation and
refuses any other, so a coordinator retrying after a lost reply is a
no-op rather than a second grant of authority. The same pattern as the
terminal certificate, for the same reason.

**Still missing:** the wire. Nothing asks the old voters for their
terminal states, moves the checkpoint to the successor, or tells a
replica to install. Every durable decision of the handoff now exists and
is recoverable; driving it between nodes is the membership workstream's
(task-m03), and task-58 onwards is what the successor's credentials have
to look like for any of it to be safe in the field.

## A credential's end has to reach the connection it opened

task-58 is the node-credential lifecycle: proactive renewal with
jitter, bounded overlap during a rotation, warm-session expiry, staged
CA rotation, the committed key/generation replacement, and the
operator-facing `coordd inspect`. Most of it is arithmetic in
`coord-node-issuer::lifecycle`, and the arithmetic is uninteresting
except for what it refuses to have: there is no `Renewal` variant that
means "serve anyway". Availability during an issuer outage is bought by
turning due at two thirds of the lifetime, not by softening the
deadline, and `an_issuer_outage_is_survivable_until_the_deadline_and_never_past_it`
walks the whole outage hour by hour to show the window is real and that
it closes.

The part that needed building rather than computing is the connection.
Certificate validation happens once, at the handshake. A connection left
alone outlives the credential that made it, and then renewal, rotation
and revocation all stop reaching the peer that already got in -- the one
peer they most need to reach. So `IdentityBinder` gained
`expires_at(certs)` and `Limits` gained `max_connection_age`, and the
transport closes a connection at the earlier of the two with
`CloseReason::Expired`.

Asking the *binder* rather than parsing the certificate in the transport
is the deliberate part: the component that decides a credential is
acceptable is the one that says how long it stays so, and a second
implementation of "when does this end" would be a second set of rules
with the weaker one deciding. The default returns `None`, which leaves
the age cap -- a weaker bound, never an absent one.

Expiry is not a refusal. Both ends hold the same deadline, so which one
closes first is a race; what is not a race is that the peer reconnects
immediately under whatever it holds now and is admitted on that. A
transport test asserts exactly that, because a close that fenced the
node would be a much worse bug than one that did not happen.

### One rule for "is this credential the voter's", named cases

`Membership::classify_credential` replaced an open-coded pair of checks
in the peer binder. It returns `Renewal`, `UncommittedKey`,
`RequiresCommit`, `Stale` or `NotAVoter`, and the binder binds exactly
`Renewal`.

The distinctions do not go on the wire. A refused peer learns that it
was refused, which is the right amount to tell it: "your key is not the
committed one" and "the configuration moved past you" are facts about
this cluster's membership, and a caller that could enumerate them could
map the fleet. They surface instead on the node itself, through `coordd
inspect`, because replacing a node is done there.

That placement had a bug worth recording. `inspect` was first wired
*after* `membership::place`, which refuses to continue when the
committed configuration does not name this certificate as a voter --
which is precisely the state an operator runs `inspect` to understand.
The operator-facing answer to "why is this node not voting" has to be
available exactly when the node cannot serve, so `inspect` now runs
before placement and starts nothing: the test asserts no store and no
journal are created, because a node whose credential is wrong must be
inspectable without first being repaired.

### An authorized replacement keeps the disk, and that took three layers

"Preserve journal shard/checkpoint and epoch metadata across authorized
replacement" is one line of the acceptance criteria and was the whole
cost of the task. A voting-key replacement is a committed configuration
transition for the *same* replica: its projection, its journal and its
checkpoints are what the new generation has to come back on, and
refusing them would make every authorized replacement a restore from
nothing. Three separate fences were each keyed on the incarnation.

**The store manifest.** `Generation::adopt` advances the stamp under the
root lock, forwards only; a root stamped *past* the presented generation
is the cloned- or restored-disk case and stays refused, which is the
same stamp read in the other direction.

The first version also wrote the new stamp into the projection
database's identity record, and that was wrong in a way worth keeping a
note about: a redb write transaction commits whatever the previous run
left uncommitted, so the materialized frontier jumped past the journal's
durable head and `attach` quarantined the domain -- the exact shape of a
lost prefix, produced by an operation meant to preserve one. Nothing may
write to the projection outside the journal-first path. The record
therefore keeps naming the generation the database was created under,
and `open_existing` accepts a record that *lags* the manifest and never
one that leads it.

**The journal stream.** `StreamKey` carries the incarnation, and
task-j01's rule was "a new incarnation gets a new stream". That rule is
right for a replica that arrives without durable state and wrong for one
that keeps it: a fresh stream leaves the projection materialized past a
journal head of zero. `StreamAllocator::adopt` re-keys the existing
mapping -- same identifier, same shard, same domain, strictly later
generation, only onto a key that owns no stream -- and
`JournaledStore::adopt_stream` persists it before use. Design Section
17.3.1 now states this; it is an extension of j01's rule, not a
contradiction of it, and the high-water mark never moves because nothing
is allocated.

**The record guards.** A carried-forward stream holds a prefix written
at the old generation and a suffix at the new one, which three guards
rejected. They now agree on one rule: the *mapping* says which
generation the stream currently serves, so appends are verified against
that (not against the stream's first record), and read and replay
verification accept a non-decreasing generation bounded above by it. A
record from a *later* generation than the mapping is still refused --
that is a stream that has moved past this node.

The provenance is not blurred by any of this. Every record carries the
incarnation that wrote it; what changed is that a stream may contain
more than one, which is exactly what "the same replica, re-keyed" means.

Each of the three layers has its own negative control, and each
reproduces a distinct failure: remove the manifest fence and the
cloned-disk assertion fails; pin the append guard to the stream's first
record and attach fails with `IncarnationMismatch`; pin read
verification and the recovery baseline fails to load; pin replay and the
owed suffix is refused. The end-to-end `coordd` test runs a node,
replaces its key, and asserts the same invocation gets the same command
identifier and the same outcome afterwards -- and then puts the retired
credential back and asserts the node refuses to start and `inspect`
names it `state=stale committed=2 presented=1`.

## A restore is the one operation that is allowed to lose things

task-59 is backup, restore and the disaster-recovery runbook. Almost all
of it is refusals, and the refusals are the deliverable: the code that
carries out a restore is a variant of the task-50 install, while the
code that decides whether one may happen is new and is where every
acceptance criterion lives.

The shape is `plan_restore` (a pure decision, reads no store, writes
nothing) and `restore_shared` (carries out exactly the decision). They
are separate so a rehearsal can take the decision and print it --
`coordd restore --plan` -- which is what makes "rehearsals obey explicit
policy" checkable rather than aspirational.

### Four things a restore must not be allowed to be

* **Ordinary recovery.** The restored store is stamped with a
  *successor* cluster identity. Restoring under the source identity is
  refused outright, because callers hold promises made by that name and
  a rewound history behind it would satisfy them incorrectly rather than
  visibly failing.
* **Unfenced.** A restore is safe only once the old cluster cannot still
  be serving, and nothing in this system can establish that: the old
  voters may be partitioned from the operator and perfectly healthy.
  `FencingAttestationV1` is therefore a *record* of an out-of-band
  action, bound to the exact abandoned cluster, the exact successor and
  the exact backup. It is not the fence. What it buys is that skipping
  the isolation has to be a deliberate false statement rather than a
  step somebody forgot, and the runbook says so in those words.
* **A different artifact.** Section 17.16.1's three artifacts are not
  interchangeable, so `Artifact` is an explicit argument and anything
  but `Shared` is refused. A local checkpoint is one incarnation's
  obligations; an observer snapshot may not even be full MVCC; neither
  is a cluster, and neither recreates a voter's local state.
* **Zero-loss.** `RestorePlan::rpo` states the boundary and when the
  snapshot was pinned, and `coordd` prints it before anything is
  decided. The recovery point is the number an operator reconciles
  against callers.

### What "never reuse stale voting authority" turned out to mean

Concretely: the restored store holds no `config_v1` row and no
`policy_v1` row. The configuration rows name the old cluster's voters
and their keys; the policy rows are the authorization decisions made
under them. Carrying either would leave the abandoned cluster deciding
things here. Membership comes from the successor's own genesis
(task-42), and `coordd restore` writes the successor's genesis policy
afterwards exactly as `init` does.

The execution frontier goes the same way. The position and the KV
revision continue, so the new cluster's own history is not rewound
within itself, but the configuration epoch is the *successor's*: the
donor's epoch belongs to a configuration this store deliberately does
not hold.

Leases needed one more step than dropping. A key attached to a lease
whose record is gone would be held forever by an authority that cannot
expire it, so `restore_shared` decodes each restored `kv_current_v1`
row and clears the attachment, counting them into the receipt. Retries
go the other way and are kept: dropping a retained result turns a
caller's retry into a second execution, which is worse than a stale
answer.

### Where the rows are allowed to be written

The restore writes a whole store's worth of rows directly into an
engine, which is precisely what task-58 established must never happen.
The difference is the window: `store::open_storage_with` runs the
restore after the generation is created and *before* it is attached to
the journal, so there are no frontiers yet to violate. That is the same
window the catch-up install has always used, and naming it explicitly
in the storage helper is what keeps it from being reinvented somewhere
it does not hold. Afterwards the projection attaches normally and every
subsequent write goes through the journal.

### The runbook is part of the deliverable

`docs/operations/disaster-recovery.md` is where the isolation step
lives, because the isolation is not code. It names three concrete
actions (revoke at the issuer and let the short-lived leaves expire;
take the addresses away; stop the nodes) and it says what a restore is
not, in the same words the refusals use. A rehearsal that skips the
isolation or restores in place is rehearsing something else -- and both
are refused, so the rehearsal will say so.

## A version is a local fact; a capability is not

task-60 is upgrades, and the useful thing it forced was noticing that
"what version is this" and "what can this cluster do" are two different
questions with two different answers, and that running them together is
how upgrade bugs happen.

**A format is local.** A binary either reads some bytes or it does not,
and nothing about that needs agreement. `coord_types::formats::Format`
is the registry: nine independently versioned formats -- command
identity, wire frame, journal record, journal metadata, store schema,
the two checkpoints, backup, adapter -- each with a decoder window,
which is the range this build reads and the one version it writes.

The separateness is load-bearing rather than tidy. The pair that
matters most is `Command` and `Wire`: an upgraded transport that changed
a retry identity would silently re-execute callers' work, so they are
two entries with two numbers and a test that asserts they are not the
same one.

What made the registry worth having rather than a document is that the
constants elsewhere are now *defined from* it -- `MANIFEST_FORMAT`,
`JOURNAL_RECORD_FORMAT_V1`, `SHARED_CHECKPOINT_FORMAT_V1`,
`logical_v1::SCHEMA_VERSION` and the rest are `Format::X.current()`.
A registry that was only checked by a test could drift for exactly as
long as nobody ran the test; one that is the definition cannot drift at
all.

The window replaced an equality check. `StoreManifestV1::decode` used to
require `format == MANIFEST_FORMAT`, which is right today and wrong the
moment a release wants to read its predecessor's stores. Now it asks
the window, and a version outside it is refused in the direction it is
outside: below is a retired format, above is one something else wrote,
and neither is guessed at.

**A capability is not local.** A feature changes replicated behaviour or
writes durable state every voter must interpret, so `coord_consensus::
feature` activates one only when *every* configured voter has reported
support -- not a majority.

That asymmetry is the thing I would want a reader to take away. A
majority is right for deciding something, as `floor::activate` does,
because the minority can be caught up afterwards from what the majority
holds. It is wrong for activating a capability, because the minority is
not behind: it *cannot* do the thing, and no amount of catching up
changes that. The same reasoning is why task-51's trim floor needs every
voter.

Three smaller rules fall out of it, each with its own control:

* **Silence is never assent.** A voter that has not reported supports
  nothing, because the silent voter is exactly the one that might be an
  old binary.
* **A report may grow and never shrink.** The only honest way to stop
  supporting a feature is to stop being a voter; a cluster that let a
  report shrink could activate something and then find a voter claiming
  it never had it.
* **An unknown active feature is a refusal, not a smaller set.** This is
  the one with a real trap in it. `ActiveFeaturesV1::features` refuses
  an identifier it does not recognize rather than skipping it, because
  the value feeds the rollback guard: a build that quietly dropped the
  feature it could not name would conclude it may serve precisely when
  it may not.

The guard itself is in `coordd`'s store opening, before the domain is
attached and long before the node could vote, and it names the feature.
"This node is too old for this cluster" is an answer an operator can
act on; a failure three steps later is not.

### The migration is the install lifecycle with one difference

`coord_storage_redb::migrate` stages a replacement generation, rewrites
every row into it through a declared `SchemaMigration`, and activates --
and activation is the only step that writes `CURRENT`, so an
interruption anywhere earlier leaves the previous generation selected
with its rows intact. The test interrupts at each activation step and
checks exactly that. The replaced generation stays on disk; reclaiming
it is `prune_unselected`, deliberately separate, so an operator can
still fall back by hand.

The one difference from an install is obligations. `InactiveGeneration::
stage` refuses a selected generation that holds `protocol_v1` rows,
because an install replaces a learner's state and a promise is not
something to be replaced. A migration rewrites this node's *own* state,
so `stage_migration` permits them and the rewrite copies them forward;
dropping them would be the amnesia every other rule in the system exists
to prevent.

`rewrite_into_new_generation` is public, which is worth explaining. The
version gate in `migrate` has nothing to accept until a second schema
version exists -- every window is one version wide today -- so a rewrite
reachable only through the gate would be code that had never been
executed. Exposing the mechanism lets the test drive the real path, with
`migrate` as its gate rather than its only door.

### What is deliberately impossible

There is no downgrade anywhere in this task: nothing lowers a format,
nothing deactivates a feature, nothing converts between engines, and
nothing rolls a live voter back to a savepoint. Rollback before
activation is running the old binary, which is what the coexistence
half is for. Rollback after activation is a restore (task-59), with
everything that costs -- which is the honest price of a one-way step,
and stating it is better than a mechanism that pretends otherwise.

## A metric that says nothing is better than one that says zero

task-61 is the observability surface, and the interesting part was not
what to measure — design Section 22.3 lists the stages — but what to do
about everything a given node does not measure.

The default answer in most systems is zero, and zero is a lie an
operator acts on. A dashboard showing zero commit latency during a
storage outage reads as "everything is fast". Zero headroom against an
unconfigured bound reads as "full", which is exactly backwards. Zero
fan-out latency on a frontend reads as a healthy voter, on a node that
does not vote.

So every reading in `coord_daemon::metrics` is a `Measure`, and an
absent one carries the reason:

* `NotThisRole` — a frontend has no journal, an observer casts no vote.
* `NoSamples` — instrumented, nothing observed yet. A stage nothing has
  passed through is not a fast stage.
* `Quarantined` — any reading would describe state the node has stopped
  trusting.
* `NoBound` — there is no configured bound to have headroom against.

Those four call for four different operator responses, and none of them
is the response to a zero. Writing them down turned out to also settle
an ambiguity in the snapshot: a stage a role *has* but has not exercised
reports honest zero counts with an unavailable latency. "Nothing has
happened here" and "this does not exist here" are different statements,
and now they look different.

### Bounded labels have to be bounded by the type

The rule is "no keys, tokens or unbounded IDs as labels", and the
tempting implementation is a `HashMap<String, String>` plus a
convention. A convention is one careless `format!` away from a
cardinality explosion and a data leak in the same line.

So there is no string map anywhere in the module. A series is broken
down by `Stage`, `Lane` or `ShardIndex` and by nothing else, each a
frozen enum or a checked integer. `ShardIndex::new` refuses an index at
or beyond `MAX_REPORTED_SHARDS`, so a node with more shards than that
aggregates rather than growing a series per shard.

The domain identity is deliberately *not* a label, which is worth
stating because it is the one that looks safe. A per-domain series on a
multi-tenant node grows with the tenants, and it also discloses which
domains exist — Section 13's point that a scalar revision leaks
aggregate activity applies to the label set too.

The same reasoning is why the snapshot needs no redaction pass. Every
field is a number or a frozen enum, so the secret scan finds nothing —
not because something filtered it, but because nothing of that kind was
ever recorded. The test asserts that directly: no credential-shaped
substrings, and no alphanumeric run longer than twenty characters,
which is what an identity, digest or key smuggled in as a label would
look like. The `coordd` test runs the same scan over the real startup
line, and its negative control — formatting the domain identity onto the
end of that line — trips it immediately.

### Three numbers that a single "write latency" would destroy

`Durability` keeps `sync`, `commit_return` and `backpressure` apart, and
the test shows why with a device that is fine and a queue that is not: a
fast sync with a slow commit-return means the queue is the problem, and
a slow sync with little backpressure means the device is. One combined
figure answers neither question, and would blame the disk in the first
case.

Backpressure especially is not slowness. It is the system declining
work, and a node that declines work is behaving correctly — folding it
into a latency makes correct behaviour look like a fault.

`Frontiers` splits the same way: `J`, `M` and `C` are three positions,
and `unmaterialized` (`J - M`) and `unreclaimed` (`M - C`) are the
derived numbers that say whether a node is keeping up with its own
durable log and what a reclamation would still have to replay. A single
"storage position" hides both.

### What the concurrency test does and does not prove

`Recorder` is atomics only. What guarantees that a diagnostics reader
cannot stall a voter is that there is no lock in the type — nothing to
take, so nothing to hold.

The test cannot prove the absence of a lock; a `Mutex`-based recorder
would pass it. What it shows is the behaviour that absence produces: a
reader taking two thousand snapshots never starves a concurrent writer
and never observes more completions than entries. The test's doc comment
says exactly that rather than claiming the stronger thing, because a
test that overclaims is worse than one that is honest about its reach.

It also found two things, once it was run often enough.

The second half of its claim was not true when it was written: the
reader loaded `entered` before `completed`, and both were relaxed, so it
could take an entry count from before an operation started beside a
completion count from after it finished and report more completions than
entries. Reading the counters in the other order is necessary and not
sufficient -- relaxed operations on separate atomics give a reader no
ordering at all. The completion counter is now the writer's last store
for an operation and carries release ordering, and the reader acquires
it first; everything that operation wrote is then visible, and every
counter read afterwards is read no earlier than the one it can never be
smaller than.

It is a small lie and a bad one. An operator who reads "more finished
than arrived" cannot tell a reporting artefact from a double count, and
the whole point of this module is that a reading means what it says.

The second was in the test. It asserted that the writer had run
alongside the readers, and on a loaded machine -- eighty-five tests in
parallel -- the reader could finish its two thousand snapshots before
the writer thread was scheduled at all. That is not a defect in the
thing under test and not a reason to drop the assertion: a run where the
two never overlapped shows nothing about either of them. The test now
waits for the first write before it starts reading, so the overlap is
established rather than assumed.

### Why the fixtures had to leave the test binary

Every fixture the project had lived inside a test binary. That is right
for a test and useless to a certification run: an API server, a Go Kine
build and a benchmark that runs for minutes all need a domain that
outlives one `cargo test` process, and none of them can construct one.

So `crates/coord-harness` writes a real domain into a directory —
authority, per-node voter and collector credentials, a genesis that
commits the key each node will present, one signed endpoint catalog, the
issuer's published keys and a strict `coordd.toml` per node — and then
starts every committed voter, waiting until each is actually serving.
Every committed voter, because a three-voter genesis with two processes
running is a quorum the domain does not have, and a suite that ran
against it would be measuring something nobody deploys.

The daemons are unchanged and unhelped. They run the production startup
checks against this material and refuse it the moment it stops agreeing
with itself, which is what makes a harness bug look like a harness bug
instead of like a result.

### The one place the harness is not the real thing, and why it says so

The credential endpoint signs a real ES256 service token with the
domain's configured issuer key on presentation of any non-empty
assertion. It is not an identity provider and its own documentation says
so in the first paragraph.

The reasoning is about what a failure would mean. What task-48 certifies
is the storage edge; the token exchange is task-35 and task-36's, and it
has its own tests. A real identity provider in the certification run
would add a second thing that can fail without testing the edge any
better, and a red run whose cause is ambiguous is worth less than a red
run whose cause is named. What is *not* weakened is the verifying side:
`coordd` runs exactly the verification it runs in production, against a
credential of exactly the shape it will see. The endpoint refuses to bind
anything but loopback, and the `test-only` crate role keeps all of it out
of every production artifact.

### Findings, and why none was findable before

The suite passed the whole storage-edge security matrix, create, read,
compare-and-swap, delete, paging and the compaction floor. Then it found
eight things. All eight are fixed here, and each became findable
only once the ones before it were.

**A watch was registered and never delivered on.** `Step::Watch` in
`bins/coordd/src/serve.rs` counted the watch and dropped the responder.
The collector had already registered the subscription with the hub by
then, so the watch existed and nothing ever wrote to it: the caller's
stream closed with no replay, no event and no progress. An API server
rebuilds every cache it has from watches, so it could not start against
this.

The daemon now holds that stream. Three things the wiring had to get
right, none of them the pump itself.

*The handover has to be one sequence.* The hub attaches a watch at the
frontier it had when the open was decided and queues everything above it
from that instant. What is below the frontier is history, and it is read
out of one pinned snapshot -- one, because reading it from several would
be reading it at several execution points -- and replayed into the same
watch before the subscription goes live. A replay that cannot be finished
ends the subscription instead of starting it live, and says which of
"compacted" or "source lost" it was, because those have different
resumptions: the first needs a fresh list, the second another replica.

*Pumping cannot be conditioned on local progress.* A revision reaches the
hub from applying a command, and a follower applies commands it learned
from its peers -- which is not work that node's own turn reports. So the
pump runs every time round the loop, before it waits on a socket, and
returns immediately when there is nothing subscribed. A subscriber is not
an event source: nothing will wake the loop on its behalf.

*A stream that is gone releases the subscription.* `Responder::push`
completes when QUIC has room, so a consumer that stopped reading blocks
that one frame rather than accumulating; when the write fails the
subscription is cancelled at the hub and drained, because a hub that went
on queueing for it would be a queue filling on behalf of a consumer that
does not exist.

Wiring it also surfaced two shape errors in the Go client, and they are
the reason the open had never reached the frontend at all. A caller ends
the request half of a stream it opens -- the frontend reads exactly one
frame from such a stream, and will not begin serving one whose sender has
not finished -- and the watch open left it open, so the frontend waited
out its frame deadline and closed the whole connection. And a cancel is
its own request on its own stream of the same connection, not a second
frame written onto the stream the frontend is delivering events on. Both
are now what every other request does; the same mistake in the Rust
integration test failed the same way, which is how they were found.

One property of the open worth naming, because a test that ignores it
tests the network instead. A watch that starts at whatever the frontier
happens to be races the first write, since the open and the write are on
different streams. A watch that names a revision does not: what preceded
the attachment is replayed and what followed is live. The API server
always names one; the certification row and the daemon test do too.

**A connection stops being answered after about sixty requests.** One
caller, one request in flight at a time, three voters: exactly 61 of 100
complete and the remaining 39 reach their deadline. Exactly 61 on every
repetition, with a two-second deadline and with a thirty-second one, so
it is an exhaustion and not a slowdown -- a slow domain would have
finished the eightieth request eventually.

The bound is the leader's command table. `coordd` builds it with
`capacity: 64`, `CommandTable::initialize` refuses with `Backpressure`
when the table is full, and nothing ever retired an executed record from
it. `retire` exists, is documented, is tested, and has no caller. So the
capacity was not a bound on unresolved work, which is what it reads as;
it was a bound on how many commands a replica could execute for as long
as it ran. The sixty-fifth caller waited out its deadline against an
idle, healthy cluster, and `Rejection::Backpressure` went into a vector
nobody drains.

A full table now reclaims what it has executed before it refuses
anything. That is what the module's own documentation already said the
rule was -- "records in ACCEPT or COMMIT are never evicted to make room,
only executed records can be retired" -- read as permission to retire
rather than as permission to forget nothing.

Two things the fix had to keep straight. Nothing unresolved is ever
touched: a record in START, PRE-ACCEPT, ACCEPT or COMMIT is an
obligation, and evicting one to serve a newer command would lose it
rather than shed load. And a retired record has to go on looking
executed to anything that depends on it, which is what `retire`'s
tombstones are for. How long a tombstone lives was answered wrongly
first, and is the subject of the last finding below.

The leader also had to stop asking the table whether a command executed.
`unexecuted_in_order` filtered on `phase_of(c) < Some(Phase::Executed)`,
and a reclaimed record reports no phase at all -- which sorts *below*
`Executed`, so a command that executed long ago would read as
outstanding, be speculated over, and have its result released a second
time to a caller that already had it. The proposal now carries the fact
itself.

**Concurrent callers were not served across a quorum.** With the table
fixed, a single caller sustained 200 requests against three voters and
500 against one. Two callers against three voters completed 13 of 100;
the same two against a *single* voter completed 100 of 100. So it was
not the client, not the volume, and not the number of sessions: it was
two commands in flight at once across a real quorum.

The conservative conflict key was the obvious suspect and the wrong one.
What the leader's table actually showed was commands sitting in ACCEPT
while every follower had *executed* them -- so the order was agreed and
the votes that would have told the leader so never arrived. The
followers had published them. They were still in the outbox, waiting on
a barrier that never became durable.

A replica's journal lowers one group per call and a group takes one
batch per domain; `Persistence::lower` says so in as many words. The
driver lowered exactly once per round, however many batches that round
had submitted. One round submits more than one whenever anything decides
two things at once -- two adoptions unblocked together, a proposal
beside the acceptance its predecessor just permitted -- and the rest
stayed queued. Nothing comes back for a queued batch on its own: the
next lowering happens only because something *else* was persisted. So
the queue lagged by one for ever, and whatever was submitted last never
became durable at all.

Then the rule that makes a vote honest finishes the job. A follower's
acknowledgement is published requiring every batch it has outstanding,
because an acknowledgement is a promise not to forget, and a promise
this replica cannot honour after a crash must not be sent. One batch
stuck at the back of the queue therefore held every later
acknowledgement behind it. The follower executed everything; the leader
heard about none of it; the caller's stream waited out its deadline
against a domain that had already agreed.

The driver now lowers until the domain's queue is empty, bounded by the
depth it started with -- so a lowering that moves nothing, an append
still in flight or an uncertain head, ends the loop rather than
spinning, and the next round tries again. The regression test is
`bins/coordd/tests/cli.rs::two_callers_at_once_are_both_served_by_a_quorum`
(two sessions, twenty-five requests each, three voters); with the loop
reduced to one lowering again it answers 3 of 25 each.

Two things this cost before it was found, both worth naming. The first
is that nothing said anything: a refused submission, a rejection the
protocol machine recorded, a send that found no route and a session that
failed to establish were all counted and none was logged, so a domain
that had stopped serving looked from its own log exactly like an idle
one. All four now say so. The second is that `Leader::take_rejections`
had no caller at all -- the list grew for the life of the process, and
the reasons in it were never read by anything. The driver drains it
every turn.

**A follower forgot a command the leader was about to name.** With the
lowering loop in, two callers against three voters ran clean: two
hundred operations, none unknown, three hundred and forty-five a second.
Then the *next* invocation against the same domain could not even bind,
and no later one ever did. Every voter had stopped serving, permanently,
on an idle machine.

The symptom is the one the table capacity produced before, and the cause
is its mirror image. Both followers' tables were full and stayed full:
sixty-four records, all of them proposals held for an order that could
never be adopted, every payload present, nothing executable. Walking the
chain down from the newest held proposal ends at leader sequence
sixty-four, whose single dependency is a command that has no phase at
all -- and which that same replica had adopted, committed, executed and
retired minutes earlier.

`retire` tombstoned a command only when a live record already depended on
it. That rule quietly assumes every dependency is written down in the
table, and on a follower it is not. A follower holds the leader's
proposals until their payloads arrive, and the dependency such a proposal
names lives in the proposal. So the sequence is: the follower executes a
command, its table fills -- with held proposals as well as its own
records, which is why it fills ahead of the leader's -- it reclaims, the
command goes without a tombstone because no record refers to it, and the
next proposal to arrive names exactly that command. `guard_accept` reads
"unknown" for a command this replica ran itself, the proposal is never
adopted, every later command queues behind it, and the table never drains
again. A new caller's session is a replicated command like any other, so
binding is what fails first, which is why it looked like a transport
fault.

The tombstone is now unconditional, and what bounds it is recency rather
than reference counting: the oldest goes once there are more tombstones
than the table has room for records. That bound is not arbitrary. A
leader names as a dependency only a command still live in its own table,
and under the conservative conflict key that is always the immediately
preceding command; a replica that remembers its last `capacity`
retirements therefore remembers every command a leader can still name,
with a large margin.

Why one caller never hit it is the same accident that hid the first two
findings. With one request in flight, the table is nearly empty when
reclamation runs, so there is rarely a held proposal to be orphaned --
four hundred sequential operations pass. With two, there is always one,
and the collapse is total: the run before the fix completed 190 of 200
at seven operations a second, the six runs after it completed 200 of 200
at about three hundred and forty-five.

The regression test is at the table, where the defect is:
`crates/coord-consensus/tests/graph.rs::a_dependency_retired_before_its_proposal_arrives_is_still_executed`
retires a command nothing refers to and then adopts an order that names
it. With the old rule it fails with `DependencyUnknown`, which is
verbatim what the wedged follower reported.
`bins/coordd/tests/cli.rs::a_quorum_keeps_answering_past_its_table_capacity`
now carries the composition's half: past the capacity sequentially, then
past it again with two callers in flight, and then a *new* session --
because the symptom was never a slow domain, it was a dead one.

**A vote that outran the proposal it was about was thrown away.** The
leader logged `Vote(WrongCommand)` a thousand times in one matrix, under
a comment reading "acknowledgements for a command this leader never
proposed are kept out: nothing to learn from". It had not proposed them
*yet*. Every voter is sent the same submission, so a voter that
initializes one before this leader does acknowledges it before this
leader has ordered it, and under concurrent callers that race is won by
a follower most of the time.

Dropping that acknowledgement loses nothing -- the follower's slow
acknowledgement after the proposal still forms a quorum -- but it costs
the fast path a round trip on the majority of commands, which is the
difference the fast path exists to make. It is now held, in the same
spirit as the proposal a follower holds until its payload arrives, and
bounded the same way: as many commands as the table has room for, with
the oldest evicted rather than the newest refused, so acknowledgements
for a command nobody will ever propose cannot turn the mechanism off.
After the fix that counter reads zero.

**And a node that is waiting for content has to keep asking.** The
request for a missing payload was sent every sixteenth *turn*, and a
turn happens when an event arrives. A domain with nothing else going on
takes no turns -- which is exactly the state a replica is in when
execution has stopped at a command it lacks -- so the second ask never
went out. The peer it asked may have had nothing to send the first time,
because its own proposal was not durable yet, and then nobody asked
again. The interval is now a duration and the loop has a timer arm for
it: a replica waiting for content comes back on its own rather than
waiting for an event that is not coming.

**And a key under a time to live did not expire.** Written with a
one-second lease, still readable a minute later. The private binding was
not disclosed to the caller, which the same test checks and which
passed, so what was missing was the expiry rather than the rule about
what a caller may see.

Worth being precise about what "the expiry" is, because the shape is the
substance, and the shape is what the fix had to keep. Design Sections 7.2-7.3 make expiry an authoritative
conditional command -- `ExpireLease` matching the binding's generation,
the expected renewal sequence and the replicated `LeaseAuthorityEpoch`,
applied only if every field still matches -- and a timer a scheduling
hint rather than permission to mutate. The deadline is `(1 + rho) * TTL`
local ticks from the observation of a committed grant or renewal, so
expiry may be late and may not be early; a restart rearms every surviving
binding for its full TTL from the new epoch's observation. The state
machine had all of that: `coord_state::expiry` arms, rearms and emits
candidates, and the planner applies them conditionally. What had no
caller was the scheduler.

So the fix is the path between them, and it has three parts.

*The leader schedules.* On becoming the one that proposes, a node orders
a fresh authority epoch for its own boot. That is not bookkeeping: an
expiry carries the epoch it was scheduled under, and the state machine
refuses an older one, so installing a new epoch is what stops a
predecessor's timers from deleting a key after this node has taken over.
Only when that epoch commits does it arm anything, and then it arms every
surviving lease for its full TTL from *that* observation -- not from
whatever the previous authority had counted. Committed lease state is
read back on an interval rather than every turn, which is the
conservative direction: a renewal this node has not seen yet can only
make a candidate stale, and a stale candidate is a no-op.

*The command is narrow, and narrowness is not what protects it.*
`ExpireLease` and `EstablishLeaseAuthority` are two appended canonical
operations, in the same spirit as `ConsumeAdmission`. But what keeps a
caller out of them is the admission beside the payload: every submission
a collector makes carries a receipt minted for a session, and these two
execute only for a command accepted with *no* admission at all, which
only a voter's own proposal is. A caller naming one is refused whatever
its session holds. That is a property worth a test of its own, and it has
one.

*Two latent defects had to come out first.* Until now every command
reached every voter as a submission, so no replica ever had to execute
one whose payload it lacked -- and a leader-originated command is exactly
that. A replica that heard a proposal before the payload left a
placeholder in its table, and `advance_pending` treated a placeholder as
acceptable: it took the proposal out of the held set, the adoption failed
against a record with no payload to accept an order *for*, and the
proposal was dropped silently. A placeholder is now held until it is
initialized. Second, a payload arriving for a command the replica already
had a record for was discarded outright, so a replica in that state could
never acquire one; it is now bound and written. And nothing ever asked:
`request_payloads` had no caller, so a replica now asks this ballot's
leader on an interval, and execution waits at the command it cannot run
rather than failing the node -- which is what it used to do, taking the
whole process down with it.

All of them had been invisible for a structural reason worth stating. The
Kine backend holds one session and issues one invocation at a time, and
the Go suite's etcd-level rows spend fewer requests than the old bound
before the watch row stops the domain for its own reason. Every Rust
integration test builds one caller and asks it a handful of questions.
Nothing in the project had ever asked the composition a hundred
questions in a row, or two at once, and nothing had ever held one of its
streams open across two writes. A Kubernetes API server does all three
while it is still booting.

**And about one operation in two hundred was never answered.** With
four callers against three voters, a run of two hundred operations ended
with one or two that reached their deadline. Not slowness: a
two-second deadline, a four-second one and a thirty-second one all lost
the same count, and the thirty-second run's straggler waited the whole
thirty seconds. A count that does not move when the deadline moves is
something that never completes.

Every voter was idle when it happened. Nothing queued for the journal,
nothing unmaterialized, nothing withheld in an outbox, every record in
every command table executed -- and a caller's stream still held open.
The collector holding that stream reported the invocation as
`awaiting-release`; it had its votes and was waiting for a release the
leader had already sent. No refusal was recorded anywhere.

The count that gave it away was the collector's own. For the lost
command it had exactly **one** vote -- the leader's -- while both
followers had refused the submission as a `Duplicate`. They had already
accepted that command; they simply never told the collector so.

A voter learns a command's content from a submission *or* from a peer.
When it learns it from a peer -- because the leader's proposal outran
the collector's submission and it asked for the payload -- it
initializes the command and acknowledges it, correctly. But
acknowledgement is owed to the collector that asked for the work, and
this voter has not been asked yet: no submission has reached it, so it
has nothing to say where the acknowledgement belongs. The daemon's rule
for that case was to hand the frame to the collector in its own process,
and that collector has never heard of the command, so the frame was
dropped. Moments later the real submission arrived, the retry key was
already bound, the replica refused it as a duplicate, and the
acknowledgement was never produced again.

The domain was never wrong: the command was ordered, executed and
durable, and every voter agreed. What was lost was one third of the
evidence the *caller's* collector needs to learn it independently, which
is the whole reason the collector counts votes rather than trusting one
replica's word.

So evidence with no known submitter is now held instead of misdelivered,
and a submission -- including one refused as a duplicate, which is
exactly the case that produces this -- is what releases it. The hold is
bounded by the thing the window is bounded by: a command can be in this
state only between this voter acknowledging it and the submission
reaching it, so the depth that matters is the commands in flight at
once. Past the bound the oldest goes and says so, because a caller's
collector is then one acknowledgement short and will not be told why.

Two things this makes visible that were not. A voter now says once that
it is holding evidence for a submitter it does not know, which is
ordinary and worth seeing; and it says once if it ever drops any, which
is not. `a_quorum_keeps_answering_past_its_table_capacity` asserts on
both, and the second half of it -- four callers in flight past the
table's capacity -- fails without the fix with a caller one or two
short of its hundred and twenty.

*A wrong turn worth recording.* The obvious first suspect was the
leader's per-command memory, which is genuinely unbounded: proposals,
vote sets and payloads are kept for the life of the process. Bounding
them made the loss dramatically worse -- every run failed instead of one
in two hundred -- and that looked like a second defect. It was the same
one. Forgetting a payload makes a follower ask a peer for it, asking a
peer is how a follower comes to hold a command no collector has asked it
for, and every one of those was a chance to drop an acknowledgement. The
bound was reverted at the time for being unsafe; with the routing fixed
it passes, which is how the coupling was finally named. Bounding that
memory is still worth doing and belongs with
[task-j07](design/tuplesky-prs-plan.md#task-j07).

**A voter that filled its table never emptied it again.** Once the
straggler above was fixed the matrix ran eight times faster, and the
re-run found the defect the old rate had been hiding. One voter of
three would stop: sixty-four records at PRE-ACCEPT, every later proposal
and submission refused for backpressure, its projection frozen at the
position it reached, and -- because a frontend reads replicated policy
out of that projection -- every read its own callers asked for refused
as unauthorized for the rest of the run. About a third of the reads in
the `scans` and `read-mostly` rows, which is what a caller bound to that
voter's frontend sees, and nothing at all in the rows that ran before it
stopped.

The cause is two lines, both the same mistake. `Follower::on_proposal`
and `Follower::on_request` each recorded a backpressure refusal and
returned. What they returned from is the function that ends by calling
`advance_pending` -- the step that adopts the records whose turn has
come, and adoption is what lets a command execute, be retired, and free
the slot that was missing. So the two doors into the machine, on the one
occasion when making progress mattered most, were exactly the two that
stopped making it. The table stayed full, and it stayed full for ever:
the leader does not re-propose, there is no message to ask it for an
order it already sent, and nothing else would prompt the replica to look
again.

The proposal was also *dropped*, which is the second half of it. A
leader's order reaches a replica once. Dropped for want of a table slot,
the command can never be adopted even after the slot frees and its
payload arrives -- it sits at PRE-ACCEPT, and with a conservative key
making the dependency chain total, so does everything ordered after it.
It is now held instead, bounded at eight times the table's own capacity,
which is generous on purpose: the failure mode of holding too few is the
wedge, and the failure mode of holding too many is memory.

**And a repair that made the thing it was repairing worse.** A replica
that lacks a command's content asks its leader for it every fifty
milliseconds until it arrives, which is right. It asked for *all* of
them -- a tableful, sixty-four -- and the leader answered with sixty-four
payload frames, several times a second, on the one bounded lane that
also carries the proposals and acknowledgements that replica was waiting
for. The lane filled, what it dropped was the traffic that would have
let the replica catch up, and it fell further behind and asked for more.
The leader's own log says it plainly: `QueueFull { lane: Control }` to
one replica, over and over, while that replica reported sixty-four
payloads missing and never fewer.

Payload transfer is now bounded on both sides at
`MAX_PAYLOAD_TRANSFER`, with a rotating cursor so that a bound on how
many are asked for at once does not mean the same few are asked for
every time and the rest never. The bound is on the answering side too,
because a peer must not be able to make this replica flood its own lane
by asking for more than the protocol's own bound.

A bound on the ask needs a matching change to when the ask goes out, and
this is the part that took a second run to see. The daemon repeated the
ask on a fifty-millisecond interval, which was right when the ask was
unbounded and wrong the moment it was not: a bounded ask on an interval
is a *rate*, and a replica behind by more than the domain produces in an
interval can never close the gap however long it runs. The soak showed
exactly that -- a follower holding ninety-nine proposals and missing
ninety-two payloads, asking for eight of them twenty times a second
while the domain committed three hundred. The interval is now the floor
for an ask nobody answered, and the ask is a window: one outstanding at
a time, and the next goes the moment the number of missing payloads
moves, which is to say as soon as the last one was answered.

Which put the two halves of the repair in direct conflict, and that is
the thing worth naming. Ask slowly and a replica behind by more than the
interval's worth can never catch up; ask as fast as the answers come and
the catch-up traffic fills the queue that the proposals and
acknowledgements it is catching up *with* are waiting in. Both were
measured, one wedge each. There is no rate that resolves it, because the
two kinds of traffic were competing for one queue -- and the transport
has had a lane for exactly this since task-31. Payload transfer now goes
down the bulk lane, which is where moving whole command payloads
belongs, and the control lane carries only what the protocol needs to
make progress. The classification reads the encoded discriminant rather
than decoding the frame, because deciding where to send a payload by
decoding it would cost more than the send;
`payload_transfer_is_recognized_from_the_encoded_discriminant` pins the
two bytes against the encoder so that a variant added above them fails a
test instead of quietly mis-routing.

`a_full_table_still_adopts_and_still_keeps_the_order_it_was_sent` fills
a follower's table exactly, refuses a submission and a proposal at the
door, and asserts both halves: the order in the refused proposal is
kept, and the records whose turn had come are adopted anyway. Reverting
either half fails it. The bound and the rotation are asserted together
by `payload_transfer_is_bounded_in_both_directions_and_still_covers_everything`:
a bound that always took the same prefix would be a wedge with a bound
on it rather than a fix.

*And what is left, stated plainly.* A replica that falls behind now
recovers instead of stopping, and nothing is lost or refused that was
not lost or refused before it fell behind. It does not recover
*quickly*: on the re-run matrix the read-heavy rows lose about three
operations in ten to the ten-second deadline, all of them belonging to
the callers bound to one frontend, at every offered rate including the
closed loop. Those are reads held pending on a projection that has not
caught up, not reads refused, and they are published in
[the results](operations/wan-results.md#the-finding-this-run-exposed)
rather than tuned away. Closing it means a catch-up path that outruns
the load that put the replica behind, which is a protocol question
rather than a bound to adjust, and it belongs with
[task-j07](design/tuplesky-prs-plan.md#task-j07) beside the leader's
per-command memory.

### What the benchmark harness had to get right to find these

A closed-loop benchmark would not have found the fifth or the sixth. It sends the next
request when the previous one returns, so a domain that serializes work
looks like a domain with a long service time and a perfectly respectable
throughput curve.

`crates/coord-wan-bench` schedules arrivals against absolute instants
from one start, and separates the wait before an operation started from
the operation itself and from the whole thing. That is what made the
findings legible rather than mysterious. A closed-loop harness would
have reported "throughput fell and latency rose", which is the shape of
a slow system. What the open-loop run showed instead was a fixed count
of fast completions followed by nothing at all, identical on every
repetition and unchanged by a fifteen-fold longer deadline. A count that
does not move when the deadline moves is a resource that ran out.

The same separation is what distinguishes the third finding from that
one. Adding a second caller does not move a count; it collapses the
completion rate while the service times of the few that get through stay
ordinary. That is a different shape, and it is why the two are written
up as two things rather than as "it gets slow under load".

The fourth needed something else again: a run that succeeds, followed by
a run against the same domain that cannot start. One invocation per row
is what made that visible. A harness that stood a domain up and tore it
down inside each row would have reported six healthy rows and never the
state the first one left behind.

The three-distribution split is not decoration. `queue` says whether the
harness was the bottleneck, `service` says what the operation cost once
it started, and `whole` — scheduled arrival to answer — is the only one a
headline may quote. A run that reported only the middle one would have
reported this defect as good news.

### Two rules the report inherits rather than invents

Every metric the harness did not read is absent *with a reason*, never
zero, exactly as `coord_daemon::metrics` does it. The daemon renders its
stage, synchronization, commit-return and frontier metrics on its own
startup and shutdown report and not on a socket a benchmark can poll, so
the report says `NoEndpoint` rather than estimating. Journal
synchronization and commit-return stay separate fields, because they are
separate things and neither stands in for the other.

And `--durability` is required. A latency figure without the durability
it was obtained under is not a slow result or a fast one; it is not a
result.

### What the impairment script actually is

`scripts/bench/wan-topology.sh` puts kernel delay, loss and asymmetry on
the exact UDP port pairs that cross a region boundary, and can drop a
region's traffic entirely while its voters keep running — which is the
Section 21.5 region loss, and is a different experiment from killing the
processes, because a partitioned voter still holds what it promised and
rejoins.

It is one host with netem on loopback. That gives real queues, real
reordering and real timer behaviour, and it does not give a shared
physical link, competing traffic or a route change. The script prints the
`--impairment` line for the run that follows rather than letting the
benchmark infer one, and a run that states no impairment says exactly
that instead of implying there was none.
