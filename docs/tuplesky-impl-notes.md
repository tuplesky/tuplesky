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

## The packet simulator diverged on its certificates, not on machine load

**Where:** `coord-transport-sim/tests/packets.rs`,
`loss_reorder_duplication_and_mtu_schedules_reproduce_and_keep_the_visible_outcome`
(task-32).

**Expected:** a deterministic packet-level simulation reproduces, so it
either passes or fails on its input.

**Actually:** it failed now and then, and an earlier version of this
entry blamed concurrent CPU load on the endpoints' real timers. That was
wrong: looping the schedule on an idle machine diverged 3 times in 200.
The test re-issued the node certificates for every world, and about one
issue in two hundred encodes to 294 bytes instead of 295, because the
serial number is random. Two worlds on the same seed therefore presented
handshakes of different sizes, and the schedule, which is keyed to
packet sizes and positions, stopped describing the same traffic.

**Did:** fixed it on task-32 ("task-32: a schedule is replayed against
the certificates it first ran with"): every world is built from the
fixture's own identities, so a replay presents the same bytes. It has
since shown 0 divergences in 400 loops and 150/150 passes.

**Revisit when:** the simulation diverges again. Look first for input
that is regenerated per world rather than fixed by the seed; a retry or
a longer deadline would hide exactly that.

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

The policy is written before the genesis pin, and the pin is the last
durable step of `init`. A store with no pin is refused by a start and
finished by `init`, so an initialization that stops anywhere before the
pin is finished rather than served half-made; the policy writes only
the rows the projection does not already hold, so finishing it twice
writes nothing twice. The other order left a pinned store with no trust
rule and no grants after a stop between the two, which every check took
for initialized and nothing would ever repair.

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
