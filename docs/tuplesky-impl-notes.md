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

## The Rust transport has no client-side unary request

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

**Revisit when:** something in Rust needs to be a client -- a control
plane tool, a cross-domain relay, `coordctl` against a live node. The
fix is a request method on `Transport` that keeps the receiving half and
returns the answer, not a change to either existing path.

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
