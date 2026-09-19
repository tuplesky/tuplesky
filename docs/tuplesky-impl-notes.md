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
