# TupleSky implementation task plan

**Status:** Review proposal, consolidated v1.5.  
**Date:** 2026-09-24.  
**Companion:** [TupleSky implementation design](tuplesky-design.md).  
**Scope:** 196 implementation tasks with stable `task-*` identifiers. The `task-01` through `task-66`, `task-s01` through `task-s04`, `task-j01` through `task-j10`, `task-o01` through `task-o06`, `task-m01` through `task-m05`, `task-c01`, `task-c02`, `task-c03`, `task-d01` through `task-d92`, `task-d94` through `task-d102` and `task-q01` suffixes and prerequisites are preserved. v1.5 adds `task-d01` through `task-d04` from review of the open implementation PRs and of multi-host readiness, `task-d05` through `task-d17` from the Jepsen client's runs, `task-d18` through `task-d33` from a review against an external SwiftPaxos correctness checklist ([review record](tuplesky-checklist-review.md)), `task-d34` from what the protocol simulator of task-d30 found, `task-d35` through `task-d44` from a review of recovery time and storage integrity (an execution chain, a scrub, and voter replacement through a prepared transition, a staged learner, terminal recovery after the seal, serving across a transition and a running membership install), `task-d45` through `task-d50` from measuring the throughput of the Jepsen client's first unthrottled runs, `task-d51` from task-d46's long runs, `task-d52` and `task-d53` from a profile of the domain thread after task-d46, `task-d54` from the Jepsen runs of task-d50, `task-d55` through `task-d57` from the Jepsen runs of task-j06, `task-d58` through `task-d61` from a profile of five voters' CPU per operation, `task-d62` through `task-d65` from a consolidated review against the reference SwiftPaxos, `task-d66` from the Jepsen runs of task-d51, `task-d67` from a Jepsen run on #98 that made no fast decision, `task-d68` from task-d62's counts after a leader change, `task-d69` from a profile of a follower's loop, `task-d70` from the Jepsen runner's profile of the transport's workers, `task-d71` from the protocol simulator's first 1,000-seed sweep, `task-d72` through `task-d75` from comparing a command's cost with etcd's on one host, `task-d76` from a Jepsen run that killed the leader, `task-d77` through `task-d81` from the protocol simulator with acknowledgements between followers lost, `task-d82` through `task-d84` from the Jepsen runs of the #173 carry, `task-d85` from the protocol simulator on task-d74's routing, `task-d86` through `task-d92` from planning the whole gap to etcd on #174, and moves committed key replacement from `task-58` to `task-m03`; the changes are listed under [Gate checklist and deferred work](#gate-checklist-and-deferred-work). Task IDs are not GitHub pull-request or issue numbers. One implementation PR corresponds to one task; its GitHub-assigned number is recorded separately. No baseline, supplement or separate amendment is needed.

## How to use this plan

**Convention:** use the full lowercase task ID in task references, anchors, branch names and PR titles (for example, `task-01: workspace and CI`). Open one PR for each task, and link that PR back to its task anchor. GitHub assigns an independent PR number; do not rename the task to match it. Historical review comments keep their old labels, but the current documents use `task-*` throughout.

The design is normative for behavior/dependency choices; this document partitions delivery. Direct prerequisites form a DAG, not a demand for serial development. Draft work may begin earlier but must be reviewed against its final base and land after prerequisites. Task count implies neither dates nor effort estimates.

Aim for one invariant or observable behavior per PR. Roughly 200-700 handwritten implementation lines plus focused tests is a review target, not a quota. Split an oversized task into explicitly named child tasks before coding and update the plan and prerequisites; each resulting task receives its own implementation PR. Never omit tests or fold unrelated cleanup into correctness work to reduce apparent size. Review generated fixtures, lockfiles and model counterexamples separately.

Every PR states problem, exact before/after behavior, design sections, test commands/results, a failure case and persistence/wire/security implications. Protocol work includes source-rule mapping and publication prerequisites. Schema changes state compatibility. Qualification assembles evidence; behavioral fixes receive focused review rather than hiding in test churn.

Reference single-store and fixed-membership compositions are early increments, not competing production architectures. No public insecure listener, simulator keys, weak-store bypass or test crypto may enter production artifacts. The authenticated fixed-membership preview remains explicitly bounded until permanent replacement and quorum-safe forgetting are complete. Production additionally requires the shared-journal, observer and client-aware integration gates below.

## Workstreams and release boundaries

| Tasks | Workstream | Boundary |
|---|---|---|
| task-01 through task-06 | Contracts, locked tooling and independent deterministic oracle | G0 |
| task-s01 through task-s02, task-07 through task-18 | Storage contract/model, strict redb reference, state, watch, leases and replicated auth | G1 |
| task-19 through task-29 | Source-mapped SwiftPaxos, durability, recovery and fast results | G2 |
| task-30 through task-43 | Native QUIC, clients and federated security | G3 authenticated preview |
| task-44 through task-48 | Go/Kine edge and actual Kubernetes conformance | G4 |
| task-49 through task-60 | Common checkpoints, semantic trimming, sealed handoff, restore and upgrades | G5 |
| task-61 through task-66 | Operability, WAN measurements and release evidence | G6, extended by task-q01 |
| task-s03 through task-s04 | Fresh isolated Fjall experiments and local comparison | Optional; not migration or second-engine production support |
| task-j01 through task-j10 | Shared journal, materialization, local checkpoint, runtime composition and multi-group qualification | task-j06 separately optional |
| task-o01 through task-o06 | Finalized streams, regional observers/relays, Kine watch/read integration | Capability-specific gates |
| task-m01 through task-m05 | Authoritative discovery, full-client Kine and integrated membership | Operational production requirement |
| task-d01 through task-d44 | Daemon runtime wiring (election, leaf renewal, reconnection and why a dial failed), one execution order on every replica, recovery bounded by execution, a Sync that leaves no stale acceptance, a new leader that chains after what it executed, a diverged node that stays stopped and says what it compared, a decision that names its admission facts, every proposal and decision reaching every voter, every vote reaching the leader, catch-up, multi-host test provisioning, and the correctness-checklist remediation (a promise a Sync cannot lower, slow decisions recovery keeps, a Sync that fits its row, collector obligations that end, client outcomes, table room for recovery, a resource contract, forgetting wired into `coordd` and a learner reinstall behind its floor, the failure and obligation contract, the real machines under simulation, and the recovery bugs that simulation found), an execution chain every voter compares, a scrub of the replicated state, a prepared transition, a staged learner, a configuration installed into a running daemon, and voter replacement through the sealed handoff | Required before task-64/task-65 qualification and task-66 |
| task-d45 through task-d54 | Throughput: a command's cost measured on every node and gated in CI, per-turn work independent of history, durable group writes, the projection's durability under the journal, re-sends only once an answer is due, reads and the fast path off the slow path, the local checkpoint off the domain thread, execution and materialization on a pipelined applier, an execution established without walking what already executed, and the journal's syncs off the domain thread | task-d45 through task-d49 and task-d51 through task-d53 required before task-64, task-q01 and task-62's remaining rows, task-d54 before task-62's remaining rows; task-d50 after its design amendment |
| task-d55 through task-d57 | From the Jepsen runs of task-j06: a publication's steps, a restart's replay and the domain thread's scheduling measured; a restarted voter that neither fills the leader's control lane nor takes the ballot from a live leader; a read's index bounded by what the voters confirming it had voted | task-d55 before task-d51's acceptance run; task-d56 before task-64; task-d57 after its design amendment |
| task-d58 through task-d61 | From a profile of five voters' CPU per operation: held reads served from one snapshot and sharing rounds; the leader's re-send without a walk over every proposal it keeps; allocation on the domain thread; a turn's frames to one peer sent together, broadcasts encoded once | task-d58 after task-d51, then task-d62, then task-d61; task-d59 and task-d60 whenever; each measured against the five-voter profile |
| task-d62 through task-d65 | From a consolidated review against the reference SwiftPaxos: a command's cost and its fast-path outcomes counted on a matched workload; speculative execution driven on the serving path under the existing release gate; collector-side early completion designed; the conflict model decided | task-d62 first; task-d63 after task-d62's learned-to-released split; task-d64 and task-d65 design-first |
| task-d66 | From the Jepsen runs of task-d51: a publication's journal part (the pointer and the retirement) run as a journal worker's job, so the domain thread takes nothing back | After task-d51 and task-d54, and after task-d58 and task-d62; its per-thread reading first, on any carry |
| task-d67 | From a Jepsen run on #98 with no fast decision in 46,000 commands: a pre-acceptance the leader never ordered no longer keeps a fast-set follower off every leader path for the rest of the ballot | Before task-d62's fast-path reasons are read on the runner; its design step first |
| task-d68 | From task-d62's counts: after a leader change, a follower's path log follows the new leader's order at once, rather than once the new leader's sequence numbers pass the old ballot's; and a leader's own path log does not keep every command it proposed | Before task-d62's runner rows with leader faults; its design step first |
| task-d69 | From a profile of a follower's loop: the outbox releases what a completion makes releasable, rather than walking every send it holds on every round | After task-d60's second PR; before task-d60's third step; its runner pair |
| task-d70 | From the Jepsen runner's profile of the transport's workers: fewer datagrams and wakeups per command, through QUIC's acknowledgement cadence, the runtime's worker count and the inbound delivery path | Its two settings as runner pairs first, while task-d69 is written; the inbound path after |
| task-d71 | From the protocol simulator at 1,000 seeds per row and size: the order recovery's possible-fast rule reads, named once, and a command whose turn has come kept within a follower's capacity | Before task-d67's and task-d68's code PR, which rebases on it; the sweep then runs weekly |
| task-d72 through task-d75 | From comparing a command's cost with etcd's on one host: a budget for CPU per operation against etcd's at the matched shape, tracked, and three levers under it that leave the order alone: a turn's proposals and acknowledgements to a peer in one frame, acknowledgements to the leader and the frontend only, and work off the leader's loop | task-d72 after task-d63's build and task-d66's job, whose pairs are its first rows; its levers after task-d72, task-d74's design check first, each as a runner pair that adds a row |
| task-d76 | From a Jepsen run that killed the leader: a new leader whose fixed fast set holds the voter just killed makes almost no fast decision for its term | After task-d72's levers; its design step first |
| task-d77 | From the protocol simulator with acknowledgements between followers lost, which task-d74's routing found: a selection re-proposed a command its candidate had executed and forgotten, from a voter behind's record of it | Before task-d74's routing |
| task-d78 | From the protocol simulator's sweep of every row with acknowledgements to followers lost and catch-up on (#167): a fast decision was dropped on its path evidence, read through a member's stale record of a command the selection held decided | Before task-d74's routing |
| task-d79 | From the same sweep (#167): a voter whose own report named an acceptance nobody could supply failed every campaign, and its rising ballots fenced every other voter's, so nobody led | Before task-d74's routing |
| task-d80 | From an independent sweep at three in four acknowledgements to followers lost, catch-up off (#169): a voter that held a command committed only from the Sync it was synchronized to lost it to the next Sync, which left it out, and a later selection re-proposed it under other dependencies | Before task-d74's routing |
| task-d81 | From an independent sweep at nine in ten acknowledgements to followers lost, catch-up on (#173): four voters waited for a position only a fifth had executed, which could not lead and which no donor rule let serve them, so nobody led | Liveness; not ahead of other work |
| task-d82 through task-d84 | From the #173 carry's Jepsen runs (#98): a leader killed and back within the transport's idle timeout left the domain leaderless for 23 to 54 s; the `service` row ordered about seven reads in ten, where the barrier serves only a bare Range; and durability costs about 1.7 journal syncs a command | task-d82 before the next fault series; task-d83 next; task-d84 after task-d74's pairs and the tokio profile task-d73 waits on |
| task-d85 | From the sweep of #167's head at half the acknowledgements between followers lost, catch-up off: a Sync received twice activated its ballot before its marker was durable, and the marker then released commands the voter had just accepted, so a decided command was reported at PRE-ACCEPT under other dependencies | Safety; before task-d74's routing |
| task-d86 through task-d92 | From planning the whole gap to etcd before the next performance run (#174): the levers the readings on record name and no task owned, each sized, so the runner measures them as one batch | Each with its reading first where it has one; task-d90's design step started now, as the long pole for p99 |
| task-d94 | From the task-d72 batch's first leader-kill run on the head (#98): a voter restarted 17 s before the final heal executed about 4 commands a second for minutes, holding 283 unordered pre-acceptances, because its catch-up pacer never asked for a page | Before the next fault run |
| task-d95 | From task-d90's design review (#181): a voter that could not read its own authorization rows when a retry was presented again landed `RetryUnauthorized` as the command's replicated result | Before task-d90's code step |
| task-d96 | From the task-d72 batch's call graph of the tokio threads (#185 review): QUIC's packet protection, send and receive are about three fifths of the threads and scale with packets, and the ACK frequency and per-step coalescing are already read, so what is left is fewer messages a command | A design entry before the next batch; no code until it is sized |
| task-d97 | From the `service` row's R4 (#98, #185 review): its means put L4 at a p50-scale gain, 0.76 to 1.36 ms off a write's release, so nothing on record says what a p99 operation waits for | Before task-d90's code step is sized against the p99 |
| task-d98 | From the next step on the gap (#185 review): a slow-path write pays two syncs in series before it is learned, the leader's and then a follower's, since the leader sends its proposal only once its batch is durable; about 95% of the register row's writes take the slow path, and two runner syncs are about 4.6 ms against etcd's `service` p99 of 3 | Before the next batch, or the batch revises the `service` target with this as the reason |
| task-d99 | From task-d98's design step (#192 review): task-60's activation rule and rows exist, but nothing in a running `coordd` records a voter's support or activates a feature, so a feature-gated change ships unreachable, and Section 23 rules out an operator flag around a correctness gate | Before the batch that carries task-d98 |
| task-d100 | From the batch's sweep and its two runner jobs (#98, #185 review): with the disk's tail off the head is core-bound from 30 callers, and a slow-path command still costs the collector's frontend about 11 api frames, about 13 datagrams, and every follower a submission's admission for an order it only adopts | After task-d98 and task-d99, which its relayed proposal and its features rest on; step A first, step B sized on the runner after it (#202 review) |
| task-d101 | From the two local profiles and the row count on #185, read against the runner's tmpfs job (#185 review): the four followers' executions and projections cost about 0.75 ms an operation, a fifth of the servers, and none of it is on a write's path | A design step now, beside task-d100's code step; its option 1 is task-d86's follower half |
| task-d102 | From #185's next steps (#185 review): at 120 callers a command costs about 16.8 datagrams and a journal group holds about 4.9 commands, since every message and group of a slow round is per command | A design step now, after task-d100's step A; it builds step B, and reads task-d84's parts 2 and 3, task-d86 and task-d101's option 1 together |
| task-q01 | Combined durable WAN/Kine qualification | Required before task-66 |

```mermaid
flowchart TD
    A["Contracts and simulator"] --> S["Storage contract and model"]
    S --> B["Strict reference storage and application"]
    A --> C["SwiftPaxos and recovery"]
    B --> C
    C --> D["Transport, identity and clients"]
    D --> K["Kine compatibility"]
    C --> L["Checkpoint and membership protocol"]
    B --> J["Shared journal and composed recovery"]
    K --> O["Observer integration"]
    J --> O
    L --> M["Client-aware membership integration"]
    O --> M
    J --> Q["Combined qualification task-q01"]
    O --> Q
    M --> Q
    Q --> R["Production release task-66"]
    B -.-> X["Optional fresh Fjall experiments"]
```

This is a workstream overview; the individual prerequisites are authoritative. Optional task-j06 is not an unconditional prerequisite of task-q01. Enabling that profile requires its separate evidence and inclusion in the applicable combined matrix. Former task-s05 through task-s08 remain retired and are not reused.

## Review index

| Task | Title | Direct prerequisites |
|---|---|---|
| [task-01](#task-01) | Lock the workspace and implement filtered GitHub CI | None |
| [task-02](#task-02) | Define identities, canonical commands and ordered-key fixtures | task-01 |
| [task-03](#task-03) | Implement the bounded postcard wire codec | task-02 |
| [task-04](#task-04) | Establish pure event/effect and durable-barrier interfaces | task-02 |
| [task-05](#task-05) | Build the deterministic world and replay format | task-04 |
| [task-06](#task-06) | Add an independent history oracle | task-02, task-05 |
| [task-07](#task-07) | Implement the redb adapter and fail-closed generation lifecycle | task-01, task-02, task-s01, task-s02 |
| [task-08](#task-08) | Implement the shared single-writer storage coordinator | task-04, task-07 |
| [task-09](#task-09) | Exercise the actual redb engine with disk faults | task-05, task-08 |
| [task-10](#task-10) | Implement the pure KV and transaction planner | task-02, task-04, task-06 |
| [task-11](#task-11) | Apply shared KV plans atomically and serve pinned MVCC views | task-08, task-09, task-10 |
| [task-12](#task-12) | Persist deduplication, result resolution and retry floors | task-11 |
| [task-13](#task-13) | Implement atomic watch replay and live handoff | task-11, task-12 |
| [task-14](#task-14) | Add bounded MVCC compaction | task-11, task-13 |
| [task-15](#task-15) | Implement native lease grant, attachment and revoke | task-10, task-11, task-12 |
| [task-16](#task-16) | Implement replicated renewal and conservative expiry | task-05, task-06, task-15 |
| [task-17](#task-17) | Add native atomic Kine primitives and private TTL bindings | task-12, task-16 |
| [task-18](#task-18) | Implement replicated sessions, policy and grant commitments | task-10, task-11, task-12 |
| [task-19](#task-19) | Freeze SwiftPaxos source mapping and bounded models | task-02, task-04, task-05 |
| [task-20](#task-20) | Persist ballots, promises and configuration guards | task-08, task-19 |
| [task-21](#task-21) | Implement the dependency graph and closure evidence | task-19, task-20 |
| [task-22](#task-22) | Implement normal leader proposal handlers | task-20, task-21 |
| [task-23](#task-23) | Implement normal follower vote and adoption handlers | task-20, task-21, task-22 |
| [task-24](#task-24) | Implement slow learning and ordered materialization | task-11, task-12, task-13, task-18, task-22, task-23 |
| [task-25](#task-25) | Implement durable recovery summaries and payload transfer | task-20, task-21, task-23 |
| [task-26](#task-26) | Implement recovery selection and new-ballot activation | task-19, task-24, task-25 |
| [task-27](#task-27) | Qualify fixed-membership crash recovery end to end | task-06, task-09, task-16, task-18, task-26 |
| [task-28](#task-28) | Implement full fast-path learning evidence | task-19, task-23, task-26, task-27 |
| [task-29](#task-29) | Add bounded speculative execution and result-release gating | task-12, task-18, task-24, task-28 |
| [task-30](#task-30) | Implement the Quinn transport adapter and TLS lifecycle | task-03, task-04, task-08 |
| [task-31](#task-31) | Implement QUIC traffic isolation and backpressure | task-30 |
| [task-32](#task-32) | Add packet-level quinn-proto simulation | task-05, task-30, task-31 |
| [task-33](#task-33) | Wire the trusted frontend collector and native dispatch | task-17, task-18, task-29, task-30, task-31 |
| [task-34](#task-34) | Implement the Rust SDK request lifecycle | task-03, task-12, task-30, task-33 |
| [task-35](#task-35) | Implement hardened external JWT and issuer verification | task-18 |
| [task-36](#task-36) | Implement RFC 8693 exchange and service credential signing | task-18, task-33, task-35 |
| [task-37](#task-37) | Bind API sessions and authorize live/replayed output | task-13, task-18, task-33, task-34, task-36 |
| [task-38](#task-38) | Implement OIDC browser login and the service code flow | task-18, task-35, task-36 |
| [task-39](#task-39) | Implement device authorization with bounded polling | task-38 |
| [task-40](#task-40) | Implement refresh families and secure CLI login | task-34, task-37, task-38, task-39 |
| [task-41](#task-41) | Implement the independent WIF node issuer | task-30, task-35 |
| [task-42](#task-42) | Bind genesis and committed membership to peer TLS | task-20, task-26, task-30, task-41 |
| [task-43](#task-43) | Compose secure daemons and qualify the native preview | task-09, task-16, task-27, task-32, task-33, task-37, task-40, task-42 |
| [task-44](#task-44) | Implement the Go postcard subset and shared fixtures | task-03 |
| [task-45](#task-45) | Implement the Go QUIC client and workload credentials | task-34, task-36, task-44 |
| [task-46](#task-46) | Implement Kine driver registration and CRUD/range backend | task-17, task-43, task-45 |
| [task-47](#task-47) | Complete Kine watches, progress, compaction and TTL | task-13, task-14, task-16, task-46 |
| [task-48](#task-48) | Certify the selected Kubernetes storage profile | task-47, task-j08 |
| [task-49](#task-49) | Export canonical shared checkpoints through portable snapshots | task-14, task-18, task-27 |
| [task-50](#task-50) | Install learner snapshots and reconcile catch-up state | task-25, task-42, task-49 |
| [task-51](#task-51) | Implement conservative all-voter checkpoint trimming | task-26, task-49, task-50 |
| [task-52](#task-52) | Model quorum-safe checkpoint activation | task-19, task-51 |
| [task-53](#task-53) | Implement quorum-safe checkpoint floors and recovery | task-26, task-50, task-52 |
| [task-54](#task-54) | Model sealed membership handoff | task-19, task-26, task-53 |
| [task-55](#task-55) | Persist old-configuration sealing and terminal recovery | task-25, task-26, task-54 |
| [task-56](#task-56) | Establish the unique handoff certificate | task-49, task-55 |
| [task-57](#task-57) | Activate the successor and recover interrupted handoffs | task-42, task-50, task-53, task-56 |
| [task-58](#task-58) | Implement node/key credential lifecycle and fencing tests | task-41, task-42, task-57 |
| [task-59](#task-59) | Implement backup, restore and disaster-recovery commands | task-49, task-50, task-57, task-58 |
| [task-60](#task-60) | Implement format/capability upgrades and rollback guards | task-03, task-07, task-50, task-57 |
| [task-61](#task-61) | Complete bounded observability and operator diagnostics | task-31, task-43, task-53, task-57 |
| [task-62](#task-62) | Build and run the matched native WAN benchmark matrix | task-29, task-32, task-43, task-53, task-61 |
| [task-63](#task-63) | Measure Kine end-to-end overhead and regression budgets | task-48, task-61, task-62 |
| [task-64](#task-64) | Run mixed-fault qualification and automatic minimization | task-09, task-27, task-32, task-40, task-48, task-53, task-57, task-58, task-60, task-d01, task-d03, task-d05, task-d06, task-d07, task-d08, task-d09, task-d10, task-d11, task-d12, task-d14, task-d15, task-d18, task-d19, task-d20, task-d21, task-d22, task-d23, task-d24, task-d25, task-d26, task-d27, task-d28, task-d29, task-d30, task-d31, task-d32, task-d33, task-d34, task-d35, task-d36, task-d37, task-d38, task-d39, task-d40, task-d41, task-d42, task-d43, task-d44, task-d45, task-d46, task-d47, task-d48, task-d49, task-d51, task-d52, task-d53, task-d56 |
| [task-65](#task-65) | Package and qualify supported deployment targets | task-43, task-48, task-59, task-60, task-61, task-d02, task-d04 |
| [task-66](#task-66) | Close security, supply-chain and production release gates | task-58, task-59, task-60, task-63, task-64, task-65, task-d02, task-q01 |
| [task-s01](#task-s01) | Define the portable engine contract and logical collection registry | task-02, task-04 |
| [task-s02](#task-s02) | Implement the model engine and common storage conformance kit | task-05, task-s01 |
| [task-s03](#task-s03) | Add an experimental single-writer Fjall adapter | task-08, task-s02 |
| [task-s04](#task-s04) | Replay fresh fixtures and compare local engine costs early | task-09, task-14, task-17, task-18, task-s03 |
| [task-j01](#task-j01) | Define the journal, sequence and materialization contracts | task-01, task-02, task-s01, task-s02 |
| [task-j02](#task-j02) | Implement the pinned raft-engine journal and postcard codec | task-j01 |
| [task-j03](#task-j03) | Integrate journal-first shared storage and atomic materialization | task-j02, task-08, task-11 |
| [task-j04](#task-j04) | Publish local recovery checkpoints and reclaim journal prefixes | task-j03, task-09 |
| [task-j05](#task-j05) | Qualify the real journal and composed persistence boundary | task-j02, task-j03, task-j04, task-j08, task-09 |
| [task-j06](#task-j06) | Replay-backed working-state materialization, promoted, off by default | task-j04; task-j05 before it may be a default |
| [task-j07](#task-j07) | Validate multi-group batching and resource isolation | task-j03, task-j05, task-31 |
| [task-j08](#task-j08) | Compose journal-backed application and serving storage | task-j03, task-43 |
| [task-j09](#task-j09) | Establish a caller's session as a replicated command | task-18, task-37, task-j08 |
| [task-j10](#task-j10) | Give the Rust client a real unary request path | task-34, task-43 |
| [task-o01](#task-o01) | Specify finalized frames and observer capabilities | task-02, task-03, task-28, task-49 |
| [task-o02](#task-o02) | Build MVCC observer install, catch-up and serving lifecycle | task-o01, task-j03, task-50 |
| [task-o03](#task-o03) | Add regional relays, bounded fan-out and source failover | task-o02, task-31 |
| [task-o04](#task-o04) | Route Kine watches to observers with correct progress | task-o02, task-48 |
| [task-o05](#task-o05) | Add observer historical reads and authoritative read fences | task-o04, task-18 |
| [task-o06](#task-o06) | Qualify observer correctness and regional scaling | task-o03, task-o04, task-o05, task-j05 |
| [task-m01](#task-m01) | Define authoritative configuration discovery and epoch records | task-02, task-19 |
| [task-c01](#task-c01) | Give the collector a submission delivery lifecycle (contract revision 3) | task-33, task-62 |
| [task-m02](#task-m02) | Make Kine a full epoch-aware trusted collector | task-m01, task-33, task-48, task-c01 |
| [task-m03](#task-m03) | Connect observer staging to sealed handoff and activation | task-m01, task-o02, task-57, task-j04, task-58, task-d40, task-d41 |
| [task-m04](#task-m04) | Implement conservative regional placement and quorum tuning | task-m03, task-m02 |
| [task-m05](#task-m05) | Qualify client-aware membership under mixed failures | task-m02, task-m03, task-m04, task-58, task-d01 |
| [task-c02](#task-c02) | Repair lost frontend evidence, and complete a half-held command from the durable record (contract revision 2) | task-23, task-33, task-62 |
| [task-c03](#task-c03) | Give the collector boundary its own monotonic clock | task-33, task-37 |
| [task-d01](#task-d01) | Wire leader election and ballot adoption into coordd | task-26, task-27, task-43, task-j08, task-d03 |
| [task-d02](#task-d02) | Drive leaf renewal inside the serving daemon | task-41, task-43, task-58 |
| [task-d03](#task-d03) | Re-dial peers and collector links on a timer | task-43, task-j08, task-c01 |
| [task-d04](#task-d04) | Provision a multi-host test domain and write its runbook | task-43, task-48, task-d03 |
| [task-d05](#task-d05) | Bound recovery reports and Syncs by what the voters executed | task-26, task-53, task-d01, task-d06 |
| [task-d06](#task-d06) | Keep one execution order on every replica when a table reclaims | task-21, task-24, task-c02 |
| [task-d07](#task-d07) | Re-send a proposal until every voter has voted on it | task-23, task-25, task-d03, task-d06 |
| [task-d08](#task-d08) | Bring a lagging voter up from a peer's executed history | task-d05, task-d09, task-d14, task-d17 |
| [task-d09](#task-d09) | Carry the leader's commit decision to every voter | task-24, task-d07 |
| [task-d10](#task-d10) | Flow-control catch-up from each voter's own frontier | task-25, task-d09 |
| [task-d11](#task-d11) | Leave no acceptance of an earlier ballot behind a Sync | task-26, task-d05 |
| [task-d12](#task-d12) | Chain a new leader's proposals after what it executed | task-26, task-d06 |
| [task-d13](#task-d13) | Keep a diverged node stopped across a restart | task-d17 |
| [task-d14](#task-d14) | Name a recovered decision's admission facts | task-d09, task-d12 |
| [task-d15](#task-d15) | Ask again for every vote the leader still needs | task-d07 |
| [task-d16](#task-d16) | Say why each address of a dial failed | task-d03 |
| [task-d17](#task-d17) | Say what a divergence stop compared | task-d12 |
| [task-d18](#task-d18) | Never let a Sync lower a durable promise | task-20, task-26, task-d11 |
| [task-d19](#task-d19) | Count only adoptions toward the slow majority | task-24, task-26, task-28 |
| [task-d20](#task-d20) | Prove the largest Sync fits its row, or refuse the campaign | task-d05, task-d14 |
| [task-d21](#task-d21) | Settle whether a recovery cycle is reachable, and never stall on one | task-26, task-d12 |
| [task-d22](#task-d22) | End every collector entry the voters refuse | task-c01, task-c02, task-d14 |
| [task-d23](#task-d23) | Tell a client what is known of its outcome | task-34, task-c02, task-d22 |
| [task-d24](#task-d24) | Keep table room for recovery work | task-d08, task-d19, task-d20 |
| [task-d25](#task-d25) | Make catch-up outpace the domain | task-d08, task-d18, task-d19 |
| [task-d26](#task-d26) | State the resource contract and test its accounting | task-d20, task-d24, task-d25 |
| [task-d27](#task-d27) | Wire quorum-safe forgetting into coordd | task-53, task-j04, task-d08, task-d26 |
| [task-d28](#task-d28) | Let a recovery report survive a lost page | task-25, task-d05 |
| [task-d29](#task-d29) | Write the failure and obligation contract | task-d18, task-d19, task-d22 |
| [task-d30](#task-d30) | Run the real replica machines in the deterministic simulator | task-05, task-d18, task-d19, task-d34 |
| [task-d31](#task-d31) | Refuse epochs other than three or five voters and read the source fast set from its ballot | task-26, task-m01 |
| [task-d32](#task-d32) | Reinstall a voter behind the forgetting floor as a learner | task-50, task-d27, task-d41 |
| [task-d33](#task-d33) | Hold the simulated domain to its budgets and to progress after healing | task-d22, task-d24, task-d26, task-d28, task-d30 |
| [task-d34](#task-d34) | Keep every decision through recovery where the protocol simulator lost one | task-d11, task-d18, task-d19, task-d21 |
| [task-d35](#task-d35) | Chain every replica's execution | task-49, task-d08, task-d12, task-d17 |
| [task-d36](#task-d36) | Compare execution chains between voters | task-d13, task-d35 |
| [task-d37](#task-d37) | Scrub the replicated state at agreed positions | task-49, task-d13, task-d36 |
| [task-d38](#task-d38) | Bind the handoff to the execution chain | task-57, task-d35, task-d37, task-d39, task-d43 |
| [task-d39](#task-d39) | Follow the domain as a staged learner | task-50, task-d08, task-d37, task-d42 |
| [task-d40](#task-d40) | Install an activated configuration into the running daemon | task-57, task-58, task-m01, task-d01, task-d39, task-d42, task-d43, task-d44 |
| [task-d41](#task-d41) | Replace one voter through the sealed handoff | task-59, task-d13, task-d38, task-d39, task-d40, task-d42, task-d43 |
| [task-d42](#task-d42) | Record an authorized, prepared transition | task-54, task-55, task-m01 |
| [task-d43](#task-d43) | Recover the terminal closure after the seal | task-55, task-56, task-d05, task-d11, task-d14, task-d44 |
| [task-d44](#task-d44) | Serve executed history across a transition | task-d08, task-d35, task-d39, task-d42 |
| [task-d45](#task-d45) | Measure a command's cost on every node, and gate on it | task-61, task-j08 |
| [task-d46](#task-d46) | Keep per-turn and per-event work independent of history | task-d06, task-d24, task-d27, task-d30, task-d45 |
| [task-d47](#task-d47) | Lower a turn's transitions as durable groups | task-j03, task-j05, task-j08, task-d24, task-d30, task-d45, task-d46 |
| [task-d48](#task-d48) | Commit the projection in one phase under the journal | task-53, task-59, task-j04, task-j05, task-d47 |
| [task-d49](#task-d49) | Re-send a proposal only once its answer is due | task-d07, task-d08, task-d15, task-d45 |
| [task-d50](#task-d50) | Serve reads and the fast path without waiting on the slow path | task-28, task-29, task-d46, task-d47, task-d49 |
| [task-d51](#task-d51) | Export the local checkpoint off the domain thread, and bound it | task-j04, task-d55 |
| [task-d52](#task-d52) | Execute and materialize on a pipelined applier | task-j03, task-j08, task-d47, task-d48 |
| [task-d53](#task-d53) | Establish an execution without walking what already executed | task-21, task-24, task-d46 |
| [task-d54](#task-d54) | Append the journal's groups on a journal worker | task-j03, task-d47, task-d52 |
| [task-d55](#task-d55) | Measure a publication's steps, a restart's replay and the domain thread's scheduling | task-j04, task-j06, task-d45, task-d54 |
| [task-d56](#task-d56) | Keep a restarted voter from stalling the domain | task-31, task-d01, task-d08, task-d10 |
| [task-d57](#task-d57) | Bound a read's index by what the confirming voters had voted | task-d50 |
| [task-d58](#task-d58) | Serve a turn's held reads from one snapshot | task-11, task-d50 |
| [task-d59](#task-d59) | Re-send without walking every proposal the leader keeps | task-d49 |
| [task-d60](#task-d60) | Allocate less on the domain thread | task-d45 |
| [task-d61](#task-d61) | Send a turn's frames to one peer together | task-31, task-d45, task-d62 |
| [task-d62](#task-d62) | Count what a command costs and why its fast path failed | task-d45, task-d50 |
| [task-d63](#task-d63) | Execute speculatively on the serving path under the existing release gate | task-29, task-d52 |
| [task-d64](#task-d64) | Design collector-side early completion | task-d63 |
| [task-d65](#task-d65) | Decide the conflict model | task-d62 |
| [task-d66](#task-d66) | Publish the pointer and retire on the journal worker | task-d51, task-d54 |
| [task-d67](#task-d67) | Keep the fast path past a pre-acceptance the leader never ordered | task-d24, task-d34, task-d71 |
| [task-d68](#task-d68) | Synchronize a follower's path log across a leader change | task-d24, task-d34 |
| [task-d69](#task-d69) | Release the outbox by what a completion makes releasable | task-d45, task-d46 |
| [task-d70](#task-d70) | Fewer datagrams and wakeups per command on the transport | task-d61, task-d62 |
| [task-d71](#task-d71) | Name the order the possible-fast rule reads, and keep the reserve's bound | task-d09, task-d26, task-d33, task-d34 |
| [task-d72](#task-d72) | Close the gap to etcd on one host | task-d62, task-d63, task-d66 |
| [task-d73](#task-d73) | Read why a lowering group ends, and size a bounded wait | task-d61, task-d62, task-d72 |
| [task-d74](#task-d74) | Send acknowledgements to the leader and the frontend only | task-d09, task-d62, task-d72 |
| [task-d75](#task-d75) | Take work off the leader's domain loop | task-d53, task-d60, task-d72 |
| [task-d76](#task-d76) | Choose a whole fast set after a failure | task-d31, task-d68 |
| [task-d77](#task-d77) | Carry what a candidate executed at COMMIT, never re-propose it | task-d10, task-d71 |
| [task-d78](#task-d78) | Stop a fast decision's path evidence at a command the selection holds decided | task-d71, task-d77 |
| [task-d79](#task-d79) | Stop campaigning on an own acceptance nobody can supply, until a Sync is installed | task-d05, task-d78 |
| [task-d80](#task-d80) | Keep a commit through the Sync that leaves it out | task-d11, task-d20, task-d79 |
| [task-d81](#task-d81) | Let a voter ahead serve the voters waiting on it | task-d08, task-d10, task-d25, task-d80 |
| [task-d82](#task-d82) | Bound a leader's silence below the transport's idle timeout | task-d01, task-d03 |
| [task-d83](#task-d83) | Serve read-only transactions at the read barrier | task-d50, task-d72 |
| [task-d84](#task-d84) | One journal sync a turn at every voter | task-d47, task-d54, task-d63, task-d72 |
| [task-d85](#task-d85) | Let a Sync sent again wait for its marker | task-d11, task-d24 |
| [task-d86](#task-d86) | Fewer projection commits per command | task-d48, task-d52, task-d72 |
| [task-d87](#task-d87) | Let held reads share one confirmation round | task-d50, task-d58, task-d72 |
| [task-d88](#task-d88) | Fewer datagrams per command on the api plane | task-d70, task-d74 |
| [task-d89](#task-d89) | Write a fresh proposal's acceptance in its proposal batch | task-d19, task-d84 |
| [task-d90](#task-d90) | Release a result before its execution group is durable | task-d06, task-d12, task-d35, task-d63, task-d84 |
| [task-d91](#task-d91) | Read and cut wake and hop latency | task-d55, task-d72 |
| [task-d92](#task-d92) | Cut a local checkpoint publication's cost a record | task-d51, task-d55, task-d72 |
| [task-d94](#task-d94) | Ask for a page when a voter's frontier creeps | task-d08 |
| [task-d95](#task-d95) | A local fault reading a retained result's authorization stops the voter | task-d12 |
| [task-d96](#task-d96) | Size the messages a command sends, before cutting them | task-d70, task-d72, task-d88 |
| [task-d97](#task-d97) | Read what a `service` p99 operation waits for | task-d55, task-d72, task-d91 |
| [task-d98](#task-d98) | Send the leader's proposal before its batch is durable | task-d30, task-d74, task-d89, task-d99 |
| [task-d99](#task-d99) | Activate a replicated feature in a running domain | task-60 |
| [task-d100](#task-d100) | Answer on the release, then relay a write through the leader while rounds are slow-only | task-d74, task-d90, task-d98, task-d99 |
| [task-d101](#task-d101) | Let followers do less: what reads a follower's projection, and what each option saves | task-d12, task-d86, task-d90 |
| [task-d102](#task-d102) | A contended round is a batch: one frame, one group and one acknowledgement a turn | task-d61, task-d84, task-d86, task-d99, task-d100, task-d101 |
| [task-q01](#task-q01) | Produce the combined durable WAN/Kine qualification report | task-j07, task-j08, task-o06, task-m05, task-63, task-64, task-d45, task-d46, task-d47, task-d48, task-d49, task-d51, task-d52, task-d53 |

## Task specifications

<a id="task-01"></a>
### task-01: Lock the workspace and implement filtered GitHub CI

**Prerequisites:** None.  
**Design:** Sections 12.4.1, 16, 21.3.

**Implement:** Create Rust/Go workspace skeleton, exact toolchain files, Cargo.lock/go.sum, explicit TLS features and checksummed tool manifest. Add xtask, format/lint/test entry points, dependency policy, Mermaid rendering and references. Include the full raft-engine Git candidate, feature/platform audit and codec smoke test; do not silently change other selections.

Add the actual GitHub Actions entry/documentation workflows and reusable Rust/Go build-test workflow, plus a repository-owned filter script and fixtures, implementing design Section 12.4.1. The lightweight classifier gates expensive jobs before toolchain setup; docs-only changes still run documentation checks and report the stable required CI gate. Pin actions/tools, use minimal permissions and preserve a manual/scheduled full-run path. These are deliverables of task-01, not scripts to add to the design package.

**Acceptance:** Build the selected graph on Linux x86_64/aarch64; record actual compiler/MSRV and feature trees. Fail checks for insecure test dependency in production, missing locks, malformed Mermaid or unreviewed override. Resolve candidate incompatibilities explicitly.

Test docs-only and mixed changes; Rust/Go source, lock/toolchain/schema/fixture/model/workflow/filter changes; unknown paths; additions/deletions/type changes/renames; more than 300 changed files; missing/shallow history; PR/push/merge-group events; and manual/scheduled full runs. Demonstrate that a docs-only PR starts no heavy build job, a code-bearing change starts the appropriate jobs, and a failed/missing/cancelled required job or classifier never yields a green gate or a permanently pending path-filtered required check. Check Markdown references, every full task ID/anchor, graph consistency and Mermaid in the lightweight job.

**Review boundary:** No service/protocol implementation. Proposed pins are not presumed compile-tested. This describes future repository checks, not local authoring scripts included in the design package.

The filter/workflows are maintained repository CI, not the temporary authoring/validation utilities excluded from the design package. No service/protocol implementation or placeholder success for not-yet-implemented test suites.

<a id="task-02"></a>
### task-02: Define identities, canonical commands and ordered-key fixtures

**Prerequisites:** task-01.  
**Design:** Sections 2.3, 4.4, 10.5.1, 17.2.

**Implement:** Add coord-types logical_v1, fixed IDs, checked revisions, stable invocation identity/errors and canonical hashes. Freeze namespace/key/revision encoding vectors. Reserve configuration epochs, finalized-frame/read-fence identities and local journal sequence separately.

**Acceptance:** Property-test encoded ordering including zeros/prefixes. Payload change changes digest; token/endpoint/epoch refresh does not change the same logical request. Reject overflow, ambiguous encodings and mistaken identity/counter reuse.

**Review boundary:** No network, ambient clocks/random IDs inside core or automatic schema evolution.

<a id="task-03"></a>
### task-03: Implement the bounded postcard wire codec

**Prerequisites:** task-02.  
**Design:** Sections 11.2, 19.1, 6.7.1, 10.5.

**Implement:** wire_v1 DTOs, frame reader/writer, explicit kinds/versions, bounded Serde types, valid/invalid vectors and fuzz target. Reserve distinct observer/configuration/collector-evidence/read-fence schemas; durable journal representation is separately versioned.

**Acceptance:** Reject all header truncations, integer/length overflow, trailing bytes, unknown versions and oversized nested collections before excessive allocation. Identity-bearing payloads canonicalize consistently.

**Review boundary:** No sockets or general Go Serde framework. Stable DTOs, not implementation-enum serialization.

<a id="task-04"></a>
### task-04: Establish pure event/effect and durable-barrier interfaces

**Prerequisites:** task-02.  
**Design:** Sections 5.1, 4.8, 18.

**Implement:** Add injected clocks/entropy, owned events/effects, PersistBatch, incarnation/boot-scoped barriers and private established/admission capabilities. Distinguish JournalDurable, Materialized and LocalCheckpointPublished from protocol establishment. Vote-producing effects carry epoch/ballot/prerequisite context; supply test ports only.

**Acceptance:** A two-barrier effect cannot release after one. Wrong-boot, duplicate/failed and obsolete-ballot completions never newly authorize it. Compile-boundary checks exclude Tokio, engine crates/system clocks from pure core.

**Review boundary:** No actual database, consensus or insecure production composition; types support but do not prove learning predicates.

<a id="task-05"></a>
### task-05: Build the deterministic world and replay format

**Prerequisites:** task-04.  
**Design:** Sections 12, 21.1.

**Implement:** Ordered virtual scheduler, lifecycle crashes/restarts, clocks, messages and named ChaCha streams. Versioned replay bundles include source/build/config/lock and explicit schedules. Add deliberately faulty actor.

**Acceptance:** Identical replay has identical visible history/digests; insertion ties explicit. An omitted durable prerequisite is reproducibly caught/minimized and saved. Wrong version fails clearly, not a false reproducibility claim.

**Review boundary:** Logical simulation is not real-engine or QUIC packet coverage. No production secret/test entropy leakage.

<a id="task-06"></a>
### task-06: Add an independent history oracle

**Prerequisites:** task-02, task-05.  
**Design:** Sections 6, 12.3, 21.1.

**Implement:** Separate reference model and complete-domain linearizability/revision/retry/conditional checker with extensible lease/auth/watch observations and pending-operation treatment.

**Acceptance:** Reject injected stale read after acknowledged write, duplicate mutation, wrongly shared revision and missing transaction event; accept valid concurrent histories. Keep performance and correctness reporting separate.

**Review boundary:** Do not reuse production planner as oracle or partition shared-revision/transaction/policy histories by key.

<a id="task-07"></a>
### task-07: Implement the redb adapter and fail-closed generation lifecycle

**Prerequisites:** task-01, task-02, task-s01, task-s02.  
**Design:** Sections 5.2, 17.1, 17.9–17.11, 17.13.

**Implement:** coord-storage-redb maps pinned cross-table snapshots, byte tables, explicit durable transactions and typed failures to common contract. Verified manifests/root locks gate opening; codecs remain common. This is the strict materialization/reference foundation; journal-first production arrives in task-j03.

**Acceptance:** Common ordered-access/transaction suites pass; wrong origin/domain/generation/engine, empty/missing/corrupt files fail closed, duplicate open excluded. Cross-catalog atomicity includes read-your-writes scans.

**Review boundary:** No implicit create-on-open, engine-local application semantics, bespoke WAL, Fjall implementation or migration. A real-engine pass does not establish composed journal recovery.

<a id="task-08"></a>
### task-08: Implement the shared single-writer storage coordinator

**Prerequisites:** task-04, task-07.  
**Design:** Sections 17.3, 17.8–17.10.

**Implement:** Strict reference StoreWorker<E>, guard validation in transactions, shared update lowering/stamps, bounded grouping and durable-view gate; call commit_durable, not redb in common code. Preserve as a reference increment; task-j03 introduces authoritative journal-first transitions and distinct projection events.

**Acceptance:** Model/redb fixtures cover visibility before completion, definite guard rejection, indeterminate commit, lost/old-boot completion. No view/publication escapes without required support. Unrelated protocol stamp updates do not invalidate application predecessor.

**Review boundary:** No direct native engine imports in coord-storage, unbounded blocking work, public storage sequences or silently weakened periodic flush profile.

<a id="task-09"></a>
### task-09: Exercise the actual redb engine with disk faults

**Prerequisites:** task-05, task-08.  
**Design:** Sections 17.3, 17.14, 21.2.

**Implement:** Pinned real redb StorageBackend with volatile/durable images and controlled writes/sync; shared fixtures, reopen and subprocess death, plus generation-manager directory faults.

**Acceptance:** Crash at each bounded transaction write/sync boundary and reopen only permitted states. Test partial persistence on sync error, ENOSPC, durable-prefix corruption quarantine and prevention of destructor flush after simulated crash.

**Review boundary:** Model engine is not byte-level redb testing. This does not certify Fjall, arbitrary OS power loss, raft-engine or the composed boundary in task-j05.

<a id="task-10"></a>
### task-10: Implement the pure KV and transaction planner

**Prerequisites:** task-02, task-04, task-06.  
**Design:** Sections 6.1–6.3, 17.4.

**Implement:** Exact/range views, compares, put/delete, chosen transaction branch, revisions and deterministic owned ReadView/ApplyPlan using fixture views. Include semantic work/response limits.

**Acceptance:** Check create/mod/version, absence, byte intervals, failed/read-only/no-op revisions and one revision for an atomic multi-key mutation. Limits fail before partial changes.

**Review boundary:** No production DB, clock-dependent lease expiry or ambient time in planner.

<a id="task-11"></a>
### task-11: Apply shared KV plans atomically and serve pinned MVCC views

**Prerequisites:** task-08, task-09, task-10.  
**Design:** Sections 17.2, 17.4, 17.9–17.10.

**Implement:** Common bounded view construction/current/history/events/execution updates through state port; ApplyBase check, fixed-revision scan and owned pages without engine imports. task-j03 later journals immutable plans before this same atomic materialization.

**Acceptance:** Model/redb identical fixtures; crash leaves no partial events/frontier mismatch; choose history versions before limit. Stale base replans; page/reverse/boundary and ahead-of-durability views are tested.

**Review boundary:** Pinned snapshot is not linearizable authority or quorum establishment. No per-engine MVCC implementations.

<a id="task-12"></a>
### task-12: Persist deduplication, result resolution and retry floors

**Prerequisites:** task-11.  
**Design:** Sections 6.5, 17.1, 10.5.3.

**Implement:** Atomic request digest/result/executed identity and effects; bounded outstanding window, ResolveRequest and replicated retirement floor. State is transferable across epochs and exact replay.

**Acceptance:** Lost response/same ID yields same currently authorized logical result. Changed payload rejected; materialization-notification crash does not duplicate. Retired/unknown session request cannot execute as new work.

**Review boundary:** No infinite retention or exactly-once guarantee across lost upstream invocation identity.

<a id="task-13"></a>
### task-13: Implement atomic watch replay and live handoff

**Prerequisites:** task-11, task-12.  
**Design:** Sections 6.4, 6.8.2–6.8.3, 19.3.

**Implement:** Replay/live registration frontier, complete revisions, bounded queues, ordered progress and resumable close/cancellation.

**Acceptance:** Mutation during registration appears with no gap; slow consumers cannot advance over omitted changes. Fragmented multi-key revision remains atomic. Loom covers local handoff boundary; progress cannot overtake pending events.

**Review boundary:** No public watch before auth composition and no progress from socket receipt.

<a id="task-14"></a>
### task-14: Add bounded MVCC compaction

**Prerequisites:** task-11, task-13.  
**Design:** Sections 6.4, 17.5.

**Implement:** Ordered retention floor and incremental history/event GC preserving needed value/tombstone at/before boundary and newer versions. Explicit active-view/watch retention. Common layer chooses deletion; physical engines reclaim afterward.

**Acceptance:** Before-floor reads return Compacted; later reads retain untouched old values. Pagination/watch resume either maintain history or fail explicitly, never skip gaps.

**Review boundary:** No semantic protocol trimming, engine-owned TTL/filter or exclusive live-file compaction.

<a id="task-15"></a>
### task-15: Implement native lease grant, attachment and revoke

**Prerequisites:** task-10, task-11, task-12.  
**Design:** Sections 7.1, 17.1.

**Implement:** Stable nonreused lease IDs/generations, owner permissions, reverse index and atomic attach/detach/revoke plans with count/worst-case byte quotas. Keep Kine private bindings distinct.

**Acceptance:** Revoke deletes only current attachments atomically; later value growth cannot evade event budget. Retry does not grant twice/allocate new revision. Unauthorized attachment cannot imply protected-key deletion.

**Review boundary:** No timers/keepalive success outside consensus or automatic logout-based ownership change.

<a id="task-16"></a>
### task-16: Implement replicated renewal and conservative expiry

**Prerequisites:** task-05, task-06, task-15.  
**Design:** Sections 7.2–7.4.

**Implement:** Renewal sequence, replicated expiry authority epoch, timer generations and conditional expiration; conservative recovery rearming under clock assumptions. Renewals stay replicated with exact retry semantics.

**Acceptance:** Renewal/expiry permutations, delayed old leader, restart, no quorum and fast-clock bounds are checked against true simulator time. Stale timer cannot delete renewed/rebound keys; delayed reply creates no new TTL anchor.

**Review boundary:** No exact expiry deadline, observer-local keepalive success or external fencing implied by lease alone.

<a id="task-17"></a>
### task-17: Add native atomic Kine primitives and private TTL bindings

**Prerequisites:** task-12, task-16.  
**Design:** Sections 6.6, 19.5.

**Implement:** Create/CAS-update/conditional-delete returns all revision/conflict metadata in one result. Kine TTL seconds map to private per-key binding; zero detaches. Stable invocation derives binding identity; expiry is conditional.

**Acceptance:** Failed CAS changes neither data nor expiry. Replaced TTL fences prior timer. Retries preserve binding. Trace proves one logical operation, no mandatory pre-read/CurrentRevision/lease-grant round trip.

**Review boundary:** No Go code, nativeLeaseID=TTL or unconditional local deletes.

<a id="task-18"></a>
### task-18: Implement replicated sessions, policy and grant commitments

**Prerequisites:** task-10, task-11, task-12.  
**Design:** Sections 9.2–9.3, 20.2–20.3.

**Implement:** Principal/ceiling, session/rule generations, one-time receipt/code commitments and ordered permission/revocation at execution. Extend independent oracle; policy can advance execution without KV revision.

**Acceptance:** Check branch-specific comparisons/operations, full range containment, lease restrictions and policy changes after admission. Revoked users cannot read protected cached retry outcomes.

**Review boundary:** No external JWT verification/signing/network/clock during deterministic replay.

<a id="task-19"></a>
### task-19: Freeze SwiftPaxos source mapping and bounded models

**Prerequisites:** task-02, task-04, task-05.  
**Design:** Sections 4, 5.1, 18.1, 21.6.

**Implement:** Pin paper/code source rules, guards, full path-learning predicates, C2 memberships and durable publication obligations. Model concrete phases, atomic dependency publication, recovery cuts and source-defined candidate selection with traceability.

**Acceptance:** Every handler/field maps to source or marked extension. Reject arbitrary fastest-majority, observer/duplicate votes and half-initialized dependencies. Permute valid phase/preaccept reports without replacing source selection by highest-phase-wins. Save counterexamples.

**Review boundary:** No optimization or unrestricted proof claim from finite models. Upstream implementation/invariant mismatches are neither proved fundamental failure nor automatically resolved.

<a id="task-20"></a>
### task-20: Persist ballots, promises and configuration guards

**Prerequisites:** task-08, task-19.  
**Design:** Sections 4.1, 4.7–4.8, 5.1, 18.1.

**Implement:** Stable promises/config-role/generation guards and source-required protocol rows. Wire recovered promises to actor. Carry same-boot epoch/ballot context on effects as well as boot identity; production persistence later uses task-j03 events.

**Acceptance:** Promise replies wait for complete durable state. Old messages cannot lower it after restart. Wrong configuration identity does not vote. Delay callbacks/batcher across election; bookkeeping cannot authorize a new obsolete vote. Atomic initialized-state/index publication and dependency-phase prerequisites hold.

**Review boundary:** No full recovery selection or Raft term semantics. Future journal integration must rerun these tests at its cut, not presume reference tests suffice.

<a id="task-21"></a>
### task-21: Implement the dependency graph and closure evidence

**Prerequisites:** task-19, task-20.  
**Design:** Sections 4.2–4.9, 18.1–18.3.

**Implement:** Immutable payload binding, path/predecessor state, exact traversal/closure, source phase guards and bounded incremental work. Publish initialization and dependency lookup atomically; persist required dependencies.

**Acceptance:** Direct-set equality differs from full path evidence. Duplicate/reordered messages converge; conflict arriving while another command initializes cannot see START as processed state. Bounds backpressure without deleting unresolved acceptance. Dependencies reach required accept/commit/execute phase before dependent transition.

**Review boundary:** No path compression, per-key conflict relaxation or receipt-order execution.

<a id="task-22"></a>
### task-22: Implement normal leader proposal handlers

**Prerequisites:** task-20, task-21.  
**Design:** Sections 4.1–4.8, 18.

**Implement:** Source-mapped leader transitions/publication, conservative conflicts and ballot-fixed fast set. Use atomic initialization and prerequisite-gated effects.

**Acceptance:** Golden traces match models; every leader reply has exact stable support. Reordered/duplicate client requests cannot bind conflicting payload. Block premature accept/commit/finalized execution while dependencies lag.

**Review boundary:** No learning shortcut, speculative public response or recovery-selection implementation hidden here.

<a id="task-23"></a>
### task-23: Implement normal follower vote and adoption handlers

**Prerequisites:** task-20, task-21, task-22.  
**Design:** Sections 4, 5.1, 18.

**Implement:** Source fast votes/leader-order adoption with all persistence/history prerequisites and atomic phase/index visibility.

**Acceptance:** Leader/follower message races, conflict arrival permutations, duplicate identities and crashes between state/vote preserve learning obligations. Half-initialized dependencies never appear; dependency-phase guards are explicit, not an inherited prototype TODO.

**Review boundary:** Matching direct dependencies is not a complete learning proof. Keep source semantics with new storage event names; no Raft terms/indexes.

<a id="task-24"></a>
### task-24: Implement slow learning and ordered materialization

**Prerequisites:** task-11, task-12, task-13, task-18, task-22, task-23.  
**Design:** Sections 4.3–4.5, 17.4, 18.3.

**Implement:** Conservative slow learner, closed dependency execution, EstablishedResult capability and deterministic application through common materializer; feed complete finalized event frontiers.

**Acceptance:** Three/five-voter logical histories match KV/transaction/retry/policy oracle. One leader response cannot establish success. No watch event precedes irrevocable application. Preserve source learning under later journal-first composition.

**Review boundary:** Externally visible fast completion remains off. No copied read-index shortcut or projection commit mistaken for quorum learning.

<a id="task-25"></a>
### task-25: Implement durable recovery summaries and payload transfer

**Prerequisites:** task-20, task-21, task-23.  
**Design:** Sections 4.8–4.9, 5, 17.1, 19.3.

**Implement:** Bounded source prior-ballot summaries, stable votes, unresolved closure/payload transfer with identity/digest checks and durable prerequisites. Define authoritative recovery cut independent of lagging projection.

**Acceptance:** Incomplete/corrupt pages never count; old required ballot state survives crash. Missing payload fetches/blocks, not fabricated empty command. Hold projection behind journal and old sends/callbacks across recovery. Source phase differences remain legal where allowed; incompatible required-equal candidates are diagnosed.

**Review boundary:** No selection from incomplete summary/application-only snapshot, physical packet-drain correctness assumption or blind union of histories.

<a id="task-26"></a>
### task-26: Implement recovery selection and new-ballot activation

**Prerequisites:** task-19, task-24, task-25.  
**Design:** Sections 4.1, 4.8–4.9, 5, 18.1.

**Implement:** Source recovery cases preserve potentially chosen commands/closure, durably bind selected Sync and activate the recovered ballot; restore exact results/retries. Resolve submitted work at authoritative cut before new reply.

**Acceptance:** Preserve learned outcomes despite lost volatile commit notifications. Competing recovery, delayed replies, lagging projection and same-boot old effects cannot establish divergence. Permuted reports give permitted stable selection; crash after Sync cannot publish incompatible result under same ballot.

**Review boundary:** No membership change, majority-of-anything, highest-phase heuristic, or dropping work for absent COMMIT marker.

<a id="task-27"></a>
### task-27: Qualify fixed-membership crash recovery end to end

**Prerequisites:** task-06, task-09, task-16, task-18, task-26.  
**Design:** Sections 4.9, 5, 12, 21.

**Implement:** Real-engine plus logical-network campaigns around vote/reply/materialization and restart combinations within budget; retain minimal regressions and recovery-report permutations.

**Acceptance:** Acknowledged outputs, retry digests and lease/policy state survive; minority cannot write. Deliberately omit durable record and detect failure. Crash after recovery result publication preserves same-ballot choice. State precise reference versus later composed-storage coverage.

**Review boundary:** Qualification does not hide protocol fixes; those get focused review. No claim that reference redb tests qualify raft-engine automatically.

<a id="task-28"></a>
### task-28: Implement full fast-path learning evidence

**Prerequisites:** task-19, task-23, task-26, task-27.  
**Design:** Sections 4.2–4.9, 18.3.

**Implement:** Exact source path-learning predicate and private establishment object sharing normal/recovery history with slow path.

**Acceptance:** Valid/invalid paths, mixed ballot/epoch, duplicate identity and recovery-phase variations are covered. Forced-slow/fast paths yield equal controlled logical results. Persisted recovery selection remains stable across restart and does not use generic phase priority.

**Review boundary:** No speculative overlay/public response plumbing or arbitrary quorum counter.

<a id="task-29"></a>
### task-29: Add bounded speculative execution and result-release gating

**Prerequisites:** task-12, task-18, task-24, task-28.  
**Design:** Sections 4.3–4.9, 17.4, 18.1.

**Implement:** Disposable deterministic overlays and digests for established fast outcomes; release binds command, closed order, permission and durable recovery evidence.

**Acceptance:** Tentative reordering leaks no value/credential. Fast result followed by crash before COMMIT propagation recovers same outcome; recovery report permutations and restart preserve Sync selection. Speculative events/tokens are impossible at public boundary.

**Review boundary:** No extra mandatory WAN commit phase, weakened predicate or durability to improve charts.

<a id="task-30"></a>
### task-30: Implement the Quinn transport adapter and TLS lifecycle

**Prerequisites:** task-03, task-04, task-08.  
**Design:** Sections 11, 19.1–19.4.

**Implement:** Bounded reliable streams, ALPN, explicit AWS-LC provider, handshake/close and owned dispatch. Separate role negotiation for collectors/voters/observers; isolated test certificates until real issuance.

**Acceptance:** Malformed frames, origin/role/version mismatch fail closed. No application 0-RTT. Transport ACK never triggers durability/establishment. Bound shutdown/work. Sparse necessary connections are supported, not an assumed fleet full mesh.

**Review boundary:** No HTTP3, voting solely from test certificate, public unauthenticated service or untrusted Kine collector role.

<a id="task-31"></a>
### task-31: Implement QUIC traffic isolation and backpressure

**Prerequisites:** task-30.  
**Design:** Sections 3.3, 11.3–11.7, 19.2–19.3.

**Implement:** Explicit CUBIC, bounded control/unary/watch/bulk connections, shared destination budget, fair group scheduling and role-specific replication capacity.

**Acceptance:** Stalled bulk/watch cannot consume all control queues/streams; large frames cannot force unlimited buffering. Measure queue/credit wait distinct from RTT. Relay fan-out respects node/destination admission.

**Review boundary:** No universal no-jitter/latency claim, unbounded pools, idle-delay batching or congestion-budget evasion by more connections.

<a id="task-32"></a>
### task-32: Add packet-level quinn-proto simulation

**Prerequisites:** task-05, task-30, task-31.  
**Design:** Sections 12.2, 21.2.

**Implement:** Pinned packet/time-driven protocol and controlled protocol RNG, separately linked test crypto/identity; shared framing/queue behavior.

**Acceptance:** Reproduce packet loss/reorder/MTU/credit schedules; cross-check message-level visible outcomes. Production graph excludes deterministic keys. Real rustls handshake/Go interoperability independently passes. Include sparse observer/collector topology under bounds.

**Review boundary:** Endpoint RNG alone does not make TLS/all ID generation deterministic.

<a id="task-33"></a>
### task-33: Wire the trusted frontend collector and native dispatch

**Prerequisites:** task-17, task-18, task-29, task-30, task-31.  
**Design:** Sections 3, 4.3, 19.4.

**Implement:** Admission interface, parallel direct voter fan-out, source-exact collector, unary and finalized watch dispatch in test composition. Freeze the collector contract for authorized Go reuse; role-scoped access never becomes general user voting access.

**Acceptance:** Packet traces have no unnecessary serial leader hop; lone reply never releases tentative data. Cancellation preserves identity/outcome resolution. Count voter identities rather than connections; bound per-domain collection.

**Review boundary:** No untrusted SDK votes or production listener before security gate. Full Go/epoch collector integration is task-m02.

<a id="task-34"></a>
### task-34: Implement the Rust SDK request lifecycle

**Prerequisites:** task-03, task-12, task-30, task-33.  
**Design:** Sections 6.5, 11.5, 19.4.

**Implement:** Credential providers, bounded warm pools, stable instance/sequence, deadlines, ResolveRequest and typed retry errors.

**Acceptance:** Reconnect/reset/timeout preserves invocation/payload. Payload change conflicts; ambiguous timeout reports unknown outcome. Stream pressure bounded, no fresh token exchange per operation.

**Review boundary:** No implicit fresh-ID retry or unauthenticated exposure of protocol evidence.

<a id="task-35"></a>
### task-35: Implement hardened external JWT and issuer verification

**Prerequisites:** task-18.  
**Design:** Sections 9, 20.1–20.2.

**Implement:** Distinct OIDC/WIF verifier types, configured algorithms/audiences/claims, bounded JWKS and hardened HTTP. Kubernetes offline JWT and explicit TokenReview are separate modes.

**Acceptance:** Reject mix-up/alg confusion/wrong audience/time/claims, token-directed endpoints and unknown-kid floods. Simulate cache staleness, clock-health failure, issuer and TokenReview outage. Record admitted receipts, never raw JWTs.

**Review boundary:** No signing, arbitrary cloud identity formats or claims directly granting permission.

<a id="task-36"></a>
### task-36: Implement RFC 8693 exchange and service credential signing

**Prerequisites:** task-18, task-33, task-35.  
**Design:** Sections 9.1–9.4, 20.2.

**Implement:** Bounded Axum exchange, canonical receipts, atomic session creation and ES256 tokens with key publication/rotation. Keep keys outside replicated state.

**Acceptance:** Execution rechecks policy changed after verification. Outage/stale keys fail closed; no raw JWT/private keys in storage/logs. Lifetime/scope ceiling and single-use receipt hold.

**Review boundary:** No long-lived WIF refresh or per-operation IdP calls.

<a id="task-37"></a>
### task-37: Bind API sessions and authorize live/replayed output

**Prerequisites:** task-13, task-18, task-33, task-34, task-36.  
**Design:** Sections 6.4–6.5, 6.9.3, 9.3, 19.4.

**Implement:** Persistent auth binding/rebind, expiry, ordered revocation and fresh permission gates for reads, retries and each selected output batch. Share barriers only for already-selected batches.

**Acceptance:** Expired warm connection cannot admit; post-revocation protected data denied even from historical/cached results. Watch progress cannot bypass output barrier. Previously authorized in-flight work follows documented completion semantics.

**Review boundary:** Token admission freezes neither policy for connection lifetime nor retroactive cancellation. Observer ordered policy replay never silently weakens this requirement.

<a id="task-38"></a>
### task-38: Implement OIDC browser login and the service code flow

**Prerequisites:** task-18, task-35, task-36.  
**Design:** Sections 8, 20.1.

**Implement:** Upstream openidconnect client plus service code/PKCE/state/nonce/exact redirects, application azp validation and bounded pending login. Keep service/upstream flow identities separate.

**Acceptance:** Wrong/missing azp and multi-audience, CSRF/mix-up, code/redirect substitution, concurrent tabs and broker restart negative tests. One code creates at most one session.

**Review boundary:** Crate is not a service authorization server; no email-based principal or public CLI secret.

<a id="task-39"></a>
### task-39: Implement device authorization with bounded polling

**Prerequisites:** task-38.  
**Design:** Sections 8.1, 20.1–20.3.

**Implement:** Service device/user codes, browser approval, expiry, bounded poll/backoff and atomic consumption using existing upstream browser login.

**Acceptance:** Concurrent pollers cannot mint multiple sessions. Denied/expired/pending/slow_down handled; attempts/floods bounded. Upstream device grant is not assumed.

**Review boundary:** No secret in user code/URL or unapproved code acceptance.

<a id="task-40"></a>
### task-40: Implement refresh families and secure CLI login

**Prerequisites:** task-34, task-37, task-38, task-39.  
**Design:** Sections 8.2, 20.3.

**Implement:** Rotating family commitments/reuse revocation; coordctl browser/device/logout/refresh with explicit OS keyring stores and serialized shared-credential updates.

**Acceptance:** Lost rotated-secret response requires documented fresh login. Concurrent refresh/revoked family/missing or locked keyring tests. Explicit Apple keychain feature; no plaintext fallback, args/log leakage or simulator entropy.

**Review boundary:** Headless uses WIF, not desktop refresh in deployment config. No unimplemented transparent secret recovery.

<a id="task-41"></a>
### task-41: Implement the independent WIF node issuer

**Prerequisites:** task-30, task-35.  
**Design:** Sections 10.1–10.2, 20.4.

**Implement:** Independently deployed issuer, protected reference CA signer, configured external trust, CSR possession and policy-constructed identities/extensions. Validate CA/key/constraints at startup.

**Acceptance:** Cold enrollment works before any voter. Reject signature/algorithm/SAN/CA/lifetime/workload errors; issuer outage fails closed. Root credentials remain protected outside quorum data.

**Review boundary:** No arbitrary CSR extension copy, quorum-dependent bootstrap or certificate-implies-vote assumption. HSM is optional implementation, not missing required service.

<a id="task-42"></a>
### task-42: Bind genesis and committed membership to peer TLS

**Prerequisites:** task-20, task-26, task-30, task-41.  
**Design:** Sections 10, 17.1, 20.4.

**Implement:** Signed/pinned genesis with durable initialization, ordinary TLS plus committed key/incarnation/role checks and exact voter identity.

**Acceptance:** Wrong origin/stale generation/frontend/observer/learner cannot vote. Duplicate/cloned identity not counted twice. Missing/rolled-back files require quarantine/new-generation lifecycle, not TOFU/reinitialize.

**Review boundary:** Fixed configuration; membership handoff remains later. Normal certificate validation must not be disabled for URI binding.

<a id="task-43"></a>
### task-43: Compose secure daemons and qualify the native preview

**Prerequisites:** task-09, task-16, task-27, task-32, task-33, task-37, task-40, task-42.  
**Design:** Sections 22.1–22.2, 23 G3.

**Implement:** Role-specific binaries, strict TOML, bounded supervised workers, startup/readiness/shutdown and secret-safe diagnostics. Document fixed-member/reference-storage preview restrictions.

Load the genesis manifest only through `verify_genesis` against the configured admin public key: at `init` before anything is pinned, and at every start before the pinned digest is compared. A manifest whose signature does not verify is refused and pins nothing, and `coordd` never reads the manifest as plain JSON. Without this, task-42's "signed/pinned genesis" is a pin without a signature check, and the pin alone is an unauthenticated per-node file (recorded on task-58). This lands as a task-43 follow-up in its own PR at the top of the stack: the change reaches every configuration and genesis fixture the later tasks write, the harness's included, and making it on the task-43 branch would have it re-resolved through each of them. The configuration gains a required `genesis_admin_key`.

**Acceptance:** Cold auth bootstrap, browser/device/WIF, warm expiry, restart, overload and disk quarantine. An unsigned, wrong-key or edited genesis manifest is refused at `init` and at start, before any pin, attach or listener. Production dependency graph excludes test keys/bypasses; cached leadership not fresh-quorum readiness.

**Review boundary:** No general-production claim; lifecycle and journal/observer/client integration gates remain. Reference preview does not supersede selected production architecture.

<a id="task-44"></a>
### task-44: Implement the Go postcard subset and shared fixtures

**Prerequisites:** task-03.  
**Design:** Sections 3.2, 6.6, 19.1.

**Implement:** adapters/kine/wire from frozen frames/schema manifest, required DTOs with checked varints/signed/length/full-consumption handling. Reserve authorized collector/configuration schemas for later client integration.

**Acceptance:** Rust→Go and Go→Rust all valid vectors; malformed corpus rejected within budget. Schema change requires reviewed fixtures and version consequences.

**Review boundary:** No Go voting state machine, cgo/FFI, native protobuf or arbitrary Serde reflection.

<a id="task-45"></a>
### task-45: Implement the Go QUIC client and workload credentials

**Prerequisites:** task-34, task-36, task-44.  
**Design:** Sections 19.4–19.5.

**Implement:** Plain quic-go streams, trust/origin binding, WIF provider/rebind, bounded pools and stable invocation retry/resolve. Handle rotating token files/single-flight refresh.

**Acceptance:** Actual Rust/Go TLS, expiry, stream reset, timeout and warm reconnect pass. No per-operation federation or HTTP3 native transport. Unknown outcomes remain explicit.

**Review boundary:** No exactly-once across lost upstream identity; epoch-aware collection is task-m02.

**Deferred to task-46:** the session binding handshake. On task-45 the client sends only the Hello, reads no HelloAck and sends no Bind, so the service token is obtained but never presented and the Rust frontend refuses the connection's requests as not bound. task-46 keeps the control stream open, reads the HelloAck, presents the token once in a Bind frame and waits for the BindAck.

<a id="task-46"></a>
### task-46: Implement Kine driver registration and CRUD/range backend

**Prerequisites:** task-17, task-43, task-45.  
**Design:** Sections 6.6, 19.5.

**Implement:** Register coord:// in frozen Kine build and exact Start/Get/Create/Update/Delete/List/Count/DbSize/CurrentRevision/error metadata mapping. Bind one domain, handle reserved/health conventions.

Configure the privileged API-server-to-Kine edge according to Section 3.1: restricted local transport or verified mTLS, with Kine accepting only authorized API-server identities for its domain. Same-region placement does not justify plaintext or unauthenticated network access.

**Acceptance:** Actual bridge tests revisions, absent/mismatch metadata, byte intervals/count/pagination and idempotent startup keys. Trace one conditional command, no WAN pre-read/revision follow-up.

Reject insecure network-listener configuration and invalid server/client identity; verify the intended local-only deployment is not reachable by untrusted workloads.

**Review boundary:** No logstructured SQL/TTL wrapper, invented counters or unsupported general etcd transaction claims. Early proxy composition is a test increment, not permanent WAN hop.

<a id="task-47"></a>
### task-47: Complete Kine watches, progress, compaction and TTL

**Prerequisites:** task-13, task-14, task-16, task-46.  
**Design:** Sections 6.6, 6.8, 19.5.

**Implement:** Backend watch, selected pin's synchronization/progress and Compact conventions with private TTL bindings and exact cursors/batches. Make waits cancellable in the chosen edge.

**Acceptance:** Replay/live gaps, future/compacted start, queued progress, reconnect and stale expiration pass through actual bridge. No skipped batch, local unconditional TTL or progress from mere receipt.

**Review boundary:** No assumption native leases match reference Lease API; no mix of old WaitForSyncTo and new EventBatch signatures.

<a id="task-48"></a>
### task-48: Certify the selected Kubernetes storage profile

**Prerequisites:** task-47, task-j08.  
**Design:** Sections 6.6, 6.8.4, 23 G4.

**Gate:** task-j08's served-request and authoritative-cut recovery tests must pass first. Having a local dispatch branch implemented is not that gate: a conformance suite run against a composition that cannot carry a request through to an answer measures nothing. Both that gate and task-j09's are now met.

The harness and the suite have landed and run: `crates/coord-harness` provisions and starts a real three-voter domain outside `cargo test`, `scripts/e2e/` stands the storage edge up in front of it, `adapters/kine/certify` drives the profile with the API server's own client library, and `.github/workflows/kubernetes-certification.yml` runs both that and a k3s control plane on it. The procedure and the current profile -- including the exact supported transaction shapes and the rows that do not pass -- are `docs/operations/kubernetes-certification.md`.

Every row of the profile passes. That is not a compatibility claim -- it is this profile, these operations, this pin, on one host -- and what would turn it into one is widening the suite: regional outage, restore, adapter and API-server restart, and the k3s control plane the workflow's second job runs.

Four gaps that suite found are fixed here. Concurrent callers are served across a quorum: two callers issuing one request each at a time against three voters used to complete 13 of 100 operations. The cause was not the conservative conflict key but the driver, which lowered one journal group per round however many batches that round submitted, so anything decided two-at-once left a batch queued that nothing came back for -- and a follower's acknowledgement, which requires every batch it has outstanding, waited behind it for ever. Watches are served: `Step::Watch` used to count the subscription and drop the responder, and the daemon now holds that stream, replays what the registration named out of one pinned snapshot and drains the hub onto it every time round its loop. Two shape errors in the Go client came out with that -- a watch open that never ended its request half, which the frontend waited out and closed the connection over, and a cancel written onto the stream the frontend delivers on rather than sent as its own request. A key written under a time to live expires: the leader now orders an authority epoch for its own boot, arms every surviving lease from the observation that epoch committed under, and proposes a conditional expiry when a deadline passes -- as two appended canonical operations that execute only for a command accepted with no admission, which only a voter's own proposal is. Getting a leader-originated command to a quorum took two latent defects out of the payload-transfer path: a placeholder was treated as acceptable, so a proposal that arrived before its payload was silently dropped, and a payload arriving for a command already known by identity was discarded rather than bound. And a connection used to stop being answered after exactly 61 requests: nothing retired an executed record from the leader's command table, so its capacity bounded how many commands a replica could execute in its lifetime. None of the four was visible before a harness drove the composition the way an API server does -- the Kine backend holds one session and one in-flight invocation, and every integration test the project had asked one caller a handful of questions.

**Implement:** Real pinned API-server storage/integration suite, exact supported versions/operations/deviations and reproducible commands. This is the compatibility base; task-o04, task-m02 subsequently qualify observer routing/full collection at selected pin.

**Acceptance:** CRUD/CAS, pagination, watch resume/progress, compaction/TTL, concurrent clients and regional failover. Any semantic failure blocks compatibility labeling; clean boot alone is insufficient.

Regional failover is not in the suite the profile above passed, and cannot be until a leader is re-elected after the leader's region is lost: nothing in `coordd` starts a campaign or adopts a higher ballot before task-d01. That acceptance row is open until task-d01 lands and the suite is widened; this task's PR does not certify it and is not held for it.

Test the API-server-to-Kine storage edge directly: unauthenticated, plaintext, wrong-CA, expired and valid-but-unauthorized/wrong-domain clients cannot read or mutate the domain. Verify API-server server-name validation, permitted client rotation and expiry, restricted local transport and absence of insecure fallback. Kubernetes end-user RBAC cannot be bypassed through a reachable Kine listener.

**Review boundary:** No blanket etcd replacement beyond tested profile or inference that later routing inherits conformance automatically.

<a id="task-49"></a>
### task-49: Export canonical shared checkpoints through portable snapshots

**Prerequisites:** task-14, task-18, task-27.  
**Design:** Sections 5.3, 17.6, 17.12, 17.16.1.

**Implement:** SharedCheckpointV1 with bounded canonical traversal/chunks/root at certified execution boundary using one pinned multi-collection view. Common digest excludes local promises/stamps/physical files.

**Acceptance:** Equal common logical state hashes equally despite node-private history/layout. Include required common retry/policy/lease/history. Mutation during export cannot mix views.

**Review boundary:** No raw mutable file copy, identity reuse or authority to trim. This common snapshot is not the LocalRecoveryCheckpointV1 in task-j04 and cannot recreate an existing voter's obligations.

<a id="task-50"></a>
### task-50: Install learner snapshots and reconcile catch-up state

**Prerequisites:** task-25, task-42, task-49.  
**Design:** Sections 10.3, 17.6, 17.13.

**Implement:** Verified shared chunks into inactive selected-engine generation, manifest/identity/schema checks, synced files/directories and active-pointer lifecycle; required unresolved closure transferred separately.

**Acceptance:** Crash each install/pointer step leaves valid selected state or fails closed. Missing/corrupt chunk blocks; learner cannot inherit donor identity or vote early. Error after selection never silently falls back to old generation.

**Review boundary:** No active-epoch promotion, reset of local promises, engine conversion or interchangeability with local recovery checkpoint.

<a id="task-51"></a>
### task-51: Implement conservative all-voter checkpoint trimming

**Prerequisites:** task-26, task-49, task-50.  
**Design:** Sections 5.3, 17.5–17.6, 17.16.5.

**Implement:** Durable identical common checkpoint/floor acknowledgements from every voter, then bounded eligible protocol trimming.

**Acceptance:** Missing voter stops trimming with bounded backpressure rather than evicting obligations. Delayed old messages cannot revive below-floor state. Public MVCC and physical journal retention remain separate; observers supply no trim votes.

**Review boundary:** Conservative reference, not production permanent-loss availability. Local redo compaction cannot substitute for semantic forgetting.

<a id="task-52"></a>
### task-52: Model quorum-safe checkpoint activation

**Prerequisites:** task-19, task-51.  
**Design:** Sections 5.3, 23 G5.

**Implement:** Prepare/readiness/activation evidence, recovery intersections, retained state and stale-message fences for trimming without all voters; bounded TLC variants and field mapping.

`coord-consensus::floor` is the vocabulary and the predicates. A voter records readiness only once it durably holds the checkpoint a candidate names, and readiness is a promise never to vote from below that position -- possession certifies nothing. A majority of the configuration's voters, all for one candidate, activates. Discovery reads promises from a majority, not certificates: a signer promised before any certificate existed and keeps the promise whether or not it ever saw one, and two majorities of one voter set intersect. A voter never records two subjects at one position, which is what makes at most one subject per position certifiable. Nothing here consults a ballot or its leader: a floor belongs to a configuration and outlives every term in it.

**Acceptance:** Signer loss, delayed activation, competing checkpoints, partitions/lagging recovery preserve obligations. Save checked invariants/counterexamples. Observer and physical-log retention do not alter quorum rules.

Every assignment of a readiness script to each of three, four and five voters, with every certification those promises allow and every majority read of them: no position is ever certified for two subjects, no majority read discovers less than a certified floor, installing certificates in any order holds the same floor and never a lower one, and a permanently absent voter does not stop certification. Three rules removed one at a time, each with the counterexample it exists for, frozen under `fixtures/counterexamples`.

**Review boundary:** Majority snapshot copy alone is not activation proof; no imported Raft shortcut.

<a id="task-53"></a>
### task-53: Implement quorum-safe checkpoint floors and recovery

**Prerequisites:** task-26, task-50, task-52.  
**Design:** Sections 5.3, 17.6, 17.16.5.

**Implement:** Reviewed floor protocol, durable publication/state retrieval and recovery honoring highest applicable activated floor.

`coord_checkpoint::floor` is the durable side of task-52's rules. Three records, in order: `CheckpointReadinessV1`, one per voter, written only through `record_readiness` so the promise rules are applied against the row already there; `ActivatedFloorV1`, the certificate, naming its signers; and the `TrimmedFloorV1` it yields, published before the first deletion exactly as task-51 publishes it. Everything after the floor exists is shared with task-51 -- the same fence, the same bounded trimming, the same deletions -- and task-51's all-voter path is unchanged.

**Acceptance:** Permanently absent voter no longer prevents bounded semantic history. Restarted lagging nodes cannot vote from discarded baseline. Crash every transition against model; later task-j05 tests composed journal too. No dependence on observer acknowledgements.

Against the same store task-51's own tests use: two promises of three certify the floor the missing voter blocked, and trimming proceeds; possession without a promise certifies nothing and a minority certifies nothing; a promise moves up, never down, and never holds two checkpoints at one boundary; an observer or a foreign checkpoint supplies no signature; a recovery reads a majority, honours the highest floor it finds and refuses a narrower read; the published certificate never moves backwards; and a crash between the certificate and the floor deletes nothing, with the next attempt recomputing the same certificate.

**Review boundary:** No dependency compression or membership change bundled here.

<a id="task-54"></a>
### task-54: Model sealed membership handoff

**Prerequisites:** task-19, task-26, task-53.  
**Design:** Sections 4.8, 10.3, 23 G5.

**Implement:** Stop/transfer state machine, old-epoch fence, potentially chosen closure, unique successor and new-quorum activation. Model concurrent and interrupted operations including delayed vote/effect obligations.

Model pre-seal cancellation, partial durable sealing, indeterminate writes and outstanding/stale sealing attempts explicitly; the recovery branch must be justified by authoritative evidence, not the coordinator label or a missing local record.

`coord_consensus::handoff` is the vocabulary and the predicates. An old voter records one stance per transition and never reverses it, and a transition it cancelled stays cancelled after another replaces it, so a seal and a cancellation cannot both certify and no retry clears a fence. A terminal certificate is selected only after the seal, from a majority of the old voters agreeing on one root, and each sealed voter reports one root once, so two coordinators cannot select two roots; before the fence an old voter can still accept work, so what it calls terminal is not. The successor is bound through the root, not checked by the module: the caller derives it from the reported terminal state, whose root covers it (task-56). The successor activates only once a majority of it has durably installed that exact root. `resume` takes durable records and nothing else -- `Evidence` has no lifecycle label and deliberately no place to put one -- and has no path from a fence back to `Stable`; a fence another transition left is `FencedByAnother`, because the domain permits one transition and a fence belongs to the configuration.

**Acceptance:** Explicit intersection/fencing obligations reject two successors, old-generation vote resurrection and minority recreation. An applied KV view or admission shutdown alone never defines terminal state.

Every assignment of a stance script to each of three old voters (including a cancellation replaced by another and followed by a late seal), crossed with how far the coordinator got before it died and with a replacement coordinator asking for a second terminal root: no fence is ever cleared, no voter ever records both stances for one transition, no voter reports two roots and no majority of everything reported certifies a second one, a terminal certificate always follows a seal, an activation always implies a majority of the successor installed the certificate's exact root, and accumulating evidence never moves the stage backwards. Seven rules removed one at a time, each with the counterexample it exists for, frozen under `fixtures/counterexamples`.

Cancellation and seal completion cannot both authorize conflicting continuations. Partial/unknown seal state never clears persistent fences or enters terminal recovery without an authorized old-quorum seal.

**Review boundary:** No borrowed Raft joint-consensus assumptions without a SwiftPaxos mapping. task-m03 is orchestration integration, not a new ad hoc algorithm.

<a id="task-55"></a>
### task-55: Persist old-configuration sealing and terminal recovery

**Prerequisites:** task-25, task-26, task-54.  
**Design:** Sections 4.8, 10.3.

**Implement:** Durable old-quorum seal disables ordinary voting across old ballots while permitting terminal recovery of every potentially chosen command/closure, including pending effects at authoritative cut.

`rows::SealRecordV1` is the row, at key `epoch || 0x04` in `protocol_v1`, written once and never rewritten or removed; trimming retains it like a promise. `BallotState::seal` writes it and publishes the seal report through the logical outbox requiring the row *and* every batch submitted before the cut -- the Section 4.8 rule applied to a fence, so a report is never built over state the replica has not finished making durable. `BallotState::recover_sealed` reads it back and `on_new_leader` then refuses every ballot of the configuration; `RecoveredProtocol.seal` carries it from the store, and the authoritative cut sees it while the projection still lags. Nothing clears a seal: not a timeout, not a missing local row, not a retry, and no method exists that would.

**Acceptance:** Restart/reconnect cannot resume old service. Work learned immediately before sealing survives even with delayed response. Competing initiators cannot bypass recovery or authorize divergent terminal histories.

The report requires the row and both batches submitted before the cut, and releases only when all three are durable. A restart recovers sealed from the row alone and refuses ballot 1, ballot 2 and `u64::MAX`; a replica that reads no row has no seal, which is not evidence of a cancellation. A second initiator is refused both while the row is in flight and after it is durable, while retrying the same transition is the same seal. A failed seal row leaves the replica unsealed, says so, and admits being asked again.

Recover coordinator loss before, during and after seal durability. Reconcile partial/indeterminate seals; do not infer a safe return to old ordinary service from a timeout or missing local seal record.

**Review boundary:** No successor activation, controller override or mere frontend-close fence.

<a id="task-56"></a>
### task-56: Establish the unique handoff certificate

**Prerequisites:** task-49, task-55.  
**Design:** Sections 4.8–4.9, 10.3, 17.6.

**Implement:** Durable selected certificate binds terminal state/root, history/retry/lease/policy/floors, successor incarnations and source-defined closure evidence. Selection stable across restart.

`coord_checkpoint::handoff::TerminalStateV1` is what a sealed old voter reports, and every field is something the configuration already decided -- there is nowhere in it for a coordinator to put a choice. `terminal_root` binds the closed boundary, the shared checkpoint root of the terminal common state (history, retries, results, leases, sessions and policy are inside it because they are inside the common collections), a digest of the source-defined selection over the reports at the seal cut, the activated floor lineage and the exact successor incarnations. `select_certificate` delegates the quorum rule to task-54's `select_terminal`, so the seal is required by the signature and there is no selection before the fence; `publish_certificate` accepts the identical certificate and refuses any other.

**Acceptance:** Racing successor sets cannot both obtain authority. Partial/mixed-root evidence rejected. Initiator loss leaves resumable durable state; latent old completion remains represented.

Thirteen separate changes to the terminal state each change its root, the successor incarnations included, so a racing successor set is a different root rather than something a check has to catch; mixed roots are refused and never merged or picked between; a minority certifies nothing; a published certificate republishes and refuses to become another, which is what lets a replacement coordinator reuse the decision rather than recompute one; and dropping a command the selection resolved changes the terminal state, so a completion latent at the fence stays represented.

**Review boundary:** No force override or source-less config assignment; full KV snapshot alone insufficient.

<a id="task-57"></a>
### task-57: Activate the successor and recover interrupted handoffs

**Prerequisites:** task-42, task-50, task-53, task-56.  
**Design:** Sections 10.3, 23 G5.

**Implement:** New quorum installs identical terminal state before durable activation; recover every phase, preserving logical identities and late-operation outcomes.

`coord_checkpoint::handoff::record_install` is one successor replica's durable record that it holds the terminal state, and it is written only from an install receipt whose verified root and boundary are the certificate's, for an installer whose replica and incarnation are the certificate's entry (`record_local_install` reads both from the installing store) -- so "installs *identical* terminal state" is evidence of having the state rather than of having been told to say so. `activate_successor` takes the successor set from the certificate rather than from a caller, so an activation cannot be computed against a different successor than the old quorum certified, and `publish_handoff_activation` checks even a first activation against the certificate's transition, root and successor majority, accepts the identical activation and refuses any other, which makes a duplicate activation a no-op rather than a second grant of authority. `LocalEvidence::read` is what one store can answer about a handoff; a coordinator combines it with the stances it gathered and asks task-54's `resume` where to carry on.

**Acceptance:** Crash points, one absent old voter, delayed old traffic, duplicate activation and new-node restart preserve authority. Partial new and sealed old configurations cannot acknowledge new unauthorized work. task-m03 later couples observers/client notifications without changing protocol.

The whole handoff is driven over a store with the coordinator dying after every durable step, and the stage comes out of the rows each time rather than from anything the loop remembers: sealed only resumes at terminal recovery, a published certificate at installing, one installation of three still at installing, two at activating, and the published activation at served. A replacement recomputes the selection from the same reports and gets the same certificate. A replica outside the successor writes no installation record and a receipt for other bytes writes none either; a minority activates nothing; an installation of another root is refused rather than counted; and one old voter absent for good stops none of it.

Resume the evidence-backed stage and reuse established terminal/activation decisions. Certified pre-seal cancellation is distinct from irreversible sealing; retries never clear a partial fence or recompute an incompatible successor.

**Review boundary:** Destruction of old quorum authority remains DR; no casual rollback after seal.

<a id="task-58"></a>
### task-58: Implement node/key credential lifecycle and fencing tests

**Prerequisites:** task-41, task-42, task-57.  
**Design:** Sections 10.4, 20.4.

**Implement:** Renewal policy and credential classification against committed membership, bounded key overlap, warm expiry/revocation and the durable adoption of an authorized replacement on the node; replace-node/inspect workflows. Include observer/collector role lifecycles without voting entitlement. The in-process renewal driver is task-d02; committed replacement of a voter by a new incarnation is task-d41, over task-d40's install of the activated membership into the running daemon, and the in-place key rotation's overlap is task-m03's; v1.5 moved all of them out of this task, where they were listed before.

`coord_node_issuer::lifecycle` holds the arithmetic -- `RenewalPolicy::decide`, `due_at` with per-node jitter, `retire_at` for the rotation overlap and `session_deadline` for a warm session -- and deliberately has no outcome that means "serve on an expired leaf". `Membership::classify_credential` is the one rule that says what a presented credential is against committed membership (`Renewal`, `UncommittedKey`, `RequiresCommit`, `Stale`, `NotAVoter`); the peer binder binds exactly `Renewal` and tells a refused peer nothing else, and `coordd inspect` reports the distinction on the node itself, before placement, starting nothing. Warm connections end at the earlier of `Limits::max_connection_age` and the credential deadline the binder reports through the new `IdentityBinder::expires_at`. An authorized replacement keeps the node's durable state: `Generation::adopt` advances the store manifest forwards only, `StreamAllocator::adopt` and `JournaledStore::adopt_stream` carry the journal stream forward, and the append, read, replay and journal-open guards take the generation from the current mapping (as a bound, not an equality) instead of from the stream's first record. The stream is carried *before* the manifest advances, reading the generation to carry from with `Generation::adoption_pending`: the manifest is the only record of that generation, so the opposite order left an interrupted replacement with a moved manifest and an unmoved stream that the next start could not tell from a fresh node, and quarantined.

**Acceptance:** Issuer outage, expired warm streams, staged CA/key rotation and cloned stale disk fail safely. Renewal never silently changes membership. Preserve journal shard/checkpoint and epoch metadata across authorized replacement.

An outage is walked hour by hour from the due point to the deadline: the answer stays "renew" and the credential stays valid the whole way down, and past the deadline it is `Expired` however long the outage runs. A warm peer connection closes at its credential's end and at the age cap, and the same peers reconnect immediately afterwards -- expiry ends a connection, it does not fence a node. A staged CA rotation admits leaves under both roots while both are trusted and refuses the outgoing one once it is dropped. The end-to-end `coordd` test that replaces a running node's voting key (same command identifier and outcome afterwards; the retired credential refused, and `inspect` reporting `state=stale committed=2 presented=1`) is pending the committed reconfiguration path, because the genesis pin admits no edited manifest; the adoption it drives is held below the pin by a store test that stops between the stream carry and the manifest write and shows the next start finishes. Writing the new generation into the projection database was tried and reverted: a redb commit there makes the previous run's uncommitted work durable and pushes the materialized frontier past the journal's head, so the in-database identity record may lag the manifest and never lead it.

**Not in this task, and fails safe without it:** two runtime pieces were deliberately left out, and neither softens a deadline. The in-process renewal driver is task-d02's and is now in place: a node configured with a `[renewal]` section sleeps until `Wait`, enrolls at the issuer at `Due` with a request signed by its committed key, checks the renewed leaf against the one it replaces, writes it where a restart reads it, and presents it on every handshake that begins afterwards, leaving open connections to end at their own deadline; at `Expired` it stops serving. A node without that section stops at its leaf's `notAfter`: its transports refuse every handshake and end every connection there, the process exits 2 with `reason=credential-expired`, and it is put back by restarting it on a renewed leaf. And installing a new committed membership does not revisit connections already bound: `PeerBinder::install` swaps the membership and touches no connection, so a peer bound under a key the new membership replaces keeps its session until its own leaf's `notAfter` or the age cap, not until `retire_at`. `PeerBinder::install` has no production caller on this branch -- a replacement here is a manifest change and a restart, which ends every connection -- and runtime overlap enforcement (re-arm or disconnect a bound peer that no longer classifies as `Renewal`, capped by `retire_at`) is split between task-d40, which installs a committed membership into a running daemon and disconnects a bound peer that no longer classifies as `Renewal`, and task-m03, which caps a key rotation's overlap at `retire_at`.

**Committed key replacement waits for the committed reconfiguration path.** The genesis pin (task-43-compose) admits no manifest change, including a voter entry moved to a higher incarnation with a new key: Section 20.4 makes that a committed lifecycle transition, and a manifest-level key change under an unchanged epoch has no representation in the configuration chain. So a replacement cannot be driven through `coordd` on this branch; the credential classification, the fencing of a left-behind disk and the interrupted-adoption recovery stand without it, and the end-to-end replacement tests, which replace a node's key in place and keep its state, run under task-m03's rotation; task-d40 installs an activated configuration into the running daemon, and task-d41 replaces a voter by a new incarnation. **Genesis signature:** `coordd` reads the manifest as plain JSON and never calls `verify_genesis`, so `init` pins whatever file it is handed; task-42's "signed/pinned genesis" holds for the pin and not for the signature. With a strict pin this is a bootstrap-time gap. Closing it means loading the manifest through `verify_genesis` against the admin key at `init` and at every start, which task-43 owns; its follow-up at the top of the stack does it.

**Review boundary:** Generic identity token does not prove exclusive voter ownership.

<a id="task-59"></a>
### task-59: Implement backup, restore and disaster-recovery commands

**Prerequisites:** task-49, task-50, task-57, task-58.  
**Design:** Sections 5.4, 7.4, 17.16, 22.2.

**Implement:** Verify logical backups/manifests, restore-as-new-cluster and runbook for old-cluster isolation/external fencing. Preserve the distinct common/local recovery artifacts and selected journal lineage.

`coord_checkpoint::restore` holds the decision. A `BackupManifestV1` binds the `SharedCheckpointV1` root it names, so a backup index repointed at different bytes fails verification rather than at the restore; `plan_restore` refuses anything but a common snapshot, refuses a successor equal to the source, and requires a `FencingAttestationV1` naming that exact abandoned cluster, that exact successor and that exact backup with a non-empty operator reference. The plan states the recovery point and the disposition of every class of state, and `restore_shared` writes exactly what the plan says: KV, history, events, retries and floors at the boundary; configuration, policy, sessions, grants and leases not carried; lease attachments cleared from the keys they held. The runbook is `docs/operations/disaster-recovery.md`; `coordd backup`, `coordd verify` and `coordd restore --plan` are its commands.

**Acceptance:** Rehearsals obey explicit KV/session/lease restore policy, never reuse stale voting authority and require external fencing action. No observer or common snapshot is mistaken for an existing voter's full local checkpoint.

The `coordd` rehearsal runs the runbook end to end: a running node is backed up, the backup is verified off the disk, a tampered chunk fails verification, a second backup into the same directory is refused, a restore without an attestation and a restore in place are both refused, `--plan` writes nothing and prints every disposition, and the restore then brings up a *different* cluster on the same bytes. The library tests hold the rest: the restored store carries no `config_v1` and no `policy_v1` row, so membership and authorization can only come from the successor's own genesis, and the one key held by a lease comes back detached. Writing to the projection happens in the window between creating the generation and attaching it to the journal (`store::open_storage_with`), because that is the only point at which a whole store's worth of rows may be written directly.

**Review boundary:** No automatic minority force-new-cluster preserving identity or zero-loss promise beyond declared backup RPO.

<a id="task-60"></a>
### task-60: Implement format/capability upgrades and rollback guards

**Prerequisites:** task-03, task-07, task-50, task-57.  
**Design:** Sections 11.2, 13, 17.7, 17.10, 17.13, 17.16.

**Implement:** Negotiation/replicated feature activation, offline same-engine schema migration/fixtures; separate command/wire/journal/common-local checkpoint/adapter formats. Reject engine/profile mismatch and document rollback limits including observer/collector capability.

`coord_types::formats` is the frozen registry: nine independently versioned formats, each with a decoder window, and the constants elsewhere in the tree are defined *from* it rather than checked against it, so the registry cannot drift. `coord_consensus::feature` holds the activation rule -- unanimity of the configured voters, no withdrawal, no deactivation -- and `coord_checkpoint::feature` its two rows, a per-voter `FeatureSupportV1` derived from the registry and the `ActiveFeaturesV1` that names its reporters. `coord_storage_redb::migrate` is the offline same-engine migration: stage, rewrite every row, activate, with `CURRENT` written last and this node's `protocol_v1` obligations carried forward, which an install still refuses to touch.

**Acceptance:** Compatible mixed binaries coexist before activation; old binary refuses unsupported active state. Interrupted migration preserves valid selection, live voter savepoint rollback prohibited. Physical engine choice changes no common hash/protocol identity.

Activation is the one-way point: while nothing is active every build serves, a majority of reports activates nothing, and an activation naming a feature identifier a build does not know is refused rather than decoded to a smaller set -- the dangerous reading, because that build would conclude it may serve precisely when it may not. `coordd` prints its own formats and features on every start and refuses the store before the domain is attached. The migration test interrupts at each activation step and finds the previous generation still selected with its rows intact; the replaced generation is left on disk, because reclaiming it is `prune_unselected`, deliberately separate. A manifest above or below the window is refused before the engine behind it is opened, and there is no operation that lowers a schema version: rollback before activation is running the old binary, and after it is a restore (task-59).

**Review boundary:** No arbitrary downgrade, live-handle rewrite or cross-engine migration. Regenerate disposable experimental fixtures independently.

<a id="task-61"></a>
### task-61: Complete bounded observability and operator diagnostics

**Prerequisites:** task-31, task-43, task-53, task-57.  
**Design:** Sections 13, 22.3.

**Implement:** Stage-specific queue/stream/durability/learning/recovery/watch metrics, protected snapshots and redacted traces. Add journal/materialization separation, commit-return, view age, headroom and engine-pressure accounting with bounded labels. Include observer roles and shard budgets.

`coord_daemon::metrics` holds it, shaped by four rules. Every reading is a `Measure`, so an unavailable one says *why* -- `NotThisRole`, `NoSamples`, `Quarantined`, `NoBound`, `NotInstrumented` -- and never reports zero. Labels are bounded by construction: `Stage`, `Lane` and a `ShardIndex` capped at `MAX_REPORTED_SHARDS` are the only breakdowns, and there is no string map anywhere, so no key, command, session or principal can become one. `Durability` keeps sync, commit-return and backpressure as three fields. `Recorder` is atomics only, so a snapshot takes no lock. `coordd` records admission, the journal and materialization into one recorder for the run (the voter's node records the latter two), reports every other stage and every lane count as `NotInstrumented`, and prints the rendered snapshot on the startup report and again at shutdown.

**Acceptance:** Scripted tail spike attributed correctly; log/label scans reveal no secrets/key patterns. Diagnostics cannot block consensus or leak speculation. Unavailable metrics are not zero; sync and commit-return/backpressure are distinct.

The tail-spike test drives a hundred fast operations through five stages and one slow one through the journal: the journal's peak carries it, the other four stay under ten milliseconds, and the mean stays well below the peak so the tail is not buried. The scan runs over the rendered snapshot for credential shapes and for any alphanumeric run longer than twenty characters, which is what an identity, digest or key smuggled in as a label would look like; the `coordd` test runs the same scan over the real startup line. `Frontiers` reports `J`, `M` and `C` separately, with `unmaterialized` and `unreclaimed` derived rather than a single storage position. Nothing speculative is recorded: the stages are the ones design Section 22.3 names, and a speculative execution has no stage of its own to leak through.

**Review boundary:** Earlier tasks still instrument their own tests; operability is not postponed until here.

<a id="task-62"></a>
### task-62: Build and run the matched native WAN benchmark matrix

**Prerequisites:** task-29, task-32, task-43, task-53, task-61.  
**Design:** Sections 14.1, 14.3, 22.3.

**Implement:** Reproducible warm/cold, loss/asymmetric RTT, hot writers, transactions, native leases and watch/snapshot driver under named durability. Reuse task-s04 optional fresh fixtures, not deferred storage instrumentation.

**Acceptance:** Publish scheduled/achieved load/errors/path rates/percentiles/samples/CPU/WAN/disk/queues. Isolate codec/transport with equal persistence/quorums. New journal/observer matrices are extended by task-j07, task-o06/task-q01; label reference results accurately. Experimental engines use separate fresh homogeneous clusters/profile.

Include the Section 21.5 five-voter 2-2-1 two-voter-region-loss schedules. Distinguish a live-leader slow path from leader recovery and a subsequently restored fast quorum; report per-region latency, queue growth and time to restore redundancy.

The harness has landed: `crates/coord-wan-bench` offers a seeded workload to a provisioned domain over the real client path with arrivals scheduled against absolute instants, `scripts/bench/wan-topology.sh` imposes the delay, loss, asymmetry and region loss between regions, and `scripts/bench/wan-matrix.sh` runs every row against one standing domain and indexes what it produced, recording a row it could not run as not run with the reason rather than as a zero. A report separates the wait before an operation started from the operation itself and from the whole thing, so a harness that cannot keep up widens a reported distribution instead of shortening the window; every metric it did not read is absent with a reason and never a zero; and `--durability` is required, because a number without one is not a result. How to run a row and how to read it is `docs/operations/wan-benchmarks.md`; the results are `docs/operations/wan-results.md`.

Running it found five defects the certification suite could not, each of which became findable only once the one before it was fixed: a follower that forgot a command the leader was about to name and wedged its domain for good; a leader that discarded every acknowledgement arriving before its own proposal and so lost the fast path under concurrency; a replica that stopped asking for a payload it was waiting on once its domain went idle; a voter's acknowledgement for a peer-learned command handed to the wrong collector, which lost about one operation in two hundred; and -- once that straggler was fixed and the matrix ran eight times faster -- a voter that filled its command table and never emptied it again, refusing every read its own frontend served for the rest of the run. All five are fixed, with a regression test and a verified negative control each, and all five are written up in `docs/tuplesky-impl-notes.md`.

A sixth was published with the matrix rather than hidden in it and has since been closed, and closing it took two fixes rather than one. The published finding was a replica that fell behind, recovered but not quickly, and lost about three operations in ten of the read-heavy rows to the caller's deadline while it did. The first half of the cause is the catch-up path: a replica repeated its bounded payload ask whenever the count of what it was missing moved, which -- because that count moves when a command arrives by identity as well as when a payload arrives -- is on nearly every turn under load, so the bulk lane the transfer was separated onto filled with answers to asks already superseded and the replica fell further behind for having asked. A replica now asks again when its last batch was answered in full and otherwise on the retry floor.

The second half was not in the catch-up path at all, and only became total once the asks were paced: the drive loop polls the peer plane and the caller's plane in a biased select, peer first, and on a busy domain the peer plane is ready on every poll -- so one voter served 4560 api events and then not one more while its peer arm took another 80000, and every caller bound to that frontend waited out its deadline against a node that was otherwise working. The bias is a budget now. With both, the re-run matrix answers every operation it offers except a handful that meet the ten-second deadline on a saturated domain, where the published run lost 113 to 126 of 400 on every read-heavy row. The impaired rows and the Section 21.5 five-voter 2-2-1 region-loss schedules need `NET_ADMIN` and iproute2, which the environment the published rows were run in does not have; they are recorded as not run, and the runner takes them unchanged on a host that does.

The rows still recorded as not run, and any reference result published again, wait on task-d45 through task-d49 and task-d51 through task-d54, so that the matrix measures the protocol rather than the sync chain and the history scans those tasks remove (the [throughput amendment](#gate-checklist-and-deferred-work)). This cannot be a prerequisite edge: task-c01 and task-c02 came out of this task's first runs, and the throughput tasks build on them (task-d46 depends on task-c02 through task-d06, and task-d49 on task-c01 through task-d07 and task-d03). A result taken before task-d49 merges is labelled with the commit it ran on and is not a reference result. Until task-d51 merges, a node with local checkpoints at their default (`limits.checkpoint_after_records = 4096`) stalls its domain thread for every export, for 1.1 to 2.5 s at a 365,000-record projection and longer as it grows, so a throughput row taken with that default is not reproducible: such a run sets `checkpoint_after_records = 0` and says so.

**Review boundary:** Optimizations are separate measured follow-ups. No nondurable headline or implicit default/migration change.

<a id="task-63"></a>
### task-63: Measure Kine end-to-end overhead and regression budgets

**Prerequisites:** task-48, task-61, task-62.  
**Design:** Sections 14.1, 14.3, 19.5, 22.3.

**Implement:** Equivalent realistic workload through API server/Kine/native paths, separating Go codec, compatibility edge, credentials, transport/consensus and event observation. Integrate later observer/full-collector results in task-q01.

**Acceptance:** Comparable raw traces show no needless sequential WAN lookup, SQL polling or per-operation federation. Budgets come from measurements. Distinguish cache/event delay from write acknowledgement.

The harness has landed. `adapters/kine/bench` offers one seeded workload three ways against one domain in one run -- `coord-wan-bench` over the Rust client, `kine-bench -arm backend` over the Go `coord://` backend, and `kine-bench -arm edge` over an etcd v3 client into `kine-coord` -- because a single arm can measure none of the stages beneath it. The backend arm instruments the Go codec, the native exchange, the credential exchanges per caller and the native commands per storage operation; the Go backend measures them only when an observer is attached, so an unobserved production path pays nothing for it, and `driver.Open` hands back the backend, the client and the credential provider together so the count of exchanges is read rather than assumed. Every field an arm cannot measure carries a reason instead of a zero. The runner is `scripts/bench/kine-overhead.sh`; how to read a difference between two arms, and what a subtraction between them is not, is `docs/operations/kine-overhead.md`.

Running it found four defects. The Go result decoder did not know `Outcome::ErrRejected`, so the planner's deterministic refusal -- a replicated budget exceeded, a sequence that is not this session's, an admission that does not authorize the operation -- reached an API server as `codes.Internal`, "undecodable result", which is the one answer that makes a client retry something that can only ever be refused again. And behind that refusal was the larger one: nothing in the serving path ever advanced a client's replicated retry floor, so every client instance served exactly one outstanding window -- 1024 invocations -- and was refused for the life of its session. The Kubernetes storage edge puts every operation an API server makes through one instance, so it died in about a minute. Both are fixed here, each with a regression test and a verified negative control, and both are written up in `docs/tuplesky-impl-notes.md`.

The other two came out of chasing the WAN matrix's open finding with this task's arms beside it. A voter's evidence for a command whose submitter it does not know yet waited on a depth bound rather than on the race it covers, so an ordinary consequence of backpressure -- a fan-out the peer's lane could not queue -- was reported as a bound that was wrong; it waits on a window now, with the bound kept as a ceiling that means something else. And a send the transport could not queue printed a line per frame, in a loop that runs as fast as the runtime turns: one run wrote a 157 MB log and filled the disk, which is its own denial of service and which destroyed the benchmark rows that were measuring the fix. Both are written up with the others.

**Review boundary:** No global fastest claim from one topology or microbenchmark.

<a id="task-64"></a>
### task-64: Run mixed-fault qualification and automatic minimization

**Prerequisites:** task-09, task-27, task-32, task-40, task-48, task-53, task-57, task-58, task-60, task-d01, task-d03, task-d05, task-d06, task-d07, task-d08, task-d09, task-d10, task-d11, task-d12, task-d14, task-d15, task-d18, task-d19, task-d20, task-d21, task-d22, task-d23, task-d24, task-d25, task-d26, task-d27, task-d28, task-d29, task-d30, task-d31, task-d32, task-d33, task-d34, task-d35, task-d36, task-d37, task-d38, task-d39, task-d40, task-d41, task-d42, task-d43, task-d44, task-d45, task-d46, task-d47, task-d48, task-d49, task-d51, task-d52, task-d53, task-d56.  
**Design:** Sections 12, 21, 23 G6.

**Implement:** Minimize combined storage/network/clock/issuer/queue/format/lease/watch/handoff faults. Retain actual redb reference suite and reusable oracles; composed journal and observer integration is explicitly exercised by later qualification. Leader loss and re-election under every fault class is in the matrix, which is why task-d01 is a prerequisite: before it a leader-region outage is an outage of the domain, and the matrix would measure the absence of an election rather than its safety.

**Acceptance:** Each failure yields replay/minimal regression; all stated invariants hold for declared matrix. Known faulty variants still detected. Scope actual engine/platform/uncontrolled scheduling rather than claiming simulator proof.

**Review boundary:** No retry-until-green, unexplained flaky quarantine or hidden protocol changes. No compulsory Fjall production qualification.

<a id="task-65"></a>
### task-65: Package and qualify supported deployment targets

**Prerequisites:** task-43, task-48, task-59, task-60, task-61, task-d02, task-d04.  
**Design:** Sections 16, 22.1–22.2.

**Implement:** Reproducible Linux x86_64/aarch64 server artifacts, supported CLI stores, locked containers/service units, firewall/secret examples and platform matrix; incorporate declared journal/observer roles/profile readiness in integrated release.

**Acceptance:** Actual filesystem crash/reopen, AWS-LC/TLS, UDP, credential store and install/upgrade smoke tests on claimed targets. Unprivileged runtime and local admin defaults. Production excludes experimental/model/test-crypto linkage. A packaged node renews its own leaf across a service restart and an issuer outage shorter than the renewal window (task-d02); no supported target relies on an operator restart to pick up a renewed leaf.

**Review boundary:** Cross-compilation is not qualification; no implied Windows support or copied local workspace tooling.

<a id="task-66"></a>
### task-66: Close security, supply-chain and production release gates

**Prerequisites:** task-58, task-59, task-60, task-63, task-64, task-65, task-d02, task-q01.  
**Design:** Sections 1.2, 15, 23.

**Implement:** Final threat-model/source-extension evidence index, SBOM/license/advisories, exact conformance scope/WAN results and operator drills with sign-offs. Include the strict shared-journal, observer and client-aware report from task-q01.

**Acceptance:** No unresolved safety-critical finding, unreviewed dependency exception or missing permanent-replacement/floor evidence. Production excludes simulator keys/bypasses. State exact supported profiles/limits; task-j06 replay mode remains off unless separately accepted/included. redb is the production state engine. The three runtime gaps the task PRs recorded have closing evidence, not a note: genesis verified against the admin key at `init` and at start (task-43), every adopted ballot surfaced to the store (task-d01), and a serving node renewing its own leaf (task-d02).

**Review boundary:** Evidence assembly, not omnibus last-minute implementation. No second-engine production approval, migration or unsupported capacity claim.

<a id="task-s01"></a>
### task-s01: Define the portable engine contract and logical collection registry

**Prerequisites:** task-02, task-04.  
**Design:** Sections 16.3, 17.8–17.10.

**Implement:** coord-store-api bounded ordered reads, pinned snapshot/unique writer, atomic commit_durable and noncommit/indeterminate outcomes. Freeze collection IDs/stamp fixtures and package boundaries. Distinguish future journal durability, atomic working-state and durable-checkpoint capabilities rather than overloading one weak commit.

**Acceptance:** No engine/runtime types or actor weak switch; ownership examples compile with non-Send worker-local transactions. Separate local sequence from public execution/revision/common hashes; define explicit journal mapping later.

**Review boundary:** No real engine, generic DB framework, planner rewrite, conversion or production composition.

<a id="task-s02"></a>
### task-s02: Implement the model engine and common storage conformance kit

**Prerequisites:** task-05, task-s01.  
**Design:** Sections 17.9, 17.12, 17.14, 21.1–21.2.

**Implement:** Deterministic model, pinned views/transactions and configurable completion/visibility/outcomes; black-box adapter tests and versioned logical setup/replay fixtures. Separate independent service oracle. Represent journal/materialization/checkpoint/establishment as different events.

**Acceptance:** Deliberate torn writes/mixed snapshots/reversed bounds/swallowed iterator errors/false durability fail. Unknown commit permits full presence/absence only, successful barriers retain promised batches. Check allocations/progress and reference semantics.

**Review boundary:** Contract harness is not actual redb/Fjall/journal crash qualification; no model linked into production.

<a id="task-s03"></a>
### task-s03: Add an experimental single-writer Fjall adapter

**Prerequisites:** task-08, task-s02.  
**Design:** Sections 16.1, 17.9, 17.11, 17.13.

**Implement:** Pinned SingleWriterTxDatabase, explicit SyncAll, cross-keyspace snapshots, grouped collection prefixes, bounded scans/error classification. Resolve features in isolated build and fresh create/same-engine reopen; wrong engine/missing expected data fails.

**Acceptance:** Same model/redb common suites and worker fixtures without semantic changes. Aggregate budget/prefix isolation/abort/read-your-writes/no early completion. Production remains redb-only. State comparison with journal holds journal/profile/topology constant; original strict single-store reference is labeled separately.

**Review boundary:** Experimental only; no conversion, cross-engine image, live switch/mixed rollout or assumed deterministic internal flush coverage. No speed claim without measurements.

<a id="task-s04"></a>
### task-s04: Replay fresh fixtures and compare local engine costs early

**Prerequisites:** task-09, task-14, task-17, task-18, task-s03.  
**Design:** Sections 17.12–17.14, 21, 22.3.

**Implement:** Existing fixtures feed store-bench/differential/compare tasks, unique run roots, prefill/churn/warmup/manifests/raw measurements. Cover protocol-shaped and multi-index changes, MVCC/lease/retry retention, pinned reads and same-engine reopen. Scheduled offered-load replay includes generator/queue, commit-entry/return and publication.

**Acceptance:** Controlled failure-free outputs/digests match; faulted histories individually satisfy oracle without requiring unacknowledged work to match. Record full source/lock/fixture/engine/features/profile/cache/maintenance/hardware/filesystem/batching/limits/seeds. Report repeated percentile samples/variation, rejections/errors/backlog, CPU/RAM/logical and available physical writes/debt/reopen. Reject silent workload/durability/budget changes; tuned/sensitivity runs explicitly labeled. Never overwrite run or production roots. Hold journal/profile fixed where included.

**Review boundary:** Early semantic/performance comparison, not production engine/WAN/Kubernetes qualification or migration. Later task-62, task-63 reuse fresh artifacts. Behavioral fixes get separate review.

<a id="task-j01"></a>
### task-j01: Define the journal, sequence and materialization contracts

**Prerequisites:** task-01, task-02, task-s01, task-s02.  
**Design:** Sections 5.2, 17.3.1–17.3.2, 17.16, 18.

**Implement:** coord-journal-api types for stream allocation/local sequence, immutable complete records, barriers, definite/indeterminate failure, applied frontier and checkpoint pointer. Common model distinguishes journal durability/materialization/protocol establishment; logical codecs stay shared.

**Acceptance:** LocalJournalSeq cannot be used as KV/consensus/fencing position. Cross-domain IDs do not collide/recycle. Record rejects mismatched domain/incarnation/index/digest or bounds. Model atomic initialization/index visibility and source dependency-phase guards; no half-state seen by competing proposal.

**Review boundary:** No real engine or weakened commit_durable. Keep native wire and durable record formats separate.

<a id="task-j02"></a>
### task-j02: Implement the pinned raft-engine journal and postcard codec

**Prerequisites:** task-j01.  
**Design:** Sections 16.4, 17.3.1–17.3.3, 17.15.

**Implement:** Map streams/entries to full pinned codec-capable engine; bounded postcard ValueCodec, durable mapping metadata, nonempty LogBatch writes and exact per-stream completion. Keep indexed KV small and payloads in entries; audit sync/error/maintenance behavior and features.

**Acceptance:** Round-trip/adversarial codec tests, sync-before-success, stream order/isolation, actual multi-group batch and recorded byte-count-to-barrier mapping. Old published-crate APIs are not assumed. Nonempty sync panic has supervised fail-stop semantics, no worker-only recovery with uncertain shared engine.

**Review boundary:** No raft-rs/Ready/term-log logic, custom physical WAL, observer replication from private journal, or engine purge authorizing logical history deletion.

<a id="task-j03"></a>
### task-j03: Integrate journal-first shared storage and atomic materialization

**Prerequisites:** task-j02, task-08, task-11.  
**Design:** Sections 4.7–4.8, 17.3.2–17.3.4, 17.4, 17.10, 18.

**Implement:** Validate/sequence immutable common transitions, journal first, then ordered atomic state application. Separate JournalDurable/Materialized/Established. Initially one pending authoritative batch per stream with multi-domain grouping, exact guards/digests and incarnation/boot/epoch/ballot effect context. Strict profile retains durable redb projection.

**Acceptance:** Replay restores exact state/results/events without ambient inputs. Atomic initialization and conflict lookup hold under yielding. Projection visibility cannot bypass authority; recovery held behind journal still summarizes all voting obligations. Late old-ballot callback may update bookkeeping but never newly authorize a vote. Ambiguous write is reconciled, not blind retried. No global packet drain or cross-domain election barrier.

**Review boundary:** Do not silently enable unsynchronized state or claim one fsync overall. No duplicate application logic in physical adapters.

<a id="task-j04"></a>
### task-j04: Publish local recovery checkpoints and reclaim journal prefixes

**Prerequisites:** task-j03, task-09.  
**Design:** Sections 17.16.1–17.16.6.

**Implement:** Export complete local obligations/state at represented sequence into inactive same-engine checkpoint; sync files/directories, journal publication reference, then later durable covered-prefix compaction. Recover selected checkpoint plus contiguous suffix; keep SharedCheckpointV1 distinct.

`LocalRecoveryCheckpointV1` carries every collection of the registry, node-private ones included, because it is what lets a replica reclaim journal prefix without forgetting an obligation. `SharedCheckpointV1` deliberately carries none of them. The two are separate artifacts under separate hash domains and neither can be mistaken for the other.

The five steps live in three crates -- the snapshot and the image in storage and the filesystem, the pointer and the retirement in the journal -- and `LocalBaseline` is where a running node drives them as one. *When* is a local setting (`limits.checkpoint_after_records`, the tolerated `J - C`): no replicated result depends on when a node images its own storage, so a node that published on a rule of its own would still be correct. The baseline is read from the journal and validated against the image before the projection is attached, because what it answers is which projection to attach.

**Acceptance:** Crash at create/sync/rename/pointer/trim/purge/old-delete steps; valid selected source plus suffix or explicit quarantine every time. Missing selected image/gap/corruption never becomes fresh initialization. Unresolved old vote persists in checkpoint even after its redo reclaimed.

On the serving path: a running daemon publishes its own baseline, retires the prefix it represents, reclaims the images it supersedes, reports the baseline it recovers on, and answers the same invocation the same way afterwards -- from an image plus a journal whose prefix is gone.

**Review boundary:** Physical checkpoint does not permit semantic forgetting, quorum-loss recovery or migration. No mutable live-file copy as consistent snapshot.

<a id="task-j05"></a>
### task-j05: Qualify the real journal and composed persistence boundary

**Prerequisites:** task-j02, task-j03, task-j04, task-j08, task-09.  
**Design:** Sections 4.8, 17.15, 17.16.6, 21.4, 21.6.

**Implement:** Pinned filesystem injection with audited unhooked/background operations; combine actual raft-engine/redb faults, subprocess death, ENOSPC/sync errors and recovery modes. Keep independent protocol/application/retry oracles.

**Acceptance:** Acknowledged outcomes survive declared failures; sync panic stops affected shared service. Distinguish model from physical evidence and report uncontrolled schedules/platforms. Inject WAL/projection-cut/order errors and same-boot callbacks; suite detects missing obligations and unauthorized late effects. No permissive recovery silently loses durable prefix.

**Review boundary:** Clean close or process kill alone does not prove power-loss behavior.

<a id="task-j06"></a>
### task-j06: Replay-backed working-state materialization, promoted, off by default

**Prerequisites:** task-j04. task-j05 before the profile may be a default or part of a supported production profile.  
**Design:** Sections 17.3.4, 17.16.

**Trigger for the promotion.** This row was optional. task-d48's row routed a non-durable projection here, to be promoted if task-d45's numbers showed the remaining projection sync still bounded throughput. #143's runs on the Jepsen runner did: after task-d54 the leader's loop was 48% busy at the same throughput, and the disk rows' p50 sat about 26 ms above tmpfs at six nodes. Every write's chain holds three syncs, the leader's journal, the follower's journal and the projection's; this profile takes the third off it. The review on #98 (5962787328) chose this path over doing task-j05 first: the profile is built and tested with the crash evidence that exists today, measured with the profile on and labelled so, and the default stays strict until task-j05 lands.

**Implement:**
- A separate engine capability, `WriteTxn::commit_working` (redb `Durability::None`), with `LocalEngine::WORKING_STATE` and `LocalEngine::sync_working`. `commit_durable` and `commit_under_journal` keep their contracts, and an engine without the capability is refused the profile rather than given a weaker success.
- A named profile, `journal.profile = "journaled-replay-v1"`; strict stays the default, and there is no switch that weakens durability under another name.
- A durable-commit cadence bounded by working commits, records applied, and time, plus a forced durable commit before every checkpoint publication (so `C <= M_durable <= J` holds when a pointer retires a prefix) and at a clean stop. Journal reclaim happens only through that publication, so it is keyed on the projection's durable stamp. redb frees pages and releases its write cache only at a durable commit, so the cadence also bounds file growth and memory.
- At start, the projection's applied stamp is validated against the journal before anything is attached: at or past the selected baseline, and naming the record the journal holds there (the next record's predecessor digest, or the record's own digest at the head). A projection that validates is replayed forward from its stamp. One that does not is discarded and the selected baseline's image installed in a new generation, after which `(C, J]` is replayed (17.16.4). Without a baseline there is nothing to install and the node refuses to serve. The live database is never the only source.
- `Materialized` means applied and visible. The projection's durable frontier is separate (`projection_durable`, on the metrics line), and checkpoint publication and reclaim use it. Replay runs before the node answers, so a resolve answered from the projection still sees every journaled command.

**Acceptance:**
- The redb faultkit crash matrix in both profiles: a crash at every write and sync of the projection, with nothing, everything or a seeded subset of what was unsynced surviving, recovers the journal's state.
- A unit test that crashes after N unsynced commits and replays them; the cadence, a requested durable commit, a checkpoint publication and a stop each make the working commits durable; an invalid projection is refused at attach and reinstalled at start.
- SIGKILL under the Jepsen kill nemesis with the profile on: every journaled command answered after the restart.
- Boot replay time bounded by the cadence.
- The 6-node disk rows run with the profile on, labelled as such, after #143's fence fix so the faults scenario runs on the same carry.
- Before it may be a default: the entire composed fault matrix of task-j05 succeeds when the unsynced live projection is discarded or invalid, and the profile is in task-q01's applicable matrix.

**Review boundary:** No generic unsafe operator switch, dual authority, old-directory fallback, durability downgrade or headline omitting maintenance. No default other than strict until task-j05's evidence.

<a id="task-j07"></a>
### task-j07: Validate multi-group batching and resource isolation

**Prerequisites:** task-j03, task-j05, task-31.  
**Design:** Sections 1, 17.3.3–17.3.4, 17.15, 11, 21.

**Implement:** Many sparse plus hot groups on bounded shard set; scheduling/sync/queue/materialization/checkpoint/index/rewrite/rejection metrics. Node-wide budgets prevent per-idle-domain full caches/threads. Evaluate explicit versus internal grouping without extra idle timers.

**Acceptance:** Reproducible low-load/saturation results retain within-domain ordering, bounded memory and no artificial idle wait. Shard failure blast radius explicit. Compare equivalent single-store reference, record whether writer pool helps or adds queueing.

**Review boundary:** No universal throughput/latency assertion or consensus choice justified by one microbenchmark.

<a id="task-j08"></a>
### task-j08: Compose journal-backed application and serving storage

**Prerequisites:** task-j03, task-43.  
**Design:** Sections 4.7–4.8, 17.3.2–17.3.4, 22.1.

**Implement:** Split the application path into shared preparation and completion around one persistence seam, so the planner, admission, retry resolution and `plan_to_batch` produce the exact immutable batch both drivers record. Route the serving profile's application *and* protocol transitions through the one shared `JournaledStore`, by domain-scoped handles that preserve cross-domain batching; keep `StoreWorker` as the explicitly named reference driver over the same application logic. Recover and attach before admitting work, and initialize watch state from the recovered frontier.

**Implement (two principals, two planes):** A process that serves clients and votes is two principals. It presents its node credential when it votes and a separate collector credential when it submits on a client's behalf, because a node certificate binds exactly one role and the role that may act for other principals is the collector's, never the voter's. Half a collector credential, or none where the process must submit to a voter elsewhere, is refused at startup. The submission itself is an enumerated raw kind on an API-class request stream, admitted from exactly a role that may submit for clients -- checked at the transport against the bound certificate, and again at the collector boundary where the receipt is minted, from one rule rather than two. Each listener offers only its own plane's ALPN, so a node's published addresses are an unlabelled list a dialler tries in order; a machine addresses a peer with an open generation and the runtime resolves it against the committed configuration on the way out, refusing a generation the configuration has replaced.

**Implement (local delivery):** A frontend co-located with one of its domain's voters delivers submissions through that voter's own bounded ingress rather than over a QUIC connection to itself. The local destination is a runtime-owned capability bound to the actual voter instance and its committed identity, never a route a request acquires by naming a replica; a frontend-only or observer-only process has none. Delivery is bounded and scheduled -- a mailbox the voter drains on its own turn -- never an inline call that drives the voter to completion inside a request handler, and every destination is enqueued independently so local backpressure cannot prevent otherwise-admissible remote submissions from being dispatched. Local is the production default; a forced-wire variant is test-only and not an operator-facing setting. Both routes converge before admission or protocol processing diverges: the local route may skip serialization, QUIC and transport authentication, never their semantic guarantees (authenticated submitting role, correct domain, current membership and incarnation, canonical request validation, admission limits, protocol checks). Dispatch reports `queued_local`, `queued_remote` and rejections by reason, keeping membership mismatch separate from queue saturation and from an unavailable destination; no counter implies a vote, durability or a successful application. Local is not free in resource accounting: frontend work is charged to frontend limits and voter work to voter limits, both bounded in message count and bytes, and scheduling prevents frontend load starving voter control and recovery work. Cancellation releases frontend response resources without pretending an admitted consensus operation was undone. Readiness distinguishes listener availability, process liveness and role readiness; "frontend initialized" means a usable verifier configuration was validated, not that a key file parsed.

**Acceptance:** The same failure-free workload through both drivers yields identical results, revisions, retry records and events. A journaled record whose projection is held is not an applied outcome and publishes nothing; another batch's or domain's completion never completes it; completion requires the matching barrier's own `Materialized` with its expected application metadata. A definite or indeterminate projection failure after journal success is reconciled or materialized, never a false definite failure or a replacement plan, and a persistently refused materialization yields to reconciliation rather than spinning. Protocol durability rests on `JournalDurable` and does not wait for materialization. Ballot recovery uses the authoritative recovery cut rather than a projection snapshot, and the recovery test fails when recovery is deliberately changed to read only the lagging projection.

Three separate processes agreeing is the gate, not one process holding a quorum: a request the caller sends to one of three voters is established only if that node's frontend submitted to the other two as this domain's collector, over the API plane and under its collector credential, and only if those voters' evidence returned to the collector that submitted rather than to the one sharing each of their processes. A submission stream opened by a role that may not act for other principals is refused at the transport; a listener refuses the other plane's ALPN; and a protocol frame addressed to a generation the configuration has replaced is not sent.

Closing tests, all required: a real three-voter cluster serves a request end to end; a co-located submission and the same submission over the wire produce the same effects, and duplicate delivery yields **one counted voter contribution** -- not necessarily one emitted evidence frame, since a retry may legitimately retransmit -- with no double-counted replica, no command applied twice and no second revision; four stages stay distinct (queue admission accepts responsibility and is not a vote; protocol evidence is released through the outbox; an application outcome requires its matching `Materialized`; collector completion is the quorum's); local evidence enters the same collector validation and voter-identity deduplication path as remote evidence, with no `self_vote` flag, pre-counted acknowledgement or local-success shortcut, so one co-located voter is not a quorum of three; a restart serves again and a retry resolves; frontend and voter budgets are enforced separately; and a process whose verifier configuration is unusable refuses to start rather than binding listeners and rejecting every caller. A successful `Bind` alone is not the served-request gate. A clean restart is integration evidence and is not task-j05's filesystem and power-loss qualification.

**Not in this task, and latent until leader election is wired:** the store stamps every transition it records with the ballot its voter holds. `Voter::new` hands it the genesis ballot and `Voter::set_ballot` moves both together, but `set_ballot` has no production caller. The points where a voter adopts a higher ballot are inside `coord-consensus` (a follower's `NewLeader`, and `PromiseOutcome::Promised` when a promise row turns durable), and neither reaches the `Voter`, so a promise for a higher ballot would still be recorded under the genesis ballot. Nothing in this build sends `NewLeader`, and `JournaledStore::fence` has no production caller, so this cannot happen yet. The work that wires leader election into `coordd` has to surface every adopted ballot (promise and sync adoption) to `Voter::set_ballot`. task-d01 owns that wiring, and does it: a campaign, a candidate's `NewLeader` and any promise the machine makes by another path reach `Voter::set_ballot`, and the store is fenced at the promised ballot.

**Review boundary:** No second writer beside or beneath `JournaledStore` on the serving profile, and no duplicate application logic in a physical adapter. Journal durability alone is never an applied application outcome, and storage never manufactures establishment. Local delivery is a transport optimization and never a consensus shortcut: no direct `StoreWorker` path for local submissions, no local route obtained from a request's contents, and no acknowledgement a voter did not produce. No second definition of which role may submit on a client's behalf, no voter credential used to submit, and no evidence returned to a collector other than the one that submitted the command. Keeps `journaled-strict-v1`: no one-fsync claim and no enablement of optional task-j06. This is integration evidence and does not substitute for task-j05's filesystem and power-loss qualification.

<a id="task-j09"></a>
### task-j09: Establish a caller's session as a replicated command

**Prerequisites:** task-18, task-37, task-j08.  
**Design:** Sections 9.3, 12, 22.1.

**Implement:** Drive the session row a command's execution authorizes against from the bound caller, as an ordinary replicated command rather than a fixture: a verified binding proposes the session establishment its claims describe, execution writes the session and its scope under the committed policy, and the row is durable before any command of that session can be authorized by it. Keep the trusted-boundary rule: the claims are verified outside replicated execution, the receipt is minted there, and nothing about the session is asserted by a command's payload.

The authentication boundary may attest the principal, the trust rule and its generation, the scope ceiling and the credential's validity bound, provided each is derived from the verified credential and configured trust. `CanonicalOperation::ConsumeAdmission` therefore carries nothing: it names one action, and the session's claims come from the admission travelling beside the payload. That admission is part of what the command durably is -- it is recorded with the payload and its digest is carried by every acknowledgement -- so two voters cannot accept one command as different session-creation facts. The command's identity stays the retry key and the canonical request, so a retry under a rotated credential is the same command.

A domain has to trust something before it can trust anyone: a session exists only under an enabled trust rule, and permission is allow-only. `coordd init` writes the domain's genesis policy -- the trust rule its configured issuer signs under and the permissions its genesis grants -- from configuration, identically on every replica and without taking an execution position.

**Acceptance:** A bound caller's first command executes against a session row this path wrote, with a revision, rather than being refused `SessionInvalid`; retry resolution of that command returns the retained result rather than re-executing it. A caller whose binding was refused establishes nothing. Two bindings of the same session converge on one row. The establishment is replicated: a replica that did not see the binding still authorizes the session's commands after learning the command that wrote it.

Four further groups, each against the real applying path rather than a direct store write. *Authority:* the payload cannot select another principal, rule, ceiling or deadline; an establishing admission authorizes exactly one action and that action exists for nothing else; a receipt of another cluster or domain, or one presented on an ingress without session-issuing authority, admits nothing. *Binding and replay:* a fresh receipt presented for an already accepted command does not replace its facts; a duplicate delivery recovers the established outcome; another command cannot reuse a consumed receipt; a quorum cannot form across senders that accepted one identity as different facts. *Ordering and time:* a trust rule revoked or regenerated before the admission is consumed creates nothing; a retired session is never resurrected; an accepted command executes the same way long after its credential expired, with no issuer and no replica clock. *Serving path:* establishment through authenticated ingress, consensus, journaled application and the acknowledgement that follows it, including restart and retry.

**Review boundary:** No session row written outside replicated execution, no bootstrap fixture in a production path, and no authorization that reads a session the cluster has not agreed on.

<a id="task-j10"></a>
### task-j10: Give the Rust client a real unary request path

**Prerequisites:** task-34, task-43.  
**Design:** Sections 3.3, 5, 22.1.

**Implement:** A client-side unary request that keeps both halves of the stream it opens, so the answer the node writes back on it can be read. `Transport::send` and `ApiDelivery` keep their current meanings -- delivering *to* a node this side dialed -- and this is added beside them, not in place of them. Bound the response size and the deadline, and preserve ambiguous-outcome semantics on timeout or cancellation: a request whose answer did not arrive is pending and resolvable by identity, never failed.

**Acceptance:** A Rust caller sends a request and reads its answer through the SDK, against the same daemon the direct-Quinn test drives. A response above the bound is refused as a bound rather than truncated. A timeout or a cancellation reports the outcome as unknown and the invocation as resolvable, and re-resolving it returns the same result. Cancelling a request releases the caller's resources without asserting the command was undone.

A question is asked only on an API-class connection this side dialed: on an accepted one the streams this side opens are output, read as a delivery at the far end, so a request written there would be answered by nobody. The request is admitted under the destination and node budgets before a stream is opened, as a reply is, and it names no group -- a group is what the lane's fair queue shares capacity between, and a question owns its stream.

**Review boundary:** The Rust SDK's live request path is described by this task's own evidence, never qualified by the direct-Quinn caller in a daemon test.

<a id="task-o01"></a>
### task-o01: Specify finalized frames and observer capabilities

**Prerequisites:** task-02, task-03, task-28, task-49.  
**Design:** Sections 3, 6.7, 6.9.2.

**Implement:** FinalizedFrameV1 origin/epoch/execution/revision/digest, complete events/common state/authorization transitions and capability snapshots. Specify exporter eligibility/source continuity and Rust/Go vectors/reference observer.

**Acceptance:** Model excludes speculation, missing history/mixed restore identities and fake KV advancement for non-KV state. Relay cannot advertise MVCC/promotion eligibility. Bounded chunking exposes no partial revision.

**Review boundary:** No private voter-journal streaming or hash-as-Byzantine-proof claim.

<a id="task-o02"></a>
### task-o02: Build MVCC observer install, catch-up and serving lifecycle

**Prerequisites:** task-o01, task-j03, task-50.  
**Design:** Sections 6.7.2–6.7.4, 6.9.

**Implement:** Authorized inactive snapshot install, validated replay cursor, atomic common state/events/frontier, source resumption and reinstall on retention exhaustion using common materializer/profile. Scope identity/domain strictly.

**Acceptance:** Kill source/observer during install/replay and verify lineage/outcomes. Same KV revision with behind policy execution is not caught up. Wrong scope/voting rejected. Offline observer neither pins source obligations indefinitely nor gates mutations.

**Review boundary:** No leader eligibility, new voter authority or automatic promotion.

<a id="task-o03"></a>
### task-o03: Add regional relays, bounded fan-out and source failover

**Prerequisites:** task-o02, task-31.  
**Design:** Sections 3.3, 6.7.4.

**Implement:** Sparse loop-free sources, bounded fan-out/queues, subscription/snapshot admission, reconnect jitter and health. Resume same finalized prefix from another eligible source; no permanently required exporter.

**Acceptance:** Slow/disconnected region cannot consume all control/voter resources. Failover neither skips nor accepts conflicting prefix. Report total distribution versus voter-NIC cost. Retention loss returns explicit compaction/reinstall state.

**Review boundary:** No observer acknowledgement in write completion or cluster-wide observer mesh.

<a id="task-o04"></a>
### task-o04: Route Kine watches to observers with correct progress

**Prerequisites:** task-o02, task-48.  
**Design:** Sections 6.8, 6.4, 6.9.3.

**Implement:** Freeze exact Kine revision/fork, route eligible regional watches/fallback, historical/live attach, complete-revision resume, per-watch markers/cancellation and needed edge patch. Commit cross-language/bridge fixtures. Preserve strict per-selected-output authorization; ordered observer revocation replay supplements it.

**Acceptance:** Actual API server list/watch-gap, future/compacted start, events queued before progress, no-match filter, chunks/restart/source loss/compaction. No SQL polling or per-event external token exchange. Source head cannot outrun delivered frontier and permission barriers cannot be skipped.

**Review boundary:** No assumption old WaitForSyncTo or new EventBatch exists at wrong pin; healthy boot is not cache correctness proof.

<a id="task-o05"></a>
### task-o05: Add observer historical reads and authoritative read fences

**Prerequisites:** task-o04, task-18.  
**Design:** Sections 6.8.1, 6.9.

**Implement:** First authorized historical reads; then capability-gated ordered ReadFence with bound invocation/options/scope/execution/revision/permission and observer snapshot wait. Handle pin/compaction/source cancellation. Keep strict output authorization and current-read fallback.

**Acceptance:** Reject fence predating a later invocation, stale policy admission and newer data with old header. No indefinite wait after retention/source loss. Differential Get/List/Count/pages; feature off uses authoritative full read.

**Review boundary:** No unproved ReadIndex clone, stale current-read downgrade or token-only static permission cache.

<a id="task-o06"></a>
### task-o06: Qualify observer correctness and regional scaling

**Prerequisites:** task-o03, task-o04, task-o05, task-j05.  
**Design:** Sections 14.3, 21.4–21.6, 23.1.

**Implement:** Combined relay/Kine/policy/compaction/storage/network failures, event-only versus MVCC capability, and rising observer/subscriber load. Include acknowledged write then voter failure/source change.

**Acceptance:** Complete histories, bounded queues and resumable exact outcomes/revisions; event/read delay separate from mutation. Added unavailable observer changes no quorum/completion condition. Publish measured supported capacity, not physical infinity inferred from no protocol cap. Source switch preserves established result and history.

**Review boundary:** No quorum-fault-tolerance gain from observer count or hidden API-server freshness bypass.

<a id="task-m01"></a>
### task-m01: Define authoritative configuration discovery and epoch records

**Prerequisites:** task-02, task-19.  
**Design:** Sections 10.5.1–10.5.3.

**Implement:** GroupConfigurationV1/BallotConfigurationV1, authenticated hints, certificate chain, endpoint generations, paginated observer discovery and bootstrap/subscription messages. Freeze rollback/trust validation preserving source quorums/evidence.

**Acceptance:** Models/vectors reject fabricated larger epoch, wrong incarnation, arbitrary per-request fast majority and observer vote. Address/certificate refresh alone cannot change voters. Historical evidence remains verifiable without live issuer.

**Review boundary:** Directory is not transition authority and controller cannot bypass handoff proof.

<a id="task-m02"></a>
### task-m02: Make Kine a full epoch-aware trusted collector

**Prerequisites:** task-m01, task-33, task-48, task-c01.  
**Design:** Sections 3.2, 10.5.

**Implement:** Authorized Go client direct fan-out, exact completion, configuration refresh, stable retry, voter identity dedup and historical result handling. Shared language-neutral Rust/Go traces; optional local sidecar measured separately.

**Acceptance:** Identical decisions on lost/reordered/mixed path/ballot/epoch evidence. No normal serial directory lookup. Offline client never blocks activation. Partial client death repaired by voters; epoch retry does not duplicate mutation. Late valid old outcome is not blindly discarded.

**Review boundary:** No leader-only trust, loose majority or arbitrary public client as trusted collector.

<a id="task-m03"></a>
### task-m03: Connect observer staging to sealed handoff and activation

**Prerequisites:** task-m01, task-o02, task-57, task-j04, task-58, task-d40, task-d41.  
**Design:** Sections 4.8, 10.3, 17.16.

**Implement:** Integrate modeled seal/terminal/activation with non-voter readiness, shared journal/certificates, authoritative notifications and finalized-stream epoch links. Support replacement and 3→5/5→3. Installing an activated membership into a running daemon is task-d40's, and replacing one voter, without observers, is task-d41's; this task builds on both and adds observer staging, the in-place key rotation's overlap, finalized-stream epoch links, authoritative client notifications and resizing.

Carry a voter's in-place voting-key rotation (Section 20.4) as the same committed transition: a successor configuration that differs from its predecessor in exactly one voter's key, sealed, certified and activated through the same handoff, with no manifest edit and no restart as the mechanism, using task-d40's install of the activated membership into the running daemon. What this task adds is the rotation's overlap: a bound peer that no longer classifies as `Renewal` because its key rotated is re-armed or disconnected by `RenewalPolicy::retire_at`, not by its leaf's `notAfter`. Re-enable task-58's ignored end-to-end replacement tests here: they replace a node's key in place and keep its state, which is this rotation.

**Acceptance:** Staged replica cannot vote early; common snapshot/current KV not local protocol recovery. Old disk stays fenced; preserve requests/revisions/leases/policy/lineage and delayed voting obligations. Physical copies may exceed five while each active voter set respects cap. A running node's voting key is replaced with the same command identifier and outcome afterwards; the retired credential is refused, `inspect` reports it stale against the committed incarnation, and a peer still bound under the retired key is disconnected by `retire_at`, not by its leaf's `notAfter`. The replaced node keeps its journal stream, checkpoints and epoch metadata.

**Review boundary:** No ad hoc dual-majority algorithm, rollback after seal without authority or self-promoted observer majority-loss rescue.

<a id="task-m04"></a>
### task-m04: Implement conservative regional placement and quorum tuning

**Prerequisites:** task-m03, task-m02.  
**Design:** Sections 1.5, 10.3.1, 10.3.3.

**Implement:** Hard failure-domain constraints, current-set leader/fast-quorum scoring versus slower voter moves, dry-run explanations, operation identities, hysteresis/residence/rate limits and initial operator approval. Status not exclusively dependent on affected tenant.

**Acceptance:** Noise does not trigger storms; reject fast layout violating regional budget. Quorum changes use ballots, not local client rewrites. Interrupted work resumes idempotently. Repair needs neither removed node nor all observers.

**Review boundary:** No universal optimizer or automatic authority from latency measurements.

<a id="task-m05"></a>
### task-m05: Qualify client-aware membership under mixed failures

**Prerequisites:** task-m02, task-m03, task-m04, task-58, task-d01.  
**Design:** Sections 4.8, 10.3, 10.5, 21.5–21.6.

**Implement:** Competing operators, coordinator failure each handoff stage, stale/isolated Kine, delayed old completions/effects, stale disks, partial successor install, cert rotation and observer/checkpoint/GC faults. Include a committed key replacement (task-m03) interrupted at each handoff stage, including a coordinator failing after the seal and before activation: the replacement completes or the retired key stays refused, never both keys admitted, and the old disk stays fenced. Leader loss during a handoff needs the election task-d01 wires.

Include the evidence-conditioned handoff recovery branches and the explicit five-voter 2-2-1 whole-region-outage cases in Section 21.5, with fixed-fast-set loss, leader-region loss, a further survivor failure, and eventual authorized repair.

**Acceptance:** No two successors/mixed-epoch majority, lost completed outcome, revision rollback or authority resurrection. Dead client/observer never becomes required ACK. Terminal cut includes latent vote obligations. Report normal/degraded handoff interruption separately from ordinary message-delay bounds.

A surviving three-voter majority progresses only after required leader recovery, and only through a valid available path. Until redundancy is restored it has no further voter-failure margin; observers cannot substitute as voters. Report that interval and degraded latency.

**Review boundary:** No absolute availability when required authority is unavailable; DR is separately declared workflow.

<a id="task-c01"></a>
### task-c01: Give the collector a submission delivery lifecycle (contract revision 3)

**Prerequisites:** task-33, task-62.  
**Design:** Sections 3.2, 4.3, 4.4.

**Implement:** Revision 1 said fan-out reaches every voter at once and said nothing about a destination that could not take it, so the transport dropped that copy and nothing re-offered it -- 317 of about 880 submissions in one measured run, with the command committing on whatever subset was free and the caller told nothing. The rule is that all-voter targeting is required and all-voter acceptance is not: every voter is offered the submission independently and without blocking, and while the command is unresolved the collector that accepted it owns re-offering what could not be queued. Reserve the pending slot *and* the envelope's bytes before any destination is offered anything, so a refusal means nothing was sent by this attempt and no later answer from a destination can become a refusal of the command. Retain the original envelope rather than a recipe; classify a full queue and an absent route as delivery backpressure and a configuration disagreement or an oversized envelope as something repeating cannot settle; bound the rate with a floor, backoff and a per-turn budget fair across commands, never the obligation; retire at settlement, recording the voters that never took it; reconcile destinations on reconfiguration.

**Acceptance:** A destination's saturation costs that destination and nothing else, and is offered again without a caller retry. Collector capacity refuses only before dispatch. A caller's timeout or disconnection does not discard the obligation. A permanently unavailable minority never blocks a quorum result, and settlement clears the retry state. A repeat carries the original command identity and admission facts and cannot be counted as a second vote. Sustained saturation stays inside the byte, command and per-turn bounds. A voter's hold for an unplaced acknowledgement outlasts the repeat schedule's ceiling, because a duplicate submission produces no effects.

**Follow-up on this task, in its own PR:** `limits.max_request_bytes` was read for the undelivered budget above and for the writer-queue check, and enforced nowhere: a submission was bounded only by the API frame class limit, so lowering the setting changed the budget and not what a caller may send. It now bounds a request at admission: a request whose cost as the protocol counts it (`LogicalRequest::cost`, the bytes of its keys, values and range ends) exceeds the setting is refused as `AdmissionRefusal::RequestTooLarge`, before any slot is reserved, with the appended frozen code `REQUEST_TOO_LARGE` (0x0006), which the SDK reports as `RetryError::RequestTooLarge` and the Kine backend as `InvalidArgument`. At the default setting the bound is the protocol's own, so nothing that validates is refused for its size. The refusal is answered under the invocation's own command identity.

**Review boundary:** Delivery only. No change to the learning predicate, the leader release gate or what evidence a command is established on; queue acceptance is never promoted into delivery evidence, and reliable end-to-end delivery is not claimed.

<a id="task-c02"></a>
### task-c02: Repair lost frontend evidence, and complete a half-held command from the durable record (contract revision 2)

**Prerequisites:** task-23, task-33, task-62.  
**Design:** Sections 3.2, 4.3, 4.4, 5.1.

**Implement:** A voter that learns a command from a peer acknowledges it before any submission has told its frontend which collector asked; the frontend holds such evidence for a while and lets it go, and a submission arriving afterwards was refused as a duplicate with no effects, so the collector was one voter short for ever on a command the domain had executed -- and when that voter was the leader, unable to learn at all. Keep, in each machine, exactly what it published to the frontend for each command it still remembers (context, required barriers, bytes), bounded by the command table's live records and tombstones and never by a count of unrelated commands. On an exact duplicate -- the retry key's binding, the canonical request, the admission facts and the acknowledged floor all as accepted -- publish it again to the frontend only, through the same outbox, so the boot fence, the durable prerequisites and the promise are judged at release as they were the first time; release in the same step. Refuse, with a named reason that changes nothing about the command, when the replica is fenced, the retained ballot is not the configured one, the batch is not yet durable (the original is still queued), the batch failed, nothing was published this boot, the command is forgotten, or a per-boot cap is reached. Split `AlreadyInitialized` from `PayloadConflict`; a presentation under other facts is a conflict, never a replay. Nothing is recomputed, voted, persisted or executed again. In the collector, complete a command that holds half of what a release needs -- the predicate without the release, or the release without the predicate -- from the durable retry record of its execution on the collector's own node, the same committed state that answers a caller's retry before anything is submitted: the record must name the command, and must agree with a held release on digest and bytes. Never from nothing; never counted as a vote. Move the runtime's parked-evidence bookkeeping (`coord_daemon::parked`) and the record lookup (`coord_daemon::settle`) out of the serving loop, with the instant as an input, so the hold expiring and the depth crowding out are schedules a test produces rather than waits for.

**Acceptance:** A late exact duplicate repairs the identified failure at a follower and at the leader and lets the collector finish under the existing predicate and release gate, with no peer sends, no second execution and no vote counted twice. A duplicate before durability adds no copy and the original still goes; after a failed batch nothing is replayed; a fenced replica replays nothing; other facts under the same identity are a conflict. Repairs of one command are bounded per boot and the bound is per command. Retention survives table pressure for unresolved work and is gone after a restart. Forcing the replay cache past what the table remembers exercises the record path: the collector completes from its own node's record, and a record that disagrees with a held release settles nothing and is reported. All-voter targeting and majority availability are unchanged. An in-process three-voter schedule over real stores, the real voter door, the real parked bookkeeping and the real collector (`coord-daemon/tests/repair.rs`) lets the parked evidence expire, and separately lets the depth crowd it out with no time passing, delivers the duplicate afterwards and shows the collector learn and release from the republished evidence alone; a refused repair (the per-boot bound spent) is followed by the record path completing the same request, and a lost release is completed the same way from the counted votes. Negative control: reverting the duplicate path to no effects fails the core regression (the same schedule with the repair's output discarded leaves the caller waiting).

**Review boundary:** Delivery repair and record settlement only. No change to the learning predicate, the leader release gate or what evidence a command is established on. The runtime's hold and depth for unplaced evidence are performance controls, not the recoverability boundary. Peer retransmission and reliable end-to-end delivery are not claimed.

<a id="task-c03"></a>
### task-c03: Give the collector boundary its own monotonic clock

**Prerequisites:** task-33, task-37.  
**Design:** Sections 3.2, 9.3.

**Implement:** `ClockHealth.now` is Unix seconds, and it is what validates a token's `exp`, `nbf`, `iat`, age and uncertainty; the collector's request deadlines add `deadline_ms` to it, so a nominal 1,500 ms deadline is 1,500 seconds. Do not change `ClockHealth`: it is the authentication clock and its units are load-bearing there. Give the collector boundary a distinct monotonic millisecond reading -- a small named type, not another ambiguous `now_ticks` -- for request deadlines and repeat scheduling, injected into the deterministic logic rather than read inside it, so submission, retry attachment and expiration share one time base that wall-clock steps cannot move. Wiring `expire()` into the serving loop, with timeout responses and caller-stream cleanup, is a behaviour change of its own and is named as such, not an incidental part of the unit fix.

**Acceptance:** A 1,500 ms deadline is not expired immediately before it and is at it; subsecond deadlines work; a wall-clock jump with no monotonic progress expires nothing; the existing zero-deadline behaviour is preserved. A caller's timeout never cancels accepted work or releases its outstanding obligation: expiration reports and retains.

**Review boundary:** Clock inputs at one boundary. Authentication time semantics are untouched.

<a id="task-d01"></a>
### task-d01: Wire leader election and ballot adoption into coordd

**Prerequisites:** task-26, task-27, task-43, task-j08, task-d03.  
**Design:** Sections 4.2, 4.8–4.9, 18.1, 22.1.

**Implement:** `coord-consensus` has the campaign (task-26), the follower's `NewLeader` handling and the promise rows; `coordd` has none of the wiring, so the genesis leader is the only leader a domain ever has and losing it is losing the domain. Give the daemon the election it is missing as runtime wiring over the existing machines, with nothing new in the protocol: a configured, bounded campaign trigger (a follower's leader silence past a jittered patience, and an operator's explicit request, so a partitioned minority does not campaign for ever at full rate); the campaign replacing the machine in place, as `Machine` is built for; `NewLeader` and the campaign's promise and payload requests sent over the peer plane; and every adopted ballot -- a follower's `NewLeader`, `PromiseOutcome::Promised` when the row turns durable, and the campaign's own bound Sync -- surfaced to `Voter::set_ballot`, and through it to the store, so nothing is stamped with a ballot the voter no longer holds. Fence the obsolete ballot's queued transitions with `JournaledStore::fence` when the promise is made, refusing the fenced work with a named reason. Keep the epoch above the ballot: a campaign changes leadership within the committed configuration and never the voter set, the fast set or the quorum table.

**Acceptance:** With the leader stopped, the remaining majority of a real three-voter domain elects a leader and serves the next request; every command established under the old ballot keeps its result and revision, and the old leader, returning, follows. A promise for a higher ballot is recorded under that ballot in the journal, and the negative control (the adopted ballot not surfaced) fails the recovery test task-j08 named. A late vote from the old ballot updates bookkeeping and authorizes nothing (Section 4.8). Two candidates campaigning at once end with one leader and no divergence. An election never changes membership, never spans domains, and an obsolete ballot's queued transitions are refused, not held.

**Review boundary:** Wiring and triggers only; source selection, promise rules and the recovery cases stay task-26's and are not reopened here. No leader lease or clock-based leadership, no election on a minority, and no change to task-m03's handoff.

<a id="task-d02"></a>
### task-d02: Drive leaf renewal inside the serving daemon

**Prerequisites:** task-41, task-43, task-58.  
**Design:** Sections 10.4, 20.4, 22.1.

**Implement:** task-58 left the renewal arithmetic (`RenewalPolicy::decide`, `due_at`, `retire_at`) reachable only through `coordd inspect`; the serving daemon builds its endpoint identity once at startup and never renews, so a leaf reaching `notAfter` takes the node out until an operator restarts it on a renewed leaf. Run the policy in the serving loop: sleep until `Wait`, enroll at the issuer at `Due` under the node's committed key and incarnation (a same-key renewal, which Section 20.4 says is not membership), rebuild the endpoint identity from the renewed leaf without dropping connections bound under the old one, and let those end at their own credential deadline or the age cap as task-58's binder already does. Bound the retry: an issuer outage keeps the node on its current leaf, retrying with backoff until the deadline and `Expired` after it, and never extends a deadline or serves on an expired leaf. Report the renewal state in the startup report and diagnostics without secrets.

**Acceptance:** A node whose leaf is due renews and keeps serving, with no restart and no connection dropped for the renewal itself; peers admit the renewed leaf as `Renewal`. With the issuer down from the due point the node serves to the deadline and fails closed at it; the issuer returning before the deadline renews, and returning after it does not revive the node without a restart. The renewed leaf carries the same key and incarnation, and membership is unchanged before, during and after. Collector and observer roles renew the same way without acquiring voting entitlement.

**Review boundary:** Renewal only; a new key or incarnation is task-m03's committed replacement and is refused here as task-58 classifies it. No deadline is softened for availability, and the issuer stays independent of the quorum.

<a id="task-d03"></a>
### task-d03: Re-dial peers and collector links on a timer

**Prerequisites:** task-43, task-j08, task-c01.  
**Design:** Sections 3.3, 19.2, 22.1.

**Implement:** Both planes are dialled once, when the serving loop starts (`PeerPlane::dial_missing` and `CollectorLinks::dial_missing`), and a closed connection only refreshes the `peers connected=` report; the transport ends every connection at its age cap (twelve hours by default) or after thirty seconds idle. So a mesh heals today only by restarting nodes: a dropped peer link stays down for the survivors, a restarted voter is re-dialled by nobody, and half a day after start the whole domain goes quiet. Drive both `dial_missing`s from the loop's existing `Deadline` mechanism: after any `Closed`, and periodically while any committed voter's lane is unheld, with bounded exponential backoff and per-peer jitter, so a permanently absent peer costs a bounded attempt rate and a full mesh does not storm (the rule that one of every pair's two connections is closed when the two meet stays). When a link returns, let due re-offers (task-c01) go out on the next turn rather than at their next scheduled time. Keep the transport's limits as they are; the age cap is not the problem and is not to be raised to hide it. Report attempts beside the connected count.

**Acceptance:** In an in-process three-voter schedule over real transports, a peer connection ended by a shortened age cap, and one ended by killing and restarting a voter, are re-established by the survivors on both planes without a restart; a submission that arrived while the link was down is delivered by its re-offer when the link returns; a restarted voter's collector links from the survivors return (the case task-j08's `holds` rule leaves one-sided today, since an inbound collector connection carries no replica identity). With all three voters up, the dial rate is bounded and observable in the attempt count; with one voter permanently absent, attempts back off to a floor and never stop. Negative control: dial-once fails the age-cap case.

**Review boundary:** Connection maintenance only. No change to what evidence is counted or when a command is established; no protocol retransmission (frames that could not be queued are still dropped and counted); no election, which is task-d01; no new configuration beyond a backoff ceiling if one is needed.

<a id="task-d04"></a>
### task-d04: Provision a multi-host test domain and write its runbook

**Prerequisites:** task-43, task-48, task-d03.  
**Design:** Sections 10.2, 20.4, 22.1–22.2.

**Implement:** `coordd` itself has no single-host assumption: it binds any address, learns peers from the signed endpoint catalog, which may carry DNS names or IP literals, and verifies identity from certificates. Provisioning does: `coord-harness provision` writes catalog addresses and listeners on `127.0.0.1` with random ports, node, collector, edge and token-service certificates whose only IP SAN is loopback, absolute paths under one run directory, and `up` spawns every voter locally; the harness token service refuses to bind anything but loopback. Add `--hosts n1=host:api_port:peer_port,...` to `provision`, each host an IP literal or a DNS name: catalog entries carry the given addresses; each node's `coordd.toml` listens on the given fixed ports; node and collector certificates carry the host's IP or DNS SAN beside the URI SAN, under the same key so the genesis stays valid; the edge and token-service certificates carry their hosts; and each `nN/` directory is a self-contained bundle whose paths do not require the same absolute location on every host (a documented install prefix is acceptable; the daemon's configuration schema is not changed for this). Give the test-only token service a `--listen` with a matching SAN, keeping it test-only. Add a per-node start (`coord-harness start --node N`, or document running `coordd` directly with `phase=live` as the ready marker). Write `docs/operations/multi-host-test.md`: build, provision on one host, copy the bundles, firewall ports (UDP for the API and peer planes, TCP for the token service and the edge), time sync within the five-second token tolerance, init and start order, the `peers connected=` and `phase=live` markers, running `kine-coord` and the certification suite from another host, killing and restarting a voter, stopping, and the known limits (the leader is the lowest node id until task-d01; no leaf renewal until task-d02; storage growth until history garbage collection is driven by the daemon).

**Acceptance:** A provision with three distinct non-loopback addresses on the local range (for example `127.0.0.2` through `127.0.0.4`, so the test runs on one CI runner while exercising non-default SANs and catalog entries) comes up, serves a request through Kine, and rejoins a killed and restarted voter (task-d03). Provisioning without `--hosts` is byte-for-byte what it was, and the Kubernetes certification workflow still passes. The runbook is followed literally once on three real hosts and the markers it names are recorded in the document. `coord-harness` stays test-only in the production dependency graph.

**Status:** The tooling and runbook merge with the real-hosts run recorded as not done. That run is the maintainer's, on their own test nodes, on a build carrying task-d03's both-lanes re-dial, task-d01's restart rule and task-d05's bounded recovery (or without restarting or losing the leader host); it lands as a follow-up documentation PR on task-d04, and task-d04 stays open until it does.

**Review boundary:** Test tooling and documentation. No production provisioning tool: signed genesis, catalog signing and node issuance for production remain task-42/task-43 and task-65 concerns. No weakening of identity checks or of the token service's test-only status, and no change to `coordd` beyond what a relocatable bundle strictly needs, which is expected to be nothing.

<a id="task-d05"></a>
### task-d05: Bound recovery reports and Syncs by what the voters executed

**Prerequisites:** task-26, task-53, task-d01, task-d06.  
**Design:** Sections 4.8–4.9.

**Implement:** Recovery today carries the whole history. Dependency rows are never pruned, `DurableLedger` reports every one, and so a report, and the Sync selected from reports, names every command the domain ever ran. A voter remembers only its last `capacity` retirements, so for an older command it executed `phase_of` answers `None`, the same answer as for a command it never heard of. After enough history, every election leaves the candidate asking for payloads of commands it executed long ago, or a follower installing placeholders for them, until the table fills and new work is refused as `Backpressure`. The Jepsen client's leader-kill run shows this as a domain that elects but never serves its final read. The deterministic cluster shows it too: at capacity 32, 200 commands before a leader loss leave the candidate waiting on 160 payloads. Bound what recovery carries by what the voters have executed. The mechanism is this task's decision, under task-26's rules. Three candidates:
- an execution floor below which no voter reports and every voter treats a selected command as executed, established from what a quorum has executed (task-53's floors are the model);
- a pruned ledger, whose dependency rows go once every voter has executed them;
- a durable "executed" answer the machine can consult for any command, not only the last `capacity`.

Whichever it is, a lagging voter below the floor catches up by the checkpoint path, not by recovery.

This is the top liveness priority. Without it a domain that has executed more than about twice its table's capacity cannot elect a leader at all: the candidate must hold the whole selection, gets its payloads back at most `MAX_PAYLOAD_TRANSFER` at a time, and a table of 64 cannot hold a selection of hundreds, so no fault that costs the leader is survivable after the first minute. As its first, separate commit, the command table's capacity becomes configuration (`coordd`'s voter configurations hard-code 64 today) with a raised default. That is a memory bound and an operational setting, documented as moving the cliff, not removing it, and not a safety switch; it lets the Jepsen and stress runs exercise the fault paths while the floor is built.

**Acceptance:** In the deterministic cluster, an election after more history than the table holds completes within one campaign attempt, with no re-campaign, and the new ballot serves, with no payload asked for a command every voter executed; the candidate never needs to hold more records than the live window. A voter that executed less than the floor is brought up by a checkpoint, and a command above it is still recovered exactly as before. The Jepsen client's leader-kill run (`--fault leader`) serves its final read. Reports and Syncs are bounded by the live window, not by history.

**Review boundary:** `coord-consensus` recovery and its durable rows, plus the daemon wiring the floor needs. No change to selection among commands above the floor, to the commit rule, or to what a command's dependencies are beyond task-d06's rule (a retired command stays its key's latest). A key's latest can sit below the floor on a quiet key, so whatever answers for commands below the floor answers for it as executed. Nothing below the floor is re-executed, and nothing above it is skipped.

<a id="task-d06"></a>
### task-d06: Keep one execution order on every replica when a table reclaims

**Prerequisites:** task-21, task-24, task-c02.  
**Design:** Sections 4.2, 4.6–4.7.

**Implement:** Every command is initialized on the conservative key and depends on that key's latest command, which is what makes the dependency chain total. `CommandTable::retire` cleared the key's latest when it retired it, and `initialize` reclaims -- retiring every executed record -- exactly when the table is full and before it computes the dependencies. So the first command a full leader proposed named no dependency. A follower still behind the command it should have named (a payload missing, its own table full) found it committed and ready, executed it first, and executed the same committed commands in another order than the leader; its frontend then answered from that state, since a votes-only delivery and a retry are both answered from this node's own execution record. The Jepsen client found it as `:valid? false` (G1a, a lost update, a PL-1 cycle), all of it through one follower, and the pause stress driver as a follower whose acknowledged appends no other voter holds. Keep the key's latest across retirement, and never evict the tombstone of a command that is still some key's latest, so the guards always answer for what the next proposal names. The same chain breaks at an election: a follower's latest command moves when a payload arrives, in arrival order, and a follower that wins re-proposes the recovered order without moving it, so its first fresh command could name a command in the middle of that order. The new leader makes the recovered tail its latest before it proposes anything new. A node whose leader's release contradicts its own execution record (`release-record-mismatch`) has diverged: it stops rather than answer from that record.

**Acceptance:** A table at capacity whose records all executed gives the next command a dependency on the last one. In a three-voter cluster, a follower that never receives one command's payload does not execute the command a full leader proposes next before it, and once the payload arrives it executes both in the leader's order. A follower that won an election after receiving the leader's last two payloads in the other order gives its first fresh command a dependency on the recovered tail, and a voter holding that tail without its payload waits for it rather than executing the fresh command first. A mismatch between a held release and the local record ends that turn without sending anything read from the record. The `settle_from_records` and retained-answer paths are recorded as trusting one execution order on every replica, which this task is what provides.

**Review boundary:** `coord-consensus`'s conflict index and tombstones, and the divergence stop in `coordd`. No change to how dependencies are chosen for a table with room, to the commit rule or to recovery selection, beyond which command a new leader's first proposal names; no new durable format.

<a id="task-d07"></a>
### task-d07: Re-send a proposal until every voter has voted on it

**Prerequisites:** task-23, task-25, task-d03, task-d06.  
**Design:** Sections 4.2, 4.7.

**Implement:** The protocol assumes a proposal reaches every voter, and the transport drops a frame by design when a lane is full, so a proposal a voter never receives is never sent again. Three triggers are known. A follower not yet linked when the proposal went out (the Jepsen client's reads that never complete through the last voters started). A frame a full lane refused: a new leader re-proposes its whole selection in one pass, and a bounded control lane refuses dozens of those frames. A re-proposal that reaches a follower before its Sync, refused as `FencedByPromise`. In each case the follower never learns the command or its order, holds it (or everything after it, since every later proposal depends on it through the conservative key) and executes nothing more until a Sync realigns it; writes still succeed where the leader answers them, and reads through that follower wait for ever. Re-sending is the protocol's job: the leader re-sends a proposal to each voter whose vote it lacks, paced and bounded per turn, until every voter has voted on it; and `from_recovered` re-proposes its selection in bounded batches rather than all at once, continuing as acknowledgements arrive.

**Acceptance:** Voters 1 and 2 serve a write, voter 3 starts afterwards, and a read through voter 3 is served. The same with a proposal dropped to a linked follower, and with a re-proposal refused ahead of its Sync. The re-send is bounded per turn and paced, and a follower missing more than one batch catches up. `shim-stress.py --fault leader` and `--fault majority` serve their final read and keep serving after the election, and a Jepsen run's throughput continues past its first election.

**Review boundary:** Protocol transfer only: what a leader re-sends, when, and in what batches. No change to what is proposed, voted, committed or executed.

<a id="task-d08"></a>
### task-d08: Bring a lagging voter up from a peer's executed history

**Prerequisites:** task-d05, task-d09, task-d14, task-d17.  
**Design:** Sections 5.3, 17.6, 17.16.

**Implement:** A voter down longer than the leader keeps what it needs has no path back: every command it lacks is one no peer re-sends, since the leader re-sends only what its table still holds, and the Jepsen runs' voters cut off for 100 s and more came back holding tables of commands they could never commit, refusing new work as `Backpressure` for the rest of the run. The design's catch-up was a peer's checkpoint, and it cannot be wired as written: a checkpoint installs into an empty store only, the journal cannot swap generations under a live voter, and nothing trims the rows a lagging voter lacks, so every peer still holds them. The voter asks a peer for the commands it executed after the voter's own `executed_through`, on the bulk lane, one page outstanding, bounded in commands and bytes, and executed before the next ask. The donor is a voter of the current configuration synchronized at the requester's ballot, the leader first; it serves positions up to its own `executed_through`, read from its durable rows only: the payload, the admission digest, the decided dependencies (its dependency row) and the executed row (position, revision, result digest). Pages are tagged with that ballot, and a page from any other ballot, or from a peer that is not a voter, is dropped. Each pulled command enters as a decided commit, not as bare execution, through the checks a Sync's COMMIT entry goes through (task-d14): a record below COMMIT under other facts is rebound, one at COMMIT or beyond under other facts is a second decision and stops the voter. It is let into a full table, and the ordinary executor runs it once its decision is durable. Where the donor keeps no dependency row, the command is installed with none, the voter's own row of it is deleted, and it goes to history once executed: no command is reported committed with other than the decided dependencies. After executing, the voter compares position, revision and result digest with the donor's executed row; a difference is a divergence stop through task-d17's description, as a third check naming catch-up, under the unchanged first line. (Amended by task-d35 and task-d36: a page carries each row's chain value and digests, a page whose chain does not continue is fetched again from another donor, and a difference after executing is decided by task-d36's majority rule rather than on one donor's word.) The donor reads its position order once, from the whole of `executed_v1`, and keeps it in memory, current with its own executions. A restart resumes from the durable frontier; the executed row is the guard against executing twice.

**Acceptance:** Two voters cut off for 100 s under load are both back within a table, with no more `Backpressure`. A follower partitioned for 30 s at 20 operations a second serves within 10 s of healing. A pulled command executed to another result than the donor's stops the voter, the stop naming the catch-up check. A page from another ballot is dropped. A full table does not block catch-up, and drains afterwards. A restart in the middle of catch-up resumes without executing anything twice. Nothing is reported as committed with other than the decided dependencies.

**Review boundary:** Catch-up transfer and installation, with no format change: two peer messages are added, and no row changes.

**Residuals.** Once quorum-certified forgetting (task-52, task-53) is wired, a voter behind that floor cannot pull below it; the forgetting task decides how that voter comes back. The donor's first read of its position order is O(history), in time and memory; a durable position index would remove it and is out of scope. A donor that itself executed in another order than the domain is not caught here: the requester trusts one peer's order at one ballot, as it trusts that ballot's Sync, and the donor's own divergence stops are what catch it. A pulled command whose decided dependencies name a command this voter executed and has since forgotten (more than a table's worth of retirements ago, and no key's latest) is not installed, and catch-up stops there. A record whose command the domain never decided, such as a submission that reached only this voter or a leader's proposal that no quorum saw, is not drained by catch-up: it leaves the table when the command is decided (a collector's re-offer, a caller's retry, or a later selection that carries it), and while it stays the voter asks for catch-up once a second and is answered with nothing. Evicting such a record once a Sync has shown it undecided would be a change to the table's rule that nothing unresolved is evicted, and is left to its own task. A candidate's selection payloads, and a campaign making progress past its ceiling (task-d01's residual), stay with task-d10's record. Windowed catch-up (task-d25) carries two more detection gaps here, and loses no order in either: a voter that wins a campaign after a restart and before its first ask skips the since-boot comparison, and an execution made durable just before a crash is at or below the boot frontier, so the first ask does not cover it.

<a id="task-d09"></a>
### task-d09: Carry the leader's commit decision to every voter

**Prerequisites:** task-24, task-d07.  
**Design:** Sections 4.3-4.5.

**Implement:** A follower commits a command only from the acknowledgements it receives itself, each published once to every voter on a lane that drops frames by design. Nothing publishes a missed one again, and no message carries a decision from the leader to a follower outside an election's Sync. With five voters a follower needs two of its peers' acknowledgements besides the leader's proposal and its own; one that missed them sits at ACCEPT on that command, and with the chain total (task-d06) on everything after it. Within a table's worth it refuses new payloads as backpressure and the leader loses its vote: the Jepsen five-node runs' followers held a thousand adopted commands with payloads and could commit none past the first partition. The leader announces its commit frontier, the highest sequence number of its ballot whose whole prefix is committed with its batch durable, on the re-send timer. A follower commits every proposal it durably adopted from that leader in that ballot at or below the frontier, in sequence order. The leader's commit is a decision under the crash-fault model, and the proposal a follower adopted carries the leader's dependencies. A command whose turn has come and which the frontier covers is let into a full table, since a table full of later commands none of which can be adopted before it would otherwise hold it out for ever; a follower's bounded payload ask names those commands first. A follower killed with adoptions in flight comes back with them restored from the rows without the ballot's sequence numbers (a ballot numbers from zero and the row names no ballot), so the frontier cannot commit them and the leader, having counted them, never re-sends them; it asks the leader for their proposals, paced by the frontier, and adopts them again with their sequence numbers.

**Acceptance:** A follower that receives no peer's acknowledgements executes everything the leader commits. A follower restarted under a live leader with adoptions in flight executes everything the leader commits. Two followers cut off from each other under a live leader both execute. A follower whose table filled while it could learn nothing catches up and executes in the leader's order. The frontier commits nothing a follower did not adopt, and a frontier from any voter but the ballot's leader is ignored. On the Jepsen five-node run, every voter's execution tracks the leader's, and service returns within seconds of the last heal, as it does on the etcd baseline; the time from the last heal to the first ok on every node is the number both runs report.

**Review boundary:** Learning from the leader's word, and admission for the command whose turn has come. No change to what is proposed, to the fast or slow predicates, to what an acknowledgement means, or to recovery.

<a id="task-d10"></a>
### task-d10: Flow-control catch-up from each voter's own frontier

**Prerequisites:** task-25, task-d09.  
**Design:** Sections 4.2, 4.7, 4.8.

**Implement:** task-d07's re-send is paced at 16 proposals per voter per 250 ms on the lane that carries new proposals, and payloads are asked 8 at a time from the leader alone, one ask outstanding; a burst of hundreds of refused frames is not repaired at that rate while new commands keep arriving. Catch-up driven from the voter's own frontier is task-d08's. A candidate's selection payloads, and a campaign that is making progress not being abandoned at its ceiling (task-d01's residual), remain recorded here. First, and ahead of the rest: a promised ballot that has not synchronized within the campaign ceiling counts as leaderless, so a candidate that stands down without a Sync holds no voter that promised it for longer. Next, a candidate more than a table behind the voters it asks does not lead. A voter restarted far behind can have its election timer fire first and win, and a leader that far behind cannot serve: what it lacks was executed and retired by the voters that would send it, a leader asks nobody for payloads, and it may hold a command undecided whose decision no reporter still holds (task-d12). The replica's own table cannot tell it is behind, since its rows say ACCEPT for commands that were committed in memory; its peers can. `NewLeader` carries the candidate's executed position, and a voter whose own executed position is more than its table capacity past it refuses to promise, replying `PromiseRefused` with its position; the refused ballot is remembered, this voter's next campaign goes above it, and it promises no other ballot at or below it, since the candidate promised itself that ballot and has to follow the next leader to catch up. Only a configured voter's refusal is honoured. A leader that refuses a higher ballot as behind steps down and campaigns above it at once, so the refused voter, which a restart's timer can fire before it hears the leader, promises and follows rather than sitting deaf above the live ballot until an election; it costs one election per refused campaign, bounded because the refused voter does not campaign again until it has caught up. The window is `limits.command_table_capacity`, which is set alike on every voter of a domain so that voters judge alike; nothing enforces that, and a voter with a larger table only refuses later. The candidate abandons its campaign on the first refusal, unless its selection is already being bound, and does not campaign again until it has executed as far as the refuser. A voter that refused never promised, so for it the ballot is leaderless and it campaigns after its patience; nobody waits out the campaign ceiling and nothing is withdrawn. The most advanced live voter is refused by nobody, so someone can always lead; a candidate whose `NewLeader` reaches only voters as far behind as itself still leads, which is catch-up's case.

**Acceptance:** A candidate that stands down after its promises leaves no voter waiting on it past the ceiling, and a live candidate is not campaigned over. A candidate whose reporters' windows are a full table binds its selection. Three voters, one restarted more than a table behind the others and the leader gone: the behind voter campaigns first and is refused by the other, which names its position and has not promised; the behind voter abandons, does not campaign again while it is behind that position, and promises and follows the refuser's campaign above the refused ballot; with the old leader back the domain serves. A replica refused at a position it can reach campaigns again, and leads, once it has executed that far. A voter that refused campaigns above the refused ballot and promises no ballot at or below it; a refusal from a replica that is not a voter is ignored. A live leader a table ahead of a voter that campaigns refuses it, leads again above the refused ballot, and the refused voter promises that ballot and follows. Bounded memory and lane use per voter throughout.

**Review boundary:** Transfer and pacing, and who is promised: `NewLeader` gains the candidate's executed position and a voter may answer it with `PromiseRefused`. No change to what is proposed, committed or executed, and no durable format change.

<a id="task-d11"></a>
### task-d11: Leave no acceptance of an earlier ballot behind a Sync

**Prerequisites:** task-26, task-d05.  
**Design:** Sections 4.8, 4.9.

**Implement:** A voter installs a Sync's entries and nothing else, so an acceptance it made under an earlier ballot, of a command the Sync re-proposes or omits, stays at ACCEPT. Its next report is labelled with the new synchronized ballot, so that acceptance reads as the new ballot's: beside a voter that adopted the command again under the new ballot, with other dependencies, every later selection fails as `IncompatibleAccepted`, and without one, a selection installs dependencies no quorum of the new ballot agreed on. At Sync installation, demote to PRE-ACCEPT every record at ACCEPT that the decision does not carry as an entry, durably in the batch of the synchronized-ballot row, keeping its payload and dependencies; a winning leader installs its own Sync the same way. A committed record is a decision and stays.

**Acceptance:** A voter that accepted a command under an earlier ballot and then installs a Sync that re-proposes it reports it at PRE-ACCEPT, before and after a restart, and a selection over its report and a new-ballot acceptance completes with the new ballot's dependencies. `IncompatibleAccepted` no longer ends campaigns in the stress runs.

**Review boundary:** Sync installation and its rows. No change to selection, the report format or the commit rule.

<a id="task-d12"></a>
### task-d12: Chain a new leader's proposals after what it executed

**Prerequisites:** task-26, task-d06.  
**Design:** Sections 4.2, 4.8, 4.9.

**Implement:** task-d06 keeps the chain total at an election by making the recovered tail the new leader's latest command, and the anchor it chose was wrong. `Leader::from_recovered` walked the selection's re-proposed commands in identity order and moved the anchor to every one the leader knew, including commands it had executed and retired long before, which it then did not propose. A selection over a behind reporter's rows is exactly that input, so the first fresh proposal depended only on the largest identity among old commands, and a voter that had executed the tail ran it beside the tail with no edge between them. A command the leader had at Commit or beyond was re-proposed chained after the anchor, not with its decided dependencies, and with an empty recovered order the first re-proposal depended on nothing. The d10 stress run found it as `release-record-mismatch`, and the Jepsen run as lost appends: guarded appends that held on the forked node and failed on the main line. A re-proposed command the new leader has at Commit or beyond is neither chained nor re-proposed. The chain starts after all of: the recovered order's last command the leader has not executed, every last command the leader committed and has not executed yet (a restarted table can hold two, a Sync-installed commit coming back from its row and the commit between them not), and the last command it executed, which the table keeps as it executes and restores from the executed rows in execution-position order on a restart. Each is the tail when the table is right, and depending on one already behind the tail orders nothing wrongly; any one alone has been wrong. A refused command took its execution position and wrote no executed row, so a restart replayed it as unexecuted: the node executed it a second time, at another position, and a new leader whose selection carried it chained after it alone, forking from everything executed since (stress run d12-11). A refusal now writes the executed row every other command writes, and nothing under the retry key. A follower also accepted in the ballot it had promised away: a proposal it held at the promise (its payload or a dependency not there yet) was adopted once it became ready, during the campaign, and its own acceptance with the old leader's proposal made a quorum there, so it executed a command the candidate's selection, taken from reports sent at the promise, never saw; the new leader chained its first command after the same predecessor (stress runs d12c-1, -7, -9 and -11, and the reviewer's `--faults 2,1` replay). A follower adopts nothing, and acknowledges no payload that reaches it, while it may not vote; activation drops what it holds. The same hazard remains where the leader holds a decided command only at ACCEPT and the decision sits in reports below the source ballot, since the source rule sent all of those to `reproposed`. This task amends task-26's selection rule: an entry at Commit in a report below the source ballot, executed-as-committed ones included, enters the selection at Commit with its dependencies and paths, while PRE-ACCEPT and ACCEPT below the source are still re-proposed; the agreement check applies to it as to any entry, so a below-source commit meeting an at-source acceptance under other dependencies is `IncompatibleAccepted`. A commit is a quorum's acceptance of one dependency set, final whatever ballot it was reached in, and the source rule chooses among acceptances. The new leader re-proposes such an entry with its decided dependencies where it has not executed it, a behind voter installing the Sync commits it without a vote, and the Sync grows by the behind reporters' commits, bounded by the reports it already reads. What this leaves is no reporter holding the decision at all, every voter that had it having retired it, while the candidate holds the command undecided: that candidate is behind by definition, and task-d10's rule catches it. A voter far behind can hold the opposite: an acceptance, from a Sync it installed, of a command whose payload never reached it and which every other voter executed long ago and so leaves out of its report (task-d05); one that retired it may no longer hold the payload either. No report then supplies the payload, and every campaign whose majority included that voter's report failed as `HalfInitialized` (stress run d12-15, and the five-node Jepsen stalls). A command the candidate itself holds or executed counts as supplied: it is selected, and committed before binding when executed. Otherwise the campaign sets the behind report aside while a majority of reports remains without it, since any majority of promises is a sound basis for the selection; it never sets aside its own, and with neither it waits for the voters that have not reported and fails only once every voter has. A collector that answered a command from this node's record compares a later release with the execution the answer came from, response, result digest, position and revision, and a difference stops the node as `release-record-mismatch` does; nothing more is answered in that pass. The first wrong answer is not prevented by that comparison; it makes the node stop rather than go on answering from a diverged state. A voter holding a decided command the Sync omits is at PRE-ACCEPT after task-d11 and learns the decision only by catch-up (task-d08); before task-d11 it sat at ACCEPT with no commit coming, so nothing regresses.

**Acceptance:** A new leader whose selection re-proposes a command it already executed gives its first fresh proposal a dependency on its executed tail, and so does a re-proposal with no recovered entry before it. A new leader holding committed commands it has not executed yet chains after them, whether or not one of them is re-proposed. A candidate restarted with no entries, whose executed rows' identity order differs from their positions, wins and chains after the command it executed last. A history read back from a trimmed store comes back in execution order. A refused command's executed row is written with its position and result digest, and a restart replays it as executed. A proposal held when the follower promises a higher ballot is not accepted after the promise, when its payload arrives. A new leader whose table holds an old command unexecuted, with the selection carrying it, still gives its first fresh proposal a dependency on the command it executed last. A below-source report holding a command at Commit under D, the candidate holding it at ACCEPT under D and at-source reporters without it select it at Commit with D, and the candidate proposes it with D; an at-source acceptance under other dependencies against that commit is `IncompatibleAccepted`. A campaign whose reports include another voter's acceptance that no report has the payload for selects it, committed, when the candidate executed it; otherwise it selects from a majority without that report, waits while no such majority has reported, and fails as `HalfInitialized` only when its own report holds the acceptance and every voter has reported. A late release that contradicts an answer given from the record, in the response, the digest, the position or the revision alone, is refused with the command named and stops `coordd`; one that agrees settles nothing further.

**Review boundary:** `from_recovered`'s anchor and which re-proposed commands it proposes, the table's committed and last executed commands, the order in which history is read back, the executed row a refusal writes, the follower's promise fence on held proposals, the source rule's treatment of commits below the source ballot, what a campaign counts as supplied and its setting aside of a report that holds an acceptance nobody can supply, and the late-release comparison. No change to the commit rule, the report format or any durable format: a refusal writes an `executed_v1` row in the existing layout.

<a id="task-d13"></a>
### task-d13: Keep a diverged node stopped across a restart

**Prerequisites:** task-d17.  
**Design:** Sections 4.7, 5.4, 17.6.

**Implement:** A node that stops on `release-record-mismatch` has executed the domain's commands in another order than the leader, and its store holds a history the domain did not decide. The stop ends the process and nothing more, so a restarted node serves again from that store. Write a durable stop marker with the stop, naming the command and the ballot, and refuse to start while it is there, until an operator clears it. A diverged node is not brought back by catch-up, which adds to a history and cannot undo one: it is rebuilt by replacement through membership (Section 5.4, task-d41).

**Acceptance:** A node stopped on a mismatch refuses to start, saying why and naming the command; it starts after the marker is cleared, and a node replaced through membership starts without it.

**Review boundary:** The marker, its check at start and its clearing. Planned only; no change to how divergence is detected.

<a id="task-d14"></a>
### task-d14: Name a recovered decision's admission facts

**Prerequisites:** task-d09, task-d12.  
**Design:** Sections 4.8, 4.9, 9.3.

**Implement:** One command is one set of attested facts: a vote under another admission digest than the ones a vote set counted is refused, so a decision has one digest. Each presentation of a request mints its own receipt, though, so a voter can hold a command under a presentation other than the one a quorum accepted, and nothing in recovery said which one that was: `ReportEntry` and `SyncEntry` carried no facts, and a new leader re-proposed a selected entry under its own record's. A follower holding the command past PRE-ACCEPT under the old leader's facts answered `AdmissionConflict` and never voted, task-d09's rebind covering PRE-ACCEPT only, so with the third voter out the entry never committed and everything chained after it waited, the leader's own lease command included. (The stress runs raf-2 and repaf-4, a leader republishing its lease command until the end, were first taken for this case; their stores hold every command under one digest, and they are a separate stall.) With every voter up, that follower stayed at ACCEPT on it for good. And a Sync COMMIT entry made a new leader holding another presentation execute the command, without a vote, under facts no quorum accepted, which for a session-establishing command writes another receipt identity into replicated state. Both entry types carry the digest the reporter's record holds; a Sync entry whose payload has not arrived reports the digest its Sync named. In the selection, copies at ACCEPT or beyond of one command under different digests are `IncompatibleAdmission`, a second decision and never a merge, whether or not either copy has its payload; an acceptance below the source ballot decides nothing and is re-proposed as before, whatever its facts. A command counts as supplied to a candidate only when it holds the payload under the named digest, or executed the command; otherwise the payload is fetched like a missing one and the candidate's record rebound before binding, so re-proposals carry the selected facts. A candidate that committed or executed a selected command under other facts stops its campaign as `IncompatibleAdmission`. A voter installing an entry whose digest differs from its record rebinds at PRE-ACCEPT and ACCEPT, fetching the payload under the named facts first; a payload under other facts than a selection names is not taken. At COMMIT or beyond a different digest is two decisions of one command: that voter does not install the entry and stops voting and executing, as a selection stops on `IncompatibleAccepted`, and `coordd` stops the process as on `release-record-mismatch`; staying stopped across a restart is task-d13's marker. A command executed and retired is compared against its payload row where one is kept; with none it has nothing to compare, and after this task nothing to fear, since a voter can only have executed a command under other facts than its decision through the path this task removes; `ExecutedRecordV1` is unchanged. The report pages and the Sync carry the digest on the wire, and the bound Sync row carries it durably: schema version 3, and a row of an earlier version is refused as corrupt rather than read as a selection without facts. Stores are started fresh.

**Acceptance:** With r0 proposing X under one presentation's facts and r1 holding a second presentation, r1's campaign with r2 out re-proposes X under r0's facts, both voters execute X and a later command, and r0 refuses no proposal as `AdmissionConflict`; with every voter up, all three execute X under those facts. A command decided and executed under one presentation's facts executes under them on a new leader that held another presentation. Two eligible copies of one command under different digests, a COMMIT beside an ACCEPT, are `IncompatibleAdmission`; an ACCEPT below the source under other facts than the source's is re-proposed without an alarm. A voter at ACCEPT under other facts than an installed Sync names fetches the named payload and rebinds; one that committed the command under other facts reports `IncompatibleAdmission` and executes nothing more. A Sync row of version 1 or 2 is refused.

**Review boundary:** The admission digest in report entries, Sync entries and the bound Sync row; the selection's agreement check on it; what a candidate counts as supplied and its rebinding before binding; a voter's rebinding at installation and its stop on a committed mismatch. No change to the vote set's admission rule, the commit rule or `ExecutedRecordV1`.

<a id="task-d15"></a>
### task-d15: Ask again for every vote the leader still needs

**Prerequisites:** task-d07.  
**Design:** Sections 4.2, 4.7.

**Implement:** task-d07's re-send sends a voter the proposals it has not adopted, but only those after the latest one whose adoption from that voter the leader counted: the chain is total, so a voter that adopted a proposal holds every earlier one. That is true of the voter and says nothing about which of its acknowledgements reached the leader. One lost acknowledgement for a proposal, with a later one counted, and that proposal is never asked for again; with the domain's other votes lost too (a follower whose frames to the leader were refused by a full lane, or that the leader cannot reach), the leader never learns the command, and everything chained after it waits at ACCEPT, its own lease command included, while the followers commit and execute it among themselves (the stress runs rep108-3 and repaf-4, and most likely raf-2). This amends task-d07's rule: a proposal the leader holds below COMMIT is sent until the voter's adoption of it arrives, however far past it that voter's counted adoptions reach. The skip stays only for a proposal the leader has committed, which is the case it was written for: a voter that executed and retired the command may keep no record to answer from, and the leader no longer needs its vote. The budget is task-d07's, oldest first per voter per round.

**Acceptance:** With one voter down and the other's acknowledgement of one proposal lost while its acknowledgement of a later one on the same key is counted, the leader executes both within a bounded number of re-send rounds; with task-d07's rule it executes neither. A proposal the leader has committed is not sent again to a voter whose adoption of a later one was counted. The `--faults 2,1` stress replay and random stress end with no leader behind its own followers, and five-node Jepsen runs show no leader republishing its lease command past the final heal.

**Review boundary:** Which proposals the leader re-sends. No change to what is proposed, voted, committed or executed, or to the re-send's pace and budget. A voter that swept a command's record before answering is recorded, not closed: answering from its executed row needs its own argument about which ballot's order it executed under.

<a id="task-d16"></a>
### task-d16: Say why each address of a dial failed

**Prerequisites:** task-d03.  
**Design:** Sections 3.3, 19.2, 22.1.

**Implement:** A node has two listeners, one per plane, and the catalog lists both of its addresses without saying which is which; a dial tries each address in turn, and the listener of the other plane refuses it by design, negotiating none of its application protocols (TLS alert 120, `no_application_protocol`). `coordd`'s dial kept only the last address's error. When the address serving the dial's plane was listed first and failed, the other listener's refusal was the one logged, so every failed peer dial in the Jepsen and stress runs said alert 120, and why the peer address failed was discarded: the likely reason `n2`'s dials to `n1` never recovered in d12-15, and `n2`'s 1336 `NotConnected` frames in rep108-3, have never been visible. The transport reports the other plane's refusal as its own error, `TransportError::WrongPlane`, and a dial that reaches no address keeps every address's error; what is logged is the error of each address that could have served the dial, and a refusal by the other plane's listener is not counted as a failure. A voter whose every address serves the other plane is said to be that.

**Acceptance:** A voter dialled on the peer plane with a closed peer address and its api address, listed in either order, is logged with the peer address's failure, not alert 120. A dial to the other plane's listener alone is refused as `WrongPlane`. A replay of the stress run that produced rep108-3 shows why the restarted voters' dials fail.

**Review boundary:** What a dial reports and logs. No change to which addresses are tried or in what order, to what a listener accepts, or to the re-dial schedule.

<a id="task-d17"></a>
### task-d17: Say what a divergence stop compared

**Prerequisites:** task-d12.  
**Design:** Sections 4.7, 17.6.

**Implement:** A node stops on `release-record-mismatch` from two checks: a release the collector holds against this node's own record of the command (task-d06), and a release that arrives after the collector answered against the answer it gave (task-d12). Both printed the command's first four bytes and a sentence saying the node executed the domain's commands in another order than the leader, whatever the comparison found; a stop in a five-node Jepsen run whose stores were not kept could not be read further than that. The collector's errors carry what they compared: which check, the full command identity, and for each side the position, revision, result digest and response length, with the release's sender, epoch, ballot and `speculative` flag, and on this node's side the same for the answer it gave from an earlier release, and which of the compared fields differ. The settle turn carries them out of the collector unchanged, and `coordd`'s stop prints them after the first line's unchanged prefix, `this node stopped: release-record-mismatch(<8 hex>)`, with this node's ballot, leader and `executed_through` at the stop, and its own `executed_v1` rows within eight positions of both positions, each as `position revision digest command`. "In another order" is said only when the positions differ.

**Acceptance:** Each check's error carries both sides and exactly the fields a forked record changes: another response, another result digest, another position, another revision (which also changes the response a caller is handed). A position-only mismatch is described as another order and a digest-only one is not. The first line's prefix is unchanged on both paths. The rows shown include the command where this node executed it, and only rows within eight positions of either side.

**Review boundary:** What a divergence stop says and what the collector's mismatch errors carry. No change to what is compared, to when a node stops, or to what it answers.

<a id="task-d18"></a>
### task-d18: Never let a Sync lower a durable promise

**Prerequisites:** task-20, task-26, task-d11.  
**Design:** Sections 4.8, 5.1.

**Implement:** A voter that has promised ballot P1 durably and then accepts a `NewLeader` for P3 queues the promise row `{P3}` and publishes `Promise(P3)` once that row is durable. If P1's Sync arrives in between, `Follower::on_sync` accepts it: the Sync's ballot equals the durable promise, and unlike `may_vote` it never looks at a promise in flight. `BallotState::mark_synced` then builds `{promised: P1, synced: P1}` and queues it after the P3 row. The journal applies rows in order, so the durable promise ends at P1 after the voter told P3's candidate it would vote in nothing below P3, and a crash brings the voter back willing to vote in P1. A Sync of a ballot below a promise in flight is held, as a Sync ahead of the promise already is, and is installed only if that promise fails. Every write of the promise row, from a promise, a Sync or a seal, carries the highest promise already queued for disk, so no row that lands later can lower one that landed earlier.

**Acceptance:** `a_sync_behind_a_promise_in_flight_does_not_lower_the_durable_promise` (checklist review, D1/D4), which fails today, passes. A model test drives every order of `NewLeader` for two ballots, the lower ballot's Sync and the storage completions of their rows: in none is the durable promise below a ballot for which `Promise` was published, and the voter installs the lower Sync only when the higher promise's row failed.

**Review boundary:** Sync admission against promises in flight and the content of promise-row writes. No change to the commit rule, the Sync's selection or any row format.

<a id="task-d19"></a>
### task-d19: Count only adoptions toward the slow majority

**Prerequisites:** task-24, task-26, task-28.  
**Design:** Sections 4.1, 4.9.

**Implement:** `VoteSet::learned` and `learned_slow` count a fast-set member's fast acknowledgement toward the slow majority when its dependencies equal the leader's (the mapping cites the prototype's `acceptFastAndSlowAck`, `Dep == nil || leaderDep.Equals(dep)`). Recovery keeps a PRE-ACCEPT only through the possible-fast rule, which needs every reporting fast-set member to hold the command with the same path. With five voters and fast set {r0, r1, r2}, a command learned from r0's proposal, r1's fast acknowledgement and r3's adoption is re-proposed with new dependencies when r2 recovers from r1, r2 and r4, although the collector may have released its result. With three voters the fast set is a majority and the possible-fast rule covers it.

Count only adoption acknowledgements toward the slow majority, leaving the fast predicate unchanged. Recovery cannot be made to keep the pre-accept instead: at five voters a recovering majority {r1, r2, r4} can hold two fast-set pre-accepts of one command, r1's with D and r2's with D′, and with neither the leader nor an ACCEPT copy among the reports, a slow decision through {r0, r1, r3} with D is indistinguishable from one through {r0, r2, r3} with D′. With adoptions only, a slow decision is the leader and `slow_size − 1` durable ACCEPT copies at its ballot, any majority of reports holds one, and selection already keeps ACCEPT at the source. The cost is latency only: every non-leader already adopts and sends its adoption acknowledgement, and `adopted_by` and the re-send already count adoptions only. The change reaches every learner that uses `VoteSet`: the leader's, the followers' `commit_learned` and the collector's release. The mapping's `acceptFastAndSlowAck` row becomes `[EXT: stricter]`, with this counterexample, and records that the paper's recovery appendix was not consulted and why the rule stands without it.

**Acceptance:** `a_slow_decision_counting_a_fast_ack_survives_recovery` (checklist review, P5/P6), which fails today, passes. A bounded model for three and five voters joins learning with selection: for every vote set that learns a command and every majority of reports consistent with it, `select` keeps the command with the learned dependencies. A collector-level test shows the collector does not release a result on a fast acknowledgement counted as an adoption. The five-node Jepsen stop `release-record-mismatch(c96f0e70)` is read again against the fix and its conclusion recorded.

**Review boundary:** The slow-learning predicate in `VoteSet`, every learner that uses it, and the mapping row. No change to the fast-learning predicate, the quorum policy, recovery selection or any row format.

<a id="task-d20"></a>
### task-d20: Prove the largest Sync fits its row, or refuse the campaign

**Prerequisites:** task-d05, task-d14.  
**Design:** Sections 4.9, 5.3.

**Implement:** `MAX_COMMAND_TABLE_CAPACITY` (1536) rests on one test that sizes a Sync from three disjoint reports whose entries carry no admission digest. A campaign selects over every complete report it holds, up to five; entries carry a digest since task-d14; adoptions past capacity and every pending Sync entry go into a report; and `sync_pending` is never cleared between Syncs, so a voter behind across failed ballots reports more each time. The candidate binds with `sync_update(..).expect("bounded")` and every installer writes the same way, so a Sync over the row limit ends the process in the middle of an election. Bound what a report may carry and clear pending Sync entries a later Sync supersedes. Derive the capacity limit from a measured worst case with real entries and five reports, or cap what a selection may carry. A Sync that would still not fit refuses the campaign with a named error.

**Acceptance:** At the largest capacity, five disjoint reports of the most a report may carry, every entry with its digest, give a Sync that encodes within the row and the frame; the same test at one entry more is refused with the named error and does not panic. A voter behind through three failed ballots reports no more than one ballot's worth.

**Review boundary:** Report and Sync size bounds, `sync_pending` retention, the capacity limit and the campaign's refusal. No change to the selection rule or the Sync format.

<a id="task-d21"></a>
### task-d21: Settle whether a recovery cycle is reachable, and never stall on one

**Prerequisites:** task-26, task-d12.  
**Design:** Sections 4.7, 4.9.

**Implement:** When the commands a new leader re-proposes depend on each other in a cycle, the ordering loop in `Leader::from_recovered` stops without an error and leaves the rest unproposed, so the ballot waits on commands no one will propose. A named error alone would only turn the silent stall loud, since every later campaign sees the same reports. First argue whether a cycle is reachable. The loop's comment covers ACCEPT copies from the source ballot only, which one leader's order cannot make cyclic; selection also merges commits from lower ballots (task-d12) and possible-fast candidates carrying their reporters' own dependencies, and whether `guard_accept` rules out every mixed case is to be argued, not assumed. If a cycle is reachable, resolve it deterministically in a way that keeps every decided command's dependencies. If it is not, a cycle is an invariant violation: the node stops as on a divergence, naming the commands, and the selection is not retried as an ordinary failure.

**Acceptance:** The reachability argument is recorded in the notes and the mapping. If reachable, a selection with such a cycle completes and the domain serves, with every decided command's dependencies unchanged. If unreachable, a constructed cycle stops the node with the commands named, and nothing more is proposed.

**Review boundary:** The re-proposal ordering and what it does on a cycle. No change to what is selected.

<a id="task-d22"></a>
### task-d22: End every collector entry the voters refuse

**Prerequisites:** task-c01, task-c02, task-d14.  
**Design:** Sections 4.3, 6.5.

**Implement:** A voter answers three refusals with no effect at all: a request under another admission's facts (`RequestFactsConflict`), another payload under a bound identity, and a duplicate whose payload it has forgotten. The collector removes an entry only when it settles, so such an entry stays pending for good and holds one of the domain's pending slots; once a key's binding leaves the resolved window, a new payload under that key is new work again. A ballot change voids the evidence the collector counted and nothing asks for it again, so a command that executed under the old ballot never settles; evidence addressed to a closed remote connection is dropped. Every refusal is answered with an explicit reply the collector settles on. A pending entry that holds neither half is solicited again after a bounded time and then resolved from the durable record. A ballot change solicits evidence again for every pending entry.

**Acceptance:** A session sends 256 retries that the voters refuse, one per pending slot, and the domain still serves new work. A command executed under a ballot that changed before its evidence reached the collector settles under the new one. Each refusal kind has a test showing the reply and the settled entry.

**Review boundary:** Voter replies to refused submissions, the collector's escalation of a pending entry and its re-solicitation on a ballot change. No change to what a voter accepts or to the evidence rules.

<a id="task-d23"></a>
### task-d23: Tell a client what is known of its outcome

**Prerequisites:** task-34, task-c02, task-d22.  
**Design:** Sections 6.5.

**Implement:** The output gate answers `NOT_ADMITTED` when it withholds the result of a command that executed, and the SDK maps `NOT_ADMITTED` to a definite failure. `ResolveRequest` is answered from the collector's memory alone, so an outcome evicted from the resolved window, or asked of another frontend, is `Unknown` even when the node holds the executed record, and the SDK treats `Unknown` as final; `retry()` after it replays the stored outcome and sends nothing. A withheld result gets its own status, distinct from not admitted. `ResolveRequest` consults the durable record. The SDK gains a "retired, result unavailable" outcome, and `retry()` after `Unknown` submits again under the same identity.

**Acceptance:** A command that executed and whose output is withheld is never reported as not admitted. A resolve through a frontend that never saw the request returns the executed result from the record. A resolve past the retained window says retired rather than unknown. A retry after `Unknown` reaches the voters and returns the retained result.

**Review boundary:** Outcome statuses on the wire and in the SDK, and the resolve path's durable lookup. No change to authorization of output.

<a id="task-d24"></a>
### task-d24: Keep table room for recovery work

**Prerequisites:** task-d08, task-d19, task-d20.  
**Design:** Sections 5.3, 13.

**Implement:** Work that finishes or recovers admitted commands can be refused by a full table. Installing a Sync entry drops its placeholder silently under backpressure, and the payload that follows goes through the bounded path, so only catch-up gets such an entry in. A record no Sync selected and no history names keeps its slot until the command is decided somewhere (task-d08's recorded residual). Sync entries enter a full table as pulled commands do. A share of the table is reserved for recovery and catch-up work. In the Sync's own install batch, as task-d11 puts its demotions there, the records present when the Sync is installed that it neither selected nor re-proposed are released from the table and the key index is repaired. No ballot has to be tracked per record: a record the new leader proposes after the Sync does not exist yet at installation, and a restart resumes the installation from the Sync row, so nothing proposed after it is released this way. The Sync is selected from a majority's reports, and selection keeps every command a quorum of an earlier ballot could have decided (at five voters only once task-d19 is in), so a command it leaves out cannot have been decided below its ballot. A later decision then reaches this voter as any other command does, through the leader or catch-up. The argument is recorded with the change, and no voter outside the majority is waited for. This takes up task-d08's residual on records decided nowhere (its requirement 7).

**Acceptance:** A voter with a full table installs a Sync whose entries it lacks and executes them. A table filled with records decided nowhere drains after the next Sync at a voter whose report the selection did not use: five voters, the old leader permanently absent, and that voter's report arriving after the selection. New work is then admitted without `Backpressure`. (With three voters and one absent, both reports left are always used, so nothing is released.) A Sync carries at most `MAX_REPORT_ENTRIES` commands: every kept entry, then re-proposals in the selection's order. What it leaves out is released by the same rule. After a leader crash right after installing such a Sync, the next campaign completes. A record the Sync selected or re-proposed, or one proposed after it, is never released. New admission never takes the reserved share.

**Review boundary:** Table admission of recovery work, the reservation and the release of undecided records. No change to the selection or commit rules.

<a id="task-d25"></a>
### task-d25: Make catch-up outpace the domain

**Prerequisites:** task-d08, task-d18, task-d19.  
**Design:** Sections 5.4, 13.

**Implement:** Catch-up installs one pulled command, waits for its batch to be durable, executes it and only then installs the next, so a voter pulls about 100 commands a second on the test host and never closes its gap under a load above that. Install a window of up to a page (64 commands and 1 MiB) in one batch ahead of execution. The dependency chain is total (task-d06), so a window of decided commits with dependency rows executes in the donor's positions through the ordinary learner, and a command whose donor kept no dependency row is a window of its own. After a restart the first ask starts at the `executed_through` read at boot, so every row executed since then is compared with the donor's and no comparison is lost. Catch-up can outpace only a domain that leaves execution headroom; task-d26's contract bounds the admitted rate below what a catching-up voter executes, and says so.

**Acceptance:** The unthrottled `follower-out` run (about 113 appends a second, a follower 3400 behind at its restart) catches up and serves, with catch-up's rate reported beside the domain's. A crash at every point of a window resumes without executing anything twice, and every row executed after the boot frontier is compared with the donor's.

**Review boundary:** Windowed installation and its restart path, as its own PR on task-d08. No change to the page format or the donor's rules.

<a id="task-d26"></a>
### task-d26: State the resource contract and test its accounting

**Prerequisites:** task-d20, task-d24, task-d25.  
**Design:** Sections 5.3, 13.

**Implement:** The configuration names limits for a request, a response, a session's outstanding requests, the table and checkpoints, and nothing more. Write a resource contract in the design with separate limits for memory, durable protocol data, application data, a single message, each queue and temporary files. Bound the admitted rate below the rate at which a catching-up voter executes (task-d25), so a returning voter's recovery time is bounded. Account for peaks, not the steady state: the candidate's report pages and their clones, the Sync's encoding, receive-side reassembly of partly arrived frames, and the executor's working set. Bound the per-boot bindings and the executed-history set, and make `advance_sync` stop rescanning every pending entry each round.

**Acceptance:** Each limit has a test that drives it and fails when it is exceeded. A long run shows memory flat in history once admission stops.

**Review boundary:** The contract text, the accounting and the bounds it adds. No change to protocol messages or rows. The executed-history set (`CommandTable.history`) and the per-key tombstones in memory are not bounded here: their owner is task-d46, which retires in-memory history above the floor continuously; the durable rows below the floor are task-d27's.

<a id="task-d27"></a>
### task-d27: Wire quorum-safe forgetting into coordd

**Prerequisites:** task-53, task-j04, task-d08, task-d26.  
**Design:** Sections 5.3, 5.4.

**Implement:** Quorum-safe floors and trim exist as libraries (task-51 to task-53), and `coordd` wires none of them: the payload, executed, dependency and proposal rows, session rows and retry records are never reclaimed, so disk grows with history and recovery reads grow with it. Wire the floor into `coordd`; trim every row below it that no voting, recovery, dependency resolution, execution or deduplication path still needs; reclaim abandoned sessions' rows and retry records by an executed command or under the activated floor, never on a local timer, since both are in the common hash task-d37 scrubs; check disk headroom before publishing a checkpoint. A voter behind the floor, which catch-up can no longer serve, stops with a named reason and waits for task-d32. Trimming stays off by default until task-d32 is in, so no voter is stopped behind a floor that nothing can bring it back from. The trim drops the execution ordinals with the identities (Section 13.1), and an ordinal is `execution_order.len()` at the moment of execution, which `CommandTable::forgotten` (task-d77) reads back as the count of executions past a command; once the trim drops ordinals that length is no count, the next ordinal would collide with a live one and `forgotten` would undercount. Give the ordinal its own counter, kept across the trim (from the review of task-d77).

**Split, from the review of its first PR.** task-d27 lands in three parts, in order. The first agrees a floor and forgets nothing: every voter exports, keeps and promises the shared checkpoint every `FLOOR_INTERVAL` executed positions, and a majority's promises activate it. It rests on work that lands first in its own PR: a third barrier space for the runtime's own batches, an applier that hands on the facts of batches it did not lower (kept only when shared), and the node reconciling them. The first part exports inline, on the serving loop, at O(common state) in reads, memory and written bytes with an fsync per chunk, so it refuses `[floor] enabled = true` at start over a measured cap of common state and stops promising once an image outgrows it; the notes record the measurement and the operator's path when a floor does not reopen. The second part's first item moves the export off the serving loop onto the pinned snapshot task-d37 builds: the snapshot is opened in `Node::execute` at the boundary, the export and the image's write run on a worker within a bound derived from the state size, and the readiness is journaled on the turn the worker reports; the cap then goes. The second part then trims below the activated floor and stops a voter behind it; the third reclaims sessions' rows and retry records. `FLOOR_INTERVAL` is a schema constant until it moves into `policy_v1` with task-d35's report and scrub intervals. The floor stays off by default until its export is off the serving loop.

**Acceptance:** A long run keeps disk bounded by the floor's distance. A voter held down past the floor stops, naming the floor and its own frontier, and serves nothing. A crash at every step of a trim leaves either the old or the new floor, never less.

**Review boundary:** Floor wiring, trim and reclamation, and, first in their own PR, the runtime barrier space, the applier's delivery of other batches' facts and the node's reconcile. No change to the floor's certificate format.

**Throughput (#98, the #173 carry):** the `service` row lost 17% of its rate over 300 s, and the register runs about 10% over 120 s, in base and head alike. A 900 s `service` run with CPU a command and the appender's and materializer's service time per 30 s reads whether cost a command rises with executed history: the executed-history set, the ordinals and the rows Section 13.1 leaves to this task. If it does, this trim is a throughput item as well as a disk one, and moves ahead of task-d84. R2 (below) found a step, not a slope, so it does not.

**R2, read locally (#174 review; [notes](../tuplesky-impl-notes.md#cost-a-command-over-executed-history-r2-for-task-d27)).** Three voters on one host, `replay`, stores on tmpfs, 750,000 operations at 10 callers, about 304,000 commands executed in 400 s. Each voter's CPU a command per 30 s window:
- **The domain loop does not grow with history.** At the leader it is 0.48 to 0.53 ms a command in every full window, first to last, and at the followers 0.39 to 0.44.
- **The process steps up once publications start, and does not slope (#174 review).** The leader's process goes from 1.345 ms a command in the first window to 1.45 to 1.48 in every later full window but the publication's (1.892), and a follower's from 1.151 to 1.27 to 1.31. So the 10 to 14% is the publications starting, the first near the end of the first window, not history slowing the path, which is task-d46's question answered for this run. Each publication's export and write rise roughly in proportion to the journal position it represents: 1.0 to 1.3 s and 0.2 to 0.3 s at 73,000 records, 9.3 to 11.0 s and 6.5 to 8.0 s at 790,000, about 16 to 26 µs a record. The rate held: 24,800 commands in the first window and 24,000 in the last full one.
- **So the batch's lever is the publication's own cost (task-d92), not this trim.** Trimming stays off by default until task-d32 is in, and task-d32 needs task-d41's chain, so the trim cannot be in the batch. It stays the long-run bound: its first reading, when it is built, is the rows the publication exports by kind at two positions, to show how much of an image is rows it reclaims (the payload, executed, dependency and proposal rows) rather than the application's keys.

<a id="task-d28"></a>
### task-d28: Let a recovery report survive a lost page

**Prerequisites:** task-25, task-d05.  
**Design:** Sections 4.9.

**Implement:** A voter publishes each recovery report page once, on a lane that drops when full, and nothing asks for a missing page, so one lost page costs the whole ballot. A regenerated report for the same ballot is refused as inconsistent. The candidate asks for the pages it lacks, a voter answers from the same report version, and the candidate's assembler is bounded across ballots.

**Acceptance:** With one page of each report dropped, the campaign completes in its ballot. Repeated interrupted campaigns leave the candidate's memory flat.

**Review boundary:** Page retransmission and assembler bounds. No change to the report format.

<a id="task-d29"></a>
### task-d29: Write the failure and obligation contract

**Prerequisites:** task-d18, task-d19, task-d22.  
**Design:** Sections 1.2, 4.8, 5.1.

**Implement:** The failure model, the transitions and the obligations they create are spread across the design, the collector specification and the notes. Write one section: tolerated failures, restart semantics, storage and corruption handling, and what restores progress; the eight transitions (receipt, admission, durable protocol state, evidence publication, decision, application, response, retirement) and which create obligations; an owner, a trigger and an escalation for each obligation; and the conditional argument that, once faults stop and admission pauses, the finite outstanding work completes. Have the harness refuse to initialize over a voter whose state is gone, as design Section 5.4 requires.

**Acceptance:** Every obligation named in the section has a test showing its owner acting after the fault it covers. The harness refuses a wiped voter. The document checks pass.

**Review boundary:** Design text and the harness refusal. No protocol change.

<a id="task-d30"></a>
### task-d30: Run the real replica machines in the deterministic simulator

**Prerequisites:** task-05, task-d18, task-d19, task-d34.  
**Design:** Sections 12.1, 12.3.

**Implement:** `coord-sim` runs only reference actors, and the multi-node tests that run `Leader` and `Follower` drop messages by hand for a few seeds. Drive the real machines in the simulator under loss, duplication, reordering, crashes and restarts at table capacity 32, for three and five voters, with a protocol oracle: one order of committed commands on every replica, promises never lowered, and every learned decision recovered with its dependencies. Failing seeds are saved as fixtures. This is what finds the next bug of task-d18's or task-d19's kind before Jepsen does; the budget and progress oracles are task-d33.

**Acceptance:** The checklist's failure-test matrix rows 1 to 4 and 10 run at both sizes under the protocol oracle and pass. The oracle finds the bugs task-d18 and task-d19 fix when their fixes are reverted.

**Review boundary:** Simulator wiring and the protocol oracle. No production code change beyond the hooks the simulator needs.

<a id="task-d31"></a>
### task-d31: Refuse epochs other than three or five voters and read the source fast set from its ballot

**Prerequisites:** task-26, task-m01.  
**Design:** Sections 1.4, 4.2.

**Implement:** The configuration validator accepts one, two and four voters, while the design allows three or five and no four-voter epoch. Recovery computes the source ballot's fast set with `c2_default`, while `verify_ballot` accepts C1 or any valid C2 set, so a ballot with another fast set would be recovered against the wrong one. Every production path builds `c2_default` today (`serve.rs`, `main.rs`, `follower.rs`), so the mismatch cannot happen yet. Refuse two and four voters. Single-voter domains are used by many `coordd` tests (`bins/coordd/tests/cli.rs`) and supported in `peers.rs` and `config.rs`, so before refusing one voter, list the tests and harness paths that start one and either keep one voter as an explicit test-only profile or move them to three. Have recovery read the fast set from the source ballot's configuration.

**Acceptance:** Two- and four-voter manifests are refused at validation, and one voter is either refused or allowed only under the named test profile, with every existing single-voter test accounted for. A recovery whose source ballot has a non-default fast set applies the possible-fast rule to that set.

**Review boundary:** Configuration validation and recovery's fast-set source. No change to the quorum policy.

<a id="task-d32"></a>
### task-d32: Reinstall a voter behind the forgetting floor as a learner

**Prerequisites:** task-50, task-d27, task-d41.  
**Design:** Sections 5.4, 17.6.

**Implement:** Once task-d27 trims below the floor, a voter whose frontier is below it cannot be served by catch-up, which pulls executed history the peers no longer hold. Such a voter is reinstalled as a learner through task-d39's staging and verification, under a prepared transition (task-d42), from a peer's current export at or above the floor, keeping nothing of its old obligations, and rejoins voting only through the path design Section 5.4 names, which is task-d41's replacement, triggered by the floor rather than by a stop.

**Acceptance:** A voter held down past the floor is reinstalled, catches up from the checkpoint and the suffix above it, and its executed rows above the checkpoint equal the leader's. A crash at every step of the reinstall leaves either the old stopped voter or the installed learner. Its old promises and acceptances are never reported.

**Review boundary:** The reinstall trigger and its wiring to task-50's install. No change to the checkpoint format or to how a learner becomes a voter.

<a id="task-d33"></a>
### task-d33: Hold the simulated domain to its budgets and to progress after healing

**Prerequisites:** task-d22, task-d24, task-d26, task-d28, task-d30.  
**Design:** Sections 12.3, 13.

**Implement:** Add to task-d30's simulator a budget oracle, which checks every limit of task-d26's resource contract at its peak, and a progress oracle, which checks that once faults stop and admission pauses every admitted command settles and every voter executes as far as the leader.

The oracles failed on the tree as it stood, as task-d30's protocol oracle did for task-d34. Their findings land first, in their own PR ahead of the oracles', each from a trace:
- Recovery selection's possible-fast rule dropped candidates a fast quorum may have decided: one whose member holds an adopted ancestor on another path, a conflicting command before one it retired, or a dependency a Sync demoted, one that follows a command no ballot decided, and one ordered after an adopted command only through another candidate. It judged candidates one at a time in identity order; each pass now judges them against the same set.
- A reclaim retired executed records in identity order, so a report's window could leave out a command executed a moment before. It retires them in the order they executed.
- A report overlaid the synchronized selection on a command this replica had since committed, or had committed by installing that selection.
- A new leader left out of its chain an entry after a re-proposed command it had committed, and never proposed what it took in after cutting its own report.
- A payload fetched for a leader's proposal, a Sync entry or a campaign's selection did not take the retry-key binding over, and a restart bound the key to whichever command sorted last.
- A leader counted a voter as following once it promised, so a voter that missed the campaign, or promised in it and lost its Sync, was never asked again. It counts a voter once it votes, and asks the others until they do.
- A superseded campaign and a promise to a leader ahead left a voter behind with nothing to fetch; a candidate with a full table refused its own selection's payloads; a waiting campaign re-assembled on every message; a Sync's release did not delete a row whose batch was in flight; and `coordd` served catch-up only at its own ballot, not at an earlier ballot of the epoch.
- `coordd`'s loop polled its callers' plane first once in 65 peer events, so a replica catching up read 2 to 7 of its callers' events a second, its collector's evidence among them, and its callers waited out their deadlines on answers already sent. Found in CI rather than by an oracle; the loop now polls it first once in 9.
- The leader re-sent every voter its first 16 unadopted proposals on every call of the 250 ms timer, so a voter that was only behind was sent the same ones four times a second, each came back as a duplicate adoption, and the leader's control lane to it filled with them; the gap between two re-sends of one proposal to one voter now doubles, up to four calls.

**Acceptance:** The checklist's failure-test matrix rows 5, 9, 12 and 14 run at three and five voters and pass, along with task-d30's rows under the new oracles. The oracles find the bugs task-d22 and task-d28 fix when their fixes are reverted, and a budget violation when task-d24's reservation or task-d26's bounds are removed. Each finding has a deterministic test that fails without its change.

**Review boundary:** The findings PR: recovery selection's possible-fast rule, the table's retirement order, reports, the new leader's chain, retry-key bindings, the leader's re-preparation of voters that have not voted and the follower's answer, the leader's proposal re-send pacing, the follower's campaign and promise handling, and `coordd`'s catch-up donor rule and its loop's plane budget. The oracles' PR: the two oracles and the scenarios they add, with no production code change beyond the hooks the simulator needs. No row or message format changes in either.

<a id="task-d34"></a>
### task-d34: Keep every decision through recovery where the protocol simulator lost one

**Prerequisites:** task-d11, task-d18, task-d19, task-d21.  
**Design:** Sections 4.1, 4.7, 4.9.

**Implement:** task-d30's simulator, running the real machines at three and five voters, found decisions that recovery lost or contradicted, each from a trace:
- The source leader's report makes the possible-fast rule skip itself. A leader's reply waits for its proposal batch, which records the command at PRE-ACCEPT. Its own ACCEPT row is a later batch, so a leader that crashes between them reports PRE-ACCEPT, and a fast decision is re-proposed. The source leader is one of the fast-set members the rule reads.
- A follower's fast acknowledgement can carry the leader's path, copied from the leader's proposal, beside its own local dependencies. Fast learning, and the rule's candidates, also require the leader's dependency set.
- task-d11 demotes only the acceptances a Sync leaves out. An acceptance it carries with other dependencies is demoted too.
- A report takes the synchronized selection's entry over a durable record that is behind it, while its installation is in flight or the Sync was superseded.
- The rule checked a candidate against conflicting adopted commands through the member's own records, so a command the member never held passed vacuously. Commands the selection orders after the candidate are exempt; an earlier ballot's decision the member may have forgotten is allowed; an at-source command the member does not hold rules the candidate out.
- A follower's ledger applies only the batches it waits on, so a batch it staged as leader and completing after it was deposed was lost to its reports.
- A replica aligned its per-key logs to the leader's order when a proposal arrived, so a fast acknowledgement's path could name a history its records did not hold. Logs align only where the replica takes the order as its own: at adoption and at a Sync's installation (the `recordLeaderHash` mapping row becomes `[EXT: stricter]`). A command pre-accepted before an alignment and left behind it keeps the head off every leader path until it is aligned itself or retired, and a new leader's log is anchored at the tails it chains after.
- The possible-fast rule dropped a candidate when its member had executed and retired an at-source command the candidate followed. It now keeps the candidate when the missing command is in its closure over the member's records, or was decided before a command in that closure the member no longer holds.
- A report left out selected entries whose installation was in flight; it now overlays the whole synchronized selection, each entry once.
- The re-proposal chain forked at a re-proposed dependency. An entry that follows a re-proposed command is re-proposed right after it, also when the new leader already committed that command, and the chain goes on after it.
- An older Sync's pending entry installed after a newer Sync was activated, at the newer ballot and over its demotion. Activating a Sync, or holding one a higher promise overtook, clears what an older one left pending.

**Acceptance:** Each item has a deterministic test that fails without its change. The protocol simulator's rows pass at 1,000 runs (100 seeds per row and size), with catch-up and without.

**Review boundary:** Recovery selection's possible-fast rule, the fast predicate's dependency check, Sync installation, reports, the follower's ledger, the per-key path log (`PathLog`, `anchor_all`) and the new leader's re-proposals (`Leader::from_recovered`). No row or message format changes.

<a id="task-d35"></a>
### task-d35: Chain every replica's execution

**Prerequisites:** task-49, task-d08, task-d12, task-d17.  
**Design:** Sections 4.7, 17.6, 17.16 (amended by this task).

**Implement:** Every replica executes the committed commands in one order, and nothing records that order in a form two replicas can compare cheaply: the result digest of each executed row says what one command answered, not what came before it or what it wrote. Keep an execution chain over the one order: `h_P = BLAKE3(domain tag ‖ version ‖ h_(P−1) ‖ P ‖ command ‖ result digest ‖ write-set digest)`.

- **The write set** is the updates the command's execution makes to the collections in the common hash (`in_common_hash()`), plus the replicated `meta_v1` fields `KV_REVISION`, `LEASE_AUTHORITY` and `RETENTION_FLOOR`, so the chain and task-d37's scrub root cover the same state. The command's own `executed_v1` row is left out, so `h_P` never hashes the row it is written into, and protocol rows are left out, since they can differ by path (task-d08 deletes a voter's own row of a pulled command). A refusal (task-d12) takes its position and contributes what it writes; an answered retry takes no position and writes nothing, so it contributes nothing. Trimming (task-d27) and physical MVCC garbage collection are not executions and are outside the chain; `Compact` is an execution, and its `RETENTION_FLOOR` write is inside it.
- **`h_0`** hashes the genesis pin and the genesis policy rows. The genesis policy is written today from node-local configuration (`write_genesis_policy`), and `sts` is optional for a peer-only voter, so correct voters can hold different `policy_v1` rows, and a voter without the trust rule may refuse a session the others establish. Move the genesis policy, or its digest, into the signed genesis manifest, refuse at `init` a voter whose configured policy differs, and confirm with a test whether a differing policy changes execution today.
- **The report interval, the scrub interval and the enforcement mode** of task-d36 and task-d37 are replicated state, not node configuration: voters compare only at positions they share, and a domain qualified in report-only mode has to turn enforcing without a new genesis, which the genesis pin never admits. The signed genesis manifest carries their initial values, and an admin-signed executed command changes them, so every voter switches at the same position and the change is in the chain. They live in `policy_v1`, which is in the common hash, so a checkpoint carries them to a learner and the scrub root covers them; the value in force at P is the one before P executes. A node-local setting of any of the three is refused at start.
- **Where it lives:** `h_P` is written in the batch that applies the command: in that command's `executed_v1` row, and in `ApplyBase` and the execution frontier, so every base guard checks that the chain continues. A restart reads the tail with the frontier.
- **Where it travels:** a shared checkpoint manifest and a backup carry the `(P, h_P)` of their boundary, and an install seeds the chain from it. A restore starts a new chain from the successor cluster's genesis, since it carries no configuration, policy, sessions or leases (`coord_checkpoint::restore`). task-d08's pages carry each row's result digest, write-set digest and `h`: the puller checks `h_Q = H(h_(Q−1) ‖ Q ‖ command ‖ r ‖ w)` before executing a pulled command, and a page that fails the check is a bad page, fetched again from another donor, not the puller's own divergence. A difference between the puller's own `h` after executing and the page's is decided by task-d36's rule, never on one donor's word; this amends task-d08's stop on a catch-up difference.
- **The manifest scales:** the shared checkpoint manifest must fit one snapshot frame, and at one descriptor per ~1 MiB chunk, each with both boundary keys, it runs out at about 6 GiB of Kine-shaped state (`ExportError::ManifestTooLarge`). Paginate the chunk descriptors under a root over descriptor pages, and state the supported state size.
- **Versioned:** the chain's encoding is a versioned format feature, admitted like the others (`feature::admit`); reports and pages carry the version, and voters on different versions do not compare.
- **Tools:** task-d17's description prints the chain's prefix, and so does the Jepsen job's `coord-jepsen-executed` (`crates/coord-jepsen` on #98's branch) once that branch carries this.

The executed row, the execution frontier and `ApplyBase`, the journal record (in `coordd`'s composition the durable record of execution), the checkpoint manifest, the backup format and the genesis manifest change. There is no production deployment yet, so old stores and artifacts are refused with a named error rather than migrated, and the Jepsen and stress harnesses start from fresh stores.

**Acceptance:** Replicas that execute one order hold equal chains at every position, across the deterministic cluster tests at three and five voters, restarts and task-d08 catch-up. One differing result, write set, refusal or order changes the chain from that position on; an answered retry leaves it unchanged. A pulled command whose result matches but whose writes differ is caught; with the write-set digest left out of the chain it is not (negative control). A page whose chain does not continue is fetched from another donor and stops nothing. A learner installed from a checkpoint continues the donor's chain. An 8 GiB Kine-shaped state exports and installs. Two voters configured with different genesis policies are refused, and so is a node-local interval or mode. An executed change of an interval or the mode takes effect at the same position on every voter, across restarts and catch-up, and a learner staged after the change scrubs at the same positions as the voters. An old-format store or artifact is refused, naming the format.

**Review boundary:** The chain's definition, where it is written and carried, what the genesis manifest binds, the command that changes the intervals and the mode, and the manifest's pagination. No change to what is executed or in what order.

<a id="task-d36"></a>
### task-d36: Compare execution chains between voters

**Prerequisites:** task-d13, task-d35.  
**Design:** Sections 5.4, 17.6 (amended by this task).

**Implement:** A replica that executes in another order, or writes something else for the same command, is found today only when its collector happens to hold half of that command's evidence (task-d06, task-d12), and the five-node stop `release-record-mismatch(c96f0e70)` could not be explained for want of anything more. No existing message can carry a comparison: follower machines ignore timers, `Committed` is the leader's and names a ballot sequence number rather than an execution position, and acknowledgements go out when a command is accepted, not when it executes. Add one.

- **Reports:** every voter sends every other voter a report of `(P, h_P)` for each P it has executed that is a multiple of the report interval in force there (task-d35), and resends its latest on a timer driven by `coordd`'s loop. The report is a new peer message kind, one per interval per peer, and each voter keeps each configured voter's reports for the last few intervals. A voter lagging past what is kept asks its peers for their durable `h` at a position it has executed.
- **The rule,** which each voter applies to itself at each reported P: only once a majority of the configured voters, itself included, report the same `h` at P does it decide. If its own `h` is that one, nothing happens. If not, it stops durably (task-d13), naming the first differing position, which it finds between the last matching report and P by asking a peer for `h` at the positions in between. A missing report is never disagreement.
- **When no majority can form:** a voter enters a durable undecided-divergence state at P, distinct from task-d13's marker, as soon as reports disagree there and no `h` can still reach a majority. Only a durably stopped voter counts as one that cannot report; a voter that has not reported, or whose report does not count at P (task-d37's floors, below), is one that could still agree. So the state is entered when every configured voter has reported and no `h` has a majority, or when the voters that can still report cannot make one. In that state the voter serves nothing and keeps reporting, and the state survives restarts until an operator clears it or disaster recovery (task-59) replaces the domain: several replicas disagree, and nothing says which are right.
- **Who counts at P:** the configured voters of the epoch that executed P, so the old voters up to a handoff's terminal position, except the forced scrub at that position, which task-d38 compares among the successors. A learner's values at or below its install position S′ are its donor's labels and never count (task-d39).
- **Alert** on the first disagreement a voter sees, whatever the rule then decides.
- **Catch-up** (task-d35) settles a difference with a donor by this rule.
- **Modes:** report-only (log and alert, no stop and no undecided state) and enforcing, held in task-d35's replicated state. A domain starts in the genesis manifest's mode, runs report-only through a qualification run, and is turned enforcing by task-d35's admin-signed command. Voters on different chain versions do not compare.

**Acceptance:** At three and five voters, in the deterministic cluster:
- a replica whose application is perturbed at one position stops once a majority has reported past it, naming that position, and no other replica stops;
- with one voter partitioned and another perturbed, no voter stops while the partitioned one is silent, and only the perturbed one stops once it reports;
- a perturbed leader is stopped by its followers' agreement;
- three disagreeing replicas enter the undecided state durably, stay in it across restarts and serve nothing;
- with one voter durably stopped and the other two disagreeing, both enter the undecided state;
- the first disagreement raises an alert;
- report-only mode stops nothing and alerts, and the executed switch to enforcing takes effect at one position on every voter;
- no voter stops across the kill, partition, restart, Sync and catch-up scenarios of task-d30.

With the comparison disabled, the same perturbation is found only through a half-held release, or not at all (negative control).

**Review boundary:** The report message, the rule, the undecided state, the modes and the stop. No change to commit or execution.

<a id="task-d37"></a>
### task-d37: Scrub the replicated state at agreed positions

**Prerequisites:** task-49, task-d13, task-d36.  
**Design:** Sections 5.3, 17.16 (amended by this task).

**Implement:** The chain says what a replica executed; it cannot say that its storage still holds it. A lost or misdirected write, or a row that rots at rest, leaves the chain intact and the state wrong until something reads the row.

- **The root:** at every position that is a multiple of the scrub interval in force there (task-d35), and at the positions task-d38 forces, `Node::execute` opens a read snapshot right after `applier.apply` returns for that position and hands it to a background thread. A position materialized by replay or by a deferred redo, which applies up to 64 records in one transaction, gets no root and counts as absent. The snapshot handle must be one that can move to that thread (`SnapshotSource::View` has no `Send` bound today). The thread computes the canonical shared-checkpoint root in one pass, with a traversal that computes only the root and keeps no chunk in memory (`export_shared` keeps them all), under export limits that are schema constants (the root depends on `chunk_target_bytes`) and a CPU and I/O budget, off the serving loop. A local checkpoint image is not used: `export_local` is itself a full logical export in one read transaction, run synchronously on the serving loop by `maintain`, and each publish reclaims the previous image.
- **The pin's cost is stated and bounded:** while the snapshot is open, redb cannot reuse pages freed after it, so the file grows by about the write volume during the scrub. The scrub's time bound is derived from the state size and the budget, and a scrub that exceeds it is abandoned and reported, never queued behind the next.
- **The floor:** the root at P covers the common state normalized to an activated floor, and the report carries that floor's subject: `(P, floor, root)`. A node records a floor's activation in its own node-private `checkpoint_v1`, at no execution position, so two correct voters can scrub P under different floors: roots are compared only between reports under the same floor. A report under another floor neither agrees nor disagrees and does not count as reported, so under task-d36's rule its voter is one that could still agree. Each voter scrubs P once, so a floor activation that straddles a scrub position leaves that position without a decision, never in the undecided state, and the next scrub position decides. Rows in the common hash are reclaimed only under an activated floor or by an executed command, never on a local timer (task-d27). MVCC history and events are already normalized to the replicated retention floor.
- **The decision:** each voter records `(P, floor, root)` durably and reports it on task-d36's path, under task-d36's rule. A missing root is absent, not different; a root that differs from a majority's under the same floor stops the voter durably; roots that disagree under one floor, where no root can still reach a majority counting every voter that could still agree, enter task-d36's undecided state.

**Acceptance:** A logically wrong but well-formed common row written directly into one voter's store, bypassing execution, stops that voter at the next scrub position, and only it. Two voters with different trimming progress under one floor report the same root, and a voter that activated a floor before scrubbing P and one that activated it after do not stop each other, neither enters the undecided state, and the next scrub position decides. A scrub skipped on one voter stops nobody. Across a long run every voter reports the same root at every scrub position. A scrub completes at 8 GiB under load within its derived bound. Execution never waits on a scrub beyond opening its snapshot, and the file's growth during a scrub is measured and within the stated bound.

**Review boundary:** Scrub positions, the snapshot root and its traversal, the floor it is compared under, the exchange and the stop. No change to the checkpoint format beyond task-d35's.

<a id="task-d38"></a>
### task-d38: Bind the handoff to the execution chain

**Prerequisites:** task-57, task-d35, task-d37, task-d39, task-d43.  
**Design:** Sections 10.3, 17.6 (amended by this task, in a design PR of its own reviewed before the implementation, since it changes the evidence behind gate G5; the same PR carries task-d42's point of no return and task-d43's terminal recovery).

**Implement:** The terminal state binds the root of a full export of the terminal common state, and a successor's install record is written only when its install receipt's root is that root (`record_install`). So every handoff reads, hashes and rewrites the whole state inside the write pause: about ten seconds per GiB of state on the estimate that motivated this task. Bind the terminal state to the execution chain at the terminal position instead, `(P, h_P)`, which task-d43's terminal recovery brings every sealed old voter to: under load the old voters have executed different prefixes when they seal, and nothing else brings them to one position. The successor set and `closure_root` stay bound field by field, and task-56's thirteen-change test is run again.

- **Install evidence:** a successor's install record is written from a continuing voter's store at P whose chain is `h_P`, or from a staged learner that task-d39 has verified and that executed the suffix to P with its own chain reaching `h_P`. A learner's `(P, h_P)` alone is its donor's label, and is not evidence.
- **The boundary check:** P is a forced scrub position of the successor epoch. Every successor, continuing voter or learner, opens its snapshot at P before it applies task-d40's epoch-boundary record at P+1, which is the only thing that changes the common state after P: continuing voters learn P only after the seal, and nothing executes between P and that record. Each successor reports its root at P under task-d36's rule among the successor's voters, and one that differs stops durably and is replaced (task-d41). Activation does not wait for these roots.
- **The exposure, stated:** a successor's state was last verified at its last completed scrub that matched a majority, if it is a continuing voter, or at its verification, if it is a learner, and not at activation. The exposure lasts until the first scrub at or after P that a majority of the successors completes.
- **Readiness** (Section 10.3.2), checked before task-d42's point of no return, after which the transition cannot be cancelled:
  - **The old majority can finish.** A majority of the old voters are live, neither stopped nor undecided, and have reported to each other within the last report interval, so after begin-sealing each can pull what it lacks (task-d44) and reach P. Their execution spread is within a stated bound, so task-d43's closure is usually a few pages. A voter behind task-d27's forgetting floor, which nothing can serve, counts as lost: sealing would turn a degraded state that can still be cancelled into a stuck one.
  - **The chain agrees.** A majority of the old voters report one `h` at the latest report position, counting the voter being replaced as unable to report. A difference visible only in the chain, such as a write later overwritten and compacted below the retention floor, passes every scrub, and would seal and then never certify (`MixedTerminal`).
  - **The learners are ready, and a majority of the successor set is healthy.** Every named learner is verified, within the gap, and has its voter-role leaf enrolled (task-d40). The ready learners and the healthy continuing voters together are a majority of the successor set, where a healthy continuing voter is live, neither stopped nor undecided, and its last completed scrub matched a majority. Health is counted over the continuing voters, not required of each, so at five voters with C lost and D stopped, replacing either one is still ready, and a stopped voter never counts. Requiring the learner keeps a transition from activating without it: at three voters, A and B alone are a majority of {A, B, C′}, and the new epoch would run at f = 0.

**Acceptance:** A handoff whose successors are two continuing voters and one staged learner activates with no full-state export or install inside the pause. The pause and the write stall across activation are both measured, at 1 GiB and at 8 GiB of state, and neither grows with the state beyond run-to-run noise. A successor whose chain at P differs is refused an install record, and an unverified learner is refused one. Replacing C at three voters with a learner installed from a voter A that has rotted: the learner's verification fails if A's export was wrong, and A is outvoted at P's scrub by B and the learner if A rotted afterwards. A learner and a continuing voter report the same root at P, each taken before the boundary command. A chain difference that no scrub shows fails readiness, and nothing is sealed. At five voters with one voter lost and another stopped, replacing either one is ready, and with a third stopped, neither is. A spread beyond the stated bound is not ready. A transition whose learner is not verified or not enrolled is not ready, even when the continuing voters alone are a majority of the successor set, and a voter behind the forgetting floor counts as lost. task-57's crash-at-every-durable-step tests pass with the new evidence.

**Review boundary:** What the terminal state and the install evidence bind, readiness, and the successor epoch's scrub at P. No change to sealing, certificate uniqueness or the activation quorum.

<a id="task-d39"></a>
### task-d39: Follow the domain as a staged learner

**Prerequisites:** task-50, task-d08, task-d37, task-d42.  
**Design:** Sections 5.4, 10.3, 17.6 (amended by this task).

**Implement:** Nothing today can bring a replica that is not a voter up to date. task-d08's donor serves only a voter synchronized at the requester's ballot (`serve_catch_up` in `coord-daemon`), a learner never receives `NewLeader` or `Sync` and so has no ballot to name, a `coordd` that is not a voter runs `Backing::Serving`, which only reads, and the checkpoint snapshot frames have no caller outside `coord-checkpoint`. And a successor staged for the voter it replaces has that voter's `ReplicaId` at a new incarnation, which nothing tells apart from the voter: `PeerProvenance` carries no role, `serve_catch_up` admits by replica alone, and `addressed()` in `coordd` sends only to the committed incarnation.

- **Identity and routing:** provenance carries the connection's bound role and incarnation. Frames on a learner-bound connection reach only the donor path, never the voting path, and replies to a learner are addressed to its bound incarnation. A donor admits a requester by (replica, incarnation, learner role) against the prepared-transition record (task-d42) read from its own store.
- **Learner mode in `coordd`:** a node started as a staged incarnation that the prepared transition names opens a new generation, fetches a current shared-checkpoint export from a donor over the transport's bulk lane, giving the snapshot frames their caller, installs it at its position S′ (task-50), and then pulls executed history from there.
- **Verification:** the learner scrubs as the voters do (task-d37) and asks the voters for the roots they recorded. It is verified at the first position at or after S′ where both its own root and a majority of the configured voters' roots exist under one floor, once its root equals theirs; any mismatch there fails it. It is not ready before that. The first scrub position alone is not enough: with one voter lost, the majority is exactly the other two, and one abandoned scrub, a replayed position with no root, or a floor mismatch there would leave the learner never ready.
- **Floors:** the install writes no floor certificate and the export leaves out `checkpoint_v1`, so a learner cannot compute its root under the voters' floor once one is active. The learner fetches the activated floor certificates from its donor, checks each against the old configuration's voters, and records them before its first scrub.
- **task-d08's contract widens:** a donor also serves the staged incarnation that the prepared transition names, with no ballot. Sealed old voters keep serving it up to the terminal position, since serving executed history is not voting.
- **Never a voter:** a learner votes on nothing and counts toward no quorum, and its reports and roots serve only its readiness, not any majority of the configured voters.
- **Size:** the supported state size is the one task-d35 states.

**Acceptance:** A learner staged under load is verified, reaches the readiness gap and stays within it. A donor refuses a learner that the prepared transition does not name, and no frame from a learner reaches the voting path. After the seal, a learner reaches the terminal position from sealed voters. A learner restarted mid-install either resumes or starts the install again, never selecting a partial generation. A learner whose root differs from the majority's at its verification position is not ready. With one voter lost, a learner still verifies across an abandoned scrub, a replayed position and a floor that activates between two scrub positions. A learner staged while a floor is active computes its root under that floor. An 8 GiB state stages.

**Review boundary:** Learner mode, identity and routing, the checkpoint transfer, verification, the floor certificates and task-d08's widened contract. No change to what a voter votes on.

<a id="task-d40"></a>
### task-d40: Install an activated configuration into the running daemon

**Prerequisites:** task-57, task-58, task-m01, task-d01, task-d39, task-d42, task-d43, task-d44.  
**Design:** Sections 4.8, 7.2, 10.3, 10.4, 20.4 (amended by this task, in a design PR of its own reviewed before the implementation, which decides what happens to the old epoch's state).

**Implement:** `coordd` starts from its genesis and never changes configuration while it runs. `PeerBinder::install` has no production caller (`coord-membership/src/binder.rs`). The machines cannot switch epoch: `ConfigurationIdentity` is fixed in the follower's configuration, the only first ballot of an epoch is the genesis ballot at start, and the store rewrites its ballot and fence into the epoch of the application base. Nothing records an activated configuration either: `ConfigurationChain::extend` needs handoff approvals signed by a majority of the previous epoch (`ActivationEvidenceV1::Handoff`), which is not task-57's activation, and nothing writes `config_v1`.

- **The epoch boundary is applied, not proposed.** `config_v1` is in the common hash, so it is inside task-d37's root, and two valid records of one epoch can hold different bytes. The old majority's handoff approvals are evidence the certificate hash does not cover (`coord-types`, `config_v1`). They differ with the majority that signed, with approvals collected again by a replacement coordinator, and with a record learned from a peer.
  - Each successor applies an epoch-boundary record at position P+1 when it installs the activated configuration. The record is derived from the certificate's covered fields and the transition's subject, which also gives it its command identity in the chain, so its bytes are identical on every successor, and there is no proposal, de-duplication or recovery path to get wrong.
  - Applying it writes those fields into `config_v1`, advances the execution frontier's configuration (which the root also hashes), completes task-d42's prepared record, and extends the chain.
  - It is an ordinary old-epoch application at P+1 that carries the epoch it enters, `enters: E+1`, a journal record change folded into task-d35's format bump. Today nothing advances the execution frontier's configuration: every frontier derivation copies it from `base.configuration` (`coord-storage`, `journaled.rs`). One frontier helper sets it to `enters`.
  - The store admits the record only when the base is the queued frontier at the certificate's old epoch, at P with chain value `h_P`; the node holds its install record and the published activation; and the updates are exactly those derived from the certificate's covered fields and the subject.
  - After it, every stamp and fence is of the new epoch with no further change: the persistence layer stamps ballots and fences with the base's epoch (`persistence.rs`), and `application_base()` reads the queued frontier.
  - Once a node knows P durably, from installing the chosen closure (task-d43) or from its install record, the store records P. The closure fixes P but not `h_P`, which is known only once the voter has executed to P, so the boundary's admission check takes `h_P` from the voter's own execution or from the certificate. From then on it admits nothing on the old-epoch base past P except the boundary record, and nothing else at P+1, and names the refusal. A late page, a pull, or a re-proposed command reaching the old machine could otherwise take P+1. The boundary's own admission check (base at P with `h_P`) could then never pass, and the node would have to be replaced; the store's refusal names the fault when it happens.
  - A proposed boundary would not hold. A client command pre-accepted at a successor can be re-proposed by the next leader's recovery ahead of it, since recovery anchors the first fresh proposal after the recovered tail (`leader.rs`). It would be stamped with the old epoch and admitted at P+1. So until it applies the boundary, a successor executes nothing past P: its old-epoch machine executes only the closure, and its new-epoch machine starts after the boundary.
  - The approvals, which task-d41's coordinator collects, are kept node-private in `checkpoint_v1`, served to a peer that asks, and read with `config_v1` when `ConfigurationChain` verifies the chain at start.
  - A node starts from the latest activated configuration rather than the genesis.
- **The protocol epoch switches after the boundary record.** Installing the activated configuration is a node-private step, recorded in `checkpoint_v1`. The node applies the boundary record, and only then rebuilds itself and its machines under the new identity, so the new epoch's ballot, fence and promise rows carry the epoch the base now has. The new epoch's first ballot is an election among the successors (task-d01). A successor that crashes between its install record and the boundary record applies the boundary at start.
- **Old state:** the design amendment decides, for each of these, what is dropped, kept or carried at the boundary, and the implementation tests each:
  - payload rows never executed;
  - the old epoch's protocol rows, including the store's ballot fence and promise rows, and the seal row;
  - the in-memory tables and outbox, and collector obligations;
  - lease authority (Section 7.2);
  - the floor lineage (`ActivatedFloorV1.configuration`);
  - task-d36 and task-d37's recorded reports, roots and undecided state;
  - `coordd`'s parked evidence and undeliverable map;
  - the renewal driver's own copy of the membership and its `votes` flag.
- **Catch-up across the switch:** a successor behind P pulls up to P under task-d44's rule, which the switched voters keep serving, and above P from the new epoch's voters.
- **The binder:** install the new membership into the running peer binder and disconnect at once a bound peer that no longer classifies as `Renewal`. The overlap an in-place key rotation allows, capped by `retire_at`, stays task-m03's.
- **Promotion:** a successor that was a learner (task-d39) enrolls a voter-role leaf for its committed key while it is staged, against task-d42's record, as part of readiness (task-d38). Renewal refuses a role change, and without the early enrollment an issuer outage at activation would leave f at 0 until the new voter re-enrolls. Promotion is then a reconnect under that leaf: the renewal driver's membership and `votes` flag are updated, and `coordd` switches from learner mode to voter mode.
- **Recovery:** a successor that crashes between the published activation and its own install learns the new epoch from a peer. Frontends and the Rust collector, which has no epoch refresh today, follow the new epoch.

- **Replicated features at promotion (task-d99, #196 review).** A joiner holds no feature rows, since images carry no `checkpoint_v1`:
  - promotion calls `coord_checkpoint::feature::promotion_missing` and completes only after the joiner's `FeatureHeld`. task-d99's driver re-sends only to configured voters, so that ordering is this task's;
  - `coord_daemon::feature::Features` takes its `EpochVoters` once, in `open`. At an epoch change it needs the new voters: abandon a prepare in flight, and clear the advertisements and the held set. Until then it drops every message from a joiner as not from a voter.

**Acceptance:** After an activation that replaces C by D, every continuing voter refuses C's old incarnation and admits D without a restart, D votes as a voter, the collector routes to D, and the new epoch elects its first leader. Every successor's position P+1 is the epoch-boundary record, with identical bytes, and a client command pre-accepted at a successor before the switch executes after it, never before. A non-boundary application offered at P+1, after the closure is installed and after the install record, is refused by name. A boundary record whose base, chain value or updates differ from the certificate's is refused, and after the boundary every new stamp and fence carries the new epoch. `config_v1` is byte-identical on every successor, including one whose approvals came from a different majority, were collected again by a replacement coordinator, or were learned from a peer, and the first scrub after activation stops nobody. A restart comes up in the activated configuration and verifies its chain. A successor that crashes before its install learns the epoch from a peer, and one that crashes between its install and the boundary record applies the boundary at start. A successor behind P at the seal reaches P and then the new epoch. D's leaf renews as a voter's after promotion, and under an issuer outage promotion is still only a reconnect. The old epoch's state is dropped, kept or carried as the design amendment decides, each with a test.

**Review boundary:** Installing an activated configuration into running nodes, the epoch-boundary record, the store's admission of it and its refusal of anything else past P, and promoting a learner. No change to the handoff rules or to credential classification.

<a id="task-d41"></a>
### task-d41: Replace one voter through the sealed handoff

**Prerequisites:** task-59, task-d13, task-d38, task-d39, task-d40, task-d42, task-d43.  
**Design:** Sections 5.4, 10.3.

**Implement:** A voter whose store is lost, corrupt or diverged cannot restart as itself (Section 5.4), and nothing replaces it today. The coordinator is an operator-run, restartable `coordd` command whose stage comes from durable evidence through task-54's `resume`, with the prepared transition as its authorization. It drives these steps:
1. an operator records the prepared transition (task-d42), naming the voter to replace and its successor incarnation, and the successor is staged (task-d39);
2. once readiness holds (task-d38), the coordinator executes task-d42's point of no return and seals the old configuration;
3. it drives terminal recovery (task-d43), collects the terminal reports and publishes the certificate;
4. it collects the old majority's handoff approvals, has a majority of the successor record its install, and activates;
5. each successor installs the new configuration and applies the epoch-boundary record at P+1, which completes the prepared record (task-d40).

Before step 2 the operator can cancel; after it the transition completes with the healthy majority readiness counted, and a successor that fails later is replaced by the next transition. Write the runbook in `docs/operations`, including the cases where more than f voters are stopped or lost, or voters are in task-d36's undecided state: sealing needs an old majority that agrees, so those cases are disaster recovery (a restore as a new cluster, task-59), not replacement. A node stopped by task-d13, task-d36 or task-d37 is replaced this way, task-d32 reuses the path for a voter behind the forgetting floor, and task-m03 builds on it.

- **Replicated features at the handoff (task-d99, #196 review).** The staged generation gets the voter's own feature rows through `carry_feature_rows`, and selection passes them to `select_installed`, which refuses a staging that lacks them. Read those rows from the active generation only once the feature driver's last batch is durable and materialized: a record installed after they were read would be missed.

**Acceptance:** A stress run and a Jepsen scenario each wipe a voter's store under load and replace it, at three and at five voters, with no anomaly, final reads on every node, and the pause and the write stall across activation measured and reported. The replaced incarnation, restarted on its old store, is refused by every voter. A coordinator killed after each durable step resumes to the same successor, and one killed between the point of no return and the first seal resumes to sealing, never to stable. A successor that never reaches readiness leaves the old configuration serving, and cancelling the prepared transition returns it to stable; a cancellation after the point of no return is refused, and a stale coordinator's seal request for a cancelled transition is refused by every voter. With more than f voters stopped, or voters undecided, the command refuses to seal and names the runbook's disaster-recovery section.

**Review boundary:** Orchestration and the runbook. No change to the handoff rules of task-54 to task-57 beyond task-d38's evidence, task-d42's point of no return, task-d43's terminal recovery and task-d44's serving rule.

<a id="task-d42"></a>
### task-d42: Record an authorized, prepared transition

**Prerequisites:** task-54, task-55, task-m01.  
**Design:** Section 10.3 (amended in task-d38's design PR).

**Implement:** The design's first handoff state, "Preparing: authorize successor and stage replicas" (Section 10.3.2), has no record. task-m01 defines only activated configurations. The handoff's one durable record is the seal row (`SealRecordV1` in `protocol_v1`); cancel stances and their tombstones exist only in task-54's pure model. A `Transition` holds only its epochs and a subject digest. `Evidence.authorized`, which task-54's `resume` reads, has no row behind it. Successor incarnations are first named in the terminal state, after the seal. So a donor has nothing to admit a staged learner against, and a coordinator nothing durable to give `resume` as its authorization. Record the transition's lifecycle in admin-signed executed commands, so every voter holds it in its own store, in the domain's order and in the chain:

- **Prepare** names the old and the successor epoch, the successor incarnations and keys, and a nonce the admin chooses, so a transition retried after a cancel is a new Prepare with a new subject. Its digest is the transition's subject, which later seal stances reference, and it is `resume`'s authorization (`Evidence.authorized`). It is refused unless its old epoch is the configuration's current epoch, and while another transition is open: the domain permits one transition at a time.
- **Begin sealing** is the point of no return, executed once readiness holds (task-d38). A voter seals for a transition only once it has executed that transition's begin-sealing command: the node checks its own store before `BallotState::seal`, which today refuses only another transition, the wrong epoch or a voter that is not voting. A seal request for any other transition, a cancelled one or one not yet begun is refused, and a voter refused as not begun pulls what it lacks (task-d44).
- **Cancel** closes a prepared transition, and is refused once begin-sealing has executed.
- **Completion:** the epoch-boundary record that each successor applies at P+1 (task-d40) completes the record, which frees the domain for the next transition.
- **Kept, and in the common hash:** the record lives in `config_v1`, beside the activated records, so it reaches a learner's checkpoint, the chain's write set and the scrub root. A completed or cancelled record is kept, and every lifecycle command naming its subject is refused, so an admin-signed Prepare replayed after its retry record is compacted cannot reopen the transition and block the domain.

A cancel cannot be an executed command without this ordering. A seal is a per-voter row in `protocol_v1`, outside the common hash and the chain's write set, so executing a cancel cannot read it, and there is no durable cancel stance for a seal to refuse against. A stale coordinator could then seal a majority after every voter had executed the cancel, or a cancel committed at a majority could be overtaken by a seal before either voter executes it. Begin-sealing and cancel are both commands in the one execution order, so exactly one of them comes first on every voter. Every seal follows an executed begin-sealing, so no voter both seals a transition and holds it cancelled. A minority seal of a cancelled transition, which would block the voter's own replacement (`FencedByAnother`), cannot form.

- **`resume`** reads an executed cancel as the cancellation and returns `Stable`, and reads an executed begin-sealing as `Sealing` even before any stance, never returning to `Stable` after it. The daemon writes no per-voter cancel stances.
- **The model:** task-54's stance scripts gain the executed begin-sealing and cancel, and the model is run again: no script seals a cancelled transition or leaves a minority fence of one.

**Acceptance:** A prepared transition, once executed, is held by every voter that executes it, across restarts. A second transition while one is open is refused, and one after the previous one completed or was cancelled is admitted; `resume` for it is not fenced by the previous transition's seal rows (`FencedByAnother`). Seal stances name its subject, and `resume` reads its authorization from it. A cancellation before begin-sealing returns the domain to stable, and every donor then refuses the staged learner. A cancellation after begin-sealing is refused, and so is begin-sealing after a cancellation. A voter refuses to seal a transition it has not executed begin-sealing for, including a stale coordinator's retry after a cancel. A Prepare naming an epoch other than the current one is refused, and so is every lifecycle command naming a completed or cancelled subject, including a replay of its Prepare after its retry record is compacted. The same transition retried after a cancel with a new nonce is admitted. The cancel committed at a majority and the seal requested before either voter executes it resolve to one outcome on every voter. A record not signed by the admin key is refused.

**Review boundary:** The record, its commands, their admission checks, the seal's check against the executed record, and what reads it. No change to certificates or activation.

<a id="task-d43"></a>
### task-d43: Recover the terminal closure after the seal

**Prerequisites:** task-55, task-56, task-d05, task-d11, task-d14, task-d44.  
**Design:** Sections 4.8, 10.3.2 (amended in task-d38's design PR).

**Implement:** Nothing after the seal brings the old voters to one terminal position. Under load they have executed different prefixes when they seal:
- `announce_committed` requires a leader that is leading, which a fenced leader is not, so a follower that missed acknowledgements never commits what the leader executed;
- no ballot can be promised after a seal (`BallotState` refuses it), so there is no terminal Sync;
- `Sealed` carries no recovery report (`coord-consensus/src/messages.rs`).

task-56's `closure_root` takes a `SyncDecision` that nothing produces or installs. Terminal recovery is Section 10.3.2's and task-55's, and no code does it. Add it:

- **Seal reports** carry the machine's recovery report, including its pending Sync entries, not the bare ledger's. It is built at the seal cut, which task-55 already makes complete before the report is published, and bounded as task-d05 bounds a campaign's. Each report also carries the reporter's executed position and chain value `(P_r, h_r)`, and nothing more of its executed history, so reports stay small. A prepare answer carries the reporter's current `(P_r, h_r)`, while the recovery-report entries stay at the seal cut. That is safe, since anything a sealed voter executes after its cut is committed history, checked against the segment's chain, and it lets a reporter that pulls (below) come within the limit in its next answer.
- **The executed segment:** after phase 1 the coordinator fetches, from the bound reporter (the one with the highest `P_r` in the majority read), its executed sequence over `(P_low, P_bound]`, where `P_low` is the majority's lowest `P_r`. For each position it carries the command, its admission digest, its decided dependencies and `h`.
  - The pages reuse the recovery report's paging (`ReportAssembler`, `MAX_REPORT_PAGES`) and are checked against the chain, `h_Q = H(h_(Q−1) ‖ Q ‖ …)`, from `h_low` to `h_bound`.
  - Every other reporter's `h_r` is checked against the segment at its `P_r`, and a mismatch is a task-d36 divergence, not a tie to break.
  - Without the segment the closure would depend on one node's executed history. At five voters with E lost, A commits and executes X1..Xk, which B and C accepted, and its frontier messages to B, C and D are lost. The closure is chosen at {A, B, C}, and then A crashes for good. Nobody else executed those commands, task-d44 serves only executed history, and every later coordinator must propose the same value. Every commit quorum is a majority that includes the leader (`quorum.rs`), and a vote counts only once its payload and dependency rows are durable (`follower.rs`). So a surviving reporter holds each such command's payload; what was missing is the decided order, which the segment carries.
- **The selection's input:** the rule a Sync uses (`recovery::select_with`) runs over the majority's seal reports.
  - Its single-ballot check exists to know that no reporter will accept anything more. A seal is that promise for every ballot of the configuration, so here the check is that every report is a seal report of this transition.
  - Reports are built from ledger records, which lose commands once they are executed and reclaimed. A campaign covers that gap with the candidate's executed tail and its `supplied` answer, and a coordinator has neither; the segment is that input. A command in the segment, or at or below `P_low`, counts as supplied. So a COMMIT entry whose payload its holder has forgotten is not `HalfInitialized`: a campaign would set that report aside (`campaign.rs`), and a bare majority has none to spare.
- **The closure is the decision, carried by reference.** The segment spans the execution gap between the slowest and the fastest reporter read, and at five voters a voter outside every commit quorum can lag without limit. A chosen value has to fit an accepted row (`MAX_ENVELOPE_PAYLOAD`, about 2 MiB) and a prepare answer (a `ProtocolEvidence` frame, 4 MiB). At 100 to 150 bytes an entry, that holds a lag of only tens of thousands of positions. After begin-sealing, a value that cannot be accepted cannot be refused either, unlike a campaign that task-d20 refuses, so the closure is never carried inline. The value the coordinator proposes holds:
  - `(P_low, h_low)`, `(P_bound, h_bound)`, and a root over the closure's pages;
  - the pages themselves: the executed segment, then every selected entry with its dependencies and the commands the rule would re-propose, chained after the selection's tail in a canonical order, each entry with the reporters that hold its payload.

  Handling of the pages:
  - **Storage:** an acceptor fetches the pages and stores them durably, checked against the root and the chain, before it writes its accepted row. So the closure survives the loss of the bound reporter and of the coordinator. A later coordinator that adopts an accepted value fetches its pages from any acceptor of that value. If none can be reached, it prepares again at a higher ballot with another majority: a value held only by an unreachable minority was not chosen, so the fresh majority may propose anew, and a coordinator never waits on a single node.
  - **Commitment:** every entry installs as committed, whatever its phase at the reporters. A Sync install commits only entries already at COMMIT, and the rest wait for a leader's re-proposal, which terminal recovery does not have, so under load a command in flight at the seal would otherwise keep every voter short of the end.
  - **The end:** P is the closure's end.
  - **The spread's limit:** `MAX_REPORT_PAGES` bounds the spread the pages admit. A reporter further behind than that first pulls through task-d44 until its report comes within the limit, rather than the handoff failing. Readiness (task-d38) keeps the usual spread to a few pages.
- **Single-decree Paxos over the closure,** under handoff ballots `(round, coordinator incarnation)`, separate from the configuration's ballots, which the seal keeps refusing:
  - a prepare at `h` writes a durable promise row and returns the voter's accepted `(h′, S′)`, if any, as the value by reference, and its seal report;
  - the coordinator proposes the accepted value with the highest `h′` among a majority's answers, or else a new closure computed from that majority's seal reports and the fetched segment;
  - an accept at `h` succeeds only if `h` is at least the voter's promise and the voter holds every page of the value, and writes a durable accepted row;
  - the rows and the pages live in `protocol_v1`, keyed by the transition;
  - a closure is chosen once a majority has accepted it at one `h`, and a voter installs only a closure it knows was chosen.
- **Install and execute:** a voter behind the majority's lowest `P_r` first pulls up to it (task-d44). It then executes the closure from its own position:
  - first the executed segment from the stored pages, taking each payload from its own ledger or from a named holder (`serve_payloads` already serves a payload to any voter that asks);
  - then the rest (task-d14's facts, and task-d11's demotion of what the closure did not select), to P;
  - only then does it make its terminal report (task-56). So the report's `closure_root` is the chosen closure's, and its `(P, h_P)` (task-d38) is what the voter executed.
- **A voter that executed past its report's position:** a voter outside the majority, or a reporter that kept executing commits it learned after its cut (sealed voters do), can have executed past the bound. Each such command was committed by a majority quorum, which intersects the majority read, so the selection covers it with the same decided dependencies, and the voter's executed suffix is a prefix of the closure. Such a voter checks `h` at every position it has already executed against the closure's, skips those positions, and executes the rest. A mismatch is a task-d36 divergence.

**Acceptance:**
- At five voters, E is lost. A commits and executes X1..Xk, which B and C accepted, its frontier messages to B, C and D are lost, the closure is chosen at {A, B, C}, and A crashes for good. B, C and D execute the closure to one P from their stored pages, taking X1..Xk's payloads from the named holders, and report one terminal root.
- At five voters, with E lost and D far behind in the majority read, the closure's pages carry D's whole lag, and the chosen value still fits one accepted row and one prepare answer. The worst case the pages admit is measured, as task-d20 measures its largest Sync. A reporter further behind than that pulls first, and the handoff completes.
- An acceptor that does not hold every page refuses to accept, and pages that fail the root or the chain are refused.
- At three voters, with C wiped and B lagging at the seal under load, A has executed and reclaimed commands B never received. B executes them from the closure's executed segment, both reach one P, and they report one terminal root. At five voters, the same holds with two voters lagging.
- At three voters with C being replaced, B holds a COMMIT entry for X without its payload, and A executed X long ago. The selection succeeds, and B takes X's payload from A.
- A command accepted but not committed at the seal ends committed in the closure, and every voter executes to P.
- A voter outside the majority that executed past the bound skips what it executed, and one whose `h` differs from the closure's at a position it executed is stopped as a task-d36 divergence. Two reports at one position with different `h` are a divergence, not a tie.
- At five voters, two coordinators reading different majorities, one of which holds a possible fast decision, choose one closure, and the voters of both majorities install that one.
- A coordinator killed once the closure is in one voter's accepted row, and a second coordinator reading another majority, lead to one chosen closure and one root.
- A command decided at a minority of acceptors before the seal is in the closure.
- A coordinator adopting a value whose answering acceptor becomes unreachable fetches the pages from another acceptor of that value, or prepares again with another majority.
- A reporter that pulls after its seal reports its current position in its next prepare answer.
- A sealed voter restarted mid-install resumes from its durable promise and accepted rows and its stored pages.
- A sealed voter never promises a ballot of the configuration.

**Review boundary:** The seal report's contents, fetching and paging the executed segment, the selection's input rule, the closure as a decision carried by reference with its pages and payload holders, the handoff Paxos with its rows and stored pages, and installing and executing the closure. No change to what the seal fences or to the certificate's selection over terminal roots.

<a id="task-d44"></a>
### task-d44: Serve executed history across a transition

**Prerequisites:** task-d08, task-d35, task-d39, task-d42.  
**Design:** Sections 5.4, 10.3.2 (amended in task-d38's design PR).

**Implement:** Once one voter seals, the old configuration decides nothing new, so a voter behind begin-sealing or behind the terminal position can learn them only from another node's executed history. Today nothing serves it:
- a sealed follower returns before acknowledging a proposal (`on_proposal` in `coord-consensus`), so a leader behind begin-sealing gets no answer to its re-sent proposals;
- a leader never pulls (`Machine::request_catch_up` in `coord-daemon`), and cannot seal before it executes begin-sealing;
- task-d08 answers only a requester synchronized at the donor's ballot (`synchronized_at`), so two voters at different ballots never serve each other;
- after the switch, the continuing voters are no longer old voters at any ballot, and a voter that never sealed can be behind P with nobody to ask.

Add one rule: a node that has executed a transition's begin-sealing serves the old epoch's executed history it holds to every old voter and every named successor of that transition, sealed or not, with no ballot.
- **Identity:** a requester is admitted by (replica, incarnation) against the prepared record, and replies go to its bound incarnation, as task-d39 routes a learner's. `serve_catch_up` admits by `ReplicaId` alone and replies to incarnation `ZERO`, which `addressed()` resolves to the committed incarnation (`coord-daemon`), so a successor that shares the replaced voter's `ReplicaId` would never get its pages. An old voter that is not a successor is served only until activation, after which task-d40's binder refuses it and the replaced incarnation is refused everywhere (task-d41).
- **Who takes part:** a node stopped by task-d13, undecided (task-d36) or diverged neither serves, nor pulls, nor makes a terminal report. The requester checks its own state before it asks.
- **What it serves:** up to the terminal position once task-d43 has chosen it. Pages are tagged by the transition's subject and checked against the chain as task-d35 checks them.
- **Pulling:** a leader behind begin-sealing steps down and pulls as a follower, rather than pulling while it holds vote sets, proposals and speculation for commands the pull retires. A seal refused because the voter has not executed begin-sealing makes that voter pull.
- **After the switch:** serving continues, to successors only, until a floor activated in the new epoch passes the terminal position; task-d40's design amendment carries the floor lineage across the boundary. task-d32 then reinstalls a voter still behind it.

Serving is not voting: nothing here counts toward a quorum.

**Acceptance:**
- At three voters with C lost, B leads, and A executes begin-sealing on the fast path. A's acknowledgement to B is dropped, and A is sealed. B steps down, pulls begin-sealing from A, executes it, and seals.
- Two voters synchronized at different ballots serve each other once either has executed begin-sealing.
- At five voters with E replaced, a D that never sealed and is behind P at activation reaches P from the switched voters.
- A successor that shares the replaced voter's `ReplicaId` at a new incarnation receives its pages, and the replaced incarnation receives none. After activation, an old voter that is not a successor is refused.
- A node that has not executed begin-sealing serves nothing under this rule, and a requester that is neither an old voter nor a named successor is refused. A stopped, undecided or diverged node neither serves nor pulls.
- Once a floor activated in the new epoch passes P, serving under this rule stops and task-d32's reinstall takes over.

**Review boundary:** The serving rule, its admission by incarnation and routing, who takes part, the leader's step-down and pull, and the pull on a seal refused as not begun. No change to what is decided or to how a voter votes.

<a id="task-q01"></a>
### task-q01: Produce the combined durable WAN/Kine qualification report

**Prerequisites:** task-j07, task-j08, task-o06, task-m05, task-63, task-64, task-d45, task-d46, task-d47, task-d48, task-d49, task-d51, task-d52, task-d53.  
**Design:** Sections 14.3, 21, 23.1.

**Implement:** Fixed/changing membership, realistic Kine object churn, replicated native leases, current/historical reads, observer watches, snapshots and actual auth. Collect complete build/config/source identifiers, raw measurements, model/trace coverage, history checks, limits and supported deployments. Include all upstream-issue schedules and post-completion observer source failover.

**Acceptance:** Strict shared-journal profile meets combined gate before task-66. Measure real cross-group sync amortization and event offload, not assumed improvement. State exact Kine pin/fork. ReadFence only enabled with its gate; replay profile absent/disabled unless task-j06 accepted and tested in applicable matrix. Retain independent redb/Fjall experimental distinction. Local document checks are not release evidence.

Require the named 2-2-1 region-loss schedules and privileged API-server/Kine edge tests, not only a generic single-node-failure test or successful Kubernetes boot.

**Review boundary:** Evidence/release review, not changing semantics to hide faults, suppressing failing schedules or folding correctness fixes into an omnibus report.

<a id="task-d45"></a>
### task-d45: Measure a command's cost on every node, and gate on it

**Prerequisites:** task-61, task-j08.  
**Design:** Sections 4.6, 17.3.3, 22.3.

**Implement:** The first unthrottled Jepsen runs (#98) served 23.8 `ok` a second where etcd served 1252.7 and SwiftPaxos 1670.3 on the same runners, and nothing `coordd` prints says why. Its metrics snapshot is printed at start, before anything is recorded, and when serving ends, which a killed daemon never reaches. The raft-engine journal counts its syncs (`WriteStats.syncs`), but nothing reads the count. `Journal.completed` misses the lowerings that `apply` and reconcile run under `Materialization`. And the domain thread's busy time is not measured at all. The cost had to be read off `strace` and `gdb` instead: about five lowerings and fifteen `fdatasync`s per command on every node, and a follower domain thread at 99% with a single client.

- **Periodically:** print the snapshot on an interval (default 10 s) as well as at the end. Add to it:
  - lowerings, counted wherever a lowering runs;
  - journal syncs and projection commits;
  - commands executed;
  - the domain thread's busy time over the interval.
- **In the harnesses:** `jepsen_summary.py` and `shim-stress.py` report, for each voter, lowerings, syncs and busy time per executed command, and the busy fraction over the run.
- **A gate:** a CI job runs a fault-free three-voter throughput run on tmpfs, at one client and at ten, three times each, over a fixed number of operations rather than a fixed time (a command's cost grows with the history before it, and at 60 s's worth of operations today one voter falls behind). It fails when the median over the repeats of the busiest voter's busy time per command, over the run or over the run's last quarter of commands, or of its syncs per command, regresses past a stated margin from a baseline recorded in the repository. It reports completed commands a second but does not gate on them: on shared runners throughput varies by more than a regression worth catching. A PR that moves the baseline says why.

**Acceptance:**
- A daemon killed with SIGKILL leaves its last interval's counters in its log.
- On today's code the summary reports the per-command lowerings and syncs that `strace` counts, within 10%.
- The gate fails when a per-turn scan of every held command is added to the domain loop, and passes without it (negative control).

**Review boundary:** Observability, the harness summaries and the CI job. No protocol or storage change.

<a id="task-d46"></a>
### task-d46: Keep per-turn and per-event work independent of history

**Prerequisites:** task-d06, task-d24, task-d27, task-d30, task-d45.  
**Design:** Sections 4.6, 4.7.

**Implement:** With one client, on loopback and on tmpfs, throughput fell from 101 to 36 commands a second over 90 s. Over the same 90 s a follower's domain thread went from 50% to 99% busy, about 5 ms to about 27 ms of CPU per command; the leader's stayed near 50%. Stack samples of that thread put it in four places:
- `Follower::missing_payloads`, which `coordd` calls twice per turn (`serve.rs`) only to test whether anything is missing, and which chains every key of `held`, `sync_pending` and `adopted` into a vector it sorts and deduplicates;
- `CommandTable::phase_of` under `Learner::commit_learned`, which walks every vote set until nothing more commits;
- `Follower::learn`;
- `Follower::awaits_rebind`, once per held command.

These maps keep executed entries until the ledger passes capacity × `HISTORY_SWEEP`, so each pass costs the history, not the change. The leader has the same shape: `on_storage` finds a proposal by barrier with a linear scan, `advance_pending` repeats over every proposal, `unexecuted_in_order` filters and sorts every proposal on each vote and apply, and `resend_unvoted` sorts every durable proposal each tick.

Make each of them O(change) or O(log n):
- keep the set of commands that lack a payload or await a rebind as it changes, and answer `coordd`'s per-turn check in constant time;
- drive `commit_learned` from the commands whose votes or phase moved, in chain order, and have `next_executable` read the next position;
- index proposals by barrier and keep `advance_pending` incremental;
- skip `unexecuted_in_order` while speculation is off, and keep the order when it is on;
- keep proposals awaiting a re-send in order of their last send;
- retire executed history continuously, a bounded amount per execution, inside the retirement window task-d27 activates. This is the in-memory history only: the `ledger`, `votes`, `proposals`, `held` and `adopted` entries above the floor, and the executed-history set and per-key tombstones task-d26 leaves unbounded. A durable row is never retired here; trimming those below the floor stays task-d27's. An undecided record keeps its slot (task-d24), and task-d06's chain stays total across retirement.

**Acceptance:** The runs below are measured with local checkpoints off (`limits.checkpoint_after_records = 0`). With them on, `export_local` traverses the whole projection on the domain thread at every publication, for a time that grows with the projection; moving it off that thread is task-d51's, not this task's.
- In task-d45's run, a follower's busy time per command at 280 s is within 20% of its value at 20 s, at one client and at ten (today it grows four- to five-fold).
- Completed commands per 30 s over a 300 s run show no downward trend beyond the run-to-run spread.
- The tmpfs ceiling at ten clients at least doubles.
- The deterministic cluster tests, task-d30's protocol simulator with `PROTOCOL_SIM_SEEDS=100` (its default is 12 seeds) and task-d24's crash tests (a leader that crashes right after a capped Sync, and the campaign after it that completes at the largest table) pass unchanged, and the simulator's decisions match the current code's on the same seeds.

**Review boundary:** Data structures and call sites in the consensus machines and `coordd`'s loop. No decision, dependency or execution order changes, and no durable row is removed.

<a id="task-d47"></a>
### task-d47: Lower a turn's transitions as durable groups

**Prerequisites:** task-j03, task-j05, task-j08, task-d24, task-d30, task-d45, task-d46.  
**Design:** Sections 17.3, 17.3.3 (already requiring this).

**Implement:** Section 17.3.3 asks for bounded group writes, initially 64 transitions or 256 KiB with no idle timer. `coordd` lowers one domain's queued batches one at a time instead (`node.rs`, `journaled.rs`). Each batch costs:
- one raft-engine write with `sync = true`;
- one redb transaction committed with `Durability::Immediate` and two-phase commit, which is two more `fdatasync`s.

The application batch of an executed command is lowered on its own as well. Measured with `strace`, that is about 5 lowerings and 15 `fdatasync`s per command on every node, all on the domain thread. On a runner's disk, at 1 to 2 ms a sync, that alone caps a node near 35 to 65 commands a second, which is the Jepsen ceiling.

- **Lower a turn's ready batches as groups** within 17.3.3's bounds. Each group is one journal write, synced once, then one projection transaction. The application batch rides in its command's group.
- **Keep the barriers per batch:** `JournalDurable` and `Materialized` still name each batch. The outbox still releases a message only after the batch it depends on is durable. A vote is still justified by final durable state, never by an intermediate update overwritten within the group (17.3.3).
- **Release frames per group:** frames released by a group's durability go out when that group is durable, not at the end of a turn that may hold 32 submissions and every ready execution.

**Acceptance:**
- **Syncs:**
  - at ten clients, journal syncs per command per node fall from about 5 to at most 1, and projection commits likewise;
  - at one client, where there is nothing to group, the count does not rise.
- **Throughput:** completed commands a second on disk at ten clients rise at least three-fold over task-d46's.
- **Crash safety:**
  - a crash between a group's journal sync and its projection commit recovers every batch in the group from the journal (task-j05's fault points);
  - moving a release ahead of its batch's sync fails the barrier tests (negative control).
- **Regression:** task-d24's crash tests (as in task-d46) and task-d30's simulator pass.

**Review boundary:** The lowering loop, the journal group, the projection transaction and when frames are released. No change to what is durable before which message.

<a id="task-d48"></a>
### task-d48: Commit the projection in one phase under the journal

**Prerequisites:** task-53, task-59, task-j04, task-j05, task-d47.  
**Design:** Sections 17.3, 17.3.4 (amended by this task).

**Implement:** In `coordd`'s composition the journal is the durable record (task-j03), yet every materialization commits redb with `Durability::Immediate` and two-phase commit, two `fdatasync`s for each group task-d47 lowers. Section 17.3.4 selects Immediate and local two-phase hardening "from the reviewed redb contract" and asks for recovery to be measured.

- **Commit in one phase, with checksums.** Amend 17.3.4's hardening line: the projection stays durable at every commit, committed in one phase with redb's checksums, and the amendment states why that is safe with the journal underneath. A commit torn by a crash rolls back to the one before it, and start-up re-lowers the journal above it. The amendment also states the residual: one-phase commit relies on the commit slot's checksum to detect a torn write, which two-phase commit does not need.
- **Measure recovery** under one-phase and two-phase commit, and state what each costs in recovery time and in syncs.
- **Write out the invariants** the current code depends on, which any later change to the projection's durability must keep:
  - the collector answers a resolve from the projection (`retained_answer` in `coordd`, `settle_from_record` in the collector), so a command the journal holds is never answered `Unknown` or `Forgotten`;
  - the divergence tooling reads `executed_v1` from the projection.

A projection committed non-durably and made durable only at checkpoints is not this task. It is task-j06's `journaled-replay` profile (17.3.4), which the gate checklist fences: it needs task-j06's qualification and cannot relax durable materialization silently. If task-d45's numbers after this task show the remaining projection sync still bounds throughput, the path is to promote task-j06 from optional. Under that profile a node replays the journal above the projection's durable mark before it serves or answers a resolve.

**Acceptance:**
- Projection syncs per group fall from two to one, and recovery time under one-phase commit is reported beside two-phase.
- SIGKILL at random points in task-09's and task-j05's disk-fault runs recovers to the journal's state, and no resolve after the restart answers `Unknown` or `Forgotten` for a command the journal holds.
- A projection commit torn at task-j05's fault points is rolled back and re-lowered from the journal. This is the test of the checksum residual the amendment states.
- Checkpoints (task-53, task-j04) and backups still cover a durable projection.

**Review boundary:** The projection's commit mode, its 17.3.4 amendment and the recovery measurement. No non-durable projection, which stays task-j06's.

<a id="task-d49"></a>
### task-d49: Re-send a proposal only once its answer is due

**Prerequisites:** task-d07, task-d08, task-d15, task-d45.  
**Design:** Section 4.6.

**Implement:** With one client, a leader refused at least 8,192 duplicate votes in 100 s for about 4,000 commands, about two a command. At one client a command takes about 20 ms, so no proposal is 250 ms old at a tick, and the re-send's missing age check is not what sends them. The window is. `resend_unvoted` counts a voter as having voted only by its slow adoption (`VoteSet::adopted_by` reads only the slow votes). It keeps every proposal that voter has not adopted whose seqnum is above its highest adoption, or whose phase is below `Commit`. A follower's fast acknowledgement does not count. So a command decided on the fast path, or learned from the leader's commit frontier before the follower adopted it, stays in the window until it leaves the maps. Each re-send reaches a follower that already holds the proposal, the follower answers with the vote it already gave, and the leader refuses that vote. Each duplicate is another step on a follower's domain thread, which is already the bottleneck (task-d46).

In this order:
- **Re-send only what the leader still needs from that voter:** a proposal that is not decided and that the voter has not acknowledged, on the fast path or the slow, with task-d15's rule for the votes the leader still needs.
- **Gate every re-send on age:** re-send a proposal only once it is older than the re-send interval since its last send, the first re-send included.
- **Make the interval adaptive:** scale it to the observed vote latency, a smoothed high percentile with today's 250 ms as the floor.
- **Make a duplicate cheap:** refusing a duplicate vote is constant-time on both sides.
- **Count re-sends by reason** in task-d45's snapshot: lost (the voter lacked the proposal), late (its vote arrived after the re-send), already acknowledged and already decided. The duplicate threshold alone cannot tell a wrong fix from a right one.
- **Hand a voter that is far behind to catch-up.** State the threshold at which the leader stops re-sending a proposal the voter never acknowledged and lets task-d08's catch-up from executed history carry it. Re-sends to a voter that is already behind add load to it: one slow pass fills the control lane, its peers drop frames, and each re-send meets the same full lane (task-d46's notes, a voter that stopped executing).

This is liveness as well as cost. The window takes `RESEND_PER_VOTER` (16) proposals a voter. Filled with proposals that voter already acknowledged, it starves the one that was really lost, which breaks task-d07's guarantee. task-d07's and task-d15's guarantees hold: every proposal a voter lacks, and every vote the leader still needs, is asked for again.

**Acceptance:**
- Duplicate-vote refusals stay below 5% of commands at one, ten and fifty clients (today at least 200% at one client).
- Re-sends counted as already acknowledged or already decided are zero in a fault-free run.
- A proposal lost in transit is re-sent within twice the interval, including when 16 or more proposals the voter acknowledged sit in its window. Counting only slow adoption fails this (negative control).
- One voter paused (`SIGSTOP`) for 5 s under ten clients executes again within a bound of being resumed, and the leader's lane to it drains (its refused-frame count stops rising) within the same bound. task-d49's PR states the bound and derives it from the re-send interval and the catch-up hand-off threshold above.
- task-d07's and task-d15's tests pass. The PR reports how `a_replica_that_falls_behind_catches_up_without_starving_its_own_catch_up` fares under load, since its full-bulk-lane assertion has failed intermittently on a loaded runner (task-d46's notes, build-test run 36900303341).

**Review boundary:** The leader's re-send window and timer, and the duplicate refusal.

<a id="task-d50"></a>
### task-d50: Serve reads and the fast path without waiting on the slow path

**Prerequisites:** task-28, task-29, task-d46, task-d47, task-d49.  
**Design:** Sections 4.5, 6.3, 17.4 (amended by this task).

**Implement:** Two latency floors remain after the throughput work.

- **Reads wait for their own execution.** A read is a transaction with the same path as a write, and a result is released only after the leader applies it: speculation (task-29) is built, but `next_speculable` has no caller outside tests.
- **The fast path rarely completes.** Every command carries the conservative key, so it depends on the one before it. A follower's fast acknowledgement carries a path hash over its own arrival order, and fast learning needs a fast quorum's paths to equal the leader's. With several collectors submitting at once, arrival orders differ and nearly every command takes the slow path.

Write the design amendment first, then implement it:
- serve linearizable reads from the leader's executed frontier under a read index or lease, or drive task-29's speculation in `coordd`, with what each one costs;
- measure the fast-path rate with task-d45's counters, and if it stays near zero under concurrent collectors, decide what the conservative key and the fast path are for.

**Acceptance:** Set by the amendment. At minimum:
- a register read's median at ten clients is one network round trip plus the leader's queue, not a full slow-path command;
- the change carries its own linearizability evidence under the Jepsen faults.

**Review boundary:** Read semantics, speculation in `coordd` and the fast path. This is the only throughput task that changes protocol behaviour.

**Amendment (this task's decision; design Sections 2.2, 4.5, 6.3, 6.8.1, 6.9.1):**
- **Reads take a leader read barrier, not speculation.**
  - A Range without an explicit revision is sent to the leader the frontend follows, and planned over the leader's snapshot. The leader first takes a read index, confirms its ballot with a slow quorum in a round started after the read arrived, and executes and materializes through the read index.
  - The ordered path stays the fallback for every refusal, deadline and frontend that follows no ballot. Configuration `[reads] path = "ordered"` turns the barrier off.
  - Speculation is not driven in `coordd`: its release still waits for the command and its prefix to be learned, so it would not remove the journal sync or the round trips.
- **The fast path stays as it is.** Measured at the leader, its share was 56% with one caller, 6% with ten and 2% with fifty. A finer conflict key is a separate correctness review. The share is reported again with reads off the chain.
- **Wire.**
  - Two collector kinds: `ReadV1` (0x0107, frontend to leader) and `ReadAnswerV1` (0x0702, leader to frontend).
  - Two protocol messages appended to `ProtocolMessage`: `ReadConfirm` and `ReadConfirmed`.
  - The 0x08xx read-fence range stays reserved for task-o05's ordered fence.
- **Acceptance:**
  - At ten callers on this container's disk, a register read's median is no more than one network round trip plus the leader's queue. In practice, a fraction of today's, which pays two journal syncs and the slow path.
  - A linearizability check of a register history through every frontend (`coord-register`, `scripts/bench/register-faults.sh`) finds no violation under a leader kill, a leader pause and a leader partition. The partition run fails when the confirmation round is skipped (`--features skip-read-confirmation`), as a negative control.
  - The Jepsen client's runs, by the owner of #98, are the external evidence.

<a id="task-d51"></a>
### task-d51: Export the local checkpoint off the domain thread, and bound it

**Prerequisites:** task-j04, task-d55.  
**Design:** Sections 17.16.1–17.16.6.

**Implement:** task-d46's long runs left one cost that grows with the state on each node's domain thread: task-j04's local checkpoint. `Domain::maintain` publishes one each time the journal runs `limits.checkpoint_after_records` (4,096 by default) past the last, and `export_local` traverses the whole projection in one read transaction on the domain thread to write it. The projection grows with every executed command, so each export costs more than the last: 1.1 to 1.3 s of the domain thread on average over a 150,000-operation run at ten clients, about 2.5 s at 365,000 projection records, during which the voter takes no event. Over 280 s a follower's busy time per command grew 2.0- to 2.3-fold with exports on and stayed within 20% with them off, and a voter fell 9,000 commands behind.

**Promoted to next, from task-j06's Jepsen runs.** At six nodes on the runner's disk, the leader spent 2.0% of a 120 s run inside publications under the strict profile (20 of them, the longest 195 ms) and 4.3% under the replay profile (21, the longest 818 ms), growing near-linearly with `represented`. Nothing is proposed, voted or read while one runs, so every operation in flight waits out the rest of it. Under replay that is most of the tail: read p95 80 → 166 ms and p99 132 → 299 ms against strict. In a closed loop the tail sets the throughput, so replay's lower median (read 45 → 27 ms) bought no throughput (318 against 310 `ok`/s). Before any code, one run settles how much of the tail this is: the six-node replay row with `checkpoint_after_records = 65536` (task-d55's harness knob). The prediction is p99 at or below strict's 132 ms, and `ok`/s up by the tail's share of the mean. task-d55's per-step timings say which step replay doubled.

- **Export from a pinned snapshot.** Open the read snapshot at the represented position on the domain thread, as task-d37's root will, and write the image from it on a background thread. task-d37 is not built yet, so the snapshot is the gated reader's (task-11), which is the same pin, and task-d37 is no longer a prerequisite. One thread, one export at a time, is the budget. The domain thread keeps the steps that order the publication: it selects the image (the pointer) only once the image and its directory are durable, then retires the prefix, in task-j04's order; the superseded images are reclaimed off the thread once the pointer is durable. Those steps take microseconds to a sync, not the projection's size.
- **Bounded, not only moved.** An image is a copy of the whole projection, so publishing every 4,096 records costs a run time quadratic in its length, whichever thread pays it. Under the replay profile the journal is the record and a restart replays from the projection's last durable commit, so the cadence is by time (every 30 s by default) and by records (65,536 by default), whichever comes later, and a restart's replay is bounded by it. task-d55's boot line measures that replay, and the faults run is its evidence. Under the strict profile the cadence stays by records, with the same default raised to 65,536 once the replay time at that interval is measured on the runner.
- **The pin's cost** is task-d37's: redb cannot reuse pages freed while the snapshot is open, so the file grows by about the write volume during the export. The bound is a deadline of 60 s from the pin: a read past it fails the export, which is abandoned and reported, never queued behind the next, and the previous baseline stays selected.

**Acceptance:**
- In task-d46's long runs with local checkpoints at their default, a follower's busy time per command at 280 s is within 20% of its value at 20 s, at one client and at ten, and no voter is more than 5,000 commands behind the leader's executed count at any 30 s snapshot. With checkpoints off those runs stayed within about 3,200, snapshot skew included; with the inline export a voter ended more than 9,000 behind.
- The domain thread's longest pass during an export stays within the bound task-d37 sets for opening its snapshot.
- On the Jepsen runner at six nodes, the replay profile's read p99 is at or below the strict profile's in the same carry, and no `checkpoint` line's `loop_ms` exceeds 10 ms at the default cadence. If one does, what is left on the domain thread is the journal's part (the drain, the pointer's sync and the retirement), and taking it off is task-d66, not this task. At 4,096 records on a local disk 26 to 32 of 70 publications exceeded it, each within 78 ms; on the runner at the default cadence 8 of 15 did, up to 99 ms, with the drain, the retirement and the append each the largest step in some.
- An image written off the thread is byte-identical to one written synchronously at the same represented position.
- task-j04's crash points (create, sync, rename, pointer, trim, purge, old-delete), with the crash now possible while the background write is in flight, recover a valid selected image plus its suffix or quarantine explicitly, every time.
- A restart's replay at the default cadence, measured by task-d55's boot line, is reported for the faults run, with `attach_ms` at a longer time limit, since the attach, not the replay, grows with the store. The runner's faults run at the default cadence replayed 0 to 5 records and attached in 75 to 161 ms at 4,100 to 10,500 journal positions, about 15 µs a position before a reclaim and 3–4 µs after one; the attach is therefore bounded by the cadence, about 1 s at 65,536, and one faults run at a cadence low enough to publish measures it after a reclaim.

**Review boundary:** When and on which thread the local image is written, what the domain thread waits for, and how often an image is taken. No change to the image's format, to what it carries or to task-j04's recovery rule.

<a id="task-d52"></a>
### task-d52: Execute and materialize on a pipelined applier

**Prerequisites:** task-j03, task-j08, task-d47, task-d48.  
**Design:** Sections 17.3, 17.3.3, 17.3.4.

**Implement:** A profile of the domain thread after task-d46 ([notes](../tuplesky-impl-notes.md#where-a-commands-time-goes-after-task-d46)) put 34% of a command's CPU in the projection's redb transaction and 7% in the applier around it, and with the stores on a disk the projection's syncs add about 1.8 ms a command, all on the domain thread. task-d47 groups the projection's commits and task-d48 takes one of each commit's syncs away, but a group's projection commit still runs on the domain thread after the group's journal sync. A group then costs the domain thread two syncs in series and every command's execution. On the Jepsen runner's disk, about 2.5 ms a sync, that bounds a voter near 1,400 commands a second even at 50 commands a group, which is etcd's rate there with nothing to spare. etcd commits its backend off the raft loop, behind its log.

- **Hand execution and materialization to an applier thread.** The domain thread decides the order and journals. It hands each executable command, in the learner's order, to the applier. The applier executes it against the projection and its own uncommitted writes, commits the projection by group, and returns each outcome as the event the machines already take (`applied`).
- **Keep every barrier.** A result is released only once its outcome is back. `Materialized` names the applier's committed position. A read from the projection and the collector's settle and retained answers read only committed rows, as today. The applier's queue is bounded, and a full queue holds the learner rather than dropping work.
- **The journal stays the record.** A command handed over and not yet committed is executed again from the journal at start-up, by task-j03's rule. An outcome the domain thread never received is recognized by its position and never applied twice.

**Acceptance:**
- With the stores on a disk, the projection's commit and syncs leave the domain thread: its busy time per command at ten clients falls by at least the projection's share measured after task-d48.
- On the Jepsen throughput scenario at ten and fifty clients, completed commands a second reach etcd's on the same runner.
- Execution order and results are unchanged: task-d30's simulator digests are identical, and applied positions are contiguous. Releasing a result before its outcome comes back fails the barrier tests (negative control).
- A crash with work in the hand-off queue, in the middle of an applier group, or after the applier's commit and before the domain thread took the outcome, recovers to the journal's state, and no resolve after the restart answers `Unknown` or `Forgotten` for a command the journal holds.

**Review boundary:** Which thread executes and materializes, and the hand-off queue between them. No change to execution order, to results, or to what is durable before which message.

<a id="task-d53"></a>
### task-d53: Establish an execution without walking what already executed

**Prerequisites:** task-21, task-24, task-d46.  
**Design:** Sections 4.2–4.9 (unchanged).

**Implement:** `Learner::established` runs on every executed command, at the leader and at every follower. It walks the command's whole dependency closure through `CommandTable::closure_step`, and that walk continues through every executed record still in the table, up to the table's capacity. `EstablishedResult::establish` then checks the closure for duplicates pairwise, and drops it. Nothing reads the closure after that check: the cursor already visits each command once, and the guard has already required every direct dependency executed, which by induction executes the whole closure. In the profile after task-d46 this was a quarter of the domain thread's CPU per command, at ten clients on tmpfs, and it grows with the table's capacity, not with the work.

- **Stop the walk at executed records.** An executed record's predecessors were established when it executed, so the closure that still needs evidence is the command's unexecuted predecessors, which the guard makes empty.
- **Check the closure in linear time.** Keep the self and duplicate checks, against a set.

**Acceptance:**
- The domain thread's busy time per command in `command-cost.sh` falls by at least 20% at one client and at ten, and `Learner::established` leaves the profile's top entries.
- task-d30's simulator digests are identical, and task-21's closure tests pass, including a placeholder dependency refused as `DependencyUnknown`.
- With a table of capacity 1,000 kept full, an execution's cost does not depend on how many executed records it holds (a test counts the records visited).

**Review boundary:** The establishment evidence's construction and check. No change to the guard, to what a command may execute after, or to execution order.

<a id="task-d54"></a>
### task-d54: Append the journal's groups on a journal worker

**Prerequisites:** task-j03, task-d47, task-d52.  
**Design:** Sections 17.3, 17.3.3 (unchanged: the shared journal worker of its diagram).

**Implement:** The Jepsen runs of task-d50 put the leader's domain loop at 82–88% busy at six nodes on the runner's disk, about 2.7 ms a command, and about 1.2 ms of it waiting in the journal's `fdatasync` on the domain thread. A group then holds what arrived during the previous sync, about 1.6 commands, so the syncs per command stay near one. Section 17.3.3 already places the append on a shared journal worker; task-j03 ran it inline.

- **Lend the journal to a worker for each group.** The domain thread seals the group, reserves each stream's entry and lends the journal with the group to an appender thread, which runs the synced append and wakes the domain loop. The loop takes the journal back with the outcome and completes, fails or leaves uncertain each entry exactly as an inline append does. task-d52's materializer is the model: one job out at a time, taken back on the loop's thread.
- **One group out at a time.** What is queued while a group is out goes as the next group once it is back, so groups are written in the order they were sealed, and a stream's records chain as before.
- **Keep every barrier.** `JournalDurable` is reported only once the outcome is taken back, so a vote or proposal still leaves only after its rows are durable. An indeterminate outcome is reconciled from the journal's durable head, wherever it is taken back. Anything that reads or writes the journal on the loop's thread (a reconcile, a checkpoint publication, a drain, a flush) takes an append that is out back first.
- **Report CPU beside busy.** The `metrics` line's cost carries the domain thread's and the process's CPU time, so a CPU per operation column can sit beside busy time, which counts a sync the loop waits for as work.
- **Count the loop's waits.** The cost also carries how often, and how long, the loop blocked taking a job back from the appender's and the materializer's threads, so busy time less CPU time is accounted for rather than inferred.

**Acceptance:**
- On the Jepsen runner at six nodes with stores on a disk, the leader's busy time per command falls by at least the journal's share (about 1.2 of 2.7 ms after task-d50).
- The leader's busy time less its loop CPU per command is reported beside the loop's counted waits on the appender and the materializer. Journal syncs per command are reported and not gated. With one group out at a time, a voter syncs once per sync time or once per arrival, whichever is rarer, so the ratio follows the load rather than the loop. A first gate of below 0.2 was replaced once the runs showed 0.40 to 1.27 across the runner's rows, with the loop no longer the bound.
- Results are those of a node that appends on its own thread, and a proposal is not sent before the append that makes its record durable is taken back (negative control).
- A boot that ends with an append lent and not started, or synced and not taken back, recovers to what the journal holds, and no resolve after the restart answers `Unknown` or `Forgotten` for a command the journal holds.

**Review boundary:** Which thread runs the journal's synced append, and the hand-off between it and the domain thread. No change to what is durable before which message, to the record format or to group contents.

<a id="task-d55"></a>
### task-d55: Measure a publication's steps, a restart's replay and the domain thread's scheduling

**Prerequisites:** task-j04, task-j06, task-d45, task-d54.  
**Design:** Sections 17.16.3, 17.16.4, 22.3 (unchanged).

**Implement:** task-j06's Jepsen runs left three costs on the domain thread that the readings could not attribute. Publications took 5.2 s of the leader's 40 s of blocked time under the replay profile, about twice the strict profile's, with no reading of which step doubled. A restart's replay was complete by the `recovered` line (`owed=0`), with no reading of how long it took. The other 35 s of busy time less loop CPU had no counted wait. task-d55 measures all three, and lets the harness set the checkpoint interval a run needs to separate them.

- **A publication's steps.** The `checkpoint` line carries, beside `took_ms`, each step's time in task-j04's order: `export_ms` (pinning the snapshot and producing the image), `write_ms` (the image file and its directory, synced), `drain_ms` (taking back an append or projection commit that was out), `sync_ms` (the replay profile's forced durable commit, zero under strict), `append_ms` (the pointer, synced), `retire_ms` (the journal's compaction) and `reclaim_ms`. The `Checkpoint` stage takes each publication's time as its sample, and a failed one as a refusal.
- **A restart's replay.** The attach records where the projection was found and where the journal's head is, and how long the replay between them took. `coordd` prints `replayed records= from= through= took_ms= attach_ms=` after its `storage` line, and the `Recovery` stage takes the replay as its one sample a start, on the startup snapshot and the serving loop's.
- **The domain thread's scheduling.** `cost.cpu.domain_scheduling` carries the thread's run-queue time from `schedstat` and its voluntary and involuntary context switches from `status`, cumulative. Busy time less CPU time less the counted pipeline waits is then attributed: run-queue time is a host with more runnable threads than cores, and what remains is a blocking call on the loop's own thread.
- **The checkpoint interval in the harness.** `coord-harness provision --checkpoint-after-records N`, or `COORD_HARNESS_CHECKPOINT_AFTER_RECORDS`, writes `[limits] checkpoint_after_records` into every voter (the section's other fields at the daemon's defaults) and records it in the description. Without it nothing is written.

**Acceptance:**
- A daemon that publishes says each step's time on its `checkpoint` line, and a restart says what it replayed. The startup snapshot's `Recovery` stage has one completed sample, and `Checkpoint` and `Recovery` are reported as observed.
- The attach's replay reading is the projection's position as the crash left it and the journal's durable head, and zero records for a projection that was durable at the head.
- A `cpu` reading written before this task still parses, with its scheduling reported as not instrumented rather than as zero.
- The harness's `[limits]` parses in `coordd` to the daemon's defaults except for the interval.
- The six-node replay row with `checkpoint_after_records = 65536`, run by the owner of #98, answers task-d51's question.

**Review boundary:** What is measured and printed, and one harness setting. No change to when anything is published, replayed or scheduled.

<a id="task-d56"></a>
### task-d56: Keep a restarted voter from stalling the domain

**Prerequisites:** task-31, task-d01, task-d08, task-d10.  
**Design:** Sections 4.9, 11.3 (unchanged).

**Implement:** In task-j06's replay faults run, one follower's restart stopped the whole domain for about 27 s, while the other three voters were a healthy majority. The leader's control lane to the restarting voter filled (`QueueFull { lane: Control }` three times, after 355 to 412 frames) and the connection was cut. The restarted voter, unable to reach the leader, campaigned with "no leader" and took ballot 3. The leader then followed it and caught up from a peer's executed history. `QueueFull` lines appear in the strict faults runs too (21 and 23 against 55 here), so the profile did not cause this.

- **What catch-up serves stays off the control lane to a voter far behind.** What filled the lane were the positions above what the voter had reported executed and below the leader's, which is the range task-d08's catch-up serves. For a voter in that state, proposals, re-sends and commit frontiers in that range are dropped, not queued, and catch-up brings it up. A full control lane is then never what cuts a voter.
- **Only a voter far behind is exempted.** A follower a few proposals behind is still in the quorum, and at three voters it may be the only other member, so dropping its proposals would stall the domain. The exemption holds only for a voter that has asked for catch-up, or whose reported executed position is more than one catch-up window (task-d25's 64 commands) behind the leader's. It never holds while the leader cannot form a quorum without that voter.
- **A restarted voter does not take the ballot from a live leader.** Before it campaigns, it holds while any peer reports a leader of its ballot or later that is reaching a majority. This is a pre-vote in effect: a peer answers a campaign it could not join without promising anything. A voter that hears no leader from a majority campaigns as before.

**Acceptance:**
- A three- and five-voter in-process test kills and restarts one follower under load, the leader's control lane to it sized to fill. No other voter's callers go more than one election timeout unanswered, and no ballot changes. Without the change, the same test shows the lane filling and a ballot change (negative control).
- At three voters under load, a follower a few proposals behind keeps receiving every proposal and the domain keeps serving; only one that has asked for catch-up or is more than a catch-up window behind is exempted, and never the only other member of the leader's quorum.
- A restarted voter cut off from the leader alone does not change the ballot while a majority still hears the leader. One cut off from a majority still campaigns.
- In the Jepsen faults run, no restart of a single follower is followed by more than one election timeout with nothing served.

**Review boundary:** What the leader queues for a voter that is behind, and when a restarted voter campaigns. No change to what a ballot decides, to recovery selection or to the catch-up protocol.

<a id="task-d57"></a>
### task-d57: Bound a read's index by what the confirming voters had voted

**Prerequisites:** task-d50.  
**Design:** Sections 2.2, 4.5, 6.3 (amended in this task's first PR).

**Implement:** task-d50's read index is the leader's next sequence number when the read arrives, and the read waits until every proposal below it has executed. Under load that is every proposal in flight. In task-j06's runs the read's index wait was 23.2 of its 35.0 ms, both counted from arrival, so 20.0 ms of it after the round confirmed, and a read cost what a write costs (p50 27 against 24 ms) where etcd's read is under its write (7 against 9). Reads are 64% of the Jepsen workload.

- **Why not the commit frontier.** The leader cannot see every completion. A fast-path one is collected from a fast quorum's votes by the client's collector. A slow-path one the collector may learn from a slow quorum's acknowledgements before the leader does (learning is all-to-all, task-d09). So a read indexed at the leader's commit frontier, even with every command still inside its fast window added, could miss a write already answered to a client. Ruling that out needs a time bound, which the design does not assume.
- **Why not the read's keys.** Every command conflicts with every other in a domain (Section 2.2), and a read returns the domain's revision, so a write to another key answered before the read arrived still bounds what the read must return.
- **What the confirmation round can carry.** Each follower's answer to the round carries the highest sequence number of this ballot it has voted on, fast or slow, when it answers. A write answered to a client before the read arrived was voted by a quorum before then, and the round starts after the read arrives. Every quorum that answers holds at least `f` followers besides the leader, so any `f + 1` followers include one that voted the write before it confirmed. Once `f + 1` followers have confirmed, the read's index is the highest number they reported plus one. It is never below this ballot's first proposal and never above the leader's next sequence number. With fewer, the index is task-d50's.
- **First, the reading that sizes it.** The read waits reported so far are cumulative from arrival: the wait after confirmation is the index wait less the confirmation wait, and the wait after the index the total less the index (task-d62). The reading reports all three, with the reads held behind their snapshot beside them. On the runner with task-d58 the third was 5.3 to 8.3 ms of every read, with 1.07 to 1.21 holds per read served: the projection's commit trailing execution, which this task does not touch and task-d52's lag does. At each read's arrival, the leader records its next sequence number less each of three positions: its executed frontier, its commit frontier, and the highest sequence it has seen a follower vote. The averages go into `cost.reads`. This lands, and runs on the Jepsen runner, before the design amendment is written.
- **What it buys.** The rule removes from the index only proposals that none of the `f + 1` fastest followers had voted when they answered the round: the sync-and-wire window, a few ms if the reading says so. If most of the 23.2 ms is proposals already voted and waiting to execute, the read's lever is the apply lag (task-d52), not the index, and the reading says that instead. At three voters, a proposal one follower has voted on is already decided with the leader's acceptance, so the read waits for an acknowledgement's transit and the apply, not a proposal's whole round.
- **The leader's own acceptance is no evidence here.** It is in every quorum, so it would put every proposal in the index. The bound is from followers only, and a round with `f` followers is not enough.
- **Once the index is short, the round is on the read's path.** task-d58 keeps one confirmation round in flight per ballot, so a read that arrives just after a round starts waits for that round and then its own: up to one round's confirmation time more. Under task-d50's index that wait hides under the index wait (4.6 to 6.3 ms of confirmation, the total unchanged). Once this task shortens the index wait it may not, so the reading counts the confirmation wait apart, and a confirmation wait that grows as the index wait shrinks is task-d58's rule, not the barrier failing.

**Acceptance:**
- The first step's reading is reported for a six-node run on the Jepsen runner, and the gate below is set from it: if the sync-and-wire window is most of the index wait, the leader's read wait (`waited_index_ms` over `served`) falls by at least half and the read p50 is below the write p50; if it is not, the task stops at the reading and the read's wait moves to task-d52's apply lag.
- A linearizability check of a register history through every frontend (`coord-register`, `scripts/bench/register-faults.sh`) finds no violation under a leader kill, a leader pause and a leader partition. A build that takes the bound from `f` followers instead of `f + 1` fails a schedule built for it (negative control), as does one that counts the leader's acceptance.
- task-d30's simulator runs the read barrier under its protocol oracle with reads interleaved, and finds no read below a write answered before it.
- The design amendment states the rule and the argument above, and why a commit-frontier index and a key-restricted one are not safe.

**Review boundary:** What the confirmation round carries and how the read's index is taken from it. No change to execution order, to when a round confirms, or to what a read may observe.

<a id="task-d58"></a>
### task-d58: Serve a turn's held reads from one snapshot

**Prerequisites:** task-11, task-d50.  
**Design:** Sections 4.5, 17.4.

**Implement:** A profile of five voters on one host ([notes](../tuplesky-impl-notes.md#a-voters-cpu-per-operation-at-five-voters)) put 25% of the leader's domain thread in `Voter::pump_reads`: about 0.14 ms of every operation, 0.27 ms of every read. Each held read that comes due pins its own snapshot (a redb read transaction and the durable metadata read that proves its stamp, 16% of the thread), and every point read on a snapshot opens its table again (`open_table` is 11% of the thread on its own).

- Evaluate every read that comes due in one pump from one snapshot. Each is due because its index has executed, and one snapshot taken after that covers all of them.
- Keep a snapshot's opened tables for its life, so a second read of a table does not walk the table tree again.
- Let the reads that arrive before a round starts share it. `Voter::on_read` pumps the barrier on each read's admission, so successive reads start successive rounds, four requests and four answers each at five voters. A round covers only reads that arrived before it started; a read never joins a round already under way.

**Acceptance:**
- In the five-voter profile, the leader's domain thread spends at least half less in `pump_reads` per read, and one snapshot is pinned per pump that serves any.
- Reads per confirmation round (`reads.served` over `reads.rounds` in the leader's `metrics` line, 1.0 under load today with 31% of rounds superseded before they confirmed) rises above one under load, and a test shows a read that arrived after a round started is never answered by that round.
- `coord-register` under task-d50's leader faults finds no violation, and task-d50's tests pass unchanged.

**Review boundary:** How many snapshots a pump takes and how a snapshot reads its tables. No change to when a read is due or what it may return.

<a id="task-d59"></a>
### task-d59: Re-send without walking every proposal the leader keeps

**Prerequisites:** task-d49.  
**Design:** Section 6.2.

**Implement:** `Leader::resend_unvoted` runs once per `RESEND_INTERVAL`, 250 ms, on the leader's domain loop (`coordd`'s `serve.rs`), not on every flush. Each call collects every durable proposal, sorts them, and walks them once per voter with two map probes an entry, to find each voter's highest adoption and then what it has not adopted. The leader keeps a proposal until the history sweep, once the ledger passes `HISTORY_SWEEP` times the table's capacity (4,000 at a capacity of 1,000), so each call walks thousands of retired proposals to find at most `per_voter` for each voter. On the Jepsen runner's frame-pointer profile that is 82 µs per command on the leader's loop, 38% of the leader's excess over a follower, and at about 430 commands a second some 9 ms per call: a stall four times a second, inside the leader's read p99. Keep the durable proposals in sequence order as they become durable and as they are replaced or swept, and each voter's highest adopted sequence number as adoptions are counted; look only past it, and before it only at the undecided proposals, which are all unsettled (task-d46). The call then costs what each voter has not adopted.

**Acceptance:**
- A unit test with hundreds of proposals executed and adopted by every voter: a call looks at none of them, and with one voter cut off, at only what it lacks.
- What a call sends, to whom and as what, is unchanged: in debug builds every call is checked against the sorted walk, across the consensus tests.
- A call's time is in `metrics` (calls, proposals looked at, total and longest time), and a pair on the Jepsen runner (three pairs, replay, throughput) shows the walk at about the proposals not yet adopted and the call at about 0.1 ms, its sends included, with the leader's loop per command and the leader's read p99 beside it. The call's time is then mostly the commit-frontier announcement every call ends with (`announce_committed`, task-d09: a `Committed` frame to each voter), not the walk; the longest calls are the ones that also re-sent.
- task-d49's tests and its duplicate-vote reading are unchanged.

**Review boundary:** How the leader finds what is due. No change to what is re-sent or when.

<a id="task-d60"></a>
### task-d60: Allocate less on the domain thread

**Prerequisites:** task-d45.  
**Design:** None.

**Implement:** On the Jepsen runner's call-graph profile with task-d59 in (Ubuntu nodes, whose glibc keeps frame pointers, with its debug symbols), a third of the leader's domain thread is glibc's allocator and copies: allocating and freeing about 22% (about 175 µs per command, most of it glibc's internal `_int_malloc`, `unlink_chunk`, `_int_free` and `malloc_consolidate`), `memcmp` 5.5% and `memmove` 5.4%. It is spread over every phase of the loop, not held by one site: the 25 largest callers make 12.4 of the allocator's 18.7 points and none holds more than 1.4. It is the cost of many small allocations that live a turn, through a general-purpose allocator. Two steps, each paired on the runner:
- **Step 1, the allocator.** `coordd` sets a global allocator with size-class heaps and thread-local caches (mimalloc), which measures the whole share at once. No call site changes.
- **Step 2, the sites,** in order from the profile's caller table: the encoders (`JournalRecordV1::encode`, `StoreEnvelopeV1::encode`, `seal_group`, about 21 µs per command) encode into a buffer the loop keeps; redb table opens and scans on the write path (`RedbView::table`, `scan`, `load_authorization`, about 21 µs) keep their tables as task-d58 does for reads; consensus bookkeeping stops cloning what could be shared (`BallotConfiguration::clone`, `StoreUpdate::clone`) and stops re-canonicalizing a request the collector canonicalized (`canonical_bytes`); and `trace::hex` formats only when the trace is logged. `memcmp` and `memmove` are split by caller before they are cut.
- **Step 3, the leader's lookups and a broadcast's copies,** after step 2's pair. The leader's `memcmp` (25 to 30 µs per command, plus 5 to 6 in `pump_reads`) is `BTreeMap` probes on 32-byte keys: the maps the leader only looks up and never iterates to produce an effect (`votes`, `proposals`, `resent`, `answered`, and the table's lookup side) become hash maps with a trivial hasher, a command identity being a uniform digest already, and every map iterated to produce an effect stays a `BTreeMap`, since the machine is deterministic by their order; task-d30's simulator and the model tests are the check. The loop closure's `memmove` (7 to 12 µs) is taken to be the per-voter copy of each broadcast frame, confirmed first with `#[inline(never)]` on the clone for one profile run, and then a `PendingSend` holds a shared buffer. A command is carried from its first decode, where a voter now derives it again in execution and at a submission, and decodes the submission frame again to find it (`voter::command_of`).
- **A candidate for the shared phases:** `seal` still encodes a record's body once for its digest and the codec encodes the record again into raft-engine's buffer; one pass that hashes and writes together takes an encoding out of `flush_queued` on every voter. It is weighed with the follower's five largest functions read one level down (`run_executions`, `flush_queued`, `on_remote_submission`, `Follower::step`, `lower_once`).

- **Step 3 is scheduled (#174 review).** Its prerequisite, task-d69, has landed, and B-tree searches on 32-byte identities are 19% of the leader's loop in task-d75's reading: 0.04 to 0.07 ms of CPU an operation at the register row. With it, `load_authorization` once a session rather than once a command (3.9% of the leader's loop, about 0.02 ms). Its runner pair is read in the batch job (task-d72).
- **Step 3's lookups, ported onto the batch head (#185's item 3, C6).** The October branch's two code commits, on `claude/task-d60-step3-lookups` from #198: the leader's lookup-only maps (`proposals`, `votes`, `resent`, `answered`) and the table's lookup-only sets are hash maps keyed per process over a command identity's leading bytes (`coord_consensus::digest`); held reads stay in place across a pump that moves none of them; a submission's command is taken from the machine rather than decoded again. Every map iterated to produce an effect stays a `BTreeMap`. The learner's `commit_learned_where`, which task-d89's gate added since, takes a lookup as `commit_learned` does. Maps added since October (`served_payloads`, `own_adoptions`) are left ordered. The protocol simulator's decision hash is identical to #198's on every row at 40 seeds a size, so nothing a machine decides moved. Local pair against #198 at one worker a voter, tmpfs, `command-cost.sh`, 5,000 operations a run, alternating: at 30 callers, four runs a side, all voters' CPU a command 1.713 to 1.638 ms (−4.4%), the leader's domain loop 0.284 to 0.258 ms (−9%), completed operations a second 4,514 to 4,691 (+3.9%); at 10 callers, eight runs a side, 2.159 to 2.160 ms, level, the leader's loop −2%. So it pays where the maps are large, about −0.075 ms a command at 30 callers against the −0.04 to −0.06 sized for C6 and C8 together.
- **Step 4, the learner's pass (#185 review, [6721031536](https://github.com/tuplesky/tuplesky/pull/185#issuecomment-6721031536)).** A follower's loop, split by handler at 30 and 120 callers on the batch's carry, put learning at 20 to 23 µs a command at 30 callers and 38 to 42 at 120: it grew with what is in flight. A pass of `Learner::commit_learned_where`, and of `Follower::commit_through_leader`, runs on every vote and storage event and visits every command at ACCEPT; about 80% of it was B-tree searches over 32-byte identities: `CommandTable::in_accept` checking each member against the records, then each candidate's vote set, then its adoption. Three changes, none of which changes what is committed or in what order:
  - **The ACCEPT set is kept exact** where a record leaves the phase (`commit`, `execute`, `restore_executed`, `demote`, `release`), so `in_accept` takes it as it stands. A debug assertion checks it against the records both ways on every call.
  - **The follower's vote sets and adoptions are hash maps** keyed per process (`coord_consensus::digest`), as the leader's have been since step 3. They are only looked up or retained; the one walk of the adoptions (`unsequenced_adoptions`) sorts what it collects, as the ordered map gave it.
  - **A pass asks only which predicate holds** (`VoteSet::learned_by`), without cloning the leader's dependencies into a `Learned` it drops, and `same_set` compares equal or short lists without building two sets.
  - **A learning pass commits only a candidate at ACCEPT** (`CommandTable::commit_accepted`), one phase lookup a candidate after its votes have learned it, so a release build in which the set had gone stale skips a member rather than committing a record below ACCEPT (#185 review). It covers a stale member, not a missing one: a record at ACCEPT outside the set would not commit by learning and would show as a stall, but only `accept` and `restore` put a record at ACCEPT and both insert it. The 1,000-seed sweep ran on the commit before the guard, where a stale member would fail an oracle instead.
  - **Read:** the protocol simulator's decision hash is identical to #200's head on all 17 rows at 40 seeds (24 of 24 tests), and the `coord-consensus` and `coord-daemon` suites pass in debug with the assertion (556 tests). Five voters on one 4-core host, two workers, both features active, tmpfs, three runs a side alternating against #200's head (`142aed7a`):

    | Callers | `ok`/s | Voters' CPU an operation | All five domain loops | A follower's loop |
    | --- | --- | --- | --- | --- |
    | 120 | 1,888 → 2,007 (+6.3%) | 1.688 → 1.570 ms (−7.0%) | 0.802 → 0.701 ms (−12.6%) | 0.157 → 0.134 ms (−14.4%) |
    | 30 | 1,761 → 1,750 | 1.890 → 1.888 ms | 0.801 → 0.771 ms (−3.7%) | 0.156 → 0.149 ms (−4.8%) |

    At 120 callers a follower's learning fell from 39 to 43 µs a command to 5 to 6 (`perf`, n2 and n4). The ranges of `ok`/s at 120 don't overlap; at 30 callers the voters' total is within its spread. So it is a throughput lever: it pays where a sweep's upper steps run.
- **`load_authorization` once a session (C8) is not in this step.** A cached authorization stands for the session, trust and policy rows at the command's position, and a stale one would change a replicated result, not only a cost. Those rows are written by session binding and retirement, policy changes, materialization and restore, and planning reads them through the in-flight groups' overlay, so a cache needs an invalidation rule stated against every writer before it is built. It is left to its own change with that rule written first, and it doesn't gate the carry: C6 alone covers what was sized for both (#200 review). It stays the next CPU lever after the batch, read against the batch's numbers beside C4's (task-d86).

**Acceptance:** Each step in a pair on the Jepsen runner (three pairs, replay, throughput, the voters' loops and tokio threads read apart, `VmRSS` and `VmHWM`), with the call-graph profile on one pair: the leader's and the followers' loop per command and the voters' CPU per operation fall, and the allocator's share of the leader's thread falls with them, or the step is not kept. Step 1 is expected at 50 to 100 µs per command on every voter if glibc's bin management is what the share is; the copies do not move. Step 1 was kept on the runner's pairs ([notes](../tuplesky-impl-notes.md#the-allocator-task-d60-step-1)): the leader's loop fell 47 to 74 µs per command and the followers' 32 to 52, a tenth, in all six pairs on two CPUs, and the leader's allocator 49 µs; the voters' CPU per operation fell 5%, about half the local pair's, the shortfall in the transport's tokio threads; the cost is about 34 MiB of resident set per voter, kept as measured. Step 2 was kept on the runner's pairs (an Intel Xeon 6973P-C): every voter's loop 7% lower in all three pairs, the leader's allocator 9.5 µs per command lower, the tokio threads unchanged, and the resident set 43 MiB per voter lower, so the collector's trace was most of what step 1 had added. task-d69 comes before step 3.

**Review boundary:** Memory allocation only. No change to any format.

<a id="task-d61"></a>
### task-d61: Send a turn's frames to one peer together

**Prerequisites:** task-31, task-d45, task-d62.  
**Design:** Section 17.5.

**Implement:** Each voter's transport threads cost 0.26 to 0.34 ms of CPU per operation in the five-voter profile, a third of a voter's CPU. Little of it is cryptography (3 to 5%): it is QUIC's per-packet work (22 to 24%), a `sendmsg` per packet (13 to 15%) and parking and waking (19 to 26%). That cost is per frame, not per byte. `sender_loop` pops one frame, takes its permits, opens a QUIC stream, writes and finishes it, and spawns a task holding the permits until the stream completes; the receiver handles each accepted stream on its own; and the leader encodes the same proposal once per destination. The reference SwiftPaxos batches acknowledgements across commands. Using task-d62's counts, send what one turn has for one peer as one write on its lane, encode a broadcast once, and amortize the stream, task and permit work. Each logical message, its voter, ballot, lane and bounds are unchanged; acknowledgements are not coalesced or superseded until the evidence rules are shown to allow it. A send to each destination keeps its own release (`PendingSend` holds the frame and the barriers it requires per destination), so encoding once means a shared buffer, not a shared send. A batch changes the unit of loss: a refused stream loses every frame in it, so frames lost per refused batch are counted, or a saturated peer reads as a quiet one.

**Acceptance:**
- task-d62's streams per command fall by the batching factor, with frames per command unchanged and no frame lost, in a pair on the Jepsen runner against one frame a stream, measured at tokio's default worker count. The leader's loop and `ok`/s are reported beside it and are not required to fall: QUIC already packed separate streams' frames into shared datagrams, so the cost per stream was never per packet, and fewer streams is what QUIC's stream limits and a WAN path care about. One worker per voter cut the transport's CPU per operation by a quarter on one four-core host and was 17 to 31% slower on the Jepsen runner ([notes](../tuplesky-impl-notes.md#transport-workers-when-the-voters-share-a-host)), so no count is fixed, and a gain from changing it is not this task's.
- Mixed fast and slow messages, duplicates, partial delivery, a disconnect, a saturated destination and a stalled minority: no batch manufactures evidence, turns a missing message into success, or lets one peer's backlog stall the others.
- task-d45's gate and the Jepsen client's runs are unchanged in what they check.

**Review boundary:** How frames to a peer are written. No change to a frame's format, to the lanes or to their bounds.

<a id="task-d62"></a>
### task-d62: Count what a command costs and why its fast path failed

**Prerequisites:** task-d45, task-d50.  
**Design:** Section 22.3.

**Implement:** A consolidated review on #98 against the reference SwiftPaxos (35c6936) found the evidence supports a large residual gap but does not attribute it: the fast path's share is low under load without a reason, and the harness's share of the runner was inferred by subtraction. Before the next implementation tranche:
- **Per command, on every voter:** peer frames, bytes and streams sent and received; journal queue, service and completion time; materializer queue and commit time.
- **Per read:** reads per confirmation round, and the reads held behind their snapshot (task-d58's `behind`). The read waits already reported are cumulative from arrival; the time after confirmation is the index wait less the confirmation wait, and the time after the index the total less the index. task-d57's sizing reading is taken that way, as three numbers.
- **Why a command did not complete on the fast path**, one count each: an ordinary path mismatch, a direct-dependency mismatch, an acknowledgement made while a `reordered` marker affected its path, a missing fast-set member, and the slow path completing first. Beside them, per voter, the pre-acceptances in its path log the leader has not ordered and the age of the oldest: a marker that stays for the rest of the ballot is one of those (task-d67), and the count tells it from a marker the next synchronization clears. The acknowledgement does not say a marker held its path, and saying so would change the wire form of every acknowledgement, so its sender counts those it sent under one and the leader counts them as path mismatches. Each command is classified from the votes counted when it executes, and one a voter did not decide from its own votes is a fifth, unclassified count, so the counts add up to the slow establishments.
- **From learned to released**, per command on the leader: the time waiting for predecessors to execute, for the group to close, and for the projection to commit it. It bounds what task-d63 can buy before task-d63 is built.
- **A matched workload** in `coord-wan-bench` and the Jepsen client: reads and writes only, with conditional writes a separate row, and a fixed number of active keys across a concurrency sweep. Each row records the commit of every system, the hardware, the storage, the transport settings and each system's fast quorum (a majority, 3 of 5, for the reference as the baseline runs it; here `coordd`'s `c2_default`, a fixed fast set of `N/2 + 1`, 3 of 5: the leader and the next two in identity order, all of which must agree), and reports `ok`/s, every completed attempt per second, CPU per operation for the servers and the client apart, and executed commands apart from barrier reads.

**Acceptance:** The counters are in `metrics` and the summary; one five-voter and one three-voter row on the Jepsen runner report them, with the fast-path reasons summing to the commands that did not complete fast. Every row is three pairs: the base and the head built and run on the same runner in one job, and the deltas reported. Two runs of task-d51's carry on one commit differed by 20% in `ok`/s and threefold in read p99, and the followers' CPU per command moved 20% between runs of one commit, more than most changes measured here; a pair cancels the runner, repeats across runners do not. A run whose voters made no fast decision is reported as a finding, not as a 0% column. A row whose system leaves the runner's cores busy (etcd left 0.04 idle) says so, and is compared by CPU per operation rather than `ok`/s, which is then the runner's limit, not the system's.

**Review boundary:** Counting only. No change to what a voter does.

<a id="task-d63"></a>
### task-d63: Execute speculatively on the serving path under the existing release gate

**Prerequisites:** task-29, task-d52.  
**Design:** Sections 4.5, 17.3.3, 17.4 (Section 6.4 is watch and output authorization; the release text is in these).

**Implement:** The reference's leader starts executing a command while its votes are still arriving and replies with the result. In `coordd` it does not: `Node::run_executions` applies only what is executable and holds the effects until the group materializes, and task-29's `coord-consensus::speculation` and `coord-storage::speculate` are not driven (the latter still takes a `StoreWorker`, not the journal-backed view). Drive them on the serving path: plan eligible proposals tentatively against a consistent journal-backed view that includes the application state already in flight, bound the overlays by count and bytes, discard them on a role change, reconcile them with the final results, and handle a non-speculable predecessor explicitly. Reuse a tentative plan at finalization rather than planning the same prefix twice. The release gate is task-29's, unchanged: a result is released only once the leader has learned the command and its predecessors.

- **Bound the gain first.** Speculation saves only the execution after learning (design Section 4.5): after learning, a result waits for its turn in execution order, for the group's hand-off and for the projection's commit, and a result goes out at `settle`, never before the projection has committed its group. task-d62's learned-to-released split says how much each part is.
- **The release rule is its own design step.** Most of what the reference's early reply buys is answering before the projection, which is a question of when a result may be released, not of speculation: once its group is in the journal (the record under the replay profile), or only once the projection has committed it. That step is decided, with its argument, before either is built, and it is independent of driving `speculate`.
- **The design step, proposed** (design Section 4.5, with one sentence each in Sections 17.3.3 and 17.4): a result is released at its group's `JournalDurable`, after the gate and the output gate, without waiting for the projection's commit.
  - Watch events, credentials and external calls keep waiting for the commit.
  - The argument: the journal already holds every released result's exact outcome, and under the replay profile today's release already rests on it alone; Section 6.3's read proof never uses when a response left, and its barrier waits for the projection.
  - What the projection's commit still gives is local visibility. So a retry or resolve at the releasing node that finds no record waits for the projection through the command's execution position, rather than falling through to `Unknown`.
  - The build amends what says otherwise: `Node`'s settle and grouping docs, `Applier::hand_off_group`, `materialize::complete`, `Leader::applied`, `spec/swiftpaxos-mapping.md`'s task-27 row, and the two node tests that assert a result waits for its projection commit.

**Acceptance:**
- A deterministic schedule where the order's evidence is established and the projection's materialization is held back: an eligible result completes without waiting for the projection, and no result is released before its evidence.
- After such a release, a read through every frontend observes it or waits; none returns older state.
- Crash and restart, a leadership change, a retry, an authorization change and a speculative result that differs from the final one, each with the outcome stated.
- Latency and CPU per operation reported together, against the five-voter profile and the Jepsen runner.

**Review boundary:** When a leader plans and executes, and what it holds until the gate. No change to the release gate, to the wire or to what the collector requires.

<a id="task-d64"></a>
### task-d64: Design collector-side early completion

**Prerequisites:** task-d63.  
**Design:** Sections 4, 6.4 (amended by this task).

**Implement:** The reference's client completes on an early, result-bearing reply plus enough acknowledgements, without waiting for the leader to learn. TupleSky's `LeaderReply` carries ordering evidence, not a result, and the collector requires its own learning predicate and the leader's release. A collector that completes early needs a tentative result bound to its order and prefix, and a predicate proving its whole ordering basis, predecessor effects, authorization and session state included. A tentative result is never a `ReleasedResult`, and a checksum is not the evidence. This task writes the design amendment and its bounded model first.

**Acceptance:** The amendment, and a model that finds no completion recovery could contradict: evidence reaching the collector before the leader learns, a missing predecessor's evidence, a ballot change, and a crash after the answer and before the application.

**Review boundary:** Design and model. Nothing is exposed until recovery keeps every outcome answered this way.

<a id="task-d65"></a>
### task-d65: Decide the conflict model

**Prerequisites:** task-d62.  
**Design:** Section 2.2 (amended by this task, if it changes).

**Implement:** Every ordered command is initialized with the one `CONSERVATIVE_KEY`, so writes to unrelated keys share one dependency and path history, and an unrelated reordering or an unresolved predecessor holds up the rest. The reference tracks keys apart. Replacing the key with the user's is not a patch: transactions, ranges, leases, policy and session state, revisions, watches and recovery all depend on the total order. With task-d62's fast-path reasons, either keep the total order and cut its cost explicitly, or write and prove a finer model. The counterexamples behind direct-dependency equality and `reordered` (task-d34) stay as tests for whichever invariant replaces them.

- **The decision, recorded** (#98). task-d65's key sweep on the runner (append, 30 clients, `key-count` 3 to 100, exponential keys then uniform) left the leader's fast share flat at 1 to 2% while `guard-failed` fell from 63% to 23%, and task-d67's code left it there: 1.6% at 3 uniform keys and 1.65% at 100. The planner sees the keys and the chain does not. Every command carries the one conservative key, so a fast decision needs each fast-set follower to have pre-accepted every earlier command in the leader's order, and thirty clients from five origins is Section 2.2's fifty-caller case (2%). This confirms Section 2.2's decision: the conservative key stays, and the fast path is not where the runner's time goes (task-d72); one hop it saves is a container round trip, about 0.1 ms.
- **The finer predicate stays this task's design step**, the WAN item, after task-d63's build and task-d66's job. A fast decision would then fix a command's per-key predecessors instead of its place in one chain, so revision assignment (deterministic application in the total order) and recovery need an order every voter computes alike from per-key dependencies, and ranges, watches, leases and policy still conflict with everything. The consensus layer is already per key (paths per key, multi-key conflicts in recovery), so the cost is the model and its proof, with the simulator's 1,000-seed sweeps as the harness, not the code.

**Acceptance:** A decision recorded with task-d62's numbers. If the model changes, its design amendment and bounded model land before any code, with recovery and retirement tests.

**Review boundary:** A decision and, if any, a design. No code in this task.

<a id="task-d66"></a>
### task-d66: Publish the pointer and retire on the journal worker

**Prerequisites:** task-d51, task-d54.  
**Design:** Sections 17.3.3, 17.16.3 (unchanged: the publication order and the shared journal worker).

**Implement:** After task-d51 a publication holds the domain thread only for the journal's part: taking the journal back from the appender and the projection's commit from the materializer (the drain), the pointer's synced append, and the retirement. On the Jepsen runner at six nodes and the default cadence, 8 of 15 publications held it over 10 ms, up to 99 ms, and no one step was the cause: the drain was the largest in four (up to 83 ms), the retirement in two (up to 82 ms), the append in two. An asynchronous append alone would leave the other two. The drain waits out a job already in flight, its sync included, and the leader's loop waited for a core about as long as it computed in those runs, so some of each step may be scheduling, not the journal.

- **First, the reading.** Each `checkpoint` line gains the domain thread's own CPU time over the drain, the append and the retirement, beside their wall time, so that a step stretched by waiting for a core is told from one doing the journal's work. It is cheap and rides any carry. If it shows the drain is mostly waiting for a core, the job does not shorten it, and the finding belongs to the voters' scheduling on a shared host, not to this task.
- **The reading, on the runner** (#98): in every publication over 10 ms, under 2.5 ms of up to 65 ms on the loop was the domain thread on a CPU or waiting for one; the rest was the thread blocked on a sync or on the job in flight. The long publications are the journal's waits, not the host's scheduling, so the job is built.
- **The reading is built** (this task's first PR): after `reclaim_ms`, `drain_cpu_ms` and `drain_runq_ms`, and the same for `sync`, `append` and `retire`, the domain thread's time on a CPU and waiting for one over each step. The CPU time is the thread's CPU clock: `schedstat`'s trails a running thread by up to a tick, measured up to 4 ms ([notes](../tuplesky-impl-notes.md#a-publications-journal-steps-on-the-domain-threads-own-clock-task-d66)). The runner's reading is next.
- **Its priority** is below every task that moves `ok`/s: at the default cadence the publications held the loop for under 0.5% of a run. This task is about the 10 ms line.
- **Then the job.** The pointer's append and the retirement go to the journal worker as one job in the same queue as the groups, behind whatever is in flight, instead of the domain thread taking the journal back for them. The loop is woken with the outcome, as for a group, and the reclaim follows as task-d51's does. task-j04's order is unchanged: the image and its directory durable, then the pointer durable, then the retirement, then the reclaim.
- **The materializer's part.** Under the replay profile the pointer may name a position only once the projection is durable through it (task-d51 skips the sync when it already is). Where it is not, the job waits for the materializer's next durable commit rather than forcing one on the domain thread.
- **The job is built** (this task's second PR): `JournaledStore::queue_checkpoint` queues the pointer and the next group carries it, right after the transitions queued before it in the domain's entry, ahead of any queued since, or as an entry of its own. The appender runs the append and then the retirement (`AppendJob::run`), and `take_published` gives the outcome once the group is taken back, where `C` advances. Under the replay profile a group carries the pointer only once the projection is durable through `C`; queueing it asks the next commit to be durable. A voter queues its pointer (`LocalBaseline::queue_local`, `take_local`); a node that serves without voting lowers only when it applies, so it publishes on its thread as before. The `checkpoint` line gains `queued_ms`, and the domain thread's step readings are `-` for a pointer it no longer publishes itself ([notes](../tuplesky-impl-notes.md#the-pointer-and-the-retirement-on-the-journal-worker-task-d66)).
- **An open row: the runtime's worker count.** On the Jepsen runner two workers a voter took 8% of the tokio threads' CPU per operation, all of it parking and waking, with five voters on four CPUs (task-d70); the runner's job runs with two from then on. Whether a dedicated host costs the same (one voter on eight cores gets eight workers for about one and a half cores of work) is read with this task's per-thread reading on a host of its own. If it does, the count becomes a configuration key with a small default, not an environment variable.

**Acceptance:**
- On the Jepsen runner at six nodes and the default cadence, no `checkpoint` line's `loop_ms` exceeds 10 ms, in two runs.
- task-j04's crash points, with the crash now possible while the pointer's job is queued or in flight, recover the previous or the new baseline, every time.
- task-d54's and task-d51's tests pass unchanged.

**Review boundary:** Which thread runs the pointer's append and the retirement, and how the domain thread learns their outcome. No change to the publication order, to the pointer's format or to recovery.


<a id="task-d67"></a>
### task-d67: Keep the fast path past a pre-acceptance the leader never ordered

**Prerequisites:** task-d24, task-d34, task-d71.  
**Design:** Sections 4.1, 4.7, 4.9 (amended in this task's first PR).

**Implement:** Run E on #98 (37576818613, five voters, one boot each, no faults) made no fast decision in 46,000 commands, where six other runs of the same day made 3.5 to 11%. Its leader alone of the seven proposed its first command before its peers were connected (`ProposalRepublished` at startup), which places the run's first commands in the window where a collector reaches some voters and not the leader. Locally (`crates/coord-daemon/tests/fast_path.rs`, five voters, the fast set `coordd` builds), one put that reaches fast-set follower n2 and not the leader, and is never presented again, leaves 0 of the next 40 commands fast; the same put at n4, outside the fast set, leaves 40 of 40. A lost first service command reproduces it too, through the commands the leader then refuses for room.

- **The mechanism.** A follower appends a command to its path log at pre-acceptance. The leader never orders it, so no synchronization takes it out, and a record leaves the log only when it executes or a durable Sync releases it (task-d24). The next command the leader orders marks it `reordered` (task-d34's F7), and from then on the follower's head is `reordered_path`, which no leader path equals. Every acknowledgement it sends fails the fast predicate until the next election, and with the fast set the leader and two followers, one such follower is enough.
- **First, the design step.** When may a follower take a pre-acceptance the leader has not ordered out of its path log, or out of its table, without letting an acknowledgement claim a history its records do not hold, and without losing what recovery needs to rebuild a possible fast decision? The candidates are a bound on the leader's synchronized order past the command (the leader's sequence has moved on and its proposals carry other dependencies), a bound in time, and the leader saying it refused the command (a refusal is already an answer to its submitter). The step states the rule and its argument before it is built, and how a command the collector presents again afterwards is ordered.
- **The design step is taken** (design Sections 4.1, 4.7 and 4.9, with task-d68's, as one step). A follower lets go of a pre-acceptance in its path log, not in its table, at the first synchronization past it: by the acceptance guard it adopts the leader's proposals on a key in the leader's order, so a command still pending before an adopted one was not ordered before it. The command is set aside: the head stops digesting it, its record, phase and acknowledgement stay, and every later pending command goes with it. The key's latest command does not move: the next pre-acceptance depends on the set-aside command, which can still be decided, and is slow on its dependencies (moving the key's latest to the chain's tail, as review asked, let protocol_sim decide two conflicting commands with neither depending on the other; see #157). This replaces task-d34's `reordered_path` marker, which kept the head off every leader path until the marked command was synchronized, released or retired. The argument is the invariant the marker protected, an equal path means an equal history: every command on the chain a head digests holds, at that replica, the order a leader that ordered the chain gave it, and setting aside only shortens the chain. Recovery reads records, not logs, and nothing is released in the ballot, so possible-fast selection and its at-source rule are unchanged. A time bound and the leader's refusal are not used: a leader out of room sends nothing (below). A command the collector presents again after it was set aside is answered from its record (its old acknowledgement, slow), and when the leader orders it, its synchronization is applied as any other: it is above the log's position, so it moves it, and the suffix, none of which chains through it, is set aside.
- **What the code says that this entry did not** (a read of the code for the design step):
  - A command leaves a log's pending suffix at its own synchronization, at a release, or when the log is rebuilt. Retiring it does not take it out: `PathLog::forget` clears its reordered mark only, so the next synchronization marks it again, for good. That catches every command installed by catch-up (appended, adopted without a synchronization, retired at once) and every command a deposed leader retired before it installed the new Sync, since a deposed leader keeps its whole log. Run E's 0 of 46,000 fits this as well as an orphan. The rule covers both: a command installed decided is not appended, and a retired command still pending is set aside.
  - A leader out of room (`Backpressure`) sends nothing to anyone, so there is no refusal to wait for. The collector does present the command again, to every voter, five seconds after its first offer while it is neither released nor learned, which the test's cluster never does.
- **Then the rule**, with what task-d62 counts beside it: the pre-acceptances a follower holds that the leader has not ordered, and the oldest one's age.

**Acceptance:**
- `the_fast_path_resumes_after_a_pre_acceptance_the_leader_never_ordered` passes (it is ignored until then), with the orphan at one fast-set follower and at both.
- task-d30's protocol simulator, every row at its usual seeds, and task-d34's and task-d24's tests, unchanged; a schedule built for the rule's edge (the command presented again just after the follower lets it go) is added and passes.
- On the Jepsen runner, a run with the voters' links held back at start makes fast decisions at the rate of a run without.
- On the Jepsen runner, the leader's fast share at one caller and at ten, at 100 uniform keys, replay, two workers, read against Section 2.2's 56% and 6%. At 30 clients it stays at Section 2.2's 1 to 2% with this task's code, which is that section's bound, not this task's (task-d65).
- task-d30's rows keep F7's seeds 52 and 86 when F7's two tests are rewritten to the set-aside rule.

**Review boundary:** When a follower lets go of a pre-acceptance the leader never ordered, and what it acknowledges after. No change to the fast predicate, to the leader's order or to recovery's selection.

<a id="task-d68"></a>
### task-d68: Synchronize a follower's path log across a leader change

**Prerequisites:** task-d24, task-d34.  
**Design:** Sections 4.1, 4.7 (amended in this task's first PR).

**Implement:** task-d62's counts, on five voters in one process (`crates/coord-daemon/tests/fast_path.rs`), show the fast path off after every leader change for as many commands as the old ballot ordered, and one more: 3 commands under the genesis leader leave the next 4 slow under its successor, 20 leave 21, 40 leave 41, whichever voter wins, all of them `path` at the new leader. On the Jepsen runner a leader fault early in a 46,000-command run would leave the fast path off for most of what follows.

- **The mechanism.** A follower's path log takes a synchronization as its new prefix only when its leader sequence number is above the highest it has taken (`PathLog::sync`, prototype `Update`). A new leader numbers its proposals from zero (`Leader::from_recovered`), and nothing resets a follower's highest at the ballot change: after 20 commands every follower's `synced_seq` is 19 before the change, after it and after the new leader's first command. Until the new leader's numbers pass 19, each follower chains its paths from the old ballot's head, and every acknowledgement it sends disagrees with the leader in its path.
- **The leader's own log.** A leader appends each command it proposes to its own path log and never synchronizes it there, and retiring a command forgets its synchronization, not its place in the pending suffix: a leader that ordered 10,150 commands holds 10,150 pending (a local five-voter run of task-d62's second part). Memory that grows with every command led, and nothing reads it while leading; the role change clears it. It is fixed with the first part or stated as bounded.
- **First, the design step.** What a follower's synchronized prefix is across ballots: sequence numbers compared within one ballot only, a log re-anchored when the follower installs the new leader's Sync at the anchor the new leader proposes from (`PathLog::anchored`), or both; and why recovery's possible-fast rule, which reads the paths a report carries, is unaffected. The step states the rule and its argument before it is built.
- **The design step is taken** (design Sections 4.1 and 4.7, with task-d67's). A log's position is the pair of the ballot whose leader numbered it and that number, compared in that order, so a new leader's first proposal is above everything an earlier ballot synchronized. A Sync's entries carry the numbers of the ballots they were ordered in: they take their commands out of the suffix (and set aside what was pending before them and after), and never move the position. The re-anchoring option is not taken: the new leader's tails come from its own table (its committed tails, its last executed command and what it re-proposes), not from the Sync, so a follower cannot compute the anchor. The cost is that the commands a follower pre-accepts before it adopts the new leader's first fresh proposal are slow; re-proposals carry their old paths, which no fresh path chains from. A leader synchronizes each of its own proposals as it makes it: its head is unchanged, its suffix stays empty, and a deposed leader carries none into its follower role. Recovery's possible-fast rule reads the paths records carry, and no record changes.

**Acceptance:**
- `the_fast_path_resumes_after_a_leader_change` passes (it is ignored until then), with the new leader inside and outside the deposed leader's fast set.
- task-d30's protocol simulator, every row at its usual seeds, and task-d34's and task-d24's tests, unchanged.
- A leader's path log holds a bounded suffix over a long run, read from a count beside task-d62's unordered pre-acceptances.
- On the Jepsen runner, a run with a leader fault makes fast decisions after the fault at the rate it made them before, read on new leaders whose fast set is whole. A fast set holding the voter just killed is task-d76's.

**Review boundary:** How a path log's synchronized prefix moves across ballots, and what a leader keeps of its own. No change to the fast predicate, to the leader's order or to recovery's selection.

<a id="task-d69"></a>
### task-d69: Release the outbox by what a completion makes releasable

**Prerequisites:** task-d45, task-d46.  
**Design:** None.

**Implement:** A local call-graph profile of five voters on task-d60's second PR put `Outbox::release` at 5 to 7% of every follower's loop, most of it its own time, not what it calls (`crates/coord-core/src/outbox.rs`, `release`). It runs at every round of every turn, after the flush (`crates/coord-daemon/src/node.rs`, `Node::carry_out`), not at every completion: each call drains every pending send into new lists, checks each one's ballot and barriers and builds the list back, and the same round walks the pending sends again to count the withheld ones. Its cost is twice the sends waiting, on every round of every voter, whatever became durable: task-d46's shape, per-event work that grows with what is held.
- **The index.** At `publish`, a send whose requirements already hold goes to a ready list; any other is filed under the one thing it still waits for: its newest required barrier not yet durable, or its `required_journal_seq` when that is the later of the two (`durable_through` only rises, so a map keyed by sequence releases a prefix). `observe` moves what a `JournalDurable` makes ready, checking the send's other barriers as `release` does today (they become durable in order, so the check is one lookup), and `release` hands over the ready list.
- **The ballot and failures.** The obsolete-ballot check moves to where the ballot changes (the leader and the follower already release on a promise), and a failed barrier drops what is filed under it, by the same index.
- **The withheld count** becomes a length.

**Acceptance:**
- A unit test with thousands of sends waiting on a far barrier and one completion releasing one send visits one.
- A debug check compares every call with the full walk across the consensus and daemon tests: what is released, what is dropped and why, and the order (`SendWhenDurable` effects are carried in the order released), all unchanged.
- task-d30's protocol simulator, every row at its usual seeds, unchanged.
- In a pair on the Jepsen runner (three pairs, replay, throughput, the call graph on one pair): the followers' loop per command falls, and `Outbox::release`'s own time leaves the follower's profile.

**The pair**, on the runner (the notes' task-d69 section has the local one), against task-d60's second PR: `Outbox::release` left the follower's profile, its own time 15.0 to 1.6 µs per command and with what it calls 18.2 to 4.1, and the leader's 12.7 to 1.4. The index's upkeep moved to `publish` and `observe` (on the follower 0.4 to 8.1 and 1.1 to 10.2 µs, where the base may have inlined some of theirs into their callers), so the outbox's three entry points cost 19.7 µs per command on the base against 22.4, within one profile's noise; the leader nets −1.8 µs, and every loop is flat (the follower's 331 against 330 µs). At 30 clients without faults little is held and the walk was cheap; what the index removes is the walk's growth with what is held, under a slow journal, a lagging peer or a fenced follower. Kept as is. If `publish` and `observe` ever outweigh the rest of the outbox in a profile, the dense increasing orders allow a cheaper structure than a `BTreeMap`.

**Review boundary:** How the outbox finds the sends a completion releases. No change to what is released, when, or in what order.

<a id="task-d70"></a>
### task-d70: Fewer datagrams and wakeups per command on the transport

**Prerequisites:** task-d61, task-d62.  
**Design:** None.

**Implement:** On the Jepsen runner's profile of the leader's transport workers (#98, three CPUs), they cost 1.1 to 1.4 times the leader's domain loop per command, and on the two faster runner CPUs they are the larger part of the voters' CPU per operation. The largest single cost is the UDP send call (19 to 40% of their CPU, 11.7 send calls per command against 6.6 peer datagrams); then parking and waking (7 to 18%; 17.4 futex and 7.0 `epoll_wait` calls per command across the leader's threads, most of it the I/O driver's `epoll_wait` and a worker parking itself); quinn's own work and packet protection are smaller.
- **Send calls follow datagrams.** quinn-udp sends one `sendmsg` per transmit and batches only with GSO, which coalesces datagrams of one connection that are ready together; a leader's turn sends to each peer on its own connection, so the calls cannot fall below one per peer per turn. What can fall is the number of datagrams.
- **ACK-only datagrams.** `lane::transport_config` sets no acknowledgement frequency, so quinn acknowledges every second ack-eliciting packet within 25 ms, and with about one data datagram per peer per command the acknowledgement often travels alone. Both ends negotiate QUIC's acknowledgement-frequency extension, so a higher ack-eliciting threshold lets it ride on the reply that follows; loss detection waits that much longer. The delay is not the lever: QUIC's own is 25 ms, so a shorter `max_ack_delay` sends an acknowledgement sooner, alone, on a connection with little traffic.
- **Wakeups follow streams.** Each accepted peer stream is read by its own spawned task, which hands its frames to the lane's channel and wakes the domain thread; the outbound side wakes a sender task, quinn's connection driver and the endpoint driver. The work per event is small and spread over the workers, so most events land on a parked worker.
- **In order, each a pair:** the runtime's worker count (`TOKIO_WORKER_THREADS=2`, no code); the acknowledgement cadence, behind an environment gate as `COORDD_PEER_STREAM_FRAMES` is, on the peer and API lanes, with the API plane's datagrams counted beside the peer plane's; then, if parking is still where it is, the inbound path without a task per stream (the accept task reads its streams, or a wake hands the domain thread a batch of frames). More commands per turn, a loop waiting a bounded few hundred microseconds for more submissions, would halve datagrams per command at that much latency; it is a trade stated here, not taken. task-d73 sizes it (#174 review).

**Acceptance:** Each lever in a pair on the Jepsen runner (three pairs, replay, throughput, the call graph on one pair, at the default worker count unless the lever is the count): kept only if the tokio threads' CPU per operation falls, `ok`/s does not, and read p99 stays within the pairs' spread; datagrams, send calls, acknowledgement frames, futex and `epoll_wait` per command are reported beside it, and task-d62's streams per command are the control.

**The pairs** ([notes](../tuplesky-impl-notes.md#the-acknowledgement-cadence-task-d70)), on the runner, three each:
- **The cadence at `8,2000`** (threshold 8, a 2 ms delay): the leader's peer ACKs fell 12 to 17%, but the api plane's rose 21% and its datagrams 4 to 7%, CPU and read p99 flat. 2 ms is shorter than QUIC's own 25 ms, and quinn takes a requested delay on every connection, so on the api plane's quiet connections an ACK left alone sooner.
- **The cadence at `8,25000`** (threshold 8, QUIC's delay): a sixth of every voter's datagrams (peer −18%, api −17%), two thirds of the leader's peer ACKs and half the api plane's, one send call per command fewer on the leader; the voters' tokio threads −8% CPU per operation, the voters −4%, `ok`/s +2%, every pair the same sign, read p99 flat. Most of the saving is the send system call; futex and `epoll_wait` hardly move.
- **The runtime's worker count at two** (against four, on 4-CPU runners carrying five voters): the voters' tokio threads −8% CPU per operation, the voters −3%, `ok`/s and read p99 flat. The saving is the workers parking and waking each other (the leader's tokio threads made 6.2 futex calls per command against 12.4); the sends, the receives and the I/O driver's `epoll_wait` are unchanged.

**Kept, as a default:** threshold 8, the delay left to the peer (`AckFrequency::DEFAULT`, `max_delay: None`), on both planes and for every endpoint built from `Limits::default()`, the SDK's included; `COORDD_ACK_FREQUENCY` stays the override, `off` asking nothing (QUIC's own cadence) for a control pair. A higher threshold has little left to take (0.6 of the leader's 5.4 datagrams per command are ACKs at 8) and would learn a loss later on a quiet link. Before it merged it was run under faults (kill, pause and partition, 300 s, two workers; [#98 6071656383](https://github.com/tuplesky/tuplesky/pull/98#issuecomment-6071656383)), two runs at `8,25000`, the same cadence as the default since the peer's own delay is 25 ms, beside two unset: all four `:valid? true` with final reads on all five, no stop and no republish after the final start; lost frames as many without the cadence (1,278 in one unset run) as with it (642 and 2); and the slowest voter serving again after a fault at a most of 95 to 134 s with it and 90 to 125 s without, the medians 2 to 34 s in both, so the faults' overlap and not the cadence sets the spread. The worker count is the runner's setting (five voters on four CPUs oversubscribe the runtime) and not yet the daemon's: an open row under task-d66.

**The third step is scheduled (#174 review),** no longer "if parking is still where it is": it is, at 7 to 18% of the tokio threads. The inbound path without a task per stream (the accept task reads its streams, or a wake hands the domain thread a batch of frames), up to 0.1 ms of CPU an operation at the register row. Its local pair is in its own PR, and its runner pair is read in the batch job (task-d72).

**Review boundary:** QUIC's acknowledgement cadence, the runtime's worker count and the inbound delivery path. No change to what is sent, to the wire format or to the peer capability.

<a id="task-d71"></a>
### task-d71: Name the order the possible-fast rule reads, and keep the reserve's bound

**Prerequisites:** task-d09, task-d26, task-d33, task-d34.  
**Design:** Sections 4.9, 13.1 (amended by this task).

**Implement:** task-d30's simulator at 1,000 seeds per row and size, ten times task-d34's bar, found three stops at the base of task-d67's code branch, two with catch-up and one without, and task-d67's set-aside, which makes more fast decisions, reached a fourth:
- **Row 5, three voters, seed 822: the possible-fast rule dropped a fast decision.** 1aac was learned fast in ballot 8 after f908, which no ballot decided; the adopted command on its key, 8e75, waited on another undecided command, de92. The rule reads an adopted command that waits on a re-proposal, beside a candidate that does too, as unordered against it (task-d33, row 3, seed 70), and re-proposed 1aac under other dependencies than the frontend learned it with. Neither command can have executed anywhere, so the only order either executes in is the one the selection's re-proposals give them: the new leader re-proposes the undecided commands one after another in identity order and an entry right after the last one it waits on (`Leader::from_recovered`), so two entries whose last differs are chained by the re-proposals between them. Only two entries that wait last on the same re-proposal are re-proposed beside each other with no edge (row 2, seed 3), and only then is the candidate re-proposed.
- **Row 3, five voters, seed 158: a follower held more records than its capacity.** In ballot 0 with no Sync, node 4, behind under backpressure, admitted the held proposals whose turn had come (task-d09) past its capacity, each as soon as the one before it was adopted and before it executed, to 34 records against 32. Section 13.1 bounds the table by its capacity, an eighth of it reserved, and lets only recovery work, which the largest Sync and catch-up page bound, past it. A command whose turn has come now takes the reserve and one record past the capacity, counted from the table so a restart that restores a full table changes nothing (row 3, three voters, seed 800, without catch-up, passed a remembered bound across a restart): it is adopted at once and is the one execution needs next, so its execution makes the room. The new leader's re-proposal of a command its Sync names is that Sync's recovery work and passes the capacity as the Sync's entries do (task-d24's capped-Sync test stopped without it).
- **Row 12, three voters, seed 387, without catch-up: two orders.** The new leader had executed 2754 long ago and forgotten it; a voter behind still reported its own pre-acceptance of it, so the selection re-proposed it, and kept 8e84, which that voter pre-accepted after it, as following a re-proposal. The new leader does not chain a command it executed after the recovered order (`Leader::from_recovered`), so 8e84 went out with 2754 alone behind it, on no order against 6de1, which conflicts with it, and one voter executed 8e84 where another had executed 6de1. The selection now takes the commands the candidate decided itself (`select_decided`): one of them is no re-proposal a candidate waits on, and 8e84 is judged, and ruled out, as any candidate is.
- **Row 14, three voters, seed 11, on task-d67's branch.** The member's closure of 36aa stopped at 7c70, which it had executed and retired, before 170c, an adopted command it still held: reclaim retires in executed order but skips a record it may not retire yet, so a later one can go first. The rule's reading through a retired command (task-d33, task-d34's `forgotten_before`) needs the selection to hold the retired command, and none did, so 36aa was re-proposed under other dependencies than the frontend learned it with. Execution is one order at every voter, so the candidate's own says what the member's would: the candidate executed 170c before 0875, which 36aa follows, so 170c precedes 36aa. The candidate now gives the rule the order it executed commands in (`CandidateDecisions::executed_before`), one ordinal per executed command as its `history` already keeps one identity, and a conflicting adopted command executed before a command in the candidate's closure precedes the candidate. Where the candidate did not execute both, the rule rules the candidate out, as before. No report carries a position: that would change the wire form of every report and need a mixed-version gate (Section 11.7), and the durable positions with it. Read alone, that order let the sweep stop again, at row 10, three voters, seed 884, without catch-up: the candidate had executed b723 long ago, a member behind held its own pre-acceptance of b723 under dependencies that reached 3c92, committed after b723, and the selection kept b723 under them, a cycle the candidate halted on. ce96, the first command executed, had ruled b723 out before, by a check that stops at the first adopted command it cannot order. Two candidates are now never kept: a command the candidate committed or executed, whose decision the member's record is not, and one whose own dependencies come back to it through the selection.

The design step names the order the rule reads, so a hole is not patched as a fifth case in its walk: the selection's entries and the candidates it keeps, with the chain its re-proposals make; the member's records along the candidate's dependencies; and the executed order over what the member retired, the candidate's own. A record keeps the path it was pre-accepted with while the member's log is re-based after it, so the member's records are read along dependencies, not along per-key paths. Then one deterministic test per shape, as task-d33 and task-d34 did.

**Acceptance:** Each shape has a deterministic test that fails without its change, and its seed is kept in `seeds.json` with what it found, and whether catch-up was on; row 14's once task-d67's branch is rebased on this one. Every row passes at 1,000 seeds per row and size, with catch-up and without, and a scheduled workflow runs that sweep weekly. Without catch-up the simulator holds every oracle but progress after healing: a voter left behind has no means to heal, and the base does not either. CI's 12 seeds stay.

**Review boundary:** Recovery selection's possible-fast rule and what the candidate tells it, the table's admission of a command whose turn has come, the simulator's healing oracle without catch-up, the sweep's workflow and the kept seeds. No change to the re-proposal chain, to the report format, or to the fast predicate.

<a id="task-d72"></a>
### task-d72: Close the gap to etcd on one host

**Prerequisites:** task-d62, task-d63, task-d66 (their pairs are this entry's first rows).  
**Design:** Section 2.2 (unchanged: the conservative key stays).

**Implement:** The one matched comparison, task-d51's register row at 30 clients, served 422 to 505 `ok`/s against etcd's 1175, at 3.5 to 4.7 ms of server CPU an operation against etcd's 1.39. TupleSky left 0.65 to 1.48 cores idle while the leader's loop queued for a CPU; etcd left 0.04. The order is not the gap, since Raft keeps a total order too. Four costs make the CPU and the latency:
- **Messages per command, not per turn.** Fast and slow acknowledgements go to every other voter and the frontend, as the reference's do: on the order of forty protocol messages a command at five voters, against Raft's eight, which carry many entries each. task-d61 cut streams per command by 60% and left about nine frames a command at the leader.
- **Durability per command:** a payload and dependency row, an acceptance row and the execution, in separate batches at every voter, against one WAL append per batch of entries.
- **The leader's single loop** as the serialization point, and about 690 µs a command on its transport threads after task-d70.
- **Latency on the closed loop:** 7.8 ms from a group closing to its release, 6.6 of it the projection's commit that task-d63's build removes, and publication stalls of 22 to 65 ms that task-d66's job removes.

This entry is the budget and the record, not a lever:
- **The measure:** CPU per operation at the matched shape against etcd's 1.39 ms, split by the leader's loop, its transport threads, the journal and the followers, with `ok`/s and p99 beside it.
- **The target that closes the entry:** CPU per operation at the register row within twice etcd's (2.8 ms), and the `service` shape's p99 within etcd's spread at the same rate. The first rows may revise it, with the reason recorded beside them.
- **The profile the `service` target is read at: `replay` (#174 review).** etcd's backend commits on an interval, with the WAL the only sync on a request's path, which is `replay`'s shape.
  - Under `strict`, every projection commit is durable before the snapshot gate moves (`ProjectionProfile::Strict`). So a barrier read's snapshot wait includes the materializer's fsync, about 4.4 of a barrier read's 12.4 ms in the last run.
  - The `strict` row stays in the record as its own line, read on its own and not against etcd. No lever here owns it.
- **The levers, in order,** each with the number it is expected to move and the pair that checks it: task-d63's build (latency: about 6.6 ms a command on the leader at closed loop), task-d66's job (latency: the stalls), then task-d73, task-d74 and task-d75 (CPU). Only the last three are expected to move CPU per operation.
- **Where the gap stands after the #173 carry (#174 review).** No single remaining lever is projected to close either target. Each of these is likely needed:
  - **The register row's CPU:** 3.58 ms against the target's 2.8, so 22% to go.
    - The voters' tokio threads are the largest part, at 1.45 ms. task-d74 halved frames a write locally for 4.5% of CPU, so per-frame cost isn't what those threads spend. The tokio-thread profile, with each thread's run-queue delay beside its CPU, comes before task-d73, and task-d73 is re-scoped to what it names.
    - Then task-d84, whose reading reads the register row's syncs a command first.
    - task-d27 too, if the 900 s `service` run shows cost a command rising with history. R2 found a step, not a slope: the publications' own cost, which task-d92 owns (task-d27).
    - task-d83 doesn't move this row: its reads are single keys, already at the barrier.
  - **The `service` row's latency:** task-d83 takes its ordered commands from about 1.03 an operation toward 0.4, and task-d84 its syncs. task-d84's reading decides whether merging syncs can reach the p99 at all (task-d84).
- **The batch (#174 review).** Every lever the readings on record name is planned now and built as one batch, so the next runner job measures the batch, not one lever at a time. Sizes are estimates unless marked measured.
  - **The register row's CPU,** from the #173 carry's 3.58 ms an operation. On the runner the voters' tokio threads take 1.45 ms, the domain loops 1.28, and the rest about 0.85; locally the rest is mostly the materializer (a fifth of each node) and the appender (7 to 9%).

    | # | Lever | Owner | Status | Size (ms an operation) | Running total before overlap (ms) |
    | --- | --- | --- | --- | --- | --- |
    | C1 | Threshold 8 as the acknowledgement cadence's default | task-d70's second step (#155) | Built, and merged into the stack with this record | −0.15, measured | 3.43 |
    | C2 | Acknowledgements to the leader and the frontend only | task-d74 (#167) | Built | about −0.16 (−4.5% locally) | 3.27 |
    | C3 | A turn takes every ready event before it lowers | task-d47, built | Already the code (corrected below); task-d73 reads R1 and sizes a bounded wait | not counted | |
    | C4 | Fewer projection commits per command | task-d86 | Planned: the runner's group sizes from the #173 carry's artifacts, then R1 | −0.15 to −0.30 | 2.97 to 3.12 |
    | C5 | The inbound path without a task per stream | task-d70's third step | Scheduled | up to −0.10 | 2.87 to 3.12 |
    | C6 | The leader's lookups as hash maps, broadcasts from one buffer | task-d60's third step | Scheduled | −0.04 to −0.07 | 2.80 to 3.08 |
    | C7 | Held reads share one confirmation round | task-d87 | Planned, its reading first | −0.02 to −0.04 | 2.76 to 3.06 |
    | C8 | `load_authorization` once a session | task-d60's third step | Scheduled | about −0.02 | 2.74 to 3.04 |
    | C9 | The api plane's datagrams | task-d88 | Planned, reading R3 first | unknown | |
    | C10 | A local checkpoint publication's cost a record | task-d92 | Planned, a profile of one publication first | up to −0.18 at the register row's length | 2.56 to 3.04 |

  - **C3 is already the code (correction, #174 review).** task-d47 groups the ready events: the loop flushes only once no event is ready, or after 64 (`FLUSH_EVENTS`), as `Node::lower_in_groups` says, and in `serve.rs` the `owes_flush` arm comes after every event arm of the biased select. `Cost::lowerings` counts "a journal group, a projection commit or both", so task-d75's 8,870 lowerings are its 4,648 appends and 4,222 commits, and 1.14 commands a lowering is not a turn's size. A group holds 2.2 commands an append and 2.4 a commit, and ends because nothing else is ready or an append is out. So C3 is out of the running total until R1 finds a group that ends while events were ready. What remains of the idea is task-d70's bounded wait, a latency trade, which task-d73 sizes against the `service` p99 as well as the register row's CPU.
  - **Projected:** 0.54 to 1.02 ms before overlap. C1 and C2 both cut datagrams, so after overlap about 0.5 to 0.95, which puts the row at roughly 2.6 to 3.1 ms before C9.
  - **So the revise-the-target clause is now likely to apply.** Reaching 2.8 ms needs the top of that range, or C9 and C10 larger than sized, and the readings behind C4 and C10 are local. This record says so now, before the batch is built.
  - **Read from the #173 carry's artifacts, with no new run:** commands an append and a commit at each voter, from each `metrics` line's `executed`, `journal_appends` and `projection_commits`, before C4 is sized (an append takes about 2.3 ms on the runner's disk, so groups may already be larger there than on tmpfs); and each publication's `checkpoint` line (task-d55), which gives C10's size at the register row exactly.
  - **Not levers:** the fast share at 30 clients is Section 2.2's bound (task-d65); fewer send calls than datagrams a peer a turn needs GSO, which quinn-udp uses only within one connection (task-d70), so C1 and C2 are the send-call levers, with task-d73's wait if it is taken.
  - **The `service` row's p50 and p99,** 23 and 61 ms against etcd's 2 and 4, read at `replay`:

    | # | Lever | Owner | Status | What it removes |
    | --- | --- | --- | --- | --- |
    | L1 | Read-only transactions at the barrier | task-d83 (#176) | Built | ordered commands from 1.03 an operation toward 0.4 |
    | L2 | The `replay` profile | a run setting | | the projection's fsync from a barrier read's snapshot wait, about 4.4 of its 12.4 ms at `strict` |
    | L3 | A fresh proposal's acceptance in its proposal batch | task-d89 | Planned, its design step first | one of the leader's three syncs in series at low load, about 2.3 ms on the runner |
    | L4 | Release before the execution group is durable | task-d90 | Its design step started now | the execution group's sync after learning, a 3.69 ms wait in the last run |
    | L5 | Held reads share one confirmation round | task-d87 | Planned | the 1.2 ms confirmation under load, and leader CPU |
    | L6 | Wake and hop latency | task-d91 | Planned, reading R4 first | run-queue waits: the tokio threads 15 ms an operation for a core with 1.87 idle, the leader's loop 1.35 ms a command against 0.65 of CPU |

  - **Projected:** a barrier read's 6.9 ms wait for its index is the in-flight writes' latency, so L3 and L4 shorten reads too. After L1 to L4 a write is one sync, at the leader and the followers in parallel, plus about two hops: roughly 3 to 4 ms on the runner before queueing, etcd's shape. p99 within etcd's spread then rests on L6, and perhaps a revised target, decided from the batch's local readings.
  - **The readings that size the levers no task owned,** all local but R4: R1 for task-d73 and task-d86, R2 for task-d92 (and task-d27's split, for the long run), R3 for task-d88, and R4 for task-d91, read from the runner's summary on the batch's base side with no extra run (the summary column is the testing session's).
  - **If the projection falls short** of 2.8 ms or of etcd's p99 once the local readings are in, which is now likely for the CPU target, the target is revised under this entry's clause, with the reason beside it, rather than run again to find out.
- **The batch, as built (task-d72's batch record).** Each lever's local reading is in its own PR. The tables above stay as they were sized; this is what the readings made of them.

  | # | Lever | Owner | Outcome | Size (ms an operation) | Running total before overlap (ms) |
  | --- | --- | --- | --- | --- | --- |
  | C1 | Threshold 8 as the acknowledgement cadence's default | task-d70's second step (#155) | Built, merged | −0.15, measured | 3.43 |
  | C2 | Acknowledgements to the leader and the frontend only | task-d74 (#167) | Built, now on the stack, with L3 stacked on it | about −0.16 (−4.5% locally) | 3.27 |
  | C3 | A turn takes every ready event before it lowers | task-d73 (#182) | R1: almost no group ends while events are ready. 97.2 to 99.9% of flushes find nothing ready, and the rest are at most 0.013 a command. The bounded wait is not taken | 0 | 3.27 |
  | C4 | Fewer projection commits per command | task-d86 | Re-sized by R1 (#182): the materializer's cost is about 60 µs a commit and 115 µs a record, so per record more than per commit. Not built; it waits for the runner's group sizes (#98) | −0.03 to −0.09, not in the batch | 3.27 |
  | C5 | The inbound path without a task per stream | task-d70's third step (#183) | Measured, not kept: the tokio workers' share and CPU a command don't move, and their wake-ups fall 3 to 8% | 0 | 3.27 |
  | C6 | The leader's lookups as hash maps, broadcasts from one buffer | task-d60's third step | Not built: B-tree searches are 1 to 2% of a leader's samples locally after task-d75 | −0.02 to −0.04, not in the batch | 3.27 |
  | C7 | Held reads share one confirmation round | task-d87 | Not built: R1 reads 1.15 reads a round at 10 callers and 2.3 at 30, so rounds are already shared | −0.02 to −0.04, not in the batch | 3.27 |
  | C8 | `load_authorization` once a session | task-d60's third step | Not built: 0.8% of a leader's samples locally | about −0.02, not in the batch | 3.27 |
  | C9 | The api plane's datagrams | task-d88 (#187) | R3 read; its lever (several returned frames a stream on a collector's link) measured, not kept: 13% fewer api datagrams, no CPU change | about −0.02, not in the batch | 3.27 |
  | C10 | A local checkpoint publication's cost a record | task-d92 (#184) | Built: about 30% less a record, and 2 to 3.6% less process CPU a command at every voter, at R2's setup | about −0.05 | 3.22 |

  | # | Lever | Owner | Outcome |
  | --- | --- | --- | --- |
  | L1 | Read-only transactions at the barrier | task-d83 (#176) | Built; on the stack, so in the next carry |
  | L2 | The `replay` profile | a run setting | Read on both sides |
  | L3 | A fresh proposal's acceptance in its proposal batch | task-d89 (#180) | Built: the leader's syncs a command fall from 2.91 to 2.00 at one caller, and 14 to 18% at ten |
  | L4 | Release before the execution group is durable | task-d90 | The design step is up for review (#181); no code before it is accepted |
  | L5 | Held reads share one confirmation round | task-d87 | Not built (C7) |
  | L6 | Wake and hop latency | task-d91 | R4 is read from the batch's base side |

  - **Projected for the register row:** about 3.22 ms an operation, against the #173 carry's 3.58, from C1, C2 and C10. Every lever left out, each at the top of its size, would bring it to about 3.03. So no lever on record reaches 2.8 ms in this batch.
  - **The CPU target for this batch stays 2.8 ms (#185 review).** The projection, 3.22 ms, comes from the levers built so far. Three local readings that could move it aren't in yet, so the clause doesn't apply until they are. Each is local and goes in its own PR:
    - **R5, where the materializer's cost a record goes** (task-d86's first step). R1 puts the materializer at about 115 µs a command at each voter, besides 60 µs a commit: on the order of half a millisecond an operation at the runner's five voters. The profile is a record's path through the materializer: the rows a command writes, by table, and redb's insert, page and freed-page costs. task-d92's profile of the export found half its thread in avoidable re-encoding, so this is the same reading for the other half of the engine.
      - **Read (local, the head, three voters, 30 callers, `replay`, tmpfs, 150,000 operations):** no avoidable copy like the export's. The materializer's samples are memmove 17.5%, clearing fresh pages 13% (page faults on pages mimalloc has just been handed), redb's `get_page` 11%, its page cache's LRU 7%, and checksums and frees after that. redb's cache is 256 MiB, so it isn't under pressure; the copy-on-write page work is the engine's own. The one lever is the allocator's: never returning freed pages to the OS (`MIMALLOC_PURGE_DELAY=-1`) takes the page clearing off. Over nine alternating pairs it was lower in seven, by a median pair of 4.3% and a mean of 2.9% of the voters' CPU, about −0.1 ms an operation, for 5 to 10% more resident memory (189 to 199 MiB against 182 to 186). That's small, and it's a setting, so it's carried as task-d86's step, not as a code lever.
      - **Read again with a bounded delay (#185 review), and not reproduced.** Six alternating runs a side, the same setup at 50,000 operations, with each voter's resident memory sampled every second. Mean CPU a command over the voters: 2.604 ms at the default 10 ms, 2.627 at 1,000 ms, 2.678 never purging. Inside each side the runs spread 0.4 to 0.7 ms, so none of the three differs from the others. Peak resident memory was 185 MiB at the default and at 1,000 ms, and 200 never purging. So no allocator setting is carried: R5's lever is within the spread, and the materializer's cost stays the engine's own.
    - **R3 on the head, the api plane's datagrams by kind** (task-d88). C5 showed that the tokio workers' cost doesn't follow stream tasks, and C1 and C2, the levers that did move it, both cut datagrams. After them the api plane is the larger plane. It's read on the head, since #167 and #180 change what crosses it: submissions, fast and slow acknowledgements, releases and the client's own traffic, with ACK-only datagrams split out.
      - **Read (#187):** the api plane sent about 10.0 datagrams a command against the peer plane's 4.1, because every frame between a collector and a voter was a stream of its own. The lever that names, several returned frames a stream on a collector's link, was built and paired on the head: the api plane's datagrams fell 13% and the voters' CPU didn't move (2.379 against 2.356 ms a command, two pairs each way). So it isn't kept, and R3 is small.
    - **R6, a fresh profile of a leader's and a follower's domain loop on the head,** at the register row's shape. The last ranking of what the loops spend is task-d75's, and this record shows it's stale: B-tree searches went from about 8% of the leader's samples to 1 to 2%. The loops are 1.28 ms an operation on the runner.
      - **Read (local, the same run as R5's):** no single lever. The leader's loop, inclusive: `Leader::step` 25%, the stops after a settle 20%, `pump_reads` 13%, `RedbView::get` 8%, `scan_page` 8%. By self time it's the allocator at about 19%, memcmp 9% and page faults 8%. A follower spends 10% in `settle_from_records`, half of it in `conflicts_for`. These spread over task-d60's third step (C6, C8: hash maps and one-buffer broadcasts, which go at the allocator and memcmp) and task-d87 (C7, `pump_reads`), each small.

    Then:
    - **If all three come up small,** the target is revised under the clause, with them as the reason. The gap that remains is then spread across the engine, the transport and the loop, with each part read.
    - **If any is large,** it's built, the batch is re-projected, and the batch job runs once.

    **All three came up small, so the target is revised under the clause.** R5's one lever, an allocator setting, read about −0.1 ms first and nothing on a second, larger set, so none is carried. R3's lever moved datagrams and not CPU. R6 found no single lever, only the allocator and comparisons that task-d60's third step and task-d87 already own. The projection stays about 3.22 ms an operation. The batch's CPU target is that, at or under 3.25 ms with `ok`/s not lower, and 2.8 ms stays the entry's target, owned by the parts named below.

    Where the gap is now, from the runner's split (tokio workers 1.45 ms, domain loops 1.28, the rest about 0.85):
    - **The voters' tokio workers:** their cost follows neither stream tasks (C5) nor datagrams (R3, #187): 13% fewer api datagrams is about 9% fewer across both planes, 0.02 to 0.05 ms of the workers' 1.45 even at the runner's price of a send. So this bucket has no lever on record. Its owner is the batch's call graph of the tokio threads, split into send, receive, QUIC, the stack's own work, and parking and wake-ups (#185 review), with task-d91's R4 for where they wait.
    - **The materializer:** its cost is per record (R1). task-d86 now starts from R5, a profile of a record's cost, not from the commit cadence.
    - **The domain loops:** task-d60's third step (C6, C8) and task-d87 (C7), each small on its own, and whatever R6 finds.
    - **A publication's export,** now led by redb's scan (task-d92's head profile). That's the engine's cost; R5 reads the same engine from the materializer's side, and task-d27's trim stays the long-run bound.
  - **The `service` p99 target is not projected to be met by this batch,** and is revised to a reading. L3 removes one of the leader's syncs in series, about 2.3 ms on the runner. L1 is already in the next carry. L4 is the long pole, and its design step is up for review (#181). If its code lands while the readings above are under way, the batch carries it, so the `service` row reads L1, L3 and L4 together rather than L3 alone. Whether p99 is then within etcd's spread rests on L4 and L6.
- **How the runner is used from here (#174 review):**
  - **No more single-lever performance pairs on the runner.** Each lever gets its local pair and its simulator sweep in its own PR. Where a lever's acceptance names a runner pair, the batch job is that pair.
  - **One batch job, once the batch is projected to close the gap:** this record puts the head at or under 2.8 ms from the local pairs, with task-d89 and task-d90 built, or the target has been revised. The job then confirms the gap closed rather than reporting a step toward it.
  - **Sides:** the base is the next carry, with #155's default in it, since C1 is measured. The head is that and every other built lever: C2, C4 to C8, C10, L3 and L4; C9 if R3 finds one, and task-d73's wait only if its sizing takes it.
    - **As built (the batch record):** the base is the next carry, `claude/task-d84-journal-syncs` (#179) with #178. The head is the base with three branches merged in:
      - `claude/task-d89-accept-in-proposal` (#180, L3). It now stacks on `claude/task-d74-acks-to-leader` (#167, C2), which holds the stack. The two levers both change what the leader sends and journals, so they are reconciled and swept as one.
      - `claude/task-d92-publication-cost` (#184, C10).
      - `claude/task-d73-group-ends` (#182), which only adds R1's counters, so the summary can read flushes by cause.

      #181 and #183 are docs only. #184 and #182 sit directly on the stack and touch no code the others do. The only conflicts in the merge are the notes' sections, which each branch appends at the end of the notes; both are kept.
    - **Before the batch's pairs, the head also holds the gate change** (#167 review, its own task stacked on #167): the frontend's gate holds a read whose session this node hasn't projected yet, instead of answering it `Pending`. Without it, the head's histories carry more indeterminate reads at followers than the base's, which moves `ok`/s for a reason that isn't CPU.
  - **Runs:** three register pairs, and `service` at `replay` and at `strict` on both sides, with the call graph on one pair, a follower's tokio threads included.
  - **Before the batch, correctness runs, not lever pairs (#185 review):** #167 and #180 change what the leader sends and journals, so they need fault runs before they merge, whatever the batch shows.
    - **The leader-kill run** is made on the head instead of on the base. The head holds everything the base does, #175's liveness target and #178's fix included, so one run covers both.
    - **One run of the standard schedule** (kill, pause and partition, 300 s, two workers) on the same head.
    - **If either stops,** the same schedule runs on the base to say whose it is, and the stop is traced from the voters' stores before it's read as any one PR's.
- **The batch, read (#98, [6619874597](https://github.com/tuplesky/tuplesky/pull/98#issuecomment-6619874597)).** Base `dcaf249` (the shim with #179 and #178), head `b59f2b0` (that with #180, #167, #186, #184, #182 and #187), on one Intel Xeon Platinum 8573C runner a pair. The #173 carry ran on AMD EPYC 7763, so absolute numbers don't compare across carries; the pair is the reading.
  - **Correctness first, all valid.** The leader-kill run on the head, the same run on base and head on one runner, and the standard schedule on the head: `:valid? true`, no stop and no panic, every voter ending equal and every node serving its final reads. #175's gap after a kill is about 6 s on both sides (median 6.1 s).
  - **The register row meets the batch's target:** server CPU an operation fell 0.23 ms (−8.2%, 2.82–2.87 to 2.59–2.62 ms) against the −6% projected, `ok`/s rose 4.8%, and p99 fell about 5 ms. The domain loops fell 0.10 ms an operation and the tokio threads 0.11. Peer datagrams fell by a third; the leader's own streams and datagrams rose about 20%, with the acknowledgements now coming to it (#167). The voters' high-water mark fell 51 MiB (−20%).
  - **L3:** the leader's syncs fell 17% at 30 clients, and in `service` from about 2.6 to 1.9 a command (−26 to −28%).
  - **C10:** export and write per 1,000 records represented fell about 39% combined, against the 30% projected.
  - **`service`:** CPU an operation −6 to −7%, p99 −1 to −2 ms (22 ms at `strict`, 12 at `replay`), not yet within etcd's 3 ms, as projected. No indeterminate reads on either side, so #186 adds no `Pending`.
  - **The tokio threads in five parts** (µs a command, base / head): QUIC's packet protection and connection state is the largest part on both, 171 / 164 at the leader and 131 / 119 at a follower, about a third of the threads. Parking and wake-ups are about a fifth (84–89 µs). Send is the part #167 moved: up at the leader (89 to 105), down at a follower (77 to 60). This is the input for the next step on the workers' bucket.
  - **R4 (task-d91), base side:** the leader's threads wait about 2.1 ms for a core an operation in all; its tokio threads wake 7.2 times an operation and run 34 µs a wake, and its materializer waits longest, 368 µs a wake.
  - **n5's slow catch-up is the catch-up pacer's, and its fix is its own task** (#189; #98, [6621159759](https://github.com/tuplesky/tuplesky/pull/98#issuecomment-6621159759)). In the first leader-kill run on the head, n5, restarted 17 s before the final heal, executed only about 4 commands a second once the load stopped, and ended at 3,387 against 4,127, holding 283 unordered pre-acceptances. A voter asked for a page only once its frontier had been still for 1 s, and any movement reset that. A voter restarted behind decides one command each time the leader's re-send reaches it, every 250 ms, so it never asked, not even the probe after start. The pacer is task-d08's, the same on both sides, so it's neither #167's nor #180's, though #167's routing, which leaves a follower to hear decisions from the leader alone, is where it shows: a local restart test crawled in 3 runs of 6 on #167's head and in none of six on the base. The fix counts a page instead of any movement, and passed the same test 8 runs of 8.
- **The next step on the gap (#185 review).** By #174's rule, the next performance run needs a step projected to close the gap, and the gap is stated again first.
  - **The target, as a ratio to etcd.** 2.8 ms was set against etcd's 1.39 ms on the #173 carry's EPYC 7763; this batch ran on a Platinum 8573C without etcd on the register row. So the target is about 2.0 times etcd's register row read on the same runner in the same job, asked of the testing session on #98. If etcd moved across runners as the base did (3.58 to 2.85 ms, ×0.80), the target on this runner is about 2.2 ms, and the head, at 2.60, is about 0.4 ms (15%) short. That's an assumption until etcd's row is read.
  - **What the split and R4 name, sized locally before anything is built:**
    - **Parking and wake-ups, about a fifth of the tokio threads** (task-d91). The leader's tokio threads wake 7.2 times an operation and run 34 µs a wake. coordd's runtime is tokio's multi-thread one at its default worker count, one a core. A local pair at one worker a voter, behind an environment setting, says whether work stealing and wake-ups across workers are a lever before anything else is built.
      - **Read:** it is. One worker a voter cut the voters' CPU a command 27% at three voters and 12% at five against tokio's default, and 10% at five against the runner's two, on one 4-core host, with latency lower too (task-d91's entry). Its lever, a worker-count setting, goes into the next batch.
    - **QUIC, send and receive, about three fifths of the tokio threads** (task-d96). They scale with packets. The ACK frequency is already 8 (task-d70), and R3 measured per-step coalescing (#187), so what's left is fewer messages a command. That's design work, so task-d96 is a design entry that sizes it from R3's frame counts at each voter count before anyone builds it.
      - **Read:** at one worker a datagram costs next to nothing (an ACK-cadence pair: 2.9 more datagrams a command, no CPU change), so fewer messages a command sizes small and none is carried (task-d96's entry).
    - **The domain loops,** about 1.08 ms an operation on the head. C6 to C8 stay small, as recorded: task-d60's third step and task-d87.
    - **`service` p99,** 12 to 22 ms against etcd's 3: task-d90 (L4, #181) is the long pole. task-d97 reads what the p99 operations wait for first, since R4's means put L4 at a p50-scale gain (#185 review). R4 on that row, asked of the testing session on #98, says how much of it is waiting for a core rather than syncs in series.
  - **Then the next batch** runs once those add up to the re-anchored target, with task-d90's code step in it if it has landed.
- **The next step, revised (#185 review, after #188 to #191): the next runner job closes the gap or names what's left with an owner, not another step toward it.**
  - **Where it stands:** the register row is 2.60 ms an operation on the head, about 2.34 projected at one worker a voter, against about 2.2 (2.0× etcd, if etcd moved as the base did): within about 0.15 ms, and uncertain until `etcd-beside` reads it. `service` at `replay` is 2 / 9 / 12 ms (p50 / p95 / p99) against etcd's 1 / 2 / 3: 4× off at p99, for a structural reason.
  - **L7, a new lever (task-d98):** the slow path pays the leader's sync and then a follower's before a command is learned, and L4 doesn't touch it. Its design step comes first.
  - **task-d90's code step (L4) starts now,** not after the batch's tails. Its design is approved (#181), point 3 is #188, and parts 2 and 4 depend on no reading. Built now, one batch reads L1, L3, L4, L7 and the tails together, rather than one job for the tails and another for L4.
  - **CPU margin, built while L4 and L7 are:** task-d60's third step (C6 and C8, −0.04 to −0.06 ms) and task-d86 (C4, −0.03 to −0.09 ms, now sizeable from the batch's group sizes: 1.92 commands a group, 2.13 a commit, at the head's leader), each with its local pair at one worker a voter. Together −0.07 to −0.15 ms, about the projected shortfall.
  - **One more distribution before the batch** (#191): the journal appender's and the projection's served and queued time a job, at every voter, so the batch can tell a round's p99 that is the disk from one that is queueing or hops.
  - **The batch, once L4 and L7 are in, or L7's design step says no:**
    - **Head:** today's head with #189, #188, #191, L4, L7 and whichever C levers measure, at one worker a voter (#190's `[runtime] worker_threads`, which beats `TOKIO_WORKER_THREADS` under test, or a per-side `voter-workers`).
    - **Base:** the last batch's head, `b59f2b0`, at two workers.
    - **`etcd-beside`** after each pair, with its setup check on the profiled image first: it is the target.
    - **Correctness first:** L4 and L7 both let something leave before it is durable, so the leader-kill run and the standard schedule run on the head before the pairs.
    - **L7 needs task-d99 first** (#192 review): its early send is behind a replicated feature, and only task-d99 makes one activatable in a running domain. The head activates `EarlyProposal` after start through task-d99's command, never through a build flag.
    - **The readings:** three register pairs, and `service` at `replay` on both sides with #191's per-voter tails and the fast share, and at `strict` as its own line.
  - **What the batch decides, set now:**
    - **CPU:** the head at or under 2.0× etcd's row closes it. Above, the record names each remaining part from the split it has: the workers, the loops and the materializer.
    - **`service` p99:** within etcd's spread closes it. Otherwise the tails name the part, and that part gets a task: the round (the journal's served tail says whether it is the disk), the release after L4 (the predecessors or the group), or what's past both (the frontend, the gate and the transport, L6). The target is revised under the clause only if no lever on record can reach it.
    - **If L7's design step says no,** the batch still runs with L4, and the record revises the `service` target with the slow path's two syncs in series as the reason.
- **The record:** the table below, one row a lever, each row a pair on the runner.

| Row | Build | Shape | `ok`/s | Server CPU an operation | Idle cores | Expected to move |
| --- | --- | --- | --- | --- | --- | --- |
| Baseline | task-d51's, `145b71b` (#146) | register, 30 clients, six nodes | 422 to 505 (etcd 1175) | 3.5 to 4.7 ms (etcd 1.39) | 0.65 to 1.48 (etcd 0.04) | |
| task-d63 | | | | | | latency: the projection's commit, about 6.6 ms a command |
| task-d66 | | | | | | latency: publication stalls of 22 to 65 ms |
| task-d73 | | | | | | CPU and p99: its bounded wait, only if its sizing takes it |
| task-d74 | | | | | | CPU: acknowledgements per command |
| task-d75 | | | | | | CPU: the leader's loop |
| The #173 carry (task-d63, task-d66, task-d74's recovery fix, task-d75, task-d77 to task-d80) against `259e0b2` | `36fe7d1` (#98) | register, 30 clients, replay, three pairs | 501.5 to 504.2 (base 505.4 to 513.4) | 3.57 to 3.59 ms (base 3.58 to 3.66) | 0.41 to 0.46 | the leader's loop: -4.1 to -6.6%; the domain loops -3.8%; tokio threads unchanged at 1.45 ms an op |
| `service`, the same carry | `36fe7d1` | wr, 1,000 uniform keys, read-only ratio 0.6, 4 clients at 300/s, strict | 140.0, below the rate (etcd 290.1) | 12.41 ms (etcd 2.86), not at equal work | 1.87 (etcd 2.51) | p50 23 ms and p99 61 ms (etcd 2 and 4): task-d83, task-d84 |
| `service` at `replay`, the matched comparison | | the same shape, replay | | | | the row the `service` target is read at (the testing session) |
- **The local record** (this entry's PR): the same split on one host, five voters, each build against the one before it, with a profile of the leader and a follower ([notes](../tuplesky-impl-notes.md#the-budget-on-one-host-task-d72)). It does not replace the runner's rows, which the Jepsen testing session runs. It reads what a lever moved before the runner does: of a write's 7.9 to 8.7 ms of the nodes' CPU, a follower costs about 1.6 ms and the leader about 1.9, so the followers carry three quarters of it, and off the domain loops the transport's QUIC threads are a third of each node's CPU and the projection's commits a fifth.
- **The shapes it is read in** (`docs/operations/jepsen.md`): `service`, a fixed rate where the metric is latency and CPU per operation at equal work, on the bridge and under the WAN profile; the append shape run against etcd too, so the multiple quoted has a denominator; and several domains on one cluster, if the harness can start them.

**Acceptance:** This entry's PR records the baseline row and task-d63's and task-d66's rows, with the `service` shape's p50, p99 and CPU per operation beside etcd's at the same rate. task-d73, task-d74 and task-d75 each add their row in their own PR. The entry closes when the target is met, or when each part of the gap that remains is named with its owner.

**Review boundary:** A budget, its measurements and the order of its levers. No change of its own.

<a id="task-d73"></a>
### task-d73: Read why a lowering group ends, and size a bounded wait

**Prerequisites:** task-d61, task-d62, task-d72.  
**Design:** None.

**Implement:** Re-scoped twice on #174, and not in the batch's running total (task-d72).
- **A turn already takes every ready event before it lowers.** task-d47's grouping flushes only once no event is ready, or after 64 events (`FLUSH_EVENTS`): `Node::lower_in_groups` says so, and in `serve.rs` the `owes_flush` arm comes after every event arm of the biased select. The 1.14 commands a lowering this entry first read was `Cost::lowerings`, which counts "a journal group, a projection commit or both": task-d75's 4,648 appends and 4,222 commits are its 8,870 lowerings. A group holds 2.2 commands an append and 2.4 a commit, and ends because nothing else is ready or an append is out.
- **First, the reading (R1, with task-d86):** why each group ends, counted by cause (no event ready, the 64-event bound, `FLUSH_QUEUED`, an append out), at 10 and 30 callers locally. A group that ends while events were ready is the only lever left of the original change, and its share sizes it.
- **Then the bounded wait, as a trade:** the loop waits a bounded few hundred microseconds for more submissions before it flushes (task-d70's stated trade). It cuts datagrams, appends and commits a command, and adds up to the wait to every command's latency. It is sized against the `service` row's p99 as well as the register row's CPU, and built only if both say so; the sizing is recorded in this entry either way.
- **The frame kind is not built (#174 review).** This entry's first step, a profile of the voters' tokio threads, is in (#98). With task-d74's pair it says their cost is per packet and per wake-up, about 13 µs a frame, not per frame. task-d70's third step, the inbound path without a task per stream, is the lever on the receiving side.

**Acceptance:** The reading recorded, with the groups' causes, and the wait's sizing against both rows. If the wait is taken: a local pair in which appends, commits and datagrams a command fall and p99 stays within the pairs' spread, the protocol simulator's sweep and the model tests (the wait alters which events share a turn, not the rule applied to any one of them), and its runner pair read in the batch job (task-d72). If it is not, the entry closes on the reading and the frame kind's reason.

**Review boundary:** When the loop flushes. No change to the rules events are handled by, to the protocol or to the wire.

<a id="task-d74"></a>
### task-d74: Send acknowledgements to the leader and the frontend only

**Prerequisites:** task-d09, task-d62, task-d72.  
**Design:** Sections 4.1, 4.3 (amended by this task, if a decision reads another follower's votes).

**Implement:** Fast and slow acknowledgements go to every other voter and to the frontend, the reference's pattern (`spec/swiftpaxos-mapping.md`). task-d09 carries the leader's commit decision to every voter, and the frontend learns from its own copy.
- **First, the design check:** which follower decision reads another follower's votes. Where none does, the broadcast goes. Where one does, the step names it and keeps that copy. One is known: a follower's own learning. `Follower::learn` runs `commit_learned` over the follower's vote sets, which peers' `FastAck` and `SlowAck` fill, and `missed_fast` reads the same sets for the metrics. task-d09's `commit_through_leader` is the other source, so with the broadcast gone a follower learns only from the leader's frontier, one message delay later than now. The frontend keeps learning from the votes it still receives, and recovery reads reports, not votes, so neither moves.
- **The step records what that delay costs.** The followers' executed frontier lags the leader's by it, and that lag is what task-d10's catch-up, the reports' window and any read served at a follower see.
- **First, a recovery fix the change found** (this task's first PR, ahead of the routing). With acknowledgements to the leader only, the protocol simulator failed six seeds the routing's base passes: with catch-up, row 5 at three voters, seeds 376 and 841; row 3 at five voters, seed 528; row 3 at three voters, seed 372; without catch-up, row 10, seed 729, and row 9, seed 144, both at three voters. In each, a recovered Sync carried two conflicting commands with no edge between them, and the voters executed them in different orders (or one command under two dependency sets). A report labels every record with the replica's synchronized ballot, and the possible-fast rule reads a fast-set member's PRE-ACCEPT with a path as its vote in that ballot. Two kinds of record were no such vote and kept their path: a payload recorded while the replica may not vote fast (after a higher promise, fetched for a proposal it held; before its ballot is active; outside the fast set), and an earlier ballot's pre-acceptance a Sync's installation kept. A selection whose majority left out the source ballot's leader adopted them beside a pre-acceptance of the source ballot with no edge to them: one member's records alone gave both. The hole is the base's too; the routing's timing reaches it. Neither keeps its path now: the record is demoted at initialization when no fast acknowledgement goes out, and every pre-acceptance a Sync does not release is demoted in its marker's batch, as task-d11 and task-d34 demote an earlier ballot's acceptance. A demoted record keeps its payload and dependencies, and possible-fast selection skips it. A follower votes fast only on a new record, and adoption keeps a new ballot's acceptance off the demotion list, so neither rule takes a path a vote gave. Pinned by `tests/activation.rs::a_payload_recorded_after_a_higher_promise_carries_no_path` and `a_sync_takes_the_path_from_a_pre_acceptance_it_re_proposes` (each fails without its rule); the six seeds are kept.
- **Then the change,** as a pair, after task-d77: with acknowledgements to the leader only, the simulator without catch-up failed row 9 at three voters, seed 628, on a hole task-d77 closes at the base.
- **Built ahead of task-d73** (this task's PR): on task-d72's local reading a write is about 35 messages at five voters, and two thirds of them are acknowledgements to followers. task-d73's gain is the commands a turn carries, about 1.2 at the leader at 30 callers (24,272 lowerings for 30,209 commands), so it is read after this one.
- **The design check, recorded** (Section 4.3): the one follower decision that read another voter's acknowledgements was its own learning, and the one metric was its `missed_fast`. The frontend, the leader and recovery read none. A follower's acknowledgements, and the leader's own adoption, go to the leader and the frontend only. The leader announces its commit frontier in the step that moves it (`Leader::announce_advanced`), as well as on the re-send timer, which repairs a lost one, so a follower commits one leader turn and one hop after the leader, not up to the re-send interval after it.
- **The routing, read locally, and why it is not kept yet** (this task's second PR, a draft). Three alternating pairs on one host, as task-d72's reading (five voters, 30 callers, 20,000 operations, replay profile): #165's head `2f7f511` against the routing with #166's fix. Frames a write, summed over the nodes, fall from 36 to 18, and datagrams from 14.3 to 9.4. A follower's CPU a write falls by 6% (1.36 to 1.28 ms, the means of the three runs), the leader's does not move (1.64 to 1.66 ms), and all nodes' falls by 4.5% (7.10 to 6.78 ms). `ok`/s is 785 on the base and 805 on the head (760 to 824 against 797 to 820), within the spread. The transport's cost on this host goes less with frames than task-d72's profile suggested. The protocol simulator without catch-up failed one seed with the routing: row 9 at three voters, seed 628. A follower learns a command only from the leader's frontier, so a follower that keeps missing a Sync's installation through repeated interrupted elections held an earlier ballot's acceptance of a command the candidate had executed and forgotten, and the selection re-proposed it under other dependencies. The base reaches the same hole with only the acknowledgements between followers lost, so it is task-d77's, and the seed passes with task-d77 under the routing. The routing is still a draft beside the reading: its gain is about 4% of the nodes' CPU.
- **Off draft next** (#98): with task-d77 to task-d80 under it, the sweep at every loss the weekly job runs, with catch-up and without, on its head, then three runner pairs at the register row. In the `service` row the fast-set followers sent 8.27 frames a command each against 4.26 to 4.28 for the others; the difference is the fast-acknowledgement broadcast this removes.

**Acceptance:** The design check recorded, with every decision it read. The protocol simulator, every row at 1,000 seeds per row and size, with catch-up and without. Three pairs on the runner: messages and datagrams per command, the followers' CPU per operation, the followers' execution lag behind the leader, `ok`/s and p99.

**Review boundary:** Who receives an acknowledgement. No change to the learning predicate, to the fast predicate or to recovery.

<a id="task-d75"></a>
### task-d75: Take work off the leader's domain loop

**Prerequisites:** task-d53, task-d60, task-d72.  
**Design:** Section 17.3.3 (unchanged: the domain actor and its workers).

**Implement:** The leader's single loop is the serialization point. On the runner it queued for a CPU while 0.65 to 1.48 cores sat idle. What it does that another thread could is read first, from the per-thread split task-d53's and task-d60's readings give (the leader's loop by phase, the follower beside it), with the idle cores as the evidence that the loop is the bound. Each candidate the reading names is its own PR with its pair.

**Acceptance:** The reading recorded in the notes before any change. Each change a pair on the runner: the leader's loop CPU per command, the idle cores, `ok`/s and p99.

**Review boundary:** What runs on the leader's domain thread. No change to the order the loop decides in, or to the protocol.

<a id="task-d76"></a>
### task-d76: Choose a whole fast set after a failure

**Prerequisites:** task-d31, task-d68.  
**Design:** Sections 4.2 and 4.9 (amended by this task); Section 11.7's mixed-version gate.

**Implement:** On the Jepsen runner, in a run whose every kill was at the leader (#98), four of five new leaders ran at 81 to 87% fast. The fifth, n5 in ballot 2, ran at 5.7%. `c2_default` is the leader and the next ⌊N/2⌋ voters in identity order, wrapping, so n5's set was {n5, n1, n2}, and n2, the leader just killed, was down for all but the last 8 s of n5's term. A C2 fast decision needs every member of the set, so a dead member is no fast decision for the term, and at five voters a kill lands in the next leader's set about one time in two. The candidate knows who promised in its campaign.
- **The choice:** a candidate chooses its C2 set among the voters that promised. That holds at every size that can elect: at three voters, the leader and the other promiser; at five, the leader and two of its promisers. A leader whose member dies later re-chooses by the mechanism Section 4.2 already names, a higher ballot with itself as leader. A set is never re-chosen within a ballot, which Section 4.2 forbids.
- **The record exists; its carriage does not.** The signed ballot record, `BallotConfigurationV1.fast_set` with a majority's promises over the ballot message, already names a fast set, and `coord-membership` turns it into a C2 configuration. But nothing on the consensus wire carries it. Every production path builds `c2_default` locally: a follower builds it when it adopts a ballot, and that is the only source configuration recovery is given (task-d31's `select_from`, `Campaign::knowing`). A candidate that never adopted the source ballot supplies none, and recovery builds `c2_default` for it. `SyncDecision` carries no fast set.
- **First, the design step.** It says:
  - how the ballot record reaches the followers, and the mixed-version gate (Section 11.7) that a new field on the Sync, or a new message, costs: the cost that kept the member's positions out of task-d71's report;
  - what replaces recovery's rule that a ballot whose configuration is not known ran under the default. Once a chosen set can exist, a candidate that never adopted the source ballot cannot assume the default. So the set is durable at every voter that adopted the ballot and readable from its report, or the campaign refuses until a reporter that held it answers;
  - why recovery stays sound with it.

**Acceptance:** The design step before code. The protocol simulator, every row at 1,000 seeds per row and size, with catch-up and without. On the runner, a run with every kill at the leader: each new leader's fast share near the others', whichever voter was killed.

**Review boundary:** How a ballot's fast set is chosen and carried. No change to the fast and slow quorum sizes or to the learning predicate.

<a id="task-d77"></a>
### task-d77: Carry what a candidate executed at COMMIT, never re-propose it

**Prerequisites:** task-d10, task-d71.  
**Design:** Section 4.9 (amended by this task).

**Implement:** task-d74's routing failed one seed without catch-up, row 9 at three voters, seed 628, and its base does not: with acknowledgements to the leader only, a follower learns a commit from the leader's frontier alone. The base reaches the same divergence when only the acknowledgements between followers are lost: at 1,000 seeds without catch-up, row 9 at three voters failed seeds 362 and 863 with every such acknowledgement lost, and 550 and 633 with half (#167). In all four:
1. one voter executes a command and later retires it past its window;
2. a selection from two reports, the candidate's own and that of the voter that never learned the commit, sees the command only in the second, at an ACCEPT below the source ballot or at PRE-ACCEPT, and re-proposes it;
3. a later leader proposes it under other dependencies, and another voter executes that.

In each the candidate of step 2 is the voter that executed and forgot the command, and it knows it did: `OwnDecisions::decided` reads the table's history, which the possible-fast rule already asks (seed 884's case). The source rule's re-proposal did not ask it.
- **Never re-propose what the candidate executed.** The selection asks the candidate about every command it would re-propose (`CandidateDecisions::executed`). One the candidate executed is carried at COMMIT under the facts it executed with, so every later selection keeps it by the commit exception. Leaving it out is not enough: a voter that holds it below the source re-proposes it from its own report when it is the next candidate.
- **The facts are the candidate's durable row.** A forgotten command's record and ledger entry are gone, but its dependency row is not (Section 13.1). The runtime reads it from the store, as it serves catch-up (task-d08), when the campaign asks (`Follower::executed_rows_wanted`, `Follower::executed_rows`), and the campaign waits for it as it waits for a payload. A command the candidate executed with no dependency row refuses the campaign (`RecoveryError::ExecutedWithoutRow`).
- **The argument** (Section 4.9): why the candidate is the only executor a selection has to ask. A reporter that forgot the command executed a table past it, and promises no candidate more than a table behind it (task-d10), so the candidate executed it too; otherwise the quorum that decided it meets the reports. A command decided fast rests there on the possible-fast rule.
- **Forgetting counts executions too** (#169 review). `CommandTable::forgotten` waits for a table's capacity of executions past the command, as well as of retirements, so the first case is task-d10's bound exactly and rests on no retirement order. It never forgets earlier than before.
- **A carried entry is an earlier ballot's decision** to the possible-fast rule, as a below-source report's entry is (#169 review): what the candidate executed precedes every command the rule keeps, and a fast-set member that forgot it holds nothing to order it by.
- **The simulator** gains a knob, `peer_ack_loss`, the loss of an acknowledgement to a voter other than its ballot's leader, and rows 16 and 17: row 9's elections with every such acknowledgement lost, and with half.

**Acceptance:** A deterministic test that fails without the change, for both reporters, the ACCEPT below the source and the PRE-ACCEPT, and one each, failing without its change, for a possible fast decision after a carried command and for a command retired ahead of ones it executed after. The runtime's read answers a forgotten command's row from a real store, and none where there is no dependency row. The seeds rows 16 and 17 fail at the base are kept in `seeds.json`. Every row passes at 1,000 seeds per row and size, with catch-up and without. task-d74's routing passes seed 628 with this under it.

**Review boundary:** What the selection carries for a command its candidate executed, and the runtime's read of its row. No change to the report format, to the wire or to the source rule for anything else.

<a id="task-d78"></a>
### task-d78: Stop a fast decision's path evidence at a command the selection holds decided

**Prerequisites:** task-d71, task-d77.  
**Design:** Section 4.9 (amended by this task).

**Implement:** The sweep task-d77 asked for, every row with the acknowledgements to followers lost and catch-up on, found one divergence the retirement rule does not explain: row 4 at three voters, seed 720, with half of them lost (#167). A command decided fast after another was dropped from a selection and re-proposed, and another voter executed a different command at its position.
- **Where it goes wrong.** The possible-fast rule's path check (`prefix_decided`) walks the member's own records before the candidate, and refuses it on a command no ballot decided. The member held its own pre-acceptance of a command the selection held at COMMIT under other dependencies; the walk went through that record to a pre-acceptance nothing decided.
- **The change.** The walk stops at a command the selection holds at COMMIT, as the order check's closure already reads the same record (task-d71). Section 4.9 has the argument: a leader synchronization re-based the member's log past that command, while its record keeps the dependencies it was pre-accepted with, so what the record names behind it was never in the leader's order before the candidate.
- **The simulator** takes `PROTOCOL_SIM_PEER_ACK_LOSS` for every row, and a kept seed names the loss it was found under, as it names catch-up. The weekly sweep runs every row with half, three in four and every such acknowledgement lost, with catch-up and without, and runs rows 16 and 17, which it did not (three in four from the review: half and all did not find row 9's seed 296).

**Acceptance:** A model test from seed 720's reports that fails without the change. The seed is kept, with its loss. Every row passes at 1,000 seeds per row and size, at no loss, half and all, with catch-up and without.

**Review boundary:** The path check's walk, the simulator's loss override, and the sweep's matrix. No change to the order check, the source rule or the wire.

<a id="task-d79"></a>
### task-d79: Stop campaigning on an own acceptance nobody can supply, until a Sync is installed

**Prerequisites:** task-d05, task-d78.  
**Design:** Section 4.9 (amended by this task).

**Implement:** The same sweep found a liveness stall, not a divergence: row 10 at five voters, seed 540, with every acknowledgement to a follower lost and catch-up on (#167). A command was accepted under one ballot's Sync and never decided, and the next two Syncs left it out, so every voter that installed them released it and deleted its payload. One voter missed both. It is still synchronized to the first, holds the acceptance pending with no payload anywhere, and every selection from its own report fails as `HalfInitialized`. Its ballots kept rising and fenced the other voters' campaigns (`FencedByPromise`), so nobody led. Catch-up could not help: every voter had executed as far, and a catch-up page carries no Sync.
- **The change.** A candidate whose campaign fails as `HalfInitialized` on its own report, with every voter answered, gives the campaign up, and does not campaign again until it installs a Sync (`FollowerRejection::Unbacked`), the way `behind` already stops one (task-d05). The other voters elect a leader, and its Sync releases the acceptance when this voter installs it.
- **Not taken:** dropping an unbacked acceptance below the source from the candidate's own report. It changes what a selection may release and would need its own safety argument, for a liveness gain this change already gives.

**Acceptance:** A deterministic test that fails without the change: a voter restored synchronized to a Sync whose acceptance nobody can supply gives up its campaign, starts no other, and campaigns again once it installs another voter's Sync. Seed 540 is kept with its loss and heals. Every row passes at 1,000 seeds per row and size, at no loss, half and all, with catch-up and without.

**Review boundary:** When a candidate stops campaigning and when it starts again. No change to the selection, the reports or the wire.

<a id="task-d80"></a>
### task-d80: Keep a commit through the Sync that leaves it out

**Prerequisites:** task-d11, task-d20, task-d79.  
**Design:** Section 4.9 (amended by this task).

**Implement:** A sweep of every row at three in four acknowledgements to followers lost, which neither half nor all reaches, found a divergence without catch-up: row 9 at three voters, seed 296 (#169). A voter far behind knew two commands decided only from the Sync it was synchronized to, since a commit is not written as a row and it had restarted. The next Sync was selected from two voters that had executed and forgotten them or never held them, and left both out. Installing it replaced the pending COMMIT entries, demoted the acceptance under one and released the other, so the voter reported a decided command at PRE-ACCEPT, without the command it was decided after. A later selection from voters as far behind re-proposed it, and the voter executed it at the position where another voter had executed the other command. The base fails 51 of row 9's 1,000 seeds at that loss, every one a campaign held by a majority that never completes; task-d77 completes them, and one reaches this hole.
- **The change.** A voter merges into its own copy of a Sync, before the marker and the row, every command it holds committed that the Sync leaves out: a pending COMMIT entry of the Sync it replaces, and a record committed and not executed. The marker then neither releases it nor demotes an acceptance under the same dependencies, the row carries it across a restart, and the report names it at COMMIT. The merged selection is checked for a cycle.
- **The simulator's budget.** The work a Sync's installation may cost counted only the Sync given, and the carried entries are examined with it, so seeds with catch-up went over it (row 12 at three voters, seed 387: 8 examinations for 1 entry). The follower counts the entries and edges it carries (`sync_carried`), and the budget counts them as given. They are bounded by what the voter holds committed and not executed.
- **Not taken:** an installer that stops campaigning when it is more than a table behind the Sync's candidate. It needs the Sync to carry the candidate's execution position, a wire change.

**Acceptance:** A deterministic test that fails without the change: a voter holding one command committed pending and one installed installs a Sync that leaves both out, keeps both, reports both at COMMIT, and does so again after a restart from its rows and after the Sync is sent again. Seed 296 is kept with its loss and passes, and so does seed 12/3/387 with catch-up, for the budget. Every row passes at 1,000 seeds per row and size, at no loss, half, three in four and all, with catch-up and without.

**Review boundary:** What a voter writes in its own Sync row. No change to the selection, the reports, the rows' layout or the wire.

**Follow-up (review, optional):** a voter holding a command pending at COMMIT that receives a proposal for it under other dependencies adopts the proposal, since `on_proposal` does not read `sync_pending`, and then holds two decisions of it. Answering such a proposal from the decision, or stopping with a named reason, would show a selection hole the sweeps have not found as a named stop in the fault runs rather than as a later mismatch.

<a id="task-d81"></a>
### task-d81: Let a voter ahead serve the voters waiting on it

**Prerequisites:** task-d08, task-d10, task-d25, task-d80.  
**Design:** Section 4.9 ("A voter ahead serves at any ballot of its epoch", added by this task).

**Implement:** A sweep at nine in ten acknowledgements to followers lost, with catch-up, stalled one seed: row 12 at five voters, seed 148 (#173 review), "not healed 400 rounds after the faults stopped", with the voters at 11, 11, 11, 11 and 12 of 12 executed. It stalls the same way with task-d80 removed and on #166, so the stack neither causes nor fixes it.
- **A second seed, at three voters.** task-d82's sweep (#175) stalled row 2 at three voters, seed 281, at three in four with catch-up, "not healed 400 rounds after the faults stopped", with the voters at 14, 14 and 15 of 15 executed. Node 2 alone executed position 15. It restarted at b0/0 while the other two are at b1/1, and it lacks a command (`Behind`). Its campaign's `NewLeader` names 15, so the other two refuse to campaign (`BehindVoters`), and as a donor it serves neither. The shape was in place before the faults stopped. Without task-d82's re-ask of `NewLeader` the seed takes another path and heals, so the re-ask changes which seeds reach the shape, not the shape. Seed 2/3/281 is kept with seed 12/5/148 in the PR that fixes it.
- **The shape.** Node 4 alone executed position 12, and never received ballot b4/3's Sync, which the other four installed. After the faults stop, node 4 campaigns and the four promise it. Its NewLeader names its frontier, 12, so each of them refuses to campaign until it has executed that far (`BehindVoters`, task-d10). Node 4 cannot lead: its selection depends on a command b4/3's Sync re-proposed, which it never saw (`Behind`). And only node 4 can serve position 12, but a donor serves only while synchronized at the request's ballot or later (`Node::serve_catch_up`, as in the simulator), and node 4 is synchronized at b1/2. So the four wait on node 4, and node 4 waits on a command only one of the four leading would send it.
- **First option: the donor.** A voter serves its executed history at any ballot of the epoch, and the requester takes such a page. Executed order is one order at every voter, so a prefix of it is valid whatever ballot its donor is synchronized to. The rule's reason, that what a donor executed then includes everything decided at the ballot, is about completeness, which a prefix of executed history does not need. This touches what task-d25's comparison with the donor assumes, and is argued against it first.
- **Second option: the wait.** A frontier learned from a NewLeader stops holding a voter back once that leader's campaign has failed.
- **Taken: the donor.** `Machine::serves_catch_up_at` replaces `synchronized_at_or_after` in `Node::serve_catch_up`, and the protocol simulator's donor follows it: a donor serves at any ballot of its epoch. The requester's rules are unchanged: it asks at the ballot it is synchronized at, takes only a page tagged with that ballot, and installs it under task-d14's rules. The wait was not taken: it would undo task-d10's guard, letting a voter campaign behind a frontier whose history only one other voter holds. On this branch, seed 12/5/148 heals with the old donor rule too, because task-d82's re-ask changes its path. Seed 2/3/281 fails with the old rule and passes with the new one.

**Acceptance:** A deterministic test of the shape that fails without the change. Seed 12/5/148 is kept at 900000 with catch-up and seed 2/3/281 at 750000 with catch-up, and both pass. Every row passes at 1,000 seeds per row and size at every loss the weekly sweep runs, with catch-up and without.

**Review boundary:** Which ballot a donor serves at, or what ends the wait on a frontier, and nothing else.

**Sweep matrix (review).** The rates between the fixed ones keep finding cases (296 at three in four, 148 at nine in ten). Rather than a fixed rate per finding, a job set that draws each seed's loss from the seed itself, across none to all, covers the gaps without multiplying jobs. It goes with this task or on its own.

<a id="task-d82"></a>
### task-d82: Bound a leader's silence below the transport's idle timeout

**Prerequisites:** task-d01, task-d03.  
**Design:** Section 11.4 and the O14 row (amended by this task).

**Implement:** A leader killed and restarted held the domain leaderless for 23 to 54 s after its restart, and no node served for 67 s at worst (the #173 carry's leader-kill run, #98). Two transport facts make it:
- **A follower sees its leader as silent only when the link goes.** The election counts a follower led while it holds a link to the leader it promised (`election.rs`), and a killed process's link closes only at QUIC's idle timeout, 30 s with a 5 s keep-alive (`coord-transport` `Limits`). So no follower campaigns for 25 to 30 s after any leader kill, in every leader-kill run.
- **A leader back within that time is locked out.** It returns under the same incarnation, so its connections have the same link key as the dead process's. When two connections compete for one lane, the one whose dialler has the lower replica identity survives (`Shared::survivor`). The restarted leader's dials lose to the peers' own connections to the dead process, which each peer keeps until its idle timeout. Meanwhile the peers promise its ballots and nothing reaches it.

The change, in two steps:
- **First: a node's idle timeout in seconds, and a campaign that asks again.** Both of a node's endpoints run an idle timeout of 5 s with a keep-alive of 1 s (`Limits::node`). That bounds both edges: a dead leader's links close within 5 s of its last packet, so the followers campaign after their patience; and a peer's connection to a dead process closes as fast, so a restarted voter's own dials then hold the lane. The api plane needs it too: a frontend's submission link to a voter is an api connection, and a write sent on one to the dead voter waited out the 30 s before it was dialled again. QUIC takes the smaller of the two ends' idle timeouts, so a link to a voter of an older build runs at 5 s too. A paused voter loses its links after 5 s, which is what a pause should look like to the election.
  - **The campaign's re-ask.** In the first 5 s a restarted voter is linked for a moment by each dial its peers then close, and a campaign it starts in that moment sends its `NewLeader`s on those connections. A `NewLeader` was published once, so the campaign then held the voter until the election replaced it at its ceiling, 16 s later. The interval that asks for missing report pages (task-d28) now also asks each voter that has not promised for its promise, once the candidate's own promise is durable.
- **Then, if the first step leaves a gap worth it: a newer boot displaces the old connection.** A per-boot nonce in the peer handshake, so a connection from a newer boot of the same identity takes the lane whoever dialled, and the old one closes. `HelloV1` has no field to carry it, so that is a peer capability and a frame, with a `spec/wire-v1.md` step of its own. Not built until the first step's row says what remains. After the first step it can buy at most the 5 s the survivors take to notice the kill. And it needs an election question answered first (#175 review): a newer boot's connection takes the lane at once, so a follower stays linked to the identity it promised, and counts itself led by a process that has forgotten its lead. Its design step has to say how a follower treats a new boot of its leader.

**Not taken:** the election reading the time since it last heard from its leader, instead of whether a link exists. The link is the transport's own clock, and a second clock in the election would need a heartbeat the protocol does not have.

**Acceptance:** A transport test that kills an endpoint and starts one under the same identity within the old idle timeout, and has the link back within a bound of seconds; a coordd test that the survivors of a stopped leader campaign within the bound, and one that a leader killed and started again at once is replaced, and the domain serves, within seconds; a consensus test of the campaign's re-ask. The protocol simulator, every row at 1,000 seeds per row and size, with catch-up and without. On the runner (the testing session): a summary column for the domain's gap after each kill, from the kill to the next `ok` on any node, and the leader-kill row within the idle timeout, the patience and one campaign, about 6 s locally (5.3 to 6.4 s), recorded beside it. The kill-any run's 1.9 s isn't a target for this row: it times a restarted node back to serving, mostly after follower kills, with no election in them (#175 review). Going lower means a shorter idle timeout, which a paused or lossy voter pays for, or the heartbeat this entry chose not to take.

**Review boundary:** A node's idle timeout and keep-alive, and a campaign asking again for the promises it lacks. No change to the election's rules, to what a promise means or to the wire.

<a id="task-d83"></a>
### task-d83: Serve read-only transactions at the read barrier

**Prerequisites:** task-d50, task-d72.  
**Design:** Section 6.3 (amended by this task).

**Implement:** The barrier serves a Range without an explicit revision (task-d50). A read of more keys is a transaction of Range branches, which is an ordered command, and so is the snapshot read every multi-key write transaction starts with. In the #173 carry's `service` row (#98), the leader executed 1.03 ordered commands an operation, and only 10,281 of 42,324 operations went to the barrier, where etcd serves the same read-only transaction without a log entry.
- **The change:** the barrier also serves a transaction with no compares and no failure branch whose every branch is a Range without an explicit revision. A failure branch runs only when a compare fails, and with no compares it never runs, so a transaction that has one is left ordered rather than argued about. It is read at the same read index, confirmation and execution point, over one gated snapshot, which the barrier already plans a read over. So the Section 6.3 argument holds unchanged: it is about the point R reads at, not how many keys R reads there.
- **What it is expected to move:** ordered commands an operation in the `service` row from about 1.03 to about 0.4, the writes alone, and the read half of every multi-key write transaction off the ordered path.
- **Two small changes with it:**
  - Learned-to-released's third part is named for what it times since task-d63: from the group closing to release at the group's journal durability, not the projection's commit, which is on the materializer and off a write's release path. The metrics field is renamed with it (`projection` to `journal`), so the testing session's summary takes the new key and caption in the same carry.
  - The `service` row shows its own equality: ordered commands and barrier reads an operation for TupleSky, requests an operation for etcd (the summary, in the testing session's harness).

**Acceptance:** The Section 6.3 amendment first. The read barrier's tests for a many-key read-only transaction: the same response bytes the ordered path records, refusal back to the ordered path where any branch is not a plain Range or carries a revision. On the runner: the `service` row's ordered commands an operation, p50, p99 and CPU an operation, beside etcd's.

**Review boundary:** Which read-only requests the barrier serves. No change to the barrier's argument, to the ordered path or to what a write transaction does.

<a id="task-d84"></a>
### task-d84: One journal sync a turn at every voter

**Prerequisites:** task-d47, task-d54, task-d63, task-d72.  
**Design:** Section 17.3.3 (amended by this task, if a turn's groups merge).

**Implement:** task-d72 names durability per command as one of the four costs and no lever owns it. In the #173 carry's `service` row (#98) every voter synced its journal 1.6 to 1.8 times a command at 140/s, each append on the leader 2.28 ms served and 0.26 ms queued, and a write's 23 ms p50 is a read and then a write, each paying syncs in series. A turn's acceptance rows and its execution group are separate journal groups. One sync a turn for both, and for the payload rows a turn records, takes the syncs a command toward one a turn.
- **First, the reading:** journal syncs a turn and a command at every voter, and which of a turn's writes each one carries, from the metrics, so the merge is sized before it is built. Besides those (#174 review):
  - **the syncs in series on one write's path,** from submission to release: the acceptance row before its acknowledgement, then the execution group before release;
  - **the projection's syncs under `strict`.**
- **Then the entry says whether merging syncs can reach task-d72's p99.** A merge lowers syncs a command, but not the two in series on one write's path.
  - With an append at about 2.3 ms on the runner, two in series are already above etcd's 4 ms p99 before any queueing. etcd has one, its WAL sync, at the leader and the followers in parallel.
  - A barrier read also waits for its index to execute, 6.9 ms in the last run, which is the in-flight writes' latency.
  - If the reading confirms that, meeting the p99 needs one of two things: a design step on whether a result may be released once its command is decided at a quorum, before its own execution group is durable; or the target revised under task-d72's own clause, with the reason beside it.
  - **Both levers on the path have tasks (#174 review):** a fresh proposal's acceptance in its proposal batch (task-d89), and release before the execution group is durable (task-d90).
- **The merge keeps every ordering Section 17.3.3 states:** an acceptance is durable before its acknowledgement goes out, and an execution before its result is released. A turn whose acceptances and execution share one sync releases neither before it.

- **The reading (task-d84's first PR; [notes](../tuplesky-impl-notes.md#journal-syncs-a-command-and-what-each-carries-task-d84)).**
  - Three voters on one host, strict, 1 and 10 callers, with the stores on tmpfs and on the disk.
  - Each voter's snapshot now counts what its journal groups held (`Cost::journal_groups`).
  - **At 1 caller the leader syncs three times a command, and each sync waits for the one before it:** the proposal row, its ACCEPT row (`Leader::advance_pending`, written once the proposal row is durable), and the execution group.
  - **Followers already share.** The fast-set follower's second protocol row shares its group with the execution group for 74 to 99% of commands.
  - **At 10 callers every voter syncs 0.94 to 1.19 times a command,** and 29 to 46% of the groups already carry both kinds (task-d47).
  - **In series on a write's path:** about one sync before the command is learned and one and a half after it. The execution group waits behind the leader's ACCEPT row, the group still out, since the appender has one group out at a time (task-d54).
  - **The projection under `strict`:** it commits once a journal group, off the result's path since task-d63, but on the same disk.
  - **At `replay`, the same groups and the same syncs a command** (review of #179). Each sync on the disk is served in 0.48 to 0.53 ms at 1 caller and 0.66 to 0.74 at 10, against `strict`'s 0.66 to 0.71 and about 1.06. So the projection's durable commits are a quarter to a third of a journal sync's time under `strict`. The `put` p50 on the disk is 2.15 to 2.35 ms at 1 caller and 6.5 to 7.1 at 10, against 3.17 to 3.24 and 9.0 to 9.6.
  - **Per turn is not counted.** A group is lent whenever none is out, so its boundary is the previous sync's return, not a turn.
- **What the reading decides: the merge is not built.**
  - The syncs that stay separate at low load wait on each other's durability, so no grouping can join them.
  - At load the groups already share.
  - Locally at 10 callers, the `put` p99 hardly moves between tmpfs and the disk (21.9 to 28.6 ms against 25.4 to 29.3), so the tail there is queueing and CPU.
  - Reaching task-d72's p99 needs a design step. Three levers, in the order review of #179 set:
    1. **A fresh proposal's ACCEPT row in its proposal batch, when the dependency guard already passes** (task-d89).
       - A re-proposal already writes it there (`Proposal::accepts`, task-d19). A fresh proposal writes it once the guard passes, in a batch of its own after the proposal row is durable.
       - At low load every dependency of a new command is long accepted, so the guard passes at proposal time. The leader's second sync then leaves the path for fast and slow commands alike, and the execution group has no ACCEPT group to wait behind.
       - The journal holds what it holds today, one sync earlier, in one atomic batch. Under load, a command whose prerequisite is still at PRE-ACCEPT keeps today's path.
       - **Its step answers:**
         - Is there a reason, besides the guard, that a fresh proposal's acceptance waits for its own proposal row? `advance_pending` names only the guard.
         - Is a prerequisite accepted in memory in an earlier batch, and durable no later than the new one? `adopt_own` relies on batches becoming durable in submission order, so it should be.
         - The leader's fast vote is its proposal, not its table phase.
    2. **A result released once its command is decided at a quorum and executed in memory,** before its own execution group is durable (task-d90).
       - This is the lever that gives etcd's shape: one sync in series.
       - It amends Section 17.3.3's ordering of an execution before its result. Its argument: execution order and results are a function of the decided commands (task-d06's chain), so a crash before the execution group is durable replays to the same result.
       - It also has to say what the collector's late-release comparison (task-d12) and the execution chain (task-d35) see across that crash.
    3. **Deferring the leader's ACCEPT row until the fast path has failed,** only if the `service` row's fast share proves high.
       - It moves a sync from fast commands to slow ones. A slow decision is the leader's durable ACCEPT plus `slow_size − 1` follower copies (task-d19), and deferred, the leader's vote waits for the fast path to fail and then for its own sync.
       - Under load nearly every command is slow: the register row's leader ran 2.6 to 3.3% fast, and task-d68's row at ten callers 5.8%.
  - The runner's pair for whichever is built reads the `service` row's p50 and p99 against this reading.

**Acceptance:** The reading recorded first. The storage crash tests across the merged group, at every cut. Three pairs on the runner: journal syncs a command, CPU an operation, the `service` row's p50 and p99.

**Review boundary:** How a turn's journal writes are grouped and synced. No change to what is durable before what.

<a id="task-d85"></a>
### task-d85: Let a Sync sent again wait for its marker

**Prerequisites:** task-d11, task-d24.  
**Design:** Section 4.9 (amended by this task).

**Implement:** The sweep of #167's head at half the acknowledgements between followers lost, without catch-up, found a divergence: row 10 at three voters, seed 986, two voters executed different commands at position 4. A voter received a Sync twice. The first copy staged the marker's batch, and `mark_synced` recorded the synchronized ballot in memory. The second copy passed `already_synced` and activated the ballot before the batch was durable. The voter adopted and acknowledged the new ballot's held proposals, and the marker's durability then released the records the selection left out, which included commands it had just accepted. Its acknowledgement still counted, so its next report named a decided command at PRE-ACCEPT under its own dependencies, and the next selection put another command at its position. The code is the same on the base; task-d74's routing only led a seed there.
- **The change.** A copy of the Sync whose marker is in flight changes nothing; the marker's durability activates the ballot, as `activate`'s contract says.
- **task-d11's test** relied on the early activation: it adopted a proposal over a record the marker was about to release. Its Sync now carries that record, and the proposal is adopted at activation, after the marker's demotion.

**Acceptance:** A deterministic test that fails without the change: a Sync sent again before its marker is durable leaves the ballot inactive and installs nothing, and the marker's durability activates it. Seed 10/3/986 is kept at 500000 without catch-up on #167, where it fails without the change, and passes. Every row passes at 1,000 seeds per row and size at every loss the weekly sweep runs, with catch-up and without, on #167's head with this merged.

**Review boundary:** When a voter activates a ballot whose Sync it has received twice. No change to the selection, the rows or the wire.

<a id="task-d86"></a>
### task-d86: Fewer projection commits per command

**Prerequisites:** task-d48, task-d52, task-d72.  
**Design:** Section 17.3.4 (amended by this task only if a commit cadence reaches `strict`).

**Implement:** Locally the materializer is a fifth of each node, 54 to 60% of it in redb's commit and 8 to 10% in its freed-page bookkeeping (task-d72's profile), so its cost is per commit far more than per record. At the register row a commit holds about 2.4 commands (task-d75's counters).
- **First, the runner's group sizes (#174 review):** commands an append and a commit at each voter, from the #173 carry's `metrics` lines (`executed`, `journal_appends`, `projection_commits`), with no new run. An append takes about 2.3 ms on the runner's disk, so groups may already be larger there than tmpfs's 2.2 and 2.4.
- **Then the reading (R1, with task-d73):** at 10 and 30 callers, the materializer's CPU per commit and per record, and commands an append and a commit. Then again with task-d73's wait, if it is taken.
- **Then, if the cost per commit still leads,** a cadence: a projection that serves no read between commits takes more records a commit, up to a bound of records and time, as task-j06's durable cadence does for its syncs. What it serves (barrier reads, watches, events, a checkpoint) asks for the next commit, so nothing waits on a commit it needs. Under `strict` a deferred commit still makes its records durable before the gate moves.
- **The loop reads the projection too (#174 review),** and a deferred commit leaves each of those reads a commit behind:
  - **The planning view** (`Applier::planning_view`): a command is planned against the projection's snapshot with its own group's writes over it. With commits deferred, the overlay holds every group since the last commit, so a plan's base is still the queued frontier the store checks.
  - **The retry lookup:** a retry answered from its record reads the same view, so it finds a binding a deferred group wrote.
  - **task-d75's settle read** (`settle_from_records`) finds an execution record only once a commit makes it visible. A half-established entry it finds no record for asks for the next commit, so an entry whose half was lost waits for no deferral.
- **What it is expected to move (#174 review):** 0.15 to 0.3 ms of CPU an operation at the register row.

**Acceptance:** The runner's group sizes and the reading recorded first. Model tests: a command planned after a deferred commit sees the earlier groups' writes, a retry is answered from a deferred group's binding, and a half-established entry's record is found within one commit. A local pair: commits a command and the materializer's CPU an operation fall, and no read, watch or event waits longer than the bound. The storage crash tests across a deferred commit, at every cut. Its runner pair is read in the batch job (task-d72).

**Review boundary:** When the projection commits and how much a commit holds. No change to what a commit records, to the journal's ordering or to what is durable before a result's release.

<a id="task-d87"></a>
### task-d87: Let held reads share one confirmation round

**Prerequisites:** task-d50, task-d58, task-d72.  
**Design:** Section 6.3 (amended by this task only if a round serves a read that arrived after it began).

**Implement:** In the #173 carry's `service` row the read barrier took one confirmation round a read (1.00 reads a round), and `pump_reads` is 14% of the leader's loop in task-d75's reading. Locally at 10 callers, rounds already carry 1.2 to 1.4 reads (task-d84's reading), so sharing exists under load.
- **First, the reading:** why `service` took one round a read. It is either because its reads, at 4 clients and 300/s, never overlapped a round, or because each read started a round of its own while one was out.
- **If the second:** a round serves every read held when it starts. A read that arrives while one is out waits for the next, which starts as soon as the one out returns. That keeps the barrier's argument as it is.
- **If the first:** the lever is the cost of a round in `pump_reads`, not its sharing, and this entry is re-scoped to it.
- **What it is expected to move (#174 review):** 0.02 to 0.04 ms of CPU an operation at the register row, more at `service`, and the 1.2 ms confirmation under load.

**Acceptance:** The reading recorded first. A model test that a read arriving after a round began is not served by it. A local pair: reads a round and the leader's loop a read. Its runner pair is read in the batch job (task-d72).

**Review boundary:** Which held reads a confirmation round serves. No change to the read index, to the confirmation's quorum or to the barrier's argument.

<a id="task-d88"></a>
### task-d88: Fewer datagrams per command on the api plane

**Prerequisites:** task-d70, task-d74.  
**Design:** None.

**Implement:** Over the voters the api plane carried 30.2 datagrams a command against the peer plane's 20.6, and 25.2 against 16.9 at threshold 8 (#155's pairs). That is the submission fanned out to every voter, every remote voter's acknowledgements and the release to the frontend, and the client's own traffic. With task-d74 in, it is the larger plane, and nothing owns it.
- **First, the reading (R3):** the api plane's frames and datagrams a command by kind (the submission, fast and slow acknowledgements, the release, the client's), at the register row locally.
- **Then the lever the reading names,** recorded in this entry before it is built.

**Acceptance:** The reading recorded first, with the lever chosen from it. A local pair: the api plane's datagrams a command and the voters' CPU an operation fall. Its runner pair is read in the batch job (task-d72).

**Review boundary:** The api plane's traffic per command. A change to what the frontend learns from is a design step of its own, not this task.

<a id="task-d89"></a>
### task-d89: Write a fresh proposal's acceptance in its proposal batch

**Prerequisites:** task-d19, task-d84.  
**Design:** Section 4.7 (amended by this task).

**Implement:** At low load the leader syncs three times a command, each once the one before it is durable: the proposal row, its ACCEPT row (`Leader::advance_pending`), and the execution group (task-d84's reading). A re-proposal already writes its ACCEPT row in its proposal batch (`Proposal::accepts`, task-d19). A fresh proposal writes it once the dependency guard passes, in a batch of its own after the proposal row is durable.
- **The change (#179 review):** when the guard already passes at proposal time, the fresh proposal's batch carries its ACCEPT row too. At low load every dependency of a new command is long accepted, so it does.
  - The leader's second sync leaves the path, for fast and slow commands alike, and the execution group has no ACCEPT group to wait behind.
  - The journal holds what it holds today, one sync earlier, in one atomic batch.
  - Under load, a command whose prerequisite is still at PRE-ACCEPT keeps today's path.
- **First, its design step answers:**
  - Is there a reason, besides the guard, that a fresh proposal's acceptance waits for its own proposal row? `advance_pending` names only the guard.
  - A prerequisite accepted in memory is in an earlier batch. Is it durable no later than the new one? Batches become durable in submission order, which `adopt_own` relies on.
  - The leader's fast vote is its proposal, not its table phase.
- **What it is expected to move:** one of the leader's three syncs in series at low load, about 2.3 ms on the runner.
- **Built (task-d89's PR; [notes](../tuplesky-impl-notes.md#a-fresh-proposals-acceptance-in-its-proposal-batch-task-d89)).** The step's answers are in Section 4.7:
  - **The guard is the only condition, but the old order held a second gate.** Learning starts from ACCEPT, so the leader never committed a command before its proposal was durable. Its fast vote is its proposal, and a follower that saw the submission first can acknowledge before then. That gate is now explicit (`Learner::commit_learned_where`), and `tests/leader.rs::an_early_fast_quorum_commits_nothing_before_the_proposal_is_durable` fails without it.
  - **A prerequisite's row is durable no later.** A domain's journal seals each record against the one before it, and a definite failure drops every batch queued behind it. The leader's tests now complete batches in that order.
  - **Every fresh proposal takes the merged batch in steady state,** at any load, since the leader accepts each of its proposals as it proposes it. The old path remains for a prerequisite held at PRE-ACCEPT, as after a recovery, with a unit test.
  - **The reading:** at one caller the leader's protocol groups fall from 2.00 a command to 1.00 and its syncs from 2.91 to 2.00, under both profiles. At ten callers they fall 14 to 18%. CPU over the voters falls 3 to 10% at one caller and is flat to 5% lower at ten. Put p50 falls 0.1 to 0.3 ms, a sync on this host's disk. The `replay` p99 at one caller is higher in both head runs than in either base run (8.7 and 9.8 ms against 5.9 and 8.0); the base's own two runs differ by 2 ms, so two runs a side do not settle it, and the batch's pairs read it again.

**Acceptance:** The Section 4.7 step first. Model tests: a fresh proposal whose guard passes carries its ACCEPT row in the proposal batch, and one whose guard fails does not. Recovery from a crash before and after that batch. The protocol simulator, every row at 1,000 seeds per row and size, with catch-up and without. task-d84's local reading again: the leader's protocol groups a command at 1 caller fall from about two to about one. Its runner pair is read in the batch job (task-d72).

**Review boundary:** When a fresh proposal's acceptance row is written. No change to the guard, to the slow quorum or to the wire.

<a id="task-d90"></a>
### task-d90: Release a result before its execution group is durable

**Prerequisites:** task-d06, task-d12, task-d35, task-d63, task-d84.  
**Design:** Sections 4.5, 17.3.3 and 17.4 (amended by this task).

**Implement:** A result is released once it is learned and its execution group is durable (task-d63). The group's sync comes after learning, a 3.69 ms wait in the last `service` run, so a write pays two syncs in series where etcd pays one: its WAL sync, at the leader and the followers in parallel. Releasing before the group is durable takes the second off the path. It is the lever that gives etcd's shape.
- **What it still waits for (#174 review):** the command decided at a quorum, and its execution in memory, which comes after every command it executes after. Only the execution group's sync leaves the path.
- **The design step first, started now** as the long pole for the p99 (#174 review). It amends Section 17.3.3's ordering of an execution before its result.
- **What the argument has to carry.** Today the releasing voter's durable execution group is a witness of the order it released. Without it, a released result rests wholly on recovery reproducing every decision and its dependencies, fast decisions through the possible-fast rule included: execution order and results are a function of the decided commands (task-d06's chain), so every recovery that may follow a release must decide the same commands with the same dependencies. The step states that for each path to a decision, and for a crash of every voter, not only the releasing one.
- **It also says** what the collector's late-release comparison (task-d12) and the execution chain (task-d35) see across that crash, and why events and watches still wait for Materialized.
- **Four preconditions, read from the code (#181 review), go in first, each with its own test:** a released `ResponseV1` carries only the command's outcome, so the oracle compares its bytes; the window a voter forgets in becomes durable-gated, since today it counts in-memory executions; a failed authorization read on a retained retry stops the node rather than landing `RetryUnauthorized`; and every vote path that counts toward a decision waits for, or reports, a durable payload row (adoption, a transferred payload's batch, `acknowledge_decided`, and a report's `payload_present`). None changes what is released today.
- **The code step's preconditions, built on `claude/task-d90-release-forget`** (point 3 is #188's). None changes what is released.
  - **Point 2, forgetting by the journal.** `CommandTable::forgotten` counts only executions this voter's journal holds, once its owner tracks them (`track_execution_durability`): a command is forgotten only once its own execution is durable, and the executions counted past it are durable ones. coordd's node turns tracking on and reports each `Established` result's position as it leaves, which is when its group is in the journal (task-d63). The models, which apply synchronously, don't track. Tests: the table forgets nothing before its executions are durable; a tracking follower reports every command it executed until the journal holds it.
  - **Point 4, each vote on its payload row.** A follower adopts a proposal only with the payload held, not with its admission digest alone, and the adoption writes the payload row when none is durable or in flight. A vote's sends wait on the batch writing its payload row. `acknowledge_decided` answers only with the payload row durable or in flight. A report names a payload present only where its row is durable, at the follower and the leader. A leader's re-proposal writes the payload row when it is not durable yet, and its own acceptance counts only over one. A payload sent for an initialized record is now counted durable, and served, once its own batch is: it was never, as only the votes' batches were read. Tests: an adoption over a record restored without its payload row waits for the payload and its batch, and the report says the payload is absent until then; a decided record is acknowledged again only with its payload row.
- **Before the release rule changes, the simulator has to reach points 2 and 4 (#195 review).** At the default seeds 10 of 11 rows give #181's decision hash bit for bit, since the models apply synchronously and don't track durability, and rows 4 and 5 rarely leave a payload row behind a dependency row. So until then their evidence is the unit tests. With the crash tests at every cut and the `ResponseV1` oracle, the code step adds:
  - **tracking on in the simulator,** with an execution group's durability reported later than its outcome (as row 18's `leader_journal_lag` does for proposal batches) and crashes cut into that window, which is the window the release rule opens;
  - **a crash between a follower's payload batch and its adoption batch,** so a restored record holds the dependency row without the payload row;
  - **a negative control for each:** with point 2's gate counting in memory, and with point 4's wait removed, some seed fails the oracle.
- **Built on `claude/task-d90-sim-cuts` (the simulator's part).** Every other row keeps its decision hash bit for bit: they flush the journal before executing, as before.
  - **Oracle 7, released results.** A result is released when a voter executes its command in memory, the earliest the rule may. Every execution of it anywhere, before a crash, after one and once healed, must give the same `ResponseV1` bytes. A response is a function of the order (position, the key's previous command and count), as a put's revision and previous value are.
  - **Oracle 8, point 2's rule:** a voter never forgets a command whose execution is not durable there. **Oracle 9, point 4's rule:** a fast or slow acknowledgement, and a report entry naming its payload present (read at the page's cut), rests on a payload row durable at the voter.
  - **Row 19, late execution.** An execution's group joins the journal behind the batches before it, as `coordd`'s does, and the machines track durability from it (`executions_durable_through`). One voter's journal at a time is slow to reach its groups, and every voter is crashed at once at each of three cuts: a command learned and executed nowhere, a result released whose group is durable nowhere, and an execution durable at one voter and not another. At the default seeds: 191 crashes of every voter (141, 19 and 81 at the three cuts), 207 executions lost to them, 6,687 executions compared with the 2,160 results released. Its table capacity is 8: a voter's undurable executions stayed within its table's capacity (at most 22 at capacity 32, even with 400 commands a run, and 8 at capacity 8), so at 32 none reached the window it forgets past.
  - **Row 19's negative control** counts executions durable as they are taken, and fails oracle 8 (three voters, seed 28: a voter forgot a command with 4 of its 13 executions durable). With oracle 8 off it failed no other oracle in 800 seeds: the journal completes in order, so nothing a voter forgets in memory is durable before its group, and the schedule never carried such a forgetting out of the voter, into a report a selection then followed.
  - **Row 20, oracle 9's sweep.** Row 2's admissions, missing most nodes, with a follower crashed while a batch writes the payload row of a record durable there without one. Oracle 9 held over 53,274 votes. Its negative control drops followers' payload rows on their way to the store, and fails oracle 9 at seed 0. The crash never fires here: the simulator completed every batch, in order, and every batch that writes a dependency row writes the payload row with it or after it, so no record was ever durable without its payload row. That is also why the first version of this row (`da424595`) passed with point 4's code reverted, and why its window and restore counts, which took a Sync's entries for records, measured placeholders nothing votes on.
  - **Row 21, a failed payload batch.** Row 20 with a follower's batch of a payload row and its dependency row failed, definitely not committed, while the journal goes on past it. A later vote's batch can then write the dependency row alone, and the crash restores such a record. At 24 seeds: 259 batches failed, 84 windows, 43 crashes in them, and 121 records restored without their payload row.
    - **What it found.** A deposed leader counted every durable proposal as a durable payload row when it became a follower (`Leader::into_recovered`). A re-proposal of an entry the leader held without its payload wrote no payload row, so the follower reported the payload present (oracle 9), counting as a supplier of a payload it could not serve. Fixed: only a proposal whose batch wrote the payload row counts, as the leader's own `served_payloads` already did. At 40 seeds a size, nine seeds failed oracle 9 without the fix and none with it; eight are kept (`fixtures/protocol_sim/seeds.json`). Since the candidate's payload fetch below, no re-proposal goes out without its payload row, and row 21 at 40 seeds a size, with and without catch-up, no longer fails without this fix: it stays, as the count `served_payloads` keeps, but nothing in the simulator reaches it now.
    - **And a campaign that never bound.** At five voters, seeds 2, 7, 10 and 35 of 40 failed progress after healing, with or without that fix: a campaign held by a majority, with every report page in, didn't complete within the election's ceiling. In seed 2, one voter's batch of a payload row and its dependency row had failed and a later batch wrote the dependency row alone, so it reported its pre-acceptance of a command without a payload, as point 4 has it. No other reporter held the command. The selection re-proposes every record below ACCEPT, so it named the command, and the candidate asked the voters that promised it for a payload none of them had, for ever. Such a command was never decided: a slow decision would be an acceptance in some report, and a fast one a pre-acceptance with its payload row at a reporting member of the fast quorum, since every vote rests on one. So the selection now leaves it out, as it does a command no reporter holds, unless a selected entry was decided after it (task-d33's case, which still re-proposes it, for the entry to execute after it; `select_decided`). The four seeds are kept. With the candidate's payload fetch below, the schedules moved: without the rule, row 21 at 40 seeds a size stalls at five voters seeds 10, 18 and 38, and without catch-up at nine seeds (three voters 1, 5, 10 and 14; five voters 10, 14, 18, 23 and 27), and none with it. Those are kept too, and 2, 7 and 35 no longer stall without the rule. A model test fails without the rule (`a_pre_acceptance_no_reporter_holds_a_payload_row_for_is_left_out`). Every other row keeps its decision hash (rows 1 to 20; row 14's is the one point 4 had already moved). Row 21 now runs as a sweep: at 40 seeds a size, with and without catch-up, every oracle holds, with 1,053 batches failed, 278 windows, 136 crashes in them and 498 records restored without their payload row (with catch-up).
  - **Row 22, the leader crashed in task-d89's window (#180 review).** Row 19's late execution, without its crash of every voter, and with the leader crashed while it has counted a fast quorum over a proposal of its own whose batch isn't durable. At 40 seeds a size it reached that window over 58 commands and crashed the leader in it 34 times (65 and 39 without catch-up), and every oracle held. With the gate removed from `Leader::learn`, five of those 80 seeds failed: the leader committed and executed in the window, crashed, and recovery ordered another command at that position (three voters seed 37; five voters seeds 3, 5, 31 and 33). Two of them are inside CI's twelve seeds, and all five are kept, so removing the gate fails CI. Row 19 still passes without the gate at 40 seeds a size, as #180's review found the sweep did: it never crashes the leader in that window.
  - **The leader's own vote.** Oracle 9 now also reads a leader's proposal when its batch becomes durable, which is when task-d89's gate lets it count: the leader's payload row has to be durable by then. Row 21 failed it at 6 of 24 seeds. A candidate held a selected command by its record alone: its payload batch had failed, or it had crashed while writing the payload row, and it came back with the dependency row only. Its `supplied` answer counts a record, so the selection kept such a command, and the binding check asked only for commands the candidate held no record of. As leader it re-proposed the command, committed selections included, since it re-proposes whatever it hasn't executed, and its own vote rested on no payload row. A leader never asks for a payload, so nothing wrote one later.
    - Fixed in two places. The selection rule above no longer counts the candidate's record as a supplier: its own report already says whether its payload row is durable. And before binding, the candidate fetches the payload of every selected command it holds unexecuted without one (`selection_missing`), as its rule says it should ("re-proposes from payloads, never from identities alone"), so the proposal batch writes the row.
    - Five seeds are kept: three voters seeds 1, 3 and 9, and without catch-up seeds 24 and 33. Each fails the check without the fetch.
    - **The fetch moves every row's decision hash.** It also fires for a placeholder, the record a proposal leaves when it arrives before its submission, which every row has. Before the fix those rows passed the new check anyway: their candidates' payloads were durable by the time the leader's vote counted. Now the candidate waits for them before it binds, which changes the schedule. With the check and without the fetch, rows 1 to 20 and 22 pass at the default seeds, and so does every row with both at 40 seeds a size, with and without catch-up. Row 21 at 40 seeds a size, with catch-up: 1,050 batches failed, 282 windows, 121 crashes in them, 407 records restored without their payload row, and 174,170 votes checked.
  - **The rule is gated with the release rule (#197 review, from #199's).** It rests on point 4 at every voter, as the release does: in a domain with a voter on a build from before #195, that voter may have acknowledged before its payload row was durable and come back with the record alone, which a report names without its payload (`payload_present` dates from task-19). Three voters: a command decided fast on {L, r1}, released by L; r1, on the older build, crashes before its payload row is durable; L goes down and r2 never had the proposal; a candidate on this build selects from {r1, r2}, finds no payload, and drops what L released. Before the rule those reports stalled the campaign until L came back, so ungated it would turn a stall into a lost decision. So it is `SelectionRules::leave_out_unheld` (`select_ruled`), carried by each campaign and set on the follower machine (`Follower::set_selection_rules`), off by default: off, the command is re-proposed as before. The batch head sets it from the node's own durable activation record of `ReleaseAtExecution`, beside the release switch. The simulator selects with it on, as a domain that activated it does, so its rows read as before (CI's seeds pass); with it off, row 21 stalls at five voters, seed 10 (the control). A model test keeps the re-proposal with it off (`without_the_rule_a_pre_acceptance_no_reporter_holds_a_payload_for_is_reproposed`).
  - **The converse of oracle 9 (#197 review).** The selection rule leaves out a pre-acceptance no reporter names a payload for. That is sound only if every voter whose vote counted, and which still holds the command unexecuted, reports its payload present; a drift in `served_payloads` (a rebind, a release, a deposed leader's count) would let a selection leave a decided command out, which is divergence, not a stall. So at each selection the simulator recomputes what the rule left out and fails if any of it was decided: counted in a quorum by a leader (`take_learned`) or by a follower as an acknowledgement reached it, learned at the frontend, or executed, unless some voter executed it durably (its executors forgot it, and catch-up serves it, task-d77). At 40 seeds a size, with and without catch-up, no row fails it; row 21 is where the rule leaves things out (78 commands at 40 seeds a size, with and without catch-up), and row 3 left out one, without catch-up. None of them was decided. The stats count the exemption apart (`left_out_executed`): left-out commands that were decided and pass only because a voter executed them durably, so they rest on catch-up and task-d77 rather than on the payload argument. It is zero in every row at 40 seeds a size, both ways. Its control hides every pre-acceptance's payload in voters' reports (`hide_preaccepted_payloads`, re-paginated so the digests hold) and fails it in row 1 (three voters, seed 12). The same hiding also fails the dependency-set oracle earlier at other seeds, since the possible-fast rule needs the payloads too.
  - **Point 4's code, each part reverted (#197 review).** Row 20's control shows oracle 9 can fire; this shows point 4's code is what keeps row 21 green. Each part reverted alone, row 21 at 40 seeds a size, with and without catch-up:
    - the adoption's payload-row write: fails from three voters seed 0, at most seeds;
    - the report reading durable rows (`report_durable_payloads`): fails from seed 0;
    - `acknowledge_decided` waiting on the row: fails at three voters seed 1 with catch-up and without, and seed 39 without (kept);
    - a vote's send waiting on a payload batch of its own: passes. The journal completes in order, so a vote's batch, submitted after the payload batch, is never durable before it, in the simulator or in `coordd`'s single journal. The follower tests that drive storage events out of order catch it: `an_adoption_waits_for_the_payload_row_not_only_its_digest` and `a_failed_payload_batch_is_written_again_by_the_next_adoption` fail without it.

    The first three fail inside CI's twelve seeds, so reverting any of them fails CI without a switch.
  - **At 1,000 seeds a row and size (the testing session's sweep on #98).** Row 21 failed five seeds, and row 22's early variant one, where #198 carries the early proposals. Two faults, each fixed here:
    - **A failed vote batch left its record unwritten.** The record stayed in the table, so the next vote on its keys named it as a dependency, durably, while no row held it. A restart lost it, the member's records stopped short of the leader's order, and a recovery read that as another order. At five voters seed 608 it re-proposed, under other dependencies, a command a fast quorum had decided; seeds 370, 459, 43 and 798 (three voters) stalled a campaign or the heal. **The fix (reworked on #197 review):** a batch that names such a record carries its payload and dependency rows (`carry_unwritten`), so they are durable with the vote that names them or fail with it. It covers the vote, rebind, adoption and Sync-install batches, and follows the dependencies on through every record unwritten. Two earlier forms were wrong. Releasing the record failed oracle 1 at seven other seeds: the command came back as a fresh pre-acceptance after the commands since. Writing the rows again at the failure looped on a fenced follower, whose store refuses every batch, until the node stopped (`a_fenced_follower_takes_a_request_and_serves_on`); and keyed on a failed vote batch, it missed a vote batch failed and then an adoption batch (`both_of_a_commands_batches_fail_then_a_later_vote_names_it`, which fails on the code before either form too). Row 21 now fails every batch queued behind a failed one, in order, as `coordd`'s journal does (`Knobs::fail_queue`, `PROTOCOL_SIM_FAIL_QUEUE=0` for one batch at a time); its seeds kept from before are replayed one batch at a time, as they were found.
    - **What the queue's failures found (#197 review).** At 1,000 seeds a size the knob failed 78 of row 21's seeds over its eight cells with the carry alone. All came from batches row 21 had never failed: adoptions, Sync installations and markers. Four faults, each fixed:
      - *A commit executed over a row of other dependencies.* A Sync's installation, or an adoption, commits in the table before its row is durable. When that batch failed, the command still executed, and a report then gave the older row promoted to COMMIT (three voters, seeds 34 and 39). Now a record whose batch failed is marked unwritten (`Follower::unwritten`). A committed one executes only once its rows are written again, durable or in flight (`next_executable`'s gate). They go out in a batch of their own on the next event that is not a failure (`carry_stale`), so a fenced queue is not written to at its failures. `a_commit_whose_adoption_failed_executes_only_over_its_rows_written_again` fails without the gate.
      - *A failed installation left the Sync's entry out of the report.* The entry is durable in the Sync's own row, but the report gave the older row at the synchronized ballot (three voters, seed 26). A failed installation now goes back to the entries pending installation, which the report names. It is installed again at the next event that is not a failure.
      - *A failed marker left the synchronized ballot advanced in memory.* Reports, and any promise row written after, labelled the old rows with the new ballot (three voters, seed 333). A failed marker now puts the synchronized ballot back (`BallotState::unmark_synced`).
      - *A dependency row durable without its payload row.* The carry skipped a record whose dependency row was durable when its payload row was not. A Sync's demotion also wrote a dependency row alone for a record whose own batch had failed. A later vote named the record, and a selection had to re-propose a command whose payload nobody held, so the campaign never bound (five voters, seeds 378 and 609). The carry now keys on the payload row too, and a demotion writes the payload row beside the dependency row when none is durable or in flight. A payload row whose batch failed while a later batch wrote the record's dependency row alone, an adoption that waited on it, is written again on the next event that is not a failure, which the rewrite at the failure used to cover.
      - *Found with the knob off, from the carry's other trajectories:* a leader crashed between its Sync row and its payload rows, so its report held acceptances without payloads while the others held them pre-accepted. The selection named the first reporter of such a command as half-initialized, a campaign set aside reports that could supply the payload, one by one, and never selected (five voters, seed 518). It now names the reporter whose acceptance lacks the payload.
      - *Found in review, of the same shape as the rewrite at the failure:* a catch-up window whose batch failed was installed again at the failure itself (task-d08, task-d25). On a store that refuses every batch, a fenced one or a failing disk, one delivered page failed 200 batches, and `carry_out` gave up after 32 rounds with the window still waiting. It is now installed at the next event that is not a failure (`catch_up_reinstall`), as `sync_reinstall` does: one failed batch a delivery (`a_store_that_refuses_every_batch_fails_one_window_a_page`). Its reach needs a journal failure, which Jepsen's kill, pause and partition faults don't cause.
    - **A report gave an older durable row for a command executed and retired while its newest row was in flight.** Execution runs ahead of the journal, so the table can let the record go first. A voter had pre-accepted a command, then installed a Sync's commit of it under other dependencies, executed it and retired it, all before the installation's row was durable. Its report gave the pre-acceptance, promoted to COMMIT (`report_executed_as_committed`), and the next selection found two dependency sets (row 22 early, five voters, seed 120, without catch-up). The report now gives the row in flight, which is also what a restart restores beside the execution.
- **The release rule, on `claude/task-d90-release-rule`.** Where the domain activated it (below), a leader's result goes to the collector as its command executes (`Node::release_executed`), before its execution group is journaled. The establishment still waits for the group's `JournalDurable`: that is when the voter counts the execution durable and may forget it in its turn (point 2), and when the learned-to-established split is stamped, so its last part, the group's sync, is now off the release path. Point 3 (#188) is merged in, since the rule rests on it. The node tests that pinned task-d63's rule are restated for this one: a staged group's results go out at the stage, before the flush journals it; a group closed at its bound releases every result as it executes, the open group's too, and its hand-off carries out the closed group's establishments; with the journal lent, every result goes out with the group's append still out, and the establishments once it is taken back. The simulator already released at in-memory execution (oracle 7), so rows 19 to 23 read the rule as built; the crash tests at every cut are row 19's.
  - **Gated as a replicated feature (#199 review).** The argument needs every voter on points 2 to 4: a build from before #195 can acknowledge before its payload row is durable, and under the rule a crash of the domain in that window could lose a result already released, where under task-d63's rule nothing had been released yet. Section 23 rules out a local flag and Section 11.7 asks for replicated activation, so the rule is `ReleaseAtExecution` with its prepare marker, through the feature path #196 adds. On this branch it is the node's switch (`Node::set_release_at_execution`), off by default; the feature and its wiring to the leader's own durable record (`follow_features`, as `EarlyProposal`'s) are on the batch head, where that path is. No held set is needed: followers do nothing differently, and activation already implies every voter's build, since each wrote its marker durably and cannot roll back; a voter added later supports it before promotion (`promotion_missing`). The commands a node holds unexecuted when the switch turns on keep the group's release, since one may rest on a vote an older build cast before its voter upgraded. Node tests: task-d63's three as the switch-off case, the three restated as the switch-on case, and a command held at the switch, which fails with the held set ignored.
  - **The switch carries the selection rule (#197 review).** The selection's drop of a re-proposal no reporter holds a payload row for rests on the same premise, point 4 at every voter, so `Node::set_release_at_execution` also sets the follower machine's `SelectionRules::leave_out_unheld` (`Node::selection_rules`), and a leader that becomes a follower (`change_role`, `step_down`) takes it with it. Node test: `the_release_rule_switch_carries_the_selection_rule_across_a_role_change`.
  - **The feature, on the batch head (#198).** `Feature::ReleaseAtExecution = 0x0006` and its marker `ReleaseAtExecutionPrepared = 0x0007` (`release-at-execution`), activated with `coordctl feature activate release-at-execution`. `Node::follow_features` sets the switch from the node's own durable activation record, beside early sends; the driver reports its set as changed when opened, so a restarted voter starts with the switch its store holds. The CLI test that activates `early-proposal` in a running domain activates this one beside it, writes under it, and finds it held after a restart. Its doc says what it attests, every voter voting on durable payload rows and forgetting by its journal, and names both rules that rest on it: the release, and the selection's drop of an unheld re-proposal (#197 review), which the node switch carries.
  - **A barrier read answered from the executed state, conditional on the batch.** The rule leaves a read right after a write waiting for that write's projection commit (the pair below, at one caller). The read-side counterpart answers a barrier read from the projection's snapshot with the groups since its last commit over it, as `planning_view` builds for planning; it rests on the same argument as the release, so it sits under the same feature. It is taken if #191's split shows the read's wait after the index as a real part of `service` p99 in the batch's reading, and dropped if not.
  - **The CLI suite in a release build:** 28 of its tests fail there on this branch's base too, because a release build refuses a one-voter genesis (`single_voter_profile`, task-d31), which most of the suite commits, and `coordd` exits 2 on it. The suite runs in debug, as CI runs it.
  - **The local pair (task-d84's reading again).** Three voters on one 4-core host, state on disk (a journal append served in about 0.77 ms p50), `command-cost.sh` at 1 and 10 callers, 3,000 operations a run, four runs a side alternating, both sides with #191's distributions merged in; the base is this branch with the rule reverted. Medians of the four runs, from the bench's client-side service times:

    | callers | path | base p50 / p99 ms | rule p50 / p99 ms |
    | --- | --- | --- | --- |
    | 1 | put | 2.00 / 3.62 | 1.61 / 3.24 |
    | 1 | transaction | 2.27 / 3.74 | 1.77 / 3.00 |
    | 1 | get | 0.59 / 1.49 | 0.64 / 2.31 |
    | 10 | put | 5.84 / 11.34 | 4.97 / 8.78 |
    | 10 | transaction | 6.06 / 11.50 | 5.09 / 9.33 |
    | 10 | get | 5.89 / 12.43 | 6.09 / 11.17 |

    A write gives up one sync after learning: −0.39 ms at p50 at one caller, about half the append's served time, and −0.87 ms (−15%) at ten, with p99 −2.56 ms there. Completed operations a second rose 7% at ten callers (1,618 to 1,737), and the leader's CPU a command didn't rise (1.37 to 1.29 ms at ten callers). A read moves the other way at one caller: the closed-loop caller's next read now arrives before the write it follows has materialized, and the barrier read waits for the projection through it (its wait after the index, 0.07 to 0.27 ms on average), so a get's p99 rose 0.8 ms there. That wait is the projection's commit, which the rule leaves where it was; at ten callers it is inside the spread.
- **Stacking with #193 (L7's code):** the two conflict only in `Leader::on_storage`'s `JournalDurable` arm, where #193 sets `early_ready` and this marks the payload served. Both are kept. Under L7 a follower's adoption of an early proposal writes its own payload row by point 4, as any adoption does.
- **The design step is recorded** in Section 4.5 ("Releasing before the execution group is durable"), as a proposal not yet in force. It rests the release on three parts: every decision is durable at a quorum before it is learned, which needs task-d89's gate on the leader's own fast vote; recovery selects every decision with its dependencies; and execution is a function of the decided commands, with each input to a result that is not the command shown to follow from the order. The rule changes only after its review, with the crash tests and the oracle below.

**Acceptance:** The design step recorded and reviewed before any code. Crash tests at every cut between the decision, the release and the execution group's durability, in which every voter crashes at each cut, not only the releasing one: the result each voter executes after recovery is the one released. A protocol simulator oracle that compares each released result with what the voters execute after healing, run over the sweep. A simulator schedule that crashes the leader after it has counted a fast quorum over its own proposal and before that proposal batch is durable, so task-d89's gate is pinned beyond its one model test; with the gate removed the sweep must fail on it (#180 review). task-d84's local reading again: one sync in series after learning fewer. Its runner pair is read in the batch job (task-d72).

**Review boundary:** When a result may be released. No change to the order of execution, to what is durable before a vote, or to what events and watches wait for.

<a id="task-d91"></a>
### task-d91: Read and cut wake and hop latency

**Prerequisites:** task-d55, task-d72.  
**Design:** None.

**Implement:** In the #173 carry's `service` row the voters' tokio threads waited 15 ms an operation for a core while 1.87 cores sat idle, and the leader's loop waited 1.35 ms a command against 0.65 of CPU. Part of every hop's latency is scheduling, and nothing owns it.
- **First, the reading (R4):** each thread's run-queue delay per wake-up (schedstat's `run_delay` over its timeslices) and wake-ups a command. It needs the runner's VMs, so it is a column of the runner's summary (the testing session's) and is read from the batch's base side, with no extra run. task-d55's `Scheduling` already reads the domain loop's.
- **Then the lever it names:** fewer wake-ups a hop (task-d70's third step, or task-d73's wait if it is taken), or where the threads run.
  - **Read (task-d72's batch, #98):** the leader's threads wait about 2.1 ms for a core an operation in all; its tokio threads wake 7.2 times an operation and run 34 µs a wake, and parking and wake-ups are about a fifth of the tokio threads.
  - **Where the threads run, read first:** a local pair, each voter's runtime at tokio's default worker count against one worker, behind an environment setting that is read only for the pair. It says whether work stealing and wake-ups across workers are a lever, and how much of the fifth they are.
  - **Read locally (#185):** the batch's head (`b59f2b0`), each voter's runtime given its worker count through tokio's own `TOKIO_WORKER_THREADS`, set for coordd alone. Same shape as the earlier pairs: 30 callers, `replay`, stores on tmpfs, 50,000 operations, sides alternating, on one 4-core host.

    | Voters | Workers a voter | Runs | Voters' CPU a command | Off the domain loops | `ok`/s | put p50 / p99 |
    | --- | --- | --- | --- | --- | --- | --- |
    | 3 | default (4) | 3 | 2.87 ms | 1.72 ms | 2,522 | 21.8 / 48.0 ms |
    | 3 | 2 | 3 | 2.49 ms (−13%) | 1.44 ms | 2,571 | 20.9 / 43.9 ms |
    | 3 | 1 | 3 | 2.09 ms (−27%) | 1.10 ms | 2,680 | 19.7 / 42.0 ms |
    | 5 | default (4) | 2 | 4.71 ms | 2.81 ms | 1,773 | 32.1 / 63.3 ms |
    | 5 | 1 | 2 | 4.15 ms (−12%) | 2.28 ms | 1,961 | 29.1 / 57.9 ms |
    | 5 | 2 | 3 | 4.34 ms | 2.56 ms | 1,889 | 30.2 / 59.4 ms |
    | 5 | 1 | 3 | 3.90 ms (−10%) | 2.13 ms | 2,045 | 27.8 / 54.7 ms |

    The last two rows are their own pair, at the runner's step: the Jepsen job already runs every voter at two workers (`voter-workers`, five voters on a 4-CPU runner), so its batch takes 2 → 1, not tokio's 4 → 1 (#185 review). Absolute figures move between pairs on this host; each pair's ratio is the reading.

    - **It's a lever, and most of it is off the domain loops:** the tokio threads' parking, wake-ups and stealing between workers.
    - **Latency doesn't pay for it.** Fewer workers also cut p50 and p99, and `ok`/s rises.
    - **The caveat: every voter here shares four cores.** tokio's default is a worker a core, so three voters run 12 workers and five run 20. The Jepsen runner co-locates its nodes the same way, and R4 read 2.1 ms an operation waiting for a core there. A node with a host of its own may gain less. That's why the default isn't changed on this reading.
  - **The lever it names:** a `[runtime] worker_threads` setting in coordd's configuration, unset meaning tokio's default. The next batch's head sets it to 1 on every node, and the batch's base doesn't. The batch's pair is then the runner's reading of it. The default is decided from that reading together with one where each node has cores of its own.
  - **Projected on the register row from the runner's step, 2 → 1 at five voters:** −10% of the head's 2.60 ms is about 2.34 ms. That's about 0.14 ms short of the re-anchored ~2.2 ms, if etcd's row confirms that target, before task-d96 or task-d90.
  - **The pair job needs the head at one worker and the base at two.** The job sets every voter's count through `TOKIO_WORKER_THREADS` (`voter-workers`), and task-d91's setting (#190) overrides it only where a node's configuration sets it. So the head's side needs a per-side `voter-workers`, or the harness writing `[runtime] worker_threads = 1` for that side (#98, the testing session's).

**Acceptance:** The reading recorded from the batch's base side, with the lever chosen from it and its local pair.

**Review boundary:** How the voters' threads are woken and scheduled. No change to the protocol or to the wire.

<a id="task-d92"></a>
### task-d92: Cut a local checkpoint publication's cost a record

**Prerequisites:** task-d51, task-d55, task-d72.  
**Design:** None.

**Implement:** R2 (task-d27, [notes](../tuplesky-impl-notes.md#cost-a-command-over-executed-history-r2-for-task-d27)) found the process's cost a command steps up once local checkpoint publications start, and does not slope with history: the leader's process goes from 1.345 ms a command in the first window to 1.45 to 1.48 in every later full window but the publication's (1.892), a follower's from 1.151 to 1.27 to 1.31, and the domain loop stays flat. The step is the publications' own cost.
- **Its size (#174 review).** The four publications' export and write sum to about 37 to 44 s against 304,000 commands, 0.12 to 0.14 ms a command a voter, which is the step. An image costs about 16 to 26 µs a record represented: 12 to 18 to export, and a write that rises from about 3 to 10 µs a record as the image grows. On the runner's 120 s register row, a voter publishing once near 65,536 records spends about 1.1 to 1.7 s, 0.15 to 0.2 ms an operation over five voters. The #173 carry's `checkpoint` lines (task-d55) give it exactly, with no new run.
- **The trim is not this batch's lever.** task-d27 keeps trimming off by default until task-d32 is in, and task-d32 needs task-d41's voter replacement and its chain (task-59, task-d13, task-d38, task-d39, task-d40, task-d42, task-d43). It stays the long-run bound on what an image holds.
- **First, a profile of one publication's `checkpoint` thread** at a few hundred thousand records: the export's reads, encoding and hashing, and the write and its syncs, recorded as a split per record.
- **With it, the growth by thread:** a split per 30 s window from `/proc/<pid>/task/*/stat` (the `checkpoint`, `materializer` and `appender` threads and the tokio workers). `Cpu` holds only the domain loop and the process, so this shows the publication's share directly, and whether the export's pin moves the materializer too: redb cannot reuse pages freed while the pin is open (task-d51).
- **Then the lever the profile leads with,** recorded in this entry before it is built. The image's format and what it carries do not change.

**Acceptance:** The profile and the per-thread split recorded first. A local pair at R2's setup: the publication's CPU a record and the step in the process's cost a command fall, and an image is byte-identical to one written before the change at the same represented position. task-j04's crash points recover as task-d51's acceptance states. Its runner pair is read in the batch job (task-d72).

**Review boundary:** How a local checkpoint publication spends its CPU. No change to the image's format, to what it carries, to when one is taken or to task-j04's recovery rule.

<a id="task-d94"></a>
### task-d94: Ask for a page when a voter's frontier creeps

**Prerequisites:** task-d08.  
**Design:** None.

**Implement:** A voter asks a peer for a page of its executed history (task-d08) only once its frontier has been still for `STILL_FOR`, 1 s, and any movement resets that clock. A voter restarted behind, holding pre-acceptances the others decided while it was gone, decides one each time the leader's re-send reaches it, every 250 ms, so its frontier is never still for a second and it never asks, not even the probe after start that the module promises. Found by the #98 review from the batch's leader-kill run on the head (the pacer is the same on base and head), and reproduced locally: a leader killed under load for 25 s and back for 8 s executed 4 commands a second for the next 40 s, holding 430. It depends on timing, and it shows up with task-d74's routing, which leaves a follower to hear decisions from the leader alone: on task-d74's head the old pacer crawled in 3 runs of 6, and on task-d84's, where followers still hear each other's acknowledgements, in none of six.
- **The window counts a page, not any movement, for a voter holding a page.** A voter that holds a page or more (`MAX_CATCH_UP_COMMANDS`, 64) unexecuted, has held work for `STILL_FOR`, and moved by less than a page in that time asks, as a still one did. A voter holding less keeps task-d08's rule, any movement starting the window over, so a healthy follower with a few commands always in flight doesn't put catch-up into its steady state (#189 review). The count is a scan of the command table, so the pacer takes it only on a turn that may ask, once a window has run, never on every pass of a follower's loop: the first push of the guard counted on every pass and cost a follower 22 to 24 µs a command in CI's command-cost job (#189 review).
- **The window starts over** when the voter is catching up anyway (a page's worth executed, or a page in hand), when it holds nothing on a turn, or when it leads. A healthy follower at low load empties between commands, so it doesn't ask; a voter behind holds work on every turn.
- **The probe after start** is made whatever the frontier does, since a voter that hasn't probed counts as holding work.

**Acceptance:** A pacer test: a frontier advancing by one every 250 ms for 60 s, holding work, asks (it made none before); the probe after start is made while the frontier creeps; a voter moving a page a window, and a healthy one that empties between commands, don't ask. It fails on the old pacer. A second: a follower holding three on every turn and executing 50 a second doesn't ask in 60 s; the same three with a still frontier asks; a page held at the same rate asks. A restart test: three voters under load, the leader killed for 25 s and started again 8 s before the load stops; within 30 s of the load stopping it holds no unordered pre-acceptance and its executed count has settled. Whether a restarted voter creeps depends on what it holds when it starts, so the restart test fails on the old pacer in some runs, not all (3 of 6 when this was written), and passes on the new one in every run (8 of 8).

**Review boundary:** When a lagging voter asks for a page. No change to what a page carries, to who serves it, or to the machine's own asks after a full page.

<a id="task-d95"></a>
### task-d95: A local fault reading a retained result's authorization stops the voter

**Prerequisites:** task-d12.  
**Design:** None.

**Implement:** A retry of a command whose record exists is admitted at execution through `retained_is_authorized` (`coord-storage::apply`), which decides whether the retained result may still be handed out. It turned a failure to read the session's authorization rows, or a stored response that doesn't decode, into `false`, and that landed `RetryUnauthorized` as the command's result at its position, a result a voter without the fault would not produce. Under today's release rule a releasing voter that hit the fault released that result once its group was durable, and task-d12's comparison stopped a node only after the answer went out.
- **The function returns `Result<bool, EngineError>`.** An engine error, and a stored response that doesn't decode (`ErrorClass::Corrupt`), are errors. At execution they are `ApplyError::Engine`, which stops the voter, as every other local fault at execution does. A session whose authorization exceeds the schema's view budget is still refused, since that is the same on every replica.
- **`retry::admit` and `retry::resolve` take a fallible authorization** and pass its error on.
- **A frontend answering a retry from the record** treats the error as having no retained answer, and the retry is answered through consensus.

**Acceptance:** A policy test: with a view whose reads fail, `retained_is_authorized` returns the engine's error rather than `false`, and admission returns it rather than `Admission::Unauthorized`; with a stored response that doesn't decode, it returns a `Corrupt` error; an authorized retained result is still `Ok(true)`. The coord-storage, coord-daemon and coordd suites pass.

**Review boundary:** Whether a local fault on the retained-result path is a result or a stop. No change to what a retained result is authorized by, to the retry record, or to the wire.

<a id="task-d96"></a>
### task-d96: Size the messages a command sends, before cutting them

**Prerequisites:** task-d70, task-d72, task-d88.  
**Design:** This entry is the sizing; a candidate it carries amends the sections it names in its own task.

**Implement:** In the task-d72 batch's call graph of the voters' tokio threads, QUIC's packet protection and connection state is about a third, and with send and receive about three fifths. They scale with packets. The ACK frequency is already 8 (task-d70), R3 measured coalescing within a step (#187, 13% fewer api datagrams and no CPU change), and C5 showed the cost doesn't follow stream tasks. So the lever left is fewer messages a command, which changes what the protocol sends, and is sized before it is built.
- **The count:** each message a write sends, by kind (the submission, its fan-out to the voters, the fast and slow acknowledgements, the commit, the release, and the client's own), and the datagrams each costs, at three and five voters, from R3's counters on the head. Beside it, what a leader-based log sends for the same write (the append to each follower, each acknowledgement, and the reply), so the count has a floor to be read against.
- **The candidates, each with the messages it saves and what it costs:** a message that can ride the next one the same peer is sent anyway (an acknowledgement on the next submission's fan-out, a commit on the next proposal), a message only one voter needs that every voter is sent, and a message whose content a peer already has. Each names the latency it adds and the protocol rule it touches.
- **The projection:** packets a command saved times the runner's cost a packet from the batch's call graph, so a candidate is carried into a batch only if it adds up to its share of the re-anchored target.

- **The count, first read (#185):** from task-d91's pairs on the batch's head (`b59f2b0`): one 4-core host, 30 callers, `replay`, tmpfs, 50,000 operations, its one-worker side. The figures are per command executed, summed over the voters. R3's counters split the api plane by kind; the peer plane has no counter by kind yet.

  | A command | 3 voters | 5 voters |
  | --- | --- | --- |
  | submissions to the voters elsewhere | 2.00 | 4.00 |
  | slow acknowledgements returned to a collector | 2.00 | 4.00 |
  | fast acknowledgements returned | 0.74 | 1.62 |
  | leader replies and releases returned | 0.53 + 0.53 | 0.76 + 0.76 |
  | reads to the leader, and their answers | 0.72 + 0.72 | 1.03 + 1.03 |
  | api frames sent, counted (callers' answers not counted) | 7.24 | 13.2 |
  | peer frames sent | 9.00 | 17.9 |
  | api datagrams sent (of them ACK-only) | 9.91 (6.20) | 20.5 (13.2) |
  | peer datagrams sent (of them ACK-only) | 3.96 (2.03) | 7.81 (4.32) |

  - **Against its floor:** a leader-based log sends 2N messages a write: the request, an append to each follower, each acknowledgement, and the reply. That's 6 at three voters and 10 at five, plus two when the caller's frontend isn't the leader. This count is about 16 frames at three voters and 31 at five, roughly three times the floor. Two kinds grow as N−1: the submission's fan-out and the slow acknowledgements.
  - **Most datagrams are ACK-only:** about 59% at three voters and 62% at five, with task-d70's threshold of 8 already on both planes. Data datagrams are about 5.6 and 10.8 a command.
  - **What the count doesn't yet say: a datagram's cost.** task-d88's pair cut 1.36 api datagrams a command and moved the voters' CPU by 0.02 ms, within its runs' spread. That pair ran at tokio's default worker count, where task-d91 found the workers' parking and wake-ups to be most of the cost. So the candidates are sized in CPU only after a datagram's cost is read at one worker. The cheapest such reading is a pair on the api plane's ACK cadence, the largest kind above, which changes no message the protocol sends.
  - **The peer plane's 9 and 18 frames** are split by kind before a candidate there is sized.
- **A datagram's cost at one worker, read (#185):** the ACK cadence both planes ask, through a reading-only override of task-d70's threshold, on the batch's head at one worker a voter. Three voters, the same shape as task-d91's pairs, three runs a side, alternating.

  | ACK threshold | datagrams a command (of them ACK-only) | voters' CPU a command | `ok`/s |
  | --- | --- | --- | --- |
  | 1 | 16.36 (8.61) | 1.935 ms | 2,812 |
  | 8 (today's) | 13.48 (8.00) | 1.966 ms | 2,785 |
  | 64 | 13.40 (7.95) | 1.934 ms | 2,815 |

  - **A datagram costs next to nothing at one worker.** 2.9 more datagrams a command moved the voters' CPU by nothing outside the runs' spread (about ±0.09 ms). That bounds a datagram at about 0.03 ms or less, and the point estimate is near zero. task-d88's pair, at the default worker count, said the same.
  - **The threshold isn't what leaves the ACK-only datagrams.** Above 8 they don't fall, so they come from QUIC's delay, on connections where no reply follows within it, not from the count.
  - **So fewer messages a command sizes small.** Removing every ACK-only datagram, the largest kind, projects at most about 0.25 ms a command at the bound and near nothing at the estimate. Cutting the fan-out or the slow acknowledgements by N−1 frames projects less, for a protocol change. None is carried into the next batch. The count stays recorded as the floor comparison, and the QUIC share in the batch's call graph is read again on the next batch's head at one worker before this entry is reopened.

**Acceptance:** The count recorded at three and five voters with its floor, each candidate sized in messages, packets and projected CPU, and the ones carried named with the design sections they amend. No code.

**Review boundary:** What the voters and the frontend send a command, counted and sized. No change to the wire or the protocol in this entry.

<a id="task-d97"></a>
### task-d97: Read what a `service` p99 operation waits for

**Prerequisites:** task-d55, task-d72, task-d91.  
**Design:** None.

**Implement:** The `service` row's p99 is 12 to 22 ms against etcd's 3 (task-d72's batch). task-d90 (L4) was to be sized against that row's R4, but R4 reports means. On the testing session's R4 for that row (#98), L4 is a p50-scale gain: 0.76 to 1.36 ms off a write's release. A sum or a mean can't say what the slowest operations waited for, so no lever on record owns the p99 yet (#185 review).
- **Distributions, not sums, on the `metrics` line:** p50, p99 and the maximum of each interval, or a log-bucketed histogram the summary folds. Three of them:
  - the leader's learned-to-released parts (predecessors, group, journal), above all the group's journal wait;
  - the domain loop's run-queue wait a turn (task-d55's `Scheduling`, today a sum);
  - the read barrier's wait.
- **And, so the account is end to end (#191 review):** each request's arrival to its answer at the node that took it, writes and reads apart; the leader's round from a submission to learned; the run queue of the sampled passes that executed a command; and the journal's and the projection's served and queued time a job, at every voter, which says whether a round's tail is the disk.
- **Read from the next batch's `service` job,** with no run of its own: the testing session's summary gains the columns, and the record says which part the p99 operations waited in.
- **Strict's publications** as a side in that job (`checkpoint-after-records: 65536`), not a run of its own. Replay's 12 ms p99, with at most one publication, already says publications aren't the whole tail.

**Acceptance:** The three distributions on the `metrics` line, with a unit test of the bucketing and the summary's fold, and the record naming what the `service` row's p99 operations waited for. task-d90's code step is then sized against that, not against the means.

**Review boundary:** What the `metrics` line reports about waits. No change to when anything is released, journaled or served.

<a id="task-d98"></a>
### task-d98: Send the leader's proposal before its batch is durable

**Prerequisites:** task-d30, task-d74, task-d89, task-d99 (its local disk pair needn't wait, below).  
**Design:** Sections 4.3 and 4.8 (amended by this task).

**Implement:** L7 (#185 review). A slow-path write pays two syncs in series before it is learned:
- the leader publishes each `Proposal` with `requires: [barrier]`, so it leaves once the leader's proposal batch is durable;
- a follower's slow acknowledgement answers that proposal, and leaves once the follower's own row and every outstanding batch are durable;
- so a slow-path write is the leader's sync, a hop, a follower's sync and a hop before the leader learns it. The fast path has one sync, since the followers take the command from the frontend's fan-out and their syncs run beside the leader's.

About 95% of the register row's writes take the slow path on the runner (a fast share of 4.7 to 4.9%), and the CI harness's leader 72 to 76% at ten callers. At the runner's 2.28 ms an append (task-d84), two syncs in series are about 4.6 ms before any queueing, above etcd's `service` p99 of 3 ms, and task-d90 (L4) doesn't touch them. etcd's leader sends before it saves (`server/etcdserver/raft.go` calls the transport's `Send` ahead of `storage.Save`, after the Raft thesis's §10.2.1), and its followers send only after saving.

- **The lever:** the leader sends its proposal once it is batched, not once it is durable, and counts its own vote only once that batch is durable. task-d89's learning gate (`Learner::commit_learned_where`) already does the second. A slow-path write then waits for the slower of the leader's sync and a follower's, plus the hops: one sync, etcd's shape.
- **Size:** about one runner sync, 2.3 ms, off every slow-path write's round at p50 and at p99. Barrier reads wait for the writes in flight, so they shorten too. No CPU change.
- **A design step first,** in its own PR, answering:
  - **Section 4.8's persist-before-send, restated:** it binds a voter's acknowledgement of another voter's proposal; the leader's proposal is where the value comes from, so the `Proposal` to followers may leave early. The `LeaderReply` to the frontend may not, since the frontend learns a fast decision from it with the followers' fast acknowledgements.
  - **The leader crashing after sending and before its batch is durable:** followers then hold PRE-ACCEPT or ACCEPT rows at ballot b for a proposal the leader's store lacks. A slow quorum can form from followers alone and the frontend learns it from their acknowledgements, so recovery has to keep it. Boot fencing has to keep a rebooted leader from proposing that command at b again with other dependencies or another seqnum.
  - **The protocol simulator,** every row at 1,000 seeds with catch-up and without, with crash cuts between the send and the leader's durability.
- **What the design step found** (Sections 4.3 and 4.8, accepted on #192):
  - **A fast decision at a follower.** A fast-set follower at three voters learns a fast decision from its own vote set: the leader's proposal and its own fast acknowledgement. With the proposal sent early, that would count over a volatile vote.
    - So the proposal leaves early only to a follower whose own acknowledgement and the proposal fall short of `fast_size`. At three voters under C2 that's the follower outside the fast set, and with the leader it makes a slow quorum. At five voters it's every follower.
    - The fast-set follower at three voters gets it once the batch is durable, as do re-sends and retries.
    - The cost: at three voters, a slow-path round whose non-fast-set follower is slow or down falls back to two syncs.
    - Followers' learning isn't changed (#192 review). `SlowOnly` at followers was the first answer. It would have had to follow the role across elections and step-downs, and it would delay a three-voter follower's fast decisions.
  - **A replicated feature gates it** (#192 review). A follower built before task-d74 hears the other followers' fast acknowledgements, and with the proposal those make a fast quorum at any follower. So the leader sends early only while a task-60 feature is active, and every configured voter must support it to activate it. Until then, today's barrier stays on every proposal.
  - **Boot fencing has one gap.** `coordd` brings the genesis leader back leading when its store is empty. With early sends, a crash before the first batch is durable leaves exactly that store. So a leader sends early only once its store holds the bound Sync of the ballot it won, or, at the genesis ballot, once its first proposal batch is durable.
  - **Recovery needs no change.** Every copy at a ballot is the leader's one order. A slow decision is durable adoptions, which never rested on the leader's row (task-d19). No fast decision is counted over a volatile proposal. So the source rule and the possible-fast rule keep what they kept.
- **The code step:**
  - `Leader::propose`, `retry_proposal` and the re-send timer publish the `Proposal` with no barrier to each follower that qualifies, while the feature is active and the leader may send early (the genesis condition). Every other copy keeps the barrier, and the leader reply keeps its own.
  - The feature: a new `coord_types::formats::Feature` variant (`EarlyProposal = 0x0004`) under task-60's unanimity rule.
  - New simulator knobs:
    - one that makes the leader's journal complete its batches later than the followers';
    - one that crashes the leader while it holds a proposal it sent whose batch isn't durable;
    - one that deposes a live leader in that window: a partition, then the heal.
  - All three are on in rows 1 and 2, and in a new row 18 at three and five voters.
  - **A negative control:** row 18 with the fast-set follower at three voters also sent early must fail the oracle on some seed. That shows the row reaches the case.
  - **Tests:**
    - **A mixed-build test:** with the feature inactive, no follower gets a proposal before its batch is durable.
    - **A `coordd` test:** a genesis leader holds its first proposal until that batch is durable, with a second batch in flight when the first completes, which keeps its own barrier. A restart after an early send comes back as a follower.
  - **A local pair before the batch, with the stores on a disk** (#192 review). On tmpfs the sync L7 removes isn't there; CI's command-cost job keeps its stores on `/dev/shm`.
    - The pair needn't wait for task-d99. Its head side is built with a cargo feature that reads `EarlyProposal` as active, as `skip-read-confirmation` serves the register check's negative control, and is never built for anything else. It sizes L7 before the wiring lands.
    - The fault runs and the batch use task-d99's activation, the production path (Section 23).
    - The pair: three and five voters, one worker a voter, sides alternating.
    - It reads #191's `round` and the journal's `served_each` and `queued_each` at every voter.
    - `round`'s p50 and p99 should fall by about one served sync plus one queue wait. If they don't, the batch doesn't carry the change.
- **Built (the code step, `claude/task-d98-early-proposal`, its own PR on #192):**
  - **The rule, in `Leader::proposal_sends`:** a copy leaves with no barrier when the node's setting is on, a proposal batch of this leadership is durable (`early_ready`), and `1 + fast_eligible(follower) < fast_size`. `propose` releases an early copy in the same step, and `retry_proposal` uses the same rule. The leader reply keeps its barrier.
  - **Two departures from the step's text:**
    - **The re-send timer is unchanged.** It re-sends only proposals whose batch is durable, so none of its copies has a barrier left to drop.
    - **The genesis condition is one rule for every leader:** no early copy until a proposal batch of this leadership is durable. A won ballot's bound Sync was submitted before that batch, and batches are durable in submission order, so the rule covers both cases Section 4.8 names. A batch in flight when the first completes keeps its barrier.
  - **The switch:** `Feature::EarlyProposal = 0x0004`. `coordd` takes the active set from `admit` at start and gives it to the node, which gives it to every leader it becomes (`Node::set_early_proposal`, `change_role`). Until task-d99 nothing activates it in a running domain. The cargo feature `early-proposal-without-activation` reads it as active, says so on stderr at start, and is built only for the local pair.
  - **A count:** `resends.early` in the metrics, the copies sent early.
  - **A follower counts no other voter's fast acknowledgement** (`FollowerRejection::PeerFastAck`). Only a build from before task-d74 sends one to a follower. With an early proposal it would complete a fast quorum at the receiving follower, so condition 2 now rests on that follower's build alone. Apart from the three-voter fast-set follower, whose proposal waits for the leader's batch, a follower takes fast decisions from the commit frontier, as task-d74 has it take its slow ones.
  - **Tests:**
    - `coord-consensus/tests/early_proposal.rs`, six model tests:
      - with the feature off, nothing leaves early (the mixed-build test);
      - at three voters only the follower outside the fast set gets it early, and at five every follower does;
      - the first batch holds every copy, and a batch in flight when it completes keeps its barrier;
      - adoptions an early copy brings back commit nothing before the batch is durable;
      - a retried batch goes early by the same rule.
    - `coord-consensus/tests/follower.rs::a_follower_never_counts_another_voters_fast_ack_toward_a_fast_decision`: at three voters, the follower outside the fast set gets r1's fast acknowledgement, equal to the leader's proposal in path and dependencies, and then the proposal. It neither learns nor commits. `equal_direct_dependencies_are_not_learning_and_guards_are_explicit` now reads `PeerFastAck` for the peer's acknowledgement.
    - `coord-daemon/tests/voter.rs::a_leader_a_node_becomes_sends_early_as_the_node_does`.
    - `coordd/tests/cli.rs::a_leader_that_sent_early_and_restarted_comes_back_following`: `EarlyProposal` written into every store, and the leader's `resends.early` above zero. Killed and started again, the leader campaigns, wins ballot 1, never leads ballot 0, and every voter reads the write.
    - The first-batch case is a model test rather than a `coordd` one, since the model test controls when a batch completes. The `coordd` test covers the restart.
  - **The simulator:** knobs `early`, `leader_journal_lag`, `window_crash` and `window_depose`, each drawn only when nonzero. So rows without them are unchanged seed for seed, with decision hashes equal to the base's. Rows 1 and 2 run each seed again with them, row 18 is the early send's own row, and the sweep workflow runs row 18.
    - **At 1,000 seeds, with catch-up and without:** rows 1, 2 and 18 are clean. Row 18 crashed a leader in the window 6,641 and 7,162 times and deposed one 2,224 and 2,427 times, and rows 1 and 2's early runs reached it 1,540 to 4,861 times.
    - **The same at half, three in four and every acknowledgement to a follower lost** (the sweep workflow's other settings): no failure. Without catch-up, a few runs learned nothing, at most two in a thousand at a size.
    - **Without catch-up, one of row 1's early runs at each size learned nothing** (seed 399 at three voters, seed 24 at five), where the base row has none. With the same knobs and the early copies off, both seeds give the same counts, so it's the lagging journal and the row's churn, not the early send.
    - **The negative control** (`row_18_fails_when_the_fast_set_follower_gets_the_proposal_early_too`) fails at three voters on seed 66. The leader crashes before its batch is durable, the fast-set follower has committed the command over the lost vote, and the restarted leader's recovery finds it accepted under two dependency sets (`IncompatibleAccepted`).
  - **The local pair:**
    - **Setup:** one 4-core host, stores on ext4 on a virtio disk, `replay`, one tokio worker a voter (`TOKIO_WORKER_THREADS=1`, coordd alone). The mix is put 50, contended 30 and get 20, sides alternate, and the head side is built with `early-proposal-without-activation`.
    - **What it reads:** #191's `round`, `served_each` and `queued_each` aren't on this branch, so it reads the bench's service latency and the voters' appender jobs.

    | Voters | Callers | Side | Runs | Operations/s | Put p50 / p99 (ms) | Contended p50 / p99 | Get p50 / p99 | Leader's slow share | Append served / queued (ms, mean over voters) |
    | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
    | 3 | 1 | off | 3 | 630 | 1.60 / 4.29 | 1.65 / 4.51 | 0.46 / 3.13 | 3% | 0.37 / 0.04 |
    | 3 | 1 | on | 3 | 652 | 1.53 / 4.35 | 1.58 / 4.76 | 0.46 / 3.43 | 32% | 0.38 / 0.04 |
    | 3 | 10 | off | 6 | 1,476 | 5.96 / 14.87 | 5.99 / 15.02 | 5.69 / 15.06 | 84% | 0.74 / 0.08 |
    | 3 | 10 | on | 6 | 1,562 | 5.65 / 14.07 | 5.70 / 13.97 | 5.56 / 15.32 | 92% | 0.79 / 0.09 |
    | 5 | 1 | off | 3 | 432 | 2.39 / 5.40 | 2.43 / 5.60 | 0.61 / 2.95 | 10% | 0.48 / 0.06 |
    | 5 | 1 | on | 3 | 456 | 2.22 / 5.39 | 2.30 / 5.46 | 0.59 / 3.00 | 62% | 0.52 / 0.06 |
    | 5 | 10 | off | 3 | 880 | 10.67 / 21.89 | 10.75 / 22.05 | 10.07 / 23.47 | 90% | 1.00 / 0.11 |
    | 5 | 10 | on | 3 | 958 | 9.83 / 20.23 | 9.90 / 20.24 | 9.45 / 22.34 | 93% | 1.09 / 0.12 |

    - **At ten callers, put p50 fell 0.31 ms at three voters and 0.84 ms at five,** and p99 0.80 and 1.66 ms. Every head run's put p50 was below every base run's, and operations/s rose 6% and 9%.
    - **That's about two fifths of the expected size at three voters, and three quarters at five.** The expected size was one served sync plus one queue wait, 0.82 and 1.11 ms here. At three voters the early copy goes only to the follower outside the fast set, so the slow quorum is the leader and that one follower. At five it's the leader and the faster two of four followers.
    - **At one caller, writes mostly take the fast path** (3% and 10% slow), which already pays one sync. The change there is under 0.2 ms.
    - **The leader's fast share isn't comparable across the sides.** With the early copy, a slow quorum often forms before the fast one. At one caller and three voters, the leader's slow decisions went from 103 to 1,060 (`missed_missing` 638 and `missed_slow_first` 422). So a head with L7 on reads its fast share as a race between the two quorums, not as conflicts.
    - **Get p99 at three voters and ten callers was higher in the first three head runs and lower in the next three.** Over six runs a side it's level, 15.06 against 15.32 ms.
    - **The runner's sync is 2.28 ms,** three times this disk's served 0.74, so the batch's pair reads the size there.

**Acceptance:** The design step accepted, or declined with the reason. If accepted: the code, the simulator's rows clean (rows 1, 2 and 18 with the new knobs, every row at 1,000 seeds with catch-up and without) and the negative control failing, the mixed-build and `coordd` tests, the local pair on a disk showing `round` fall, the leader-kill run and the standard schedule `:valid? true` on its head, and the next batch's `service` row read with it. If declined, the batch revises the `service` target, with the slow path's two syncs in series as the reason.

**Review boundary:** When the leader's proposal leaves and to which followers, the feature that allows it, and when a genesis leader may send early. Followers' learning and acknowledgements, the leader reply, recovery and the release rule are unchanged.

<a id="task-d99"></a>
### task-d99: Activate a replicated feature in a running domain

**Prerequisites:** task-60.  
**Design:** Sections 13 and 17.7 (amended by this task's design step, if it is accepted).

**Implement:** task-60 built the rule and its rows: `coord_consensus::feature`'s unanimity of the configured voters, and `coord_checkpoint::feature`'s `FeatureSupportV1` and `ActiveFeaturesV1`. `coordd` uses one piece of it, `admit` at start, which refuses a build that lacks an active feature. Nothing records support or activates (#192 review):
- `record_support` and `activate_feature` have no caller outside their tests;
- the only other writer of `ActiveFeaturesV1` is `bins/coordd/tests/cli.rs`'s `write_activation`, which writes the row into a store directly;
- `coordctl` has no feature command.

So every feature-gated change, task-d98's `EarlyProposal` first, would ship unreachable. Section 23 rules out an operator flag around a correctness gate. This task wires the path every later feature needs too:
- **Support reports through the order.** Each voter's `FeatureSupportV1` is a committed command, so every voter's unanimity check reads the same reports. A voter submits its own when its build's support differs from what the domain recorded for it.
- **Activation as an operator command** (`coordctl`). Executing it runs `activate_feature`, which refuses unless every configured voter has reported the feature.
- **One switch point.** Consensus code reads an active feature from its executed state, so a leader switches at one point in the order.
- **The harnesses:** the Jepsen job and the bench scripts activate a feature after start, on the side that should have it, so a batch reads the production path.
- **A design step first,** answering:
  - **Commands older builds can't decode** (#192 review).
    - The report and activation commands would be new command variants, and the `Command` format's window is 1..1, so a build from before task-d99 can't decode them.
    - No voter can skip a committed command it can't apply without diverging. So the first report a task-d99 voter submits at start would land in front of every older voter in a rolling upgrade.
    - **Options:**
      - A voter submits its report only once every configured voter's link advertises a capability for the new commands. The transport already negotiates per link (`CAPABILITY_FRAMES_PER_STREAM`, task-d61), and Section 11.7 forbids negotiation only as the authority for activation, not for when to submit.
      - Or reports go in only by operator command, after the rollout.
  - **The admission window, and how a leader knows a follower holds the activation** (#192 review).
    - `admit` reads only the voter's own store, so a follower that doesn't hold the activation yet still admits an older build. A feature whose safety rests on every voter's build needs a rule for that window.
    - The rule named: a gated behavior acts toward a voter only once that voter is known to hold the activation. For task-d98, the leader sends early only to such a follower.
    - **"Holds" means durable in its store, not executed,** since `admit` reads the store. A command executes before its execution group is durable, and under task-d90 its result is released before then too.
    - **No message carries that today.** A follower's executed position reaches another voter only in `NewLeader`, `PromiseRefused`, `CatchUpRequest` and `CatchUpPage`.
    - So the follower tells the leader once its store holds the activation, and repeats it at start and on a new ballot. The leader's set is volatile: empty after a restart or a new ballot until the message is repeated. Absence means not eligible, so an older follower that never sends it is safe.
  - **A voter that rolls back after reporting** (#192 review).
    - `record_support` refuses a report that shrinks, by design. But task-60's entry says rollback before activation is running the old binary.
    - A voter that reported and then rolled back leaves its larger report standing, and unanimity would count a build it no longer runs.
    - So either `admit` also refuses a build that lacks what the voter's own durable report promised, which binds a report from the moment it's durable, or the design says why a stale report is harmless.
  - **Membership:** what a configuration change does to an active feature and to the reports, which name voters of an epoch.
  - **What a refused activation says,** and where an operator reads which voters lack support.

- **The design step's proposal** ([Section 13](tuplesky-design.md#s13-feature-activation), proposed):
  - **Support is advertised as a peer message** (`FeatureSupport`), not committed. An older build drops a peer message it can't decode (`MalformedPeerMessage`), so a rolling upgrade needs no capability gate, and nothing reaches the order that some voter can't apply.
  - **Activation is two phases at the leader, on an operator's request** (`coordctl feature activate`):
    - **prepare:** every voter writes its durable `FeatureSupportV1` and answers, and the leader records each answer;
    - **activate:** the leader writes `ActiveFeaturesV1` over those rows and sends it. A voter installs it only if its own durable report names the feature.
  - **A report binds once durable, through a guard every build already runs (#194 review).** Prepare also writes a marker feature id (`EarlyProposalPrepared`) into the voter's own `ActiveFeaturesV1` record. Every build since `admit` refuses a store whose record names an id it doesn't know or support, so a voter rolled back to a build from before task-d99 refuses to start. A check in the new `admit` alone would not bind an older build. The marker gates no behavior. Preparing is the operator's step, so starting a new binary binds nothing.
  - **The window:** a follower says `FeatureHeld` once its activation row is durable, again at start and on a new ballot. The leader keeps that set in memory, per follower incarnation, and a gated behavior acts toward a follower only while it is in the set. For task-d98, the early send also needs it.
  - **#194 review, four more rules:**
    - **a generation replacement keeps the feature rows:** the handoff copies the voter's own support row, markers and activation into the staged generation, and selection refuses a generation that lacks one the active one holds, since `checkpoint_v1` is node-private and no image carries it;
    - **the held set is per follower boot,** and a follower's entry clears on a new peer link from it, since a restart keeps the incarnation;
    - **the node's own switch turns on at runtime,** once its own activation row is durable;
    - **a leader change in the middle:** a follower's `FeatureHeld` carries the activation record it holds, and a new leader whose own support row names the feature installs that record unchanged and resumes the re-sends, since `activate_feature`'s reports are in the old leader's store. `activate` is idempotent, and `feature status` shows a partial state;
    - **a voter that joins after an activation** holds no feature rows, so promotion to voter requires a current advertisement of every active feature, and the leader prepares and activates for that voter alone before the promotion completes (Section 22's learner rule). The joiner installs the record unchanged, with the original reporters, and `feature status` places a voter in the active set from its `FeatureHeld`, not from the reporters.
  - **No ordered switch:** a feature that changes what every voter executes needs its own.
  - **Membership:** everything names its epoch. An activation stays, and a prepare in flight at a change is run again.
  - **Operator output:** a refusal names the voters missing, and `coordctl feature status` reads the leader's view.
- **The code step, once the design is accepted:**
  - the five peer messages, appended to `ProtocolMessage`: `FeatureSupport`, `FeaturePrepare`, `FeaturePrepared`, `FeatureActivated` and `FeatureHeld`;
  - the leader's prepare and activate driver and its re-send;
  - the follower's handlers;
  - the prepare marker ids in `Feature`, written with the support row, and `admit` reading a marker as a requirement on `Supported` only;
  - the leader's held set, keyed by follower boot, and task-d98's `sends_early_to` reading it;
  - the node's switch turned on when its own activation row is durable;
  - the handoff's copy of the feature rows into a staged generation, and selection's refusal of one that lacks them;
  - a new leader's resumption from a follower's `FeatureHeld`, installing the record it carries;
  - the promotion check and the single-voter prepare and activate for a joining voter;
  - `coordctl feature activate` and `feature status`, over the admin API;
  - the harnesses' activation.
- **Built (the code step):**
  - **Rows and markers** (`coord_types::formats`, `coord_checkpoint::feature`): `EarlyProposalPrepared = 0x0005`; `prepare_feature` writes the support row and the marker in one batch; `install_activation` installs another voter's record unchanged over this voter's durable support row, keeping its own markers; `admit` reads a marker as a requirement on `Supported` and returns only what turns a behaviour on. Each rule also takes explicit state (`prepare_rows`, `activation_over`, `installed_record`, `check_support`), since a batch is durable in the journal before its rows are in the view.
  - **Messages:** the five, appended to `ProtocolMessage`. `FeatureActivated { feature, record }` and `FeatureHeld { record }` carry the record as `FeatureRecord { epoch, features, reporters }`, the row without the cluster and domain, which the link names.
  - **The driver** (`coord_daemon::feature`, wired through `Node` and `Voter` as the floor is): advertisements at start and on each new peer link; the leader's prepare, its record of each durable answer, its activation once every report is durable in its store, and the send. A follower installs and says `FeatureHeld` once durable.
  - **The window:** the leader keeps who said `FeatureHeld`, cleared on any new peer link from that voter (a restart has only new links, so this is at least as strict as per boot) and on a new ballot. `Leader::set_early_holders` narrows `sends_early_to` to it. The node's switch follows its own durable record.
  - **The re-send,** once a second while leading: it asks a voter whose advertisement it has not heard since its last link (a follower answers the leader's `FeatureSupport` with its own), and sends the prepare and the record to every voter not holding an active feature. That one path finishes an activation after a leader change, with the record a follower's `FeatureHeld` gave the new leader, and prepares and activates a voter that joined after one.
  - **The operator:** `coordd` takes requests on `<state_directory>/admin.sock`, the owner's alone; `coordctl feature --socket <path> activate <name> | status`. A follower refuses and names the leader.
  - **Handoff and promotion:** `FeatureRows`, `carry_feature_rows` and `select_installed`'s refusal of a staging without the active generation's rows, and `promotion_missing`. No running daemon installs a generation or promotes a learner yet; task-d40 and task-d41 call these where they do.
  - **Harnesses:** `scripts/bench/activate.sh DIR FEATURE...`, and `ACTIVATE=early-proposal` on `command-cost.sh`. The Jepsen job is the testing session's: it runs `coordctl feature --socket <state>/admin.sock activate early-proposal` against each node after start until one accepts.

**Acceptance:** The design step accepted. Then the code, with tests:
- activation is refused without every configured voter's support;
- an activated feature survives a restart, and a build without it is refused at start;
- a prepared store is refused at start by a build from before task-d99, through that build's own `admit`;
- after prepare and activate, a voter replaced through install and handoff refuses a build without the feature at its next start;
- a follower restarted after it said `FeatureHeld` gets no early proposal until it says it again;
- a node's early send turns on once its own activation row is durable, with no restart;
- a leader change between activation and the last follower's install still finishes the activation, with the old leader's record and reporters;
- a voter added after an activation is refused promotion on a build without the feature, and on a build with it ends up holding the activation;
- the window rule holds in a mixed-build test that includes a voter from before task-d99, not only a voter without the feature;
- a voter that rolled back after reporting is handled as the design step decides;
- `coordctl`'s command works against a running domain;
- the harnesses' activation runs.

**Review boundary:** How a feature becomes active in a running domain. No feature's behavior changes here.

<a id="task-d100"></a>
### task-d100: Answer on the release, then relay a write through the leader while rounds are slow-only

**Prerequisites:** task-d74, task-d90, task-d98, task-d99.  
**Design:** Section 4.3 (amended by this task's design step: answering on the release, and the leader relay), and Section 4.6's rule on a hop to the leader.

**Implement:** Item b of task-d72's levers (#185 review), in two steps built in order (#202 review). The design step is in Section 4.3; this entry is its code step.
- **Step A: the collector answers on the release alone, for every command.**
  - **The wire.** A mark on a submission, carried on the leader's proposal: the command is answered on the release. For a marked command the leader sends no `LeaderReply`, and every voter sends its fast and slow acknowledgements to the leader only. The release envelope carries the admission digest of the payload the leader decided, outside the result's bytes.
  - **The collector.** It marks commands while its node reads `Feature::AnswerOnRelease` active, and answers a marked command on the leader's non-speculative release alone, with `check_release` unchanged, once the release's admission equals the one it submitted under. A release under other facts is held, as a release without learning is today. The `(None, true) => "votes"` corroboration of `settle_from_record` goes.
  - **The leader's evidence repair** (`repair_evidence`, task-c02) publishes the release again for an exact duplicate of a marked command it has executed, instead of its reply. The daemon rebuilds the release from the leader's retry record and its table record's admission, since the `EvidenceStore` keeps no release. A duplicate ahead of the projection waits for it through the command's position, and one ahead of the release waits for the release; neither is refused. A follower repairs nothing for a marked command.
  - **The frontend's hold** (`parked.rs`) holds nothing for a marked command, and goes once no supported build leaves a command unmarked.
  - **The switch:** `Feature::AnswerOnRelease`, read on the collector's side, which reads no feature today.
- **Step B: the leader relay while rounds are slow-only.** Built inside task-d102's batched round, under its `BatchedRound` feature, not alone; what follows is what that task builds of it.
  - **The wire.** A relayed `Submit`, marked as such and sent to the ballot's leader only. The advice bit on the release envelope. A `Proposal` form that carries the payload record beside the vote. A refusal naming `NotLeading` for a relayed submission at a voter that isn't leading.
  - **The leader** sends its release to the origin it recorded. A duplicate relayed submission records its origin, and a command already executed is released to it at once.
  - **The followers.** A relayed proposal's payload is checked as a `PayloadResponse` is: its identity is the proposal's command, and its admission digest the proposal's vote's; one that fails either is dropped and fetched as today. A relayed proposal initializes the command from its payload and adopts it in one batch (payload, dependency and ACCEPT rows), with no pre-acceptance and no fast acknowledgement. The path log applies the adoption as a synchronization of a command not in the suffix (Section 4.1).
  - **The collector.**
    - It relays per command while its ballot's leader advises relay. The advice is a bit on each release: the leader reads the fast share of fanned-out commands over a window, advises relay below a threshold with hysteresis, and while relaying advises fan-out for short stretches that every collector takes together, counting only commands decided inside them. The threshold, the hysteresis and the stretches are settings.
    - A relayed command is answered on the release alone, as every marked command is under step A, the fallback included.
    - On a ballot change it re-sends to the new leader. On no release within a bound it falls back to today's all-voter submission.
    - A collector with no voter beside it keeps fanning out.
  - **The switch:** `Feature::LeaderRelay`, activated after `AnswerOnRelease`. Until both are active, nothing relays.
  - **task-d84's sync budget, its parts 2 and 3 (on #185), serve fanned-out rounds only:** a relayed adoption is part 2 for every relayed command, and part 3 doesn't arise. Their size is read at the fan-out share the switch leaves, on the `service` row.
- **What it is expected to move** (Section 4.3), on the runner, at its register mix of 0.74 commands an operation at 30 callers and 0.93 at 120:
  - **step A:** about −0.26 ms a command in the voters' threads and up to −0.28 in the kernel, from about two thirds of the datagrams; about −0.19 to −0.24 ms an operation in the servers;
  - **step B, on top:** about −0.13 ms a command from the remaining datagrams, −0.12 to −0.16 from the followers' `on_request`, and about −0.03 from one protocol row a command instead of two at each follower; about −0.21 to −0.30 ms an operation;
  - together at most about −0.40 to −0.54 ms an operation in the servers, before what moves into the adoption.
  
  Bar 1 takes the servers' part. Step B is sized again on the runner after step A, before it is built. The relayed proposal's added bytes, and whether they still fit the proposal's datagram, are read off in the code step.

**Acceptance:**
- **Model tests, step A:**
  - a marked command is answered on a final release alone, and a speculative release is held;
  - a release whose admission differs from the submitted one is held, and the entry settles from the record or a voter's `OtherFacts` refusal, with the answer today's collector gives;
  - a connection lost after execution with the leader still leading: the re-sent submission gets the release again from the leader's evidence repair, rebuilt from its records; a duplicate that arrives before the release waits for it; and past what the leader's table remembers, its `Forgotten` refusal settles the entry from the record;
  - marked and unmarked commands mix while the feature activates, and neither waits for evidence nobody sends.
- **Model tests, step B:**
  - a relayed command is learned on the slow path and released to its collector;
  - a follower adopts a relayed proposal in one batch and sends no fast acknowledgement, and drops a relayed payload whose identity or admission differs from the proposal's;
  - relayed and fanned-out commands mix on one key and every decision agrees, the fanned-out ones only slower;
  - several collectors relaying and fanning out on conflicting keys: a probe stretch reads, after a tail of the advice's one-completion lag at each collector, the share it would read with no relaying, and the advice returns to fan-out when the share recovers;
  - a collector acting on stale or missing advice stays correct;
  - each leader-change case in Section 4.3: never proposed, adopted below a slow quorum, decided, sent to a voter that isn't leading;
  - the fallback to the all-voter submission, completed on the release alone.
- **The protocol simulator:** an answer-on-release setting and a relay setting (the share of commands relayed, and switching during a run) over the sweep at every loss and both catch-up settings, with every oracle, the released `ResponseV1` compared byte for byte included.
- **Crash tests:** the leader crashing after a relayed proposal left and before its batch is durable, and every voter crashing between the release and the execution group's durability.
- **Measurement:**
  - a local pair for each step, with frames and datagrams a command, the followers' `on_request`, rows and journal groups a command at a follower, the voters' CPU an operation and the leader's tokio CPU a command, and the fast share, at 30 and 120 callers;
  - the switch's settings chosen from a pair against the `service` row's p99;
  - the runner pair for each step in the batch job (task-d72), step A's before step B is built.

**Review boundary:** How a write reaches the voters and how its result reaches the collector. No change to the learning predicates, recovery, what a vote rests on, or the result's bytes.

<a id="task-d101"></a>
### task-d101: Let followers do less: what reads a follower's projection, and what each option saves

**Prerequisites:** task-d12, task-d86, task-d90.  
**Design:** Section 17.4 (amended by this task's design step: what a follower's projection is for).

**Implement:** The two local profiles and the row count on #185, read against the runner's tmpfs job at 30 callers (#185 review): the four followers' materializers cost about 0.55 ms an operation and their executions about 0.19, a fifth of the servers, the largest block bar 1 had no lever for. Since task-d90 none of it is on a write's path. The design step is in Section 17.4; this entry is its code steps, each after its own review.
- **First, the readers.** Every reader of a follower's projection in `coordd`, checked against Section 17.4's table: which wait on a commit (its frontend's gate for a new session, a watch at the node, a signature or external call at the node, a resolve the projection can't yet answer, a half-held entry settling from the record, a page of catch-up arriving), which read a consistent older prefix correctly (retry answers, which carry their request, the comparisons with the leader's result, catch-up served), and which force a commit (the checkpoint, the forgetting floor, an election). A reader the table misses is added to it before any option is built.
- **Option 1, a follower's own commit cadence,** as task-d86's follower half. A follower's materializer merges commits up to a bound of records and time; a reader that waits asks for the next commit, a catch-up page's end is a commit point, and a watch's events wait out the time bound (events at journal durability being a later design step of their own). The record bound sits under `PIPELINE_RECORDS`, or `PIPELINE_RECORDS` rises with it, since a drain waits on the domain thread. task-d86's size is restated as the leader's half and the followers'. About −0.25 ms an operation over four followers on the runner's tmpfs fit at 30 callers, most of it inside C4's counted −0.2.
- **Option 2, fewer rows a command:** protocol rows in the journal only, the retry window retired in batches, one history-and-event row a revision. Each is its own design step, about −0.03 to −0.04 ms an operation a row on the runner, about −0.15 to −0.2 together.
- **Option 3, followers apply the leader's write set,** is sized at −0.06 to −0.075 ms an operation (before about 1.4 KB a command to four followers) and gated on the domain loops' cost model (task-d72): it is built only if that reading shows a follower's executions are a block the loops' parity budget needs cut, and only once its argument is written and reviewed, starting from the four checks that compare a voter's execution with the leader's today (Section 17.4): each command still executed independently by one follower, by rotation, and compared with the leader's write set.
- **Option 4, lazy execution,** is not built: within a bound it is option 1 with a larger batch, and without one it changes failover time. Its one further saving, coalescing a row a batch writes twice, goes with option 1 and is sized from a count of the rows a batch coalesces, at most about −0.05.

**Acceptance:**
- **The design step** recorded and reviewed before any code, with the readers' table checked against the code.
- **Option 1:**
  - model tests that the gate asks for and gets the next commit, and a watch's events at a follower go within the time bound;
  - that a retry against a projection behind the journal is submitted, and that a resolve of a command this node has executed but not committed waits for the commit and is never answered `Unknown` or `Forgotten`;
  - that `settle_from_records` at a follower after a ballot change asks for and gets the commit;
  - that a catch-up page's end commits, so the next page's request doesn't wait out the bound;
  - that an election commits before the new leader serves, and that the checkpoint and the forgetting floor force their commits;
  - the record bound checked against `PIPELINE_RECORDS`, with no drain at the bound's setting;
  - the storage crash tests at every cut between a follower's execution and its delayed commit, under `strict` and `replay`;
  - a local pair with commands a commit at each voter, the followers' materializer CPU a command, the gate's and watches' wait and catch-up's pages a second, at 30 and 120 callers, and a row with watches open at every node; the runner pair in the batch job (task-d72).
- **Option 3, if built:** its argument reviewed, the protocol simulator with every oracle over the sweep, and a negative control: a follower whose own execution is altered on its rotation's commands stops the node.

**Review boundary:** When a follower commits its projection, and what it executes. No change to the protocol, to what a vote rests on, or to what is released.

<a id="task-d102"></a>
### task-d102: A contended round is a batch: one frame, one group and one acknowledgement a turn

**Prerequisites:** task-d61, task-d84, task-d86, task-d99, task-d100, task-d101.  
**Design:** Section 4.3 (amended by this task's design step: a contended round is a batch), and Sections 4.8 and 4.9, which the acknowledgement through a position rests on and doesn't change.

**Implement:** From #185's next steps (#185 review): every per-command message and group of a slow round folded into one a turn, for one domain. The design step is in Section 4.3; this entry is its code steps, each its own PR after its own review, in this order.
- **First, the reading,** on step A's head at five voters, register row, 30 and 120 callers. It reads:
  - datagrams a command by plane and kind, with ACK-only datagrams on their own line;
  - the batch at each hop: commands a proposal frame, positions an acknowledgement frame, and releases a release frame (each one today, by construction);
  - the events the leader's loop takes between two flushes, which bounds those batches;
  - commands a journal group and a projection commit at each voter;
  - the servers' CPU a command.

  The figures in the acceptance below are read against it.
- **The batched frames (`Feature::BatchedRound`),** with task-d100's step B built here and not alone:
  - **a turn is the leader loop's work between two lowered groups.** The leader queues what it orders for each follower and each collector, and cuts the frames where the loop lowers a journal group (no event ready, `owes_flush`) or at the frame bound. No timer cuts a frame, and one caller gets a frame of one at once;
  - one proposal frame a follower a turn, carrying every proposal of the turn in sequence-number order with its vote, a relayed command's payload record, and the commit frontier; split only at the transport's frame bound. A turn that orders nothing for a follower sends it the frontier on its own, as today;
  - step B's relay, switch, advice bit and fallbacks as Section 4.3's B1 and B2 state them, the advice bit once a release frame;
  - one release frame a collector a turn, each entry checked as one release is today and independent of the others: one `check_release` holds or refuses doesn't hold or refuse the rest;
  - a follower adopts a frame's proposals in one journal group, each checked and adopted as step B adopts one.
- **The acknowledgement through a position (`Feature::AckRanges`),** after the frames:
  - `AdoptedThrough { ballot, ranges }`, its ranges bounded, sent once a batch is durable; above the bound, and for an adoption with no sequence number, today's `SlowAck`;
  - the leader counts a `SlowAck` with its own proposal's command and admission for each position newly covered, and drops a position it didn't number. A covered position counts only for the proposal the leader holds at that position now, and counts nothing if that proposal was replaced or swept;
  - a debug assertion in `put_proposal` that a proposal is never replaced within one ballot, which every simulator row runs. Reading `repropose`'s callers doesn't settle whether such a replacement happens; the counting rule above holds either way;
  - per voter, the contiguous position counted (`adopted_through`, task-d59) and the ranges above it.
- **The groups and the commits, under their own entries in this order:** task-d84's parts 2 and 3 (one journal sync a turn), then task-d86's leader half and task-d101's option 1 (a follower's commit cadence). This task's acceptance reads them together.
- **After the first milestone:** a collector's submissions folded into one frame a turn. The goal's datagram target needs it, and it is sized from the first milestone's reading.
- **Not built:** the extension across domains Section 4.3 describes.

**Acceptance:**
- **The design step** recorded and reviewed before any code, the acknowledgement's argument with it.
- **Model tests, the frames:**
  - a turn's proposals to a follower leave in one frame, in sequence-number order, and a follower adopts them in one journal group;
  - a payload in a frame whose identity or admission differs from its vote's is dropped and fetched, and the frame's other proposals are adopted;
  - a turn's releases to a collector leave in one frame, and a collector answers each entry exactly as it answers one release; an entry held or refused leaves the frame's other entries answered;
  - a turn that orders nothing for a follower sends it the commit frontier on its own;
  - with one caller, each frame leaves at the first flush, holding one command;
  - a frame lost, duplicated or split at the bound changes no decision, and the re-send (task-d49, task-d59) re-sends what a voter hasn't acknowledged.
- **Model tests, the acknowledgement:**
  - a range is sent only once every adoption it covers, and every outstanding batch of the voter, is durable;
  - the leader counts a covered position once, with its own proposal's command and admission, and never toward a fast quorum;
  - a hole (a payload fetched, a lost frame) leaves the positions above it counted, and a proposal held for a rebind (`awaits_rebind`) or refused is never covered;
  - an adoption restored after a restart is acknowledged by `SlowAck` until `ProposalRequest` gives it a position;
  - a position past the leader's numbering, or of another ballot, is dropped;
  - a covered position whose proposal the leader replaced or swept counts nothing, and the replacing proposal is counted only once a range covers its own position.
- **The protocol simulator,** with both features on in every row and the oracle over the sweep, at three and five voters, with and without catch-up, and the weekly sweep's knob added by the testing session. Two negative controls, each caught by the oracle: a voter that covers a position before its adoption is durable, under crash; and a leader that counts a range under the admission of another proposal.
- **The storage crash tests** at every cut between a frame's adoption batch and its acknowledgement.
- **The counts,** at five voters on the register row, on step A's counters, in a local pair and then the runner pair in the batch job (task-d72):
  - **datagrams, two targets,** with ACK-only datagrams on their own line:
    - the first milestone (task-d72's 2.0× budget): at 120 callers, at most 5 datagrams a command over both planes (about 16.8 on step A's base);
    - the goal (parity): at most 3 a command at 120 callers, with a collector's submissions in one frame a turn;
  - at least 10 commands a journal group at every voter (about 4.9);
  - the servers' CPU a command at 120 callers at most half of its value at 30.

  A miss is read before the next part is built: which hop's batch is small, and why.

**Review boundary:** What a slow round's frames carry and how many there are, and what a slow acknowledgement's frame claims. No change to what a vote rests on, to the quorum rules, to recovery's selection or rows, or to the fast path.

## Gate checklist and deferred work

task-s01, task-s02 feed the strict storage reference through task-07. Optional task-s03, task-s04 need not merge to release redb; retired task-s05 through task-s08 are not replaced by migration or mixed-engine support gates. Same-engine crash/restore, common/local checkpoints, safe replacement and schema lifecycle remain requirements.

G3 requires task-43/transitive prerequisites, G4 task-48, G5 checkpoint/replacement/restore/upgrade through task-60 rather than merely all-voter task-51, and G6 task-66 including task-q01. Fixed-member observer previews may precede dynamic membership, but general production combines both. Code merged is not evidence that acceptance passed.

**v1.5 amendment, from review of the open implementation PRs.** Three runtime gaps the task PRs recorded as unowned now have owners. task-d01 wires leader election and ballot adoption into `coordd` (recorded on task-j08); it is a prerequisite of task-64 and task-m05, and the open regional-failover row of task-48 waits on it. task-d02 drives leaf renewal inside the serving daemon (recorded on task-58); it is a prerequisite of task-65 and task-66. task-43 verifies the genesis signature at `init` and at start (recorded on task-58). Committed voting-key/incarnation replacement moves from task-58 to task-m03, where a committed membership is first installed into a running daemon, with its interrupted cases under task-m05; task-58 keeps classification, fencing and the durable adoption, and becomes a prerequisite of task-m03. The later review of recovery time moves the install of an activated configuration into a running daemon to task-d40 and the replacement of a voter to task-d41, ahead of task-m03, which keeps the in-place key rotation, with its overlap and task-58's end-to-end tests, and observers, notifications and resizing. The unenforced `max_request_bytes` bound is a follow-up on task-c01, in its own PR. A review of what a manual test on separate hosts would meet added two more: task-d03 re-dials peers and collector links on a timer, since both planes are dialled once at startup and every connection ends at the transport's age cap, so a mesh heals today only by restarting nodes; it is a prerequisite of task-d01 and task-64. task-d04 provisions a multi-host test domain from the harness and writes the runbook; it is a prerequisite of task-65. The Jepsen client's leader-kill run added one more: task-d05 bounds recovery reports and Syncs by what the voters executed, since both carry the whole history today and an election after enough of it cannot complete or leaves the new ballot refusing work; it is a prerequisite of task-64, and task-d04's real-hosts run waits on it. The same client's runs found two more. task-d06 keeps one execution order on every replica when a table reclaims: retiring a key's latest command broke the dependency chain, so a follower behind a full leader executed the committed commands in another order and answered from it; being safety, it goes ahead of task-d05. task-d07 re-sends a proposal until every voter has voted on it, since a proposal a voter never received (not linked yet, refused by a full lane, or ahead of its Sync) is never sent again and that voter holds everything after it until a Sync. Both are prerequisites of task-64. A later reading of the Jepsen runs found that no domain served past its first election: task-d05 is its first cause and becomes the top liveness priority, with the table capacity made configuration as its first commit, and task-d07 its second, broadened from a missed proposal to every proposal a voter did not receive. They follow task-d06 in that order. The five-node Jepsen runs found one root cause behind the stall that remained: a follower learns a decision only from its peers' acknowledgements, each sent once on a lane that drops, so one missed quorum stops it for good. task-d09 has the leader carry its commit frontier to every voter; it is the top liveness priority. task-d10 then makes catch-up flow-controlled and driven by each voter's own frontier, and task-d08 wires the design's checkpoint catch-up for a voter behind the leader's retention. All three are prerequisites of task-64. The stress runs with those carried found a recovery gap: a Sync installs only its entries, so an acceptance of an earlier ballot survives it and is reported as the new ballot's. task-d11 demotes such an acceptance at installation; it is a prerequisite of task-64, ahead of the next five-node run. The next stress and Jepsen runs found both safety failures that remained to be one fork: a new leader chained its first fresh proposal after an old command it had executed long before, not after its tail. task-d12 anchors the new leader's chain at what it executed and has the collector compare a late release with the answer it gave; it is a prerequisite of task-64. The node that stops on a divergence serves again after a restart; task-d13 keeps it stopped until it is replaced (Section 5.4); catch-up adds to a history and cannot undo one. The stress runs with task-d10's refusal carried found a stall that the refusal only hid: a new leader re-proposed a recovered command under the facts of its own presentation, and a follower that had accepted another presentation never voted on it. task-d14 has recovery name the facts a command was accepted under; it is a prerequisite of task-64. The stress run rep108-3 then showed what repaf-4, and most likely raf-2, had been: a leader that never learned a proposal whose acknowledgement it lost behind a counted one, since task-d07's re-send stopped asking for it; task-d15 has the leader ask again for every vote it still needs, amending task-d07's rule, and is a prerequisite of task-64. Every failed peer dial in those runs said TLS alert 120, which is the other plane's listener refusing a dial by design; a dial logged only the last address's error, so why the right address failed was never seen. task-d16 has a dial say why each address failed. A five-node Jepsen run stopped a node on `release-record-mismatch` and kept no store to read it from, and the stop said only the command's first bytes; task-d17 has the stop say what it compared, both sides and this node's executed rows around them. A checkpoint cannot be installed into a live voter and nothing trims the rows a peer would serve, so task-d08 brings a lagging voter up from a peer's executed history instead, taking in task-d10's catch-up, and task-d13 no longer waits on it. A review of the stack against an external SwiftPaxos correctness checklist ([review record](tuplesky-checklist-review.md)) found two safety bugs, each shown by a failing test, and gaps in bounds and ownership; it adds task-d18 through task-d33, all prerequisites of task-64, in this order. Safety first: task-d18 keeps a Sync from lowering a durable promise, task-d19 counts only adoptions toward a slow decision so that recovery keeps every one at five voters, task-d21 settles whether a recovery cycle is reachable and never stalls on one, and task-d30 runs the real replica machines in the simulator under a protocol oracle. Then bounded recovery time: task-d20 proves the largest Sync fits its row or refuses the campaign, task-d28 lets a report survive a lost page, task-d24 keeps table room for recovery work and releases records decided nowhere, task-d25 makes catch-up outpace the domain with windowed installation, and task-d22 ends every collector entry the voters refuse. Then bounded storage: task-d26 states the resource contract, task-d27 wires quorum-safe forgetting into `coordd`, and task-d32 reinstalls a voter behind its floor as a learner. Then the contract: task-d23 tells a client what is known of its outcome, task-d29 writes the failure and obligation contract, task-d31 refuses voter counts other than three or five and reads the source fast set from its ballot, and task-d33 holds the simulated domain to its budgets and to progress after healing; the bugs its oracles found land first, in their own PR. task-d30's simulator then found decisions recovery lost or contradicted: task-d34 fixes them, ahead of task-d30, and is a prerequisite of task-64. A review of recovery time and storage integrity adds task-d35 through task-d44, also prerequisites of task-64. task-d35 chains every replica's execution, and task-d36 has the voters compare their chains under a majority rule that never counts a missing report as disagreement and records a disagreement no majority can settle; both go with the safety work, after task-d18 and task-d19, since they would have explained `release-record-mismatch(c96f0e70)`. task-d37 scrubs the replicated state at agreed positions from a snapshot pinned off the serving loop, with the bounded-storage work. task-d42 records the prepared transition the rest rests on, with an executed point of no return that orders a cancellation against the seal, task-d39 adds a staged learner and widens task-d08's contract to serve it, task-d44 serves executed history to every old voter and named successor once begin-sealing has executed, task-d43 recovers the terminal closure after the seal, which nothing does today, as a closure chosen by single-decree Paxos, task-d38 binds the handoff to the chain, so a handoff pause no longer grows with the state, with the boundary state checked by a scrub of the successor epoch (its design amendment, which also carries task-d42's point of no return, task-d43's terminal recovery and task-d44's serving rule, lands as its own reviewed PR first), task-d40 installs an activated configuration into the running daemon through an epoch-boundary record each successor applies at P+1 and promotes the learner (its design amendment on the old epoch's state also lands first), and task-d41 replaces one voter through the sealed handoff; all of them go ahead of task-d32, which reuses task-d41's path, and of task-m03, which builds on them and keeps the in-place key rotation with task-58's end-to-end tests. None of task-d01 through task-d34 changes the design: each is work the design already required and the plan had not named. task-d35 through task-d40 and task-d42 through task-d44 do change it: each amends the design sections it names in its own PR, or in task-d38's. task-d35 changes the executed row, the execution frontier and apply base, the journal record, the checkpoint manifest (paginated), the backup format and the genesis manifest, and adds a command that changes the report and scrub intervals and the mode; task-d36 adds a peer message and a durable undecided-divergence state; task-d37 adds durable roots; task-d40 an epoch-boundary record that carries the epoch it enters; task-d42 a transition's commands and its record in `config_v1`; and task-d43 the seal report's recovery report and executed position, the closure's stored pages, and the handoff promise and accepted rows. This is accepted because nothing is deployed in production yet.

**Throughput amendment, from the Jepsen client's first unthrottled runs.**
- **What the runs measured.** On the CI runners, TupleSky served 23.8 `ok` a second where etcd served 1252.7 and SwiftPaxos 1670.3. Locally, on loopback with three voters, the ceiling held at 38 to 46 completed commands a second from one client to fifty, and stores on tmpfs raised it only by about a third.
- **Where the time goes.** Two costs, both on each node's single domain thread:
  - about five lowerings and fifteen `fdatasync`s per command on every node, nothing grouped across commands. This is the ceiling on a runner's disk.
  - per-turn and per-event scans over the whole retained history, which saturate a follower with one client. This is the ceiling where syncs are cheap.
  - Re-sends of proposals a voter already acknowledged, since the re-send window counts only slow adoptions, add duplicate votes to the busy follower.
- **The tasks.** task-d45 measures a command's cost on every node and gates on it in CI. task-d46 makes per-turn work independent of history. task-d47 lowers a turn's transitions as the durable groups Section 17.3.3 already requires. task-d48 commits the projection in one phase under the journal; a non-durable projection stays task-j06's `journaled-replay` profile, promoted from optional only if the numbers justify it. task-d49 re-sends only what the leader still needs from a voter, and only once its answer is due. task-d51 moves task-j04's local checkpoint export off the domain thread onto task-d37's snapshot, which task-d46's long runs showed is the cost left there that grows with the state.
- **After task-d46, a profile of the domain thread** (`perf`, ten clients, stores on tmpfs; [notes](../tuplesky-impl-notes.md#where-a-commands-time-goes-after-task-d46)) put about 1.15 ms of CPU on each command: 34% in the projection's redb transaction (its commit alone 24%), 25% in `Learner::established` walking every live executed predecessor to build a closure that only a duplicate check reads, 11% in the rest of consensus and 6% in the journal. With the stores on a disk, the journal's and the projection's syncs add about 4.8 ms a command, all on the same thread. Two tasks follow. task-d52 moves execution and materialization onto a pipelined applier, so that the domain thread pays the journal and consensus and the projection's commit and sync overlap the next group: after task-d47 and task-d48 a group still costs the domain thread two syncs in series, which bounds a voter near etcd's rate on the Jepsen runner's disk with nothing to spare. task-d53 establishes an execution from its direct dependencies, which the guard already requires executed, instead of walking every executed predecessor still in the table.
- **After task-d50, the Jepsen runs** put the leader's domain loop near saturation on the runner's disk with about 1.2 of its 2.7 ms a command waiting in the journal's sync on that thread, so a group held about 1.6 commands. task-d54 runs the journal's appends on the journal worker Section 17.3.3 already describes, so that a group holds what arrives during a sync. A proposal split into a tentative frame and a later durable vote, which would take one of the three serial syncs off a command's path, is a wire and evidence change with a second frame per command; it is deferred, not refused, to be sized as a Section 4/6 amendment with its own bounded model once task-d54's numbers are in.
- **After task-j06, the Jepsen runs** showed the replay profile taking a third of the CPU per operation and 18 ms off every median at six nodes, and no throughput. Each publication of the local checkpoint stalls the domain thread for a time that grows with the projection, so the closed loop's mean is set by the tail. task-d51 is promoted to next and bounded as well as moved, after one run with the publication interval raised. task-d55 measures a publication's steps, a restart's replay and the domain thread's scheduling, which that run and task-d51's acceptance read. task-d56 keeps one restarted follower from stalling the domain, which the replay faults run found and the strict ones show the start of. task-d57 bounds a read's index by what the followers confirming its round had voted, which is the largest lever left on the median: neither the leader's commit frontier nor the read's keys bound it safely. The proposal split stays deferred behind all four.
- **After task-d55, a profile of five voters** on one four-core host ([notes](../tuplesky-impl-notes.md#a-voters-cpu-per-operation-at-five-voters)) measured the voters at 4.3 ms of CPU per operation, the Jepsen runner's 4.1, and the load generator at 0.2. On the Jepsen runner what is left outside the voters is about 2.0, by subtraction rather than measured (task-d62 measures it), and the voters alone are near twice etcd with its client. Of the voters' 4.3 ms: 43% on the domain threads, 32% on the transport threads, 17% on the materializers, 7% on the appenders. Four tasks follow. task-d58 serves a turn's held reads from one snapshot, a quarter of the leader's domain thread. task-d59 re-sends without walking every proposal the leader keeps. task-d60 allocates less on the domain thread, a quarter to a third of it. task-d61 sends a turn's frames to one peer together, since the transport's cost is per frame. None of them is a gate. Each is measured against the same profile. task-d58 comes first, after task-d51, since it is local to the leader's domain thread and already sized; task-d61 after task-d62's counts.
- **A consolidated review against the reference SwiftPaxos** on #98 found that `coordd` does not drive the speculative execution task-29 built, that the collector cannot complete on an early result as the reference's client does, that every command shares one conflict key, that path resynchronization is stricter than the reference's, and that the transport carries one stream per frame with no batching across commands. Those differences survive tmpfs. Four tasks follow. task-d62 counts what a command costs, why its fast path failed and where a learned command waits before release, on a workload matched across TupleSky, etcd and the reference, and goes first after task-d58. task-d61 takes in the review's transport batching and shared broadcast encoding, and task-d58 its read-round batching. task-d63 drives speculative execution under the existing release gate, its gain bounded first by task-d62's learned-to-released split, with when a result may be released (once its group is in the journal, or once the projection committed it) as its own design step. task-d64 designs collector-side early completion, and task-d65 decides the conflict model; both are design first. The stronger invariants the source mapping records stay unless a proof replaces them. The reference's fast quorum, as the baseline runs it, is any majority (3 of 5, no quorum file); `coordd`'s is `c2_default`, a fixed fast set of 3 of 5 (the leader and the next two, all of which must agree; `3N/4 + 1` is the C1 branch, taken only by a ballot that names it), so both need three of five, but TupleSky needs a named three and the two fast-path shares are not the same predicate's.
- **The Jepsen runs of task-d51** at six nodes held the domain thread over 10 ms in 8 of 15 publications at the default cadence, up to 99 ms, all of it the journal's part: the drain, the pointer's append and the retirement, each the largest step in some. task-d66 runs the pointer and the retirement as a journal worker's job, after a reading of the domain thread's own CPU over those steps separates the journal's work from waiting for a core. The same runs measured the harness directly at 0.65 to 0.95 ms an operation, and the servers at 3.5 to 4.7 ms against etcd's 1.4.
- **A Jepsen run on #98 with no fast decision** in 46,000 commands is a fast-set follower holding a pre-acceptance the leader never ordered: its path log stays off every leader path until the next election. task-d67 decides when a follower lets go of one, with its argument, then builds it; task-d62 counts such pre-acceptances beside the fast-path reasons.
- **After a leader change**, task-d62's counts show the fast path off for as many commands as the old ballot ordered: a follower's path log takes the new leader's synchronizations as its prefix only once their sequence numbers pass the old ballot's. task-d68 decides what the prefix is across ballots, with its argument, then builds it.
- **task-d61's first PR** puts what a turn queues for a peer on one stream, where both ends offer a new peer capability (`0x0020`, `spec/wire-v1.md`), and encodes a leader's proposal once for all voters. On five voters on one host, measured in one binary with it on and off: streams per command fell 3.2 to 4.2 times, the transport threads' CPU per operation by 9% (0.99 against 1.09 ms over the five voters, every pair the same way), and datagrams per command by only 5 to 7% ([notes](../tuplesky-impl-notes.md#several-frames-a-stream-task-d61-streams-fell-packets-hardly-did)). A turn's frames to a peer were already sharing packets, two frames a datagram at the leader, so what is left of the transport's cost is per packet and per wake-up: about five datagrams, five send calls, one and a half ACK frames and eleven futex calls a command at the leader. Fewer of those needs fewer turns a command or fewer ACK-only packets. On the Jepsen runner, in three pairs on one host against one frame a stream, the leader's streams per command fell 60% (10.3 to 4.1), datagrams 2%, no voter lost a frame, and the leader's loop, the voters' CPU and `ok`/s moved inside the pairs' spread. So task-d61's acceptance is the streams cut, not a CPU gain, and task-d63 no longer waits on it.
- **task-d59's PR** keeps the durable proposals by sequence number and each voter's highest adoption, so the leader's re-send timer looks only at what a voter has not adopted. On the Jepsen runner, in three pairs on one host against task-d61's build ([notes](../tuplesky-impl-notes.md#the-leaders-re-send-past-each-voters-adoption-task-d59)): the leader's loop fell 0.078 ms per command (0.067 to 0.084, lower in every pair), the leader's excess over a follower 29% (0.22 to 0.16 ms), the voters' CPU per operation 4% and `ok`/s rose 5%; `resend_unvoted` left the profile (about 56 µs per command, 4 to 5 ms a call before). A call looks at 20 to 22 proposals and takes 0.09 to 0.11 ms, which is mostly the commit-frontier announcement it ends with (`announce_committed`, a `Committed` frame to each voter), not the walk; its acceptance is restated as measured. The leader's read p99 did not move: a 4 to 5 ms stall four times a second is about 2% of the loop's time, under a p99 of about 80 ms.
- **A call-graph profile beside a follower**, with task-d59 in, on Ubuntu nodes whose glibc keeps frame pointers, found the leader's loop 175 µs per command over a follower's, spread over `stops_on`, the reads, `settle`, `stops_on_flush` and `on_peer_plane`, and four phases every voter runs (`stops_on_flush`, `on_transport`, `settle`, `on_peer_plane`) at two thirds of each loop. A third of the leader's thread is glibc's allocator and copies, spread over every phase. task-d60 is rewritten from it: first the allocator, then the sites.
- **task-d60's first PR** sets mimalloc (v3.3.2) as `coordd`'s global allocator. On the Jepsen runner, six pairs on two CPUs against task-d59's build ([notes](../tuplesky-impl-notes.md#the-allocator-task-d60-step-1)): every voter's loop fell about a tenth, the leader's allocator 49 µs per command, the voters' CPU per operation 5% and the resident set rose about 34 MiB per voter. What is left of the allocator is the number of allocations, so step 2's sites keep their order from the head's caller table; `memcmp` is the leader's map probes and `memmove` likely the per-voter copy of a broadcast, the latter confirmed before it is cut. On the faster runner (an EPYC 9V45) the voters' tokio threads, 1.15 ms of CPU per operation, now cost more than their domain loops, 1.04, and fell 3.5% where the loops fell a tenth: the transport is the larger CPU pool, most of it QUIC's per-packet work, the kernel's send and receive path, and parking and waking, which an allocator does not touch, so a transport worker is profiled next.
- **task-d60's second PR** cuts five allocation sites ([notes](../tuplesky-impl-notes.md#the-allocation-sites-task-d60-step-2)): a memory leak is fixed: the collector's golden trace, an unbounded list that grew by one formatted event per transition for the daemon's life and that nothing in `coordd` read (the profile's `trace::hex`), is kept only when asked for; a ballot configuration's voter sets and a sealed journal record's body are shared rather than cloned; a record is encoded without a copy and measured without encoding; and a submission's command is derived from the bytes its canonical check compared. Locally, in three pairs against the first PR: the leader's loop 6% lower and each voter's resident set 17 MiB lower, in every pair; all threads' CPU per operation flat. On the runner, three pairs on an Intel Xeon 6973P-C: every voter's loop 7% lower in all three, the leader's allocator 9.5 µs per command lower (the trace, the journal record's encoder and the configuration's clone gone from its caller table), a follower's `flush_queued` 33 to 18 µs, the tokio threads unchanged, `ok`/s 1.3% higher, and the resident set 43 MiB per voter lower (21%); kept.
- **A profile of a follower's loop**, local, on that PR, read its five largest functions one level down and found `Outbox::release` at 5 to 7% of it: a walk over every send waiting, on every round. task-d69 indexes the outbox by what a send waits for, ahead of task-d60's third step.
- **The Jepsen runner's profile of the leader's transport workers**, on three CPUs, put them at 1.1 to 1.4 times the leader's domain loop per command: the UDP send call 19 to 40% of their CPU at 11.7 calls per command, parking and waking 7 to 18% at 17.4 futex and 7.0 `epoll_wait` calls. A 7% cut to every loop moved `ok`/s 1.3%: on the faster runner CPUs the loops no longer bound throughput, the voters' total CPU does, and the transport is 41% of it. task-d70 takes the transport's datagrams and wakeups per command, its two settings first as pairs; at threshold 8 with QUIC's own delay the voters sent a sixth fewer datagrams and spent 4% less CPU per operation, and that cadence became the default.
- **The Jepsen runs of task-d67's code** (#98): after four of five leader changes the new leader ran 81 to 87% fast; the fifth's fixed fast set held the voter just killed, which task-d76 takes. Under load the share stayed at Section 2.2's 1 to 2%, which task-d65 records as confirming that section's decision. The publications over 10 ms spent under 2.5 ms on a CPU or waiting for one, so task-d66's job is built.
- **Against etcd on one host**, the one matched comparison is 2.5 times behind, at 3.5 to 4.7 ms of server CPU an operation against etcd's 1.39, and none of the gap is the order. task-d72 sets the budget and keeps the record; task-d73 through task-d75 are its levers, after task-d63's build and task-d66's job.
- **Gating.** task-d45 through task-d49 and task-d51 through task-d53 are prerequisites of task-64 and task-q01, so that the qualification and the combined report measure the protocol rather than these costs. task-d56 is a prerequisite of task-64, as liveness under faults. task-62's remaining rows, and any reference result it publishes again, wait on them and on task-d54 for the same reason. This is stated in task-62 rather than as a prerequisite, since task-62's first runs produced task-c01 and task-c02, which the throughput tasks build on. task-d50, reads and the fast path off the slow path, changes protocol behaviour and waits on its own design amendment.
- **Design changes.** task-d45 through task-d47, task-d49, task-d51 through task-d56 and task-d58 through task-d62, task-d66, task-d69, task-d70, task-d72, task-d73, task-d75, task-d88, task-d91, task-d92, task-d94, task-d95, task-d96 and task-d97 do not change the design; task-d98 amends Sections 4.3 and 4.8, and task-d99 Sections 13 and 17.7 with the activation path and the admission window; task-d74 amends Sections 4.1 and 4.3 if a decision reads another follower's votes, and task-d76 amends Section 4.2; task-d71 amends Sections 4.9 and 13.1, and task-d77 through task-d80 and task-d85 Section 4.9, and task-d81 Section 6.7.3 if it changes which ballot a donor serves at; task-d82 amends Section 11.4 and the O14 row, task-d83 Section 6.3 and task-d84 Section 17.3.3; task-d89 amends Section 4.7, and task-d90 Sections 4.5, 17.3.3 and 17.4; task-d86 amends Section 17.3.4 only if a commit cadence reaches `strict`, and task-d87 Section 6.3 only if a round serves a read that arrived after it began; task-d67 amends Sections 4.1, 4.7 and 4.9 with its rule, and task-d68 Sections 4.1 and 4.7 with its, and task-d63's release-rule step amends Sections 4.5, 17.3.3 and 17.4; task-d64 amends Sections 4 and 6.4, and task-d65 Section 2.2 if it changes the model. task-d48 amends Section 17.3.4's projection hardening; task-d50 amends Sections 4.5, 6.3 and 17.4; task-d57 amends Sections 2.2, 4.5 and 6.3.

task-j06 is optional and cannot silently relax durable materialization. ReadFence is its own capability gate. Observers do not improve quorum fault tolerance or acquire voting rights by catching up. Interface drift in Kine is resolved at one explicit pin, not mixed across examples. Strict per-output authorization remains authoritative even for regional observers.

Deferred/non-goals: full Rust etcd wire server, cross-domain transactions, transparent sharding, finer conflict predicates despite exposed revisions, unproved read shortcuts, arbitrary cloud identity formats, new physical database engine, live engine migration/switching, mixed-engine production, unreliable authoritative DATAGRAM and FIPS claims. Optimizations cite measured bottlenecks while retaining correctness checks.

The upstream #1/#2 safeguards are embedded in affected protocol, storage, membership and observer acceptance fields; they are not a separate appendix to apply. The reviewed issues did not supply a completed weaker-invariant proof or demonstrated end-to-end safety counterexample; this plan neither invents one nor treats the prototype as authoritative.

## Review record template

```markdown
## Behavior and invariant
What changes, and what remains true?

## Scope
Implementation files, models, fixtures and explicit exclusions.

## Dependencies and compatibility
Plan IDs, base commits, format/features and rollback limits.

## Evidence
Commands actually run, platforms, seeds, negative cases and results.
Do not label planned or skipped tests as passing.

## Persistence, protocol and security
Durable prerequisites, external effects, identity and failure handling.

## Reviewer focus
Critical correctness questions with source/model references.
```

## Provenance and validation boundary

This plan integrates the original 70 tasks, the 19 journal/observer/membership tasks and source-issue acceptance refinements from repository commit `2acf4eb724a36dcdb74baeb0c3b13368bc1317eb`. One index and complete specifications replace the layered documents; historical inputs remain in Git history. Direct dependencies retain the documented task-66→task-q01 release extension.

The design package contains only Markdown design/plan/navigation, not local consolidation scripts, validators, generated graphs/reports, workflows or workspace files. Proposed future service tooling and CI in tasks remain intentional engineering deliverables. Dependency/source references are in design Section 24. No code build, model run, real crash qualification or benchmark is claimed by document consolidation.
