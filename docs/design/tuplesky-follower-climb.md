# A behind follower's climb

**Status:** design step only, no code (the lead's order on #185, 2026-11-14, revised to the lead's design review that evening, and again to review's second design review and the lead's decisions of 2026-11-15), under [task-d25](tuplesky-prs-plan.md#task-d25) (catch-up outpaces the domain) and [task-d56](tuplesky-prs-plan.md#task-d56) (a voter far behind is left to catch-up). It covers:
- what held testing's n5 back, from its table and the code (Section 2);
- whether the live stream can reach a follower at all on a lossy path (Section 3);
- two ways for a follower to climb, compared, and the one proposed (Sections 4 and 5);
- the invariant, the memory bound, and the worst case for a 100k lag at 140 ms (Sections 6 and 9).

Code facts are read at the carry `f612ca20`, except those this revision added in Sections 4.4 and 8, which are read at `5fa2d0a1`. Every figure says whether it was measured, read from code, or estimated.

## 1. The case

Testing's +loss0.1 cell at 480 ([#98, 6884311018](https://github.com/tuplesky/tuplesky/pull/98#issuecomment-6884311018); run 43638305372, shim 7d649f11). n5 is 70 ms from the leader n1, which is also its only donor. From n5's `metrics` lines:

| n5 up (s) | Leader executed | n5 executed | Behind | `table` (cap 1000) | Oldest wait | Kept − kept taken | `not_taken`/s | Pages | Asked ahead | Taken stale |
|---|---|---|---|---|---|---|---|---|---|---|
| 40 | 41,229 | 41,925 | −696 | 626 / 188 / 0 | uncommitted, Accept | 0 | 0 | 3 | 0 | 3 |
| 50 | 64,428 | 60,311 | 4,117 | 874 / 851 / 0 | uncommitted, PreAccept | 0 | 0 | 11 | 2 | 10 |
| 60 | 90,326 | 73,124 | 17,202 | 875 / 875 / 0 | uncommitted, PreAccept | 6,913 | 497 | 90 | 68 | 49 |
| 70 | 115,000 | 84,348 | 30,652 | 875 / 875 / 0 | uncommitted, PreAccept | 7,134 | 2,290 | 167 | 124 | 82 |
| 100 | 193,328 | 112,893 | 80,435 | 875 / 875 / 0 | uncommitted, PreAccept | 7,134 | 2,410 | 399 | 356 | 161 |
| 130 | 269,215 | 140,973 | 128,242 | 875 / 875 / 0 | uncommitted, PreAccept | 7,134 | 2,314 | 621 | 578 | 189 |

From 60 s:
- **n5 executes 870 to 980 commands a second** against the leader's 2,570, all from catch-up pages: 7.1 to 8.1 pages a second of 116 to 123 commands. Its loop is 19 to 23% busy.
- **n5 logged 53 `QueueFull { lane: Control }` lines toward n1**, all before 70 s.
- **The leader relays every command to n5,** and n5 drops each payload: `not_taken` grows at the leader's whole rate. At testing's estimate of about 255 B a payload, that is 580 to 650 KB/s, or about 42 MB between 60 and 130 s.

**n3 in the same run climbed** ([#98, 6884059202](https://github.com/tuplesky/tuplesky/pull/98#issuecomment-6884059202)):
- It was 8k behind at about 40 s and level by 60 s.
- It kept 85,490 relayed payloads in all and took 85,260 of them.
- Its `not_taken` stopped at 233.

The difference between n3 and n5 is the subject of Sections 2 and 4.

## 2. What held n5 back

### 2.1 (a) The table: 875 records n5 cannot adopt

**What "oldest wait" names** (`CommandTable::waits`, `commands.rs:1139-1172`):
- It is the record below EXECUTED that comes first in the follower's `order`, which is the leader's sequence number of an adopted proposal (`follower.rs:4040-4045`).
- A record that is not adopted has no `order`, and sorts after every adopted one, by `CommandId`.
- coordd drops the command id before it prints the line (`serve.rs:2128-2138`).

So "uncommitted, PreAccept" at every reading from 50 s says two things. **No adopted record was unexecuted at any reading.** And the named record is one n5 took into its table without adopting the leader's proposal for it. The line can't say whether it is the same record each time, and the evidence below says it is not.

**What is in the table.** While the table had admission room, a relayed proposal's payload entered it at PRE-ACCEPT (`take_relayed_payload`, `follower.rs:5985-6006`), and so did a kept payload, through `retake_kept` after each execution (`follower.rs:3922-3925`, `6070-6110`). The record stays at PRE-ACCEPT until the leader's proposal for it is adopted (`commands.rs:774-781`).

**Why none of them commits.** There is no per-command commit message. A follower's record reaches COMMIT by one of these paths:

| Path | What it needs | For a relayed record far ahead of n5 |
|---|---|---|
| The learner, from votes (`learner.rs:147-188`) | the record at ACCEPT (`in_accept`, `learner.rs:161`) | It is at PRE-ACCEPT. |
| The leader's frontier, `Committed { through }` (`commit_through_leader`, `follower.rs:5009-5030`) | the record at ACCEPT, adopted durably, with its sequence number at or below `through` | `through` is far past it, but it is not adopted. |
| Adoption of the held proposal (`held_blocker`, `follower.rs:6583-6735`), then the frontier | every dependency of the leader's proposal at ACCEPT or later here (`guard_accept`, `phase.rs:66-73`) | Under the conservative key the chain is total, so its dependency is the leader's previous command. That command is in the gap between n5's frontier and the relays, which only catch-up fills. |
| A Sync of a new ballot (`follower.rs:2965-2978`) | a new ballot | The ballot didn't change. |
| Catch-up installation (`install_pulled`, `follower.rs:4529-4687`) | the page to reach the command's position | Yes, once catch-up gets there. |

So the leader's commits for these commands did reach n5: its frontier passed them. They can't apply until each record is adopted, and adoption waits for the gap.

**What resolves each record.**
- When catch-up executes a held proposal's predecessor, the proposal is adopted, and the frontier commits it at once. Behind it, every held proposal whose predecessor is now at ACCEPT adopts in turn, and they execute through the learner between windows (`follower.rs:3045-3056`).
- If catch-up reaches a command itself, `install_pulled` adopts it with the donor's decided dependencies, commits it, and drops its held proposal and kept payload (`follower.rs:4630-4657`).
- Nothing else removes it: `reclaim` retires only EXECUTED records (`commands.rs:604-652`), and undecided records are released only at a Sync (`follower.rs:6913-6915`).

So the table isn't stuck behind one entry: its membership turns over. Catch-up passes records and resolves them, and `retake_kept` refills each freed slot with a kept payload. The relays and kept payloads that arrive are always about one gap ahead of n5, so the table is always full of commands n5 can't use yet. The reading "875 / 875 / 0, oldest PreAccept" is that state, not one record stuck.

**What it costs.**
- **No progress is blocked.** A catch-up window needs no table room (`Room::Installed`, `commands.rs:809-819`), and pulled commands retire as they execute (`follower.rs:3972-3979`). The 1000/1000/1 reading at 90 s is 875 records with a window of 125 installed.
- **CPU is spent.** Each refused relay walks the whole table twice in `reclaim` (through `initialize_relayed` and then `table.expect`, `commands.rs:625-696`). `retake_kept` walks it again after each execution. At about 2,500 refused relays a second, that is about 5,000 walks of 875 records a second. Its share of n5's 19 to 23% busy loop isn't measured.

**What should resolve it.** A relayed payload should enter the table only when its turn has come, that is, when its proposal's dependencies are at ACCEPT here (today's reserve room). Before that, it should wait outside the table, in the kept run of Section 4.1. Then the table holds only what can be adopted, and a refused relay costs no walk.

### 2.2 (b) The 53 `QueueFull { lane: Control }` lines

**They are not catch-up asks.** `CatchUpRequest`, `CatchUpPage`, `PayloadRequest` and `PayloadResponse` go on the bulk lane (`is_payload_transfer`, `messages.rs:95-110`; lane choice at `serve.rs:614-626`). The control frames n5 sends n1 are protocol frames:
- a `SlowAck` for each proposal it adopts (`follower.rs:6697-6732`, `5359-5400`);
- acknowledgements again when the leader re-sends a proposal (`follower.rs:6150-6172`, `6287-6351`);
- a `FastAck`, and a `ProposalRequest` after a restart (`follower.rs:5157-5234`, `5811-5831`);
- `ReadConfirmed`, feature, floor-readiness and election frames.

**What the lines count.**
- A send is refused when its queue is at depth. The queue is one per destination, lane and domain group: 256 frames on the control lane, with no byte bound of its own (`endpoint.rs:1844-1863`; `lane.rs:116-165`).
- coordd logs a refusal only at each doubling from 64 within an episode. An episode ends at the next send to that replica that succeeds (`UNDELIVERABLE_SAID_AT`, `serve.rs:1685`, `4336-4381`).
- So 53 lines are at least 53 × 64 = 3,392 refused frames. The "can send to n1 again, after N frames" lines in the same log give each episode's total, and testing can read them.

**What happens to a refused frame.**
- **It is dropped.** `send_many` maps it to an error (`endpoint.rs:1775-1785`), and `send_turn` keeps nothing (`serve.rs:633-642`).
- **The metrics line can't see it.** It is counted only in `unavailable`, on the shutdown line. The per-lane `refused` field reads "not instrumented" (`serve.rs:2350-2365`).
- **The protocol repairs each kind one level up:**
  - for a lost acknowledgement, the leader re-sends unvoted proposals, 16 a voter every 250 ms with back-off, and hands decided ones off after 2 s (`leader.rs:240-311`, `1542-1678`);
  - a `ProposalRequest` is asked again at the next `Committed`;
  - the leader re-sends feature frames.

**Why the lane filled (inferred).**
- Adoption has no cap a step (`follower.rs:6581-6616`), and the outbox releases a whole journal group at once (`node.rs:2306-2326`).
- So when catch-up passed a predecessor and a chain of held proposals adopted behind it, n5 offered hundreds of `SlowAck`s to a 256-frame queue in one call.
- That fits the timing. The lines run from about 45 to 70 s, while held chains were adopting. They stop once n5 adopts nothing outside its windows, and duplicates of held proposals get no answer (`follower.rs:6146-6149`).

**The leader's side.** n1 sends to n5 through the same `send_turn`. A full control queue toward n5 drops n1's frames the same way, and counts and logs them the same way ("could not send to n5"). A dropped proposal reaches n5 again only through the leader's re-send. Testing's reading names no such lines from n1, which fits n5 receiving every relay (`taken` + `known` + `not_taken` is the leader's rate). Testing can confirm it from n1's log.

**Does the peer plane need #298's treatment?** No frame here is lost for good. But each one is a cost we make, not the network:
- a dropped `SlowAck` (about 100 B) is replaced by the leader re-sending the proposal and its payload (about 380 B, Section 3);
- that re-send travels toward the follower, on the path that is already full;
- the vote is late by 250 ms to 2 s.

The proposal, in step 3:
- keep refused control frames and offer them again, as #298 does for the api plane's returns, within the same bounds (25 to 200 ms, at most 2 s, bounded bytes a connection);
- count refusals by lane on the metrics line.

The keep's 2 s limit is no longer than the leader's hand-off, so a frame kept to its limit still races the re-sends no worse than today. Acknowledgements are idempotent at the leader. Sending acknowledgements a frame at a time, not a command at a time, would remove the burst itself. That is a protocol change and is left out (Section 10). Section 4.4 removes most of it at its source: no acknowledgement for a proposal the leader has committed.

### 2.3 (c) One page per two round trips

**The schedule today** (`continue_catch_up`, `follower.rs:4137-4178`):
- One ask is out at a time ("one slot, taken once", `follower.rs:4146-4168`), and at most two pages are in hand: the one executing and the one asked ahead.
- The ask for the next page goes out in the step that takes a page with nothing queued (`follower.rs:4412-4413`), or when the window before it has executed (`follower.rs:3926-3929`).
- So one page comes per ask-to-page cycle C, never two a round trip. A second page in a round trip needs a second ask in flight, which is Section 7.

**What C is made of.**
- 7.5 pages a second is C ≈ 133 ms on a 70 ms round trip.
- The donor's serve time is 0.77 ms on average (n1's `served`).
- The other ~62 ms is the page's transfer on the bulk connection plus both loops' latency (inferred). The bulk lane is its own QUIC connection with its own CUBIC controller (`lane.rs:1-6`, `157-165`, `214`). A page that needs more than one window takes more than one round trip.
- Execution is not the limit: readings show no window installed between pages.

**What the counters count.**
- `pages` counts every page taken, stale ones included (`follower.rs:4367`).
- `asked_ahead` counts asks published from the end of the page in hand (`follower.rs:4174`).
- `taken_stale` counts pages whose start was below the executed frontier when taken (`follower.rs:4271`, `4380`).

**Why pages were stale.**
- The pacer was silent after 60 s (`pacer_asks` stayed at 10). So per the code, a page is stale only if n5 executed past the ask's start outside a catch-up window before the page came.
- That is the held chains of Section 2.1: adopted once catch-up passed a predecessor, committed by the frontier, and executed between windows.
- The counts agree. From 60 to 130 s, n5 executed 67,849 commands over 531 pages, which is 127.8 a page, more than the 125 a page can hold. So at least about 1,500 commands ran outside windows.
- `pulled` against executed would give the exact split.

**What a stale page costs.**
- **The donor pays in full:** it reads, encodes and sends a whole page (`node.rs:2008-2060`).
- **n5 uses only the entries past its frontier** (`follower.rs:4333-4341`).
- **The chain continues only if the page answered the latest ask.** A stale answer to an older ask asks nothing (`stale_unasked`, `follower.rs:4384-4387`), and it can make the next real page be dropped as `ignored_busy` (`follower.rs:4278-4300`).
- **The share is large:** 140 of the 531 pages from 60 to 130 s were stale, 26%. That share of the bulk connection, the path that bounded intake, carried commands n5 already had.

### 2.4 (d) Kept pinned at 7,134

**It is not the byte bound.**
- The bound is `MAX_KEPT_RELAYED_BYTES` = 4 MiB (`follower.rs:434`).
- Each kept payload is charged `size_of::<PayloadRecordV1>()` plus its request bytes (`kept_relayed_size`, `follower.rs:436-441`).
- From the record's field types, that is about 250 B for the record (not compiled) plus about 55 to 90 B of request, so about 315 to 350 B.
- 7,134 entries are then about 2.2 to 2.5 MB. The bound would bind at 12 to 13 thousand, not at 7,134. Filling 4 MiB at 7,134 would need 588 B each.

**It is the held-proposal cap.**
- A payload is kept only beside a held proposal (`keep_relayed`, `follower.rs:6026-6033`).
- Held proposals are capped at capacity × `HELD_PROPOSAL_SLACK` = 1,000 × 8 = 8,000 (`follower.rs:6242`).
- The 875 relays in the table hold proposals too, whose payloads are in the table rather than kept. So 7,134 + 875 = 8,009: the cap, within a few records.
- `kept − kept_taken` isn't an exact count anyway: `unkeep` drops entries without counting them (`follower.rs:4657`, `6043-6048`).

**Past the cap, everything is dropped.**
- A new proposal isn't held (`follower.rs:6242-6244`), so its payload isn't kept and counts `not_taken`. That is the leader's whole rate, about 600 KB/s for 80 s.
- Nothing tells the leader. Its `proposal_sends` goes to every other voter, with the payload attached (`leader.rs:2746-2767`, `2490-2503`).

**Why n3 climbed and n5 didn't.**
- A held chain adopts all at once when catch-up reaches its start, but only up to its first missing command.
- n3 was 8k behind and filled its hole before its held proposals reached the cap: `not_taken` stopped at 233. Its kept run stayed whole, and it drained.
- n5's hole was 4k at 50 s, but its pages came at about 950 a second. Filling the hole took about 4 s, during which about 11k proposals arrived, more than the 8,000 cap.
- Past the cap, n5 dropped proposals, and every dropped one is a new hole in its kept run. From then on, whatever it kept sat behind a hole that only pages could fill. The kept set became a scatter of commands one gap ahead, which Section 2.1 shows in the table.

## 3. The entry: can the live stream reach a follower?

A follower stays level only if the leader's stream to it fits the path. If it doesn't, no climb holds it level.

**Bytes a command to one follower** (estimated):
- the payload record, about 255 B (testing's estimate from n1's `payload_bytes`, 898.3 B a command over about 3.5 copies);
- the proposal around it, about 100 B (sequence number, ballot, command id, dependencies by id);
- the commit frontier and QUIC framing, a few percent.

So about 380 B a command, about 1.0 MB/s at R = 2,600. n1's `proposals.proposal_bytes`, `frontier_bytes` and `traffic.sent_bytes`, over commands and followers, give the exact figure, and testing can read them from the same run.

**The path.** A loss-based controller at 0.1% loss settles near 1.22/√p ≈ 39 packets a round trip. At 1,200 to 1,450 B a packet:

| | 70 ms | 140 ms |
|---|---|---|
| Live stream to one follower at R = 2,600 (about 380 B a command) | about 1.0 MB/s | about 1.0 MB/s |
| Loss-bound window at 0.1% (estimate) | 0.67 to 0.81 MB/s | 0.33 to 0.40 MB/s |
| Observed (testing, `traffic.received_bytes`, #98) | n5 received 0.98 to 0.99 MB/s while level, and 1.34 to 1.71 MB/s once behind | none in this run |

**What this says.**
- **At 70 ms the estimate under-reads.** While level, n5 received 0.98 to 0.99 MB/s, nearly all of it the relayed stream on n1's control connection, against the estimated 0.67 to 0.81 for one connection.
  - Once behind, n5 received 1.34 to 1.71 MB/s. That is all streams in, mostly from n1 (leader and donor), and not split by connection.
  - Over the control and bulk connections together, that is about twice the one-connection estimate. So it doesn't settle a single connection's window.
  - #297's `paths[]` (the window and losses on each n1 → n5 connection) reads it directly, and n5's run predates #297.
- **At 140 ms, by the same estimate,** the window halves and the stream doesn't fit, by a factor of 2.5 to 3. A follower 140 ms from the leader, on a 0.1%-loss path with CUBIC, would then not stay level at 2,600 a second, whatever catch-up does: once the leader's queue toward it fills, frames are dropped (Section 2.2), and each is a hole in what the follower keeps.
- **That 140 ms conclusion is an estimate, and the estimate under-reads at 70 ms.** It waits on `paths[]` or a 140 ms cell before it feeds the `bbr` decision.
- The fixes, if it holds, are the controller or fewer bytes a command:
  - **The controller:** `bbr` on the control lane. It is #299's switch, and the default is the lead's decision.
  - **Fewer bytes:** the payload record's request bytes are, by the same estimate, only about 55 to 90 B of its 255. The rest is the retry key and the admission facts, which travel with every copy, to every follower. Sending the admission once per session, by reference, would bring a copy near the request's size. That is a format change, sized here and not proposed: a counting build gives the split first.

**The design's claim, then.** Deep keep (Section 4.1) is made of the live stream, so it levels a follower only where the stream reaches it:
- at 70 ms, n5's path, where the stream did reach it;
- on paths not bound by loss, or under `bbr`.

On a loss-bound 140 ms path with CUBIC, if the estimate holds, neither deep keep nor pages only levels the follower (Section 4.3).

**etcd, for comparison.**
- Testing's socket statistics aren't in the comments I have, so this is from the protocol, not measured: etcd sends each entry to each follower once, in an append message. That is the request's protobuf plus about 20 to 30 B of entry and message framing, so probably 100 to 150 B a command.
- I'll ask testing for `ss -ti` bytes acknowledged per peer connection, for both systems in the same cell.

## 4. Two ways to climb

Notation:
- **R:** the domain's commit rate.
- **H:** the hole, the gap when the follower starts keeping.
- **I:** intake from pages while the hole is filled. It is at most E_p and at most the path.
- **E_p:** the follower's rate executing pulled commands, installed a page to a batch. task-d25's local cost gives 3,500 a second, and the runner estimate 4,500.
- **E_k:** its rate draining a kept run by adoption, one proposal at a time, as today's path would. This is a different cost from E_p:
  - a kept proposal adopted outside a frame takes a batch of its own;
  - today it sends a `SlowAck` to the leader and to the frontend (Section 4.4);
  - it is committed by the frontier, and then executes (`advance_pending`).
- **E_d:** the drain's rate. It is E_p for the run's decided part, which this design installs a window at a time (Section 4.1), and E_k for what is still adopted.
- **c:** the memory charge for a kept command.

**E_k is measured as a range** (testing, from n3's 10 s metrics lines in the +loss0.1 run, #98).
- In the 10 s in which n3 drained its kept payloads, it executed 2,778 a second while the leader ran at 2,320. It took 26,496 kept payloads and 4,320 commands from 102 pages.
- That interval also holds the time before the hole was filled, so 2,800 a second is a lower bound.
- The loop was 66% busy, at 0.144 ms of domain CPU a command against 0.082 ms when level. So it runs out at about 4,200 a second.

The tables use E_k = 2,800, the measured figure, and give 4,200, its bound, beside it. The code step counts E_p and E_k on the metrics line.

The figures use R = 2,600 (the 480 step's leader) and c = 500 B. The figure c is today's charge of about 330 B plus the held proposal, which today's bound doesn't charge; the code step measures it.

### 4.1 Deep keep (the lead's alternative)

**The mechanism.**
1. A behind follower keeps every relayed proposal and payload from the first one it can't take, as one run in the leader's sequence order, outside the table.
   - The bound is in bytes, set by configuration (64 MiB here), and it charges the held proposal too.
   - It starts a run only when its gap fits one (Section 8).
2. Catch-up fills only the hole: from the follower's executed position up to the run's first command. The hole doesn't move, so page intake needs to be above zero, not above R.
3. Once the hole is filled, the run drains in sequence order, with no network:
   - **Its decided part, nearly all of it, as windows.** A kept proposal of the current ballot at or below the highest commit frontier F has received in that ballot is decided, with the dependencies it carries (the argument of Section 8). F installs those as committed records a window at a time, as a page's decided entries are (`install_pulled`): one batch a window, no adoption and no `SlowAck`. They execute at E_p.
   - **The part above the frontier** is adopted one at a time, as a live relay is: its predecessor is at ACCEPT, and the next frontier commits it. That is about R times a round trip and the frontier's interval, a few hundred commands.
   - **Why windows:** adopted one at a time, the run would drain at the measured E_k = 2,800, below (1 + m)·R = 3,900 and only 200 a second over R. The windows make the drain cost what a page costs.
   - the table's 1,000 bound holds: a window takes the catch-up reserve, as a page does, an adopted command enters the table only in its turn, and execution frees the room.
4. The follower is level when the run is drained. It never left the live stream, so there is no handoff.

**What breaks the run, and what then.**
- **A command missing from the run** stops adoption at it, under the conservative key, as at n5. Pages then go on past that point, because the follower has nothing executable, as today's pacer rule says (task-d25). A gap costs pages, not correctness.
- **The kept bound reached,** because intake fell after the run started: later relays are not kept, because each would sit behind a new hole. The run's end is fixed, and F asks for the exemption (Section 8), as it would have at the start.

**Its numbers** (a stop of t seconds, so H = R·t), with E_d the drain's rate:
- Filling the hole takes H/I. Meanwhile the run grows at R, to R·H/I, which is c·R²·t/I of memory.
- The run drains at E_d − R, so the follower is level after (H/I)·E_d/(E_d − R).
- It needs E_d > R, the run to fit the bound (R·H/I ≤ bound / c), and the entry condition of Section 3.
- 64 MiB at 500 B is about 134k commands, 52 s of the live stream at R = 2,600.

| A 100k gap, R = 2,600 | I | Hole filled | Run at its peak | Level, drained as windows (E_d = E_p) | Adopted one at a time, E_k = 2,800 | E_k = 4,200 |
|---|---|---|---|---|---|---|
| Pages at the margin, a path without loss, E_p = 4,500 | 3,900 | 25.6 s | 67k, 33 MB | 61 s | 359 s | 67 s |
| The same, E_p = 3,500 | 3,500 | 28.6 s | 74k, 37 MB | 111 s | 400 s | 75 s |
| A loss-bound path at 140 ms with CUBIC, one or four donors | | | | never, if Section 3's estimate holds | never | never |

E_p = 3,500 is task-d25's local cost, and E_k's 4,200 a bound read on the runner, so the second row's last cell compares across machines.

**n5's prediction** (review's second design review), from its own figures:
- At 50 s n5 was 4,117 behind, its pages came at about 950 a second, and the leader ran at 2,570.
- So the hole fills in 4.3 s, and the run peaks near 11k, about 5.6 MB.
- Drained as windows, n5 is level after 10 s at E_p = 4,500, or 17 s at 3,500. That is the rerun's prediction (Section 10).
- Adopted one at a time, it would take 61 s at the measured E_k = 2,800, and 11 s at its bound of 4,200.

Pages only never closes n5's gap, since 950 is below R.

### 4.2 Pages only (this design's first version)

The relays are dropped, and pages must outrun the domain: intake I > R, with I = min(K·P/C, E_p, path).
- The follower is level after H/(I − R).
- It needs (1 + m)·R ≤ min(K_avail·P/C, E_p, path), where K_avail = min(`K_MAX`, 5·D) is the pipeline the D donors able to serve the range allow, at most 5 asks to each (Section 7).
- The first term is Codex's condition on #301. With pages cut short at the byte bound, say 40 commands a page at C = 160 ms, `K_MAX` = 8 caps intake at 8 × 40 / 0.16 = 2,000 a second. That is below R = 2,600, so the follower loses ground however much it could execute.
- One donor alone caps it lower: K_avail = 5, so 125-command pages at C = 250 ms give 5 × 125 / 0.25 = 2,500 a second, below R = 2,600, where `K_MAX` would have given 4,000.
- A 100k gap on a path without loss closes in 77 s at E_p = 4,500 (the pipeline at 3,900), and 111 s at E_p = 3,500.

### 4.3 Side by side

| | Deep keep | Pages only |
|---|---|---|
| What intake from pages must beat | 0 | R, with margin |
| Bounded by | E_p for the run's decided part, once the hole is filled | min(pipeline, E_p, path) |
| 100k at 140 ms, loss-bound, CUBIC | never, if Section 3's estimate holds | never |
| 100k without loss, E_p = 4,500 | 61 s (359 s adopted one at a time, at E_k = 2,800) | 77 s |
| 100k without loss, E_p = 3,500 | 111 s (400 s at E_k = 2,800) | 111 s |
| n5 (4,117 behind, pages at 950 a second) | 10 to 17 s (61 s at E_k = 2,800) | never |
| Memory | the kept bound, and a run only when the gap fits it | 9 pages: about 1 MiB typically, 27 MiB at worst |
| Network | the live stream it must carry anyway, plus the hole's pages | every command twice: the relay it drops, and the page |
| Where it fails | the run's bound, E_p ≤ R, or the stream not fitting the path (Section 3) | any path or pipeline below (1 + m)·R |

**Which is faster.** At the same page intake I, deep keep is no slower than pages only exactly when its drain rate E_d ≥ I:
- (H/I)·E_d/(E_d − R) ≤ H/(I − R) reduces to E_d ≥ I.
- **Drained as windows, E_d = E_p ≥ I always,** since pages are installed at E_p too. So deep keep is never slower than pages only, and equal where pages run at E_p.
- **Adopted one at a time, at the measured E_k = 2,800, it is far slower** wherever I is above 2,800: 359 s against 77 s for 100k at the margin. That is why the decided part drains as windows.
- Where I ≤ R, as at n5, only deep keep closes at all.

So deep keep's case rests on the stream reaching the follower (Section 3) and on the drain by windows. The code step measures E_p and E_k in the same run.

### 4.4 No acknowledgement the leader doesn't need

**Today the drain acknowledges every command it adopts.**
- Each adoption sends a `SlowAck` to the leader and to the frontend (`advance_pending`, `follower.rs:6627`), at E_k a second, for commands the leader decided long ago.
- Adopted one at a time, a run would make Section 2.2's burst continuous: 2,800 to 4,200 frames a second into a 256-frame queue.
- Drained as windows (Section 4.1), the run's decided part sends none, since it isn't adopted. The rule below still covers held chains adopted all at once at or below the frontier, which is Section 2.2's burst itself.
- Each one is work at the leader (`collect`, `leader.rs:4147`).

**The rule.** F sends the leader no `SlowAck` for a proposal of the current ballot whose sequence number is at or below the highest commit frontier (`Committed { through }`) F has received in that ballot. Above it, F sends as today. There is no wire change.

**The leader needs none.** Read at the carry `5fa2d0a1`:
- **Learning.** The proposal is committed at the leader, which is what the frontier says. A vote for it changes nothing `learn` reads.
- **Release.** `release_ready`'s gate reads the table's phase and the learner's fast decisions, not a voter's adoption (`leader.rs:2394`).
- **Memory.** The leader drops a proposal, its votes and its payload when its table forgets the command, after execution (`forget_history`, `forget_one`, `leader.rs:1210`, `1270`). No map waits on a voter's acknowledgement.
- **Re-sends.**
  - `resend_unvoted` skips a committed proposal once the voter adopts a later one. It re-sends a decided one only until it is `RESEND_HANDOFF_CALLS` (8) calls old, then trickles one a voter a call. Its own comment says the leader needs nothing from the voter for it (`leader.rs:1535-1541`).
  - Without the drain's acknowledgements, F's `adopted_through` stays at its last acknowledged proposal. So decided proposals past it are re-sent for at most 8 calls, at most `RESEND_PER_VOTER` (16) a call.
  - That already happens today while F's run waits, since F acknowledges a kept proposal only when it adopts it in turn. F drops each re-send as a duplicate of a proposal it holds (`follower.rs:6191`).
- **Pacing.** `note_adoption` samples F's re-send interval, and `top_up` sends F's next held-back re-proposal on its answer (`leader.rs:4201-4204`). Without these acknowledgements, F's interval stays where it was, capped at `RESEND_INTERVAL_CAP` (4 calls), and held-back re-proposals go on the timer.
- **A forgotten command.** An acknowledgement for a command the leader's table has forgotten is held in `early_votes`, and evicts that map's oldest entry (`hold_vote`, `leader.rs:4097`). The rule removes that churn.

**The frontend.** F's acknowledgement also goes to its frontend (`FollowerConfig::frontend`), which counts it toward a pending entry or settles it (`collector.rs:1462-1481`).
- This step leaves that copy as it is.
- Whether a frontend can wait on a behind voter's acknowledgement for a committed command is the code step's to check. If none can, that copy goes too.

## 5. The proposal

Deep keep first, K pages in flight for the hole, no acknowledgement the leader doesn't need, and the exemption only for a gap no run fits:
1. **Deep keep** (Section 4.1).
   - Its decided part drains as windows, at E_p, not by adoption at E_k.
   - A run starts only when the gap fits it (Section 8).
   - The kept bound is in bytes, set by configuration, 64 MiB by default, per behind follower per domain (Section 9).
   - The held-proposal bound follows it, rather than being a multiple of the table.
   - Relays don't enter the table before their turn (Section 2.1).
2. **K pages in flight** (Section 7) fill the hole sooner. That shortens the time the run grows, and so the memory deep keep needs: the run's peak is R·H/I.
3. **No acknowledgement for a proposal the leader has committed** (Section 4.4).
4. **The exemption** (Section 8), while F's gap doesn't fit a run: a long stop. F rejoins by keeping a run once its gap fits. There is no handoff.
5. **A ballot change keeps the run's decided part** (Section 8).
6. **A keep for refused control frames** on the peer plane, and refusals counted by lane (Section 2.2).
7. **Asks from the frontier,** bounded above, and stopping at the run's first command (Section 7). That removes the stale pages of Section 2.3, which the run's own executions would otherwise make on purpose.

One versioned extension of `CatchUpRequest` carries the exemption flag, the upper bound and the stop. It is sent only once task-d99's feature for it is active in the domain, as early proposals were, so an old donor never decodes one.

## 6. The invariant

While a follower F is behind:
1. **What it keeps is one run** in the leader's sequence order, of at most `KEPT_BOUND` bytes, proposal and payload charged.
   - It starts only when the gap fits it.
   - Nothing in the run enters the table before its turn.
2. **It holds at most `K_MAX` pages** asked for or outside the table, beside the one window the reserve holds. A retry takes the slot of the ask it retires, and an answer no live ask covers is dropped (Section 7).
3. **Pages fill only the hole,** from its executed position to the run's first command. They don't ask below the frontier, or past an ask's bound.
4. **It is level within (H/I)·E_p/(E_p − R),** draining its decided part as windows, under the conditions below.

**Conditions**, each with the margin m = 1/2:
- **Execution:** E_p ≥ (1 + m)·R. Section 13.1's admission rule states it for the execution rate. Adopted one at a time, the measured E_k = 2,800 would fail it, which is why the decided part drains as windows. The admission ceiling stays the owner's (task-d26).
- **The entry:** the live stream fits the path, so bytes a command × R × (1 + m) ≤ the path's rate (Section 3).
- **The run:** R·G/I ≤ `KEPT_BOUND` / (c·(1 + m)) when F starts keeping, for its gap G (Section 8).
- **While exempt,** where pages alone carry it:
  - K_avail·P/C ≥ (1 + m)·R, with K_avail = min(`K_MAX`, 5·D) for the D donors able to serve the range (Section 7), as well as E_p and the path.
  - A follower that is pipeline-limited (Codex's 40-command pages, or one donor) still gains where K_avail·P/C > R, at that rate less R.
  - Below R it falls behind, and the metrics (Section 10) say which bound it met.

**Why m = 1/2.** Under pages alone, the gap closes at least at R/2, so a voter stopped for t seconds is back within 2t. Under deep keep, the margin on E_p drains the run at least at R/2.

## 7. Pages in flight

**How many.** While the hole is open, K = clamp(⌈I_target·C / P⌉, 2, K_avail).
- I_target is (1 + m)·R while exempt, and E_p otherwise.
- K_avail = min(`K_MAX`, 5·D) for the D donors at or past the range.
- Today K is 2: one page in hand and one asked ahead.

The inputs:
- **R:** the advance of the newest page's `through` over the last second or more. Pages from any donor count, and the highest `through` seen wins, as `owed_through` does today.
- **C:** each donor's ask-to-page time, smoothed by an eighth a sample.
- **P:** the mean commands a page over the last eight pages.

**Which ranges.**
- Execution positions are the domain's, the same at every voter, so the follower asks consecutive ranges after its tail: `after` = T, T + P, and so on.
- **Each ask carries an upper bound,** the next range's start, so a donor serves only that range. A page cut short at the byte bound leaves a hole, which is asked for at once, and exactly: the ask is the hole, not a page from its start (review's first item 3).
- **A page that overlaps what F holds** (in the queue, the window or the run) is taken for the rest. The commands F already holds are dropped and counted as `overlap` on the metrics line.
- The upper bound and the stop (below) ride the versioned extension (Section 5). Until it is active, a hole's ask brings a page from its start, as today, and the overlap is dropped and counted.
- **`K_MAX` counts live asks and pages held.** Each range has at most one live ask, and a retry replaces the ask it retires rather than adding one (below), so at most 9 pages are held, under jitter too.
- No ask starts below the executed frontier.
- **No ask starts past the newest `through` F has seen.** A donor answers nothing for an `after` at or past its executed position (`serve_catch_up`, counted as `nothing_after`), and each such ask would be asked again after 2C + 100 ms (review's first item 4).
- Pages are taken into a queue by position, outside the table. The window installs one page at a time, in order, and the reserve and the report bound are unchanged.

**The hole's end** (review's second item 6).
- F knows the run's first command, not its position, and asks are by position.
- So each ask names that command, and a donor stops its page before it.
- An ask already out for a range wholly past it is answered with commands F holds in the run. That is at most K − 1 pages, dropped and counted as `overlap`.
- The page that reaches the command closes the hole.

**Which donors.**
- Asks go round the executors at the leader's frontier, so pages come over up to (voters − 1) bulk connections, each with its own window.
- The leader takes one ask in K, and while F wants the exemption, at least one every half second (Section 8).
- At most 5 asks are outstanding to any one donor (Section 9), so with one donor K is at most 5, and with two or more `K_MAX` binds. Section 6's condition and Section 4.2's intake use K_avail, not `K_MAX`, for that reason (Codex on #301).
- A donor behind the range answers less or nothing, and the range is asked again of the leader.

**A lost ask or page** (Codex on #301). A range asked more than 2C + 100 ms ago and not answered is retired and asked again, of another donor where there is one.
- **The retry takes the retired ask's slot.** It never needs a free one, so a lost ask at the head is asked again even with every other slot holding a later page. Without this, the pipeline could stay full with nothing installable.
- **An answer is taken only while a live ask from that donor covers its range,** and taking it closes the ask. A late answer to a retired ask, or a second answer for a range already taken, is dropped and counted as `late`. With one donor, the retry goes to the same donor, and whichever answer comes first is taken.
- **The pacer can't recover a lost ask:** `request_catch_up` declines while catch-up is busy (`declined_busy`, `follower.rs:4125`). This rule does. The pacer stays for a chain that has stopped with nothing outstanding.

**A ballot change** (review's first item 7). A queued page carries its ballot, and one of a ballot F has left stops the install and clears the queue (`Pulled::Stopped`).
- The design keeps that. Up to `K_MAX` pages, about 1,000 commands, are asked again at the new ballot: a fraction of a second at the intakes above.
- They're counted as `stopped_ballot`, as today.

**Campaigns.** A voter with a window in hand refuses to campaign (`CatchingUp`). A climbing voter has one nearly all the time, so in effect it doesn't campaign for the climb's length. That is intended for a voter that far behind: its report would be a table behind the domain.

The code PR argues #223's and task-d25's invariants again, with K in flight:
- no command installed twice or skipped;
- stale, out-of-order and passed pages handled;
- the reserve holds.

## 8. The exemption, and rejoining

**F's rule.** F keeps a run only while its gap fits one: R·G/I ≤ `KEPT_BOUND` / (c·(1 + m)).
- G is its gap, I its page intake over the last second, and R the domain's rate (Section 7).
- The rule applies from the start. A voter back after a long stop whose gap doesn't fit keeps nothing, rather than filling 64 MiB it can't use and holding it for the whole climb. It asks for the exemption in its first ask.

**What the exemption is for** (review's second item 4).
- A run overflows only when I < R·H·c/`KEPT_BOUND`.
- For a gap up to `KEPT_BOUND`/c, about 134k at 64 MiB and 500 B, that means I < R, where pages alone don't close either.
- So the exemption closes a gap only when the gap is over about 134k, on a path that carries more than R: a long stop. It's sized for that case.

**When.** The leader withholds proposals, re-sends and relayed payloads from F while all of these hold:
1. **F's latest ask to the leader carries the field.** A follower that doesn't send it is never exempted, which is today's behaviour.
2. **That ask is within the last second.** While it wants the exemption, F asks the leader at least every half second, so a slow cycle doesn't let it lapse between asks (review's first item 6).
3. **The leader forms a majority without F** (review's first item 5).
   - The leader judges it from adoptions: the voters other than F that adopted one of its proposals of this ballot in the last second are, with the leader, a majority.
   - The exemption ends in the turn this stops holding. At three voters with one down, F is never exempted.
   - If voters stop until the leader needs F, nothing commits until F is relayed to again and votes, and F's table is then as full as it was. That is today's behaviour for a voter that is behind and needed.

The leader still sends F the commit frontier, Syncs, read confirmations and feature messages.

**Rejoining** (review's second item 4).
- F stops sending the field once its gap fits a run, by the same rule.
- The leader relays to F again from its next proposal. F keeps the stream as a run, and pages fill the hole below it. That is the state a restarted voter is in, which deep keep already handles.
- Proposals made while F was exempt are in the hole, and pages bring them.
- There is no handoff: no send of the in-flight set, no fit in the table's admission room, no threshold on an ask's `after` at the leader, and no gap estimate at the leader. That closes review's first items 1 and 2.

**F's gap estimate.**
- G is the newest page's `through`, plus R times that page's age, less F's executed position.
- During an election the domain commits nothing, so the age term is capped at 2C, and while no page has come in 2C, G is held rather than extrapolated.
- The error is then at most R·2C, about 830 commands at R = 2,600 and C = 160 ms, against the rule's 134k / (1 + m), about 89k.
- Only F uses it.

**A change of leader.** The exemption is the old leader's state and goes with it. The new leader relays to F until F's ask says otherwise.

**A ballot change and the run** (review's second item 5). `activate` clears `held` and `kept` today (`follower.rs:2474-2475`). So with a 64 MiB run, a leader change during a climb drops up to 134k kept commands, and the hole becomes the old hole and the old run. The design keeps the run's decided part:
- **What is decided.** A kept proposal of the old ballot whose sequence number is at or below the highest commit frontier F received from that ballot's leader is decided, with the dependencies it carries. That is how `commit_through_leader` reads the frontier for a proposal F adopted (`follower.rs:5053`). A ballot change takes away F's right to adopt it (task-d12), not the fact.
- **That part stays.** At the change it goes into the table in turn as a committed record, as a page's decided entry does (`install_pulled`), not by adoption. It is the drain's own path (Section 4.1), so the change only stops the adoption of what is above the frontier.
  - Its payload is checked against the proposal's admission digest, as a kept payload is today.
  - A Sync of the new ballot can't demote a commit (task-d80).
  - A selection naming one of these commands under other facts halts F, as it does for any commit (task-d14).
- **The part above the frontier is dropped.** Undecided, it is the new leader's to decide. It is at most what the old leader proposed after the last frontier F received: about R times a round trip and the frontier's interval, a few hundred commands at 70 to 140 ms, which pages bring.
- **So a climb through one leader change costs the election,** plus the dropped part's pages.
- **The code PR argues this** against task-d12, task-d14 and task-d80, once for both uses: the drain by windows and the change. If the argument fails, the drain falls back to adoption one at a time, at E_k, and a ballot change to clearing the run, with the loss bounded by the run: the run's size over I, in seconds of pages.
- **Test:** a leader change during the drain (Section 10).

## 9. Bounds

**Memory per behind follower, per domain:**
- **The kept run:** at most `KEPT_BOUND`, 64 MiB by default, charging the proposal and the payload. Today it is 4 MiB of payloads, plus up to 8,000 held proposals that aren't charged.
  - The bound is per behind follower per domain (review's second item 7). A node that hosts several domains and comes back behind in all of them holds a run in each.
  - A node-wide budget, shared across its domains, comes with multi-domain hosting.
- **Pages:** at most `K_MAX` = 8 asked for or outside the table, and one window in it: 9 pages and 1,125 commands. Today it is 2 pages.
  - `MAX_CATCH_UP_BYTES` (1 MiB) bounds a page beyond its first command, which is exempt so that a command of any size is still served (`messages.rs:68-76`). A request may be up to 2 MiB (`MAX_REQUEST_BYTES`).
  - So a page is at most about 3 MiB with its records, and 9 pages are at most about 27 MiB.
  - In the common case, 125 commands of a few hundred bytes, 9 pages are about 1 MiB.
- **The table:** capacity, reserve and report bound unchanged. The run enters it only in turn.
- **At the donors:** at most 5 asks outstanding to one donor (Section 7), so at most about 15 MiB a donor at the worst page size, inside the transport's 16 MiB a destination. `CatchUpRequest` carries no byte limit, so the follower bounds by count.
- **At the leader:** a few words a follower for the exemption, and the time of its last ask.

**A 100k lag at 140 ms, R = 2,600, E_p = 4,500:**

| The path | Deep keep, 64 MiB | Pages only |
|---|---|---|
| Without loss, or `bbr` (pages at the margin) | 61 s drained as windows (359 s adopted one at a time, at E_k = 2,800) | 77 s |
| CUBIC at 0.1% loss, one or four donors | never, if Section 3's estimate holds | never |

**Pages at the margin assume two or more donors.** With one, K_avail = 5, and 125-command pages reach (1 + m)·R = 3,900 a second only while C ≤ 5 × 125 / 3,900 = 160 ms. At 140 ms round trip that leaves 20 ms for the donor to serve a page; past it, one donor holds intake below the margin (Section 4.2).

**On a loss-bound 140 ms path with CUBIC, if Section 3's estimate holds, nothing in this design closes the gap.**
- The live stream itself doesn't fit, so the follower falls behind before any climb starts, and deep keep, made of that stream, can't help.
- The fix is the controller or fewer bytes a command, not the climb.
- The estimate under-reads n5's path at 70 ms, so `paths[]` or a 140 ms cell settles it first.

## 10. Steps, tests and what's left out

**Step 1, the follower** (no wire change):
- the kept run, sized by bytes from configuration, charging the proposal, and started only when the gap fits it;
- relays into the table only in turn;
- no `SlowAck` to the leader at or below the commit frontier (Section 4.4);
- the run's decided part installed as committed records a window at a time, in the drain and across a ballot change (Sections 4.1 and 8);
- K pages in flight for the hole, from the frontier, and not past the newest `through` seen (the upper bound and the stop come with step 2);
- refused control frames kept and offered again, as on the api plane.

New counts on the metrics line:
- the run's commands and bytes, and whether it is at its bound;
- held proposals now;
- the hole's size, K, C and R;
- E_p and E_k, each over the last 10 s;
- pages stale on arrival, `overlap` commands, `late` answers dropped, and commands executed outside windows;
- acknowledgements withheld;
- refusals by lane.

**Step 2, the leader and the ask:** one versioned extension of `CatchUpRequest`, behind task-d99's feature activation, carrying the exemption flag, the upper bound and the stop command.
- The leader's exemption, with its majority rule and its one-second window.
- Counts of followers exempted and relays withheld.

**Tests and acceptance:**
- **In-process: a voter stopped under load and returned 100k behind, on a link of 140 ms round trip.**
  - Without loss, under `bbr` and `cubic`: it levels within Section 9's time for its measured E_p.
  - **With the link's rate set below the live stream's** (a bandwidth limit on the link, not loss), the bottleneck is there by construction. The test asserts three things: the voter doesn't level; the metrics name the path as the bound (the leader's queue toward it, and `paths[]`); and at a domain rate below the link's rate over the bytes a command, with the margin, it does hold level.
  - **With 0.1% loss under `cubic` and no rate limit,** the test doesn't assume the path is the bound, since Section 3's estimate is unsettled (Codex on #301). It first reads the leader's rate toward the voter from `paths[]` while the voter is level, then asserts against that reading:
    - where the reading carries R·c·(1 + m), the voter levels within Section 9's time, as without loss;
    - where it doesn't, the three assertions of the rate-limited case hold, with the reading as the path's rate.
  - In every case it never holds more than its kept bound or 9 pages, and never more than 5 asks outstanding to one donor.
- **A lost head ask with the pipeline full:** with the first range's answer dropped and the other 8 slots holding later pages, the retry takes the head's slot, the hole fills, and held pages never exceed 9. The head's late answer, delivered after the retry's, is dropped and counted as `late`.
- **A voter returned past the run's bound,** 200k behind on a path that carries more than R:
  - it is exempted from its first ask, and keeps nothing until its gap fits a run;
  - then it keeps one and levels;
  - while it is exempt, the leader sends it frontiers and control frames only.
- **n5's shape:** a follower whose hole takes longer to fill than the old held cap allowed. With deep keep, nothing past its kept run is dropped, and it drains its run once the hole is filled.
- **A leader change during the drain:** F keeps the run's decided part, executes it in order without divergence, and levels within Section 4.1's time plus the election.
- **The drain's acknowledgements:** during a drain, F sends the leader no `SlowAck` at or below the frontier it holds, and the leader's counted adoptions from F don't grow.
- **Negative controls:**
  - with the held cap of today (8,000), n5's shape drops relays and doesn't level;
  - K fixed at 2 fills the hole more slowly, by the ratio Section 7 gives;
  - with the run adopted one at a time, as today's path would, the drain runs at E_k, and the 100k case takes several times as long;
  - with the exemption off, the leader's bytes to the follower run at its whole rate;
  - with the acknowledgement rule off, the leader's counted adoptions from F grow by the run;
  - with the run cleared at a ballot change, as today, the hole grows by the run.
- **Testing:** the +loss0.1 cell at 480 again.
  - **The prediction for n5** (Section 4.1): level within 10 to 17 s of its first page, by E_p from 4,500 down to 3,500, with a run near 11k commands, about 6 MB.
  - Read:
    - n5's executed rate beside the leader's;
    - the run's size;
    - `not_taken` flat;
    - the time from its first page to level;
    - E_p and E_k;
    - `paths[]` for its control and bulk connections;
    - n1's `proposals` bytes and `traffic.sent_bytes` a command a follower, which is Section 3's figure.

**Left out:**
- **Acknowledgements a frame at a time,** which would remove the rest of Section 2.2's burst. That is a protocol change. Section 4.4 removes most of it at its source.
- **The admission sent once per session,** which would take about 165 to 200 B off every copy (Section 3). That is a format change, sized first by a counting build.
- **Two windows installed at once,** which would raise E_p and needs the reserve for two windows.
- **The admission ceiling against E_p** (the owner's, task-d26).
- **The controller per lane** (#299; `bbr` stays a switch, the lead's decision). Section 3 is an input to it.
- **A node-wide kept budget** across domains, which comes with multi-domain hosting (Section 9).
- **A behind follower's frontend sending its callers elsewhere,** which is storm fix 1's.
