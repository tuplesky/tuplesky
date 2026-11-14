# A behind follower's climb

**Status:** design step only, no code (the lead's order on #185, 2026-11-14, revised to the lead's design review that evening), under [task-d25](tuplesky-prs-plan.md#task-d25) (catch-up outpaces the domain) and [task-d56](tuplesky-prs-plan.md#task-d56) (a voter far behind is left to catch-up). It covers:
- what held testing's n5 back, from its table and the code (Section 2);
- whether the live stream can reach a follower at all on a lossy path (Section 3);
- two ways for a follower to climb, compared, and the one proposed (Sections 4 and 5);
- the invariant, the memory bound, and the worst case for a 100k lag at 140 ms (Sections 6 and 9).

Every code fact is read at the carry `f612ca20`. Every figure says whether it was measured, read from code, or estimated.

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

The keep's 2 s limit is no longer than the leader's hand-off, so a frame kept to its limit still races the re-sends no worse than today. Acknowledgements are idempotent at the leader. Sending acknowledgements a frame at a time, not a command at a time, would remove the burst itself. That is a protocol change and is left out (Section 10).

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
| Observed | n5's control connection carried every relay at about 2,500 a second | none in this run |

**What this says.**
- At 70 ms the stream sits at the path's capacity. The estimate puts it above, but n5 received every relay, so the real window was at least the stream's rate. The margin is unknown. #297's `paths[]` (the window and losses on n1 → n5 control) reads it directly, and n5's run predates #297.
- At 140 ms the window halves the rate and the stream doesn't fit, by a factor of 2.5 to 3. **A follower 140 ms from the leader, on a 0.1%-loss path with CUBIC, can't stay level at 2,600 a second, whatever catch-up does.** Once the leader's queue toward it fills, frames are dropped (Section 2.2), and each is a hole in what the follower keeps.
- The fixes are the controller, or fewer bytes a command:
  - **The controller:** `bbr` on the control lane. It is #299's switch, and the default is the lead's decision.
  - **Fewer bytes:** the payload record's request bytes are, by the same estimate, only about 55 to 90 B of its 255. The rest is the retry key and the admission facts, which travel with every copy, to every follower. Sending the admission once per session, by reference, would bring a copy near the request's size. That is a format change, sized here and not proposed: a counting build gives the split first.

**etcd, for comparison.**
- Testing's socket statistics aren't in the comments I have, so this is from the protocol, not measured: etcd sends each entry to each follower once, in an append message. That is the request's protobuf plus about 20 to 30 B of entry and message framing, so probably 100 to 150 B a command.
- I'll ask testing for `ss -ti` bytes acknowledged per peer connection, for both systems in the same cell.

## 4. Two ways to climb

Notation:
- **R:** the domain's commit rate.
- **H:** the hole, the gap when the follower starts keeping.
- **I:** intake from pages while the hole is filled. It is at most E and at most the path.
- **E:** the follower's execution rate.
- **c:** the memory charge for a kept command.

The figures use R = 2,600 (the 480 step's leader), E = 3,500 (local cost, task-d25) or 4,500 (the runner estimate), and c = 500 B. The figure c is today's charge of about 330 B plus the held proposal, which today's bound doesn't charge; the code step measures it.

### 4.1 Deep keep (the lead's alternative)

**The mechanism.**
1. A behind follower keeps every relayed proposal and payload from the first one it can't take, as one run in the leader's sequence order, outside the table. The bound is in bytes, set by configuration (64 MiB here), and it charges the held proposal too.
2. Catch-up fills only the hole: from the follower's executed position up to the run's first command. The hole doesn't move, so page intake needs to be above zero, not above R.
3. Once the hole is filled, the run adopts in sequence order. Each proposal's predecessor is at ACCEPT, the leader's frontier (far ahead) commits it, and it executes at E with no network. The table's 1,000 bound holds: a kept command enters the table only when its turn has come (reserve room), and execution frees the room.
4. The follower is level when the run is drained. It never left the live stream, so there is no handoff.

**What breaks the run, and what then.**
- **A command missing from the run** stops adoption at it, under the conservative key, as at n5. Pages then go on past that point, because the follower has nothing executable, as today's pacer rule says (task-d25). A gap costs pages, not correctness.
- **The kept bound reached:** later relays are not kept, because each would sit behind a new hole. The run's end is then fixed. Past that, the follower needs the leader to stop relaying, and that is the exemption (Section 8).

**Its numbers** (a stop of t seconds, so H = R·t):
- Filling the hole takes H/I. Meanwhile the run grows at R, to R·H/I, which is c·R²·t/I of memory.
- The run drains at E − R, so the follower is level after (H/I)·E/(E − R).
- It needs E > R, the run to fit the bound (R·H/I ≤ bound / c), and the entry condition of Section 3.
- 64 MiB at 500 B is about 134k commands, 52 s of the live stream at R = 2,600.

| A 100k gap, R = 2,600 | I | Hole filled | Run at its peak | Level at E = 3,500 | Level at E = 4,500 |
|---|---|---|---|---|---|
| One donor, a loss-bound path at 140 ms | about 420 | 238 s | 619k, 310 MB | over the bound | over the bound |
| Four donors, loss-bound at 140 ms | about 1,700 | 59 s | 153k, 76 MB | over 64 MiB; with 80 MiB, 229 s | over 64 MiB; with 80 MiB, 139 s |
| Pages at the margin, a path without loss | min(3,900, E) | 25.6 to 28.6 s | 67k to 74k, 33 to 37 MB | 111 s | 61 s |

### 4.2 Pages only (this design's first version)

The relays are dropped, and pages must outrun the domain: intake I > R, with I = min(K·P/C, E, path).
- The follower is level after H/(I − R).
- It needs (1 + m)·R ≤ min(`K_MAX`·P/C, E, path). The first term is Codex's condition on #301: with pages cut short at the byte bound, say 40 commands a page at C = 160 ms, `K_MAX` = 8 caps intake at 8 × 40 / 0.16 = 2,000 a second, below R = 2,600, so the follower loses ground however much it could execute.
- On a loss-bound path at 140 ms, four donors give about 1,700 a second, so it never closes.

### 4.3 Side by side

| | Deep keep | Pages only |
|---|---|---|
| What intake from pages must beat | 0 | R, with margin |
| Bounded by | E, once the hole is filled | min(pipeline, E, path) |
| 100k at 140 ms, loss-bound, four donors | 139 to 229 s with an 80 MiB bound | never |
| 100k on a path without loss, E = 4,500 | 61 s | 77 s (the pipeline at 3,900) |
| Memory | the kept bound, configured | 9 pages (9 MiB) |
| Network | the live stream it must carry anyway, plus the hole's pages | every command twice: the relay it drops, and the page |
| Where it fails | the run reaching its bound, or the stream not fitting the path (Section 3) | any path or pipeline below (1 + m)·R |

Where both work, deep keep is never slower: with I ≥ E, (H/I)·E/(E − R) ≤ H/(E − R).

## 5. The proposal

Deep keep first, K pages in flight for the hole, and the exemption only past the kept bound:
1. **Deep keep** (Section 4.1). The kept bound is in bytes, set by configuration, 64 MiB by default. The held-proposal bound follows it, rather than being a multiple of the table. Relays don't enter the table before their turn (Section 2.1).
2. **K pages in flight** (Section 7) fill the hole sooner. That shortens the time the run grows, and so the memory deep keep needs: the run's peak is R·H/I.
3. **The exemption** (Section 8), only when the run's end is fixed at the bound. The leader then stops relaying to F, and pages alone carry it, as in Section 4.2.
4. **A keep for refused control frames** on the peer plane, and refusals counted by lane (Section 2.2).
5. **Asks from the frontier.** An ask is never for a range below the executed frontier, and K pages stop at the kept run's start. That removes the stale pages of Section 2.3, which the run's own executions would otherwise make on purpose.

## 6. The invariant

While a follower F is behind:
1. **What it keeps is one run** in the leader's sequence order, of at most `KEPT_BOUND` bytes, proposal and payload charged. Nothing in the run enters the table before its turn.
2. **It holds at most `K_MAX` pages** asked for or outside the table, beside the one window the reserve holds.
3. **Pages fill only the hole,** from its executed position to the run's start, and they don't ask below the frontier.
4. **It is level within (H/I)·E/(E − R),** under the conditions below.

**Conditions**, each with the margin m = 1/2:
- **Execution:** E ≥ (1 + m)·R. This is Section 13.1's admission rule with the margin stated. The admission ceiling stays the owner's (task-d26).
- **The entry:** the live stream fits the path, so bytes a command × R × (1 + m) ≤ the path's rate (Section 3).
- **The run:** R·H/I ≤ `KEPT_BOUND` / c, for the gap H the follower starts from.
- **Past the run's bound,** where pages alone carry it: `K_MAX`·P/C ≥ (1 + m)·R, as well as E and the path. A follower that is pipeline-limited (Codex's 40-command pages) still gains where `K_MAX`·P/C > R, at that rate less R. Below R it falls behind, and the metrics (Section 10) say which bound it met.

**Why m = 1/2.** Under pages alone, the gap closes at least at R/2, so a voter stopped for t seconds is back within 2t. Under deep keep, the margin on E drains the run at least at R/2.

## 7. Pages in flight

**How many.** While the hole is open, K = clamp(⌈I_target·C / P⌉, 2, `K_MAX`), where I_target is (1 + m)·R past the run's bound, and E otherwise. Today K is 2: one page in hand and one asked ahead.
- **R:** the advance of the newest page's `through` over the last second or more. Pages from any donor count, and the highest `through` seen wins, as `owed_through` does today.
- **C:** each donor's ask-to-page time, smoothed by an eighth a sample.
- **P:** the mean commands a page over the last eight pages.

**Which ranges.**
- Execution positions are the domain's, the same at every voter, so the follower asks consecutive ranges after its tail: `after` = T, T + P, and so on.
- A page cut short at the byte bound leaves a hole, which is asked for at once.
- No ask starts below the executed frontier, and none past the kept run's start.
- Pages are taken into a queue by position, outside the table. The window installs one page at a time, in order, and the reserve and the report bound are unchanged.

**Which donors.**
- Asks go round the executors at the leader's frontier, so pages come over up to (voters − 1) bulk connections, each with its own window.
- The leader takes one ask in K.
- A donor behind the range answers less or nothing, and the range is asked again of the leader.

**A lost ask or page.** A range asked more than 2C + 100 ms ago and not answered is asked again of another donor. The pacer stays the backstop.

The code PR argues #223's and task-d25's invariants again, with K in flight:
- no command installed twice or skipped;
- stale, out-of-order and passed pages handled;
- the reserve holds.

## 8. The exemption, past the kept bound

**When.** The leader withholds proposals, re-sends and payloads from F while all of these hold:
1. **F has said that its kept run is at its bound.** It does so in its catch-up ask: one field in `CatchUpRequest`, versioned, so a follower that doesn't send it is never exempted, which is today's behaviour.
2. **That ask is within the last second.**
3. **The leader forms a majority without F.** At three voters with one down, F is never exempted.

The leader keeps sending F the commit frontier.

**Why not "F's last vote is behind" (this design's first condition 2).** It measures the wrong thing:
- A follower behind in execution still votes while its table has room. So a lagging vote doesn't mean F refuses what is sent.
- Under deep keep, F takes relays into its run without voting on them: a kept proposal is adopted, and voted for, only in its turn. So a lagging vote doesn't mean F refuses either.
- What means "cannot take what is sent" is F's run having reached its bound. Only F can see it, so F says it.

**The handoff** applies only after an exemption. F is then a pages-only follower.
- When its gap is within `CLIMB_LEAVE` = 500, F asks the leader alone.
- In the turn that serves such an ask, the leader serves the page, so F's chain is owed the leader's executed position E_L. In the same turn it sends F every proposal not yet executed at E_L, then the live stream.
- Pages cover everything up to E_L, and proposals everything after. The total is about 860 commands, within F's admission room of 875.

**A change of leader.** The exemption is the old leader's state and goes with it. The new leader relays to F until F says again that it is past its bound.

**The gap estimate and a leader change.** Pages alone use the gap G, the newest page's `through` plus R times that page's age, less F's executed position.
- During an election the domain commits nothing, so R times the age overstates G by up to R times the age.
- So the age term is capped at 2C, and while no page has come in 2C, G is held rather than extrapolated.
- Then the error is at most R·2C, about 830 commands at R = 2,600 and C = 160 ms.
- That is more than `CLIMB_LEAVE`, so the handoff is decided by the leader, from its own executed position, not by F's estimate. F's estimate only says when to ask the leader alone.
- Under deep keep the hole's end is the run's first command, which F knows exactly, so no estimate is needed.

## 9. Bounds

**Memory per behind follower:**
- **The kept run:** at most `KEPT_BOUND`, 64 MiB by default, charging the proposal and the payload. Today it is 4 MiB of payloads, plus up to 8,000 held proposals that aren't charged.
- **Pages:** at most `K_MAX` = 8 asked for or outside the table, and one window in it: 9 pages, 1,125 commands and 9 MiB. Today it is 2 pages.
- **The table:** capacity, reserve and report bound unchanged. The run enters it only in turn.
- **At the donors:** at most `K_MAX` pages outstanding for one follower, 8 MiB, inside the transport's 16 MiB a destination.
- **At the leader:** a few words a follower for the exemption.

**A 100k lag at 140 ms, R = 2,600, E = 4,500:**

| The path | Deep keep, 64 MiB | Deep keep, 80 MiB | Pages only |
|---|---|---|---|
| Without loss, or `bbr` (pages at the margin) | 61 s | 61 s | 77 s |
| CUBIC at 0.1% loss, four donors | the run passes 64 MiB at about 88k behind, so exempt, then never | 139 s | never |
| CUBIC at 0.1% loss, one donor | exempt, then never | exempt, then never | never |

**The worst case is that last row, and nothing in this design closes it.** At 140 ms with 0.1% loss and CUBIC, the live stream itself doesn't fit the path (Section 3): the follower falls behind before any climb starts. The fix is the controller or fewer bytes a command, not the climb.

## 10. Steps, tests and what's left out

**Step 1, the follower** (no wire change):
- the kept run, sized by bytes from configuration, charging the proposal;
- relays into the table only in turn;
- K pages in flight for the hole, asked from the frontier and not past the run's start;
- refused control frames kept and offered again, as on the api plane.

New counts on the metrics line:
- the run's commands and bytes, and whether it is at its bound;
- held proposals now;
- the hole's size, K, C and R;
- pages stale on arrival, and commands executed outside windows;
- refusals by lane.

**Step 2, the leader** (one versioned field in `CatchUpRequest`): the exemption past the kept bound, and the handoff. Counts of followers exempted and relays withheld.

**Tests and acceptance:**
- **In-process:** a voter stopped under load and returned 100k behind, on a link of 140 ms round trip, without loss and with 0.1% loss, under `bbr` and `cubic`. It levels within Section 9's time for that row, or is exempted where Section 9 says it is. It never holds more than its bound or 9 pages.
- **n5's shape:** a follower whose hole takes longer to fill than the old held cap allowed. With deep keep, nothing past its kept run is dropped, and it drains its run once the hole is filled.
- **The handoff test,** after an exemption: no proposal not executed at E_L is missed.
- **Negative controls:**
  - with the held cap of today (8,000), n5's shape drops relays and doesn't level;
  - K fixed at 2 fills the hole more slowly, by the ratio Section 7 gives;
  - with the exemption off past the bound, the leader's bytes to the follower run at its whole rate.
- **Testing:** the +loss0.1 cell at 480 again. Read:
  - n5's executed rate beside the leader's;
  - the run's size;
  - `not_taken` flat;
  - the time from its first page to level;
  - `paths[]` for its control and bulk connections;
  - n1's `proposals` bytes and `traffic.sent_bytes` a command a follower, which is Section 3's figure.

**Left out:**
- **Acknowledgements a frame at a time,** which would remove the burst of Section 2.2. That is a protocol change.
- **The admission sent once per session,** which would take about 165 to 200 B off every copy (Section 3). That is a format change, sized first by a counting build.
- **Two windows installed at once,** which would raise E and needs the reserve for two windows.
- **The admission ceiling against E** (the owner's, task-d26).
- **The controller per lane** (#299; `bbr` stays a switch, the lead's decision). Section 3 is an input to it.
- **A behind follower's frontend sending its callers elsewhere,** which is storm fix 1's.
