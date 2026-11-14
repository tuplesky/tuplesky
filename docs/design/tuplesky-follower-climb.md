# A behind follower's climb

**Status:** design step only, no code (the lead's order on #185, 2026-11-14), under [task-d25](tuplesky-prs-plan.md#task-d25) (catch-up outpaces the domain) and [task-d56](tuplesky-prs-plan.md#task-d56) (a voter far behind is left to catch-up). It covers:
- the invariant: a behind follower's intake exceeds the domain's commit rate by a stated margin, in bounded memory;
- how many catch-up pages it keeps in flight, from the measured round trip and commit rate;
- what the leader does with relayed payloads for a follower that is refusing them;
- how the follower rejoins the live stream;
- the memory bound, and the worst-case climb for a 100k lag at 140 ms.

It changes nothing in what a ballot decides or in the catch-up protocol's messages: every ask is today's `CatchUpRequest`, and every donor serves as it does today. The code PR follows the lead's decision after testing's +loss1 cell.

## 1. The case

Testing's +loss0.1 cell at 480 ([#98, 6884311018](https://github.com/tuplesky/tuplesky/pull/98#issuecomment-6884311018)). n5 is 70 ms from the leader n1, which is also its donor. From 60 s to the run's end:
- **The leader executes about 2,570 commands a second.** n5 executes 870 to 980 a second, all of it from catch-up pages: 7.1 to 8.1 pages a second of 116 to 123 commands. So it falls further behind by about 1,600 a second, to 128,242 at 130 s.
- **The catch-up window reads 0 most of the time.** n5 waits for the next page, not on execution: its loop is 19 to 23% busy.
- **Its table holds 875 records, every one unexecuted, the oldest at PRE-ACCEPT.** That is the admission bound.
- **Kept relayed payloads stop at 7,134**, the 4 MiB bound (`MAX_KEPT_RELAYED_BYTES`). Past it, `not_taken` grows at the leader's whole rate, about 600 KB/s of payload the leader sends n5 and n5 drops.

## 2. Why it doesn't climb today

**One page per ask-to-page cycle.** At most one ask is out (`continue_from` is one slot), and at most two pages are in hand.
- The next ask goes out only when the follower is back to one page in hand: the page asked ahead has been installed, which happens once the window before it has executed (`continue_catch_up`, task-d25).
- So intake is at most 125 commands per cycle C, where C is the time from an ask to its page. At C = 160 ms, about one round trip of 140 ms plus the donor's turn and the transfer, that is 780 a second.

**The bulk connection's window under loss.** n5 took a page every 2 round trips, not every one, and the window bound explains it:
- A page of about 120 commands at about 1 KB each in a page (payload, dependency row, execution record) is about 85 packets.
- At 0.1% loss, a loss-based controller's window settles near 1.22/√p, about 40 packets. So a page takes two to three round trips to arrive.
- One connection then carries about 40 packets a round trip: about 59 commands, so about 840 a second at 70 ms. That is n5's 870 to 980.
- At 140 ms the same connection carries about 420 a second.
- More asks on the same connection don't raise that. More connections do (one per donor), and so does a controller that isn't bounded by loss (#299's `bbr`).
- This is an estimate from the counts. #297's `paths[]` for the bulk lane (`cwnd`, `lost_packets`) reads it directly, and n5's run predates #297.

**The relay keeps going.** The leader never stops or slows fresh relays to a voter behind it (`proposal_sends`). task-d56 planned that, and it was never built. The follower does what it can with what arrives:
- each slot catch-up frees in its table takes another PRE-ACCEPT record from the kept payloads (`retake_kept`). The record can't be adopted, because what it depends on is far ahead of what the follower has executed;
- past the kept bound, proposals are held without their payload (up to `HELD_PROPOSAL_SLACK` tables), and their payloads are fetched from the leader, eight an ask. Those fetches go on the bulk lane, the same lane as the pages.
- None of this helps the climb: every command it brings, catch-up brings again, payload included (a page reads each command's payload).

**What doesn't limit it here.** The window is one page, installed durable before it executes (the reserve, [Section 13.1 of the design](tuplesky-design.md#131-resource-contract)). Its cost is about 0.28 ms a command locally (task-d25), so a voter executes pulled commands at about 3,500 a second locally. testing's n5 reading, 21% of a loop at about 950 a second, is about 0.22 ms of loop a command, which would be about 4,500 a second. Both are estimates; the window was idle in n5's run.

## 3. The invariant

A voter is **climbing** while its gap G, the domain's executed position less its own, is more than `CLIMB_ENTER` = 1,000 commands (eight pages). It stops climbing when G falls to `CLIMB_LEAVE` = 500 (four pages). The gap is measured from what the pages say: the newest page's `through` (its donor's executed position), plus R times that page's age, less the voter's executed position.

While climbing:
1. **Its intake I, the commands a second it installs and executes from pages, is at least (1 + m) × R**, where R is the domain's commit rate as the voter measures it and m = 1/2.
2. **What it holds for the climb is bounded:** at most `K_MAX` = 8 pages asked for or in hand outside the table, beside the one window the reserve holds. Its kept payloads and held proposals don't grow, because nothing new is relayed to it (Section 5).

**Why m = 1/2.** The gap closes at least at R/2, so a voter that stopped for t seconds (a gap of R·t) is back within 2t. The margin also covers the error in R and C, each smoothed over about a second, and a burst of half again the rate.

**What the voter controls, and what it doesn't.**
- **It controls the pipeline**, K·P/C (Section 4).
- **The execution of pulled commands, E, is the host's.**
- **The path is the network's and the controller's.** Under loss with CUBIC, that is about one window of packets a round trip per connection.

So I = min(K·P/C, E, path). The invariant holds only where E and the path allow (1 + m)·R. That is Section 13.1's existing rule, that admission stays below the rate a catching-up voter executes, with the margin made explicit. Where they don't, the voter still gains at I − R if that is positive, and the metrics (Section 8) say which bound it met. The admission ceiling stays the owner's decision ([task-d26](tuplesky-prs-plan.md#task-d26)).

## 4. Pages in flight

**How many.** While climbing, K = clamp(⌈(1 + m)·R·C / P⌉, 2, `K_MAX`). Otherwise K is 2, today's one page in hand and one asked ahead.
- **R:** the advance of the newest page's `through` over the last second or more. That is the donor's executed position, which moves at the domain's rate. A page from any donor counts, and the highest `through` seen wins, as `owed_through` does today.
- **C:** each donor's ask-to-page time, smoothed by an eighth a sample (as a smoothed round trip is). It includes the donor's turn and the transfer, so it reads the path as well as the round trip.
- **P:** the mean commands a page over the last eight pages taken: 125, or fewer where pages are cut at the byte bound.
- **Example:** R = 2,600 and C = 160 ms give K = ⌈1.5 × 2,600 × 0.16 / 125⌉ = 5, a pipeline of about 3,900 a second. `K_MAX` = 8 gives 6,250.

**Which ranges.** Execution positions are the domain's, one per command and the same at every voter (a window executes in its donor's positions), so the voter can ask ahead of what it has.
- It keeps K asks out, for consecutive ranges after its tail: `after` = T, T + P, T + 2P, and so on.
- A page cut short at the byte bound leaves a hole between its end and the next range. The hole is asked for at once.
- Pages are taken into a queue by position, outside the table.
- The window still installs one page at a time from the queue's front, in position order. The reserve and the report bound are unchanged.

**Which donors.**
- Asks go round the executors at the leader's frontier, so pages come over up to (voters − 1) connections, each with its own window.
- The leader takes one ask in every K, enough for its exemption rule (Section 5). This keeps its share of serving small: a page costs a donor 0.77 ms of its loop on the runner (n1 in the case) and 3.4 to 3.7 ms locally (task-d25).
- A donor behind the range answers less, or nothing. Its range is asked again of the leader.

**A lost ask or page.** A range asked more than 2C + 100 ms ago and not answered is asked again of another donor. The pacer stays the backstop it is today.

**What #223's and task-d25's invariants become with K in flight.** The code PR argues each one again:
- no command installed twice or skipped;
- a stale page, an out-of-order page, and a page that arrives after the frontier passed it;
- the reserve holds with a window installed and up to `K_MAX` pages outside the table.

The simulator's catch-up rows are swept with K from 2 to 8.

## 5. The leader's relays to a voter that is climbing

**It stops sending them, and catch-up carries the voter.** The leader withholds proposals, re-sends and relayed payloads from voter F while all of these hold. This is task-d56's exemption, made concrete:
1. F asked the leader for catch-up within the last second, and that ask's `after` was more than `CLIMB_ENTER` below the leader's executed position.
2. F's last vote is more than a table's admission room (875 proposals) behind the leader's last proposal, so F could not take what is sent.
3. The leader forms a majority without F. At three voters with one down, F is never exempted. The fixed fast quorum is not a condition: F, not voting, is already missing from it.

It keeps sending F the commit frontier (`Committed`), one small frame a turn.

**Why stop, rather than keep sending within a budget.**
- **Nothing relayed to F while it is climbing reaches its table in time to be used.** Each command comes again in a page, payload included.
- **What it costs is real.** In the case it was about 600 KB/s on the control connection to n5. It also brought payload fetches for held proposals on the bulk lane, against the pages.
- **A budget's only use is at the rejoin edge**, and Section 6 makes that edge exact.

**On F's side while climbing:**
- It asks no payload for a held proposal; the pages bring it.
- What it kept and held before the exemption stays within its bounds. A page that installs a command drops that command's held proposal, kept payload and Sync entry, as `install_pulled` does today.

## 6. Rejoining the live stream

**The handoff is one leader turn.**
- When F's gap is at most `CLIMB_LEAVE`, F sends its asks to the leader alone.
- The leader serves an ask from F whose `after` is within `CLIMB_LEAVE` of its executed position E_L. In that same turn it:
  1. serves the page. F's chain is then owed E_L, and it asks until it has executed through E_L, as it does today;
  2. ends F's exemption. It sends F every proposal in its table not yet executed at E_L (the in-flight set, at most its admission room), through the re-send path, and then the live stream.

**Nothing is missed.** What F gets by pages covers every position up to E_L. What it gets by proposals covers every command not executed at the leader at that turn, and every command proposed after it. An overlap is harmless: a page's command that F already holds is adopted, and a proposal for a command F has executed is known.

**It fits in F's table.** F holds:
- the leader's in-flight set, about R times the commit latency (about 520 at 2,600 a second and 0.2 s);
- the live stream for the time its last pages take: 500 commands at I, about 0.13 s, so about 340.

That is about 860, within the 875 admission room. The held-proposal slack takes any excess, as it does today.

**Hysteresis.** F climbs again only when G exceeds `CLIMB_ENTER` again, and the leader exempts it only on a new ask at that distance. A voter at the edge doesn't flap.

**A change of leader during the climb.** The exemption is the old leader's state and goes with it. The new leader relays to F until F asks it from the climb's distance, so the cost is bounded by today's behaviour.

## 7. Bounds

**Memory, per climbing voter:**
- **Pages:** at most `K_MAX` asked for or held outside the table, and one window in it. That is 9 pages: at most 1,125 commands and 9 MiB, or nine of its largest commands. Today it is 2 pages and 2 MiB.
- **The table:** its capacity, reserve and report bound are unchanged. Nothing new is relayed while F climbs, so it holds only what it held at the start and the window.
- **Kept payloads and held proposals:** at most 4 MiB and `HELD_PROPOSAL_SLACK` tables, as today, and not growing while exempt.
- **At the donors:** at most `K_MAX` pages outstanding for one voter across its donors, 8 MiB, inside the transport's 16 MiB a destination.
- **At the leader:** a few words a voter for the exemption.

**A 100k lag at 140 ms.** The time is G / (I − R), plus about one cycle to start. The example uses R = 2,600 (the 480 step's leader), C = 160 ms and `K_MAX` = 8:

| What bounds intake | I (commands/s) | Gap closes at | 100k |
|---|---|---|---|
| Today: one ask a cycle, a path without loss | 125 / 0.16 s = 780 | no | never |
| Today, n5's case scaled to 140 ms (CUBIC, 0.1% loss, one donor) | about 420 | no | never |
| The climb, CUBIC at 0.1% loss, four donors | about 1,700 | no | never |
| The climb, a path not bound by loss, E = 3,500 (local cost) | 3,500 | 900/s | 111 s |
| The climb at the stated margin, I = 1.5 × R | 3,900 | 1,300/s | 77 s |
| The climb, E = 4,500 (the runner estimate) | 4,500 | 1,900/s | 53 s |

**The worst case at that commit rate, where the path isn't bound by loss: 111 s.**
- That case is execution-bound. Locally, a voter executes pulled commands at 1.35 times the 480 step's rate, short of the margin.
- At a commit rate of 1,000 a second the same voter closes 100k in 40 s.

**Where the path is a loss-bound CUBIC connection at 140 ms, no pipeline closes it at 2,600 a second.**
- Four donors carry about 1,700 a second, so it holds level only below that rate.
- That is an input to the `bbr` decision on #299 (the bulk lane is where it would matter most). It is not part of this design.

## 8. Steps, tests and what's left out

**Step 1: the follower** (no wire change):
- K pages in flight, sized from R and C, over the donors;
- holes and lost ranges asked again;
- no payload ask for a held proposal while climbing.
- **New counts on the metrics line:**
  - the climb's state, G, K, C and R;
  - kept payloads now, their count and bytes (today only `kept` and `kept_taken` are there, and `unkeep` takes payloads out without counting `kept_taken`);
  - held proposals now.

**Step 2: the leader** (no wire change):
- the exemption (Section 5) and the handoff (Section 6);
- counts of the voters exempted and the relays withheld.

**Tests and acceptance:**
- **In-process.** A voter stopped under load and returned 100k behind, on a link of 140 ms round trip, with and without 0.1% loss, under `bbr` and `cubic`. It rejoins within the time Section 7 gives for that link and controller. It never holds more than 9 pages. While exempt, the leader sends it nothing but frontiers.
- **The handoff test.** A voter rejoined at a turn where the leader holds proposals not yet executed misses none of them.
- **Negative controls:**
  - K fixed at 2 never closes the same gap;
  - with the exemption off, the leader's bytes to the voter run at its whole rate;
  - with the handoff's in-flight send left out, the handoff test fails.
- **Testing:** the +loss0.1 cell at 480 again.
  - n5's executed rate while behind, beside the leader's;
  - `not_taken` flat while it is exempt;
  - the time from its first page to rejoin;
  - `paths[]` for its bulk connections.

**Left out:**
- **Two windows installed at once,** to hide a window's persist behind the one before it. That raises E, and needs the reserve for two windows: a change to the report bound in Section 13.1.
- **The admission ceiling against E** (the owner's, task-d26).
- **The controller per lane** (#299; the lead's decision: `bbr` stays a switch).
- **A climbing voter's frontend sending its callers elsewhere**, which is storm fix 1's.
