# The single domain under the batching rule

**Status:** design step only ([task-d115](tuplesky-prs-plan.md#task-d115)), no code. It covers:
- the audit against the owner's rule;
- the cost model, per command and per turn;
- the proposals that follow from the rule;
- levers 1 and 2 (option 3 and the proposal frame as a unit) and the leader-log end state they lead to.

What can be built without deciding the end state is in [Section 6.4](#64-what-can-be-built-without-deciding). Building option 3 or the leader log waits for the owner.

**Measured on:** the carry `4e12af9b`, locally. Five voters on tmpfs, `replay`, one tokio worker each, the four features (`early-proposal`, `release-at-execution`, `answer-on-release`, `batched-round`), the bar's mix (put 33, get 34, contended 33), five frontends. Figures for the runner (AMD EPYC 7763) are scaled from the local ones part by part (Section 3). Lever numbers are the budget's on #185 ([6834470684](https://github.com/tuplesky/tuplesky/pull/185#issuecomment-6834470684)).

## 1. The rule

> Journal and store are for recovery. Storing command state individually, paying a storage round trip per command, is not batching. Batching for storage is one storage round trip per batch. Same for networking: one syscall per batch instead of one per command.

**What it means here.**
- **The batch is a turn:** what the domain loop appends to the journal at once (`Node::flush`). At 120 callers locally a leader's turn holds 9.4 commands and a follower's 12.3.
- **The journal is the recovery record.** Every row the projection holds is lowered from a journal record, and under `replay` a lost projection is rebuilt from the journal. So a row written only so that recovery can read it is a second copy.
- **The projection holds what reads need:** current values and events, sessions, leases, policy. It is written behind, once a batch, as one sorted write set. It is not read on the hot path: execution runs from memory.
- **The network:** one send syscall a peer a turn and one receive a batch. The datagrams follow the bytes.
- **What the rule leaves per command:** executing the command, encoding its bytes, and answering its caller.

## 2. The audit

The full table is on #185 ([6835865051](https://github.com/tuplesky/tuplesky/pull/185#issuecomment-6835865051)). It was taken with tracefs over 4 s of a run at 120 callers: every syscall of the voters' threads, every wake-up, and uprobes on the turn, the command, the journal, the projection's writes and every store read. Tracing slowed the run from about 2,400 commands a second to about 1,800, but the turns kept their size.

| Per turn | Leader | Follower | The rule allows | Verdict |
|---|---:|---:|---|---|
| Journal appends, syncs | 0.96, 0.96 | 1.02, 1.02 | 1, 1 | Follows the rule |
| Projection transactions = commits | 0.88 | 0.72 | 1 a batch | Follows the rule |
| Rows written | 148 (15.5 a command) | 161 (13.5 a command) | the batch's application rows | about 85% are protocol state or the meta row (below) |
| Store read transactions, by execution | 15.0 | 17.0 | 0 from memory | ×15 / ×17 |
| Point gets, by execution | 204 | 177 | 0 | about 200 a turn |
| Store reads by intake | 0 | 0 | 0 | Follows the rule |
| Peer send syscalls | 1.9 a follower | 2.2 | 1 a peer | ×1.9 / ×2.2 |
| Peer datagrams sent | 6.5 a follower | 2.7 | the frame's bytes | follows the bytes |
| Client-plane send syscalls | 16.5 | 10.9 | about 6.9 / 3.9 | ×2.4 / ×2.8, mostly ACK-only datagrams |
| Wake-ups, loop to tokio | 10.5 | 4.4 | 1 | ×10.5 / ×4.4 |
| tokio's `epoll_wait`s | 30 | 17 | 1 | ×30 / ×17 |
| Loop passes | about 57 | about 13 | 1 | inferred from the run-queue sample |

**The rows by collection.** A uprobe at `apply_updates` read each element's collection id (offset 48 of a 56-byte `StoreUpdate`). It sampled the first 240 rows of every commit, 74 to 87% of all rows, over 3 s:

| Collection | Leader | Follower | Rows a command at a follower | Under the rule |
|---|---:|---:|---:|---|
| `ProtocolV1` | 27.1% | 16.1% | 2.2 | journal only |
| `RetryV1` | 15.8% | 18.1% | 2.5 | in memory, journal for recovery |
| `PendingPayloadV1` | 13.6% | 16.1% | 2.2 | journal only |
| `PayloadV1`, `ExecutionV1`, `DecidedV1`, `RetryFloorV1` | 6.2 to 6.3% each | 7.1 to 7.2% each | 1 each | journal only |
| `MetaV1` | 6.2% | 7.0% | 1 | once a commit, not once a command |
| `KvCurrentV1`, `EventsV1` | 6.2% each | 7.0% each | 1 each | **stay: what reads need** |

Of about 14 rows a command at a follower, 2 are application rows. A commit writes them in lowering order, not sorted.

## 3. The cost model today

**How it was read.** Four runs at 30, 60, 120 and 240 callers locally. Each part's CPU a command is read from the threads' `/proc` times, and fitted as `a + b/k`:
- `a` is µs a command, `b` is µs a turn;
- `k` is the leader's commands a turn, from its journal appends;
- the largest residual is 8 µs a command.

The figures are summed over the five voters; "followers" is the four together.

| Part (five voters) | Per command `a` (µs) | Per turn `b` (µs) | At `k` = 9.55, share per command |
|---|---:|---:|---:|
| Leader's loop | 198 | 147 | 93% |
| Followers' loops | 539 | 388 | 93% |
| Tokio | 191 | 733 | 71% |
| Materializers | 284 | 64 | 98% |
| Appenders | 79 | 108 | 87% |
| **Servers** | **1,291** | **1,440** | **90%** |

At 0.66 commands an operation, this predicts:

| Commands a turn | 4 | 8 | 16 |
|---|---:|---:|---:|
| Locally (ms/op) | 1.090 | 0.971 | 0.911 |
| On the 7763 (ms/op) | 1.83 | 1.62 | 1.51 |

**How the 7763 column was scaled.** Each part is multiplied by the 7763's figure over the local one at 120 callers: the leader's loop 1.70, followers 1.61, tokio 1.99, materializers 1.37, appenders 1.21. Then 0.043 is added for the 7763's other threads. The 7763's own turn size isn't measured, so its column assumes the local one.

**What this says.** Nine tenths of the servers' CPU is paid per command. A turn four times larger (4 to 16 commands) takes off 0.18 ms an operation locally, and nothing larger is in reach at these loads. **The rule is met mostly by not doing per-command work at all, not by grouping it.** The largest steps below remove work; only some move it to the turn.

**A follower's loop by phase** (perf at 30 and 240 callers, one follower, split the same way):

| Phase | `a` (µs a command) | `b` (µs a turn) | What it is |
|---|---:|---:|---|
| Execution | 55.5 | 17 | Planning view from redb, authorization, retry, landing the plan's rows, `Follower::applied` |
| Peer events in | 28.5 | 16 | Taking each proposal in: per-command accept state, the payload |
| Frontend role | 17.2 | 6 | Its own callers' frames, retained answers |
| Settle | 13.4 | 0 | The turn's effects, per command |
| Flush and finish | 7.3 | 13 | Lowering, the journal hand-off |
| Hashing | 4.9 | 0 | blake3 over payloads |
| Pacing and runtime | 5.7 | 43 | The loop's passes |
| Other | 3.4 | 30 | Sends, reads, the rest |

## 4. The proposals under the rule

Each proposal below states what it removes, whether it is right, its size, its cost, and whether it moves work from per command to per turn. The sizes are summed into Section 7.

### 4.1 Proposal 2: protocol state in the journal only (lever 7)

This proposal leads, as review asked ([6835409559](https://github.com/tuplesky/tuplesky/pull/185#issuecomment-6835409559)).

**What it removes.** About 11 of a follower's 14 rows a command: payload, pending payload, dependency, decided, execution, retry and retry-floor rows. The meta row goes to once a commit.

**What reads those rows today, and where each read goes instead.** While it runs, the protocol machine holds the live part of this in memory: undecided and recently executed commands. The rows exist for a restart, and for history older than memory holds.

| Reader | Where it reads under the rule |
|---|---|
| Boot | The image's protocol section, then the journal above it |
| Catch-up served to a peer (task-d08) | The journal by position, through an in-memory index from execution position to journal entry, bounded by retention |
| A campaign's report | Memory, as today; after a restart, what boot rebuilt |
| Retained answers, resolve and settlement from the record | A bounded window of execution records in memory, and the journal past it |
| A retry's answer | The retry window in memory (task-d104's design, on #207), and the journal for recovery |

**Is it right?** Yes. Each of these rows is a copy of a journal record, kept so that recovery and old history can be read. No vote rests on them: a vote already rests on the journal, not the projection (task-d90), so proposal 2 changes no vote's durability.

**The cost.**
- **Journal retention** then follows what the image doesn't carry: undecided commands, the retry window, and the executed history donors serve.
- **The image** gains a protocol section.
- **Boot** replays from that section and the journal.
- **The readers above change,** and task-j05's crash matrix has to cover each one.

**Size.**
- Rows fall about 85%.
- The materializer's work that scales with rows is about 75 to 85% of it: assignment 24%, inserts and removes 40%, and its commits 20 to 28%, whose dirty pages follow the rows.
- So the materializers fall 65 to 72%: about −0.12 ms an operation locally and −0.16 to −0.18 on the 7763.
- That is larger than the budget's −0.05 to −0.08, which had no count of the rows, and larger than review's −0.07 to −0.14, which left out the retry and meta rows.

**Per command to per turn?** No. It removes the per-command rows. What is left, 2 application rows a command, stays per command.

**What it supersedes, for the rows that leave.**
- task-d113's keying of those rows by order;
- task-d103's proposal row;
- task-d104's retry rows. Its window in memory agrees with the rule and becomes part of this proposal.

**Build.** A design step for the recovery readers, then about 3 to 4 weeks.

**Its design step** is task-d117 ([design Section 17.17](tuplesky-design.md#s17-17)). It changes the readers' table above in one place: executed history is served from immutable segments written off the loop, not from the journal by position, so the journal's retention still ends at the published image and doesn't follow the slowest voter.

### 4.2 Proposal 1: execution from memory, the store written behind

**What it removes.**
- **The read transactions:** each command opens one for its planning view, 1.4 to 1.6 a command.
- **Tables opened:** 17.5 a command at a follower and 26 at the leader.
- **Point gets:** 14 and 22 a command.
- **Scan pages:** 3 and 4 a command.

These are execution's reads of authorization (`load_authorization`, 1.1 to 1.6 a command), session and retry state, and the keys the command touches.

**Under the rule.** Execution reads memory:
- the key index, in memory since task-d105, with current values beside it;
- sessions, authorization and the retry window.

The store is read at boot, and for anything memory doesn't hold. The write set lands behind, once a batch, sorted by collection and key.

**Is it right?** Yes, for the hot path. The cost is memory:
- **either the current value of every key,** which is etcd's choice for its index (it reads values through bbolt's mmap);
- **or a working-set cache whose misses are read in one view a batch.** That meets the rule's "at most one a batch".

The choice is the step's.

**First step, which needs no decision: lever 5 reshaped.** One read view a turn, with each table opened once. At the leader, read transactions fall from 15 a turn to 1, and tables opened from about 240 a turn to about 19. The gets stay per command until memory holds the state.

**Size.**
- The read view and snapshot are 37% of a follower's execution (21 µs a command each) and 3% of the leader's loop.
- That is −0.06 ms an operation locally and about −0.10 on the 7763: the budget's 0.066 at the followers, plus the leader and the snapshot.
- The gets that become lookups in memory aren't sized.

**Per command to per turn?** Partly. The view a command becomes none, or one a turn. The gets stay per command but become cheap.

**Build.** About 2 to 3 weeks after proposal 2, which brings the retry window and sessions into memory.

### 4.3 Proposal 3: one journal record and one append a turn

**Today:**
- one append and one sync a turn, which already follows the rule;
- inside each append, two records a command (a payload record and a protocol record), so about 20 records a turn.

**Is it right?** Yes. Under the leader log a turn's frame is one record.

**Size: not measured.** The appenders' per-command part is 79 µs. If half of it is per record, this takes off about −0.02 ms an operation locally and on the 7763. The copy, checksum and compression work stays per byte.

**Per command to per turn?** Yes, the records.

**Cost.** A journal record format, and a replay that splits a turn's record. About 1 to 2 weeks.

### 4.4 Proposal 4: the peer plane

**Today:**
- the leader makes 1.9 send syscalls a follower a turn;
- a follower makes 2.2 a turn toward the leader;
- receives run 2.4 a peer a turn, and half of them return `EAGAIN`: quinn-udp reads until the socket is empty.

**GSO and GRO, locally: both active.**
- **GSO:** the leader's peer `sendmsg` averages 4,288 B. It carries 3.5 datagrams a send, against quinn's cap of 10 (`MAX_TRANSMIT_SEGMENTS`).
- **GRO:** a follower's peer datagrams arrive about 3.6 to a message.

**On the runner's veth.**
- **GSO needs nothing from the device:** the kernel segments in software when the device can't.
- **GRO depends on the receiving device's setting.** Testing reads it from `send_calls` against `datagrams_sent` on the metrics line. The receive side needs quinn-udp's `gro_segments`, which the metrics line doesn't carry yet.

**Datagrams a turn's frame takes, a follower,** at about 1,225 B of payload a datagram:

| Commands a turn | 4 | 8 | 16 |
|---|---:|---:|---:|
| 805 B a command (today) | 3 | 6 | 11 (two sends) |
| About 250 B a command (lever 3) | 1 | 2 | 4 |

**Votes once a frame (lever 2):** a follower's 2.2 sends a turn become 1. **One wake-up a turn (lever 8):** the leader's 10.5 hand-offs to tokio become 1.

**Is it right?** Yes. Send syscalls are already within 2× of the rule. The audit's misses are the wake-ups and the empty receives.

**Size.** In levers 2, 3 and 8 below. Lever 3 is smaller than the budget's top, as review said: GSO already sends its 2.77 datagrams in 0.79 send calls, so a datagram saved is a segment saved, not a syscall.

### 4.5 Proposal 5: the client plane

**What already follows the rule.** Each bench caller holds its own QUIC connection and one operation in flight, so a connection gets at most one answer a turn. One send an answer is already one send a connection a turn.

**What can follow it:**
- The collector traffic: one frame a collector a turn, for the frontend's submissions to the leader and for the releases back. Lever 4 does the releases, which go one frame a release today.
- The receive side, if the socket is read once a turn (Section 7).

**What can't, with a one-op-at-a-time client:**
- An answer to each caller is its own send.
- Independent callers' requests arrive spread across a turn. Reading them once a turn holds the first one up to a turn.

**ACK-only datagrams.** 8.2 a turn at the leader and 5.6 at a follower are ACK-only: most of the client plane's excess over its floor. That is decision (c). #187 cut 13% of the api plane's datagrams with a longer ACK delay and moved no CPU.

**What would take it lower:** clients that share a connection, such as a proxy, kine's adapter or a pipelined SDK. One send then carries a turn's answers to that connection.

## 5. The four small PRs under the rule

- **Lever 4 (release frame, acks by range): agrees.** It moves releases from one frame a release to one a collector a turn. Goes ahead first.
- **Lever 8 (one hand-off a turn): agrees.** It is the audit's largest wake-up miss.
  - The audit also found that task-d97's run-queue sample reads `/proc/thread-self/schedstat` at one pass in 16: 35 of the leader loop's 55 syscalls a turn. It should go by time. That fix goes ahead of lever 8, with the loop's other per-pass housekeeping (Section 8).
- **Lever 5 (one read view a group): agrees, reshaped** as proposal 1's first step: one read view a turn, tables opened once.
- **Lever 6: agrees, last** (it reaches 3.7% of the materializer).
  - Part 1 (#251) is a map indexed by collection: −4.3% at the materializers.
  - Part 2, the retry window in memory, agrees with the rule and is folded into proposal 2.

## 6. Levers 1 and 2 and the leader-log end state

### 6.1 The end state

**The leader log:**
- **The leader** executes from memory and puts a turn's commands and their write sets in one frame, which is one journal record.
- **A follower:**
  - journals the frame;
  - acknowledges it once (lever 2);
  - lands the write sets behind, once a batch (lever 1, option 3);
  - holds no per-command consensus state.
- **The projection** holds no protocol rows (proposal 2).
- **A follower's plumbing** (settle, flush, pacing) is paid per frame, not per command.
- **What a follower still does per command:** its frontend role, landing the rows, and the executions its rotation assigns it.

### 6.2 What the single domain gives up

- **The fast path without the leader.** Today a collector proposes to its fast set, which includes the leader, and decides in one round trip to that set. In the leader log every command goes collector → leader → a majority → collector.
  - On a LAN, and under load where the relay (task-d100) already sends everything through the leader, this changes nothing.
  - On a WAN, a collector far from the leader pays the leg to the leader and then the leader's round trip, where the fast path pays its round trip to the fast set. That is the latency the design chose SwiftPaxos for ([leader locality remains relevant](README.md#performance-interpretation-leader-locality-remains-relevant)).
- **Independent execution at every voter,** and so detecting a divergence by a majority of executions.
- **Contended commands decided in rounds of their own:** they take the leader's order.

### 6.3 The fault model

**A diverging leader** (a bug, corruption or nondeterminism in its execution).

*Today*, every voter executes independently. A divergence shows at:
- a held release against the record (task-d06), which stops the node;
- a late release against an answer (task-d12);
- catch-up against its donor (task-d08);
- the forgetting floor's root every 4,096 positions (task-d27).

*With option 3*, followers land the leader's write sets, so they agree with whatever it executed and the floor's root no longer separates anyone.

*What replaces those checks:*
- **Rotation (#203's option 3):** follower i executes the commands whose identity hashes to i, compares its result with the leader's write set, and stops on a difference.
- **The leader's result digest** on its next message, 32 B a command, compared at `Follower::applied`.
- **task-d35's chain and task-d37's scrub,** at agreed positions.

*What the step must still argue:*
- **One execution is one against one.** Rotation detects; it doesn't decide which side is wrong. The step must say what decides: a second follower executing that command, with the majority's loser stopped, or the domain stopping for an operator.
- **Recovering a follower that landed wrong write sets.** Under `replay` its projection is rebuilt from its last good image and re-executed from the journal, without landing.
- **The open argument from #203:** that one independent execution a command, over a pre-state made by the leader's earlier write sets, detects what the four checks above detect today.

**A new leader executing what wasn't landed.** Commands decided under the old leader may have write sets that some followers never received.
- **The order and payloads of a decided command are in a majority's journals,** so its write set can always be recomputed.
- **A new leader:**
  1. lands the write sets it holds, up to the first position it lacks;
  2. executes, from that state, every decided command above it;
  3. ships those write sets before it serves.
- **Correctness rests on determinism,** as it does today: the new leader's execution at position p over the state at p − 1 equals the old leader's.
- **Every voter keeps the execution engine,** which rotation runs anyway.
- **An election takes longer** by executing that tail: a turn or two in steady state, bounded by the decided-not-landed window.
- **If the old leader was diverging,** the recomputed write set differs from one some follower already landed. That follower compares the two for the same position and stops on a difference, and the first case above applies.

### 6.4 What can be built without deciding

Everything that helps either end state:
- **Proposals 1, 2 and 3.**
- **Levers 3, 4, 5, 6 and 8.**
- **Lever 2's parts that don't assume a single proposer:** a frame acknowledged once by range, and forgetting by range.
- **The result digest on the leader's next message,** with its comparison at `Follower::applied`.
- **task-d35, task-d36 and task-d37.**

What needs the owner's decision:
- **Option 3:** followers no longer execute every command.
- **The frame as the only proposal path:** no fast path without the leader.

### 6.5 An alternative: voters that journal and vote but keep no projection

**The shape.** Of five voters, three keep a projection and execute every command: the leader and two followers. The other two are witnesses.
- A witness takes proposals in, journals them, votes and acknowledges.
- It keeps no projection, executes nothing and runs no materializer.
- It counts toward every quorum it counts toward today. Intake reads nothing from the store (Section 2), so its votes and dependencies need no projection.

**What it saves.** Per witness, a follower's execution (Section 3: 55.5 µs a command) and its materializer (about 57 µs a command a follower, in the run Section 3 was fitted to).
- For two witnesses that is about 225 µs a command: about 0.15 ms an operation locally and 0.22 on the 7763, at today's costs.
- Built after proposals 1 and 2, it saves less: they shrink both parts at every voter.
- The leader log saves more: four followers stop executing, and rotation executes a quarter at each.

**What it keeps that the leader log gives up (Section 6.2):**
- the fast path without the leader;
- independent execution of every command at three voters. A divergence is decided two executions against one, not one against one.

**What it gives up:**
- **Projection copies:** five become three. An executor lost with its disk is rebuilt from the other two.
- **Who can lead:** only an executor can lead without first installing an image and replaying. While witnesses are fewer than a majority, every majority holds an executor, so the domain stays live through any two failures. With two executors down, the last one must lead, and nothing can take over from it without a rebuild.
- **Reads:** reads and retained answers are served only where a projection is. A witness's frontend forwards them, or witnesses run no frontend.
- **Catch-up donors:** only executors can serve executed history (task-d08). A witness behind catches up its journal only.
- **Detection by every voter:** the floor's root and the release checks (Section 6.3) compare three executions, not five.
- **Even load:** executors carry more CPU than witnesses. On identical hosts, capacity is set by an executor's host, which the servers' sum doesn't show.

**What it depends on:**
- **Proposal 2.** Until protocol state is in the journal only, a witness would still keep a store for its protocol rows and would save only execution and the application rows.
- **The forgetting floor and checkpoints follow the executors.** A witness trims its journal at the floor the executors' images cover.

**Not built.** It needs a voter role in the configuration, an election rule that prefers executors, and read, frontend and catch-up rules for witnesses. It is a decision for the owner beside option 3, not a step toward it.

## 7. The cost model after each step

**Reading the table.**
- Each row is cumulative: the per-command `a` and per-turn `b` of each part, five voters, local, in µs.
- The last four columns are predicted ms an operation at 4, 8 and 16 commands a turn locally, and on the 7763 at the measured turn size (9.55).
- "Moves" says whether the step moves work from the per-command column to the per-turn column.

| Step (cumulative) | Leader loop | Followers' loops | Tokio | Materializers | Appenders | Moves? | k=4 | k=8 | k=16 | 7763 |
|---|---|---|---|---|---|---|---:|---:|---:|---:|
| Today (fit) | 198 + 147/k | 539 + 388/k | 191 + 733/k | 284 + 64/k | 79 + 108/k | — | 1.090 | 0.971 | 0.911 | 1.58 |
| #251, lever 6 part 1 (measured) | 198 + 147/k | 539 + 388/k | 191 + 733/k | 272 + 64/k | 79 + 108/k | No: cheaper | 1.082 | 0.963 | 0.904 | 1.57 |
| Proposal 2: protocol state in the journal | 198 + 147/k | 539 + 388/k | 191 + 733/k | 87 + 64/k | 79 + 108/k | No: removes rows | 0.960 | 0.841 | 0.781 | 1.41 |
| Proposal 1: execution from memory | 191 + 147/k | 455 + 388/k | 191 + 733/k | 87 + 64/k | 79 + 108/k | Partly | 0.900 | 0.781 | 0.721 | 1.31 |
| Proposal 3: one record a turn | 191 + 147/k | 455 + 388/k | 191 + 733/k | 87 + 64/k | 49 + 108/k | Yes | 0.880 | 0.761 | 0.702 | 1.28 |
| Lever 4: release frame, acks by range | 181 + 147/k | 447 + 388/k | 179 + 733/k | 87 + 64/k | 49 + 108/k | Yes | 0.860 | 0.741 | 0.682 | 1.25 |
| Lever 8: one hand-off a turn | 176 + 147/k | 447 + 388/k | 159 + 733/k | 87 + 64/k | 49 + 108/k | Yes | 0.843 | 0.725 | 0.665 | 1.22 |
| Lever 3: the payload by reference | 176 + 147/k | 439 + 388/k | 143 + 733/k | 87 + 64/k | 49 + 108/k | No: fewer bytes | 0.828 | 0.709 | 0.649 | 1.19 |
| Lever 2: the frame as a unit | 170 + 147/k | 351 + 428/k | 135 + 733/k | 87 + 64/k | 49 + 108/k | Yes | 0.767 | 0.645 | 0.584 | 1.08 |
| Lever 1: option 3 with rotation | 170 + 147/k | 287 + 428/k | 145 + 733/k | 87 + 64/k | 49 + 108/k | No: removes executions | 0.731 | 0.609 | 0.548 | 1.03 |
| End state: a follower's plumbing per frame | 153 + 167/k | 208 + 476/k | 145 + 733/k | 87 + 64/k | 49 + 108/k | Yes | 0.679 | 0.551 | 0.488 | 0.93 |

**The basis of each step** (local µs a command, five voters):
- **#251:** measured, −12 at the materializers.
- **Proposal 2:** −185 at the materializers (Section 4.1). Range −175 to −195.
- **Proposal 1:** the read view and snapshot, −21 a follower and −7 at the leader.
- **Proposal 3:** half the appenders' per-command part, −30. Not measured.
- **Lever 4:** the budget's −0.02 to −0.05 on the 7763, split over the leader's loop, the followers' acks in and tokio.
- **Lever 8:** −20 of tokio's park and wake (14 to 28% of tokio), and −5 of the leader loop's `eventfd` writes.
- **Lever 3:**
  - peer datagrams fall from 7.3 to about 3.5 a command;
  - at about a quarter of tokio's cost, which is per datagram under GSO (crypto 7%, quinn's per-packet work, kernel segments), that is −16;
  - plus −2 of each follower's hashing and decoding.
- **Lever 2:** half to two thirds of a follower's intake and of its learning and forgetting (`Follower::applied`), −22 a follower. +10 a frame a follower, plus the leader's acks in.
- **Lever 1:**
  - a follower's execution after proposal 1 (34.5) becomes landing (about 10) plus a quarter of the executions by rotation (8.6): −16 a follower;
  - +10 at tokio for the write sets' bytes.
- **End state:** three quarters of a follower's settle, flush and pacing per frame (−20 a follower, +12 a frame), and half of the leader's settle and flush.

**What takes tokio and the client plane lower.** In the end state tokio is the largest part: 0.29 ms an operation on the 7763.
- **Per turn, 733 µs over five voters:** the hand-offs, the wake-ups and the runtime's scheduling.
- **Per command, about 145:** the datagrams (quinn's per-packet work, crypto, the kernel).

Two things go further than the levers:
1. **The transport driven from the loop's turn.** quinn-proto is sans-IO. The loop would read its sockets once a turn (`recvmmsg`) and write each peer's transmits with one GSO `sendmsg` at the turn's end, with no hand-off, no `epoll` a datagram and no runtime. That is the rule's network form taken literally.
   - **It removes** park, wake, `epoll` and the runtime's scheduling, about 30 to 40% of tokio by the profile.
   - **Modeled as** tokio at 120 + 450/k: the end state goes from 0.93 to 0.86 on the 7763 at the measured turn size, and 0.80 at 16 a turn.
   - **The cost:**
     - the transport is rewritten around quinn-proto;
     - the client plane's connections are served from the loop or from a second loop of their own;
     - a datagram that arrives mid-turn waits for the turn's end.
   - **Its own design step.** About 4 to 6 weeks.
2. **Fewer datagrams a command:** lever 3 on the peer plane, and shared client connections on the client plane (Section 4.5).

Neither crypto (7% of tokio) nor QUIC itself is the difference.

**Does it reach parity?**
- **Without a protocol decision** (proposals 1 to 3, and levers 3, 4, 6 and 8), the servers end at about 1.19 ms an operation on the 7763. That is −0.39, more than review's −0.20 to −0.32 because proposal 2 is now sized from the measured rows.
- **The leader-log end state** reaches 0.93 at the measured turn size and 0.86 at 16 a turn, against etcd's 0.887 (#185, run with `--snapshot-count 100`; testing re-reads it against etcd's default).
- **With the transport on the loop's turn,** 0.86 and 0.80.

So parity needs the leader log and either larger turns or the transport change. Every figure but #251's is modeled from counts and profile shares, not measured as a pair. Each step's acceptance reads its own pair.

## 8. Order, and what the owner decides

**Without a decision, in order:**
1. Lever 4 (#253).
2. The loop's per-pass housekeeping (review's check on #185): the run-queue sample on a timer, and `redial` only on a change.
3. Execution a group at a time at every voter, not only the leader (the same check). It brings the followers under the rule's one read view a batch.
4. Lever 8.
5. Proposal 2's design step, then its code.
6. Proposal 1: lever 5 reshaped as its first step, then memory.
7. Proposal 3.
8. Lever 3.
9. Lever 6's rest.

**For the owner:**
- **Option 3 and the leader log,** with Section 6.2's trade and Section 6.3's open arguments.
- **Or witnesses (Section 6.5):** two voters that journal and vote but keep no projection. They keep the fast path and three independent executions, and give up two projection copies and leadership at the witnesses.
- **Whether the transport moves onto the loop's turn.**
