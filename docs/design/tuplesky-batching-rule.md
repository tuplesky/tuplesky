# The single domain under the batching rule

**Status:** design step only ([task-d115](tuplesky-prs-plan.md#task-d115)), no code. It covers:
- the audit against the owner's rule;
- the cost model, per command and per turn;
- the proposals that follow from the rule;
- levers 1 and 2 (option 3 and the proposal frame as a unit) and the leader-log end state they lead to;
- the buffer as the unit at every stage, filled by the load (Section 9);
- the frame as the unit of decision, a protocol change put to the owner (Section 10).

What can be built without deciding the end state is in [Section 6.4](#64-what-can-be-built-without-deciding). The owner has parked option 3 and the leader log ([Section 8](#8-order-and-what-the-owner-decides)).

**Measured on:** the carry `4e12af9b`, locally. Five voters on tmpfs, `replay`, one tokio worker each, the four features (`early-proposal`, `release-at-execution`, `answer-on-release`, `batched-round`), the bar's mix (put 33, get 34, contended 33), five frontends. Figures for the runner (AMD EPYC 7763) are scaled from the local ones part by part (Section 3). Lever numbers are the budget's on #185 ([6834470684](https://github.com/tuplesky/tuplesky/pull/185#issuecomment-6834470684)).

## 1. The rule

> Journal and store are for recovery. Storing command state individually, paying a storage round trip per command, is not batching. Batching for storage is one storage round trip per batch. Same for networking: one syscall per batch instead of one per command.

**What it means here.**
- **The batch is a turn:** what the domain loop appends to the journal at once (`Node::flush`). At 120 callers locally a leader's turn holds 9.4 commands and a follower's 12.3. Section 9 makes it a buffer that can span turns, filled by the load.
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
| Reads inside the materializer's write transaction | 30 (3.2 a command) | 26 (2.1 a command) | 0 | proposal 2 removes them (review on #252) |
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

**Review's check (#252, [6837290600](https://github.com/tuplesky/tuplesky/pull/252#issuecomment-6837290600)).** Review ran the same setup untraced, with a build that counts every row a commit applies, not the first 240:
- every row of the table above that its counters reach agrees: commands, commits, peer sends and datagrams, ACK-only datagrams, and rows a command (15.51 and 13.51);
- a follower writes 2.0 `ProtocolV1` and 2.0 `PendingPayloadV1` rows a command, 2.51 `RetryV1`, and one each of the other seven;
- by bytes the application rows are more than by count: 873 B of a follower's 2,801 B a command, 31%;
- the store reads, wake-ups and `epoll_wait`s need the tracer, and weren't checked.

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
- **Review's knockout holds it** (#252): a lowering that skipped the payload, protocol and executed rows and wrote the meta row once a commit took a follower from 13.5 rows and 2,801 B a command to 5.6 and 1,028 B. The materializers fell 48%; with the retry rows, which can't leave until their window is in memory, 59 to 69%. That is −0.13 measured and −0.16 to −0.18 in all on the 7763.
- **The knockout pays nothing for what the design adds back.** task-d117's design step (#258) adds 20 to 30 µs a command over five voters, for the recovery map, the history buffer and the retry window's versions, so net about −133 to −143 by review's reading on #258 ([6839148455](https://github.com/tuplesky/tuplesky/pull/258#issuecomment-6839148455)): the assignment still runs over the map, and the gross took it out with the rows. Section 7 keeps the gross until that step's pair.
- **The retry rows go only with their window in memory.** Review's first knockout dropped them without it: each command's record carried 63 retry updates in place of 2.5, and the run slowed. That orders task-d117's steps.

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
- inside each append, three records a command at every voter (a protocol, a payload and an application record, by `journal_groups`), so about 29 a turn at the leader (review on #252).

**Is it right?** Yes. Under the leader log a turn's frame is one record.

**Size: none; the earlier estimate is withdrawn.** Review's profile of the appender (#252) finds its work per byte, not per record:
- encoding 40%, half of it a second encode inside `JournalRecordV1::verify`;
- the digest 16%, lz4 19%, the file write 7%, and waiting for a job 5%.

There is nothing per record to halve, so this proposal stays unsized until a profile shows something per record.

**The lever that is there:** one encode for both the digest and the write. `verify` encodes the body to hash it, and the engine encodes it again to write it. One encode is about a fifth of the thread, −0.015 ms an operation on the 7763, with no format change. It is a small PR (Section 5).

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

## 5. The small PRs under the rule

- **Lever 4 (release frame, acks by range): agrees.** It moves releases from one frame a release to one a collector a turn. Goes ahead first.
- **Lever 8 (one hand-off a turn): agrees.** It is the audit's largest wake-up miss.
  - The audit also found that task-d97's run-queue sample reads `/proc/thread-self/schedstat` at one pass in 16: 35 of the leader loop's 55 syscalls a turn. It should go by time. That fix goes ahead of lever 8, with the loop's other per-pass housekeeping (Section 8).
- **Lever 5 (one read view a group): agrees, reshaped** as proposal 1's first step: one read view a turn, tables opened once.
- **Lever 6: agrees, last** (it reaches 3.7% of the materializer).
  - Part 1 (#251) is a map indexed by collection. Review's pair reads −1.2% at the materializers, not this document's −4.3%: −3 to −6 µs a command over five voters, −0.003 to −0.007 ms an operation on the 7763.
  - Part 2, the retry window in memory, agrees with the rule and is folded into proposal 2.
- **The appender's one encode (review on #252): agrees.** One encode for the record's digest and its write, about a fifth of the appender, −0.015 ms an operation on the 7763.

## 6. Levers 1 and 2 and the leader-log end state

**Parked by the owner (Section 8):** the leader log and option 3 (6.1 to 6.4), witnesses as 6.5 has them, and lever 2 as Section 7's parked row sizes it. They stay here as the record of the trade. Section 6.6 is the alternative still open, and Section 6.7 redesigns lever 2 without a protocol change.

### 6.1 The end state

**The leader log:**
- **The leader** puts a turn's commands in one frame, which is one journal record. It executes each command from memory once it is decided, as every voter does today, and the command's write set goes in the leader's next frame. So a frame carries a turn's new commands and the write sets of the commands decided since the last one.
- **A follower:**
  - journals the frame;
  - acknowledges it once (lever 2);
  - lands the write sets behind, once a batch (lever 1, option 3);
  - holds no per-command consensus state.
- **The projection** holds no protocol rows (proposal 2).
- **A follower's plumbing** (settle, flush, pacing) is paid per frame, not per command.
- **What a follower still does per command:** its frontend role, landing the rows, and the executions its rotation assigns it.

**Why the leader executes after the decision, not ahead of it** (review on #252).
- **Ahead of it,** a turn's write sets would go in the same frame as its commands. Whoever holds a decided command would then hold its write set, and Section 6.3's second case couldn't arise.
- **But the leader would execute in memory what it may lose.** A command it proposed and that isn't decided when it loses the lead may be decided at another position by the next leader, or not at all. Its execution has to be undone.
- **The undo** is the one task-29's speculation states (task-d63): results held in an overlay in memory, the overlay dropped on a role change, and nothing released before the command and its prefix are learned. `coordd` doesn't drive that speculation today (task-d50).
- **Today a voter executes only what is decided, so nothing is undone.** Executing ahead would be a fourth thing given up (Section 6.2).
- **This end state executes after the decision,** and Section 6.3's second case is the price.

### 6.2 What the single domain gives up

- **The fast path without the leader.** Today a collector proposes to its fast set, which includes the leader, and decides in one round trip to that set. In the leader log every command goes collector → leader → a majority → collector.
  - On a LAN, and under load where the relay (task-d100) already sends everything through the leader, this changes nothing.
  - On a WAN, a collector far from the leader pays the leg to the leader and then the leader's round trip, where the fast path pays its round trip to the fast set. That is the latency the design chose SwiftPaxos for ([leader locality remains relevant](README.md#performance-interpretation-leader-locality-remains-relevant)).
- **Independent execution at every voter,** and so detecting a divergence by a majority of executions.
- **Contended commands decided in rounds of their own:** they take the leader's order.
- **If the leader executed ahead of the decision** (Section 6.1), execution thrown away at each leader change, and a speculative overlay held at every leader. One more thing would need undoing (review on #260): a turn's write sets go in the same frame as its commands, and the frame is a journal record, so the write set of a command not yet decided is in the leader's journal and in every follower's that took the frame. A replay or a landing would then take a frame's write set only once its command is decided at that position. The end state above doesn't execute ahead, so it doesn't give this up.

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
- **An election takes longer** by executing that tail. Its bound is the decided-not-landed window: the command table, at most 1,000 commands (`MAX_TABLE_CAPACITY`). Since a write set follows its decision by one frame, in practice it is a frame or two.
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

Both are parked (Section 8). Section 6.7 redesigns lever 2 to need neither.

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
  - **Against etcd it isn't like for like** (review on #252): each of etcd's five members holds the whole store. If the bar is etcd's five, the owner says whether three copies of five may be read against it.
- **Who can lead:** only an executor can lead without first installing an image and replaying. While witnesses are fewer than a majority, every majority holds an executor, so the domain stays live through any two failures. With two executors down, the last one must lead, and nothing can take over from it without a rebuild.
- **Reads:** reads and retained answers are served only where a projection is. A witness's frontend forwards them, or witnesses run no frontend.
- **Catch-up donors:** only executors can serve executed history (task-d08). A witness behind catches up its journal only.
- **Detection by every voter:** the floor's root and the release checks (Section 6.3) compare three executions, not five.
- **Even load:** executors carry more CPU than witnesses. On identical hosts, capacity is set by an executor's host, which the servers' sum doesn't show.

**What it depends on:**
- **Proposal 2.** Until protocol state is in the journal only, a witness would still keep a store for its protocol rows and would save only execution and the application rows.
- **The forgetting floor and checkpoints follow the executors.** A witness trims its journal at the floor the executors' images cover.

**Not built, and parked** (Section 8). It needs a voter role in the configuration, an election rule that prefers executors, and read, frontend and catch-up rules for witnesses. It is a decision for the owner beside option 3, not a step toward it.

### 6.6 Voters that don't execute or materialize on the hot path

**Why this section.** The owner's answers on #252 (relayed by the lead on #185): "Not yet convinced the leader log is needed if batching is done correctly. Ditto [witnesses]. OTOH, as long as any of the journal-only voters can recover as a leader, it's OK. I'm willing to trade (reasonable) recovery time for maximum throughput."

So this section asks what a voter has to do on the hot path, and what it can do later or not at all, under one condition: any such voter can take over as leader within a stated bound.

**The shape: lazy voters.**
- **On the hot path, a lazy voter does what the protocol needs.** It journals, votes, computes dependencies and learns decisions, as every voter does today. The fast path, any voter's right to propose, and recovery are unchanged.
- **It neither executes nor materializes on the hot path.** Its projection follows the decided order behind, a batch at a time, at a bounded lag.
- **Eager voters** execute on the hot path, as today: the leader and `e` followers.
- **Roles follow the ballot and the leader's word.** The leader is eager. It names the `e` eager followers in its frames, the next voters in the ballot's order by default, and names another when one stops answering (point 3). The rest are lazy. A voter that becomes eager first executes its tail (below).

**1. What a lazy voter must hold.**
- **To vote and detect conflicts: keys, not state.**
  - A dependency is a conflict on a key. The voter needs each live command's key set (from its payload), each key's latest command, the command table and the journal: what the machine holds today.
  - Intake reads nothing from the store (Section 2's audit), so no vote needs a projection.
  - A conditional write's outcome depends on state, but its conflicts don't: it conflicts on its keys whatever it finds there.
- **To serve its frontend:**
  - **Writes** are answered from the release the leader sends (task-d100's relay), as today. The lazy voter's own execution isn't on that path.
  - **Retries** go through consensus as today. A retry's dedup happens at execution (`retry::admit`), so exactly-once state lives in the eager voters' retry windows. The lazy voter's window lags, and nothing reads it above its landed frontier.
  - **Reads, watches and resolves** need current state. A lazy voter's frontend forwards them to an eager voter. That costs a hop and, on the 7763, about +0.003 ms an operation a lazy frontend (the bar's 34% reads, a fifth of them at that frontend, at about 20 µs of tokio a forwarded read). Total read work doesn't change: each read is still answered once.
  - **Authorization reads this node's own store** (#259's review). `StorePolicySource::barrier` reads the session, the policy and the grants when the frontend admits a request and again on a result's way out, and `retained` reads the retry row when a request arrives. At a lazy voter that state is as old as its last landed batch.
    - **So a batch ends by time as well as by positions:** at a quarter of the table or 50 ms, whichever comes first. Today the bound is the materializer's lag.
    - **Ending batches by time doesn't bound the age by itself; a check before authorizing does** (Codex on #259, twice). A landing can stall while the frontend still answers, and so can the decisions reaching this voter. So freshness is progress the leader confirmed, not an empty backlog:
      - **The leader's beacon.** The leader sends each lazy voter its commit frontier at least every 50 ms, idle or not: the frontier it announces at a turn's end under load, and a beacon of tens of bytes when idle.
      - **Only while it holds a quorum, counted from its own sends** (Codex on #263). A leader sends a beacon only while a quorum has acknowledged, at its ballot, a message it sent within the last 50 ms by its own clock. Each acknowledgement names the round it answers, and its freshness runs from when the leader sent that round, not from when the acknowledgement arrives. So an acknowledgement delayed in the network counts for no longer than a prompt one.
      - **Both quorums are majorities of the voters, the leader counting itself:** the one whose acknowledgements keep the beacon going, and the new leader's promise quorum. The bound below needs them to overlap. Of five voters, the leader and any two followers keep it going, and a new ballot needs three promises (#263's review).
      - **What a quorum acknowledges when idle** (#263's review). Under load, the leader's frames are the rounds, and each follower's vote names the frame it answers. When idle, the leader sends a round of tens of bytes to every follower, eager and lazy, and each acknowledges it at the leader's ballot, naming it, unless it has promised a higher ballot. So an idle leader with any two followers answering keeps its quorum.
        - Without that acknowledgement, an idle leader would stop beaconing 50 ms after its last acknowledged frame. Every lazy frontend would then fail closed: safe, but a hop for every request after any idle gap.
        - The acknowledgement is tens of bytes from each follower every 50 ms, about 80 a second at the leader of five voters.
      - **The round goes every 50 ms while it leads; the beacon rides it only while the quorum holds** (#266's review). The leader sends a round at least every 50 ms for as long as it holds its ballot, whether or not its quorum holds. It puts its frontier on a round, as the beacon, only if a quorum has acknowledged, at its ballot, a round it sent within the last 50 ms.
        - If the round went only while the quorum held, one lapse would end it until a write: a leader paused past 50 ms while idle would send nothing, so nothing would be acknowledged and nothing would restore its quorum, and every lazy frontend would fail closed until the next frame.
        - With the round kept going, the first round after the lapse goes without the frontier, a quorum acknowledges it, and the round after it carries the beacon again: within 50 ms and a round trip of the lapse's end, with no write.
        - A round without the frontier makes no frontend fresh. So the bound below counts only rounds that carry it, and a deposed leader's rounds never regain a quorum: the voters that promised the new ballot don't acknowledge them.
      - A voter that has promised a higher ballot no longer acknowledges the old one. So a deposed leader stops within 50 ms of sending the last round a quorum acknowledged at its ballot, wherever it is and however late those acknowledgements arrive.
      - **The age:** the time since this voter received the latest beacon, at the current ballot, whose frontier it has landed through. Decisions or landings that stall, alone or together, both make it grow.
      - **Under 100 ms**, the frontend authorizes from its own store. **Over it, it fails closed:** admission and the result's authorization go to an eager voter (the stricter way, below). If none answers, the request is deferred, as the disclosure gate defers today.
      - **The bound,** for a revocation committed at time `t`:
        - a beacon from the leader that committed it holds a frontier behind it only if sent before `t`, so it keeps a lazy frontend fresh until at most `t` + 100 ms plus a message delay;
        - a deposed leader's quorum overlaps the new one in a voter whose last acknowledgement to the old leader came before its promise to the new ballot, so before `t`. The round that acknowledgement answers was sent earlier still. The old leader's last beacon therefore leaves before `t` + 50 ms, however late that acknowledgement reaches it, and keeps a frontend fresh until at most `t` + 150 ms plus a message delay;
        - so the revoked grant can authorize at a lazy frontend for at most 150 ms plus one message delay after `t`, across a leader change too, at any traffic.
      - It assumes clock rates agree within a few percent over 150 ms, as leader leases do, not that clocks agree. The stricter way assumes nothing about clocks.
    - **A session opened a moment ago** isn't found there until its batch lands. The frontend's disclosure gate defers rather than refuses, as it does at a lagging replica today.
    - **A grant revoked a moment ago** still authorizes a result on its way out at a lazy frontend, for up to that bound: 150 ms plus a message delay. That is the owner's to accept.
    - **The stricter way** sends admission and the result's authorization to an eager voter: a hop for every request at a lazy frontend, not only for reads.
    - **At the bar's load the time bound ends batches at about 120 positions,** not 250. The merged current rows then save a little less: under +0.002 ms an operation for three lazy voters.
- **To be a catch-up donor:**
  - the protocol part (order, payloads, decided records) from its journal and task-d117's segments, to any asker;
  - executed history, with each position's result, up to its landed frontier. Above that, an eager voter serves.

**2. Where its state comes from: three ways.**
- **(a) Its own execution, behind, in large batches.**
  - **No trust:** it executes every command itself, as today, only later.
  - **It saves almost nothing in CPU.** Execution stays per command. What a batch saves is the per-turn and per-commit costs (about 3 µs a command locally), and current rows that a batch writes twice (below).
  - So it is the fallback, not the plan.
- **(b) The eager voters' batches, agreed, landed behind.**
  - At each batch's end (point 3 sizes the batch), each eager voter sends the lazy voters two digests through that position:
    - **its history digest** (task-d117, point 3): each position's command, admission digest, revision and result digest. The result digest is over the result's bytes as the retry window keeps them, so a retry answered from a landed record gets the bytes two executions agreed on (#266's review);
    - **its write-set digest:** a digest of the batch's merged write set in a canonical order, by collection and key, each key with its last value or its tombstone (#259's review).
  - The eager follower ships each lazy voter the batch: the merged write set and each position's execution record.
  - **A lazy voter lands a batch only once two executions agree on both digests, and only bytes whose write-set digest two voters sent.** It lands it as one commit, sorted.
  - **It journals only records whose history digest two voters sent** (#263's review). Before the append, it computes the history digest over the shipped execution records, through the batch's last position, and compares it with the two eager voters' history digests. The write-set digest checks the rows; this checks the records the retry window and executed history are rebuilt from.
  - It executes the batch itself, as in (a), when:
    - no agreement comes within a bound (for example, too few eager voters are up);
    - the digests differ, in which case its execution counts toward the majority that decides;
    - or the shipped bytes don't match the agreed write-set digest;
    - or the shipped execution records don't match the agreed history digest.
  - **Why the write set needs its own digest.** The history digest covers results, not rows, and the bytes a lazy voter lands are built and shipped by the eager follower alone. Without the second digest, a write set that is wrong under results that are right would be landed by every lazy voter: a fault in the merge, or state at the eager follower that has diverged and hasn't shown in a result yet. In a later dispute, each lazy voter would execute from those rows, find the eager follower's result, and outvote the correct leader, which would stop.
  - **Why the records need the same check** (#263's review). The records are journaled at the landing, and boot and a takeover rebuild the retry window from them. A wrong result shipped beside a right write set, journaled as received, would be retained: after a takeover this voter would answer that command's retries with it. Checked against the agreed history digest, every record a lazy voter journals is one two executions agreed on.
  - **With both checks, two executions are enough** for one faulty execution. What a lazy voter holds through batch N−1 is what two executions agreed on, byte for byte. Its own execution of a disputed batch N is a third, independent one, from agreed state.
  - **Trust:** the agreement of two independent executions of the same order and payloads, on results and on rows, with the lazy voters as tie-breakers. The leader's write set alone isn't trusted. That is the difference from option 3, where followers land the leader's write sets on the hot path (Section 6.3).
  - **The leader's share.** It merges and hashes the batch's write set too, about 1 KB a command: about +1.5 µs a command at the leader and +0.5 at the eager follower, which merges the batch already (point 5).
- **(c) The eager voters' full images, installed every interval.**
  - It costs the application state's size every interval: about 3.5 MB of current rows for the bench's 10,000 keys, and all the event rows retained, which grow until task-d114 collects them.
  - It suits a small state with no history. Otherwise it is (b) with more bytes, so it isn't taken.

**Recommended: (b), with (a) as its fallback.**

**3. How a lazy voter comes to lead.**
- **An election prefers eager voters,** as Section 6.5's witnesses needed: a lazy voter delays its campaign by one election timeout. So a lazy voter leads only when every eager voter is down or cut off.
- **At takeover it finishes the protocol's recovery (the Sync) as today, then executes its tail before it answers:**
  - it lands any agreed batch it holds;
  - then it executes, from its landed frontier to the decided frontier, from the payloads in its journal.
  - New commands can be ordered meanwhile. Their answers wait for the tail.
- **The tail is bounded by positions, not by time.** A batch is a quarter of the command table: 250 positions at the default table of 1,000 (`limits.command_table_capacity`). A lazy voter that keeps landing has a tail of at most one batch plus what is in flight. At the bar's load that is a batch about every 0.1 s; at a lower load it is a longer time but no more commands.
- **The batch is sized by the command table, not by the floor** (Codex on #259).
  - **Why.** A lazy voter executes a command only when it lands it, and the table retires only executed records (`CommandTable::reclaim`). So every position a lazy voter hasn't landed stays in its table.
  - **The limit.** Admission stops at seven eighths of the table, 875 at the default; the rest is the recovery reserve (`RECOVERY_RESERVE_PARTS`).
  - **What goes wrong at the floor's 4,096 positions.** A batch that large would fill the table four times over before its first landing. The lazy voter would then refuse new commands, and at `e = 1` ordering needs a lazy voter's vote.
  - **With the window below at half the table, 500 positions,** a lazy voter's table holds at most 500 positions it hasn't landed, plus what is undecided in flight: under 875.
  - **Landing counts as execution for the table.** It marks a batch's positions executed. They retire once the landing's commit is durable, as an execution group's do today (task-d90, point 2).
  - **What makes a landing durable** (#259's review). Today an execution is durable once the journal holds its outcome (`executions_durable_through`), and under `replay` the projection is synced on a cadence, not at each commit. If a landing's positions retired only at a projection sync, the table would fill between syncs: 875 positions is about 0.4 s at the bar's load. So:
    - **each landing first journals the batch's execution records,** once they match the agreed history digest (point 2 (b)): one append a batch, about ten to twenty a second at the bar's load. The positions retire once that append is durable, as an execution group's do;
    - **the write set goes to the projection in one commit,** synced on the profile's cadence, as today's commits are;
    - **why the journal** (Codex on #259). Proposal 2 takes the retry window and executed history out of the projection: an eager voter rebuilds them at boot from its image and the execution records in its journal. A lazy voter whose landings weren't journaled would boot without the retry entries of every batch landed since its image, and after a takeover would execute a retried command again. With the execution records journaled, it rebuilds the retry window, the executed history and the history digest the same way, through its journaled execution frontier;
    - **after a crash** the voter boots from its image and journal (task-d117). Its projection's stamp gives the landed frontier. Positions journaled above it are executed again from the journal's payloads, or landed again from a donor's batch, and each outcome must match its journaled record. No position is skipped;
    - **its cost** is the appender's per-byte work on records the eager follower already ships: inferred under +0.002 ms an operation for three lazy voters, inside the rounding of Section 7's lazy rows.
  - **The table, its reserve and the recovery reports are unchanged.** A smaller configured table gives smaller batches, so a domain with lazy voters keeps the default.
  - **Batches don't need the floor's boundaries.** The history digest (task-d117, point 3) runs over every executed position, so two executions can be compared at any position.
    - Each eager voter sends its digest through each batch's last position: a position and 32 bytes, in a small peer frame to the lazy voters.
    - That is about ten a second to each lazy voter at the bar's load.
    - The floor's root still carries the digest at its own boundaries.
- **The window is enforced, not assumed** (Codex on #259). Batching alone bounds the tail only while the lazy voter keeps landing. If its batches stall (their delivery, its materializer, or its host), the eager voters pass batches without it. So:
  - **The window:** a lazy voter's lag is its decided frontier minus its landed frontier, in positions. The window is two batches, 500 positions: one being landed and one being agreed. It is half the table, so a lazy voter within it keeps voting (above).
  - **Past the window, it executes for itself.** It stops waiting for agreement and executes from its landed frontier, as in (a), until it is back within one batch. Its own execution retires records as it goes, so its table drains with its lag. Its own execution runs at the tail's rate, about 14,000 commands a second on the 7763 (inferred, point 5), against about 2,400 a second at the bar's load, so it gains on the domain. A host that can't keep that pace shows it on the metrics line as a lag that keeps growing. Once its table fills, it refuses new commands, as any voter that falls behind does today, and the domain orders without it while a quorum remains.
  - **Past the window, it doesn't campaign first.** Its campaign waits a second election timeout, so a voter within the window takes over before it. If no voter within the window is up, it campaigns after that wait anyway, so the domain stays live.
  - **The domain never waits for a lazy voter.** Ordering is unchanged, so a stalled lazy voter costs only its own catch-up.
  - **So the bound:** while any voter within the window is up, a takeover's tail is at most the window, 500 positions, about 0.04 s on the 7763, and at most one batch, about 0.02 s, in steady state. With every voter within it down, the voter that takes over pays its own lag at the tail's rate, and its lag was on the metrics line beforehand.
- **The new eager follower** (the next voter in the new ballot's order) executes its own tail in the background. Until it catches up, the lazy voters find no agreement and fall back to (a), so their lag stays bounded.
- **It ends as an eager voter.** From the takeover on, it executes on the hot path.
- **In time, the larger part is the election, not the tail.**
  - Losing the leader alone has no tail: the eager follower is eager, and an election prefers it.
  - Losing both eager voters together costs one more election timeout (a lazy voter's campaign waits), then the tail's 0.02 to 0.04 s.
- **The tail's rate assumes execution from memory that is warm.** A landing updates proposal 1's in-memory state as it commits: the merged write set is laid over the voter's map in the same step. So a lazy voter's tail runs from warm memory. Without proposal 1, at today's price of execution, the tail is still under 0.1 s.
- **The eager follower down, with no change of ballot.**
  - If roles followed the ballot alone, the role wouldn't move: no agreement would come, every lazy voter would execute for itself, and the whole saving would be gone until that voter returned or the ballot changed.
  - **So the leader names the eager follower.** The role is in the leader's frames, not in the ballot. When the named voter's digests stop arriving for an agreement bound, the leader names the next voter in the ballot's order. That voter executes its tail first, as a new eager follower does after a takeover. Meanwhile the lazy voters fall back to (a).
  - So one follower down costs the saving only for its tail's time, not until it returns.

**4. How many eager voters, and what that buys for divergence.**
- **Today** every voter executes. A divergence shows at a held release (task-d06), a late release (task-d12), catch-up (task-d08) and the floor's root (task-d27).
- **`e = 2`, three executions a command.** A batch whose digests differ is decided two against one at once, and the loser stops (task-d36's majority rule). Lazy voters never land a minority's state.
- **`e = 1`, two executions a command.** A difference is detected at the batch's end, not decided. Each lazy voter then executes the disputed batch itself, and the majority of the executions decides. Steady state pays for two executions, and a fault pays for five, once.
- **`e = 0`** leaves one execution. Two can't agree, so every lazy voter falls back to (a), and nothing is saved.
- **Detection comes earlier than the floor's today:** at most one batch, 250 positions, against the floor's 4,096. What a lazy voter adds is a tie-break, not a check of its own on the hot path.
- **Not caught by more executions:** a fault common to every build, such as a deterministic bug. That holds today too.

**Recommended: `e = 1`,** with the lazy voters' execution as the tie-break.

**5. The trade.**

**The terms:**
- **Baseline:** Section 7's costs after every step that needs no decision: the "Lever 3" row, 1.20 ms an operation on the 7763.
- **No double counting with proposals 1 and 2:** a follower's execution is the 34.5 µs a command left after proposal 1, and a voter's materializer the 17.4 left after proposal 2.
- **Converted per part:** the followers' loops ×1.61, materializers ×1.37, tokio ×1.99, at 0.66 commands an operation.

**What a lazy voter changes:**

| | (a) own execution behind | (b) agreed batches landed |
|---|---|---|
| Followers' loop | −1.8 µs (per-turn execution cost amortized) | −34.5 µs (no execution) |
| Materializer | −1.3 µs (one commit a batch), and the merged current rows | the merged current rows, +2 µs to decode the batch |
| Tokio | 0 | +2 µs (the batch's bytes) |
| The eager follower's tokio: the sender (#259's review) | 0 | +2 µs (about 1 KB a command shipped to each lazy voter) |
| Forwarded reads (7763) | +0.003 ms an operation | +0.003 ms an operation |
| **Net, 7763, a lazy voter** | **−0.004 to −0.005 ms an operation** | **−0.030 to −0.032 ms an operation** |
| The write-set check, once for the domain | 0 | +1.5 µs at the leader, +0.5 at the eager follower: +0.002 ms an operation |

**The merged current rows.**
- In the bench, half the commands are puts over 10,000 keys and half are contended writes on 16 hot keys. A batch writes each key's current row once.
- So current rows a command fall from 1 to about 0.48 at a batch of 2,400 commands, 0.42 at 7,200 and 0.29 at 24,000.
- At about 8 µs a row in the materializer locally, that is 4.3 to 5.9 µs a command a lazy voter.

**The curve.** On the 7763 at 120 callers, with `R` the commands a second:
- `R` is taken as 2,400, the local rate; the 7763 runs fewer.
- The tail executes at about 14,000 commands a second. That is 55.5 µs of execution and 16 of merged materialization a command on the 7763, inferred.

| Lag or batch interval | Batch, commands | Recovery at takeover: the tail | (a), 4 lazy voters | (b), `e = 2` (2 lazy) | (b), `e = 1` (3 lazy) |
|---|---:|---:|---:|---:|---:|
| 1 s | 2,400 | 0.17 s | 1.19 | 1.15 | 1.12 |
| 3 s | 7,200 | 0.51 s | 1.19 | 1.15 | 1.11 |
| 10 s | 24,000 | 1.7 s | 1.18 | 1.14 | 1.11 |
| 250 positions or 50 ms, whichever first (recommended) | 250 | 0.02 s; 0.04 s at the window's edge | 1.19 | 1.15 | 1.12 |

The last three columns are the servers' ms an operation, from 1.20. The two right-hand columns count the sender and the write-set check. The rows by time need a batch larger than a quarter of today's largest table. Such a table changes the recovery reports' bound (task-d20), so those rows only show how the CPU moves with the interval.

**What the curve says:**
- **The CPU is nearly flat in the interval.** A longer batch merges more current rows, but from 250 positions to 24,000 the difference is under 0.01 ms an operation.
- **Recovery is linear in it.** So the interval should be short.
- **Batches by positions** bound the tail at any load, and the command table bounds the batch from above: a quarter of it.
- **The choice that moves the CPU is `e`:** −0.01 for (a), −0.06 at `e = 2` and −0.09 at `e = 1`, the sender and the write-set check counted.
- **It adds to proposals 1 and 2, the appender's encode and the levers that need no decision.** With lever 2's redesign (Section 6.7) it overlaps in `Follower::applied`, which both take part of: about 5 µs a lazy voter, counted once.
- **Every figure is inferred** from the cost table's shares, not from a pair.

**Against the alternatives.**
- **Option 3 with rotation (Section 7's lever 1):** −0.03 on the 7763 with `Follower::applied` counted once, with one execution a command and rotation for detection. (b) at `e = 1` saves more, −0.09, keeps two executions and the lazy voters' tie-break, keeps the fast path, and needs no leader log.
- **Witnesses (Section 6.5):** they can't lead without a rebuild. A lazy voter can, within the tail's bound, which is the owner's condition.
- **Copies of the store:** a lazy voter keeps a full one, behind by at most a batch. So the domain keeps five copies, as etcd's five members do; with witnesses it keeps three.

**6. What it costs to build.** About 5 to 7 weeks, after proposals 1 and 2 (task-d117), which it rests on: history digests and segments, and execution from memory.
- **Roles from the ballot,** and an election that prefers eager voters.
- **The batch:** a quarter of the table's positions or 50 ms, as a peer frame with the merged write set and the execution records. The eager follower sends it. The leader's load changes only by merging and hashing the batch's write set.
- **The agreement:** each eager voter's history digest and write-set digest through each batch's last position, in a small peer frame to the lazy voters.
- **The landing:** the batch's execution records, checked against the agreed history digest and journaled in one append, then its write set lowered as one sorted commit, of bytes whose write-set digest two voters sent, laid over the in-memory state in the same step. Boot rebuilds the retry window and executed history from the journaled records, as at an eager voter.
- **The eager follower named by the leader,** and named again when it stops answering.
- **The fallbacks:** own execution when there is no agreement within a bound, on a difference, and past the window.
- **The window:** the lag tracked, and the campaign's second timeout past it.
- **The takeover:** execute the tail before answering.
- **The frontend:** forwarding reads, watches and resolves to an eager voter, and the age check before authorizing from its own store.
- **The leader's beacon:** a round to every follower at least every 50 ms while it leads, acknowledged by each and named in each acknowledgement; when idle, a round of tens of bytes. The round carries the commit frontier, as the beacon, only while a majority, the leader counting itself, has acknowledged a round the leader sent within the last 50 ms by its own clock.
- **The donor rule:** a lazy voter serves executed history only up to its landed frontier.
- **The metrics line:** lag in positions, tail and takeover times, agreements, fallbacks.

**It changes who executes, so it is the owner's to decide,** beside the parked leader log, option 3 and witnesses. It doesn't change what is decided: votes, dependencies, the fast path and recovery are as today. **What the owner trades** (#259's review):
- **Two executions check each other, where five do today.** With the write-set digest, they check rows as well as results.
- **A lazy frontend's authorization is as old as the leader's last beacon it has landed through,** checked before each authorization (point 1): a revoked grant can still authorize a result there for up to 150 ms plus a message delay, across a leader change too. Past it, the frontend asks an eager voter or defers. The bound assumes clock rates agree within a few percent, as leader leases do.
- **One follower down costs the saving for its tail's time,** while the leader names another.
- **Half of a lazy voter's table is the window.** At the window's edge it has room for 375 commands in flight, and at `e = 1` ordering needs one lazy voter's vote. The bar's 120 callers put about 80 there.

**7. The fault runs it needs** (testing's, on the runner):
- **A lazy voter wins an election and serves within the bound:**
  - kill the leader and the eager follower together under load;
  - read the time from the kill to the first answer: the election, the Sync and the tail. The tail should be at most about 0.02 s at 250 positions.
- **A restarted lazy voter:** it boots from its image and journal (task-d117), resumes landing, and is caught up by donors. Killed just after a landing, it then takes over: a retry of a command from that landing is answered from its retained result, not executed again.
- **Two in a row:**
  - kill the eager voters, then the lazy voter that took over once it serves;
  - a second lazy voter takes over within the same bound;
  - the digests agree at the next batch's end.
- **A difference at one eager voter,** in a build that changes one result: the lazy voters execute the batch, the majority decides, and the loser stops.
- **A wrong write set under right results** at the eager follower, in a build that changes one shipped row: the lazy voters refuse to land it, execute the batch themselves, and the leader isn't stopped.
- **A wrong result under a right write set** at the eager follower, in a build that changes one shipped result (#263's review): the lazy voters refuse the batch before journaling it and execute it themselves, the leader isn't stopped, and a retry of that command after a lazy voter takes over gets the right answer.
- **An idle leader keeps its beacon:** with no load for several seconds, the lazy voters' beacon age stays under 100 ms, and no lazy frontend fails closed.
- **An idle leader gets its beacon back after a pause** (#266's review): with no load, the leader paused for 200 ms (a build that holds its loop). The lazy voters' beacon age is back under 100 ms within 50 ms and a round trip of the pause's end, with no write, and no beacon goes out while its quorum is stale.
- **The eager follower stopped without a change of ballot:** the leader names another, and agreement resumes after that voter's tail.
- **A revoked grant at a lazy frontend:** the time from the revocation's commit to the first refusal there is under 150 ms plus a message delay. It stays under that with that voter's landing stalled (a build that holds its landings), with its decisions and batches stalled together, with the old leader and that voter cut off from the rest while a new leader commits the revocation, and with the old leader's acknowledgements at its ballot delayed until after the new leader commits it (Codex on #263). The frontend fails closed to an eager voter, or defers.
- **The lag under load:** the lazy voters' landed frontier stays within one batch while the leader and eager follower run at the bar's load, and their tables stay under the admission limit, with no initialization refused for room.
- **A stalled lazy voter:**
  - hold one lazy voter's batches back under load until it passes the window;
  - it executes for itself and comes back within one batch;
  - kill the eager voters while it is past the window: a lazy voter within the window takes over, within the window's bound.

### 6.7 Lever 2 redesigned: a frame taken as a unit, without a protocol change

**Why this section.** The owner isn't yet convinced the leader log is needed "if batching is done correctly" (Section 8). Lever 2 as Section 7 sized it assumed a single proposer, and is parked. This section redesigns it under four conditions:
- any voter may still propose;
- the fast path stays;
- a follower's work is batched by the frame it already receives (task-d102's `ProposalFrame`);
- each command's vote still reaches whoever counts it.

**What a follower does today.** This is read from the code at `b614c162`.
- A follower does almost everything once a command, from intake to forgetting.
- Its only once-a-turn work is at the daemon: one journal append for the turn's batches, and execution groups of up to 32 (`GROUP_COMMANDS`).
- The leader already has once-a-turn hooks (`cut_frames`, the frontier announced at a turn's end). The follower has none:
  - **`on_proposal_frame`** counts the frame, then runs the whole of `on_proposal` or `on_relayed_proposal` for each entry. Each of those ends in `advance_pending` and `learn`.
  - **The daemon decodes each peer frame twice**: once in the voter and again in the follower.

**What the protocol needs per command, against what we do.**

| Stage | What the protocol needs | What the code does per command | Under this lever |
| --- | --- | --- | --- |
| Taking the leader's proposals in | The sender is the ballot's leader and the ballot is current: once a message. Per command: the admission matches, and the guard holds (direct dependencies at ACCEPT or COMMIT before ACCEPT, [design Section 4.7](tuplesky-design.md#s4-7)). | The seal, fence and sender checks again for each entry. `held`, `held_check`, a `VoteSet` built from a clone of the proposal, `note_lacking` (four indexes), and `advance_pending` and `learn` after each entry. | The frame's checks once. An entry whose payload is in hand and whose guard holds is adopted in the frame's one pass, without `held` or a `VoteSet`. The rest go the held way as today. `advance_pending` and `learn` run once, at the frame's end. |
| The path logs ([design Section 4.1](tuplesky-design.md#s4-1)) | The leader's order enters a replica's logs at adoption, key by key. An equal path means an equal history. | `PathLog::sync`, then `recompute` for each key of each command, hashing the remaining suffix each time. | A frame's synchronizations applied key by key in sequence order, with each key's suffix recomputed once, at the frame's end. |
| The adoption's durability | A vote leaves only once what it claims is durable ([design Section 4.8](tuplesky-design.md#s4-8)). Each command's dependencies are its own. | One `PersistBatch`, one barrier, one `pending` entry and one deferred vote per command. `payload_row` scans the in-flight batches twice. `carry_unwritten` walks the dependencies, and `ledger.written` scans the staged batches. | One batch a frame, holding each adopted command's dependency row and payload row. One barrier and one `pending` entry name the frame's adoptions; the frame's vote also waits on every barrier still pending, as today. `ledger.stage` already takes several records a barrier. The payload rows in flight are looked up through an index, not scanned. |
| The vote out | Each command's slow vote reaches the leader. An unmarked command's also reaches its frontend. A `SlowAck` is `{replica, ballot, command, admission}`. | One encode and one `PendingSend` per command. Its `requires` lists every pending barrier, so the k-th entry of a frame waits on at least k barriers. | One vote message a frame to the leader, carrying each command's vote as a range of the leader's sequence numbers, with the exceptions listed (below). |
| Learning | A follower commits what it adopted up to the leader's commit frontier (`Committed { through }`, task-d09). That is already a range. | `learn` after every entry and every storage event. Each call allocates the ACCEPT set twice (`commit_learned`, `commit_through_leader`), so a frame of N commands costs at least 2N passes. | `learn` once a frame and once a step of storage events. The commit frontier is applied through an index of durable adoptions by sequence number, with no allocation of the ACCEPT set. |
| Storage events | Durability, as above. | One machine step per `JournalDurable` event. The `served_payloads` check scans the other in-flight barriers. | One barrier a frame, so one event a frame, and an index for the check. |
| `Follower::applied` | Execution in the decided order, a command's dependencies executed first. | `Learner::established` per command: a clone of the dependencies, a closure walk that allocates, `table.execute`. Then `EstablishedResult::establish`, `learn` and `forget_history`. | Execution's order bookkeeping stays per command (`table.execute`). The closure walk, `learn` and forgetting run once an execution group. |
| Forgetting | Nothing. It is memory, not protocol. A command is forgotten only once its execution is durable (task-d90, point 2). | Up to four commands a call (`FORGET_PER_EXECUTION`), each taken out of about ten maps. | By range, at an execution group's end, up to the durable execution frontier. The rule that a command waits for its durable execution is kept. |
| Settle and flush | — | Each event stepped one at a time. | Fewer events: one a frame. |

**The vote message.**
- **Its content:** the follower's replica, the ballot, the first and last sequence numbers of the leader's proposals it is voting on, and the sequence numbers in that range it isn't voting on yet. A command that is held for its payload, refused, or being rebound is an exception, and its vote goes later, alone or in a later frame's range.
- **The leader expands it into the votes it counts today.** For each sequence number in the range and not an exception, it adds `SlowAck { replica, ballot, command, admission }` to that command's `VoteSet`. The command and admission come from its own proposal under that number.
- **Why that is the same vote:**
  - A follower adopts a proposal only under the proposal's admission. It refuses a mismatch, or holds the proposal and rebinds it to the leader's payload first.
  - So the admission the leader fills in is the one the `SlowAck` would have carried. The ballot is the frame's.
  - A `SlowAck` carries nothing else: no dependencies, sequence number or path. The adoption row behind it is a function of the proposal too.
- **Its prerequisites:** the frame's one barrier, its payload rows, and every barrier still pending when it is published, from earlier frames too. If any of them fails, the vote message isn't sent.
  - **Why the earlier frames' barriers** (Codex on #261). A command in this frame can depend on an adoption from an earlier frame whose batch is still out. `carry_unwritten` doesn't copy a dependency row that `ledger.written` already counts as staged, so this frame's batch needn't hold it.
  - `Follower::publish_to_leader_and_frontend` waits on every pending barrier for this reason today. The vote message keeps that rule.
  - So the votes it carries are durable before it leaves, as each `SlowAck` is today. What changes is the count: a frame waits on its own barrier and those still out, where the k-th entry of a frame waits on at least k barriers today.
- **The frontend.** A relayed command is marked, and its acknowledgement goes to the leader only (task-d100), so at load the vote message is the follower's only acknowledgement. An unmarked command's acknowledgement to its frontend stays per command, as today. That frontend doesn't hold the leader's numbering to read a range by.
- **Fast acknowledgements stay per command in content.** A follower's `FastAck` is its own pre-acceptance of a submission it took, sent before the leader's proposal arrives. Its dependencies and paths are the follower's own, and can differ from the leader's. They can share a hand-off a turn (lever 8), but they can't become a range.
- **Re-acknowledgement** (task-d07). A leader re-sends a proposal it lacks a vote for, and the follower answers with a range as well: the commands it holds adopted in that range.
- **On the wire it is a new message,** sent only while a replicated feature is active (task-d99), as `ProposalFrame` is behind `BatchedRound`.

**Taking a frame's path synchronizations at once: why it's the same.**
- A frame's entries are taken in one step, and no acknowledgement leaves between two of them. So no head between two adoptions is ever observed.
- What is observed is the logs' state at the frame's end.
- Each synchronization's set-aside and position change are applied as today, in sequence order. Only the suffix's digest is deferred. It is a function of the synchronized digest and the suffix, so it is recomputed once a key at the frame's end instead of after each. The position, the suffix and the digest come out the same.
- An early synchronization held for an append ([design Section 4.1](tuplesky-design.md#s4-1)) is applied at that append, as today.
- **The code step's test:** for random frames, suffixes and early synchronizations, a frame taken as a unit leaves every path log equal to taking its entries one at a time.

**What would need a protocol change, and isn't proposed:**
- **The frame as the unit of decision,** all of it or none. That is the leader log (Section 6.1).
- **Recording a frame's order in place of each command's dependencies.** Recovery reads each command's dependency record ([design Section 4.9](tuplesky-design.md#s4-9)), so its reports would change.
- **Folding a follower's fast acknowledgement into the leader's proposal.** A fast acknowledgement comes before the proposal by design. That is the fast path.
- **Ending an unmarked command's acknowledgement to its frontend.** The frontend's learning of the fast path needs them.

So nothing in this lever changes what is voted, what a quorum is, what is durable before a vote leaves, or what recovery reads.

**Its size** (inferred from shares, at `k` = 9.55). A follower's loop is about 145 µs a command. By review's profile on #252, its shares are:

| Share of a follower's loop | Profile | Under this lever, a follower |
| --- | --- | --- |
| Taking each proposal in: accept state | 8.0 to 8.4% (about 12 µs) | half to two thirds: −6 to −8 µs |
| Taking each proposal in: the payload | 6.1 to 7.0% (about 10 µs) | stays: its decode and hashes are per command |
| Settle: batches, barriers, events, votes out | 10.0% (about 14.5 µs) | a third to a half: −5 to −7 µs |
| `Follower::applied` | 7.1 to 7.5% (about 11 µs) | forgetting by range and `learn` once a group: −4 to −6 µs |
| Flush: the lowering | 7.3% (about 10.6 µs) | stays: rows are per command, and proposal 2 removes most of them |

- **Per follower:** −15 to −21 µs a command, plus about 4 µs a frame.
- **The leader's acknowledgements in and tokio's bytes** were counted in lever 4's row, as its acknowledgement by range. They are not counted again here.
- **Section 7 takes −18 a follower:** followers' loops 439 + 388/k become 367 + 404/k. That is −0.075 ms an operation on the 7763: 1.20 becomes 1.13.
- **Every figure here is inferred,** and each step's acceptance reads its own pair.

**With lazy voters (Section 6.6).**
- The two overlap only in `Follower::applied`, about 5 µs a lazy voter here, counted once.
- At `e = 1` together they come to about 1.06 on the 7763: 1.13, less 0.087 for three lazy voters with the sender and the write-set check (#259's review), plus 0.016 for the overlap. With the transport on the loop's turn, about 0.99.

**Code steps, each its own PR.** Since the decision of 2026-11-13, 10.7 replaces this list: step 1 stays as #269, steps 2 to 4 are folded into 10.7's steps 2 and 6, and step 5 is dropped for the frame's acknowledgement (10.8).
1. **Learning once a frame and once a step of events,** and the commit frontier through an index by sequence number. No wire or row change.
2. **`Follower::applied` and forgetting once an execution group.** No wire or row change.
3. **The frame's intake in one pass,** with the path logs once a key, and the property test above. No wire or row change.
4. **One batch and one barrier a frame,** with an index of payload rows in flight. A crash loses a frame's adoptions whole, as it loses a batch today. The leader's re-send (task-d07) brings them back.
5. **The vote message,** behind a replicated feature, and the leader's expansion of it.

Steps 1 to 4 need no wire change and no decision. Step 5 needs a feature activation, not a protocol decision. About 3 to 4 weeks for steps 1 to 4, and 1 to 2 for step 5.

**Each step's acceptance.**
- **A pair** of the followers' loops.
- **The audit's counts a turn:** persist batches, barriers and storage events at a follower, acknowledgement frames out, and passes over the ACCEPT set.
- **Step 5's tests:**
  - **A property test:** a vote message expands at the leader to exactly the votes that per-command acknowledgements would have added.
  - **`protocol_sim` rows with frames taken as a unit and vote messages,** under loss. A lost vote message is repaired by the leader's re-send and a re-acknowledgement by range.
  - **The crash matrix's cuts** at a frame's batch.

## 7. The cost model after each step

**Reading the table.**
- Each row is cumulative: the per-command `a` and per-turn `b` of each part, five voters, local, in µs.
- The last four columns are predicted ms an operation at 4, 8 and 16 commands a turn locally, and on the 7763 at the measured turn size (9.55).
- "Moves" says whether the step moves work from the per-command column to the per-turn column.
- The rows marked parked are the leader log's (Section 8). They stay so the trade can be read, and they continue from the Lever 3 row, not from lever 2 redesigned.

| Step (cumulative) | Leader loop | Followers' loops | Tokio | Materializers | Appenders | Moves? | k=4 | k=8 | k=16 | 7763 |
|---|---|---|---|---|---|---|---:|---:|---:|---:|
| Today (fit) | 198 + 147/k | 539 + 388/k | 191 + 733/k | 284 + 64/k | 79 + 108/k | — | 1.090 | 0.971 | 0.911 | 1.58 |
| #251, lever 6 part 1 (review's pair) | 198 + 147/k | 539 + 388/k | 191 + 733/k | 279 + 64/k | 79 + 108/k | No: cheaper | 1.086 | 0.968 | 0.908 | 1.58 |
| Proposal 2: protocol state in the journal | 198 + 147/k | 539 + 388/k | 191 + 733/k | 94 + 64/k | 79 + 108/k | No: removes rows | 0.964 | 0.845 | 0.786 | 1.41 |
| Proposal 1: execution from memory | 191 + 147/k | 455 + 388/k | 191 + 733/k | 94 + 64/k | 79 + 108/k | Partly | 0.904 | 0.785 | 0.726 | 1.32 |
| The appender's one encode (in place of proposal 3) | 191 + 147/k | 455 + 388/k | 191 + 733/k | 94 + 64/k | 63 + 86/k | No: removes an encode | 0.890 | 0.773 | 0.715 | 1.30 |
| Lever 4: release frame, acks by range | 181 + 147/k | 447 + 388/k | 179 + 733/k | 94 + 64/k | 63 + 86/k | Yes | 0.870 | 0.753 | 0.695 | 1.27 |
| Lever 8: one hand-off a turn | 176 + 147/k | 447 + 388/k | 159 + 733/k | 94 + 64/k | 63 + 86/k | Yes | 0.854 | 0.737 | 0.678 | 1.23 |
| Lever 3: the payload by reference | 176 + 147/k | 439 + 388/k | 143 + 733/k | 94 + 64/k | 63 + 86/k | No: fewer bytes | 0.838 | 0.721 | 0.662 | 1.20 |
| Lever 2 redesigned: a frame taken as a unit (Section 6.7) | 176 + 147/k | 367 + 404/k | 143 + 733/k | 94 + 64/k | 63 + 86/k | Yes | 0.793 | 0.675 | 0.616 | 1.13 |
| Lever 2: the frame as a unit (parked) | 170 + 147/k | 351 + 428/k | 135 + 733/k | 94 + 64/k | 63 + 86/k | Yes | 0.777 | 0.657 | 0.597 | 1.10 |
| Lever 1: option 3 with rotation (parked) | 170 + 147/k | 315 + 428/k | 145 + 733/k | 94 + 64/k | 63 + 86/k | No: removes executions | 0.760 | 0.640 | 0.580 | 1.07 |
| End state: a follower's plumbing per frame (parked) | 153 + 167/k | 236 + 476/k | 145 + 733/k | 94 + 64/k | 63 + 86/k | Yes | 0.708 | 0.582 | 0.519 | 0.98 |

**The basis of each step** (local µs a command, five voters):
- **#251:** review's pair reads −1.2% at the materializers, −3 to −6 (Section 5). The table takes −5.
- **Proposal 2:** −185 at the materializers (Section 4.1). Range −175 to −195.
  - Review's knockout holds it: −0.13 measured on the 7763, and −0.16 to −0.18 with the retry rows.
  - task-d117's design (#258) adds 20 to 30 back. The table keeps the gross until that step's pair.
- **Proposal 1:** the read view and snapshot, −21 a follower and −7 at the leader.
- **Proposal 3: withdrawn** (Section 4.3). In its place, the appender's one encode: a fifth of the appenders' terms, −16 a command and −22 a turn, by review's profile.
- **Lever 4:** the budget's −0.02 to −0.05 on the 7763, split over the leader's loop, the followers' acks in and tokio.
- **Lever 8:** −20 of tokio's park and wake (14 to 28% of tokio), and −5 of the leader loop's `eventfd` writes.
- **Lever 3:**
  - peer datagrams fall from 7.3 to about 3.5 a command;
  - at about a quarter of tokio's cost, which is per datagram under GSO (crypto 7%, quinn's per-packet work, kernel segments), that is −16;
  - plus −2 of each follower's hashing and decoding.
- **Lever 2 redesigned (Section 6.7):** −15 to −21 a follower, inferred from the follower loop's shares; the table takes −18, and +4 a frame a follower. The leader's acknowledgements in and tokio's bytes stay in lever 4's row.
- **Lever 2 (parked):** half to two thirds of a follower's intake and of its learning and forgetting (`Follower::applied`), −22 a follower. +10 a frame a follower, plus the leader's acks in.
- **Lever 1 (parked):**
  - a follower's execution after proposal 1 (34.5) becomes landing (about 10) plus a quarter of the executions by rotation (8.6);
  - that was −16 a follower, but about 7 of it is `Follower::applied`, which lever 2 already takes: a follower that lands a write set still learns and forgets the command (review on #252);
  - counted once, −9 a follower;
  - +10 at tokio for the write sets' bytes.
- **End state (parked):** three quarters of a follower's settle, flush and pacing per frame (−20 a follower, +12 a frame), and half of the leader's settle and flush.

**Measured so far,** each a local pair on #185's setup:
- **#251:** −1.2% at the materializers (review's pair).
- **Lever 4 (#253, merged):** two readings. Mine: −0.049 ms an operation, on a saturated host. Review's (#253, [6837943539](https://github.com/tuplesky/tuplesky/pull/253#issuecomment-6837943539)): no change, servers' CPU 0.823 against 0.831 ms an operation and completions 4,176 against 4,166 a second, over four rounds; the counts reproduced. The model has −0.020 locally.
- **The loop's housekeeping (#254):** the leader's loop −0.0045 ms an operation. It has no row.
- **Lever 8 (#256):** tokio −0.017 ms an operation, and the leader's loop unchanged. That is the modeled total (−0.0165), but it comes from the followers' wake-ups and tokio's syscalls, not the leader's hand-offs.
- **Proposal 1's snapshot half (#257):** −7 to −9 a follower and −6 to −12 at the leader, against the row's −21 and −7. The rest of the row, the view build and the table opens, is its next step.

**The 7763 column** takes one factor a part and the local turn size. Testing's batch-depth table ([#98, 6835373016](https://github.com/tuplesky/tuplesky/pull/98#issuecomment-6835373016)) gives the runner's own turn, and the column is re-read with it.

**What takes tokio and the client plane lower.** In the end state tokio is 0.29 ms an operation on the 7763, beside the followers' loops' 0.30.
- **Per turn, 733 µs over five voters:** the hand-offs, the wake-ups and the runtime's scheduling.
- **Per command, about 145:** the datagrams (quinn's per-packet work, crypto, the kernel).

Two things go further than the levers:
1. **The transport driven from the loop's turn.** quinn-proto is sans-IO. The loop would read its sockets once a turn (`recvmmsg`) and write each peer's transmits with one GSO `sendmsg` at the turn's end, with no hand-off, no `epoll` a datagram and no runtime. That is the rule's network form taken literally.
   - **It removes** park, wake, `epoll` and the runtime's scheduling, about 30 to 40% of tokio by the profile.
   - **Modeled as** tokio at 120 + 450/k: the end state goes from 0.98 to 0.91 on the 7763 at the measured turn size.
   - **The cost:**
     - the transport is rewritten around quinn-proto;
     - the client plane's connections are served from the loop or from a second loop of their own;
     - a datagram that arrives mid-turn waits for the turn's end.
   - **Its own design step,** about 4 to 6 weeks, and undecided (Section 8).
2. **Fewer datagrams a command:** lever 3 on the peer plane, and shared client connections on the client plane (Section 4.5).

Neither crypto (7% of tokio) nor QUIC itself is the difference.

**Does it reach parity?**
- **Without a protocol decision** (proposals 1 and 2, the appender's encode, and levers 3, 4, 6 and 8), the servers end at about 1.20 ms an operation on the 7763. That is −0.38, more than review's −0.20 to −0.32 on #185 because proposal 2 is now sized from the measured rows.
- **Read the end state at the measured turn.** The 16-a-turn column is outside what either host reached: at 240 callers review's leader turn is 10.5 commands. A turn of 16 at the bar's load means holding a turn open, which is latency paid for CPU (review on #252).
- **The leader-log end state (parked)** reaches 0.98 at the measured turn, and 0.91 with the transport on the loop's turn.
- **Lever 2 redesigned (Section 6.7)** takes the no-decision row to 1.13.
- **Lazy voters (Section 6.6)** at `e = 1` take the no-decision row to 1.12, the sender and the write-set check counted (#259's review). With lever 2 redesigned, about 1.06, and with the transport too, about 0.99, counting `Follower::applied` once.
- **Against the owner's exit,** which is completions (Section 8), parity needs the servers at about 0.80 to 0.87. No modeled row reaches it at the measured turn.

Every figure but the measured ones above is modeled from counts and profile shares, not measured as a pair. Each step's acceptance reads its own pair.

## 8. Order, and what the owner decides

**The exit is completions,** meeting etcd's, as the lead has relayed the owner's bar. Section 7 reads servers' CPU against etcd's 0.887. In the owner's terms (review on #252, [6837290600](https://github.com/tuplesky/tuplesky/pull/252#issuecomment-6837290600)):
- **On the 7763's four CPUs, completions follow the whole box's CPU an operation.** Summing testing's columns at 120 callers ([#98, 6833683813](https://github.com/tuplesky/tuplesky/pull/98#issuecomment-6833683813) and [6835373016](https://github.com/tuplesky/tuplesky/pull/98#issuecomment-6835373016)) gives 2.09 ms for us and 1.30 for etcd. That predicts 0.63× etcd's completions, and the sweep read 0.66×. At 30 callers it predicts 0.71×, and the sweep read 0.72×.
- **Our client side and kernel cost 0.50 against etcd's 0.42.** So completions parity at 120 callers needs the servers at about 0.80 to 0.87, a little under 0.887.
- **No modeled row reaches it at the measured turn** (Section 7):
  - without a decision, 1.20, and 1.13 with lever 2 redesigned (Section 6.7);
  - lazy voters at `e = 1`, 1.12;
  - the leader-log end state, 0.98, and 0.91 with the transport on the loop's turn;
  - lazy voters with lever 2 redesigned, about 1.06, and with the transport too, about 0.99.
- **The bar may move.** etcd's 0.887 is from `--snapshot-count 100`, which testing is re-reading at etcd's default.

**The owner's answers on #252,** relayed by the lead on #185:

> 1. Not yet convinced the leader log is needed if batching is done correctly. 2. Ditto [witnesses]. OTOH, as long as any of the journal-only voters can recover as a leader, it's OK. I'm willing to trade (reasonable) recovery time for maximum throughput.

**Parked:**
- **The leader log and option 3** (Sections 6.1 to 6.4), with lever 1 and the end state in Section 7.
- **Witnesses as Section 6.5 has them.** They can't lead without a rebuild, and the owner's condition is that any journal-only voter can recover as a leader.
- **Lever 2 as Section 7's parked row sizes it.** Section 6.7 redesigns it as batching without a protocol change, and that version needs no decision:
  - a follower takes a frame in one pass, and learns, settles and flushes once a frame;
  - it votes once a frame, the message carrying each command's vote as a range of sequence numbers with the exceptions listed;
  - it forgets by range, once an execution group;
  - the fast path and any voter's right to propose stay;
  - nothing voted, counted, made durable or read by recovery changes. Only its vote message needs a feature activation.

**Undecided: the transport on the loop's turn** (Section 7).
- **Its size beside the leader log's,** on the 7763 at the measured turn: the transport −0.07, the leader log's three rows −0.23.
- **Its own design step,** about 4 to 6 weeks.
- **Lever 8 is built and measured first** (#256): tokio −0.017 ms an operation locally. What it leaves of tokio's per-turn part sizes the transport's step.

**Three choices left to a step, which the owner should know** (review on #252):
- **Proposal 1's memory:** every key's current value in memory, or a cache. That is memory a domain, which is cost a tenant under the multi-tenant goal.
- **What decides a mismatch** (Section 6.3): a second execution, or the domain stops for an operator. That is availability against one execution's word. Rotation under option 3 meets it a command at a time, and lazy voters at `e = 1` meet it a batch at a time, with their own executions as the tie-break (Section 6.6).
- **Proposal 2's price beyond its weeks:** it supersedes task-d113's rows by order, merged this week, and every recovery reader changes at once. So the crash matrix and the fault runs are earned again (#258). It follows from the rule, so it isn't a new decision, but its price is the owner's to see.

**Without a decision, in order:**
1. Lever 4 (#253, merged).
2. The loop's per-pass housekeeping (#254): the run-queue sample on a timer, and `redial` only on a change.
3. Execution a group at a time at every voter (#257). It brings the followers under one read snapshot a group.
4. Lever 8 (#256).
5. Proposal 2's design step (#258), then its code.
6. Proposal 1: each table opened once a group, then memory.
7. The appender's one encode, in place of proposal 3.
8. Lever 3.
9. Lever 6's rest.
10. Lever 2 redesigned (Section 6.7): its four steps without a wire change, then its vote message behind a feature.

**For the owner:**
- **Lazy voters (Section 6.6):** voters that vote on the hot path and land the eager voters' agreed batches behind, at a lag of one batch, a quarter of the command table. Any of them can lead after executing its tail: one batch in steady state, and at most an enforced window of two while any voter within it is up. At one eager follower beside the leader, about −0.09 ms an operation on the 7763, the sender and the write-set check counted. The domain keeps five copies of the store, three of them behind.
- **Whether the transport moves onto the loop's turn,** once lever 8 is on the carry.
- **If witnesses are reopened:** whether three copies of five may be read against etcd's five (Section 6.5).

## 9. The buffer: the unit at every stage, filled by the load

**The owner's goal,** relayed by the lead on #185:

> One goal for batching is to do better than per turn batching with per buffer (with configurable size) batching that can cover multiple turns, which trades off client perceived latency with overall throughput.

> Ideally the batching behavior is adaptive, latency neutral at low throughput and filling the buffer at high throughput.

**Why a hold at the leader isn't it.** The ceilings' build 5 held the leader's turn for 32 batches or 2 ms ([6840649501](https://github.com/tuplesky/tuplesky/pull/185#issuecomment-6840649501), Section 2).
- **It deepened only the leader:** 9.4 to 11.8 commands an append at 120 callers. A follower's appends stayed at 11 to 13, because a follower cuts its own (Section 9.2).
- **It saved at most 5 to 7%.** Nearly all the cost is per command (Section 3), and the per-turn part is 0.04 to 0.05 ms an operation at depth 10.
- **At 30 callers its timer cost 10 to 20% of completions for nothing.** Thirty closed-loop callers never fill a 32-command buffer, so only the timer ended it.

So depth buys little while the work stays per command, and a timer pays latency where the load doesn't fill the buffer. This section makes the buffer the unit at every stage, so that what is done once a command today is done once a buffer, and lets the load, not a timer, fill it. The figures are the per-command cost pass's ([6841973195](https://github.com/tuplesky/tuplesky/pull/185#issuecomment-6841973195)): build 5 and the carry at 120 callers, µs a command, summed over the five voters, since a follower's row counts four times. The code is read at the carry `c74219e6`.

### 9.1 What a buffer is

- **A buffer** is the commands a stage takes as one unit of work: one call, one journal append, one frame, one vote message, one send, one commit.
- **It closes as soon as the stage downstream is free.** With nothing in flight downstream, it goes at once with what it holds, even one command. At low load nothing waits, so it is latency neutral.
- **While the downstream is busy, arrivals accumulate,** up to the stage's cap. Depth then grows with the load by itself: it is what arrives during one downstream service time.
- **The cap is the only size knob,** in commands and in bytes. It bounds memory, a record's and a frame's size, and how long a buffer's first command waits on the rest. It is a ceiling, not a target.
- **No timer closes a buffer,** except at the store, where nothing downstream is busy (Section 9.3, stage 8).
- **A buffer isn't the loop's turn.** If the downstream is still busy when a pass ends, the buffer stays open into the next pass.
- **The knobs** go in a new `[batching]` section of `coordd.toml` (`crates/coord-daemon/src/config.rs`, beside `[journal]`). Each has today's value as its default, so the default changes nothing. All of them are compile-time constants today.

### 9.2 Where the loop already follows its downstream, and where it cuts

**Already load-following:**
- **The journal: one append in flight** (`lend_append`). While an append is out, everything queued waits, and the next group is all of it, up to the group's limit. That is this section's rule exactly.
- **The loop's idle flush.** The `owes_flush` arm of `Domain::serve` flushes only when no event is ready on either plane. So the loop drains its input before it lowers, and at low load it lowers at once.
- **The store: one commit out a domain** (`hand_off`), lending everything pending. Under `strict` the next commit goes as soon as the last returns. Under `replay` the commit hold of task-d86 waits for 32 records or 5 ms unless a reader asks.
- **The store's durable cadence under `replay`:** a durable commit every 64 commits, 4,096 records or 100 ms. That is etcd's shape: its backend commits every 100 ms or 10,000 writes, while its WAL syncs each `Ready`.

**Cut by a count or a moment, not by the downstream:**
- **`FLUSH_EVENTS` = 64** (`serve.rs`). The loop flushes after 64 events however many more are ready. Every peer frame and vote counts, and about three quarters of a leader's events are peer events. So 64 events hold about 16 caller operations, which is about 10.6 commands at 0.66 commands an operation. Review measured 10.1, 11.4 and 11.4 commands an append at 120, 240 and 480 callers (relayed by the lead): the cut, not the load, sets the leader's depth.
- **The journal group: 64 records or 256 KiB** (`GroupLimits::DEFAULT`), and `FLUSH_QUEUED` = 64 batches to match. It is the next cut once `FLUSH_EVENTS` is lifted.
- **Frames are cut at every flush** (`cut_frames` in `flush_queued`), even while the append that carries their proposals is still out. A frame can't leave before that append is durable, so cutting it earlier only makes more, smaller frames.
- **A follower flushes at every lull.** The frame and the commit frontier leave the leader at different moments: the frame behind its barrier, the frontier with none. So they arrive as two events, and each gets its own append (the cost pass's Section 5). That, not the frame's size, is why a follower's depth doesn't follow the leader's.
- **Execution groups: 32 commands** (`GROUP_COMMANDS`), or 128 batches queued, or 256 records not yet materialized.
- **Per command on the wire:**
  - a collector's submission is one stream and one event;
  - a release is one frame per command, though a pass's frames to a collector share one `send_many` (lever 4);
  - a follower's acknowledgement is one message per command (Section 6.7).

### 9.3 Each stage under the rule

| # | Stage | Downstream busy means | Cap (today) | Done once a buffer instead of once a command | µs a command taken, five voters | Cost pass rows |
|---|---|---|---|---|---:|---|
| 1 | A follower's intake, append, vote and settle | its own append is out | the leader's frame | the frame's checks, `advance_pending` and `learn` once; one batch, one barrier, one storage event and one vote message a frame; forgetting by range at a group's end | 95 (23.8 a follower), plus the leader's forgetting 7.0 and its votes in by range 6.8 | follower 2, 3, 4, 6, 10, 14, 15, 18, 19; leader 2, 5 |
| 2 | Execution | the group's append is out | 32 commands | one authorized view, one retry admission, one closure walk a group | 49 (leader 11.6, followers 9.3 each) | leader 6, 9, 24; follower 1 |
| 3 | The loop's pass | an event is ready | 64 events | the pass's housekeeping once a buffer, not once a pass | 12 (leader 3.3, followers 2.2 each) | leader 12, follower 12 |
| 4 | The leader's proposals | its append is out | 64 batches | one `PersistBatch` and one barrier for the proposals made while the last append was out; the barrier list once | 10.2 | leader 4, 19 |
| 5 | The leader's reads | a read round is being served | none | one view a read round; due reads by range | 5.4 | leader 1 |
| 6 | The frame | its proposals' append is out | 1 MiB | cut when that append is lent: one frame a follower an append, encoded once for all four | 3.7 | leader 17 |
| 7 | The journal record | an append is out | 64 records, 256 KiB | already one append a buffer; one record a buffer saves nothing more, since the appender's work is per byte (Section 4.3) | 0 | — |
| 8 | The store | a commit is out; under `replay`, until a reader asks | 32 records or 5 ms (`replay`) | one commit for several buffers, with execution from memory in front (Section 9.6) | 15 to 25, modeled | — |
| 9 | Intake | the collector has submissions in flight to that voter | 1 | one frame of a collector's queued submissions, behind a feature | not sized: tokio's streams, outside the pass | — |
| 10 | Releases and answers | the loop's hand-off to that connection is held (lever 8) | 1 | one release frame a collector a buffer, behind a feature | not sized: tokio's streams, outside the pass | leader 11 |

**Ranked by what they take, five voters:** stage 1 (109 µs a command with the leader's half), stage 2 (49), stage 8 (15 to 25), stage 3 (12), stage 4 (10), stage 5 (5), stage 6 (4). Stages 1 to 6 come to about 190 µs a command, 0.126 ms an operation at 0.66 commands an operation. That is the cost pass's classes A and B, now each with a stage.

**Overlaps, counted once:**
- Stage 1 is Section 6.7's lever 2 (steps 1 to 4, and step 5 for the vote message and the leader's votes in by range).
- Stage 2's view and retry rows are lever 5's and proposals 1 and 2's.
- Stage 5 is lever 5's.

**What isn't batching and is ordered beside it:** the cost pass's class C at followers, about 9 µs a follower (the payload kept as the frame's bytes, the records sealed from the bytes already encoded, one decode of a peer frame). Then the leader's `propose` path (8.1). Row 16, a follower checking the relayed payload's identity again, stays: it is a check, not batching.

### 9.4 Caps that don't cut each other

A stage's cap must not end a buffer the stage before it built. So:
- **The loop's pass ends when no event is ready, or at a cap counted in commands.** Peer events don't count toward it.
  - Two knobs: `[batching] flush_events`, default 64 (today's `FLUSH_EVENTS`), and `flush_commands`, default none.
  - The buffered configuration sets `flush_events` to none and `flush_commands` to the cap. The defaults keep today's behaviour.
- **The journal group's limit follows the buffer's,** in records and in bytes: at least the records a full buffer writes. Otherwise the group cuts the leader's buffer, as 64 records would at about 21 commands.
- **The frame closes when its proposals' append is lent,** not at each flush. So a frame is exactly the leader's buffer, up to its 1 MiB.
- **A follower's buffer is the leader's frame.**
  - **The frontier rides the frame when a frame is going out.** `Committed` is published in the same hand-off as the frames, behind the frame's barrier, so the two arrive together. Followers learn those commits one leader append later. That delays followers' execution and reads at followers, not answers, since the leader answers on its own release.
  - **When no frame is going out, the frontier goes on its own, as today.** An isolated write is an example: its frontier moves only once its votes return, after its frame has left, and an idle leader cuts no frame for it to ride (`Leader::cut_frames` emits nothing without queued proposals). So the leader announces it at the end of the turn that moved it, as `announce_advanced` does at the turn's end today, not on the 250 ms re-send timer. The rule changes only which hand-off carries the frontier while frames flow. It never holds the frontier back for a frame that may not come.
  - **A standalone frontier is a follower's buffer too.** The follower takes it as one event, commits what it adopted up to it (`commit_through_leader`), and flushes at that event's end, since its appender is free. So at low load a follower executes one message after the leader's commit, as it does today (Codex on #267).
  - **A follower flushes at a frame's end** when its appender is free, not at every lull.
  - **One batch, one barrier and one vote message a frame** (Section 6.7, steps 4 and 5).
  - Its append's limit follows the frame's: about two records a command, the adoption and the execution.
- **The execution group's cap is at least the buffer's,** so a decided buffer executes as one group.
- **The store's commit spans buffers:** its cap is at least one buffer, and is set on its own (Section 9.6).
- **Every cap stays within the command table's room for new admission:** capacity less an eighth, 875 of the default 1,000. What is buffered across the stages, plus what is decided and not yet forgotten, must fit there. So no stage's cap should pass an eighth of the table (125, the catch-up page) unless the table grows with it.

**A precondition of raising any cap: no pass over what is in flight once an event.**
- **Review's direct test** ([6842978632](https://github.com/tuplesky/tuplesky/pull/185#issuecomment-6842978632), Section 3): the loop's three bounds at 512, 480 callers.
  - A turn reached 25 commands, and the other threads saved 84 to 105 µs a command, 5 to 7% of the servers.
  - The leader's loop rose 49 to 50 µs a command, 25%, and gave most of it back. The net was −2 to −3%, and completions fell 3 to 4%.
- **45 of those 49 µs are three passes.** Each walks what is in flight and runs once an event, so a deeper buffer, which holds more in flight, makes each cost more a command:

| Role | Pass | Runs | Walks | Cost pass row | µs a command, capped / bounds at 512 |
|---|---|---|---|---|---|
| Leader | `pump_reads`: `ReadBarrier::take_due` with `Leader::executed_below` | after every peer event and every turn | for each held read, the unsettled proposals below its index | 1 | 8.3 / 32.5 |
| Leader | `Leader::learn`: `Learner::commit_learned_where` | each acknowledgement, each storage event | every command at ACCEPT | 2, and row 4's `learn` | 8.4 / 24.0 |
| Leader | `adopt_own` | each command | every unsettled barrier, into a set | 19 | 4.7 / 10.4 |
| Follower | `Follower::learn`: `commit_learned` and `commit_through_leader` | each frame entry, each storage event | every command at ACCEPT, twice | 14, and row 2's `learn` | 6.6 / 10.9, a follower |

- **Other per-event work that grows with what is in flight.** This is read from the code at `c74219e6`, not measured at 512:
  - **A follower's vote** (`publish_to_leader_and_frontend`, row 18) lists every pending barrier in its `requires`, so the k-th adoption of a frame waits on at least k barriers.
  - **A follower's `payload_row`** scans every in-flight payload batch (`durable_payloads`), twice an adoption.
  - **`DurableLedger::written`** scans the staged batches, once for each dependency `carry_unwritten` looks at.
  - **A follower's `JournalDurable`** checks each payload it made durable against every other in-flight payload batch (`served_payloads`).
- **So these go by range, or by what changed, before any cap is raised** (Section 9.8, step 4):
  - the leader's due reads from the lowest unexecuted proposal;
  - learning, at both roles, of only the commands whose votes, adoption or durability changed, with the commit frontier through an index by sequence number;
  - the barrier list once a turn at the leader and once a frame at a follower;
  - the payload rows in flight through an index.
- **With them, depth is worth what the other threads save,** 5 to 7% at depth 25. Without them it is worth nothing.

### 9.5 Depth, cost and latency

**Expected depth with the rule** (commands an append at the leader; a follower's follows by Section 9.4):

| Load | Today | With the rule, default caps | With caps raised to 125 |
|---|---:|---:|---:|
| 30 callers | 3.6 to 3.9 | the same | the same: the load doesn't fill a buffer |
| 120 callers | 9.4 to 10.1 | the same | 11 to 16, expected: about 80 commands are in flight over every stage |
| A saturating load (480 callers, or an open loop above capacity) | 11.4, cut by `FLUSH_EVENTS` | 11.4 | up to the cap |

**Servers' ms an operation against depth,** local, modeled. It is `floor + P/k`:
- `P` is the per-buffer part: the per-turn part today, 0.045 to 0.095 ms an operation at depth 10 (the ceilings' fit and Section 3's), plus the moved rows done once a buffer at one to two times one command's cost.
- The floor is build 5 at 120 (0.672), less the per-turn part and the 0.126 the stages take.

| Depth `k` | 4 | 10 | 16 | 32 | 64 | 125 |
|---|---:|---:|---:|---:|---:|---:|
| Build 5 at 120 callers today | 0.74 to 0.82 | 0.672 | 0.64 to 0.66 | 0.61 to 0.64 | 0.59 to 0.63 | 0.59 to 0.63 |
| Build 5, stages 1 to 6 once a buffer | 0.65 to 0.75 | 0.56 to 0.57 | 0.52 to 0.55 | 0.49 to 0.52 | 0.47 to 0.51 | 0.46 to 0.51 |
| The carry, stages 1 to 6 once a buffer | 1.05 to 1.15 | 0.96 to 0.97 | 0.92 to 0.95 | 0.89 to 0.92 | 0.87 to 0.91 | 0.86 to 0.91 |

- **The table assumes Section 9.4's precondition.** Without it, review's direct test at depth 25 measured −2 to −3%: the leader's loop gave back most of what the other threads saved. And every take here is an estimate. Review carries the class A and B takes at half until a pair measures one, since #264 came in at a third of its estimate. Class C is firm.
- **Most of the gain is the stages, not the depth.** At today's depth (10), doing stages 1 to 6 once a buffer takes about 0.10 ms an operation, −16%. Depth 64 takes another 0.05 to 0.09.
- **Depth 64 needs a load that fills it.** At 120 closed-loop callers about 80 commands are in flight over all the stages, so one stage's buffer sees 11 to 16. The raised caps are for loads beyond today's bar: more callers, or callers that share a connection (a proxy, kine's adapter, a pipelined SDK).
- **At 30 callers** the rule adds nothing and takes nothing from depth. The stages still save their once-a-buffer part, about 0.06 to 0.09 ms an operation at depth 4.

**Added latency:**
- **At low load, none:** every buffer closes at once, as the loop's idle flush does today.
- **At 120 callers, about none at the default caps.** With caps raised, a pass can take more events before it flushes. A command waits at most for the events ready at its arrival, which it would also wait for today: it waits for them behind the flush instead of in front of it.
- **At a saturating load, the trade the owner names.** A buffer's first command waits on the rest of it: up to the cap times the leader loop's work a command, about 0.2 ms locally. At a cap of 64 that is up to about 13 ms, and up to about 25 ms at 125. Its p99 rises by that much, while queueing already dominates. Completions rise with the cost saved, and in a closed loop at a fixed number of callers the mean latency falls with them.

### 9.6 The storage third: what proposals 1 and 2 and a store commit across buffers keep of build 1

Build 1, storage off at every voter, took 0.369 ms an operation off the carry at 120 callers (1.073 to 0.704, −34%) and 0.419 at 30 (1.267 to 0.848, −33%). Lazy voters would keep about a fifth of build 2's −35% (−6 to −8% of the carry), and the lead will recommend that the owner park them. Here, the same way, is what three things keep of build 1. Execution from memory is proposal 1, protocol rows out of the store at every voter is proposal 2, and the third is one store commit across several buffers.

| At 120 callers, local | ms an operation | Basis |
|---|---:|---|
| **Execution from memory, the store still written** | −0.146 | the ceilings' row 1a, measured, one run |
| **Proposal 2** | −0.115 to −0.136 | the materializers left after row 1a (0.209), less 55 to 65%: Section 4.1's −185 of 284 µs, with 20 to 30 added back for task-d117's rows (#258) |
| **One commit across buffers** | −0.007 to −0.019 | 10 to 20% of the materializers left (0.073 to 0.094). The commit's own cost is a larger share once proposal 2 has removed the rows: in #233's model by order it is 35.5 µs a command at 8 a commit and 29.8 at 30. task-d86's hold measured −5% at a follower at 120 callers and −15% at 30. |
| **Together** | **−0.268 to −0.301** | **73 to 82% of build 1's 0.369, so −25 to −28% of the carry** |
| Left of build 1 | 0.07 to 0.10 | the application rows (`KvCurrentV1`, `EventsV1`), the meta row once a commit, and redb's commit |

**At 30 callers:**
- Execution from memory measured −0.082 in its one run.
- Proposal 2 takes −0.127 to −0.150.
- One commit across buffers takes −0.012 to −0.026. It is larger here because commits are small: 4 a commit.
- Together, −0.221 to −0.258: 53 to 62% of 0.419, so −17 to −20% of the carry.

**Under `replay` only.** A commit that spans buffers leaves the projection further behind the journal. Under `strict` the commit is on the answer's path, so its buffer is the execution group's and it doesn't span. With execution from memory, no plan reads the projection, so the commit can wait for its cap. What still reads the projection asks for the commit, as with task-d86's hold: a read at a barrier, a watch, a catch-up page, a checkpoint. The 5 ms then bounds only what doesn't ask. Recovery replays from the projection's durable stamp, which the durable cadence bounds, as today.

### 9.7 Recovery and correctness

**No answer before its record is durable at a quorum.**
- A buffer changes how many commands share one barrier, record, frame or vote message. It doesn't change what an answer waits on.
- A release still needs its command decided: its leader's batch durable, with a quorum's votes or the fast path's. With one barrier a buffer, that is the buffer's append.
- A vote message leaves only once every barrier it names is durable, and every barrier still pending before it (Section 6.7). So each vote it carries is durable before it leaves, as each `SlowAck` is today.

**A takeover mid-buffer.** A buffer is not a unit of decision. That would be the leader log (Section 6.1), which stays parked.
- Each command keeps its own proposal, dependency record and votes. A vote message expands at the leader into exactly the votes that per-command acknowledgements would have added (Section 6.7's property test).
- A leader that fails mid-buffer leaves some of its commands adopted at some followers and not at others, as it does command by command today. Recovery reads each command's records, unchanged.
- A buffer whose append didn't complete is lost whole at that voter, as a batch is today:
  - at the leader, its commands were never answered, and their callers retry;
  - at a follower, its adoptions were never voted, and the leader's re-send (task-d07) brings them back.

**The command table's bounds.**
- Admission stops at capacity less an eighth. A buffered command holds its table entry from intake as it does now, so buffering adds no entries. It only lengthens how long one is held, by at most one buffer at each stage.
- That is why Section 9.4 keeps every cap within an eighth of the table: the reserve still holds one catch-up window, and backpressure still stops admission before recovery's room is touched.
- The held proposals' bound (`HELD_PROPOSAL_SLACK` tables) is per command and unchanged.

**Liveness.** With no timer, a buffer always closes:
- when its downstream frees, and every downstream frees by completing: an append, a commit, a frame's acknowledgement;
- or at its cap.

The store's 5 ms bound stays for readers that don't ask. The loop's 1 ms hand-off bound (`HAND_OFF_WITHIN`) still releases what it holds.

### 9.8 The code steps, in the lead's order

The order is by the five-voter sum, each step one PR, with a both-orders pair on the runner for any that claims over 5%:
1. **Stage 1, lever 2 (Section 6.7), smallest step first.** The first is learning once a frame and once a step of events, with the commit frontier through an index by sequence number. Then:
   - `Follower::applied` and forgetting by range at both roles;
   - the frame's intake in one pass;
   - one batch and one barrier a frame, with the frontier riding the frame when one is going out (on its own otherwise) and the follower's flush at a frame's end;
   - the vote message, behind a feature.
2. **Class C at followers** (about 9 µs a follower).
3. **The leader's proposals:** the `propose` path's copies (8.1), then one barrier a buffer (stage 4, 10.2).
4. **The passes over what is in flight, by range or by what changed** (Section 9.4's precondition): the leader's rows 1, 2 and 19, and at a follower `learn` beyond the frame, the vote's barrier list and the payload-row scans. They come before any cap is raised. Their acceptance is the pair review ran ([6842978632](https://github.com/tuplesky/tuplesky/pull/185#issuecomment-6842978632)), the bounds at 512 against capped at 480 callers, with the leader's loop no longer rising.
5. **The caps** (Section 9.4): `flush_commands` beside the event count, which can then be set to none, the journal group's limit following the buffer, the frame cut at the lend, and the execution group's cap. Each is a knob whose default is today's value. A pair at 120 and 480 callers, and at 30, reads the depth and latency of Section 9.5.
6. **Stages 2 and 5** with lever 5 and proposal 1; **stage 8** after proposal 2.
7. **Stages 9 and 10** (one frame of submissions, one release frame a collector), behind a feature, once tokio's per-stream share is read.

**Each step's acceptance:**
- a pair of the loops it changes, and a profile of the rows it claims, before and after. That measures the take, which settles the A and B estimates step by step (review carries them at half until then);
- the audit's counts a buffer (batches, barriers, storage events, appends and vote messages a command);
- for the caps, depth and p50/p99 at 30, 120 and 480 callers against Section 9.5's table.

## 10. The frame as the unit of decision

**Decided, 2026-11-13.** The owner: "Agreed on all 4 points."
1. **The frame is the unit of decision.** It goes ahead.
2. **Lazy voters are deferred.** 6.6 stays a design, and no step below builds it. Every follower still executes and materializes.
3. **The fast path is kept.** Recovery is the source rule with frames as its entries (10.3): the variant of 10 to 14 weeks (10.6).
4. **The store the exit is read on waits for the barrier-on disk reading.** Each step's checkpoint below is read on tmpfs, locally and on the runner. Whether we meet or beat TLS etcd is read on the store the owner picks once testing's run with write barriers on is in.

10.7 is the step plan, and 10.8 says what it does to the named-cost queue. The rest of this section is the case as it was put to the owner, kept as the record.

**As put to the owner: two decisions, and a choice inside the first.** Servers' CPU, ms an operation at 120 callers on the EPYC 7763 [on the 9V45], storage on. TLS etcd reads 1.07–1.13 [0.44–0.51]. Every figure is a best case (10.1).

1. **The frame as the unit of decision** (this section). A follower takes the leader's frame whole: one append, one acknowledgement and one decision a frame.
   - **Gives:** −0.38 [about −0.25], the difference between 10.1's rows. About −0.06 [−0.04] of it is step 2's journal format, which doesn't need the decision (10.5). Alone it reaches 0.79–0.82 [0.50–0.52]: 0.70 to 0.77 of etcd on the 7763, and 0.98 to 1.18 times etcd on the 9V45.
   - **Weeks:** 10 to 14 with the fast path kept, 8 to 11 with it dropped (10.6).
   - **Gives up:** it is a protocol change. Frame positions become durable and mean the same across ballots (10.2). Recovery and Sync read frames (10.3). Each step activates through task-d99, and a domain doesn't go back below it (10.5). The fault runs are earned again (10.6).
2. **Lazy voters** (6.6). Voters that vote on the hot path and land the eager voters' agreed batches one batch behind.
   - **Gives:** −0.16 [−0.09 to −0.10], measured with every follower lazy. At 6.6's one eager follower, with the sender and the write-set check paid, about −0.09 [−0.06], inferred (Section 8).
   - **Alone** it reaches 1.01–1.06 [0.65–0.68], or 1.08–1.13 [0.68–0.72] at one eager follower. That is at etcd on the 7763, with no margin, and 1.3 to 1.6 times etcd on the 9V45.
   - **Weeks:** 5 to 7, after proposals 1 and 2 (6.6).
   - **Gives up:** the domain keeps five copies of the store, three of them a batch behind. A revoked grant can still authorize a result at a lazy frontend for up to 150 ms plus a message delay (6.6).
   - **With the frame:** 0.63–0.66 [0.40–0.42], or 0.70–0.73 [0.44–0.47] at one eager follower. On the 9V45 it is the only row under etcd, 0.78 to 0.95 of it at best. At one eager follower it is 0.86 to 1.07 of etcd: at it, not under.
3. **If the frame is the unit: the fast path kept at low load, or dropped** (10.4). Both give the same floor at load.
   - **Kept, the recommendation.** Below the relay's threshold, a fast quorum on the leader's path decides a command before its frame does. Recovery stays today's source and possible-fast rules, with frames as their entries (10.3).
   - **Dropped.** About 2 to 3 weeks less. Recovery becomes Multi-Paxos's slot rule, and the possible-fast rule goes (10.3). It gives up what 6.2 lists: a collector far from the leader pays the leg to it at every load.

**Where the frame's figure comes from.** The structural floors round ([#185, 6854728753](https://github.com/tuplesky/tuplesky/pull/185#issuecomment-6854728753)) measured a follower that handles the leader's frame as a unit, one append and one acknowledgement a frame.
- **At 120 callers** that takes −0.46 ms an operation off build 1's 0.86 locally [−0.67 of 1.31 on the 7763], at about 10 commands a frame. Its followers neither execute nor decode, so the floor holds the lazy voters' saving too.
  - Lazy voters (6.6) and lever 2 redesigned (6.7) reach about −0.16 of it without a protocol change.
  - About −0.04 [−0.06] is the seal and settle records. One record and one durability event a group in place of two a command is a local journal format change, not a protocol change (10.5, step 2).
  - **The rest, up to about −0.26 [−0.38], needs the frame to be the unit of decision.** It is each command's own intake, vote, dependency record, rows, settle and forget at every follower.
  - It is an upper bound: a residual of separate pairs (review on #185).
- **At 5 callers** the floor is −0.50 of 1.86 [−0.72 of 2.92], storage off. That is the frame and the lazy voters together. The split between them, the other levers and etcd at 5 callers aren't measured.
- **At 30 callers,** where a frame is 4.9 commands, the floor's pair is queued. It comes into 10.1 when it lands.

This section covers the slow path's unit only. Option 3 and the leader executing for everyone (6.1 to 6.3) stay parked.

### 10.1 The totals, four ways

Servers' CPU, ms an operation at 120 callers, storage on. Every row is a best case (the bases below).

| | Local | EPYC 7763 | EPYC 9V45 |
| --- | ---: | ---: | ---: |
| Neither | 0.79–0.84 | 1.17–1.22 | 0.74–0.78 |
| Lazy voters only | 0.67–0.72 | 1.01–1.06 | 0.65–0.68 |
| The frame only | 0.55–0.58 | 0.79–0.82 | 0.50–0.52 |
| Both | 0.43–0.46 | 0.63–0.66 | 0.40–0.42 |
| TLS etcd, testing's runs | — | 1.07–1.13 | 0.44–0.51 |

**At 6.6's one eager follower,** lazy voters take about −0.09 on the 7763 in place of −0.16 (Section 8, inferred). "Lazy voters only" becomes 1.08–1.13 [0.68–0.72], and "Both" 0.70–0.73 [0.44–0.47].

**The bases:**
- **Neither:** build 1, less lever 2 redesigned (−0.04 to −0.06, estimated), long-lived API streams (−0.045, measured) and the bytes path (−0.044, measured). Then plus Section 9.6's storage, 0.07–0.10. The pairs overlap at the followers, so this is the best case.
- **Lazy voters:** −0.12 locally, −0.16 on the 7763, measured with every follower lazy: no follower executed or materialized, and none paid for a batch, an agreement or a landing. 6.6's design keeps one eager follower and pays for those. They need the owner's ≤150 ms revocation bound.
- **Both:** the measured floor, 0.364 [0.545], then the same storage. That floor is the follower above, with all four followers lazy, plus votes once a frame and the transport switches. It is before the bytes path and before step 5 (10.5).
- **The frame only:** "Both" with the lazy voters' −0.12 [−0.16] added back.
- **TLS etcd,** testing's runs at 120 callers: on the 7763, 43350571102 (1.082, 1.128) and 43429015655 (1.073); on the 9V45, 43353880409 (0.439) and 43437990761 (0.514).
- **The 9V45:** scaled part by part from the 7763 with the runner's profiles of our build ([#98, 6856833756](https://github.com/tuplesky/tuplesky/pull/98#issuecomment-6856833756) and [6851041797](https://github.com/tuplesky/tuplesky/pull/98#issuecomment-6851041797)). Our transport there is 0.58× the 7763's, and our loops, appender and materializer 0.655×. Weighted by each total's transport share, every row scales by 0.63 to 0.64.

**What they say:**
- **The frame is the larger of the two:** −0.38 on the 7763, against the lazy voters' −0.16 at best. Alone, it is under etcd on the 7763, and at 0.98 to 1.18 times etcd on the 9V45, without the revocation bound.
- **Lazy voters alone reach etcd on the 7763,** at 0.89 to 0.99 of it at best and 0.96 to 1.06 at one eager follower. That reaches it with no margin.
- **On the 9V45, getting under etcd takes both.** Either alone is 0.98 to 1.6 times etcd.
  - The faster core helps etcd more. Its transport falls 2.1× and its consensus and storage 2.3×, against our 1.7× and 1.5×.
  - Our excess is consensus, execution and storage, mostly at the followers (6856833756).
- **Neither** is 1.04 to 1.14 times etcd on the 7763, and 1.45 to 1.77 on the 9V45.

### 10.2 The wire and the journal

**On the wire:**
- **The frame:** task-d102's `ProposalFrame` gets a position in the ballot's frame log and its predecessor's digest. A follower accepts it all or none, in position order, with every payload inside.
- **Positions mean the same across ballots, and are durable.** Today a proposal's number is ballot-scoped and never persisted ([design 4.1](tuplesky-design.md#s4-1)), and 4.9 declined positions in recovery reports. The frame log reverses both: a frame's position is the domain's, carried from ballot to ballot and journaled with the frame.
- **`FrameAccept { replica, ballot, through }`** acknowledges a prefix of frames once their records are durable. It replaces each frame command's `SlowAck`. The leader counts a majority a frame.
- **`Committed { through }`** counts frame positions.
- **A recovery report** adds each accepted frame above the voter's commit frontier as (position, ballot, digest), and a Sync names frames the same way.

**In the journal:**
- **One record a frame:** its bytes as received, its ballot and its position, durable before the acknowledgement leaves.
- **A follower drops, for a frame command,** its dependency row, adoption record, payload row and vote state. The frame holds the payload, the position is the order, and the frame's phase is the command's.
- **Under proposal 2** (4.1), the frame record is the protocol state.

### 10.3 Recovery and takeover

**Today.** Recovery's source and possible-fast rules read each command's durable phase, ballot, dependencies and path ([design 4.9](tuplesky-design.md#s4-9)).

**For frame commands, they read the frame log instead.** A voter accepts frames in order, so its report is a prefix. A decided frame is in every majority, so under either rule below it is selected whole.

**What selection is depends on the fast path (10.4).**
- **With the fast path kept, the recommendation: the source rule, with frames as its entries.** It is today's rule (4.9), not Multi-Paxos's:
  1. The frames accepted at the source ballot, the highest among the majority's reports, by position.
  2. Reports below the source ballot supply committed frames only.
  3. Then the candidates the possible-fast rule keeps, after the selected frames.
- **Why not the slot rule there.** A slot rule protects what a majority accepted at a position. A fast decision is not that: it fixes a command's place by a fast quorum's equal paths, before any frame holding it is accepted. So a frame accepted only at an older ballot can take the place a fast decision already has. Review's case on #276 ([6858204830](https://github.com/tuplesky/tuplesky/pull/276#issuecomment-6858204830)), five voters L0, L, M1, M2 and Y:
  - **At b0,** L0's frames at positions p to p + 3 reach only Y. They are not decided.
  - **At b,** L's majority is L, M1 and M2, its fast set. Its Sync selects through p − 1. A collector then proposes c fast, and M1 and M2 pre-accept it after p − 1, as L orders it. c is decided and answered. L's own frame p, which holds c, reaches nobody, and L stops.
  - **At b2,** the reports are M2's, Y's and L0's. The slot rule selects b0's frames at p to p + 3, since nothing else is at those positions, and c can only go after them, though its answer was computed right after p − 1. The source rule takes b as the source: Y and L0 are below it and supply only what they hold committed, so b0's frames aren't selected and c takes position p.
- **With the fast path dropped, 10.4's alternative: the slot rule.** Every command is decided by its frame alone, so a decision is a majority's acceptance at a position. At each position, the selection takes the frame accepted at the highest ballot among a majority's reports, as Multi-Paxos selects a slot. The possible-fast rule and the per-command records it reads go.

**Order.** Every fast and slow quorum includes the ballot's leader ([design 4.1](tuplesky-design.md#s4-1)), so the leader proposes every command under a sequence number, fast or not. Every command is therefore in a frame.
- A fast decision decides a command early, by a fast quorum on the leader's path.
- A command's place is its frame's position, then its index in the frame.
- **Its dependencies are the conflicting commands at lower places.** A voter computes them from the order and the payloads' keys, so they need no record of their own.
- **That covers a fast command decided before a conflicting frame command** (Codex on #276). The leader proposed it first, so its place is lower. A follower that lacks it can't execute the frame command until it gets it, as with a missing dependency today.
- **With the fast path kept, a fast command keeps its record of its path and dependencies** for the possible-fast rule. Its place agrees with them, as its path agrees with the leader's order today.
- Task-d71's rule reads the leader's order today through the records along dependencies. The order becomes a source it reads directly.
- **What a voter carries into a Sync and what it serves** (task-d77, task-d80, task-d81) go by position: a committed frame is carried and served as a committed command is.

**A takeover mid-frame.**
- **The new leader re-proposes the selected frames whole,** at its ballot and in order, before new ones. No frame is ever split.
- **A frame no reporter in the majority holds was not decided as a frame.** Its frame commands come back through their clients' retries.
  - **With the fast path kept, a fast command in it may have been decided.** The possible-fast rule keeps it, and the new leader re-proposes it in a new frame after the selected ones.
  - Its place holds: the member that pre-accepted it on the leader's path had accepted every frame before it, so those frames are all selected, and it comes right after them.
  - With the fast path dropped, there is no such command.
- **Execution resumes at the durable execution position** (task-d90), which can be inside a frame.
- **A follower missing frame f waits for the leader's re-send** (task-d07) before taking f + 1. That is head-of-line blocking at one follower, bounded by the re-send.

### 10.4 The fast path, a voter's right to propose, and what stays per command

**The fast path: kept, narrowed to low load. This is the recommendation.**
- **Every command is in the leader's frames** (10.3).
- **Below the relay's threshold** (task-d100), a collector also proposes it fast, as today. A fast quorum on the leader's path decides it before its frame does, with per-command records and possible-fast recovery.
- **At load,** collectors already relay to the leader, and the frame decides.
- **Any voter still proposes and can lead.**

**The alternative: the frame as the only path** (6.4). Recovery is the slot rule over one source, and the possible-fast rule goes (10.3), about 2 to 3 weeks less. It gives up what 6.2 lists: a collector far from the leader pays the leg to it. Both give the same floor at load.

**Per command, unchanged:**
- **Identity and admission facts.**
- **Execution in the decided order, in groups as today.**
- **A `ResponseV1` kept for the retry window,** and the frontend's answer.
- **The retry window:** a client's sequences and `ack_through`, with deduplication at execution.
- **Reads at their barrier.**

### 10.5 The staged path

Each step has its own PRs and pair.
- **Every activation goes through task-d99,** with readers a release ahead of writers, so old and new records are both readable.
- **A domain doesn't go back below a step it has activated.**

| Step | Activation | Delivers, 7763 at 120 |
| --- | --- | --- |
| 1. Without a protocol change: rows (a) to (c), lever 2, API streams, the bytes path, and lazy voters if the owner takes them | Lever 2's vote message; lazy voters | To 1.01–1.06 with lazy voters, 1.17–1.22 without |
| 2. One journal record a frame, and one record and one durability event a group for seal and settle, read beside the per-command records | None (local format) | About −0.07: the appender −0.01, and the seal and settle records −0.06 |
| 3. Recovery and restart read frame records and reports | None | 0 |
| 4. Followers take frames as the unit; the leader counts once a frame | `FrameDecision` | Up to −0.38 (follower), at about 10 commands a frame |
| 5. The leader proposes by range, without per-command dependencies or records (27 + 13.5 + 16.8 µs a command) | The same | About −0.06, inferred |

10.7 turns these steps into PRs, each with the measurement that bounds it before it is built and the checkpoint it has to reach.

### 10.6 Fault runs and weeks

**Fault runs:**
- **`protocol_sim` rows** at three and five voters, with the oracle checking one order everywhere, over rows 1, 9, 21 and 23. Each of these is a row:
  - a leader crash with a frame at 1 to n − 1 followers;
  - repeated elections with different frames at one position;
  - frames lost or reordered;
  - a follower frames behind;
  - conflicting fast and frame commands;
  - activation over old records.
- **A model test:** a decided frame is selected whole by every later Sync.
- **The crash matrix's cuts at a frame record:** torn, and durable but not acknowledged.
- **The runner's fault runs:** the leader killed or partitioned under load, a follower restarted mid-frame, and activation in a running domain.

**Weeks:**
- the design step, 1–2;
- steps 2 to 5, 1, 3–4, 2–3 and 1–2;
- the simulator, model and crash matrix, 2, alongside them;
- the runner's runs, 1–2.

**About 10 to 14 weeks with the fast path kept, 8 to 11 with it dropped.** Step 1, already in flight, isn't counted, and neither are lazy voters' 5 to 7 (6.6).

### 10.7 The step plan

**Where it stands (2026-11-13).** Not yet the plan of record. Review on #283 ([6865705600](https://github.com/tuplesky/tuplesky/pull/283#issuecomment-6865705600)), backed by the lead, let steps 0 and 1 start as written. This text answers that review's 16 points, each in the step it changes, marked (R1) to (R16).

**The rules every step keeps.**
- **Bounded before it's built.** Inferred savings have come in at a quarter to a third of their estimates. So no step is built on an inferred figure.
  - Its saving is measured first on a scratch build, which gets no PR, as a pair against the carry.
  - Where the bound is under what a pair can see, a profiled pair measures it: the named functions' share before and after, as for #282.
- **What a pair can see.** The base runs alone span about ±3% at 120 callers. In the journal re-verify pair (#284, 10.8) that was 0.848 to 0.891 ms an operation, and 1,319 to 1,398 µs a command for the five voters. So a saving under about 0.03 ms an operation is read in the part it moves (the appender, a follower's loop, the leader's loop) and in the counts a turn, not in the total.
- **Every reading is one pair (R13).** Two builds are compared by running them against each other, never as a difference of two pairs against a third. That difference is what left today's −0.38 an upper bound.
  - Four runs of the base and three of the change, alternating.
  - Each change run is read against the base runs on either side of it.
- **The checkpoint (R14).** A step passes when, in the part named:
  - the fall, as µs a command, is at least half its bound;
  - and it is larger than the base runs' own spread of that same quantity, over at least the four base runs that gave the spread.
  - A part read from a profile is its share of samples times its thread's CPU a command, both from the same runs. A share alone moves when anything else on the thread moves.
  - The pair runs at 120 and 30 callers, five voters on tmpfs, replay.
  - A step that misses its checkpoint stops the plan: the next step isn't built until the lead has the reading.
- **Every checkpoint reads latency (R16).** The owner's goal for batching is latency neutral at low load.
  - Each pair also runs the same alternation at 5 callers and at one.
  - It reports p50 and p99 of a get's and a put's service time at 1, 5 and 30 callers, and the commands a frame at each load, since the decision's saving follows that.
  - A step whose p50 or p99 at 1 or 5 callers rises by more than the base runs' spread doesn't pass, whatever its CPU.
- **A null step is read part by part (R15).** Steps 3 and 4 inactive should change nothing. A null pair's total inside ±3% lets through what it can't see, and three such steps could add 6% unseen. So a null step passes only when:
  - the counts a turn are unchanged;
  - and each part's CPU a command is inside its own base runs' spread: each loop, the appender, the materializer and tokio.
  - **After steps 2 and 4, one pair runs the carry against the carry before step 1,** at 120 and 30 callers and, for latency, at 5 and 1.
- **Shippable after every step.** A step does one of three things:
  - it changes nothing on the wire or in a record;
  - it writes only what the release before it already reads;
  - or it is dark behind a replicated feature (task-d99).
- **Feature gating.**
  - A feature activates only once every voter's build knows it: task-d99 counts every configured voter's advertisement.
  - **Prerequisites (R8),** in `requires` (`feature.rs`), as `BatchedRound` requires `AnswerOnRelease` today: `FrameLog` requires `BatchedRound`, and `FrameDecision` requires `FrameLog`. The order is `AnswerOnRelease`, `BatchedRound`, `FrameLog`, `FrameDecision`, each with its prepare marker as for the others.
    - **`AnswerOnRelease`:** under it a collector marks every new submission and answers it on its leader's release alone. A collector counts `SlowAck` today (`collector.rs`), and 5b ends it for frame commands, so without this an unmarked command would wait for votes that no longer come.
    - **`BatchedRound`:** step 3's frame is `ProposalFrame` with a position, and `ProposalFrame` is behind it.
  - **Who has to know (R9).** Step 3's PR lists, by role, every decoder of each kind steps 3 to 5 add or change: the frame's position, predecessor and boundary bit; `FrameAccept`; `Committed { through }` by position; frames in reports and Syncs; the frame fetch. The activation waits for each role on that list.
    - **Voters:** all of them, as task-d99 counts.
    - **Collectors,** at voters and at frontends, read `SlowAck`. Under `AnswerOnRelease` they already answer on the release, so they need nothing new. A frontend isn't in task-d99's count.
    - **A node whose build can't run an active feature refuses at open** (`an_activation_record_this_build_cannot_run_is_refused_at_open`, `tests/feature.rs`).
    - **Pinned in step 3's PR, as a row:** whether a frontend or observer already running when `FrameDecision` activates stops on the record or carries on. If an observer reads `Committed` or a report, the count grows to observers.
  - **A release (R10)** is a carry commit the lead names as one once testing's runner has run a domain on it.
    - "Readers a release ahead of writers" means a writer's PR merges only after a release that holds its reader.
    - The pairs: 1a before 1b; step 3 before 4a; step 4 before 5c.
  - A build knows `FrameDecision` only from the PR that completes it (5c). Until then its paths run in the simulator and the tests only.
  - A domain doesn't go back below a feature it has activated.
- **Mixed versions are rows (R10).** Steps 3, 4 and 5 each add these, beside `tests/feature.rs`'s activation tests:
  - a voter on the reader's build beside voters on the writer's, with the feature inactive: the domain runs as today, and nothing new is sent or written;
  - the activation refused while that voter is there (`Missing`, as `activation_is_refused_without_every_voter_and_by_a_follower`);
  - that voter upgraded, the activation, and the domain running with the feature active;
  - that voter restarted on the reader's build after the activation: it refuses at open.
- **Recovery.** Until step 5 activates, recovery decides from the per-command records it reads today. What a step adds is read beside them or checked against them, never used in their place.
- **The order.** The largest measured saving first, among the steps whose prerequisites are in.
  - The decision's saving is the largest: up to −0.26 locally [−0.38], an upper bound. But it needs steps 2 to 4 under it, and steps 3 and 4 save nothing.
  - Steps 1 and 2 need nothing. Step 0's bounds decide which of them goes first. The default is 1, then 2. Step 2 goes before the wire and journal steps either way, since its saving lands weeks before the decision's.
  - Steps 3, 4 and 5 are in the order safety needs, and only past the go/no-go below. Steps 6 and 7 follow step 5, ordered by their bounds.

**The go/no-go for steps 3 to 5 (R12), written before B2 is measured.** B2 − B1 is the decision's saving: servers' CPU, ms an operation, locally at 120 callers, read as one pair of B2 against B1.
- **Go at −0.10 or more** [about −0.15 on the 7763, 40% of the −0.38 the owner agreed on]. With steps 1 and 2, the frame alone then puts servers' CPU at about 0.85 to 0.94 of TLS etcd on the 7763 (10.1).
- **Between −0.05 and −0.10, back to the owner.** The frame alone is then at 0.85 to 1.02 of etcd on the 7763: at it, and under it only at the band's better end. The owner chooses one of three:
  - steps 3 to 5 anyway;
  - lazy voters (6.6) first;
  - neither.
- **Under −0.05, stop.** The frame alone is then at or over etcd on the 7763, and the decision doesn't pay for 10 to 14 weeks and a protocol change. The reading goes to the owner beside lazy voters' −0.16 [7763].
- **Latency, at any of the three.** If B2's p50 or p99 at 1 or 5 callers is above B1's by more than the base runs' spread, that goes to the owner with the CPU reading.
- **The 9V45 isn't decided by this.** Getting under etcd there takes both the frame and lazy voters (10.1), whatever B2 shows.

**The steps.** The bounds now are local at 120 callers [7763]. Every one of them is to be measured at step 0 before its step is built.

| Step | Activation | Bound | Checkpoint |
| --- | --- | --- | --- |
| 0. The bounds | — | — | — |
| 1. One application record and one durability event a group, split at the store's bounds | None: local format | B0 | Half of B0, in the appender and the materializer |
| 2. A follower takes a frame in one pass: its adoptions in a few batches, split at the store's bounds | None | B1; −15 to −21 µs a follower inferred (6.7) | Half of B1, in a follower's loop |
| 3. The frame log's kinds, read only, and the frame fetch | None: nothing writes or sends them | 0 | A null step |
| 4. Frame records written in order beside the per-command ones; recovery selects both ways and checks they agree | `FrameLog`, dark | B1W against B1, one pair: the cost added | Inactive, a null step. Active, no more than its bound, and no disagreement |
| 5. The frame decides above a boundary position | `FrameDecision` | B2 − B1, one pair; −0.26 [−0.38] an upper bound | The go/no-go above, then half of B2 − B1; then the runner's pair |
| 6. Learning by frame position, forgetting by range | With `FrameDecision` | −4 to −6 µs a follower, inferred (6.7) | Half of its profiled share |
| 7. The leader's own per-command records go | `FrameDecision` | About −0.06 [7763], inferred: B3 | Half of B3, in the leader's loop |

**Step 0: the bounds.** No PR. Each bound is a scratch build measured as one pair: against the carry, or against another bound where the step's saving is their difference.
- **B0: one application record a group.** Consecutive application outcomes are sealed as one journal record with one durability event, nothing else changed. It bounds step 1.
- **B1: a follower takes a frame in one pass.** It writes every per-command record as today, with one batch, one barrier and one storage event a frame (cut at 64 adoptions), and its votes in one hand-off. It bounds step 2.
- **B1W: B1 with step 4's writes, on B1's branch.** Decisions stay per command, as at step 4.
  - The leader and every follower also journal each frame record, by step 4's rule: once its entries are adopted and its predecessor's record written, in a batch of its own.
  - Every payload is in the frame.
  - It is measured as one pair against B1, and bounds step 4's cost: what `FrameLog` adds to step 2, which step 4 is built on. Measured against B2, it would read the cost of putting per-command records back beside frame decisions instead.
  - Recovery's second selection runs only at a Sync, so a pair without an election reads the records and the payloads. Its cost at a Sync is read in step 4's takeover rows.
- **B2: the frame decides, on B1's branch,** so that B2 − B1 is the decision alone.
  - It is build 6's follower that takes the frame as a unit, with execution and materialization put back: build 6's followers neither execute nor decode, so its floor holds the lazy voters' saving, which the owner deferred.
  - It is measured as one pair against B1 (R13).
- **B3: the leader proposes by range,** without per-command dependency rows or records. It bounds step 7.
- **What every pair reports:**
  - CPU a command by part;
  - the counts a turn: records, batches, barriers and storage events a command;
  - commands a frame;
  - p50 and p99 at 1, 5 and 30 callers.
- **The 30-caller floor again.** The pair of build 1 against build 6 at 30 callers ran, but its run directories were deleted for disk before its CPU was read. Only operations a second are left: build 6 at +42% over build 1, each run read against the base runs on either side of it (+28 to +50% by run; 2,766 a second against 3,871 on the means). B2's pair at 30 replaces it.
- **Where it stands:** B0 and B1 are written, on a local scratch branch off the carry at `ba947188`. Their pairs are queued at 120, 30, 5 and 1 callers. B1W, B2 and B3 come after.
- Under a week. Steps 1 and 2 need only B0 and B1.

**Step 1: one application record and one durability event a group.** This is the application half of 10.5's step 2 without the frame record, and the fifth item of 10.8.
- **Which half is which.** 10.5 named the journal part "seal and settle", and its two halves fall in different steps.
  - **Step 1** is each command's application outcome record at every voter: sealed, appended and made durable once a command. B0 bounds it.
  - **Step 2** is the protocol half and the settle: a follower's adoption record a command, and the storage event its machine settles for each. One batch a frame (2b) takes both to a few a frame, and B1 bounds them with the rest of step 2.
  - So the floors round's −0.04 [−0.06] for the seal and settle records splits between B0 and B1. Step 1's bound is B0 alone.
- **PRs:**
  - 1a: the journal's reader and replay take a group's application record, and nothing writes one;
  - 1b: the applier writes one application record a group in place of one a command, and waits on one durability event for it.
- **How a group's record is bounded (Codex).** An execution group holds up to 32 commands (`GROUP_COMMANDS`), and the applier's queue is bounded at 16 MiB, not at the record's 4 MiB and 4,096 updates. Each outcome is under the record's bounds today, but 32 together needn't be. Since #280 a record the store refuses stops the voter.
  - **The rule:** the same as step 2's. A group's outcomes go in records cut in execution order. A record closes before an outcome that would take it past 1,024 updates or 1 MiB of rows.
  - **An outcome over either bound goes alone,** in a record of its own, as every outcome does today.
  - **One durability event a record.** The group's answers wait on its last record, since the journal keeps records in order.
  - At the bench's sizes a group is one record. Groups of large outcomes take a few.
  - B0 cuts a run at half the journal group's byte limit and at the record's update bound. At the bench's sizes neither binds, so it bounds step 1 as written.
- **What recovery reads:** replay reads a group record as the application outcome of each command it names. Those are the same facts, in one record. The decision's records are untouched.
- **Mixed versions:** the format is local, since only this voter reads its journal. 1b raises the local image version, which 1a's release reads and an older release refuses to open. So a voter goes back at most to 1a's release.
- **Pinned by:**
  - a group of 32 outcomes near 1 MiB each, cut into records under the bounds;
  - a lone outcome over 1 MiB, in a record of its own;
  - a group past 1,024 updates;
  - a compile-time check of the bounds against the record's, like `journaled.rs`'s for the commit rows;
  - the crash matrix's cuts at the group record: torn, durable but not acknowledged, and between two records of one group;
  - a journal holding both forms replays to the same state as either alone;
  - a fixture: 1a's release opens 1b's journal.
- **Bound:** B0. **Checkpoint:** half of B0, in the appender and the materializer, as µs a command. Application records a command at every voter fall from one to one a group.

**Step 2: a follower takes a frame in one pass.** This is lever 2's steps 3 and 4 (6.7), in the form steps 4 and 5 extend.
- **PRs:**
  - 2a: the frame's checks run once. Each adoptable entry is adopted in one pass, without `held` or a `VoteSet`, and the path logs' suffixes are recomputed once a key at the frame's end.
  - 2b: a frame's adoptions go in as few batches as the store's bounds allow, each with one barrier and one `pending` entry. The payload rows in flight are found through an index. Votes stay one a command, in one hand-off a turn (lever 8).
- **How a frame's batch is split (R1).** Since #280 a batch the store refuses stops the voter. Full frames are what a new leader's backlog sends (#242), so one batch a frame would stop every follower at once, under a fault.
  - **The sizes.** A frame is cut at 1 MiB (`publish_frame`), and holds a larger request alone, up to 2 MiB. An entry is about 135 bytes without its payload and about 300 with a small one, so a full frame holds 3,500 to 7,700 entries and up to about 15,000 updates. A record is refused above 4,096 updates or 4 MiB, of which the store keeps 512 bytes for the record's header (`RECORD_HEADER_ALLOWANCE`).
  - **The rule.** The adoptions go in batches cut in frame order. A batch closes before an adoption that would take it past 1,024 updates or 1 MiB of rows. Those are task-d106's bounds for a follower's commit rows (`COMMIT_ROWS_BATCH_UPDATES`, `COMMIT_ROWS_BATCH_BYTES`), well under the record's.
  - **An adoption over either bound goes alone.** It is at most a 2 MiB payload row and a few small rows, under the record's bounds.
  - **Each batch's votes go when it is durable.** The journal keeps batches in order, so the frame's last batch durable means all are.
  - At 120 callers a frame (about 10 commands) is one batch. A full 1 MiB frame of the smallest commands is about fifteen.
- **What recovery reads:** the same records as today. A crash keeps a prefix of a frame's batches. The adoptions in the batches it loses come back with the leader's re-send (task-d07), as a lost batch does today.
- **Mixed versions:** nothing changes on the wire or in a record.
- **Pinned by:**
  - 6.7's property test: a frame taken as a unit leaves every path log as taking its entries one at a time does;
  - **a 1 MiB frame of the smallest commands:** taken in batches each under the record's bounds, the voter still running, and every adoption voted;
  - **a lone 2 MiB request:** its adoption in a batch alone, under the bounds;
  - a compile-time check that the split's bounds, with one largest adoption and the header allowance, are under the record's. `journaled.rs` checks the commit rows the same way: `COMMIT_ROWS_BATCH_BYTES + MAX_ENVELOPE_PAYLOAD + RECORD_HEADER_ALLOWANCE <= MAX_RECORD_BYTES`;
  - the counts a turn: batches, barriers and storage events a frame, and passes over the ACCEPT set;
  - the crash matrix's cuts at a frame's batch and between two of its batches;
  - `protocol_sim`'s rows, unchanged.
- **Bound:** B1. The inferred −15 to −21 µs a follower is about −0.04 to −0.05 ms an operation locally. A quarter to a third of that is −0.01 to −0.02, under what a pair sees, so the profiled pair may be the whole reading. Step 2 is built even then, since step 5 stands on it.
- **Checkpoint:** half of B1 in a follower's loop, as µs a command, and one batch, one barrier and one storage event a frame at 120 callers. Then the drift pair against the carry before step 1.

**Step 3: the frame log's kinds, read only.**
- **One PR.** Each piece is a new kind that every voter decodes before any voter sends or writes one:
  - a frame with its position in the domain's frame log, its predecessor's digest and its boundary bit (step 5), sent only while `FrameLog` is active, as `ProposalFrame` is behind `BatchedRound`;
  - the journal's frame record;
  - frame entries in a recovery report and in a Sync;
  - **the frame fetch (R7):** `FrameRequest { position, ballot, digest }`, answered with the frame record's bytes, which the asker checks against the digest. It is to frames what `PayloadRequest` is to payloads today.
- **What recovery reads:** what it reads today.
- **Mixed versions:** this is the release every voter has to run before `FrameLog` can activate. Its PR also lists the decoders by role (R9).
- **Pinned by:**
  - each new kind's round trip;
  - fixtures: a report and a Sync with frames decode, and one without them still does;
  - while `FrameLog` is inactive, nothing sends or writes the new kinds;
  - the row of a frontend or observer running when a feature it can't run activates (R9).
- **Bound:** 0. **Checkpoint:** a null step.

**Step 4: frame records written, and recovery checked against them.** Dark, behind `FrameLog`.
- **PRs:**
  - 4a: with `FrameLog` active, the leader gives each frame the next position in the domain's frame log, durable across ballots, and its predecessor's digest, and journals it. A follower journals the frame record: its bytes as received, its ballot and its position.
    - **When the frame log starts (Codex).** task-d99's activation doesn't reach the order: each voter's own durable record turns a feature on, and the leader learns who holds it from `FeatureHeld` (`feature.rs`). So the leader starts giving frames positions once its own record holds `FrameLog` and every configured voter is in its holders set (`Features::holders`).
    - A frame carries its position, so a follower journals the frame record by what the frame says, not by when its own record became durable.
  - 4b: a recovery report adds each accepted frame above the voter's commit frontier as (position, ballot, digest), the leader's own included, and a Sync names frames the same way. Recovery selects both ways, by 4.9's rules over the per-command records and by 10.3's source rule over the frames, and checks they agree.
- **Every frame carries every payload from 4a (R4).** Today a frame carries a payload only for a relayed command.
  - Under `FrameLog`, a fast command's payload also crosses the wire in the frame, a second time, below the relay's threshold.
  - So the records step 4 writes are the ones step 5 reads, and step 4's check speaks for step 5.
  - B1W counts the second crossing, at 1 and 5 callers, where it falls.
- **When a follower writes a frame record (R2).** It writes frame f's record once it has adopted every entry of f and written f − 1's.
  - So a voter's frame records are in position order with no gap, and its report's frames are a prefix, as 10.3 needs.
  - It still adopts entries out of order, as today. Frame f + 1's entries are adopted while f is missing, and f + 1's record waits for f's.
- **Where the record goes (R1).** It goes in a batch of its own, after the frame's adoption batches.
  - A frame record is at most a 2 MiB request and its framing, under the record's 4 MiB. It never shares a batch with the per-command rows, whose payload rows hold the same bytes again.
  - The journal keeps a prefix, so a durable frame record means every adoption before it is durable.
- **What "agree" means (R2).** Take the commands in frames at `FrameLog` ballots, less those both selections take by the possible-fast rule. Let C be what the per-command rule selects, and F what the frame rule selects, in frame order. They agree when:
  1. every command of F is in C, and the dependencies C's records give it are the conflicting commands before it in F's order;
  2. every command in C and not in F is in a frame above F's last, which some reporter in the majority adopted without reporting the frame: there is a gap below it at that reporter, or the frame's record isn't durable there yet.
  - Clause 2 is what step 4 allows and step 5 ends. At step 4 a follower votes for an entry as it adopts it, so the leader can decide a command by its votes before any majority holds its frame whole. At step 5 a follower acknowledges only a prefix of whole frames.
  - With no reporter in that state, C and F are equal.
- **What recovery reads:** it decides from the per-command records, as today. The frame selection is computed and compared, and a new leader fetches a selected frame it lacks (step 3) to know its commands.
- **A disagreement fails the run (R11).** That holds in the simulator's oracle, the crash matrix and the runner's fault runs, whose verdict reads the count (testing's). It is never only logged.
- **Mixed versions:** `FrameLog` activates only once every voter runs step 3's release, and a domain doesn't go back below it. The rows above apply.
  - In production it activates with `FrameDecision`, not alone. Alone it pays the frame's bytes a second time at the appender and on the wire, and saves nothing.
  - Alone it runs in the simulator, the crash matrix and the runner's fault runs.
- **Pinned by:**
  - `protocol_sim` rows at three and five voters over rows 1, 9, 21 and 23, each of 10.6's cases a row, with the oracle that the two selections agree at every Sync;
  - review's case on #276 (L0, L, M1, M2 and Y, 10.3) as a pinned seed: the case where a slot rule would select b0's frames;
  - the model test that a decided frame is selected whole by every later Sync;
  - the crash matrix's cuts at a frame record: torn, durable but not acknowledged, and between the frame's last adoption batch and its record;
  - **replacing an unselected frame (R3).** A voter holds b0's frame at p durably, then takes b2's frame at p, and restarts both between the two records and after both:
    - after both, replay ends with b2's frame at p;
    - between them, it ends with b0's, and b2's leader re-sends.
  - the lone 2 MiB request under `FrameLog`: its adoption batch and its frame record each under the record's bounds (R1).
- **Bound:** B1W against B1, one pair: the cost `FrameLog` adds.
- **Checkpoint:** inactive, a null step. Active, no more than that bound, and no disagreement in the rows above. Then the drift pair against the carry before step 1.

**Step 5: the frame decides.** `FrameDecision`, which requires `FrameLog`. Each PR lands dark, and the build knows the feature only from 5c. It starts only past the go/no-go above.
- **The boundary is a frame position the leader chooses (R6; Codex).** task-d99's activation doesn't reach the order: each voter's own durable record turns a feature on, at its own time, and the leader learns who holds it from `FeatureHeld`. So no frame holds the activation, and a frame's rule can't depend on when a voter's record became durable.
  - **Each frame says which it is:** a bit under its digest. The leader sets it on every frame it cuts once its own record holds `FrameDecision` and every configured voter is in its holders set. The first flagged frame in the chain is at position a. Every frame from a on is decided as a frame, and every frame below a per command.
  - **So the chain records the boundary.** Every voter that holds a frame holds its rule, durably. A follower needn't know when anyone's activation became durable, or hold frame a itself, to know a frame's rule.
  - **A follower takes a flagged frame whole and in order,** and an unflagged one entry by entry, as at step 4.
  - **Flagged frames are a suffix of the chain:** a frame whose predecessor is flagged is flagged. Recovery and the oracle check it, and a frame that breaks it is refused.
  - **A new leader** whose selected chain ends in a flagged frame flags every frame it cuts. One whose chain ends unflagged flags again by the rule above.
  - **A selection is a prefix of frames,** so a takeover that doesn't select a selects nothing above it, and the boundary moves to a later frame.
  - **A voter whose own record doesn't hold `FrameDecision` doesn't vote on a flagged frame,** as for a frame it can't take, until the re-send activates it. The leader flags only with every configured voter holding the feature, so this is a voter added after the activation.
- **PRs:**
  - 5a: recovery and Sync select frame commands by frames, under the source rule with frames as its entries (10.3).
    - A new leader fetches any selected frame it lacks (step 3), then re-proposes the selected frames whole, at its ballot and in order, before new ones.
    - Fast commands keep their records and the possible-fast rule.
  - **Every voter reports frame commands by frames, the deposed leader included (R5).** The leader journals each frame from 4a, so from 5a its report names its frame commands by frames, as a follower's does.
    - Its per-command records stay until step 7, for its own use, and are no longer reported for frame commands.
    - A selection never reads both kinds of report for one command.
  - 5b: a follower's frame record is its only record for a frame command: no dependency row, adoption record, payload row or vote state. It acknowledges a prefix of frames with `FrameAccept { replica, ballot, through }` once their records are durable.
  - 5c: the leader counts a majority a frame, `Committed { through }` counts frame positions, and the build declares the feature.
- **What recovery reads:**
  - for a frame command, frames, from every reporter;
  - for a fast command, its records, as today;
  - for a command in a frame below a, its per-command records. Reports carry them beside frames until the voter's commit frontier reaches a.
- **Mixed versions:** it activates through task-d99, once every voter runs 5c's release, and never goes back. The boundary then follows by the leader's rule above. The rows above apply.
- **Pinned by:**
  - step 4's rows with `FrameDecision` active, the oracle one order everywhere;
  - **the boundary (R6):**
    - the activation in a running domain over old records, with frame a cut only once every voter holds it;
    - the leader ahead of a follower across it, the follower at a − 3 when the leader cuts a + 2;
    - a takeover that doesn't select frame a;
    - a new leader whose selected chain ends flagged, which keeps flagging;
    - a voter added after the activation, which votes on no flagged frame until the re-send activates it;
  - conflicting fast and frame commands;
  - a follower frames behind, waiting for the re-send;
  - **a voter that rejoins across the boundary (R7),** its execution frontier below a and its donor's above:
    - the donor's pages (task-d25) for frame commands come from its frame records for the payloads, and from the order for the dependencies (a page entry's `decided` is `None`, as for a command the donor keeps no row for);
    - a reinstall from a checkpoint above a replays frame records;
  - **a new leader that lacks a selected frame (R7)** fetches it before re-proposing it;
  - the replacement cut of step 4, with no per-command records beside the frame records (R3);
  - the crash matrix at a frame record with no per-command records beside it;
  - the runner's fault runs (testing's): the leader killed or partitioned under load, a follower restarted mid-frame, and the activation in a running domain.
- **Bound:** B2 − B1, one pair. Today's −0.26 [−0.38] is an upper bound: a residual of separate pairs.
- **Checkpoint:** half of B2 − B1 as five voters' CPU a command, locally at 120 and 30 callers, with the latency rule. Then testing's pair on the 7763 against the carry before it.

**Step 6: learning by frame position, forgetting by range.** This is lever 2's step 2 (6.7) with frames.
- **One PR:**
  - `learn` and the closure walk run once an execution group;
  - forgetting goes by range, up to the durable execution frontier;
  - the commit frontier is read by position, where #269 reads it by sequence number.
- **What recovery reads:** as at step 5. No wire change.
- **Pinned by:**
  - the counts: passes over the ACCEPT set a frame, and forgets a group;
  - the simulator's oracle that a command is forgotten only once its execution is durable (task-d90).
- **Bound:** a profiled pair's share of `learn`, `forget_history` and `Learner::established`, as µs a command, before it's built. **Checkpoint:** half of that.

**Step 7: the leader's own per-command records go.** For frame commands, the leader's per-command dependency rows and records go, as the followers' did at step 5 (27 + 13.5 + 16.8 µs a command, 10.5).
- **One PR.** The leader's report already comes from its frame log from 5a (R5), so this changes what it writes, not what it reports.
- **What recovery reads:** frames, at every voter.
- **Mixed versions:** under `FrameDecision`.
- **Pinned by:** step 5's rows, with the leader crashing at each cut.
- **Bound:** B3. **Checkpoint:** half of B3, in the leader's loop.

**Weeks.** The total is about 11 to 15, against 10.6's 10 to 14 with the design step done. The batch split, the frame fetch, the boundary bit and the mixed-version rows add about a week across steps 2 to 4, and step 7 loses its second PR.

| Step | Weeks |
| --- | ---: |
| 0 | under 1 |
| 1 | 1 |
| 2 | 1 to 2 |
| 3 | 1 to 2 |
| 4 | 2 to 3 |
| 5 | 3 to 4 |
| 6 | 1 |
| 7 | 1 |

- The runner's runs take 1 to 2, alongside step 5.
- The simulator, model and crash-matrix work is inside the steps, since each step pins its own.

### 10.8 The named-cost queue against the frame work

The lead's question, item by item (#185, 21:05): does the frame work subsume it? The items that survive and are cheap are done. The rest are dropped here, each with why.

| Item | Subsumed? | Why | What happens |
| --- | --- | --- | --- |
| The intake index (#282) | No | The leader keeps its per-command rows until step 7, and fast commands keep theirs. The executed part answers the retry window at every voter. Step 5 shrinks a follower's intake part, not the leader's. | Kept: #282, merged. Its own measurement: −34 to −63% a command. The end-to-end pair doesn't see it. Its profiled pair does: the intake code in the materializer falls by 5.6 µs a command at the leader (−33%) and 4.0 at a follower (−26%), each more than the base runs' spread. |
| The journal verified once, with each record's encoded length stored (`claude/journal-verify-once`) | No | Every record is still sealed or decoded, the frame record included, and a frame's record is the largest | Kept: #284, merged. The part it moves is the appender, about −6 µs a command at each voter: −36% at the leader and −38% at a follower at 120 callers, −30% and −29% at 30. Five voters' total at 120, −67 µs a command, is inside what a pair sees (base runs of 1,319 to 1,398), so it isn't a reading of it. |
| The small items: schedstat through one held descriptor, `is_leading` read once, encode buffers sized once (`claude/small-costs`) | No | Loop housekeeping and encoding, the same with frames | Kept. Built; its suites, microbench and profiled pair run after step 0's pairs. |
| The storage step with #269: lever 2's step 4, one batch and one barrier a frame | Yes | It is step 2b, built once in the form steps 4 and 5 extend: the frame's batch is where the frame record goes | Dropped as an item of its own. #269 itself, lever 2's step 1, is kept: learning once a frame doesn't wait for step 6. |
| The journal's format: one record a group | No | An application outcome belongs to an execution group, at every voter, the leader included, with frames or without | Kept: it is step 1. The settle is step 2's (10.7). |

**Lever 2's vote message by range (6.7, step 5): dropped.** Step 5b's `FrameAccept` is the frame's acknowledgement, all or none, with no exceptions to list. Between steps 2 and 5, votes stay one a command, in one hand-off a turn.
