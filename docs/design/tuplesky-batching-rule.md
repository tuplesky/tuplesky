# The single domain under the batching rule

**Status:** design step only ([task-d115](tuplesky-prs-plan.md#task-d115)), no code. It covers:
- the audit against the owner's rule;
- the cost model, per command and per turn;
- the proposals that follow from the rule;
- levers 1 and 2 (option 3 and the proposal frame as a unit) and the leader-log end state they lead to.

What can be built without deciding the end state is in [Section 6.4](#64-what-can-be-built-without-deciding). The owner has parked option 3 and the leader log ([Section 8](#8-order-and-what-the-owner-decides)).

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
- **The knockout pays nothing for what the design adds back.** task-d117's design step (#258) adds 20 to 30 µs a command over five voters, for the recovery map, the history buffer and the retry window's versions, so net −155 to −165. Section 7 keeps the gross until that step's pair.
- **The retry rows go only with their window in memory.** Review's first knockout dropped them without it: each command's record carried 63 retry updates in place of 2.5, and the run slowed. That orders task-d117's steps.

**Per command to per turn?** No. It removes the per-command rows. What is left, 2 application rows a command, stays per command.

**What it supersedes, for the rows that leave.**
- task-d113's keying of those rows by order;
- task-d103's proposal row;
- task-d104's retry rows. Its window in memory agrees with the rule and becomes part of this proposal.

**Build.** A design step for the recovery readers, then about 3 to 4 weeks.

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
- **If the leader executed ahead of the decision** (Section 6.1), execution thrown away at each leader change, and a speculative overlay held at every leader. The end state above doesn't execute ahead, so it doesn't give this up.

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
- **Roles follow the ballot.** The leader and the next `e` voters in the ballot's order are eager; the rest are lazy. A voter that becomes eager first executes its tail (below).

**1. What a lazy voter must hold.**
- **To vote and detect conflicts: keys, not state.**
  - A dependency is a conflict on a key. The voter needs each live command's key set (from its payload), each key's latest command, the command table and the journal: what the machine holds today.
  - Intake reads nothing from the store (Section 2's audit), so no vote needs a projection.
  - A conditional write's outcome depends on state, but its conflicts don't: it conflicts on its keys whatever it finds there.
- **To serve its frontend:**
  - **Writes** are answered from the release the leader sends (task-d100's relay), as today. The lazy voter's own execution isn't on that path.
  - **Retries** go through consensus as today. A retry's dedup happens at execution (`retry::admit`), so exactly-once state lives in the eager voters' retry windows. The lazy voter's window lags, and nothing reads it above its landed frontier.
  - **Reads, watches and resolves** need current state. A lazy voter's frontend forwards them to an eager voter. That costs a hop and, on the 7763, about +0.003 ms an operation a lazy frontend (the bar's 34% reads, a fifth of them at that frontend, at about 20 µs of tokio a forwarded read). Total read work doesn't change: each read is still answered once.
- **To be a catch-up donor:**
  - the protocol part (order, payloads, decided records) from its journal and task-d117's segments, to any asker;
  - executed history, with each position's result, up to its landed frontier. Above that, an eager voter serves.

**2. Where its state comes from: three ways.**
- **(a) Its own execution, behind, in large batches.**
  - **No trust:** it executes every command itself, as today, only later.
  - **It saves almost nothing in CPU.** Execution stays per command. What a batch saves is the per-turn and per-commit costs (about 3 µs a command locally), and current rows that a batch writes twice (below).
  - So it is the fallback, not the plan.
- **(b) The eager voters' batches, agreed, landed behind.**
  - At each batch's end (point 3 sizes the batch), each eager voter sends the lazy voters its history digest through that position (task-d117, point 3). The eager follower ships each lazy voter the batch: the merged write set and each position's execution record.
  - A lazy voter lands a batch only once at least two executions agree on its digest. It lands it as one commit, sorted.
  - It executes the batch itself, as in (a), when:
    - no agreement comes within a bound (for example, too few eager voters are up);
    - or the digests differ, in which case its execution counts toward the majority that decides.
  - **Trust:** the agreement of two independent executions of the same order and payloads, with the lazy voters as tie-breakers. The leader's write set alone isn't trusted. That is the difference from option 3, where followers land the leader's write sets on the hot path (Section 6.3).
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
| Forwarded reads (7763) | +0.003 ms an operation | +0.003 ms an operation |
| **Net, 7763, a lazy voter** | **−0.004 to −0.005 ms an operation** | **−0.033 to −0.035 ms an operation** |

**The merged current rows.**
- In the bench, half the commands are puts over 10,000 keys and half are contended writes on 16 hot keys. A batch writes each key's current row once.
- So current rows a command fall from 1 to about 0.48 at a batch of 2,400 commands, 0.42 at 7,200 and 0.29 at 24,000.
- At about 8 µs a row in the materializer locally, that is 4.3 to 5.9 µs a command a lazy voter.

**The curve.** On the 7763 at 120 callers, with `R` the commands a second:
- `R` is taken as 2,400, the local rate; the 7763 runs fewer.
- The tail executes at about 14,000 commands a second. That is 55.5 µs of execution and 16 of merged materialization a command on the 7763, inferred.

| Lag or batch interval | Batch, commands | Recovery at takeover: the tail | (a), 4 lazy voters | (b), `e = 2` (2 lazy) | (b), `e = 1` (3 lazy) |
|---|---:|---:|---:|---:|---:|
| 1 s | 2,400 | 0.17 s | 1.19 | 1.14 | 1.10 |
| 3 s | 7,200 | 0.51 s | 1.19 | 1.14 | 1.10 |
| 10 s | 24,000 | 1.7 s | 1.19 | 1.14 | 1.10 |
| 250 positions, a quarter of the table (recommended) | 250 | 0.02 s; 0.04 s at the window's edge | 1.19 | 1.14 | 1.10 |

The last three columns are the servers' ms an operation, from 1.20. The rows by time need a batch larger than a quarter of today's largest table. Such a table changes the recovery reports' bound (task-d20), so those rows only show how the CPU moves with the interval.

**What the curve says:**
- **The CPU is nearly flat in the interval.** A longer batch merges more current rows, but from 250 positions to 24,000 the difference is under 0.01 ms an operation.
- **Recovery is linear in it.** So the interval should be short.
- **Batches by positions** bound the tail at any load, and the command table bounds the batch from above: a quarter of it.
- **The choice that moves the CPU is `e`:** −0.01 for (a), −0.07 at `e = 2` and −0.10 at `e = 1`.
- **It adds to proposals 1 and 2, the appender's encode and the levers that need no decision.** With lever 2's redesign (Section 6.7) it overlaps in `Follower::applied`, which both take part of: about 5 µs a lazy voter, counted once.
- **Every figure is inferred** from the cost table's shares, not from a pair.

**Against the alternatives.**
- **Option 3 with rotation (Section 7's lever 1):** −0.03 on the 7763 with `Follower::applied` counted once, with one execution a command and rotation for detection. (b) at `e = 1` saves more, −0.10, keeps two executions and the lazy voters' tie-break, keeps the fast path, and needs no leader log.
- **Witnesses (Section 6.5):** they can't lead without a rebuild. A lazy voter can, within the tail's bound, which is the owner's condition.
- **Copies of the store:** a lazy voter keeps a full one, behind by at most a batch. So the domain keeps five copies, as etcd's five members do; with witnesses it keeps three.

**6. What it costs to build.** About 5 to 7 weeks, after proposals 1 and 2 (task-d117), which it rests on: history digests and segments, and execution from memory.
- **Roles from the ballot,** and an election that prefers eager voters.
- **The batch:** a quarter of the table's positions, as a peer frame with the merged write set and the execution records. The eager follower sends it, so the leader's load doesn't change.
- **The agreement:** each eager voter's history digest through each batch's last position, in a small peer frame to the lazy voters.
- **The landing:** a batch lowered as one sorted commit.
- **The fallbacks:** own execution when there is no agreement within a bound, on a difference, and past the window.
- **The window:** the lag tracked, and the campaign's second timeout past it.
- **The takeover:** execute the tail before answering.
- **The frontend:** forwarding reads, watches and resolves to an eager voter.
- **The donor rule:** a lazy voter serves executed history only up to its landed frontier.
- **The metrics line:** lag in positions, tail and takeover times, agreements, fallbacks.

**It changes who executes, so it is the owner's to decide,** beside the parked leader log, option 3 and witnesses. It doesn't change what is decided: votes, dependencies, the fast path and recovery are as today.

**7. The fault runs it needs** (testing's, on the runner):
- **A lazy voter wins an election and serves within the bound:**
  - kill the leader and the eager follower together under load;
  - read the time from the kill to the first answer: the election, the Sync and the tail. The tail should be at most about 0.02 s at 250 positions.
- **A restarted lazy voter:** it boots from its image and journal (task-d117), resumes landing, and is caught up by donors.
- **Two in a row:**
  - kill the eager voters, then the lazy voter that took over once it serves;
  - a second lazy voter takes over within the same bound;
  - the digests agree at the next batch's end.
- **A difference at one eager voter,** in a build that changes one result: the lazy voters execute the batch, the majority decides, and the loser stops.
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
| The adoption's durability | A vote leaves only once what it claims is durable ([design Section 4.8](tuplesky-design.md#s4-8)). Each command's dependencies are its own. | One `PersistBatch`, one barrier, one `pending` entry and one deferred vote per command. `payload_row` scans the in-flight batches twice. `carry_unwritten` walks the dependencies, and `ledger.written` scans the staged batches. | One batch a frame, holding each adopted command's dependency row and payload row. One barrier and one `pending` entry name the frame's adoptions. `ledger.stage` already takes several records a barrier. The payload rows in flight are looked up through an index, not scanned. |
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
- **Its prerequisites:** the frame's one barrier, and its payload rows. So the votes it carries are durable before it leaves, as each `SlowAck` is today.
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
- At `e = 1` together they come to about 1.05 on the 7763: 1.13, less 0.097 for three lazy voters, plus 0.016 for the overlap. With the transport on the loop's turn, about 0.98.

**Code steps, each its own PR:**
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
- **Lever 4 (#253, merged):** −0.049 ms an operation, on a saturated host. The model has −0.020 locally.
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
- **Lazy voters (Section 6.6)** at `e = 1` take the Lever 3 row to 1.10. With lever 2 redesigned, about 1.05, and with the transport too, about 0.98, counting `Follower::applied` once.
- **Against the owner's exit,** which is completions (Section 8), parity needs the servers at about 0.80 to 0.87. No modeled row reaches it at the measured turn.

Every figure but the measured ones above is modeled from counts and profile shares, not measured as a pair. Each step's acceptance reads its own pair.

## 8. Order, and what the owner decides

**The exit is completions,** meeting etcd's, as the lead has relayed the owner's bar. Section 7 reads servers' CPU against etcd's 0.887. In the owner's terms (review on #252, [6837290600](https://github.com/tuplesky/tuplesky/pull/252#issuecomment-6837290600)):
- **On the 7763's four CPUs, completions follow the whole box's CPU an operation.** Summing testing's columns at 120 callers ([#98, 6833683813](https://github.com/tuplesky/tuplesky/pull/98#issuecomment-6833683813) and [6835373016](https://github.com/tuplesky/tuplesky/pull/98#issuecomment-6835373016)) gives 2.09 ms for us and 1.30 for etcd. That predicts 0.63× etcd's completions, and the sweep read 0.66×. At 30 callers it predicts 0.71×, and the sweep read 0.72×.
- **Our client side and kernel cost 0.50 against etcd's 0.42.** So completions parity at 120 callers needs the servers at about 0.80 to 0.87, a little under 0.887.
- **No modeled row reaches it at the measured turn** (Section 7):
  - without a decision, 1.20, and 1.13 with lever 2 redesigned (Section 6.7);
  - lazy voters at `e = 1`, 1.10;
  - the leader-log end state, 0.98, and 0.91 with the transport on the loop's turn;
  - lazy voters with lever 2 redesigned, about 1.05, and with the transport too, about 0.98.
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
- **Lazy voters (Section 6.6):** voters that vote on the hot path and land the eager voters' agreed batches behind, at a lag of one batch, a quarter of the command table. Any of them can lead after executing its tail: one batch in steady state, and at most an enforced window of two while any voter within it is up. At one eager follower beside the leader, about −0.10 ms an operation on the 7763. The domain keeps five copies of the store, three of them behind.
- **Whether the transport moves onto the loop's turn,** once lever 8 is on the carry.
- **If witnesses are reopened:** whether three copies of five may be read against etcd's five (Section 6.5).
