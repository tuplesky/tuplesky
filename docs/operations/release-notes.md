# Release notes

What an operator should know about a build before running it. The
newest entry comes first.

## Alpha: durability

Acknowledged writes survive process crashes and modeled power loss (fsync semantics, tested with a fault-injecting filesystem). Not validated with real power cuts, lying storage devices or on-disk bit rot. Damage found at recovery is refused rather than repaired, and the node recovers from its peers. No background scrubbing.

## Alpha: a command whose payload is lost with its holders (task-d121)

When every voter that held a command's payload is lost for good, a
domain without the rule can stop electing a leader: a campaign whose
selection must carry the command gives up on it, or waits for a voter
that never answers. An election still succeeds where the surviving
selection has nothing to carry for that command. With the rule, the
voters left usually decide such a command as nothing and lead, but not
always:

- **The rule stays off where the order check can't read the order.**
  Where a selection's order check dropped a possible fast decision
  behind a dependency no report holds, the rule stays off for that
  selection. Such a decision may have been made fast, under dependencies
  the check couldn't read. A surviving majority then still gives up or
  waits, as without the rule.
- **The domain can stay without a leader after the loss.** If every
  selection after the loss meets that case, no voter leads again until
  the lost storage, or every voter, is restored. The simulator finds
  such runs (seeds 363 and 582, after the only holder is destroyed).

Design [4.9](../design/tuplesky-design.md#s4-9) states the rule and this
limit.

- **Turning it on.** The rule is a feature, on in a domain where both
  `release-at-execution` and `decided-nothing` are active. There are two
  ways:
  - its genesis manifest names both (its `features`);
  - a running domain activates them live while every voter is up:
    `coordctl feature activate release-at-execution`, then
    `coordctl feature activate decided-nothing`. The second is refused
    until the first is active.

  Either way it needs every voter, so it can't be switched on after the
  loss. `coordctl feature status` shows a running domain's active set. A
  domain with neither active runs without the rule.
- **What a caller sees.** The command takes effect as nothing. Its
  submitter gets a definite refusal, `decided-nothing`, once the
  nothing is decided, never before. A retry of the command meets the
  same refusal once the nothing is committed. Before that it is held,
  and it is answered with the command's release.
- **A wiped voter is caught only where another voter recorded it.**
  Each voter writes a random store identity at `init`. A voter refuses
  a peer, or a candidate, that comes back under another identity than
  the one it recorded. A candidate whose store was wiped before any
  voter the campaign needs had recorded its identity is not caught.
  A voter restored alone from an old copy of its own store carries
  that store's identity, and no check finds it (design
  [5.4](../design/tuplesky-design.md#s5-4)). Restore every voter from
  the same backup, as the disaster-recovery runbook says.
- **The manifest stands in for every voter preparing the feature.**
  In a domain whose genesis names the feature, every voter writes the
  same activation record from the signed manifest at `init`, and no
  activation round runs. A domain whose genesis doesn't name the
  feature activates it live, as above.

## Alpha: clients are trusted (#312 finding 2)

Alpha assumes trusted, authenticated clients. A bound client that stops
reading its connection can stall the domain loop for about 10 s for
each frame it blocks. The stall clears when the write times out, or on a
restart. Connections without a client certificate are bounded before
they bind (#334).

## Alpha: a voter's policy is bounded in staleness (#320)

A voter's frontend answers bindings, output and watches from its own
store's policy only while it has confirmed, within an allowance, that
its store holds everything the domain had committed. Design
[6.4](../design/tuplesky-design.md#s6-4) states the rule.

- **Setting.** `[limits] max_policy_staleness_millis`, 5,000 by
  default, 100 to 60,000. The voter asks the leader every half
  allowance.
- **What a stale node does.** It refuses every bind before it reads the
  credential. The connection closes as `Rejected` with the reason
  "bind unavailable", and the kine adapter returns gRPC `Unavailable`.
  Clients keep their credential and bind again, later or at another
  frontend. Output waits as pending and watches close as source-lost.
  A node is stale at boot until its first ask is confirmed, after a
  heal until it has caught up, and whenever it is cut off from a
  quorum for longer than the allowance.
- **A process with no voter** (`role = "frontend"`) is never fresh and
  refuses every bind.
- **Suspend.** The allowance runs on the monotonic clock, which does
  not advance while a host is suspended or a VM frozen. A voter resumed
  after a freeze answers for at most what was left of one allowance
  (5 s by default), then stops until a new ask is confirmed. Alpha
  accepts this; it has no clock that counts suspended time.
- **No rolling upgrade from a build without the bound.** A build before
  #320 does not know the ask's frame (`0x0108`) and closes the
  connection it arrives on. A voter with the bound under such a leader
  stays stale and refuses every bind, which fails closed. A voter
  without the bound answers from its own store as before, with no
  bound. Stop every voter, then start them all on the new build.
