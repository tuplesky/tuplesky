# Release notes

What an operator should know about a build before running it. The
newest entry comes first.

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
