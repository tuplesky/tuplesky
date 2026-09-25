# Running a test domain across hosts

This is the runbook behind [task-d04](../design/tuplesky-prs-plan.md#task-d04):
three voters on three machines, provisioned by the test harness, with the
Kubernetes storage edge and the certification suite driven from a fourth (or
from one of the three). It is a test procedure. The credentials are fixture
credentials, the credential endpoint signs on presentation of any assertion,
and nothing here is a way to provision a production domain -- that remains
task-42, task-43 and task-65's.

`coordd` itself has no single-host assumption. It binds what its
configuration says, learns its peers from the signed endpoint catalog -- IP
literals or DNS names -- and verifies each peer's certificate against the host
part of the address it dialled, then decides which voter answered from the
node-identity URI in that certificate. What was single-host was the harness:
loopback catalog entries and listeners, certificates valid only for
`127.0.0.1`, absolute paths into one run directory, and a credential endpoint
that would bind nothing else. `coord-harness provision --hosts` removes those
assumptions from the harness and changes nothing in the daemon.

## What runs where

| Host | Runs | Listens |
| --- | --- | --- |
| voter host 1 (`n1`) | `coordd` from the `n1/` bundle | UDP api port, UDP peer port |
| voter host 2 (`n2`) | `coordd` from the `n2/` bundle | UDP api port, UDP peer port |
| voter host 3 (`n3`) | `coordd` from the `n3/` bundle | UDP api port, UDP peer port |
| client host | the run directory, `coord-harness issuer`, `kine-coord`, the certification suite | TCP issuer port, TCP edge port |

The client host may be one of the voter hosts. It is where the domain is
provisioned and where the run directory stays: `harness.json` and the data
source Kine is given name files by their absolute path on this host.

The examples below use `10.0.0.1`, `10.0.0.2` and `10.0.0.3` for the voters,
`10.0.0.9` for the client host, and ports 7001 (api), 7002 (peer), 7443
(issuer) and 2379 (edge). Any addresses and free ports will do; a host may be
a DNS name instead of an address.

## 1. Build

On a machine of the same architecture and C library as the hosts:

```text
cargo build --locked --release -p coordd -p coord-harness
(cd adapters/kine && go build -o ../../target/release/kine-coord ./cmd/kine-coord)
```

Copy `target/release/coordd` and `target/release/coord-harness` to every voter
host, and all three binaries to the client host. The certification suite runs
from a checkout of this repository with Go installed; it can be the client host
or any host that can reach the edge.

## 2. Provision, on the client host

```text
coord-harness provision --dir /srv/tuplesky/run \
  --hosts n1=10.0.0.1:7001:7002,n2=10.0.0.2:7001:7002,n3=10.0.0.3:7001:7002 \
  --issuer-listen 10.0.0.9:7443 \
  --edge-host 10.0.0.9 --edge-port 2379
```

What `--hosts` changes, and nothing else does:

* **The catalog** lists each voter at the address given, IPv6 literals in
  brackets (`n1=[fd00::1]:7001:7002`). It is signed by voter 1, as before.
* **Each `coordd.toml`** listens on the given fixed ports. It binds the
  host's own address when the host is an IP literal, so a node never answers
  on an interface it was not placed on and several "hosts" can share one
  machine's loopback range. It binds `0.0.0.0` (or `[::]`) for a DNS name,
  because a strict configuration's listener is an address and not a name.
  Pass `--listen-any` when a listed address is not on any of the host's
  interfaces -- a cloud instance's public address behind one-to-one NAT --
  and every non-loopback listener binds the unspecified address instead. The
  QUIC stack answers each datagram from the address it arrived at, so a
  wildcard listener is still reached at the listed address.
* **Node and collector certificates** carry the host -- an IP SAN for an
  address, a DNS SAN for a name -- beside the node-identity URI, in place of
  `127.0.0.1`. Each is issued once, so the key the genesis commits is the key
  the leaf holds. A peer dials the host from the catalog and verifies the
  certificate against it; without the host in the certificate every
  handshake fails.
* **Each `nN/` directory is a self-contained bundle.** It holds its own copy
  of `genesis.json` (the manifest as the admin signed it), the admin's public
  key `genesis-admin.pem` a node verifies it against at `init` and at every
  start, and `endpoints.bin` beside its credentials, and its
  `coordd.toml` names every file relative to the bundle, with
  `state_directory = "."`. `coordd` opens a relative path against its
  *working directory*, not against the configuration file -- this task did
  not change that -- so a bundle is run from inside itself. Copy it anywhere;
  the examples use `/opt/tuplesky/nN`.
* **The credential endpoint** (`--issuer-listen`) gets a certificate for its
  host and a URL naming it, so a Kine build on another host can exchange its
  assertion there. Without the flag it stays on loopback. `localhost` (and any
  name under it) is refused as that host, because it only reaches the machine
  it is resolved on; leave the flag out for a loopback endpoint.
* **The storage edge** (`--edge-host`) gets a server certificate for its host
  and an endpoint URL naming it. An API server verifies the edge against the
  host of the endpoint it is configured with unless told otherwise.

Without `--hosts`, `--issuer-listen` and `--edge-host`, provisioning writes
exactly what it always has; `crates/coord-harness/tests/provision.rs` pins
that.

A genesis commits its voters by key. Re-provisioning makes a new domain with
new keys, and every bundle has to be copied again; a node is never
re-initialized on its own.

## 3. Copy the bundles

```text
scp -r /srv/tuplesky/run/n1 10.0.0.1:/opt/tuplesky/
scp -r /srv/tuplesky/run/n2 10.0.0.2:/opt/tuplesky/
scp -r /srv/tuplesky/run/n3 10.0.0.3:/opt/tuplesky/
```

Keep the key files readable only by the user that runs `coordd` (the harness
writes them `0600`; `rsync -a` preserves that). A bundle carries its own
node's private keys and no other node's.

## 4. Open the ports

| Port | Protocol | On | From |
| --- | --- | --- | --- |
| peer (7002) | UDP | every voter host | the other two voter hosts |
| api (7001) | UDP | every voter host | the other two voter hosts (collector links), and the client host for voter 1 |
| issuer (7443) | TCP | client host | the host running `kine-coord` (loopback when it is the client host) |
| edge (2379) | TCP | client host | the host running the certification suite or the API server |

Both planes are QUIC, so both are UDP; there is no TCP port on a voter. Kine
reaches the domain at one voter's api port -- voter 1's by default, or the one
`coord-harness dsn --voter N` names.

## 5. Synchronize time

A frontend refuses a service token whose `iat` is later than its own clock
plus five seconds (`CLOCK_UNCERTAINTY_SECONDS` in `bins/coordd/src/serve.rs`),
and treats one that expires within five seconds of its clock as expired. The
issuer host's clock and every voter host's clock therefore have to agree to
well within five seconds: run NTP or chrony on every host and check
`chronyc tracking` or `timedatectl` before starting. A skewed issuer shows up
as Kine failing to bind a session, not as a network error. The harness
certificates are valid from 1975 to 4096, so certificate time does not
constrain this.

## 6. Start the voters

On each voter host, with `N` its voter number:

```text
coord-harness start --dir /opt/tuplesky --node N --coordd /usr/local/bin/coordd
```

`start` runs `coordd init` from the bundle if it has no store yet (and never
otherwise), starts `coordd` from the bundle directory, waits for it to report
`coordd phase=live`, prints

```text
harness node-ready node=nN pid=PID api=10.0.0.N:7001 output=/opt/tuplesky/nN/coordd.log
```

and stays in the foreground until the daemon exits. The daemon's process
identifier is written to `nN/coordd.pid`, and everything the daemon says is
appended to `nN/coordd.log`, across restarts. Stopping `start` itself does not
stop the daemon: it does not forward signals, so a supervisor that stops the
wrapper leaves `coordd` running. Stop the daemon by its process identifier
(step 11).

Without the harness binary, the same thing by hand, sending both of the
daemon's streams to the log the next step reads (the mesh lines go to stderr,
`coordd phase=live` to stdout):

```text
cd /opt/tuplesky/nN
coordd --config coordd.toml init                   # once; refuses a store that exists
coordd --config coordd.toml >> coordd.log 2>&1 &   # ready when the log says `coordd phase=live`
```

Start order between voters does not matter: every voter re-dials the others
on a timer (task-d03), so a voter started first finds the others as they
arrive. The domain establishes requests once a majority -- two -- are up and
linked, and voter 1 is one of them (see the limits below).

## 7. Check the mesh

On each voter host:

```text
grep -E '^(coordd phase=|peers connected=|voters submittable=)' /opt/tuplesky/nN/coordd.log
```

A full mesh ends with both of

```text
peers connected=2 of 2 attempts=A bulk=2
voters submittable=2 of 2 attempts=A
```

`peers connected=` is the peer plane (voter-to-voter consensus traffic);
`voters submittable=` is this node's collector links over the api plane to the
voters it submits to. Each line is printed when its count changes, so the last
one is current. `attempts=` counts dials; it keeps rising, slowly, only while
a voter is unreachable. A count stuck at 0 on every node is almost always a
firewall or a certificate that does not carry the address the catalog lists.

## 8. Start the credential endpoint and the storage edge, on the client host

```text
RUN=/srv/tuplesky/run
coord-harness issuer --dir $RUN &          # prints `issuer listening 10.0.0.9:7443`
```

The endpoint signs a token for any non-empty assertion, so where it listens is
its whole containment. What is enforced: it binds a loopback address always.
Off loopback it binds only for a domain provisioned with a non-loopback
`--issuer-listen` host, only on the port the provisioned URL names, and only at
that host's own address or at the unspecified address (`0.0.0.0` or `[::]`).
The endpoint's certificate names the issuer and exactly one host it is reached
at, and the URL's host has to be that host. The issuer's own name
`sts.tuplesky.harness`, which every issuer certificate carries, is not a host,
and a host that only means loopback (a loopback address or `localhost`) never
lets it bind anything else. So editing `harness.json` does not widen it. What is not enforced:
which interfaces the machine has. The unspecified address listens on all of
them, so use it only where the provisioned address is not on an interface
(behind NAT, say), and firewall the port.

```text
kine-coord \
  -endpoint "$(coord-harness dsn --dir $RUN)" \
  -ca-file $RUN/roots.pem \
  -listener tls://10.0.0.9:2379 \
  -server-cert-file $RUN/edge-server.pem -server-key-file $RUN/edge-server.key \
  -client-ca-file $RUN/edge-client-ca.pem \
  -allowed-client kube-apiserver.tuplesky.harness &
```

The listener is the `edge.listen` of `harness.json`. Ready when `kine-coord`
prints `kine-coord: serving`. Run it on the host that holds the run directory,
or with the run directory at the same path, since the DSN names the assertion
and the endpoint's CA by absolute path. Do not hand-edit the DSN's `sts=` host:
the endpoint's certificate names the provisioned host and nothing else.

## 9. Serve requests through Kine

From a checkout on any host that can reach the edge and holds the run
directory at the same path (the client host is simplest):

```text
cd adapters/kine
COORD_CERTIFY_HARNESS=/srv/tuplesky/run/harness.json go test ./certify/ -count=1 -v
```

That is the storage-profile suite of
[kubernetes-certification.md](kubernetes-certification.md), through the API
server's own client library, across hosts. A narrower smoke test is
`-run '^(TestTheAuthorizedClientIsServed|TestCreateReadCompareAndSwapDelete)$'`.
Set `COORD_CERTIFY_PREFIX` to a fresh value for each pass against one domain.

## 10. Kill and restart a voter

Any voter can be killed. Killing the leader (voter 1 at first) also exercises
the election described under the limits.

```text
kill -9 "$(cat /opt/tuplesky/n3/coordd.pid)"
```

A killed voter closes nothing, so the survivors notice only when its links
reach the transport's thirty-second idle timeout; within about half a minute
their logs say `voters submittable=1 of 2` and `peers connected=1 of 2`.
Requests keep being served by the remaining two. Wait for both lines before
restarting it: until then a survivor may still hold the peer link it dialled
to the killed process, and the restarted voter's own dials lose to that link
until it idles out, so it rejoins the peer plane up to thirty seconds late.
Restart it exactly as it was started:

```text
coord-harness start --dir /opt/tuplesky --node 3 --coordd /usr/local/bin/coordd
```

It recovers from its own store (`init` is skipped), dials the others itself,
and the survivors dial it again on their re-dial timer (task-d03; the ceiling
between attempts is ten seconds). Within seconds of its `node-ready` line every
voter reports the full mesh of step 7 again, and requests through Kine and
through voter 3's own frontend are served.

## 11. Stop

End the edge and the endpoint first, so nothing is in flight, then the voters:

```text
kill %1 %2                                   # kine-coord and the issuer, on the client host
kill "$(cat /opt/tuplesky/nN/coordd.pid)"    # on each voter host; `start` exits with it
```

Each bundle's `state/` and `journal/` survive a stop, and `start` resumes from
them. To start over, remove the run directory and every bundle and provision
again.

## Known limits

* **A stopped leader is replaced after the idle timeout.** Voter 1 leads the
  genesis ballot. When it stops, the others notice when its links end, at the
  transport's thirty-second idle timeout, and one of them campaigns after a
  jittered second: its log says `this voter campaigns for ballot 1` and then
  `this voter leads ballot 1`, and the other's says `this voter follows
  ballot 1`. Voter 1, restarted, is told of the new ballot when the leader
  sees its link return and says `this voter follows ballot 1`. To move
  leadership on purpose, send `SIGUSR1` to the voter that should lead
  (`kill -USR1 "$(cat /opt/tuplesky/n2/coordd.pid)"`). A frontend running
  without a voter beside it keeps counting evidence under the genesis ballot
  and establishes nothing after an election; every voter here runs its own
  frontend, and Kine can be pointed at any of them.
* **Leaf renewal needs a node issuer, which the harness does not run.** A
  node with a `[renewal]` section in its `coordd.toml` renews its own leaf
  while it serves (task-d02): it enrolls at the issuer when the leaf falls
  due and keeps serving, its connections moving to the renewed leaf as the
  old one's end closes them. A node holding a collector leaf renews that
  one the same way, under the same section. Every node, renewing or not,
  stops at the `notAfter` of either leaf with `reason=credential-expired`. The harness issues
  its leaves from a local test authority valid until the year 4096 and
  writes no `[renewal]` section, so a test domain never renews and never
  needs to; the startup report says `renewal not-configured`. Testing
  renewal across hosts means running a node issuer at an `https://` URL
  and adding the section by hand: a release build refuses
  `allow_insecure_loopback`.
* **Storage grows until history garbage collection is driven by the daemon.**
  The serving daemon publishes local checkpoints, but it does not yet drive
  collection of the store's revision history, so a long soak grows each
  bundle's storage without bound. Size the disk for the run.
* **A DNS name is resolved once, at start.** Each voter resolves the catalog's
  names when it starts and dials the first address a name resolves to; a
  change in DNS reaches it at its next restart. A name listened for on
  `0.0.0.0` has to resolve to an IPv4 address.
* **The native benchmark is single-host.** `coord-wan-bench`'s caller binds a
  loopback client endpoint and parses frontends as IP literals, so it does not
  drive a domain on other hosts; this runbook covers Kine and the
  certification suite.

## Rehearsed on one machine

`scripts/e2e/multi-host-local.sh RUN_DIR` follows steps 2 to 10 with the three
voters on `127.0.0.2`, `127.0.0.3` and `127.0.0.4`, all on ports 27001 and
27002, and the issuer and edge on `127.0.0.5`. Each bundle is copied away from
where it was provisioned before it is started. It serves a request through
Kine, kills voter 3, waits for both survivors to see it go, restarts it, waits
for all three to report the full mesh again, and serves a request through Kine
again. The Kubernetes certification workflow runs it, and
`bins/coordd/tests/multi_host.rs` does the same without Kine on every `cargo
test`.

A rehearsal on one machine is not the runbook followed on three hosts: it
shares one clock, one kernel and no firewall.

## Recorded on real hosts

**Not yet done.** This runbook has not been followed on three real hosts, and
no markers from such a run are recorded here. When it is, record the date, the
commit, the hosts' operating system and network (same subnet, across zones,
across regions), and on each voter host the output of step 7 before and after
step 10, with the certification suite's result from step 9.

The run is made by the maintainer on their own test nodes, and lands as a
follow-up documentation PR on task-d04; task-d04 stays open in the plan until
it does. Two conditions hold for it:

* It follows this document with the review's corrections in it, not an
  earlier copy.
* It runs on a build that carries task-d03's both-lanes re-dial and task-d01's
  restart rule. On a build without them, step 10 kills and restarts a voter
  other than the leader (voter 1): a leader restarted there within the idle
  timeout comes back on the ballot it had, with an empty table, and what the
  run records is then that defect rather than the domain.
