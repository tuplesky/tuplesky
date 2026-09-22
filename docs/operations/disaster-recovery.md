# Disaster recovery: isolating a cluster and restoring from a backup

This is the runbook for the one situation the rest of the system
deliberately cannot handle by itself: a cluster whose quorum is gone or
whose state is lost beyond ordinary recovery. It implements design
Sections 5.4, 7.4, 17.16 and 22.2, and the code that enforces it is
`coord_checkpoint::restore` (task-59).

Read the whole page before starting. A restore is not reversible and it
is not ordinary recovery.

## What a restore is not

* **It is not recovery.** Ordinary recovery preserves this cluster's
  identity and everything it acknowledged, within the stated budget. A
  restore does neither: it establishes a *new* cluster from a snapshot
  taken at some earlier boundary, and everything after that boundary is
  gone.
* **It is not a way to get a minority serving again.** Nothing here
  promotes a surviving minority, and no observer may promote itself.
  Losing the old quorum's authority does not confer authority anywhere
  else.
* **It is not zero-loss.** The recovery point is whatever the backup
  captured. `coordd restore --plan` prints it before anything is
  written, and it is the number to reconcile against callers, not an
  implementation detail.

## Before you start: the isolation is yours to perform

A restore is safe only once the old cluster cannot still be serving, and
this system cannot establish that. The old voters may be partitioned
from you and perfectly healthy; from their side nothing has happened.
So the isolation is an action you take outside TupleSky, and the restore
only records that you say you took it.

Perform at least one of these, and prefer more than one:

1. **Revoke the old cluster's node credentials at the issuer** and let
   the outstanding leaves expire. Credential lifetimes are short and
   bounded (task-58), and warm connections end with the credential that
   authenticated them, so this is self-completing rather than a promise.
2. **Take the old cluster's addresses away**: remove its load-balancer
   backends, its DNS records and its service endpoints, so no client
   reaches it even if a node is running.
3. **Stop the nodes, or take their storage away.** Powering nodes off is
   the most direct form and the easiest to verify.

Suspected compromise of a voter is a separate matter: it needs a valid
configuration fence, not merely a revoked credential, before you may
assume it cannot affect consensus. None of this adds Byzantine
tolerance.

Record what you did and when. The next step makes you state it.

## 1. Choose the backup and verify it

A backup is one directory, written by a node of the cluster it came
from:

```text
coordd --config <node.toml> backup --out <backup/>
```

It holds `backup.json` (the backup manifest, which you will read), the
artifact's own manifest and its chunks, and it is never written over an
existing one. Every `coordd` invocation names its configuration with
`--config` before the subcommand; the flags of the subcommand come after
it.

Verify the backup you have chosen:

```text
coordd --config <coordd.toml> verify --dir <backup/>
```

This recomputes every chunk digest and the artifact root, checks the
backup manifest binds that exact root, and prints the boundary and the
time the snapshot was pinned. A backup index that has been repointed at
different bytes fails here.

Verification reads no store and needs no cluster: any configuration
this build parses will do, including that of a machine whose own store
is the one that was lost and one whose certificate no cluster names as a
voter. Run it wherever the backup is.

Check that the artifact is a **common snapshot** (`SharedCheckpointV1`).
The three artifacts of design Section 17.16.1 are not interchangeable
and the restore refuses the other two:

| Artifact | What it is | Restores a cluster? |
|---|---|---|
| `SharedCheckpointV1` | common state at a boundary | yes |
| `LocalRecoveryCheckpointV1` | one voter incarnation's whole storage, obligations included | no |
| Observer snapshot | a declared capability's view; may not even be full MVCC | no |

Neither of the other two recreates an existing voter's local state
either. If what you have is a local checkpoint of a voter that is
otherwise healthy, you want ordinary recovery, not this page.

## 2. Establish the successor cluster's identity and genesis

The restored cluster is a **new cluster** with a new cluster identity,
and its membership comes from its own genesis (task-42) and from
nowhere else. Generate the successor's genesis manifest and node
credentials exactly as for a new deployment, and write the
configuration (`<successor.toml>` below) of the node the restore will
fill. Do **not** run `coordd init` on that node: the restore creates
its first generation itself, and a node that already has one is
refused.

Restoring under the old cluster's identity is refused, and it is worth
knowing why it is refused rather than merely discouraged: callers hold
promises made by that name, and a rewound history behind the same name
would satisfy them incorrectly rather than visibly failing.

## 3. Write the fencing attestation

```json
{
  "abandoned": <the "source" value of backup.json>,
  "successor": <the successor's cluster id, in the same form>,
  "backup": <the "root" value of backup.json>,
  "action": "revoked node certificates at the issuer and removed the LB backends, ticket DR-91",
  "at": 1700000600
}
```

Identities and digests are written the way `backup.json` writes them:
as JSON arrays of byte values, sixteen for a cluster identity and
thirty-two for a digest. Copy `source` and `root` out of `backup.json`
verbatim, and write the successor's cluster identity -- the sixteen
bytes its genesis manifest names as `cluster` -- in the same form. A
value in any other encoding is refused as unreadable, before anything
is checked.

The attestation is not the fence. The fence is what you did in step 0;
this is your record of it, bound to the exact clusters and the exact
backup so that it cannot be reused for a different restore. What it buys
is that a restore performed without the isolation has to be a deliberate
false statement rather than a step somebody forgot.

`action` must say what was actually done. An empty or whitespace
reference is refused.

## 4. Plan the restore and read what it will lose

```text
coordd --config <successor.toml> restore --plan --dir <backup/> --fencing <fencing.json>
```

This runs on the successor node, under its own configuration: the
successor's identity is the one its committed genesis gives that node,
which is why the restore is refused under the old cluster's
configuration.

The plan prints the recovery point and the disposition of every class of
state. Check the recovery point against what callers were told, and
expect these dispositions:

| State | Disposition | Why |
|---|---|---|
| KV rows, history, events | restored at the boundary | it is what the backup is for |
| retained retry results and floors | restored at the boundary | dropping them turns a caller's retry into a second execution |
| configuration epochs and certificates | **not carried** | they name the old voters and their keys |
| sessions and auth grants | **invalidated** | a session established against the old cluster is not a session here |
| leases and their reverse index | **revoked** | nobody can renew a lease granted by a cluster that no longer exists |
| lease attachments on restored keys | **detached** | a key attached to a revoked lease would be held by an authority that cannot expire it |
| promises, votes, obligations | never present | no shared artifact carries them |

Dropping the configuration rows is what "never reuse stale voting
authority" means concretely. The restored store holds no certificate
naming any voter.

## 5. Restore

```text
coordd --config <successor.toml> restore --dir <backup/> --fencing <fencing.json>
```

The same invocation without `--plan`, on the same node.

The restore writes into a fresh generation of the *successor* cluster
and selects nothing until it has finished, so a failure or a crash at
any point leaves a store that simply was never restored. The receipt it
writes last is the only evidence a restore completed; a restored store
can always say which cluster it came from, at which boundary, what was
dropped and what you told it had been done.

## 6. Afterwards

* **Callers re-establish everything.** Sessions, leases and watches are
  all re-created against the new cluster. Watch consumers resynchronize
  from the restored revision; they are not resumed from a cursor issued
  by the old cluster.
* **Tell callers the recovery point.** Work acknowledged after the
  backup's boundary did happen and is gone. This is the number from
  step 4.
* **Do not bring the old cluster back.** If the isolation is ever
  reversed, two clusters exist that both believe they are authoritative
  for the same history, and nothing in the protocol resolves that.

## Rehearsing

Rehearse this on a cluster you are willing to lose, at least: take a
backup, verify it, write an attestation, plan, restore into a new
cluster identity, and check the recovery point and the dispositions
against this page. A rehearsal that skips the isolation step or restores
under the same cluster identity is rehearsing something else; both are
refused, so the rehearsal will tell you.
