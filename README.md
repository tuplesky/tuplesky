# TupleSky

A proposed Rust multi-region coordination service with SwiftPaxos, postcard over QUIC, Kine integration, and independently scalable non-voting read/watch replicas.

Read the [implementation design](docs/design/tuplesky-design.md) and [implementation task plan](docs/design/tuplesky-prs-plan.md). The [review index](docs/design/README.md) describes their scope.

These are proposed engineering work, not implemented or verified features. The documents integrate the original contracts, observer/membership/journal updates, and upstream-issue safeguards. Earlier drafts remain in Git history rather than as competing review documents. The design package contains only Markdown documentation; local workspace scripts and generated reports are not included. Maintained CI workflows and a change filter are task-01 deliverables.

## Building

The workspace is locked to exact toolchains and dependency versions; see
[docs/build/toolchain.md](docs/build/toolchain.md) for the pins and the
dependency audit and [docs/build/ci.md](docs/build/ci.md) for CI routing.
Operator procedures live under `docs/operations/`: see
[disaster recovery](docs/operations/disaster-recovery.md) for isolating a
cluster and restoring from a backup,
[Kubernetes certification](docs/operations/kubernetes-certification.md) for
the storage profile qualification and what it currently reports, and
[WAN benchmarks](docs/operations/wan-benchmarks.md) for the measured matrix
and how to read one of its runs, and
[Jepsen tests](docs/operations/jepsen.md) for the native client a Jepsen test
drives and what it has found.

```text
cargo xtask check-tools --install   # pinned cargo-deny, cargo-nextest, mermaid-cli
cargo xtask ci                      # what a pull request runs
```

Implementation follows the task plan in order; each task lands as one
reviewable change titled `task-NN: ...`.
