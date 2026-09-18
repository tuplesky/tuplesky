# TupleSky

A proposed Rust multi-region coordination service with SwiftPaxos, postcard over QUIC, Kine integration, and independently scalable non-voting read/watch replicas.

Read the [implementation design](docs/design/tuplesky-design.md) and [implementation task plan](docs/design/tuplesky-prs-plan.md). The [review index](docs/design/README.md) describes their scope.

These are proposed engineering work, not implemented or verified features. The documents integrate the original contracts, observer/membership/journal updates, and upstream-issue safeguards. Earlier drafts remain in Git history rather than as competing review documents. The design package contains only Markdown documentation; local workspace scripts and generated reports are not included. Maintained CI workflows and a change filter are task-01 deliverables.
