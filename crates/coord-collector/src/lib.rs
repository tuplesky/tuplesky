//! Trusted frontend collector and native dispatch (task-33; design
//! Sections 3, 3.2, 3.3, 4.3, 4.4, 19.4).
//!
//! The native SDK never talks to voters: it speaks to a trusted frontend
//! that admits the request, canonicalizes it, fans the canonical command
//! out to *every* voter in parallel, collects the acknowledgements as
//! source-exact SwiftPaxos evidence counted by voter identity, and
//! releases a result only when its own learning predicate holds *and* the
//! leader's release gate published the exact response. A leader result
//! alone, a loosely counted majority, or votes mixed across ballots,
//! epochs, commands or connections never establish success.
//!
//! * [`admission`]: the admission interface at the boundary: a caller's
//!   verified session, the canonical request, cluster/domain/session
//!   checks and per-session bounds, minting a sealed
//!   [`coord_core::capability::AdmissionReceipt`].
//! * [`collector`]: the collector contract (frozen in
//!   `spec/collector-v1.md` for the authorized Go collector): fan-out plan,
//!   evidence intake through [`coord_consensus::VoteSet`], the release
//!   rule, cancellation that preserves identity and outcome resolution,
//!   per-domain bounds and the golden event trace.
//! * [`wire`]: the collector frames (`Submit`, `Evidence`, `Release`)
//!   registered in `spec/wire-v1.md`.
//! * [`ingress`]: the voter side: a `Submit` from an authorized collector
//!   role becomes an admitted request; frames from any other role never
//!   do, and nothing arriving on an API-class connection is ever a vote.
//! * [`dispatch`]: native unary dispatch (requests, resolution) and
//!   finalized watch dispatch over the storage watch hub, which publishes
//!   applied revisions only: tentative values never reach a watch.
//!
//! The crate holds no sockets and no runtime: the transport hands it
//! frames with bound identities and takes back frames and fan-out plans,
//! so the same code composes in the deterministic test harness and in
//! the daemon (task-43). No production listener exists here.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod admission;
pub mod codes;
pub mod collector;
pub mod dispatch;
pub mod ingress;
pub mod trace;
pub mod wire;

pub use admission::{Admission, AdmissionLimits, AdmissionRefusal, Caller};
pub use collector::{
    Collector, CollectorConfig, EvidenceError, Expired, FanOut, HoldReason, OFFER_CEILING_MILLIS,
    OfferOutcome, Offered, Progress, Release, Resolution, SubmitRefusal, Submitted,
};
pub use dispatch::{Action, Delivery, Dispatcher};
pub use ingress::{IngressError, admitted_from_submit, frontend_frame};
pub use trace::CollectorEvent;
pub use wire::{
    CollectorWireError, KIND_EVIDENCE, KIND_RELEASE, KIND_SUBMIT, SubmitV1, decode_evidence,
    decode_release, decode_submit, evidence_frame, evidence_frame_from_bytes, release_frame,
    submit_frame,
};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "production";
