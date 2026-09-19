//! API session binding and output authorization (task-37; design
//! Sections 6.4, 6.5, 6.9.3, 9.3, 11.5, 19.4).
//!
//! A warm API connection is bound to one replicated session by the STS
//! service token it presents once ([`wire::BindV1`]); the binding keeps
//! the session, principal, scope ceiling, rule generation and expiry.
//! Rebinding refreshes validity and can never change the identity: a
//! token for another session is refused. Nothing is admitted before a
//! binding or after its validity ends.
//!
//! Admission never freezes policy. Every disclosure of protected data
//! (a unary result with entries, a retained result answered to a retry
//! or a resolution, and each watch batch) is authorized against a
//! **fresh** [`gate::AuthorizationBarrier`] read from replicated policy
//! at an ordered position: the session must still be active under its
//! trust rule generation, and the principal must hold a read permission
//! covering every key the output carries. A barrier is loaded for one
//! bounded selection (one delivery, one bounded watch pump) and dropped;
//! it is never a lease. Ordered revocation therefore denies protected
//! data even from historical or cached results, and watch progress can
//! never cross a batch that was not authorized. Work admitted before a
//! revocation still executes at its position (the state machine decides
//! there); its acknowledgement is delivered, its protected data is not.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod binding;
pub mod frontend;
pub mod gate;
pub mod wire;

pub use binding::{BindError, Binding, BindingConfig, verify_bind};
/// How many keys in a JWKS document a caller's token could actually be
/// verified with. A frontend whose set has none refuses every caller,
/// which is worth finding out at startup rather than at the first bind.
pub use coord_sts::usable_verification_keys;
pub use frontend::{BoundFrontend, Ingress};
pub use gate::{
    AuthorizationBarrier, PolicyError, PolicySource, StorePolicySource, protected_keys,
};
pub use wire::{
    BindAckV1, BindV1, KIND_BIND, KIND_BIND_ACK, MAX_TOKEN_BYTES, SessionWireError, bind_ack_frame,
    bind_frame, decode_bind, decode_bind_ack,
};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "production";
