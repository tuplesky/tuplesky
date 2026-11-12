//! Admission receipts for tests outside the boundary crates.
//!
//! Only `coord-collector` and `coord-session` may mint a receipt in their
//! library sources (`cargo xtask check-deps`), and the check reads
//! `#[cfg(test)]` modules too. A unit test elsewhere that needs a request
//! admitted, to reach a crate's private items, takes its receipt from here:
//! this crate is test-only, so nothing it mints reaches a production
//! artifact.

use coord_core::capability::{AdmissionReceipt, AttestedAdmission, VerifierToken};

/// A receipt for submitting the request `attested` describes, as a boundary
/// would mint it.
pub fn submitting(attested: AttestedAdmission) -> AdmissionReceipt {
    AdmissionReceipt::submitting(VerifierToken::for_boundary(), attested)
}
