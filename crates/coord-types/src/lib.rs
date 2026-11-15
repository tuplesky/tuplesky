//! Shared identities, canonical commands and ordered-key encoding.
//!
//! task-01 establishes only the crate boundary. task-02 adds the `logical_v1`
//! types, fixed identifiers, checked revisions, invocation identity and the
//! frozen ordered-key vectors. This crate never performs I/O, reads clocks or
//! generates randomness (design Sections 2.3 and 16.3).
#![forbid(unsafe_code)]
#![no_std]
extern crate alloc;

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "core";
