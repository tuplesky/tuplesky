//! Definite and indeterminate journal failures (design Sections 17.15 and
//! 17.16.6).
//!
//! A [`JournalFailure::Definite`] failure carries specific evidence that
//! nothing was appended (a guard rejection before append, or noncommit
//! evidence); the caller may replan. A [`JournalFailure::Indeterminate`]
//! failure arose after submission: the record may or may not be durable,
//! dependent effects and admission stay blocked, and only recovery of the
//! actual valid records resolves it. A timeout is never definite.

use alloc::string::String;
use core::fmt;

use coord_core::event::StorageError;

/// Journal failure class.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum JournalErrorClass {
    /// A guard (origin, index, digest, predecessor, bounds) rejected the
    /// record before append.
    GuardRejected,
    /// I/O error of uncertain effect.
    Io,
    /// Detected corruption; quarantine the affected scope.
    Corrupt,
    /// Out of space.
    NoSpace,
    /// A caller budget (records, bytes) was exceeded.
    Limit,
    /// Engine busy or backlog full; retry later without semantic change.
    Busy,
    /// Capability unsupported by the engine.
    Unsupported,
}

/// Journal error with redacted diagnostic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalError {
    /// Class.
    pub class: JournalErrorClass,
    /// Redacted diagnostic (no keys or values).
    pub diagnostic: String,
}

impl JournalError {
    /// Construct.
    pub fn new(class: JournalErrorClass, diagnostic: impl Into<String>) -> Self {
        JournalError {
            class,
            diagnostic: diagnostic.into(),
        }
    }
}

impl fmt::Display for JournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.class, self.diagnostic)
    }
}

impl core::error::Error for JournalError {}

/// How an append failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JournalFailure {
    /// Specific evidence that nothing was appended; the reservation is
    /// released and the caller may replan.
    Definite(JournalError),
    /// Outcome unknown after submission; the stream head is uncertain until
    /// the actual durable records are recovered. Never blind-retry.
    Indeterminate(JournalError),
}

impl JournalFailure {
    /// A guard rejected the record before anything was submitted.
    pub fn rejected_before_append(diagnostic: impl Into<String>) -> Self {
        JournalFailure::Definite(JournalError::new(
            JournalErrorClass::GuardRejected,
            diagnostic,
        ))
    }

    /// The engine failed after the batch was submitted.
    pub fn after_submission(error: JournalError) -> Self {
        JournalFailure::Indeterminate(error)
    }

    /// The underlying error.
    pub const fn error(&self) -> &JournalError {
        match self {
            JournalFailure::Definite(e) | JournalFailure::Indeterminate(e) => e,
        }
    }

    /// Whether the failure is definite.
    pub const fn is_definite(&self) -> bool {
        matches!(self, JournalFailure::Definite(_))
    }

    /// The storage-event class reported to the machine. Corruption always
    /// quarantines; indeterminate failures are never reported as definite.
    pub const fn storage_error(&self) -> StorageError {
        match self {
            JournalFailure::Definite(e) | JournalFailure::Indeterminate(e)
                if matches!(e.class, JournalErrorClass::Corrupt) =>
            {
                StorageError::Quarantine
            }
            JournalFailure::Definite(e) if matches!(e.class, JournalErrorClass::NoSpace) => {
                StorageError::NoSpace
            }
            JournalFailure::Definite(_) => StorageError::DefinitelyNotCommitted,
            JournalFailure::Indeterminate(_) => StorageError::Indeterminate,
        }
    }
}

impl fmt::Display for JournalFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JournalFailure::Definite(e) => write!(f, "definitely not appended: {e}"),
            JournalFailure::Indeterminate(e) => write!(f, "indeterminate append: {e}"),
        }
    }
}

impl core::error::Error for JournalFailure {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_error_classes() {
        assert_eq!(
            JournalFailure::rejected_before_append("index").storage_error(),
            StorageError::DefinitelyNotCommitted
        );
        assert_eq!(
            JournalFailure::after_submission(JournalError::new(JournalErrorClass::Io, "sync"))
                .storage_error(),
            StorageError::Indeterminate
        );
        // An indeterminate out-of-space stays indeterminate: the batch may
        // have reached the log before the sync failed.
        assert_eq!(
            JournalFailure::after_submission(JournalError::new(JournalErrorClass::NoSpace, "sync"))
                .storage_error(),
            StorageError::Indeterminate
        );
        assert_eq!(
            JournalFailure::Definite(JournalError::new(JournalErrorClass::NoSpace, "pre"))
                .storage_error(),
            StorageError::NoSpace
        );
        for f in [
            JournalFailure::Definite(JournalError::new(JournalErrorClass::Corrupt, "x")),
            JournalFailure::Indeterminate(JournalError::new(JournalErrorClass::Corrupt, "x")),
        ] {
            assert_eq!(f.storage_error(), StorageError::Quarantine);
        }
        assert!(JournalFailure::rejected_before_append("g").is_definite());
    }
}
