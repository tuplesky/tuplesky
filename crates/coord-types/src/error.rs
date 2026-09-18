//! Bounded error types. None carry secrets or user data.

use core::fmt;

/// A checked counter would exceed its maximum. Counters never wrap or recycle
/// (design Section 18.3); the caller must stop, not continue.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CounterOverflow {
    /// Name of the counter type, for diagnostics.
    pub counter: &'static str,
}

impl fmt::Display for CounterOverflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} overflow: counter cannot advance", self.counter)
    }
}

impl core::error::Error for CounterOverflow {}

/// Decoding of an identity, key or fixed-width field failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DecodeError {
    /// Input shorter than the fixed encoding requires.
    Truncated,
    /// Bytes remained after a complete encoding (ambiguous framing).
    TrailingBytes,
    /// An escape or terminator sequence that the encoder never produces.
    InvalidEscape {
        /// Offset of the offending byte within the encoded key.
        offset: usize,
    },
    /// A counter field exceeds its declared maximum.
    OutOfRange,
    /// The encoded value is not the canonical encoding of its content.
    NonCanonical,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::Truncated => f.write_str("truncated encoding"),
            DecodeError::TrailingBytes => f.write_str("trailing bytes after encoding"),
            DecodeError::InvalidEscape { offset } => write!(f, "invalid escape at offset {offset}"),
            DecodeError::OutOfRange => f.write_str("field out of range"),
            DecodeError::NonCanonical => f.write_str("non-canonical encoding"),
        }
    }
}

impl core::error::Error for DecodeError {}

/// A logical operation violates the frozen schema limits or rules.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ValidationError {
    /// Exact keys must be non-empty.
    EmptyKey,
    /// Key longer than the semantic key limit.
    KeyTooLong,
    /// Value longer than the semantic value limit.
    ValueTooLong,
    /// A half-open range whose end is not greater than its start.
    EmptyRange,
    /// Too many comparisons plus branch operations in one transaction.
    TransactionTooLarge,
    /// Two writes in one transaction branch select a common key (an exact
    /// key written twice, or an interval delete overlapping another write).
    DuplicateKeyInBranch,
    /// A comparison operand does not match its target (counters for
    /// version/revision targets, bytes for the value, a lease for the lease).
    OperandMismatch,
    /// A nested transaction, which the initial schema excludes.
    NestedTransaction,
    /// The whole request exceeds the logical request byte budget.
    RequestTooLarge,
    /// TTL of zero is not a grant.
    ZeroTtl,
    /// TTL exceeds the maximum grantable lease duration.
    TtlTooLong,
    /// A compaction target of zero is meaningless.
    ZeroRevision,
    /// Pagination limit exceeds the maximum page size.
    LimitTooLarge,
    /// A Kine TTL binding identity must be present exactly when the TTL is
    /// positive.
    BindingMismatch,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            ValidationError::EmptyKey => "empty exact key",
            ValidationError::KeyTooLong => "key exceeds limit",
            ValidationError::ValueTooLong => "value exceeds limit",
            ValidationError::EmptyRange => "range end must exceed range start",
            ValidationError::TransactionTooLarge => "transaction exceeds work limit",
            ValidationError::DuplicateKeyInBranch => "overlapping key writes within one branch",
            ValidationError::OperandMismatch => "comparison operand does not match its target",
            ValidationError::NestedTransaction => "nested transactions are unsupported",
            ValidationError::RequestTooLarge => "request exceeds byte budget",
            ValidationError::ZeroTtl => "lease TTL must be positive",
            ValidationError::TtlTooLong => "lease TTL exceeds limit",
            ValidationError::ZeroRevision => "revision must be positive",
            ValidationError::BindingMismatch => "binding identity must match a positive TTL",
            ValidationError::LimitTooLarge => "page limit exceeds maximum",
        };
        f.write_str(text)
    }
}

impl core::error::Error for ValidationError {}

/// Stable invocation identity rules were violated.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IdentityError {
    /// The same retry key was presented with a different canonical payload.
    /// The state machine accepts at most the first payload (Section 4.4).
    RequestIdentityConflict,
    /// A request sequence at or below the retired floor is never new work.
    SequenceTooOld,
    /// A request sequence skipped ahead of the bounded outstanding window.
    SequenceOutOfWindow,
}

impl fmt::Display for IdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            IdentityError::RequestIdentityConflict => "request identity conflict",
            IdentityError::SequenceTooOld => "request sequence too old",
            IdentityError::SequenceOutOfWindow => "request sequence outside window",
        };
        f.write_str(text)
    }
}

impl core::error::Error for IdentityError {}
