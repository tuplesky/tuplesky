//! `logical_v1`: the frozen canonical operation schema (design Sections 4.4,
//! 6.1, 19.3).
//!
//! The postcard encoding of [`CanonicalOperation`] is part of command
//! identity, so **field order, variant order and integer widths are frozen**.
//! New variants may only be appended; changing an existing variant requires a
//! new schema (`logical_v2`). Transport framing, tokens, connection and stream
//! identifiers, membership epochs and endpoint generations are deliberately
//! absent: they are admission/envelope context, not the logical request.
//!
//! Unordered collections are normalized by [`CanonicalOperation::canonicalize`]
//! before encoding; encoding a non-canonical value is a programming error
//! caught by [`CanonicalOperation::validate`].

use alloc::vec::Vec;

use serde::{Deserialize, Serialize};

use crate::error::ValidationError;
use crate::ids::{KvRevision, LeaseId, NamespaceId};

/// Semantic limits (Section 19.3). Local scheduling budgets may be lower but
/// cannot change a chosen result.
pub mod limits {
    /// Maximum key length in bytes.
    pub const MAX_KEY_BYTES: usize = 8 * 1024;
    /// Maximum value length in bytes.
    pub const MAX_VALUE_BYTES: usize = 1024 * 1024;
    /// Maximum logical request size including comparisons and operations.
    pub const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;
    /// Maximum comparisons plus branch operations in one transaction.
    pub const MAX_TXN_WORK: usize = 128;
    /// Maximum lease TTL in seconds (one week).
    pub const MAX_LEASE_TTL_SECONDS: u32 = 7 * 24 * 3600;
    /// Maximum rows in one page.
    pub const MAX_PAGE_LIMIT: u32 = 10_000;
}

/// Version tag of this schema, included in every command hash.
pub const SCHEMA_VERSION: u16 = 1;

/// An exact key or half-open interval `[key, range_end)` within a namespace.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct KeyRange {
    /// Start key (exact key when `range_end` is `None`).
    pub key: Vec<u8>,
    /// Exclusive end; `None` selects exactly `key`.
    pub range_end: Option<Vec<u8>>,
}

impl KeyRange {
    /// An exact-key selector.
    pub fn exact(key: impl Into<Vec<u8>>) -> Self {
        KeyRange {
            key: key.into(),
            range_end: None,
        }
    }

    /// A half-open interval selector.
    pub fn interval(key: impl Into<Vec<u8>>, range_end: impl Into<Vec<u8>>) -> Self {
        KeyRange {
            key: key.into(),
            range_end: Some(range_end.into()),
        }
    }

    fn validate(&self) -> Result<(), ValidationError> {
        if self.key.is_empty() {
            return Err(ValidationError::EmptyKey);
        }
        if self.key.len() > limits::MAX_KEY_BYTES {
            return Err(ValidationError::KeyTooLong);
        }
        if let Some(end) = &self.range_end {
            if end.len() > limits::MAX_KEY_BYTES {
                return Err(ValidationError::KeyTooLong);
            }
            if end.as_slice() <= self.key.as_slice() {
                return Err(ValidationError::EmptyRange);
            }
        }
        Ok(())
    }

    fn byte_cost(&self) -> usize {
        self.key.len() + self.range_end.as_ref().map_or(0, Vec::len)
    }
}

/// Read a key or interval. `revision` selects an explicit historical
/// revision; `None` is the latest linearizable state.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RangeOp {
    /// Selector.
    pub range: KeyRange,
    /// Explicit historical revision, or latest.
    pub revision: Option<KvRevision>,
    /// Maximum rows; `0` means the schema maximum.
    pub limit: u32,
    /// Return keys and metadata only.
    pub keys_only: bool,
    /// Return only the count.
    pub count_only: bool,
}

/// Create or update one key.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PutOp {
    /// Key.
    pub key: Vec<u8>,
    /// Value.
    pub value: Vec<u8>,
    /// Optional native lease attachment.
    pub lease: Option<LeaseId>,
    /// Return the previous value.
    pub prev_kv: bool,
}

/// Kine `Create`: create the key if absent, with an optional private TTL
/// binding (design Section 6.6). The binding identity is derived at the
/// trusted boundary from the stable request so retries reproduce it; it is
/// present exactly when `ttl_seconds > 0`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct KineCreateOp {
    /// Key.
    pub key: Vec<u8>,
    /// Value.
    pub value: Vec<u8>,
    /// Kine-facing TTL in seconds (`0`: none).
    pub ttl_seconds: u32,
    /// Derived hidden binding identity when `ttl_seconds > 0`.
    pub binding: Option<LeaseId>,
}

/// Kine `Update`: compare the key's modification revision and update it,
/// replacing (or with TTL `0`, removing) its private TTL binding.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct KineUpdateOp {
    /// Key.
    pub key: Vec<u8>,
    /// Value.
    pub value: Vec<u8>,
    /// Expected current modification revision.
    pub expected_mod_revision: KvRevision,
    /// Kine-facing TTL in seconds (`0`: none).
    pub ttl_seconds: u32,
    /// Derived hidden binding identity when `ttl_seconds > 0`.
    pub binding: Option<LeaseId>,
}

/// Kine `Delete`: delete the key when its modification revision matches
/// (`None`: unconditionally).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct KineDeleteOp {
    /// Key.
    pub key: Vec<u8>,
    /// Expected current modification revision; `None` deletes unconditionally.
    pub expected_mod_revision: Option<KvRevision>,
}

/// Delete a key or interval atomically.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DeleteRangeOp {
    /// Selector.
    pub range: KeyRange,
    /// Return the previous values.
    pub prev_kv: bool,
}

/// What a transaction comparison inspects.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum CompareTarget {
    /// Version counter of the key (0 when absent).
    Version,
    /// Creation revision (0 when absent).
    CreateRevision,
    /// Modification revision (0 when absent).
    ModRevision,
    /// Value bytes.
    Value,
    /// Attached lease.
    Lease,
}

/// Comparison result required for the success branch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum CompareResult {
    /// Equal.
    Equal,
    /// Greater than.
    Greater,
    /// Less than.
    Less,
    /// Not equal.
    NotEqual,
}

/// Operand of a comparison, matching [`CompareTarget`].
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum CompareOperand {
    /// Unsigned counter (version or revision).
    Counter(u64),
    /// Value bytes.
    Bytes(Vec<u8>),
    /// Lease identity (`None` compares against "no lease").
    Lease(Option<LeaseId>),
}

/// One comparison of a transaction. All comparisons form a conjunction, so
/// their order is not semantic; [`CanonicalOperation::canonicalize`] sorts
/// them.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Compare {
    /// Key inspected (exact key; range comparisons are not in v1).
    pub key: Vec<u8>,
    /// Field inspected.
    pub target: CompareTarget,
    /// Required relation.
    pub result: CompareResult,
    /// Operand.
    pub operand: CompareOperand,
}

/// An operation allowed inside a transaction branch (no nesting in v1).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum BranchOp {
    /// Read.
    Range(RangeOp),
    /// Write.
    Put(PutOp),
    /// Delete.
    DeleteRange(DeleteRangeOp),
}

/// Atomic transaction: exactly one branch executes.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TxnOp {
    /// Conjunction of comparisons (sorted when canonical).
    pub compares: Vec<Compare>,
    /// Branch executed when all comparisons hold (ordered).
    pub success: Vec<BranchOp>,
    /// Branch executed otherwise (ordered).
    pub failure: Vec<BranchOp>,
}

/// The canonical logical operation. Discriminant order is frozen.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum CanonicalOperation {
    /// Range read.
    Range(RangeOp),
    /// Put.
    Put(PutOp),
    /// Delete range.
    DeleteRange(DeleteRangeOp),
    /// Transaction.
    Txn(TxnOp),
    /// Grant a native lease. The lease identity is derived at the trusted
    /// boundary from the stable request identity so retries reproduce it.
    LeaseGrant {
        /// Derived lease identity.
        lease_id: LeaseId,
        /// Requested TTL in seconds.
        ttl_seconds: u32,
    },
    /// Replicated renewal.
    LeaseKeepAlive {
        /// Lease to renew.
        lease_id: LeaseId,
    },
    /// Explicit revocation, deleting attached keys atomically.
    LeaseRevoke {
        /// Lease to revoke.
        lease_id: LeaseId,
    },
    /// Authoritative existence/TTL read.
    LeaseTimeToLive {
        /// Lease to inspect.
        lease_id: LeaseId,
        /// Include attached keys.
        keys: bool,
    },
    /// Ordered MVCC retention floor.
    Compact {
        /// Revision at or below which history may be discarded.
        revision: KvRevision,
    },
    /// Kine create-if-absent with atomic private TTL binding (appended;
    /// discriminant 9).
    KineCreate(KineCreateOp),
    /// Kine compare-mod-revision-and-update with TTL replacement
    /// (discriminant 10).
    KineUpdate(KineUpdateOp),
    /// Kine conditional delete (discriminant 11).
    KineDelete(KineDeleteOp),
}

/// A canonical logical request: domain-scoped tenant plus operation.
///
/// Together with the [`crate::identity::RetryKey`] this is everything that
/// participates in command identity.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LogicalRequest {
    /// Schema version; always [`SCHEMA_VERSION`] for this type.
    pub schema_version: u16,
    /// Tenant namespace.
    pub namespace: NamespaceId,
    /// Operation.
    pub operation: CanonicalOperation,
}

impl LogicalRequest {
    /// Build a request for this schema version.
    pub fn new(namespace: NamespaceId, operation: CanonicalOperation) -> Self {
        LogicalRequest {
            schema_version: SCHEMA_VERSION,
            namespace,
            operation,
        }
    }

    /// Normalize unordered collections in place.
    pub fn canonicalize(&mut self) {
        self.operation.canonicalize();
    }

    /// Validate limits, rules and canonical form.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.schema_version != SCHEMA_VERSION {
            // A different version is not this type's schema; treat as too
            // large/unsupported rather than silently reinterpreting.
            return Err(ValidationError::RequestTooLarge);
        }
        self.operation.validate()
    }

    /// Canonical postcard encoding (the identity payload). Fails only when
    /// the request is invalid or not canonical.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ValidationError> {
        self.validate()?;
        postcard::to_allocvec(self).map_err(|_| ValidationError::RequestTooLarge)
    }
}

impl CanonicalOperation {
    /// Normalize unordered collections (transaction comparisons) in place.
    pub fn canonicalize(&mut self) {
        if let CanonicalOperation::Txn(txn) = self {
            txn.compares.sort();
            txn.compares.dedup();
        }
    }

    /// Whether the value is in canonical form (see [`Self::canonicalize`]).
    pub fn is_canonical(&self) -> bool {
        match self {
            CanonicalOperation::Txn(txn) => txn.compares.windows(2).all(|w| w[0] < w[1]),
            _ => true,
        }
    }

    /// Validate limits and schema rules. Non-canonical transactions are
    /// rejected so two encodings of one logical request cannot exist.
    pub fn validate(&self) -> Result<(), ValidationError> {
        let mut cost = 0usize;
        match self {
            CanonicalOperation::Range(r) => cost += validate_range(r)?,
            CanonicalOperation::Put(p) => cost += validate_put(p)?,
            CanonicalOperation::DeleteRange(d) => {
                d.range.validate()?;
                cost += d.range.byte_cost();
            }
            CanonicalOperation::Txn(txn) => {
                if !self.is_canonical() {
                    return Err(ValidationError::RequestTooLarge);
                }
                let work = txn.compares.len() + txn.success.len() + txn.failure.len();
                if work > limits::MAX_TXN_WORK {
                    return Err(ValidationError::TransactionTooLarge);
                }
                for c in &txn.compares {
                    if c.key.is_empty() {
                        return Err(ValidationError::EmptyKey);
                    }
                    if c.key.len() > limits::MAX_KEY_BYTES {
                        return Err(ValidationError::KeyTooLong);
                    }
                    cost += c.key.len();
                    match (&c.target, &c.operand) {
                        (
                            CompareTarget::Version
                            | CompareTarget::CreateRevision
                            | CompareTarget::ModRevision,
                            CompareOperand::Counter(_),
                        )
                        | (CompareTarget::Lease, CompareOperand::Lease(_)) => {}
                        (CompareTarget::Value, CompareOperand::Bytes(b)) => {
                            if b.len() > limits::MAX_VALUE_BYTES {
                                return Err(ValidationError::ValueTooLong);
                            }
                            cost += b.len();
                        }
                        _ => return Err(ValidationError::OperandMismatch),
                    }
                }
                for branch in [&txn.success, &txn.failure] {
                    // Every write selector of the branch so far. A branch may
                    // write each key at most once (design Section 6.4), so an
                    // exact key or an interval delete must not select a key
                    // any earlier write of the branch already selects.
                    let mut written: Vec<WriteSelector<'_>> = Vec::new();
                    for op in branch {
                        let selector = match op {
                            BranchOp::Range(r) => {
                                cost += validate_range(r)?;
                                None
                            }
                            BranchOp::Put(p) => {
                                cost += validate_put(p)?;
                                Some(WriteSelector {
                                    key: &p.key,
                                    end: None,
                                })
                            }
                            BranchOp::DeleteRange(d) => {
                                d.range.validate()?;
                                cost += d.range.byte_cost();
                                Some(WriteSelector {
                                    key: &d.range.key,
                                    end: d.range.range_end.as_deref(),
                                })
                            }
                        };
                        if let Some(sel) = selector {
                            if written.iter().any(|w| w.overlaps(&sel)) {
                                return Err(ValidationError::DuplicateKeyInBranch);
                            }
                            written.push(sel);
                        }
                    }
                }
            }
            CanonicalOperation::LeaseGrant { ttl_seconds, .. } => {
                if *ttl_seconds == 0 {
                    return Err(ValidationError::ZeroTtl);
                }
                if *ttl_seconds > limits::MAX_LEASE_TTL_SECONDS {
                    return Err(ValidationError::TtlTooLong);
                }
            }
            CanonicalOperation::LeaseKeepAlive { .. }
            | CanonicalOperation::LeaseRevoke { .. }
            | CanonicalOperation::LeaseTimeToLive { .. } => {}
            CanonicalOperation::Compact { revision } => {
                if *revision == KvRevision::ZERO {
                    return Err(ValidationError::ZeroRevision);
                }
            }
            CanonicalOperation::KineCreate(c) => {
                cost += validate_kine_write(&c.key, &c.value, c.ttl_seconds, c.binding)?;
            }
            CanonicalOperation::KineUpdate(u) => {
                cost += validate_kine_write(&u.key, &u.value, u.ttl_seconds, u.binding)?;
                if u.expected_mod_revision == KvRevision::ZERO {
                    return Err(ValidationError::ZeroRevision);
                }
            }
            CanonicalOperation::KineDelete(d) => {
                validate_key(&d.key)?;
                if d.expected_mod_revision == Some(KvRevision::ZERO) {
                    return Err(ValidationError::ZeroRevision);
                }
                cost += d.key.len();
            }
        }
        if cost > limits::MAX_REQUEST_BYTES {
            return Err(ValidationError::RequestTooLarge);
        }
        Ok(())
    }
}

/// The keys one branch write selects: an exact key or a half-open interval.
#[derive(Clone, Copy)]
struct WriteSelector<'a> {
    key: &'a [u8],
    end: Option<&'a [u8]>,
}

impl WriteSelector<'_> {
    /// Whether some key is selected by both writes.
    fn overlaps(&self, other: &Self) -> bool {
        match (self.end, other.end) {
            (None, None) => self.key == other.key,
            (Some(end), None) => self.key <= other.key && other.key < end,
            (None, Some(end)) => other.key <= self.key && self.key < end,
            (Some(a_end), Some(b_end)) => self.key < b_end && other.key < a_end,
        }
    }
}

fn validate_range(r: &RangeOp) -> Result<usize, ValidationError> {
    r.range.validate()?;
    if r.limit > limits::MAX_PAGE_LIMIT {
        return Err(ValidationError::LimitTooLarge);
    }
    Ok(r.range.byte_cost())
}

fn validate_key(key: &[u8]) -> Result<(), ValidationError> {
    if key.is_empty() {
        return Err(ValidationError::EmptyKey);
    }
    if key.len() > limits::MAX_KEY_BYTES {
        return Err(ValidationError::KeyTooLong);
    }
    Ok(())
}

fn validate_put(p: &PutOp) -> Result<usize, ValidationError> {
    validate_key(&p.key)?;
    if p.value.len() > limits::MAX_VALUE_BYTES {
        return Err(ValidationError::ValueTooLong);
    }
    Ok(p.key.len() + p.value.len())
}

fn validate_kine_write(
    key: &[u8],
    value: &[u8],
    ttl_seconds: u32,
    binding: Option<LeaseId>,
) -> Result<usize, ValidationError> {
    validate_key(key)?;
    if value.len() > limits::MAX_VALUE_BYTES {
        return Err(ValidationError::ValueTooLong);
    }
    if ttl_seconds > limits::MAX_LEASE_TTL_SECONDS {
        return Err(ValidationError::TtlTooLong);
    }
    if binding.is_some() != (ttl_seconds > 0) {
        return Err(ValidationError::BindingMismatch);
    }
    Ok(key.len() + value.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    fn ns() -> NamespaceId {
        NamespaceId([9; 16])
    }

    #[test]
    fn empty_key_and_empty_range_are_rejected() {
        let op = CanonicalOperation::Range(RangeOp {
            range: KeyRange::exact(b"".to_vec()),
            revision: None,
            limit: 0,
            keys_only: false,
            count_only: false,
        });
        assert_eq!(op.validate(), Err(ValidationError::EmptyKey));
        let op = CanonicalOperation::DeleteRange(DeleteRangeOp {
            range: KeyRange::interval(b"b".to_vec(), b"a".to_vec()),
            prev_kv: false,
        });
        assert_eq!(op.validate(), Err(ValidationError::EmptyRange));
        let op = CanonicalOperation::DeleteRange(DeleteRangeOp {
            range: KeyRange::interval(b"a".to_vec(), b"a".to_vec()),
            prev_kv: false,
        });
        assert_eq!(op.validate(), Err(ValidationError::EmptyRange));
    }

    #[test]
    fn limits_fail_before_encoding() {
        let big = CanonicalOperation::Put(PutOp {
            key: vec![1; limits::MAX_KEY_BYTES + 1],
            value: vec![],
            lease: None,
            prev_kv: false,
        });
        assert_eq!(big.validate(), Err(ValidationError::KeyTooLong));
        let big = CanonicalOperation::Put(PutOp {
            key: vec![1],
            value: vec![0; limits::MAX_VALUE_BYTES + 1],
            lease: None,
            prev_kv: false,
        });
        assert_eq!(big.validate(), Err(ValidationError::ValueTooLong));
        assert_eq!(
            LogicalRequest::new(ns(), big).canonical_bytes(),
            Err(ValidationError::ValueTooLong)
        );
        let ops: Vec<BranchOp> = (0..=limits::MAX_TXN_WORK)
            .map(|i| {
                BranchOp::Put(PutOp {
                    key: vec![1, i as u8, (i >> 8) as u8],
                    value: vec![],
                    lease: None,
                    prev_kv: false,
                })
            })
            .collect();
        let txn = CanonicalOperation::Txn(TxnOp {
            compares: vec![],
            success: ops,
            failure: vec![],
        });
        assert_eq!(txn.validate(), Err(ValidationError::TransactionTooLarge));
        let many_puts: Vec<BranchOp> = (0..3)
            .map(|_| {
                BranchOp::Put(PutOp {
                    key: vec![1],
                    value: vec![0; limits::MAX_VALUE_BYTES],
                    lease: None,
                    prev_kv: false,
                })
            })
            .collect();
        let txn = CanonicalOperation::Txn(TxnOp {
            compares: vec![],
            success: many_puts,
            failure: vec![],
        });
        assert!(matches!(
            txn.validate(),
            Err(ValidationError::DuplicateKeyInBranch | ValidationError::RequestTooLarge)
        ));
    }

    #[test]
    fn duplicate_key_write_in_branch_is_rejected() {
        let txn = CanonicalOperation::Txn(TxnOp {
            compares: vec![],
            success: vec![
                BranchOp::Put(PutOp {
                    key: b"k".to_vec(),
                    value: vec![],
                    lease: None,
                    prev_kv: false,
                }),
                BranchOp::DeleteRange(DeleteRangeOp {
                    range: KeyRange::exact(b"k".to_vec()),
                    prev_kv: false,
                }),
            ],
            failure: vec![],
        });
        assert_eq!(txn.validate(), Err(ValidationError::DuplicateKeyInBranch));
    }

    fn put(key: &[u8]) -> BranchOp {
        BranchOp::Put(PutOp {
            key: key.to_vec(),
            value: vec![],
            lease: None,
            prev_kv: false,
        })
    }

    fn delete(key: &[u8], end: Option<&[u8]>) -> BranchOp {
        BranchOp::DeleteRange(DeleteRangeOp {
            range: KeyRange {
                key: key.to_vec(),
                range_end: end.map(<[u8]>::to_vec),
            },
            prev_kv: false,
        })
    }

    fn branch(ops: Vec<BranchOp>) -> CanonicalOperation {
        CanonicalOperation::Txn(TxnOp {
            compares: vec![],
            success: ops,
            failure: vec![],
        })
    }

    #[test]
    fn overlapping_interval_writes_in_branch_are_rejected() {
        let dup = Err(ValidationError::DuplicateKeyInBranch);
        // Interval delete followed by a put inside it, and the reverse order.
        assert_eq!(
            branch(vec![delete(b"a", Some(b"z")), put(b"m")]).validate(),
            dup
        );
        assert_eq!(
            branch(vec![put(b"m"), delete(b"a", Some(b"z"))]).validate(),
            dup
        );
        // Two overlapping interval deletes, including containment.
        assert_eq!(
            branch(vec![delete(b"a", Some(b"m")), delete(b"c", Some(b"z"))]).validate(),
            dup
        );
        assert_eq!(
            branch(vec![delete(b"a", Some(b"z")), delete(b"c", Some(b"d"))]).validate(),
            dup
        );
        // Disjoint selectors are fine: the exclusive end is not selected.
        assert_eq!(
            branch(vec![delete(b"a", Some(b"m")), put(b"m")]).validate(),
            Ok(())
        );
        assert_eq!(
            branch(vec![delete(b"a", Some(b"m")), delete(b"m", Some(b"z"))]).validate(),
            Ok(())
        );
        assert_eq!(
            branch(vec![put(b"a"), delete(b"b", None), put(b"c")]).validate(),
            Ok(())
        );
        // The failure branch is checked independently of the success branch.
        let both = CanonicalOperation::Txn(TxnOp {
            compares: vec![],
            success: vec![put(b"m")],
            failure: vec![delete(b"a", Some(b"z"))],
        });
        assert_eq!(both.validate(), Ok(()));
    }

    #[test]
    fn compare_operands_must_match_their_targets() {
        let compare = |target, operand| {
            CanonicalOperation::Txn(TxnOp {
                compares: vec![Compare {
                    key: b"k".to_vec(),
                    target,
                    result: CompareResult::Equal,
                    operand,
                }],
                success: vec![],
                failure: vec![],
            })
        };
        let bad = Err(ValidationError::OperandMismatch);
        assert_eq!(
            compare(CompareTarget::Value, CompareOperand::Counter(1)).validate(),
            bad
        );
        assert_eq!(
            compare(CompareTarget::Lease, CompareOperand::Bytes(vec![1])).validate(),
            bad
        );
        assert_eq!(
            compare(CompareTarget::ModRevision, CompareOperand::Lease(None)).validate(),
            bad
        );
        assert_eq!(
            compare(CompareTarget::Version, CompareOperand::Bytes(vec![])).validate(),
            bad
        );
        assert_eq!(
            compare(CompareTarget::Value, CompareOperand::Bytes(vec![1])).validate(),
            Ok(())
        );
        assert_eq!(
            compare(CompareTarget::Lease, CompareOperand::Lease(None)).validate(),
            Ok(())
        );
        assert_eq!(
            compare(CompareTarget::CreateRevision, CompareOperand::Counter(0)).validate(),
            Ok(())
        );
    }

    #[test]
    fn compares_are_normalized_and_non_canonical_is_rejected() {
        let c1 = Compare {
            key: b"b".to_vec(),
            target: CompareTarget::Version,
            result: CompareResult::Equal,
            operand: CompareOperand::Counter(1),
        };
        let c2 = Compare {
            key: b"a".to_vec(),
            target: CompareTarget::Version,
            result: CompareResult::Equal,
            operand: CompareOperand::Counter(1),
        };
        let mut op = CanonicalOperation::Txn(TxnOp {
            compares: vec![c1.clone(), c2.clone(), c1.clone()],
            success: vec![],
            failure: vec![],
        });
        assert!(!op.is_canonical());
        assert_eq!(op.validate(), Err(ValidationError::RequestTooLarge));
        op.canonicalize();
        assert!(op.is_canonical());
        assert_eq!(op.validate(), Ok(()));
        if let CanonicalOperation::Txn(t) = &op {
            assert_eq!(t.compares, vec![c2, c1]);
        }
    }

    #[test]
    fn lease_and_compact_rules() {
        let lease = LeaseId([1; 16]);
        assert_eq!(
            CanonicalOperation::LeaseGrant {
                lease_id: lease,
                ttl_seconds: 0
            }
            .validate(),
            Err(ValidationError::ZeroTtl)
        );
        assert_eq!(
            CanonicalOperation::LeaseGrant {
                lease_id: lease,
                ttl_seconds: limits::MAX_LEASE_TTL_SECONDS + 1
            }
            .validate(),
            Err(ValidationError::TtlTooLong)
        );
        assert_eq!(
            CanonicalOperation::Compact {
                revision: KvRevision::ZERO
            }
            .validate(),
            Err(ValidationError::ZeroRevision)
        );
        assert_eq!(
            CanonicalOperation::LeaseKeepAlive { lease_id: lease }.validate(),
            Ok(())
        );
    }

    #[test]
    fn wrong_schema_version_is_rejected() {
        let mut req = LogicalRequest::new(
            ns(),
            CanonicalOperation::LeaseRevoke {
                lease_id: LeaseId([2; 16]),
            },
        );
        req.schema_version = 2;
        assert!(req.validate().is_err());
    }
}
