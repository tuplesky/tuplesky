//! Fixed identifiers and checked counters (design Sections 2.3, 10.5.1, 18.3).
//!
//! Identifiers are 16 opaque bytes allocated once at a trusted boundary or
//! derived from a stable identity; they are never generated inside core
//! crates. Counters are distinct newtypes without conversions between them:
//! a [`LocalJournalSeq`] is not an [`ExecutionPosition`], a [`KvRevision`] is
//! not a [`ConfigurationEpoch`], and none of them wrap.

use core::fmt;

use serde::{Deserialize, Serialize};

use crate::error::{CounterOverflow, DecodeError};

macro_rules! fixed_id {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        pub struct $name(pub [u8; 16]);

        impl $name {
            /// Byte length of the identifier.
            pub const LEN: usize = 16;

            /// Borrow the raw bytes.
            pub const fn as_bytes(&self) -> &[u8; 16] {
                &self.0
            }

            /// Parse exactly sixteen bytes; any other length is rejected.
            pub fn from_slice(bytes: &[u8]) -> Result<Self, DecodeError> {
                match bytes.len() {
                    16 => {
                        let mut out = [0u8; 16];
                        out.copy_from_slice(bytes);
                        Ok(Self(out))
                    }
                    n if n < 16 => Err(DecodeError::Truncated),
                    _ => Err(DecodeError::TrailingBytes),
                }
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}(", stringify!($name))?;
                for b in &self.0 {
                    write!(f, "{b:02x}")?;
                }
                f.write_str(")")
            }
        }
    };
}

fixed_id!(
    /// Live deployment or restored-history identity (a restore mints a new one).
    ClusterId
);
fixed_id!(
    /// Transaction, revision, watch, lease, retry and authorization namespace.
    DomainId
);
fixed_id!(
    /// Tenant namespace inside a domain; the first component of every KV row key.
    NamespaceId
);
fixed_id!(
    /// Replicated session identity; never recycled.
    SessionId
);
fixed_id!(
    /// A client process instance within a session (stable across reconnects).
    ClientInstanceId
);
fixed_id!(
    /// Stable replica identity; membership binds it to a key generation.
    ReplicaId
);
fixed_id!(
    /// Native lease identity; ownership generations are never reused.
    LeaseId
);
fixed_id!(
    /// Identity of an ordered read fence (Section 6.9); reserved, distinct from a command.
    ReadFenceId
);
fixed_id!(
    /// Stable principal identity: the human `(issuer, subject)` or workload
    /// identity a session executes as, hashed at the trusted boundary. Owns
    /// leases; never a QUIC session, token or connection.
    PrincipalId
);
fixed_id!(
    /// Identity of a trust rule (issuer mapping) sessions are created under;
    /// its generation is stored in every session it admits.
    TrustRuleId
);
fixed_id!(
    /// Stable identity of one permission rule in `policy_v1`.
    PolicyRuleId
);

macro_rules! checked_counter {
    ($(#[$doc:meta])* $name:ident, max = $max:expr) => {
        $(#[$doc])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        pub struct $name(u64);

        /// Decoding goes through [`Self::new`], so a serialized value above
        /// [`Self::MAX`] is rejected exactly like a public construction.
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let raw = u64::deserialize(deserializer)?;
                Self::new(raw).map_err(|_| {
                    serde::de::Error::custom(concat!(
                        stringify!($name),
                        " exceeds its maximum"
                    ))
                })
            }
        }

        impl $name {
            /// Largest representable value.
            pub const MAX: $name = $name($max);
            /// The zero value (no position/revision established yet).
            pub const ZERO: $name = $name(0);

            /// Construct from a raw value, rejecting anything above [`Self::MAX`].
            pub const fn new(value: u64) -> Result<Self, DecodeError> {
                if value > $max { Err(DecodeError::OutOfRange) } else { Ok(Self(value)) }
            }

            /// Raw value.
            pub const fn get(self) -> u64 {
                self.0
            }

            /// The next value, or an overflow error. Never wraps.
            pub const fn checked_next(self) -> Result<Self, CounterOverflow> {
                if self.0 >= $max {
                    Err(CounterOverflow { counter: stringify!($name) })
                } else {
                    Ok(Self(self.0 + 1))
                }
            }

            /// Advance in place; on overflow the counter is left unchanged.
            pub const fn advance(&mut self) -> Result<Self, CounterOverflow> {
                match self.checked_next() {
                    Ok(next) => {
                        *self = next;
                        Ok(next)
                    }
                    Err(e) => Err(e),
                }
            }

            /// Big-endian fixed-width encoding used inside ordered keys.
            pub const fn to_be_bytes(self) -> [u8; 8] {
                self.0.to_be_bytes()
            }

            /// Decode exactly eight big-endian bytes.
            pub fn from_be_slice(bytes: &[u8]) -> Result<Self, DecodeError> {
                if bytes.len() < 8 {
                    return Err(DecodeError::Truncated);
                }
                if bytes.len() > 8 {
                    return Err(DecodeError::TrailingBytes);
                }
                let mut raw = [0u8; 8];
                raw.copy_from_slice(bytes);
                Self::new(u64::from_be_bytes(raw))
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.0)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

/// Revisions are capped so Kine can expose them as `int64`.
pub const KV_REVISION_MAX: u64 = i64::MAX as u64;

checked_counter!(
    /// KV revision: increases once per command that actually mutates KV; all of
    /// that command's changes share it. Capped at `i64::MAX` for Kine.
    KvRevision,
    max = KV_REVISION_MAX
);
checked_counter!(
    /// Established command position in the domain's execution order,
    /// including reads and internal commands; never speculative receipt order.
    ExecutionPosition,
    max = u64::MAX
);
checked_counter!(
    /// Configuration epoch: exact authorized voter identities and quorum policy.
    ConfigurationEpoch,
    max = u64::MAX
);
checked_counter!(
    /// Endpoint generation: routing/certificate refresh without voting authority.
    EndpointGeneration,
    max = u64::MAX
);
checked_counter!(
    /// Observer catalog generation: serving topology, not quorum size.
    CatalogGeneration,
    max = u64::MAX
);
checked_counter!(
    /// One local storage stream's recoverable order. Not a public counter, not
    /// an execution position, revision, ballot or fencing token.
    LocalJournalSeq,
    max = u64::MAX
);
checked_counter!(
    /// Position of a finalized frame in the exported established history.
    /// Reserved for task-o01; distinct from execution position on purpose.
    FinalizedFrameSeq,
    max = u64::MAX
);
checked_counter!(
    /// Per-(session, client instance) request sequence; retries reuse it.
    RequestSequence,
    max = u64::MAX
);
checked_counter!(
    /// Lease ownership generation; never reused for the same lease ID.
    LeaseGeneration,
    max = u64::MAX
);
checked_counter!(
    /// Replica incarnation: a new generation after storage loss; old ones are fenced.
    ReplicaIncarnation,
    max = u64::MAX
);
checked_counter!(
    /// Replicated lease expiry authority epoch (Section 7.2): a recovered
    /// leader establishes a new one before scheduling expiration, and a
    /// conditional expiration from an older epoch is rejected. `ZERO` means
    /// no authority has been established yet.
    LeaseAuthorityEpoch,
    max = u64::MAX
);

/// Ballot: leadership and the source-defined fast set, subordinate to an
/// epoch. Ballots in different epochs are incomparable; ordering within an
/// epoch is by number, then leader (deterministic tie-break).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Ballot {
    /// Epoch this ballot belongs to.
    pub epoch: ConfigurationEpoch,
    /// Ballot number within the epoch.
    pub number: u64,
    /// Leader replica of this ballot.
    pub leader: ReplicaId,
}

impl Ballot {
    /// Compare two ballots of the same epoch; `None` when epochs differ.
    pub fn compare_same_epoch(&self, other: &Ballot) -> Option<core::cmp::Ordering> {
        if self.epoch != other.epoch {
            return None;
        }
        Some(
            self.number
                .cmp(&other.number)
                .then_with(|| self.leader.cmp(&other.leader)),
        )
    }

    /// The next ballot number for the same epoch with a (possibly different)
    /// leader; a different fast set always requires a higher ballot.
    pub fn successor(&self, leader: ReplicaId) -> Result<Ballot, CounterOverflow> {
        let number = self
            .number
            .checked_add(1)
            .ok_or(CounterOverflow { counter: "Ballot" })?;
        Ok(Ballot {
            epoch: self.epoch,
            number,
            leader,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialization_enforces_the_counter_bound() {
        use serde::de::IntoDeserializer;
        type E = serde::de::value::Error;
        let below: Result<KvRevision, E> =
            KvRevision::deserialize(KV_REVISION_MAX.into_deserializer());
        assert_eq!(below.unwrap(), KvRevision::MAX);
        let above: Result<KvRevision, E> =
            KvRevision::deserialize((KV_REVISION_MAX + 1).into_deserializer());
        assert!(above.is_err());
        let above: Result<KvRevision, E> = KvRevision::deserialize(u64::MAX.into_deserializer());
        assert!(above.is_err());
        let unbounded: Result<ExecutionPosition, E> =
            ExecutionPosition::deserialize(u64::MAX.into_deserializer());
        assert_eq!(unbounded.unwrap(), ExecutionPosition::MAX);
        // Postcard round trip of an encoded out-of-range value fails as well.
        let bytes = postcard::to_allocvec(&u64::MAX).unwrap();
        assert!(postcard::from_bytes::<KvRevision>(&bytes).is_err());
        let bytes = postcard::to_allocvec(&KvRevision::MAX).unwrap();
        assert_eq!(
            postcard::from_bytes::<KvRevision>(&bytes).unwrap(),
            KvRevision::MAX
        );
    }

    #[test]
    fn revision_caps_at_i64_max() {
        let max = KvRevision::new(KV_REVISION_MAX).unwrap();
        assert!(max.checked_next().is_err());
        assert_eq!(
            KvRevision::new(KV_REVISION_MAX + 1),
            Err(DecodeError::OutOfRange)
        );
        let mut r = KvRevision::ZERO;
        assert_eq!(r.advance().unwrap().get(), 1);
        assert_eq!(r.get(), 1);
    }

    #[test]
    fn advance_leaves_counter_unchanged_on_overflow() {
        let mut p = ExecutionPosition::MAX;
        assert!(p.advance().is_err());
        assert_eq!(p, ExecutionPosition::MAX);
    }

    #[test]
    fn fixed_ids_reject_wrong_lengths() {
        assert_eq!(
            DomainId::from_slice(&[0u8; 15]),
            Err(DecodeError::Truncated)
        );
        assert_eq!(
            DomainId::from_slice(&[0u8; 17]),
            Err(DecodeError::TrailingBytes)
        );
        assert!(DomainId::from_slice(&[7u8; 16]).is_ok());
    }

    #[test]
    fn be_bytes_round_trip_and_bounds() {
        let r = KvRevision::new(0x0102030405060708).unwrap();
        assert_eq!(r.to_be_bytes(), [1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(KvRevision::from_be_slice(&r.to_be_bytes()).unwrap(), r);
        assert_eq!(
            KvRevision::from_be_slice(&[0xff; 8]),
            Err(DecodeError::OutOfRange)
        );
        assert_eq!(
            KvRevision::from_be_slice(&[0; 7]),
            Err(DecodeError::Truncated)
        );
        assert_eq!(
            KvRevision::from_be_slice(&[0; 9]),
            Err(DecodeError::TrailingBytes)
        );
    }

    #[test]
    fn ballots_in_different_epochs_are_incomparable() {
        let a = Ballot {
            epoch: ConfigurationEpoch::ZERO,
            number: 5,
            leader: ReplicaId([1; 16]),
        };
        let b = Ballot {
            epoch: ConfigurationEpoch::new(1).unwrap(),
            number: 1,
            leader: ReplicaId([1; 16]),
        };
        assert_eq!(a.compare_same_epoch(&b), None);
        let c = a.successor(ReplicaId([2; 16])).unwrap();
        assert_eq!(a.compare_same_epoch(&c), Some(core::cmp::Ordering::Less));
        let max = Ballot {
            number: u64::MAX,
            ..a
        };
        assert!(max.successor(ReplicaId([2; 16])).is_err());
    }
}
