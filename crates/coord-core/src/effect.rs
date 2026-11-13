//! Owned effects, persistence batches and durable barriers.

use alloc::vec::Vec;
use core::fmt;

use coord_types::ids::{
    Ballot, ConfigurationEpoch, DomainId, ExecutionPosition, LocalJournalSeq, ReplicaId,
    ReplicaIncarnation,
};
use serde::{Deserialize, Serialize};

use crate::capability::{EstablishedResult, ReleasedResult};

/// Random 16-byte identity of one process boot, allocated by the world.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BootId(pub [u8; 16]);

impl fmt::Debug for BootId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BootId({:02x}{:02x}..)", self.0[0], self.0[1])
    }
}

/// Identity of a durable barrier: scoped to the node incarnation and boot so a
/// completion from an earlier boot can never satisfy a barrier of this one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BarrierId {
    /// Node incarnation.
    pub node_generation: ReplicaIncarnation,
    /// Boot that issued the barrier.
    pub boot_id: BootId,
    /// Per-boot sequence, allocated by the machine.
    pub sequence: u64,
}

/// Logical collection identifier (frozen registry in `coord-store-api`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CollectionId(pub u16);

/// One immutable logical update inside a batch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreUpdate {
    /// Target collection.
    pub collection: CollectionId,
    /// Ordered key bytes.
    #[serde(with = "scratch_bytes::vec")]
    pub key: Vec<u8>,
    /// New value, or `None` for a tombstone/delete.
    #[serde(with = "scratch_bytes::opt")]
    pub value: Option<Vec<u8>>,
}

/// SCRATCH (structural floors, the appender): byte fields through serde's
/// bytes path, not one `u8` element at a time. Postcard writes a sequence
/// of `u8` as its length and then each byte raw, which is exactly what it
/// writes for bytes, so the encoding is unchanged; reading accepts either.
pub mod scratch_bytes {
    use alloc::vec::Vec;
    use core::fmt;

    use serde::de::{self, SeqAccess, Visitor};
    use serde::{Deserializer, Serializer};

    struct BytesVisitor;

    impl<'de> Visitor<'de> for BytesVisitor {
        type Value = Vec<u8>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("bytes")
        }
        fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Vec<u8>, E> {
            Ok(v.to_vec())
        }
        fn visit_borrowed_bytes<E: de::Error>(self, v: &'de [u8]) -> Result<Vec<u8>, E> {
            Ok(v.to_vec())
        }
        fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Vec<u8>, E> {
            Ok(v)
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<u8>, A::Error> {
            let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(4096));
            while let Some(b) = seq.next_element::<u8>()? {
                out.push(b);
            }
            Ok(out)
        }
    }

    /// `Vec<u8>` as bytes.
    pub mod vec {
        use super::*;
        /// Serialize as bytes.
        pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
            s.serialize_bytes(v)
        }
        /// Deserialize from bytes (or a sequence of `u8`).
        pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
            d.deserialize_byte_buf(BytesVisitor)
        }
    }

    /// `Option<Vec<u8>>` as an option of bytes.
    pub mod opt {
        use super::*;

        struct AsBytes<'a>(&'a [u8]);
        impl serde::Serialize for AsBytes<'_> {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_bytes(self.0)
            }
        }

        struct OptVisitor;
        impl<'de> Visitor<'de> for OptVisitor {
            type Value = Option<Vec<u8>>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("optional bytes")
            }
            fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(None)
            }
            fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
                d.deserialize_byte_buf(BytesVisitor).map(Some)
            }
        }

        /// Serialize as an option of bytes.
        #[allow(clippy::ref_option)]
        pub fn serialize<S: Serializer>(v: &Option<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
            match v {
                Some(b) => s.serialize_some(&AsBytes(b)),
                None => s.serialize_none(),
            }
        }
        /// Deserialize an option of bytes.
        pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<u8>>, D::Error> {
            d.deserialize_option(OptVisitor)
        }
    }
}

/// The application base a batch was computed against (design Section 18.1).
/// Materialization rechecks it; a mismatch replans instead of applying.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ApplyBase {
    /// Configuration epoch.
    pub configuration: ConfigurationEpoch,
    /// Execution position the plan extends.
    pub execution_position: ExecutionPosition,
}

/// A complete immutable persistence request. Durability of the whole batch
/// is reported by exactly one `StorageEvent` naming `barrier`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistBatch {
    /// Barrier completed when the batch is durable.
    pub barrier: BarrierId,
    /// Base the updates were computed against, when they are an application.
    pub base: Option<ApplyBase>,
    /// Updates, applied atomically.
    pub updates: Vec<StoreUpdate>,
}

/// Context bound to every vote-producing effect (design Section 4.8).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EffectContext {
    /// Domain.
    pub domain: DomainId,
    /// Replica incarnation that produced the effect.
    pub replica_incarnation: ReplicaIncarnation,
    /// Boot that produced the effect.
    pub boot_id: BootId,
    /// Membership epoch.
    pub configuration: ConfigurationEpoch,
    /// Ballot under which the effect was produced.
    pub ballot: Ballot,
    /// Journal sequence that must be durable before release.
    pub required_journal_seq: LocalJournalSeq,
}

/// Timer identity with a logical generation; events from an older generation
/// are ignored.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TimerId {
    /// Timer name (per-machine constant).
    pub name: u32,
    /// Generation; re-arming increments it.
    pub generation: u64,
}

/// Peer identity for sends.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PeerId {
    /// Replica.
    pub replica: ReplicaId,
    /// Incarnation the sender addresses; an old incarnation never counts.
    pub incarnation: ReplicaIncarnation,
}

/// Request for an owned, bounded read view at an established base.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadViewRequest {
    /// Correlation identity.
    pub request: u64,
    /// Base the view must reflect.
    pub base: ApplyBase,
    /// Collections and key prefixes needed, bounded by the requester.
    pub prefixes: Vec<(CollectionId, Vec<u8>)>,
    /// Maximum bytes the view may hold.
    pub max_bytes: u32,
}

/// Owned effects a machine emits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    /// Persist a batch; completion arrives as a `StorageEvent`.
    Persist(PersistBatch),
    /// Build an owned read view; completion arrives as `Event::ViewReady`.
    ReadView(ReadViewRequest),
    /// Send an encoded frame to a peer once every barrier is durable in this
    /// boot and the context ballot is still current.
    SendWhenDurable {
        /// Binding context.
        context: EffectContext,
        /// Barriers that must complete first (all of them).
        requires: Vec<BarrierId>,
        /// Destination.
        to: PeerId,
        /// Complete encoded frame bytes.
        frame: Vec<u8>,
    },
    /// Arm a timer.
    ArmTimer {
        /// Timer.
        id: TimerId,
        /// Ticks from now.
        after_ticks: u64,
    },
    /// Cancel a timer (any generation at or below).
    CancelTimer {
        /// Timer.
        id: TimerId,
    },
    /// Publish an established result to the trusted boundary.
    Established(EstablishedResult),
    /// Request entropy from the world; answered by `Event::Entropy`.
    RequestEntropy {
        /// Correlation identity.
        request: u64,
    },
    /// Publish a released result (task-29) to the trusted boundary: the
    /// established result and its exact response, speculative (before
    /// materialization, under the complete learning predicate) or final.
    /// Never events or credentials.
    Released(ReleasedResult),
}
