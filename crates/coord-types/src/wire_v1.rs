//! `wire_v1`: bounded frames and stable DTOs for the native transport
//! (design Sections 11.2, 19.1, 6.7.1, 10.5).
//!
//! Frame layout (all integers big-endian):
//!
//! ```text
//! u32 frame_length      // excludes these four bytes, includes kind + version
//! u16 message_kind
//! u16 schema_version
//! postcard_payload[frame_length - 4]
//! ```
//!
//! [`FrameReader`] reads bounded exact frames from a byte stream: it rejects
//! lengths below four, lengths above the kind's class limit, incomplete
//! frames at end of stream and any bytes after the last complete frame. The
//! checks happen before payload allocation. [`decode`] then maps
//! `(kind, version)` to a typed DTO, rejecting unknown kinds, unsupported
//! versions, trailing payload bytes, integer overflow and oversized nested
//! collections.
//!
//! DTOs are stable wire types, not implementation enums: field and variant
//! order are frozen (see `spec/wire-v1.md`). Kind ranges for protocol
//! evidence, configuration, observer replication, snapshots, collector
//! evidence and read fences are reserved here; their DTOs arrive with their
//! tasks. The durable journal representation is versioned separately.

use alloc::vec::Vec;
use core::fmt;
use core::marker::PhantomData;

use serde::de::{SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::identity::{CommandId, RetryKey};
use crate::ids::{ClusterId, DomainId, KvRevision, NamespaceId, ReplicaIncarnation};
use crate::logical_v1::{LogicalRequest, limits};

/// Fixed header length: length field plus kind plus version.
pub const HEADER_LEN: usize = 8;
/// Minimum legal `frame_length` (kind and version only).
pub const MIN_FRAME_LENGTH: u32 = 4;

/// Message kind ranges. Each range has one frame class and size limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum KindRange {
    /// Connection negotiation and close.
    Negotiation,
    /// Native unary API.
    Api,
    /// Watch streams.
    Watch,
    /// SwiftPaxos protocol evidence between voters (task-19+).
    ProtocolEvidence,
    /// Authoritative configuration records and hints (task-m01).
    Configuration,
    /// Finalized-frame observer replication (task-o01).
    ObserverReplication,
    /// Snapshot and recovery pages (task-49/task-50).
    Snapshot,
    /// Trusted-collector evidence (task-33/task-m02).
    CollectorEvidence,
    /// Ordered read fences (task-o05).
    ReadFence,
}

impl KindRange {
    /// Classify a raw kind. `None` for kinds outside every reserved range.
    pub const fn of(kind: u16) -> Option<KindRange> {
        match kind >> 8 {
            0x00 => Some(KindRange::Negotiation),
            0x01 => Some(KindRange::Api),
            0x02 => Some(KindRange::Watch),
            0x03 => Some(KindRange::ProtocolEvidence),
            0x04 => Some(KindRange::Configuration),
            0x05 => Some(KindRange::ObserverReplication),
            0x06 => Some(KindRange::Snapshot),
            0x07 => Some(KindRange::CollectorEvidence),
            0x08 => Some(KindRange::ReadFence),
            _ => None,
        }
    }

    /// Maximum `frame_length` for frames of this range (Section 19.3).
    pub const fn max_frame_length(self) -> u32 {
        match self {
            KindRange::Negotiation => 64 * 1024,
            KindRange::Api => 3 * 1024 * 1024,
            KindRange::Watch => 8 * 1024 * 1024 + 64 * 1024,
            KindRange::ProtocolEvidence => 4 * 1024 * 1024,
            KindRange::Configuration => 256 * 1024,
            KindRange::ObserverReplication => 8 * 1024 * 1024 + 64 * 1024,
            KindRange::Snapshot => 1024 * 1024 + 64 * 1024,
            KindRange::CollectorEvidence => 1024 * 1024,
            KindRange::ReadFence => 64 * 1024,
        }
    }
}

/// Registered message kinds with frozen discriminants.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum MessageKind {
    /// First frame on a connection: role, origin and capabilities.
    Hello = 0x0001,
    /// Acceptance of a hello with negotiated capabilities.
    HelloAck = 0x0002,
    /// Orderly close with a reason.
    Close = 0x0003,
    /// Client request carrying a canonical logical request.
    Request = 0x0100,
    /// Final result of a request.
    Response = 0x0101,
    /// Resolve the outcome of a previously submitted request.
    ResolveRequest = 0x0102,
    /// Open a watch.
    WatchOpen = 0x0200,
    /// One complete-revision event batch.
    WatchEvents = 0x0201,
    /// Progress notification.
    WatchProgress = 0x0202,
    /// Watch closed by either side.
    WatchClose = 0x0203,
}

impl MessageKind {
    /// Map a raw kind to a registered one.
    pub const fn from_u16(kind: u16) -> Option<MessageKind> {
        Some(match kind {
            0x0001 => MessageKind::Hello,
            0x0002 => MessageKind::HelloAck,
            0x0003 => MessageKind::Close,
            0x0100 => MessageKind::Request,
            0x0101 => MessageKind::Response,
            0x0102 => MessageKind::ResolveRequest,
            0x0200 => MessageKind::WatchOpen,
            0x0201 => MessageKind::WatchEvents,
            0x0202 => MessageKind::WatchProgress,
            0x0203 => MessageKind::WatchClose,
            _ => return None,
        })
    }

    /// Raw kind.
    pub const fn as_u16(self) -> u16 {
        self as u16
    }

    /// Range of this kind.
    pub const fn range(self) -> KindRange {
        match KindRange::of(self.as_u16()) {
            Some(r) => r,
            None => KindRange::Negotiation,
        }
    }

    /// Schema versions this decoder supports for the kind.
    pub const fn supported_versions(self) -> &'static [u16] {
        &[1]
    }
}

/// Errors from framing and decoding. None carries payload bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WireError {
    /// `frame_length` below four.
    LengthBelowMinimum {
        /// Declared length.
        length: u32,
    },
    /// `frame_length` above the class limit for its kind, or the kind is
    /// outside every reserved range (then the smallest limit applies).
    LengthAboveClassLimit {
        /// Declared length.
        length: u32,
        /// Applicable limit.
        limit: u32,
    },
    /// The stream ended inside a header or payload.
    IncompleteFrame {
        /// Bytes present.
        have: usize,
        /// Bytes needed for the current frame.
        need: usize,
    },
    /// Kind not registered.
    UnsupportedKind {
        /// Raw kind.
        kind: u16,
    },
    /// Kind registered but version unsupported.
    UnsupportedVersion {
        /// Kind.
        kind: MessageKind,
        /// Raw version.
        version: u16,
    },
    /// Payload did not decode as the DTO (includes varint overflow, bad
    /// enum discriminants and truncated fields).
    MalformedPayload,
    /// Bytes remained after the DTO.
    TrailingPayloadBytes {
        /// Number of extra bytes.
        extra: usize,
    },
    /// A bounded collection or byte string exceeded its declared limit.
    CollectionTooLarge {
        /// Declared element count.
        declared: usize,
        /// Limit.
        limit: usize,
    },
    /// An identity-bearing payload was not in canonical encoding.
    NonCanonicalPayload,
    /// The payload does not fit the frame class.
    PayloadTooLarge,
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WireError::LengthBelowMinimum { length } => {
                write!(f, "frame length {length} below minimum")
            }
            WireError::LengthAboveClassLimit { length, limit } => {
                write!(f, "frame length {length} above class limit {limit}")
            }
            WireError::IncompleteFrame { have, need } => {
                write!(f, "incomplete frame: have {have} of {need} bytes")
            }
            WireError::UnsupportedKind { kind } => {
                write!(f, "unsupported message kind {kind:#06x}")
            }
            WireError::UnsupportedVersion { kind, version } => {
                write!(f, "unsupported version {version} for {kind:?}")
            }
            WireError::MalformedPayload => f.write_str("malformed payload"),
            WireError::TrailingPayloadBytes { extra } => {
                write!(f, "{extra} trailing payload bytes")
            }
            WireError::CollectionTooLarge { declared, limit } => {
                write!(f, "collection of {declared} exceeds limit {limit}")
            }
            WireError::NonCanonicalPayload => f.write_str("non-canonical identity payload"),
            WireError::PayloadTooLarge => f.write_str("payload too large for frame class"),
        }
    }
}

impl core::error::Error for WireError {}

/// A complete frame with its raw payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    /// Raw kind (may be unregistered; `decode` rejects it then).
    pub kind: u16,
    /// Raw schema version.
    pub version: u16,
    /// Postcard payload.
    pub payload: Vec<u8>,
}

/// Validate a header and return the total frame size including the length
/// field. Performed before any payload allocation.
pub fn check_header(header: &[u8; HEADER_LEN]) -> Result<usize, WireError> {
    let length = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
    let kind = u16::from_be_bytes([header[4], header[5]]);
    if length < MIN_FRAME_LENGTH {
        return Err(WireError::LengthBelowMinimum { length });
    }
    let limit = KindRange::of(kind).map_or(
        KindRange::ReadFence.max_frame_length(),
        KindRange::max_frame_length,
    );
    if length > limit {
        return Err(WireError::LengthAboveClassLimit { length, limit });
    }
    Ok(4 + length as usize)
}

/// Encode one frame. Fails when the payload exceeds the class limit.
pub fn encode_frame(kind: u16, version: u16, payload: &[u8]) -> Result<Vec<u8>, WireError> {
    let length = u32::try_from(
        payload
            .len()
            .checked_add(4)
            .ok_or(WireError::PayloadTooLarge)?,
    )
    .map_err(|_| WireError::PayloadTooLarge)?;
    let limit = KindRange::of(kind).map_or(
        KindRange::ReadFence.max_frame_length(),
        KindRange::max_frame_length,
    );
    if length > limit {
        return Err(WireError::PayloadTooLarge);
    }
    let mut out = Vec::with_capacity(4 + length as usize);
    out.extend_from_slice(&length.to_be_bytes());
    out.extend_from_slice(&kind.to_be_bytes());
    out.extend_from_slice(&version.to_be_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

/// Incremental bounded frame reader over a byte stream.
///
/// Feed bytes with [`FrameReader::push`], pull frames with
/// [`FrameReader::next_frame`], and call [`FrameReader::finish`] when the
/// stream ends: leftover bytes are an error, never silently ignored.
#[derive(Debug, Default)]
pub struct FrameReader {
    buf: Vec<u8>,
    start: usize,
}

impl FrameReader {
    /// New empty reader.
    pub const fn new() -> Self {
        FrameReader {
            buf: Vec::new(),
            start: 0,
        }
    }

    /// Append stream bytes. Buffering is bounded by the largest class limit
    /// plus one header because [`Self::next_frame`] must be drained between
    /// pushes to keep it that way; callers enforce their own read budget.
    pub fn push(&mut self, bytes: &[u8]) {
        if self.start > 0 && self.start == self.buf.len() {
            self.buf.clear();
            self.start = 0;
        }
        self.buf.extend_from_slice(bytes);
    }

    /// Bytes buffered but not yet consumed as frames.
    pub fn pending(&self) -> usize {
        self.buf.len() - self.start
    }

    /// Next complete frame, `Ok(None)` when more bytes are needed. Header
    /// errors are returned as soon as the header is complete.
    pub fn next_frame(&mut self) -> Result<Option<Frame>, WireError> {
        let avail = &self.buf[self.start..];
        if avail.len() < HEADER_LEN {
            return Ok(None);
        }
        let mut header = [0u8; HEADER_LEN];
        header.copy_from_slice(&avail[..HEADER_LEN]);
        let total = check_header(&header)?;
        if avail.len() < total {
            return Ok(None);
        }
        let kind = u16::from_be_bytes([header[4], header[5]]);
        let version = u16::from_be_bytes([header[6], header[7]]);
        let payload = avail[HEADER_LEN..total].to_vec();
        self.start += total;
        if self.start == self.buf.len() {
            self.buf.clear();
            self.start = 0;
        }
        Ok(Some(Frame {
            kind,
            version,
            payload,
        }))
    }

    /// The stream has ended: any buffered partial frame is an error.
    pub fn finish(&self) -> Result<(), WireError> {
        let avail = &self.buf[self.start..];
        if avail.is_empty() {
            return Ok(());
        }
        let need = if avail.len() >= HEADER_LEN {
            let mut header = [0u8; HEADER_LEN];
            header.copy_from_slice(&avail[..HEADER_LEN]);
            check_header(&header)?
        } else {
            HEADER_LEN
        };
        Err(WireError::IncompleteFrame {
            have: avail.len(),
            need,
        })
    }
}

/// Bounded byte string: at most `N` bytes. The bound is checked before any
/// allocation during deserialization.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BoundedBytes<const N: usize>(Vec<u8>);

impl<const N: usize> BoundedBytes<N> {
    /// Wrap bytes, rejecting more than `N`.
    pub fn new(bytes: Vec<u8>) -> Result<Self, WireError> {
        if bytes.len() > N {
            return Err(WireError::CollectionTooLarge {
                declared: bytes.len(),
                limit: N,
            });
        }
        Ok(BoundedBytes(bytes))
    }

    /// Borrow the bytes.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Take the bytes.
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }
}

impl<const N: usize> fmt::Debug for BoundedBytes<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BoundedBytes<{N}>({} bytes)", self.0.len())
    }
}

impl<const N: usize> Serialize for BoundedBytes<N> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

struct BytesVisitor<const N: usize>;

impl<'de, const N: usize> Visitor<'de> for BytesVisitor<N> {
    type Value = BoundedBytes<N>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "at most {N} bytes")
    }

    fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
        if v.len() > N {
            return Err(E::invalid_length(v.len(), &self));
        }
        Ok(BoundedBytes(v.to_vec()))
    }

    fn visit_borrowed_bytes<E: serde::de::Error>(self, v: &'de [u8]) -> Result<Self::Value, E> {
        self.visit_bytes(v)
    }

    fn visit_byte_buf<E: serde::de::Error>(self, v: Vec<u8>) -> Result<Self::Value, E> {
        if v.len() > N {
            return Err(E::invalid_length(v.len(), &self));
        }
        Ok(BoundedBytes(v))
    }
}

impl<'de, const N: usize> Deserialize<'de> for BoundedBytes<N> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_bytes(BytesVisitor::<N>)
    }
}

/// Bounded sequence: at most `N` elements. The declared length is checked
/// against `N` before any element is allocated.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BoundedVec<T, const N: usize>(Vec<T>);

impl<T, const N: usize> BoundedVec<T, N> {
    /// Wrap a vector, rejecting more than `N` elements.
    pub fn new(items: Vec<T>) -> Result<Self, WireError> {
        if items.len() > N {
            return Err(WireError::CollectionTooLarge {
                declared: items.len(),
                limit: N,
            });
        }
        Ok(BoundedVec(items))
    }

    /// Borrow the elements.
    pub fn as_slice(&self) -> &[T] {
        &self.0
    }

    /// Take the elements.
    pub fn into_inner(self) -> Vec<T> {
        self.0
    }
}

impl<T: fmt::Debug, const N: usize> fmt::Debug for BoundedVec<T, N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BoundedVec<{N}>")?;
        f.debug_list().entries(&self.0).finish()
    }
}

impl<T: Serialize, const N: usize> Serialize for BoundedVec<T, N> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(&self.0)
    }
}

struct VecVisitor<T, const N: usize>(PhantomData<T>);

impl<'de, T: Deserialize<'de>, const N: usize> Visitor<'de> for VecVisitor<T, N> {
    type Value = BoundedVec<T, N>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a sequence of at most {N} elements")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        if let Some(declared) = seq.size_hint()
            && declared > N
        {
            return Err(serde::de::Error::invalid_length(declared, &self));
        }
        let mut items = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(N));
        while let Some(item) = seq.next_element()? {
            if items.len() >= N {
                return Err(serde::de::Error::invalid_length(N + 1, &self));
            }
            items.push(item);
        }
        Ok(BoundedVec(items))
    }
}

impl<'de, T: Deserialize<'de>, const N: usize> Deserialize<'de> for BoundedVec<T, N> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_seq(VecVisitor::<T, N>(PhantomData))
    }
}

/// Bound for human-readable reason strings.
pub const MAX_REASON_BYTES: usize = 256;
/// Bound for negotiated capability lists.
pub const MAX_CAPABILITIES: usize = 64;
/// Bound for events in one watch batch (transport chunk of a revision).
pub const MAX_EVENTS_PER_BATCH: usize = 4096;
/// Bound for opaque result bytes in a response.
pub const MAX_RESULT_BYTES: usize = 8 * 1024 * 1024;

/// Peer role bound at the first frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PeerRole {
    /// Native SDK client (untrusted).
    Client,
    /// Trusted frontend collector.
    Frontend,
    /// Authorized Kine collector for one domain.
    KineCollector,
    /// Voting replica.
    Voter,
    /// Non-voting observer or relay.
    Observer,
    /// Staging learner.
    Learner,
}

/// First frame on a connection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HelloV1 {
    /// Declared role.
    pub role: PeerRole,
    /// Cluster the peer claims to belong to.
    pub cluster_id: ClusterId,
    /// Domain scope of this connection.
    pub domain_id: DomainId,
    /// Node incarnation for voters/observers (`None` for clients).
    pub incarnation: Option<ReplicaIncarnation>,
    /// Requested capabilities (frozen numeric identifiers, sorted).
    pub capabilities: BoundedVec<u16, MAX_CAPABILITIES>,
}

/// Acceptance of a hello.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HelloAckV1 {
    /// Capabilities granted (subset of requested).
    pub capabilities: BoundedVec<u16, MAX_CAPABILITIES>,
    /// Maximum concurrent unary requests the peer may keep in flight.
    pub max_inflight: u32,
}

/// Orderly close.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloseV1 {
    /// Machine-readable code.
    pub code: u16,
    /// Bounded, redacted reason.
    pub reason: BoundedBytes<MAX_REASON_BYTES>,
}

/// A client request. The logical request travels as its canonical
/// `logical_v1` postcard bytes so identity is preserved bit-exactly.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestV1 {
    /// Stable invocation identity.
    pub retry_key: RetryKey,
    /// Canonical `LogicalRequest` encoding; checked by [`RequestV1::logical`].
    pub logical: BoundedBytes<{ limits::MAX_REQUEST_BYTES + 64 * 1024 }>,
    /// Client deadline in milliseconds from admission (0 = none).
    pub deadline_ms: u32,
}

impl RequestV1 {
    /// Build from a canonical request.
    pub fn new(
        retry_key: RetryKey,
        request: &LogicalRequest,
        deadline_ms: u32,
    ) -> Result<Self, WireError> {
        let bytes = request
            .canonical_bytes()
            .map_err(|_| WireError::NonCanonicalPayload)?;
        Ok(RequestV1 {
            retry_key,
            logical: BoundedBytes::new(bytes)?,
            deadline_ms,
        })
    }

    /// Decode the embedded logical request and verify it is canonical: the
    /// decoded value must validate and re-encode to exactly the same bytes.
    pub fn logical(&self) -> Result<LogicalRequest, WireError> {
        let (request, rest): (LogicalRequest, &[u8]) =
            postcard::take_from_bytes(self.logical.as_slice())
                .map_err(|_| WireError::MalformedPayload)?;
        if !rest.is_empty() {
            return Err(WireError::TrailingPayloadBytes { extra: rest.len() });
        }
        let canonical = request
            .canonical_bytes()
            .map_err(|_| WireError::NonCanonicalPayload)?;
        if canonical != self.logical.as_slice() {
            return Err(WireError::NonCanonicalPayload);
        }
        Ok(request)
    }
}

/// Outcome of a request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutcomeV1 {
    /// Established result.
    Ok {
        /// KV revision produced, if the command mutated KV.
        revision: Option<KvRevision>,
        /// Encoded result (schema per operation; opaque here).
        result: BoundedBytes<MAX_RESULT_BYTES>,
    },
    /// Established error.
    Err {
        /// Frozen error code.
        code: u16,
        /// Bounded, redacted detail.
        detail: BoundedBytes<MAX_REASON_BYTES>,
    },
    /// Outcome not yet resolvable; retry `ResolveRequest` later.
    Pending,
    /// Outcome unknown to this endpoint (session retired or history lost).
    Unknown,
}

/// Final response.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseV1 {
    /// Identity the response belongs to.
    pub command_id: CommandId,
    /// Outcome.
    pub outcome: OutcomeV1,
}

/// Resolve a previously submitted request by identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolveRequestV1 {
    /// Stable invocation identity.
    pub retry_key: RetryKey,
    /// Expected command identity (mismatch is a conflict).
    pub command_id: CommandId,
}

/// Open a watch on a key or interval.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchOpenV1 {
    /// Client-chosen watch identifier, unique per connection.
    pub watch_id: u64,
    /// Namespace.
    pub namespace: NamespaceId,
    /// Start key.
    pub key: BoundedBytes<{ limits::MAX_KEY_BYTES }>,
    /// Exclusive end (`None` for an exact key).
    pub range_end: Option<BoundedBytes<{ limits::MAX_KEY_BYTES }>>,
    /// First revision to deliver (inclusive); `None` for live only.
    pub start_revision: Option<KvRevision>,
    /// Include previous values.
    pub prev_kv: bool,
    /// Request progress notifications.
    pub progress_notify: bool,
}

/// Event type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventKindV1 {
    /// Put (create or update).
    Put,
    /// Delete (including lease expiry/revocation).
    Delete,
}

/// One KV event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventV1 {
    /// Event type.
    pub kind: EventKindV1,
    /// Key.
    pub key: BoundedBytes<{ limits::MAX_KEY_BYTES }>,
    /// New value (empty for delete).
    pub value: BoundedBytes<{ limits::MAX_VALUE_BYTES }>,
    /// Creation revision.
    pub create_revision: KvRevision,
    /// Modification revision.
    pub mod_revision: KvRevision,
    /// Version counter.
    pub version: u64,
    /// Previous value when requested.
    pub prev_value: Option<BoundedBytes<{ limits::MAX_VALUE_BYTES }>>,
}

/// A transport chunk of one revision's events. `complete` marks the last
/// chunk of that revision; consumers publish nothing before it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchEventsV1 {
    /// Watch.
    pub watch_id: u64,
    /// Revision of every event in this chunk.
    pub revision: KvRevision,
    /// Events.
    pub events: BoundedVec<EventV1, MAX_EVENTS_PER_BATCH>,
    /// Last chunk of the revision.
    pub complete: bool,
}

/// Progress: every relevant event through `revision` has crossed this
/// watch's delivery point.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchProgressV1 {
    /// Watch.
    pub watch_id: u64,
    /// Delivered complete revision.
    pub revision: KvRevision,
}

/// Why a watch closed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WatchCloseReasonV1 {
    /// Client cancelled.
    Cancelled,
    /// Start revision was compacted; resume requires a new list.
    Compacted,
    /// Consumer too slow; resume from the last complete revision.
    SlowConsumer,
    /// Authorization lost.
    Unauthorized,
    /// Source lost; resume elsewhere.
    SourceLost,
}

/// Watch close.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchCloseV1 {
    /// Watch.
    pub watch_id: u64,
    /// Reason.
    pub reason: WatchCloseReasonV1,
    /// Last complete revision delivered (resume point).
    pub last_complete_revision: Option<KvRevision>,
}

/// Every decodable message of this schema version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MessageV1 {
    /// [`HelloV1`].
    Hello(HelloV1),
    /// [`HelloAckV1`].
    HelloAck(HelloAckV1),
    /// [`CloseV1`].
    Close(CloseV1),
    /// [`RequestV1`].
    Request(RequestV1),
    /// [`ResponseV1`].
    Response(ResponseV1),
    /// [`ResolveRequestV1`].
    ResolveRequest(ResolveRequestV1),
    /// [`WatchOpenV1`].
    WatchOpen(WatchOpenV1),
    /// [`WatchEventsV1`].
    WatchEvents(WatchEventsV1),
    /// [`WatchProgressV1`].
    WatchProgress(WatchProgressV1),
    /// [`WatchCloseV1`].
    WatchClose(WatchCloseV1),
}

impl MessageV1 {
    /// Kind of this message.
    pub const fn kind(&self) -> MessageKind {
        match self {
            MessageV1::Hello(_) => MessageKind::Hello,
            MessageV1::HelloAck(_) => MessageKind::HelloAck,
            MessageV1::Close(_) => MessageKind::Close,
            MessageV1::Request(_) => MessageKind::Request,
            MessageV1::Response(_) => MessageKind::Response,
            MessageV1::ResolveRequest(_) => MessageKind::ResolveRequest,
            MessageV1::WatchOpen(_) => MessageKind::WatchOpen,
            MessageV1::WatchEvents(_) => MessageKind::WatchEvents,
            MessageV1::WatchProgress(_) => MessageKind::WatchProgress,
            MessageV1::WatchClose(_) => MessageKind::WatchClose,
        }
    }

    /// Encode as a complete frame (version 1).
    pub fn encode(&self) -> Result<Vec<u8>, WireError> {
        let payload = match self {
            MessageV1::Hello(m) => postcard::to_allocvec(m),
            MessageV1::HelloAck(m) => postcard::to_allocvec(m),
            MessageV1::Close(m) => postcard::to_allocvec(m),
            MessageV1::Request(m) => postcard::to_allocvec(m),
            MessageV1::Response(m) => postcard::to_allocvec(m),
            MessageV1::ResolveRequest(m) => postcard::to_allocvec(m),
            MessageV1::WatchOpen(m) => postcard::to_allocvec(m),
            MessageV1::WatchEvents(m) => postcard::to_allocvec(m),
            MessageV1::WatchProgress(m) => postcard::to_allocvec(m),
            MessageV1::WatchClose(m) => postcard::to_allocvec(m),
        }
        .map_err(|_| WireError::PayloadTooLarge)?;
        encode_frame(self.kind().as_u16(), 1, &payload)
    }
}

fn decode_exact<'a, T: Deserialize<'a>>(payload: &'a [u8]) -> Result<T, WireError> {
    let (value, rest): (T, &[u8]) =
        postcard::take_from_bytes(payload).map_err(|_| WireError::MalformedPayload)?;
    if !rest.is_empty() {
        return Err(WireError::TrailingPayloadBytes { extra: rest.len() });
    }
    Ok(value)
}

/// Decode a frame into a typed message. Unknown kinds and unsupported
/// versions are rejected before the payload is inspected.
pub fn decode(frame: &Frame) -> Result<MessageV1, WireError> {
    let kind =
        MessageKind::from_u16(frame.kind).ok_or(WireError::UnsupportedKind { kind: frame.kind })?;
    if !kind.supported_versions().contains(&frame.version) {
        return Err(WireError::UnsupportedVersion {
            kind,
            version: frame.version,
        });
    }
    let p = frame.payload.as_slice();
    Ok(match kind {
        MessageKind::Hello => MessageV1::Hello(decode_exact(p)?),
        MessageKind::HelloAck => MessageV1::HelloAck(decode_exact(p)?),
        MessageKind::Close => MessageV1::Close(decode_exact(p)?),
        MessageKind::Request => MessageV1::Request(decode_exact(p)?),
        MessageKind::Response => MessageV1::Response(decode_exact(p)?),
        MessageKind::ResolveRequest => MessageV1::ResolveRequest(decode_exact(p)?),
        MessageKind::WatchOpen => MessageV1::WatchOpen(decode_exact(p)?),
        MessageKind::WatchEvents => MessageV1::WatchEvents(decode_exact(p)?),
        MessageKind::WatchProgress => MessageV1::WatchProgress(decode_exact(p)?),
        MessageKind::WatchClose => MessageV1::WatchClose(decode_exact(p)?),
    })
}

/// Decode every frame of a complete stream; used by fixtures and the fuzz
/// harness. Any error aborts, and leftover bytes are an error.
pub fn decode_stream(bytes: &[u8]) -> Result<Vec<MessageV1>, WireError> {
    let mut reader = FrameReader::new();
    reader.push(bytes);
    let mut out = Vec::new();
    while let Some(frame) = reader.next_frame()? {
        out.push(decode(&frame)?);
    }
    reader.finish()?;
    Ok(out)
}

/// Fuzz entry point: must never panic or allocate beyond the class limits.
/// Successful decodes must re-encode and decode to the same messages.
pub fn fuzz_entry(data: &[u8]) {
    if let Ok(messages) = decode_stream(data) {
        for message in &messages {
            let encoded = message.encode().expect("decoded message re-encodes");
            let again = decode_stream(&encoded).expect("re-encoded frame decodes");
            assert_eq!(again.len(), 1);
            assert_eq!(&again[0], message);
        }
    }
}
