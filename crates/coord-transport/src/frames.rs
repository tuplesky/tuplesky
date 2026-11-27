//! Exact frames on streams: the `wire_v1` reader over bounded stream
//! reads, never socket chunks as messages.

use std::time::Duration;

use coord_types::wire_v1::{Frame, FrameReader, HEADER_LEN, WireError, encode_frame};
use quinn::RecvStream;
use tokio::time::timeout;

/// Kind of a peer-evidence frame: an opaque consensus message (the
/// `coord-consensus` protocol encoding) in the protocol-evidence class.
pub const KIND_PEER_EVIDENCE: u16 = 0x0300;

/// The only peer-evidence frame version this build understands. A frame
/// of any other version is refused at the boundary, not passed inward.
pub const PEER_EVIDENCE_VERSION: u16 = 1;

/// The capability a peer-plane `Hello` offers, and its `HelloAck`
/// grants, when a peer stream may carry several peer-evidence frames one
/// after another rather than exactly one (task-d61; registered in
/// `spec/wire-v1.md`). Each frame is unchanged; only how many share a
/// stream is. A link where either end does not offer it carries one
/// frame a stream, as before it existed.
pub const CAPABILITY_FRAMES_PER_STREAM: u16 = 0x0020;

/// The most frames one peer stream may carry where
/// [`CAPABILITY_FRAMES_PER_STREAM`] is granted. A stream with more is a
/// protocol violation: the bound is the protocol's, not either end's
/// configuration, so a sender configured for more still sends no more.
pub const MAX_FRAMES_PER_STREAM: usize = 256;

/// Why a frame could not be read.
#[derive(Debug)]
pub enum FrameError {
    /// The frame violates the wire schema (length, class limit, kind).
    Wire(WireError),
    /// The stream ended before the frame was complete.
    Truncated,
    /// Bytes followed the frame on a one-frame stream.
    Trailing,
    /// The frame did not arrive within the deadline.
    Timeout,
    /// The stream failed.
    Stream(String),
    /// The frame is larger than the receive budget could ever hold.
    Budget(crate::budget::BudgetError),
    /// The header named a kind, or a length, the read does not admit
    /// (task-d122). Nothing past the header was read.
    Refused {
        /// The kind the header named.
        kind: u16,
        /// The whole length it named, length field included.
        len: usize,
    },
}

/// What a frame's header must say before anything past it is read
/// (task-d122): its kind, where only one is admitted, and its whole
/// length, length field included, at most `max_len`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Admit {
    /// The one kind admitted, or any kind.
    pub kind: Option<u16>,
    /// The longest whole frame admitted.
    pub max_len: usize,
}

impl Admit {
    /// Check a complete header in `reader`, if there is one: `Ok(true)`
    /// once it is admitted, `Ok(false)` while it is incomplete.
    fn check(self, reader: &FrameReader) -> Result<bool, FrameError> {
        let Some(len) = reader.pending_frame_len() else {
            return Ok(false);
        };
        let len = len.map_err(FrameError::Wire)?;
        let kind = reader.pending_kind().unwrap_or(0);
        if self.kind.is_some_and(|k| k != kind) || len > self.max_len {
            return Err(FrameError::Refused { kind, len });
        }
        Ok(true)
    }
}

/// The receive budget a one-frame stream is read under (task-d26), and
/// the lane it arrived on.
pub type ReceiveBudget<'a> = Option<(&'a crate::budget::Budget, crate::lane::Lane)>;

const CHUNK: usize = 16 * 1024;

/// Read exactly one frame from `recv`, within `deadline`. With
/// `exact_stream`, the stream must end right after the frame.
pub async fn read_frame(
    recv: &mut RecvStream,
    deadline: Duration,
    exact_stream: bool,
) -> Result<Frame, FrameError> {
    read_frame_within(recv, deadline, exact_stream, None).await
}

/// [`read_frame`], holding the frame's whole length of `budget` from its
/// header until it is read (task-d26). The wait for room counts against
/// the deadline.
pub async fn read_frame_within(
    recv: &mut RecvStream,
    deadline: Duration,
    exact_stream: bool,
    budget: ReceiveBudget<'_>,
) -> Result<Frame, FrameError> {
    timeout(deadline, read_frame_inner(recv, exact_stream, budget, None))
        .await
        .map_err(|_| FrameError::Timeout)?
}

/// [`read_frame`], reading nothing past the header until the header is
/// one `admit` takes (task-d122), and holding no receive budget: what a
/// connection that has not bound may send is small and one to a
/// connection, so it is bounded by `admit` and by the cap on such
/// connections rather than by the budget every bound reader shares.
pub async fn read_admitted(
    recv: &mut RecvStream,
    deadline: Duration,
    exact_stream: bool,
    admit: Admit,
) -> Result<Frame, FrameError> {
    timeout(
        deadline,
        read_frame_inner(recv, exact_stream, None, Some(admit)),
    )
    .await
    .map_err(|_| FrameError::Timeout)?
}

async fn read_frame_inner(
    recv: &mut RecvStream,
    exact_stream: bool,
    budget: ReceiveBudget<'_>,
    admit: Option<Admit>,
) -> Result<Frame, FrameError> {
    let mut reader = FrameReader::new();
    let mut buf = vec![0u8; CHUNK];
    let mut _held = None;
    let mut admitted = admit.is_none();
    loop {
        if !admitted && let Some(admit) = admit {
            admitted = admit.check(&reader)?;
        }
        if let (None, Some((budget, lane)), Some(len)) =
            (&_held, budget, reader.pending_frame_len())
        {
            let len = len.map_err(FrameError::Wire)?;
            _held = Some(
                budget
                    .acquire(lane, len)
                    .await
                    .map_err(FrameError::Budget)?,
            );
        }
        // Under a budget, nothing past the header is read before the
        // frame holds its permit (Codex review): a read of a whole chunk
        // first would leave each waiting stream holding up to a chunk of
        // payload the budget never counted.
        let want = match (&_held, budget) {
            (None, Some(_)) => HEADER_LEN.saturating_sub(reader.pending()).max(1),
            _ if !admitted => HEADER_LEN.saturating_sub(reader.pending()).max(1),
            _ => CHUNK,
        };
        if let Some(frame) = reader.next_frame().map_err(FrameError::Wire)? {
            if exact_stream {
                // Nothing may follow: the reader must be empty and the
                // stream must end.
                reader.finish().map_err(|_| FrameError::Trailing)?;
                match recv.read(&mut buf).await {
                    Ok(None) => {}
                    Ok(Some(_)) => return Err(FrameError::Trailing),
                    Err(e) => return Err(FrameError::Stream(e.to_string())),
                }
            }
            return Ok(frame);
        }
        match recv.read(&mut buf[..want]).await {
            Ok(Some(n)) => reader.push(&buf[..n]).map_err(FrameError::Wire)?,
            Ok(None) => return Err(FrameError::Truncated),
            Err(e) => return Err(FrameError::Stream(e.to_string())),
        }
    }
}

/// Frames read one after another from one peer stream, until it ends
/// (task-d61).
///
/// Bytes read past a frame belong to the next one, so the reader lives
/// with the stream. Under a receive budget it never reads past the frame
/// it holds a permit for: the next frame's header is read only once the
/// current frame is complete, and its payload only once it holds its own
/// permit, so a waiting stream holds no bytes the budget did not count.
pub struct FrameStream {
    recv: RecvStream,
    reader: FrameReader,
    buf: Vec<u8>,
}

impl FrameStream {
    /// Read frames from `recv`.
    pub fn new(recv: RecvStream) -> Self {
        FrameStream {
            recv,
            reader: FrameReader::new(),
            buf: vec![0u8; CHUNK],
        }
    }

    /// The next frame within `deadline`, holding its whole length of
    /// `budget` while it is read; `None` once the stream ends between
    /// frames. A stream that ends inside a frame is truncated.
    pub async fn next_frame(
        &mut self,
        deadline: Duration,
        budget: ReceiveBudget<'_>,
    ) -> Result<Option<Frame>, FrameError> {
        timeout(
            deadline,
            Self::inner(&mut self.recv, &mut self.reader, &mut self.buf, budget),
        )
        .await
        .map_err(|_| FrameError::Timeout)?
    }

    async fn inner(
        recv: &mut RecvStream,
        reader: &mut FrameReader,
        buf: &mut [u8],
        budget: ReceiveBudget<'_>,
    ) -> Result<Option<Frame>, FrameError> {
        let mut _held = None;
        loop {
            if let Some(frame) = reader.next_frame().map_err(FrameError::Wire)? {
                return Ok(Some(frame));
            }
            let len = reader
                .pending_frame_len()
                .transpose()
                .map_err(FrameError::Wire)?;
            if let (None, Some((budget, lane)), Some(len)) = (&_held, budget, len) {
                _held = Some(
                    budget
                        .acquire(lane, len)
                        .await
                        .map_err(FrameError::Budget)?,
                );
            }
            // Under a budget, the rest of this frame's header, then the
            // rest of this frame: never into the next one.
            let want = match (budget, len) {
                (None, _) => CHUNK,
                (Some(_), None) => HEADER_LEN - reader.pending(),
                (Some(_), Some(len)) => len - reader.pending(),
            };
            match recv.read(&mut buf[..want.clamp(1, CHUNK)]).await {
                Ok(Some(n)) => reader.push(&buf[..n]).map_err(FrameError::Wire)?,
                Ok(None) => {
                    return match reader.finish() {
                        Ok(()) => Ok(None),
                        Err(_) => Err(FrameError::Truncated),
                    };
                }
                Err(e) => return Err(FrameError::Stream(e.to_string())),
            }
        }
    }
}

/// Encode an opaque consensus message as a peer-evidence frame.
pub fn evidence_frame(message: &[u8]) -> Result<Vec<u8>, WireError> {
    encode_frame(KIND_PEER_EVIDENCE, PEER_EVIDENCE_VERSION, message)
}

/// A long-lived control stream read frame by frame. One QUIC read can
/// carry the current frame and the beginning of the next, so the reader
/// that holds those bytes lives with the stream: reading a `Hello` and a
/// `Close` that arrived in one datagram must not lose the close.
pub struct ControlStream {
    recv: RecvStream,
    reader: FrameReader,
    buf: Vec<u8>,
}

impl ControlStream {
    /// Wrap a freshly accepted or opened control stream.
    pub fn new(recv: RecvStream) -> Self {
        ControlStream {
            recv,
            reader: FrameReader::new(),
            buf: vec![0u8; CHUNK],
        }
    }

    /// Read the next control frame, within `deadline`. Bytes read past it
    /// stay buffered for the following call.
    pub async fn next_frame(&mut self, deadline: Duration) -> Result<Frame, FrameError> {
        timeout(
            deadline,
            Self::inner(&mut self.recv, &mut self.reader, &mut self.buf, None),
        )
        .await
        .map_err(|_| FrameError::Timeout)?
    }

    /// [`Self::next_frame`], admitting only a frame whose header `admit`
    /// takes (task-d122). Nothing past a frame's header is read before
    /// it is admitted, and nothing past the frame after: a peer can keep
    /// no more than the admitted length buffered here, however long it
    /// leaves the stream half written.
    pub async fn next_admitted(
        &mut self,
        deadline: Duration,
        admit: Admit,
    ) -> Result<Frame, FrameError> {
        timeout(
            deadline,
            Self::inner(&mut self.recv, &mut self.reader, &mut self.buf, Some(admit)),
        )
        .await
        .map_err(|_| FrameError::Timeout)?
    }

    async fn inner(
        recv: &mut RecvStream,
        reader: &mut FrameReader,
        buf: &mut [u8],
        admit: Option<Admit>,
    ) -> Result<Frame, FrameError> {
        loop {
            let mut want = buf.len();
            if let Some(admit) = admit {
                // Read the header alone, then no further than the frame.
                want = match reader.pending_frame_len() {
                    None => HEADER_LEN.saturating_sub(reader.pending()).max(1),
                    Some(_) => {
                        admit.check(reader)?;
                        let len = reader.pending_frame_len().and_then(Result::ok).unwrap_or(0);
                        len.saturating_sub(reader.pending()).clamp(1, buf.len())
                    }
                };
            }
            if let Some(frame) = reader.next_frame().map_err(FrameError::Wire)? {
                return Ok(frame);
            }
            match recv.read(&mut buf[..want]).await {
                Ok(Some(n)) => reader.push(&buf[..n]).map_err(FrameError::Wire)?,
                Ok(None) => return Err(FrameError::Truncated),
                Err(e) => return Err(FrameError::Stream(e.to_string())),
            }
        }
    }
}
