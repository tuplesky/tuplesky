//! Exact frames on streams: the `wire_v1` reader over bounded stream
//! reads, never socket chunks as messages.

use std::time::Duration;

use coord_types::wire_v1::{Frame, FrameReader, WireError, encode_frame};
use quinn::RecvStream;
use tokio::time::timeout;

/// Kind of a peer-evidence frame: an opaque consensus message (the
/// `coord-consensus` protocol encoding) in the protocol-evidence class.
pub const KIND_PEER_EVIDENCE: u16 = 0x0300;

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
}

const CHUNK: usize = 16 * 1024;

/// Read exactly one frame from `recv`, within `deadline`. With
/// `exact_stream`, the stream must end right after the frame.
pub async fn read_frame(
    recv: &mut RecvStream,
    deadline: Duration,
    exact_stream: bool,
) -> Result<Frame, FrameError> {
    timeout(deadline, read_frame_inner(recv, exact_stream))
        .await
        .map_err(|_| FrameError::Timeout)?
}

async fn read_frame_inner(recv: &mut RecvStream, exact_stream: bool) -> Result<Frame, FrameError> {
    let mut reader = FrameReader::new();
    let mut buf = vec![0u8; CHUNK];
    loop {
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
        match recv.read(&mut buf).await {
            Ok(Some(n)) => reader.push(&buf[..n]),
            Ok(None) => return Err(FrameError::Truncated),
            Err(e) => return Err(FrameError::Stream(e.to_string())),
        }
    }
}

/// Encode an opaque consensus message as a peer-evidence frame.
pub fn evidence_frame(message: &[u8]) -> Result<Vec<u8>, WireError> {
    encode_frame(KIND_PEER_EVIDENCE, 1, message)
}
