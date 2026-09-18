//! Ordered-key encoding for namespace/key/revision rows (design Section 17.2).
//!
//! Layout of a current-state row key:
//!
//! ```text
//! namespace (16 bytes) || escape(key) || 0x00 0x00
//! ```
//!
//! and of a historical row key:
//!
//! ```text
//! namespace (16 bytes) || escape(key) || 0x00 0x00 || revision (8 bytes big-endian)
//! ```
//!
//! `escape` copies every byte except `0x00`, which becomes `0x00 0xff`. The
//! terminator `0x00 0x00` cannot appear inside an escaped key, so unsigned
//! lexicographic order of encodings equals the order of
//! `(namespace, key, revision)` with keys compared as unsigned byte strings
//! (a proper prefix sorts first). Decoding rejects any escape or terminator
//! the encoder never produces, so every row has exactly one encoding.
//!
//! API rejection of empty exact keys is independent of this encoding, which
//! encodes the empty key as the bare terminator.

use alloc::vec::Vec;

use crate::error::DecodeError;
use crate::ids::{KvRevision, NamespaceId};

/// Escape byte following `0x00` for a literal zero.
pub const ESCAPED_ZERO: u8 = 0xff;
/// Length of the namespace prefix.
pub const NAMESPACE_LEN: usize = 16;
/// Length of the appended revision.
pub const REVISION_LEN: usize = 8;

/// Append the escaped key and terminator to `out`.
fn escape_into(key: &[u8], out: &mut Vec<u8>) {
    out.reserve(key.len() + 2);
    for &b in key {
        if b == 0 {
            out.push(0);
            out.push(ESCAPED_ZERO);
        } else {
            out.push(b);
        }
    }
    out.push(0);
    out.push(0);
}

/// Encode a current-state row key.
pub fn encode_current(namespace: &NamespaceId, key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(NAMESPACE_LEN + key.len() + 2);
    out.extend_from_slice(namespace.as_bytes());
    escape_into(key, &mut out);
    out
}

/// Encode a historical row key with its revision.
pub fn encode_history(namespace: &NamespaceId, key: &[u8], revision: KvRevision) -> Vec<u8> {
    let mut out = Vec::with_capacity(NAMESPACE_LEN + key.len() + 2 + REVISION_LEN);
    out.extend_from_slice(namespace.as_bytes());
    escape_into(key, &mut out);
    out.extend_from_slice(&revision.to_be_bytes());
    out
}

/// The smallest history row key of `key` (revision zero) and the exclusive
/// upper bound of all history rows of exactly `key`.
pub fn history_bounds(namespace: &NamespaceId, key: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let lower = encode_history(namespace, key, KvRevision::ZERO);
    let mut upper = encode_current(namespace, key);
    upper.extend_from_slice(&[0xff; REVISION_LEN]);
    (lower, upper)
}

/// Decoded components of a row key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedKey {
    /// Namespace prefix.
    pub namespace: NamespaceId,
    /// Unescaped key bytes.
    pub key: Vec<u8>,
    /// Revision suffix, present for history rows.
    pub revision: Option<KvRevision>,
}

/// Decode namespace, key and optional revision; rejects ambiguous input.
fn decode(encoded: &[u8], expect_revision: bool) -> Result<DecodedKey, DecodeError> {
    if encoded.len() < NAMESPACE_LEN + 2 {
        return Err(DecodeError::Truncated);
    }
    let namespace = NamespaceId::from_slice(&encoded[..NAMESPACE_LEN])?;
    let body = &encoded[NAMESPACE_LEN..];
    let mut key = Vec::with_capacity(body.len());
    let mut i = 0;
    let terminator_end;
    loop {
        let Some(&b) = body.get(i) else {
            return Err(DecodeError::Truncated);
        };
        if b != 0 {
            key.push(b);
            i += 1;
            continue;
        }
        match body.get(i + 1) {
            None => return Err(DecodeError::Truncated),
            Some(&ESCAPED_ZERO) => {
                key.push(0);
                i += 2;
            }
            Some(0) => {
                terminator_end = i + 2;
                break;
            }
            Some(_) => {
                return Err(DecodeError::InvalidEscape {
                    offset: NAMESPACE_LEN + i + 1,
                });
            }
        }
    }
    let rest = &body[terminator_end..];
    let revision = if expect_revision {
        if rest.len() < REVISION_LEN {
            return Err(DecodeError::Truncated);
        }
        if rest.len() > REVISION_LEN {
            return Err(DecodeError::TrailingBytes);
        }
        Some(KvRevision::from_be_slice(rest)?)
    } else {
        if !rest.is_empty() {
            return Err(DecodeError::TrailingBytes);
        }
        None
    };
    Ok(DecodedKey {
        namespace,
        key,
        revision,
    })
}

/// Decode a current-state row key.
pub fn decode_current(encoded: &[u8]) -> Result<DecodedKey, DecodeError> {
    decode(encoded, false)
}

/// Decode a historical row key.
pub fn decode_history(encoded: &[u8]) -> Result<DecodedKey, DecodeError> {
    decode(encoded, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    fn ns(b: u8) -> NamespaceId {
        NamespaceId([b; 16])
    }

    #[test]
    fn escaping_and_termination() {
        let enc = encode_current(&ns(1), &[0, 1, 0]);
        assert_eq!(&enc[16..], &[0, 0xff, 1, 0, 0xff, 0, 0]);
        assert_eq!(decode_current(&enc).unwrap().key, vec![0, 1, 0]);
        let empty = encode_current(&ns(1), &[]);
        assert_eq!(&empty[16..], &[0, 0]);
        assert_eq!(decode_current(&empty).unwrap().key, Vec::<u8>::new());
    }

    #[test]
    fn ambiguous_encodings_are_rejected() {
        let mut bad = ns(1).0.to_vec();
        bad.extend_from_slice(&[b'a', 0, 5, 0, 0]);
        assert_eq!(
            decode_current(&bad),
            Err(DecodeError::InvalidEscape { offset: 18 })
        );
        let mut truncated = ns(1).0.to_vec();
        truncated.extend_from_slice(&[b'a', 0]);
        assert_eq!(decode_current(&truncated), Err(DecodeError::Truncated));
        let mut trailing = encode_current(&ns(1), b"a");
        trailing.push(7);
        assert_eq!(decode_current(&trailing), Err(DecodeError::TrailingBytes));
        assert_eq!(decode_current(&[0; 10]), Err(DecodeError::Truncated));
        // A history key decoded as current has trailing bytes; a current key
        // decoded as history is truncated.
        let hist = encode_history(&ns(1), b"a", KvRevision::new(3).unwrap());
        assert_eq!(decode_current(&hist), Err(DecodeError::TrailingBytes));
        assert_eq!(
            decode_history(&encode_current(&ns(1), b"a")),
            Err(DecodeError::Truncated)
        );
        let mut long = hist.clone();
        long.push(0);
        assert_eq!(decode_history(&long), Err(DecodeError::TrailingBytes));
        let mut out_of_range = encode_current(&ns(1), b"a");
        out_of_range.extend_from_slice(&[0xff; 8]);
        assert_eq!(decode_history(&out_of_range), Err(DecodeError::OutOfRange));
    }

    #[test]
    fn history_bounds_cover_exactly_that_key() {
        let (lo, hi) = history_bounds(&ns(1), b"k");
        let r1 = encode_history(&ns(1), b"k", KvRevision::new(1).unwrap());
        let rmax = encode_history(&ns(1), b"k", KvRevision::MAX);
        let other = encode_history(&ns(1), b"k\0", KvRevision::ZERO);
        let longer = encode_history(&ns(1), b"kk", KvRevision::ZERO);
        assert!(lo <= r1 && r1 < hi);
        assert!(rmax < hi);
        assert!(!(lo <= other && other < hi));
        assert!(!(lo <= longer && longer < hi));
    }
}
