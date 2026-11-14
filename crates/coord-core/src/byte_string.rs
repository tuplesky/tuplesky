//! A byte string field encoded as one: its length, then its bytes, copied
//! once.
//!
//! serde's derive serializes a `Vec<u8>` as a sequence of `u8`. postcard
//! writes that as a varint length and then each element, one `serialize_u8`
//! and one push a byte, and reads it back one `next_element` a byte. Through
//! `serialize_bytes`, postcard writes the same varint length and the same
//! bytes in one copy, and reads them in one. So a field moved to this path
//! keeps its encoding byte for byte: nothing written before it changes
//! meaning, and a node without it reads what a node with it writes.
//!
//! Reading accepts either shape a format may hand over: bytes (postcard) or
//! a sequence of `u8` (a format with no bytes type of its own).
//!
//! On a `Vec<u8>` field: `#[serde(with = "coord_core::byte_string")]`.
//! On an `Option<Vec<u8>>` field:
//! `#[serde(with = "coord_core::byte_string::option")]`.

use alloc::vec::Vec;
use core::fmt;

use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserializer, Serialize, Serializer};

/// The most a sequence's declared length reserves before its elements
/// arrive; a longer sequence grows as it is read.
const SEQ_RESERVE_LIMIT: usize = 4096;

struct BytesVisitor;

impl<'de> Visitor<'de> for BytesVisitor {
    type Value = Vec<u8>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a byte string")
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
        let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(SEQ_RESERVE_LIMIT));
        while let Some(b) = seq.next_element::<u8>()? {
            out.push(b);
        }
        Ok(out)
    }
}

/// Write a `Vec<u8>` field as a byte string.
pub fn serialize<S: Serializer>(bytes: &[u8], s: S) -> Result<S::Ok, S::Error> {
    s.serialize_bytes(bytes)
}

/// Read a `Vec<u8>` field from a byte string, or from a sequence of `u8`.
pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
    d.deserialize_byte_buf(BytesVisitor)
}

/// An `Option<Vec<u8>>` field as an option of a byte string.
pub mod option {
    use super::{BytesVisitor, Deserializer, Serialize, Serializer, Vec, Visitor, de, fmt};

    struct AsBytes<'a>(&'a [u8]);

    impl Serialize for AsBytes<'_> {
        fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            s.serialize_bytes(self.0)
        }
    }

    struct OptionVisitor;

    impl<'de> Visitor<'de> for OptionVisitor {
        type Value = Option<Vec<u8>>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("an optional byte string")
        }

        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            d.deserialize_byte_buf(BytesVisitor).map(Some)
        }
    }

    /// Write the field as an option of a byte string.
    #[allow(clippy::ref_option)] // serde's `with` hands the field by reference
    pub fn serialize<S: Serializer>(v: &Option<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(b) => s.serialize_some(&AsBytes(b)),
            None => s.serialize_none(),
        }
    }

    /// Read the field as an option of a byte string, or of a sequence of `u8`.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<u8>>, D::Error> {
        d.deserialize_option(OptionVisitor)
    }
}
