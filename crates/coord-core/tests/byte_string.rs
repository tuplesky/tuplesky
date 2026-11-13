//! `StoreUpdate`'s key and value are byte strings
//! (`coord_core::byte_string`). postcard writes them exactly as it wrote the
//! derived sequence of `u8`, and each shape reads what the other wrote. So
//! journals, rows and messages written before the change read the same after
//! it, and a node with it and a node without it read each other.

use coord_core::byte_string;
use coord_core::effect::{CollectionId, StoreUpdate};
use serde::de::value::{Error as ValueError, SeqDeserializer};
use serde::{Deserialize, Serialize};

/// `StoreUpdate` as derived before the change: each byte a sequence element.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SequenceUpdate {
    collection: CollectionId,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

impl SequenceUpdate {
    fn to_bytes_path(&self) -> StoreUpdate {
        StoreUpdate {
            collection: self.collection,
            key: self.key.clone(),
            value: self.value.clone(),
        }
    }
}

/// Keys and values on both sides of postcard's varint length boundaries
/// (127/128 and 16,383/16,384), empty, absent, and past 64 KiB.
fn cases() -> Vec<SequenceUpdate> {
    let mut out = Vec::new();
    for key_len in [0usize, 1, 9, 127, 128, 300] {
        for value_len in [
            None,
            Some(0usize),
            Some(1),
            Some(127),
            Some(128),
            Some(630),
            Some(16_383),
            Some(16_384),
            Some(70_000),
        ] {
            out.push(SequenceUpdate {
                collection: CollectionId(7),
                key: (0..key_len).map(|i| (i * 31 % 251) as u8).collect(),
                value: value_len.map(|n| (0..n).map(|i| (i * 17 % 253) as u8).collect()),
            });
        }
    }
    out
}

fn shape(update: &SequenceUpdate) -> String {
    format!(
        "key {} B, value {:?} B",
        update.key.len(),
        update.value.as_ref().map(Vec::len)
    )
}

#[test]
fn the_bytes_path_encodes_as_the_derived_sequence_did() {
    for old in cases() {
        let new = old.to_bytes_path();
        let sequence = postcard::to_allocvec(&old).unwrap();
        let bytes = postcard::to_allocvec(&new).unwrap();
        assert_eq!(sequence, bytes, "{}", shape(&old));

        let read_new: StoreUpdate = postcard::from_bytes(&sequence).unwrap();
        assert_eq!(
            read_new,
            new,
            "the bytes path reads the sequence: {}",
            shape(&old)
        );
        let read_old: SequenceUpdate = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(
            read_old,
            old,
            "the sequence reads the bytes path: {}",
            shape(&old)
        );
    }
}

#[test]
fn a_cut_or_overlong_row_is_refused_on_both_paths() {
    for old in cases() {
        let encoded = postcard::to_allocvec(&old).unwrap();
        for cut in [1, encoded.len() / 2, encoded.len() - 1] {
            let prefix = &encoded[..cut];
            assert!(
                postcard::from_bytes::<SequenceUpdate>(prefix).is_err(),
                "sequence, cut at {cut}: {}",
                shape(&old)
            );
            assert!(
                postcard::from_bytes::<StoreUpdate>(prefix).is_err(),
                "bytes path, cut at {cut}: {}",
                shape(&old)
            );
        }
    }
    // A key that declares about 4 GiB in a five-byte row.
    let overlong = [7u8, 0xff, 0xff, 0xff, 0x0f];
    assert!(postcard::from_bytes::<SequenceUpdate>(&overlong).is_err());
    assert!(postcard::from_bytes::<StoreUpdate>(&overlong).is_err());
}

#[test]
fn a_format_without_bytes_writes_and_reads_both_alike() {
    for old in cases()
        .into_iter()
        .filter(|c| c.value.as_ref().is_none_or(|v| v.len() <= 630))
    {
        let new = old.to_bytes_path();
        let sequence = serde_json::to_string(&old).unwrap();
        let bytes = serde_json::to_string(&new).unwrap();
        assert_eq!(sequence, bytes, "{}", shape(&old));
        let read_new: StoreUpdate = serde_json::from_str(&sequence).unwrap();
        assert_eq!(read_new, new, "{}", shape(&old));
        let read_old: SequenceUpdate = serde_json::from_str(&bytes).unwrap();
        assert_eq!(read_old, old, "{}", shape(&old));
    }
}

#[test]
fn a_sequence_of_u8_reads_as_the_same_bytes() {
    for len in [0usize, 1, 4096, 4097, 70_000] {
        let bytes: Vec<u8> = (0..len).map(|i| (i * 13 % 251) as u8).collect();
        let read =
            byte_string::deserialize(SeqDeserializer::<_, ValueError>::new(bytes.iter().copied()))
                .unwrap();
        assert_eq!(read, bytes, "{len} B");
    }
}
