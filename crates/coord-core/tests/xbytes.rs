//! SCRATCH, TEST-ONLY, MUST NEVER MERGE (structural floors, the appender):
//! `StoreUpdate`'s byte fields through serde's bytes path encode exactly as
//! the derived sequence of `u8` did, decode what it wrote, and cost what.
//! `cargo test --release -p coord-core --test xbytes -- --include-ignored --nocapture`
#![allow(missing_docs)]

use std::time::Instant;

use coord_core::effect::{CollectionId, StoreUpdate};
use serde::{Deserialize, Serialize};

/// The derived shape before the change: each byte a sequence element.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct OldUpdate {
    collection: CollectionId,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

fn cases() -> Vec<OldUpdate> {
    let mut out = Vec::new();
    for klen in [0usize, 1, 9, 127, 128, 300] {
        for v in [None, Some(0usize), Some(1), Some(127), Some(128), Some(630), Some(16_384), Some(70_000)] {
            out.push(OldUpdate {
                collection: CollectionId(7),
                key: (0..klen).map(|i| (i * 31 % 251) as u8).collect(),
                value: v.map(|n| (0..n).map(|i| (i * 17 % 253) as u8).collect()),
            });
        }
    }
    out
}

#[test]
fn the_bytes_path_encodes_as_the_derived_sequence_did() {
    for old in cases() {
        let new = StoreUpdate { collection: old.collection, key: old.key.clone(), value: old.value.clone() };
        let a = postcard::to_allocvec(&old).unwrap();
        let b = postcard::to_allocvec(&new).unwrap();
        assert_eq!(a, b, "key {} value {:?}", old.key.len(), old.value.as_ref().map(Vec::len));
        let back: StoreUpdate = postcard::from_bytes(&a).unwrap();
        assert_eq!(back, new);
        let back_old: OldUpdate = postcard::from_bytes(&b).unwrap();
        assert_eq!(back_old, old);
    }
}

#[test]
#[ignore = "scratch measurement, run by hand in release"]
fn what_a_row_costs_through_each_path() {
    for n in [150usize, 630, 2500] {
        let old = OldUpdate { collection: CollectionId(3), key: vec![2; 9], value: Some((0..n).map(|i| (i * 13 % 251) as u8).collect()) };
        let new = StoreUpdate { collection: old.collection, key: old.key.clone(), value: old.value.clone() };
        let iters = 200_000usize;
        let t = Instant::now();
        let mut sink = 0usize;
        for _ in 0..iters { sink += postcard::to_allocvec(&old).unwrap().len(); }
        let enc_old = t.elapsed().as_nanos() as f64 / iters as f64;
        let t = Instant::now();
        for _ in 0..iters { sink += postcard::to_allocvec(&new).unwrap().len(); }
        let enc_new = t.elapsed().as_nanos() as f64 / iters as f64;
        let bytes = postcard::to_allocvec(&old).unwrap();
        let t = Instant::now();
        for _ in 0..iters { sink += postcard::from_bytes::<OldUpdate>(&bytes).unwrap().key.len(); }
        let dec_old = t.elapsed().as_nanos() as f64 / iters as f64;
        let t = Instant::now();
        for _ in 0..iters { sink += postcard::from_bytes::<StoreUpdate>(&bytes).unwrap().key.len(); }
        let dec_new = t.elapsed().as_nanos() as f64 / iters as f64;
        println!("XBYTES value={n} encode_ns sequence={enc_old:.0} bytes={enc_new:.0} decode_ns sequence={dec_old:.0} bytes={dec_new:.0} (sink {})", sink % 7);
    }
}
