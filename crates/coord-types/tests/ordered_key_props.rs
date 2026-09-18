//! Property tests: encoded ordering equals `(namespace, key, revision)`
//! ordering under unsigned byte comparison, including zeros and prefixes.

use coord_types::ids::{KvRevision, NamespaceId};
use coord_types::ordered_key::{
    decode_current, decode_history, encode_current, encode_history, history_bounds,
};
use proptest::prelude::*;

fn key_strategy() -> impl Strategy<Value = Vec<u8>> {
    // Bias toward the interesting alphabet: zero, escape byte, 0x01 and 0xff.
    prop::collection::vec(
        prop_oneof![Just(0u8), Just(0xffu8), Just(1u8), any::<u8>()],
        0..12,
    )
}

fn ns_strategy() -> impl Strategy<Value = NamespaceId> {
    prop_oneof![
        Just([0u8; 16]),
        Just([1u8; 16]),
        Just([0xffu8; 16]),
        any::<[u8; 16]>()
    ]
    .prop_map(NamespaceId)
}

fn rev_strategy() -> impl Strategy<Value = KvRevision> {
    prop_oneof![
        Just(0u64),
        Just(1u64),
        Just(255u64),
        Just(256u64),
        Just(i64::MAX as u64),
        0..=i64::MAX as u64
    ]
    .prop_map(|r| KvRevision::new(r).unwrap())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 2000, ..ProptestConfig::default() })]

    #[test]
    fn current_order_matches_tuple_order(
        a in (ns_strategy(), key_strategy()),
        b in (ns_strategy(), key_strategy()),
    ) {
        let ea = encode_current(&a.0, &a.1);
        let eb = encode_current(&b.0, &b.1);
        let expected = (a.0.as_bytes(), a.1.as_slice()).cmp(&(b.0.as_bytes(), b.1.as_slice()));
        prop_assert_eq!(ea.cmp(&eb), expected);
    }

    #[test]
    fn history_order_matches_tuple_order(
        a in (ns_strategy(), key_strategy(), rev_strategy()),
        b in (ns_strategy(), key_strategy(), rev_strategy()),
    ) {
        let ea = encode_history(&a.0, &a.1, a.2);
        let eb = encode_history(&b.0, &b.1, b.2);
        let expected = (a.0.as_bytes(), a.1.as_slice(), a.2).cmp(&(b.0.as_bytes(), b.1.as_slice(), b.2));
        prop_assert_eq!(ea.cmp(&eb), expected);
    }

    #[test]
    fn round_trip(ns in ns_strategy(), key in key_strategy(), rev in rev_strategy()) {
        let cur = decode_current(&encode_current(&ns, &key)).unwrap();
        prop_assert_eq!(&cur.namespace, &ns);
        prop_assert_eq!(&cur.key, &key);
        prop_assert_eq!(cur.revision, None);
        let hist = decode_history(&encode_history(&ns, &key, rev)).unwrap();
        prop_assert_eq!(&hist.key, &key);
        prop_assert_eq!(hist.revision, Some(rev));
    }

    #[test]
    fn history_bounds_select_exactly_that_key(
        ns in ns_strategy(), key in key_strategy(), other in key_strategy(), rev in rev_strategy()
    ) {
        let (lo, hi) = history_bounds(&ns, &key);
        let row = encode_history(&ns, &other, rev);
        let inside = lo <= row && row < hi;
        prop_assert_eq!(inside, other == key);
    }

    #[test]
    fn mutated_encodings_never_decode_to_a_different_valid_key(
        ns in ns_strategy(), key in key_strategy(), idx in 0usize..64, byte in any::<u8>()
    ) {
        // Flipping a byte inside the escaped body either fails to decode or
        // decodes to a key whose re-encoding equals the mutated bytes (so the
        // mapping stays injective).
        let mut enc = encode_current(&ns, &key);
        let i = 16 + idx % (enc.len() - 16);
        enc[i] = byte;
        if let Ok(decoded) = decode_current(&enc) {
            prop_assert_eq!(encode_current(&decoded.namespace, &decoded.key), enc);
        }
    }
}

#[test]
fn prefix_and_zero_edge_cases() {
    let ns = NamespaceId([0; 16]);
    let cases: [&[u8]; 8] = [
        b"", b"\0", b"\0\0", b"\0\xff", b"a", b"a\0", b"a\0\0", b"ab",
    ];
    let mut encoded: Vec<Vec<u8>> = cases.iter().map(|k| encode_current(&ns, k)).collect();
    let sorted = encoded.clone();
    encoded.sort();
    assert_eq!(encoded, sorted, "cases are listed in ascending key order");
    // Every proper prefix sorts before its extension, and the current key
    // sorts before every history row of the same key.
    for k in cases {
        let cur = encode_current(&ns, k);
        let h0 = encode_history(&ns, k, KvRevision::ZERO);
        assert!(cur < h0);
        assert!(cur.len() < h0.len());
    }
}
