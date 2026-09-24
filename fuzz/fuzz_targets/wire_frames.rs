#![no_main]
//! Fuzz the bounded frame reader and DTO decoder: no panics, no unbounded
//! allocation, and every successful decode re-encodes to itself.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    coord_types::wire_v1::fuzz_entry(data);
});
