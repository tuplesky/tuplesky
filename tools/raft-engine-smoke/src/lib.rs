//! Smoke test for the pinned `tikv/raft-engine` candidate (design Section 16.4).
//!
//! This crate proves three things about the selected Git revision under the
//! workspace's locked resolution and feature selection:
//!
//! 1. It compiles with `default-features = false` (no `rhai` scripting, no
//!    `internals`, no nightly allocator) on the supported targets.
//! 2. The codec-aware entry APIs introduced by the pinned commit
//!    (`ValueCodec`, `MessageExt<C>`, `LogBatch::add_entries_with`,
//!    `Engine::fetch_entries_to_with`) exist and accept a bounded postcard
//!    codec written here, so the task-j02 journal adapter does not need the
//!    engine's optional bincode/JSON codecs.
//! 3. `Engine::write(&mut batch, true)` returns a byte count (not a sequence)
//!    and grouped writes to several groups survive reopen.
//!
//! It is **not** the journal adapter: no stream allocation, guards, barriers or
//! materialization live here (those are task-j01 through task-j03).
#![forbid(unsafe_code)]

use raft_engine::{MessageExt, Result, ValueCodec};
use serde::{Deserialize, Serialize};

/// A minimal entry shape for the smoke test. The real durable record format is
/// `JournalRecordV1` (task-j01) and is versioned independently of this type.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmokeEntry {
    /// Local sequence used as the engine entry index.
    pub index: u64,
    /// Opaque payload bytes.
    pub payload: Vec<u8>,
}

/// Bounded postcard `ValueCodec` for [`SmokeEntry`].
///
/// Decoding refuses trailing bytes and payloads over [`MAX_PAYLOAD_BYTES`] so
/// a corrupt or oversized on-disk value cannot be silently accepted.
#[derive(Clone, Copy, Debug, Default)]
pub struct PostcardSmokeCodec;

/// Maximum payload accepted by the smoke codec.
pub const MAX_PAYLOAD_BYTES: usize = 1 << 20;

impl ValueCodec<SmokeEntry> for PostcardSmokeCodec {
    fn encode_to(v: &SmokeEntry, buf: &mut Vec<u8>) -> Result<()> {
        if v.payload.len() > MAX_PAYLOAD_BYTES {
            return Err(raft_engine::Error::Corruption(format!(
                "payload {} exceeds {} bytes",
                v.payload.len(),
                MAX_PAYLOAD_BYTES
            )));
        }
        // `to_extend` appends; bytes already in `buf` belong to other records.
        let extended = postcard::to_extend(v, core::mem::take(buf))
            .map_err(|e| raft_engine::Error::Corruption(format!("postcard encode: {e}")))?;
        *buf = extended;
        Ok(())
    }

    fn decode(bytes: &[u8]) -> Result<SmokeEntry> {
        let (entry, rest): (SmokeEntry, &[u8]) = postcard::take_from_bytes(bytes)
            .map_err(|e| raft_engine::Error::Corruption(format!("postcard decode: {e}")))?;
        if !rest.is_empty() {
            return Err(raft_engine::Error::Corruption(format!(
                "{} trailing bytes after entry",
                rest.len()
            )));
        }
        if entry.payload.len() > MAX_PAYLOAD_BYTES {
            return Err(raft_engine::Error::Corruption(
                "oversized payload".to_owned(),
            ));
        }
        Ok(entry)
    }
}

/// `MessageExt` binding [`SmokeEntry`] to [`PostcardSmokeCodec`].
#[derive(Clone, Copy, Debug, Default)]
pub struct SmokeExt;

impl MessageExt<PostcardSmokeCodec> for SmokeExt {
    type Entry = SmokeEntry;

    fn index(e: &Self::Entry) -> u64 {
        e.index
    }
}

/// Features the workspace expects to be *disabled* in the pinned engine.
///
/// The audit in `tests/` asserts the crate builds without them. Enabling any
/// of them is a reviewed change to `Cargo.toml`, never an implicit default.
pub const EXPECTED_DISABLED_FEATURES: &[&str] = &[
    "scripting",
    "internals",
    "nightly",
    "swap",
    "failpoints",
    "serde-bincode",
    "serde-json",
];

/// Pinned revision recorded for the audit report.
pub const PINNED_REVISION: &str = "097c499a19fbb38754c73aa2f31532329df7c0c6";
