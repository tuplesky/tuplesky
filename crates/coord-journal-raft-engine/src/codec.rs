//! Bounded postcard value codec for journal records (design Section 16.4).
//!
//! The engine stores each `JournalRecordV1` as one entry whose index is the
//! record's `LocalJournalSeq`. Encoding is the record's own durable
//! encoding; decoding re-derives the digest and refuses trailing bytes, so
//! an entry that does not match its record is corruption, never data.

use coord_journal_api::record::{JournalRecordV1, MAX_RECORD_BYTES};
use raft_engine::{MessageExt, ValueCodec};

/// `ValueCodec` for [`JournalRecordV1`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecordCodec;

fn corruption(what: &str, detail: impl core::fmt::Display) -> raft_engine::Error {
    raft_engine::Error::Corruption(format!("{what}: {detail}"))
}

impl ValueCodec<JournalRecordV1> for RecordCodec {
    fn encode_to(v: &JournalRecordV1, buf: &mut Vec<u8>) -> raft_engine::Result<()> {
        let bytes = v
            .encode()
            .map_err(|e| corruption("journal record encode", e))?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(corruption(
                "journal record encode",
                "record exceeds MAX_RECORD_BYTES",
            ));
        }
        // Append only: bytes already in `buf` belong to other records.
        buf.extend_from_slice(&bytes);
        Ok(())
    }

    fn decode(bytes: &[u8]) -> raft_engine::Result<JournalRecordV1> {
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(corruption(
                "journal record decode",
                "entry exceeds MAX_RECORD_BYTES",
            ));
        }
        JournalRecordV1::decode(bytes).map_err(|e| corruption("journal record decode", e))
    }
}

/// `MessageExt` binding [`JournalRecordV1`] to [`RecordCodec`]; the entry
/// index is the local journal sequence.
#[derive(Clone, Copy, Debug, Default)]
pub struct RecordExt;

impl MessageExt<RecordCodec> for RecordExt {
    type Entry = JournalRecordV1;

    fn index(e: &Self::Entry) -> u64 {
        e.seq().get()
    }
}
