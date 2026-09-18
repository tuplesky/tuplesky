//! Codec/platform smoke test against the pinned raft-engine revision.

use raft_engine::{Config, Engine, LogBatch, ReadableSize, ValueCodec};
use raft_engine_smoke::{PostcardSmokeCodec, SmokeEntry, SmokeExt};

fn config(dir: &tempfile::TempDir) -> Config {
    Config {
        dir: dir.path().to_string_lossy().into_owned(),
        target_file_size: ReadableSize::mb(1),
        purge_threshold: ReadableSize::mb(8),
        ..Config::default()
    }
}

fn entries(group: u64, first: u64, count: u64) -> Vec<SmokeEntry> {
    (first..first + count)
        .map(|index| SmokeEntry {
            index,
            payload: format!("g{group}-i{index}").into_bytes(),
        })
        .collect()
}

#[test]
fn codec_round_trip_rejects_trailing_bytes() {
    let entry = SmokeEntry {
        index: 7,
        payload: vec![0, 1, 2, 0xff],
    };
    let mut buf = b"prefix".to_vec();
    PostcardSmokeCodec::encode_to(&entry, &mut buf).unwrap();
    assert_eq!(&buf[..6], b"prefix", "encode_to must append, never clobber");
    let decoded = PostcardSmokeCodec::decode(&buf[6..]).unwrap();
    assert_eq!(decoded, entry);

    let mut trailing = buf[6..].to_vec();
    trailing.push(0);
    assert!(
        PostcardSmokeCodec::decode(&trailing).is_err(),
        "trailing byte must be rejected"
    );
    assert!(
        PostcardSmokeCodec::decode(&[]).is_err(),
        "truncated input must be rejected"
    );
}

#[test]
fn grouped_synced_writes_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let written_bytes;
    {
        let engine = Engine::open(config(&dir)).unwrap();
        let mut batch = LogBatch::default();
        // Two logical streams (groups) in one batch: the multi-group write path.
        batch
            .add_entries_with::<SmokeExt, PostcardSmokeCodec>(11, &entries(11, 1, 3))
            .unwrap();
        batch
            .add_entries_with::<SmokeExt, PostcardSmokeCodec>(22, &entries(22, 1, 2))
            .unwrap();
        batch
            .put(11, b"meta".to_vec(), b"stream-11".to_vec())
            .unwrap();
        written_bytes = engine.write(&mut batch, true).unwrap();
        // The return value is a byte count, never a sequence number (Section 17.3.3).
        assert!(written_bytes > 0);
        assert!(batch.is_empty(), "a written batch is consumed");

        // An empty batch with sync=true performs a sync and writes zero bytes.
        let mut empty = LogBatch::default();
        assert_eq!(engine.write(&mut empty, true).unwrap(), 0);
    }
    {
        let engine = Engine::open(config(&dir)).unwrap();
        let mut got = Vec::new();
        let n = engine
            .fetch_entries_to_with::<SmokeExt, PostcardSmokeCodec>(11, 1, 4, None, &mut got)
            .unwrap();
        assert_eq!(n, 3);
        assert_eq!(got, entries(11, 1, 3));
        assert_eq!(engine.first_index(11), Some(1));
        assert_eq!(engine.last_index(11), Some(3));
        assert_eq!(engine.last_index(22), Some(2));
        assert_eq!(engine.get(11, b"meta"), Some(b"stream-11".to_vec()));
        let single = engine
            .get_entry_with::<SmokeExt, PostcardSmokeCodec>(22, 2)
            .unwrap();
        assert_eq!(single, Some(entries(22, 2, 1).remove(0)));
        assert_eq!(
            engine
                .get_entry_with::<SmokeExt, PostcardSmokeCodec>(22, 3)
                .unwrap(),
            None
        );
        let mut groups = engine.raft_groups();
        groups.sort_unstable();
        assert_eq!(groups, vec![11, 22]);
    }
}

#[test]
fn oversized_payload_is_refused_before_write() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(config(&dir)).unwrap();
    let mut batch = LogBatch::default();
    let huge = SmokeEntry {
        index: 1,
        payload: vec![0u8; raft_engine_smoke::MAX_PAYLOAD_BYTES + 1],
    };
    let err = batch
        .add_entries_with::<SmokeExt, PostcardSmokeCodec>(1, &[huge])
        .unwrap_err();
    assert!(matches!(err, raft_engine::Error::Corruption(_)));
    // The batch stays usable after a codec failure.
    batch
        .add_entries_with::<SmokeExt, PostcardSmokeCodec>(1, &entries(1, 1, 1))
        .unwrap();
    assert!(engine.write(&mut batch, true).unwrap() > 0);
}
