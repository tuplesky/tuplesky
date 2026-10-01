//! How long a projection takes to reopen after a crash, committed in two
//! phases and in one (task-d48, Section 17.3.4). A measurement, not a
//! check: ignored by default, run by hand and reported in the notes.
//!
//! ```text
//! RECOVERY_DIR=/path/on/the/disk RECOVERY_MIB=64,256 \
//!   cargo test --release -p coord-storage-redb --test recovery_time -- --ignored --nocapture
//! ```
//!
//! The crash image is the database file copied while it is still open,
//! after its last commit has returned: redb marks a file it has open as
//! needing recovery and clears the mark only on a clean close, so opening
//! the copy runs the repair a crash would. Quick-repair stays off in both
//! modes.

use std::path::Path;
use std::time::Instant;

use coord_storage_redb::RedbEngine;
use coord_store_api::engine::{LocalEngine, WriteTxn};
use coord_store_api::registry::Collection;

const CACHE: usize = 64 << 20;
const ROW: usize = 4096;
const ROWS_PER_COMMIT: u64 = 256;

fn backend(path: &Path, create: bool) -> redb::backends::FileBackend {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(create)
        .truncate(create)
        .open(path)
        .unwrap();
    redb::backends::FileBackend::new(file).unwrap()
}

/// Fill a fresh database with `mib` MiB of rows, a MiB a commit; return
/// how long the commits took and how long reopening its crash image did.
fn measure(dir: &Path, mib: u64, one_phase: bool) -> (f64, f64, u64) {
    let path = dir.join(format!("projection-{mib}-{one_phase}.redb"));
    let crashed = dir.join(format!("crashed-{mib}-{one_phase}.redb"));
    let mut engine = RedbEngine::create_on_backend(backend(&path, true), CACHE).unwrap();
    if one_phase {
        engine.commit_under_journal();
    }
    let value = vec![0x5a; ROW];
    let started = Instant::now();
    for commit in 0..mib * (1 << 20) / (ROW as u64 * ROWS_PER_COMMIT) {
        let mut txn = engine.begin_write().unwrap();
        for row in 0..ROWS_PER_COMMIT {
            let key = (commit * ROWS_PER_COMMIT + row).to_be_bytes();
            txn.put(Collection::KvCurrentV1.id(), &key, &value).unwrap();
        }
        txn.commit_durable().unwrap();
    }
    let writing = started.elapsed().as_secs_f64();
    std::fs::copy(&path, &crashed).unwrap();
    drop(engine);
    std::fs::remove_file(&path).unwrap();
    let bytes = std::fs::metadata(&crashed).unwrap().len();

    let started = Instant::now();
    let engine = RedbEngine::from_backend(backend(&crashed, false), CACHE).unwrap();
    let reopening = started.elapsed().as_secs_f64();
    drop(engine);
    std::fs::remove_file(&crashed).unwrap();
    (writing, reopening, bytes)
}

#[test]
#[ignore = "a measurement; run by hand"]
fn recovery_time_after_a_crash_in_one_and_two_phases() {
    let dir = std::env::var("RECOVERY_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir());
    let sizes: Vec<u64> = std::env::var("RECOVERY_MIB")
        .unwrap_or_else(|_| "64".into())
        .split(',')
        .map(|s| s.trim().parse().unwrap())
        .collect();
    println!("| MiB written | file MiB | commit | commits took s | reopen after crash s |");
    println!("| --- | --- | --- | --- | --- |");
    for mib in sizes {
        for one_phase in [false, true] {
            let (writing, reopening, bytes) = measure(&dir, mib, one_phase);
            println!(
                "| {mib} | {} | {} | {writing:.2} | {reopening:.3} |",
                bytes >> 20,
                if one_phase { "one phase" } else { "two phases" },
            );
        }
    }
}
