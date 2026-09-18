//! Fault-injecting `redb::StorageBackend` (task-09; design Sections 17.3,
//! 17.14, 21.2 fidelity level B).
//!
//! The real redb engine runs over [`FaultBackend`], which keeps two byte
//! images:
//!
//! * the **volatile** image, what the process sees (OS page cache);
//! * the **durable** image, what survives a crash.
//!
//! Writes and length changes go to the volatile image and are remembered as
//! unsynced; `sync_data` moves them to the durable image. A [`FaultPlan`]
//! scripts, by operation ordinal, a write failure, a sync failure (which
//! may persist any permitted subset of the unsynced writes), an ENOSPC
//! window and a crash. A crash **freezes** the backend: every later
//! operation fails, so a destructor or background flush can never reach the
//! durable image, and the crash image is derived from the durable image
//! plus a seeded, possibly torn and reordered, subset of the unsynced tail.
//! The next process is a fresh backend over that crash image.
//!
//! This qualifies the engine's use of write/sync boundaries under
//! controlled schedules. It is not a model of arbitrary OS power loss,
//! Fjall, raft-engine or the composed journal boundary (task-j05).
#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rand_chacha::ChaCha12Rng;
use rand_core::{Rng, SeedableRng};

/// Which unsynced writes survive a crash or a failed sync.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Tail {
    /// Nothing unsynced survives.
    #[default]
    None,
    /// Everything unsynced survives (as if the OS had flushed it).
    All,
    /// A seeded subset survives, each write possibly torn to a prefix and
    /// applied in an arbitrary order.
    Seeded(u64),
}

/// Scripted faults by operation ordinal (1-based, counting every write,
/// set_len and sync call).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FaultPlan {
    /// Fail this write with a generic I/O error.
    pub fail_write_at: Option<u64>,
    /// Fail this sync; the unsynced tail is persisted per `tail`.
    pub fail_sync_at: Option<u64>,
    /// Inclusive ENOSPC window: writes and length changes fail with
    /// `StorageFull` while the ordinal lies inside it.
    pub enospc: Option<(u64, u64)>,
    /// Crash immediately after this operation completes.
    pub crash_after: Option<u64>,
    /// Tail policy for crashes and failed syncs.
    pub tail: Tail,
}

/// One recorded operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    /// `write(offset, len)`.
    Write {
        /// Offset.
        offset: u64,
        /// Length.
        len: usize,
    },
    /// `set_len(len)`.
    SetLen {
        /// New length.
        len: u64,
    },
    /// `sync_data()`.
    Sync,
    /// A read (not counted as an ordinal).
    Read {
        /// Offset.
        offset: u64,
        /// Length.
        len: usize,
    },
    /// An operation rejected because the backend was frozen.
    Rejected,
    /// An operation that failed by script.
    Failed,
}

#[derive(Debug, Default)]
struct Images {
    durable: Vec<u8>,
    volatile: Vec<u8>,
    /// Unsynced writes in order: `(offset, bytes)`; length changes are
    /// recorded as `(len, empty)` markers handled by `apply_tail`.
    unsynced: Vec<(u64, Vec<u8>)>,
    unsynced_len: Option<u64>,
}

/// Shared state so the test can inspect and derive images after a crash.
#[derive(Debug, Default)]
pub struct Shared {
    images: Mutex<Images>,
    ops: AtomicU64,
    frozen: AtomicBool,
    log: Mutex<Vec<Op>>,
    plan: Mutex<FaultPlan>,
}

impl Shared {
    /// Number of counted operations so far.
    pub fn ops(&self) -> u64 {
        self.ops.load(Ordering::SeqCst)
    }

    /// Whether the backend is frozen (crashed).
    pub fn is_frozen(&self) -> bool {
        self.frozen.load(Ordering::SeqCst)
    }

    /// Operation log.
    pub fn log(&self) -> Vec<Op> {
        self.log.lock().unwrap().clone()
    }

    /// Current durable image (what a crash right now would preserve
    /// before any tail policy).
    pub fn durable(&self) -> Vec<u8> {
        self.images.lock().unwrap().durable.clone()
    }

    /// Current volatile image.
    pub fn volatile(&self) -> Vec<u8> {
        self.images.lock().unwrap().volatile.clone()
    }

    /// Number of unsynced writes.
    pub fn unsynced(&self) -> usize {
        self.images.lock().unwrap().unsynced.len()
    }

    /// Freeze now (a crash not scheduled by the plan).
    pub fn crash(&self) {
        self.frozen.store(true, Ordering::SeqCst);
    }

    /// Replace the plan (for example to open an ENOSPC window later).
    pub fn set_plan(&self, plan: FaultPlan) {
        *self.plan.lock().unwrap() = plan;
    }

    /// The image the next process sees after a crash under `tail`.
    pub fn crash_image(&self, tail: Tail) -> Vec<u8> {
        let images = self.images.lock().unwrap();
        apply_tail(&images, tail)
    }
}

fn apply_tail(images: &Images, tail: Tail) -> Vec<u8> {
    let mut out = images.durable.clone();
    let mut writes: Vec<(u64, Vec<u8>)> = images.unsynced.clone();
    match tail {
        Tail::None => return out,
        Tail::All => {}
        Tail::Seeded(seed) => {
            let mut rng = ChaCha12Rng::seed_from_u64(seed);
            // Keep each write with probability 1/2, possibly torn, and
            // shuffle the survivors.
            writes.retain(|_| rng.next_u32() % 2 == 0);
            for (_, bytes) in &mut writes {
                if rng.next_u32() % 4 == 0 {
                    let keep = (rng.next_u32() as usize) % (bytes.len() + 1);
                    bytes.truncate(keep);
                }
            }
            for i in (1..writes.len()).rev() {
                let j = (rng.next_u32() as usize) % (i + 1);
                writes.swap(i, j);
            }
        }
    }
    if let Some(len) = images.unsynced_len
        && !matches!(tail, Tail::None)
    {
        out.resize(len as usize, 0);
    }
    for (offset, bytes) in writes {
        let end = offset as usize + bytes.len();
        if end > out.len() {
            out.resize(end, 0);
        }
        out[offset as usize..end].copy_from_slice(&bytes);
    }
    out
}

/// The backend.
#[derive(Debug)]
pub struct FaultBackend {
    shared: Arc<Shared>,
}

impl FaultBackend {
    /// New backend over an initial durable image (empty for a fresh
    /// database) with a plan.
    pub fn new(image: Vec<u8>, plan: FaultPlan) -> (FaultBackend, Arc<Shared>) {
        let shared = Arc::new(Shared::default());
        {
            let mut images = shared.images.lock().unwrap();
            images.durable = image.clone();
            images.volatile = image;
        }
        *shared.plan.lock().unwrap() = plan;
        (
            FaultBackend {
                shared: shared.clone(),
            },
            shared,
        )
    }

    fn frozen_err() -> io::Error {
        io::Error::new(
            io::ErrorKind::BrokenPipe,
            "backend frozen after simulated crash",
        )
    }

    /// Count an operation; returns its ordinal or the frozen error.
    fn begin(&self, op: Op) -> io::Result<u64> {
        if self.shared.frozen.load(Ordering::SeqCst) {
            self.shared.log.lock().unwrap().push(Op::Rejected);
            return Err(Self::frozen_err());
        }
        let n = self.shared.ops.fetch_add(1, Ordering::SeqCst) + 1;
        self.shared.log.lock().unwrap().push(op);
        Ok(n)
    }

    fn maybe_crash(&self, n: u64) {
        let plan = *self.shared.plan.lock().unwrap();
        if plan.crash_after == Some(n) {
            self.shared.frozen.store(true, Ordering::SeqCst);
        }
    }

    fn in_enospc(&self, n: u64) -> bool {
        let plan = *self.shared.plan.lock().unwrap();
        plan.enospc.is_some_and(|(lo, hi)| n >= lo && n <= hi)
    }
}

impl redb::StorageBackend for FaultBackend {
    fn len(&self) -> io::Result<u64> {
        if self.shared.frozen.load(Ordering::SeqCst) {
            return Err(Self::frozen_err());
        }
        Ok(self.shared.images.lock().unwrap().volatile.len() as u64)
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        if self.shared.frozen.load(Ordering::SeqCst) {
            self.shared.log.lock().unwrap().push(Op::Rejected);
            return Err(Self::frozen_err());
        }
        self.shared.log.lock().unwrap().push(Op::Read {
            offset,
            len: out.len(),
        });
        let images = self.shared.images.lock().unwrap();
        let end = offset as usize + out.len();
        if end > images.volatile.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "read past end",
            ));
        }
        out.copy_from_slice(&images.volatile[offset as usize..end]);
        Ok(())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        let n = self.begin(Op::SetLen { len })?;
        if self.in_enospc(n) {
            self.shared.log.lock().unwrap().push(Op::Failed);
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "simulated ENOSPC",
            ));
        }
        {
            let mut images = self.shared.images.lock().unwrap();
            images.volatile.resize(len as usize, 0);
            images.unsynced_len = Some(len);
        }
        self.maybe_crash(n);
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        let n = self.begin(Op::Sync)?;
        let plan = *self.shared.plan.lock().unwrap();
        let mut images = self.shared.images.lock().unwrap();
        if plan.fail_sync_at == Some(n) {
            // A failed sync may have persisted any permitted subset.
            let persisted = apply_tail(&images, plan.tail);
            images.durable = persisted;
            // What was not persisted stays unsynced; we do not know which,
            // so treat all as still pending relative to the new durable image.
            drop(images);
            self.shared.log.lock().unwrap().push(Op::Failed);
            self.maybe_crash(n);
            return Err(io::Error::other("simulated sync failure"));
        }
        images.durable = images.volatile.clone();
        images.unsynced.clear();
        images.unsynced_len = None;
        drop(images);
        self.maybe_crash(n);
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        let n = self.begin(Op::Write {
            offset,
            len: data.len(),
        })?;
        let plan = *self.shared.plan.lock().unwrap();
        if self.in_enospc(n) {
            self.shared.log.lock().unwrap().push(Op::Failed);
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "simulated ENOSPC",
            ));
        }
        if plan.fail_write_at == Some(n) {
            self.shared.log.lock().unwrap().push(Op::Failed);
            return Err(io::Error::other("simulated write failure"));
        }
        {
            let mut images = self.shared.images.lock().unwrap();
            let end = offset as usize + data.len();
            if end > images.volatile.len() {
                images.volatile.resize(end, 0);
            }
            images.volatile[offset as usize..end].copy_from_slice(data);
            images.unsynced.push((offset, data.to_vec()));
        }
        self.maybe_crash(n);
        Ok(())
    }

    fn close(&self) -> io::Result<()> {
        Ok(())
    }
}
