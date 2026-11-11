//! Fault-injecting raft-engine `FileSystem` (task-j05 step 1; design
//! Sections 17.15, 21.4).
//!
//! The pinned raft-engine journal runs over [`FaultFs`], which passes every
//! file operation through to the real files and keeps, for each log file,
//! two byte images:
//!
//! * the **volatile** image, what the process sees (the real file, mirrored);
//! * the **durable** image, what survives a crash.
//!
//! Writes, truncations and allocations go to the volatile image and are
//! remembered, in order, as unsynced operations; a file's `sync` moves them
//! to its durable image. A [`FaultPlan`] scripts, by operation ordinal, a
//! write failure (EIO, optionally after a sector-aligned prefix of the
//! buffer landed, as a real short write does), a sync failure (which may
//! persist any permitted subset of the unsynced operations), an ENOSPC
//! window and a crash. A crash **halts** the device: every later write,
//! truncation, allocation and sync returns success and changes nothing, as
//! if the process had died at the crash and what follows never ran, so a
//! destructor or background thread can never reach the files or the durable
//! image. (Failing them instead would not do: the pinned engine panics on a
//! failed sync, and a second sync from a destructor while that panic
//! unwinds aborts the process.) A test therefore counts no acknowledgement
//! it observes once [`Shared::is_frozen`] holds. A create, rename or delete
//! after the crash fails, since it would change the directory.
//! [`Shared::materialize`] then rewrites every
//! tracked file on disk as its durable image plus a seeded, possibly torn and
//! reordered, subset of its unsynced tail, and the next process opens the
//! journal over those files.
//!
//! # What this does not control
//!
//! Operations that never reach a `FileSystem` are outside the injection and
//! are reported, not faulted (the audit in this crate's tests names them):
//! raft-engine's directory creation, listing and lock file
//! (`pipe_builder.rs`), its directory sync after rotating to a new log
//! file (`pipe.rs`'s `rotate_imp`, which panics if the sync fails), and the
//! journal adapter's own `std::fs` calls. Reads are never faulted, and are
//! allowed after a crash.
//!
//! # Directory entries
//!
//! A created or renamed file's directory entry is durable only once a
//! directory sync covers it (#210 review). Until then a crash loses a
//! created file, and leaves a renamed one under its old name. Two syncs
//! cover an entry:
//!
//! * the journal adapter's, through [`coord_journal_raft_engine::DirSync`],
//!   which this file system implements and counts as an operation: the
//!   adapter syncs the journal's directory once the engine has created its
//!   first log file, before the creation is acknowledged;
//! * raft-engine's own at rotation, which bypasses the file system. In the
//!   pinned `rotate_imp` it is the only thing between the new file's first
//!   sync and the engine's next operation, so the kit takes a rotated
//!   file's entry as durable when the first counted operation after that
//!   sync begins. A crash at that sync loses the entry.
//!
//! A file the kit first sees already on disk has a durable entry. A delete
//! is modelled as standing once it returns, which is kinder than a real
//! filesystem: a purged file whose delete no directory sync covered can
//! come back after a power loss. raft-engine syncs no directory after a
//! purge, so that case is named, not modelled.
//!
//! # Sync after a failed sync
//!
//! A failed sync keeps pending exactly what it didn't persist, and a later
//! sync persists it. Linux can instead mark those pages clean after a
//! failed `fsync`, so a later `fsync` reports success without persisting
//! them. The pinned engine panics on a failed sync, so nothing reaches that
//! case here; a fail-stopped journal's later syncs belong to task-j05's
//! step 5.
//!
//! This qualifies the journal's use of write and sync boundaries under
//! controlled schedules. It is not a model of arbitrary OS power loss.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::collections::BTreeMap;
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use coord_journal_raft_engine::DirSync;
use raft_engine::env::{DefaultFileSystem, FileSystem, Handle, Permission, WriteExt};
use rand_chacha::ChaCha12Rng;
use rand_core::{Rng, SeedableRng};

/// Which unsynced operations survive a crash or a failed sync.
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

/// Granularity at which a failing write is torn: a real disk persists or
/// drops whole sectors, never part of one. (The seeded crash tail still
/// tears at byte granularity, as a deliberately pessimistic model.)
pub const SECTOR_BYTES: usize = 512;

/// `EIO`, the error a failing device returns for a write.
const EIO: i32 = 5;

/// Scripted faults by operation ordinal (1-based, counting every write,
/// truncation, allocation and sync of a log file, and every create, open,
/// rename and delete).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FaultPlan {
    /// Fail this write with `EIO`.
    pub fail_write_at: Option<u64>,
    /// Bytes of the failing write that reach the file (and the unsynced
    /// log) before the error is returned: a short or torn write. Rounded
    /// down to whole sectors ([`SECTOR_BYTES`]) and capped below the
    /// buffer's length, since a write that landed completely did not fail.
    pub fail_write_prefix: usize,
    /// Fail this sync; the unsynced tail is persisted per `tail`.
    pub fail_sync_at: Option<u64>,
    /// Inclusive ENOSPC window: writes, truncations and allocations fail
    /// with `StorageFull` while the ordinal lies inside it.
    pub enospc: Option<(u64, u64)>,
    /// Crash immediately after this operation completes.
    pub crash_after: Option<u64>,
    /// A lying device, for negative controls: this sync reports success
    /// and persists nothing. A suite that still passes with it set does
    /// not check what it claims to.
    pub lie_on_sync_at: Option<u64>,
    /// A lying directory, for negative controls: this directory sync
    /// reports success and makes no entry durable.
    pub lie_on_dir_sync_at: Option<u64>,
    /// Tail policy for a failed sync (the crash's own tail is chosen at
    /// [`Shared::materialize`]).
    pub tail: Tail,
}

/// One recorded operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    /// A write of `len` bytes at `offset` to `file`.
    Write {
        /// File name.
        file: String,
        /// Offset.
        offset: u64,
        /// Length that landed.
        len: usize,
    },
    /// A length change of `file` (truncate, or an allocation that grew it).
    SetLen {
        /// File name.
        file: String,
        /// New length.
        len: u64,
    },
    /// A sync of `file`.
    Sync {
        /// File name.
        file: String,
    },
    /// A create, open, rename or delete (directory operations).
    Dir {
        /// What it was, with the file names.
        what: String,
    },
    /// A directory sync through [`DirSync`].
    DirSync {
        /// The directory's name.
        dir: String,
    },
    /// An allocation that didn't grow `file`, so changed no image.
    Allocate {
        /// File name.
        file: String,
    },
    /// An operation rejected because the file system was frozen.
    Rejected,
    /// An operation that failed by script.
    Failed,
}

/// One unsynced operation, in issue order.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Unsynced {
    Write { offset: u64, bytes: Vec<u8> },
    SetLen { len: u64 },
}

/// Whether a file's directory entry survives a crash.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
enum Entry {
    /// It was on disk when the kit first saw it, or a directory sync
    /// covered it.
    #[default]
    Durable,
    /// Created since: a crash loses the file.
    Created,
    /// Renamed from this path since: a crash leaves it there.
    RenamedFrom(PathBuf),
}

#[derive(Debug, Default)]
struct Images {
    durable: Vec<u8>,
    volatile: Vec<u8>,
    unsynced: Vec<Unsynced>,
    entry: Entry,
    /// Created or renamed into a directory that already held a log file:
    /// a rotation's, whose directory raft-engine syncs after its first
    /// sync.
    rotated: bool,
}

impl Images {
    fn from_disk(path: &Path) -> io::Result<Self> {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e),
        };
        Ok(Self {
            durable: bytes.clone(),
            volatile: bytes,
            ..Self::default()
        })
    }

    fn write(&mut self, offset: u64, bytes: &[u8]) {
        let end = offset as usize + bytes.len();
        if end > self.volatile.len() {
            self.volatile.resize(end, 0);
        }
        self.volatile[offset as usize..end].copy_from_slice(bytes);
        self.unsynced.push(Unsynced::Write {
            offset,
            bytes: bytes.to_vec(),
        });
    }

    fn set_len(&mut self, len: u64) {
        self.volatile.resize(len as usize, 0);
        self.unsynced.push(Unsynced::SetLen { len });
    }
}

type Files = BTreeMap<PathBuf, Arc<Mutex<Images>>>;

/// The state every handle and writer of one [`FaultFs`] shares, and the
/// test's view of it.
#[derive(Debug, Default)]
pub struct Shared {
    plan: Mutex<FaultPlan>,
    ops: AtomicU64,
    frozen: AtomicBool,
    log: Mutex<Vec<Op>>,
    files: Mutex<Files>,
    /// The directory of a rotated file that has had its first sync: the
    /// pinned engine syncs it next, outside the kit.
    rotation_synced: Mutex<Option<PathBuf>>,
}

impl Shared {
    /// Counted operations so far.
    pub fn ops(&self) -> u64 {
        self.ops.load(Ordering::SeqCst)
    }

    /// Whether a crash has halted the device. An acknowledgement observed
    /// once this holds was never given: the process died before it.
    pub fn is_frozen(&self) -> bool {
        self.frozen.load(Ordering::SeqCst)
    }

    /// Every operation so far, in order.
    pub fn log(&self) -> Vec<Op> {
        self.log.lock().unwrap().clone()
    }

    /// Unsynced operations across every tracked file.
    pub fn unsynced(&self) -> usize {
        self.files
            .lock()
            .unwrap()
            .values()
            .map(|f| f.lock().unwrap().unsynced.len())
            .sum()
    }

    /// The tracked files, by path.
    pub fn files(&self) -> Vec<PathBuf> {
        self.files.lock().unwrap().keys().cloned().collect()
    }

    /// Crash now: the device halts (see the crate's documentation).
    pub fn crash(&self) {
        self.frozen.store(true, Ordering::SeqCst);
    }

    /// Replace the plan (ordinals keep counting).
    pub fn set_plan(&self, plan: FaultPlan) {
        *self.plan.lock().unwrap() = plan;
    }

    /// The tracked files whose directory entry a crash now would lose or
    /// undo.
    pub fn unsynced_entries(&self) -> Vec<PathBuf> {
        self.files
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, f)| f.lock().unwrap().entry != Entry::Durable)
            .map(|(p, _)| p.clone())
            .collect()
    }

    /// Freeze, then rewrite every tracked file on disk as its crash image
    /// under `tail`. Each file draws its own subset from `tail`'s seed and
    /// its position among the tracked files, so one seed covers every file.
    /// A file whose entry no directory sync covered is removed, or put
    /// back under the name it was renamed from. Call it once the journal
    /// that used this file system is dropped; the next journal opens the
    /// files with a fresh [`FaultFs`].
    pub fn materialize(&self, tail: Tail) -> io::Result<()> {
        self.crash();
        let files = self.files.lock().unwrap();
        for (i, (path, images)) in files.iter().enumerate() {
            let images = images.lock().unwrap();
            let tail = match tail {
                Tail::Seeded(seed) => Tail::Seeded(seed ^ ((i as u64 + 1) << 32)),
                other => other,
            };
            let bytes = apply_tail(&images, tail);
            match &images.entry {
                Entry::Durable => std::fs::write(path, bytes)?,
                Entry::Created => remove_if_present(path)?,
                Entry::RenamedFrom(from) => {
                    remove_if_present(path)?;
                    std::fs::write(from, bytes)?;
                }
            }
        }
        Ok(())
    }

    /// The next ordinal, or `None` once halted (the operation then does
    /// nothing and reports success). An operation that begins after a
    /// rotated file's first sync finds raft-engine's directory sync done.
    fn begin(&self) -> Option<u64> {
        if self.is_frozen() {
            self.log.lock().unwrap().push(Op::Rejected);
            return None;
        }
        if let Some(dir) = self.rotation_synced.lock().unwrap().take() {
            self.entries_durable(&dir);
        }
        Some(self.ops.fetch_add(1, Ordering::SeqCst) + 1)
    }

    /// A directory sync of `dir` covered every entry in it.
    fn entries_durable(&self, dir: &Path) {
        for (path, images) in self.files.lock().unwrap().iter() {
            if path.parent() == Some(dir) {
                images.lock().unwrap().entry = Entry::Durable;
            }
        }
    }

    /// Whether `dir` holds a tracked log file other than `except`.
    fn holds_another(&self, dir: &Path, except: &Path) -> bool {
        self.files
            .lock()
            .unwrap()
            .keys()
            .any(|p| p.parent() == Some(dir) && p != except)
    }

    /// The next ordinal for a directory operation, which fails once halted.
    fn begin_dir(&self) -> io::Result<u64> {
        self.begin()
            .ok_or_else(|| io::Error::other("file system halted by a simulated crash"))
    }

    fn record(&self, op: Op) {
        self.log.lock().unwrap().push(op);
    }

    fn end(&self, n: u64) {
        if self.plan.lock().unwrap().crash_after == Some(n) {
            self.crash();
        }
    }

    fn full(&self, n: u64) -> bool {
        self.plan
            .lock()
            .unwrap()
            .enospc
            .is_some_and(|(from, to)| (from..=to).contains(&n))
    }

    fn images(&self, path: &Path) -> io::Result<Arc<Mutex<Images>>> {
        let mut files = self.files.lock().unwrap();
        if let Some(images) = files.get(path) {
            return Ok(images.clone());
        }
        let images = Arc::new(Mutex::new(Images::from_disk(path)?));
        files.insert(path.to_path_buf(), images.clone());
        Ok(images)
    }
}

fn remove_if_present(path: &Path) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

fn apply_tail(images: &Images, tail: Tail) -> Vec<u8> {
    split_tail(images, tail).0
}

/// The image `tail` leaves, and the unsynced operations it did not
/// persist, in issue order: the ones it dropped, and the unwritten rest of
/// a write it tore. A failed sync keeps exactly those pending, so a later
/// crash image never replays an operation the failed sync already
/// persisted over what came after it.
fn split_tail(images: &Images, tail: Tail) -> (Vec<u8>, Vec<Unsynced>) {
    let seed = match tail {
        Tail::None => return (images.durable.clone(), images.unsynced.clone()),
        // Everything unsynced survives in issue order: exactly the volatile
        // image, including a shrink that followed an earlier write.
        Tail::All => return (images.volatile.clone(), Vec::new()),
        Tail::Seeded(seed) => seed,
    };
    let mut rng = ChaCha12Rng::seed_from_u64(seed);
    // Keep each operation with probability 1/2, tear surviving writes to a
    // prefix sometimes, and shuffle the survivors. A length change is one
    // of the operations: it survives, or not, and lands wherever the
    // shuffle puts it.
    let (mut kept, mut rest) = (Vec::new(), Vec::new());
    for (i, op) in images.unsynced.iter().enumerate() {
        if rng.next_u32() % 2 == 0 {
            kept.push((i, op.clone()));
        } else {
            rest.push((i, op.clone()));
        }
    }
    for (i, op) in &mut kept {
        if let Unsynced::Write { offset, bytes } = op
            && rng.next_u32() % 4 == 0
        {
            let keep = (rng.next_u32() as usize) % (bytes.len() + 1);
            let unwritten = bytes.split_off(keep);
            if !unwritten.is_empty() {
                rest.push((
                    *i,
                    Unsynced::Write {
                        offset: *offset + keep as u64,
                        bytes: unwritten,
                    },
                ));
            }
        }
    }
    for i in (1..kept.len()).rev() {
        let j = (rng.next_u32() as usize) % (i + 1);
        kept.swap(i, j);
    }
    let mut out = images.durable.clone();
    for (_, op) in kept {
        match op {
            Unsynced::Write { offset, bytes } => {
                let end = offset as usize + bytes.len();
                if end > out.len() {
                    out.resize(end, 0);
                }
                out[offset as usize..end].copy_from_slice(&bytes);
            }
            Unsynced::SetLen { len } => out.resize(len as usize, 0),
        }
    }
    rest.sort_by_key(|(i, _)| *i);
    (out, rest.into_iter().map(|(_, op)| op).collect())
}

fn name(path: &Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// The file system: raft-engine's own, with every log file mirrored and
/// faulted.
pub struct FaultFs {
    inner: DefaultFileSystem,
    shared: Arc<Shared>,
}

impl FaultFs {
    /// A file system under `plan`, and the test's view of it.
    pub fn new(plan: FaultPlan) -> (Arc<FaultFs>, Arc<Shared>) {
        let shared = Arc::new(Shared {
            plan: Mutex::new(plan),
            ..Shared::default()
        });
        (
            Arc::new(FaultFs {
                inner: DefaultFileSystem,
                shared: shared.clone(),
            }),
            shared,
        )
    }

    fn handle(
        &self,
        path: &Path,
        inner: <DefaultFileSystem as FileSystem>::Handle,
    ) -> io::Result<FaultHandle> {
        Ok(FaultHandle {
            inner: Arc::new(inner),
            images: self.shared.images(path)?,
            file: name(path),
            path: path.to_path_buf(),
            shared: self.shared.clone(),
        })
    }
}

/// A log file's handle.
pub struct FaultHandle {
    inner: Arc<<DefaultFileSystem as FileSystem>::Handle>,
    images: Arc<Mutex<Images>>,
    file: String,
    path: PathBuf,
    shared: Arc<Shared>,
}

impl FaultHandle {
    /// A rotated file's first successful sync: raft-engine syncs its
    /// directory next (see the crate's documentation).
    fn rotation_synced(&self) {
        let images = self.images.lock().unwrap();
        if images.rotated && images.entry != Entry::Durable {
            *self.shared.rotation_synced.lock().unwrap() =
                self.path.parent().map(Path::to_path_buf);
        }
    }

    fn set_len(&self, offset: usize) -> io::Result<()> {
        let Some(n) = self.shared.begin() else {
            return Ok(());
        };
        if self.shared.full(n) {
            self.shared.record(Op::Failed);
            self.shared.end(n);
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "simulated ENOSPC",
            ));
        }
        self.inner.truncate(offset)?;
        self.images.lock().unwrap().set_len(offset as u64);
        self.shared.record(Op::SetLen {
            file: self.file.clone(),
            len: offset as u64,
        });
        self.shared.end(n);
        Ok(())
    }
}

impl Handle for FaultHandle {
    fn truncate(&self, offset: usize) -> io::Result<()> {
        self.set_len(offset)
    }

    fn file_size(&self) -> io::Result<usize> {
        self.inner.file_size()
    }

    fn sync(&self) -> io::Result<()> {
        let Some(n) = self.shared.begin() else {
            return Ok(());
        };
        let plan = *self.shared.plan.lock().unwrap();
        if plan.fail_sync_at == Some(n) {
            // A failed sync may have persisted any permitted subset; what
            // it did not persist, and only that, stays pending against the
            // new image.
            let mut images = self.images.lock().unwrap();
            let (persisted, pending) = split_tail(&images, plan.tail);
            images.durable = persisted;
            images.unsynced = pending;
            drop(images);
            self.shared.record(Op::Failed);
            self.shared.end(n);
            return Err(io::Error::other("simulated sync failure"));
        }
        if plan.lie_on_sync_at == Some(n) {
            self.rotation_synced();
            self.shared.record(Op::Sync {
                file: self.file.clone(),
            });
            self.shared.end(n);
            return Ok(());
        }
        self.inner.sync()?;
        let mut images = self.images.lock().unwrap();
        images.durable = images.volatile.clone();
        images.unsynced.clear();
        drop(images);
        self.rotation_synced();
        self.shared.record(Op::Sync {
            file: self.file.clone(),
        });
        self.shared.end(n);
        Ok(())
    }
}

/// A log file's writer.
pub struct FaultWriter {
    inner: <DefaultFileSystem as FileSystem>::Writer,
    offset: u64,
    handle: Arc<FaultHandle>,
}

impl Write for FaultWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let shared = self.handle.shared.clone();
        let Some(n) = shared.begin() else {
            return Ok(buf.len());
        };
        if shared.full(n) {
            shared.record(Op::Failed);
            shared.end(n);
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "simulated ENOSPC",
            ));
        }
        let plan = *shared.plan.lock().unwrap();
        if plan.fail_write_at == Some(n) {
            let prefix = (plan.fail_write_prefix / SECTOR_BYTES * SECTOR_BYTES)
                .min(buf.len().saturating_sub(1) / SECTOR_BYTES * SECTOR_BYTES);
            if prefix > 0 {
                let landed = self.inner.write(&buf[..prefix])?;
                self.handle
                    .images
                    .lock()
                    .unwrap()
                    .write(self.offset, &buf[..landed]);
                self.offset += landed as u64;
            }
            shared.record(Op::Failed);
            shared.end(n);
            return Err(io::Error::from_raw_os_error(EIO));
        }
        let landed = self.inner.write(buf)?;
        self.handle
            .images
            .lock()
            .unwrap()
            .write(self.offset, &buf[..landed]);
        shared.record(Op::Write {
            file: self.handle.file.clone(),
            offset: self.offset,
            len: landed,
        });
        self.offset += landed as u64;
        shared.end(n);
        Ok(landed)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl Seek for FaultWriter {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        if self.handle.shared.is_frozen() {
            // Nothing lands any more; keep the position the engine expects.
            self.offset = match pos {
                SeekFrom::Start(o) => o,
                SeekFrom::Current(d) => self.offset.saturating_add_signed(d),
                SeekFrom::End(_) => return self.inner.seek(pos),
            };
            return Ok(self.offset);
        }
        self.offset = self.inner.seek(pos)?;
        Ok(self.offset)
    }
}

impl WriteExt for FaultWriter {
    fn truncate(&mut self, offset: usize) -> io::Result<()> {
        if self.handle.shared.is_frozen() {
            self.offset = offset as u64;
            return Ok(());
        }
        self.handle.set_len(offset)?;
        // The inner writer's own truncate also moves its offset.
        self.inner.seek(SeekFrom::Start(offset as u64))?;
        self.offset = offset as u64;
        Ok(())
    }

    fn allocate(&mut self, offset: usize, size: usize) -> io::Result<()> {
        let shared = self.handle.shared.clone();
        let Some(n) = shared.begin() else {
            return Ok(());
        };
        if shared.full(n) {
            shared.record(Op::Failed);
            shared.end(n);
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "simulated ENOSPC",
            ));
        }
        self.inner.allocate(offset, size)?;
        // fallocate without KEEP_SIZE grows the file with zeros.
        let end = (offset + size) as u64;
        let mut images = self.handle.images.lock().unwrap();
        if end > images.volatile.len() as u64 {
            images.set_len(end);
            drop(images);
            shared.record(Op::SetLen {
                file: self.handle.file.clone(),
                len: end,
            });
        } else {
            // Recorded all the same, so the log's index stays the ordinal.
            drop(images);
            shared.record(Op::Allocate {
                file: self.handle.file.clone(),
            });
        }
        shared.end(n);
        Ok(())
    }
}

impl FileSystem for FaultFs {
    type Handle = FaultHandle;
    type Reader = <DefaultFileSystem as FileSystem>::Reader;
    type Writer = FaultWriter;

    fn create<P: AsRef<Path>>(&self, path: P) -> io::Result<Self::Handle> {
        let path = path.as_ref();
        let n = self.shared.begin_dir()?;
        let inner = self.inner.create(path)?;
        // A created file starts empty, whatever the path held before, and
        // its entry isn't durable until a directory sync covers it.
        let rotated = path
            .parent()
            .is_some_and(|dir| self.shared.holds_another(dir, path));
        self.shared.files.lock().unwrap().insert(
            path.to_path_buf(),
            Arc::new(Mutex::new(Images {
                entry: Entry::Created,
                rotated,
                ..Images::default()
            })),
        );
        let handle = self.handle(path, inner)?;
        self.shared.record(Op::Dir {
            what: format!("create {}", name(path)),
        });
        self.shared.end(n);
        Ok(handle)
    }

    fn open<P: AsRef<Path>>(&self, path: P, perm: Permission) -> io::Result<Self::Handle> {
        let path = path.as_ref();
        let n = self.shared.begin_dir()?;
        let inner = self.inner.open(path, perm)?;
        let handle = self.handle(path, inner)?;
        self.shared.record(Op::Dir {
            what: format!("open {}", name(path)),
        });
        self.shared.end(n);
        Ok(handle)
    }

    fn delete<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let path = path.as_ref();
        let n = self.shared.begin_dir()?;
        self.inner.delete(path)?;
        self.shared.files.lock().unwrap().remove(path);
        self.shared.record(Op::Dir {
            what: format!("delete {}", name(path)),
        });
        self.shared.end(n);
        Ok(())
    }

    fn rename<P: AsRef<Path>>(&self, src: P, dst: P) -> io::Result<()> {
        let (src, dst) = (src.as_ref(), dst.as_ref());
        let n = self.shared.begin_dir()?;
        self.inner.rename(src, dst)?;
        let rotated = dst
            .parent()
            .is_some_and(|dir| self.shared.holds_another(dir, src));
        let mut files = self.shared.files.lock().unwrap();
        if let Some(images) = files.remove(src) {
            {
                let mut f = images.lock().unwrap();
                f.entry = match std::mem::take(&mut f.entry) {
                    Entry::Durable => Entry::RenamedFrom(src.to_path_buf()),
                    other => other,
                };
                f.rotated = rotated;
            }
            files.insert(dst.to_path_buf(), images);
        }
        drop(files);
        self.shared.record(Op::Dir {
            what: format!("rename {} {}", name(src), name(dst)),
        });
        self.shared.end(n);
        Ok(())
    }

    fn new_reader(&self, handle: Arc<Self::Handle>) -> io::Result<Self::Reader> {
        self.inner.new_reader(handle.inner.clone())
    }

    fn new_writer(&self, handle: Arc<Self::Handle>) -> io::Result<Self::Writer> {
        Ok(FaultWriter {
            inner: self.inner.new_writer(handle.inner.clone())?,
            offset: 0,
            handle,
        })
    }
}

impl DirSync for FaultFs {
    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        let n = self.shared.begin_dir()?;
        let lie = self.shared.plan.lock().unwrap().lie_on_dir_sync_at == Some(n);
        if !lie {
            std::fs::File::open(dir)?.sync_all()?;
            self.shared.entries_durable(dir);
        }
        self.shared.record(Op::DirSync { dir: name(dir) });
        self.shared.end(n);
        Ok(())
    }
}
