//! The tail guard: the only truncations the pinned engine may make at
//! open are of a torn, unacknowledged write (task-j05 step 2, #211).
//!
//! Under `TolerateTailCorruption` the pinned engine treats the first broken
//! batch it reads in the last log file as a torn tail, truncates the file
//! there and syncs (`pipe_builder.rs`), and a broken file header the same
//! way, at offset 0. It also truncates at a last batch whose entries block
//! fails its checksum, in any file. One damaged byte anywhere in the last
//! file then silently drops every acknowledged group after it, and the
//! file's own evidence with them.
//!
//! [`TailGuard`] wraps the engine's [`FileSystem`]. Every truncation the
//! engine asks of a file handle (`Handle::truncate`, which the pinned
//! engine calls only from recovery) is checked first, against the file's
//! bytes, and refused unless what it would remove is the remains of one
//! write that never finished:
//!
//! * **Only the last file of its queue.** Rotation closes, and so syncs, a
//!   file before it creates the next, so nothing in a file with a later
//!   one after it is torn.
//! * **A file header only if it was never written.** Any sync of a file
//!   persists its header, so a file header that does not decode is
//!   truncated only if it is all zeros, and then nothing in the file was
//!   ever synced.
//! * **Only one batch, the last.** Recycling is off and the adapter writes
//!   every group with `sync`, so after the last synced batch there are only
//!   the engine's preallocated zeros, or the remains of the one batch in
//!   flight. Refused: a non-zero byte past the end the broken batch's
//!   header claims, or a whole batch anywhere after it (a well-formed
//!   header whose item block's checksum matches, wherever the broken
//!   batch's header says it ends).
//! * **Only a batch that was not written whole.** A batch's item block is
//!   its last part and ends in a checksum. If the block's checksum matches
//!   at some end, the batch was written whole there, and a header that
//!   claims another length is damage. (A length flipped upward would
//!   otherwise claim the preallocated zeros after the batch as its own
//!   unwritten sectors.)
//! * **Only a write that did not finish.** A batch whose checksum, its last
//!   four bytes, is zero, or that claims to run past the end of the file,
//!   was cut short: a buffered write can stop at any byte. Otherwise
//!   (etcd's torn-entry test) the broken batch is split at sector
//!   boundaries ([`SECTOR_BYTES`]) and is torn only if one of those parts
//!   is all zeros: a write that did not finish leaves at least one sector
//!   it never reached, still holding the preallocated zeros, and a bit
//!   flip, or any damage to a batch written whole, leaves none. A batch
//!   header that does not decode is torn only if one of its own parts is
//!   all zeros; the batch's end is then unknown, and the write is taken to
//!   run to the end of the file.
//!
//! A refusal happens before the truncation reaches the file, so the file
//! keeps its evidence, and the open fails with
//! [`TailRefused`](crate::guard::TailRefused), which names the file, the
//! position and the reason. A voter that refuses to open is recovered from
//! its peers; one that opened short would have forgotten votes it
//! acknowledged.
//!
//! **What it assumes.** A sector is written whole or not at all. A device
//! that tears inside a sector can leave a torn write with no all-zero
//! part, which is refused as damage: safe, but a crash that needs its
//! peers. So is a torn write whose only unwritten part is a sliver too
//! short to tell from a batch's own zeros.
//!
//! **What it still can't tell apart:** damage to the last batch, with
//! nothing after it, when the batch has a sector part of zeros, whether its
//! own bytes were zeros there or the device lost a sector it had written.
//! That batch is truncated.
//!
//! **What it refuses that a crash can leave:** a purge writes several
//! batches to the `.rewrite` queue before it syncs, so a crash inside one
//! can leave a torn batch with another after it. That is refused too;
//! nothing in it is lost, since the append files it rewrites are kept until
//! it syncs, but the voter is recovered from its peers. A crash during
//! recovery's own truncation is not tested yet.
//!
//! Reads go through the wrapped file system, so the check sees what the
//! engine sees.

use std::fmt;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use raft_engine::env::{FileSystem, Handle, Permission};

/// Sector size the torn-write test splits at: the smallest a device
/// writes atomically.
pub const SECTOR_BYTES: usize = 512;

/// The pinned engine's batch header: `{ u56 len | u8 compression | u64
/// item offset }`, big-endian (`log_batch.rs`).
const BATCH_HEADER_LEN: usize = 16;

/// The pinned engine's file header magic (`format.rs`).
const FILE_MAGIC_LEN: usize = 53;

/// The pinned engine's version-2 file header: its magic, a `u64` version
/// and a `u64` alignment (`format.rs`).
const FILE_HEADER_LEN: usize = FILE_MAGIC_LEN + 8 + 8;

/// A batch's footer checksum (`log_batch.rs`).
const FOOTER_CHECKSUM_LEN: usize = 4;

/// Leading zero bytes a batch header's 7-byte length field can have: a
/// batch is at least a header long, so its last byte or the one before is
/// not zero.
const LEN_FIELD_ZERO_PREFIX: usize = 6;

/// File name suffixes of the pinned engine's two queues (`format.rs`).
const QUEUE_SUFFIXES: [&str; 2] = [".raftlog", ".rewrite"];

/// Width of the sequence number in a log file's name.
const SEQ_WIDTH: usize = 16;

/// A truncation the guard refused: damage, not a torn tail.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TailRefused {
    /// The log file.
    pub file: PathBuf,
    /// Where the engine asked to truncate it.
    pub offset: usize,
    /// The file's length.
    pub len: usize,
    /// Why it is not a torn tail.
    pub reason: &'static str,
}

impl fmt::Display for TailRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "journal file {} is corrupt at offset {} of {}: {}; recovery would have \
             truncated it there, discarding acknowledged records, and was refused",
            self.file.display(),
            self.offset,
            self.len,
            self.reason
        )
    }
}

impl std::error::Error for TailRefused {}

/// The [`TailRefused`] an engine I/O error carries, if it is one.
pub fn tail_refusal(e: &io::Error) -> Option<&TailRefused> {
    e.get_ref()
        .and_then(|inner| inner.downcast_ref::<TailRefused>())
}

/// A [`FileSystem`] whose handles refuse every truncation that is not of
/// a torn tail.
pub struct TailGuard<F: FileSystem> {
    inner: Arc<F>,
}

impl<F: FileSystem> TailGuard<F> {
    /// Guard `inner`.
    pub fn new(inner: Arc<F>) -> Self {
        TailGuard { inner }
    }

    fn handle(&self, path: &Path, inner: F::Handle) -> GuardedHandle<F> {
        GuardedHandle {
            inner: Arc::new(inner),
            fs: self.inner.clone(),
            path: path.to_path_buf(),
        }
    }
}

/// A file handle under [`TailGuard`].
pub struct GuardedHandle<F: FileSystem> {
    inner: Arc<F::Handle>,
    fs: Arc<F>,
    path: PathBuf,
}

impl<F: FileSystem> FileSystem for TailGuard<F> {
    type Handle = GuardedHandle<F>;
    type Reader = F::Reader;
    type Writer = F::Writer;

    fn create<P: AsRef<Path>>(&self, path: P) -> io::Result<Self::Handle> {
        let inner = self.inner.create(path.as_ref())?;
        Ok(self.handle(path.as_ref(), inner))
    }

    fn open<P: AsRef<Path>>(&self, path: P, perm: Permission) -> io::Result<Self::Handle> {
        let inner = self.inner.open(path.as_ref(), perm)?;
        Ok(self.handle(path.as_ref(), inner))
    }

    fn delete<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        self.inner.delete(path)
    }

    fn rename<P: AsRef<Path>>(&self, src_path: P, dst_path: P) -> io::Result<()> {
        self.inner.rename(src_path, dst_path)
    }

    fn reuse<P: AsRef<Path>>(&self, src_path: P, dst_path: P) -> io::Result<()> {
        self.inner.reuse(src_path, dst_path)
    }

    fn reuse_and_open<P: AsRef<Path>>(&self, src_path: P, dst_path: P) -> io::Result<Self::Handle> {
        let inner = self
            .inner
            .reuse_and_open(src_path.as_ref(), dst_path.as_ref())?;
        Ok(self.handle(dst_path.as_ref(), inner))
    }

    fn delete_metadata<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        self.inner.delete_metadata(path)
    }

    fn exists_metadata<P: AsRef<Path>>(&self, path: P) -> bool {
        self.inner.exists_metadata(path)
    }

    fn new_reader(&self, handle: Arc<Self::Handle>) -> io::Result<Self::Reader> {
        self.inner.new_reader(handle.inner.clone())
    }

    fn new_writer(&self, handle: Arc<Self::Handle>) -> io::Result<Self::Writer> {
        self.inner.new_writer(handle.inner.clone())
    }
}

impl<F: FileSystem> Handle for GuardedHandle<F> {
    fn truncate(&self, offset: usize) -> io::Result<()> {
        let bytes = {
            let mut reader = self.fs.new_reader(self.inner.clone())?;
            reader.seek(SeekFrom::Start(0))?;
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes)?;
            bytes
        };
        let refused = |reason| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                TailRefused {
                    file: self.path.clone(),
                    offset,
                    len: bytes.len(),
                    reason,
                },
            )
        };
        let Some((seq, suffix)) = self
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(parse_log_name)
        else {
            return Err(refused("not a log file the engine names"));
        };
        if later_file(&self.path, seq, suffix)? {
            return Err(refused(
                "a later file of the same queue exists, so this one was finished and synced",
            ));
        }
        if let Err(reason) = torn_tail(&bytes, offset, seq) {
            return Err(refused(reason));
        }
        self.inner.truncate(offset)
    }

    fn file_size(&self) -> io::Result<usize> {
        self.inner.file_size()
    }

    fn sync(&self) -> io::Result<()> {
        self.inner.sync()
    }
}

/// Whether a file of `suffix`'s queue with a sequence above `seq` is in
/// `path`'s directory.
fn later_file(path: &Path, seq: u64, suffix: &str) -> io::Result<bool> {
    let Some(dir) = path.parent() else {
        return Ok(false);
    };
    for entry in std::fs::read_dir(dir)? {
        let name = entry?.file_name();
        if let Some((other, other_suffix)) = name.to_str().and_then(parse_log_name)
            && other_suffix == suffix
            && other > seq
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn parse_log_name(name: &str) -> Option<(u64, &'static str)> {
    let suffix = QUEUE_SUFFIXES.into_iter().find(|s| name.ends_with(s))?;
    if name.len() != SEQ_WIDTH + suffix.len() {
        return None;
    }
    Some((name[..SEQ_WIDTH].parse().ok()?, suffix))
}

/// `Ok` if what lies at and after `offset` in `bytes` is the remains of
/// one write that never finished, and the reason otherwise.
pub fn torn_tail(bytes: &[u8], offset: usize, seq: u64) -> Result<(), &'static str> {
    if offset > bytes.len() {
        return Err("the truncation point is past the end of the file");
    }
    let tail = &bytes[offset..];
    if tail.iter().all(|&b| b == 0) {
        // Only preallocated zeros: nothing is removed.
        return Ok(());
    }
    if offset == 0 {
        // Any sync of a file persists its header, so a header that was
        // never written means nothing in the file was ever synced.
        return if bytes[..FILE_HEADER_LEN.min(bytes.len())]
            .iter()
            .all(|&b| b == 0)
        {
            Ok(())
        } else {
            Err("the file header does not decode, and it is not unwritten")
        };
    }
    // Where the one write in flight would have ended.
    let (end, unfinished) = match batch_header(tail) {
        Some((len, item_offset)) => {
            let claimed = offset.saturating_add(len);
            if let Some(whole) = whole_batch_end(bytes, offset + item_offset, signature(bytes, seq))
                && whole != claimed
            {
                return Err("a whole batch is there, but its header claims another length");
            }
            // A batch written whole ends in its checksum, zero by chance once
            // in 2^32, and lies inside the file. A write that stopped short
            // (a buffered write can stop at any byte) leaves neither.
            let unfinished = claimed > bytes.len()
                || bytes[claimed - FOOTER_CHECKSUM_LEN..claimed]
                    .iter()
                    .all(|&b| b == 0);
            (claimed.min(bytes.len()), unfinished)
        }
        None => {
            // The header's own sector parts, through the end of the sector
            // its last byte is in: one of them unwritten, or damage.
            let header_sector_end =
                (offset + BATCH_HEADER_LEN).div_ceil(SECTOR_BYTES) * SECTOR_BYTES;
            if !has_unwritten_part(bytes, offset, header_sector_end.min(bytes.len())) {
                return Err("a batch header that neither decodes nor was torn");
            }
            // The batch's length is unknown; whatever was written of it
            // runs to the end of the file.
            (bytes.len(), true)
        }
    };
    if bytes[end..].iter().any(|&b| b != 0) {
        return Err("data follows the broken batch, so it was not the last write");
    }
    if batches_follow(bytes, offset, signature(bytes, seq)) {
        return Err("a whole batch follows the broken one, so it was not the last write");
    }
    if unfinished || has_unwritten_part(bytes, offset, end) {
        Ok(())
    } else {
        Err("the broken batch was written whole: no sector of it is unwritten")
    }
}

/// Whether some part of `bytes[start..end]`, split at sector boundaries,
/// is unwritten: all zeros, and longer than a batch's own bytes are zero
/// by nature. The first part starts with the batch header, whose length
/// field has at most [`LEN_FIELD_ZERO_PREFIX`] leading zero bytes; any
/// other part counts from [`BATCH_HEADER_LEN`] bytes.
fn has_unwritten_part(bytes: &[u8], start: usize, end: usize) -> bool {
    let mut at = start;
    while at < end {
        let part_end = ((at / SECTOR_BYTES + 1) * SECTOR_BYTES).min(end);
        let len = part_end - at;
        let long_enough = if at == start {
            len > LEN_FIELD_ZERO_PREFIX
        } else {
            len >= BATCH_HEADER_LEN
        };
        if long_enough && bytes[at..part_end].iter().all(|&b| b == 0) {
            return true;
        }
        at = part_end;
    }
    false
}

/// Whether a whole batch starts after `offset`: a well-formed header
/// whose item block's checksum matches at the end the header claims. A
/// header alone proves nothing (the broken batch's own header, read a byte
/// later, is often well formed); a matching checksum does.
fn batches_follow(bytes: &[u8], offset: usize, signature: Option<u32>) -> bool {
    let last = bytes.iter().rposition(|&b| b != 0).map_or(0, |i| i + 1);
    (offset + 1..last).any(|at| {
        batch_header(&bytes[at..]).is_some_and(|(len, item_offset)| {
            at + len <= bytes.len() && whole_at(bytes, at + item_offset, at + len, signature)
        })
    })
}

/// Whether the item block `bytes[items..end]` ends in its own checksum.
fn whole_at(bytes: &[u8], items: usize, end: usize, signature: Option<u32>) -> bool {
    if end < items + FOOTER_CHECKSUM_LEN + 1 {
        return false;
    }
    let body = &bytes[items..end - FOOTER_CHECKSUM_LEN];
    let stored = u32::from_le_bytes(
        bytes[end - FOOTER_CHECKSUM_LEN..end]
            .try_into()
            .expect("four bytes"),
    );
    crc32(body) == stored ^ signature.unwrap_or(0)
}

fn crc32(data: &[u8]) -> u32 {
    !data.iter().fold(!0u32, |state, &b| crc_step(state, b))
}

fn crc_step(state: u32, byte: u8) -> u32 {
    CRC_TABLE[((state ^ u32::from(byte)) & 0xff) as usize] ^ (state >> 8)
}

/// The length and item offset a well-formed batch header at the start of
/// `tail` claims.
fn batch_header(tail: &[u8]) -> Option<(usize, usize)> {
    let header = tail.get(..BATCH_HEADER_LEN)?;
    let len_and_type = u64::from_be_bytes(header[..8].try_into().ok()?);
    let item_offset = u64::from_be_bytes(header[8..].try_into().ok()?);
    let compression = (len_and_type & 0xff) as u8;
    let len = len_and_type >> 8;
    // The pinned engine's two compression types: none and lz4.
    let well_formed =
        compression <= 1 && item_offset >= BATCH_HEADER_LEN as u64 && item_offset <= len;
    if !well_formed {
        return None;
    }
    Some((
        usize::try_from(len).ok()?,
        usize::try_from(item_offset).ok()?,
    ))
}

/// The signature a version-2 file XORs into every batch's footer checksum:
/// the low 32 bits of its sequence (`pipe_log.rs`). `None` for version 1.
fn signature(bytes: &[u8], seq: u64) -> Option<u32> {
    let version = bytes.get(FILE_MAGIC_LEN..FILE_MAGIC_LEN + 8)?;
    (u64::from_be_bytes(version.try_into().ok()?) >= 2).then_some(seq as u32)
}

/// Where a batch whose item block starts at `items` ends, if it was
/// written whole: the first end at which the block's trailing checksum
/// (`crc32` of the block, XOR the file's signature, little-endian) matches.
/// The item block is the batch's last part, so it bounds the batch whatever
/// its header claims.
fn whole_batch_end(bytes: &[u8], items: usize, signature: Option<u32>) -> Option<usize> {
    let last = bytes.iter().rposition(|&b| b != 0).map_or(0, |i| i + 1);
    let limit = (last + FOOTER_CHECKSUM_LEN).min(bytes.len());
    let mut state = !0u32;
    let mut at = items;
    while at + FOOTER_CHECKSUM_LEN <= limit {
        if at > items {
            let stored = u32::from_le_bytes(bytes[at..at + FOOTER_CHECKSUM_LEN].try_into().ok()?);
            if !state == stored ^ signature.unwrap_or(0) {
                return Some(at + FOOTER_CHECKSUM_LEN);
            }
        }
        state = crc_step(state, bytes[at]);
        at += 1;
    }
    None
}

/// CRC-32 (IEEE), the engine's checksum (`crc32fast`).
const CRC_TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 {
                0xEDB8_8320 ^ (c >> 1)
            } else {
                c >> 1
            };
            k += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
};

#[cfg(test)]
mod tests {
    use super::*;

    fn header(len: u64, item_offset: u64) -> [u8; 16] {
        let mut h = [0u8; 16];
        h[..8].copy_from_slice(&(len << 8).to_be_bytes());
        h[8..].copy_from_slice(&item_offset.to_be_bytes());
        h
    }

    /// A file of `prefix` bytes of earlier batches, then one batch of
    /// `len` bytes at `prefix`, then zeros to `total`.
    fn file(prefix: usize, len: usize, total: usize) -> Vec<u8> {
        let mut bytes = vec![0xab; prefix];
        bytes.extend_from_slice(&header(len as u64, 16));
        bytes.extend(std::iter::repeat_n(0xcd, len - 16));
        bytes.resize(total, 0);
        bytes
    }

    #[test]
    fn preallocated_zeros_are_a_torn_tail() {
        assert_eq!(torn_tail(&file(700, 600, 4096), 1300, 1), Ok(()));
    }

    #[test]
    fn a_batch_cut_at_a_sector_boundary_is_torn() {
        let mut bytes = file(700, 1500, 4096);
        // The write reached 1024 and no further.
        bytes[1024..2200].fill(0);
        assert_eq!(torn_tail(&bytes, 700, 1), Ok(()));
    }

    #[test]
    fn a_batch_whose_header_sector_was_not_written_is_torn() {
        let mut bytes = file(700, 1500, 4096);
        // Sectors after the first landed; the header's did not.
        bytes[700..1024].fill(0);
        assert_eq!(torn_tail(&bytes, 700, 1), Ok(()));
    }

    #[test]
    fn a_bit_flip_in_a_batch_written_whole_is_damage() {
        let mut bytes = file(700, 1500, 4096);
        bytes[1500] ^= 0x01;
        assert_eq!(
            torn_tail(&bytes, 700, 1),
            Err("the broken batch was written whole: no sector of it is unwritten")
        );
    }

    #[test]
    fn a_batch_followed_by_data_is_damage() {
        let mut bytes = file(700, 600, 4096);
        // A later, well-formed batch.
        bytes[1300..1316].copy_from_slice(&header(200, 16));
        bytes[1316..1500].fill(0x11);
        // Damage the earlier one so the engine stops there.
        bytes[900] ^= 0x01;
        assert_eq!(
            torn_tail(&bytes, 700, 1),
            Err("data follows the broken batch, so it was not the last write")
        );
    }

    #[test]
    fn a_damaged_header_is_damage() {
        let mut bytes = file(700, 600, 4096);
        bytes[703] = 0xff;
        bytes[708] = 0x07;
        assert!(torn_tail(&bytes, 700, 1).is_err());
    }

    #[test]
    fn a_damaged_file_header_is_damage_and_an_unwritten_one_is_not() {
        let mut bytes = file(0, 600, 4096);
        bytes[..69].fill(0x42);
        assert!(
            torn_tail(&bytes, 0, 1).is_err(),
            "a batch follows the header"
        );
        let mut alone = vec![0x42; 69];
        alone.resize(4096, 0);
        assert!(torn_tail(&alone, 0, 1).is_err(), "the header was synced");
        assert_eq!(torn_tail(&[0; 4096], 0, 1), Ok(()), "never written");
        assert_eq!(torn_tail(&[], 0, 1), Ok(()), "created, nothing written");
    }

    #[test]
    fn a_header_across_a_sector_boundary_is_torn_if_one_side_is_unwritten() {
        // The batch starts 7 bytes before a boundary; the sector after it
        // landed, the one holding the header's start did not.
        let mut bytes = file(505, 1000, 4096);
        bytes[505..512].fill(0);
        assert_eq!(torn_tail(&bytes, 505, 1), Ok(()));
        // Both sides landed but the header is garbage: damage.
        let mut bytes = file(505, 1000, 4096);
        bytes[505..521].fill(0xee);
        assert!(torn_tail(&bytes, 505, 1).is_err());
    }

    #[test]
    fn a_whole_batch_after_the_broken_one_is_never_claimed_by_it() {
        // Two whole batches, then the preallocated zeros.
        let (mut bytes, batches) = signed_file(5, &[(600, 30), (400, 20)]);
        let (a, a_len) = batches[0];
        // The first one's length now reaches past the second into the
        // zeros, and its own footer is damaged too, so nothing says where
        // it really ends.
        bytes[a..a + 16].copy_from_slice(&header(a_len as u64 + 2000, 616));
        bytes[a + a_len - 1] ^= 0x01;
        assert_eq!(
            torn_tail(&bytes, a, 5),
            Err("a whole batch follows the broken one, so it was not the last write")
        );
    }

    #[test]
    fn a_length_fields_own_zeros_are_not_an_unwritten_sector() {
        // The header starts 4 bytes before a boundary: those 4 bytes are
        // the length field's leading zeros. A flip elsewhere in the batch.
        let mut bytes = file(508, 600, 4096);
        assert_eq!(bytes[508..512], [0, 0, 0, 0]);
        bytes[800] ^= 0x01;
        assert_eq!(
            torn_tail(&bytes, 508, 1),
            Err("the broken batch was written whole: no sector of it is unwritten")
        );
    }

    #[test]
    fn the_checksum_is_crc32_ieee() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    /// A version-2 file of sequence `seq` holding `batches` (entries and
    /// item block lengths), each written whole with its footer checksum
    /// signed, then zeros to 4096 bytes. Returns the file and each batch's
    /// offset and length.
    fn signed_file(seq: u64, batches: &[(usize, usize)]) -> (Vec<u8>, Vec<(usize, usize)>) {
        let mut bytes = vec![0x5a; FILE_MAGIC_LEN];
        bytes.extend_from_slice(&2u64.to_be_bytes());
        bytes.extend_from_slice(&0u64.to_be_bytes());
        let mut placed = Vec::new();
        for &(entries, items) in batches {
            let offset = bytes.len();
            let len = BATCH_HEADER_LEN + entries + items + FOOTER_CHECKSUM_LEN;
            bytes.extend_from_slice(&header(len as u64, (BATCH_HEADER_LEN + entries) as u64));
            bytes.extend(std::iter::repeat_n(0xcd, entries));
            let item_block: Vec<u8> = (0..items).map(|i| i as u8 | 1).collect();
            bytes.extend_from_slice(&item_block);
            bytes.extend_from_slice(&(crc32(&item_block) ^ seq as u32).to_le_bytes());
            placed.push((offset, len));
        }
        bytes.resize(4096, 0);
        (bytes, placed)
    }

    fn signed(seq: u64, entries: usize, items: usize) -> (Vec<u8>, usize, usize) {
        let (bytes, placed) = signed_file(seq, &[(entries, items)]);
        (bytes, placed[0].0, placed[0].1)
    }

    #[test]
    fn a_whole_last_batch_with_its_length_flipped_upward_is_damage() {
        let (mut bytes, offset, len) = signed(7, 900, 40);
        assert_eq!(
            whole_batch_end(&bytes, offset + 16 + 900, Some(7)),
            Some(offset + len)
        );
        // The length now reaches into the preallocated zeros.
        bytes[offset..offset + 16].copy_from_slice(&header(len as u64 + 1500, 916));
        assert_eq!(
            torn_tail(&bytes, offset, 7),
            Err("a whole batch is there, but its header claims another length")
        );
        // Under another file's signature the block doesn't verify, and the
        // zeros read as unwritten: the signature is what tells them apart.
        assert_eq!(torn_tail(&bytes, offset, 8), Ok(()));
    }

    #[test]
    fn a_whole_batch_with_damaged_entries_is_damage_and_torn_entries_are_not() {
        // The engine's broken-entries-block truncation: the item block
        // verifies, the entries block does not.
        let (mut bytes, offset, _) = signed(3, 1500, 40);
        bytes[offset + 600] ^= 0x01;
        assert_eq!(
            torn_tail(&bytes, offset, 3),
            Err("the broken batch was written whole: no sector of it is unwritten")
        );
        let (mut bytes, offset, _) = signed(3, 1500, 40);
        bytes[512..1024].fill(0);
        assert_eq!(torn_tail(&bytes, offset, 3), Ok(()));
    }

    #[test]
    fn a_batch_whose_write_stopped_short_is_torn() {
        // The write stopped mid-sector: its end, the checksum, is zeros.
        let (mut bytes, offset, len) = signed(4, 700, 40);
        bytes[offset + 500..offset + len].fill(0);
        assert_eq!(torn_tail(&bytes, offset, 4), Ok(()));
        // The same batch written whole with a flip near its end is damage.
        let (mut bytes, offset, len) = signed(4, 700, 40);
        bytes[offset + len - 10] ^= 0x01;
        assert!(torn_tail(&bytes, offset, 4).is_err());
        // And a file that ends inside the batch: never all written.
        let (bytes, offset, len) = signed(4, 700, 40);
        assert_eq!(torn_tail(&bytes[..offset + len - 100], offset, 4), Ok(()));
    }

    #[test]
    fn log_names_parse_by_queue() {
        assert_eq!(
            parse_log_name("0000000000000003.raftlog"),
            Some((3, ".raftlog"))
        );
        assert_eq!(
            parse_log_name("0000000000000001.rewrite"),
            Some((1, ".rewrite"))
        );
        assert_eq!(parse_log_name("0000000000000003.raftlog.reserved"), None);
        assert_eq!(parse_log_name("LOCK"), None);
    }
}
