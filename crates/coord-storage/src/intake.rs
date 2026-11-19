//! Dependency rows by intake order (task-d113, step 1; design Section
//! 17.1, as amended by task-d113's design step).
//!
//! The machine names a command's dependency row, and the leader's
//! proposal row, by command: `epoch, tag, command` (`rows::dependency_key`,
//! `rows::proposal_key`), and the journal's records keep naming them so.
//! Keyed so in `protocol_v1`, each row landed on a leaf no other command
//! in the commit touched, and copied that leaf and the branch page above
//! it (#185, #233). The store's lowering keys them instead by **intake
//! order**: `epoch, tag, intake order, command`, where the intake order is
//! the applied stamp's sequence of the record that brings the row and the
//! row's ordinal within that record. A commit's rows then share the
//! right-hand leaf of their region.
//!
//! * **The key** ([`intake_key`]): the epoch (8 bytes, big-endian), the tag,
//!   the sequence (8 bytes, big-endian), the ordinal (4 bytes, big-endian)
//!   and the 32-byte command, 53 bytes in all. The command stays in the
//!   key, as a suffix, so the value's format does not change and a key
//!   names its command without reading the row. An epoch's dependency rows
//!   stay contiguous, so boot's scan of them is the range it was.
//! * **The sequence** is the one the applied stamp takes for the record:
//!   the journal sequence under the journaled store, and the store
//!   sequence the worker gives the batch. `project` refuses a record that
//!   is not the stamp's next and `attach` a lost prefix, and the worker
//!   numbers batches in the same stamp, so a sequence is used once in a
//!   projection's life, and replay lowers the same record at the same
//!   sequence. The ordinal is the update's place among the record's
//!   updates.
//! * **The index** ([`IntakeIndex`]): each live intake-keyed row, in
//!   memory, under its whole command, a few rows a command (#185, lead
//!   17:43). No durable row is keyed by command, so no random leaf comes
//!   back. It is built at open from the rows ([`IntakeIndex::build`]),
//!   kept by the lowering, and published with the commit that changes it
//!   (rule (d)). It had kept each row under an 8-byte prefix of its
//!   command, and read each answer's row to settle whose it was.
//! * **One assignment** ([`Assigner`]): the lowering and the recovery
//!   cut's overlay map a by-command key through the same function, so the
//!   cut reads the keys materialization will write (rule (e)).
//!
//! A snapshot reader looks up only executed commands through the index
//! (rule (c)): [`crate::protocol::catch_up_entry`] reads the command's
//! executed row in its snapshot first.
//!
//! **Step 2: the payload and executed rows by order.** The machine and the
//! applier name a command's payload (`payload_v1`) and its executed row
//! (`executed_v1`) by command too, and the lowering keys them by order:
//!
//! * a payload not yet executed goes to the node-private
//!   `pending_payload_v1`, by intake order ([`pending_key`]), and the
//!   index's intake part holds it as it holds a dependency row;
//! * an executed row goes to `execution_v1` under its position
//!   ([`crate::codecs::position_key`]), its value naming the command, and
//!   the command's pending payload **moves** in the same transaction to
//!   `payload_v1` under the same position. The index's **executed part**
//!   maps the command's prefix to the position, for as long as the row is
//!   kept;
//! * rule (a): a payload put for a command the executed part holds writes
//!   nothing, since its payload has moved;
//! * rule (b): an executed row with no pending payload to move is refused,
//!   so `payload_v1` has no hole at a position.
//!
//! **The bound** (the lead on #237, #240's proposal): the same transaction
//! deletes the command's dependency rows, and the leader's proposal rows,
//! and writes the decided record, the dependency row of the latest epoch
//! that holds one, to the node-private `decided_v1` under the position,
//! with that epoch ([`crate::codecs::decided_value`]). The intake part
//! then holds only rows of commands not yet executed, whatever the
//! history. A later dependency row put or deleted by command for an
//! executed command follows its record to `decided_v1`, and a later
//! proposal row is dropped: the proposal is settled.

use std::collections::BTreeSet;
use std::ops::Bound;

use coord_consensus::digest::DigestMap;
use coord_consensus::rows::{DEPENDENCY_TAG, PROPOSAL_TAG};
use coord_core::effect::{CollectionId, StoreUpdate};
use coord_store_api::engine::{Direction, EngineError, ErrorClass, OrderedRead, ScanRequest};
use coord_store_api::registry::{Collection, meta_fields};
use coord_types::CommandId;
use coord_types::identity::Digest32;
use coord_types::ids::ExecutionPosition;

use crate::codecs::{
    ExecutionRecordV1, decided_value, decode_executed, decode_execution, encode_execution,
    position_key, position_of, split_decided,
};

/// Length of a by-command key: epoch, tag, command.
pub const COMMAND_KEY_LEN: usize = 8 + 1 + 32;
/// Length of an intake key: epoch, tag, sequence, ordinal, command.
pub const INTAKE_KEY_LEN: usize = 8 + 1 + 8 + 4 + 32;
/// Length of a `pending_payload_v1` key: sequence, ordinal, command.
pub const PENDING_KEY_LEN: usize = 8 + 4 + 32;

/// The index region the pending payloads are kept in: an epoch no store
/// reaches and a tag no protocol row has, so it is apart from every
/// epoch's dependency and proposal rows.
const PENDING_REGION: ([u8; 8], u8) = (u64::MAX.to_be_bytes(), 0xff);

pub use coord_store_api::registry::STORE_FORMAT_VERSION;

/// The bytes [`STORE_FORMAT_VERSION`] is recorded as.
pub const fn format_version_bytes() -> [u8; 4] {
    STORE_FORMAT_VERSION.to_be_bytes()
}

/// Where a row came in: the applied stamp's sequence of the record that
/// brought it, and its ordinal within that record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IntakeOrder {
    /// The record's sequence.
    pub seq: u64,
    /// The update's place among the record's updates.
    pub ordinal: u32,
}

/// Whether `tag` names rows this layout keys by intake order.
const fn by_intake(tag: u8) -> bool {
    tag == DEPENDENCY_TAG || tag == PROPOSAL_TAG
}

/// The `protocol_v1` key of a dependency or proposal row under this
/// layout.
pub fn intake_key(epoch: [u8; 8], tag: u8, order: IntakeOrder, command: &CommandId) -> Vec<u8> {
    let mut out = Vec::with_capacity(INTAKE_KEY_LEN);
    out.extend_from_slice(&epoch);
    out.push(tag);
    out.extend_from_slice(&order.seq.to_be_bytes());
    out.extend_from_slice(&order.ordinal.to_be_bytes());
    out.extend_from_slice(command.as_bytes());
    out
}

/// The `pending_payload_v1` key of a payload not yet executed (step 2).
pub fn pending_key(order: IntakeOrder, command: &CommandId) -> Vec<u8> {
    let mut out = Vec::with_capacity(PENDING_KEY_LEN);
    out.extend_from_slice(&order.seq.to_be_bytes());
    out.extend_from_slice(&order.ordinal.to_be_bytes());
    out.extend_from_slice(command.as_bytes());
    out
}

/// Read a `pending_payload_v1` key.
pub fn parse_pending(key: &[u8]) -> Option<(IntakeOrder, CommandId)> {
    if key.len() != PENDING_KEY_LEN {
        return None;
    }
    let mut seq = [0u8; 8];
    seq.copy_from_slice(&key[..8]);
    let mut ordinal = [0u8; 4];
    ordinal.copy_from_slice(&key[8..12]);
    Some((
        IntakeOrder {
            seq: u64::from_be_bytes(seq),
            ordinal: u32::from_be_bytes(ordinal),
        },
        command_at(key, 12),
    ))
}

/// What a `protocol_v1` key is to this layout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolKey {
    /// A dependency or proposal row named by command, as the machine and
    /// the journal name it. Never a key in the projection.
    ByCommand {
        /// The epoch's bytes.
        epoch: [u8; 8],
        /// The tag.
        tag: u8,
        /// The command.
        command: CommandId,
    },
    /// A dependency or proposal row as the projection keys it.
    Intake {
        /// The epoch's bytes.
        epoch: [u8; 8],
        /// The tag.
        tag: u8,
        /// Where it came in.
        order: IntakeOrder,
        /// The command.
        command: CommandId,
    },
    /// Any other row: a promise, a Sync, a seal, or a row this layout does
    /// not know. Lowered as it is.
    Other,
}

fn command_at(key: &[u8], at: usize) -> CommandId {
    let mut id = [0u8; 32];
    id.copy_from_slice(&key[at..at + 32]);
    CommandId(Digest32(id))
}

/// Read a `protocol_v1` key.
pub fn parse(key: &[u8]) -> ProtocolKey {
    let Some(&tag) = key.get(8) else {
        return ProtocolKey::Other;
    };
    if !by_intake(tag) {
        return ProtocolKey::Other;
    }
    let mut epoch = [0u8; 8];
    epoch.copy_from_slice(&key[..8]);
    match key.len() {
        COMMAND_KEY_LEN => ProtocolKey::ByCommand {
            epoch,
            tag,
            command: command_at(key, 9),
        },
        INTAKE_KEY_LEN => {
            let mut seq = [0u8; 8];
            seq.copy_from_slice(&key[9..17]);
            let mut ordinal = [0u8; 4];
            ordinal.copy_from_slice(&key[17..21]);
            ProtocolKey::Intake {
                epoch,
                tag,
                order: IntakeOrder {
                    seq: u64::from_be_bytes(seq),
                    ordinal: u32::from_be_bytes(ordinal),
                },
                command: command_at(key, 21),
            }
        }
        _ => ProtocolKey::Other,
    }
}

/// The by-command key an intake key stands for, or `None` for any other
/// key: what the machine named the row (for tools and tests that compare
/// rows across layouts).
pub fn command_key_of(key: &[u8]) -> Option<Vec<u8>> {
    match parse(key) {
        ProtocolKey::Intake {
            epoch,
            tag,
            command,
            ..
        } => {
            let mut out = Vec::with_capacity(COMMAND_KEY_LEN);
            out.extend_from_slice(&epoch);
            out.push(tag);
            out.extend_from_slice(command.as_bytes());
            Some(out)
        }
        _ => None,
    }
}

/// One of a command's intake-keyed rows: its region (the epoch and the
/// tag) and its intake order, ordered so that a command's rows of one
/// region are together and its epochs oldest first. 24 bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct RowAt {
    epoch: u64,
    tag: u8,
    seq: u64,
    ordinal: u32,
}

impl RowAt {
    fn new(epoch: [u8; 8], tag: u8, order: IntakeOrder) -> Self {
        RowAt {
            epoch: u64::from_be_bytes(epoch),
            tag,
            seq: order.seq,
            ordinal: order.ordinal,
        }
    }

    const fn order(self) -> IntakeOrder {
        IntakeOrder {
            seq: self.seq,
            ordinal: self.ordinal,
        }
    }

    const fn epoch_bytes(self) -> [u8; 8] {
        self.epoch.to_be_bytes()
    }

    fn is_in(self, epoch: [u8; 8], tag: u8) -> bool {
        self.epoch == u64::from_be_bytes(epoch) && self.tag == tag
    }
}

/// A command's intake-keyed rows, in order: its pending payload, and its
/// dependency and proposal rows, one an epoch. A handful.
type CommandRows = Vec<RowAt>;

fn prefix(command: &CommandId) -> u64 {
    let mut p = [0u8; 8];
    p.copy_from_slice(&command.as_bytes()[..8]);
    u64::from_be_bytes(p)
}

/// The bytes an intake part's row takes, an estimate the metrics line
/// multiplies the rows by (#185, lead 17:43, the intake index). A command
/// is one entry of a digest map, its 32-byte identity and its rows'
/// vector, 57 bytes with the control byte, at 8/7 to 16/7 entries a
/// bucket by the map's fill; and its rows, 24 bytes each and the
/// allocation's own. At three rows a command (a leader's: the pending
/// payload, a dependency row and a proposal row), 49 to 70 bytes a row.
/// The ordered sets this replaced took 39 to 46 (#237's review).
pub const ENTRY_BYTES: u64 = 64;

/// The bytes an executed part's entry takes, an estimate the metrics line
/// multiplies the entries by: a digest map's 16-byte prefix and position
/// and its control byte, at 8/7 to 16/7 entries a bucket by the map's
/// fill, 19 to 39. The ordered set this replaced took 27.1 as #243's
/// review measured it.
pub const EXECUTED_ENTRY_BYTES: u64 = 30;

/// The executed part: each kept `execution_v1` row's position, by its
/// command's 8-byte prefix, one probe a lookup. A prefix two executed
/// commands share (their first eight bytes agree) keeps the first
/// position in `first` and the rest in `more`.
#[derive(Clone, Debug, Default)]
struct Executed {
    first: DigestMap<u64, u64>,
    more: BTreeSet<(u64, u64)>,
}

impl Executed {
    fn insert(&mut self, (prefix, position): (u64, u64)) {
        match self.first.get(&prefix) {
            None => {
                self.first.insert(prefix, position);
            }
            Some(&held) if held == position => {}
            Some(_) => {
                self.more.insert((prefix, position));
            }
        }
    }

    fn remove(&mut self, (prefix, position): (u64, u64)) {
        match self.first.get(&prefix) {
            Some(&held) if held == position => {
                let next = self
                    .more
                    .range((prefix, 0)..=(prefix, u64::MAX))
                    .next()
                    .copied();
                match next {
                    Some(next) => {
                        self.more.remove(&next);
                        self.first.insert(prefix, next.1);
                    }
                    None => {
                        self.first.remove(&prefix);
                    }
                }
            }
            Some(_) => {
                self.more.remove(&(prefix, position));
            }
            None => {}
        }
    }

    /// The positions of `prefix`. A command executes once, so at most one
    /// of them is a given command's; their order is not kept.
    fn of(&self, prefix: u64) -> impl Iterator<Item = u64> + '_ {
        self.first.get(&prefix).copied().into_iter().chain(
            self.more
                .range((prefix, 0)..=(prefix, u64::MAX))
                .map(|&(_, position)| position),
        )
    }

    fn contains(&self, (prefix, position): (u64, u64)) -> bool {
        self.first.get(&prefix) == Some(&position) || self.more.contains(&(prefix, position))
    }

    fn len(&self) -> usize {
        self.first.len() + self.more.len()
    }
}

/// Equal when they hold the same entries, whichever of a shared prefix's
/// positions each keeps first.
impl PartialEq for Executed {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self.first.iter().all(|(&p, &at)| other.contains((p, at)))
            && self.more.iter().all(|&e| other.contains(e))
    }
}

impl Eq for Executed {}

/// The index by command of a projection's intake-keyed rows, in memory
/// (design Section 17.1, "1. The index by command", its intake part).
///
/// It holds each live dependency, proposal or pending payload row, and it
/// is the projection's at [`IntakeIndex::stamp`]: the lowering publishes a
/// commit's changes with the commit, before the read gate moves, so a
/// snapshot the gate hands out is never ahead of it.
///
/// **By the whole command** (#185, lead 17:43): the intake part is a map
/// from each command with a live row to its rows, a slab of a few rows a
/// command, so a lookup is one probe and its answer is the command's own.
/// It had kept each row by an 8-byte prefix of the command in an ordered
/// set an epoch and tag, so a lookup was a range in a tree, and each
/// answer was read from the store to settle whose it was. The intake part
/// holds only commands not yet executed (the bound), so the whole
/// identity costs little.
///
/// The executed part keeps its prefix: it holds an entry for each kept
/// execution, the history, and the row at a position names its command.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IntakeIndex {
    /// The intake part: each command with a live intake-keyed row, and its
    /// rows.
    live: DigestMap<CommandId, CommandRows>,
    /// The intake part's rows, over every command.
    len: usize,
    /// The executed part (step 2): each kept `execution_v1` row, as the
    /// command's prefix and the position.
    executed: Executed,
    /// The journal sequence of the last commit published into it.
    stamp: u64,
}

impl IntakeIndex {
    /// Build the index of `view`'s rows, and check the layout: a store of
    /// another format is refused with the reason.
    ///
    /// The format recorded at initialization must be this build's. A store
    /// that records none (one an engine initialized without recording it,
    /// or the in-memory model) is read as this layout only if it holds no
    /// row keyed by command, which is what the format before this one
    /// wrote.
    pub fn build<V: OrderedRead>(view: &V, stamp: u64) -> Result<Self, EngineError> {
        let recorded = view.get(Collection::MetaV1.id(), meta_fields::FORMAT_VERSION)?;
        if let Some(recorded) = &recorded
            && recorded.as_slice() != format_version_bytes()
        {
            return Err(EngineError::new(
                ErrorClass::Unsupported,
                format!(
                    "store format {} is not this build's ({}): a store of another layout \
                     is not converted (task-d113 keyed the dependency rows by intake order; \
                     task-d105 writes no history rows)",
                    describe(recorded),
                    STORE_FORMAT_VERSION
                ),
            ));
        }
        let mut index = IntakeIndex {
            stamp,
            ..IntakeIndex::default()
        };
        scan_all(view, Collection::ProtocolV1, |key, _| match parse(key) {
            ProtocolKey::Intake {
                epoch,
                tag,
                order,
                command,
            } => {
                index.insert(command, RowAt::new(epoch, tag, order));
                Ok(())
            }
            ProtocolKey::ByCommand { .. } => Err(EngineError::new(
                ErrorClass::Unsupported,
                format!(
                    "store format 1 (dependency rows keyed by command, before task-d113) is \
                     not this build's ({STORE_FORMAT_VERSION}): a store of another layout is \
                     not converted"
                ),
            )),
            ProtocolKey::Other => Ok(()),
        })?;
        scan_all(view, Collection::PendingPayloadV1, |key, _| {
            let (order, command) = parse_pending(key).ok_or_else(|| {
                EngineError::new(
                    ErrorClass::Corrupt,
                    "a pending payload key of the wrong length",
                )
            })?;
            let (epoch, tag) = PENDING_REGION;
            index.insert(command, RowAt::new(epoch, tag, order));
            Ok(())
        })?;
        scan_all(view, Collection::ExecutionV1, |key, value| {
            let position = position_of(key)?;
            let record = decode_execution(value)?;
            index
                .executed
                .insert((prefix(&record.command), position.get()));
            Ok(())
        })?;
        // A store of format 2 kept its executed rows in `executed_v1`.
        if !view
            .scan_page(
                Collection::ExecutedV1.id(),
                &ScanRequest {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                    direction: Direction::Forward,
                    resume_after: None,
                    max_rows: 1.try_into().expect("non-zero"),
                    max_bytes: (1 << 20).try_into().expect("non-zero"),
                },
            )?
            .rows
            .is_empty()
        {
            return Err(EngineError::new(
                ErrorClass::Unsupported,
                format!(
                    "store format 2 (executed rows in executed_v1 by command, before task-d113's \
                     step 2) is not this build's ({STORE_FORMAT_VERSION}): a store of another \
                     layout is not converted"
                ),
            ));
        }
        Ok(index)
    }

    fn insert(&mut self, command: CommandId, row: RowAt) {
        let rows = self.live.entry(command).or_default();
        if let Err(at) = rows.binary_search(&row) {
            rows.insert(at, row);
            self.len += 1;
        }
    }

    /// `command`'s rows, in order.
    fn rows(&self, command: &CommandId) -> &[RowAt] {
        self.live
            .get(command)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    /// Rows held in the intake part.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the intake part holds no row.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The journal sequence of the last commit published into it.
    pub const fn stamp(&self) -> u64 {
        self.stamp
    }

    /// The intake orders of `command`'s rows of an epoch and tag, in
    /// order. Each is the command's: the index keeps the whole command.
    pub fn candidates(
        &self,
        epoch: [u8; 8],
        tag: u8,
        command: &CommandId,
    ) -> impl Iterator<Item = IntakeOrder> + '_ {
        self.rows(command)
            .iter()
            .filter(move |row| row.is_in(epoch, tag))
            .map(|row| row.order())
    }

    /// The positions `command` may have executed at: every executed entry
    /// whose prefix is the command's. The row at the position, which
    /// names its command, settles whose each is.
    pub fn executed_candidates(
        &self,
        command: &CommandId,
    ) -> impl Iterator<Item = ExecutionPosition> + '_ {
        self.executed
            .of(prefix(command))
            .filter_map(|position| ExecutionPosition::new(position).ok())
    }

    /// Executed entries held: the executed part's size.
    pub fn executed_len(&self) -> usize {
        self.executed.len()
    }

    /// Publish `delta`, the changes of the commit stamped `stamp`, made
    /// over this index as it is: each command it changes takes the rows
    /// the delta leaves it.
    ///
    /// Debug builds check the delta was made over this index at this
    /// stamp (#282's review): a command's rows are replaced whole, so a
    /// delta published onto another base would put back rows a later
    /// commit had changed.
    pub fn publish(&mut self, delta: IndexDelta, stamp: u64) {
        debug_assert_eq!(
            delta.base, self.stamp,
            "an index delta made over the index at one stamp, published onto it at another"
        );
        for (command, rows) in delta.live {
            let held = self.live.get(&command).map_or(0, Vec::len);
            self.len = self.len - held + rows.len();
            if rows.is_empty() {
                self.live.remove(&command);
            } else {
                self.live.insert(command, rows);
            }
        }
        for e in delta.executed_removed {
            self.executed.remove(e);
        }
        for e in delta.executed_added {
            self.executed.insert(e);
        }
        self.stamp = stamp;
    }

    /// Drop every entry: the store quarantined, and the next open rebuilds
    /// it from the rows.
    pub fn clear(&mut self) {
        *self = IntakeIndex::default();
    }
}

/// Call `f` with each row of `collection` in `view`, in key order.
fn scan_all<V: OrderedRead>(
    view: &V,
    collection: Collection,
    mut f: impl FnMut(&[u8], &[u8]) -> Result<(), EngineError>,
) -> Result<(), EngineError> {
    let mut resume: Option<Vec<u8>> = None;
    loop {
        let page = view.scan_page(
            collection.id(),
            &ScanRequest {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
                direction: Direction::Forward,
                resume_after: resume.take(),
                max_rows: 4096.try_into().expect("non-zero"),
                max_bytes: (4 << 20).try_into().expect("non-zero"),
            },
        )?;
        for row in &page.rows {
            f(&row.key, &row.value)?;
        }
        match page.rows.last() {
            Some(last) if !page.exhausted => resume = Some(last.key.clone()),
            _ => return Ok(()),
        }
    }
}

fn describe(recorded: &[u8]) -> String {
    match <[u8; 4]>::try_from(recorded) {
        Ok(bytes) => u32::from_be_bytes(bytes).to_string(),
        Err(_) => format!("of {} unreadable bytes", recorded.len()),
    }
}

/// The index's changes since a base: one commit's, held until its outcome
/// is known (rule (d)), or an overlay's over the index at its snapshot's
/// stamp (rule (e)). It is published onto the base it was made over.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IndexDelta {
    /// Each command whose intake-keyed rows this changes, with the rows
    /// the changes leave it: none, if it has none left.
    live: DigestMap<CommandId, CommandRows>,
    /// The same commit's changes to the key index (task-d105), which
    /// moves with this one.
    pub(crate) kv: crate::kv_index::KvDelta,
    executed_added: BTreeSet<(u64, u64)>,
    executed_removed: BTreeSet<(u64, u64)>,
    /// The stamp of the index it was made over, which
    /// [`IntakeIndex::publish`] checks in debug builds.
    base: u64,
}

impl IndexDelta {
    /// Whether it changes nothing.
    pub fn is_empty(&self) -> bool {
        self.live.is_empty()
            && self.executed_added.is_empty()
            && self.executed_removed.is_empty()
            && self.kv.is_empty()
    }

    fn execute(&mut self, e: (u64, u64)) {
        if !self.executed_removed.remove(&e) {
            self.executed_added.insert(e);
        }
    }

    fn unexecute(&mut self, e: (u64, u64)) {
        if !self.executed_added.remove(&e) {
            self.executed_removed.insert(e);
        }
    }
}

/// How one update is lowered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Assigned {
    /// Write it under its own key.
    AsIs,
    /// Write it under this key.
    Key(Vec<u8>),
    /// Write nothing: a delete of a row by command that is not there, or a
    /// payload put for a command already executed (rule (a)).
    Skip,
    /// Write these in its place, in order: an executed row, written by
    /// position with its payload moved (step 2), or a payload written to
    /// the pending collection.
    Writes(Vec<StoreUpdate>),
}

/// The key assignment (rules (d) and (e)): the one function the lowering
/// and the recovery cut's overlay both lower a record's updates through.
///
/// It reads the index at the state its rows are at (`base`), keeps its own
/// changes (`delta`), and reads a row through `read` at that state, its
/// own earlier updates included. Each update is to be applied
/// before the next is assigned.
///
/// The intake part keys each command whole and is kept with every write
/// to an intake-keyed row, so where it holds a row the row is there: a
/// rewrite or a delete takes its key from the index, without reading the
/// row. Rows are read for their bytes (the pending payload an execution
/// moves, the dependency row its decided record copies), and debug builds
/// read every row the index answers with and check it is there.
pub struct Assigner<'a> {
    base: &'a IntakeIndex,
    delta: IndexDelta,
}

/// Debug builds: check that a row the index holds is there.
#[cfg(debug_assertions)]
fn check_held(
    read: &mut impl FnMut(CollectionId, &[u8]) -> Result<Option<Vec<u8>>, EngineError>,
    collection: CollectionId,
    key: &[u8],
) -> Result<(), EngineError> {
    assert!(
        read(collection, key)?.is_some(),
        "the intake index holds a row that is not there: {key:02x?}"
    );
    Ok(())
}

impl<'a> Assigner<'a> {
    /// An assignment over `base`.
    pub fn new(base: &'a IntakeIndex) -> Self {
        Assigner {
            base,
            delta: IndexDelta {
                base: base.stamp,
                ..IndexDelta::default()
            },
        }
    }

    /// The changes made so far.
    pub fn into_delta(self) -> IndexDelta {
        self.delta
    }

    /// Take `update` into the key index's changes (task-d105), before it
    /// is written: `old` reads a row as it stands before it.
    pub fn observe_kv(
        &mut self,
        update: &StoreUpdate,
        old: impl FnOnce(&[u8]) -> Result<Option<Vec<u8>>, EngineError>,
    ) -> Result<(), EngineError> {
        self.delta.kv.observe(update, old)
    }

    /// `command`'s rows as this assignment has them: as its own changes
    /// left them, else as the base holds them.
    fn rows(&self, command: &CommandId) -> &[RowAt] {
        match self.delta.live.get(command) {
            Some(rows) => rows.as_slice(),
            None => self.base.rows(command),
        }
    }

    /// `command`'s rows to change, taken from the base at the first
    /// change.
    fn rows_mut(&mut self, command: &CommandId) -> &mut CommandRows {
        let base = self.base;
        self.delta
            .live
            .entry(*command)
            .or_insert_with(|| base.rows(command).to_vec())
    }

    fn add(&mut self, command: &CommandId, row: RowAt) {
        if self.rows(command).binary_search(&row).is_err() {
            let rows = self.rows_mut(command);
            if let Err(at) = rows.binary_search(&row) {
                rows.insert(at, row);
            }
        }
    }

    fn remove(&mut self, command: &CommandId, row: RowAt) {
        if self.rows(command).binary_search(&row).is_ok() {
            let rows = self.rows_mut(command);
            if let Ok(at) = rows.binary_search(&row) {
                rows.remove(at);
            }
        }
    }

    /// The intake order of `command`'s row of an epoch and tag, if it has
    /// one: the first, if an image left it more than one.
    fn row_of(&self, epoch: [u8; 8], tag: u8, command: &CommandId) -> Option<IntakeOrder> {
        self.rows(command)
            .iter()
            .find(|row| row.is_in(epoch, tag))
            .map(|row| row.order())
    }

    /// The position `command` executed at, if the executed part names one
    /// whose row is the command's.
    fn executed_at(
        &self,
        command: &CommandId,
        read: &mut impl FnMut(CollectionId, &[u8]) -> Result<Option<Vec<u8>>, EngineError>,
    ) -> Result<Option<ExecutionPosition>, EngineError> {
        let p = prefix(command);
        let removed = &self.delta.executed_removed;
        let positions = self
            .base
            .executed
            .of(p)
            .filter(|position| !removed.contains(&(p, *position)))
            .chain(
                self.delta
                    .executed_added
                    .range((p, 0)..=(p, u64::MAX))
                    .map(|&(_, position)| position),
            );
        for position in positions {
            let position = ExecutionPosition::new(position)
                .map_err(|_| EngineError::new(ErrorClass::Corrupt, "an executed entry at zero"))?;
            if let Some(row) = read(Collection::ExecutionV1.id(), &position_key(position))?
                && decode_execution(&row)?.command == *command
            {
                return Ok(Some(position));
            }
        }
        Ok(None)
    }

    /// `command`'s pending payload, if one is there: its intake order and
    /// its bytes, read for an execution to move.
    fn pending_of(
        &self,
        command: &CommandId,
        read: &mut impl FnMut(CollectionId, &[u8]) -> Result<Option<Vec<u8>>, EngineError>,
    ) -> Result<Option<(IntakeOrder, Vec<u8>)>, EngineError> {
        let (epoch, tag) = PENDING_REGION;
        let Some(order) = self.row_of(epoch, tag, command) else {
            return Ok(None);
        };
        let value = read(
            Collection::PendingPayloadV1.id(),
            &pending_key(order, command),
        )?;
        debug_assert!(
            value.is_some(),
            "the intake index holds a pending payload that is not there"
        );
        Ok(value.map(|value| (order, value)))
    }

    /// At `command`'s execution at `position` (the bound): delete its
    /// dependency and proposal rows, and write the latest epoch's
    /// dependency row to `decided_v1` under the position. Its pending
    /// payload's row has been taken by the caller; the command is left
    /// with no row in the intake part.
    fn move_decided(
        &mut self,
        command: &CommandId,
        position: ExecutionPosition,
        read: &mut impl FnMut(CollectionId, &[u8]) -> Result<Option<Vec<u8>>, EngineError>,
    ) -> Result<Vec<StoreUpdate>, EngineError> {
        let protocol = Collection::ProtocolV1.id();
        let rows = std::mem::take(self.rows_mut(command));
        let dependencies = rows.iter().filter(|row| row.tag == DEPENDENCY_TAG);
        let proposals = rows.iter().filter(|row| row.tag == PROPOSAL_TAG);
        let mut out: Vec<StoreUpdate> = dependencies
            .clone()
            .chain(proposals)
            .map(|row| StoreUpdate {
                collection: protocol,
                key: intake_key(row.epoch_bytes(), row.tag, row.order(), command),
                value: None,
            })
            .collect();
        // The decided record is the latest epoch's dependency row: read
        // for its bytes, from the latest down past any row not there.
        for row in dependencies.rev() {
            let key = intake_key(row.epoch_bytes(), row.tag, row.order(), command);
            if let Some(value) = read(protocol, &key)? {
                out.push(StoreUpdate {
                    collection: Collection::DecidedV1.id(),
                    key: position_key(position).to_vec(),
                    value: Some(decided_value(row.epoch_bytes(), &value)),
                });
                break;
            }
            if cfg!(debug_assertions) {
                panic!("the intake index holds a dependency row that is not there");
            }
        }
        Ok(out)
    }

    /// A by-command update of a dependency or proposal row of `command`
    /// with no row of its own here: if the command has executed, its rows
    /// moved at execution. A dependency row follows its record to
    /// `decided_v1`, unless the record there is of a later epoch, and is
    /// deleted there only in its own epoch. A proposal row writes nothing.
    fn after_execution(
        &self,
        epoch: [u8; 8],
        tag: u8,
        command: &CommandId,
        value: Option<&[u8]>,
        read: &mut impl FnMut(CollectionId, &[u8]) -> Result<Option<Vec<u8>>, EngineError>,
    ) -> Result<Option<Assigned>, EngineError> {
        let Some(position) = self.executed_at(command, read)? else {
            return Ok(None);
        };
        if tag != DEPENDENCY_TAG {
            return Ok(Some(Assigned::Skip));
        }
        let decided = Collection::DecidedV1.id();
        let key = position_key(position).to_vec();
        let held = match read(decided, &key)? {
            Some(bytes) => Some(u64::from_be_bytes(split_decided(&bytes)?.0)),
            None => None,
        };
        let this = u64::from_be_bytes(epoch);
        Ok(Some(match value {
            Some(value) if held.is_none_or(|held| held <= this) => {
                Assigned::Writes(vec![StoreUpdate {
                    collection: decided,
                    key,
                    value: Some(decided_value(epoch, value)),
                }])
            }
            None if held == Some(this) => Assigned::Writes(vec![StoreUpdate {
                collection: decided,
                key,
                value: None,
            }]),
            _ => Assigned::Skip,
        }))
    }

    /// Assign `update`, the `ordinal`th of the record at `seq`. `read`
    /// reads a row at the state the record's earlier updates have left.
    pub fn assign(
        &mut self,
        seq: u64,
        ordinal: u32,
        update: &StoreUpdate,
        mut read: impl FnMut(CollectionId, &[u8]) -> Result<Option<Vec<u8>>, EngineError>,
    ) -> Result<Assigned, EngineError> {
        if update.collection == Collection::PayloadV1.id() {
            return self.assign_payload(seq, ordinal, update, &mut read);
        }
        if update.collection == Collection::ExecutedV1.id() {
            return self.assign_executed(update, &mut read);
        }
        if update.collection != Collection::ProtocolV1.id() {
            return Ok(Assigned::AsIs);
        }
        match parse(&update.key) {
            ProtocolKey::Other => Ok(Assigned::AsIs),
            // A row named by its intake key: a trim step's deletion. It is
            // lowered as it is, and the index follows it. No writer puts a
            // row under its intake key (every put names its row by command,
            // and this lowering keys it); the put is followed all the same.
            ProtocolKey::Intake {
                epoch,
                tag,
                order,
                command,
            } => {
                let row = RowAt::new(epoch, tag, order);
                if update.value.is_some() {
                    self.add(&command, row);
                } else {
                    self.remove(&command, row);
                }
                Ok(Assigned::AsIs)
            }
            ProtocolKey::ByCommand {
                epoch,
                tag,
                command,
            } => {
                let found = self.row_of(epoch, tag, &command);
                #[cfg(debug_assertions)]
                if let Some(order) = found {
                    check_held(
                        &mut read,
                        Collection::ProtocolV1.id(),
                        &intake_key(epoch, tag, order, &command),
                    )?;
                }
                match (&update.value, found) {
                    // A rewrite stays in place, under its intake key.
                    (Some(_), Some(order)) => {
                        Ok(Assigned::Key(intake_key(epoch, tag, order, &command)))
                    }
                    (Some(value), None) => {
                        if let Some(moved) =
                            self.after_execution(epoch, tag, &command, Some(value), &mut read)?
                        {
                            return Ok(moved);
                        }
                        let order = IntakeOrder { seq, ordinal };
                        self.add(&command, RowAt::new(epoch, tag, order));
                        Ok(Assigned::Key(intake_key(epoch, tag, order, &command)))
                    }
                    (None, Some(order)) => {
                        self.remove(&command, RowAt::new(epoch, tag, order));
                        Ok(Assigned::Key(intake_key(epoch, tag, order, &command)))
                    }
                    (None, None) => Ok(self
                        .after_execution(epoch, tag, &command, None, &mut read)?
                        .unwrap_or(Assigned::Skip)),
                }
            }
        }
    }

    /// A payload named by command (step 2): to the pending collection, or
    /// nothing if the command has executed (rule (a)).
    fn assign_payload(
        &mut self,
        seq: u64,
        ordinal: u32,
        update: &StoreUpdate,
        read: &mut impl FnMut(CollectionId, &[u8]) -> Result<Option<Vec<u8>>, EngineError>,
    ) -> Result<Assigned, EngineError> {
        let Ok(bytes) = <[u8; 32]>::try_from(update.key.as_slice()) else {
            // Keyed by position already: an image's row, or a trim step's.
            return Ok(Assigned::AsIs);
        };
        let command = CommandId(Digest32(bytes));
        let pending = Collection::PendingPayloadV1.id();
        let (epoch, tag) = PENDING_REGION;
        if self.executed_at(&command, read)?.is_some() {
            // Rule (a): its payload has moved to its position, and a put
            // would leave a pending row nothing moves or deletes. A delete
            // never takes an executed command's payload.
            return Ok(Assigned::Skip);
        }
        // Only the key is needed: a rewrite or a delete takes the row's
        // key from the index.
        let found = self.row_of(epoch, tag, &command);
        #[cfg(debug_assertions)]
        if let Some(order) = found {
            check_held(read, pending, &pending_key(order, &command))?;
        }
        match (&update.value, found) {
            (Some(value), Some(order)) => Ok(Assigned::Writes(vec![StoreUpdate {
                collection: pending,
                key: pending_key(order, &command),
                value: Some(value.clone()),
            }])),
            (Some(value), None) => {
                let order = IntakeOrder { seq, ordinal };
                self.add(&command, RowAt::new(epoch, tag, order));
                Ok(Assigned::Writes(vec![StoreUpdate {
                    collection: pending,
                    key: pending_key(order, &command),
                    value: Some(value.clone()),
                }]))
            }
            (None, Some(order)) => {
                self.remove(&command, RowAt::new(epoch, tag, order));
                Ok(Assigned::Writes(vec![StoreUpdate {
                    collection: pending,
                    key: pending_key(order, &command),
                    value: None,
                }]))
            }
            (None, None) => Ok(Assigned::Skip),
        }
    }

    /// An executed row named by command (step 2): written to `execution_v1`
    /// under its position, with the command's pending payload moved to
    /// `payload_v1` under the same position, and its decided record moved
    /// to `decided_v1` (the bound). With no pending payload to move, it is
    /// refused (rule (b)).
    fn assign_executed(
        &mut self,
        update: &StoreUpdate,
        read: &mut impl FnMut(CollectionId, &[u8]) -> Result<Option<Vec<u8>>, EngineError>,
    ) -> Result<Assigned, EngineError> {
        let bytes = <[u8; 32]>::try_from(update.key.as_slice()).map_err(|_| {
            EngineError::new(ErrorClass::Corrupt, "an executed row not named by command")
        })?;
        let command = CommandId(Digest32(bytes));
        let execution = Collection::ExecutionV1.id();
        let payloads = Collection::PayloadV1.id();
        let Some(value) = &update.value else {
            // Its rows by position go with it.
            let Some(position) = self.executed_at(&command, read)? else {
                return Ok(Assigned::Skip);
            };
            self.delta.unexecute((prefix(&command), position.get()));
            let key = position_key(position).to_vec();
            return Ok(Assigned::Writes(vec![
                StoreUpdate {
                    collection: execution,
                    key: key.clone(),
                    value: None,
                },
                StoreUpdate {
                    collection: payloads,
                    key: key.clone(),
                    value: None,
                },
                StoreUpdate {
                    collection: Collection::DecidedV1.id(),
                    key,
                    value: None,
                },
            ]));
        };
        let executed = decode_executed(value)?;
        let position = executed.position;
        let key = position_key(position).to_vec();
        let row = StoreUpdate {
            collection: execution,
            key: key.clone(),
            value: Some(encode_execution(&ExecutionRecordV1 { command, executed })?),
        };
        if self.executed_at(&command, read)? == Some(position) {
            // Written again with the same position: its payload has moved.
            return Ok(Assigned::Writes(vec![row]));
        }
        let Some((order, payload)) = self.pending_of(&command, read)? else {
            return Err(EngineError::new(
                ErrorClass::Corrupt,
                format!(
                    "a command executed at position {} with no pending payload to move \
                     (task-d113, rule (b))",
                    position.get()
                ),
            ));
        };
        self.delta.execute((prefix(&command), position.get()));
        let mut writes = vec![
            row,
            StoreUpdate {
                collection: Collection::PendingPayloadV1.id(),
                key: pending_key(order, &command),
                value: None,
            },
            StoreUpdate {
                collection: payloads,
                key,
                value: Some(payload),
            },
        ];
        // The command's rows all go: the pending payload's above, and its
        // dependency and proposal rows here.
        writes.extend(self.move_decided(&command, position, read)?);
        Ok(Assigned::Writes(writes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(n: u8) -> CommandId {
        CommandId(Digest32([n; 32]))
    }

    #[test]
    fn keys_round_trip_and_keep_an_epoch_contiguous() {
        let epoch = 7u64.to_be_bytes();
        let order = IntakeOrder {
            seq: 0x0102_0304,
            ordinal: 9,
        };
        let key = intake_key(epoch, DEPENDENCY_TAG, order, &cmd(3));
        assert_eq!(key.len(), INTAKE_KEY_LEN);
        assert_eq!(
            parse(&key),
            ProtocolKey::Intake {
                epoch,
                tag: DEPENDENCY_TAG,
                order,
                command: cmd(3)
            }
        );
        let logical = coord_consensus::rows::dependency_key(
            coord_types::ids::ConfigurationEpoch::new(7).unwrap(),
            &cmd(3),
        );
        assert_eq!(command_key_of(&key), Some(logical.clone()));
        assert!(matches!(parse(&logical), ProtocolKey::ByCommand { .. }));
        // Later intake sorts later, whatever the command.
        let later = intake_key(
            epoch,
            DEPENDENCY_TAG,
            IntakeOrder {
                seq: order.seq + 1,
                ordinal: 0,
            },
            &cmd(0),
        );
        assert!(later > key);
    }

    /// Two commands whose first eight bytes agree, the prefix the index
    /// once kept rows by: each keeps its own row, and a rewrite lands on
    /// its own.
    #[test]
    fn two_commands_of_one_prefix_keep_their_own_rows() {
        let epoch = 1u64.to_be_bytes();
        let mut a = [1u8; 32];
        let mut b = [1u8; 32];
        a[31] = 0xaa;
        b[31] = 0xbb;
        let (a, b) = (CommandId(Digest32(a)), CommandId(Digest32(b)));
        let base = IntakeIndex::default();
        let mut rows: BTreeSet<Vec<u8>> = BTreeSet::new();
        let mut assigner = Assigner::new(&base);
        let put = |c: &CommandId| StoreUpdate {
            collection: Collection::ProtocolV1.id(),
            key: coord_consensus::rows::dependency_key(
                coord_types::ids::ConfigurationEpoch::new(1).unwrap(),
                c,
            ),
            value: Some(vec![1]),
        };
        for (i, c) in [a, b].iter().enumerate() {
            let Assigned::Key(k) = assigner
                .assign(5, i as u32, &put(c), |_, k| {
                    Ok(rows.contains(k).then(Vec::new))
                })
                .unwrap()
            else {
                panic!("a new row is keyed");
            };
            rows.insert(k);
        }
        // b's rewrite lands on b's row, not on a's under the same prefix.
        let Assigned::Key(k) = assigner
            .assign(6, 0, &put(&b), |_, k| Ok(rows.contains(k).then(Vec::new)))
            .unwrap()
        else {
            panic!("a rewrite is keyed");
        };
        assert_eq!(
            k,
            intake_key(
                epoch,
                DEPENDENCY_TAG,
                IntakeOrder { seq: 5, ordinal: 1 },
                &b
            )
        );
        let delta = assigner.into_delta();
        assert_eq!(delta.live.len(), 2);
        assert!(delta.live.values().all(|rows| rows.len() == 1), "{delta:?}");
    }

    /// The rows an assignment's updates leave, by collection and key: what
    /// the tests below read through, applying each assignment as the
    /// lowering does.
    #[derive(Default)]
    struct Rows(std::collections::BTreeMap<(u16, Vec<u8>), Vec<u8>>);

    impl Rows {
        fn get(&self, collection: CollectionId, key: &[u8]) -> Option<Vec<u8>> {
            self.0.get(&(collection.0, key.to_vec())).cloned()
        }

        fn apply(&mut self, update: &StoreUpdate) {
            let k = (update.collection.0, update.key.clone());
            match &update.value {
                Some(v) => {
                    self.0.insert(k, v.clone());
                }
                None => {
                    self.0.remove(&k);
                }
            }
        }

        fn assign(
            &mut self,
            assigner: &mut Assigner<'_>,
            seq: u64,
            ordinal: u32,
            update: StoreUpdate,
        ) -> Result<Assigned, EngineError> {
            let assigned = assigner.assign(seq, ordinal, &update, |c, k| Ok(self.get(c, k)))?;
            match &assigned {
                Assigned::AsIs => self.apply(&update),
                Assigned::Key(key) => self.apply(&StoreUpdate {
                    key: key.clone(),
                    ..update
                }),
                Assigned::Skip => {}
                Assigned::Writes(writes) => writes.iter().for_each(|w| self.apply(w)),
            }
            Ok(assigned)
        }

        fn of(&self, collection: Collection) -> usize {
            let id = collection.id().0;
            self.0.keys().filter(|(c, _)| *c == id).count()
        }
    }

    fn payload(command: &CommandId, value: Option<u8>) -> StoreUpdate {
        StoreUpdate {
            collection: Collection::PayloadV1.id(),
            key: command.as_bytes().to_vec(),
            value: value.map(|v| vec![v]),
        }
    }

    fn executed(command: &CommandId, position: u64) -> StoreUpdate {
        StoreUpdate {
            collection: Collection::ExecutedV1.id(),
            key: crate::codecs::executed_key(command),
            value: Some(
                crate::codecs::encode_executed(&crate::codecs::ExecutedRecordV1 {
                    position: ExecutionPosition::new(position).unwrap(),
                    revision: None,
                    result_digest: Digest32([position as u8; 32]),
                })
                .unwrap(),
            ),
        }
    }

    /// A payload is pending by intake order until its command executes;
    /// then the executed row is written by position and the payload moves
    /// to the same position in the same assignment (step 2).
    #[test]
    fn an_execution_moves_the_pending_payload_to_its_position() {
        let base = IntakeIndex::default();
        let mut rows = Rows::default();
        let mut assigner = Assigner::new(&base);
        let c = cmd(4);
        let Assigned::Writes(w) = rows
            .assign(&mut assigner, 3, 1, payload(&c, Some(9)))
            .unwrap()
        else {
            panic!("a payload is written pending");
        };
        let pending = pending_key(IntakeOrder { seq: 3, ordinal: 1 }, &c);
        assert_eq!(w[0].key, pending);
        assert_eq!(
            parse_pending(&pending),
            Some((IntakeOrder { seq: 3, ordinal: 1 }, c))
        );
        assert_eq!(rows.of(Collection::PendingPayloadV1), 1);
        assert_eq!(rows.of(Collection::PayloadV1), 0);
        // Written again before it executes: in place.
        rows.assign(&mut assigner, 4, 0, payload(&c, Some(9)))
            .unwrap();
        assert_eq!(rows.of(Collection::PendingPayloadV1), 1);

        rows.assign(&mut assigner, 5, 0, executed(&c, 17)).unwrap();
        let at = position_key(ExecutionPosition::new(17).unwrap());
        assert_eq!(
            rows.of(Collection::PendingPayloadV1),
            0,
            "the pending row is gone"
        );
        assert_eq!(rows.get(Collection::PayloadV1.id(), &at), Some(vec![9]));
        let row = decode_execution(&rows.get(Collection::ExecutionV1.id(), &at).unwrap()).unwrap();
        assert_eq!(row.command, c);
        assert_eq!(row.executed.position.get(), 17);
        assert_eq!(
            rows.of(Collection::ExecutedV1),
            0,
            "nothing is keyed by command"
        );

        // Rule (a): a payload put after the command executed writes
        // nothing, and the moved payload stays.
        assert_eq!(
            rows.assign(&mut assigner, 6, 0, payload(&c, Some(1)))
                .unwrap(),
            Assigned::Skip
        );
        assert_eq!(rows.of(Collection::PendingPayloadV1), 0);
        assert_eq!(rows.get(Collection::PayloadV1.id(), &at), Some(vec![9]));

        // The same execution written again: only its row.
        let Assigned::Writes(w) = rows.assign(&mut assigner, 7, 0, executed(&c, 17)).unwrap()
        else {
            panic!("a re-put is written");
        };
        assert_eq!(w.len(), 1);
        assert_eq!(rows.get(Collection::PayloadV1.id(), &at), Some(vec![9]));

        let delta = assigner.into_delta();
        assert_eq!(delta.executed_added.len(), 1);
        assert!(
            delta.live.values().all(Vec::is_empty),
            "the pending entry was added and moved: {delta:?}"
        );
    }

    /// Rule (b): an execution with no pending payload to move is refused,
    /// rather than leaving an executed command without its payload. The
    /// negative control: with the payload pending, the same row is taken.
    #[test]
    fn an_execution_without_a_pending_payload_is_refused() {
        let base = IntakeIndex::default();
        let mut rows = Rows::default();
        let mut assigner = Assigner::new(&base);
        let err = rows
            .assign(&mut assigner, 1, 0, executed(&cmd(2), 5))
            .unwrap_err();
        assert_eq!(err.class, ErrorClass::Corrupt);
        assert!(err.diagnostic.contains("rule (b)"), "{}", err.diagnostic);

        rows.assign(&mut assigner, 1, 0, payload(&cmd(2), Some(1)))
            .unwrap();
        rows.assign(&mut assigner, 1, 1, executed(&cmd(2), 5))
            .unwrap();
        assert_eq!(rows.of(Collection::ExecutionV1), 1);
    }

    /// A payload delete takes a pending row, and nothing of an executed
    /// command's (rule (a)); an executed row's delete takes its rows by
    /// position with it.
    #[test]
    fn deletes_take_the_rows_they_name() {
        let base = IntakeIndex::default();
        let mut rows = Rows::default();
        let mut assigner = Assigner::new(&base);
        let (a, b) = (cmd(1), cmd(2));
        rows.assign(&mut assigner, 1, 0, payload(&a, Some(1)))
            .unwrap();
        rows.assign(&mut assigner, 1, 1, payload(&b, Some(2)))
            .unwrap();
        rows.assign(&mut assigner, 2, 0, executed(&b, 3)).unwrap();
        rows.assign(&mut assigner, 3, 0, payload(&a, None)).unwrap();
        assert_eq!(rows.of(Collection::PendingPayloadV1), 0);
        assert_eq!(
            rows.assign(&mut assigner, 3, 1, payload(&b, None)).unwrap(),
            Assigned::Skip
        );
        assert_eq!(rows.of(Collection::PayloadV1), 1);
        let gone = StoreUpdate {
            value: None,
            ..executed(&b, 3)
        };
        rows.assign(&mut assigner, 4, 0, gone.clone()).unwrap();
        assert_eq!(rows.of(Collection::PayloadV1), 0);
        assert_eq!(rows.of(Collection::ExecutionV1), 0);
        assert_eq!(
            rows.assign(&mut assigner, 4, 1, gone).unwrap(),
            Assigned::Skip
        );
    }

    /// A delta is published onto the index it was made over (#282's
    /// review), and debug builds refuse one made at another stamp. Here two
    /// deltas are made over one index and both published: the second's
    /// commands would take the rows it left them, over whatever the first
    /// had changed.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "published onto it at another")]
    fn a_delta_made_over_another_stamp_is_refused_in_debug_builds() {
        let mut base = IntakeIndex::default();
        let mut rows = Rows::default();
        let (first, second) = {
            let mut a = Assigner::new(&base);
            rows.assign(&mut a, 1, 0, payload(&cmd(1), Some(1)))
                .unwrap();
            let mut b = Assigner::new(&base);
            rows.assign(&mut b, 2, 0, payload(&cmd(2), Some(2)))
                .unwrap();
            (a.into_delta(), b.into_delta())
        };
        base.publish(first, 1);
        base.publish(second, 2);
    }

    fn protocol_row(tag: u8, epoch: u64, command: &CommandId, value: Option<u8>) -> StoreUpdate {
        let epoch = coord_types::ids::ConfigurationEpoch::new(epoch).unwrap();
        StoreUpdate {
            collection: Collection::ProtocolV1.id(),
            key: if tag == DEPENDENCY_TAG {
                coord_consensus::rows::dependency_key(epoch, command)
            } else {
                coord_consensus::rows::proposal_key(epoch, command)
            },
            value: value.map(|v| vec![v]),
        }
    }

    /// The bound: an execution deletes the command's dependency rows, in
    /// every epoch, and its proposal rows, and writes the latest epoch's
    /// record to `decided_v1` under the position, with that epoch. The
    /// intake part keeps no entry of the command. A later dependency row
    /// follows its record there, and a later proposal row writes nothing.
    #[test]
    fn an_execution_moves_the_decided_record_to_its_position() {
        let c = cmd(6);
        let other = cmd(7);
        // Its rows, brought by earlier commits and published: the index the
        // execution's assignment reads them through is the base.
        let mut base = IntakeIndex::default();
        let mut rows = Rows::default();
        {
            let mut assigner = Assigner::new(&base);
            rows.assign(&mut assigner, 1, 0, payload(&c, Some(1)))
                .unwrap();
            rows.assign(
                &mut assigner,
                1,
                1,
                protocol_row(DEPENDENCY_TAG, 1, &c, Some(10)),
            )
            .unwrap();
            rows.assign(
                &mut assigner,
                1,
                2,
                protocol_row(PROPOSAL_TAG, 1, &c, Some(20)),
            )
            .unwrap();
            rows.assign(
                &mut assigner,
                2,
                0,
                protocol_row(DEPENDENCY_TAG, 2, &c, Some(11)),
            )
            .unwrap();
            rows.assign(
                &mut assigner,
                2,
                1,
                protocol_row(DEPENDENCY_TAG, 2, &other, Some(12)),
            )
            .unwrap();
            let delta = assigner.into_delta();
            base.publish(delta, 2);
        }
        assert_eq!(base.len(), 5, "{base:?}");
        assert_eq!(rows.of(Collection::ProtocolV1), 4);

        let mut assigner = Assigner::new(&base);
        rows.assign(&mut assigner, 3, 0, executed(&c, 9)).unwrap();
        let at = position_key(ExecutionPosition::new(9).unwrap());
        assert_eq!(
            rows.of(Collection::ProtocolV1),
            1,
            "only the other command's row is left"
        );
        assert_eq!(
            rows.get(Collection::DecidedV1.id(), &at),
            Some(decided_value(2u64.to_be_bytes(), &[11])),
            "the latest epoch's record, with its epoch"
        );
        assert_eq!(rows.of(Collection::DecidedV1), 1);

        // A rewrite of the record by command, in its epoch, follows it.
        assert!(matches!(
            rows.assign(
                &mut assigner,
                4,
                0,
                protocol_row(DEPENDENCY_TAG, 2, &c, Some(13))
            )
            .unwrap(),
            Assigned::Writes(_)
        ));
        assert_eq!(
            rows.get(Collection::DecidedV1.id(), &at),
            Some(decided_value(2u64.to_be_bytes(), &[13]))
        );
        // An earlier epoch's does not replace it, nor delete it.
        for value in [Some(14), None] {
            assert_eq!(
                rows.assign(
                    &mut assigner,
                    4,
                    1,
                    protocol_row(DEPENDENCY_TAG, 1, &c, value)
                )
                .unwrap(),
                Assigned::Skip
            );
        }
        // A proposal row of a settled proposal writes nothing.
        assert_eq!(
            rows.assign(
                &mut assigner,
                4,
                2,
                protocol_row(PROPOSAL_TAG, 2, &c, Some(21))
            )
            .unwrap(),
            Assigned::Skip
        );
        assert_eq!(rows.of(Collection::ProtocolV1), 1);

        let delta = assigner.into_delta();
        base.publish(delta, 4);
        assert_eq!(
            base.len(),
            1,
            "the intake part holds only the other command"
        );
        assert_eq!(base.executed_len(), 1);

        // A delete in its epoch takes it; the executed row's delete would too.
        let mut assigner = Assigner::new(&base);
        rows.assign(
            &mut assigner,
            5,
            0,
            protocol_row(DEPENDENCY_TAG, 2, &c, None),
        )
        .unwrap();
        assert_eq!(rows.of(Collection::DecidedV1), 0);
        rows.assign(
            &mut assigner,
            5,
            1,
            protocol_row(DEPENDENCY_TAG, 2, &c, Some(15)),
        )
        .unwrap();
        assert_eq!(rows.of(Collection::DecidedV1), 1);
        rows.assign(
            &mut assigner,
            6,
            0,
            StoreUpdate {
                value: None,
                ..executed(&c, 9)
            },
        )
        .unwrap();
        assert_eq!(rows.of(Collection::DecidedV1), 0);
        assert_eq!(rows.of(Collection::ExecutionV1), 0);
    }

    /// A measurement, run by hand (`cargo test --release -p coord-storage
    /// --lib -- --ignored assignment_cost`): the assignment's cost a
    /// command, by commands a commit (#185, lead 17:43, the intake index).
    ///
    /// Each command is taken in (its payload, a dependency row and a
    /// proposal row, one record) and later executed (its executed row, one
    /// record), as a leader lowers it. The index starts with 2,000 commands
    /// taken in and not executed, and 50,000 executed. Rows are read from an
    /// ordered map in memory, the same before and after a change.
    #[test]
    #[ignore = "a measurement, run by hand"]
    fn assignment_cost_by_commands_a_commit() {
        fn id(n: u64) -> CommandId {
            let mut bytes = [0u8; 32];
            let mut x = n.wrapping_add(1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            for chunk in bytes.chunks_mut(8) {
                x ^= x >> 31;
                x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
                x ^= x >> 29;
                chunk.copy_from_slice(&x.to_be_bytes());
            }
            CommandId(Digest32(bytes))
        }
        struct Run {
            base: IntakeIndex,
            rows: Rows,
            seq: u64,
            next: u64,
            oldest: u64,
            position: u64,
        }
        impl Run {
            fn commit(&mut self, intake: u64, execute: u64) {
                let mut assigner = Assigner::new(&self.base);
                for _ in 0..intake {
                    let c = id(self.next);
                    self.next += 1;
                    let updates = [
                        payload(&c, Some(7)),
                        protocol_row(DEPENDENCY_TAG, 1, &c, Some(1)),
                        protocol_row(PROPOSAL_TAG, 1, &c, Some(2)),
                    ];
                    for (i, u) in updates.into_iter().enumerate() {
                        self.rows
                            .assign(&mut assigner, self.seq, i as u32, u)
                            .unwrap();
                    }
                    self.seq += 1;
                }
                for _ in 0..execute {
                    let c = id(self.oldest);
                    self.oldest += 1;
                    self.rows
                        .assign(&mut assigner, self.seq, 0, executed(&c, self.position))
                        .unwrap();
                    self.position += 1;
                    self.seq += 1;
                }
                let delta = assigner.into_delta();
                self.base.publish(delta, self.seq);
            }
        }
        let mut run = Run {
            base: IntakeIndex::default(),
            rows: Rows::default(),
            seq: 1,
            next: 0,
            oldest: 0,
            position: 1,
        };
        for _ in 0..20 {
            run.commit(100, 0);
        }
        for _ in 0..500 {
            run.commit(100, 100);
        }
        for k in [4u64, 16, 64, 256] {
            let commits = 20_000 / k;
            let start = std::time::Instant::now();
            for _ in 0..commits {
                run.commit(k, k);
            }
            let ns = start.elapsed().as_nanos() as f64 / (commits * k) as f64;
            println!("{k:>4} commands a commit: {ns:>8.0} ns a command (taken in and executed)");
        }
    }
}
