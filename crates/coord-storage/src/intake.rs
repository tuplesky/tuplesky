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
//!   memory, by an 8-byte prefix of its command. No durable row is keyed
//!   by command, so no random leaf comes back. A prefix two commands share
//!   keeps both entries, and the full key, which carries the command,
//!   settles which is whose. It is built at open from the rows
//!   ([`IntakeIndex::build`]), kept by the lowering, and published with the
//!   commit that changes it (rule (d)).
//! * **One assignment** ([`Assigner`]): the lowering and the recovery
//!   cut's overlay map a by-command key through the same function, so the
//!   cut reads the keys materialization will write (rule (e)).
//!
//! A snapshot reader looks up only executed commands through the index
//! (rule (c)): [`crate::protocol::catch_up_entry`] reads the command's
//! executed row in its snapshot first.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use coord_consensus::rows::{DEPENDENCY_TAG, PROPOSAL_TAG};
use coord_core::effect::StoreUpdate;
use coord_store_api::engine::{Direction, EngineError, ErrorClass, OrderedRead, ScanRequest};
use coord_store_api::registry::{Collection, meta_fields};
use coord_types::CommandId;
use coord_types::identity::Digest32;

/// Length of a by-command key: epoch, tag, command.
pub const COMMAND_KEY_LEN: usize = 8 + 1 + 32;
/// Length of an intake key: epoch, tag, sequence, ordinal, command.
pub const INTAKE_KEY_LEN: usize = 8 + 1 + 8 + 4 + 32;

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

impl IntakeOrder {
    const MIN: IntakeOrder = IntakeOrder { seq: 0, ordinal: 0 };
    const MAX: IntakeOrder = IntakeOrder {
        seq: u64::MAX,
        ordinal: u32::MAX,
    };
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

/// One index entry: epoch, tag, the command's 8-byte prefix and the
/// row's intake order. Ordered so that one command's entries in an epoch
/// and tag are a range.
type Entry = (u64, u8, u64, IntakeOrder);

fn prefix(command: &CommandId) -> u64 {
    let mut p = [0u8; 8];
    p.copy_from_slice(&command.as_bytes()[..8]);
    u64::from_be_bytes(p)
}

fn entry(epoch: [u8; 8], tag: u8, order: IntakeOrder, command: &CommandId) -> Entry {
    (u64::from_be_bytes(epoch), tag, prefix(command), order)
}

fn range_of<'a>(
    entries: &'a BTreeSet<Entry>,
    epoch: [u8; 8],
    tag: u8,
    command: &CommandId,
) -> impl Iterator<Item = &'a Entry> + use<'a> {
    let e = u64::from_be_bytes(epoch);
    let p = prefix(command);
    entries.range((
        Bound::Included((e, tag, p, IntakeOrder::MIN)),
        Bound::Included((e, tag, p, IntakeOrder::MAX)),
    ))
}

/// One entry as the index stores it, within its epoch and tag: the
/// command's prefix, the sequence and the ordinal, 24 bytes.
type Slot = (u64, u64, u32);

/// The bytes an entry takes, slot and set nodes together, at the most
/// review measured on #237 (39 to 46): what the metrics line multiplies
/// the entries by. The design step had inferred 24 to 32.
pub const ENTRY_BYTES: u64 = 46;

/// The index by command of a projection's intake-keyed rows, in memory
/// (design Section 17.1, "1. The index by command", its intake part).
///
/// It holds an entry for each live dependency or proposal row, and it is
/// the projection's at [`IntakeIndex::stamp`]: the lowering publishes a
/// commit's changes with the commit, before the read gate moves, so a
/// snapshot the gate hands out is never ahead of it.
///
/// Entries are kept by epoch and tag, of which a store has a few, each an
/// ordered set of 24-byte slots: about 30 bytes a live row with the set's
/// own overhead.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IntakeIndex {
    regions: BTreeMap<(u64, u8), BTreeSet<Slot>>,
    len: usize,
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
                    "store format {} is not this build's ({}): task-d113 keys the dependency \
                     rows by intake order, and a store of another layout is not converted",
                    describe(recorded),
                    STORE_FORMAT_VERSION
                ),
            ));
        }
        let mut index = IntakeIndex {
            stamp,
            ..IntakeIndex::default()
        };
        let mut resume: Option<Vec<u8>> = None;
        loop {
            let page = view.scan_page(
                Collection::ProtocolV1.id(),
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
                match parse(&row.key) {
                    ProtocolKey::Intake {
                        epoch,
                        tag,
                        order,
                        command,
                    } => {
                        index.insert(entry(epoch, tag, order, &command));
                    }
                    ProtocolKey::ByCommand { .. } => {
                        return Err(EngineError::new(
                            ErrorClass::Unsupported,
                            "store format 1 (dependency rows keyed by command, before task-d113) \
                             is not this build's (2): a store of another layout is not converted",
                        ));
                    }
                    ProtocolKey::Other => {}
                }
            }
            match page.rows.last() {
                Some(last) if !page.exhausted => resume = Some(last.key.clone()),
                _ => break,
            }
        }
        Ok(index)
    }

    fn insert(&mut self, (epoch, tag, prefix, order): Entry) {
        if self
            .regions
            .entry((epoch, tag))
            .or_default()
            .insert((prefix, order.seq, order.ordinal))
        {
            self.len += 1;
        }
    }

    fn remove(&mut self, (epoch, tag, prefix, order): Entry) {
        if let Some(region) = self.regions.get_mut(&(epoch, tag))
            && region.remove(&(prefix, order.seq, order.ordinal))
        {
            self.len -= 1;
            if region.is_empty() {
                self.regions.remove(&(epoch, tag));
            }
        }
    }

    /// The entries of `command`'s prefix in an epoch and tag, in order.
    fn of(
        &self,
        epoch: [u8; 8],
        tag: u8,
        command: &CommandId,
    ) -> impl Iterator<Item = Entry> + use<'_> {
        let e = u64::from_be_bytes(epoch);
        let p = prefix(command);
        self.regions
            .get(&(e, tag))
            .into_iter()
            .flat_map(move |region| {
                region.range((
                    Bound::Included((p, 0, 0)),
                    Bound::Included((p, u64::MAX, u32::MAX)),
                ))
            })
            .map(move |&(p, seq, ordinal)| (e, tag, p, IntakeOrder { seq, ordinal }))
    }

    /// Entries held.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether it holds no entry.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The journal sequence of the last commit published into it.
    pub const fn stamp(&self) -> u64 {
        self.stamp
    }

    /// The intake orders a row of `command` may have, in order: every
    /// entry whose prefix is the command's. The key settles whose each is.
    pub fn candidates(
        &self,
        epoch: [u8; 8],
        tag: u8,
        command: &CommandId,
    ) -> impl Iterator<Item = IntakeOrder> + '_ {
        self.of(epoch, tag, command).map(|(_, _, _, order)| order)
    }

    /// Publish `delta`, the changes of the commit stamped `stamp`.
    pub fn publish(&mut self, delta: IndexDelta, stamp: u64) {
        for e in delta.removed {
            self.remove(e);
        }
        for e in delta.added {
            self.insert(e);
        }
        self.stamp = stamp;
    }

    /// Drop every entry: the store quarantined, and the next open rebuilds
    /// it from the rows.
    pub fn clear(&mut self) {
        *self = IntakeIndex::default();
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
/// stamp (rule (e)).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IndexDelta {
    added: BTreeSet<Entry>,
    removed: BTreeSet<Entry>,
}

impl IndexDelta {
    /// Whether it changes nothing.
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty()
    }

    fn add(&mut self, e: Entry) {
        if !self.removed.remove(&e) {
            self.added.insert(e);
        }
    }

    fn remove(&mut self, e: Entry) {
        if !self.added.remove(&e) {
            self.removed.insert(e);
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
    /// Write nothing: a delete of a row by command that is not there.
    Skip,
}

/// The key assignment (rules (d) and (e)): the one function the lowering
/// and the recovery cut's overlay both lower a record's updates through.
///
/// It reads the index at the state its rows are at (`base`), keeps its own
/// changes (`delta`), and asks `exists` whether a key is a row at that
/// state, its own earlier updates included. Each update is to be applied
/// before the next is assigned.
pub struct Assigner<'a> {
    base: &'a IntakeIndex,
    delta: IndexDelta,
}

impl<'a> Assigner<'a> {
    /// An assignment over `base`.
    pub fn new(base: &'a IntakeIndex) -> Self {
        Assigner {
            base,
            delta: IndexDelta::default(),
        }
    }

    /// The changes made so far.
    pub fn into_delta(self) -> IndexDelta {
        self.delta
    }

    fn live(&self, epoch: [u8; 8], tag: u8, command: &CommandId) -> Vec<IntakeOrder> {
        let mut out: Vec<IntakeOrder> = self
            .base
            .of(epoch, tag, command)
            .filter(|e| !self.delta.removed.contains(e))
            .chain(range_of(&self.delta.added, epoch, tag, command).copied())
            .map(|(_, _, _, order)| order)
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }

    /// Assign `update`, the `ordinal`th of the record at `seq`.
    pub fn assign(
        &mut self,
        seq: u64,
        ordinal: u32,
        update: &StoreUpdate,
        mut exists: impl FnMut(&[u8]) -> Result<bool, EngineError>,
    ) -> Result<Assigned, EngineError> {
        if update.collection != Collection::ProtocolV1.id() {
            return Ok(Assigned::AsIs);
        }
        match parse(&update.key) {
            ProtocolKey::Other => Ok(Assigned::AsIs),
            // A row named by its intake key: a trim step's deletion. It is
            // lowered as it is, and the index follows it.
            ProtocolKey::Intake {
                epoch,
                tag,
                order,
                command,
            } => {
                let e = entry(epoch, tag, order, &command);
                if update.value.is_some() {
                    self.delta.add(e);
                } else {
                    self.delta.remove(e);
                }
                Ok(Assigned::AsIs)
            }
            ProtocolKey::ByCommand {
                epoch,
                tag,
                command,
            } => {
                let mut found = None;
                for order in self.live(epoch, tag, &command) {
                    if exists(&intake_key(epoch, tag, order, &command))? {
                        found = Some(order);
                        break;
                    }
                    // Another command's entry under the same prefix: a row
                    // of this command at that order is not there.
                }
                match (&update.value, found) {
                    // A rewrite stays in place, under its intake key.
                    (Some(_), Some(order)) => {
                        Ok(Assigned::Key(intake_key(epoch, tag, order, &command)))
                    }
                    (Some(_), None) => {
                        let order = IntakeOrder { seq, ordinal };
                        self.delta.add(entry(epoch, tag, order, &command));
                        Ok(Assigned::Key(intake_key(epoch, tag, order, &command)))
                    }
                    (None, Some(order)) => {
                        self.delta.remove(entry(epoch, tag, order, &command));
                        Ok(Assigned::Key(intake_key(epoch, tag, order, &command)))
                    }
                    (None, None) => Ok(Assigned::Skip),
                }
            }
        }
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

    #[test]
    fn a_shared_prefix_keeps_both_entries_and_the_key_settles_whose() {
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
                .assign(5, i as u32, &put(c), |k| Ok(rows.contains(k)))
                .unwrap()
            else {
                panic!("a new row is keyed");
            };
            rows.insert(k);
        }
        // b's rewrite lands on b's row, not on a's under the same prefix.
        let Assigned::Key(k) = assigner
            .assign(6, 0, &put(&b), |k| Ok(rows.contains(k)))
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
        assert_eq!(assigner.into_delta().added.len(), 2);
    }
}
