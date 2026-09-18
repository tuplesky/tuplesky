//! Command descriptors with atomic initialization, path evidence, bounded
//! capacity and closure traversal (tasks 20-21; design Section 4.7;
//! prototype `getCmdDescSeq`, `getDepAndHashes`, `keyInfo`,
//! `recordLeaderHash`).
//!
//! Installing a command's initialized state (payload binding, phase,
//! dependencies, per-key path digests) and exposing it through the
//! conflict index is one transition of [`CommandTable::initialize`]. A
//! descriptor created by early leader evidence ([`CommandTable::expect`])
//! is a placeholder: it is not in the conflict index and reports no
//! phase, so a conflicting command initialized meanwhile cannot see it as
//! processed state and the dependency-phase guards treat it as unknown.
//!
//! Capacity bounds new work: when the table is full, initialization is
//! refused with [`InitError::Backpressure`]; records in ACCEPT or COMMIT
//! are never evicted to make room, only executed records can be retired.

use alloc::collections::BTreeMap;
use alloc::vec::Vec;

use coord_types::CommandId;
use coord_types::identity::Digest32;
use serde::{Deserialize, Serialize};

use crate::graph::{ClosureCursor, ClosureProgress, PathLog, combined_path};
use crate::phase::{GuardViolation, Phase, guard_accept, guard_commit, guard_execute};

/// A command as this replica knows it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandRecord {
    /// Phase.
    pub phase: Phase,
    /// Direct dependencies (local order until the leader's is adopted).
    pub deps: Vec<CommandId>,
    /// Conflict keys of the payload.
    pub keys: Vec<Vec<u8>>,
    /// Digest of the bound payload (`None` for a placeholder).
    pub payload: Option<Digest32>,
    /// Per-key path digests through this command at initialization.
    pub paths: Vec<(Vec<u8>, Digest32)>,
    /// Combined path evidence (what a fast acknowledgement carries).
    pub path: Digest32,
}

/// Why initialization did not happen.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InitError {
    /// The command is already initialized (a duplicate or reordered
    /// message); nothing changed.
    AlreadyInitialized,
    /// A different payload was presented for an initialized command.
    PayloadConflict,
    /// The table is full; new work is refused, nothing is evicted.
    Backpressure,
}

/// Why a record was not retired.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetireError {
    /// Unknown command.
    Unknown,
    /// Only executed commands may be retired; unresolved acceptance is
    /// never deleted.
    NotExecuted(Phase),
}

/// What initialization produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Initialized {
    /// Local direct dependencies.
    pub deps: Vec<CommandId>,
    /// Per-key path digests.
    pub paths: Vec<(Vec<u8>, Digest32)>,
    /// Combined path evidence.
    pub path: Digest32,
}

/// Per-key conflict information (prototype `lightKeyInfo` plus `HashLog`).
#[derive(Clone, Debug, Default)]
struct KeyState {
    last: Option<CommandId>,
    log: PathLog,
}

/// The command table of one replica in one domain.
#[derive(Clone, Debug, Default)]
pub struct CommandTable {
    records: BTreeMap<CommandId, CommandRecord>,
    keys: BTreeMap<Vec<u8>, KeyState>,
    capacity: Option<usize>,
}

impl CommandTable {
    /// Unbounded table.
    pub const fn new() -> Self {
        CommandTable {
            records: BTreeMap::new(),
            keys: BTreeMap::new(),
            capacity: None,
        }
    }

    /// A table admitting at most `capacity` records (placeholders included).
    pub const fn with_capacity(capacity: usize) -> Self {
        CommandTable {
            records: BTreeMap::new(),
            keys: BTreeMap::new(),
            capacity: Some(capacity),
        }
    }

    /// Rebuild a table from complete authoritative dependency rows
    /// (design Section 4.7: derived indexes come from the records, never
    /// from receipt order). The conflict index names, per key, the record
    /// no other record of that key depends on; path logs resume at that
    /// record's digest.
    pub fn restore(
        capacity: Option<usize>,
        records: impl IntoIterator<Item = (CommandId, CommandRecord)>,
    ) -> Self {
        let mut table = CommandTable {
            records: BTreeMap::new(),
            keys: BTreeMap::new(),
            capacity,
        };
        for (c, r) in records {
            table.records.insert(c, r);
        }
        let mut per_key: BTreeMap<Vec<u8>, Vec<CommandId>> = BTreeMap::new();
        for (c, r) in &table.records {
            if r.payload.is_none() {
                continue;
            }
            for key in &r.keys {
                per_key.entry(key.clone()).or_default().push(*c);
            }
        }
        for (key, members) in per_key {
            let depended: alloc::collections::BTreeSet<CommandId> = members
                .iter()
                .flat_map(|c| table.records[c].deps.iter().copied())
                .collect();
            let last = members
                .iter()
                .copied()
                .filter(|c| !depended.contains(c))
                .max();
            let mut state = KeyState::default();
            if let Some(last) = last {
                let digest = table.records[&last]
                    .paths
                    .iter()
                    .find(|(k, _)| *k == key)
                    .map(|(_, d)| *d)
                    .unwrap_or_else(crate::graph::empty_path);
                state.last = Some(last);
                state.log = PathLog::resumed(digest);
            }
            table.keys.insert(key, state);
        }
        table
    }

    /// Every initialized record.
    pub fn records(&self) -> impl Iterator<Item = (&CommandId, &CommandRecord)> {
        self.records.iter().filter(|(_, r)| r.payload.is_some())
    }

    fn full(&self) -> bool {
        self.capacity.is_some_and(|c| self.records.len() >= c)
    }

    /// Create a placeholder for a command known by identity only (leader
    /// evidence arrived before the payload). Idempotent; never changes an
    /// initialized record. Refused under backpressure.
    pub fn expect(&mut self, command: CommandId) -> Result<(), InitError> {
        if self.records.contains_key(&command) {
            return Ok(());
        }
        if self.full() {
            return Err(InitError::Backpressure);
        }
        self.records.insert(
            command,
            CommandRecord {
                phase: Phase::Start,
                deps: Vec::new(),
                keys: Vec::new(),
                payload: None,
                paths: Vec::new(),
                path: crate::graph::empty_path(),
            },
        );
        Ok(())
    }

    /// Bind the payload, compute the local dependencies and path digests
    /// from the conflict index and publish the command in the index,
    /// atomically. A repeated initialization with the same payload is
    /// `AlreadyInitialized` and changes nothing.
    pub fn initialize(
        &mut self,
        command: CommandId,
        payload: Digest32,
        keys: Vec<Vec<u8>>,
    ) -> Result<Initialized, InitError> {
        match self.records.get(&command) {
            Some(existing) if existing.payload.is_some() => {
                return Err(if existing.payload == Some(payload) {
                    InitError::AlreadyInitialized
                } else {
                    InitError::PayloadConflict
                });
            }
            Some(_) => {}
            None if self.full() => return Err(InitError::Backpressure),
            None => {}
        }
        let mut deps = Vec::new();
        let mut paths = Vec::with_capacity(keys.len());
        for key in &keys {
            let state = self.keys.entry(key.clone()).or_default();
            if let Some(last) = state.last
                && last != command
                && !deps.contains(&last)
            {
                deps.push(last);
            }
            state.last = Some(command);
            let digest = state.log.append(command);
            paths.push((key.clone(), digest));
        }
        let path = combined_path(&paths);
        self.records.insert(
            command,
            CommandRecord {
                phase: Phase::PreAccept,
                deps: deps.clone(),
                keys,
                payload: Some(payload),
                paths: paths.clone(),
                path,
            },
        );
        Ok(Initialized { deps, paths, path })
    }

    /// The leader ordered `command` at `seqnum` with these per-key path
    /// digests: align this replica's logs so later commands' paths follow
    /// the leader's order (prototype `recordLeaderHash`/`updateLogs`).
    pub fn record_leader_path(
        &mut self,
        command: CommandId,
        seqnum: u64,
        paths: &[(Vec<u8>, Digest32)],
    ) {
        for (key, digest) in paths {
            self.keys
                .entry(key.clone())
                .or_default()
                .log
                .sync(command, seqnum, *digest);
        }
    }

    /// Current path head of a key (evidence the next command would carry).
    pub fn path_head(&self, key: &[u8]) -> Digest32 {
        self.keys
            .get(key)
            .map_or_else(crate::graph::empty_path, |k| k.log.head())
    }

    /// The record, placeholder included.
    pub fn record(&self, command: &CommandId) -> Option<&CommandRecord> {
        self.records.get(command)
    }

    /// Phase of an initialized command; a placeholder reports none.
    pub fn phase_of(&self, command: &CommandId) -> Option<Phase> {
        self.records
            .get(command)
            .filter(|r| r.payload.is_some())
            .map(|r| r.phase)
    }

    /// Conflicting commands for `keys` as the index sees them now.
    pub fn conflicts(&self, keys: &[Vec<u8>]) -> Vec<CommandId> {
        let mut out = Vec::new();
        for key in keys {
            if let Some(last) = self.keys.get(key).and_then(|k| k.last)
                && !out.contains(&last)
            {
                out.push(last);
            }
        }
        out
    }

    /// Adopt the leader's order for an initialized command (ACCEPT), under
    /// the source guard.
    pub fn accept(
        &mut self,
        command: CommandId,
        deps: Vec<CommandId>,
    ) -> Result<(), GuardViolation> {
        guard_accept(&deps, |c| self.phase_of(c))?;
        let record = self.initialized_mut(&command)?;
        record.deps = deps;
        record.phase = Phase::Accept;
        Ok(())
    }

    /// Mark a learned command (COMMIT), under the source guard.
    pub fn commit(&mut self, command: CommandId) -> Result<(), GuardViolation> {
        let deps = self.initialized(&command)?.deps.clone();
        guard_commit(&deps, |c| self.phase_of(c))?;
        self.initialized_mut(&command)?.phase = Phase::Commit;
        Ok(())
    }

    /// Mark an executed command, under the source guard.
    pub fn execute(&mut self, command: CommandId) -> Result<(), GuardViolation> {
        let deps = self.initialized(&command)?.deps.clone();
        guard_execute(&deps, |c| self.phase_of(c))?;
        self.initialized_mut(&command)?.phase = Phase::Executed;
        Ok(())
    }

    /// Forget an executed command. Unresolved acceptance is never deleted
    /// for capacity; the conflict index keeps the identity so later
    /// commands still depend on it.
    pub fn retire(&mut self, command: &CommandId) -> Result<(), RetireError> {
        match self.records.get(command) {
            None => Err(RetireError::Unknown),
            Some(r) if r.phase != Phase::Executed => Err(RetireError::NotExecuted(r.phase)),
            Some(_) => {
                self.records.remove(command);
                Ok(())
            }
        }
    }

    /// Start an exact closure traversal from an initialized command.
    pub fn closure_start(&self, root: CommandId) -> Result<ClosureCursor, GuardViolation> {
        let record = self.initialized(&root)?;
        Ok(ClosureCursor::start(root, &record.deps))
    }

    /// Advance a traversal by at most `budget` visits; a placeholder or
    /// unknown dependency stops it as `DependencyUnknown`.
    pub fn closure_step(
        &self,
        cursor: ClosureCursor,
        budget: usize,
    ) -> Result<ClosureProgress, GuardViolation> {
        cursor
            .step(budget, |c| {
                self.records
                    .get(c)
                    .filter(|r| r.payload.is_some())
                    .map(|r| r.deps.clone())
            })
            .map_err(|dep| GuardViolation::DependencyUnknown { dep })
    }

    fn initialized(&self, command: &CommandId) -> Result<&CommandRecord, GuardViolation> {
        self.records
            .get(command)
            .filter(|r| r.payload.is_some())
            .ok_or(GuardViolation::DependencyUnknown { dep: *command })
    }

    fn initialized_mut(
        &mut self,
        command: &CommandId,
    ) -> Result<&mut CommandRecord, GuardViolation> {
        self.records
            .get_mut(command)
            .filter(|r| r.payload.is_some())
            .ok_or(GuardViolation::DependencyUnknown { dep: *command })
    }

    /// Number of records (placeholders included).
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the table is empty.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}
