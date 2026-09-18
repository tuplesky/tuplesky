//! Command descriptors with atomic initialization (design Section 4.7;
//! prototype `getCmdDescSeq`, `getDepAndHashes`, `keyInfo`).
//!
//! Installing a command's initialized state (payload binding, phase,
//! dependencies) and exposing it through the conflict index is one
//! transition of [`CommandTable::initialize`]. A descriptor created by
//! early leader evidence ([`CommandTable::expect`]) is a placeholder: it
//! is not in the conflict index and reports no phase, so a conflicting
//! command initialized meanwhile cannot see it as processed state and the
//! dependency-phase guards treat it as unknown.

use alloc::collections::BTreeMap;
use alloc::vec::Vec;

use coord_types::CommandId;
use coord_types::identity::Digest32;
use serde::{Deserialize, Serialize};

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
}

/// Why initialization did not happen.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InitError {
    /// The command is already initialized (a duplicate or reordered
    /// message); nothing changed.
    AlreadyInitialized,
    /// A different payload was presented for an initialized command.
    PayloadConflict,
}

/// Per-key conflict information (prototype `lightKeyInfo`): the last
/// command touching the key.
#[derive(Clone, Debug, Default)]
struct KeyInfo {
    last: Option<CommandId>,
}

/// The command table of one replica in one domain.
#[derive(Clone, Debug, Default)]
pub struct CommandTable {
    records: BTreeMap<CommandId, CommandRecord>,
    keys: BTreeMap<Vec<u8>, KeyInfo>,
}

impl CommandTable {
    /// Empty table.
    pub const fn new() -> Self {
        CommandTable {
            records: BTreeMap::new(),
            keys: BTreeMap::new(),
        }
    }

    /// Create a placeholder for a command known by identity only (leader
    /// evidence arrived before the payload). Idempotent; never changes an
    /// initialized record.
    pub fn expect(&mut self, command: CommandId) {
        self.records.entry(command).or_insert(CommandRecord {
            phase: Phase::Start,
            deps: Vec::new(),
            keys: Vec::new(),
            payload: None,
        });
    }

    /// Bind the payload, compute the local dependencies from the conflict
    /// index and publish the command in the index, atomically. Returns the
    /// dependencies. A repeated initialization with the same payload is
    /// `AlreadyInitialized` and changes nothing.
    pub fn initialize(
        &mut self,
        command: CommandId,
        payload: Digest32,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<CommandId>, InitError> {
        if let Some(existing) = self.records.get(&command)
            && existing.payload.is_some()
        {
            return Err(if existing.payload == Some(payload) {
                InitError::AlreadyInitialized
            } else {
                InitError::PayloadConflict
            });
        }
        let mut deps = Vec::new();
        for key in &keys {
            if let Some(last) = self.keys.get(key).and_then(|k| k.last)
                && last != command
                && !deps.contains(&last)
            {
                deps.push(last);
            }
        }
        for key in &keys {
            self.keys.entry(key.clone()).or_default().last = Some(command);
        }
        self.records.insert(
            command,
            CommandRecord {
                phase: Phase::PreAccept,
                deps: deps.clone(),
                keys,
                payload: Some(payload),
            },
        );
        Ok(deps)
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
