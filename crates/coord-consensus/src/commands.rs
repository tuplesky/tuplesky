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
//!
//! "Only executed records can be retired" is a rule about what may be
//! forgotten, not a reason to forget nothing. A table that never retired
//! anything would make its capacity a bound on how many commands a
//! replica may execute *in its lifetime* rather than on how much
//! unresolved work it holds: the sixty-fifth command of a
//! sixty-four-record table would be refused for ever, on an otherwise
//! idle replica. So [`CommandTable::reclaim`] runs when the table is
//! full and before anything is refused, and backpressure afterwards
//! means what it says -- this much work really is outstanding.

use alloc::collections::{BTreeMap, BTreeSet, VecDeque};
use alloc::vec::Vec;

use coord_types::CommandId;
use coord_types::identity::Digest32;
use serde::{Deserialize, Serialize};

use crate::graph::{ClosureCursor, ClosureProgress, PathLog, combined_path};
use crate::phase::{GuardViolation, Phase, guard_accept, guard_commit, guard_execute};

/// A command as this replica knows it.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CommandRecord {
    /// Phase.
    pub phase: Phase,
    /// Direct dependencies (local order until the leader's is adopted).
    pub deps: Vec<CommandId>,
    /// Conflict keys of the payload.
    pub keys: Vec<Vec<u8>>,
    /// Digest of what was bound beside the command identity: the
    /// admission this replica accepted the command under
    /// ([`coord_core::capability::admission_digest`]). `None` for a
    /// placeholder, which has bound nothing yet.
    ///
    /// The identity is already the record's key, so repeating it here
    /// would bind nothing. What is not in the identity, and must be, is
    /// the admission: a second presentation of the same command under
    /// different attested facts is a [`InitError::PayloadConflict`]
    /// rather than a silent replacement, and the digest travels in this
    /// replica's acknowledgements so no quorum can form across replicas
    /// that accepted different facts.
    pub payload: Option<Digest32>,
    /// Per-key path digests through this command: the local ones at
    /// initialization, replaced by the leader's once its order is
    /// recorded, so a record restored after a crash resumes its logs at
    /// the synchronized anchor rather than a stale local digest.
    pub paths: Vec<(Vec<u8>, Digest32)>,
    /// Leader sequence number the paths were synchronized at, if the
    /// leader's order has been recorded for this command.
    pub synced_seq: Option<u64>,
    /// Combined path evidence (what a fast acknowledgement carries).
    pub path: Digest32,
}

impl CommandRecord {
    /// This record, demoted by a Sync that does not carry it
    /// ([`CommandTable::demote`]): PRE-ACCEPT, with no path evidence.
    pub(crate) fn demote(&mut self) {
        self.phase = Phase::PreAccept;
        self.path = crate::graph::demoted_path();
    }
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
    /// The payload digest that was bound (what this replica's
    /// acknowledgements carry).
    pub payload: Digest32,
}

/// Per-key conflict information (prototype `lightKeyInfo` plus `HashLog`).
#[derive(Clone, Debug, Default)]
struct KeyState {
    last: Option<CommandId>,
    /// Other commands the next command on the key depends on besides
    /// `last`: the other tails a new leader anchored (task-d12).
    also: Vec<CommandId>,
    log: PathLog,
}

/// The command table of one replica in one domain.
#[derive(Clone, Debug, Default)]
pub struct CommandTable {
    records: BTreeMap<CommandId, CommandRecord>,
    keys: BTreeMap<Vec<u8>, KeyState>,
    /// Commands retired after executing: what this replica still
    /// remembers having executed, for the dependency guards.
    executed: BTreeSet<CommandId>,
    /// The same commands in the order they were retired, so the oldest
    /// is the one dropped when the memory reaches its bound.
    retired: VecDeque<CommandId>,
    /// Every command this replica executed and retired, however long
    /// ago: the executed answer (task-d05).
    ///
    /// The tombstones above are bounded by recency, and what they bound is
    /// what this replica still keeps *about* a command (the evidence it
    /// can replay, for one). Whether it executed the command at all is a
    /// different question, and one recovery asks of commands far older
    /// than any recency bound: a Sync names commands by identity, and a
    /// replica that answered "unknown" for one it executed long ago
    /// treated it as work still to do -- a placeholder, a payload to
    /// fetch, a table filling with history. So that answer is kept for
    /// every command, at the cost of one identity each.
    history: BTreeSet<CommandId>,
    /// The last `capacity` commands retired, in the order they were: the
    /// window a recovery report still names (task-d05).
    ///
    /// Not the tombstones. Those also keep every key's latest command for
    /// the guards, however old, so with commands on ever new keys they
    /// grow with the keys; a report bounded by them grew with them.
    recent: VecDeque<CommandId>,
    /// The same commands, for lookup.
    recent_set: BTreeSet<CommandId>,
    capacity: Option<usize>,
    /// The command this replica executed last. With every command on one
    /// key, it is the tail of the order everything executed so far
    /// follows, which a new leader chains its first proposals after.
    last_executed: Option<CommandId>,
}

impl CommandTable {
    /// Unbounded table.
    pub const fn new() -> Self {
        CommandTable {
            records: BTreeMap::new(),
            keys: BTreeMap::new(),
            executed: BTreeSet::new(),
            retired: VecDeque::new(),
            history: BTreeSet::new(),
            recent: VecDeque::new(),
            recent_set: BTreeSet::new(),
            capacity: None,
            last_executed: None,
        }
    }

    /// A table admitting at most `capacity` records (placeholders included).
    pub const fn with_capacity(capacity: usize) -> Self {
        CommandTable {
            records: BTreeMap::new(),
            keys: BTreeMap::new(),
            executed: BTreeSet::new(),
            retired: VecDeque::new(),
            history: BTreeSet::new(),
            recent: VecDeque::new(),
            recent_set: BTreeSet::new(),
            capacity: Some(capacity),
            last_executed: None,
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
            executed: BTreeSet::new(),
            retired: VecDeque::new(),
            history: BTreeSet::new(),
            recent: VecDeque::new(),
            recent_set: BTreeSet::new(),
            capacity,
            last_executed: None,
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

    /// Retire every executed record, and say how many went.
    ///
    /// This is the only thing that ever makes room. Nothing unresolved is
    /// touched: a record in START, PRE-ACCEPT, ACCEPT or COMMIT is work
    /// this replica still owes, and evicting one to serve a newer command
    /// would lose an obligation rather than shed load.
    ///
    /// A live record that depended on a retired one keeps seeing it as
    /// executed through the tombstone [`CommandTable::retire`] leaves, and
    /// the tombstone goes with the last record that referenced it, so the
    /// bookkeeping stays bounded by the live set rather than by history.
    pub fn reclaim(&mut self) -> usize {
        let executed: Vec<CommandId> = self
            .records
            .iter()
            .filter(|(_, r)| r.phase == Phase::Executed)
            .map(|(c, _)| *c)
            .collect();
        let mut retired = 0;
        for command in executed {
            if self.retire(&command).is_ok() {
                retired += 1;
            }
        }
        retired
    }

    /// Whether the table is full after reclaiming what it may.
    fn full_after_reclaim(&mut self) -> bool {
        if !self.full() {
            return false;
        }
        self.reclaim();
        self.full()
    }

    /// Drop the placeholder of `command`, if that is all the table holds
    /// of it (task-d20): a record with a payload is never touched.
    pub fn forget_placeholder(&mut self, command: &CommandId) -> bool {
        match self.records.get(command) {
            Some(r) if r.payload.is_none() && r.phase == Phase::Start => {
                self.records.remove(command);
                true
            }
            _ => false,
        }
    }

    /// Create a placeholder for a command known by identity only (leader
    /// evidence arrived before the payload). Idempotent; never changes an
    /// initialized record. Refused under backpressure.
    pub fn expect(&mut self, command: CommandId) -> Result<(), InitError> {
        if self.records.contains_key(&command) {
            return Ok(());
        }
        if self.full_after_reclaim() {
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
                synced_seq: None,
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
        self.initialize_bounded(command, payload, keys, true)
    }

    /// [`CommandTable::initialize`] past the table's capacity (task-d09).
    ///
    /// For a command whose turn has come and that is already decided
    /// elsewhere: a full table of later commands, none of which can be
    /// adopted before it, would otherwise hold it out for ever. The
    /// caller vouches for that; the table cannot tell.
    pub fn initialize_beyond_capacity(
        &mut self,
        command: CommandId,
        payload: Digest32,
        keys: Vec<Vec<u8>>,
    ) -> Result<Initialized, InitError> {
        self.initialize_bounded(command, payload, keys, false)
    }

    fn initialize_bounded(
        &mut self,
        command: CommandId,
        payload: Digest32,
        keys: Vec<Vec<u8>>,
        bounded: bool,
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
            None => {
                if bounded && self.full_after_reclaim() {
                    return Err(InitError::Backpressure);
                }
            }
        }
        let mut deps = Vec::new();
        let mut paths = Vec::with_capacity(keys.len());
        for key in &keys {
            let state = self.keys.entry(key.clone()).or_default();
            for tail in state.last.into_iter().chain(state.also.drain(..)) {
                if tail != command && !deps.contains(&tail) {
                    deps.push(tail);
                }
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
                synced_seq: None,
                path,
            },
        );
        Ok(Initialized {
            deps,
            paths,
            path,
            payload,
        })
    }

    /// Bind `payload` in place of the admission a command was initialized
    /// under, while nothing has been accepted over it (task-d09).
    ///
    /// For a follower that took its own submission under other attested
    /// facts than the ones the leader proposed: every presentation of a
    /// command mints its own admission receipt, so a submitter that
    /// presents again after a lost link reaches the voters under facts the
    /// leader never saw. At PRE-ACCEPT the record holds only this
    /// replica's local order, which adoption replaces, and its fast
    /// acknowledgement under the other facts is one no quorum counts:
    /// every learning predicate needs the leader's proposal, and a vote
    /// set counts nothing under other facts than the ones it bound.
    /// At ACCEPT the record's facts are what it acknowledged, but an
    /// acknowledgement is not a decision: a selection that names other
    /// facts for the command rebinds it too, since a decision has one
    /// digest and the selection names it (task-d14). A committed or
    /// executed record is never rebound. Returns whether the record was
    /// rebound.
    pub fn rebind(&mut self, command: &CommandId, payload: Digest32) -> bool {
        match self.records.get_mut(command) {
            Some(record) if record.payload.is_some() && record.phase <= Phase::Accept => {
                record.payload = Some(payload);
                true
            }
            _ => false,
        }
    }

    /// Take back an acceptance of an earlier ballot that a Sync did not
    /// carry: the record returns to PRE-ACCEPT, keeping its payload and
    /// dependencies, which at PRE-ACCEPT decide nothing and which adoption
    /// replaces (task-d11). Only an ACCEPT is demoted; a commit is a
    /// decision and stays. Returns whether the record was demoted.
    ///
    /// The path goes too ([`crate::graph::demoted_path`]): it was evidence
    /// about the earlier ballot, and a report labels the record with the
    /// synchronized one, so kept it would let the next selection's
    /// fast-path analysis take the old order as the new ballot's.
    pub fn demote(&mut self, command: &CommandId) -> bool {
        match self.records.get_mut(command) {
            Some(record) if record.payload.is_some() && record.phase == Phase::Accept => {
                record.demote();
                true
            }
            _ => false,
        }
    }

    /// The path log of `key`, if any command touched it.
    pub fn log(&self, key: &[u8]) -> Option<&PathLog> {
        self.keys.get(key).map(|k| &k.log)
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
        // Record the synchronized anchors so the durable row written when
        // the order is adopted carries them: a restored log then resumes
        // where the live one stands.
        if let Some(record) = self.records.get_mut(&command)
            && record.synced_seq.is_none_or(|s| seqnum >= s)
        {
            record.synced_seq = Some(seqnum);
            for (key, digest) in paths {
                match record.paths.iter_mut().find(|(k, _)| k == key) {
                    Some(entry) => entry.1 = *digest,
                    None => record.paths.push((key.clone(), *digest)),
                }
            }
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

    /// Phase of an initialized command; a placeholder reports none. A
    /// command this replica executed and retired reports `Executed`,
    /// however long ago it was retired.
    pub fn phase_of(&self, command: &CommandId) -> Option<Phase> {
        match self.records.get(command) {
            Some(r) => r.payload.is_some().then_some(r.phase),
            None => self.history.contains(command).then_some(Phase::Executed),
        }
    }

    /// Conflicting commands for `keys` as the index sees them now.
    pub fn conflicts(&self, keys: &[Vec<u8>]) -> Vec<CommandId> {
        let mut out = Vec::new();
        for key in keys {
            let Some(state) = self.keys.get(key) else {
                continue;
            };
            for tail in state.last.iter().chain(&state.also) {
                if !out.contains(tail) {
                    out.push(*tail);
                }
            }
        }
        out
    }

    /// Adopt the leader's order for an initialized command (ACCEPT), under
    /// the source guard.
    ///
    /// Phases only advance: a delayed or duplicate ACCEPT for a command
    /// already committed or executed is idempotent and changes neither
    /// the phase nor the dependencies the later phase was reached with.
    pub fn accept(
        &mut self,
        command: CommandId,
        deps: Vec<CommandId>,
    ) -> Result<(), GuardViolation> {
        if self.phase_of(&command).is_some_and(|p| p > Phase::Accept) {
            return Ok(());
        }
        guard_accept(&deps, |c| self.phase_of(c))?;
        let record = self.initialized_mut(&command)?;
        record.deps = deps;
        record.phase = Phase::Accept;
        Ok(())
    }

    /// Make `command` the latest command on `key`: the next command
    /// initialized on the key depends on it.
    ///
    /// The latest command on a key is the tail of the order, the command
    /// no other command of the key depends on ([`CommandTable::restore`]
    /// rebuilds it that way). `initialize` moves it on every payload,
    /// which on a leader is its own proposal order and on a follower is
    /// arrival order. A follower that wins an election re-proposes the
    /// recovered order without initializing anything, so its latest is
    /// still whatever payload reached it last, possibly a command in the
    /// middle of that order. The new leader anchors the recovered tail
    /// here, before its first fresh proposal (task-d06).
    ///
    /// The path log is re-anchored with it ([`PathLog::anchored`]). Left
    /// as it was, it digested this replica's own appends, which need not
    /// pass through the tails: the next command's path then named another
    /// history than its dependencies, and a follower whose appends were
    /// the same reached that path over a record the leader's order had
    /// replaced (task-d34, F7: protocol_sim row 10, three voters, seed 52).
    pub fn anchor(&mut self, key: &[u8], command: CommandId) {
        self.anchor_all(key, &[command]);
    }

    /// [`CommandTable::anchor`] at several commands: the next command
    /// initialized on the key depends on all of them (task-d12). For a new
    /// leader that cannot tell which of its committed tails is the last:
    /// depending on every one of them follows whichever it is.
    pub fn anchor_all(&mut self, key: &[u8], tails: &[CommandId]) {
        let state = self.keys.entry(key.to_vec()).or_default();
        state.last = tails.first().copied();
        state.also = tails.iter().skip(1).copied().collect();
        state.log = PathLog::anchored(tails);
    }

    /// Adopt the leader's order and path evidence for a command
    /// ([`CommandTable::accept`] plus the evidence the proposal or Sync
    /// entry carried): from then on the record reports the leader's path,
    /// which a later recovery compares fast-set evidence against.
    pub fn adopt(
        &mut self,
        command: CommandId,
        deps: Vec<CommandId>,
        paths: Option<&[(Vec<u8>, Digest32)]>,
        path: Digest32,
    ) -> Result<(), GuardViolation> {
        self.accept(command, deps)?;
        let record = self.initialized_mut(&command)?;
        if let Some(paths) = paths {
            record.paths = paths.to_vec();
        }
        record.path = path;
        Ok(())
    }

    /// Mark a learned command (COMMIT), under the source guard. Idempotent
    /// for a command already committed or executed.
    pub fn commit(&mut self, command: CommandId) -> Result<(), GuardViolation> {
        let record = self.initialized(&command)?;
        if record.phase >= Phase::Commit {
            return Ok(());
        }
        let deps = record.deps.clone();
        guard_commit(&deps, |c| self.phase_of(c))?;
        self.initialized_mut(&command)?.phase = Phase::Commit;
        Ok(())
    }

    /// Mark an executed command, under the source guard. Idempotent for a
    /// command already executed.
    pub fn execute(&mut self, command: CommandId) -> Result<(), GuardViolation> {
        let record = self.initialized(&command)?;
        if record.phase >= Phase::Executed {
            return Ok(());
        }
        let deps = record.deps.clone();
        guard_execute(&deps, |c| self.phase_of(c))?;
        self.initialized_mut(&command)?.phase = Phase::Executed;
        self.last_executed = Some(command);
        Ok(())
    }

    /// The command this replica executed last, if it remembers one.
    pub const fn last_executed(&self) -> Option<CommandId> {
        self.last_executed
    }

    /// The last commands on `key` this replica has committed and not yet
    /// executed: the committed records no other committed record of the
    /// key depends on (task-d12).
    ///
    /// Execution follows the dependencies, so every such command comes
    /// after [`CommandTable::last_executed`] in the key's order, and a
    /// command that must follow everything this replica holds decided
    /// follows these. There is one unless the replica lacks a command
    /// between two committed ones: then both look last, and nothing here
    /// says which one is, so a command that follows every one of them is
    /// the only safe successor.
    pub fn committed_tails(&self, key: &[u8]) -> Vec<CommandId> {
        let committed: Vec<(&CommandId, &CommandRecord)> = self
            .records
            .iter()
            .filter(|(_, r)| r.phase == Phase::Commit && r.keys.iter().any(|k| k == key))
            .collect();
        let depended: BTreeSet<CommandId> = committed
            .iter()
            .flat_map(|(_, r)| r.deps.iter().copied())
            .collect();
        committed
            .into_iter()
            .map(|(c, _)| *c)
            .filter(|c| !depended.contains(c))
            .collect()
    }

    /// Mark a command executed from durable evidence (its executed identity
    /// row) without consulting the guards: the materializer already applied
    /// it at its position.
    ///
    /// A command with no record here is one whose rows are gone (a trimmed
    /// prefix): it is remembered as executed, which is all there is left
    /// to say about it.
    pub fn restore_executed(&mut self, command: &CommandId) {
        self.last_executed = Some(*command);
        match self.records.get_mut(command) {
            Some(r) if r.payload.is_some() => r.phase = Phase::Executed,
            Some(_) => {}
            None => {
                self.history.insert(*command);
            }
        }
    }

    /// Forget an executed command. Unresolved acceptance is never deleted
    /// for capacity. What the table keeps is a tombstone saying that it
    /// executed, so the guards can still answer for it.
    ///
    /// The conflict index keeps naming it where it is a key's latest
    /// command. It used to stop: an executed command's effects are
    /// complete, so it seemed to need no successor ordered after it. That
    /// holds on the replica that executed it and nowhere else. The table
    /// reclaims exactly when it is full, before it computes the next
    /// command's dependencies, so the first command a full leader proposed
    /// named no dependency at all. A follower still behind it -- a payload
    /// missing, its own table full -- found that command ready, with
    /// nothing ordering it after the backlog, and executed it first: the
    /// same committed set in two orders, and a frontend on that follower
    /// answering from a state the leader never had. Named, the retired
    /// command costs a replica that already executed it nothing (the
    /// tombstone answers EXECUTED) and costs a lagging one the wait until
    /// it has executed it, which is the order every replica must keep.
    ///
    /// The tombstone is unconditional, and that is the point. A
    /// dependency is named by whoever holds the evidence, and not all of
    /// that evidence is in this table: a follower holds the leader's
    /// proposals until their payloads arrive, and the dependency such a
    /// proposal names lives in the proposal, not in any record here. A
    /// table that tombstoned only what a live record already depends on
    /// would drop exactly the command the next proposal is about to name,
    /// and the guard would then read "unknown" for a command this replica
    /// executed itself -- a proposal that can never be adopted, with
    /// every later command waiting behind it for ever.
    ///
    /// What bounds the memory is recency, not reference counting: the
    /// oldest tombstone goes once there are more of them than the table
    /// has room for records -- except one that is still some key's latest
    /// command, which the next proposal on that key will name, and which
    /// the guards must go on answering for. There is at most one of those
    /// per key. A leader names as a dependency only a command live in its
    /// own table or its key's latest, so a replica that remembers its last
    /// `capacity` retirements and every key's latest remembers every
    /// command a leader can still name. An unbounded table keeps them all,
    /// which is what unbounded means.
    pub fn retire(&mut self, command: &CommandId) -> Result<(), RetireError> {
        let keys = match self.records.get(command) {
            None => return Err(RetireError::Unknown),
            Some(r) if r.phase != Phase::Executed => {
                return Err(RetireError::NotExecuted(r.phase));
            }
            Some(r) => r.keys.clone(),
        };
        self.records.remove(command);
        for key in &keys {
            if let Some(state) = self.keys.get_mut(key) {
                state.log.forget(command);
            }
        }
        self.history.insert(*command);
        if self.executed.insert(*command) {
            self.retired.push_back(*command);
        }
        if let Some(bound) = self.capacity
            && self.recent_set.insert(*command)
        {
            self.recent.push_back(*command);
            while self.recent.len() > bound {
                if let Some(oldest) = self.recent.pop_front() {
                    self.recent_set.remove(&oldest);
                }
            }
        }
        if let Some(bound) = self.capacity {
            let mut latest = Vec::new();
            while self.retired.len() > bound {
                let Some(oldest) = self.retired.pop_front() else {
                    break;
                };
                if self
                    .keys
                    .values()
                    .any(|k| k.last == Some(oldest) || k.also.contains(&oldest))
                {
                    latest.push(oldest);
                } else {
                    self.executed.remove(&oldest);
                }
            }
            // Kept, and kept in the queue, so each goes in its turn once a
            // newer command has taken its key.
            for command in latest.into_iter().rev() {
                self.retired.push_front(command);
            }
        }
        Ok(())
    }

    /// Commands this replica executed and retired, and still remembers
    /// having executed.
    pub fn tombstones(&self) -> &BTreeSet<CommandId> {
        &self.executed
    }

    /// Whether this replica executed `command` and retired it longer ago
    /// than its last `capacity` retirements (task-d05).
    ///
    /// Such a command is history. Every voter that reports it has
    /// executed it, and a recovery report, a Sync and the payloads a
    /// replica serves leave it out; a voter that has not executed it by
    /// then is further behind than recovery carries anyone, and catches
    /// up another way. It may still be a key's latest command, kept as a
    /// tombstone for the guards: that answers for it when a proposal
    /// names it, and needs nothing of the report. An unbounded table
    /// keeps every tombstone, and forgets only what it never held a
    /// record of.
    pub fn forgotten(&self, command: &CommandId) -> bool {
        self.history.contains(command)
            && match self.capacity {
                Some(_) => !self.recent_set.contains(command),
                None => !self.executed.contains(command),
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
            .step(budget, |c| match self.records.get(c) {
                Some(r) => r.payload.is_some().then(|| r.deps.clone()),
                // A retired executed dependency: complete, nothing beyond it
                // is still needed for ordering. Any executed command, not
                // only one still in the tombstone window: a restart retires
                // everything it executed at once, and a record still live
                // after it can reach further back than the window
                // (task-d05).
                None => self.history.contains(c).then(Vec::new),
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

    /// Whether this table holds `command`'s payload, not merely its
    /// identity.
    ///
    /// The difference matters wherever a record is about to be changed.
    /// A placeholder says "a command with this identity exists and I
    /// have been told about it"; only an initialized record has the
    /// dependencies, the keys and the admission that make it a command
    /// this replica can accept an order for or execute.
    pub fn is_initialized(&self, command: &CommandId) -> bool {
        self.records
            .get(command)
            .is_some_and(|r| r.payload.is_some())
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
