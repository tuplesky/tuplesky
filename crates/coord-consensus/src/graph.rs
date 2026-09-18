//! Dependency paths and closure traversal (task-21; design Sections
//! 4.1-4.3, 4.7, 18.2; prototype `swift/dpath.go` `HashLog`).
//!
//! A per-key path log is a hash chain over the commands that touched the
//! key in the order this replica processed them, anchored at the prefix
//! the leader has ordered (`sync`). Its head is the dependency-path
//! evidence a fast acknowledgement carries: two replicas with equal heads
//! saw the same ordered prefix, which is strictly stronger than equal
//! direct dependency sets. Nothing is compressed: the head is recomputed
//! over the whole pending suffix after every synchronization.
//!
//! Closure traversal is exact (every transitive dependency of an
//! initialized command) and incremental: a cursor advances by a bounded
//! number of visits per step so a turn never spends unbounded work, and a
//! placeholder or unknown dependency stops it with the identity that is
//! missing.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::vec::Vec;

use coord_types::CommandId;
use coord_types::identity::{Digest32, HashDomain};
use serde::{Deserialize, Serialize};

/// Chain one command onto a path digest.
pub fn chain(previous: &Digest32, command: &CommandId) -> Digest32 {
    HashDomain::DependencyPath.digest(&[&previous.0, command.as_bytes()])
}

/// The digest of an empty path.
pub fn empty_path() -> Digest32 {
    HashDomain::DependencyPath.digest(&[])
}

/// One key's conflict log (prototype `HashLog`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathLog {
    /// Leader sequence number of the highest synchronized command.
    synced_seq: Option<u64>,
    /// Path digest through the synchronized prefix.
    synced_hash: Digest32,
    /// Commands after the synchronized prefix, in local order.
    pending: Vec<CommandId>,
    /// Digest of the whole log.
    head: Digest32,
    /// Leader synchronizations that arrived before the command was
    /// appended locally.
    early: BTreeMap<CommandId, (u64, Digest32)>,
}

impl Default for PathLog {
    fn default() -> Self {
        PathLog::new()
    }
}

impl PathLog {
    /// An empty log.
    pub fn new() -> Self {
        let empty = empty_path();
        PathLog {
            synced_seq: None,
            synced_hash: empty,
            pending: Vec::new(),
            head: empty,
            early: BTreeMap::new(),
        }
    }

    /// Digest of the whole log: the path evidence for the next command.
    pub const fn head(&self) -> Digest32 {
        self.head
    }

    /// Commands after the synchronized prefix, in local order.
    pub fn pending(&self) -> &[CommandId] {
        &self.pending
    }

    /// Highest synchronized leader sequence number.
    pub const fn synced_seq(&self) -> Option<u64> {
        self.synced_seq
    }

    fn recompute(&mut self) {
        let mut head = self.synced_hash;
        for c in &self.pending {
            head = chain(&head, c);
        }
        self.head = head;
    }

    /// Append a command in local order and return the path digest through
    /// it. A leader synchronization that arrived early is applied instead
    /// (prototype `Append` with `pendingUpd`).
    pub fn append(&mut self, command: CommandId) -> Digest32 {
        if let Some((seq, hash)) = self.early.remove(&command) {
            self.sync_known(command, seq, hash);
            return hash;
        }
        self.pending.push(command);
        self.head = chain(&self.head, &command);
        self.head
    }

    /// The leader ordered `command` at `seq` with path digest `hash`
    /// through it: the command leaves the pending suffix and, when `seq` is
    /// the highest seen, becomes the synchronized prefix; the head is
    /// recomputed over the remaining suffix (prototype `Update`).
    pub fn sync(&mut self, command: CommandId, seq: u64, hash: Digest32) {
        if !self.pending.contains(&command) {
            // Not appended yet: remember it for the append.
            self.early.insert(command, (seq, hash));
            return;
        }
        self.sync_known(command, seq, hash);
    }

    fn sync_known(&mut self, command: CommandId, seq: u64, hash: Digest32) {
        self.pending.retain(|c| *c != command);
        if self.synced_seq.is_none_or(|s| seq > s) {
            self.synced_seq = Some(seq);
            self.synced_hash = hash;
        }
        self.recompute();
    }
}

/// Combine per-key path digests into one evidence digest, independent of
/// the order keys are listed in.
pub fn combined_path(paths: &[(Vec<u8>, Digest32)]) -> Digest32 {
    let mut sorted: Vec<&(Vec<u8>, Digest32)> = paths.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    let mut parts: Vec<&[u8]> = Vec::with_capacity(sorted.len() * 2);
    for (key, digest) in sorted {
        parts.push(key.as_slice());
        parts.push(&digest.0);
    }
    HashDomain::DependencyPath.digest(&parts)
}

/// An in-progress closure traversal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClosureCursor {
    root: CommandId,
    frontier: Vec<CommandId>,
    visited: BTreeSet<CommandId>,
    visits: usize,
}

impl ClosureCursor {
    /// Start a traversal at `root` with `deps` its direct dependencies.
    pub fn start(root: CommandId, deps: &[CommandId]) -> Self {
        let mut visited = BTreeSet::new();
        visited.insert(root);
        ClosureCursor {
            root,
            frontier: deps.to_vec(),
            visited,
            visits: 0,
        }
    }

    /// Root.
    pub const fn root(&self) -> CommandId {
        self.root
    }

    /// Commands still to visit.
    pub fn frontier(&self) -> &[CommandId] {
        &self.frontier
    }

    /// Advance by at most `budget` visits. `deps_of` returns the direct
    /// dependencies of an initialized command, or `None` for a placeholder
    /// or unknown identity, which stops the traversal.
    pub fn step(
        mut self,
        budget: usize,
        mut deps_of: impl FnMut(&CommandId) -> Option<Vec<CommandId>>,
    ) -> Result<ClosureProgress, CommandId> {
        let mut spent = 0;
        while spent < budget {
            let Some(next) = self.frontier.pop() else {
                let mut members = self.visited;
                members.remove(&self.root);
                return Ok(ClosureProgress::Complete(Closure {
                    root: self.root,
                    members,
                    visits: self.visits,
                }));
            };
            if !self.visited.insert(next) {
                continue;
            }
            let deps = deps_of(&next).ok_or(next)?;
            spent += 1;
            self.visits += 1;
            for d in deps {
                if !self.visited.contains(&d) {
                    self.frontier.push(d);
                }
            }
        }
        if self.frontier.is_empty() {
            let mut members = self.visited;
            members.remove(&self.root);
            return Ok(ClosureProgress::Complete(Closure {
                root: self.root,
                members,
                visits: self.visits,
            }));
        }
        Ok(ClosureProgress::Continue(self))
    }
}

/// Result of one closure step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClosureProgress {
    /// More work remains; continue from the cursor.
    Continue(ClosureCursor),
    /// The closure is complete.
    Complete(Closure),
}

/// The exact transitive dependency closure of a command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Closure {
    /// Root.
    pub root: CommandId,
    /// Every transitive dependency (root excluded).
    pub members: BTreeSet<CommandId>,
    /// Commands visited (work spent).
    pub visits: usize,
}
