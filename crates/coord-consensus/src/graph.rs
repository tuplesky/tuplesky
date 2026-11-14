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
use coord_types::ids::Ballot;
use serde::{Deserialize, Serialize};

/// Chain one command onto a path digest.
pub fn chain(previous: &Digest32, command: &CommandId) -> Digest32 {
    HashDomain::DependencyPath.digest(&[&previous.0, command.as_bytes()])
}

/// The digest of an empty path.
pub fn empty_path() -> Digest32 {
    HashDomain::DependencyPath.digest(&[])
}

/// The path evidence of a record a Sync demoted (task-d11): no path at
/// all. A demoted record was pre-accepted in an earlier ballot, and the
/// path it had then is no evidence about the synchronized one, whose label
/// its report carries. Recovery counts no record with this path as a
/// fast-path candidate. No acknowledgement carries it: a follower's fast
/// acknowledgement carries the path it computed at initialization, a
/// combination of at least one key, and a single-part digest is none of
/// those (every part is length-prefixed).
pub fn demoted_path() -> Digest32 {
    HashDomain::DependencyPath.digest(&[b"demoted"])
}

/// The head of a log re-anchored after `tails` ([`PathLog::anchored`]): a
/// digest no [`chain`] link or follower log reaches except by synchronizing
/// to a path chained from it.
pub fn anchored_path(tails: &[CommandId]) -> Digest32 {
    let mut sorted: Vec<&CommandId> = tails.iter().collect();
    sorted.sort();
    sorted.dedup();
    let mut parts: Vec<&[u8]> = Vec::with_capacity(sorted.len() + 1);
    parts.push(b"anchored");
    for tail in sorted {
        parts.push(tail.as_bytes());
    }
    HashDomain::DependencyPath.digest(&parts)
}

/// Where a synchronization put a path log (task-d68): the ballot whose
/// leader numbered the command, and that number.
///
/// Numbers are compared within one ballot only. A proposal enters a log
/// only in the ballot its replica holds, a later activation than any
/// position the log took before, so a proposal of a different ballot is
/// above. That needs no order on ballots, which is partial across epochs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    /// The ballot whose leader numbered the command.
    pub ballot: Ballot,
    /// Its number in that ballot.
    pub seq: u64,
}

impl Position {
    /// Whether this position is above `other`: of another ballot, or
    /// numbered higher in the same one.
    pub fn is_above(&self, other: &Position) -> bool {
        self.ballot != other.ballot || self.seq > other.seq
    }
}

/// One key's conflict log (prototype `HashLog`; task-d34, amended by
/// task-d67 and task-d68).
///
/// Every command on the chain the head digests holds, at this replica,
/// the order a leader that ordered the chain gave it. A command leaves
/// the pending suffix at its synchronization, at a release, or by being
/// set aside: its record stays, and only the head stops digesting it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathLog {
    /// Position of the synchronization the prefix digest is through.
    position: Option<Position>,
    /// Path digest through the synchronized prefix.
    synced_hash: Digest32,
    /// Commands after the synchronized prefix, in local order, each
    /// pre-accepted with the one before it as its dependency on the key.
    pending: Vec<CommandId>,
    /// Digest of the whole log.
    head: Digest32,
    /// Leader synchronizations of commands not appended here yet, held
    /// for the append.
    early: BTreeMap<CommandId, (Option<Position>, Digest32)>,
    /// Commands whose synchronization was applied, until the command table
    /// retires them: a duplicate or reordered copy of an applied
    /// synchronization is recognized instead of being taken for evidence
    /// about a command not appended yet.
    applied: BTreeSet<CommandId>,
    /// Commands set aside and not since synchronized, retired or
    /// released: pre-acceptances the leader has not ordered where this
    /// replica's log put them (task-d67). For the counts only; nothing
    /// in the log reads it.
    #[serde(default)]
    set_aside: BTreeSet<CommandId>,
}

impl Default for PathLog {
    fn default() -> Self {
        PathLog::new()
    }
}

impl PathLog {
    /// An empty log.
    pub fn new() -> Self {
        PathLog::resumed(empty_path())
    }

    /// A log resumed at a synchronized digest with no pending suffix (used
    /// when rebuilding from durable records). Its position is below every
    /// synchronization's.
    pub fn resumed(digest: Digest32) -> Self {
        PathLog {
            position: None,
            synced_hash: digest,
            pending: Vec::new(),
            head: digest,
            early: BTreeMap::new(),
            applied: BTreeSet::new(),
            set_aside: BTreeSet::new(),
        }
    }

    /// A log whose next command follows `tails` (task-d34, F7): a new
    /// leader anchors its first fresh proposal after the recovered order's
    /// tails, which its own appends need not have passed through, so its
    /// head digests them rather than those appends. Its paths from here
    /// are chained from [`anchored_path`], which a follower's log equals
    /// only once it synchronized to one of them.
    pub fn anchored(tails: &[CommandId]) -> Self {
        PathLog::resumed(anchored_path(tails))
    }

    /// Digest of the whole log: the path evidence for the next command.
    pub const fn head(&self) -> Digest32 {
        self.head
    }

    /// Commands after the synchronized prefix, in local order.
    pub fn pending(&self) -> &[CommandId] {
        &self.pending
    }

    /// Position of the synchronization the prefix is through.
    pub const fn position(&self) -> Option<Position> {
        self.position
    }

    fn recompute(&mut self) {
        let mut head = self.synced_hash;
        for c in &self.pending {
            head = chain(&head, c);
        }
        self.head = head;
    }

    /// Take `commands` out of the suffix, keeping their records.
    fn set_aside(&mut self, commands: impl IntoIterator<Item = CommandId>) {
        self.set_aside.extend(commands);
    }

    /// Append a command in local order and return the path digest through
    /// it. A synchronization of it held for the append is applied instead,
    /// and the command does not enter the suffix: every command appended
    /// before it, before or after the synchronization arrived, was
    /// pre-accepted without it (prototype `Append` with `pendingUpd`).
    pub fn append(&mut self, command: CommandId) -> Digest32 {
        if let Some((position, hash)) = self.early.remove(&command) {
            self.apply(command, position, hash);
            return hash;
        }
        self.pending.push(command);
        self.head = chain(&self.head, &command);
        self.head
    }

    /// A command installed already decided (catch-up), which does not
    /// enter the suffix (task-d67): a synchronization held for it is
    /// applied, and the path through it is the log's head after.
    pub fn install(&mut self, command: CommandId) -> Digest32 {
        if let Some((position, hash)) = self.early.remove(&command) {
            self.apply(command, position, hash);
        }
        self.head
    }

    /// A leader ordered `command`, not appended here yet, with path digest
    /// `hash` through it: held for its append ([`PathLog::append`]).
    pub fn hold(&mut self, command: CommandId, position: Option<Position>, hash: Digest32) {
        if self.applied.contains(&command) || self.early.contains_key(&command) {
            return;
        }
        self.early.insert(command, (position, hash));
    }

    /// A leader ordered `command`, initialized here, with path digest
    /// `hash` through it, at `position` when it is a proposal of this
    /// replica's ballot and at none when it is a Sync's entry (task-d67,
    /// task-d68). A duplicate changes nothing.
    pub fn sync(&mut self, command: CommandId, position: Option<Position>, hash: Digest32) {
        if self.applied.contains(&command) {
            return;
        }
        self.early.remove(&command);
        self.apply(command, position, hash);
    }

    /// Every suffix command pre-accepted before `command` is set aside,
    /// and the whole suffix when it is not in it (set aside, installed
    /// decided, or appended only now): this replica adopts a leader's
    /// proposals on a key in that leader's order, so a command still
    /// pending before it was not ordered before it. A position above the
    /// log's becomes the log's with `hash`, and the suffix after the
    /// command chains through it. Otherwise the rest of the suffix, which
    /// chained through it, is set aside too.
    fn apply(&mut self, command: CommandId, position: Option<Position>, hash: Digest32) {
        self.applied.insert(command);
        self.set_aside.remove(&command);
        let at = self.pending.iter().position(|c| *c == command);
        let mut pending = core::mem::take(&mut self.pending);
        let after = match at {
            Some(i) => {
                let after = pending.split_off(i + 1);
                pending.pop();
                after
            }
            None => Vec::new(),
        };
        self.set_aside(pending);
        let moves = position.is_some_and(|p| self.position.as_ref().is_none_or(|q| p.is_above(q)));
        if moves {
            self.position = position;
            self.synced_hash = hash;
            self.pending = after;
        } else {
            self.set_aside(after);
        }
        self.recompute();
    }

    /// Forget a retired command: its applied synchronization. One still
    /// pending is set aside, with every later suffix command: the chain is
    /// linear.
    pub fn forget(&mut self, command: &CommandId) {
        self.applied.remove(command);
        self.early.remove(command);
        self.set_aside.remove(command);
        if let Some(i) = self.pending.iter().position(|c| c == command) {
            let later = self.pending.split_off(i + 1);
            self.pending.pop();
            self.set_aside(later);
            self.recompute();
        }
    }

    /// Commands set aside and not since synchronized, retired or released
    /// (task-d67).
    pub fn set_aside_commands(&self) -> &BTreeSet<CommandId> {
        &self.set_aside
    }

    /// Take a released command out of the log (task-d24): it leaves the
    /// pending suffix, and the head is recomputed without it, so the next
    /// command's path is the one a replica that never held it computes.
    pub fn remove(&mut self, command: &CommandId) {
        self.pending.retain(|c| c != command);
        self.applied.remove(command);
        self.early.remove(command);
        self.set_aside.remove(command);
        self.recompute();
    }

    /// Commands whose synchronization is applied and not yet forgotten.
    pub fn applied(&self) -> &BTreeSet<CommandId> {
        &self.applied
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
    /// The dependency list being expanded and how far it got, when a
    /// step ran out of budget in the middle of one.
    expanding: Option<(Vec<CommandId>, usize)>,
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
            expanding: None,
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

    /// Units of work known to remain: frontier entries plus the rest of
    /// a dependency list being expanded (more may be discovered).
    pub fn remaining(&self) -> usize {
        self.frontier.len()
            + self
                .expanding
                .as_ref()
                .map_or(0, |(deps, index)| deps.len() - index)
    }

    /// Advance by at most `budget` units of work, where every frontier
    /// entry taken (visited or duplicate) and every dependency edge
    /// examined costs one unit, so a step's work is bounded whatever the
    /// size of a dependency list or of the frontier; an expansion cut off
    /// mid-list resumes where it stopped. `deps_of` returns the direct
    /// dependencies of an initialized command, or `None` for a placeholder
    /// or unknown identity, which stops the traversal.
    pub fn step(
        mut self,
        budget: usize,
        mut deps_of: impl FnMut(&CommandId) -> Option<Vec<CommandId>>,
    ) -> Result<ClosureProgress, CommandId> {
        let mut spent = 0;
        loop {
            if let Some((deps, index)) = &mut self.expanding {
                if *index < deps.len() {
                    if spent >= budget {
                        return Ok(ClosureProgress::Continue(self));
                    }
                    let d = deps[*index];
                    *index += 1;
                    spent += 1;
                    if !self.visited.contains(&d) {
                        self.frontier.push(d);
                    }
                    continue;
                }
                self.expanding = None;
            }
            let Some(&next) = self.frontier.last() else {
                let mut members = self.visited;
                members.remove(&self.root);
                return Ok(ClosureProgress::Complete(Closure {
                    root: self.root,
                    members,
                    visits: self.visits,
                }));
            };
            if spent >= budget {
                return Ok(ClosureProgress::Continue(self));
            }
            self.frontier.pop();
            spent += 1;
            if !self.visited.insert(next) {
                continue;
            }
            let deps = deps_of(&next).ok_or(next)?;
            self.visits += 1;
            self.expanding = Some((deps, 0));
        }
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
