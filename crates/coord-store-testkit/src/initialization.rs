//! Atomic initialization and index visibility under yielding (task-j01;
//! design Sections 4.7, 17.3.2 and 17.16.4).
//!
//! Two conflicting proposals of one domain are driven through every
//! interleaving of their steps (submit, durable, accept submit, accept
//! durable) against the [`ModelJournal`]. Installing a command's
//! initialized state (payload binding, phase, dependencies) and exposing it
//! through the conflict index is one atomic transition that happens when
//! the record is durable; the conflict lookup and the source
//! dependency-phase guards (`coord_consensus::phase`) read only installed
//! state. Placeholders created by early leader evidence are never in the
//! index. Deliberate [`InitMisbehavior`]s exist so the checker can prove it
//! detects a half-initialized command seen by a competing proposal, a
//! ghost left by a definitely failed append, a placeholder masquerading as
//! processed state, and a vote justified by volatile rather than durable
//! phase.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;

use coord_consensus::phase::{Phase, guard_accept};
use coord_core::effect::{BarrierId, BootId, StoreUpdate};
use coord_journal_api::engine::JournalEngine;
use coord_journal_api::failure::JournalFailure;
use coord_journal_api::group::{GroupEntry, GroupLimits, GroupWrite};
use coord_journal_api::head::{HeadError, Reconciled, StreamHead};
use coord_journal_api::record::{
    GENESIS_PREDECESSOR, JOURNAL_RECORD_FORMAT_V1, JournalRecordV1, LifecycleRecordV1, RecordBody,
    RecordDraft, RecordOrigin, TransitionContext,
};
use coord_journal_api::stream::{
    ShardId, StorageStreamId, StreamHighWater, StreamKey, StreamMappingV1,
};
use coord_store_api::registry::Collection;
use coord_types::CommandId;
use coord_types::identity::Digest32;
use coord_types::ids::{
    Ballot, ClusterId, ConfigurationEpoch, DomainId, ReplicaId, ReplicaIncarnation,
};
use serde::{Deserialize, Serialize};

use crate::journal::{AppendScript, ModelJournal};

/// Deliberate misbehaviors the checker must detect. The honest world
/// enables none of them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum InitMisbehavior {
    /// The conflict index is updated at submission, before the record is
    /// durable (a definite failure then leaves a ghost).
    IndexBeforeDurable,
    /// Installation is split: the index is updated in one step and the
    /// payload/phase row in a later one.
    SplitInstall,
    /// A placeholder created by early leader evidence is listed in the
    /// conflict index.
    PlaceholderVisible,
    /// The ACCEPT guard reads the proposer's volatile phase instead of the
    /// installed durable one.
    GuardFromVolatile,
}

/// How an append is made to fail during exploration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FailKind {
    /// Definite noncommit: the proposal replans.
    Definite,
    /// Indeterminate with the group durable: reconciliation finds it.
    IndeterminatePresent,
    /// Indeterminate with the group absent: reconciliation retries.
    IndeterminateAbsent,
}

/// World configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorldConfig {
    /// Misbehavior to enable, if any.
    pub misbehavior: Option<InitMisbehavior>,
    /// Make the `n`-th append (1-based) fail this way.
    pub fail_append: Option<(u64, FailKind)>,
}

/// An invariant violation found by the checker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Violation {
    /// A command in the conflict index has no installed payload/phase.
    HalfStateVisible {
        /// Command.
        id: CommandId,
    },
    /// A command in the conflict index has no durable initialization.
    VisibleBeforeDurable {
        /// Command.
        id: CommandId,
    },
    /// A proposal entered ACCEPT with a dependency whose ACCEPT is not
    /// durable and installed.
    VoteFromVolatileState {
        /// Dependency.
        dep: CommandId,
    },
    /// Neither or both of two conflicting proposals depend on the other.
    ConflictUnordered,
    /// No proposal can make progress.
    Deadlock,
    /// The journal or head refused a step the world considered valid.
    Unexpected(String),
}

/// What exploration covered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Explored {
    /// Complete schedules (terminal states) examined.
    pub schedules: usize,
    /// Distinct states visited.
    pub states: usize,
}

/// The installed row of an initialized command (the value of its
/// `protocol_v1` update).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CommandRow {
    phase: Phase,
    payload: Digest32,
    deps: Vec<CommandId>,
    keys: Vec<Vec<u8>>,
}

/// Installed command state and the derived conflict index.
#[derive(Clone, Debug, Default)]
struct Table {
    /// `None` is a placeholder (identity known, nothing installed).
    entries: BTreeMap<CommandId, Option<CommandRow>>,
    index: BTreeMap<Vec<u8>, BTreeSet<CommandId>>,
}

impl Table {
    fn expect(&mut self, id: CommandId) {
        self.entries.entry(id).or_insert(None);
    }

    fn index_only(&mut self, id: CommandId, keys: &[Vec<u8>]) {
        self.entries.entry(id).or_insert(None);
        for k in keys {
            self.index.entry(k.clone()).or_default().insert(id);
        }
    }

    fn row_only(&mut self, id: CommandId, row: CommandRow) {
        self.entries.insert(id, Some(row));
    }

    /// Atomic installation from a durable record: row and index together.
    fn install(&mut self, record: &JournalRecordV1) {
        for u in record.body().updates() {
            let id = CommandId(Digest32(u.key.clone().try_into().expect("command key")));
            let row: CommandRow =
                postcard::from_bytes(u.value.as_deref().expect("row")).expect("row decodes");
            for k in &row.keys {
                self.index.entry(k.clone()).or_default().insert(id);
            }
            self.entries.insert(id, Some(row));
        }
    }

    fn lookup(&self, keys: &[Vec<u8>]) -> Vec<CommandId> {
        let mut out = BTreeSet::new();
        for k in keys {
            if let Some(ids) = self.index.get(k) {
                out.extend(ids.iter().copied());
            }
        }
        out.into_iter().collect()
    }

    fn phase_of(&self, id: &CommandId) -> Option<Phase> {
        self.entries
            .get(id)
            .and_then(|e| e.as_ref().map(|r| r.phase))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    Submit,
    Durable,
    AcceptSubmit,
    AcceptDurable,
    Done,
}

#[derive(Clone, Debug)]
struct Proposal {
    id: CommandId,
    keys: Vec<Vec<u8>>,
    payload: Digest32,
    step: Step,
    deps: Vec<CommandId>,
    inflight: Option<(GroupWrite, JournalRecordV1)>,
    init_durable: bool,
    accept_durable: bool,
    volatile_phase: Option<Phase>,
    /// Split installation: the row of this durable record is still pending.
    split_row: Option<JournalRecordV1>,
}

/// The world: one domain stream, two conflicting proposals and a
/// placeholder known only by identity.
#[derive(Clone, Debug)]
pub struct World {
    config: WorldConfig,
    journal: ModelJournal,
    table: Table,
    head: StreamHead,
    last_digest: Digest32,
    origin: RecordOrigin,
    proposals: Vec<Proposal>,
    placeholder: CommandId,
    barriers: u64,
    appends_seen: u64,
}

fn context() -> TransitionContext {
    let epoch = ConfigurationEpoch::new(1).unwrap();
    TransitionContext {
        boot: BootId([0xb0; 16]),
        configuration: epoch,
        ballot: Ballot {
            epoch,
            number: 1,
            leader: ReplicaId([1; 16]),
        },
    }
}

impl World {
    /// Build the world: genesis appended, mapping durable, both proposals
    /// at `Submit`, placeholder expected.
    pub fn new(config: WorldConfig) -> Self {
        let key = StreamKey {
            cluster: ClusterId([1; 16]),
            domain: DomainId([2; 16]),
            incarnation: ReplicaIncarnation::new(1).unwrap(),
        };
        let stream = StorageStreamId::FIRST;
        let origin = RecordOrigin {
            cluster: key.cluster,
            domain: key.domain,
            replica: ReplicaId([1; 16]),
            incarnation: key.incarnation,
            stream,
        };
        let mut journal = ModelJournal::new();
        journal
            .persist_mapping(
                StreamHighWater::from_durable(1),
                &StreamMappingV1 {
                    stream,
                    key,
                    shard: ShardId::new(0).unwrap(),
                    retired: false,
                },
            )
            .unwrap();
        let mut head = StreamHead::open(stream, coord_types::ids::LocalJournalSeq::ZERO);
        let genesis = JournalRecordV1::seal(RecordDraft {
            origin,
            seq: head.next_seq().unwrap(),
            predecessor: GENESIS_PREDECESSOR,
            body: RecordBody::Lifecycle(LifecycleRecordV1::Genesis {
                format: JOURNAL_RECORD_FORMAT_V1,
            }),
        })
        .unwrap();
        let barrier = BarrierId {
            node_generation: origin.incarnation,
            boot_id: BootId([0xb0; 16]),
            sequence: 0,
        };
        head.reserve(barrier, NonZeroU32::new(1).unwrap()).unwrap();
        let mut group = GroupWrite::new(GroupLimits::DEFAULT);
        group
            .push(GroupEntry::new(barrier, stream, vec![genesis.clone()]).unwrap())
            .unwrap();
        journal.append_group(&group).unwrap();
        head.complete_durable(barrier).unwrap();
        let placeholder = CommandId(Digest32([0xcc; 32]));
        let mut table = Table::default();
        table.expect(placeholder);
        if config.misbehavior == Some(InitMisbehavior::PlaceholderVisible) {
            table.index_only(placeholder, &[b"k".to_vec()]);
        }
        let proposal = |tag: u8| Proposal {
            id: CommandId(Digest32([tag; 32])),
            keys: vec![b"k".to_vec()],
            payload: Digest32([tag ^ 0xff; 32]),
            step: Step::Submit,
            deps: Vec::new(),
            inflight: None,
            init_durable: false,
            accept_durable: false,
            volatile_phase: None,
            split_row: None,
        };
        World {
            config,
            journal,
            table,
            head,
            last_digest: genesis.digest(),
            origin,
            proposals: vec![proposal(0xa1), proposal(0xb2)],
            placeholder,
            barriers: 1,
            appends_seen: 1,
        }
    }

    fn misbehaves(&self, m: InitMisbehavior) -> bool {
        self.config.misbehavior == Some(m)
    }

    fn record(&self, id: CommandId, row: &CommandRow) -> Result<JournalRecordV1, Violation> {
        JournalRecordV1::seal(RecordDraft {
            origin: self.origin,
            seq: self
                .head
                .next_seq()
                .map_err(|e| Violation::Unexpected(e.to_string()))?,
            predecessor: self.last_digest,
            body: RecordBody::ProtocolTransition {
                context: context(),
                updates: vec![StoreUpdate {
                    collection: Collection::ProtocolV1.id(),
                    key: id.as_bytes().to_vec(),
                    value: Some(postcard::to_allocvec(row).unwrap()),
                }],
            },
        })
        .map_err(|e| Violation::Unexpected(e.to_string()))
    }

    /// Submit a record for proposal `i`; `false` when blocked by the
    /// one-outstanding-batch rule.
    fn submit(&mut self, i: usize, row: CommandRow) -> Result<bool, Violation> {
        let id = self.proposals[i].id;
        let record = self.record(id, &row)?;
        self.barriers += 1;
        let barrier = BarrierId {
            node_generation: self.origin.incarnation,
            boot_id: BootId([0xb0; 16]),
            sequence: self.barriers,
        };
        match self.head.reserve(barrier, NonZeroU32::new(1).unwrap()) {
            Ok(_) => {}
            Err(HeadError::BatchOutstanding { .. }) => return Ok(false),
            Err(e) => return Err(Violation::Unexpected(e.to_string())),
        }
        let mut group = GroupWrite::new(GroupLimits::DEFAULT);
        group
            .push(GroupEntry::new(barrier, self.origin.stream, vec![record.clone()]).unwrap())
            .map_err(|e| Violation::Unexpected(e.to_string()))?;
        self.proposals[i].inflight = Some((group, record));
        Ok(true)
    }

    /// Drive the in-flight append of proposal `i` to an outcome. Returns
    /// the durable record, or `None` when the append definitely did not
    /// happen (or was reconciled as absent) and the proposal must replan.
    fn durable(&mut self, i: usize) -> Result<Option<JournalRecordV1>, Violation> {
        let (group, record) = self.proposals[i].inflight.take().expect("in flight");
        let barrier = group.entries()[0].barrier();
        self.appends_seen += 1;
        if let Some((n, kind)) = self.config.fail_append
            && n == self.appends_seen
        {
            self.journal.script_append(match kind {
                FailKind::Definite => AppendScript::DefinitelyNotCommitted,
                FailKind::IndeterminatePresent => AppendScript::Indeterminate { applied: true },
                FailKind::IndeterminateAbsent => AppendScript::Indeterminate { applied: false },
            });
        }
        match self.journal.append_group(&group) {
            Ok(_) => {
                self.head
                    .complete_durable(barrier)
                    .map_err(|e| Violation::Unexpected(e.to_string()))?;
                self.last_digest = record.digest();
                Ok(Some(record))
            }
            Err(JournalFailure::Definite(_)) => {
                self.head
                    .fail_definite(barrier)
                    .map_err(|e| Violation::Unexpected(e.to_string()))?;
                Ok(None)
            }
            Err(JournalFailure::Indeterminate(_)) => {
                self.head
                    .fail_indeterminate(barrier)
                    .map_err(|e| Violation::Unexpected(e.to_string()))?;
                // Reconcile from the actual durable head, never blind-retry.
                let recovered = self
                    .journal
                    .durable_head(self.origin.stream)
                    .map_err(|e| Violation::Unexpected(e.to_string()))?;
                match self.head.reconcile(recovered) {
                    Ok(Reconciled::Present(_)) => {
                        self.last_digest = record.digest();
                        Ok(Some(record))
                    }
                    Ok(Reconciled::Absent(_)) => Ok(None),
                    Err(e) => Err(Violation::Unexpected(e.to_string())),
                }
            }
        }
    }

    /// Run one step of proposal `i`. `Ok(false)` means the step is blocked
    /// and nothing changed.
    fn step(&mut self, i: usize) -> Result<bool, Violation> {
        let p = self.proposals[i].clone();
        match p.step {
            Step::Submit => {
                let deps = self.table.lookup(&p.keys);
                let row = CommandRow {
                    phase: Phase::PreAccept,
                    payload: p.payload,
                    deps: deps.clone(),
                    keys: p.keys.clone(),
                };
                if !self.submit(i, row.clone())? {
                    return Ok(false);
                }
                if self.misbehaves(InitMisbehavior::IndexBeforeDurable) {
                    // Complete in-memory state, but nothing durable yet.
                    self.table.row_only(p.id, row);
                    self.table.index_only(p.id, &p.keys);
                }
                self.proposals[i].deps = deps;
                self.proposals[i].step = Step::Durable;
            }
            Step::Durable => match self.durable(i)? {
                Some(record) => {
                    self.proposals[i].init_durable = true;
                    if self.misbehaves(InitMisbehavior::SplitInstall) {
                        self.table.index_only(p.id, &p.keys);
                        self.proposals[i].split_row = Some(record);
                    } else {
                        self.table.install(&record);
                    }
                    self.proposals[i].step = Step::AcceptSubmit;
                }
                None => self.proposals[i].step = Step::Submit,
            },
            Step::AcceptSubmit => {
                if let Some(record) = p.split_row {
                    // The second half of a split installation.
                    for u in record.body().updates() {
                        let row: CommandRow =
                            postcard::from_bytes(u.value.as_deref().unwrap()).unwrap();
                        self.table.row_only(p.id, row);
                    }
                    self.proposals[i].split_row = None;
                    return Ok(true);
                }
                let volatile = self.misbehaves(InitMisbehavior::GuardFromVolatile);
                let phase_of = |dep: &CommandId| {
                    if volatile
                        && let Some(q) = self.proposals.iter().find(|q| q.id == *dep)
                        && q.volatile_phase.is_some()
                    {
                        return q.volatile_phase;
                    }
                    self.table.phase_of(dep)
                };
                if guard_accept(&p.deps, phase_of).is_err() {
                    return Ok(false);
                }
                let row = CommandRow {
                    phase: Phase::Accept,
                    payload: p.payload,
                    deps: p.deps.clone(),
                    keys: p.keys.clone(),
                };
                if !self.submit(i, row)? {
                    return Ok(false);
                }
                // The guard passed: every dependency's ACCEPT must be
                // durable and installed.
                for dep in &p.deps {
                    let installed = self.table.phase_of(dep) >= Some(Phase::Accept);
                    let durable = self
                        .proposals
                        .iter()
                        .any(|q| q.id == *dep && q.accept_durable);
                    if !(installed && durable) {
                        return Err(Violation::VoteFromVolatileState { dep: *dep });
                    }
                }
                self.proposals[i].volatile_phase = Some(Phase::Accept);
                self.proposals[i].step = Step::AcceptDurable;
            }
            Step::AcceptDurable => match self.durable(i)? {
                Some(record) => {
                    self.proposals[i].accept_durable = true;
                    self.table.install(&record);
                    self.proposals[i].step = Step::Done;
                }
                None => self.proposals[i].step = Step::AcceptSubmit,
            },
            Step::Done => return Ok(false),
        }
        Ok(true)
    }

    /// Invariants that must hold in every reachable state.
    fn check(&self) -> Result<(), Violation> {
        for ids in self.table.index.values() {
            for id in ids {
                let row = self.table.entries.get(id).and_then(|e| e.as_ref());
                if row.is_none() {
                    return Err(Violation::HalfStateVisible { id: *id });
                }
                let durable = self.proposals.iter().any(|p| p.id == *id && p.init_durable);
                if !durable {
                    return Err(Violation::VisibleBeforeDurable { id: *id });
                }
            }
        }
        // The placeholder is known by identity only and never a phase.
        debug_assert_eq!(self.table.phase_of(&self.placeholder), None);
        Ok(())
    }

    fn terminal(&self) -> Result<(), Violation> {
        let a = &self.proposals[0];
        let b = &self.proposals[1];
        let a_on_b = a.deps.contains(&b.id);
        let b_on_a = b.deps.contains(&a.id);
        if a_on_b == b_on_a {
            return Err(Violation::ConflictUnordered);
        }
        Ok(())
    }

    fn done(&self) -> bool {
        self.proposals.iter().all(|p| p.step == Step::Done)
    }

    fn signature(&self) -> String {
        let mut s = String::new();
        for p in &self.proposals {
            s.push_str(&format!(
                "{:?}/{}/{}/{}/{}|",
                p.step,
                p.deps.len(),
                p.init_durable,
                p.accept_durable,
                p.split_row.is_some()
            ));
        }
        s.push_str(&format!("{}/{}", self.head.durable(), self.appends_seen));
        s
    }
}

/// Explore every interleaving of the two proposals' steps from `config`.
pub fn explore(config: WorldConfig) -> Result<Explored, Violation> {
    let mut schedules = 0usize;
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut stack = vec![World::new(config)];
    while let Some(world) = stack.pop() {
        world.check()?;
        if !seen.insert(world.signature()) {
            continue;
        }
        if world.done() {
            world.terminal()?;
            schedules += 1;
            continue;
        }
        let mut progressed = false;
        for i in 0..world.proposals.len() {
            let mut next = world.clone();
            if next.step(i)? {
                progressed = true;
                stack.push(next);
            }
        }
        if !progressed {
            return Err(Violation::Deadlock);
        }
    }
    Ok(Explored {
        schedules,
        states: seen.len(),
    })
}
