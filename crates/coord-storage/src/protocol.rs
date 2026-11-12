//! Protocol rows in the projection (task-20, task-27): reading the
//! recovered promise of an epoch, and everything else a restarted replica
//! wires its consensus machine from, so the ballot state, the dependency
//! table, the bound Sync selections, the payloads and the execution
//! frontier all come from durable state, never from a message.

use std::ops::Bound;

use coord_consensus::rows::{
    DEPENDENCY_TAG, PromiseRecordV1, SYNC_TAG, SealRecordV1, decode_dependency, decode_payload,
    decode_promise, decode_seal, decode_sync, promise_key, seal_key,
};
use coord_consensus::{CatchUpEntry, CommandRecord, PayloadRecordV1, SyncDecision};
use coord_store_api::engine::{Direction, EngineError, ErrorClass, OrderedRead, ScanRequest};
use coord_store_api::registry::Collection;
use coord_types::ids::{Ballot, ConfigurationEpoch, ExecutionPosition};
use coord_types::logical_v1::LogicalRequest;
use coord_types::{CommandId, RetryKey};

use crate::codecs::{self, ExecutedRecordV1, ExecutionRecordV1, position_key};
use crate::intake::{ProtocolKey, intake_key};
use crate::view::GatedView;
use crate::views::ViewBudget;

/// The epoch's durable promise row, if any.
pub fn read_promise<V: OrderedRead>(
    view: &V,
    epoch: ConfigurationEpoch,
) -> Result<Option<PromiseRecordV1>, EngineError> {
    match view.get(Collection::ProtocolV1.id(), &promise_key(epoch))? {
        Some(bytes) => Ok(Some(decode_promise(&bytes)?)),
        None => Ok(None),
    }
}

/// Everything a restarted replica recovers its consensus role from
/// (design Section 5.2): the promise row, the dependency rows of the
/// epoch, the Sync selections bound in it, the payloads of the recovered
/// commands and the executed identities. Volatile knowledge (votes held in
/// memory, commit notifications, timers) is deliberately absent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveredProtocol {
    /// Promised and synchronized ballots.
    pub promise: Option<PromiseRecordV1>,
    /// The durable seal of this configuration, if this replica wrote
    /// one (task-55).
    ///
    /// Read here rather than inferred anywhere, because it is the only
    /// thing that makes "a restart cannot resume old service" true: a
    /// replica comes back sealed because its row says so. Its absence
    /// says this replica has no seal, never that the transition was
    /// cancelled -- that is a quorum's fact, not one replica's.
    pub seal: Option<SealRecordV1>,
    /// Dependency rows, keyed by command, in command order.
    pub records: Vec<(CommandId, CommandRecord)>,
    /// Bound Sync selections, in key (ballot number, leader) order.
    pub syncs: Vec<(Ballot, SyncDecision)>,
    /// Payloads of the recovered commands (a recovered command whose
    /// payload row is missing is a corrupt projection, reported as such).
    pub payloads: Vec<(CommandId, PayloadRecordV1)>,
    /// Executed identities with their positions, in position order.
    ///
    /// Trimming may remove the dependency rows of executed commands, so
    /// this is what still has rows, not the whole executed history.
    pub executed: Vec<(CommandId, ExecutionPosition)>,
    /// Every other executed identity: commands whose executed row
    /// survives and whose dependency row does not, in execution order
    /// (task-d05; the order since task-d12).
    ///
    /// A checkpoint trim removes the dependency rows of an executed
    /// prefix and keeps its executed rows. Read only through the
    /// dependency rows, those commands came back unknown, and a live
    /// record naming one -- the first proposal after a reclaim names the
    /// retired latest -- failed the execution guard on every restart.
    /// This is the executed answer for them.
    pub history: Vec<CommandId>,
    /// The durable execution frontier: how far this store has actually
    /// executed, whatever protocol rows survive.
    pub frontier: ExecutionPosition,
}

impl Default for RecoveredProtocol {
    fn default() -> Self {
        RecoveredProtocol {
            promise: None,
            seal: None,
            records: Vec::new(),
            syncs: Vec::new(),
            payloads: Vec::new(),
            executed: Vec::new(),
            history: Vec::new(),
            frontier: ExecutionPosition::ZERO,
        }
    }
}

impl RecoveredProtocol {
    /// The highest executed position among the executed identities.
    /// The highest executed position among the executed identities that
    /// still have protocol rows.
    ///
    /// This is evidence from the surviving rows, not the store's
    /// execution frontier: trimming removes the rows of an executed
    /// prefix and leaves the frontier where it was. A caller asking how
    /// far this store has executed wants
    /// [`RecoveredProtocol::execution_frontier`]; this one is for
    /// comparing the rows against it.
    pub fn executed_through(&self) -> ExecutionPosition {
        self.executed
            .iter()
            .map(|(_, p)| *p)
            .max()
            .unwrap_or(ExecutionPosition::ZERO)
    }

    /// How far this store has executed, from the durable baseline.
    ///
    /// Independent of which protocol rows survive, so a trimmed executed
    /// prefix does not move it: deriving the frontier from the rows
    /// reported zero, or an older position, on a store that had executed
    /// well past it, and the next command would then be planned at a
    /// position already used.
    pub const fn execution_frontier(&self) -> ExecutionPosition {
        self.frontier
    }

    /// The Sync this replica bound for a ballot it leads and still holds
    /// the promise for: the campaign to resume (Section 4.9) rather than
    /// reselect.
    pub fn resumable_sync(&self, replica: &coord_types::ids::ReplicaId) -> Option<&SyncDecision> {
        let promised = self.promise.as_ref()?.promised;
        if promised.leader != *replica {
            return None;
        }
        self.syncs
            .iter()
            .find(|(b, _)| *b == promised)
            .map(|(_, d)| d)
    }

    /// The retry key of every recovered payload (drivers rebuilding
    /// bindings).
    pub fn bindings(&self) -> Vec<(RetryKey, CommandId)> {
        self.payloads
            .iter()
            .map(|(c, p)| (p.retry_key, *c))
            .collect()
    }
}

fn corrupt(what: &'static str) -> EngineError {
    EngineError::new(ErrorClass::Corrupt, what)
}

/// Read the epoch's protocol state from a durable view, paged within
/// `budget` rows and bytes per page.
///
/// Every row the store holds is read, however many pages that takes
/// (task-d05). The budget bounds each page, not the recovery: a voter's
/// protocol rows are what it owes the domain, and their number is bounded
/// by the checkpoint trim, not by the schema's view budget. That budget
/// exists for the execution path, where overrunning it is a replicated
/// result; applied to the whole recovery, it made a healthy voter whose
/// history outgrew it unable ever to start again. What recovery keeps in
/// memory is what the machine keeps for the life of the process anyway,
/// as with its executed history.
pub fn read_protocol<V: OrderedRead>(
    view: &V,
    epoch: ConfigurationEpoch,
    budget: ViewBudget,
) -> Result<RecoveredProtocol, EngineError> {
    let promise = read_promise(view, epoch)?;
    let seal = match view.get(Collection::ProtocolV1.id(), &seal_key(epoch))? {
        None => None,
        Some(bytes) => Some(decode_seal(&bytes)?),
    };
    let mut lower = epoch.to_be_bytes().to_vec();
    let mut upper = lower.clone();
    lower.push(DEPENDENCY_TAG);
    upper.push(SYNC_TAG + 1);
    let mut records = Vec::new();
    let mut syncs = Vec::new();
    let mut resume: Option<Vec<u8>> = None;
    loop {
        let page = view.scan_page(
            Collection::ProtocolV1.id(),
            &ScanRequest {
                lower: Bound::Included(lower.clone()),
                upper: Bound::Excluded(upper.clone()),
                direction: Direction::Forward,
                resume_after: resume.clone(),
                max_rows: budget.max_rows.max(1).try_into().expect("non-zero"),
                max_bytes: budget.max_bytes.max(1).try_into().expect("non-zero"),
            },
        )?;
        for row in &page.rows {
            match row.key.get(8) {
                // Keyed by intake order (task-d113): the command is the
                // key's suffix.
                Some(&DEPENDENCY_TAG) => match crate::intake::parse(&row.key) {
                    ProtocolKey::Intake { command, .. } => {
                        records.push((command, decode_dependency(&row.value)?));
                    }
                    ProtocolKey::ByCommand { .. } => {
                        return Err(corrupt(
                            "a dependency row keyed by command: a store of the layout before \
                             task-d113",
                        ));
                    }
                    ProtocolKey::Other => {}
                },
                Some(&SYNC_TAG) if row.key.len() == 33 => {
                    let decision = decode_sync(&row.value)?.decision;
                    syncs.push((decision.ballot, decision));
                }
                _ => {}
            }
        }
        match page.rows.last() {
            Some(last) if !page.exhausted => resume = Some(last.key.clone()),
            _ => break,
        }
    }
    // In command order, as the rows were when they were keyed by command,
    // and one row a command: a rewrite stays in place under its intake key,
    // so a second row of a command is a broken lowering.
    records.sort_unstable_by_key(|(command, _)| *command);
    if records.windows(2).any(|w| w[0].0 == w[1].0) {
        return Err(corrupt("two dependency rows of one command in an epoch"));
    }
    // Every executed row, in the order executed (task-d113, step 2: keyed
    // by position, so no sort). The commands with a dependency row here
    // are the machine's executed answers; the rest are its history.
    let mut executions: std::collections::BTreeMap<CommandId, ExecutionPosition> =
        std::collections::BTreeMap::new();
    let mut in_order: Vec<CommandId> = Vec::new();
    scan_executions(
        view,
        Bound::Unbounded,
        Bound::Unbounded,
        budget,
        |position, row| {
            if position != row.executed.position {
                return Err(corrupt("an executed row under another position's key"));
            }
            executions.insert(row.command, position);
            in_order.push(row.command);
            Ok(())
        },
    )?;
    // A payload not yet executed is pending, by intake order: read them
    // all, which the table's capacity bounds.
    let mut pending: std::collections::BTreeMap<CommandId, Vec<u8>> =
        std::collections::BTreeMap::new();
    scan_rows(view, Collection::PendingPayloadV1, budget, |key, value| {
        let (_, command) = crate::intake::parse_pending(key)
            .ok_or_else(|| corrupt("a pending payload key of the wrong length"))?;
        pending.insert(command, value.to_vec());
        Ok(())
    })?;
    let mut payloads = Vec::with_capacity(records.len());
    let mut executed = Vec::new();
    for (command, record) in &records {
        let position = executions.get(command).copied();
        if record.payload.is_some() {
            let bytes = match position {
                Some(position) => view.get(Collection::PayloadV1.id(), &position_key(position))?,
                None => pending.get(command).cloned(),
            };
            match bytes {
                Some(bytes) => {
                    let payload = decode_payload(&bytes)?;
                    // The row is only this command's payload if it rehashes
                    // to this identity. A well-formed payload under the
                    // wrong key would otherwise be bound to a retry key and
                    // served to peers, and the mismatch would surface only
                    // when the command executed.
                    let request: LogicalRequest = postcard::from_bytes(&payload.logical)
                        .map_err(|_| corrupt("recovered payload is not a canonical request"))?;
                    let derived = CommandId::derive(&payload.retry_key, &request)
                        .map_err(|_| corrupt("recovered payload has no derivable identity"))?;
                    if derived != *command {
                        return Err(corrupt("recovered payload does not match its command key"));
                    }
                    payloads.push((*command, payload));
                }
                None => return Err(corrupt("recovered command without its payload row")),
            }
        }
        if let Some(position) = position {
            executed.push((*command, position));
        }
    }
    executed.sort_by_key(|(_, p)| *p);
    // Every executed identity without a dependency row among `records`
    // (task-d05), in the order executed, not key order (task-d12): a
    // restart replays them into the machine's table, and the last one
    // replayed is the command a new leader chains its first proposals
    // after when its selection gives it nothing to follow.
    let known: std::collections::BTreeSet<CommandId> = records.iter().map(|(c, _)| *c).collect();
    let history: Vec<CommandId> = in_order
        .into_iter()
        .filter(|c| !known.contains(c))
        .collect();
    let frontier = crate::lowering::DurableMeta::read(view)?
        .frontier
        .execution_position;
    Ok(RecoveredProtocol {
        promise,
        seal,
        records,
        syncs,
        payloads,
        executed,
        history,
        frontier,
    })
}

/// Every executed row whose position is within `radius` of one of
/// `around`, in position order (task-d17).
///
/// What a node that stopped on a release its execution contradicts shows
/// of its own execution: the commands it executed next to where it put
/// the command and next to where the leader did. Keyed by position
/// (task-d113, step 2), each is a range of `execution_v1`.
pub fn executed_near<V: OrderedRead>(
    view: &V,
    around: &[ExecutionPosition],
    radius: u64,
    budget: ViewBudget,
) -> Result<Vec<(CommandId, ExecutedRecordV1)>, EngineError> {
    let mut rows: std::collections::BTreeMap<ExecutionPosition, (CommandId, ExecutedRecordV1)> =
        std::collections::BTreeMap::new();
    for centre in around {
        let low = ExecutionPosition::new(centre.get().saturating_sub(radius).max(1))
            .map_err(|_| corrupt("a position at zero"))?;
        let high = ExecutionPosition::new(centre.get().saturating_add(radius))
            .map_err(|_| corrupt("a position at zero"))?;
        scan_executions(
            view,
            Bound::Included(low),
            Bound::Included(high),
            budget,
            |position, row| {
                rows.insert(position, (row.command, row.executed));
                Ok(())
            },
        )?;
    }
    Ok(rows.into_values().collect())
}

/// At most `max` executed rows from `after` (exclusive) through `through`
/// (inclusive), in position order, as (position, row): what a donor serves
/// a lagging voter from (task-d08), one range of `execution_v1` since
/// task-d113's step 2. Fewer than `max` where the rows run out or one
/// page of `budget` does: a page is a prefix, so a short read is one too.
pub fn executed_between<V: OrderedRead>(
    view: &V,
    after: ExecutionPosition,
    through: ExecutionPosition,
    max: usize,
    budget: ViewBudget,
) -> Result<Vec<(ExecutionPosition, ExecutionRecordV1)>, EngineError> {
    if max == 0 {
        return Ok(Vec::new());
    }
    let page = view.scan_page(
        Collection::ExecutionV1.id(),
        &ScanRequest {
            lower: Bound::Excluded(position_key(after).to_vec()),
            upper: Bound::Included(position_key(through).to_vec()),
            direction: Direction::Forward,
            resume_after: None,
            max_rows: budget
                .max_rows
                .clamp(1, u32::try_from(max).unwrap_or(u32::MAX))
                .try_into()
                .expect("non-zero"),
            max_bytes: budget.max_bytes.max(1).try_into().expect("non-zero"),
        },
    )?;
    page.rows
        .iter()
        .map(|row| {
            Ok((
                codecs::position_of(&row.key)?,
                codecs::decode_execution(&row.value)?,
            ))
        })
        .collect()
}

/// Every executed row at or below `through`, as (position, command), in
/// position order: one range of `execution_v1` (task-d113, step 2). What
/// a reader with no gate behind its view -- the trim's -- asks "executed
/// at or below" with, since a command's position is not a lookup without
/// the index's executed part.
pub fn executed_through<V: OrderedRead>(
    view: &V,
    through: ExecutionPosition,
    budget: ViewBudget,
) -> Result<Vec<(ExecutionPosition, CommandId)>, EngineError> {
    let mut out = Vec::new();
    scan_executions(
        view,
        Bound::Unbounded,
        Bound::Included(through),
        budget,
        |position, row| {
            out.push((position, row.command));
            Ok(())
        },
    )?;
    Ok(out)
}

/// Every executed row, as (position, command), in position order
/// (task-d08). Keyed by position since task-d113's step 2, so it is one
/// scan with no sort.
pub fn executed_order<V: OrderedRead>(
    view: &V,
    budget: ViewBudget,
) -> Result<Vec<(ExecutionPosition, CommandId)>, EngineError> {
    let mut order = Vec::new();
    scan_executions(
        view,
        Bound::Unbounded,
        Bound::Unbounded,
        budget,
        |position, row| {
            order.push((position, row.command));
            Ok(())
        },
    )?;
    Ok(order)
}

/// The executed row of `command`, through the index's executed part of the
/// gate `gated` came through (task-d113, step 2): the position, and the
/// row there, which names its command.
pub fn executed_record<V: OrderedRead>(
    gated: &GatedView<V>,
    command: &CommandId,
) -> Result<Option<ExecutedRecordV1>, EngineError> {
    let Some(gate) = gated.gate() else {
        return Ok(None);
    };
    let candidates: Vec<ExecutionPosition> = gate.index().executed_candidates(command).collect();
    for position in candidates {
        if let Some(bytes) = gated
            .view()
            .get(Collection::ExecutionV1.id(), &position_key(position))?
        {
            let row = codecs::decode_execution(&bytes)?;
            if row.command == *command {
                return Ok(Some(row.executed));
            }
        }
    }
    Ok(None)
}

/// Call `f` with each `execution_v1` row between `lower` and `upper`, in
/// position order, paged within `budget`.
fn scan_executions<V: OrderedRead>(
    view: &V,
    lower: Bound<ExecutionPosition>,
    upper: Bound<ExecutionPosition>,
    budget: ViewBudget,
    mut f: impl FnMut(ExecutionPosition, ExecutionRecordV1) -> Result<(), EngineError>,
) -> Result<(), EngineError> {
    let key = |b: Bound<ExecutionPosition>| b.map(|p| position_key(p).to_vec());
    let (lower, upper) = (key(lower), key(upper));
    let mut resume: Option<Vec<u8>> = None;
    loop {
        let page = view.scan_page(
            Collection::ExecutionV1.id(),
            &ScanRequest {
                lower: lower.clone(),
                upper: upper.clone(),
                direction: Direction::Forward,
                resume_after: resume.take(),
                max_rows: budget.max_rows.max(1).try_into().expect("non-zero"),
                max_bytes: budget.max_bytes.max(1).try_into().expect("non-zero"),
            },
        )?;
        for row in &page.rows {
            f(
                codecs::position_of(&row.key)?,
                codecs::decode_execution(&row.value)?,
            )?;
        }
        match page.rows.last() {
            Some(last) if !page.exhausted => resume = Some(last.key.clone()),
            _ => return Ok(()),
        }
    }
}

/// Call `f` with each row of `collection`, in key order, paged within
/// `budget`.
fn scan_rows<V: OrderedRead>(
    view: &V,
    collection: Collection,
    budget: ViewBudget,
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
                max_rows: budget.max_rows.max(1).try_into().expect("non-zero"),
                max_bytes: budget.max_bytes.max(1).try_into().expect("non-zero"),
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

/// What a donor sends a lagging voter of one command it executed
/// (task-d08), read from its durable rows: the execution from
/// `execution_v1` and the payload from `payload_v1`, both at the command's
/// position (task-d113, step 2), and the decided record from the epoch's
/// dependency row in `protocol_v1`, if it keeps one.
///
/// `None` when the command has no executed row or no payload here: a
/// donor serves only what it executed and can show.
///
/// The dependency row is keyed by intake order (task-d113), and found
/// through the index by command of the gate `gated` came through. Only for
/// a command the snapshot shows executed (rule (c)): an executed command's
/// row is never deleted and written again under a new key, so an index
/// newer than the snapshot names the row the snapshot holds, or none.
pub fn catch_up_entry<V: OrderedRead>(
    gated: &GatedView<V>,
    epoch: ConfigurationEpoch,
    command: &CommandId,
) -> Result<Option<CatchUpEntry>, EngineError> {
    let Some(executed) = executed_record(gated, command)? else {
        return Ok(None);
    };
    catch_up_entry_at(gated, epoch, command, executed)
}

/// [`catch_up_entry`] for a command whose executed row the caller has read
/// in `gated`'s snapshot: a range of `execution_v1` names its command.
pub fn catch_up_entry_at<V: OrderedRead>(
    gated: &GatedView<V>,
    epoch: ConfigurationEpoch,
    command: &CommandId,
    executed: ExecutedRecordV1,
) -> Result<Option<CatchUpEntry>, EngineError> {
    let view = gated.view();
    let ExecutedRecordV1 {
        position,
        revision,
        result_digest,
    } = executed;
    let Some(payload) = view.get(Collection::PayloadV1.id(), &position_key(position))? else {
        return Ok(None);
    };
    let payload = decode_payload(&payload)?;
    let decided = executed_dependency_row(gated, epoch, command)?
        .map(|bytes| decode_dependency(&bytes))
        .transpose()?;
    Ok(Some(CatchUpEntry {
        command: *command,
        payload,
        decided,
        position,
        revision,
        result_digest,
    }))
}

/// The dependency row of `command` in `epoch`, which the caller has seen
/// executed in `gated`'s snapshot (rule (c)), through the index by command.
fn executed_dependency_row<V: OrderedRead>(
    gated: &GatedView<V>,
    epoch: ConfigurationEpoch,
    command: &CommandId,
) -> Result<Option<Vec<u8>>, EngineError> {
    let Some(gate) = gated.gate() else {
        return Ok(None);
    };
    let epoch = epoch.to_be_bytes();
    let candidates: Vec<_> = gate
        .index()
        .candidates(epoch, DEPENDENCY_TAG, command)
        .collect();
    for order in candidates {
        // The key carries the command: a candidate of another command
        // under the same prefix is not a row of this one.
        if let Some(bytes) = gated.view().get(
            Collection::ProtocolV1.id(),
            &intake_key(epoch, DEPENDENCY_TAG, order, command),
        )? {
            return Ok(Some(bytes));
        }
    }
    Ok(None)
}
