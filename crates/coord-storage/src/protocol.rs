//! Protocol rows in the projection (task-20, task-27): reading the
//! recovered promise of an epoch, and everything else a restarted replica
//! wires its consensus machine from, so the ballot state, the dependency
//! table, the bound Sync selections, the payloads and the execution
//! frontier all come from durable state, never from a message.

use std::ops::Bound;

use coord_consensus::rows::{
    DEPENDENCY_TAG, PromiseRecordV1, SYNC_TAG, SealRecordV1, decode_dependency, decode_payload,
    decode_promise, decode_seal, decode_sync, payload_key, promise_key, seal_key,
};
use coord_consensus::{CommandRecord, PayloadRecordV1, SyncDecision};
use coord_store_api::engine::{Direction, EngineError, ErrorClass, OrderedRead, ScanRequest};
use coord_store_api::registry::Collection;
use coord_types::identity::Digest32;
use coord_types::ids::{Ballot, ConfigurationEpoch, ExecutionPosition};
use coord_types::logical_v1::LogicalRequest;
use coord_types::{CommandId, RetryKey};

use crate::codecs::{self, ExecutedRecordV1};
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
    /// Dependency rows, keyed by command.
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
    /// Every other executed identity: commands whose `executed_v1` row
    /// survives and whose dependency row does not, in key order
    /// (task-d05).
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

/// Read the epoch's protocol state from a durable view, within `budget`
/// rows per collection scan (a budget overrun is a limit error, never a
/// silently truncated recovery).
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
    let mut seen = 0u32;
    // The budget bounds the whole recovery, not one page: every row of
    // every page, and every payload and executed row read below, is
    // charged against it, so a projection of many individually small rows
    // cannot make recovery allocate without limit.
    let mut bytes_left = budget.max_bytes;
    let charge = |taken: usize, left: &mut u32| -> Result<(), EngineError> {
        let taken = u32::try_from(taken).unwrap_or(u32::MAX);
        match left.checked_sub(taken) {
            Some(rest) => {
                *left = rest;
                Ok(())
            }
            None => Err(EngineError::new(
                ErrorClass::Limit,
                "protocol rows exceed the recovery byte budget",
            )),
        }
    };
    loop {
        let page = view.scan_page(
            Collection::ProtocolV1.id(),
            &ScanRequest {
                lower: Bound::Included(lower.clone()),
                upper: Bound::Excluded(upper.clone()),
                direction: Direction::Forward,
                resume_after: resume.clone(),
                max_rows: budget.max_rows.max(1).try_into().expect("non-zero"),
                max_bytes: bytes_left.max(1).try_into().expect("non-zero"),
            },
        )?;
        for row in &page.rows {
            seen += 1;
            if seen > budget.max_rows {
                return Err(EngineError::new(
                    ErrorClass::Limit,
                    "protocol rows exceed the recovery budget",
                ));
            }
            charge(row.key.len() + row.value.len(), &mut bytes_left)?;
            match row.key.get(8) {
                Some(&DEPENDENCY_TAG) if row.key.len() == 41 => {
                    let mut id = [0u8; 32];
                    id.copy_from_slice(&row.key[9..]);
                    records.push((CommandId(Digest32(id)), decode_dependency(&row.value)?));
                }
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
    let mut payloads = Vec::with_capacity(records.len());
    let mut executed = Vec::new();
    for (command, record) in &records {
        if record.payload.is_some() {
            match view.get(Collection::PayloadV1.id(), &payload_key(command))? {
                Some(bytes) => {
                    charge(bytes.len(), &mut bytes_left)?;
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
        if let Some(bytes) =
            view.get(Collection::ExecutedV1.id(), &codecs::executed_key(command))?
        {
            charge(bytes.len(), &mut bytes_left)?;
            let ExecutedRecordV1 { position, .. } = codecs::decode_executed(&bytes)?;
            executed.push((*command, position));
        }
    }
    executed.sort_by_key(|(_, p)| *p);
    let history = read_history(view, &records, budget)?;
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

/// Every executed identity without a dependency row among `records`,
/// read from `executed_v1` itself (task-d05).
///
/// Paged within `budget` per page, and not charged to the recovery's
/// running total: the answer is one identity per command executed, which
/// the machine keeps in memory for the life of the process anyway (its
/// executed answer). It grows with the history until a floor lets
/// `executed_v1` forget a prefix every voter executed, which is the same
/// bound that set is waiting for.
fn read_history<V: OrderedRead>(
    view: &V,
    records: &[(CommandId, CommandRecord)],
    budget: ViewBudget,
) -> Result<Vec<CommandId>, EngineError> {
    let known: std::collections::BTreeSet<CommandId> = records.iter().map(|(c, _)| *c).collect();
    let mut history = Vec::new();
    let mut resume: Option<Vec<u8>> = None;
    loop {
        let page = view.scan_page(
            Collection::ExecutedV1.id(),
            &ScanRequest {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
                direction: Direction::Forward,
                resume_after: resume.clone(),
                max_rows: budget.max_rows.max(1).try_into().expect("non-zero"),
                max_bytes: budget.max_bytes.max(1).try_into().expect("non-zero"),
            },
        )?;
        for row in &page.rows {
            let id: [u8; 32] = row
                .key
                .as_slice()
                .try_into()
                .map_err(|_| corrupt("an executed row's key is not a command identity"))?;
            let command = CommandId(Digest32(id));
            if !known.contains(&command) {
                history.push(command);
            }
        }
        match page.rows.last() {
            Some(last) if !page.exhausted => resume = Some(last.key.clone()),
            _ => break,
        }
    }
    Ok(history)
}
