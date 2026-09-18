//! Speculation companion of the applier (task-29; design Sections 4.5,
//! 17.4): tentative results over a disposable overlay.
//!
//! The leader asks for the result a proposal would produce if every
//! earlier unexecuted proposal executed first, in its order. The companion
//! builds the ordinary authorized view at the durable snapshot, replays
//! the tentative plans of that prefix onto it (a copy-on-write overlay:
//! the durable state is never touched), runs the same pure planner, and
//! returns the exact response, its digest and the position. Nothing here
//! publishes events, writes rows, signs anything or advances a frontier;
//! the overlay is dropped when its command executes or the role changes.
//!
//! Only KV operations are speculated: reads at the current revision,
//! puts without a lease, deletes and transactions of those. Leases,
//! explicit-revision reads, compaction, Kine primitives and internal
//! commands take the ordinary finalized path, as does any invocation the
//! retry layer would not admit as new work (a retained result is served
//! by the applier, never re-planned). The overlay is bounded in commands
//! and bytes; beyond the bound the chain stops until it drains.

use std::collections::BTreeMap;

use coord_consensus::{PayloadRecordV1, SpeculationRequest, TentativeOutcome};
use coord_state::planner::apply_to_map;
use coord_state::{ApplyPlan, Mutation, PlanError, PlanLimits, plan};
use coord_store_api::engine::{EngineError, LocalEngine};
use coord_types::CommandId;
use coord_types::logical_v1::{BranchOp, CanonicalOperation, LogicalRequest, PutOp, RangeOp};

use crate::retry::{self, Admission, RetryBinding};
use crate::view::ViewError;
use crate::views::{ViewBudget, ViewBuildError, build_authorized_view};
use crate::worker::StoreWorker;

/// Bounds of one overlay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpeculationLimits {
    /// Maximum tentative plans held.
    pub max_commands: usize,
    /// Maximum bytes of tentative mutations held.
    pub max_bytes: usize,
}

impl Default for SpeculationLimits {
    fn default() -> Self {
        SpeculationLimits {
            max_commands: coord_consensus::DEFAULT_SPECULATION_BOUND,
            max_bytes: 4 << 20,
        }
    }
}

/// The tentative plans of unexecuted proposals, in the leader's order.
#[derive(Debug, Default)]
pub struct Overlay {
    plans: BTreeMap<CommandId, ApplyPlan>,
    bytes: usize,
}

impl Overlay {
    /// Empty overlay.
    pub fn new() -> Self {
        Overlay::default()
    }

    /// Plans held.
    pub fn len(&self) -> usize {
        self.plans.len()
    }

    /// Whether nothing is held.
    pub fn is_empty(&self) -> bool {
        self.plans.is_empty()
    }

    /// Bytes of tentative mutations held.
    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    /// Drop the plan of a command that executed (or whose proposal is
    /// gone).
    pub fn retire(&mut self, command: &CommandId) {
        if let Some(p) = self.plans.remove(command) {
            self.bytes = self.bytes.saturating_sub(plan_bytes(&p));
        }
    }

    /// Drop everything (role change).
    pub fn clear(&mut self) {
        self.plans.clear();
        self.bytes = 0;
    }
}

/// Why a command was not speculated: it takes the ordinary finalized
/// path.
#[derive(Debug)]
pub enum SpeculationRefused {
    /// Not a speculable operation (lease, explicit revision, compaction,
    /// Kine, internal).
    NotSpeculable,
    /// A prefix command has no tentative plan here.
    PrefixUnknown(CommandId),
    /// The overlay is at its bound.
    OverBudget,
    /// The retry layer would not treat the invocation as new work.
    NotAdmitted(Admission),
    /// The view could not be built.
    View(ViewBuildError),
    /// The planner refused.
    Plan(PlanError),
    /// No durable view is available.
    Unavailable(ViewError),
    /// Engine failure.
    Engine(EngineError),
    /// The payload does not decode, or rehashes to another identity.
    MalformedPayload,
}

impl From<EngineError> for SpeculationRefused {
    fn from(e: EngineError) -> Self {
        SpeculationRefused::Engine(e)
    }
}

impl From<ViewError> for SpeculationRefused {
    fn from(e: ViewError) -> Self {
        SpeculationRefused::Unavailable(e)
    }
}

fn put_speculable(p: &PutOp) -> bool {
    p.lease.is_none()
}

fn range_speculable(r: &RangeOp) -> bool {
    r.revision.is_none()
}

/// Whether the planner's result for `op` depends only on the KV entries
/// of its keys at the current revision.
pub fn speculable(op: &CanonicalOperation) -> bool {
    match op {
        CanonicalOperation::Put(p) => put_speculable(p),
        CanonicalOperation::DeleteRange(_) => true,
        CanonicalOperation::Range(r) => range_speculable(r),
        CanonicalOperation::Txn(t) => t.success.iter().chain(t.failure.iter()).all(|b| match b {
            BranchOp::Put(p) => put_speculable(p),
            BranchOp::DeleteRange(_) => true,
            BranchOp::Range(r) => range_speculable(r),
        }),
        _ => false,
    }
}

fn plan_bytes(plan: &ApplyPlan) -> usize {
    plan.mutations
        .iter()
        .map(|m| match m {
            Mutation::Write { key, entry } => key.len() + entry.value.len(),
            Mutation::Delete { key, .. } => key.len(),
            _ => 0,
        })
        .sum::<usize>()
        + plan.response_bytes_hint()
}

trait ResponseHint {
    fn response_bytes_hint(&self) -> usize;
}

impl ResponseHint for ApplyPlan {
    fn response_bytes_hint(&self) -> usize {
        self.events.len() * 32
    }
}

/// Compute the tentative outcome of `request.command` over the overlay.
/// The overlay gains the plan on success; a refusal leaves it unchanged.
pub fn speculate<E: LocalEngine>(
    worker: &StoreWorker<E>,
    overlay: &mut Overlay,
    limits: &SpeculationLimits,
    request: &SpeculationRequest,
    payload: &PayloadRecordV1,
) -> Result<TentativeOutcome, SpeculationRefused> {
    let logical: LogicalRequest =
        postcard::from_bytes(&payload.logical).map_err(|_| SpeculationRefused::MalformedPayload)?;
    let actual = CommandId::derive(&payload.retry_key, &logical)
        .map_err(|_| SpeculationRefused::MalformedPayload)?;
    if actual != request.command {
        return Err(SpeculationRefused::MalformedPayload);
    }
    if !speculable(&logical.operation) {
        return Err(SpeculationRefused::NotSpeculable);
    }
    for p in &request.prefix {
        if !overlay.plans.contains_key(p) {
            return Err(SpeculationRefused::PrefixUnknown(*p));
        }
    }
    if overlay.plans.len() >= limits.max_commands || overlay.bytes >= limits.max_bytes {
        return Err(SpeculationRefused::OverBudget);
    }
    let binding = RetryBinding {
        retry_key: payload.retry_key,
        command_id: request.command,
    };
    let gated = worker.reader().snapshot()?;
    match retry::admit(gated.view(), &binding)? {
        Admission::New => {}
        other => return Err(SpeculationRefused::NotAdmitted(other)),
    }
    let mut view = build_authorized_view(
        &gated,
        logical.namespace,
        &binding.retry_key.session_id,
        &logical,
        ViewBudget::default(),
    )
    .map_err(SpeculationRefused::View)?;
    drop(gated);
    // Replay the prefix onto the view: current entries and the revision
    // counter; the authorization context is untouched because no
    // speculable operation changes it.
    for p in &request.prefix {
        let plan = &overlay.plans[p];
        apply_to_map(&mut view.current, plan);
        if let Some(r) = plan.revision {
            view.kv_revision = r;
        }
    }
    view.base.execution_position = request
        .position
        .get()
        .checked_sub(1)
        .and_then(|p| coord_types::ids::ExecutionPosition::new(p).ok())
        .ok_or(SpeculationRefused::NotSpeculable)?;
    let planned =
        plan(&logical, &view, &PlanLimits::default()).map_err(SpeculationRefused::Plan)?;
    if planned.position != request.position {
        return Err(SpeculationRefused::NotSpeculable);
    }
    let response = postcard::to_allocvec(&planned.response)
        .map_err(|_| SpeculationRefused::MalformedPayload)?;
    let outcome = TentativeOutcome {
        command: request.command,
        position: planned.position,
        revision: planned.revision,
        result_digest: retry::result_digest(&response),
        response,
        prefix: request.prefix.clone(),
    };
    overlay.bytes += plan_bytes(&planned);
    overlay.plans.insert(request.command, planned);
    Ok(outcome)
}
