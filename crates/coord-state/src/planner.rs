//! The planner.

use alloc::collections::BTreeMap;
use alloc::vec::Vec;

use coord_types::error::ValidationError;
use coord_types::ids::{KvRevision, LeaseId};
use coord_types::logical_v1::{
    BranchOp, CanonicalOperation, Compare, CompareOperand, CompareResult, CompareTarget,
    DeleteRangeOp, KeyRange, LogicalRequest, PutOp, RangeOp, limits,
};

use crate::limits::PlanLimits;
use crate::plan::{ApplyPlan, KvEvent, KvEventKind, Mutation, Outcome, RangeItem, Response};
use crate::view::{KvEntry, ReadView};

/// Why no plan was produced. Nothing was changed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanError {
    /// The request violates the frozen schema (limits, rules, canonical form).
    Invalid(ValidationError),
    /// The request's namespace differs from the view's.
    NamespaceMismatch,
    /// The view lacks the historical snapshot the request needs; rebuild it.
    ViewIncomplete,
    /// The response would exceed the semantic response budget.
    ResponseTooLarge,
    /// The mutation would exceed the events-per-revision budget.
    TooManyEvents,
    /// The range delete would remove more keys than allowed.
    TooManyDeletes,
    /// Attaching to a lease that does not exist.
    LeaseNotFound,
    /// The domain revision or execution position cannot advance.
    CounterOverflow,
    /// Lease operations are planned by the lease planner (task-15).
    Unsupported,
}

impl From<ValidationError> for PlanError {
    fn from(e: ValidationError) -> Self {
        PlanError::Invalid(e)
    }
}

/// Working overlay used while planning a branch: entries changed so far.
struct Overlay<'v> {
    view: &'v ReadView,
    changed: BTreeMap<Vec<u8>, Option<KvEntry>>,
}

impl Overlay<'_> {
    fn get(&self, key: &[u8]) -> Option<&KvEntry> {
        match self.changed.get(key) {
            Some(e) => e.as_ref(),
            None => self.view.current.get(key),
        }
    }

    fn select(&self, range: &KeyRange) -> Vec<(Vec<u8>, KvEntry)> {
        let in_range = |k: &[u8]| match &range.range_end {
            None => k == range.key.as_slice(),
            Some(end) => k >= range.key.as_slice() && k < end.as_slice(),
        };
        let mut out: BTreeMap<Vec<u8>, KvEntry> = self
            .view
            .current
            .iter()
            .filter(|(k, _)| in_range(k))
            .map(|(k, e)| (k.clone(), e.clone()))
            .collect();
        for (k, e) in &self.changed {
            if !in_range(k) {
                continue;
            }
            match e {
                Some(e) => {
                    out.insert(k.clone(), e.clone());
                }
                None => {
                    out.remove(k);
                }
            }
        }
        out.into_iter().collect()
    }
}

fn response_cost(items: &[RangeItem]) -> usize {
    items
        .iter()
        .map(|i| i.key.len() + i.entry.value.len() + 48)
        .sum()
}

/// Estimated encoded size of a complete outcome, transaction results
/// included, so the response budget applies to the whole response and not
/// only to each range result on its own.
fn outcome_cost(outcome: &Outcome) -> usize {
    match outcome {
        Outcome::Put { prev } => prev.as_ref().map_or(0, |e| e.value.len() + 48),
        Outcome::Delete { prev, .. } => response_cost(prev),
        Outcome::Range { items, .. } => response_cost(items),
        Outcome::Txn { results, .. } => results.iter().map(outcome_cost).sum(),
        Outcome::Compacted | Outcome::ErrCompacted | Outcome::ErrFutureRevision => 0,
    }
}

fn read(
    view: &ReadView,
    overlay: &Overlay<'_>,
    r: &RangeOp,
    limits: &PlanLimits,
) -> Result<Outcome, PlanError> {
    let items: Vec<RangeItem> = match r.revision {
        None => overlay
            .select(&r.range)
            .into_iter()
            .map(|(key, entry)| RangeItem { key, entry })
            .collect(),
        Some(rev) => {
            if rev > view.kv_revision {
                return Ok(Outcome::ErrFutureRevision);
            }
            if rev < view.compact_floor {
                return Ok(Outcome::ErrCompacted);
            }
            let hist = view.historical_at(rev).ok_or(PlanError::ViewIncomplete)?;
            let in_range = |k: &[u8]| match &r.range.range_end {
                None => k == r.range.key.as_slice(),
                Some(end) => k >= r.range.key.as_slice() && k < end.as_slice(),
            };
            hist.entries
                .iter()
                .filter(|(k, _)| in_range(k))
                .map(|(k, e)| RangeItem {
                    key: k.clone(),
                    entry: e.clone(),
                })
                .collect()
        }
    };
    let count = items.len() as u64;
    if r.count_only {
        return Ok(Outcome::Range {
            items: Vec::new(),
            count,
            more: false,
        });
    }
    // Zero selects the schema maximum page (`RangeOp` contract), never an
    // unbounded page.
    let limit = if r.limit == 0 {
        limits::MAX_PAGE_LIMIT as usize
    } else {
        r.limit as usize
    };
    let more = items.len() > limit;
    let mut items: Vec<RangeItem> = items.into_iter().take(limit).collect();
    if r.keys_only {
        for item in &mut items {
            item.entry.value.clear();
        }
    }
    if response_cost(&items) > limits.max_response_bytes {
        return Err(PlanError::ResponseTooLarge);
    }
    Ok(Outcome::Range { items, count, more })
}

fn put(
    view: &ReadView,
    overlay: &mut Overlay<'_>,
    p: &PutOp,
    revision: KvRevision,
    mutations: &mut Vec<Mutation>,
    events: &mut Vec<KvEvent>,
) -> Result<Outcome, PlanError> {
    if let Some(lease) = p.lease
        && !view.leases.contains(&lease)
    {
        return Err(PlanError::LeaseNotFound);
    }
    let prev = overlay.get(&p.key).cloned();
    let entry = KvEntry {
        value: p.value.clone(),
        create_revision: prev.as_ref().map_or(revision, |e| e.create_revision),
        mod_revision: revision,
        version: prev.as_ref().map_or(1, |e| e.version + 1),
        lease: p.lease,
        lease_generation: None,
    };
    if let Some(old) = prev.as_ref().and_then(|e| e.lease)
        && Some(old) != p.lease
    {
        mutations.push(Mutation::LeaseDetach {
            lease: old,
            key: p.key.clone(),
        });
    }
    if let Some(lease) = p.lease
        && prev.as_ref().and_then(|e| e.lease) != Some(lease)
    {
        mutations.push(Mutation::LeaseAttach {
            lease,
            key: p.key.clone(),
        });
    }
    mutations.push(Mutation::Write {
        key: p.key.clone(),
        entry: entry.clone(),
    });
    events.push(KvEvent {
        kind: KvEventKind::Put,
        key: p.key.clone(),
        entry: Some(entry.clone()),
        prev: prev.clone(),
    });
    overlay.changed.insert(p.key.clone(), Some(entry));
    Ok(Outcome::Put {
        prev: if p.prev_kv { prev } else { None },
    })
}

fn delete(
    overlay: &mut Overlay<'_>,
    d: &DeleteRangeOp,
    limits: &PlanLimits,
    mutations: &mut Vec<Mutation>,
    events: &mut Vec<KvEvent>,
) -> Result<Outcome, PlanError> {
    let selected = overlay.select(&d.range);
    if selected.len() > limits.max_delete_keys {
        return Err(PlanError::TooManyDeletes);
    }
    let mut prev = Vec::new();
    let mut deleted = 0u64;
    for (key, entry) in selected {
        if let Some(lease) = entry.lease {
            mutations.push(Mutation::LeaseDetach {
                lease,
                key: key.clone(),
            });
        }
        mutations.push(Mutation::Delete {
            key: key.clone(),
            prev: entry.clone(),
        });
        events.push(KvEvent {
            kind: KvEventKind::Delete,
            key: key.clone(),
            entry: None,
            prev: Some(entry.clone()),
        });
        overlay.changed.insert(key.clone(), None);
        deleted += 1;
        if d.prev_kv {
            prev.push(RangeItem { key, entry });
        }
    }
    Ok(Outcome::Delete { deleted, prev })
}

fn compare(overlay: &Overlay<'_>, c: &Compare) -> bool {
    let entry = overlay.get(&c.key);
    let ordering = match (&c.target, &c.operand) {
        (CompareTarget::Version, CompareOperand::Counter(v)) => {
            entry.map_or(0, |e| e.version).cmp(v)
        }
        (CompareTarget::CreateRevision, CompareOperand::Counter(v)) => {
            entry.map_or(0, |e| e.create_revision.get()).cmp(v)
        }
        (CompareTarget::ModRevision, CompareOperand::Counter(v)) => {
            entry.map_or(0, |e| e.mod_revision.get()).cmp(v)
        }
        (CompareTarget::Value, CompareOperand::Bytes(b)) => entry
            .map_or(&[][..], |e| e.value.as_slice())
            .cmp(b.as_slice()),
        (CompareTarget::Lease, CompareOperand::Lease(l)) => entry.and_then(|e| e.lease).cmp(l),
        _ => return false,
    };
    match c.result {
        CompareResult::Equal => ordering.is_eq(),
        CompareResult::Greater => ordering.is_gt(),
        CompareResult::Less => ordering.is_lt(),
        CompareResult::NotEqual => ordering.is_ne(),
    }
}

/// Plan `request` against `view` under `limits`.
///
/// Returns a plan or an error; the error cases leave no partial state
/// because planning never mutates. The returned plan's `revision` is
/// `Some` only when KV actually changes.
pub fn plan(
    request: &LogicalRequest,
    view: &ReadView,
    limits: &PlanLimits,
) -> Result<ApplyPlan, PlanError> {
    request.validate()?;
    if request.namespace != view.namespace {
        return Err(PlanError::NamespaceMismatch);
    }
    let position = view
        .base
        .execution_position
        .checked_next()
        .map_err(|_| PlanError::CounterOverflow)?;
    // The next KV revision is only needed by a branch that emits events;
    // reads, compaction, empty deletes and read-only transactions still plan
    // at `KvRevision::MAX` because they consume no revision.
    let next_revision = view.kv_revision.checked_next().ok();
    let mut overlay = Overlay {
        view,
        changed: BTreeMap::new(),
    };
    let mut mutations = Vec::new();
    let mut events = Vec::new();
    let outcome = match &request.operation {
        CanonicalOperation::Range(r) => read(view, &overlay, r, limits)?,
        CanonicalOperation::Put(p) => put(
            view,
            &mut overlay,
            p,
            next_revision.ok_or(PlanError::CounterOverflow)?,
            &mut mutations,
            &mut events,
        )?,
        CanonicalOperation::DeleteRange(d) => {
            delete(&mut overlay, d, limits, &mut mutations, &mut events)?
        }
        CanonicalOperation::Txn(t) => {
            let succeeded = t.compares.iter().all(|c| compare(&overlay, c));
            let branch = if succeeded { &t.success } else { &t.failure };
            let mut results = Vec::with_capacity(branch.len());
            for op in branch {
                results.push(match op {
                    BranchOp::Range(r) => read(view, &overlay, r, limits)?,
                    BranchOp::Put(p) => put(
                        view,
                        &mut overlay,
                        p,
                        next_revision.ok_or(PlanError::CounterOverflow)?,
                        &mut mutations,
                        &mut events,
                    )?,
                    BranchOp::DeleteRange(d) => {
                        delete(&mut overlay, d, limits, &mut mutations, &mut events)?
                    }
                });
            }
            Outcome::Txn { succeeded, results }
        }
        CanonicalOperation::Compact { revision } => {
            let target = (*revision).min(view.kv_revision);
            if target > view.compact_floor {
                mutations.push(Mutation::CompactTo { revision: target });
            }
            Outcome::Compacted
        }
        CanonicalOperation::LeaseGrant { .. }
        | CanonicalOperation::LeaseKeepAlive { .. }
        | CanonicalOperation::LeaseRevoke { .. }
        | CanonicalOperation::LeaseTimeToLive { .. } => return Err(PlanError::Unsupported),
    };
    if events.len() > limits.max_events_per_revision {
        return Err(PlanError::TooManyEvents);
    }
    let event_bytes: usize = events
        .iter()
        .map(|e| {
            e.key.len()
                + e.entry.as_ref().map_or(0, |x| x.value.len())
                + e.prev.as_ref().map_or(0, |x| x.value.len())
        })
        .sum();
    // The budget covers the complete response: every transaction result
    // plus the events, not each range result on its own.
    if outcome_cost(&outcome).saturating_add(event_bytes) > limits.max_response_bytes {
        return Err(PlanError::ResponseTooLarge);
    }
    let mutates = !events.is_empty();
    let revision = if mutates {
        Some(next_revision.ok_or(PlanError::CounterOverflow)?)
    } else {
        None
    };
    let header = revision.unwrap_or(view.kv_revision);
    Ok(ApplyPlan {
        base: view.base,
        position,
        revision,
        mutations,
        events,
        response: Response {
            revision: header,
            outcome,
        },
    })
}

/// Apply a plan to an in-memory map of current entries (the same shape as
/// `ReadView::current`). Used by fixtures and tests to derive the next view
/// without an engine; common storage performs the real lowering.
pub fn apply_to_map(current: &mut BTreeMap<Vec<u8>, KvEntry>, plan: &ApplyPlan) {
    for m in &plan.mutations {
        match m {
            Mutation::Write { key, entry } => {
                current.insert(key.clone(), entry.clone());
            }
            Mutation::Delete { key, .. } => {
                current.remove(key);
            }
            Mutation::LeaseAttach { .. }
            | Mutation::LeaseDetach { .. }
            | Mutation::CompactTo { .. } => {}
        }
    }
}

/// Convenience for building lease sets in fixtures.
pub fn lease_set(leases: &[LeaseId]) -> alloc::collections::BTreeSet<LeaseId> {
    leases.iter().copied().collect()
}
