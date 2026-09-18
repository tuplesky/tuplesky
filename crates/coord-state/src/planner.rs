//! The planner.

use alloc::collections::BTreeMap;
use alloc::vec::Vec;

use coord_types::error::ValidationError;
use coord_types::ids::{KvRevision, LeaseGeneration, LeaseId};
use coord_types::logical_v1::{
    BranchOp, CanonicalOperation, Compare, CompareOperand, CompareResult, CompareTarget,
    DeleteRangeOp, KeyRange, LogicalRequest, PutOp, RangeOp,
};

use crate::internal::InternalCommand;
use crate::lease::{LeasePurpose, LeaseRecord, LeaseStatus, attachment_cost};
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
    /// The view's lease records disagree with its entries or reverse index
    /// (a lease referenced by an entry is missing, or counts do not match);
    /// rebuild the view.
    ViewInconsistent,
    /// The domain revision or execution position cannot advance.
    CounterOverflow,
    /// The operation is not planned by this planner.
    Unsupported,
}

/// Why a branch stopped: a planner error, or a recorded failure outcome
/// that discards every mutation planned so far.
enum Abort {
    Error(PlanError),
    Fail(Outcome),
}

impl From<PlanError> for Abort {
    fn from(e: PlanError) -> Self {
        Abort::Error(e)
    }
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
    leases: BTreeMap<LeaseId, LeaseRecord>,
}

impl Overlay<'_> {
    fn get(&self, key: &[u8]) -> Option<&KvEntry> {
        match self.changed.get(key) {
            Some(e) => e.as_ref(),
            None => self.view.current.get(key),
        }
    }

    fn lease(&self, id: &LeaseId) -> Option<&LeaseRecord> {
        self.leases.get(id).or_else(|| self.view.leases.get(id))
    }

    /// The lease as native operations in this namespace see it: an active
    /// native lease of this namespace, or nothing.
    fn visible_lease(&self, id: &LeaseId) -> Result<LeaseRecord, Abort> {
        match self.lease(id) {
            Some(r) if r.is_native_active() && r.namespace == self.view.namespace => Ok(r.clone()),
            _ => Err(Abort::Fail(Outcome::ErrLeaseNotFound)),
        }
    }

    /// The lease an attached entry references; it must be in the view.
    fn attached_lease(&self, id: &LeaseId) -> Result<LeaseRecord, Abort> {
        self.lease(id)
            .cloned()
            .ok_or(Abort::Error(PlanError::ViewInconsistent))
    }

    fn detach(&mut self, id: &LeaseId, entry: &KvEntry, key: &[u8]) -> Result<(), Abort> {
        let mut record = self.attached_lease(id)?;
        record.attached_keys = record
            .attached_keys
            .checked_sub(1)
            .ok_or(Abort::Error(PlanError::ViewInconsistent))?;
        record.attached_bytes = record
            .attached_bytes
            .checked_sub(attachment_cost(key, &entry.value))
            .ok_or(Abort::Error(PlanError::ViewInconsistent))?;
        self.leases.insert(*id, record);
        Ok(())
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
            let hist = view
                .historical
                .as_ref()
                .filter(|h| h.revision == rev)
                .ok_or(PlanError::ViewIncomplete)?;
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
    let limit = if r.limit == 0 {
        usize::MAX
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
    overlay: &mut Overlay<'_>,
    p: &PutOp,
    revision: KvRevision,
    limits: &PlanLimits,
    mutations: &mut Vec<Mutation>,
    events: &mut Vec<KvEvent>,
) -> Result<Outcome, Abort> {
    let prev = overlay.get(&p.key).cloned();
    let old_lease = prev.as_ref().and_then(|e| e.lease);
    let mut generation: Option<LeaseGeneration> = None;
    if let Some(lease) = p.lease {
        let record = overlay.visible_lease(&lease)?;
        if record.owner != overlay.view.principal {
            return Err(Abort::Fail(Outcome::ErrLeasePermission));
        }
        generation = Some(record.generation);
    }
    // Detach from a previous, different lease.
    if let (Some(old), Some(prev_entry)) = (old_lease, prev.as_ref())
        && Some(old) != p.lease
    {
        overlay.detach(&old, prev_entry, &p.key)?;
        mutations.push(Mutation::LeaseDetach {
            lease: old,
            key: p.key.clone(),
        });
    }
    // Attach, or recheck the quota of an already attached key whose value
    // may have grown.
    if let Some(lease) = p.lease {
        let mut record = overlay.attached_lease(&lease)?;
        let cost = attachment_cost(&p.key, &p.value);
        if old_lease == Some(lease) {
            let prev_entry = prev.as_ref().expect("attached entry exists");
            let old_cost = attachment_cost(&p.key, &prev_entry.value);
            record.attached_bytes = record
                .attached_bytes
                .checked_sub(old_cost)
                .ok_or(Abort::Error(PlanError::ViewInconsistent))?;
        } else {
            record.attached_keys = record
                .attached_keys
                .checked_add(1)
                .ok_or(Abort::Fail(Outcome::ErrLeaseQuota))?;
        }
        record.attached_bytes = record
            .attached_bytes
            .checked_add(cost)
            .ok_or(Abort::Fail(Outcome::ErrLeaseQuota))?;
        if record.attached_keys > limits.max_lease_attachments
            || record.attached_bytes > limits.max_lease_bytes
        {
            return Err(Abort::Fail(Outcome::ErrLeaseQuota));
        }
        overlay.leases.insert(lease, record);
        mutations.push(Mutation::LeaseAttach {
            lease,
            key: p.key.clone(),
            generation: generation.expect("checked above"),
            mod_revision: revision,
        });
    }
    let entry = KvEntry {
        value: p.value.clone(),
        create_revision: prev.as_ref().map_or(revision, |e| e.create_revision),
        mod_revision: revision,
        version: prev.as_ref().map_or(1, |e| e.version + 1),
        lease: p.lease,
        lease_generation: generation,
    };
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

/// Delete one current entry: detach it from its lease, tombstone it and
/// emit its event.
fn delete_entry(
    overlay: &mut Overlay<'_>,
    key: Vec<u8>,
    entry: KvEntry,
    mutations: &mut Vec<Mutation>,
    events: &mut Vec<KvEvent>,
) -> Result<(), Abort> {
    if let Some(lease) = entry.lease {
        overlay.detach(&lease, &entry, &key)?;
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
        prev: Some(entry),
    });
    overlay.changed.insert(key, None);
    Ok(())
}

fn delete(
    overlay: &mut Overlay<'_>,
    d: &DeleteRangeOp,
    limits: &PlanLimits,
    mutations: &mut Vec<Mutation>,
    events: &mut Vec<KvEvent>,
) -> Result<Outcome, Abort> {
    let selected = overlay.select(&d.range);
    if selected.len() > limits.max_delete_keys {
        return Err(PlanError::TooManyDeletes.into());
    }
    let mut prev = Vec::new();
    let mut deleted = 0u64;
    for (key, entry) in selected {
        delete_entry(overlay, key.clone(), entry.clone(), mutations, events)?;
        deleted += 1;
        if d.prev_kv {
            prev.push(RangeItem { key, entry });
        }
    }
    Ok(Outcome::Delete { deleted, prev })
}

/// The attached keys of `lease` as the view carries them, checked against
/// the record's accounting.
fn attached_keys(
    view: &ReadView,
    lease: &LeaseId,
    record: &LeaseRecord,
) -> Result<Vec<Vec<u8>>, Abort> {
    let keys: Vec<Vec<u8>> = view
        .lease_keys
        .get(lease)
        .map(|k| k.iter().cloned().collect())
        .unwrap_or_default();
    if keys.len() != record.attached_keys as usize {
        return Err(Abort::Error(if view.lease_keys.contains_key(lease) {
            PlanError::ViewInconsistent
        } else {
            PlanError::ViewIncomplete
        }));
    }
    Ok(keys)
}

fn grant(overlay: &mut Overlay<'_>, lease_id: LeaseId, ttl_seconds: u32) -> Result<Outcome, Abort> {
    if overlay.lease(&lease_id).is_some() {
        return Err(Abort::Fail(Outcome::ErrLeaseExists));
    }
    let generation = LeaseGeneration::new(1).expect("one is valid");
    overlay.leases.insert(
        lease_id,
        LeaseRecord {
            namespace: overlay.view.namespace,
            generation,
            owner: overlay.view.principal,
            ttl_seconds,
            renewal_sequence: 0,
            purpose: LeasePurpose::Native,
            status: LeaseStatus::Active,
            attached_keys: 0,
            attached_bytes: 0,
        },
    );
    Ok(Outcome::LeaseGranted {
        lease_id,
        generation,
        ttl_seconds,
    })
}

fn revoke(
    overlay: &mut Overlay<'_>,
    lease_id: LeaseId,
    mutations: &mut Vec<Mutation>,
    events: &mut Vec<KvEvent>,
) -> Result<Outcome, Abort> {
    let record = overlay.visible_lease(&lease_id)?;
    if record.owner != overlay.view.principal {
        return Err(Abort::Fail(Outcome::ErrLeasePermission));
    }
    let deleted = end_lease(
        overlay,
        lease_id,
        &record,
        LeaseStatus::Revoked,
        mutations,
        events,
    )?;
    Ok(Outcome::LeaseRevoked { deleted })
}

fn time_to_live(
    overlay: &Overlay<'_>,
    lease_id: LeaseId,
    keys: bool,
    limits: &PlanLimits,
) -> Result<Outcome, Abort> {
    let record = overlay.visible_lease(&lease_id)?;
    if record.owner != overlay.view.principal {
        return Err(Abort::Fail(Outcome::ErrLeasePermission));
    }
    let keys = if keys {
        let keys = attached_keys(overlay.view, &lease_id, &record)?;
        let cost: usize = keys.iter().map(|k| k.len() + 8).sum();
        if cost > limits.max_response_bytes {
            return Err(PlanError::ResponseTooLarge.into());
        }
        Some(keys)
    } else {
        None
    };
    Ok(Outcome::LeaseTimeToLive {
        lease_id,
        generation: record.generation,
        granted_ttl_seconds: record.ttl_seconds,
        renewal_sequence: record.renewal_sequence,
        keys,
    })
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

fn plan_operation(
    request: &LogicalRequest,
    view: &ReadView,
    limits: &PlanLimits,
    overlay: &mut Overlay<'_>,
    next_revision: KvRevision,
    mutations: &mut Vec<Mutation>,
    events: &mut Vec<KvEvent>,
) -> Result<Outcome, Abort> {
    Ok(match &request.operation {
        CanonicalOperation::Range(r) => read(view, overlay, r, limits)?,
        CanonicalOperation::Put(p) => put(overlay, p, next_revision, limits, mutations, events)?,
        CanonicalOperation::DeleteRange(d) => delete(overlay, d, limits, mutations, events)?,
        CanonicalOperation::Txn(t) => {
            let succeeded = t.compares.iter().all(|c| compare(overlay, c));
            let branch = if succeeded { &t.success } else { &t.failure };
            let mut results = Vec::with_capacity(branch.len());
            for op in branch {
                results.push(match op {
                    BranchOp::Range(r) => read(view, overlay, r, limits)?,
                    BranchOp::Put(p) => put(overlay, p, next_revision, limits, mutations, events)?,
                    BranchOp::DeleteRange(d) => delete(overlay, d, limits, mutations, events)?,
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
        CanonicalOperation::LeaseGrant {
            lease_id,
            ttl_seconds,
        } => grant(overlay, *lease_id, *ttl_seconds)?,
        CanonicalOperation::LeaseRevoke { lease_id } => {
            revoke(overlay, *lease_id, mutations, events)?
        }
        CanonicalOperation::LeaseTimeToLive { lease_id, keys } => {
            time_to_live(overlay, *lease_id, *keys, limits)?
        }
        CanonicalOperation::LeaseKeepAlive { lease_id } => keep_alive(overlay, *lease_id)?,
    })
}

fn keep_alive(overlay: &mut Overlay<'_>, lease_id: LeaseId) -> Result<Outcome, Abort> {
    let mut record = overlay.visible_lease(&lease_id)?;
    if record.owner != overlay.view.principal {
        return Err(Abort::Fail(Outcome::ErrLeasePermission));
    }
    record.renewal_sequence = record
        .renewal_sequence
        .checked_add(1)
        .ok_or(Abort::Error(PlanError::CounterOverflow))?;
    let outcome = Outcome::LeaseKeptAlive {
        lease_id,
        generation: record.generation,
        renewal_sequence: record.renewal_sequence,
        ttl_seconds: record.ttl_seconds,
    };
    overlay.leases.insert(lease_id, record);
    Ok(outcome)
}

/// End a lease that matched its expiration conditions: delete exactly the
/// current attachments and mark the record.
fn end_lease(
    overlay: &mut Overlay<'_>,
    lease_id: LeaseId,
    record: &LeaseRecord,
    status: LeaseStatus,
    mutations: &mut Vec<Mutation>,
    events: &mut Vec<KvEvent>,
) -> Result<u64, Abort> {
    let keys = attached_keys(overlay.view, &lease_id, record)?;
    let mut deleted = 0u64;
    for key in keys {
        // Only a current attachment is deleted: an entry that no longer
        // references this lease means the index and entries disagree.
        let entry = overlay
            .get(&key)
            .filter(|e| e.lease == Some(lease_id))
            .cloned()
            .ok_or(Abort::Error(PlanError::ViewInconsistent))?;
        delete_entry(overlay, key, entry, mutations, events)?;
        deleted += 1;
    }
    let mut record = overlay.attached_lease(&lease_id)?;
    if record.attached_keys != 0 || record.attached_bytes != 0 {
        return Err(Abort::Error(PlanError::ViewInconsistent));
    }
    record.status = status;
    overlay.leases.insert(lease_id, record);
    Ok(deleted)
}

fn plan_internal_command(
    command: &InternalCommand,
    overlay: &mut Overlay<'_>,
    mutations: &mut Vec<Mutation>,
    events: &mut Vec<KvEvent>,
) -> Result<Outcome, Abort> {
    let view = overlay.view;
    match command {
        InternalCommand::EstablishLeaseAuthority { epoch, .. } => {
            if *epoch <= view.lease_authority {
                return Err(Abort::Fail(Outcome::ErrStaleAuthority));
            }
            mutations.push(Mutation::LeaseAuthority { epoch: *epoch });
            Ok(Outcome::LeaseAuthorityEstablished { epoch: *epoch })
        }
        InternalCommand::ExpireLease {
            lease_id,
            generation,
            expected_renewal_sequence,
            authority_epoch,
            ..
        } => {
            if *authority_epoch != view.lease_authority {
                return Err(Abort::Fail(Outcome::ErrStaleAuthority));
            }
            let record = match overlay.lease(lease_id) {
                Some(r) if r.status == LeaseStatus::Active => r.clone(),
                _ => return Err(Abort::Fail(Outcome::ExpireStale)),
            };
            if record.namespace != view.namespace {
                return Err(Abort::Error(PlanError::NamespaceMismatch));
            }
            if record.generation != *generation
                || record.renewal_sequence != *expected_renewal_sequence
            {
                return Err(Abort::Fail(Outcome::ExpireStale));
            }
            let deleted = end_lease(
                overlay,
                *lease_id,
                &record,
                LeaseStatus::Expired,
                mutations,
                events,
            )?;
            Ok(Outcome::LeaseExpired { deleted })
        }
    }
}

/// Plan `request` against `view` under `limits`.
///
/// Returns a plan or an error; the error cases leave no partial state
/// because planning never mutates. The returned plan's `revision` is
/// `Some` only when KV actually changes. Lease failures (`ErrLease*`) are
/// plans with a recorded outcome and no mutations.
pub fn plan(
    request: &LogicalRequest,
    view: &ReadView,
    limits: &PlanLimits,
) -> Result<ApplyPlan, PlanError> {
    request.validate()?;
    if request.namespace != view.namespace {
        return Err(PlanError::NamespaceMismatch);
    }
    finish(view, limits, |overlay, next_revision, mutations, events| {
        plan_operation(
            request,
            view,
            limits,
            overlay,
            next_revision,
            mutations,
            events,
        )
    })
}

/// Plan an internal replicated command against `view`. Same contract as
/// [`plan`]: a stale or mismatching command yields a recorded no-op
/// outcome with no mutations.
pub fn plan_internal(
    command: &InternalCommand,
    view: &ReadView,
    limits: &PlanLimits,
) -> Result<ApplyPlan, PlanError> {
    if command.namespace() != view.namespace {
        return Err(PlanError::NamespaceMismatch);
    }
    finish(view, limits, |overlay, _, mutations, events| {
        plan_internal_command(command, overlay, mutations, events)
    })
}

type Planned = Result<Outcome, Abort>;

fn finish(
    view: &ReadView,
    limits: &PlanLimits,
    body: impl FnOnce(&mut Overlay<'_>, KvRevision, &mut Vec<Mutation>, &mut Vec<KvEvent>) -> Planned,
) -> Result<ApplyPlan, PlanError> {
    let position = view
        .base
        .execution_position
        .checked_next()
        .map_err(|_| PlanError::CounterOverflow)?;
    let next_revision = view
        .kv_revision
        .checked_next()
        .map_err(|_| PlanError::CounterOverflow)?;
    let mut overlay = Overlay {
        view,
        changed: BTreeMap::new(),
        leases: BTreeMap::new(),
    };
    let mut mutations = Vec::new();
    let mut events = Vec::new();
    let outcome = match body(&mut overlay, next_revision, &mut mutations, &mut events) {
        Ok(outcome) => outcome,
        Err(Abort::Error(e)) => return Err(e),
        Err(Abort::Fail(outcome)) => {
            // A recorded failure: nothing changes, execution still advances.
            return Ok(ApplyPlan {
                base: view.base,
                position,
                revision: None,
                mutations: Vec::new(),
                events: Vec::new(),
                response: Response {
                    revision: view.kv_revision,
                    outcome,
                },
            });
        }
    };
    for (lease, record) in overlay.leases {
        mutations.push(Mutation::LeaseWrite { lease, record });
    }
    if events.len() > limits.max_events_per_revision {
        return Err(PlanError::TooManyEvents);
    }
    let response_bytes: usize = events
        .iter()
        .map(|e| {
            e.key.len()
                + e.entry.as_ref().map_or(0, |x| x.value.len())
                + e.prev.as_ref().map_or(0, |x| x.value.len())
        })
        .sum();
    if response_bytes > limits.max_response_bytes {
        return Err(PlanError::ResponseTooLarge);
    }
    let mutates = !events.is_empty();
    let revision = if mutates { Some(next_revision) } else { None };
    let header = if mutates {
        next_revision
    } else {
        view.kv_revision
    };
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
            | Mutation::LeaseWrite { .. }
            | Mutation::LeaseAuthority { .. }
            | Mutation::CompactTo { .. } => {}
        }
    }
}

/// Apply a plan's lease record writes to an in-memory lease map (the same
/// shape as `ReadView::leases`). Fixture/test companion of [`apply_to_map`].
pub fn apply_leases(leases: &mut BTreeMap<LeaseId, LeaseRecord>, plan: &ApplyPlan) {
    for m in &plan.mutations {
        if let Mutation::LeaseWrite { lease, record } = m {
            leases.insert(*lease, record.clone());
        }
    }
}
