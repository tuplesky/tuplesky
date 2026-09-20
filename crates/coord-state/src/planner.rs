//! The planner.

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::vec;
use alloc::vec::Vec;

use coord_core::effect::ApplyBase;
use coord_types::error::ValidationError;
use coord_types::ids::{KvRevision, LeaseAuthorityEpoch, LeaseGeneration, LeaseId, NamespaceId};
use coord_types::logical_v1::{
    BranchOp, CanonicalOperation, Compare, CompareOperand, CompareResult, CompareTarget,
    DeleteRangeOp, KeyRange, KineCreateOp, KineDeleteOp, KineUpdateOp, LogicalRequest, PutOp,
    RangeOp, limits,
};

use crate::internal::InternalCommand;
use crate::lease::{LeasePurpose, LeaseRecord, LeaseStatus, attachment_cost};
use crate::limits::PlanLimits;
use crate::plan::{
    ApplyPlan, KineKv, KvEvent, KvEventKind, Mutation, Outcome, RangeItem, RejectionReason,
    Response,
};
use crate::policy::{
    Action, Authorization, GrantKind, GrantRecord, GrantState, KeyInterval, SessionRecord,
};
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

impl PlanError {
    /// The terminal rejection this error records, if it is one: a
    /// deterministic property of the request and the state rather than a
    /// view that must be rebuilt. A terminal rejection is the command's
    /// result (see [`rejection_plan`]); the other errors are the caller's
    /// to retry with a fresh view.
    pub const fn terminal(self) -> Option<RejectionReason> {
        match self {
            PlanError::Invalid(_) => Some(RejectionReason::Invalid),
            PlanError::NamespaceMismatch => Some(RejectionReason::NamespaceMismatch),
            PlanError::ResponseTooLarge => Some(RejectionReason::ResponseTooLarge),
            PlanError::TooManyEvents => Some(RejectionReason::TooManyEvents),
            PlanError::TooManyDeletes => Some(RejectionReason::TooManyDeletes),
            PlanError::CounterOverflow => Some(RejectionReason::CounterOverflow),
            PlanError::Unsupported => Some(RejectionReason::Unsupported),
            // The view is wrong, not the request.
            PlanError::ViewIncomplete | PlanError::ViewInconsistent => None,
        }
    }
}

/// The plan of a command that was chosen to execute and is then rejected
/// for `reason`: it takes its execution position and produces a durable,
/// retry-resolvable result, and changes nothing. Without it a chosen
/// command that no state can satisfy would block every successor.
pub fn rejection_plan(view: &ReadView, reason: RejectionReason) -> Result<ApplyPlan, PlanError> {
    rejection_plan_at(view.base, view.kv_revision, reason)
}

/// The same rejection built from the base and revision alone, for a
/// command that cannot even be given a view: the state it would have to
/// read exceeds the schema's budget, so there is nothing to plan against
/// and the rejection is the whole result.
pub fn rejection_plan_at(
    base: ApplyBase,
    kv_revision: KvRevision,
    reason: RejectionReason,
) -> Result<ApplyPlan, PlanError> {
    let position = base
        .execution_position
        .checked_next()
        .map_err(|_| PlanError::CounterOverflow)?;
    Ok(ApplyPlan {
        base,
        position,
        revision: None,
        mutations: Vec::new(),
        events: Vec::new(),
        response: Response {
            revision: kv_revision,
            outcome: Outcome::ErrRejected { reason },
        },
    })
}

/// Why a branch stopped: a planner error, or a recorded failure outcome
/// that discards every mutation planned so far.
enum Abort {
    Error(PlanError),
    Fail(Box<Outcome>),
}

impl From<PlanError> for Abort {
    fn from(e: PlanError) -> Self {
        Abort::Error(e)
    }
}

/// A recorded failure outcome that aborts the plan.
fn fail(outcome: Outcome) -> Abort {
    Abort::Fail(Box::new(outcome))
}

fn interval_of(range: &KeyRange) -> KeyInterval {
    match &range.range_end {
        None => KeyInterval::exact(&range.key),
        Some(end) => KeyInterval {
            lower: range.key.clone(),
            upper: Some(end.clone()),
        },
    }
}

/// Require `action` over `interval` under `auth`; a trusted context (no
/// authorization) permits everything.
fn check_permission(
    auth: Option<&Authorization>,
    namespace: &NamespaceId,
    action: Action,
    interval: &KeyInterval,
) -> Result<(), Abort> {
    let Some(auth) = auth else {
        return Ok(());
    };
    if auth.valid_session().is_none() {
        return Err(fail(Outcome::ErrSessionInvalid));
    }
    if !auth.permits(namespace, action, interval) {
        return Err(fail(Outcome::ErrPermissionDenied));
    }
    Ok(())
}

/// Require `action` over `interval` under the view's authorization.
fn authorize(view: &ReadView, action: Action, interval: &KeyInterval) -> Result<(), Abort> {
    check_permission(
        view.authorization.as_ref(),
        &view.namespace,
        action,
        interval,
    )
}

/// The permissions one branch operation needs. An operation that returns
/// existing values (`prev_kv`) needs `Read` besides its own action: a
/// write-only principal never learns what it overwrote or deleted.
fn branch_op_permissions(op: &BranchOp) -> Vec<(Action, KeyInterval)> {
    match op {
        BranchOp::Range(r) => vec![(Action::Read, interval_of(&r.range))],
        BranchOp::Put(p) => put_permissions(p),
        BranchOp::DeleteRange(d) => delete_permissions(d),
    }
}

fn put_permissions(p: &PutOp) -> Vec<(Action, KeyInterval)> {
    let interval = KeyInterval::exact(&p.key);
    let mut out = vec![(Action::Write, interval.clone())];
    if p.lease.is_some() {
        out.push((Action::LeaseAttach, interval.clone()));
    }
    if p.prev_kv {
        out.push((Action::Read, interval));
    }
    out
}

fn delete_permissions(d: &DeleteRangeOp) -> Vec<(Action, KeyInterval)> {
    let interval = interval_of(&d.range);
    let mut out = vec![(Action::Delete, interval.clone())];
    if d.prev_kv {
        out.push((Action::Read, interval));
    }
    out
}

/// The permissions `request` needs, given the branch `txn_succeeded`
/// selects for a transaction (`None`: both branches, for a request whose
/// branch is not yet known). Comparisons read their keys; Kine update and
/// delete return the entry they saw and so read it; a revocation deletes
/// its attachments, which are checked against the loaded index by the
/// planner and are not enumerable here.
fn request_permissions(
    request: &LogicalRequest,
    txn_succeeded: Option<bool>,
) -> Vec<(Action, KeyInterval)> {
    match &request.operation {
        CanonicalOperation::Range(r) => vec![(Action::Read, interval_of(&r.range))],
        CanonicalOperation::Put(p) => put_permissions(p),
        CanonicalOperation::DeleteRange(d) => delete_permissions(d),
        CanonicalOperation::Txn(t) => {
            let mut out: Vec<(Action, KeyInterval)> = t
                .compares
                .iter()
                .map(|c| (Action::Read, KeyInterval::exact(&c.key)))
                .collect();
            let branches: &[&Vec<BranchOp>] = match txn_succeeded {
                Some(true) => &[&t.success],
                Some(false) => &[&t.failure],
                None => &[&t.success, &t.failure],
            };
            for branch in branches {
                for op in branch.iter() {
                    out.extend(branch_op_permissions(op));
                }
            }
            out
        }
        CanonicalOperation::Compact { .. } => vec![(Action::Compact, KeyInterval::all())],
        CanonicalOperation::LeaseGrant { .. } => vec![(Action::LeaseGrant, KeyInterval::all())],
        CanonicalOperation::LeaseRevoke { .. } => vec![(Action::LeaseRevoke, KeyInterval::all())],
        CanonicalOperation::LeaseTimeToLive { .. } => {
            vec![(Action::LeaseInspect, KeyInterval::all())]
        }
        CanonicalOperation::LeaseKeepAlive { .. } => vec![(Action::LeaseRenew, KeyInterval::all())],
        // It touches no key, so there is no interval to permit. What
        // authorizes it is the purpose of the admission it was
        // submitted with, which is not a permission and is checked
        // where admissions are.
        CanonicalOperation::ConsumeAdmission => Vec::new(),
        CanonicalOperation::KineCreate(c) => vec![(Action::Write, KeyInterval::exact(&c.key))],
        CanonicalOperation::KineUpdate(u) => vec![
            (Action::Write, KeyInterval::exact(&u.key)),
            (Action::Read, KeyInterval::exact(&u.key)),
        ],
        CanonicalOperation::KineDelete(d) => vec![
            (Action::Delete, KeyInterval::exact(&d.key)),
            (Action::Read, KeyInterval::exact(&d.key)),
        ],
    }
}

fn authorize_all(
    view: &ReadView,
    permissions: impl IntoIterator<Item = (Action, KeyInterval)>,
) -> Result<(), Abort> {
    for (action, interval) in permissions {
        authorize(view, action, &interval)?;
    }
    Ok(())
}

/// Whether a retained result of `request` (with `outcome`) may be handed
/// out again under `auth` now: the session must still be executable and
/// the principal must still hold every permission the request needed,
/// exactly as the planner checks a fresh execution. A denial is the
/// recorded outcome the planner would produce. A revocation's deleted
/// attachments are no longer enumerable; its result is a count, which
/// reveals no value, so the revoke permission suffices for it.
///
/// The retained result reveals what the request returned, so losing the
/// permission that produced it (a read rule removed, a session retired)
/// protects it from that point on even though the identity executed.
pub fn authorize_retained(
    auth: &Authorization,
    namespace: &NamespaceId,
    request: &LogicalRequest,
    outcome: &Outcome,
) -> Result<(), Outcome> {
    let succeeded = match outcome {
        Outcome::Txn { succeeded, .. } => Some(*succeeded),
        // A retained denial reveals nothing about the store; replaying it
        // is what a fresh execution would record again.
        Outcome::ErrPermissionDenied | Outcome::ErrSessionInvalid => return Ok(()),
        _ => None,
    };
    for (action, interval) in request_permissions(request, succeeded) {
        if let Err(Abort::Fail(denied)) = check_permission(Some(auth), namespace, action, &interval)
        {
            return Err(*denied);
        }
    }
    Ok(())
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
            _ => Err(fail(Outcome::ErrLeaseNotFound)),
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
        // A Kine private binding is private to one key version: once that
        // version is gone the binding has ended and can never match again.
        if record.purpose == LeasePurpose::KinePrivate && record.attached_keys == 0 {
            record.status = LeaseStatus::Replaced;
        }
        self.leases.insert(*id, record);
        Ok(())
    }

    /// Kine-facing view of an entry: the TTL of its live private binding
    /// (a native lease is not a Kine TTL and reports zero), never the
    /// binding identity.
    fn kine_kv(&self, key: &[u8], entry: &KvEntry) -> KineKv {
        let ttl_seconds = entry
            .lease
            .and_then(|l| self.lease(&l))
            .filter(|r| r.purpose == LeasePurpose::KinePrivate && r.status == LeaseStatus::Active)
            .map_or(0, |r| r.ttl_seconds);
        KineKv::project(key, entry, ttl_seconds)
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

fn kine_kv_cost(kv: &KineKv) -> usize {
    kv.key.len() + kv.value.len() + 48
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
        // A Kine result returns the current entry whether or not the
        // comparison matched, so a mismatch is charged like a read.
        Outcome::KineUpdated { current, .. } => current.as_ref().map_or(0, kine_kv_cost),
        Outcome::KineDeleted { prev, .. } => prev.as_ref().map_or(0, kine_kv_cost),
        Outcome::KineCreated | Outcome::ErrKeyExists => 0,
        Outcome::LeaseTimeToLive { keys, .. } => keys
            .as_ref()
            .map_or(0, |keys| keys.iter().map(|k| k.len() + 8).sum()),
        Outcome::Compacted
        | Outcome::ErrCompacted
        | Outcome::ErrFutureRevision
        | Outcome::LeaseGranted { .. }
        | Outcome::LeaseRevoked { .. }
        | Outcome::ErrLeaseNotFound
        | Outcome::ErrLeaseExists
        | Outcome::ErrLeasePermission
        | Outcome::ErrLeaseQuota
        | Outcome::LeaseKeptAlive { .. }
        | Outcome::LeaseExpired { .. }
        | Outcome::ExpireStale
        | Outcome::LeaseAuthorityEstablished { .. }
        | Outcome::ErrStaleAuthority
        | Outcome::ErrSessionInvalid
        | Outcome::ErrPermissionDenied
        | Outcome::SessionCreated { .. }
        | Outcome::SessionRetired
        | Outcome::ErrReceiptConsumed
        | Outcome::ErrTrustRuleInvalid
        | Outcome::GrantCommitted
        | Outcome::ErrGrantExists
        | Outcome::ErrGrantUnavailable
        | Outcome::RefreshAdvanced { .. }
        | Outcome::ErrRefreshReuse
        | Outcome::PolicyUpdated
        | Outcome::ErrRejected { .. } => 0,
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
            return Err(fail(Outcome::ErrLeasePermission));
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
                .ok_or(fail(Outcome::ErrLeaseQuota))?;
        }
        record.attached_bytes = record
            .attached_bytes
            .checked_add(cost)
            .ok_or(fail(Outcome::ErrLeaseQuota))?;
        if record.attached_keys > limits.max_lease_attachments
            || record.attached_bytes > limits.max_lease_bytes
        {
            return Err(fail(Outcome::ErrLeaseQuota));
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
        return Err(fail(Outcome::ErrLeaseExists));
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
        return Err(fail(Outcome::ErrLeasePermission));
    }
    // Revoking deletes every attached key: owning the lease does not
    // bypass the current delete policy over those keys.
    for key in attached_keys(overlay.view, &lease_id, &record)? {
        authorize(overlay.view, Action::Delete, &KeyInterval::exact(&key))?;
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
        return Err(fail(Outcome::ErrLeasePermission));
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
    next_revision: Option<KvRevision>,
    mutations: &mut Vec<Mutation>,
    events: &mut Vec<KvEvent>,
) -> Result<Outcome, Abort> {
    // Only a branch that emits events consumes the next revision; every
    // other operation plans without one.
    let next_revision = || next_revision.ok_or(PlanError::CounterOverflow);
    // Any request needs an executable session; the specific permission is
    // checked per operation (and per selected branch) below.
    if let Some(auth) = &view.authorization
        && auth.valid_session().is_none()
    {
        return Err(fail(Outcome::ErrSessionInvalid));
    }
    Ok(match &request.operation {
        // A client's request asking to consume an admission. Reaching
        // here means the admission it was submitted with was for
        // submitting under an existing session, not for establishing
        // one: the payload named the action and the authority to take
        // it was never attested. It is refused, and the session it
        // hoped to create is not created.
        //
        // An establishment reaches execution through its own path,
        // selected by the *receipt's* purpose rather than by anything
        // the payload says.
        CanonicalOperation::ConsumeAdmission => {
            return Err(fail(Outcome::ErrPermissionDenied));
        }
        CanonicalOperation::Range(r) => {
            authorize(view, Action::Read, &interval_of(&r.range))?;
            read(view, overlay, r, limits)?
        }
        CanonicalOperation::Put(p) => {
            authorize_all(view, put_permissions(p))?;
            put(overlay, p, next_revision()?, limits, mutations, events)?
        }
        CanonicalOperation::DeleteRange(d) => {
            authorize_all(view, delete_permissions(d))?;
            delete(overlay, d, limits, mutations, events)?
        }
        CanonicalOperation::Txn(t) => {
            for c in &t.compares {
                authorize(view, Action::Read, &KeyInterval::exact(&c.key))?;
            }
            let succeeded = t.compares.iter().all(|c| compare(overlay, c));
            let branch = if succeeded { &t.success } else { &t.failure };
            for op in branch {
                authorize_all(view, branch_op_permissions(op))?;
            }
            let mut results = Vec::with_capacity(branch.len());
            for op in branch {
                results.push(match op {
                    BranchOp::Range(r) => read(view, overlay, r, limits)?,
                    BranchOp::Put(p) => {
                        put(overlay, p, next_revision()?, limits, mutations, events)?
                    }
                    BranchOp::DeleteRange(d) => delete(overlay, d, limits, mutations, events)?,
                });
            }
            Outcome::Txn { succeeded, results }
        }
        CanonicalOperation::Compact { revision } => {
            authorize(view, Action::Compact, &KeyInterval::all())?;
            let target = (*revision).min(view.kv_revision);
            if target > view.compact_floor {
                mutations.push(Mutation::CompactTo { revision: target });
            }
            Outcome::Compacted
        }
        CanonicalOperation::LeaseGrant {
            lease_id,
            ttl_seconds,
        } => {
            authorize(view, Action::LeaseGrant, &KeyInterval::all())?;
            grant(overlay, *lease_id, *ttl_seconds)?
        }
        CanonicalOperation::LeaseRevoke { lease_id } => {
            authorize(view, Action::LeaseRevoke, &KeyInterval::all())?;
            revoke(overlay, *lease_id, mutations, events)?
        }
        CanonicalOperation::LeaseTimeToLive { lease_id, keys } => {
            authorize(view, Action::LeaseInspect, &KeyInterval::all())?;
            time_to_live(overlay, *lease_id, *keys, limits)?
        }
        CanonicalOperation::LeaseKeepAlive { lease_id } => {
            authorize(view, Action::LeaseRenew, &KeyInterval::all())?;
            keep_alive(overlay, *lease_id)?
        }
        CanonicalOperation::KineCreate(c) => {
            authorize(view, Action::Write, &KeyInterval::exact(&c.key))?;
            kine_create(overlay, c, next_revision()?, limits, mutations, events)?
        }
        CanonicalOperation::KineUpdate(u) => {
            // The result returns the entry seen, matched or not.
            authorize(view, Action::Write, &KeyInterval::exact(&u.key))?;
            authorize(view, Action::Read, &KeyInterval::exact(&u.key))?;
            kine_update(overlay, u, next_revision()?, limits, mutations, events)?
        }
        CanonicalOperation::KineDelete(d) => {
            authorize(view, Action::Delete, &KeyInterval::exact(&d.key))?;
            authorize(view, Action::Read, &KeyInterval::exact(&d.key))?;
            kine_delete(overlay, d, mutations, events)?
        }
    })
}

/// Write `key` for Kine: detach (and thereby end) the previous private
/// binding, create the new one when a TTL is given, write the entry.
#[allow(clippy::too_many_arguments)]
fn kine_write(
    overlay: &mut Overlay<'_>,
    key: &[u8],
    value: &[u8],
    ttl_seconds: u32,
    binding: Option<LeaseId>,
    prev: Option<KvEntry>,
    revision: KvRevision,
    limits: &PlanLimits,
    mutations: &mut Vec<Mutation>,
    events: &mut Vec<KvEvent>,
) -> Result<KvEntry, Abort> {
    if let Some(prev_entry) = &prev
        && let Some(old) = prev_entry.lease
    {
        overlay.detach(&old, prev_entry, key)?;
        mutations.push(Mutation::LeaseDetach {
            lease: old,
            key: key.to_vec(),
        });
    }
    let mut generation = None;
    if let Some(binding) = binding {
        if overlay.lease(&binding).is_some() {
            return Err(fail(Outcome::ErrLeaseExists));
        }
        let cost = attachment_cost(key, value);
        if cost > limits.max_lease_bytes || limits.max_lease_attachments == 0 {
            return Err(fail(Outcome::ErrLeaseQuota));
        }
        let generation_one = LeaseGeneration::new(1).expect("one is valid");
        overlay.leases.insert(
            binding,
            LeaseRecord {
                namespace: overlay.view.namespace,
                generation: generation_one,
                owner: overlay.view.principal,
                ttl_seconds,
                renewal_sequence: 0,
                purpose: LeasePurpose::KinePrivate,
                status: LeaseStatus::Active,
                attached_keys: 1,
                attached_bytes: cost,
            },
        );
        mutations.push(Mutation::LeaseAttach {
            lease: binding,
            key: key.to_vec(),
            generation: generation_one,
            mod_revision: revision,
        });
        generation = Some(generation_one);
    }
    let entry = KvEntry {
        value: value.to_vec(),
        create_revision: prev.as_ref().map_or(revision, |e| e.create_revision),
        mod_revision: revision,
        version: prev.as_ref().map_or(1, |e| e.version + 1),
        lease: binding,
        lease_generation: generation,
    };
    mutations.push(Mutation::Write {
        key: key.to_vec(),
        entry: entry.clone(),
    });
    events.push(KvEvent {
        kind: KvEventKind::Put,
        key: key.to_vec(),
        entry: Some(entry.clone()),
        prev,
    });
    overlay.changed.insert(key.to_vec(), Some(entry.clone()));
    Ok(entry)
}

fn kine_create(
    overlay: &mut Overlay<'_>,
    c: &KineCreateOp,
    revision: KvRevision,
    limits: &PlanLimits,
    mutations: &mut Vec<Mutation>,
    events: &mut Vec<KvEvent>,
) -> Result<Outcome, Abort> {
    if overlay.get(&c.key).is_some() {
        return Err(fail(Outcome::ErrKeyExists));
    }
    kine_write(
        overlay,
        &c.key,
        &c.value,
        c.ttl_seconds,
        c.binding,
        None,
        revision,
        limits,
        mutations,
        events,
    )?;
    Ok(Outcome::KineCreated)
}

fn kine_update(
    overlay: &mut Overlay<'_>,
    u: &KineUpdateOp,
    revision: KvRevision,
    limits: &PlanLimits,
    mutations: &mut Vec<Mutation>,
    events: &mut Vec<KvEvent>,
) -> Result<Outcome, Abort> {
    let Some(prev) = overlay.get(&u.key).cloned() else {
        return Ok(Outcome::KineUpdated {
            updated: false,
            current: None,
        });
    };
    if prev.mod_revision != u.expected_mod_revision {
        return Ok(Outcome::KineUpdated {
            updated: false,
            current: Some(overlay.kine_kv(&u.key, &prev)),
        });
    }
    let entry = kine_write(
        overlay,
        &u.key,
        &u.value,
        u.ttl_seconds,
        u.binding,
        Some(prev),
        revision,
        limits,
        mutations,
        events,
    )?;
    Ok(Outcome::KineUpdated {
        updated: true,
        current: Some(KineKv::project(&u.key, &entry, u.ttl_seconds)),
    })
}

fn kine_delete(
    overlay: &mut Overlay<'_>,
    d: &KineDeleteOp,
    mutations: &mut Vec<Mutation>,
    events: &mut Vec<KvEvent>,
) -> Result<Outcome, Abort> {
    let Some(prev) = overlay.get(&d.key).cloned() else {
        return Ok(Outcome::KineDeleted {
            deleted: true,
            prev: None,
        });
    };
    let seen = overlay.kine_kv(&d.key, &prev);
    if let Some(expected) = d.expected_mod_revision
        && prev.mod_revision != expected
    {
        return Ok(Outcome::KineDeleted {
            deleted: false,
            prev: Some(seen),
        });
    }
    delete_entry(overlay, d.key.clone(), prev, mutations, events)?;
    Ok(Outcome::KineDeleted {
        deleted: true,
        prev: Some(seen),
    })
}

fn keep_alive(overlay: &mut Overlay<'_>, lease_id: LeaseId) -> Result<Outcome, Abort> {
    let mut record = overlay.visible_lease(&lease_id)?;
    if record.owner != overlay.view.principal {
        return Err(fail(Outcome::ErrLeasePermission));
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
                return Err(fail(Outcome::ErrStaleAuthority));
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
            // The epoch must be the established one, and one must have been
            // established: the zero sentinel a fresh domain carries before
            // any `EstablishLeaseAuthority` never authorizes an expiration.
            if view.lease_authority == LeaseAuthorityEpoch::ZERO
                || *authority_epoch != view.lease_authority
            {
                return Err(fail(Outcome::ErrStaleAuthority));
            }
            let record = match overlay.lease(lease_id) {
                Some(r) if r.status == LeaseStatus::Active => r.clone(),
                _ => return Err(fail(Outcome::ExpireStale)),
            };
            if record.namespace != view.namespace {
                return Err(Abort::Error(PlanError::NamespaceMismatch));
            }
            if record.generation != *generation
                || record.renewal_sequence != *expected_renewal_sequence
            {
                return Err(fail(Outcome::ExpireStale));
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
        InternalCommand::ConsumeAdmission {
            receipt,
            code,
            refresh_family,
            window,
            ..
        } => {
            // Single use: a consumed receipt or an existing session identity
            // never creates a second session.
            if view.grants.contains_key(&receipt.receipt_id)
                || view.sessions.contains_key(&receipt.session)
            {
                return Err(fail(Outcome::ErrReceiptConsumed));
            }
            // Current replicated policy decides, not the verifier.
            match view.trust_rules.get(&receipt.trust_rule) {
                Some(r) if r.enabled && r.generation == receipt.rule_generation => {}
                _ => return Err(fail(Outcome::ErrTrustRuleInvalid)),
            }
            let mut writes = Vec::new();
            if let Some(code) = code {
                match view.grants.get(code) {
                    Some(g) if g.kind == GrantKind::Code && g.state == GrantState::Pending => {
                        writes.push((
                            *code,
                            GrantRecord {
                                state: GrantState::Consumed,
                                session: Some(receipt.session),
                                ..g.clone()
                            },
                        ));
                    }
                    _ => return Err(fail(Outcome::ErrGrantUnavailable)),
                }
            }
            if let Some(family) = refresh_family {
                match view.grants.get(family) {
                    Some(g)
                        if g.kind == GrantKind::RefreshFamily
                            && g.state == GrantState::Pending
                            && g.session.is_none() =>
                    {
                        writes.push((
                            *family,
                            GrantRecord {
                                session: Some(receipt.session),
                                ..g.clone()
                            },
                        ));
                    }
                    _ => return Err(fail(Outcome::ErrGrantUnavailable)),
                }
            }
            for (commitment, record) in writes {
                mutations.push(Mutation::GrantWrite { commitment, record });
            }
            mutations.push(Mutation::GrantWrite {
                commitment: receipt.receipt_id,
                record: GrantRecord {
                    kind: GrantKind::Receipt,
                    state: GrantState::Consumed,
                    generation: 0,
                    current_secret: None,
                    session: Some(receipt.session),
                },
            });
            mutations.push(Mutation::SessionWrite {
                session: receipt.session,
                record: Some(SessionRecord {
                    principal: receipt.principal,
                    scope_ceiling: receipt.scope_ceiling,
                    trust_rule: receipt.trust_rule,
                    rule_generation: receipt.rule_generation,
                    active: true,
                    window: *window,
                    receipt_id: receipt.receipt_id,
                    expires_at: receipt.expires_at,
                }),
            });
            Ok(Outcome::SessionCreated {
                session: receipt.session,
            })
        }
        InternalCommand::RetireSession { session, .. } => match view.sessions.get(session) {
            Some(record) if record.active => {
                mutations.push(Mutation::SessionWrite {
                    session: *session,
                    record: Some(SessionRecord {
                        active: false,
                        ..record.clone()
                    }),
                });
                Ok(Outcome::SessionRetired)
            }
            _ => Err(fail(Outcome::ErrSessionInvalid)),
        },
        InternalCommand::CommitGrant {
            commitment, kind, ..
        } => {
            if view.grants.contains_key(commitment) || *kind == GrantKind::Receipt {
                return Err(fail(Outcome::ErrGrantExists));
            }
            mutations.push(Mutation::GrantWrite {
                commitment: *commitment,
                record: GrantRecord {
                    kind: *kind,
                    state: GrantState::Pending,
                    generation: 0,
                    current_secret: (*kind == GrantKind::RefreshFamily).then_some(*commitment),
                    session: None,
                },
            });
            Ok(Outcome::GrantCommitted)
        }
        InternalCommand::AdvanceRefresh {
            family,
            presented,
            next,
            ..
        } => {
            let record = match view.grants.get(family) {
                Some(g) if g.kind == GrantKind::RefreshFamily && g.state != GrantState::Revoked => {
                    g.clone()
                }
                _ => return Err(fail(Outcome::ErrGrantUnavailable)),
            };
            if record.current_secret != Some(*presented) {
                // A retired (or unknown) secret: the family is compromised.
                // Revoke it and retire its session at this position.
                mutations.push(Mutation::GrantWrite {
                    commitment: *family,
                    record: GrantRecord {
                        state: GrantState::Revoked,
                        ..record.clone()
                    },
                });
                if let Some(session) = record.session
                    && let Some(s) = view.sessions.get(&session)
                    && s.active
                {
                    mutations.push(Mutation::SessionWrite {
                        session,
                        record: Some(SessionRecord {
                            active: false,
                            ..s.clone()
                        }),
                    });
                }
                return Ok(Outcome::ErrRefreshReuse);
            }
            let generation = record
                .generation
                .checked_add(1)
                .ok_or(Abort::Error(PlanError::CounterOverflow))?;
            mutations.push(Mutation::GrantWrite {
                commitment: *family,
                record: GrantRecord {
                    generation,
                    current_secret: Some(*next),
                    ..record
                },
            });
            Ok(Outcome::RefreshAdvanced { generation })
        }
        InternalCommand::PutPolicyRule {
            principal,
            rule,
            record,
            ..
        } => {
            if let Some(r) = record
                && r.principal != *principal
            {
                return Err(PlanError::Invalid(ValidationError::RequestTooLarge).into());
            }
            mutations.push(Mutation::PolicyRuleWrite {
                principal: *principal,
                rule: *rule,
                record: record.clone(),
            });
            Ok(Outcome::PolicyUpdated)
        }
        InternalCommand::PutTrustRule { rule, record, .. } => {
            // Generations only advance (or stay, to disable or re-enable
            // the current one): a stale write carrying an older generation
            // would silently revalidate every session admitted under it.
            if view
                .trust_rules
                .get(rule)
                .is_some_and(|current| record.generation < current.generation)
            {
                return Err(fail(Outcome::ErrTrustRuleInvalid));
            }
            mutations.push(Mutation::TrustRuleWrite {
                rule: *rule,
                record: record.clone(),
            });
            Ok(Outcome::PolicyUpdated)
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
    body: impl FnOnce(
        &mut Overlay<'_>,
        Option<KvRevision>,
        &mut Vec<Mutation>,
        &mut Vec<KvEvent>,
    ) -> Planned,
) -> Result<ApplyPlan, PlanError> {
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
                    outcome: *outcome,
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
    // The response alone has to fit the envelope that retains it. The
    // budget above is work, and events are stored in rows of their own;
    // this is the durable representation of the result itself.
    if outcome_cost(&outcome) > limits.max_retained_response_bytes {
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
            | Mutation::LeaseWrite { .. }
            | Mutation::LeaseAuthority { .. }
            | Mutation::SessionWrite { .. }
            | Mutation::GrantWrite { .. }
            | Mutation::PolicyRuleWrite { .. }
            | Mutation::TrustRuleWrite { .. }
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
