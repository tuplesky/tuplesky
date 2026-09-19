//! Building the planner's owned `ReadView` and bounded pages from a gated
//! snapshot (design Sections 17.2 and 17.4).

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::ops::Bound;

use coord_state::lease::{LeaseRecord, LeaseStatus};
use coord_state::policy::Authorization;
use coord_state::view::{HistoricalView, KvEntry, ReadView};
use coord_state::{InternalCommand, KvEvent};
use coord_store_api::engine::{Direction, EngineError, ErrorClass, OrderedRead, ScanRequest};
use coord_store_api::registry::Collection;
use coord_types::ids::{KvRevision, LeaseId, NamespaceId, PrincipalId, SessionId};
use coord_types::logical_v1::{BranchOp, CanonicalOperation, KeyRange, LogicalRequest, RangeOp};
use coord_types::ordered_key;

use crate::codecs;
use crate::view::GatedView;

/// Bounds on how much a view may load.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ViewBudget {
    /// Maximum rows loaded.
    pub max_rows: u32,
    /// Maximum bytes loaded.
    pub max_bytes: u32,
}

impl ViewBudget {
    /// The budget the execution path uses. It is part of the schema, the
    /// same on every replica and every build, because a command that
    /// overruns it is rejected as a replicated result: a locally tunable
    /// number here would make one node's resource setting another node's
    /// history.
    pub const SCHEMA: ViewBudget = ViewBudget {
        max_rows: 10_000,
        max_bytes: 16 * 1024 * 1024,
    };
}

impl Default for ViewBudget {
    fn default() -> Self {
        ViewBudget::SCHEMA
    }
}

/// Why a view could not be built.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ViewBuildError {
    /// Engine failure.
    Engine(EngineError),
    /// The request touches more rows/bytes than the budget allows.
    BudgetExceeded,
}

impl From<EngineError> for ViewBuildError {
    fn from(e: EngineError) -> Self {
        ViewBuildError::Engine(e)
    }
}

/// Raw `(key, value)` rows.
type RawRows = Vec<(Vec<u8>, Vec<u8>)>;

/// One owned page of current entries and whether the interval is exhausted.
pub type CurrentPage = (Vec<(Vec<u8>, KvEntry)>, bool);

/// A selector the request touches.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Touch {
    Exact(Vec<u8>),
    Interval(Vec<u8>, Vec<u8>),
}

fn touches(op: &CanonicalOperation) -> Vec<Touch> {
    fn of_range(r: &KeyRange) -> Touch {
        match &r.range_end {
            None => Touch::Exact(r.key.clone()),
            Some(end) => Touch::Interval(r.key.clone(), end.clone()),
        }
    }
    let mut out = Vec::new();
    match op {
        CanonicalOperation::Range(r) => out.push(of_range(&r.range)),
        CanonicalOperation::Put(p) => out.push(Touch::Exact(p.key.clone())),
        CanonicalOperation::KineCreate(c) => out.push(Touch::Exact(c.key.clone())),
        CanonicalOperation::KineUpdate(u) => out.push(Touch::Exact(u.key.clone())),
        CanonicalOperation::KineDelete(d) => out.push(Touch::Exact(d.key.clone())),
        CanonicalOperation::DeleteRange(d) => out.push(of_range(&d.range)),
        CanonicalOperation::Txn(t) => {
            for c in &t.compares {
                out.push(Touch::Exact(c.key.clone()));
            }
            for b in t.success.iter().chain(&t.failure) {
                match b {
                    BranchOp::Range(r) => out.push(of_range(&r.range)),
                    BranchOp::Put(p) => out.push(Touch::Exact(p.key.clone())),
                    BranchOp::DeleteRange(d) => out.push(of_range(&d.range)),
                }
            }
        }
        _ => {}
    }
    out
}

/// Leases a request names directly.
fn named_leases(op: &CanonicalOperation) -> Vec<LeaseId> {
    let mut out = Vec::new();
    match op {
        CanonicalOperation::Put(p) => out.extend(p.lease),
        CanonicalOperation::KineCreate(c) => out.extend(c.binding),
        CanonicalOperation::KineUpdate(u) => out.extend(u.binding),
        CanonicalOperation::Txn(t) => {
            for b in t.success.iter().chain(&t.failure) {
                if let BranchOp::Put(p) = b {
                    out.extend(p.lease);
                }
            }
        }
        CanonicalOperation::LeaseGrant { lease_id, .. }
        | CanonicalOperation::LeaseKeepAlive { lease_id }
        | CanonicalOperation::LeaseRevoke { lease_id }
        | CanonicalOperation::LeaseTimeToLive { lease_id, .. } => out.push(*lease_id),
        CanonicalOperation::Range(_)
        | CanonicalOperation::DeleteRange(_)
        | CanonicalOperation::KineDelete(_)
        | CanonicalOperation::Compact { .. } => {}
    }
    out
}

/// The lease whose attached keys the request needs, if any.
fn needs_attachments(op: &CanonicalOperation) -> Option<LeaseId> {
    match op {
        CanonicalOperation::LeaseRevoke { lease_id } => Some(*lease_id),
        CanonicalOperation::LeaseTimeToLive {
            lease_id,
            keys: true,
        } => Some(*lease_id),
        _ => None,
    }
}

/// Exclusive upper bound of every key starting with `prefix`.
fn prefix_upper(prefix: &[u8]) -> Bound<Vec<u8>> {
    let mut upper = prefix.to_vec();
    while let Some(last) = upper.pop() {
        if last != 0xff {
            upper.push(last + 1);
            return Bound::Excluded(upper);
        }
    }
    Bound::Unbounded
}

struct Budget {
    rows: u32,
    bytes: u32,
}

impl Budget {
    fn charge(&mut self, rows: usize, bytes: usize) -> Result<(), ViewBuildError> {
        let rows = u32::try_from(rows).map_err(|_| ViewBuildError::BudgetExceeded)?;
        let bytes = u32::try_from(bytes).map_err(|_| ViewBuildError::BudgetExceeded)?;
        self.rows = self
            .rows
            .checked_sub(rows)
            .ok_or(ViewBuildError::BudgetExceeded)?;
        self.bytes = self
            .bytes
            .checked_sub(bytes)
            .ok_or(ViewBuildError::BudgetExceeded)?;
        Ok(())
    }
}

/// Scan every row of `collection` in `[lower, upper)` within the budget.
/// Every history row in `[lower, upper)`, read in bounded pages.
///
/// The work is local and the pages bound it; what the pages must not do
/// is decide a replicated outcome, so nothing here is charged to the
/// view budget. The range itself is bounded by the replicated retention
/// floor, which is the same on every replica.
fn scan_history<V: OrderedRead>(
    view: &V,
    lower: Vec<u8>,
    upper: Bound<Vec<u8>>,
) -> Result<RawRows, ViewBuildError> {
    /// One page of history rows. Large enough for any schema-valid row.
    const PAGE_BYTES: u32 = 16 * 1024 * 1024;
    let mut out = Vec::new();
    let mut request = ScanRequest {
        lower: Bound::Included(lower),
        upper,
        direction: Direction::Forward,
        resume_after: None,
        max_rows: NonZeroU32::new(256).expect("nonzero"),
        max_bytes: NonZeroU32::new(PAGE_BYTES).expect("nonzero"),
    };
    loop {
        let page = view.scan_page(Collection::KvHistoryV1.id(), &request)?;
        let last = page.rows.last().map(|r| r.key.clone());
        out.extend(page.rows.into_iter().map(|r| (r.key, r.value)));
        if page.exhausted {
            return Ok(out);
        }
        match last {
            Some(key) => request.resume_after = Some(key),
            // A page that is neither exhausted nor advancing would loop.
            None => {
                return Err(ViewBuildError::Engine(EngineError::new(
                    ErrorClass::Corrupt,
                    "history scan made no progress",
                )));
            }
        }
    }
}

fn scan_all<V: OrderedRead>(
    view: &V,
    collection: Collection,
    lower: Vec<u8>,
    upper: Bound<Vec<u8>>,
    budget: &mut Budget,
) -> Result<RawRows, ViewBuildError> {
    let mut out = Vec::new();
    let mut request = ScanRequest {
        lower: Bound::Included(lower),
        upper,
        direction: Direction::Forward,
        resume_after: None,
        max_rows: NonZeroU32::new(256).expect("nonzero"),
        // The page byte limit is the remaining view budget, so any
        // schema-valid row (a value near `MAX_VALUE_BYTES` plus key and
        // envelope) fits a page while the budget allows it.
        max_bytes: NonZeroU32::new(budget.bytes).ok_or(ViewBuildError::BudgetExceeded)?,
    };
    loop {
        request.max_bytes = NonZeroU32::new(budget.bytes).ok_or(ViewBuildError::BudgetExceeded)?;
        let page = view.scan_page(collection.id(), &request)?;
        for row in &page.rows {
            budget.charge(1, row.key.len() + row.value.len())?;
        }
        let last = page.rows.last().map(|r| r.key.clone());
        out.extend(page.rows.into_iter().map(|r| (r.key, r.value)));
        if page.exhausted {
            return Ok(out);
        }
        request.resume_after = last;
    }
}

fn current_bounds(namespace: &NamespaceId, touch: &Touch) -> (Vec<u8>, Bound<Vec<u8>>) {
    match touch {
        Touch::Exact(k) => {
            let key = codecs::current_key(namespace, k);
            (key.clone(), Bound::Included(key))
        }
        Touch::Interval(lo, hi) => (
            codecs::current_key(namespace, lo),
            Bound::Excluded(codecs::current_key(namespace, hi)),
        ),
    }
}

fn history_bounds(namespace: &NamespaceId, touch: &Touch) -> (Vec<u8>, Bound<Vec<u8>>) {
    match touch {
        Touch::Exact(k) => {
            let (lo, hi) = ordered_key::history_bounds(namespace, k);
            (lo, Bound::Excluded(hi))
        }
        Touch::Interval(lo, hi) => (
            codecs::history_key(namespace, lo, KvRevision::ZERO),
            Bound::Excluded(codecs::history_key(namespace, hi, KvRevision::ZERO)),
        ),
    }
}

/// Load the reverse index of `lease` in `namespace` and the current entries
/// it points at into `read_view`.
fn load_attachments<V: OrderedRead>(
    view: &V,
    namespace: NamespaceId,
    lease: LeaseId,
    read_view: &mut ReadView,
    budget: &mut Budget,
) -> Result<(), ViewBuildError> {
    let prefix = codecs::lease_key_prefix(&lease, &namespace);
    let upper = prefix_upper(&prefix);
    let mut keys = BTreeSet::new();
    for (row_key, _) in scan_all(view, Collection::LeaseKeysV1, prefix, upper, budget)? {
        let (_, ns, key) = codecs::decode_lease_key_row(&row_key)?;
        if ns != namespace {
            continue;
        }
        if !read_view.current.contains_key(&key) {
            let current = codecs::current_key(&namespace, &key);
            if let Some(value) = view.get(Collection::KvCurrentV1.id(), &current)? {
                budget.charge(1, current.len() + value.len())?;
                read_view
                    .current
                    .insert(key.clone(), codecs::decode_current(&value)?);
            }
        }
        keys.insert(key);
    }
    read_view.lease_keys.insert(lease, keys);
    Ok(())
}

/// Load the records of `lease_ids` that exist into `read_view`.
fn load_leases<V: OrderedRead>(
    view: &V,
    lease_ids: impl IntoIterator<Item = LeaseId>,
    read_view: &mut ReadView,
    budget: &mut Budget,
) -> Result<(), ViewBuildError> {
    for id in lease_ids {
        let row = codecs::lease_row_key(&id);
        if let Some(value) = view.get(Collection::LeaseV1.id(), &row)? {
            budget.charge(1, row.len() + value.len())?;
            read_view.leases.insert(id, codecs::decode_lease(&value)?);
        }
    }
    Ok(())
}

/// Load the authorization context of `session` in `namespace`: the session
/// record, its trust rule and every permission rule of its principal.
pub fn load_authorization<V: OrderedRead>(
    view: &V,
    namespace: NamespaceId,
    session: &SessionId,
    budget: ViewBudget,
) -> Result<Authorization, ViewBuildError> {
    let mut budget = Budget {
        rows: budget.max_rows,
        bytes: budget.max_bytes,
    };
    let mut out = Authorization {
        session: None,
        trust_rule: None,
        rules: Vec::new(),
    };
    let key = codecs::session_key(session);
    let Some(bytes) = view.get(Collection::SessionV1.id(), &key)? else {
        return Ok(out);
    };
    budget.charge(1, key.len() + bytes.len())?;
    let record = codecs::decode_session(&bytes)?;
    let rule_key = codecs::trust_rule_key(&record.trust_rule);
    if let Some(bytes) = view.get(Collection::PolicyV1.id(), &rule_key)? {
        budget.charge(1, rule_key.len() + bytes.len())?;
        out.trust_rule = Some(codecs::decode_trust_rule(&bytes)?);
    }
    let prefix = codecs::policy_rule_prefix(&record.principal);
    let upper = prefix_upper(&prefix);
    for (_, value) in scan_all(view, Collection::PolicyV1, prefix, upper, &mut budget)? {
        let rule = codecs::decode_policy_rule(&value)?;
        if rule.namespace == namespace {
            out.rules.push(rule);
        }
    }
    out.session = Some(record);
    Ok(out)
}

/// Build the view a client request executing under `session` needs: the
/// same as [`build_read_view`] plus the authorization context, with the
/// principal taken from the session record (deny by default when absent).
pub fn build_authorized_view<V: OrderedRead>(
    gated: &GatedView<V>,
    namespace: NamespaceId,
    session: &SessionId,
    request: &LogicalRequest,
    budget: ViewBudget,
) -> Result<ReadView, ViewBuildError> {
    let authorization = load_authorization(gated.view(), namespace, session, budget)?;
    let principal = authorization
        .session
        .as_ref()
        .map_or(PrincipalId([0; 16]), |s| s.principal);
    let mut read_view = build_read_view(gated, namespace, principal, request, budget)?;
    read_view.authorization = Some(authorization);
    Ok(read_view)
}

/// Build the view an internal command needs: the authority epoch and, for
/// an expiration, the lease record, its reverse index and the entries it
/// points at.
pub fn build_internal_view<V: OrderedRead>(
    gated: &GatedView<V>,
    command: &InternalCommand,
    budget: ViewBudget,
) -> Result<ReadView, ViewBuildError> {
    let view = gated.view();
    let mut budget = Budget {
        rows: budget.max_rows,
        bytes: budget.max_bytes,
    };
    let namespace = command.namespace();
    let kv_revision = codecs::read_kv_revision(view)?;
    let mut read_view = ReadView::empty(
        gated.meta().frontier.as_base(),
        namespace,
        PrincipalId([0; 16]),
        kv_revision,
    );
    read_view.compact_floor = codecs::read_retention_floor(view)?;
    read_view.lease_authority = codecs::read_lease_authority(view)?;
    if let Some(lease) = command.lease() {
        load_attachments(view, namespace, lease, &mut read_view, &mut budget)?;
        let mut ids: BTreeSet<LeaseId> = BTreeSet::from([lease]);
        ids.extend(read_view.current.values().filter_map(|e| e.lease));
        load_leases(view, ids, &mut read_view, &mut budget)?;
    }
    for rule in command.trust_rules() {
        let key = codecs::trust_rule_key(&rule);
        if let Some(bytes) = view.get(Collection::PolicyV1.id(), &key)? {
            budget.charge(1, key.len() + bytes.len())?;
            read_view
                .trust_rules
                .insert(rule, codecs::decode_trust_rule(&bytes)?);
        }
    }
    let mut sessions: BTreeSet<SessionId> = command.sessions().into_iter().collect();
    for commitment in command.grants() {
        let key = codecs::grant_key(&commitment);
        if let Some(bytes) = view.get(Collection::AuthGrantV1.id(), &key)? {
            budget.charge(1, key.len() + bytes.len())?;
            let record = codecs::decode_grant(&bytes)?;
            sessions.extend(record.session);
            read_view.grants.insert(commitment, record);
        }
    }
    for session in sessions {
        let key = codecs::session_key(&session);
        if let Some(bytes) = view.get(Collection::SessionV1.id(), &key)? {
            budget.charge(1, key.len() + bytes.len())?;
            read_view
                .sessions
                .insert(session, codecs::decode_session(&bytes)?);
        }
    }
    Ok(read_view)
}

/// One page of the active-lease recovery scan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActiveLeasePage {
    /// Active records found on this page, in lease id order.
    pub leases: Vec<(LeaseId, LeaseRecord)>,
    /// Last lease row examined (active or tombstone); pass it back as
    /// `after` to continue. `None` when the collection is exhausted.
    pub next: Option<LeaseId>,
}

/// One page of the active leases a recovering scheduler arms: every
/// `lease_v1` row after `after`, in lease id order, examined within
/// `budget`, keeping the active ones. Revoked and expired leases remain as
/// tombstones and count against the budget, so a page may carry few or no
/// active leases while still advancing; the caller continues from `next`
/// until it is `None`. The budget bounds one page, never the recovery, so
/// a lease table of any size is recovered in bounded steps.
pub fn active_leases_page<V: OrderedRead>(
    view: &V,
    after: Option<LeaseId>,
    budget: ViewBudget,
) -> Result<ActiveLeasePage, ViewBuildError> {
    let mut budget = Budget {
        rows: budget.max_rows,
        bytes: budget.max_bytes,
    };
    let mut request = ScanRequest {
        lower: Bound::Unbounded,
        upper: Bound::Unbounded,
        direction: Direction::Forward,
        resume_after: after.map(|id| codecs::lease_row_key(&id)),
        max_rows: NonZeroU32::new(256).expect("nonzero"),
        max_bytes: NonZeroU32::new(budget.bytes).ok_or(ViewBuildError::BudgetExceeded)?,
    };
    let mut leases = Vec::new();
    let mut last: Option<LeaseId> = None;
    loop {
        request.max_rows =
            NonZeroU32::new(budget.rows.min(256)).ok_or(ViewBuildError::BudgetExceeded)?;
        request.max_bytes = NonZeroU32::new(budget.bytes).ok_or(ViewBuildError::BudgetExceeded)?;
        let page = view.scan_page(Collection::LeaseV1.id(), &request)?;
        for row in &page.rows {
            if budget.charge(1, row.key.len() + row.value.len()).is_err() {
                // The page is full: stop before this row, which the next
                // page examines first.
                return match last {
                    Some(_) => Ok(ActiveLeasePage { leases, next: last }),
                    None => Err(ViewBuildError::BudgetExceeded),
                };
            }
            let id = LeaseId::from_slice(&row.key)
                .map_err(|_| EngineError::new(ErrorClass::Corrupt, "lease row key"))?;
            let record = codecs::decode_lease(&row.value)?;
            if record.status == LeaseStatus::Active {
                leases.push((id, record));
            }
            last = Some(id);
        }
        if page.exhausted {
            return Ok(ActiveLeasePage { leases, next: None });
        }
        if budget.rows == 0 || budget.bytes == 0 {
            return match last {
                Some(_) => Ok(ActiveLeasePage { leases, next: last }),
                None => Err(ViewBuildError::BudgetExceeded),
            };
        }
        request.resume_after = last.map(|id| codecs::lease_row_key(&id));
    }
}

/// Every active lease record (what a recovering scheduler arms), in lease
/// id order, collected page by page with `budget` bounding each page (see
/// [`active_leases_page`]). The result holds only the active records; the
/// tombstones the scan passes over are not retained.
pub fn active_leases<V: OrderedRead>(
    view: &V,
    budget: ViewBudget,
) -> Result<Vec<(LeaseId, LeaseRecord)>, ViewBuildError> {
    let mut out = Vec::new();
    let mut after = None;
    loop {
        let page = active_leases_page(view, after, budget)?;
        out.extend(page.leases);
        match page.next {
            Some(next) => after = Some(next),
            None => return Ok(out),
        }
    }
}

/// Build the owned view a request needs from a gated snapshot.
///
/// Current entries cover every touched key/interval. When the request reads
/// an explicit revision `R` at or above the retention floor and at or below
/// the current revision, a historical snapshot is built by scanning every
/// version in the interval and choosing, per key, the greatest version at
/// or below `R`, excluding tombstones, before any limit applies. Lease
/// records are loaded for every lease the request names and every lease a
/// loaded entry references; a revocation or key-listing inspection also
/// loads the lease's reverse index and the entries it points at.
pub fn build_read_view<V: OrderedRead>(
    gated: &GatedView<V>,
    namespace: NamespaceId,
    principal: PrincipalId,
    request: &LogicalRequest,
    budget: ViewBudget,
) -> Result<ReadView, ViewBuildError> {
    let view = gated.view();
    let mut budget = Budget {
        rows: budget.max_rows,
        bytes: budget.max_bytes,
    };
    let kv_revision = codecs::read_kv_revision(view)?;
    let compact_floor = codecs::read_retention_floor(view)?;
    let mut read_view = ReadView::empty(
        gated.meta().frontier.as_base(),
        namespace,
        principal,
        kv_revision,
    );
    read_view.compact_floor = compact_floor;
    read_view.lease_authority = codecs::read_lease_authority(view)?;
    let touched = touches(&request.operation);
    let mut seen: BTreeSet<Vec<u8>> = BTreeSet::new();
    for touch in &touched {
        let (lower, upper) = current_bounds(&namespace, touch);
        for (row_key, value) in scan_all(view, Collection::KvCurrentV1, lower, upper, &mut budget)?
        {
            let decoded = ordered_key::decode_current(&row_key)
                .map_err(|_| EngineError::new(ErrorClass::Corrupt, "kv_current key"))?;
            if decoded.namespace != namespace {
                continue;
            }
            if seen.insert(decoded.key.clone()) {
                read_view
                    .current
                    .insert(decoded.key, codecs::decode_current(&value)?);
            }
        }
    }
    // Attached keys of a lease the request revokes or lists, and their
    // current entries (so a revocation deletes exactly what is attached).
    if let Some(lease) = needs_attachments(&request.operation) {
        load_attachments(view, namespace, lease, &mut read_view, &mut budget)?;
    }
    // Lease records: named by the request or referenced by loaded entries.
    let mut lease_ids: BTreeSet<LeaseId> = named_leases(&request.operation).into_iter().collect();
    lease_ids.extend(read_view.current.values().filter_map(|e| e.lease));
    load_leases(view, lease_ids, &mut read_view, &mut budget)?;
    // One historical snapshot per explicit revision the request reads, top
    // level or inside a transaction branch, covering every range that reads
    // that revision. Revisions outside the retained window get no snapshot:
    // the planner answers those as compacted or future.
    for r in coord_state::historical_revisions(&request.operation) {
        if r > kv_revision || r < compact_floor {
            continue;
        }
        let mut chosen: BTreeMap<Vec<u8>, Option<KvEntry>> = BTreeMap::new();
        for touch in historical_touches(&request.operation, r) {
            let (lower, upper) = history_bounds(&namespace, &touch);
            // Physical versions are not charged to the replicated budget.
            // How many obsolete versions of a key are still on disk is a
            // local matter: garbage collection runs at its own pace on
            // each replica, so charging them made the budget, and the
            // durable rejection that follows it, depend on local cleanup
            // progress. Two replicas holding the same logical state at
            // the same compaction floor could then disagree about the
            // same chosen command, one executing it and one recording a
            // rejection. The selection below is charged instead, and it
            // is the same everywhere.
            for (row_key, value) in scan_history(view, lower, upper)? {
                let decoded = ordered_key::decode_history(&row_key)
                    .map_err(|_| EngineError::new(ErrorClass::Corrupt, "kv_history key"))?;
                let version = decoded.revision.expect("history key carries a revision");
                if decoded.namespace != namespace || version > r {
                    continue;
                }
                // Rows arrive in (key, revision) order, so the last version
                // at or below R for a key wins.
                chosen.insert(decoded.key, codecs::decode_history(&value)?.entry);
            }
        }
        let entries: BTreeMap<Vec<u8>, KvEntry> = chosen
            .into_iter()
            .filter_map(|(k, e)| e.map(|e| (k, e)))
            .collect();
        // What the request actually reads: one version per key, the same
        // on every replica whatever each has collected.
        for (key, entry) in &entries {
            budget.charge(1, key.len() + entry.value.len())?;
        }
        read_view.historical.push(HistoricalView {
            revision: r,
            entries,
        });
    }
    Ok(read_view)
}

/// The ranges of `op` that read revision `r`.
fn historical_touches(op: &CanonicalOperation, r: KvRevision) -> Vec<Touch> {
    fn of_range(range: &RangeOp) -> Touch {
        match &range.range.range_end {
            None => Touch::Exact(range.range.key.clone()),
            Some(end) => Touch::Interval(range.range.key.clone(), end.clone()),
        }
    }
    let mut out = Vec::new();
    match op {
        CanonicalOperation::Range(range) if range.revision == Some(r) => out.push(of_range(range)),
        CanonicalOperation::Txn(t) => {
            for b in t.success.iter().chain(&t.failure) {
                if let BranchOp::Range(range) = b
                    && range.revision == Some(r)
                {
                    out.push(of_range(range));
                }
            }
        }
        _ => {}
    }
    out
}

/// One bounded page of current entries in `namespace` over `[lower, upper)`
/// (`upper` `None` for an open end), in either direction, resuming after an
/// exclusive cursor. Owned rows; never a live iterator.
pub fn scan_current_page<V: OrderedRead>(
    view: &V,
    namespace: &NamespaceId,
    lower: &[u8],
    upper: Option<&[u8]>,
    direction: Direction,
    resume_after: Option<&[u8]>,
    max_rows: u32,
) -> Result<CurrentPage, EngineError> {
    let upper_bound = match upper {
        Some(u) => Bound::Excluded(codecs::current_key(namespace, u)),
        None => {
            // End of this namespace: the next namespace prefix.
            let mut next = namespace.0;
            let mut carry = true;
            for b in next.iter_mut().rev() {
                if carry {
                    let (v, c) = b.overflowing_add(1);
                    *b = v;
                    carry = c;
                }
            }
            if carry {
                Bound::Unbounded
            } else {
                Bound::Excluded(next.to_vec())
            }
        }
    };
    let request = ScanRequest {
        lower: Bound::Included(codecs::current_key(namespace, lower)),
        upper: upper_bound,
        direction,
        resume_after: resume_after.map(|k| codecs::current_key(namespace, k)),
        max_rows: NonZeroU32::new(max_rows.max(1)).expect("nonzero"),
        max_bytes: NonZeroU32::new(8 * 1024 * 1024).expect("nonzero"),
    };
    let page = view.scan_page(Collection::KvCurrentV1.id(), &request)?;
    let mut rows = Vec::with_capacity(page.rows.len());
    for row in page.rows {
        let decoded = ordered_key::decode_current(&row.key)
            .map_err(|_| EngineError::new(ErrorClass::Corrupt, "kv_current key"))?;
        rows.push((decoded.key, codecs::decode_current(&row.value)?));
    }
    Ok((rows, page.exhausted))
}

/// One stored event with the namespace it was written in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredEvent {
    /// Namespace of the key.
    pub namespace: NamespaceId,
    /// The event.
    pub event: KvEvent,
}

/// The complete event set of one revision, in ordinal order. `None` when
/// the revision produced no events (or was never applied).
pub fn events_at<V: OrderedRead>(
    view: &V,
    revision: KvRevision,
) -> Result<Option<Vec<KvEvent>>, EngineError> {
    Ok(stored_events_at(view, revision)?
        .map(|events| events.into_iter().map(|e| e.event).collect()))
}

/// [`events_at`] keeping each event's namespace, for consumers that filter
/// by namespace (watch replay must not relabel a batch with the watch's
/// namespace: a revision may carry keys of several namespaces).
pub fn stored_events_at<V: OrderedRead>(
    view: &V,
    revision: KvRevision,
) -> Result<Option<Vec<StoredEvent>>, EngineError> {
    let request = ScanRequest {
        lower: Bound::Included(codecs::event_key(revision, 0)),
        upper: Bound::Included(codecs::event_key(revision, u32::MAX)),
        direction: Direction::Forward,
        resume_after: None,
        max_rows: NonZeroU32::new(u32::MAX).expect("nonzero"),
        max_bytes: NonZeroU32::new(u32::MAX).expect("nonzero"),
    };
    let page = view.scan_page(Collection::EventsV1.id(), &request)?;
    if page.rows.is_empty() {
        return Ok(None);
    }
    let mut events = Vec::with_capacity(page.rows.len());
    for (expected, row) in page.rows.iter().enumerate() {
        let (rev, ordinal) = codecs::decode_event_key(&row.key)?;
        if rev != revision || ordinal as usize != expected {
            return Err(EngineError::new(
                ErrorClass::Corrupt,
                "event ordinals not contiguous",
            ));
        }
        let record = codecs::decode_event(&row.value)?;
        events.push(StoredEvent {
            namespace: record.namespace,
            event: KvEvent {
                kind: record.kind,
                key: record.key,
                entry: record.entry,
                prev: record.prev,
            },
        });
    }
    Ok(Some(events))
}
