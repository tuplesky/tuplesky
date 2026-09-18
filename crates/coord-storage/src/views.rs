//! Building the planner's owned `ReadView` and bounded pages from a gated
//! snapshot (design Sections 17.2 and 17.4).

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::ops::Bound;

use coord_state::KvEvent;
use coord_state::view::{HistoricalView, KvEntry, ReadView};
use coord_store_api::engine::{Direction, EngineError, ErrorClass, OrderedRead, ScanRequest};
use coord_store_api::registry::Collection;
use coord_types::ids::{KvRevision, NamespaceId};
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

impl Default for ViewBudget {
    fn default() -> Self {
        ViewBudget {
            max_rows: 10_000,
            max_bytes: 16 * 1024 * 1024,
        }
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

/// Build the owned view a request needs from a gated snapshot.
///
/// Current entries cover every touched key/interval. When the request reads
/// an explicit revision `R` at or above the retention floor and at or below
/// the current revision, a historical snapshot is built by scanning every
/// version in the interval and choosing, per key, the greatest version at
/// or below `R`, excluding tombstones, before any limit applies.
pub fn build_read_view<V: OrderedRead>(
    gated: &GatedView<V>,
    namespace: NamespaceId,
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
    let mut read_view = ReadView::empty(gated.meta().frontier.as_base(), namespace, kv_revision);
    read_view.compact_floor = compact_floor;
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
            for (row_key, value) in
                scan_all(view, Collection::KvHistoryV1, lower, upper, &mut budget)?
            {
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
        let entries = chosen
            .into_iter()
            .filter_map(|(k, e)| e.map(|e| (k, e)))
            .collect();
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
