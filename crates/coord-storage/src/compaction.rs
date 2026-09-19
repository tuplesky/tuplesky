//! Bounded MVCC compaction (task-14; design Sections 6.4, 17.5).
//!
//! The replicated retention floor (`Compact` commands, `RETENTION_FLOOR`)
//! says what history the domain no longer promises. Local garbage
//! collection is separate, incremental and bounded:
//!
//! * The **effective floor** is the replicated floor lowered to the oldest
//!   revision any active hold still needs: pinned pagination views and
//!   watches register [`RetentionHolds`] explicitly, so history they rely
//!   on is never removed under them.
//! * History GC keeps, for every key, the newest version or tombstone at or
//!   below the effective floor plus every newer version, and deletes the
//!   rest. Event GC deletes complete revisions below the effective floor.
//! * Each [`plan_gc`] call examines and deletes at most a budgeted number of
//!   rows and returns one batch of deletions plus persisted cursors, so the
//!   common layer chooses deletion in bounded steps and the engine reclaims
//!   space afterward. The cursor records the floor it was computed under; a
//!   higher floor restarts the pass.
//!
//! Protocol state is untouched: semantic forgetting is a different floor
//! with different rules (`coord_checkpoint::trim`, task-51). Engines own no
//! TTL or filter logic, and no exclusive live-file rewrite happens.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::ops::Bound;

use coord_core::effect::StoreUpdate;
use coord_store_api::engine::{Direction, EngineError, ErrorClass, OrderedRead, ScanRequest};
use coord_store_api::registry::Collection;
use coord_types::ids::KvRevision;
use coord_types::ordered_key;
use serde::{Deserialize, Serialize};

use crate::codecs;
use crate::sync::{Arc, Mutex, lock};

/// `meta_v1` field holding the history GC cursor.
pub const HISTORY_GC_FIELD: &[u8] = b"history_gc_cursor";
/// `meta_v1` field holding the event GC frontier.
pub const EVENTS_GC_FIELD: &[u8] = b"events_gc_through";
/// Record kind of the GC cursor rows.
pub const GC_RECORD_KIND: u16 = 0x0004;

/// Persisted history GC cursor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryCursor {
    /// Effective floor the pass was computed under.
    pub floor: KvRevision,
    /// Last fully processed history row key; `None` before the first row.
    pub after: Option<Vec<u8>>,
    /// Whether the pass under `floor` completed.
    pub done: bool,
}

/// Persisted event GC frontier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventsCursor {
    /// Revisions at or below this are deleted.
    pub through: KvRevision,
}

/// Explicit retention holds from active views and watches.
#[derive(Clone, Default)]
pub struct RetentionHolds {
    inner: Arc<Mutex<Holds>>,
}

#[derive(Default)]
struct Holds {
    next: u64,
    active: BTreeMap<u64, KvRevision>,
}

/// A held revision; dropping it releases the hold.
pub struct HoldGuard {
    holds: RetentionHolds,
    id: u64,
    revision: KvRevision,
}

impl HoldGuard {
    /// Held revision.
    pub fn revision(&self) -> KvRevision {
        self.revision
    }
}

impl Drop for HoldGuard {
    fn drop(&mut self) {
        lock(&self.holds.inner).active.remove(&self.id);
    }
}

impl RetentionHolds {
    /// Hold history at `revision` (and everything newer) until the guard drops.
    pub fn hold(&self, revision: KvRevision) -> HoldGuard {
        let mut h = lock(&self.inner);
        let id = h.next;
        h.next += 1;
        h.active.insert(id, revision);
        HoldGuard {
            holds: self.clone(),
            id,
            revision,
        }
    }

    /// Oldest held revision, if any.
    pub fn oldest(&self) -> Option<KvRevision> {
        lock(&self.inner).active.values().min().copied()
    }

    /// Number of active holds.
    pub fn len(&self) -> usize {
        lock(&self.inner).active.len()
    }

    /// Whether no hold is active.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Per-call work budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GcBudget {
    /// Maximum history rows examined per call.
    pub max_examined: usize,
    /// Maximum rows deleted per call (history plus events).
    pub max_deletes: usize,
}

impl Default for GcBudget {
    fn default() -> Self {
        GcBudget {
            max_examined: 4096,
            max_deletes: 1024,
        }
    }
}

/// One bounded step of garbage collection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcPlan {
    /// Deletions and cursor updates to apply as one protocol batch.
    pub updates: Vec<StoreUpdate>,
    /// Effective floor this step was computed under.
    pub effective_floor: KvRevision,
    /// Rows examined.
    pub examined: usize,
    /// Rows deleted.
    pub deleted: usize,
    /// Whether history and events are fully collected up to the effective floor.
    pub done: bool,
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, EngineError> {
    let payload = postcard::to_allocvec(value)
        .map_err(|_| EngineError::new(ErrorClass::Limit, "gc cursor encode"))?;
    coord_store_api::envelope::StoreEnvelopeV1 {
        record_kind: GC_RECORD_KIND,
        schema_version: 1,
        payload,
    }
    .encode()
}

fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, EngineError> {
    let env = coord_store_api::envelope::StoreEnvelopeV1::decode(bytes)?;
    if env.record_kind != GC_RECORD_KIND || env.schema_version != 1 {
        return Err(EngineError::new(ErrorClass::Corrupt, "gc cursor record"));
    }
    let (v, rest): (T, &[u8]) = postcard::take_from_bytes(&env.payload)
        .map_err(|_| EngineError::new(ErrorClass::Corrupt, "gc cursor decode"))?;
    if !rest.is_empty() {
        return Err(EngineError::new(ErrorClass::Corrupt, "gc cursor trailing"));
    }
    Ok(v)
}

/// Read the persisted cursors.
pub fn cursors<V: OrderedRead>(
    view: &V,
) -> Result<(Option<HistoryCursor>, EventsCursor), EngineError> {
    let meta = Collection::MetaV1.id();
    let history = match view.get(meta, HISTORY_GC_FIELD)? {
        Some(b) => Some(decode(&b)?),
        None => None,
    };
    let events = match view.get(meta, EVENTS_GC_FIELD)? {
        Some(b) => decode(&b)?,
        None => EventsCursor {
            through: KvRevision::ZERO,
        },
    };
    Ok((history, events))
}

/// The floor GC may actually use right now.
pub fn effective_floor<V: OrderedRead>(
    view: &V,
    holds: &RetentionHolds,
) -> Result<KvRevision, EngineError> {
    let floor = codecs::read_retention_floor(view)?;
    Ok(match holds.oldest() {
        Some(held) => floor.min(held),
        None => floor,
    })
}

/// Compute one bounded GC step from a gated snapshot.
pub fn plan_gc<V: OrderedRead>(
    view: &V,
    holds: &RetentionHolds,
    budget: GcBudget,
) -> Result<GcPlan, EngineError> {
    let floor = effective_floor(view, holds)?;
    let (history_cursor, events_cursor) = cursors(view)?;
    let mut updates = Vec::new();
    let mut examined = 0usize;
    let mut deleted = 0usize;
    let meta = Collection::MetaV1.id();

    // ---- history ----
    let mut cursor = match history_cursor {
        Some(c) if c.floor == floor => c,
        _ => HistoryCursor {
            floor,
            after: None,
            done: false,
        },
    };
    let mut history_done = cursor.done || floor == KvRevision::ZERO;
    if !history_done {
        // Scan forward from the cursor, grouping rows by key; only complete
        // groups are decided in this step.
        let mut request = ScanRequest {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
            direction: Direction::Forward,
            resume_after: cursor.after.clone(),
            max_rows: NonZeroU32::new(256).expect("nonzero"),
            max_bytes: NonZeroU32::new(1 << 20).expect("nonzero"),
        };
        // (namespace, key) -> versions (row key, revision, is tombstone)
        let mut group: Vec<(Vec<u8>, KvRevision, bool)> = Vec::new();
        let mut group_key: Option<(coord_types::ids::NamespaceId, Vec<u8>)> = None;
        let mut last_complete: Option<Vec<u8>> = cursor.after.clone();
        let mut exhausted = false;
        'scan: loop {
            let page = view.scan_page(Collection::KvHistoryV1.id(), &request)?;
            for row in &page.rows {
                let decoded = ordered_key::decode_history(&row.key)
                    .map_err(|_| EngineError::new(ErrorClass::Corrupt, "history key"))?;
                let revision = decoded.revision.expect("history key has revision");
                let tombstone = codecs::decode_history(&row.value)?.entry.is_none();
                let this_key = (decoded.namespace, decoded.key);
                if group_key.as_ref() != Some(&this_key) {
                    // Previous group is complete: decide it.
                    if let Some(prev_last) =
                        decide_group(&mut group, floor, &mut updates, &mut deleted)
                    {
                        last_complete = Some(prev_last);
                    }
                    if examined >= budget.max_examined || deleted >= budget.max_deletes {
                        break 'scan;
                    }
                    group_key = Some(this_key);
                }
                group.push((row.key.clone(), revision, tombstone));
                examined += 1;
            }
            if page.exhausted {
                if let Some(prev_last) = decide_group(&mut group, floor, &mut updates, &mut deleted)
                {
                    last_complete = Some(prev_last);
                }
                exhausted = true;
                break;
            }
            request.resume_after = page.rows.last().map(|r| r.key.clone());
        }
        cursor.after = last_complete;
        cursor.done = exhausted;
        history_done = exhausted;
        updates.push(StoreUpdate {
            collection: meta,
            key: HISTORY_GC_FIELD.to_vec(),
            value: Some(encode(&cursor)?),
        });
    }

    // ---- events ----
    let mut events_through = events_cursor.through;
    let mut events_done = true;
    if floor > KvRevision::ZERO {
        let target = KvRevision::new(floor.get() - 1).expect("bounded");
        while events_through < target {
            if deleted >= budget.max_deletes {
                events_done = false;
                break;
            }
            let next = events_through
                .checked_next()
                .map_err(|_| EngineError::new(ErrorClass::Limit, "revision overflow"))?;
            let request = ScanRequest {
                lower: Bound::Included(codecs::event_key(next, 0)),
                upper: Bound::Included(codecs::event_key(next, u32::MAX)),
                direction: Direction::Forward,
                resume_after: None,
                max_rows: NonZeroU32::new(u32::MAX).expect("nonzero"),
                max_bytes: NonZeroU32::new(u32::MAX).expect("nonzero"),
            };
            let page = view.scan_page(Collection::EventsV1.id(), &request)?;
            for row in page.rows {
                updates.push(StoreUpdate {
                    collection: Collection::EventsV1.id(),
                    key: row.key,
                    value: None,
                });
                deleted += 1;
            }
            events_through = next;
        }
        if events_through != events_cursor.through {
            updates.push(StoreUpdate {
                collection: meta,
                key: EVENTS_GC_FIELD.to_vec(),
                value: Some(encode(&EventsCursor {
                    through: events_through,
                })?),
            });
        }
    }
    Ok(GcPlan {
        updates,
        effective_floor: floor,
        examined,
        deleted,
        done: history_done && events_done,
    })
}

/// Decide one key's versions: keep the newest version at or below the
/// floor and everything above it; delete older ones. Returns the last row
/// key of the group (the cursor position) when the group was non-empty.
fn decide_group(
    group: &mut Vec<(Vec<u8>, KvRevision, bool)>,
    floor: KvRevision,
    updates: &mut Vec<StoreUpdate>,
    deleted: &mut usize,
) -> Option<Vec<u8>> {
    if group.is_empty() {
        return None;
    }
    let last_key = group.last().map(|(k, _, _)| k.clone());
    let newest_at_or_below = group
        .iter()
        .filter(|(_, r, _)| *r <= floor)
        .map(|(_, r, _)| *r)
        .max();
    for (row_key, revision, _tombstone) in group.drain(..) {
        if revision <= floor && Some(revision) != newest_at_or_below {
            updates.push(StoreUpdate {
                collection: Collection::KvHistoryV1.id(),
                key: row_key,
                value: None,
            });
            *deleted += 1;
        }
    }
    last_key
}
