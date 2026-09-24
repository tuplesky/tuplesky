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
//! Protocol state is untouched (semantic forgetting is task-51+), engines
//! own no TTL or filter logic, and no exclusive live-file rewrite happens.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::ops::Bound;

use coord_core::effect::StoreUpdate;
use coord_store_api::engine::{Direction, EngineError, ErrorClass, OrderedRead, ScanRequest};
use coord_store_api::registry::Collection;
use coord_types::ids::{KvRevision, NamespaceId};
use coord_types::ordered_key;
use serde::{Deserialize, Serialize};

use crate::codecs;
use crate::sync::{Arc, Mutex, lock};

/// Logical identity of a history row: `(namespace, key)`.
type HistoryKey = (NamespaceId, Vec<u8>);

/// `meta_v1` field holding the history GC cursor.
pub const HISTORY_GC_FIELD: &[u8] = b"history_gc_cursor";
/// `meta_v1` field holding the event GC frontier.
pub const EVENTS_GC_FIELD: &[u8] = b"events_gc_through";
/// Record kind of the GC cursor rows.
pub const GC_RECORD_KIND: u16 = 0x0004;

/// Byte limit of one history scan page: the largest schema-valid history row
/// (an envelope payload plus its ordered key) always fits, so a
/// maximum-size value cannot stall collection with a `Limit` error.
const HISTORY_PAGE_BYTES: u32 = coord_store_api::envelope::MAX_ENVELOPE_PAYLOAD as u32 + 64 * 1024;

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
        // Stream rows in (namespace, key, revision) order and decide each
        // one on arrival: a version above the floor is kept; a version at or
        // below the floor is the candidate to keep for its key until a newer
        // version at or below the floor arrives, which deletes it. Only the
        // current candidate is undecided, so the budget is checked on every
        // row and the cursor rests on the last decided row: a hot key with
        // thousands of versions costs at most one re-examined row per step.
        let mut request = ScanRequest {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
            direction: Direction::Forward,
            resume_after: cursor.after.clone(),
            max_rows: NonZeroU32::new(256).expect("nonzero"),
            max_bytes: NonZeroU32::new(HISTORY_PAGE_BYTES).expect("nonzero"),
        };
        // Newest version at or below the floor seen so far for the current
        // key: (row key, (namespace, key)).
        let mut candidate: Option<(Vec<u8>, HistoryKey)> = None;
        let mut last_decided: Option<Vec<u8>> = cursor.after.clone();
        let mut exhausted = false;
        'scan: loop {
            let page = view.scan_page(Collection::KvHistoryV1.id(), &request)?;
            for row in &page.rows {
                if examined >= budget.max_examined || deleted >= budget.max_deletes {
                    break 'scan;
                }
                let decoded = ordered_key::decode_history(&row.key)
                    .map_err(|_| EngineError::new(ErrorClass::Corrupt, "history key"))?;
                let revision = decoded.revision.expect("history key has revision");
                examined += 1;
                let this_key = (decoded.namespace, decoded.key);
                match candidate.take() {
                    Some((row_key, key)) if key == this_key && revision <= floor => {
                        // A newer version at or below the floor supersedes it.
                        updates.push(StoreUpdate {
                            collection: Collection::KvHistoryV1.id(),
                            key: row_key.clone(),
                            value: None,
                        });
                        deleted += 1;
                        last_decided = Some(row_key);
                        candidate = Some((row.key.clone(), this_key));
                    }
                    Some((row_key, _)) => {
                        // Key changed or this version is above the floor: the
                        // candidate is the kept version of its key.
                        last_decided = Some(row_key);
                        if revision <= floor {
                            candidate = Some((row.key.clone(), this_key));
                        } else {
                            last_decided = Some(row.key.clone());
                        }
                    }
                    None => {
                        if revision <= floor {
                            candidate = Some((row.key.clone(), this_key));
                        } else {
                            last_decided = Some(row.key.clone());
                        }
                    }
                }
            }
            if page.exhausted {
                if let Some((row_key, _)) = candidate.take() {
                    last_decided = Some(row_key);
                }
                exhausted = true;
                break;
            }
            request.resume_after = page.rows.last().map(|r| r.key.clone());
        }
        cursor.after = last_decided;
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
            // The deletion budget applies inside a revision too: delete as
            // many of its rows as the budget allows and advance the frontier
            // only once the whole revision is gone (the next step finds the
            // remaining rows).
            let mut partial = false;
            for row in page.rows {
                if deleted >= budget.max_deletes {
                    partial = true;
                    break;
                }
                updates.push(StoreUpdate {
                    collection: Collection::EventsV1.id(),
                    key: row.key,
                    value: None,
                });
                deleted += 1;
            }
            if partial {
                events_done = false;
                break;
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
