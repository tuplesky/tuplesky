//! Watch replay and live handoff (task-13; design Sections 6.4, 6.8.2-6.8.3,
//! 19.3).
//!
//! Registration is subscribe-first: under the hub lock the watcher is
//! attached at the published frontier `F`; every revision published after
//! that is queued live. The caller then replays revisions `start..=F` from a
//! snapshot (the events of a durable revision are immutable) and marks the
//! replay complete. Live batches are handed out only after that, so a
//! mutation applied during registration appears exactly once and in order.
//!
//! Queues are bounded per watch. Overflow closes the watch with the last
//! complete revision it delivered; the consumer resumes from there and the
//! gap is replayed from storage. Nothing is ever skipped silently.
//!
//! Progress states that every matching event through a revision has been
//! delivered to the consumer. It is derived from what the consumer took out
//! of the queue, never from what arrived, so it cannot overtake pending
//! events.
//!
//! Every batch selected for delivery passes the caller's output
//! authorization decision (Section 6.4); a denial closes the watch.

use std::collections::{BTreeMap, VecDeque};

use coord_state::{KvEvent, KvEventKind};
use coord_store_api::engine::{EngineError, OrderedRead};
use coord_types::ids::{KvRevision, NamespaceId};

use crate::codecs;
use crate::sync::{Arc, Mutex, lock};

/// Watch identity within a hub.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WatchId(pub u64);

/// What a watch selects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WatchSpec {
    /// Namespace.
    pub namespace: NamespaceId,
    /// Start key.
    pub key: Vec<u8>,
    /// Exclusive end; `None` for the exact key.
    pub range_end: Option<Vec<u8>>,
    /// First revision to deliver (inclusive); `None` means live only.
    pub start_revision: Option<KvRevision>,
    /// Include previous entries.
    pub prev_kv: bool,
    /// Emit progress notifications.
    pub progress_notify: bool,
    /// Maximum queued events before the watch is closed as a slow consumer.
    pub queue_capacity: usize,
}

impl WatchSpec {
    fn matches(&self, namespace: &NamespaceId, key: &[u8]) -> bool {
        if *namespace != self.namespace {
            return false;
        }
        match &self.range_end {
            None => key == self.key.as_slice(),
            Some(end) => key >= self.key.as_slice() && key < end.as_slice(),
        }
    }

    fn filter(&self, namespace: &NamespaceId, events: &[KvEvent]) -> Vec<KvEvent> {
        events
            .iter()
            .filter(|e| self.matches(namespace, &e.key))
            .map(|e| {
                let mut e = e.clone();
                if !self.prev_kv {
                    e.prev = None;
                }
                e
            })
            .collect()
    }
}

/// One complete revision as delivered to a watch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WatchBatch {
    /// Revision of every event.
    pub revision: KvRevision,
    /// Matching events of that revision, in ordinal order.
    pub events: Vec<KvEvent>,
}

/// Why a watch closed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloseReason {
    /// Cancelled by its owner.
    Cancelled,
    /// The start revision is below the retention floor.
    Compacted,
    /// The queue overflowed; resume from `last_complete_revision`.
    SlowConsumer,
    /// Output authorization denied a batch.
    Unauthorized,
    /// The hub shut down.
    HubClosed,
}

/// An item taken from a watch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WatchItem {
    /// A complete revision.
    Batch(WatchBatch),
    /// Every matching event through `revision` has been delivered.
    Progress(KvRevision),
    /// The watch is closed; `last_complete_revision` is the resume point.
    Closed {
        /// Reason.
        reason: CloseReason,
        /// Last complete revision delivered (zero when none).
        last_complete_revision: KvRevision,
    },
}

/// A registration handed back to the caller for replay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Registration {
    /// The watch.
    pub id: WatchId,
    /// Revisions the caller must replay from storage: `from..=through`
    /// (`None` when the watch is live only or starts beyond the frontier).
    pub replay: Option<(KvRevision, KvRevision)>,
}

struct Watcher {
    spec: WatchSpec,
    /// Frontier at registration: live batches are strictly above it.
    frontier: KvRevision,
    replay: VecDeque<WatchBatch>,
    replay_complete: bool,
    live: VecDeque<WatchBatch>,
    queued_events: usize,
    /// Highest revision whose matching events were all handed out.
    delivered_complete: KvRevision,
    /// Highest revision processed by the hub for this watch (matching or
    /// not); progress may advance to it only once the queue is drained.
    processed_through: KvRevision,
    /// Highest progress already announced.
    announced: KvRevision,
    /// Highest live revision processed while replay was still running;
    /// folded into `processed_through` when replay completes.
    live_processed: KvRevision,
    closed: Option<CloseReason>,
}

struct HubState {
    published: KvRevision,
    retention_floor: KvRevision,
    next_id: u64,
    watchers: BTreeMap<WatchId, Watcher>,
    closed: bool,
}

/// The watch hub of one domain.
#[derive(Clone)]
pub struct WatchHub {
    inner: Arc<Mutex<HubState>>,
}

impl WatchHub {
    /// A hub whose published frontier and retention floor start at the
    /// given durable values.
    pub fn new(published: KvRevision, retention_floor: KvRevision) -> Self {
        WatchHub {
            inner: Arc::new(Mutex::new(HubState {
                published,
                retention_floor,
                next_id: 1,
                watchers: BTreeMap::new(),
                closed: false,
            })),
        }
    }

    /// Published frontier.
    pub fn published(&self) -> KvRevision {
        lock(&self.inner).published
    }

    /// Announce a newly applied revision with its complete event set. Must
    /// be called in revision order after the revision is durable; a gap is
    /// a programming error and is rejected.
    pub fn publish(
        &self,
        namespace: NamespaceId,
        revision: KvRevision,
        events: &[KvEvent],
    ) -> Result<(), PublishError> {
        let mut state = lock(&self.inner);
        if state.closed {
            return Err(PublishError::HubClosed);
        }
        let expected = state
            .published
            .checked_next()
            .map_err(|_| PublishError::Overflow)?;
        if revision != expected {
            return Err(PublishError::Gap {
                expected,
                got: revision,
            });
        }
        state.published = revision;
        for watcher in state.watchers.values_mut() {
            if watcher.closed.is_some() || revision <= watcher.frontier {
                continue;
            }
            let matching = watcher.spec.filter(&namespace, events);
            if watcher.replay_complete {
                watcher.processed_through = revision;
            } else {
                watcher.live_processed = revision;
            }
            if matching.is_empty() {
                continue;
            }
            if watcher.queued_events + matching.len() > watcher.spec.queue_capacity {
                // Never drop silently: close with the resume point. The
                // queued batches remain deliverable so the consumer can
                // drain them before it sees the close.
                watcher.closed = Some(CloseReason::SlowConsumer);
                continue;
            }
            watcher.queued_events += matching.len();
            watcher.live.push_back(WatchBatch {
                revision,
                events: matching,
            });
        }
        Ok(())
    }

    /// Record a new retention floor (task-14 drives it).
    pub fn set_retention_floor(&self, floor: KvRevision) {
        lock(&self.inner).retention_floor = floor;
    }

    /// Attach a watch at the current frontier. Live batches above the
    /// frontier queue from now on; the caller replays `registration.replay`
    /// from a snapshot and then calls [`WatchHub::replay_complete`].
    pub fn register(&self, spec: WatchSpec) -> Result<Registration, CloseReason> {
        let mut state = lock(&self.inner);
        if state.closed {
            return Err(CloseReason::HubClosed);
        }
        let frontier = state.published;
        let replay = match spec.start_revision {
            Some(start) if start < state.retention_floor => return Err(CloseReason::Compacted),
            Some(start) if start <= frontier => Some((start, frontier)),
            _ => None,
        };
        let id = WatchId(state.next_id);
        state.next_id += 1;
        let start_delivered = match spec.start_revision {
            Some(start) => KvRevision::new(start.get().saturating_sub(1)).expect("bounded"),
            None => frontier,
        };
        state.watchers.insert(
            id,
            Watcher {
                spec,
                frontier,
                replay: VecDeque::new(),
                replay_complete: replay.is_none(),
                live: VecDeque::new(),
                queued_events: 0,
                delivered_complete: start_delivered,
                processed_through: if replay.is_none() {
                    frontier
                } else {
                    start_delivered
                },
                announced: start_delivered,
                live_processed: frontier,
                closed: None,
            },
        );
        Ok(Registration { id, replay })
    }

    /// Feed one replayed revision (in order). Filtering applies here too.
    pub fn replay(
        &self,
        id: WatchId,
        namespace: NamespaceId,
        revision: KvRevision,
        events: &[KvEvent],
    ) -> Result<(), ReplayError> {
        let mut state = lock(&self.inner);
        let watcher = state
            .watchers
            .get_mut(&id)
            .ok_or(ReplayError::UnknownWatch)?;
        if watcher.replay_complete {
            return Err(ReplayError::ReplayFinished);
        }
        if revision > watcher.frontier {
            return Err(ReplayError::BeyondFrontier {
                frontier: watcher.frontier,
            });
        }
        if revision <= watcher.processed_through {
            return Err(ReplayError::OutOfOrder {
                processed_through: watcher.processed_through,
            });
        }
        watcher.processed_through = revision;
        let matching = watcher.spec.filter(&namespace, events);
        if matching.is_empty() {
            return Ok(());
        }
        if watcher.queued_events + matching.len() > watcher.spec.queue_capacity {
            return Err(ReplayError::QueueFull);
        }
        watcher.queued_events += matching.len();
        watcher.replay.push_back(WatchBatch {
            revision,
            events: matching,
        });
        Ok(())
    }

    /// The caller finished replaying through the frontier.
    pub fn replay_complete(&self, id: WatchId) -> Result<(), ReplayError> {
        let mut state = lock(&self.inner);
        let watcher = state
            .watchers
            .get_mut(&id)
            .ok_or(ReplayError::UnknownWatch)?;
        watcher.replay_complete = true;
        watcher.processed_through = watcher
            .processed_through
            .max(watcher.frontier)
            .max(watcher.live_processed);
        Ok(())
    }

    /// Take the next item for a watch. `authorize` decides, for the batch
    /// selected right now, whether the consumer may receive it. `None`
    /// means nothing is pending.
    pub fn next(
        &self,
        id: WatchId,
        authorize: impl FnOnce(&WatchBatch) -> bool,
    ) -> Option<WatchItem> {
        let mut state = lock(&self.inner);
        let watcher = state.watchers.get_mut(&id)?;
        let batch = if let Some(b) = watcher.replay.pop_front() {
            Some(b)
        } else if watcher.replay_complete {
            watcher.live.pop_front()
        } else {
            None
        };
        if let Some(batch) = batch {
            watcher.queued_events -= batch.events.len();
            if !authorize(&batch) {
                let last = watcher.delivered_complete;
                state.watchers.remove(&id);
                return Some(WatchItem::Closed {
                    reason: CloseReason::Unauthorized,
                    last_complete_revision: last,
                });
            }
            watcher.delivered_complete = batch.revision;
            watcher.announced = batch.revision;
            return Some(WatchItem::Batch(batch));
        }
        if let Some(reason) = watcher.closed {
            let last = watcher.delivered_complete;
            state.watchers.remove(&id);
            return Some(WatchItem::Closed {
                reason,
                last_complete_revision: last,
            });
        }
        // Progress only once the queue is drained and only up to what the
        // hub processed for this watch.
        if watcher.spec.progress_notify
            && watcher.replay_complete
            && watcher.processed_through > watcher.announced
        {
            watcher.announced = watcher.processed_through;
            watcher.delivered_complete = watcher.processed_through;
            return Some(WatchItem::Progress(watcher.processed_through));
        }
        None
    }

    /// Cancel a watch; the close is delivered on the next `next`, after any
    /// already queued batches.
    pub fn cancel(&self, id: WatchId) -> bool {
        let mut state = lock(&self.inner);
        match state.watchers.get_mut(&id) {
            Some(w) => {
                w.closed.get_or_insert(CloseReason::Cancelled);
                true
            }
            None => false,
        }
    }

    /// Close the hub: every watch closes after draining.
    pub fn close(&self) {
        let mut state = lock(&self.inner);
        state.closed = true;
        for w in state.watchers.values_mut() {
            w.closed.get_or_insert(CloseReason::HubClosed);
        }
    }

    /// Number of open watches.
    pub fn open_watches(&self) -> usize {
        lock(&self.inner).watchers.len()
    }
}

/// Publish failures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishError {
    /// Revisions must be published contiguously.
    Gap {
        /// Expected revision.
        expected: KvRevision,
        /// Presented revision.
        got: KvRevision,
    },
    /// Revision counter exhausted.
    Overflow,
    /// The hub is closed.
    HubClosed,
}

/// Replay failures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplayError {
    /// No such watch.
    UnknownWatch,
    /// Replay already marked complete.
    ReplayFinished,
    /// Replay must stay at or below the registration frontier.
    BeyondFrontier {
        /// Frontier.
        frontier: KvRevision,
    },
    /// Replayed revisions must increase.
    OutOfOrder {
        /// Highest revision processed.
        processed_through: KvRevision,
    },
    /// The replay batch does not fit; pace the replay.
    QueueFull,
}

/// Replay the events of `from..=through` from a snapshot into the watch.
/// Returns `Compacted` when `from` is below the snapshot's retention floor.
pub fn replay_from_view<V: OrderedRead>(
    hub: &WatchHub,
    view: &V,
    id: WatchId,
    namespace: NamespaceId,
    from: KvRevision,
    through: KvRevision,
) -> Result<(), ReplayFromViewError> {
    let floor = codecs::read_retention_floor(view)?;
    if from < floor {
        return Err(ReplayFromViewError::Compacted { floor });
    }
    let mut rev = from;
    loop {
        if let Some(events) = crate::views::events_at(view, rev)? {
            hub.replay(id, namespace, rev, &events)?;
        } else {
            // A revision without events (never produced) still counts as
            // processed so progress can pass it.
            hub.replay(id, namespace, rev, &[])?;
        }
        if rev >= through {
            break;
        }
        rev = rev.checked_next().map_err(|_| {
            ReplayFromViewError::Engine(EngineError::new(
                coord_store_api::engine::ErrorClass::Limit,
                "revision overflow",
            ))
        })?;
    }
    hub.replay_complete(id)?;
    Ok(())
}

/// Replay-from-view failures.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplayFromViewError {
    /// Engine failure.
    Engine(EngineError),
    /// Below the retention floor.
    Compacted {
        /// Floor.
        floor: KvRevision,
    },
    /// Hub replay failure.
    Replay(ReplayError),
}

impl From<EngineError> for ReplayFromViewError {
    fn from(e: EngineError) -> Self {
        ReplayFromViewError::Engine(e)
    }
}

impl From<ReplayError> for ReplayFromViewError {
    fn from(e: ReplayError) -> Self {
        ReplayFromViewError::Replay(e)
    }
}

/// A transport fragment of a revision batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fragment {
    /// Revision.
    pub revision: KvRevision,
    /// Events of this fragment.
    pub events: Vec<KvEvent>,
    /// Whether this is the last fragment of the revision.
    pub complete: bool,
}

/// Split a batch into transport fragments of at most `max_events` each. A
/// revision with no events still yields one complete fragment.
pub fn chunk(batch: &WatchBatch, max_events: usize) -> Vec<Fragment> {
    let max = max_events.max(1);
    if batch.events.is_empty() {
        return vec![Fragment {
            revision: batch.revision,
            events: Vec::new(),
            complete: true,
        }];
    }
    let chunks: Vec<&[KvEvent]> = batch.events.chunks(max).collect();
    let n = chunks.len();
    chunks
        .into_iter()
        .enumerate()
        .map(|(i, c)| Fragment {
            revision: batch.revision,
            events: c.to_vec(),
            complete: i + 1 == n,
        })
        .collect()
}

/// Consumer-side reassembly: nothing is exposed until the revision is
/// complete, and fragments must not interleave revisions.
#[derive(Debug, Default)]
pub struct RevisionAssembler {
    pending: Option<WatchBatch>,
}

/// Assembly failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AssembleError {
    /// A fragment of another revision arrived before the pending one completed.
    Interleaved {
        /// Pending revision.
        pending: KvRevision,
        /// Arriving revision.
        got: KvRevision,
    },
}

impl RevisionAssembler {
    /// Push a fragment; returns the complete batch when the revision closes.
    pub fn push(&mut self, fragment: Fragment) -> Result<Option<WatchBatch>, AssembleError> {
        match &mut self.pending {
            Some(batch) if batch.revision != fragment.revision => Err(AssembleError::Interleaved {
                pending: batch.revision,
                got: fragment.revision,
            }),
            Some(batch) => {
                batch.events.extend(fragment.events);
                if fragment.complete {
                    Ok(self.pending.take())
                } else {
                    Ok(None)
                }
            }
            None => {
                let batch = WatchBatch {
                    revision: fragment.revision,
                    events: fragment.events,
                };
                if fragment.complete {
                    Ok(Some(batch))
                } else {
                    self.pending = Some(batch);
                    Ok(None)
                }
            }
        }
    }

    /// Whether a revision is partially assembled (a close now loses it, and
    /// the consumer resumes from the last complete revision).
    pub fn has_partial(&self) -> bool {
        self.pending.is_some()
    }
}

/// Convenience: whether an event is a deletion.
pub fn is_delete(event: &KvEvent) -> bool {
    event.kind == KvEventKind::Delete
}
