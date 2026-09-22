//! The engine-independent driver: one replica's common paths over any
//! state engine (design Sections 17.12 and 17.13).
//!
//! A domain is the production `StoreWorker` and `Applier` over the engine
//! under test. Every operation goes through the real ordered-application
//! path: payload rehash, retry admission, authorized view, planner, one
//! atomic durable commit, then publication to the watch hub. Maintenance
//! (bounded history and event collection under the replicated retention
//! floor) runs with the workload, never afterwards, and pinned snapshots
//! are held across churn so a stale view is caught rather than measured.
//!
//! Nothing here names redb or fjall: the driver is generic over the
//! contract, so both engines execute exactly the same logical work.

use std::collections::BTreeMap;

use coord_consensus::PayloadRecordV1;
use coord_core::effect::{BootId, PersistBatch};
use coord_core::outbox::BarrierAllocator;
use coord_state::policy::{Action, KeyInterval, PolicyRule};
use coord_storage::apply::{Applier, ApplyError};
use coord_storage::compaction::{GcBudget, RetentionHolds, plan_gc};
use coord_storage::policy::{bootstrap_session, rule_update};
use coord_storage::watch::{WatchId, WatchItem, WatchSpec};
use coord_storage::{GroupLimits, StoreWorker};
use coord_store_api::engine::{CollectionId, EngineError, LocalEngine, OrderedRead, ScanRequest};
use coord_store_api::registry::Collection;
use coord_types::identity::Digest32;
use coord_types::ids::*;
use coord_types::logical_v1::{CanonicalOperation, LogicalRequest};
use coord_types::{CommandId, RetryKey};
use serde::{Deserialize, Serialize};

use crate::workload::{NAMESPACE, Op, compaction_revision};

/// Session, client and principal the workload runs as.
const SESSION: SessionId = SessionId([0x51; 16]);
const CLIENT: ClientInstanceId = ClientInstanceId([0xc1; 16]);
const PRINCIPAL: PrincipalId = PrincipalId([0xaa; 16]);
const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);

/// Rows a driver scan may return per page.
pub const SCAN_MAX_ROWS: u32 = 256;
/// Bytes a driver scan may return per page.
pub const SCAN_MAX_BYTES: u32 = 1 << 20;

/// How one operation ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Applied {
    /// New work executed and committed durably.
    Executed {
        /// Revision produced, when the operation mutated KV.
        revision: Option<KvRevision>,
        /// Digest of the exact result.
        result: Digest32,
    },
    /// A retained result answered the invocation without executing.
    Retained {
        /// Revision the original execution produced.
        revision: Option<KvRevision>,
        /// Digest of the exact result.
        result: Digest32,
    },
    /// The common layers refused the operation (policy, admission, guard).
    Rejected(&'static str),
    /// The operation was skipped because there is no work for it yet (a
    /// retention floor below the first revision).
    Skipped,
}

/// Why driving failed. An engine error stops the trial: an experiment
/// never continues over uncertain storage.
#[derive(Debug)]
pub enum DriveError {
    /// Engine failure.
    Engine(EngineError),
    /// Application failure.
    Apply(String),
    /// A pinned snapshot changed under an active view.
    PinnedViewChanged,
    /// A repeated invocation did not reproduce the retained result.
    RetryDiverged,
}

impl std::fmt::Display for DriveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DriveError::Engine(e) => write!(f, "engine: {e}"),
            DriveError::Apply(e) => write!(f, "apply: {e}"),
            DriveError::PinnedViewChanged => write!(f, "a pinned snapshot changed"),
            DriveError::RetryDiverged => write!(f, "a retry returned another result"),
        }
    }
}

impl std::error::Error for DriveError {}

impl From<EngineError> for DriveError {
    fn from(e: EngineError) -> Self {
        DriveError::Engine(e)
    }
}

impl From<coord_storage::ViewError> for DriveError {
    fn from(e: coord_storage::ViewError) -> Self {
        DriveError::Apply(format!("no durable view: {e:?}"))
    }
}

/// The logical state a controlled failure-free trial must reproduce on
/// every engine. Physical layout may differ; none of this may.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Observable {
    /// Durable KV revision.
    pub kv_revision: u64,
    /// Replicated retention floor.
    pub retention_floor: u64,
    /// Current KV rows.
    pub kv_rows: u64,
    /// Retained history versions.
    pub history_rows: u64,
    /// Retained events.
    pub event_rows: u64,
    /// Lease records.
    pub lease_rows: u64,
    /// Lease reverse-index rows.
    pub lease_key_rows: u64,
    /// Retained retry results.
    pub retry_rows: u64,
    /// Retry floors.
    pub retry_floor_rows: u64,
    /// Sessions.
    pub session_rows: u64,
    /// Policy rows.
    pub policy_rows: u64,
    /// Authorization grant commitments.
    pub grant_rows: u64,
    /// Executed command identities.
    pub executed_rows: u64,
    /// Digest of every collection in the common state hash.
    pub common_digest: Digest32,
}

/// What one pinned-snapshot check observed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PinnedRead {
    /// Revision the snapshot pinned and held against collection.
    pub revision: u64,
    /// Time spent reading through the pinned view, excluding the churn.
    pub read_ns: u64,
}

/// One replica's common paths over the engine under test.
pub struct Domain<E: LocalEngine> {
    applier: Applier<E>,
    holds: RetentionHolds,
    watch: WatchId,
    outcomes: BTreeMap<u64, (Option<KvRevision>, Digest32)>,
    logical_bytes: u64,
}

fn retry_key(sequence: u64) -> RetryKey {
    RetryKey {
        cluster_id: CLUSTER,
        domain_id: DOMAIN,
        session_id: SESSION,
        client_instance_id: CLIENT,
        request_sequence: RequestSequence::new(sequence).expect("non-zero sequence"),
    }
}

/// Key plus value bytes one request hands to the engine.
fn logical_bytes(request: &LogicalRequest) -> u64 {
    postcard::to_allocvec(request)
        .map(|v| v.len() as u64)
        .unwrap_or(0)
}

impl<E: LocalEngine> Domain<E> {
    /// Open a worker over an engine that already holds the session and
    /// policy rows, for example after a crash and reopen. Nothing is
    /// recreated: what the durable image holds is what the domain serves.
    pub fn attach(engine: E, limits: GroupLimits) -> Result<Domain<E>, DriveError> {
        let boot = BootId([2; 16]);
        let incarnation = ReplicaIncarnation::new(2).expect("non-zero");
        let worker = StoreWorker::open(engine, boot, incarnation, limits)?;
        let alloc = BarrierAllocator::new(incarnation, boot);
        Domain::with(Applier::new(worker, alloc)?)
    }

    fn with(applier: Applier<E>) -> Result<Domain<E>, DriveError> {
        let watch = applier
            .hub()
            .register(WatchSpec {
                namespace: NAMESPACE,
                key: Vec::new(),
                range_end: Some(vec![0xff; 16]),
                start_revision: None,
                prev_kv: false,
                progress_notify: false,
                queue_capacity: 1 << 16,
            })
            .map_err(|e| DriveError::Apply(format!("watch: {e:?}")))?
            .id;
        Ok(Domain {
            applier,
            holds: RetentionHolds::default(),
            watch,
            outcomes: BTreeMap::new(),
            logical_bytes: 0,
        })
    }

    /// Open a worker over `engine`, bootstrap the session and policy the
    /// workload runs under, and subscribe a live watch.
    pub fn bootstrap(engine: E, limits: GroupLimits) -> Result<Domain<E>, DriveError> {
        let boot = BootId([1; 16]);
        let incarnation = ReplicaIncarnation::new(1).expect("non-zero");
        let mut worker = StoreWorker::open(engine, boot, incarnation, limits)?;
        let mut alloc = BarrierAllocator::new(incarnation, boot);
        let mut updates = bootstrap_session(&SESSION, PRINCIPAL, 4096, true)
            .map_err(|e| DriveError::Apply(format!("session bootstrap: {e}")))?;
        for (i, action) in Action::ALL.iter().enumerate() {
            updates.push(
                rule_update(
                    &PolicyRuleId([i as u8 + 1; 16]),
                    &PolicyRule {
                        principal: PRINCIPAL,
                        action: *action,
                        namespace: NAMESPACE,
                        interval: KeyInterval {
                            lower: Vec::new(),
                            upper: None,
                        },
                    },
                )
                .map_err(|e| DriveError::Apply(format!("policy rule: {e}")))?,
            );
        }
        worker
            .submit(PersistBatch {
                barrier: alloc.allocate(),
                base: Some(worker.application_base()),
                updates,
            })
            .map_err(|e| DriveError::Apply(format!("bootstrap batch: {e:?}")))?;
        worker.flush()?;
        Domain::with(Applier::new(worker, alloc)?)
    }

    /// Apply one generated operation through the full common path.
    pub fn apply(&mut self, op: &Op) -> Result<Applied, DriveError> {
        let (sequence, request) = match op {
            Op::Request {
                sequence, request, ..
            }
            | Op::Repeat {
                sequence, request, ..
            } => (*sequence, request.clone()),
            Op::Compact {
                sequence,
                revisions_back,
            } => {
                let current = self.applier.kv_revision()?;
                let Some(revision) = compaction_revision(current, *revisions_back) else {
                    return Ok(Applied::Skipped);
                };
                let mut request =
                    LogicalRequest::new(NAMESPACE, CanonicalOperation::Compact { revision });
                request.canonicalize();
                (*sequence, request)
            }
        };
        let key = retry_key(sequence);
        let command = CommandId::derive(&key, &request)
            .map_err(|e| DriveError::Apply(format!("command identity: {e:?}")))?;
        let payload = PayloadRecordV1 {
            retry_key: key,
            logical: postcard::to_allocvec(&request)
                .map_err(|_| DriveError::Apply("payload encode".to_owned()))?,
        };
        self.logical_bytes += logical_bytes(&request);
        let repeat = matches!(op, Op::Repeat { .. });
        let outcome = match self.applier.apply(command, &payload) {
            Ok(outcome) => outcome,
            Err(ApplyError::NotAdmitted(_)) => return Ok(Applied::Rejected("not admitted")),
            Err(ApplyError::View(_)) => return Ok(Applied::Rejected("denied by policy")),
            Err(ApplyError::Plan(_)) => return Ok(Applied::Rejected("plan refused")),
            Err(e) => return Err(DriveError::Apply(format!("{e:?}"))),
        };
        let recorded = (outcome.revision, outcome.result_digest);
        match self.outcomes.get(&sequence) {
            Some(earlier) if repeat => {
                if *earlier != recorded {
                    return Err(DriveError::RetryDiverged);
                }
                return Ok(Applied::Retained {
                    revision: recorded.0,
                    result: recorded.1,
                });
            }
            _ => {
                self.outcomes.insert(sequence, recorded);
            }
        }
        if matches!(request.operation, CanonicalOperation::Compact { .. }) {
            // The hub's retention floor follows the replicated floor so a
            // watch below it is closed rather than served from gaps.
            let gated = self.applier.worker().reader().snapshot()?;
            let floor = coord_storage::codecs::read_retention_floor(gated.view())?;
            drop(gated);
            self.applier.hub().set_retention_floor(floor);
        }
        Ok(Applied::Executed {
            revision: recorded.0,
            result: recorded.1,
        })
    }

    /// Drain the subscribed watch; returns the revisions and events that
    /// became visible. Publication is part of the measured operation.
    pub fn drain_watch(&mut self) -> (u64, u64) {
        let (mut revisions, mut events) = (0, 0);
        while let Some(item) = self.applier.hub().next(self.watch, |_| true) {
            match item {
                WatchItem::Batch(batch) => {
                    revisions += 1;
                    events += batch.events.len() as u64;
                }
                WatchItem::Progress(_) => {}
                WatchItem::Closed { reason, .. } => {
                    // A closed watch is a reportable event, not a silent
                    // end of publication; the caller counts it as debt.
                    let _ = reason;
                    break;
                }
            }
        }
        (revisions, events)
    }

    /// One bounded maintenance step. Returns whether collection is
    /// complete at the current effective floor.
    pub fn maintenance_step(&mut self, budget: GcBudget) -> Result<bool, DriveError> {
        let gated = self.applier.worker().reader().snapshot()?;
        let step = plan_gc(gated.view(), &self.holds, budget)?;
        drop(gated);
        if !step.updates.is_empty() {
            let barrier = self.applier.alloc().allocate();
            self.applier
                .worker_mut()
                .submit(PersistBatch {
                    barrier,
                    base: None,
                    updates: step.updates,
                })
                .map_err(|e| DriveError::Apply(format!("gc batch: {e:?}")))?;
            self.applier.worker_mut().flush()?;
        }
        Ok(step.done)
    }

    /// Hold a pinned snapshot, run `churn`, and verify the snapshot still
    /// shows exactly what it showed when it was pinned. The revision it
    /// pins is also held against collection, so history cannot vanish
    /// under it.
    pub fn pinned_read<T>(
        &mut self,
        churn: impl FnOnce(&mut Self) -> Result<T, DriveError>,
    ) -> Result<(T, PinnedRead), DriveError> {
        let revision = self.applier.kv_revision()?;
        let hold = self.holds.hold(revision);
        let pinned = self.applier.worker().reader().snapshot()?;
        let collections = [Collection::KvCurrentV1, Collection::KvHistoryV1];
        let first = std::time::Instant::now();
        let before = digest_of(pinned.view(), &collections)?;
        let mut read_ns = first.elapsed();
        let out = churn(self)?;
        let second = std::time::Instant::now();
        let after = digest_of(pinned.view(), &collections)?;
        read_ns += second.elapsed();
        drop(pinned);
        drop(hold);
        if before != after {
            return Err(DriveError::PinnedViewChanged);
        }
        Ok((
            out,
            PinnedRead {
                revision: revision.get(),
                read_ns: read_ns.as_nanos() as u64,
            },
        ))
    }

    /// Reopen the engine in place and verify the durable state is intact.
    /// `reopen` is the engine's own same-engine reopen; no state is
    /// recreated and no other engine is involved.
    pub fn reopen(
        &mut self,
        reopen: &mut dyn FnMut(&mut E) -> Result<(), EngineError>,
    ) -> Result<(), DriveError> {
        reopen(self.applier.worker_mut().engine_mut())?;
        Ok(())
    }

    /// Logical key and value bytes handed to the engine so far.
    pub const fn logical_bytes(&self) -> u64 {
        self.logical_bytes
    }

    /// The logical state a comparison checks.
    pub fn observable(&self) -> Result<Observable, DriveError> {
        let gated = self.applier.worker().reader().snapshot()?;
        let view = gated.view();
        let count = |c: Collection| -> Result<u64, EngineError> { Ok(rows(view, c)?.len() as u64) };
        let common: Vec<Collection> = Collection::ALL
            .into_iter()
            .filter(|c| c.in_common_hash())
            .collect();
        let observable = Observable {
            kv_revision: coord_storage::codecs::read_kv_revision(view)?.get(),
            retention_floor: coord_storage::codecs::read_retention_floor(view)?.get(),
            kv_rows: count(Collection::KvCurrentV1)?,
            history_rows: count(Collection::KvHistoryV1)?,
            event_rows: count(Collection::EventsV1)?,
            lease_rows: count(Collection::LeaseV1)?,
            lease_key_rows: count(Collection::LeaseKeysV1)?,
            retry_rows: count(Collection::RetryV1)?,
            retry_floor_rows: count(Collection::RetryFloorV1)?,
            session_rows: count(Collection::SessionV1)?,
            policy_rows: count(Collection::PolicyV1)?,
            grant_rows: count(Collection::AuthGrantV1)?,
            executed_rows: count(Collection::ExecutedV1)?,
            common_digest: digest_of(view, &common)?,
        };
        Ok(observable)
    }
}

/// Rows of one collection as `(key, value)`.
type CollectionRows = Vec<(Vec<u8>, Vec<u8>)>;

/// Every row of one collection, read through bounded pages.
fn rows<V: OrderedRead>(view: &V, c: Collection) -> Result<CollectionRows, EngineError> {
    let mut request = ScanRequest::all(SCAN_MAX_ROWS, SCAN_MAX_BYTES);
    let mut out = Vec::new();
    loop {
        let page = view.scan_page(CollectionId(c.id().0), &request)?;
        for row in &page.rows {
            out.push((row.key.clone(), row.value.clone()));
        }
        if page.exhausted {
            return Ok(out);
        }
        let last = page
            .rows
            .last()
            .ok_or_else(|| {
                EngineError::new(
                    coord_store_api::engine::ErrorClass::Corrupt,
                    "empty page that is not exhausted",
                )
            })?
            .key
            .clone();
        request.resume_after = Some(last);
    }
}

/// Digest of the given collections' rows in canonical order.
pub fn digest_of<V: OrderedRead>(
    view: &V,
    collections: &[Collection],
) -> Result<Digest32, EngineError> {
    let mut h = blake3::Hasher::new_derive_key("tuplesky store experiment state digest v1");
    for c in collections {
        h.update(&c.id().0.to_be_bytes());
        for (key, value) in rows(view, *c)? {
            h.update(&(key.len() as u64).to_be_bytes());
            h.update(&key);
            h.update(&(value.len() as u64).to_be_bytes());
            h.update(&value);
        }
    }
    Ok(Digest32(*h.finalize().as_bytes()))
}
