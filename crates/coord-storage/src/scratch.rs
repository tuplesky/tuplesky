//! SCRATCH (ceilings, never merged): test-only switches that take storage
//! off the hot path, to measure what the architecture costs without it.
//!
//! - `COORDD_X_MEM_EXEC=1`: execution and the read fast path plan against
//!   an in-memory map of the application rows ([`Shadow`]), seeded from the
//!   projection at boot and extended as each batch is accepted, so no store
//!   read is on the hot path.
//! - [`set_no_mat`]: a projection commit is pretended (the in-memory stamp,
//!   frontier and gate move, nothing is written), so no materializer runs.
//!   The node sets it from `COORDD_X_STORE_OFF_AFTER` (every voter) or
//!   `COORDD_X_FOLLOWERS_IDLE_AFTER` (followers only).
//! - [`set_stale_reads`]: a follower that no longer executes serves the read
//!   fast path from its stale projection, skipping the position and floor
//!   checks.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

use coord_core::effect::{CollectionId, StoreUpdate};
use coord_store_api::engine::{
    Direction, EngineError, ErrorClass, OrderedRead, Row, RowPage, ScanRequest,
};
use coord_store_api::registry::Collection;

static NO_MAT: AtomicBool = AtomicBool::new(false);
static STALE_READS: AtomicBool = AtomicBool::new(false);

/// Whether projection commits are pretended.
pub fn no_mat() -> bool {
    NO_MAT.load(Ordering::Relaxed)
}

/// Pretend projection commits from now on, or stop.
pub fn set_no_mat(on: bool) {
    if NO_MAT.swap(on, Ordering::Relaxed) != on {
        eprintln!("SCRATCH no-materializer {on}");
    }
}

/// Whether the read fast path serves a stale projection.
pub fn stale_reads() -> bool {
    STALE_READS.load(Ordering::Relaxed)
}

/// Serve the read fast path from the stale projection, or stop.
pub fn set_stale_reads(on: bool) {
    if STALE_READS.swap(on, Ordering::Relaxed) != on {
        eprintln!("SCRATCH stale-reads {on}");
    }
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.parse().ok()
}

/// `COORDD_X_MEM_EXEC=1`.
pub fn mem_exec() -> bool {
    static V: OnceLock<bool> = OnceLock::new();
    *V.get_or_init(|| {
        let on = std::env::var("COORDD_X_MEM_EXEC").is_ok_and(|v| v == "1");
        if on {
            eprintln!("SCRATCH mem-exec on");
        }
        on
    })
}

/// `COORDD_X_STORE_OFF_AFTER`: executed commands after which no voter
/// materializes.
pub fn store_off_after() -> Option<u64> {
    static V: OnceLock<Option<u64>> = OnceLock::new();
    *V.get_or_init(|| env_u64("COORDD_X_STORE_OFF_AFTER"))
}

/// `COORDD_X_FOLLOWERS_IDLE_AFTER`: executed commands after which a
/// follower neither executes nor materializes.
pub fn followers_idle_after() -> Option<u64> {
    static V: OnceLock<Option<u64>> = OnceLock::new();
    *V.get_or_init(|| env_u64("COORDD_X_FOLLOWERS_IDLE_AFTER"))
}

/// The application rows in memory, by collection, under their own keys.
#[derive(Default)]
pub struct Shadow {
    rows: RwLock<BTreeMap<CollectionId, BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl Shadow {
    /// Every row of every collection `view` holds but the histories.
    pub fn seed<V: OrderedRead>(view: &V) -> Result<Self, EngineError> {
        let mut rows: BTreeMap<CollectionId, BTreeMap<Vec<u8>, Vec<u8>>> = BTreeMap::new();
        let mut count = 0usize;
        for collection in Collection::ALL {
            if matches!(collection, Collection::KvHistoryV1 | Collection::EventsV1) {
                continue;
            }
            let id = collection.id();
            let mut request = ScanRequest::all(4096, 16 << 20);
            loop {
                let page = view.scan_page(id, &request)?;
                let last = page.rows.last().map(|r| r.key.clone());
                for row in page.rows {
                    rows.entry(id).or_default().insert(row.key, row.value);
                    count += 1;
                }
                if page.exhausted || last.is_none() {
                    break;
                }
                request.resume_after = last;
            }
        }
        eprintln!("SCRATCH shadow seeded with {count} rows");
        Ok(Shadow {
            rows: RwLock::new(rows),
        })
    }

    /// Lay one batch's updates over the rows, in order.
    pub fn extend(&self, updates: &[StoreUpdate]) {
        let mut rows = self.rows.write().expect("shadow lock");
        for update in updates {
            let table = rows.entry(update.collection).or_default();
            match &update.value {
                Some(value) => {
                    table.insert(update.key.clone(), value.clone());
                }
                None => {
                    table.remove(&update.key);
                }
            }
        }
    }

    /// One row.
    pub fn get(&self, collection: CollectionId, key: &[u8]) -> Option<Vec<u8>> {
        let rows = self.rows.read().expect("shadow lock");
        rows.get(&collection).and_then(|t| t.get(key).cloned())
    }

    /// One page of a scan.
    pub fn scan_page(
        &self,
        collection: CollectionId,
        request: &ScanRequest,
    ) -> Result<RowPage, EngineError> {
        let rows = self.rows.read().expect("shadow lock");
        let Some(table) = rows.get(&collection) else {
            return Ok(RowPage {
                rows: Vec::new(),
                exhausted: true,
            });
        };
        let mut lower = request.lower.clone();
        let mut upper = request.upper.clone();
        if let Some(cursor) = &request.resume_after {
            match request.direction {
                Direction::Forward => lower = Bound::Excluded(cursor.clone()),
                Direction::Reverse => upper = Bound::Excluded(cursor.clone()),
            }
        }
        // An empty or inverted interval is no rows, not a panic.
        let empty = match (&lower, &upper) {
            (Bound::Included(l), Bound::Included(u)) => l > u,
            (Bound::Included(l), Bound::Excluded(u))
            | (Bound::Excluded(l), Bound::Included(u))
            | (Bound::Excluded(l), Bound::Excluded(u)) => l >= u,
            _ => false,
        };
        if empty {
            return Ok(RowPage {
                rows: Vec::new(),
                exhausted: true,
            });
        }
        let range = table.range::<Vec<u8>, _>((lower, upper));
        let iter: Box<dyn Iterator<Item = (&Vec<u8>, &Vec<u8>)>> = match request.direction {
            Direction::Forward => Box::new(range),
            Direction::Reverse => Box::new(range.rev()),
        };
        let mut out = Vec::new();
        let mut bytes = 0usize;
        for (key, value) in iter {
            let cost = key.len() + value.len();
            if out.is_empty() && cost > request.max_bytes.get() as usize {
                return Err(EngineError::new(
                    ErrorClass::Limit,
                    "row larger than the page byte budget",
                ));
            }
            if out.len() as u32 >= request.max_rows.get()
                || (!out.is_empty() && bytes + cost > request.max_bytes.get() as usize)
            {
                return Ok(RowPage {
                    rows: out,
                    exhausted: false,
                });
            }
            bytes += cost;
            out.push(Row {
                key: key.clone(),
                value: value.clone(),
            });
        }
        Ok(RowPage {
            rows: out,
            exhausted: true,
        })
    }
}

/// A shared handle on the shadow.
pub type ShadowRef = Arc<Shadow>;
