//! Reference KV/transaction model. Written independently of the production
//! planner from design Sections 6.1-6.3 and 6.5.

use std::collections::BTreeMap;

use coord_types::ids::KvRevision;
use coord_types::logical_v1::{
    BranchOp, CanonicalOperation, Compare, CompareOperand, CompareResult, CompareTarget,
    DeleteRangeOp, KeyRange, PutOp, RangeOp, limits,
};

use crate::history::{WatchEvent, WatchEventKind};

/// A stored key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct KvEntry {
    /// Value.
    pub value: Vec<u8>,
    /// Creation revision.
    pub create_revision: u64,
    /// Last modification revision.
    pub mod_revision: u64,
    /// Version counter (1 at creation).
    pub version: u64,
    /// Attached lease (opaque here).
    pub lease: Option<[u8; 16]>,
}

/// One item of a range result.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RangeItem {
    /// Key.
    pub key: Vec<u8>,
    /// Entry.
    pub entry: KvEntry,
}

/// Operation outcome as the model computes it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Outcome {
    /// Put applied; previous entry when requested.
    Put {
        /// Previous entry if requested and present.
        prev: Option<KvEntry>,
    },
    /// Delete applied.
    Delete {
        /// Keys deleted.
        deleted: u64,
        /// Previous entries if requested.
        prev: Vec<RangeItem>,
    },
    /// Read.
    Range {
        /// Items (empty when count-only).
        items: Vec<RangeItem>,
        /// Total matching keys.
        count: u64,
        /// Whether more items exist beyond the limit.
        more: bool,
    },
    /// Transaction.
    Txn {
        /// Whether the success branch ran.
        succeeded: bool,
        /// Outcomes of the executed branch, in order.
        results: Vec<Outcome>,
    },
    /// Compaction applied.
    Compacted,
    /// Read below the compaction floor.
    ErrCompacted,
    /// Read at a revision the domain has not reached.
    ErrFutureRevision,
    /// Lease operations are outside this model's scope (extension point).
    Unsupported,
}

/// Model response: header revision plus outcome.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ModelResponse {
    /// Domain revision after the operation (unchanged for non-mutations).
    pub revision: u64,
    /// Outcome.
    pub outcome: Outcome,
}

/// One history row: `(revision, key)` with the entry stored at that revision.
type HistoryRow<'a> = (&'a (u64, Vec<u8>), &'a Option<KvEntry>);

/// The reference model of one domain.
#[derive(Clone, Debug, Default)]
pub struct KvModel {
    current: BTreeMap<Vec<u8>, KvEntry>,
    /// Per-revision snapshots of changed keys: `(revision, key) -> entry or tombstone`.
    history: BTreeMap<(u64, Vec<u8>), Option<KvEntry>>,
    /// Events produced by each revision.
    events: BTreeMap<u64, Vec<WatchEvent>>,
    revision: u64,
    compact_floor: u64,
    /// Retry table: retry identity -> response returned the first time.
    retries: BTreeMap<u64, ModelResponse>,
}

impl KvModel {
    /// Keys currently attached to `lease`, in order.
    pub fn attached_keys(&self, lease: [u8; 16]) -> Vec<Vec<u8>> {
        self.current
            .iter()
            .filter(|(_, e)| e.lease == Some(lease))
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Current revision.
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Events of a revision, if it mutated KV.
    pub fn events_at(&self, revision: u64) -> Option<&[WatchEvent]> {
        self.events.get(&revision).map(Vec::as_slice)
    }

    /// Fingerprint of the complete state for search memoization.
    pub fn fingerprint(&self) -> [u8; 32] {
        let mut h = blake3::Hasher::new();
        h.update(&self.revision.to_be_bytes());
        h.update(&self.compact_floor.to_be_bytes());
        for (k, e) in &self.current {
            h.update(&(k.len() as u64).to_be_bytes());
            h.update(k);
            h.update(&(e.value.len() as u64).to_be_bytes());
            h.update(&e.value);
            h.update(&e.create_revision.to_be_bytes());
            h.update(&e.mod_revision.to_be_bytes());
            h.update(&e.version.to_be_bytes());
            match &e.lease {
                Some(l) => {
                    h.update(&[1]);
                    h.update(l);
                }
                None => {
                    h.update(&[0]);
                }
            }
        }
        for (id, r) in &self.retries {
            h.update(&id.to_be_bytes());
            h.update(&r.revision.to_be_bytes());
        }
        *h.finalize().as_bytes()
    }

    /// Apply an operation. `retry` is the stable invocation identity when
    /// the caller supplied one: a repeated identity returns the first
    /// response without mutating (Section 6.5).
    pub fn apply(&mut self, op: &CanonicalOperation, retry: Option<u64>) -> ModelResponse {
        if let Some(id) = retry
            && let Some(prior) = self.retries.get(&id)
        {
            return prior.clone();
        }
        let response = self.apply_inner(op);
        if let Some(id) = retry {
            self.retries.insert(id, response.clone());
        }
        response
    }

    fn apply_inner(&mut self, op: &CanonicalOperation) -> ModelResponse {
        match op {
            CanonicalOperation::Range(r) => ModelResponse {
                revision: self.revision,
                outcome: self.read(r),
            },
            CanonicalOperation::Put(p) => {
                let mut events = Vec::new();
                let outcome = self.put(p, self.revision + 1, &mut events);
                self.commit(events);
                ModelResponse {
                    revision: self.revision,
                    outcome,
                }
            }
            CanonicalOperation::DeleteRange(d) => {
                let mut events = Vec::new();
                let outcome = self.delete(d, self.revision + 1, &mut events);
                if events.is_empty() {
                    // Deleting nothing is not a mutation (Section 6.2).
                    ModelResponse {
                        revision: self.revision,
                        outcome,
                    }
                } else {
                    self.commit(events);
                    ModelResponse {
                        revision: self.revision,
                        outcome,
                    }
                }
            }
            CanonicalOperation::Txn(t) => {
                let succeeded = t.compares.iter().all(|c| self.compare(c));
                let branch = if succeeded { &t.success } else { &t.failure };
                let next = self.revision + 1;
                let mut events = Vec::new();
                let mut results = Vec::new();
                for b in branch {
                    results.push(match b {
                        BranchOp::Range(r) => self.read(r),
                        BranchOp::Put(p) => self.put(p, next, &mut events),
                        BranchOp::DeleteRange(d) => self.delete(d, next, &mut events),
                    });
                }
                if !events.is_empty() {
                    self.commit(events);
                }
                ModelResponse {
                    revision: self.revision,
                    outcome: Outcome::Txn { succeeded, results },
                }
            }
            CanonicalOperation::Compact { revision } => {
                let target = revision.get().min(self.revision);
                if target > self.compact_floor {
                    self.compact_floor = target;
                    // Keep the newest version (or tombstone) at or below the
                    // floor for every key plus all newer versions, so a read
                    // exactly at the floor stays answerable (Section 17.5).
                    let mut newest: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
                    for (r, k) in self.history.keys() {
                        if *r <= target {
                            let e = newest.entry(k.clone()).or_insert(*r);
                            *e = (*e).max(*r);
                        }
                    }
                    self.history
                        .retain(|(r, k), _| *r > target || newest.get(k) == Some(r));
                }
                ModelResponse {
                    revision: self.revision,
                    outcome: Outcome::Compacted,
                }
            }
            CanonicalOperation::LeaseGrant { .. }
            | CanonicalOperation::LeaseKeepAlive { .. }
            | CanonicalOperation::LeaseRevoke { .. }
            | CanonicalOperation::LeaseTimeToLive { .. }
            | CanonicalOperation::KineCreate(_)
            | CanonicalOperation::KineUpdate(_)
            | CanonicalOperation::KineDelete(_) => ModelResponse {
                revision: self.revision,
                outcome: Outcome::Unsupported,
            },
        }
    }

    fn commit(&mut self, events: Vec<WatchEvent>) {
        self.revision += 1;
        for e in &events {
            let entry = match e.kind {
                WatchEventKind::Put => self.current.get(&e.key).cloned(),
                WatchEventKind::Delete => None,
            };
            self.history.insert((self.revision, e.key.clone()), entry);
        }
        self.events.insert(self.revision, events);
    }

    fn put(&mut self, p: &PutOp, revision: u64, events: &mut Vec<WatchEvent>) -> Outcome {
        let prev = self.current.get(&p.key).cloned();
        let entry = KvEntry {
            value: p.value.clone(),
            create_revision: prev.as_ref().map_or(revision, |e| e.create_revision),
            mod_revision: revision,
            version: prev.as_ref().map_or(1, |e| e.version + 1),
            lease: p.lease.map(|l| l.0),
        };
        self.current.insert(p.key.clone(), entry);
        events.push(WatchEvent {
            kind: WatchEventKind::Put,
            key: p.key.clone(),
            value: p.value.clone(),
        });
        Outcome::Put {
            prev: if p.prev_kv { prev } else { None },
        }
    }

    fn delete(
        &mut self,
        d: &DeleteRangeOp,
        _revision: u64,
        events: &mut Vec<WatchEvent>,
    ) -> Outcome {
        let keys: Vec<Vec<u8>> = self.select(&d.range).map(|(k, _)| k.clone()).collect();
        let mut prev = Vec::new();
        for k in &keys {
            if let Some(entry) = self.current.remove(k) {
                if d.prev_kv {
                    prev.push(RangeItem {
                        key: k.clone(),
                        entry,
                    });
                }
                events.push(WatchEvent {
                    kind: WatchEventKind::Delete,
                    key: k.clone(),
                    value: Vec::new(),
                });
            }
        }
        Outcome::Delete {
            deleted: keys.len() as u64,
            prev,
        }
    }

    fn select<'a>(
        &'a self,
        range: &'a KeyRange,
    ) -> impl Iterator<Item = (&'a Vec<u8>, &'a KvEntry)> + 'a {
        let (lo, hi) = (range.key.clone(), range.range_end.clone());
        self.current.iter().filter(move |(k, _)| match &hi {
            None => **k == lo,
            Some(h) => **k >= lo && **k < *h,
        })
    }

    fn read(&self, r: &RangeOp) -> Outcome {
        let items: Vec<RangeItem> = match r.revision {
            None => self
                .select(&r.range)
                .map(|(k, e)| RangeItem {
                    key: k.clone(),
                    entry: e.clone(),
                })
                .collect(),
            Some(rev) => {
                let rev = rev.get();
                if rev > self.revision {
                    return Outcome::ErrFutureRevision;
                }
                if rev < self.compact_floor {
                    return Outcome::ErrCompacted;
                }
                self.historical(rev, &r.range)
            }
        };
        let count = items.len() as u64;
        if r.count_only {
            return Outcome::Range {
                items: Vec::new(),
                count,
                more: false,
            };
        }
        // Zero selects the schema maximum page (`RangeOp` contract), never
        // an unbounded page.
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
        Outcome::Range { items, count, more }
    }

    /// State of `range` as of `rev`: greatest version of each key at or
    /// below `rev`, excluding tombstones, from the current state rolled back
    /// through history. Keys untouched since `rev` keep their current entry.
    fn historical(&self, rev: u64, range: &KeyRange) -> Vec<RangeItem> {
        let mut snapshot: BTreeMap<Vec<u8>, Option<KvEntry>> = self
            .current
            .iter()
            .map(|(k, e)| (k.clone(), Some(e.clone())))
            .collect();
        // Walk history newer than `rev` from newest to oldest, restoring the
        // previous version of each key that changed after `rev`.
        let mut newer: Vec<HistoryRow<'_>> = self.history.range((rev + 1, Vec::new())..).collect();
        newer.sort_by(|a, b| b.0.cmp(a.0));
        for ((r, key), _entry) in newer {
            let older = self
                .history
                .range(..(*r, key.clone()))
                .rev()
                .find(|((_, k), _)| k == key)
                .map(|(_, e)| e.clone())
                .unwrap_or(None);
            // A key first created after `rev` had no version at `rev`.
            let older = older.filter(|e| e.mod_revision <= rev);
            snapshot.insert(key.clone(), older);
        }
        snapshot
            .into_iter()
            .filter_map(|(k, e)| e.map(|e| RangeItem { key: k, entry: e }))
            .filter(|item| match &range.range_end {
                None => item.key == range.key,
                Some(h) => item.key >= range.key && item.key < *h,
            })
            .collect()
    }

    /// Whether every comparison holds against the current state (the
    /// conjunction selecting a transaction's success branch).
    pub fn compares_hold(&self, compares: &[Compare]) -> bool {
        compares.iter().all(|c| self.compare(c))
    }

    fn compare(&self, c: &Compare) -> bool {
        let entry = self.current.get(&c.key);
        let ordering = match (&c.target, &c.operand) {
            (CompareTarget::Version, CompareOperand::Counter(v)) => {
                entry.map_or(0, |e| e.version).cmp(v)
            }
            (CompareTarget::CreateRevision, CompareOperand::Counter(v)) => {
                entry.map_or(0, |e| e.create_revision).cmp(v)
            }
            (CompareTarget::ModRevision, CompareOperand::Counter(v)) => {
                entry.map_or(0, |e| e.mod_revision).cmp(v)
            }
            (CompareTarget::Value, CompareOperand::Bytes(b)) => entry
                .map_or(&[][..], |e| e.value.as_slice())
                .cmp(b.as_slice()),
            (CompareTarget::Lease, CompareOperand::Lease(l)) => {
                entry.and_then(|e| e.lease).cmp(&l.map(|x| x.0))
            }
            _ => return false,
        };
        match c.result {
            CompareResult::Equal => ordering.is_eq(),
            CompareResult::Greater => ordering.is_gt(),
            CompareResult::Less => ordering.is_lt(),
            CompareResult::NotEqual => ordering.is_ne(),
        }
    }

    /// Whether an operation would mutate KV from the current state.
    pub fn would_mutate(&self, op: &CanonicalOperation) -> bool {
        let mut probe = self.clone();
        let before = probe.revision;
        probe.apply_inner(op);
        probe.revision != before
    }

    /// Convenience: the current revision as a typed value.
    pub fn typed_revision(&self) -> KvRevision {
        KvRevision::new(self.revision).expect("dense revisions stay bounded")
    }
}
