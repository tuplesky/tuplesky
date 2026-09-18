//! Structural checks and the complete-domain linearizability search.

use std::collections::{BTreeMap, HashSet};

use coord_types::logical_v1::CanonicalOperation;

use crate::history::{History, Observation, OpId, WatchEvent};
use crate::model::{KvModel, ModelResponse};
use crate::report::{LatencyReport, Verdict, Violation};

/// One invocation with its optional response.
#[derive(Clone, Debug)]
struct Op {
    id: OpId,
    invoke: u64,
    respond: Option<u64>,
    retry: Option<u64>,
    op: CanonicalOperation,
    response: Option<ModelResponse>,
}

/// One observed watch batch: the ordered events of a revision and the
/// earliest real time any watcher delivered it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct WatchBatch {
    tick: u64,
    events: Vec<WatchEvent>,
}

struct Prepared {
    ops: Vec<Op>,
    /// Watch batches by revision, events in delivery order.
    watches: BTreeMap<u64, WatchBatch>,
    violations: Vec<Violation>,
}

fn prepare(history: &History) -> Prepared {
    let mut ops: Vec<Op> = Vec::new();
    let mut index: BTreeMap<OpId, usize> = BTreeMap::new();
    let mut violations = Vec::new();
    let mut watches: BTreeMap<u64, WatchBatch> = BTreeMap::new();
    let mut last_watch_revision = 0u64;
    for obs in history.observations() {
        match obs {
            Observation::Invoke {
                id,
                tick,
                retry,
                op,
                ..
            } => {
                if index.contains_key(id) {
                    violations.push(Violation::MalformedHistory {
                        detail: format!("duplicate invocation id {id}"),
                    });
                    continue;
                }
                index.insert(*id, ops.len());
                ops.push(Op {
                    id: *id,
                    invoke: *tick,
                    respond: None,
                    retry: *retry,
                    op: op.clone(),
                    response: None,
                });
            }
            Observation::Respond { id, tick, response } => match index.get(id) {
                Some(&i) if ops[i].response.is_none() => {
                    if *tick < ops[i].invoke {
                        violations.push(Violation::MalformedHistory {
                            detail: format!("response before invocation for {id}"),
                        });
                    }
                    ops[i].respond = Some(*tick);
                    ops[i].response = Some(response.clone());
                }
                Some(_) => violations.push(Violation::MalformedHistory {
                    detail: format!("second response for {id}"),
                }),
                None => violations.push(Violation::MalformedHistory {
                    detail: format!("response for unknown invocation {id}"),
                }),
            },
            Observation::WatchBatch {
                tick,
                revision,
                events,
            } => {
                if *revision < last_watch_revision {
                    violations.push(Violation::WatchOrder {
                        revision: *revision,
                    });
                }
                last_watch_revision = last_watch_revision.max(*revision);
                // Order and multiplicity within a revision batch are part of
                // what consumers observe, so batches compare as sequences.
                match watches.get_mut(revision) {
                    Some(existing) if existing.events != *events => {
                        violations.push(Violation::ConflictingWatchBatches {
                            revision: *revision,
                        })
                    }
                    Some(existing) => existing.tick = existing.tick.min(*tick),
                    None => {
                        watches.insert(
                            *revision,
                            WatchBatch {
                                tick: *tick,
                                events: events.clone(),
                            },
                        );
                    }
                }
            }
        }
    }
    Prepared {
        ops,
        watches,
        violations,
    }
}

fn is_mutation(op: &CanonicalOperation) -> bool {
    matches!(
        op,
        CanonicalOperation::Put(_)
            | CanonicalOperation::DeleteRange(_)
            | CanonicalOperation::Txn(_)
    )
}

/// Revisions must be unique among acknowledged mutations: a response whose
/// revision advanced belongs to exactly one operation. A retry identity binds
/// one canonical operation to one response: a second invocation under the
/// same identity must carry the same operation (a changed payload must be
/// rejected, never served the cached result) and receive the same response.
fn check_revisions(ops: &[Op], violations: &mut Vec<Violation>) {
    let mut seen: BTreeMap<u64, OpId> = BTreeMap::new();
    let mut by_retry: BTreeMap<u64, &Op> = BTreeMap::new();
    for op in ops {
        if let Some(retry) = op.retry {
            if let Some(first) = by_retry.get(&retry) {
                let same_op = first.op == op.op;
                let same_response = match (&first.response, &op.response) {
                    (Some(a), Some(b)) => a == b,
                    _ => true,
                };
                if !same_op || !same_response {
                    violations.push(Violation::RetryInconsistent {
                        first: first.id,
                        second: op.id,
                    });
                }
                continue;
            }
            by_retry.insert(retry, op);
        }
        let Some(resp) = &op.response else { continue };
        if !is_mutation(&op.op) {
            continue;
        }
        let mutated = match &resp.outcome {
            crate::model::Outcome::Put { .. } => true,
            crate::model::Outcome::Delete { deleted, .. } => *deleted > 0,
            crate::model::Outcome::Txn { results, .. } => results.iter().any(|r| match r {
                crate::model::Outcome::Put { .. } => true,
                crate::model::Outcome::Delete { deleted, .. } => *deleted > 0,
                _ => false,
            }),
            _ => false,
        };
        if !mutated {
            continue;
        }
        if let Some(first) = seen.get(&resp.revision) {
            violations.push(Violation::SharedRevision {
                first: *first,
                second: op.id,
                revision: resp.revision,
            });
        } else {
            seen.insert(resp.revision, op.id);
        }
    }
}

struct Search<'a> {
    ops: &'a [Op],
    watches: &'a BTreeMap<u64, WatchBatch>,
    /// Highest revision any watch batch reported; the witness must produce
    /// every one of them.
    max_watched: u64,
    memo: HashSet<(Vec<u64>, [u8; 32])>,
    nodes: u64,
    best_explained: usize,
    best_stuck: Option<OpId>,
    limit: u64,
}

impl Search<'_> {
    fn bits(done: &[bool]) -> Vec<u64> {
        let mut out = vec![0u64; done.len().div_ceil(64)];
        for (i, d) in done.iter().enumerate() {
            if *d {
                out[i / 64] |= 1 << (i % 64);
            }
        }
        out
    }

    /// Depth-first search for a sequential order. Returns the witness.
    fn run(
        &mut self,
        model: KvModel,
        done: &mut Vec<bool>,
        order: &mut Vec<OpId>,
    ) -> Option<Vec<OpId>> {
        self.nodes += 1;
        if self.nodes > self.limit {
            return None;
        }
        let completed_done = self
            .ops
            .iter()
            .enumerate()
            .filter(|(i, o)| o.respond.is_some() && done[*i])
            .count();
        if completed_done > self.best_explained {
            self.best_explained = completed_done;
            self.best_stuck = None;
        }
        // Success: every completed operation is placed and every observed
        // watch revision was produced by a placed mutation. A batch that the
        // model never generated (including one attributable only to a
        // pending mutation the witness left unapplied) is not explained.
        if self
            .ops
            .iter()
            .enumerate()
            .all(|(i, o)| done[i] || o.respond.is_none())
            && self.max_watched <= model.revision()
        {
            return Some(order.clone());
        }
        let key = (Self::bits(done), model.fingerprint());
        if !self.memo.insert(key) {
            return None;
        }
        // Minimal response tick among not-yet-linearized completed ops: any
        // candidate must have been invoked before that response.
        let horizon = self
            .ops
            .iter()
            .enumerate()
            .filter(|(i, _)| !done[*i])
            .filter_map(|(_, o)| o.respond)
            .min()
            .unwrap_or(u64::MAX);
        for i in 0..self.ops.len() {
            if done[i] {
                continue;
            }
            let op = &self.ops[i];
            if op.invoke > horizon {
                continue;
            }
            let mut next = model.clone();
            let produced = next.apply(&op.op, op.retry);
            if let Some(expected) = &op.response
                && *expected != produced
            {
                if completed_done >= self.best_explained {
                    self.best_stuck = Some(op.id);
                }
                continue;
            }
            // A mutation that advanced the revision must agree with any
            // observed watch batch for that revision, event for event and in
            // order, and the batch cannot have been delivered before the
            // mutation was even invoked: a watch event observed earlier than
            // its mutation's invocation is a speculative delivery.
            if produced.revision != model.revision()
                && let Some(observed) = self.watches.get(&produced.revision)
            {
                let modeled = next.events_at(produced.revision).unwrap_or(&[]);
                if modeled != observed.events.as_slice() || op.invoke > observed.tick {
                    continue;
                }
            }
            done[i] = true;
            order.push(op.id);
            if let Some(w) = self.run(next, done, order) {
                return Some(w);
            }
            order.pop();
            done[i] = false;
        }
        None
    }
}

/// Check a complete-domain history. Returns the verdict; latency is
/// reported separately by [`latency`].
pub fn check_history(history: &History) -> Verdict {
    check_history_bounded(history, 5_000_000)
}

/// [`check_history`] with an explicit search-node budget.
pub fn check_history_bounded(history: &History, node_limit: u64) -> Verdict {
    let mut prepared = prepare(history);
    check_revisions(&prepared.ops, &mut prepared.violations);
    let mut search = Search {
        ops: &prepared.ops,
        watches: &prepared.watches,
        max_watched: prepared.watches.keys().next_back().copied().unwrap_or(0),
        memo: HashSet::new(),
        nodes: 0,
        best_explained: 0,
        best_stuck: None,
        limit: node_limit,
    };
    let mut done = vec![false; prepared.ops.len()];
    let mut order = Vec::new();
    let witness = search.run(KvModel::default(), &mut done, &mut order);
    let completed = prepared.ops.iter().filter(|o| o.respond.is_some()).count();
    let mut violations = prepared.violations;
    let witness = match witness {
        Some(w) => w,
        None => {
            violations.push(Violation::NotLinearizable {
                explained: search.best_explained,
                completed,
                stuck_at: search.best_stuck,
            });
            Vec::new()
        }
    };
    Verdict {
        violations,
        search_nodes: search.nodes,
        witness,
    }
}

/// Latency statistics of a history (independent of correctness).
pub fn latency(history: &History) -> LatencyReport {
    let prepared = prepare(history);
    let samples: Vec<u64> = prepared
        .ops
        .iter()
        .filter_map(|o| o.respond.map(|r| r.saturating_sub(o.invoke)))
        .collect();
    let pending = prepared.ops.iter().filter(|o| o.respond.is_none()).count();
    LatencyReport::from_samples(samples, pending)
}
