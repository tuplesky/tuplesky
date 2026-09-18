//! Differential property test: random operation sequences planned over
//! fixture views must produce the same responses, revisions and events as
//! the independent oracle model.

use std::collections::BTreeMap;

use coord_core::effect::ApplyBase;
use coord_oracle::model::{KvModel, Outcome as OracleOutcome};
use coord_state::planner::apply_to_map;
use coord_state::{HistoricalView, KvEntry, Outcome, PlanLimits, ReadView, plan};
use coord_types::ids::*;
use coord_types::logical_v1::*;
use proptest::prelude::*;

const NS: NamespaceId = NamespaceId([1; 16]);

fn key() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        Just(b"a".to_vec()),
        Just(b"a\0".to_vec()),
        Just(b"ab".to_vec()),
        Just(b"b".to_vec()),
        Just(b"c".to_vec())
    ]
}

fn value() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..4)
}

fn range() -> impl Strategy<Value = KeyRange> {
    prop_oneof![
        key().prop_map(KeyRange::exact),
        Just(KeyRange::interval(b"a".to_vec(), b"c".to_vec())),
        Just(KeyRange::interval(b"a".to_vec(), b"b".to_vec())),
        Just(KeyRange::interval(b"b".to_vec(), b"d".to_vec())),
    ]
}

fn branch_op() -> impl Strategy<Value = BranchOp> {
    prop_oneof![
        (key(), value(), any::<bool>()).prop_map(|(k, v, p)| BranchOp::Put(PutOp {
            key: k,
            value: v,
            lease: None,
            prev_kv: p
        })),
        (range(), any::<bool>()).prop_map(|(r, p)| BranchOp::DeleteRange(DeleteRangeOp {
            range: r,
            prev_kv: p
        })),
        range().prop_map(|r| BranchOp::Range(RangeOp {
            range: r,
            revision: None,
            limit: 0,
            keys_only: false,
            count_only: false
        })),
    ]
}

fn compare() -> impl Strategy<Value = Compare> {
    (
        key(),
        0u64..4,
        prop_oneof![
            Just(CompareResult::Equal),
            Just(CompareResult::Greater),
            Just(CompareResult::Less),
            Just(CompareResult::NotEqual)
        ],
    )
        .prop_map(|(k, n, r)| Compare {
            key: k,
            target: CompareTarget::ModRevision,
            result: r,
            operand: CompareOperand::Counter(n),
        })
}

fn op() -> impl Strategy<Value = CanonicalOperation> {
    prop_oneof![
        (key(), value(), any::<bool>()).prop_map(|(k, v, p)| CanonicalOperation::Put(PutOp {
            key: k,
            value: v,
            lease: None,
            prev_kv: p
        })),
        (range(), any::<bool>()).prop_map(|(r, p)| CanonicalOperation::DeleteRange(
            DeleteRangeOp {
                range: r,
                prev_kv: p
            }
        )),
        (range(), 0u32..3, any::<bool>(), any::<bool>()).prop_map(|(r, l, k, c)| {
            CanonicalOperation::Range(RangeOp {
                range: r,
                revision: None,
                limit: l,
                keys_only: k,
                count_only: c,
            })
        }),
        (range(), 1u64..6).prop_map(|(r, rev)| CanonicalOperation::Range(RangeOp {
            range: r,
            revision: Some(KvRevision::new(rev).unwrap()),
            limit: 0,
            keys_only: false,
            count_only: false
        })),
        (
            prop::collection::vec(compare(), 0..2),
            prop::collection::vec(branch_op(), 0..3),
            prop::collection::vec(branch_op(), 0..2)
        )
            .prop_map(|(c, s, f)| CanonicalOperation::Txn(TxnOp {
                compares: c,
                success: s,
                failure: f
            })),
        (1u64..5).prop_map(|r| CanonicalOperation::Compact {
            revision: KvRevision::new(r).unwrap()
        }),
    ]
}

/// Fixture store: current entries plus full history so historical views
/// can be built the way common storage would.
#[derive(Default)]
struct FixtureStore {
    current: BTreeMap<Vec<u8>, KvEntry>,
    /// (revision, key) -> entry or tombstone.
    history: BTreeMap<(u64, Vec<u8>), Option<KvEntry>>,
    revision: u64,
    position: u64,
    floor: u64,
}

impl FixtureStore {
    /// Compaction keeps the newest version (or tombstone) at or below the
    /// floor for every key plus everything newer (design Section 17.5).
    fn compact(&mut self, floor: u64) {
        let mut newest: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
        for (r, k) in self.history.keys() {
            if *r <= floor {
                let e = newest.entry(k.clone()).or_insert(*r);
                *e = (*e).max(*r);
            }
        }
        self.history
            .retain(|(r, k), _| *r > floor || newest.get(k) == Some(r));
        self.floor = floor;
    }

    fn historical(&self, rev: u64) -> HistoricalView {
        let mut entries: BTreeMap<Vec<u8>, KvEntry> = BTreeMap::new();
        // Greatest version at or below rev per key.
        let mut keys: Vec<Vec<u8>> = self.history.keys().map(|(_, k)| k.clone()).collect();
        keys.sort();
        keys.dedup();
        for k in keys {
            let latest = self
                .history
                .range(..=(rev, k.clone()))
                .rev()
                .find(|((_, kk), _)| *kk == k)
                .and_then(|(_, e)| e.clone());
            if let Some(e) = latest {
                entries.insert(k, e);
            }
        }
        HistoricalView {
            revision: KvRevision::new(rev).unwrap(),
            entries,
        }
    }

    fn view(&self, op: &CanonicalOperation) -> ReadView {
        let base = ApplyBase {
            configuration: ConfigurationEpoch::ZERO,
            execution_position: ExecutionPosition::new(self.position).unwrap(),
        };
        let mut v = ReadView::empty(
            base,
            NS,
            PrincipalId([9; 16]),
            KvRevision::new(self.revision).unwrap(),
        );
        v.current = self.current.clone();
        v.compact_floor = KvRevision::new(self.floor).unwrap();
        for r in coord_state::historical_revisions(op) {
            if r.get() <= self.revision && r.get() >= self.floor {
                v.historical.push(self.historical(r.get()));
            }
        }
        v
    }
}

fn convert(outcome: &Outcome) -> OracleOutcome {
    let entry = |e: &KvEntry| coord_oracle::model::KvEntry {
        value: e.value.clone(),
        create_revision: e.create_revision.get(),
        mod_revision: e.mod_revision.get(),
        version: e.version,
        lease: e.lease.map(|l| l.0),
    };
    let items = |items: &[coord_state::RangeItem]| {
        items
            .iter()
            .map(|i| coord_oracle::model::RangeItem {
                key: i.key.clone(),
                entry: entry(&i.entry),
            })
            .collect()
    };
    match outcome {
        Outcome::Put { prev } => OracleOutcome::Put {
            prev: prev.as_ref().map(entry),
        },
        Outcome::Delete { deleted, prev } => OracleOutcome::Delete {
            deleted: *deleted,
            prev: items(prev),
        },
        Outcome::Range {
            items: i,
            count,
            more,
        } => OracleOutcome::Range {
            items: items(i),
            count: *count,
            more: *more,
        },
        Outcome::Txn { succeeded, results } => OracleOutcome::Txn {
            succeeded: *succeeded,
            results: results.iter().map(convert).collect(),
        },
        Outcome::Compacted => OracleOutcome::Compacted,
        Outcome::ErrCompacted => OracleOutcome::ErrCompacted,
        Outcome::ErrFutureRevision => OracleOutcome::ErrFutureRevision,
        other => panic!("the oracle model has no lease outcomes: {other:?}"),
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 400, ..ProptestConfig::default() })]

    #[test]
    fn planner_matches_oracle(ops in prop::collection::vec(op(), 1..12)) {
        let mut store = FixtureStore::default();
        let mut oracle = KvModel::default();
        for mut op in ops {
            op.canonicalize();
            let request = LogicalRequest::new(NS, op.clone());
            if request.validate().is_err() {
                continue;
            }
            let view = store.view(&op);
            let planned = plan(&request, &view, &PlanLimits::default()).unwrap();
            let expected = oracle.apply(&op, None);
            prop_assert_eq!(planned.response.revision.get(), expected.revision, "header revision");
            prop_assert_eq!(convert(&planned.response.outcome), expected.outcome, "outcome for {:?}", op);
            // Events: same set as the oracle's for that revision.
            if let Some(r) = planned.revision {
                let modeled: Vec<(coord_oracle::WatchEventKind, Vec<u8>, Vec<u8>)> = oracle.events_at(r.get()).unwrap().iter().map(|e| (e.kind, e.key.clone(), e.value.clone())).collect();
                let planned_events: Vec<(coord_oracle::WatchEventKind, Vec<u8>, Vec<u8>)> = planned.events.iter().map(|e| (
                    match e.kind { coord_state::KvEventKind::Put => coord_oracle::WatchEventKind::Put, coord_state::KvEventKind::Delete => coord_oracle::WatchEventKind::Delete },
                    e.key.clone(),
                    e.entry.as_ref().map(|x| x.value.clone()).unwrap_or_default(),
                )).collect();
                prop_assert_eq!(planned_events, modeled, "events");
                // Record history for later historical views.
                for e in &planned.events {
                    store.history.insert((r.get(), e.key.clone()), e.entry.clone());
                }
                store.revision = r.get();
            } else {
                prop_assert!(planned.events.is_empty());
            }
            for m in &planned.mutations {
                if let coord_state::Mutation::CompactTo { revision } = m {
                    store.compact(revision.get());
                }
            }
            apply_to_map(&mut store.current, &planned);
            store.position += 1;
        }
    }
}
