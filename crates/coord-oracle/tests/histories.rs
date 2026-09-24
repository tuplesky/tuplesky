//! Acceptance: reject a stale read after an acknowledged write, a duplicate
//! mutation, a wrongly shared revision and a missing transaction event;
//! accept valid concurrent histories including pending operations.

use coord_oracle::check::{check_history, latency};
use coord_oracle::model::{KvEntry, KvModel, ModelResponse, Outcome, RangeItem};
use coord_oracle::{History, Violation, WatchEvent, WatchEventKind};
use coord_types::ids::KvRevision;
use coord_types::logical_v1::*;

fn put(key: &[u8], value: &[u8]) -> CanonicalOperation {
    CanonicalOperation::Put(PutOp {
        key: key.to_vec(),
        value: value.to_vec(),
        lease: None,
        prev_kv: false,
    })
}

fn get(key: &[u8]) -> CanonicalOperation {
    CanonicalOperation::Range(RangeOp {
        range: KeyRange::exact(key.to_vec()),
        revision: None,
        limit: 0,
        keys_only: false,
        count_only: false,
    })
}

fn get_at(key: &[u8], rev: u64) -> CanonicalOperation {
    CanonicalOperation::Range(RangeOp {
        range: KeyRange::exact(key.to_vec()),
        revision: Some(KvRevision::new(rev).unwrap()),
        limit: 0,
        keys_only: false,
        count_only: false,
    })
}

fn entry(value: &[u8], create: u64, modr: u64, version: u64) -> KvEntry {
    KvEntry {
        value: value.to_vec(),
        create_revision: create,
        mod_revision: modr,
        version,
        lease: None,
    }
}

fn put_resp(revision: u64) -> ModelResponse {
    ModelResponse {
        revision,
        outcome: Outcome::Put { prev: None },
    }
}

fn read_resp(revision: u64, key: &[u8], entry: Option<KvEntry>) -> ModelResponse {
    let items = entry
        .map(|e| {
            vec![RangeItem {
                key: key.to_vec(),
                entry: e,
            }]
        })
        .unwrap_or_default();
    let count = items.len() as u64;
    ModelResponse {
        revision,
        outcome: Outcome::Range {
            items,
            count,
            more: false,
        },
    }
}

fn cas(key: &[u8], expect_mod: u64, value: &[u8]) -> CanonicalOperation {
    let mut op = CanonicalOperation::Txn(TxnOp {
        compares: vec![Compare {
            key: key.to_vec(),
            target: CompareTarget::ModRevision,
            result: CompareResult::Equal,
            operand: CompareOperand::Counter(expect_mod),
        }],
        success: vec![BranchOp::Put(PutOp {
            key: key.to_vec(),
            value: value.to_vec(),
            lease: None,
            prev_kv: false,
        })],
        failure: vec![BranchOp::Range(RangeOp {
            range: KeyRange::exact(key.to_vec()),
            revision: None,
            limit: 0,
            keys_only: false,
            count_only: false,
        })],
    });
    op.canonicalize();
    op
}

#[test]
fn valid_concurrent_history_is_accepted() {
    // Two clients: A puts x=1 (rev 1) while B's read overlaps and may see
    // either state; then B puts y=2 (rev 2) and A reads at revision 1.
    let mut h = History::new();
    h.invoke(1, 1, 0, None, put(b"x", b"1"));
    h.invoke(2, 2, 1, None, get(b"x"));
    h.respond(2, 2, read_resp(0, b"x", None)); // read linearized before the put
    h.respond(1, 3, put_resp(1));
    h.invoke(3, 2, 4, None, put(b"y", b"2"));
    h.invoke(4, 1, 4, None, get(b"x"));
    h.respond(4, 5, read_resp(2, b"x", Some(entry(b"1", 1, 1, 1))));
    h.respond(3, 6, put_resp(2));
    h.invoke(5, 1, 7, None, get_at(b"y", 1));
    h.respond(5, 8, read_resp(2, b"y", None));
    h.invoke(6, 1, 9, None, get_at(b"x", 2));
    h.respond(6, 10, read_resp(2, b"x", Some(entry(b"1", 1, 1, 1))));
    let verdict = check_history(&h);
    assert!(verdict.ok(), "{:?}", verdict.violations);
    assert_eq!(verdict.witness.len(), 6);
    let lat = latency(&h);
    assert_eq!(lat.completed, 6);
    assert_eq!(lat.pending, 0);
    assert_eq!(lat.max, Some(3));
}

#[test]
fn stale_read_after_acknowledged_write_is_rejected() {
    let mut h = History::new();
    h.invoke(1, 1, 0, None, put(b"x", b"1"));
    h.respond(1, 1, put_resp(1));
    // Invoked strictly after the acknowledgement, yet reports the old state.
    h.invoke(2, 2, 2, None, get(b"x"));
    h.respond(2, 3, read_resp(0, b"x", None));
    let verdict = check_history(&h);
    assert!(!verdict.ok());
    assert!(
        verdict
            .violations
            .iter()
            .any(|v| matches!(v, Violation::NotLinearizable { .. })),
        "{:?}",
        verdict.violations
    );
    // The same read overlapping the write is fine.
    let mut ok = History::new();
    ok.invoke(1, 1, 0, None, put(b"x", b"1"));
    ok.invoke(2, 2, 0, None, get(b"x"));
    ok.respond(1, 1, put_resp(1));
    ok.respond(2, 3, read_resp(0, b"x", None));
    assert!(check_history(&ok).ok());
}

#[test]
fn duplicate_mutation_under_one_retry_key_is_rejected() {
    // A retried put (same retry identity) must return the same result; two
    // revisions mean it executed twice.
    let mut h = History::new();
    h.invoke(1, 1, 0, Some(77), put(b"x", b"1"));
    h.respond(1, 1, put_resp(1));
    h.invoke(2, 1, 2, Some(77), put(b"x", b"1"));
    h.respond(2, 3, put_resp(2));
    let verdict = check_history(&h);
    assert!(
        verdict.violations.iter().any(|v| matches!(
            v,
            Violation::RetryInconsistent {
                first: 1,
                second: 2
            }
        )),
        "{:?}",
        verdict.violations
    );
    // The correct retry outcome is accepted and consumes no revision.
    let mut ok = History::new();
    ok.invoke(1, 1, 0, Some(77), put(b"x", b"1"));
    ok.respond(1, 1, put_resp(1));
    ok.invoke(2, 1, 2, Some(77), put(b"x", b"1"));
    ok.respond(2, 3, put_resp(1));
    ok.invoke(3, 2, 4, None, get(b"x"));
    ok.respond(3, 5, read_resp(1, b"x", Some(entry(b"1", 1, 1, 1))));
    let verdict = check_history(&ok);
    assert!(verdict.ok(), "{:?}", verdict.violations);
}

#[test]
fn wrongly_shared_revision_is_rejected() {
    let mut h = History::new();
    h.invoke(1, 1, 0, None, put(b"x", b"1"));
    h.respond(1, 1, put_resp(1));
    h.invoke(2, 2, 2, None, put(b"y", b"2"));
    h.respond(2, 3, put_resp(1));
    let verdict = check_history(&h);
    assert!(
        verdict.violations.iter().any(|v| matches!(
            v,
            Violation::SharedRevision {
                first: 1,
                second: 2,
                revision: 1
            }
        )),
        "{:?}",
        verdict.violations
    );
    assert!(
        verdict
            .violations
            .iter()
            .any(|v| matches!(v, Violation::NotLinearizable { .. }))
    );
}

#[test]
fn missing_transaction_event_is_rejected() {
    let txn = {
        let mut op = CanonicalOperation::Txn(TxnOp {
            compares: vec![],
            success: vec![
                BranchOp::Put(PutOp {
                    key: b"a".to_vec(),
                    value: b"1".to_vec(),
                    lease: None,
                    prev_kv: false,
                }),
                BranchOp::Put(PutOp {
                    key: b"b".to_vec(),
                    value: b"2".to_vec(),
                    lease: None,
                    prev_kv: false,
                }),
            ],
            failure: vec![],
        });
        op.canonicalize();
        op
    };
    let resp = ModelResponse {
        revision: 1,
        outcome: Outcome::Txn {
            succeeded: true,
            results: vec![Outcome::Put { prev: None }, Outcome::Put { prev: None }],
        },
    };
    let both = vec![
        WatchEvent {
            kind: WatchEventKind::Put,
            key: b"a".to_vec(),
            value: b"1".to_vec(),
        },
        WatchEvent {
            kind: WatchEventKind::Put,
            key: b"b".to_vec(),
            value: b"2".to_vec(),
        },
    ];
    let mut ok = History::new();
    ok.invoke(1, 1, 0, None, txn.clone());
    ok.respond(1, 1, resp.clone());
    ok.watch(2, 1, both.clone());
    assert!(check_history(&ok).ok());

    // The watch shows only half of the revision.
    let mut half = History::new();
    half.invoke(1, 1, 0, None, txn.clone());
    half.respond(1, 1, resp.clone());
    half.watch(2, 1, both[..1].to_vec());
    let verdict = check_history(&half);
    assert!(!verdict.ok());

    // Two batches for one revision that disagree.
    let mut conflict = History::new();
    conflict.invoke(1, 1, 0, None, txn);
    conflict.respond(1, 1, resp);
    conflict.watch(2, 1, both.clone());
    conflict.watch(3, 1, both[..1].to_vec());
    assert!(
        check_history(&conflict)
            .violations
            .iter()
            .any(|v| matches!(v, Violation::ConflictingWatchBatches { revision: 1 }))
    );
}

#[test]
fn pending_operations_may_or_may_not_have_taken_effect() {
    // A put without a response, then a read that sees it: accepted.
    let mut seen = History::new();
    seen.invoke(1, 1, 0, None, put(b"x", b"1"));
    seen.invoke(2, 2, 5, None, get(b"x"));
    seen.respond(2, 6, read_resp(1, b"x", Some(entry(b"1", 1, 1, 1))));
    let verdict = check_history(&seen);
    assert!(verdict.ok(), "{:?}", verdict.violations);
    assert_eq!(verdict.witness, vec![1, 2]);
    // ...and a read that does not see it: also accepted.
    let mut unseen = History::new();
    unseen.invoke(1, 1, 0, None, put(b"x", b"1"));
    unseen.invoke(2, 2, 5, None, get(b"x"));
    unseen.respond(2, 6, read_resp(0, b"x", None));
    let verdict = check_history(&unseen);
    assert!(verdict.ok(), "{:?}", verdict.violations);
    assert_eq!(verdict.witness, vec![2]);
    assert_eq!(latency(&unseen).pending, 1);
    // But an acknowledged write cannot vanish later.
    let mut vanished = History::new();
    vanished.invoke(1, 1, 0, None, put(b"x", b"1"));
    vanished.respond(1, 1, put_resp(1));
    vanished.invoke(2, 2, 2, None, get(b"x"));
    vanished.respond(2, 3, read_resp(1, b"x", Some(entry(b"1", 1, 1, 1))));
    vanished.invoke(3, 2, 4, None, get(b"x"));
    vanished.respond(3, 5, read_resp(1, b"x", None));
    assert!(!check_history(&vanished).ok());
}

#[test]
fn conditional_transactions_and_compaction() {
    let mut h = History::new();
    h.invoke(1, 1, 0, None, put(b"k", b"v1"));
    h.respond(1, 1, put_resp(1));
    // CAS expecting mod revision 1 succeeds at revision 2.
    h.invoke(2, 1, 2, None, cas(b"k", 1, b"v2"));
    h.respond(
        2,
        3,
        ModelResponse {
            revision: 2,
            outcome: Outcome::Txn {
                succeeded: true,
                results: vec![Outcome::Put { prev: None }],
            },
        },
    );
    // A stale CAS (still expecting 1) fails and returns the current entry
    // through the failure branch without consuming a revision.
    h.invoke(3, 2, 4, None, cas(b"k", 1, b"v3"));
    h.respond(
        3,
        5,
        ModelResponse {
            revision: 2,
            outcome: Outcome::Txn {
                succeeded: false,
                results: vec![Outcome::Range {
                    items: vec![RangeItem {
                        key: b"k".to_vec(),
                        entry: entry(b"v2", 1, 2, 2),
                    }],
                    count: 1,
                    more: false,
                }],
            },
        },
    );
    // Historical read at revision 1 sees v1.
    h.invoke(4, 2, 6, None, get_at(b"k", 1));
    h.respond(4, 7, read_resp(2, b"k", Some(entry(b"v1", 1, 1, 1))));
    // Compact to 2, then the revision-1 read is Compacted.
    h.invoke(
        5,
        1,
        8,
        None,
        CanonicalOperation::Compact {
            revision: KvRevision::new(2).unwrap(),
        },
    );
    h.respond(
        5,
        9,
        ModelResponse {
            revision: 2,
            outcome: Outcome::Compacted,
        },
    );
    h.invoke(6, 2, 10, None, get_at(b"k", 1));
    h.respond(
        6,
        11,
        ModelResponse {
            revision: 2,
            outcome: Outcome::ErrCompacted,
        },
    );
    h.invoke(7, 2, 12, None, get_at(b"k", 9));
    h.respond(
        7,
        13,
        ModelResponse {
            revision: 2,
            outcome: Outcome::ErrFutureRevision,
        },
    );
    let verdict = check_history(&h);
    assert!(verdict.ok(), "{:?}", verdict.violations);

    // A CAS that claims success against the wrong expectation is rejected.
    let mut bad = History::new();
    bad.invoke(1, 1, 0, None, put(b"k", b"v1"));
    bad.respond(1, 1, put_resp(1));
    bad.invoke(2, 1, 2, None, cas(b"k", 5, b"v2"));
    bad.respond(
        2,
        3,
        ModelResponse {
            revision: 2,
            outcome: Outcome::Txn {
                succeeded: true,
                results: vec![Outcome::Put { prev: None }],
            },
        },
    );
    assert!(!check_history(&bad).ok());
}

#[test]
fn model_semantics_match_design_rules() {
    let mut m = KvModel::default();
    // Same-value put is a mutation; deleting nothing is not.
    assert_eq!(m.apply(&put(b"a", b"1"), None).revision, 1);
    assert_eq!(m.apply(&put(b"a", b"1"), None).revision, 2);
    let del_none = CanonicalOperation::DeleteRange(DeleteRangeOp {
        range: KeyRange::exact(b"zzz".to_vec()),
        prev_kv: false,
    });
    let r = m.apply(&del_none, None);
    assert_eq!(r.revision, 2);
    assert_eq!(
        r.outcome,
        Outcome::Delete {
            deleted: 0,
            prev: vec![]
        }
    );
    // Range delete of two keys uses one revision with two events.
    m.apply(&put(b"b", b"2"), None);
    let del = CanonicalOperation::DeleteRange(DeleteRangeOp {
        range: KeyRange::interval(b"a".to_vec(), b"c".to_vec()),
        prev_kv: true,
    });
    let r = m.apply(&del, None);
    assert_eq!(r.revision, 4);
    assert_eq!(m.events_at(4).map(<[WatchEvent]>::len), Some(2));
    if let Outcome::Delete { deleted, prev } = r.outcome {
        assert_eq!(deleted, 2);
        assert_eq!(prev.len(), 2);
    } else {
        panic!("expected delete outcome");
    }
    // Historical read at revision 3 still sees both keys; version of "a" is 2.
    let hist = CanonicalOperation::Range(RangeOp {
        range: KeyRange::interval(b"a".to_vec(), b"c".to_vec()),
        revision: Some(KvRevision::new(3).unwrap()),
        limit: 0,
        keys_only: true,
        count_only: false,
    });
    if let Outcome::Range { items, count, .. } = m.apply(&hist, None).outcome {
        assert_eq!(count, 2);
        assert_eq!(items[0].entry.version, 2);
        assert!(items[0].entry.value.is_empty(), "keys-only strips values");
    } else {
        panic!("expected range");
    }
    // Lease operations are outside the KV model (extension point).
    assert_eq!(
        m.apply(
            &CanonicalOperation::LeaseKeepAlive {
                lease_id: coord_types::ids::LeaseId([1; 16])
            },
            None
        )
        .outcome,
        Outcome::Unsupported
    );
}

#[test]
fn compaction_keeps_the_newest_version_at_the_floor() {
    // Put a@1, compact to 1, delete a@2: a read at revision 1 must still see
    // the version written at revision 1 (Section 17.5 retention).
    let mut m = KvModel::default();
    m.apply(&put(b"a", b"v"), None);
    m.apply(
        &CanonicalOperation::Compact {
            revision: KvRevision::new(1).unwrap(),
        },
        None,
    );
    m.apply(
        &CanonicalOperation::DeleteRange(DeleteRangeOp {
            range: KeyRange::exact(b"a".to_vec()),
            prev_kv: false,
        }),
        None,
    );
    let r = m.apply(&get_at(b"a", 1), None);
    assert_eq!(r.revision, 2);
    assert_eq!(
        r.outcome,
        Outcome::Range {
            items: vec![RangeItem {
                key: b"a".to_vec(),
                entry: entry(b"v", 1, 1, 1)
            }],
            count: 1,
            more: false
        }
    );
    // Below the floor is still Compacted.
    m.apply(&put(b"b", b"w"), None);
    m.apply(
        &CanonicalOperation::Compact {
            revision: KvRevision::new(3).unwrap(),
        },
        None,
    );
    assert_eq!(
        m.apply(&get_at(b"a", 2), None).outcome,
        Outcome::ErrCompacted
    );
    assert_eq!(
        m.apply(&get_at(b"b", 3), None).outcome.clone(),
        Outcome::Range {
            items: vec![RangeItem {
                key: b"b".to_vec(),
                entry: entry(b"w", 3, 3, 1)
            }],
            count: 1,
            more: false
        }
    );
}

#[test]
fn retry_identity_binds_the_operation_not_only_the_response() {
    // put(x) then put(y) under one retry key: the second is served the
    // cached first response, which the retry contract forbids for a changed
    // payload even though the responses agree.
    let mut h = History::new();
    h.invoke(1, 1, 0, Some(7), put(b"k", b"x"));
    h.respond(1, 1, put_resp(1));
    h.invoke(2, 1, 2, Some(7), put(b"k", b"y"));
    h.respond(2, 3, put_resp(1));
    let verdict = check_history(&h);
    assert!(verdict.violations.iter().any(|v| matches!(
        v,
        Violation::RetryInconsistent {
            first: 1,
            second: 2
        }
    )));
    // The same payload retried is fine.
    let mut ok = History::new();
    ok.invoke(1, 1, 0, Some(7), put(b"k", b"x"));
    ok.respond(1, 1, put_resp(1));
    ok.invoke(2, 1, 2, Some(7), put(b"k", b"x"));
    ok.respond(2, 3, put_resp(1));
    assert!(check_history(&ok).ok());
}

#[test]
fn every_watch_revision_must_be_produced_by_the_witness() {
    let event = |value: &[u8]| WatchEvent {
        kind: WatchEventKind::Put,
        key: b"a".to_vec(),
        value: value.to_vec(),
    };
    // A batch for a revision no operation produced is unexplained.
    let mut orphan = History::new();
    orphan.watch(1, 1, vec![event(b"v")]);
    assert!(!check_history(&orphan).ok());

    // A batch attributable to a pending mutation forces the witness to
    // apply that mutation, and its content must match.
    let mut pending_match = History::new();
    pending_match.invoke(1, 1, 0, None, put(b"a", b"v"));
    pending_match.watch(5, 1, vec![event(b"v")]);
    assert!(check_history(&pending_match).ok());
    let mut pending_mismatch = History::new();
    pending_mismatch.invoke(1, 1, 0, None, put(b"a", b"v"));
    pending_mismatch.watch(5, 1, vec![event(b"other")]);
    assert!(!check_history(&pending_mismatch).ok());
}

#[test]
fn watch_delivery_cannot_precede_the_mutation_invocation() {
    let event = WatchEvent {
        kind: WatchEventKind::Put,
        key: b"a".to_vec(),
        value: b"v".to_vec(),
    };
    // Event delivered at tick 1 for a put invoked at tick 10: speculative.
    let mut early = History::new();
    early.invoke(1, 1, 10, None, put(b"a", b"v"));
    early.respond(1, 12, put_resp(1));
    early.watch(1, 1, vec![event.clone()]);
    assert!(!check_history(&early).ok());
    // Delivered at or after the invocation: fine, even before the response.
    let mut ok = History::new();
    ok.invoke(1, 1, 10, None, put(b"a", b"v"));
    ok.respond(1, 12, put_resp(1));
    ok.watch(10, 1, vec![event]);
    assert!(check_history(&ok).ok());
}

#[test]
fn watch_batch_order_within_a_revision_is_significant() {
    let txn = {
        let mut op = CanonicalOperation::Txn(TxnOp {
            compares: vec![],
            success: vec![
                BranchOp::Put(PutOp {
                    key: b"a".to_vec(),
                    value: b"1".to_vec(),
                    lease: None,
                    prev_kv: false,
                }),
                BranchOp::Put(PutOp {
                    key: b"b".to_vec(),
                    value: b"2".to_vec(),
                    lease: None,
                    prev_kv: false,
                }),
            ],
            failure: vec![],
        });
        op.canonicalize();
        op
    };
    let resp = ModelResponse {
        revision: 1,
        outcome: Outcome::Txn {
            succeeded: true,
            results: vec![Outcome::Put { prev: None }, Outcome::Put { prev: None }],
        },
    };
    let a = WatchEvent {
        kind: WatchEventKind::Put,
        key: b"a".to_vec(),
        value: b"1".to_vec(),
    };
    let b = WatchEvent {
        kind: WatchEventKind::Put,
        key: b"b".to_vec(),
        value: b"2".to_vec(),
    };
    let mut reversed = History::new();
    reversed.invoke(1, 1, 0, None, txn.clone());
    reversed.respond(1, 1, resp.clone());
    reversed.watch(2, 1, vec![b.clone(), a.clone()]);
    assert!(!check_history(&reversed).ok());
    let mut duplicated = History::new();
    duplicated.invoke(1, 1, 0, None, txn.clone());
    duplicated.respond(1, 1, resp.clone());
    duplicated.watch(2, 1, vec![a.clone(), a.clone(), b.clone()]);
    assert!(!check_history(&duplicated).ok());
    // Two deliveries of the identical ordered batch are one batch.
    let mut twice = History::new();
    twice.invoke(1, 1, 0, None, txn);
    twice.respond(1, 1, resp);
    twice.watch(2, 1, vec![a.clone(), b.clone()]);
    twice.watch(3, 1, vec![a, b]);
    assert!(check_history(&twice).ok());
}

#[test]
fn zero_limit_reads_the_schema_maximum_page() {
    let mut m = KvModel::default();
    let n = limits::MAX_PAGE_LIMIT as usize + 1;
    for i in 0..n {
        m.apply(&put(format!("k{i:06}").as_bytes(), b"v"), None);
    }
    let r = m.apply(
        &CanonicalOperation::Range(RangeOp {
            range: KeyRange::interval(b"k".to_vec(), b"l".to_vec()),
            revision: None,
            limit: 0,
            keys_only: true,
            count_only: false,
        }),
        None,
    );
    match r.outcome {
        Outcome::Range { items, count, more } => {
            assert_eq!(items.len(), limits::MAX_PAGE_LIMIT as usize);
            assert_eq!(count, n as u64);
            assert!(more);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn fingerprint_distinguishes_lease_state() {
    let mut with_lease = KvModel::default();
    with_lease.apply(
        &CanonicalOperation::Put(PutOp {
            key: b"a".to_vec(),
            value: b"v".to_vec(),
            lease: Some(coord_types::ids::LeaseId([9; 16])),
            prev_kv: false,
        }),
        None,
    );
    let mut without = KvModel::default();
    without.apply(&put(b"a", b"v"), None);
    assert_ne!(with_lease.fingerprint(), without.fingerprint());
}
