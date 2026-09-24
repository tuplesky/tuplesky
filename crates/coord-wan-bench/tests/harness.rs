//! What the benchmark harness has to be true of before a number it
//! produces means anything.

use coord_types::ids::NamespaceId;
use coord_wan_bench::report::{Absent, Measured, ServerSide};
use coord_wan_bench::workload::{Kind, Mix, Workload};
use rand_core::SeedableRng;

fn workload(mix: Mix) -> Workload {
    Workload {
        namespace: NamespaceId([0x5e; 16]),
        keyspace: 1000,
        hot_keys: 8,
        value_bytes: 64,
        transaction_keys: 3,
        scan_limit: 16,
        mix,
    }
}

/// The same seed offers the same work, so two runs differ in the domain
/// and not in what was asked of it.
#[test]
fn a_seed_reproduces_the_offered_work() {
    let workload = workload(Mix::CONTROL_PLANE);
    let generate = |seed: u64| {
        let mut rng = rand_chacha::ChaCha12Rng::seed_from_u64(seed);
        (0..200)
            .map(|_| workload.next(&mut rng))
            .collect::<Vec<_>>()
    };
    let first = generate(7);
    let again = generate(7);
    let other = generate(8);
    assert_eq!(first, again, "one seed offered two workloads");
    assert_ne!(first, other, "two seeds offered one workload");
}

/// A weight of zero removes a kind, so a matrix row that is meant to be
/// write-only is write-only.
#[test]
fn a_zero_weight_never_appears() {
    let workload = workload(Mix::WRITE_ONLY);
    let mut rng = rand_chacha::ChaCha12Rng::seed_from_u64(1);
    for _ in 0..500 {
        let (kind, _) = workload.next(&mut rng);
        assert!(
            matches!(kind, Kind::Put | Kind::ContendedTransaction),
            "a removed kind was offered: {kind:?}"
        );
    }
}

/// Hot writers contend: every contended transaction lands in the small key
/// set the run asked for, which is what makes it a contention
/// measurement rather than a spread of independent writes.
#[test]
fn conditional_writes_contend_on_the_hot_keys() {
    let mut spec = workload(Mix::WRITE_ONLY);
    spec.hot_keys = 4;
    let mut rng = rand_chacha::ChaCha12Rng::seed_from_u64(3);
    let mut seen = std::collections::BTreeSet::new();
    for _ in 0..400 {
        let (kind, request) = spec.next(&mut rng);
        if kind != Kind::ContendedTransaction {
            continue;
        }
        let coord_types::logical_v1::CanonicalOperation::Txn(txn) = &request.operation else {
            panic!("a conditional write is a transaction");
        };
        seen.insert(txn.compares[0].key.clone());
    }
    assert!(!seen.is_empty(), "no conditional write was offered");
    assert!(
        seen.len() <= 4,
        "conditional writes spread over {} keys, not 4",
        seen.len()
    );
}

/// A scan is a range over the object keys that follow its start, so the
/// row limit is what bounds it. A range that ended just past the start
/// key would return that key and nothing else, and the scan rows of a
/// matrix would be point reads under another name.
#[test]
fn a_scan_ranges_over_the_keys_that_follow_its_start() {
    let mut spec = workload(Mix {
        put: 0,
        get: 0,
        contended: 0,
        transaction: 0,
        scan: 1,
    });
    // One key to start from, so every scan starts at object zero.
    spec.keyspace = 1;
    let mut rng = rand_chacha::ChaCha12Rng::seed_from_u64(5);
    let (kind, request) = spec.next(&mut rng);
    assert_eq!(kind, Kind::Scan);
    let coord_types::logical_v1::CanonicalOperation::Range(range) = &request.operation else {
        panic!("a scan is a range read");
    };
    let end = range.range.range_end.as_deref().expect("a scan is bounded");
    for index in 0..4u32 {
        let key = format!("/registry/bench/objects/{index:08}").into_bytes();
        assert!(
            range.range.key.as_slice() <= key.as_slice() && key.as_slice() < end,
            "object {index} is outside the scan"
        );
    }
    assert!(
        b"/registry/bench/other/00000000".as_slice() >= end,
        "the scan left the object prefix"
    );
    assert_eq!(range.limit, 16, "the row limit is what bounds a scan");
}

/// A mix has to name something.
#[test]
fn an_empty_mix_is_refused() {
    assert!(Mix::parse("put=0,get=0").is_err());
    assert!(Mix::parse("put=1,fly=2").is_err());
    assert!(Mix::parse("put").is_err());
    assert_eq!(Mix::parse("put=1,get=3").expect("parsed").total(), 4);
}

/// The metrics this harness cannot read are absent with a reason, and
/// never zero. A reader has to be able to tell "nothing happened" from
/// "nobody measured", because only one of those permits a comparison.
#[test]
fn unread_metrics_say_why_rather_than_reporting_zero() {
    let server = ServerSide::unreadable();
    assert_eq!(server.sync.why(), Some(Absent::NoEndpoint));
    assert_eq!(server.commit_return.why(), Some(Absent::NoEndpoint));
    assert_eq!(server.queues.why(), Some(Absent::NoEndpoint));
    assert_eq!(server.disk.why(), Some(Absent::NotOnThisHost));
    assert_eq!(server.wan.why(), Some(Absent::NotOnThisHost));
    assert!(server.sync.observed().is_none());

    // And the rendering keeps the distinction: an absence serializes as
    // a reason, not as a number a reader could average.
    let rendered = serde_json::to_string(&server).expect("json");
    assert!(rendered.contains("NoEndpoint"), "{rendered}");
    assert!(!rendered.contains(":0"), "{rendered}");

    let observed: Measured<u64> = Measured::Observed(3);
    assert_eq!(observed.observed(), Some(&3));
    assert_eq!(observed.why(), None);
}

/// The contended kind is reported as what it is. Its compare holds for
/// any key written before and both of its branches write, so it cannot
/// detect a stale read, and a matrix row that called it compare-and-swap
/// would be publishing optimistic-concurrency numbers nobody measured.
/// The older `cas` spelling still parses, to this kind.
#[test]
fn the_contended_transaction_is_not_reported_as_compare_and_swap() {
    assert_eq!(Kind::ContendedTransaction.name(), "contended-transaction");
    let legacy = Mix::parse("cas=3").expect("the older spelling parses");
    assert_eq!(legacy.contended, 3);
    assert_eq!(Mix::parse("contended=3").expect("parses"), legacy);

    let spec = workload(Mix::parse("contended=1").expect("parses"));
    let mut rng = rand_chacha::ChaCha12Rng::seed_from_u64(9);
    let (kind, request) = spec.next(&mut rng);
    assert_eq!(kind, Kind::ContendedTransaction);
    let coord_types::logical_v1::CanonicalOperation::Txn(txn) = &request.operation else {
        panic!("a contended write is a transaction");
    };
    // Both branches write: what the name has to be honest about.
    assert!(!txn.success.is_empty() && !txn.failure.is_empty());
}
