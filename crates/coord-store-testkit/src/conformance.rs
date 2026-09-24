//! Black-box conformance checks for any `coord-store-api` adapter.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::ops::Bound;

use coord_core::effect::CollectionId;
use coord_store_api::engine::{
    CommitFailure, Direction, ErrorClass, LocalEngine, OrderedRead, ScanRequest, SnapshotSource,
    WriteTxn,
};
use coord_store_api::registry::Collection;

/// Outcome the harness should force on the next commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScriptedOutcome {
    /// Definite noncommit.
    DefinitelyNotCommitted,
    /// Indeterminate, with the batch applied durably.
    IndeterminateApplied,
    /// Indeterminate, with the batch absent.
    IndeterminateAbsent,
}

/// What an adapter's test harness must provide beyond the engine itself.
pub trait ConformanceHarness {
    /// The engine under test.
    type Engine: LocalEngine;

    /// The engine.
    fn engine(&mut self) -> &mut Self::Engine;

    /// Force the next commit's outcome; `false` when the harness cannot
    /// (the related checks are then reported as skipped, never as passed).
    fn script_next_commit(&mut self, outcome: ScriptedOutcome) -> bool;

    /// Inject an iterator failure after `rows` rows of the next scan;
    /// `false` when unsupported.
    fn inject_iterator_error(&mut self, rows: usize) -> bool;

    /// Crash the process model and reopen the durable image.
    fn crash_and_reopen(&mut self);
}

/// Result of one named check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckResult {
    /// Passed.
    Passed,
    /// Failed with an explanation.
    Failed(String),
    /// Skipped because the harness lacks a fault hook.
    Skipped(String),
}

/// Report of a suite run.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConformanceReport {
    /// Results by check name, in execution order.
    pub results: Vec<(&'static str, CheckResult)>,
}

impl ConformanceReport {
    /// Whether every check passed (skips count as not passed).
    pub fn all_passed(&self) -> bool {
        self.results.iter().all(|(_, r)| *r == CheckResult::Passed)
    }

    /// Names of failed checks.
    pub fn failed(&self) -> Vec<&'static str> {
        self.results
            .iter()
            .filter(|(_, r)| matches!(r, CheckResult::Failed(_)))
            .map(|(n, _)| *n)
            .collect()
    }

    /// Names of skipped checks.
    pub fn skipped(&self) -> Vec<&'static str> {
        self.results
            .iter()
            .filter(|(_, r)| matches!(r, CheckResult::Skipped(_)))
            .map(|(n, _)| *n)
            .collect()
    }
}

type Check = Result<(), String>;
type RowSpec<'a> = (CollectionId, &'a [u8], Option<&'a [u8]>);
type OwnedRowSpec = (CollectionId, Vec<u8>, Option<Vec<u8>>);
type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

const KV: CollectionId = Collection::KvCurrentV1.id();
const HIST: CollectionId = Collection::KvHistoryV1.id();

fn req(
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
    direction: Direction,
    max_rows: u32,
    max_bytes: u32,
) -> ScanRequest {
    ScanRequest {
        lower: lower.map(<[u8]>::to_vec),
        upper: upper.map(<[u8]>::to_vec),
        direction,
        resume_after: None,
        max_rows: NonZeroU32::new(max_rows).unwrap(),
        max_bytes: NonZeroU32::new(max_bytes).unwrap(),
    }
}

fn commit<E: LocalEngine>(engine: &mut E, rows: &[RowSpec<'_>]) -> Result<(), String> {
    let mut tx = engine
        .begin_write()
        .map_err(|e| format!("begin_write: {e}"))?;
    for (c, k, v) in rows {
        match v {
            Some(v) => tx.put(*c, k, v).map_err(|e| format!("put: {e}"))?,
            None => tx.delete(*c, k).map_err(|e| format!("delete: {e}"))?,
        }
    }
    tx.commit_durable().map_err(|e| format!("commit: {e}"))
}

fn all_rows<V: OrderedRead>(view: &V, c: CollectionId) -> Result<KeyValues, String> {
    let mut out = Vec::new();
    let mut request = req(
        Bound::Unbounded,
        Bound::Unbounded,
        Direction::Forward,
        3,
        1 << 20,
    );
    let mut pages = 0;
    loop {
        let page = view
            .scan_page(c, &request)
            .map_err(|e| format!("scan: {e}"))?;
        pages += 1;
        if pages > 10_000 {
            return Err("no progress: too many pages".to_owned());
        }
        if page.rows.is_empty() && !page.exhausted {
            return Err("empty non-exhausted page".to_owned());
        }
        for r in &page.rows {
            out.push((r.key.clone(), r.value.clone()));
        }
        if page.exhausted {
            return Ok(out);
        }
        request.resume_after = Some(page.rows.last().unwrap().key.clone());
    }
}

fn check_ordered_access<H: ConformanceHarness>(h: &mut H) -> Check {
    let keys: Vec<Vec<u8>> = vec![
        vec![],
        vec![0],
        vec![0, 0],
        vec![0, 0xff],
        vec![0xff],
        b"a".to_vec(),
        b"a\0".to_vec(),
        b"ab".to_vec(),
        b"b".to_vec(),
    ];
    let rows: Vec<RowSpec<'_>> = keys
        .iter()
        .map(|k| (KV, k.as_slice(), Some(k.as_slice())))
        .collect();
    commit(h.engine(), &rows)?;
    commit(h.engine(), &[(HIST, b"a", Some(b"other-collection"))])?;
    let view = h.engine().reader().snapshot().map_err(|e| e.to_string())?;
    // Point reads including the empty key and absence.
    for k in &keys {
        if view.get(KV, k).map_err(|e| e.to_string())? != Some(k.clone()) {
            return Err(format!("point read of {k:?} wrong"));
        }
    }
    if view.get(KV, b"zz").map_err(|e| e.to_string())?.is_some() {
        return Err("absent key returned a value".to_owned());
    }
    // Full scan in unsigned order; other collections never leak.
    let scanned = all_rows(&view, KV)?;
    let mut sorted = keys.clone();
    sorted.sort();
    if scanned.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>() != sorted {
        return Err(format!("scan order/contents wrong: {scanned:?}"));
    }
    // Prefix and bounds.
    let page = view
        .scan_page(
            KV,
            &req(
                Bound::Included(b"a"),
                Bound::Excluded(b"b"),
                Direction::Forward,
                100,
                1 << 20,
            ),
        )
        .map_err(|e| e.to_string())?;
    let got: Vec<&[u8]> = page.rows.iter().map(|r| r.key.as_slice()).collect();
    if got != vec![b"a".as_slice(), b"a\0", b"ab"] || !page.exhausted {
        return Err(format!("bounded scan wrong: {got:?}"));
    }
    // Reverse with cursor.
    let mut rev = req(
        Bound::Unbounded,
        Bound::Included(b"ab"),
        Direction::Reverse,
        2,
        1 << 20,
    );
    let page = view.scan_page(KV, &rev).map_err(|e| e.to_string())?;
    let got: Vec<&[u8]> = page.rows.iter().map(|r| r.key.as_slice()).collect();
    if got != vec![b"ab".as_slice(), b"a\0"] || page.exhausted {
        return Err(format!("reverse page wrong: {got:?}"));
    }
    rev.resume_after = Some(b"a\0".to_vec());
    let page = view.scan_page(KV, &rev).map_err(|e| e.to_string())?;
    let got: Vec<&[u8]> = page.rows.iter().map(|r| r.key.as_slice()).collect();
    // Unsigned order: `0xff` sorts after `b`, so below `a\0` come `a` then `\0\xff`.
    if got != vec![b"a".as_slice(), b"\0\xff"] {
        return Err(format!("reverse resume wrong: {got:?}"));
    }
    // Tombstone: delete then read.
    commit(h.engine(), &[(KV, b"ab", None)])?;
    let view = h.engine().reader().snapshot().map_err(|e| e.to_string())?;
    if view.get(KV, b"ab").map_err(|e| e.to_string())?.is_some() {
        return Err("deleted key still readable".to_owned());
    }
    Ok(())
}

fn check_bounded_progress<H: ConformanceHarness>(h: &mut H) -> Check {
    let big = vec![7u8; 500];
    commit(
        h.engine(),
        &[
            (KV, b"p1", Some(b"x")),
            (KV, b"p2", Some(&big)),
            (KV, b"p3", Some(b"y")),
        ],
    )?;
    let view = h.engine().reader().snapshot().map_err(|e| e.to_string())?;
    // Byte budget below the big row: the first page returns p1 only, then
    // the next page must be a typed Limit error, not an empty page.
    let mut r = req(
        Bound::Included(b"p"),
        Bound::Excluded(b"q"),
        Direction::Forward,
        10,
        100,
    );
    let page = view.scan_page(KV, &r).map_err(|e| e.to_string())?;
    if page.rows.len() != 1 || page.exhausted {
        return Err(format!(
            "byte budget page wrong: {} rows, exhausted={}",
            page.rows.len(),
            page.exhausted
        ));
    }
    r.resume_after = Some(b"p1".to_vec());
    match view.scan_page(KV, &r) {
        Err(e) if e.class == ErrorClass::Limit => {}
        Err(e) => return Err(format!("oversized row: wrong error class {:?}", e.class)),
        Ok(p) => {
            return Err(format!(
                "oversized row returned a page: {} rows exhausted={}",
                p.rows.len(),
                p.exhausted
            ));
        }
    }
    // Row budget: pages of one row make progress and terminate.
    let mut r = req(
        Bound::Included(b"p"),
        Bound::Excluded(b"q"),
        Direction::Forward,
        1,
        1 << 20,
    );
    let mut seen = Vec::new();
    for _ in 0..10 {
        let page = view.scan_page(KV, &r).map_err(|e| e.to_string())?;
        seen.extend(page.rows.iter().map(|x| x.key.clone()));
        if page.exhausted {
            break;
        }
        r.resume_after = Some(
            page.rows
                .last()
                .ok_or("empty non-exhausted page")?
                .key
                .clone(),
        );
    }
    if seen != vec![b"p1".to_vec(), b"p2".to_vec(), b"p3".to_vec()] {
        return Err(format!("row-budget pagination wrong: {seen:?}"));
    }
    Ok(())
}

fn check_iterator_errors<H: ConformanceHarness>(h: &mut H) -> Result<CheckResult, String> {
    commit(
        h.engine(),
        &[
            (KV, b"i1", Some(b"1")),
            (KV, b"i2", Some(b"2")),
            (KV, b"i3", Some(b"3")),
        ],
    )?;
    if !h.inject_iterator_error(1) {
        return Ok(CheckResult::Skipped(
            "harness cannot inject iterator errors".to_owned(),
        ));
    }
    let view = h.engine().reader().snapshot().map_err(|e| e.to_string())?;
    match view.scan_page(
        KV,
        &req(
            Bound::Included(b"i"),
            Bound::Excluded(b"j"),
            Direction::Forward,
            10,
            1 << 20,
        ),
    ) {
        Err(_) => Ok(CheckResult::Passed),
        Ok(page) => Ok(CheckResult::Failed(format!(
            "iterator error swallowed: {} rows exhausted={}",
            page.rows.len(),
            page.exhausted
        ))),
    }
}

fn check_transactions<H: ConformanceHarness>(h: &mut H) -> Check {
    commit(h.engine(), &[(KV, b"t1", Some(b"old"))])?;
    let reader = h.engine().reader();
    let before = reader.snapshot().map_err(|e| e.to_string())?;
    {
        let engine = h.engine();
        let mut tx = engine.begin_write().map_err(|e| e.to_string())?;
        tx.put(KV, b"t1", b"new").map_err(|e| e.to_string())?;
        tx.put(HIST, b"t1@2", b"new").map_err(|e| e.to_string())?;
        tx.delete(KV, b"t-missing").map_err(|e| e.to_string())?;
        // Read-your-writes: point and scan.
        if tx.get(KV, b"t1").map_err(|e| e.to_string())? != Some(b"new".to_vec()) {
            return Err("point read-your-writes failed".to_owned());
        }
        let page = tx
            .scan_page(
                HIST,
                &req(
                    Bound::Included(b"t1"),
                    Bound::Excluded(b"t2"),
                    Direction::Forward,
                    10,
                    1 << 20,
                ),
            )
            .map_err(|e| e.to_string())?;
        if page.rows.len() != 1 {
            return Err("scan read-your-writes failed".to_owned());
        }
        // Nothing is visible before completion.
        let during = reader.snapshot().map_err(|e| e.to_string())?;
        if during.get(KV, b"t1").map_err(|e| e.to_string())? != Some(b"old".to_vec())
            || during
                .get(HIST, b"t1@2")
                .map_err(|e| e.to_string())?
                .is_some()
        {
            return Err("pending writes visible before commit".to_owned());
        }
        tx.commit_durable().map_err(|e| e.to_string())?;
    }
    // Pinned snapshot stays pinned for point reads and for scan pages: an
    // adapter whose scans open a fresh transaction per page would show the
    // new history row here even though `before` predates the commit.
    if before.get(KV, b"t1").map_err(|e| e.to_string())? != Some(b"old".to_vec()) {
        return Err("snapshot not pinned".to_owned());
    }
    let pinned_scan = before
        .scan_page(
            HIST,
            &req(
                Bound::Included(b"t1"),
                Bound::Excluded(b"t2"),
                Direction::Forward,
                10,
                1 << 20,
            ),
        )
        .map_err(|e| e.to_string())?;
    if !pinned_scan.rows.is_empty() {
        return Err(format!(
            "scan through a pinned snapshot saw {} row(s) committed after it",
            pinned_scan.rows.len()
        ));
    }
    let pinned_kv = before
        .scan_page(
            KV,
            &req(
                Bound::Included(b"t1"),
                Bound::Excluded(b"t2"),
                Direction::Forward,
                10,
                1 << 20,
            ),
        )
        .map_err(|e| e.to_string())?;
    if pinned_kv.rows.len() != 1 || pinned_kv.rows[0].value != b"old" {
        return Err("scan through a pinned snapshot does not see the pinned value".to_owned());
    }
    let after = reader.snapshot().map_err(|e| e.to_string())?;
    let kv = after.get(KV, b"t1").map_err(|e| e.to_string())?;
    let hist = after.get(HIST, b"t1@2").map_err(|e| e.to_string())?;
    if kv != Some(b"new".to_vec()) || hist != Some(b"new".to_vec()) {
        return Err(format!(
            "cross-collection atomicity violated: kv={kv:?} hist={hist:?}"
        ));
    }
    // Abort and drop discard.
    {
        let mut tx = h.engine().begin_write().map_err(|e| e.to_string())?;
        tx.put(KV, b"t1", b"aborted").map_err(|e| e.to_string())?;
        tx.abort().map_err(|e| e.to_string())?;
    }
    {
        let mut tx = h.engine().begin_write().map_err(|e| e.to_string())?;
        tx.put(KV, b"t1", b"dropped").map_err(|e| e.to_string())?;
        drop(tx);
    }
    let view = reader.snapshot().map_err(|e| e.to_string())?;
    if view.get(KV, b"t1").map_err(|e| e.to_string())? != Some(b"new".to_vec()) {
        return Err("abort or drop leaked writes".to_owned());
    }
    // Torn-write detection: a multi-row commit is all or nothing.
    commit(
        h.engine(),
        &[
            (KV, b"m1", Some(b"1")),
            (KV, b"m2", Some(b"2")),
            (KV, b"m3", Some(b"3")),
            (HIST, b"m1", Some(b"1")),
        ],
    )?;
    let view = reader.snapshot().map_err(|e| e.to_string())?;
    for k in [b"m1".as_slice(), b"m2", b"m3"] {
        if view.get(KV, k).map_err(|e| e.to_string())?.is_none() {
            return Err(format!("torn write: {k:?} missing after commit"));
        }
    }
    if view.get(HIST, b"m1").map_err(|e| e.to_string())?.is_none() {
        return Err("torn write: history row missing".to_owned());
    }
    Ok(())
}

fn check_durability<H: ConformanceHarness>(h: &mut H) -> Check {
    commit(
        h.engine(),
        &[
            (KV, b"d1", Some(b"durable")),
            (HIST, b"d1", Some(b"durable")),
        ],
    )?;
    h.crash_and_reopen();
    let view = h.engine().reader().snapshot().map_err(|e| e.to_string())?;
    if view.get(KV, b"d1").map_err(|e| e.to_string())? != Some(b"durable".to_vec())
        || view.get(HIST, b"d1").map_err(|e| e.to_string())?.is_none()
    {
        return Err("false durability: committed batch lost on reopen".to_owned());
    }
    Ok(())
}

fn check_commit_outcomes<H: ConformanceHarness>(h: &mut H) -> Result<CheckResult, String> {
    commit(h.engine(), &[(KV, b"c0", Some(b"base"))])?;
    if !h.script_next_commit(ScriptedOutcome::DefinitelyNotCommitted) {
        return Ok(CheckResult::Skipped(
            "harness cannot script commit outcomes".to_owned(),
        ));
    }
    {
        let mut tx = h.engine().begin_write().map_err(|e| e.to_string())?;
        tx.put(KV, b"c1", b"never").map_err(|e| e.to_string())?;
        match tx.commit_durable() {
            Err(CommitFailure::DefinitelyNotCommitted(_)) => {}
            other => {
                return Ok(CheckResult::Failed(format!(
                    "expected definite noncommit, got {other:?}"
                )));
            }
        }
    }
    // A definite noncommit must be absent from live snapshots at once, not
    // only after volatile state is discarded by a reopen: publishing it and
    // dropping it later would expose data the caller is free to replan.
    let live = h.engine().reader().snapshot().map_err(|e| e.to_string())?;
    if live.get(KV, b"c1").map_err(|e| e.to_string())?.is_some() {
        return Ok(CheckResult::Failed(
            "definitely-not-committed batch is visible before reopen".to_owned(),
        ));
    }
    drop(live);
    h.crash_and_reopen();
    let view = h.engine().reader().snapshot().map_err(|e| e.to_string())?;
    if view.get(KV, b"c1").map_err(|e| e.to_string())?.is_some() {
        return Ok(CheckResult::Failed(
            "definitely-not-committed batch is present".to_owned(),
        ));
    }
    for (outcome, name) in [
        (ScriptedOutcome::IndeterminateApplied, "applied"),
        (ScriptedOutcome::IndeterminateAbsent, "absent"),
    ] {
        if !h.script_next_commit(outcome) {
            return Ok(CheckResult::Skipped(
                "harness cannot script indeterminate commits".to_owned(),
            ));
        }
        let key_a = format!("ind-{name}-a").into_bytes();
        let key_b = format!("ind-{name}-b").into_bytes();
        {
            let mut tx = h.engine().begin_write().map_err(|e| e.to_string())?;
            tx.put(KV, &key_a, b"1").map_err(|e| e.to_string())?;
            tx.put(HIST, &key_b, b"2").map_err(|e| e.to_string())?;
            match tx.commit_durable() {
                Err(CommitFailure::Indeterminate(_)) => {}
                other => {
                    return Ok(CheckResult::Failed(format!(
                        "expected indeterminate, got {other:?}"
                    )));
                }
            }
        }
        h.crash_and_reopen();
        let view = h.engine().reader().snapshot().map_err(|e| e.to_string())?;
        let a = view.get(KV, &key_a).map_err(|e| e.to_string())?.is_some();
        let b = view.get(HIST, &key_b).map_err(|e| e.to_string())?.is_some();
        if a != b {
            return Ok(CheckResult::Failed(format!(
                "indeterminate commit partially present (a={a}, b={b})"
            )));
        }
        if name == "applied" && !a {
            return Ok(CheckResult::Failed(
                "harness reported applied but batch absent".to_owned(),
            ));
        }
        if name == "absent" && a {
            return Ok(CheckResult::Failed(
                "harness reported absent but batch present".to_owned(),
            ));
        }
    }
    // Successful barriers after an indeterminate one retain their batches.
    commit(h.engine(), &[(KV, b"c2", Some(b"after"))])?;
    h.crash_and_reopen();
    let view = h.engine().reader().snapshot().map_err(|e| e.to_string())?;
    if view.get(KV, b"c2").map_err(|e| e.to_string())?.is_none() {
        return Ok(CheckResult::Failed("later durable batch lost".to_owned()));
    }
    Ok(CheckResult::Passed)
}

fn check_reference_semantics<H: ConformanceHarness>(h: &mut H) -> Check {
    // Differential against an independent map over a fixed sequence.
    let mut model: BTreeMap<(u16, Vec<u8>), Vec<u8>> = BTreeMap::new();
    let ops: Vec<OwnedRowSpec> = (0..60u32)
        .map(|i| {
            let c = if i % 3 == 0 { HIST } else { KV };
            let key = format!("r{}", i % 7).into_bytes();
            let value = if i % 5 == 4 {
                None
            } else {
                Some(format!("v{i}").into_bytes())
            };
            (c, key, value)
        })
        .collect();
    for chunk in ops.chunks(4) {
        let rows: Vec<RowSpec<'_>> = chunk
            .iter()
            .map(|(c, k, v)| (*c, k.as_slice(), v.as_deref()))
            .collect();
        commit(h.engine(), &rows)?;
        for (c, k, v) in chunk {
            match v {
                Some(v) => {
                    model.insert((c.0, k.clone()), v.clone());
                }
                None => {
                    model.remove(&(c.0, k.clone()));
                }
            }
        }
    }
    let view = h.engine().reader().snapshot().map_err(|e| e.to_string())?;
    for c in [KV, HIST] {
        // Other checks leave rows behind; compare only this check's prefix.
        let got: KeyValues = all_rows(&view, c)?
            .into_iter()
            .filter(|(k, _)| k.starts_with(b"r"))
            .collect();
        let expected: KeyValues = model
            .iter()
            .filter(|((cc, _), _)| *cc == c.0)
            .map(|((_, k), v)| (k.clone(), v.clone()))
            .collect();
        if got != expected {
            return Err(format!("collection {} differs from reference map", c.0));
        }
    }
    Ok(())
}

fn record(report: &mut ConformanceReport, name: &'static str, result: Check) {
    report.results.push((
        name,
        match result {
            Ok(()) => CheckResult::Passed,
            Err(e) => CheckResult::Failed(e),
        },
    ));
}

/// Run every check against a fresh harness. Checks are independent by key
/// prefix, so one engine instance serves all of them.
pub fn run_all<H: ConformanceHarness>(h: &mut H) -> ConformanceReport {
    let mut report = ConformanceReport::default();
    record(&mut report, "ordered_access", check_ordered_access(h));
    record(&mut report, "bounded_progress", check_bounded_progress(h));
    match check_iterator_errors(h) {
        Ok(r) => report.results.push(("iterator_errors", r)),
        Err(e) => report
            .results
            .push(("iterator_errors", CheckResult::Failed(e))),
    }
    record(&mut report, "transactions", check_transactions(h));
    record(&mut report, "durability", check_durability(h));
    match check_commit_outcomes(h) {
        Ok(r) => report.results.push(("commit_outcomes", r)),
        Err(e) => report
            .results
            .push(("commit_outcomes", CheckResult::Failed(e))),
    }
    record(
        &mut report,
        "reference_semantics",
        check_reference_semantics(h),
    );
    report
}
