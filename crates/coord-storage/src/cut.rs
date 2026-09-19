//! The authoritative durable recovery cut (design Sections 4.8 and 17.10).
//!
//! A recovery summary must describe authoritative protocol state at a
//! defined durable cut, not at whatever the projection happens to show.
//! Under the journal-first profile the projection lags the journal by
//! construction: a vote can be durable at local sequence 41 while
//! materialization has completed through 40. Reading the projection alone
//! would omit that voting obligation, which Section 4.8 names as a
//! prohibited composition.
//!
//! [`RecoveryCut`] closes that window without waiting for materialization.
//! It layers the complete immutable updates of the durable-but-unmaterialized
//! records, in journal order, over the gated snapshot the projection proves,
//! and implements the ordinary `OrderedRead` contract so the existing
//! readers ([`crate::protocol::read_protocol`] and friends) run over it
//! unchanged. No application logic is duplicated: the overlay replays the
//! same `StoreUpdate` rows the materializer would write.
//!
//! The cut is a read of durable facts, never an authorization: a row that
//! is only journaled is evidence a recovery summary must include, not a
//! learned or established outcome, and the overlay is never written back.

use std::collections::{BTreeMap, VecDeque};

use coord_core::effect::{CollectionId, StoreUpdate};
use coord_store_api::engine::{
    Direction, EngineError, ErrorClass, OrderedRead, Row, RowPage, ScanRequest,
};
use coord_types::ids::LocalJournalSeq;

use crate::view::GatedView;

/// The durable-but-unmaterialized rows of one stream, in journal order
/// (a later record of the same key wins, exactly as materialization would
/// leave it). `None` is a tombstone.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CutOverlay {
    rows: BTreeMap<(CollectionId, Vec<u8>), Option<Vec<u8>>>,
}

impl CutOverlay {
    /// Empty overlay: the cut is exactly the materialized snapshot.
    pub fn new() -> Self {
        CutOverlay::default()
    }

    /// Apply one record's updates in order.
    pub fn extend(&mut self, updates: &[StoreUpdate]) {
        for update in updates {
            self.rows.insert(
                (update.collection, update.key.clone()),
                update.value.clone(),
            );
        }
    }

    /// Rows held (diagnostic).
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether the overlay adds nothing to the snapshot.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// The overlay's entries of one collection inside the request's
    /// interval and past its cursor, in scan order.
    fn in_scan_order(
        &self,
        collection: CollectionId,
        request: &ScanRequest,
    ) -> VecDeque<(Vec<u8>, Option<Vec<u8>>)> {
        let mut selected: Vec<(Vec<u8>, Option<Vec<u8>>)> = self
            .rows
            .range((collection, Vec::new())..)
            .take_while(|((c, _), _)| *c == collection)
            .filter(|((_, key), _)| request.contains(key) && request.past_cursor(key))
            .map(|((_, key), value)| (key.clone(), value.clone()))
            .collect();
        if request.direction == Direction::Reverse {
            selected.reverse();
        }
        selected.into()
    }
}

/// A gated snapshot plus the journal suffix that is durable but not yet
/// materialized: the authoritative cut a recovery summary is built from.
pub struct RecoveryCut<V> {
    snapshot: GatedView<V>,
    overlay: CutOverlay,
    durable: LocalJournalSeq,
}

impl<V: OrderedRead> RecoveryCut<V> {
    /// Bind a snapshot to the overlay of the records durable through
    /// `durable` but not represented by the snapshot's stamp.
    pub const fn new(
        snapshot: GatedView<V>,
        overlay: CutOverlay,
        durable: LocalJournalSeq,
    ) -> Self {
        RecoveryCut {
            snapshot,
            overlay,
            durable,
        }
    }

    /// The journal sequence the cut is defined at (`J`).
    pub const fn durable(&self) -> LocalJournalSeq {
        self.durable
    }

    /// The materialized sequence the underlying snapshot proves (`M`).
    pub fn materialized(&self) -> LocalJournalSeq {
        self.snapshot.meta().stamp.journal_seq()
    }

    /// The overlay the cut adds over the snapshot.
    pub const fn overlay(&self) -> &CutOverlay {
        &self.overlay
    }
}

/// Which side of the merge the next key comes from.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Overlay,
    Base,
    /// The same key is in both; the overlay wins.
    Both,
}

impl<V: OrderedRead> OrderedRead for RecoveryCut<V> {
    fn get(&self, collection: CollectionId, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        match self.overlay.rows.get(&(collection, key.to_vec())) {
            Some(value) => Ok(value.clone()),
            None => self.snapshot.view().get(collection, key),
        }
    }

    fn scan_page(
        &self,
        collection: CollectionId,
        request: &ScanRequest,
    ) -> Result<RowPage, EngineError> {
        let mut overlay = self.overlay.in_scan_order(collection, request);
        let mut base: VecDeque<Row> = VecDeque::new();
        let mut base_cursor = request.resume_after.clone();
        let mut base_exhausted = false;
        let mut rows: Vec<Row> = Vec::new();
        let mut bytes = 0usize;
        loop {
            if base.is_empty() && !base_exhausted {
                let page = self.snapshot.view().scan_page(
                    collection,
                    &ScanRequest {
                        resume_after: base_cursor.clone(),
                        ..request.clone()
                    },
                )?;
                base_exhausted = page.exhausted;
                base.extend(page.rows);
                if base.is_empty() && !base_exhausted {
                    // The adapter contract forbids an empty page that is
                    // not exhausted; treating it as end of data would lose
                    // rows, so it fails closed instead of looping.
                    return Err(EngineError::new(
                        ErrorClass::Corrupt,
                        "snapshot returned an empty page that is not exhausted",
                    ));
                }
                continue;
            }
            let side = match (overlay.front(), base.front()) {
                (None, None) => break,
                (Some(_), None) => Side::Overlay,
                (None, Some(_)) => Side::Base,
                (Some((key, _)), Some(row)) => match (request.direction, key.cmp(&row.key)) {
                    (_, std::cmp::Ordering::Equal) => Side::Both,
                    (Direction::Forward, std::cmp::Ordering::Less)
                    | (Direction::Reverse, std::cmp::Ordering::Greater) => Side::Overlay,
                    _ => Side::Base,
                },
            };
            // A tombstone in the overlay removes the key from the cut, so
            // it is consumed without producing a row and without counting
            // against the page budget.
            if side != Side::Base
                && let Some((_, None)) = overlay.front()
            {
                let (key, _) = overlay.pop_front().expect("front exists");
                if side == Side::Both {
                    base.pop_front();
                    base_cursor = Some(key);
                }
                continue;
            }
            let (key, value) = match side {
                Side::Base => {
                    let row = base.front().expect("front exists");
                    (row.key.clone(), row.value.clone())
                }
                Side::Overlay | Side::Both => {
                    let (key, value) = overlay.front().expect("front exists");
                    (key.clone(), value.clone().expect("tombstone handled above"))
                }
            };
            let cost = key.len() + value.len();
            if rows.is_empty() && cost > request.max_bytes.get() as usize {
                return Err(EngineError::new(
                    ErrorClass::Limit,
                    "row larger than the page byte budget",
                ));
            }
            if rows.len() as u32 >= request.max_rows.get()
                || (!rows.is_empty() && bytes + cost > request.max_bytes.get() as usize)
            {
                return Ok(RowPage {
                    rows,
                    exhausted: false,
                });
            }
            match side {
                Side::Base => {
                    base.pop_front();
                    base_cursor = Some(key.clone());
                }
                Side::Overlay => {
                    overlay.pop_front();
                }
                Side::Both => {
                    overlay.pop_front();
                    base.pop_front();
                    base_cursor = Some(key.clone());
                }
            }
            bytes += cost;
            rows.push(Row { key, value });
        }
        Ok(RowPage {
            rows,
            exhausted: true,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Bound;

    use coord_store_api::engine::Row;

    use crate::lowering::DurableMeta;

    /// A minimal honest ordered view: the merge must work against the
    /// adapter contract, including bounded pages and exclusive cursors.
    struct Base(BTreeMap<(CollectionId, Vec<u8>), Vec<u8>>);

    impl OrderedRead for Base {
        fn get(
            &self,
            collection: CollectionId,
            key: &[u8],
        ) -> Result<Option<Vec<u8>>, EngineError> {
            Ok(self.0.get(&(collection, key.to_vec())).cloned())
        }

        fn scan_page(
            &self,
            collection: CollectionId,
            request: &ScanRequest,
        ) -> Result<RowPage, EngineError> {
            let mut keys: Vec<&Vec<u8>> = self
                .0
                .keys()
                .filter(|(c, k)| *c == collection && request.contains(k) && request.past_cursor(k))
                .map(|(_, k)| k)
                .collect();
            if request.direction == Direction::Reverse {
                keys.reverse();
            }
            let mut rows = Vec::new();
            let mut bytes = 0usize;
            for key in keys {
                let value = self.0[&(collection, key.clone())].clone();
                let cost = key.len() + value.len();
                if rows.len() as u32 >= request.max_rows.get()
                    || (!rows.is_empty() && bytes + cost > request.max_bytes.get() as usize)
                {
                    return Ok(RowPage {
                        rows,
                        exhausted: false,
                    });
                }
                bytes += cost;
                rows.push(Row {
                    key: key.clone(),
                    value,
                });
            }
            Ok(RowPage {
                rows,
                exhausted: true,
            })
        }
    }

    const KV: CollectionId = CollectionId(0x0007);

    fn base(rows: &[(&[u8], &[u8])]) -> Base {
        Base(
            rows.iter()
                .map(|(k, v)| ((KV, k.to_vec()), v.to_vec()))
                .collect(),
        )
    }

    fn update(key: &[u8], value: Option<&[u8]>) -> StoreUpdate {
        StoreUpdate {
            collection: KV,
            key: key.to_vec(),
            value: value.map(<[u8]>::to_vec),
        }
    }

    fn cut(rows: &[(&[u8], &[u8])], updates: &[StoreUpdate]) -> RecoveryCut<Base> {
        let mut overlay = CutOverlay::new();
        overlay.extend(updates);
        RecoveryCut::new(
            crate::view::GatedView::new(base(rows), DurableMeta::initial()),
            overlay,
            LocalJournalSeq::ZERO,
        )
    }

    fn scan(cut: &RecoveryCut<Base>, request: &ScanRequest) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut out = Vec::new();
        let mut resume = request.resume_after.clone();
        loop {
            let page = cut
                .scan_page(
                    KV,
                    &ScanRequest {
                        resume_after: resume.clone(),
                        ..request.clone()
                    },
                )
                .expect("page");
            out.extend(page.rows.iter().map(|r| (r.key.clone(), r.value.clone())));
            match page.rows.last() {
                Some(last) if !page.exhausted => resume = Some(last.key.clone()),
                _ => break,
            }
        }
        out
    }

    #[test]
    fn the_cut_shows_journaled_rows_and_hides_journaled_tombstones() {
        let cut = cut(
            &[(b"a", b"1"), (b"b", b"2"), (b"d", b"4")],
            &[
                update(b"b", Some(b"2b")),
                update(b"c", Some(b"3")),
                update(b"d", None),
            ],
        );
        assert_eq!(cut.get(KV, b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(cut.get(KV, b"b").unwrap(), Some(b"2b".to_vec()));
        assert_eq!(cut.get(KV, b"c").unwrap(), Some(b"3".to_vec()));
        assert_eq!(cut.get(KV, b"d").unwrap(), None);
        assert_eq!(
            scan(&cut, &ScanRequest::all(16, 4096)),
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2b".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
            ]
        );
        assert_eq!(cut.overlay().len(), 3);
    }

    #[test]
    fn the_merge_survives_paging_bounds_cursors_and_reverse_order() {
        let rows: Vec<(Vec<u8>, Vec<u8>)> = (0u8..8).map(|i| (vec![b'k', i], vec![i, i])).collect();
        let base_rows: Vec<(&[u8], &[u8])> = rows
            .iter()
            .filter(|(k, _)| k[1] % 2 == 0)
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        let updates: Vec<StoreUpdate> = rows
            .iter()
            .filter(|(k, _)| k[1] % 2 == 1)
            .map(|(k, v)| update(k, Some(v)))
            .collect();
        let cut = cut(&base_rows, &updates);
        // One row per page still yields the whole interval in order.
        let all = scan(&cut, &ScanRequest::all(1, 4096));
        assert_eq!(all, rows);
        // A byte budget that admits one row at a time behaves the same.
        assert_eq!(scan(&cut, &ScanRequest::all(16, 4)), rows);
        // Bounds and the exclusive cursor apply to both sides.
        let bounded = ScanRequest {
            lower: Bound::Included(vec![b'k', 2]),
            upper: Bound::Excluded(vec![b'k', 6]),
            resume_after: Some(vec![b'k', 2]),
            ..ScanRequest::all(16, 4096)
        };
        assert_eq!(
            scan(&cut, &bounded)
                .iter()
                .map(|(k, _)| k[1])
                .collect::<Vec<u8>>(),
            vec![3, 4, 5]
        );
        // Reverse scans resume below the prior key.
        let reverse = ScanRequest {
            direction: Direction::Reverse,
            resume_after: Some(vec![b'k', 5]),
            ..ScanRequest::all(2, 4096)
        };
        assert_eq!(
            scan(&cut, &reverse)
                .iter()
                .map(|(k, _)| k[1])
                .collect::<Vec<u8>>(),
            vec![4, 3, 2, 1, 0]
        );
    }

    #[test]
    fn a_row_larger_than_the_page_budget_is_a_limit_error_not_an_endless_empty_page() {
        let cut = cut(&[], &[update(b"big", Some(&[0u8; 64]))]);
        let request = ScanRequest::all(16, 8);
        let err = cut.scan_page(KV, &request).expect_err("oversized row");
        assert_eq!(err.class, coord_store_api::engine::ErrorClass::Limit);
    }
}
