//! Loom model of the registration/publish handoff boundary (task-13).
//! Run with `cargo xtask loom` (`RUSTFLAGS=--cfg loom`).
#![cfg(loom)]

use coord_state::{KvEvent, KvEventKind};
use coord_storage::watch::{WatchHub, WatchItem, WatchSpec};
use coord_types::ids::{KvRevision, NamespaceId};
use loom::sync::{Arc, Mutex};
use loom::thread;

const NS: NamespaceId = NamespaceId([0x11; 16]);

fn rev(n: u64) -> KvRevision {
    KvRevision::new(n).unwrap()
}

fn event(key: &[u8]) -> KvEvent {
    KvEvent {
        kind: KvEventKind::Put,
        key: key.to_vec(),
        entry: None,
        prev: None,
    }
}

/// A publisher applies revision 2 (store first, then hub) while a watcher
/// registers from revision 1 and replays from the store. Under every
/// interleaving the watcher observes revisions 1 and 2 exactly once, in
/// order, and progress never overtakes them.
#[test]
fn registration_never_loses_or_duplicates_a_concurrent_revision() {
    loom::model(|| {
        // Durable store of events: revision 1 already applied.
        let store = Arc::new(Mutex::new(vec![(rev(1), vec![event(b"k")])]));
        let hub = WatchHub::new(rev(1), KvRevision::ZERO);

        let publisher = {
            let store = store.clone();
            let hub = hub.clone();
            thread::spawn(move || {
                store.lock().unwrap().push((rev(2), vec![event(b"k")]));
                hub.publish(NS, rev(2), &[event(b"k")]).unwrap();
            })
        };

        let watcher = {
            let store = store.clone();
            let hub = hub.clone();
            thread::spawn(move || {
                let spec = WatchSpec {
                    namespace: NS,
                    key: b"k".to_vec(),
                    range_end: None,
                    start_revision: Some(rev(1)),
                    prev_kv: false,
                    progress_notify: true,
                    queue_capacity: 8,
                };
                let registration = hub.register(spec).unwrap();
                let (from, through) = registration.replay.unwrap();
                let rows = store.lock().unwrap().clone();
                let mut r = from;
                loop {
                    let events = rows
                        .iter()
                        .find(|(x, _)| *x == r)
                        .map(|(_, e)| e.clone())
                        .unwrap_or_default();
                    hub.replay(registration.id, NS, r, &events).unwrap();
                    if r >= through {
                        break;
                    }
                    r = r.checked_next().unwrap();
                }
                hub.replay_complete(registration.id).unwrap();
                registration.id
            })
        };

        publisher.join().unwrap();
        let id = watcher.join().unwrap();
        let mut seen = Vec::new();
        let mut progress = None;
        while let Some(item) = hub.next(id, |_| true) {
            match item {
                WatchItem::Batch(b) => {
                    assert!(
                        progress.is_none(),
                        "progress must not precede a pending batch"
                    );
                    seen.push(b.revision.get());
                }
                WatchItem::Progress(r) => progress = Some(r),
                WatchItem::Closed { .. } => panic!("unexpected close"),
            }
        }
        assert_eq!(seen, vec![1, 2]);
        // Progress, when reported at all, never exceeds the delivered history.
        assert!(progress.is_none_or(|r| r == rev(2)));
    });
}
