//! Ordered discrete-event queue keyed by `(virtual_tick, insertion_sequence)`.

use std::collections::BTreeMap;

/// Explicit ordering key of a scheduled event.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventKey {
    /// Virtual time.
    pub tick: u64,
    /// Insertion sequence, breaking ties in insertion order.
    pub sequence: u64,
}

/// A discrete-event queue.
#[derive(Debug)]
pub struct Schedule<T> {
    queue: BTreeMap<EventKey, T>,
    next_sequence: u64,
    now: u64,
}

impl<T> Default for Schedule<T> {
    fn default() -> Self {
        Schedule {
            queue: BTreeMap::new(),
            next_sequence: 0,
            now: 0,
        }
    }
}

impl<T> Schedule<T> {
    /// Current virtual time.
    pub const fn now(&self) -> u64 {
        self.now
    }

    /// Schedule `item` at `tick` (clamped to now); returns its key.
    pub fn insert_at(&mut self, tick: u64, item: T) -> EventKey {
        let key = EventKey {
            tick: tick.max(self.now),
            sequence: self.next_sequence,
        };
        self.next_sequence += 1;
        self.queue.insert(key, item);
        key
    }

    /// Schedule `item` after `delay` ticks.
    pub fn insert_after(&mut self, delay: u64, item: T) -> EventKey {
        self.insert_at(self.now.saturating_add(delay), item)
    }

    /// Remove a scheduled item by key.
    pub fn remove(&mut self, key: &EventKey) -> Option<T> {
        self.queue.remove(key)
    }

    /// Pop the next item, advancing virtual time to its tick.
    pub fn pop(&mut self) -> Option<(EventKey, T)> {
        let (key, item) = self.queue.pop_first()?;
        self.now = key.tick;
        Some((key, item))
    }

    /// Remove every item matching the predicate (e.g. a crashed node's timers).
    pub fn retain(&mut self, mut keep: impl FnMut(&EventKey, &T) -> bool) {
        self.queue.retain(|k, v| keep(k, v));
    }

    /// Number of scheduled items.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Whether nothing is scheduled.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ties_are_broken_by_insertion_order() {
        let mut s = Schedule::default();
        s.insert_at(5, "second-inserted-at-5");
        s.insert_at(5, "third-inserted-at-5");
        s.insert_at(3, "first");
        let order: Vec<&str> = std::iter::from_fn(|| s.pop().map(|(_, v)| v)).collect();
        assert_eq!(
            order,
            vec!["first", "second-inserted-at-5", "third-inserted-at-5"]
        );
    }

    #[test]
    fn time_never_moves_backwards() {
        let mut s = Schedule::default();
        s.insert_at(10, 'a');
        s.pop();
        assert_eq!(s.now(), 10);
        let key = s.insert_at(2, 'b');
        assert_eq!(key.tick, 10, "past ticks are clamped to now");
        let removed = s.remove(&key);
        assert_eq!(removed, Some('b'));
        assert!(s.is_empty());
    }
}
