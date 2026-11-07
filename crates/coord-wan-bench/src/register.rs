//! A register history and the check that it is linearizable (task-d50).
//!
//! Each key is a register. Every write puts a value no other write
//! puts, and the domain stamps it with a revision no other write has, so
//! the order the domain chose for a key's writes is known: the revision
//! order. A read names the write it saw by the modification revision it
//! returned (zero for a key never written), and its value says which
//! write that was. What is left to check is that this order, and the
//! place each read takes in it, agree with real time. For a register
//! whose writes are totally ordered this way, a history is linearizable
//! exactly when, for every key:
//!
//! 1. a read saw a value some write put, at that write's revision;
//! 2. a read did not see a write invoked only after the read completed;
//! 3. a write that completed before another was invoked has the lower
//!    revision;
//! 4. a read invoked after a write completed saw that write or a later
//!    one (no stale read);
//! 5. a write invoked after a read completed is later than what the read
//!    saw;
//! 6. a read invoked after another read completed saw the same write or a
//!    later one.
//!
//! Given these, the linearization is the writes in revision order with
//! each read placed after the write it saw, the reads of one write in an
//! order real time allows (interval orders have one).
//!
//! An operation whose outcome the caller never learned may or may not
//! have happened. A write of that kind takes the revision a read saw it
//! at, when one did, and is otherwise left out; a read of that kind is
//! left out. Only operations the caller saw complete constrain what came
//! after them.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// One operation of a register history.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Op {
    /// The caller that issued it.
    pub caller: u32,
    /// The frontend that caller reached.
    pub frontend: u32,
    /// The register.
    pub key: u32,
    /// Nanoseconds from the run's start when it was invoked.
    pub invoked_ns: u64,
    /// Nanoseconds from the run's start when its outcome came back (or
    /// the caller gave up on it).
    pub completed_ns: u64,
    /// What it was and how it ended.
    pub kind: OpKind,
}

/// What an operation was and how it ended.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpKind {
    /// A write of a value no other write puts.
    Write {
        /// The value.
        value: u64,
        /// The revision it was established at; `None` when the caller
        /// never learned whether it happened.
        revision: Option<u64>,
    },
    /// A read that completed.
    Read {
        /// The value it saw; `None` for a key never written.
        value: Option<u64>,
        /// The modification revision it saw (zero for none).
        revision: u64,
    },
}

/// A way a history is not linearizable, naming the operations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Violation {
    /// The register.
    pub key: u32,
    /// Which rule it breaks (the module's numbering).
    pub rule: u8,
    /// What was seen.
    pub detail: String,
}

struct Write<'a> {
    op: &'a Op,
    revision: Option<u64>,
    done: bool,
}

/// Every violation in `history`, per key in key order.
pub fn check(history: &[Op]) -> Vec<Violation> {
    let mut keys: BTreeMap<u32, Vec<&Op>> = BTreeMap::new();
    for op in history {
        keys.entry(op.key).or_default().push(op);
    }
    let mut out = Vec::new();
    for (key, ops) in keys {
        check_key(key, &ops, &mut out);
    }
    out
}

fn check_key(key: u32, ops: &[&Op], out: &mut Vec<Violation>) {
    let mut violation = |rule: u8, detail: String| {
        out.push(Violation { key, rule, detail });
    };
    let mut writes: BTreeMap<u64, Write<'_>> = BTreeMap::new();
    let mut reads: Vec<(&Op, Option<u64>, u64)> = Vec::new();
    for op in ops {
        match op.kind {
            OpKind::Write { value, revision } => {
                writes.insert(
                    value,
                    Write {
                        op,
                        revision,
                        done: revision.is_some(),
                    },
                );
            }
            OpKind::Read { value, revision } => reads.push((op, value, revision)),
        }
    }

    // 1 and 2: what each read saw was written, at that revision, by a
    // write invoked before the read completed. A write whose outcome was
    // never learned takes the revision a read saw it at.
    for (read, value, revision) in &reads {
        let Some(value) = value else {
            if *revision != 0 {
                violation(1, format!("a read saw no value at revision {revision}"));
            }
            continue;
        };
        let Some(write) = writes.get_mut(value) else {
            violation(
                1,
                format!(
                    "a read (caller {}) saw value {value:#x}, which no write put",
                    read.caller
                ),
            );
            continue;
        };
        match write.revision {
            Some(at) if at != *revision => violation(
                1,
                format!(
                    "a read saw value {value:#x} at revision {revision}; it was written at {at}"
                ),
            ),
            Some(_) => {}
            None => write.revision = Some(*revision),
        }
        if write.op.invoked_ns > read.completed_ns {
            violation(
                2,
                format!(
                    "a read (caller {}, completed at {} ns) saw a write invoked at {} ns",
                    read.caller, read.completed_ns, write.op.invoked_ns
                ),
            );
        }
    }

    let placed: Vec<&Write<'_>> = writes.values().filter(|w| w.revision.is_some()).collect();
    let rev = |w: &Write<'_>| w.revision.expect("placed");

    // 3: walking writes from the highest revision down, the earliest
    // completion among the higher ones must not precede this one's
    // invocation.
    let mut by_revision: Vec<&&Write<'_>> = placed.iter().collect();
    by_revision.sort_by_key(|w| std::cmp::Reverse(rev(w)));
    let mut earliest: Option<&Write<'_>> = None;
    for w in by_revision {
        if let Some(higher) = earliest
            && higher.op.completed_ns < w.op.invoked_ns
        {
            violation(
                3,
                format!(
                    "a write at revision {} completed at {} ns, before a write at revision {} was invoked at {} ns",
                    rev(higher),
                    higher.op.completed_ns,
                    rev(w),
                    w.op.invoked_ns
                ),
            );
        }
        if w.done && earliest.is_none_or(|e| w.op.completed_ns < e.op.completed_ns) {
            earliest = Some(w);
        }
    }

    // 4: the latest revision among writes completed before a read was
    // invoked is at most what the read saw.
    let mut completed: Vec<(u64, u64)> = placed
        .iter()
        .filter(|w| w.done)
        .map(|w| (w.op.completed_ns, rev(w)))
        .collect();
    completed.sort_unstable();
    let mut latest = Vec::with_capacity(completed.len());
    let mut most = 0;
    for (_, revision) in &completed {
        most = most.max(*revision);
        latest.push(most);
    }
    for (read, _, revision) in &reads {
        let before = completed.partition_point(|(at, _)| *at < read.invoked_ns);
        if before > 0 && latest[before - 1] > *revision {
            violation(
                4,
                format!(
                    "a read (caller {}, frontend {}, invoked at {} ns) saw revision {revision}; revision {} had completed before it",
                    read.caller,
                    read.frontend,
                    read.invoked_ns,
                    latest[before - 1]
                ),
            );
        }
    }

    // 5: the earliest revision among writes invoked after a read
    // completed is above what the read saw.
    let mut invoked: Vec<(u64, u64)> = placed.iter().map(|w| (w.op.invoked_ns, rev(w))).collect();
    invoked.sort_unstable();
    let mut lowest = vec![u64::MAX; invoked.len() + 1];
    for i in (0..invoked.len()).rev() {
        lowest[i] = lowest[i + 1].min(invoked[i].1);
    }
    for (read, _, revision) in &reads {
        let after = invoked.partition_point(|(at, _)| *at <= read.completed_ns);
        if lowest[after] <= *revision {
            violation(
                5,
                format!(
                    "a write at revision {} was invoked after a read (caller {}, completed at {} ns) that saw revision {revision}",
                    lowest[after], read.caller, read.completed_ns
                ),
            );
        }
    }

    // 6: what reads completed before a read was invoked saw is at most
    // what it saw.
    let mut seen: Vec<(u64, u64)> = reads
        .iter()
        .map(|(read, _, revision)| (read.completed_ns, *revision))
        .collect();
    seen.sort_unstable();
    let mut highest = Vec::with_capacity(seen.len());
    let mut most = 0;
    for (_, revision) in &seen {
        most = most.max(*revision);
        highest.push(most);
    }
    for (read, _, revision) in &reads {
        let before = seen.partition_point(|(at, _)| *at < read.invoked_ns);
        if before > 0 && highest[before - 1] > *revision {
            violation(
                6,
                format!(
                    "a read (caller {}, frontend {}, invoked at {} ns) saw revision {revision}; an earlier read had seen {}",
                    read.caller,
                    read.frontend,
                    read.invoked_ns,
                    highest[before - 1]
                ),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(caller: u32, at: (u64, u64), value: u64, revision: Option<u64>) -> Op {
        Op {
            caller,
            frontend: 0,
            key: 1,
            invoked_ns: at.0,
            completed_ns: at.1,
            kind: OpKind::Write { value, revision },
        }
    }

    fn read(caller: u32, at: (u64, u64), seen: Option<(u64, u64)>) -> Op {
        Op {
            caller,
            frontend: 0,
            key: 1,
            invoked_ns: at.0,
            completed_ns: at.1,
            kind: OpKind::Read {
                value: seen.map(|(v, _)| v),
                revision: seen.map_or(0, |(_, r)| r),
            },
        }
    }

    fn rules(history: &[Op]) -> Vec<u8> {
        check(history).into_iter().map(|v| v.rule).collect()
    }

    #[test]
    fn a_sequential_history_is_linearizable() {
        let history = [
            read(1, (0, 1), None),
            write(1, (2, 3), 10, Some(5)),
            read(2, (4, 5), Some((10, 5))),
            write(2, (6, 7), 11, Some(8)),
            read(1, (8, 9), Some((11, 8))),
        ];
        assert_eq!(rules(&history), Vec::<u8>::new());
    }

    #[test]
    fn a_read_concurrent_with_a_write_may_see_either_side() {
        let history = [
            write(1, (0, 1), 10, Some(5)),
            write(1, (2, 10), 11, Some(8)),
            read(2, (3, 4), Some((10, 5))),
            read(3, (5, 6), Some((11, 8))),
        ];
        assert_eq!(rules(&history), Vec::<u8>::new());
    }

    #[test]
    fn a_stale_read_is_found() {
        let history = [
            write(1, (0, 1), 10, Some(5)),
            write(1, (2, 3), 11, Some(8)),
            read(2, (4, 5), Some((10, 5))),
        ];
        assert_eq!(rules(&history), vec![4]);
    }

    #[test]
    fn a_read_of_the_initial_state_after_a_write_completed_is_stale() {
        let history = [write(1, (0, 1), 10, Some(5)), read(2, (2, 3), None)];
        assert_eq!(rules(&history), vec![4]);
    }

    #[test]
    fn a_read_from_the_future_is_found() {
        let history = [
            read(2, (0, 1), Some((10, 5))),
            write(1, (2, 3), 10, Some(5)),
        ];
        assert!(rules(&history).contains(&2));
    }

    #[test]
    fn writes_out_of_real_time_order_are_found() {
        let history = [write(1, (0, 1), 10, Some(8)), write(1, (2, 3), 11, Some(5))];
        assert_eq!(rules(&history), vec![3]);
    }

    #[test]
    fn reads_that_go_back_in_time_are_found() {
        let history = [
            write(1, (0, 1), 10, Some(5)),
            write(1, (2, 20), 11, Some(8)),
            read(2, (3, 4), Some((11, 8))),
            read(3, (5, 6), Some((10, 5))),
        ];
        assert_eq!(rules(&history), vec![6]);
    }

    #[test]
    fn a_write_after_a_read_must_be_later_than_what_it_saw() {
        // The read saw revision 8; a write invoked after it completed
        // was placed at 6, between the two the read straddles.
        let history = [
            write(1, (0, 1), 10, Some(5)),
            write(2, (0, 20), 11, Some(8)),
            read(2, (2, 3), Some((11, 8))),
            write(3, (4, 30), 12, Some(6)),
        ];
        assert!(rules(&history).contains(&5));
    }

    #[test]
    fn a_value_no_write_put_is_found() {
        let history = [read(2, (0, 1), Some((99, 5)))];
        assert_eq!(rules(&history), vec![1]);
    }

    #[test]
    fn a_read_at_another_revision_than_its_write_is_found() {
        let history = [
            write(1, (0, 1), 10, Some(5)),
            read(2, (2, 3), Some((10, 6))),
        ];
        assert_eq!(rules(&history), vec![1]);
    }

    #[test]
    fn a_write_whose_outcome_was_never_learned_may_have_happened_or_not() {
        let seen = [
            write(1, (0, u64::MAX), 10, None),
            read(2, (5, 6), Some((10, 5))),
            read(3, (7, 8), Some((10, 5))),
        ];
        assert_eq!(rules(&seen), Vec::<u8>::new());
        let unseen = [write(1, (0, u64::MAX), 10, None), read(2, (5, 6), None)];
        assert_eq!(rules(&unseen), Vec::<u8>::new());
        // Seen once, it is placed: a later read may not go back before it.
        let back = [
            write(1, (0, u64::MAX), 10, None),
            read(2, (5, 6), Some((10, 5))),
            read(3, (7, 8), None),
        ];
        assert_eq!(rules(&back), vec![6]);
    }

    #[test]
    fn registers_are_checked_separately() {
        let mut other = write(1, (0, 1), 10, Some(9));
        other.key = 2;
        let history = [other, read(2, (2, 3), None)];
        assert_eq!(rules(&history), Vec::<u8>::new());
    }
}
