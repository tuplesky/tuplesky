//! From learned to released, per command on the leader (task-d62).
//!
//! A command the leader has committed waits three times before its
//! result goes out: for its predecessors to execute, which is also the
//! execution loop reaching it; for its execution group to close; and for
//! the projection to commit the group, which a pipelined store does on
//! the materializer's thread (task-d52). Each command is stamped at the
//! four instants and the three waits are summed over the commands that
//! reached the last, so a reader divides by [`ReleaseSplit::commands`]
//! for the means. It bounds what executing on the serving path
//! (task-d63) could buy before that is built.
//!
//! A command executed without being stamped learned -- one this replica
//! did not commit from its own votes while leading -- is not timed.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use coord_types::CommandId;

/// Commands stamped and not yet released past which no new one is
/// stamped: more than a leader holds in flight, so reaching it means
/// releases are not being counted, and the stamps are not let grow.
const STAMPS: usize = 1 << 14;

/// The three waits from learned to released, summed (task-d62).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReleaseSplit {
    /// Commands timed from learned to released.
    pub commands: u64,
    /// From committed to applied: waiting for predecessors to execute.
    pub predecessors: Duration,
    /// From applied to its group closing.
    pub group: Duration,
    /// From the group closing to the result released: the projection
    /// committing it.
    pub projection: Duration,
}

#[derive(Clone, Copy, Debug)]
struct Stamps {
    learned: Instant,
    applied: Option<Instant>,
    closed: Option<Instant>,
}

/// The stamps of the commands in flight and the waits of those released.
#[derive(Debug, Default)]
pub struct ReleaseTiming {
    stamps: BTreeMap<CommandId, Stamps>,
    split: ReleaseSplit,
}

impl ReleaseTiming {
    /// `commands` were committed at `now`.
    pub fn learned(&mut self, commands: impl IntoIterator<Item = CommandId>, now: Instant) {
        for command in commands {
            if self.stamps.len() >= STAMPS {
                return;
            }
            self.stamps.entry(command).or_insert(Stamps {
                learned: now,
                applied: None,
                closed: None,
            });
        }
    }

    /// `command` was applied at `now`.
    pub fn applied(&mut self, command: &CommandId, now: Instant) {
        if let Some(stamps) = self.stamps.get_mut(command) {
            stamps.applied.get_or_insert(now);
        }
    }

    /// The group holding `command` closed at `now`.
    pub fn closed(&mut self, command: &CommandId, now: Instant) {
        if let Some(stamps) = self.stamps.get_mut(command) {
            stamps.closed.get_or_insert(now);
        }
    }

    /// `command`'s result was released at `now`. A command applied outside
    /// a group closed when it was applied.
    pub fn released(&mut self, command: &CommandId, now: Instant) {
        let Some(stamps) = self.stamps.remove(command) else {
            return;
        };
        let Some(applied) = stamps.applied else {
            return;
        };
        let closed = stamps.closed.unwrap_or(applied);
        self.split.commands += 1;
        self.split.predecessors += applied.saturating_duration_since(stamps.learned);
        self.split.group += closed.saturating_duration_since(applied);
        self.split.projection += now.saturating_duration_since(closed);
    }

    /// Drop the stamps of commands in flight: the role that would have
    /// released them is gone.
    pub fn clear(&mut self) {
        self.stamps.clear();
    }

    /// The waits of the commands released so far.
    pub const fn split(&self) -> ReleaseSplit {
        self.split
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command(n: u8) -> CommandId {
        CommandId(coord_types::identity::Digest32([n; 32]))
    }

    #[test]
    fn each_wait_is_the_interval_between_its_two_stamps() {
        let t0 = Instant::now();
        let ms = Duration::from_millis;
        let mut timing = ReleaseTiming::default();
        timing.learned([command(1), command(2)], t0);
        timing.applied(&command(1), t0 + ms(3));
        timing.closed(&command(1), t0 + ms(5));
        timing.released(&command(1), t0 + ms(12));
        // Applied outside a group: it closed when it was applied.
        timing.applied(&command(2), t0 + ms(4));
        timing.released(&command(2), t0 + ms(6));
        assert_eq!(
            timing.split(),
            ReleaseSplit {
                commands: 2,
                predecessors: ms(3 + 4),
                group: ms(2),
                projection: ms(7 + 2),
            }
        );
    }

    #[test]
    fn a_command_not_stamped_learned_or_cleared_is_not_timed() {
        let t0 = Instant::now();
        let mut timing = ReleaseTiming::default();
        timing.applied(&command(1), t0);
        timing.released(&command(1), t0);
        timing.learned([command(2)], t0);
        timing.applied(&command(2), t0);
        timing.clear();
        timing.released(&command(2), t0);
        assert_eq!(timing.split(), ReleaseSplit::default());
    }
}
