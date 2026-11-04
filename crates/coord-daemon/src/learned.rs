//! From learned to released, per command on the leader (task-d62).
//!
//! A command the leader has committed waits three times before its
//! result goes out: for its predecessors to execute, which is also the
//! execution loop reaching it; for its execution group to close; and for
//! the group to be durable in the journal, which releases it since
//! task-d63. The projection commits the group on the materializer's
//! thread (task-d52), off the release path; this split timed that commit
//! before task-d63 and was named for it (task-d83). Each command is stamped at the
//! four instants and the three waits are summed over the commands that
//! reached the last, so a reader divides by [`ReleaseSplit::commands`]
//! for the means. It bounds what executing on the serving path
//! (task-d63) could buy before that is built.
//!
//! A command executed without being stamped learned -- one this replica
//! did not commit from its own votes while leading -- is not timed.
//!
//! Before learned, a submission's arrival at the leader's loop is stamped
//! too, and the round from there to learned goes into its own
//! distribution (task-d97, #191 review): the consensus round's tail,
//! beside the release's.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use coord_types::CommandId;

use crate::metrics::ReleaseDistributions;

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
    /// From the group closing to the result released: the group's journal
    /// sync, the release rule's wait since task-d63.
    pub journal: Duration,
    /// How each of the three waits, and the whole from learned to
    /// released, was distributed over the commands (task-d97): the sums
    /// give the means, these the tail.
    pub distributions: ReleaseDistributions,
}

#[derive(Clone, Copy, Debug)]
struct Stamps {
    learned: Instant,
    applied: Option<Instant>,
    closed: Option<Instant>,
}

/// A submission stamped and not learned for this long is let go when the
/// stamps are full: a retry of a command already decided, or one another
/// leader decided, is never learned here.
const SUBMITTED_FOR: Duration = Duration::from_secs(10);

/// The stamps of the commands in flight and the waits of those released.
#[derive(Debug, Default)]
pub struct ReleaseTiming {
    stamps: BTreeMap<CommandId, Stamps>,
    submitted: BTreeMap<CommandId, Instant>,
    split: ReleaseSplit,
}

impl ReleaseTiming {
    /// `command` was submitted to this leader at `now`. The first stamp
    /// stands: a retry through another collector is the same command,
    /// still in the same round.
    pub fn submitted(&mut self, command: CommandId, now: Instant) {
        if self.submitted.len() >= STAMPS {
            self.submitted
                .retain(|_, at| now.saturating_duration_since(*at) < SUBMITTED_FOR);
            if self.submitted.len() >= STAMPS {
                return;
            }
        }
        self.submitted.entry(command).or_insert(now);
    }

    /// `commands` were committed at `now`.
    pub fn learned(&mut self, commands: impl IntoIterator<Item = CommandId>, now: Instant) {
        for command in commands {
            if let Some(submitted) = self.submitted.remove(&command) {
                self.split
                    .distributions
                    .round
                    .record(now.saturating_duration_since(submitted));
            }
            if self.stamps.len() >= STAMPS {
                continue;
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
        let predecessors = applied.saturating_duration_since(stamps.learned);
        let group = closed.saturating_duration_since(applied);
        let journal = now.saturating_duration_since(closed);
        self.split.commands += 1;
        self.split.predecessors += predecessors;
        self.split.group += group;
        self.split.journal += journal;
        let d = &mut self.split.distributions;
        d.predecessors.record(predecessors);
        d.group.record(group);
        d.journal.record(journal);
        d.whole
            .record(now.saturating_duration_since(stamps.learned));
    }

    /// Drop the stamps of commands in flight: the role that would have
    /// released them is gone.
    pub fn clear(&mut self) {
        self.stamps.clear();
        self.submitted.clear();
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
        let split = timing.split();
        assert_eq!(
            (
                split.commands,
                split.predecessors,
                split.group,
                split.journal
            ),
            (2, ms(3 + 4), ms(2), ms(7 + 2))
        );
        // Each command is counted once in each distribution, at its own
        // wait: the longest whole is the first command's 12 ms.
        let d = split.distributions;
        for one in [d.predecessors, d.group, d.journal, d.whole] {
            assert_eq!(one.count(), 2);
        }
        assert_eq!(d.predecessors.max, ms(4));
        assert_eq!(d.journal.max, ms(7));
        assert_eq!(d.whole.max, ms(12));
    }

    /// The round from a submission's arrival to learned is its own
    /// distribution (#191 review): a command learned without a
    /// submission stamped -- one this leader recovered, say -- is not in
    /// it, and a second submission of the same command keeps the first
    /// stamp.
    #[test]
    fn the_round_runs_from_the_first_submission_to_learned() {
        let t0 = Instant::now();
        let ms = Duration::from_millis;
        let mut timing = ReleaseTiming::default();
        timing.submitted(command(1), t0);
        timing.submitted(command(1), t0 + ms(2));
        timing.submitted(command(2), t0 + ms(1));
        timing.learned([command(1), command(3)], t0 + ms(5));
        timing.learned([command(2)], t0 + ms(4));
        let round = timing.split().distributions.round;
        assert_eq!(round.count(), 2);
        assert_eq!(round.max, ms(5));
        // Learned again, nothing more: the stamp went with the first.
        timing.learned([command(1)], t0 + ms(9));
        assert_eq!(timing.split().distributions.round.count(), 2);
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
