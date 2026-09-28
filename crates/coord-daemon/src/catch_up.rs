//! Bringing a lagging voter up from a peer's executed history (task-d08):
//! the two runtime halves the protocol machine does not hold.
//!
//! The donor half answers a [`coord_consensus::ProtocolMessage::CatchUpRequest`]
//! from its own durable rows. Its store keys `executed_v1` by command
//! identity, so which command it executed at a position is not a lookup:
//! [`ExecutedOrder`] reads the whole table once, the first time a peer
//! asks, and is kept current from this node's own executions after that.
//! A page reads nothing but the rows of the commands it carries.
//!
//! The requester half, [`Pacer`], decides when a voter asks and whom. A
//! voter that holds work it is not executing, and whose frontier has not
//! moved for [`STILL_FOR`], asks the leader; one that is not answered
//! asks the other voters in turn. The machine asks again at once when a
//! full page is executed, so the pacer is the floor for an ask nobody
//! answered, not the rate a voter catches up at.

use std::time::{Duration, Instant};

use coord_consensus::{CatchUpEntry, MAX_CATCH_UP_BYTES, MAX_CATCH_UP_COMMANDS};
use coord_storage::views::ViewBudget;
use coord_storage::{Applier, Persistence};
use coord_types::CommandId;
use coord_types::ids::{ConfigurationEpoch, ExecutionPosition, ReplicaId};

/// How long a voter holding work it does not execute waits, with its
/// frontier still, before it asks a peer for what it is missing.
pub const STILL_FOR: Duration = Duration::from_secs(1);

/// How long an ask goes unanswered before the next one, to the next
/// donor.
pub const UNANSWERED: Duration = Duration::from_secs(1);

/// How many unanswered asks go to the leader before the other voters are
/// asked in turn.
pub const LEADER_ASKS: u32 = 2;

/// This node's executed commands in position order, in memory (task-d08).
///
/// Built once from `executed_v1` -- a read of the whole table, O(history)
/// in time and memory -- and extended by [`ExecutedOrder::note`] as this
/// node executes, so no page rescans the table. A durable position index
/// is what would make it cheaper, and is out of this task's scope.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExecutedOrder {
    /// The position of `commands[0]`.
    first: u64,
    commands: Vec<CommandId>,
}

impl ExecutedOrder {
    /// The order of `rows`, as (position, command) in position order.
    ///
    /// Only the contiguous run that ends at the highest position is kept:
    /// a page is contiguous from the requester's frontier, and a gap
    /// below the run -- which a store that forgot a prefix would show --
    /// is simply not served.
    pub fn from_rows(rows: &[(ExecutionPosition, CommandId)]) -> Self {
        let Some(&(last, _)) = rows.last() else {
            return ExecutedOrder::default();
        };
        let mut start = rows.len() - 1;
        while start > 0 && rows[start - 1].0.get() + 1 == rows[start].0.get() {
            start -= 1;
        }
        let commands: Vec<CommandId> = rows[start..].iter().map(|(_, c)| *c).collect();
        ExecutedOrder {
            first: last.get() + 1 - commands.len() as u64,
            commands,
        }
    }

    /// Read this node's order from its store.
    pub fn read<P: Persistence>(applier: &Applier<P>) -> Option<Self> {
        let gated = applier.store().reader().snapshot().ok()?;
        let rows =
            coord_storage::protocol::executed_order(gated.view(), ViewBudget::default()).ok()?;
        Some(ExecutedOrder::from_rows(&rows))
    }

    /// This node executed `command` at `position`. Returns `false` when
    /// that is not the next position the order holds, and the order is to
    /// be read again.
    pub fn note(&mut self, position: ExecutionPosition, command: CommandId) -> bool {
        if self.commands.is_empty() {
            self.first = position.get();
        }
        if position.get() != self.first + self.commands.len() as u64 {
            return false;
        }
        self.commands.push(command);
        true
    }

    /// The commands executed after `after`, through `through`, as
    /// (position, command).
    pub fn between(
        &self,
        after: ExecutionPosition,
        through: ExecutionPosition,
    ) -> impl Iterator<Item = (ExecutionPosition, CommandId)> + '_ {
        let from = after.get().saturating_add(1);
        let skip = if from >= self.first && !self.commands.is_empty() {
            usize::try_from(from - self.first).unwrap_or(usize::MAX)
        } else {
            // Below what this order holds, or nothing held: a page has to
            // start at the requester's frontier, and cannot.
            usize::MAX
        };
        self.commands
            .iter()
            .enumerate()
            .skip(skip)
            .map(|(i, c)| (self.first + i as u64, *c))
            .take_while(move |(p, _)| *p <= through.get())
            .filter_map(|(p, c)| ExecutionPosition::new(p).ok().map(|p| (p, c)))
    }
}

/// The page a donor sends for `after` (task-d08): the commands it
/// executed after `after` and through `through`, read from its durable
/// rows, contiguous, at most [`MAX_CATCH_UP_COMMANDS`] of them and at most
/// [`MAX_CATCH_UP_BYTES`] beyond the first.
///
/// Stops at the first command whose rows it cannot read, or whose
/// executed row does not say the position the order does: the page is a
/// prefix of what the donor can show, never a page with a hole.
pub fn page<P: Persistence>(
    applier: &Applier<P>,
    order: &ExecutedOrder,
    epoch: ConfigurationEpoch,
    after: ExecutionPosition,
    through: ExecutionPosition,
) -> Vec<CatchUpEntry> {
    let Ok(gated) = applier.store().reader().snapshot() else {
        return Vec::new();
    };
    let mut entries = Vec::new();
    let mut bytes = 0usize;
    for (position, command) in order.between(after, through) {
        if entries.len() >= MAX_CATCH_UP_COMMANDS {
            break;
        }
        let Ok(Some(entry)) =
            coord_storage::protocol::catch_up_entry(gated.view(), epoch, &command)
        else {
            break;
        };
        if entry.position != position {
            break;
        }
        let len = entry.encoded_len();
        if !entries.is_empty() && bytes.saturating_add(len) > MAX_CATCH_UP_BYTES {
            break;
        }
        bytes = bytes.saturating_add(len);
        entries.push(entry);
    }
    entries
}

/// The stop a catch-up divergence is (task-d08), in the form every
/// divergence stop takes (task-d17): the donor's execution on the
/// release side, this node's on its own.
///
/// Where this node had executed the command already, before the position
/// the donor names, its own side is its `executed_v1` row of it, read
/// from `applier`; one that cannot be read shows position zero.
pub fn mismatch<P: Persistence>(
    applier: &Applier<P>,
    divergence: &coord_consensus::CatchUpDivergence,
    epoch: ConfigurationEpoch,
) -> coord_collector::Mismatch {
    use coord_collector::{Differs, Mismatch, MismatchCheck, ReleaseOrigin, Said};
    let donor = divergence.donor;
    let release = Said {
        release: Some(ReleaseOrigin {
            sender: donor.donor,
            epoch,
            ballot: donor.ballot,
            speculative: false,
        }),
        position: donor.position,
        revision: donor.revision,
        result_digest: donor.result_digest,
        response_len: 0,
    };
    let own = match divergence.own {
        Some(own) => Said {
            release: None,
            position: own.position,
            revision: own.revision,
            result_digest: own.result_digest,
            response_len: own.response_len,
        },
        None => {
            let record = executed_record(applier, &divergence.command);
            Said {
                release: None,
                position: record.map_or(ExecutionPosition::ZERO, |r| r.position),
                revision: record.and_then(|r| r.revision),
                result_digest: record.map_or(coord_types::identity::Digest32([0; 32]), |r| {
                    r.result_digest
                }),
                response_len: 0,
            }
        }
    };
    Mismatch {
        command: divergence.command,
        check: MismatchCheck::CatchUpAgainstDonor,
        differs: Differs {
            position: release.position != own.position,
            revision: release.revision != own.revision,
            result_digest: release.result_digest != own.result_digest,
            response: false,
        },
        release,
        own,
    }
}

/// This node's `executed_v1` row of `command`, if it has one.
fn executed_record<P: Persistence>(
    applier: &Applier<P>,
    command: &CommandId,
) -> Option<coord_storage::codecs::ExecutedRecordV1> {
    let gated = applier.store().reader().snapshot().ok()?;
    let bytes = coord_store_api::engine::OrderedRead::get(
        gated.view(),
        coord_store_api::registry::Collection::ExecutedV1.id(),
        &coord_storage::codecs::executed_key(command),
    )
    .ok()??;
    coord_storage::codecs::decode_executed(&bytes).ok()
}

/// What a voter's catch-up looks like to the pacer on one turn.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Standing {
    /// What it executed through.
    pub executed: ExecutionPosition,
    /// Whether it holds work it has not executed.
    pub holds: bool,
    /// Whether a page is in hand.
    pub busy: bool,
    /// Whether it leads: a leader has nobody to catch up from.
    pub leads: bool,
}

/// When a voter asks a peer for what it is missing, and whom (task-d08).
#[derive(Clone, Debug, Default)]
pub struct Pacer {
    /// The frontier last seen, and since when it has not moved.
    still: Option<(ExecutionPosition, Instant)>,
    /// When the last ask went, if one is outstanding.
    asked: Option<Instant>,
    /// Asks gone unanswered since the frontier last moved.
    unanswered: u32,
    /// Whether the last turn found a voter that may have to ask: one
    /// that holds work, does not lead, and has no page in hand. Only
    /// such a voter has a deadline ([`Pacer::next_deadline`]).
    wanting: bool,
    /// Asks made (diagnostic).
    pub asks: u64,
}

impl Pacer {
    /// Whether to ask now, and whom: the leader first, the other voters
    /// in turn once the leader has not answered [`LEADER_ASKS`] times.
    /// `voters` is the configuration's, in a fixed order; `me` and
    /// `leader` are this voter and the ballot's leader.
    pub fn due(
        &mut self,
        now: Instant,
        standing: Standing,
        me: ReplicaId,
        leader: ReplicaId,
        voters: &[ReplicaId],
    ) -> Option<ReplicaId> {
        self.wanting = standing.holds && !standing.leads && !standing.busy;
        let moved = self.still.is_none_or(|(at, _)| at != standing.executed);
        if moved || standing.busy {
            self.still = Some((standing.executed, now));
            self.asked = None;
            self.unanswered = 0;
            return None;
        }
        if standing.leads || !standing.holds {
            self.asked = None;
            self.unanswered = 0;
            return None;
        }
        let since = self.still.map_or(now, |(_, since)| since);
        if now.saturating_duration_since(since) < STILL_FOR {
            return None;
        }
        if let Some(at) = self.asked {
            if now.saturating_duration_since(at) < UNANSWERED {
                return None;
            }
            self.unanswered = self.unanswered.saturating_add(1);
        }
        // Moved on before anything else, so the deadline moves past this
        // turn whether or not there is anyone to ask.
        self.asked = Some(now);
        let others: Vec<ReplicaId> = voters.iter().copied().filter(|v| *v != me).collect();
        let donor = if self.unanswered < LEADER_ASKS && leader != me {
            leader
        } else {
            let turn = (self.unanswered.saturating_sub(LEADER_ASKS)) as usize;
            *others.get(turn % others.len().max(1))?
        };
        self.asks += 1;
        Some(donor)
    }

    /// When the loop has to give this voter a turn even if nothing
    /// arrives: the moment a still frontier has been still long enough to
    /// ask, or an ask has gone unanswered long enough to ask again.
    /// `None` for a voter with nothing to ask for.
    ///
    /// A follower with held work and no traffic is exactly the voter this
    /// is for: nothing else wakes an idle loop, and a healthy follower has
    /// no election deadline either.
    pub fn next_deadline(&self) -> Option<Instant> {
        if !self.wanting {
            return None;
        }
        match (self.asked, self.still) {
            (Some(at), _) => Some(at + UNANSWERED),
            (None, Some((_, since))) => Some(since + STILL_FOR),
            (None, None) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coord_types::identity::Digest32;

    fn c(n: u8) -> CommandId {
        CommandId(Digest32([n; 32]))
    }

    fn p(n: u64) -> ExecutionPosition {
        ExecutionPosition::new(n).expect("position")
    }

    fn r(n: u8) -> ReplicaId {
        ReplicaId([n; 16])
    }

    #[test]
    fn the_order_keeps_the_run_ending_at_the_highest_position() {
        let order = ExecutedOrder::from_rows(&[(p(1), c(1)), (p(3), c(3)), (p(4), c(4))]);
        let served: Vec<_> = order.between(p(2), p(10)).collect();
        assert_eq!(served, vec![(p(3), c(3)), (p(4), c(4))]);
        // A frontier below the run cannot start a contiguous page.
        assert_eq!(order.between(p(1), p(10)).count(), 0);
    }

    #[test]
    fn the_order_is_extended_by_execution_and_bounded_by_through() {
        let mut order = ExecutedOrder::from_rows(&[(p(1), c(1)), (p(2), c(2))]);
        assert!(order.note(p(3), c(3)));
        assert!(!order.note(p(5), c(5)), "a gap is refused");
        let served: Vec<_> = order.between(p(1), p(2)).collect();
        assert_eq!(served, vec![(p(2), c(2))]);
        assert_eq!(order.between(ExecutionPosition::ZERO, p(3)).count(), 3);
    }

    #[test]
    fn an_empty_order_starts_at_the_first_execution_it_sees() {
        let mut order = ExecutedOrder::default();
        assert!(order.note(p(7), c(7)));
        assert_eq!(order.between(p(6), p(7)).count(), 1);
    }

    fn standing(executed: u64, holds: bool) -> Standing {
        Standing {
            executed: p(executed),
            holds,
            busy: false,
            leads: false,
        }
    }

    #[test]
    fn a_voter_asks_the_leader_once_still_and_then_the_others_in_turn() {
        let voters = [r(1), r(2), r(3)];
        let t0 = Instant::now();
        let mut pacer = Pacer::default();
        assert_eq!(pacer.due(t0, standing(5, true), r(3), r(1), &voters), None);
        let t = t0 + STILL_FOR;
        assert_eq!(
            pacer.due(t, standing(5, true), r(3), r(1), &voters),
            Some(r(1))
        );
        // One ask outstanding: nothing more until it goes unanswered.
        assert_eq!(pacer.due(t, standing(5, true), r(3), r(1), &voters), None);
        let t = t + UNANSWERED;
        assert_eq!(
            pacer.due(t, standing(5, true), r(3), r(1), &voters),
            Some(r(1))
        );
        let t = t + UNANSWERED;
        assert_eq!(
            pacer.due(t, standing(5, true), r(3), r(1), &voters),
            Some(r(1))
        );
        let t = t + UNANSWERED;
        assert_eq!(
            pacer.due(t, standing(5, true), r(3), r(1), &voters),
            Some(r(2))
        );
        // Progress starts over with the leader.
        let t = t + UNANSWERED;
        assert_eq!(pacer.due(t, standing(9, true), r(3), r(1), &voters), None);
        let t = t + STILL_FOR;
        assert_eq!(
            pacer.due(t, standing(9, true), r(3), r(1), &voters),
            Some(r(1))
        );
    }

    /// The pacer registers when it next has to look, so an idle loop
    /// wakes for it, and the deadline moves on once the turn it woke has
    /// asked (task-d08).
    #[test]
    fn the_pacer_says_when_it_next_has_to_look() {
        let voters = [r(1), r(2), r(3)];
        let t0 = Instant::now();
        let mut pacer = Pacer::default();
        assert_eq!(pacer.next_deadline(), None);
        pacer.due(t0, standing(5, true), r(3), r(1), &voters);
        assert_eq!(pacer.next_deadline(), Some(t0 + STILL_FOR));
        let t = t0 + STILL_FOR;
        assert_eq!(
            pacer.due(t, standing(5, true), r(3), r(1), &voters),
            Some(r(1))
        );
        assert_eq!(pacer.next_deadline(), Some(t + UNANSWERED));
        // Nothing held: nothing to wake for.
        pacer.due(t, standing(5, false), r(3), r(1), &voters);
        assert_eq!(pacer.next_deadline(), None);
        // A page in hand makes progress by execution, not by the clock.
        let busy = Standing {
            busy: true,
            ..standing(5, true)
        };
        pacer.due(t, busy, r(3), r(1), &voters);
        assert_eq!(pacer.next_deadline(), None);
        // Alone, with nobody to ask, the deadline still moves on.
        let mut alone = Pacer::default();
        alone.due(t0, standing(5, true), r(3), r(3), &[r(3)]);
        let t = t0 + STILL_FOR;
        assert_eq!(alone.due(t, standing(5, true), r(3), r(3), &[r(3)]), None);
        assert_eq!(alone.next_deadline(), Some(t + UNANSWERED));
    }

    #[test]
    fn a_voter_with_nothing_held_or_that_leads_does_not_ask() {
        let voters = [r(1), r(2), r(3)];
        let t0 = Instant::now();
        let mut pacer = Pacer::default();
        pacer.due(t0, standing(5, false), r(3), r(1), &voters);
        let t = t0 + STILL_FOR * 3;
        assert_eq!(pacer.due(t, standing(5, false), r(3), r(1), &voters), None);
        let leads = Standing {
            leads: true,
            ..standing(5, true)
        };
        assert_eq!(pacer.due(t, leads, r(1), r(1), &voters), None);
        let busy = Standing {
            busy: true,
            ..standing(5, true)
        };
        assert_eq!(pacer.due(t, busy, r(3), r(1), &voters), None);
    }
}
