//! Completing what a collector half holds from this node's own durable
//! record of a command's execution (task-c02).
//!
//! A collector that holds the leader's release but not the votes, or the
//! votes but not the release, is the shape a lost frontend delivery
//! leaves behind: the acknowledgement or the release went to a frontend
//! that did not yet know which collector had asked, and that frontend
//! stopped holding it. A voter repairs the first case when the
//! submission reaches it again ([`coord_consensus::ReplayRefusal`] names
//! the cases where it cannot); this is the path for both when the command
//! has executed *here*, because then this node's committed state says
//! what the missing half would have said. It is the same record that
//! answers a caller's retry before anything is submitted, read the same
//! way, and trusted for the same reason.
//!
//! What this module owns is the read: which half-held commands this
//! node's store has a record for, under that command's own identity. What
//! the collector does with a record is the collector's
//! ([`coord_collector::Collector::settle_from_record`]), and it is the
//! collector that decides whether its own evidence corroborates it.

use coord_collector::Mismatch;
use coord_storage::codecs::{ExecutedRecordV1, RetryRecordV1};
use coord_storage::views::ViewBudget;
use coord_storage::{Applier, Persistence};
use coord_types::{CommandId, RetryKey};

/// Up to `per_turn` of `half`, starting where the last turn left off.
///
/// `cursor` is the turn's own bookkeeping: pass the value the last call
/// returned, and get the value to pass next time. What one turn looks
/// at is bounded; what is looked at *eventually* is not, and that is
/// the point of the rotation. Taking the first `per_turn` every time
/// would let a command that can never settle here -- no record of it on
/// this node, or a record that disagrees with the release -- occupy the
/// front of the list for as long as it is pending, and everything
/// behind it would wait with it, unexamined. Starting where the last
/// turn stopped means every half-held command is reached within
/// `len / per_turn` turns of becoming half held, whatever sits in front
/// of it.
pub fn window<T: Clone>(half: Vec<T>, cursor: usize, per_turn: usize) -> (Vec<T>, usize) {
    if half.is_empty() || per_turn == 0 {
        return (Vec::new(), 0);
    }
    let len = half.len();
    let start = cursor % len;
    let take = per_turn.min(len);
    let taken = half.into_iter().cycle().skip(start).take(take).collect();
    (taken, start + take)
}

/// The durable records this node holds for `half`, under each command's
/// own identity.
///
/// A record bound to the retry key but naming another command is a
/// conflict, which replicated execution decides at that command's own
/// position; it is not something to settle from, and it is left out. A
/// key with no record yet is left out the same way: the command has not
/// executed here, and nothing here says what it will produce.
///
/// One snapshot serves every lookup, so the answer is one consistent
/// reading of the store rather than a reading per command.
pub fn records_for<P: Persistence>(
    applier: &Applier<P>,
    half: impl IntoIterator<Item = (CommandId, RetryKey)>,
) -> Vec<(CommandId, RetryRecordV1)> {
    let Ok(gated) = applier.store().reader().snapshot() else {
        return Vec::new();
    };
    half.into_iter()
        .filter_map(|(command, key)| {
            let record = coord_storage::retry::lookup(gated.view(), &key).ok()??;
            (record.command_id == command).then_some((command, record))
        })
        .collect()
}

/// The half-established commands whose retry key this node's store holds
/// bound to another command, with that command (task-d22): the domain
/// executed the other one under the key, and these are rejected wherever
/// they execute. Read from one snapshot, as [`records_for`].
pub fn conflicts_for<P: Persistence>(
    applier: &Applier<P>,
    half: impl IntoIterator<Item = (CommandId, RetryKey)>,
) -> Vec<(CommandId, CommandId)> {
    let Ok(gated) = applier.store().reader().snapshot() else {
        return Vec::new();
    };
    half.into_iter()
        .filter_map(|(command, key)| {
            let record = coord_storage::retry::lookup(gated.view(), &key).ok()??;
            (record.command_id != command).then_some((command, record.command_id))
        })
        .collect()
}

/// What a node's store holds for the half-held commands of one turn,
/// read from one snapshot: each command's own record, and the commands
/// whose retry key it holds bound to another command, with that command.
#[derive(Debug, Default)]
pub struct Records {
    /// As [`records_for`] gives them.
    pub records: Vec<(CommandId, RetryRecordV1)>,
    /// As [`conflicts_for`] gives them.
    pub conflicts: Vec<(CommandId, CommandId)>,
}

/// The read of the half-held commands a domain does every turn
/// (task-d75): one snapshot and one lookup a key, for the records and the
/// conflicts both, and no snapshot at all when nothing it would read can
/// have changed.
///
/// A record only appears with a projection commit, and a commit moves
/// the completed frontier, so a command found with no record stays
/// without one until the frontier moves: until then it is not looked up
/// again, and a turn whose commands are all such takes no snapshot. With
/// a domain serving its callers, nearly every command is half held for
/// a while before its record appears, and every turn pinned a snapshot,
/// read the durable metadata and opened the retry table twice to find
/// none (a twentieth of a follower's domain loop). A command that has a
/// record, or one bound to another command, is read every turn, as
/// before: what the collector makes of it is the collector's.
///
/// What it remembers is bounded by what is still half held, which the
/// caller says with [`RecordReads::retain`] every turn: on a node whose
/// store takes no commands -- a frontend-only one -- the frontier never
/// moves, and every command that was ever half held there for a turn
/// would otherwise stay remembered for the life of the process (Codex
/// review of #168).
#[derive(Debug, Default)]
pub struct RecordReads {
    /// The completed frontier `absent` was read at.
    at: Option<u64>,
    /// The commands found with no record at `at`.
    absent: std::collections::BTreeSet<CommandId>,
}

impl RecordReads {
    /// Forget every command not in `pending`, the commands still half
    /// held.
    pub fn retain(&mut self, pending: impl IntoIterator<Item = CommandId>) {
        if self.absent.is_empty() {
            return;
        }
        let pending: std::collections::BTreeSet<CommandId> = pending.into_iter().collect();
        self.absent.retain(|command| pending.contains(command));
    }

    /// How many commands it remembers as having no record.
    pub fn remembered(&self) -> usize {
        self.absent.len()
    }

    /// What the store holds for `half`.
    pub fn read<P: Persistence>(
        &mut self,
        applier: &Applier<P>,
        half: impl IntoIterator<Item = (CommandId, RetryKey)>,
    ) -> Records {
        let reader = applier.store().reader();
        let completed = reader.completed();
        if self.at != Some(completed) {
            self.at = Some(completed);
            self.absent.clear();
        }
        let half: Vec<(CommandId, RetryKey)> = half
            .into_iter()
            .filter(|(command, _)| !self.absent.contains(command))
            .collect();
        let mut out = Records::default();
        if half.is_empty() {
            return out;
        }
        // Pinned after the frontier was read, so it shows at least what
        // the frontier says: a record it lacks was not there at `at`
        // either.
        let Ok(gated) = reader.snapshot() else {
            return out;
        };
        for (command, key) in half {
            match coord_storage::retry::lookup(gated.view(), &key) {
                Ok(Some(record)) if record.command_id == command => {
                    out.records.push((command, record));
                }
                Ok(Some(record)) => out.conflicts.push((command, record.command_id)),
                Ok(None) => {
                    self.absent.insert(command);
                }
                Err(_) => {}
            }
        }
        out
    }
}

/// What one turn of settling from records sends, or the command that
/// stopped it.
#[derive(Debug, PartialEq, Eq)]
pub enum Settled<D> {
    /// Every record was offered.
    Offered {
        /// The commands the collector accepted a record for.
        settled: u64,
        /// What goes out.
        deliveries: Vec<D>,
    },
    /// The collector holds a release this node's own record of the
    /// command contradicts (task-d06). This node executed the domain's
    /// commands in another order than the leader, so nothing it read
    /// from that record this turn goes out, and nothing after the
    /// mismatch is offered. What was compared comes with it (task-d17).
    Diverged(Box<Mismatch>),
}

/// Offer each of `records` to `settle` in turn, and say what goes out.
///
/// `settle` is the collector's own decision
/// ([`coord_collector::Dispatcher::settle_from_record`] in `coordd`).
/// A refusal for want of corroboration, or of a command no longer
/// pending, is ordinary and passed over. A mismatch is not: it ends the
/// turn at once, and the deliveries already settled in it are dropped
/// with it, since they come from the same execution record.
pub fn offer<D>(
    records: impl IntoIterator<Item = (CommandId, RetryRecordV1)>,
    mut settle: impl FnMut(CommandId, &RetryRecordV1) -> Result<Option<D>, coord_collector::SettleError>,
) -> Settled<D> {
    let mut settled = 0;
    let mut deliveries = Vec::new();
    for (command, record) in records {
        match settle(command, &record) {
            Ok(delivery) => {
                settled += 1;
                deliveries.extend(delivery);
            }
            Err(coord_collector::SettleError::Mismatch(mismatch)) => {
                return Settled::Diverged(mismatch);
            }
            Err(_) => {}
        }
    }
    Settled::Offered {
        settled,
        deliveries,
    }
}

/// How far either side of a stopped command's two positions the stop
/// shows this node's executed rows (task-d17).
pub const NEAR_POSITIONS: u64 = 8;

/// This node's `executed_v1` rows within [`NEAR_POSITIONS`] of where it
/// executed `mismatch`'s command and of where the leader's release put
/// it, in position order (task-d17).
///
/// Read once, when the node stops on the mismatch, so that the stop
/// shows the order this node executed in around the command. A store
/// that cannot be read shows nothing, and the stop still says what it
/// compared.
pub fn executed_near<P: Persistence>(
    applier: &Applier<P>,
    mismatch: &Mismatch,
) -> Vec<(CommandId, ExecutedRecordV1)> {
    let Ok(gated) = applier.store().reader().snapshot() else {
        return Vec::new();
    };
    coord_storage::protocol::executed_near(
        gated.view(),
        &[mismatch.own.position, mismatch.release.position],
        NEAR_POSITIONS,
        ViewBudget::default(),
    )
    .unwrap_or_default()
}
