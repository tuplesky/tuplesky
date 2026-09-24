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

use coord_storage::codecs::RetryRecordV1;
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
