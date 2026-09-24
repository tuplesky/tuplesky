//! Evidence a voter produced for a submitter it does not know yet
//! (task-c02).
//!
//! A voter learns a command's content from a submission *or* from a
//! peer, and when it learns it from a peer it acknowledges a command no
//! collector has yet asked it for. That acknowledgement belongs to
//! whichever collector did the asking, and the submission naming it is
//! usually already in flight; giving it to the collector in this process
//! instead loses it, because that collector has never heard of the
//! command. So the frame waits here for the submission to say where it
//! goes.
//!
//! What waits here is held under two bounds, a hold and a depth, and
//! both are *performance* controls, not correctness boundaries. What is
//! let go is not lost: the voter that produced it keeps what it
//! published, and the submission that eventually names the command -- a
//! collector's repeat, or a caller's retry -- has the voter publish it
//! again through the same outbox, to that collector alone. The hold
//! exists so that the ordinary race, won in microseconds, is served from
//! here rather than from that repair, and so that a loaded voter holds a
//! window's evidence rather than a whole depth of it; the depth is the
//! ceiling under that window. Neither is what makes a caller's request
//! complete.
//!
//! Time is an input here, not something this module reads: the runtime
//! passes the instant it is at, so the hold expiring and the depth
//! crowding out are facts a test can produce without waiting for them.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use coord_core::event::PeerProvenance;
use coord_types::CommandId;

use crate::voter::Origin;

/// One frame of evidence waiting for the submission that places it.
#[derive(Clone, Debug)]
pub struct Held {
    /// The command the frame is about.
    pub command: CommandId,
    /// The committed identity the frame's sender is bound to.
    pub provenance: PeerProvenance,
    /// The frame, exactly as the voter published it.
    pub bytes: Vec<u8>,
    /// When it was parked, so the hold can be applied to it.
    since: Instant,
}

/// What one pass over the held evidence did.
#[derive(Debug, Default)]
pub struct Routed {
    /// Frames whose submitter is now known, oldest first, with where
    /// each is owed.
    pub ready: Vec<(Origin, PeerProvenance, Vec<u8>)>,
    /// Frames let go because they waited longer than the race can take.
    pub unclaimed: usize,
}

/// The evidence one voter is holding for want of a submitter, oldest
/// first.
#[derive(Debug)]
pub struct Parked {
    hold: Duration,
    depth: usize,
    queue: VecDeque<Held>,
}

impl Parked {
    /// Hold evidence for at most `hold`, and at most `depth` frames of
    /// it.
    pub const fn new(hold: Duration, depth: usize) -> Self {
        Parked {
            hold,
            depth,
            queue: VecDeque::new(),
        }
    }

    /// How long evidence waits here before it is let go.
    pub const fn hold(&self) -> Duration {
        self.hold
    }

    /// How many frames are waiting.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Whether nothing is waiting.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// When the oldest frame here will have waited the whole hold: the
    /// instant [`Parked::route`] next has something to let go of without
    /// a submission arriving. `None` when nothing is waiting.
    ///
    /// The runtime wakes on it. The hold is applied only when a turn
    /// runs, and a voter nobody is submitting to runs none, so without
    /// the wake what it holds would outlive the hold until unrelated
    /// traffic arrived.
    pub fn next_expiry(&self) -> Option<Instant> {
        self.queue.front().map(|oldest| oldest.since + self.hold)
    }

    /// The commands waiting, oldest first (diagnostic).
    pub fn commands(&self) -> impl Iterator<Item = &CommandId> {
        self.queue.iter().map(|h| &h.command)
    }

    /// Hold `bytes` for `command`, parked at `now`.
    ///
    /// Returns how many frames were crowded out to make room: past the
    /// depth the oldest goes, which is the one whose submission is least
    /// likely still coming. The command it was about is decided and
    /// durable either way, and the submission that names it has the
    /// voter publish its evidence again.
    pub fn park(
        &mut self,
        command: CommandId,
        provenance: PeerProvenance,
        bytes: Vec<u8>,
        now: Instant,
    ) -> usize {
        self.queue.push_back(Held {
            command,
            provenance,
            bytes,
            since: now,
        });
        let mut crowded_out = 0;
        while self.queue.len() > self.depth {
            self.queue.pop_front();
            crowded_out += 1;
        }
        crowded_out
    }

    /// Take out what can be routed at `now`: every frame whose submitter
    /// `origin_of` now names, in the order it was parked, and let go of
    /// what has waited the whole hold without one.
    ///
    /// Called after submissions are taken in, which is the only thing
    /// that can supply an origin, and at the end of every turn; on a node
    /// nobody is submitting to, the runtime takes that turn when
    /// [`Parked::next_expiry`] passes.
    pub fn route(
        &mut self,
        now: Instant,
        origin_of: impl Fn(&CommandId) -> Option<Origin>,
    ) -> Routed {
        let mut routed = Routed::default();
        if self.queue.is_empty() {
            return routed;
        }
        let mut still_waiting = VecDeque::with_capacity(self.queue.len());
        for held in core::mem::take(&mut self.queue) {
            match origin_of(&held.command) {
                Some(origin) => routed.ready.push((origin, held.provenance, held.bytes)),
                // Past the window the race takes, so the submission is
                // not late, it is not coming. Let it go rather than hold
                // it until something newer needs the room.
                None if now.duration_since(held.since) >= self.hold => routed.unclaimed += 1,
                None => still_waiting.push_back(held),
            }
        }
        self.queue = still_waiting;
        routed
    }
}
