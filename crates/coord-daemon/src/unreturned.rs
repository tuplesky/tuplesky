//! What a voter owes a collector in another process and the transport
//! would not take yet.
//!
//! A voter's evidence and its leader's releases go back to the collector
//! that submitted the command, on that collector's api connection, one
//! hand-off a connection a round (task-d115). The transport admits each
//! frame to the connection's lane queue or refuses it, and a lane whose
//! streams are slow to finish -- a lossy link, a congestion window just
//! cut -- fills its queue. A refused frame used to be counted
//! `unreturnable` and dropped. For a release that was the only copy the
//! collector would get: it had another only when it asked the leader
//! again, a second later, which is the ladder the +loss1 reading on #185
//! traced its whole-second steps to.
//!
//! So a frame refused because the queue was full is kept here, for its
//! connection, and offered again on a doubling schedule. A frame for a
//! connection with frames kept goes behind them rather than past them,
//! so what a voter owes one collector still leaves in the order the
//! voter made it, and a full queue is not retried once a frame. A frame
//! refused for good -- no connection, or too large for the lane -- is let
//! go as before, and so is everything kept for a connection that has
//! gone.
//!
//! Everything here is held under three bounds, frames, bytes and a hold,
//! and they are *performance* controls, not correctness boundaries: what
//! they let go is recovered as it was before this existed, by the
//! collector asking again. The hold is longer than the collector waits
//! before it asks, so a kept frame normally leaves long before it.
//!
//! Time is an input here, as in [`crate::parked`]: the runtime passes the
//! instant it is at, so the schedule and the hold are facts a test can
//! produce without waiting for them.

use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

use coord_transport::{Refused, SendError};

/// The first wait before a connection's kept frames are offered again.
///
/// Soon, because a queue that was full a moment ago is often free a
/// moment later; not a turn, because the drain that ends a full queue is
/// the lane's sender finishing streams, and offering again every turn
/// would spend the loop on refusals while it happens.
pub const FIRST_WAIT: Duration = Duration::from_millis(25);

/// The longest wait between offers: doubling stops here.
///
/// Well inside the second the collector waits before it asks the leader
/// for a release again (`coord_collector::SOLICIT_LEADER_AFTER_MILLIS`),
/// so a kept release is offered several times before the collector's
/// ask would make the leader publish it again.
pub const LONGEST_WAIT: Duration = Duration::from_millis(200);

/// What one call did with the frames it was given or found due.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Offered {
    /// Hand-offs to the transport: one a connection offered to.
    pub handoffs: u64,
    /// Frames the transport admitted.
    pub returned: u64,
    /// Of those, frames that had been kept here first.
    pub returned_again: u64,
    /// Frames that came to be kept here: refused because the lane's
    /// queue was full, or queued behind frames already kept.
    pub kept: u64,
    /// Frames let go: refused for good, past a bound, or kept for longer
    /// than the hold.
    pub unreturnable: u64,
}

impl Offered {
    /// Add `other`'s counts to these.
    pub fn add(&mut self, other: Self) {
        self.handoffs += other.handoffs;
        self.returned += other.returned;
        self.returned_again += other.returned_again;
        self.kept += other.kept;
        self.unreturnable += other.unreturnable;
    }
}

/// One frame kept for a later offer.
#[derive(Debug)]
struct Kept {
    /// When it was first kept, so the hold can be applied to it.
    since: Instant,
    /// The frame, as the voter made it.
    frame: Vec<u8>,
}

/// What is kept for one connection.
#[derive(Debug)]
struct Owed {
    /// In the order the voter made them.
    frames: VecDeque<Kept>,
    /// When they are offered again.
    due: Instant,
    /// The wait that set `due`, doubled when an offer admits nothing.
    wait: Duration,
}

/// The frames this voter is keeping for collectors whose lanes were full.
#[derive(Debug)]
pub struct Unreturned {
    owed: BTreeMap<u64, Owed>,
    frames: usize,
    bytes: usize,
    max_frames: usize,
    max_bytes: usize,
    hold: Duration,
}

impl Unreturned {
    /// Keep at most `max_frames` frames and `max_bytes` bytes of them,
    /// each for at most `hold`.
    pub fn new(max_frames: usize, max_bytes: usize, hold: Duration) -> Self {
        Unreturned {
            owed: BTreeMap::new(),
            frames: 0,
            bytes: 0,
            max_frames,
            max_bytes,
            hold,
        }
    }

    /// Whether nothing is kept.
    pub fn is_empty(&self) -> bool {
        self.owed.is_empty()
    }

    /// Frames kept now.
    pub const fn frames(&self) -> usize {
        self.frames
    }

    /// Bytes of the frames kept now.
    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    /// When the next connection's kept frames are due to be offered
    /// again, if any are kept.
    pub fn next_due(&self) -> Option<Instant> {
        self.owed.values().map(|owed| owed.due).min()
    }

    /// Return `frames` to `connection`, through `offer`, behind anything
    /// kept for it.
    ///
    /// With nothing kept for the connection they are offered now, and
    /// what the transport refuses for a full queue is kept. With frames
    /// kept they join the back and wait for the kept ones' turn: offered
    /// now, they would go past the kept ones, or meet the full queue
    /// that refused those.
    pub fn send<F>(
        &mut self,
        connection: u64,
        frames: Vec<Vec<u8>>,
        now: Instant,
        offer: F,
    ) -> Offered
    where
        F: FnOnce(Vec<Vec<u8>>) -> Vec<Result<(), Refused>>,
    {
        let mut offered = Offered::default();
        if frames.is_empty() {
            return offered;
        }
        if self.owed.contains_key(&connection) {
            for frame in frames {
                self.keep_back(connection, Kept { since: now, frame }, now, &mut offered);
            }
            return offered;
        }
        let sinces = vec![None; frames.len()];
        offered.handoffs += 1;
        let results = offer(frames);
        self.settle(connection, sinces, results, now, FIRST_WAIT, &mut offered);
        offered
    }

    /// Offer again, through `offer`, the frames of every connection whose
    /// turn has come, and let go of those kept longer than the hold.
    pub fn offer_due<F>(&mut self, now: Instant, mut offer: F) -> Offered
    where
        F: FnMut(u64, Vec<Vec<u8>>) -> Vec<Result<(), Refused>>,
    {
        let mut offered = Offered::default();
        let due: Vec<u64> = self
            .owed
            .iter()
            .filter(|(_, owed)| owed.due <= now)
            .map(|(connection, _)| *connection)
            .collect();
        for connection in due {
            let Some(owed) = self.owed.remove(&connection) else {
                continue;
            };
            let mut sinces = Vec::with_capacity(owed.frames.len());
            let mut frames = Vec::with_capacity(owed.frames.len());
            for kept in owed.frames {
                self.frames -= 1;
                self.bytes -= kept.frame.len();
                if now.saturating_duration_since(kept.since) >= self.hold {
                    offered.unreturnable += 1;
                    continue;
                }
                sinces.push(Some(kept.since));
                frames.push(kept.frame);
            }
            if frames.is_empty() {
                continue;
            }
            offered.handoffs += 1;
            let results = offer(connection, frames);
            let wait = owed.wait.saturating_mul(2).min(LONGEST_WAIT);
            self.settle(connection, sinces, results, now, wait, &mut offered);
        }
        offered
    }

    /// Count what an offer did, and keep, in order, what it refused for a
    /// full queue. `sinces` is `None` for a frame offered for the first
    /// time. The kept frames are due again after `FIRST_WAIT` if the
    /// offer admitted any -- the queue is draining -- and after `wait`
    /// if it admitted none.
    fn settle(
        &mut self,
        connection: u64,
        sinces: Vec<Option<Instant>>,
        results: Vec<Result<(), Refused>>,
        now: Instant,
        wait: Duration,
        offered: &mut Offered,
    ) {
        debug_assert_eq!(sinces.len(), results.len());
        let mut admitted = false;
        let mut again = Vec::new();
        for (since, result) in sinces.into_iter().zip(results) {
            match result {
                Ok(()) => {
                    admitted = true;
                    offered.returned += 1;
                    if since.is_some() {
                        offered.returned_again += 1;
                    }
                }
                Err(Refused { error, frame }) if error.passes() => {
                    let fresh = since.is_none();
                    let kept = Kept {
                        since: since.unwrap_or(now),
                        frame,
                    };
                    again.push((fresh, kept));
                }
                Err(Refused {
                    error: SendError::NotConnected,
                    ..
                }) => {
                    // The connection has gone, and with it everything
                    // owed on it: what is still kept goes too.
                    offered.unreturnable += 1 + again.len() as u64;
                    again.clear();
                    self.forget(connection, offered);
                }
                Err(_) => offered.unreturnable += 1,
            }
        }
        let wait = if admitted { FIRST_WAIT } else { wait };
        for (fresh, kept) in again {
            if self.frames + 1 > self.max_frames || self.bytes + kept.frame.len() > self.max_bytes {
                offered.unreturnable += 1;
                continue;
            }
            if fresh {
                offered.kept += 1;
            }
            self.frames += 1;
            self.bytes += kept.frame.len();
            self.owed
                .entry(connection)
                .or_insert_with(|| Owed {
                    frames: VecDeque::new(),
                    due: now + wait,
                    wait,
                })
                .frames
                .push_back(kept);
        }
    }

    /// Keep `kept` at the back of `connection`'s frames, if the bounds
    /// allow; the connection already has frames kept.
    fn keep_back(&mut self, connection: u64, kept: Kept, now: Instant, offered: &mut Offered) {
        if self.frames + 1 > self.max_frames || self.bytes + kept.frame.len() > self.max_bytes {
            offered.unreturnable += 1;
            return;
        }
        let owed = self.owed.entry(connection).or_insert_with(|| Owed {
            frames: VecDeque::new(),
            due: now + FIRST_WAIT,
            wait: FIRST_WAIT,
        });
        self.frames += 1;
        self.bytes += kept.frame.len();
        offered.kept += 1;
        owed.frames.push_back(kept);
    }

    /// Let go of everything kept for `connection`.
    fn forget(&mut self, connection: u64, offered: &mut Offered) {
        if let Some(owed) = self.owed.remove(&connection) {
            for kept in owed.frames {
                self.frames -= 1;
                self.bytes -= kept.frame.len();
                offered.unreturnable += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coord_transport::Lane;

    const HOLD: Duration = Duration::from_secs(2);

    fn full() -> SendError {
        SendError::QueueFull { lane: Lane::Unary }
    }

    /// A transport whose queue for one connection takes `room` frames
    /// and refuses the rest for a full queue, recording what it took.
    struct Lane1 {
        room: usize,
        taken: Vec<Vec<u8>>,
        offers: usize,
    }

    impl Lane1 {
        fn new(room: usize) -> Self {
            Lane1 {
                room,
                taken: Vec::new(),
                offers: 0,
            }
        }

        fn offer(&mut self, frames: Vec<Vec<u8>>) -> Vec<Result<(), Refused>> {
            self.offers += 1;
            frames
                .into_iter()
                .map(|frame| {
                    if self.room > 0 {
                        self.room -= 1;
                        self.taken.push(frame);
                        Ok(())
                    } else {
                        Err(Refused {
                            error: full(),
                            frame,
                        })
                    }
                })
                .collect()
        }
    }

    fn frames(range: std::ops::Range<u8>) -> Vec<Vec<u8>> {
        range.map(|i| vec![i; 4]).collect()
    }

    /// A refused frame is kept and leaves once the queue drains, and the
    /// frames made meanwhile leave behind it, in the order they were
    /// made, without being offered before it.
    #[test]
    fn a_frame_refused_for_a_full_queue_is_kept_and_leaves_first_once_it_drains() {
        let t0 = Instant::now();
        let mut kept = Unreturned::new(64, 1 << 20, HOLD);
        let mut lane = Lane1::new(2);
        let first = kept.send(7, frames(0..4), t0, |f| lane.offer(f));
        assert_eq!(
            first,
            Offered {
                handoffs: 1,
                returned: 2,
                kept: 2,
                ..Offered::default()
            }
        );
        assert_eq!(kept.frames(), 2);
        assert_eq!(kept.next_due(), Some(t0 + FIRST_WAIT));
        // Made while two are kept: queued behind them, not offered.
        let behind = kept.send(7, frames(4..6), t0, |f| lane.offer(f));
        assert_eq!(behind.handoffs, 0);
        assert_eq!(behind.kept, 2);
        assert_eq!(lane.offers, 1);
        // Not due yet: nothing is offered.
        let early = kept.offer_due(t0 + FIRST_WAIT / 2, |_, f| lane.offer(f));
        assert_eq!(early, Offered::default());
        // The queue drains, and at the due time all four leave, in order.
        lane.room = 8;
        let again = kept.offer_due(t0 + FIRST_WAIT, |c, f| {
            assert_eq!(c, 7);
            lane.offer(f)
        });
        assert_eq!(again.returned, 4);
        assert_eq!(again.returned_again, 4);
        assert_eq!(again.unreturnable, 0);
        assert_eq!(lane.taken, frames(0..6));
        assert!(kept.is_empty());
        assert_eq!((kept.frames(), kept.bytes()), (0, 0));
        assert_eq!(kept.next_due(), None);
    }

    /// An offer that admits nothing doubles the wait, up to the longest;
    /// one that admits anything starts it again from the first.
    #[test]
    fn the_wait_doubles_while_nothing_is_admitted_and_starts_again_when_something_is() {
        let t0 = Instant::now();
        let mut kept = Unreturned::new(64, 1 << 20, Duration::from_secs(60));
        let mut lane = Lane1::new(0);
        kept.send(1, frames(0..3), t0, |f| lane.offer(f));
        let mut now = t0;
        let mut waits = Vec::new();
        for _ in 0..6 {
            let due = kept.next_due().unwrap();
            waits.push(due - now);
            now = due;
            let offered = kept.offer_due(now, |_, f| lane.offer(f));
            assert_eq!(offered.returned, 0);
        }
        let ms = |d: Duration| d.as_millis() as u64;
        assert_eq!(
            waits.iter().map(|w| ms(*w)).collect::<Vec<_>>(),
            vec![25, 50, 100, 200, 200, 200]
        );
        // One frame admitted: the next wait is the first again.
        lane.room = 1;
        let due = kept.next_due().unwrap();
        let offered = kept.offer_due(due, |_, f| lane.offer(f));
        assert_eq!(offered.returned, 1);
        assert_eq!(kept.next_due(), Some(due + FIRST_WAIT));
        assert_eq!(kept.frames(), 2);
    }

    /// A frame refused for good is let go, and a connection that has
    /// gone takes everything kept for it with it.
    #[test]
    fn a_frame_refused_for_good_is_let_go_and_a_gone_connection_takes_its_kept_frames() {
        let t0 = Instant::now();
        let mut kept = Unreturned::new(64, 1 << 20, HOLD);
        let too_large = kept.send(3, frames(0..1), t0, |f| {
            f.into_iter()
                .map(|frame| {
                    Err(Refused {
                        error: SendError::TooLarge { bytes: 4, limit: 1 },
                        frame,
                    })
                })
                .collect()
        });
        assert_eq!(too_large.unreturnable, 1);
        assert!(kept.is_empty());
        let mut lane = Lane1::new(0);
        kept.send(3, frames(0..3), t0, |f| lane.offer(f));
        kept.send(3, frames(3..4), t0, |f| lane.offer(f));
        assert_eq!(kept.frames(), 4);
        let gone = kept.offer_due(t0 + FIRST_WAIT, |_, f| {
            f.into_iter()
                .map(|frame| {
                    Err(Refused {
                        error: SendError::NotConnected,
                        frame,
                    })
                })
                .collect()
        });
        assert_eq!(gone.unreturnable, 4);
        assert_eq!(gone.returned, 0);
        assert!(kept.is_empty());
        assert_eq!((kept.frames(), kept.bytes()), (0, 0));
    }

    /// What would pass a bound is let go rather than kept, and what has
    /// been kept longer than the hold is let go when its turn comes,
    /// without being offered.
    #[test]
    fn the_bounds_and_the_hold_let_go_of_what_is_past_them() {
        let t0 = Instant::now();
        let mut kept = Unreturned::new(3, 1 << 20, HOLD);
        let mut lane = Lane1::new(0);
        let offered = kept.send(1, frames(0..5), t0, |f| lane.offer(f));
        assert_eq!((offered.kept, offered.unreturnable), (3, 2));
        assert_eq!(kept.frames(), 3);
        let behind = kept.send(1, frames(5..6), t0, |f| lane.offer(f));
        assert_eq!((behind.kept, behind.unreturnable), (0, 1));
        // By bytes: 4 bytes a frame, room for two.
        let mut by_bytes = Unreturned::new(64, 8, HOLD);
        let offered = by_bytes.send(1, frames(0..3), t0, |f| lane.offer(f));
        assert_eq!((offered.kept, offered.unreturnable), (2, 1));
        assert_eq!((by_bytes.frames(), by_bytes.bytes()), (2, 8));
        // Past the hold: let go unoffered.
        let offers = lane.offers;
        let late = kept.offer_due(t0 + HOLD, |_, f| lane.offer(f));
        assert_eq!(late.unreturnable, 3);
        assert_eq!(late.handoffs, 0);
        assert_eq!(lane.offers, offers);
        assert!(kept.is_empty());
    }

    /// Connections are kept apart: one whose queue is full holds back
    /// nothing owed on another.
    #[test]
    fn one_connections_full_queue_holds_back_nothing_owed_on_another() {
        let t0 = Instant::now();
        let mut kept = Unreturned::new(64, 1 << 20, HOLD);
        let mut full_lane = Lane1::new(0);
        let mut free_lane = Lane1::new(8);
        kept.send(1, frames(0..2), t0, |f| full_lane.offer(f));
        let other = kept.send(2, frames(2..4), t0, |f| free_lane.offer(f));
        assert_eq!(other.returned, 2);
        assert_eq!(free_lane.taken, frames(2..4));
        assert_eq!(kept.frames(), 2);
    }
}
