//! SCRATCH (ceilings, never merged): switches that skip or stub work on
//! the hot path, and timers for the regions that can't be stubbed without
//! writing the real thing, so each row's ceiling is measured before the
//! row is built (#185, the lead's 05:35 method). Correctness is not kept
//! with a switch on.
//!
//! The daemon sets the switches from the environment and the clock from
//! `Instant`; this crate is `no_std` and reads neither.
//!
//! - [`l2`] (`COORDD_X_L2`): a follower adopts once a proposal frame, not
//!   once an entry, and learns once a turn after storage events and
//!   executions, not once each (Section 9.8 step 1's once-a-frame rows).
//! - [`noc`] (`COORDD_X_NOC`): a follower does not check a relayed
//!   payload's identity again, and its voter does not decode a proposal
//!   frame before the follower does (class C at followers, rows 16 and 11).
//! - [`bar`] (`COORDD_X_BAR`): the leader's own vote requires its own
//!   barrier alone, not every unsettled one (row 19).
//! - [`prop`] (`COORDD_X_PROP`): the leader's `propose` skips the canonical
//!   re-encode check and keeps no encoded rows in the `Proposal` (what the
//!   propose-copies branch leaves, rows 13 and 22).
//! - [`timing`] (`COORDD_X_TIME`): the regions in [`Region`] are timed.
#![allow(unsafe_code)]

use alloc::string::String;
use core::fmt::Write as _;
use core::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

static L2: AtomicBool = AtomicBool::new(false);
static NOC: AtomicBool = AtomicBool::new(false);
static BAR: AtomicBool = AtomicBool::new(false);
static PROP: AtomicBool = AtomicBool::new(false);
static TIME: AtomicBool = AtomicBool::new(false);
static IN_FRAME: AtomicBool = AtomicBool::new(false);
static DEFER_LEARN: AtomicBool = AtomicBool::new(false);
static LEARN_OWED: AtomicBool = AtomicBool::new(false);
static CLOCK: AtomicUsize = AtomicUsize::new(0);

/// Set the switches.
pub fn set(l2: bool, noc: bool, bar: bool, prop: bool, time: bool) {
    L2.store(l2, Ordering::Relaxed);
    NOC.store(noc, Ordering::Relaxed);
    BAR.store(bar, Ordering::Relaxed);
    PROP.store(prop, Ordering::Relaxed);
    TIME.store(time, Ordering::Relaxed);
}

/// Section 9.8 step 1's once-a-frame rows, stubbed.
pub fn l2() -> bool {
    L2.load(Ordering::Relaxed)
}

/// Class C at followers, stubbed.
pub fn noc() -> bool {
    NOC.load(Ordering::Relaxed)
}

/// The leader's own vote requires its own barrier alone.
pub fn bar() -> bool {
    BAR.load(Ordering::Relaxed)
}

/// What the propose-copies branch leaves, stubbed.
pub fn prop() -> bool {
    PROP.load(Ordering::Relaxed)
}

/// Whether the regions are timed.
pub fn timing() -> bool {
    TIME.load(Ordering::Relaxed)
}

/// Inside a proposal frame's entries (with [`l2`]).
pub fn in_frame() -> bool {
    IN_FRAME.load(Ordering::Relaxed)
}

/// Mark a proposal frame's entries begun or done.
pub fn set_in_frame(on: bool) {
    IN_FRAME.store(on, Ordering::Relaxed);
}

/// Learning is put off to the turn's end (with [`l2`]).
pub fn defer_learn() -> bool {
    DEFER_LEARN.load(Ordering::Relaxed)
}

/// Put learning off, or stop; returns what it was.
pub fn set_defer_learn(on: bool) -> bool {
    DEFER_LEARN.swap(on, Ordering::Relaxed)
}

/// Note a learn put off.
pub fn owe_learn() {
    LEARN_OWED.store(true, Ordering::Relaxed);
}

/// Whether a learn was put off.
pub fn learn_owed() -> bool {
    LEARN_OWED.load(Ordering::Relaxed)
}

/// Take the owed learn.
pub fn take_learn_owed() -> bool {
    LEARN_OWED.swap(false, Ordering::Relaxed)
}

/// The clock the timers read, in nanoseconds.
pub fn set_clock(clock: fn() -> u64) {
    CLOCK.store(clock as usize, Ordering::Relaxed);
}

fn now() -> u64 {
    let p = CLOCK.load(Ordering::Relaxed);
    if p == 0 {
        return 0;
    }
    // SAFETY: only `set_clock` stores here, and it stores a `fn() -> u64`.
    let clock = unsafe { core::mem::transmute::<usize, fn() -> u64>(p) };
    clock()
}

/// A timed region.
#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub enum Region {
    /// A follower's `on_proposal_frame`, whole.
    FFrame,
    /// A follower's `advance_pending`, whole.
    FAdvance,
    /// A follower's `learn`.
    FLearn,
    /// A follower's `on_storage_event` (row 2's settle, with its learn).
    FSettle,
    /// A follower's `forget_history` (row 4).
    FForget,
    /// A follower's `Learner::established` in `applied` (row 6).
    FEstablished,
    /// A follower's `publish_to_leader_and_frontend` (row 18).
    FVoteOut,
    /// A follower's identity check of a relayed payload (row 16).
    FIdentity,
    /// A follower's adoption rows encoded (row 7).
    FRows,
    /// A follower's decode of a peer frame (row 11's second).
    FDecode,
    /// The voter's decode of a peer frame (row 11's first), any role.
    VDecode,
    /// The leader's `on_storage` (row 4).
    LSettle,
    /// The leader's `adopt_own` (row 19).
    LAdoptOwn,
    /// The leader's `propose`.
    LPropose,
    /// A journal record sealed, its updates encoded again (row 5).
    Seal,
    /// A frontend's retained-answer check of a request (row 8).
    Retained,
    /// A frontend's re-offers and solicits (row 8).
    Reoffer,
}

const REGIONS: usize = Region::Reoffer as usize + 1;
const NAMES: [&str; REGIONS] = [
    "f_frame",
    "f_advance",
    "f_learn",
    "f_settle",
    "f_forget",
    "f_established",
    "f_vote_out",
    "f_identity",
    "f_rows",
    "f_decode",
    "v_decode",
    "l_settle",
    "l_adopt_own",
    "l_propose",
    "seal",
    "retained",
    "reoffer",
];

#[allow(clippy::declare_interior_mutable_const)]
const ZERO: AtomicU64 = AtomicU64::new(0);
static NS: [AtomicU64; REGIONS] = [ZERO; REGIONS];
static COUNT: [AtomicU64; REGIONS] = [ZERO; REGIONS];

/// A region being timed; it counts when dropped.
#[must_use]
pub struct Timer(usize, u64);

/// Time `region` until the returned timer drops.
pub fn time(region: Region) -> Timer {
    if timing() {
        Timer(region as usize, now())
    } else {
        Timer(region as usize, 0)
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        if self.1 != 0 {
            let spent = now().saturating_sub(self.1);
            NS[self.0].fetch_add(spent, Ordering::Relaxed);
            COUNT[self.0].fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// The regions timed so far: `name=count/ns` each.
pub fn report() -> String {
    let mut out = String::new();
    for (i, name) in NAMES.iter().enumerate() {
        let n = COUNT[i].load(Ordering::Relaxed);
        if n > 0 {
            let _ = write!(out, " {name}={n}/{}", NS[i].load(Ordering::Relaxed));
        }
    }
    out
}

/// The postcard tag of `ProtocolMessage::ProposalFrame`.
pub const PROPOSAL_FRAME_TAG: u8 = 28;

#[cfg(test)]
mod tests {
    #[test]
    fn the_proposal_frame_tag_is_its_variant() {
        let leader = coord_types::ReplicaId([1; 16]);
        let frame = crate::ProtocolMessage::ProposalFrame {
            replica: leader,
            ballot: coord_types::Ballot {
                epoch: coord_types::ConfigurationEpoch::ZERO,
                number: 1,
                leader,
            },
            proposals: alloc::vec::Vec::new(),
        }
        .encode();
        assert_eq!(frame[0], super::PROPOSAL_FRAME_TAG);
    }
}

/// Learning put off while this lives, where `on` (with [`l2`]); what it
/// was comes back when it drops.
pub struct DeferLearn(Option<bool>);

impl DeferLearn {
    /// Put learning off from here, where `on`.
    pub fn new(on: bool) -> Self {
        if on {
            DeferLearn(Some(set_defer_learn(true)))
        } else {
            DeferLearn(None)
        }
    }
}

impl Drop for DeferLearn {
    fn drop(&mut self) {
        if let Some(was) = self.0 {
            set_defer_learn(was);
        }
    }
}

static HOP: AtomicBool = AtomicBool::new(false);

/// A follower's frontend hands off at once after a submission to a
/// remote leader and after an answer from its release (`COORDD_X_HOP`).
pub fn hop() -> bool {
    HOP.load(Ordering::Relaxed)
}

/// Set [`hop`].
pub fn set_hop(on: bool) {
    HOP.store(on, Ordering::Relaxed);
}
