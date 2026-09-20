//! Leaf renewal, bounded overlap and warm-session deadlines (task-58;
//! design Sections 10.4, 20.4).
//!
//! A node's certificate is short-lived on purpose, which makes renewal
//! an ordinary, continuous part of running rather than an incident. The
//! rules here are the ones that keep it from becoming one:
//!
//! * **Renew before the deadline, not at it.** [`RenewalPolicy::decide`]
//!   turns due at a fraction of the lifetime, with jitter, so a cluster
//!   does not renew in lockstep and a node that cannot reach the issuer
//!   has the rest of the lifetime to keep trying.
//! * **Expiry is never bypassed.** An expired leaf is
//!   [`Renewal::Expired`], and there is no variant that means "carry on
//!   anyway". Availability is what the early renewal window is for;
//!   past the deadline the credential is not valid and no amount of
//!   wanting to serve makes it so.
//! * **Overlap is bounded.** During a rotation two leaves for one node
//!   are valid at once. [`RenewalPolicy::retire_at`] says when the
//!   replaced one stops being accepted -- the earlier of its own expiry
//!   and a bounded window after the replacement arrived -- so a key
//!   being rotated away from is not usable for the rest of its natural
//!   life.
//! * **A renewal does not extend a warm session.**
//!   [`RenewalPolicy::session_deadline`] is the earlier of the
//!   credential's expiry and the connection-lifetime cap, computed from
//!   the credential the session was *bound* under. A session that wants
//!   to outlive it rebinds; nothing here quietly lets it run on.
//!
//! None of this decides membership. A renewal keeps the node's
//! generation and key, and a generation or key change is a committed
//! configuration transition -- `coord_membership::Membership` says which
//! is which.

/// One issued leaf, as the holder sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Leaf {
    /// When it became valid (unix seconds).
    pub issued_at: u64,
    /// When it stops being valid (unix seconds).
    pub expires_at: u64,
}

impl Leaf {
    /// Its whole lifetime in seconds, zero if it never was valid.
    pub const fn lifetime(&self) -> u64 {
        self.expires_at.saturating_sub(self.issued_at)
    }
}

/// When a node renews, and how long a replaced leaf stays accepted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RenewalPolicy {
    /// Renew once this fraction of the lifetime is spent (numerator).
    pub renew_at_num: u64,
    /// Renew once this fraction of the lifetime is spent (denominator).
    pub renew_at_den: u64,
    /// Jitter window in seconds, spread deterministically per node.
    pub jitter_secs: u64,
    /// The longest a replaced leaf stays accepted after its
    /// replacement arrives.
    pub max_overlap_secs: u64,
    /// The longest an authenticated connection may live, whatever its
    /// credential's expiry.
    pub max_session_secs: u64,
}

impl Default for RenewalPolicy {
    /// Renew at two thirds of the lifetime, with an hour of jitter, a
    /// ten-minute overlap and a twelve-hour session cap.
    fn default() -> Self {
        RenewalPolicy {
            renew_at_num: 2,
            renew_at_den: 3,
            jitter_secs: 3600,
            max_overlap_secs: 600,
            max_session_secs: 12 * 3600,
        }
    }
}

/// What to do about a leaf right now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Renewal {
    /// Nothing yet; come back at this instant.
    Wait {
        /// When the leaf becomes due.
        until: u64,
    },
    /// Renew now. Still valid, so the node keeps serving while it
    /// tries, and keeps trying for the rest of the lifetime.
    Due,
    /// The leaf is no longer valid. There is deliberately no variant
    /// that means "serve anyway".
    Expired,
}

impl RenewalPolicy {
    /// The instant `leaf` becomes due, with this node's jitter.
    ///
    /// The jitter is derived from the holder's own identity rather than
    /// drawn at random: a node that re-derived it after a restart would
    /// move its own deadline, and a cluster whose nodes all restarted
    /// together would resynchronize exactly when it must not.
    pub const fn due_at(&self, leaf: &Leaf, jitter_seed: u64) -> u64 {
        let lifetime = leaf.lifetime();
        let den = if self.renew_at_den == 0 {
            1
        } else {
            self.renew_at_den
        };
        let spent = lifetime / den * self.renew_at_num;
        let jitter = if self.jitter_secs == 0 {
            0
        } else {
            jitter_seed % self.jitter_secs
        };
        // Never past the expiry: a due point beyond it would mean the
        // node never renews at all.
        let due = leaf.issued_at.saturating_add(spent).saturating_add(jitter);
        if due >= leaf.expires_at {
            leaf.expires_at.saturating_sub(1)
        } else {
            due
        }
    }

    /// What to do about `leaf` at `now`.
    pub const fn decide(&self, leaf: &Leaf, now: u64, jitter_seed: u64) -> Renewal {
        if now >= leaf.expires_at {
            return Renewal::Expired;
        }
        let due = self.due_at(leaf, jitter_seed);
        if now >= due {
            Renewal::Due
        } else {
            Renewal::Wait { until: due }
        }
    }

    /// When the replaced leaf stops being accepted, given that its
    /// replacement arrived at `replaced_at`.
    ///
    /// The earlier of its own expiry and the overlap window. Both
    /// bounds matter: without the window a key being rotated away from
    /// stays usable for the rest of its natural life, and without the
    /// expiry a long window would extend one.
    pub const fn retire_at(&self, replaced: &Leaf, replaced_at: u64) -> u64 {
        let window = replaced_at.saturating_add(self.max_overlap_secs);
        if window < replaced.expires_at {
            window
        } else {
            replaced.expires_at
        }
    }

    /// Whether `replaced` is still accepted at `now`.
    pub const fn accepts_replaced(&self, replaced: &Leaf, replaced_at: u64, now: u64) -> bool {
        now < self.retire_at(replaced, replaced_at)
    }

    /// The deadline of a session opened at `opened_at` under `leaf`.
    ///
    /// The earlier of the credential's expiry and the session cap, and
    /// it is computed from the credential the session was bound under.
    /// A renewal produces a different leaf and therefore a different
    /// deadline for sessions bound afterwards; it does not move this
    /// one.
    pub const fn session_deadline(&self, leaf: &Leaf, opened_at: u64) -> u64 {
        let capped = opened_at.saturating_add(self.max_session_secs);
        if capped < leaf.expires_at {
            capped
        } else {
            leaf.expires_at
        }
    }
}
