//! Leaf renewal, bounded overlap and warm-session deadlines (task-58;
//! design Sections 10.4, 20.4).
//!
//! The property every case here is about: a credential's expiry is a
//! hard edge, and everything that makes renewal survivable happens
//! *before* it. There is no outcome that means "serve on an expired
//! leaf", and the tests are mostly about the shape of the window that
//! makes not needing one realistic.

use coord_node_issuer::{Leaf, Renewal, RenewalPolicy};

const HOUR: u64 = 3600;

fn leaf(issued_at: u64, lifetime: u64) -> Leaf {
    Leaf {
        issued_at,
        expires_at: issued_at + lifetime,
    }
}

fn policy() -> RenewalPolicy {
    RenewalPolicy {
        renew_at_num: 2,
        renew_at_den: 3,
        jitter_secs: HOUR,
        max_overlap_secs: 600,
        max_session_secs: 12 * HOUR,
    }
}

/// Renewal turns due with most of the lifetime left, and the node keeps
/// serving and retrying until the deadline.
#[test]
fn a_leaf_becomes_due_well_before_it_expires_and_stays_valid_meanwhile() {
    let p = policy();
    let l = leaf(1_000, 24 * HOUR);

    // Not due at the start.
    assert_eq!(
        p.decide(&l, 1_000, 0),
        Renewal::Wait {
            until: 1_000 + 16 * HOUR
        }
    );
    // Due at two thirds, and still valid: the node has eight hours of
    // retries left, which is the whole point of renewing early.
    assert_eq!(p.decide(&l, 1_000 + 16 * HOUR, 0), Renewal::Due);
    assert_eq!(p.decide(&l, 1_000 + 23 * HOUR, 0), Renewal::Due);
    assert_ne!(p.decide(&l, 1_000 + 23 * HOUR, 0), Renewal::Expired);
    // And at the deadline it is over. There is no third answer.
    assert_eq!(p.decide(&l, 1_000 + 24 * HOUR, 0), Renewal::Expired);
    assert_eq!(p.decide(&l, u64::MAX, 0), Renewal::Expired);
}

/// The jitter spreads the cluster, is bounded, and is the same on every
/// run for one node.
#[test]
fn jitter_spreads_the_cluster_within_its_bound_and_does_not_move_on_a_restart() {
    let p = policy();
    let l = leaf(0, 24 * HOUR);
    let base = 16 * HOUR;
    let mut seen = std::collections::BTreeSet::new();
    for seed in 0..64u64 {
        let due = p.due_at(&l, seed * 97);
        assert!(
            (base..base + HOUR).contains(&due),
            "due {due} outside the jitter window"
        );
        seen.insert(due);
        // Asking twice is asking once: a node that re-derived its
        // jitter after a restart would move its own deadline, and a
        // cluster that all restarted together would resynchronize
        // exactly when it must not.
        assert_eq!(p.due_at(&l, seed * 97), due);
    }
    assert!(
        seen.len() > 32,
        "the jitter barely spread the cluster: {} distinct instants",
        seen.len()
    );

    // A jitter that would push the due point past the expiry cannot: a
    // node that never turned due would never renew.
    let wide = RenewalPolicy {
        jitter_secs: 100 * HOUR,
        ..p
    };
    let due = wide.due_at(&l, 99 * HOUR);
    assert!(due < l.expires_at, "due {due} is not before the expiry");
    assert_eq!(wide.decide(&l, due, 99 * HOUR), Renewal::Due);
}

/// A replaced leaf is retired by the overlap window or its own expiry,
/// whichever comes first.
#[test]
fn the_overlap_during_a_rotation_is_bounded_at_both_ends() {
    let p = policy();

    // The window ends first: a key being rotated away from does not
    // stay usable for the rest of its natural life.
    let long = leaf(0, 24 * HOUR);
    let replaced_at = 16 * HOUR;
    assert_eq!(p.retire_at(&long, replaced_at), replaced_at + 600);
    assert!(p.accepts_replaced(&long, replaced_at, replaced_at + 599));
    assert!(!p.accepts_replaced(&long, replaced_at, replaced_at + 600));
    assert!(!p.accepts_replaced(&long, replaced_at, replaced_at + 601));

    // The expiry ends first: a generous window never extends a
    // credential past its own deadline.
    let nearly_over = leaf(0, 16 * HOUR + 60);
    assert_eq!(
        p.retire_at(&nearly_over, replaced_at),
        nearly_over.expires_at
    );
    assert!(!p.accepts_replaced(&nearly_over, replaced_at, nearly_over.expires_at));

    // A replacement that arrives after the old leaf already expired
    // retires it at the expiry, not later.
    assert_eq!(
        p.retire_at(&nearly_over, nearly_over.expires_at + HOUR),
        nearly_over.expires_at
    );
}

/// A warm session's deadline is the earlier of the credential's expiry
/// and the connection cap, and a renewal does not move it.
#[test]
fn a_renewal_does_not_extend_a_session_bound_under_the_leaf_it_replaced() {
    let p = policy();

    // The cap binds on a long credential.
    let long = leaf(0, 48 * HOUR);
    assert_eq!(p.session_deadline(&long, 0), 12 * HOUR);
    // The expiry binds on a short one: a session opened near the end of
    // a credential ends with it, cap or no cap.
    let short = leaf(0, 24 * HOUR);
    assert_eq!(p.session_deadline(&short, 20 * HOUR), short.expires_at);

    // The renewal. A session bound under the old leaf keeps the old
    // leaf's deadline; sessions bound after the renewal get the new
    // one. Nothing lets the first run on.
    let opened_at = 20 * HOUR;
    let before = p.session_deadline(&short, opened_at);
    let renewed = leaf(22 * HOUR, 24 * HOUR);
    assert_eq!(p.session_deadline(&short, opened_at), before);
    assert!(
        p.session_deadline(&renewed, 23 * HOUR) > before,
        "the new credential should carry a later deadline than the one it replaced"
    );
}

/// Degenerate policies do not produce a leaf that never renews or one
/// that renews before it exists.
#[test]
fn a_degenerate_policy_still_renews_inside_the_lifetime() {
    let l = leaf(1_000, HOUR);
    for p in [
        RenewalPolicy {
            renew_at_num: 0,
            ..policy()
        },
        RenewalPolicy {
            renew_at_den: 0,
            ..policy()
        },
        RenewalPolicy {
            renew_at_num: 99,
            renew_at_den: 1,
            ..policy()
        },
        RenewalPolicy {
            jitter_secs: 0,
            ..policy()
        },
    ] {
        let due = p.due_at(&l, 12_345);
        assert!(
            (l.issued_at..l.expires_at).contains(&due),
            "due {due} outside the leaf's own window for {p:?}"
        );
        assert_eq!(p.decide(&l, l.expires_at, 12_345), Renewal::Expired);
    }
}

/// A prolonged issuer outage is survivable up to the deadline and not
/// one second past it (task-58; design Section 10.4).
///
/// The renewal window is what makes an issuer a non-critical dependency
/// for a running cluster: the node turns due with a third of its
/// lifetime left and keeps serving and retrying the whole way down. What
/// it does not have is an escape hatch. At the deadline the credential
/// is not valid, and "the issuer is down" is a reason the node cannot
/// renew, never a reason it may carry on without having renewed --
/// availability is bought by the window, and paying for it at the
/// deadline would mean expiry never meant anything.
#[test]
fn an_issuer_outage_is_survivable_until_the_deadline_and_never_past_it() {
    let p = policy();
    let l = leaf(0, 24 * HOUR);
    let seed = 7 * 97;
    let due = p.due_at(&l, seed);

    // The outage begins before the node is even due and lasts past the
    // deadline. Every hour of it, the answer is the same one: renew,
    // and the credential is still good.
    let mut retries = 0;
    let mut hour = due;
    while hour < l.expires_at {
        assert_eq!(
            p.decide(&l, hour, seed),
            Renewal::Due,
            "the node stopped trying at {hour}"
        );
        retries += 1;
        hour += HOUR;
    }
    assert!(
        retries >= 7,
        "the window left only {retries} hourly retries; an outage has to be survivable"
    );

    // And then it is over, and stays over however long the outage runs.
    for at in [
        l.expires_at,
        l.expires_at + 1,
        l.expires_at + 24 * HOUR,
        u64::MAX,
    ] {
        assert_eq!(
            p.decide(&l, at, seed),
            Renewal::Expired,
            "an expired credential was usable at {at}"
        );
    }

    // A session bound before the outage does not outlive the credential
    // either: the deadline it carries is the credential's, so the
    // outage cannot be ridden out on a warm connection.
    assert!(p.session_deadline(&l, due) <= l.expires_at);

    // Once the issuer comes back, the new leaf resets the whole window
    // -- there is no penalty carried forward from the outage.
    let renewed = leaf(l.expires_at - HOUR, 24 * HOUR);
    assert_eq!(
        p.decide(&renewed, l.expires_at, seed),
        Renewal::Wait {
            until: p.due_at(&renewed, seed)
        }
    );
}
