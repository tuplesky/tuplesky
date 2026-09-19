//! Acceptance for a co-located voter's ingress (task-j08).
//!
//! The local route exists so a frontend need not dial a voter in its own
//! process. These tests hold the two things that stops it from becoming
//! a shortcut: it is a capability the committed configuration grants,
//! and it is an accounted queue rather than a free one.

use coord_daemon::mailbox::{Ingress, IngressBudget};
use coord_daemon::{LocalIngress, Saturated};
use coord_membership::genesis::{GenesisManifest, VoterSeed};
use coord_membership::membership::Membership;
use coord_types::ids::{DomainId, ReplicaId, ReplicaIncarnation};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn b64url(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(A[((n >> (18 - 6 * i)) & 0x3f) as usize] as char);
            }
        }
    }
    out
}

const DOMAIN: DomainId = DomainId([0x22; 16]);

fn replica(n: u8) -> ReplicaId {
    ReplicaId([n; 16])
}

fn membership() -> Membership {
    let manifest = GenesisManifest {
        cluster: hex(&[0x11; 16]),
        domain: hex(&DOMAIN.0),
        epoch: 1,
        voters: (1u8..=3)
            .map(|n| VoterSeed {
                node: hex(&[n; 16]),
                incarnation: u64::from(n) * 7,
                public_key: b64url(&[n; 32]),
            })
            .collect(),
        issuer_roots: vec![b64url(&[0xca; 8])],
        wif_rules: vec![serde_json::json!({ "issuer": "test" })],
        admin: hex(&[0xa; 16]),
        protocol_version: 1,
    };
    Membership::from_genesis(&manifest).expect("membership")
}

/// A process gets a voter's ingress because the configuration says it is
/// that voter, not because something named a replica id. A
/// frontend-only or observer-only process asking for one gets nothing to
/// hold.
#[test]
fn an_ingress_exists_only_for_a_committed_voter() {
    let membership = membership();

    assert!(
        Ingress::new(&membership, replica(9), IngressBudget::default()).is_none(),
        "a replica the configuration does not name as a voter has no ingress here"
    );
    assert!(Ingress::new(&membership, replica(2), IngressBudget::default()).is_some());
}

/// The incarnation an ingress carries is the configuration's. There is
/// no constructor that takes one, so a runtime cannot claim a generation
/// of a node the cluster did not commit to.
#[test]
fn the_incarnation_is_the_configurations_not_the_runtimes() {
    let membership = membership();
    let ingress = Ingress::new(&membership, replica(3), IngressBudget::default()).expect("voter");

    assert_eq!(ingress.replica(), replica(3));
    assert_eq!(
        ingress.incarnation(),
        ReplicaIncarnation::new(21).unwrap(),
        "voter 3 is committed at incarnation 21"
    );
    assert_eq!(LocalIngress::domain(&ingress.route()), DOMAIN);
}

/// Local is not free. The queue refuses once its frame bound is reached
/// rather than growing into the process's memory, and the refusal is
/// counted so a voter falling behind is visible.
#[test]
fn a_local_ingress_refuses_by_frame_count_rather_than_growing() {
    let membership = membership();
    let ingress = Ingress::new(
        &membership,
        replica(1),
        IngressBudget {
            frames: 2,
            bytes: 1 << 20,
        },
    )
    .expect("voter");
    let route = ingress.route();

    assert_eq!(route.offer(b"one"), Ok(()));
    assert_eq!(route.offer(b"two"), Ok(()));
    assert_eq!(route.offer(b"three"), Err(Saturated));
    assert_eq!(ingress.depth(), 2);
    assert_eq!(ingress.counts(), (2, 1), "two accepted, one refused");
}

/// The byte bound applies too: a few large frames fill an ingress that
/// is nowhere near its frame count, which is the bound that matters when
/// the queue's cost is memory rather than turns.
#[test]
fn a_local_ingress_refuses_by_bytes_as_well() {
    let membership = membership();
    let ingress = Ingress::new(
        &membership,
        replica(1),
        IngressBudget {
            frames: 1024,
            bytes: 16,
        },
    )
    .expect("voter");
    let route = ingress.route();

    assert_eq!(route.offer(&[0u8; 10]), Ok(()));
    assert_eq!(
        route.offer(&[0u8; 10]),
        Err(Saturated),
        "20 bytes would exceed the 16-byte bound"
    );
    assert_eq!(ingress.bytes(), 10);
    // A frame larger than the whole budget is refused rather than
    // admitted as a special case: the bound means one thing.
    let empty = Ingress::new(
        &membership,
        replica(1),
        IngressBudget {
            frames: 1024,
            bytes: 16,
        },
    )
    .expect("voter");
    assert_eq!(empty.route().offer(&[0u8; 17]), Err(Saturated));
    assert_eq!(empty.depth(), 0);
}

/// The voter takes its frames in the order the frontend offered them,
/// and taking them gives the room back -- otherwise the ingress would
/// fill once and never accept again.
#[test]
fn taking_frames_returns_them_in_order_and_frees_the_room() {
    let membership = membership();
    let ingress = Ingress::new(
        &membership,
        replica(1),
        IngressBudget {
            frames: 2,
            bytes: 1 << 20,
        },
    )
    .expect("voter");
    let route = ingress.route();
    route.offer(b"first").expect("room");
    route.offer(b"second").expect("room");
    assert_eq!(route.offer(b"third"), Err(Saturated));

    assert_eq!(ingress.take(1), vec![b"first".to_vec()], "oldest first");
    assert_eq!(ingress.bytes(), b"second".len());
    assert_eq!(route.offer(b"third"), Ok(()), "the taken slot came back");
    assert_eq!(ingress.take(8), vec![b"second".to_vec(), b"third".to_vec()]);
    assert_eq!(ingress.depth(), 0);
    assert_eq!(ingress.bytes(), 0);
}

/// A drain is bounded. The voter runtime interleaves ingress with its
/// own timers, recovery and storage work, so a take that ran to
/// exhaustion would let a busy frontend decide how long the voter went
/// without doing any of them.
#[test]
fn a_take_is_bounded_so_the_voter_can_do_its_own_work() {
    let membership = membership();
    let ingress = Ingress::new(&membership, replica(1), IngressBudget::default()).expect("voter");
    let route = ingress.route();
    for n in 0..10u8 {
        route.offer(&[n]).expect("room");
    }

    let taken = ingress.take(3);

    assert_eq!(taken.len(), 3, "the budget was honoured, not the backlog");
    assert_eq!(ingress.depth(), 7, "the rest waits for the next turn");
}
