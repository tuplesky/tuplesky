//! Acceptance for the startup sequence and the production genesis store
//! (task-43; design Sections 10.5, 22.1).
//!
//! The order of `Boot -> StorageValidated -> IdentityValidated ->
//! MembershipChecked -> ProtocolRecovered` is a safety property, not
//! documentation: each step rests on the one before it, and the step that
//! writes the pin must never run for a node that has not proved the
//! storage and identity it is pinning against. These tests hold the order
//! and the fail-closed behaviour of every durable-initialization outcome.

use coord_daemon::lifecycle::QuarantineReason;
use coord_daemon::startup::{NodeJournal, Startup, StartupError, StartupPhase, StoreGenesis};
use coord_membership::genesis::{GenesisManifest, VoterSeed};
use coord_membership::init::{InitError, StoreFailure};
use coord_store_api::engine::{LocalEngine, OrderedRead, SnapshotSource, WriteTxn};
use coord_store_api::registry::{Collection, meta_fields};
use coord_store_testkit::model::ModelEngine;

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

/// A manifest whose voters parse. Signature verification is
/// `coord-membership`'s own acceptance; what matters here is that two
/// manifests differing in any field have different digests, which is
/// what the pin compares.
fn manifest(domain: u8) -> GenesisManifest {
    GenesisManifest {
        cluster: hex(&[0x11; 16]),
        domain: hex(&[domain; 16]),
        epoch: 1,
        voters: (1u8..=3)
            .map(|n| VoterSeed {
                node: hex(&[n; 16]),
                incarnation: 1,
                public_key: b64url(&[n; 32]),
            })
            .collect(),
        issuer_roots: vec![b64url(&[0xca; 8])],
        wif_rules: vec![serde_json::json!({ "issuer": "test" })],
        admin: hex(&[0xa; 16]),
        protocol_version: 1,
    }
}

/// This node's journal, as durable initialization sees it.
#[derive(Default)]
struct Journal {
    present: bool,
    unreadable: bool,
    /// Every establish/pin ordering decision the store made, in order.
    log: Vec<&'static str>,
}

impl NodeJournal for Journal {
    fn intact(&self) -> Result<bool, StoreFailure> {
        if self.unreadable {
            return Err(StoreFailure);
        }
        Ok(self.present)
    }
    fn establish(&mut self) -> Result<(), StoreFailure> {
        self.present = true;
        self.log.push("establish");
        Ok(())
    }
}

fn engine() -> ModelEngine {
    ModelEngine::new()
}

fn pinned(engine: &ModelEngine) -> Option<Vec<u8>> {
    engine
        .reader()
        .snapshot()
        .unwrap()
        .get(Collection::MetaV1.id(), meta_fields::GENESIS_DIGEST)
        .unwrap()
}

/// Drive a startup as far as membership, returning the outcome.
fn run_to_membership(
    startup: &mut Startup,
    engine: &mut ModelEngine,
    journal: &mut Journal,
    manifest: &GenesisManifest,
) -> Result<bool, StartupError> {
    startup.storage_validated()?;
    startup.identity_validated()?;
    let mut store = StoreGenesis::new(engine, journal);
    let initialized = startup.check_membership(manifest, &mut store)?;
    Ok(initialized.first_boot)
}

/// A first boot establishes the journal, then pins the manifest, and the
/// pin lands in the store's own meta collection where a later boot reads
/// it back.
#[test]
fn a_first_boot_establishes_the_journal_before_it_pins_the_manifest() {
    let mut engine = engine();
    let mut journal = Journal::default();
    let manifest = manifest(0x22);
    let mut startup = Startup::new();

    assert_eq!(startup.reached(), StartupPhase::Boot);
    assert_eq!(pinned(&engine), None, "nothing is pinned before a boot");

    let first =
        run_to_membership(&mut startup, &mut engine, &mut journal, &manifest).expect("first boot");
    assert!(first, "the first boot reports itself as one");
    assert_eq!(startup.reached(), StartupPhase::MembershipChecked);
    startup.protocol_recovered().expect("recovered");
    assert!(startup.recovered());

    // The journal came first. A node that pinned and then crashed would
    // return pinned with no journal, which its own returning-node check
    // reads as a lost journal.
    assert_eq!(journal.log, vec!["establish"]);
    assert_eq!(
        pinned(&engine).as_deref(),
        Some(&manifest.digest().0[..]),
        "the pin is the manifest's digest, in meta_v1"
    );
}

/// A returning node matches its pin and does not pin again.
#[test]
fn a_returning_node_matches_its_pin() {
    let mut engine = engine();
    let mut journal = Journal::default();
    let manifest = manifest(0x22);
    let first = run_to_membership(&mut Startup::new(), &mut engine, &mut journal, &manifest)
        .expect("first boot");
    assert!(first);

    // A second boot of the same node against the same manifest.
    let mut again = Startup::new();
    let second =
        run_to_membership(&mut again, &mut engine, &mut journal, &manifest).expect("second boot");
    assert!(!second, "a returning node is not a first boot");
    assert_eq!(
        journal.log,
        vec!["establish"],
        "the journal is not recreated"
    );
}

/// A node handed another manifest quarantines. It does not reinitialize,
/// and the digest it was initialized under is left exactly as it was.
#[test]
fn another_manifest_quarantines_rather_than_reinitializing() {
    let mut engine = engine();
    let mut journal = Journal::default();
    let ours = manifest(0x22);
    run_to_membership(&mut Startup::new(), &mut engine, &mut journal, &ours).expect("first boot");
    let before = pinned(&engine);

    let theirs = manifest(0x33);
    assert_ne!(ours.digest(), theirs.digest(), "the manifests differ");
    let error = run_to_membership(&mut Startup::new(), &mut engine, &mut journal, &theirs)
        .expect_err("another manifest is refused");
    assert!(
        matches!(
            error,
            StartupError::Genesis(InitError::DigestMismatch { .. })
        ),
        "{error:?}"
    );
    assert_eq!(error.quarantine_reason(), QuarantineReason::Genesis);
    assert_eq!(pinned(&engine), before, "the pin is not overwritten");
}

/// A pinned node whose journal is gone is a returning voter with no
/// history: it quarantines rather than coming back empty.
#[test]
fn a_pinned_node_without_its_journal_quarantines() {
    let mut engine = engine();
    let mut journal = Journal::default();
    let manifest = manifest(0x22);
    run_to_membership(&mut Startup::new(), &mut engine, &mut journal, &manifest)
        .expect("first boot");

    // The journal is lost between boots; the pin survives.
    journal.present = false;
    let error = run_to_membership(&mut Startup::new(), &mut engine, &mut journal, &manifest)
        .expect_err("a lost journal is refused");
    assert_eq!(error, StartupError::Genesis(InitError::JournalLost));
    assert_eq!(error.quarantine_reason(), QuarantineReason::Genesis);
    assert!(
        pinned(&engine).is_some(),
        "quarantine leaves the durable state alone"
    );
}

/// A damaged digest record is not an uninitialized node. Reading it as
/// absent would re-pin whatever manifest the node was handed next, which
/// is exactly the silent reinitialization the pin exists to prevent.
#[test]
fn a_damaged_pin_is_not_read_as_an_uninitialized_node() {
    let mut engine = engine();
    let mut journal = Journal::default();
    let manifest = manifest(0x22);
    run_to_membership(&mut Startup::new(), &mut engine, &mut journal, &manifest)
        .expect("first boot");

    // Truncate the pinned digest in place.
    let mut txn = engine.begin_write().unwrap();
    txn.put(
        Collection::MetaV1.id(),
        meta_fields::GENESIS_DIGEST,
        &[0u8; 31],
    )
    .unwrap();
    txn.commit_durable().unwrap();

    let other = manifest_of_another_cluster();
    let error = run_to_membership(&mut Startup::new(), &mut engine, &mut journal, &other)
        .expect_err("a damaged pin is refused");
    assert_eq!(error, StartupError::Genesis(InitError::Store));
    assert_eq!(error.quarantine_reason(), QuarantineReason::Genesis);
    assert_eq!(
        pinned(&engine).as_deref(),
        Some(&[0u8; 31][..]),
        "nothing was re-pinned over the damaged record"
    );
}

fn manifest_of_another_cluster() -> GenesisManifest {
    GenesisManifest {
        cluster: hex(&[0x99; 16]),
        ..manifest(0x44)
    }
}

/// An unreadable journal is a store failure, not an answer. It must not
/// be read as "no journal" (which would quarantine for the wrong reason)
/// or as "journal present" (which would let an empty voter serve).
#[test]
fn an_unreadable_journal_is_a_store_failure() {
    let mut engine = engine();
    let mut journal = Journal::default();
    let manifest = manifest(0x22);
    run_to_membership(&mut Startup::new(), &mut engine, &mut journal, &manifest)
        .expect("first boot");

    journal.unreadable = true;
    let error = run_to_membership(&mut Startup::new(), &mut engine, &mut journal, &manifest)
        .expect_err("an unreadable journal is refused");
    assert_eq!(error, StartupError::Genesis(InitError::Store));
}

/// No step may run before the one it rests on. In particular the step
/// that writes the pin refuses out of order *without* writing it: a pin
/// must never be the side effect of a startup that skipped the storage
/// and identity it pins against.
#[test]
fn no_step_may_run_before_the_one_it_rests_on() {
    let manifest = manifest(0x22);

    // Identity before storage.
    let mut startup = Startup::new();
    assert_eq!(
        startup.identity_validated(),
        Err(StartupError::OutOfOrder {
            attempted: StartupPhase::IdentityValidated,
            reached: StartupPhase::Boot,
        })
    );

    // Membership before identity: refused, and nothing is pinned.
    let mut engine = engine();
    let mut journal = Journal::default();
    let mut startup = Startup::new();
    startup.storage_validated().unwrap();
    let error = {
        let mut store = StoreGenesis::new(&mut engine, &mut journal);
        startup
            .check_membership(&manifest, &mut store)
            .expect_err("membership before identity")
    };
    assert_eq!(
        error,
        StartupError::OutOfOrder {
            attempted: StartupPhase::MembershipChecked,
            reached: StartupPhase::StorageValidated,
        }
    );
    assert_eq!(pinned(&engine), None, "a refused step pinned nothing");
    assert!(journal.log.is_empty(), "and established no journal");
    // It is the composition's own fault, so it is not reported as
    // evidence against the disk or the manifest.
    assert_eq!(error.quarantine_reason(), QuarantineReason::Worker);

    // Protocol recovery before membership.
    let mut startup = Startup::new();
    startup.storage_validated().unwrap();
    startup.identity_validated().unwrap();
    assert_eq!(
        startup.protocol_recovered(),
        Err(StartupError::OutOfOrder {
            attempted: StartupPhase::ProtocolRecovered,
            reached: StartupPhase::IdentityValidated,
        })
    );

    // And no step runs twice: a second storage_validated is as out of
    // order as a skipped one.
    let mut startup = Startup::new();
    startup.storage_validated().unwrap();
    assert_eq!(
        startup.storage_validated(),
        Err(StartupError::OutOfOrder {
            attempted: StartupPhase::StorageValidated,
            reached: StartupPhase::StorageValidated,
        })
    );
    assert!(!startup.recovered());
}

/// Storage and protocol failures are a disk quarantine; identity and
/// genesis failures are a genesis quarantine. The distinction is what an
/// operator acts on, so it is pinned here rather than left to a caller.
#[test]
fn each_failure_quarantines_for_the_reason_an_operator_acts_on() {
    assert_eq!(
        StartupError::storage("no such directory").quarantine_reason(),
        QuarantineReason::Disk
    );
    assert_eq!(
        StartupError::protocol("promise row is corrupt").quarantine_reason(),
        QuarantineReason::Disk
    );
    assert_eq!(
        StartupError::identity("trust bundle is empty").quarantine_reason(),
        QuarantineReason::Genesis
    );
    assert_eq!(
        StartupError::Genesis(InitError::Store).quarantine_reason(),
        QuarantineReason::Genesis
    );
    // The messages name the step, so a quarantine line says which one.
    assert!(
        StartupError::storage("x")
            .to_string()
            .starts_with("storage:")
    );
    assert!(
        StartupError::OutOfOrder {
            attempted: StartupPhase::MembershipChecked,
            reached: StartupPhase::Boot,
        }
        .to_string()
        .contains("MembershipChecked attempted after only Boot")
    );
}
