//! The genesis manifest this node was initialized under, pinned in its
//! own store (design Sections 10.2, 17.1, 22.1).
//!
//! The manifest is re-read from its file on every start, and a file can
//! be edited. Without a pin, a node initialized under one set of voters
//! and one policy would start under whatever the file said next, as long
//! as the cluster and domain were unchanged -- and the committed
//! configuration it votes by would be the operator's latest edit rather
//! than the one every replica agreed on. So `coordd init` pins the
//! manifest's digest in the store it creates, and every later start runs
//! the startup sequence through `check_membership` against that pin: the
//! same manifest is this node's, any other is a genesis quarantine.
//!
//! A start never pins. The pin is the last durable step of `coordd
//! init`, after the generation is created, attached to the journal and
//! holds the domain's genesis policy, so a store with no pin is one
//! whose initialization did not finish -- it stopped somewhere before
//! the pin was durable. Serving it would pin whatever manifest it was
//! handed then, which is trust on first use by another name, and would
//! serve a domain whose policy may never have been written. It is
//! refused, and `coordd init` finishes it: every step before the pin
//! can be run again (the policy writes only the rows that are missing),
//! and a generation that was never pinned has never been served, so it
//! holds no history to lose.
//!
//! # What the pin admits
//!
//! Exactly one kind of change: a forward replacement of existing voters.
//! A manifest that differs from the pinned one passes the pin if and
//! only if every field is identical -- cluster, domain, epoch, the voter
//! set (the same replica ids, in the same order), issuer roots,
//! workload-identity rules, admin and protocol version -- except that
//! one or more existing voters' `(incarnation, public_key)` entries have
//! moved to a strictly higher incarnation. Any other difference is a
//! genesis quarantine, and so is an incarnation that moved backwards or
//! a key that changed at the same incarnation.
//!
//! Why this one: on this branch an authorized node/key replacement
//! (task-58) is committed *through the manifest*. The operator hands the
//! replaced node a genesis in which its voter entry has moved to the next
//! incarnation with its new key, because there is not yet a committed
//! reconfiguration to carry the change; that path is later work, and
//! once a replacement is committed there the pin can go back to
//! admitting nothing. A replacement moves a voter forward and changes
//! nothing else, and forward is the only direction a replaced voter may
//! go: the backward move is the left-behind disk or the retired
//! credential that task-58 fences. Every other field is the agreement
//! the replicas were initialized under, and an edit to it is exactly
//! what the pin is there to refuse.
//!
//! An admitted replacement is re-pinned durably -- the new manifest and
//! its digest in one transaction -- before anything is adopted:
//! `store::Opened::attach`, which carries the journal's stream and
//! advances the projection's manifest, runs only after this check. A
//! stop before the re-pin leaves the old pin and nothing adopted, and the
//! next start decides again; a stop after it and before the adoption
//! leaves a pin that matches the manifest, and the next start finishes
//! the adoption through task-58's interrupted-adoption path.
//!
//! Deciding that needs the pinned manifest itself, not only its digest,
//! so `init` records it beside the pin (`meta_fields::GENESIS_MANIFEST`,
//! before the digest, so a pin never exists without it). A store whose
//! pinned manifest is missing, or does not hash to its pin, admits no
//! change at all.

use coord_daemon::{NodeJournal, Startup, StartupError, StoreGenesis};
use coord_membership::genesis::GenesisManifest;
use coord_membership::init::{GenesisStore, InitError, StoreFailure};
use coord_storage_redb::RedbEngine;
use coord_storage_redb::lifecycle::Generation;
use coord_store_api::engine::{LocalEngine, OrderedRead, SnapshotSource, WriteTxn};
use coord_store_api::registry::{Collection, meta_fields};

/// What the pin is being checked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Intent {
    /// `coordd init`: the store was just created (or its creation is
    /// being finished), so the manifest is pinned.
    Initialize,
    /// A start: the pin must already be there, and must match.
    Serve,
}

/// Why the node is not running under the manifest it was handed.
#[derive(Debug)]
pub enum GenesisError {
    /// The store was created but its initialization never pinned a
    /// manifest.
    NeverPinned,
    /// A step of the startup sequence refused.
    Startup(StartupError),
}

impl GenesisError {
    /// How the process quarantines for this.
    pub const fn quarantine_reason(&self) -> coord_daemon::QuarantineReason {
        match self {
            GenesisError::NeverPinned => coord_daemon::QuarantineReason::Genesis,
            GenesisError::Startup(e) => e.quarantine_reason(),
        }
    }
}

impl core::fmt::Display for GenesisError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            GenesisError::NeverPinned => write!(
                f,
                "genesis quarantine: this store was never pinned to a genesis \
                 manifest, so its initialization did not finish. Run \
                 `coordd init` to finish it"
            ),
            GenesisError::Startup(StartupError::Genesis(InitError::DigestMismatch {
                pinned,
                offered,
            })) => write!(
                f,
                "genesis quarantine: this node was initialized under genesis {} \
                 and was handed genesis {}. A node's genesis is fixed when it \
                 is initialized, and the only change a start accepts is an \
                 existing voter moving to a higher incarnation; restore the \
                 manifest it was initialized under",
                short(&pinned.0),
                short(&offered.0)
            ),
            GenesisError::Startup(e) => write!(f, "{:?} quarantine: {e}", e.quarantine_reason()),
        }
    }
}

impl core::error::Error for GenesisError {}

/// The node's durable history, as durable initialization asks after it.
///
/// On this build a node's history is its journal and the projection
/// generation attached to it. By the time the pin is checked,
/// `store::open_storage` has already opened both and matched them to
/// this node's identity (and created them, for `coordd init`, or reused
/// the journal an interrupted one left with no history in it), so the
/// history exists and establishing it is already done. The pin lives on
/// the generation, in its `meta_v1` beside the identity the lifecycle
/// wrote there, and is checked before the generation is attached to the
/// journal.
struct GenerationHistory;

impl NodeJournal for GenerationHistory {
    fn intact(&self) -> Result<bool, StoreFailure> {
        Ok(true)
    }
    fn establish(&mut self) -> Result<(), StoreFailure> {
        Ok(())
    }
}

/// Whether `generation` was created by a `coordd init` that stopped
/// before it pinned a manifest. `None` when the store cannot say.
pub fn unfinished(generation: &mut Generation) -> Option<bool> {
    let mut history = GenerationHistory;
    let store = StoreGenesis::new(generation.engine(), &mut history);
    store.pinned_digest().ok().map(|pin| pin.is_none())
}

/// A voter a forward replacement moved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Moved {
    /// The voter's node identity, as the manifest writes it.
    pub node: String,
    /// The pinned incarnation.
    pub from: u64,
    /// The presented one, strictly higher.
    pub to: u64,
}

/// Whether `presented` is `pinned` with one or more existing voters moved
/// forward, and nothing else changed; the voters that moved if so.
///
/// `None` for every other difference, including none at all: identical
/// manifests have identical digests and never reach this.
pub fn forward_replacement(
    pinned: &GenesisManifest,
    presented: &GenesisManifest,
) -> Option<Vec<Moved>> {
    // Destructured, so a field added to the manifest is a compile error
    // here rather than a field an edit could change unnoticed.
    let GenesisManifest {
        cluster,
        domain,
        epoch,
        voters,
        issuer_roots,
        wif_rules,
        admin,
        protocol_version,
    } = presented;
    if *cluster != pinned.cluster
        || *domain != pinned.domain
        || *epoch != pinned.epoch
        || *issuer_roots != pinned.issuer_roots
        || *wif_rules != pinned.wif_rules
        || *admin != pinned.admin
        || *protocol_version != pinned.protocol_version
        || voters.len() != pinned.voters.len()
    {
        return None;
    }
    let mut moved = Vec::new();
    for (was, now) in pinned.voters.iter().zip(voters) {
        if now.node != was.node {
            return None;
        }
        if now == was {
            continue;
        }
        if now.incarnation <= was.incarnation {
            return None;
        }
        moved.push(Moved {
            node: now.node.clone(),
            from: was.incarnation,
            to: now.incarnation,
        });
    }
    (!moved.is_empty()).then_some(moved)
}

fn meta(engine: &RedbEngine, key: &[u8]) -> Result<Option<Vec<u8>>, StoreFailure> {
    engine
        .reader()
        .snapshot()
        .map_err(|_| StoreFailure)?
        .get(Collection::MetaV1.id(), key)
        .map_err(|_| StoreFailure)
}

/// The manifest `pin` was pinned from, if the store holds it and it
/// hashes to the pin.
fn pinned_manifest(
    engine: &RedbEngine,
    pin: coord_types::identity::Digest32,
) -> Result<Option<GenesisManifest>, StoreFailure> {
    let Some(bytes) = meta(engine, meta_fields::GENESIS_MANIFEST)? else {
        return Ok(None);
    };
    Ok(serde_json::from_slice::<GenesisManifest>(&bytes)
        .ok()
        .filter(|pinned| pinned.digest() == pin))
}

/// Record `manifest` beside the pin, durably, before the digest is
/// pinned.
fn record(engine: &mut RedbEngine, manifest: &GenesisManifest) -> Result<(), StoreFailure> {
    let bytes = serde_json::to_vec(manifest).map_err(|_| StoreFailure)?;
    let mut txn = engine.begin_write().map_err(|_| StoreFailure)?;
    txn.put(
        Collection::MetaV1.id(),
        meta_fields::GENESIS_MANIFEST,
        &bytes,
    )
    .map_err(|_| StoreFailure)?;
    txn.commit_durable().map_err(|_| StoreFailure)
}

/// Re-pin `manifest`: the manifest and its digest in one durable
/// transaction, so neither is ever the other's predecessor.
fn repin(engine: &mut RedbEngine, manifest: &GenesisManifest) -> Result<(), StoreFailure> {
    let bytes = serde_json::to_vec(manifest).map_err(|_| StoreFailure)?;
    let mut txn = engine.begin_write().map_err(|_| StoreFailure)?;
    txn.put(
        Collection::MetaV1.id(),
        meta_fields::GENESIS_MANIFEST,
        &bytes,
    )
    .map_err(|_| StoreFailure)?;
    txn.put(
        Collection::MetaV1.id(),
        meta_fields::GENESIS_DIGEST,
        &manifest.digest().0,
    )
    .map_err(|_| StoreFailure)?;
    txn.commit_durable().map_err(|_| StoreFailure)
}

/// Run the startup sequence over `generation` through the membership
/// check: pin `manifest` for [`Intent::Initialize`], require and match
/// the pin for [`Intent::Serve`] -- re-pinning first when `manifest` is a
/// forward replacement of the pinned one (see the module documentation).
pub fn check(
    generation: &mut Generation,
    manifest: &GenesisManifest,
    intent: Intent,
) -> Result<Startup, GenesisError> {
    let failed = |_| GenesisError::Startup(StartupError::Genesis(InitError::Store));
    let pin = {
        let mut history = GenerationHistory;
        let store = StoreGenesis::new(generation.engine(), &mut history);
        store.pinned_digest().map_err(failed)?
    };
    match (intent, pin) {
        (Intent::Serve, None) => return Err(GenesisError::NeverPinned),
        // Recorded before `check_membership` pins the digest, so a pin
        // never exists without the manifest it was pinned from.
        (Intent::Initialize, None) => record(generation.engine(), manifest).map_err(failed)?,
        (Intent::Serve, Some(pin)) if pin != manifest.digest() => {
            let moved = pinned_manifest(generation.engine(), pin)
                .map_err(failed)?
                .and_then(|pinned| forward_replacement(&pinned, manifest));
            // Anything but a forward replacement falls through to the
            // membership check, which refuses it as a genesis quarantine
            // with the store untouched.
            if let Some(moved) = moved {
                repin(generation.engine(), manifest).map_err(failed)?;
                for voter in moved {
                    println!(
                        "genesis re-pinned for a replacement: voter={} incarnation {} -> {}",
                        voter.node.get(..8).unwrap_or(&voter.node),
                        voter.from,
                        voter.to
                    );
                }
            }
        }
        _ => {}
    }
    let mut history = GenerationHistory;
    let mut store = StoreGenesis::new(generation.engine(), &mut history);
    let mut startup = Startup::new();
    // `store::open_storage` has matched the journal and the generation
    // to this node's identity,
    // and the caller has verified the credentials that identity came
    // from, before this is reached.
    startup.storage_validated().map_err(GenesisError::Startup)?;
    startup
        .identity_validated()
        .map_err(GenesisError::Startup)?;
    startup
        .check_membership(manifest, &mut store)
        .map_err(GenesisError::Startup)?;
    Ok(startup)
}

fn short(bytes: &[u8]) -> String {
    bytes[..4].iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use coord_membership::genesis::VoterSeed;

    fn manifest() -> GenesisManifest {
        GenesisManifest {
            cluster: "11".repeat(16),
            domain: "22".repeat(16),
            epoch: 1,
            voters: (1u8..=3)
                .map(|n| VoterSeed {
                    node: format!("{n:02x}").repeat(16),
                    incarnation: 1,
                    public_key: format!("key-{n}-1"),
                })
                .collect(),
            issuer_roots: vec!["root".into()],
            wif_rules: vec![serde_json::json!({ "issuer": "test" })],
            admin: "0a".repeat(16),
            protocol_version: 1,
        }
    }

    #[test]
    fn a_voter_moved_forward_with_a_new_key_is_admitted() {
        let pinned = manifest();
        let mut presented = pinned.clone();
        presented.voters[0].incarnation = 2;
        presented.voters[0].public_key = "key-1-2".into();
        assert_eq!(
            forward_replacement(&pinned, &presented),
            Some(vec![Moved {
                node: "01".repeat(16),
                from: 1,
                to: 2
            }])
        );
        // More than one at once, and by more than one generation.
        presented.voters[2].incarnation = 5;
        presented.voters[2].public_key = "key-3-5".into();
        assert_eq!(
            forward_replacement(&pinned, &presented).map(|m| m.len()),
            Some(2)
        );
    }

    #[test]
    fn a_voter_moved_backwards_or_rekeyed_in_place_is_not() {
        let pinned = {
            let mut m = manifest();
            m.voters[0].incarnation = 2;
            m
        };
        let mut back = pinned.clone();
        back.voters[0].incarnation = 1;
        back.voters[0].public_key = "key-1-1-again".into();
        assert_eq!(forward_replacement(&pinned, &back), None);

        let mut rekeyed = pinned.clone();
        rekeyed.voters[0].public_key = "another".into();
        assert_eq!(forward_replacement(&pinned, &rekeyed), None);

        // A forward move beside a backward one is refused as a whole.
        let mut mixed = pinned.clone();
        mixed.voters[1].incarnation = 2;
        mixed.voters[0].incarnation = 1;
        assert_eq!(forward_replacement(&pinned, &mixed), None);
    }

    #[test]
    fn any_other_edit_is_not_even_beside_a_replacement() {
        let pinned = manifest();
        let replaced = |edit: &dyn Fn(&mut GenesisManifest)| {
            let mut m = pinned.clone();
            m.voters[0].incarnation = 2;
            m.voters[0].public_key = "key-1-2".into();
            edit(&mut m);
            forward_replacement(&pinned, &m)
        };
        assert!(replaced(&|_| {}).is_some(), "the control case");
        let edits: [&dyn Fn(&mut GenesisManifest); 11] = [
            &|m| m.cluster = "33".repeat(16),
            &|m| m.domain = "44".repeat(16),
            &|m| m.epoch = 2,
            &|m| m.issuer_roots.push("another".into()),
            &|m| m.wif_rules = vec![serde_json::json!({ "issuer": "other" })],
            &|m| m.admin = "0b".repeat(16),
            &|m| m.protocol_version = 2,
            // The voter set: one swapped for a stranger, one added, one
            // removed, and the same voters in another order.
            &|m| m.voters[1].node = "09".repeat(16),
            &|m| {
                m.voters.push(VoterSeed {
                    node: "04".repeat(16),
                    incarnation: 1,
                    public_key: "key-4-1".into(),
                })
            },
            &|m| {
                m.voters.pop();
            },
            &|m| m.voters.swap(1, 2),
        ];
        for (i, edit) in edits.iter().enumerate() {
            assert_eq!(replaced(edit), None, "edit {i} was admitted");
        }
        // And an identical manifest is no replacement at all.
        assert_eq!(forward_replacement(&pinned, &pinned), None);
    }
}
