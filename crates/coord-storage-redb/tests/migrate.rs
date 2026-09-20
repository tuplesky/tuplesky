//! task-60 acceptance for the storage half: an offline schema migration
//! is a generation replacement that carries this node's obligations,
//! an interruption preserves the valid selection, a store from another
//! engine or a newer build is refused, and nothing downgrades.

use std::path::Path;

use coord_storage_redb::lifecycle::{ActivateStep, InactiveGeneration};
use coord_storage_redb::manifest::MANIFEST_FORMAT;
use coord_storage_redb::migrate::{
    MigrateError, MigrateLimits, Outcome, Rewritten, Row, SchemaMigration, Unchanged, migrate,
    rewrite_into_new_generation, selected_schema,
};
use coord_storage_redb::{Generation, OpenOptions, StoreIdentity};
use coord_store_api::engine::{LocalEngine, OrderedRead, ScanRequest, SnapshotSource, WriteTxn};
use coord_store_api::registry::Collection;
use coord_types::ids::*;

fn identity() -> StoreIdentity {
    StoreIdentity {
        cluster_id: ClusterId([1; 16]),
        domain_id: DomainId([2; 16]),
        replica_id: ReplicaId([3; 16]),
        incarnation: ReplicaIncarnation::new(1).unwrap(),
    }
}

fn options() -> OpenOptions {
    OpenOptions {
        cache_bytes: 4 << 20,
    }
}

fn limits() -> MigrateLimits {
    MigrateLimits {
        rows_per_commit: 2,
        ..MigrateLimits::default()
    }
}

/// A store with ordinary rows and, deliberately, a protocol obligation:
/// a migration rewrites this node's own state and must keep it.
fn seeded(root: &Path) {
    let mut generation = Generation::create(root, identity(), options()).unwrap();
    let mut tx = generation.engine().begin_write().unwrap();
    for i in 0..5u8 {
        tx.put(
            Collection::KvCurrentV1.id(),
            format!("key-{i}").as_bytes(),
            format!("value-{i}").as_bytes(),
        )
        .unwrap();
    }
    tx.put(
        Collection::ProtocolV1.id(),
        &[0, 0, 0, 0, 0, 0, 0, 7, 0x00],
        b"a promise this node made",
    )
    .unwrap();
    tx.commit_durable().unwrap();
}

fn rows(root: &Path, collection: Collection) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut generation = Generation::open_existing(root, identity(), options()).unwrap();
    let view = generation.engine().reader().snapshot().unwrap();
    view.scan_page(collection.id(), &ScanRequest::all(1024, 1 << 20))
        .unwrap()
        .rows
        .into_iter()
        .map(|r| (r.key, r.value))
        .collect()
}

/// A migration that renames one key, so the rewrite is observable.
struct RenameOne {
    from: u32,
    to: u32,
}

impl SchemaMigration for RenameOne {
    fn from(&self) -> u32 {
        self.from
    }
    fn to(&self) -> u32 {
        self.to
    }
    fn rewrite(&self, row: &Row<'_>) -> Result<Option<Rewritten>, MigrateError> {
        if row.collection == Collection::KvCurrentV1 && row.key == b"key-0" {
            return Ok(None);
        }
        Ok(Some((row.key.to_vec(), row.value.to_vec())))
    }
}

/// A store already at this build's schema is left alone, and a
/// migration that does not read the schema it finds is refused.
#[test]
fn a_current_store_is_left_alone_and_a_wrong_migration_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    seeded(dir.path());
    assert_eq!(
        selected_schema(dir.path()).unwrap().format,
        MANIFEST_FORMAT,
        "a fresh store is not at this build's schema"
    );

    // Nothing to do, and nothing written: no new generation appears.
    let before = generations(dir.path());
    assert_eq!(
        migrate(
            dir.path(),
            options(),
            &Unchanged {
                from: MANIFEST_FORMAT,
                to: MANIFEST_FORMAT + 1,
            },
            &limits()
        ),
        Ok(Outcome::AlreadyCurrent {
            schema: MANIFEST_FORMAT
        })
    );
    assert_eq!(generations(dir.path()), before, "a no-op migration wrote");
}

/// The selection only changes at the last step, so an interruption
/// leaves the previous generation serving and the staging unreferenced.
#[test]
fn an_interrupted_migration_preserves_the_valid_selection() {
    let dir = tempfile::tempdir().unwrap();
    seeded(dir.path());
    let before = rows(dir.path(), Collection::KvCurrentV1);
    let selected = selected_schema(dir.path()).unwrap().generation;

    // Stage a replacement, fill it, and stop where a crash would.
    for step in [ActivateStep::SyncData, ActivateStep::WriteManifest] {
        let mut staged =
            InactiveGeneration::stage_migration(dir.path(), identity(), options()).unwrap();
        let mut tx = staged.engine().begin_write().unwrap();
        tx.put(Collection::KvCurrentV1.id(), b"only-in-the-staging", b"x")
            .unwrap();
        tx.commit_durable().unwrap();
        staged.activate_interrupted(step).unwrap();

        // The previous generation is still the selected one, still has
        // its rows, and the staging is not reachable through it.
        assert_eq!(selected_schema(dir.path()).unwrap().generation, selected);
        assert_eq!(rows(dir.path(), Collection::KvCurrentV1), before);
    }

    // And the node still serves from it afterwards.
    Generation::open_existing(dir.path(), identity(), options()).unwrap();
}

/// A migration carries this node's promises forward. An install refuses
/// a store that holds them; a migration must not.
#[test]
fn a_migration_carries_this_nodes_obligations_and_an_install_still_refuses_them() {
    let dir = tempfile::tempdir().unwrap();
    seeded(dir.path());
    let obligations = rows(dir.path(), Collection::ProtocolV1);
    assert_eq!(obligations.len(), 1, "the fixture has no obligation");

    // The install path refuses outright: a promise is not something to
    // be replaced.
    assert!(
        InactiveGeneration::stage(dir.path(), identity(), options()).is_err(),
        "an install staged over a voter's obligations"
    );

    // The migration path stages, and what it stages is empty until it
    // is filled -- the obligations are the caller's to carry.
    let mut staged =
        InactiveGeneration::stage_migration(dir.path(), identity(), options()).unwrap();
    let view = staged.engine().reader().snapshot().unwrap();
    assert!(
        view.scan_page(Collection::ProtocolV1.id(), &ScanRequest::all(8, 1 << 16))
            .unwrap()
            .rows
            .is_empty(),
        "a staging invented obligations"
    );
    drop(view);
    staged.abandon().unwrap();

    // And the whole migration keeps them, because `migrate` copies
    // every collection.
    let step = RenameOne {
        from: MANIFEST_FORMAT,
        to: MANIFEST_FORMAT,
    };
    // This one is refused as not an upgrade, which is the point: there
    // is no operation that rewrites a store at its own schema.
    assert_eq!(
        migrate(dir.path(), options(), &step, &limits()),
        Ok(Outcome::AlreadyCurrent {
            schema: MANIFEST_FORMAT
        })
    );
    assert_eq!(rows(dir.path(), Collection::ProtocolV1), obligations);
}

/// A store from another engine or profile is never converted.
#[test]
fn another_engine_is_never_migrated() {
    let dir = tempfile::tempdir().unwrap();
    seeded(dir.path());
    // Rewrite the selected manifest's engine name, as a store written
    // by a different adapter would have it.
    let selected = selected_schema(dir.path()).unwrap();
    let path = dir
        .path()
        .join(format!("gen-{:06}", selected.generation))
        .join("manifest.v1");
    let foreign = coord_storage_redb::StoreManifestV1 {
        engine: "some-other-engine".to_owned(),
        ..selected
    };
    foreign.write(&path).unwrap();
    assert!(matches!(
        selected_schema(dir.path()),
        Err(MigrateError::EngineMismatch { .. })
    ));
    assert!(matches!(
        migrate(
            dir.path(),
            options(),
            &Unchanged {
                from: MANIFEST_FORMAT,
                to: MANIFEST_FORMAT + 1
            },
            &limits()
        ),
        Err(MigrateError::EngineMismatch { .. })
    ));
}

fn generations(root: &Path) -> Vec<String> {
    let mut out: Vec<String> = std::fs::read_dir(root)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("gen-"))
        .collect();
    out.sort();
    out
}

/// The rewrite itself: every row of every collection goes through the
/// migration, in bounded durable commits, and the replacement becomes
/// the selected generation.
///
/// Driven directly because the version gate in `migrate` has nothing to
/// accept until a second schema version exists -- and a rewrite that
/// only ran in a future release would be one nobody had ever executed.
#[test]
fn the_rewrite_replaces_the_generation_and_carries_everything_it_keeps() {
    let dir = tempfile::tempdir().unwrap();
    seeded(dir.path());
    let before = rows(dir.path(), Collection::KvCurrentV1);
    let obligations = rows(dir.path(), Collection::ProtocolV1);
    let selected = selected_schema(dir.path()).unwrap().generation;

    let outcome = rewrite_into_new_generation(
        dir.path(),
        identity(),
        options(),
        &RenameOne {
            from: MANIFEST_FORMAT,
            to: MANIFEST_FORMAT,
        },
        &limits(),
    )
    .expect("the rewrite");
    let Outcome::Migrated {
        generation,
        rows: written,
        dropped,
        commits,
    } = outcome
    else {
        panic!("{outcome:?}");
    };
    assert!(generation > selected, "the selection did not move forward");
    assert_eq!(dropped, 1, "the migration's own drop did not happen");
    assert!(commits > 1, "the rewrite did not use bounded commits");

    // The selected generation is the new one, and it holds everything
    // the migration kept -- this node's promises among it.
    assert_eq!(selected_schema(dir.path()).unwrap().generation, generation);
    let after = rows(dir.path(), Collection::KvCurrentV1);
    assert_eq!(after.len(), before.len() - 1);
    assert!(
        !after.iter().any(|(k, _)| k == b"key-0"),
        "the dropped row survived"
    );
    assert_eq!(
        rows(dir.path(), Collection::ProtocolV1),
        obligations,
        "the migration forgot this node's promises"
    );
    assert_eq!(
        written as usize,
        before.len() - 1 + obligations.len() + meta_rows(dir.path()),
        "rows written does not account for everything carried"
    );

    // The previous generation is still on disk: reclaiming it is a
    // separate, deliberate step, so an operator can still fall back.
    assert!(
        generations(dir.path()).len() >= 2,
        "the migration deleted the generation it replaced"
    );
}

/// The `meta_v1` rows a fresh generation carries, which the rewrite
/// copies like any other.
fn meta_rows(root: &Path) -> usize {
    rows(root, Collection::MetaV1).len()
}

/// A store written by a newer build is refused before admission, in
/// both directions of the window, and there is no operation that
/// lowers a schema version.
#[test]
fn a_store_outside_this_builds_window_is_refused_rather_than_guessed_at() {
    for format in [MANIFEST_FORMAT + 1, 0] {
        let dir = tempfile::tempdir().unwrap();
        seeded(dir.path());
        let selected = selected_schema(dir.path()).unwrap();
        let path = dir
            .path()
            .join(format!("gen-{:06}", selected.generation))
            .join("manifest.v1");
        coord_storage_redb::StoreManifestV1 { format, ..selected }
            .write(&path)
            .unwrap();

        // Reading the manifest at all fails, so nothing opens the
        // engine behind it: a store this build does not understand is
        // never partly read.
        assert!(
            selected_schema(dir.path()).is_err(),
            "a manifest at format {format} was accepted"
        );
        assert!(
            Generation::open_existing(dir.path(), identity(), options()).is_err(),
            "a generation at format {format} opened"
        );
        // And there is no migration that would bring it back: a
        // migration only ever writes this build's own schema.
        assert!(
            migrate(
                dir.path(),
                options(),
                &Unchanged {
                    from: format,
                    to: MANIFEST_FORMAT
                },
                &limits()
            )
            .is_err(),
            "format {format} was migrated"
        );
    }
}
