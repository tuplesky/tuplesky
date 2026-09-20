//! task-60 acceptance for the registry half: the formats are separate,
//! their windows are real, and nothing in the table is a downgrade.

use coord_types::formats::{Feature, Format, FormatError, Supported, admit};

/// Every format has its own identifier and its own name, and the table
/// is complete and in order.
#[test]
fn the_registry_is_complete_distinct_and_ordered() {
    let mut ids: Vec<u16> = Format::ALL.iter().map(|f| f.id()).collect();
    let before = ids.clone();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), Format::ALL.len(), "two formats share an id");
    assert_eq!(before, ids, "the registry is not in identifier order");
    assert!(ids.iter().all(|id| *id != 0), "zero is never a format id");

    let mut names: Vec<&str> = Format::ALL.iter().map(|f| f.name()).collect();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), Format::ALL.len(), "two formats share a name");

    for format in Format::ALL {
        assert_eq!(
            Format::from_id(format.id()),
            Some(format),
            "{} does not round-trip through its id",
            format.name()
        );
    }
    assert_eq!(Format::from_id(0), None);
    assert_eq!(Format::from_id(0xffff), None);
}

/// A window is a range this build actually reads, and anything outside
/// it is refused in the direction it is outside.
#[test]
fn a_version_outside_the_window_is_refused_and_says_which_way() {
    for format in Format::ALL {
        let window = format.window();
        assert!(
            window.oldest >= 1 && window.oldest <= window.current,
            "{} has an impossible window {window:?}",
            format.name()
        );
        assert!(format.supports(window.oldest));
        assert!(format.supports(window.current));
        assert_eq!(admit(format, window.current), Ok(()));

        // Below: a format that has been retired.
        if window.oldest > 1 {
            let old = window.oldest - 1;
            assert!(!format.supports(old));
            assert_eq!(
                admit(format, old),
                Err(FormatError::Retired {
                    format,
                    found: old,
                    oldest: window.oldest,
                })
            );
        }
        // Above: something else wrote it. Never guessed at, because
        // guessing is how a store gets written by a build that did not
        // understand it.
        let newer = window.current + 1;
        assert!(!format.supports(newer));
        assert_eq!(
            admit(format, newer),
            Err(FormatError::Newer {
                format,
                found: newer,
                current: window.current,
            })
        );
        assert!(admit(format, 0).is_err(), "zero is never a version");
    }
}

/// The formats are independent: bumping one says nothing about another.
///
/// The property the test can actually hold is that the registry gives
/// each its own entry, so a change to one is a change to one line.
/// What it guards against is a single "schema version" creeping back in
/// -- in particular, the command identity and the wire frame must never
/// be the same number, because an upgraded transport that changed a
/// retry identity would silently re-execute callers' work.
#[test]
fn the_command_identity_and_the_wire_frame_are_different_formats() {
    assert_ne!(Format::Command.id(), Format::Wire.id());
    assert_ne!(Format::SharedCheckpoint.id(), Format::LocalCheckpoint.id());
    assert_ne!(Format::JournalRecord.id(), Format::JournalMetadata.id());
    assert_ne!(Format::StoreSchema.id(), Format::KineAdapter.id());
    // And the one that is load-bearing for identity: the canonical
    // command encoding this build hashes is the registry's.
    assert_eq!(
        u32::from(coord_types::logical_v1::SCHEMA_VERSION),
        Format::Command.current()
    );
}

/// Features are frozen, distinct, and every one in the registry is one
/// this build supports.
#[test]
fn every_feature_in_the_registry_is_one_this_build_supports() {
    let mut ids: Vec<u16> = Feature::ALL.iter().map(|f| f.id()).collect();
    let before = ids.clone();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), Feature::ALL.len(), "two features share an id");
    assert_eq!(
        before, ids,
        "the feature registry is not in identifier order"
    );
    assert!(ids.iter().all(|id| *id != 0));

    for feature in Feature::ALL {
        assert_eq!(Feature::from_id(feature.id()), Some(feature));
        assert!(
            Supported::supports(feature),
            "{} is in the registry but this build does not support it",
            feature.name()
        );
    }
    assert_eq!(Feature::from_id(0), None);
    assert_eq!(Supported::features().len(), Feature::ALL.len());
}
