//! The frozen format registry and the replicated feature set (task-60;
//! design Sections 11.2, 13, 17.7, 17.10, 17.13, 17.16).
//!
//! Two different things live here, and keeping them apart is the point
//! of the module.
//!
//! **Formats** are how bytes are written. There are several of them and
//! they are deliberately independent: a command's identity, a wire
//! frame, a journal record, a store's schema, the two kinds of
//! checkpoint, a backup and an adapter's DTOs each have their own
//! version and their own transition. Bumping one must not force the
//! others, and more importantly must not silently *appear* to change
//! another: an upgraded transport cannot change a retry identity, and a
//! physical engine choice cannot change a common hash. [`Format`] is the
//! one place that says which versions this build reads and writes, and
//! the tests beside it check that every constant in the tree agrees
//! with it, so the registry cannot drift away from the code.
//!
//! Each format carries a **decoder window**: the oldest version this
//! build still reads, and the one it writes. A window wider than one
//! version is what lets binaries of two releases coexist. Anything
//! outside it fails before admission rather than being guessed at --
//! [`Format::supports`] is the check, and "unsupported format" is
//! always a refusal, never a best effort.
//!
//! **Features** are what the cluster does, and unlike formats they are
//! not a local decision. A feature changes replicated behaviour or
//! durable state that every voter must be able to read, so it becomes
//! active only once *every* configured voter has reported that it
//! supports it, and once active an old binary refuses to serve rather
//! than operating on state it cannot fully interpret. That is the
//! asymmetry the whole upgrade story rests on: compatible binaries
//! coexist freely *before* activation, and activation is the one-way
//! point after which they do not.
//!
//! Neither half permits a downgrade. There is no operation here that
//! lowers a format, deactivates a feature, or rewrites a live handle;
//! rollback before activation is "run the old binary", and rollback
//! after activation is a restore (task-59), with everything that costs.

use serde::{Deserialize, Serialize};

/// The versions of one format this build can read, and the one it
/// writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Window {
    /// Oldest version still decoded.
    pub oldest: u32,
    /// Version written.
    pub current: u32,
}

/// One independently versioned format.
///
/// The discriminants are frozen: they appear in durable records and in
/// the support a voter reports, so they are assigned explicitly and
/// never reordered or recycled.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(u16)]
pub enum Format {
    /// Canonical logical command encoding: what a command identity is
    /// derived from. An upgraded transport never changes this.
    Command = 0x0001,
    /// Wire frame header and message schema.
    Wire = 0x0002,
    /// Journal record format.
    JournalRecord = 0x0003,
    /// Journal metadata (the engine's own durable header).
    JournalMetadata = 0x0004,
    /// Store schema: the generation manifest and the collection
    /// registry it pins.
    StoreSchema = 0x0005,
    /// `SharedCheckpointV1`.
    SharedCheckpoint = 0x0006,
    /// `LocalRecoveryCheckpointV1`.
    LocalCheckpoint = 0x0007,
    /// Backup manifest.
    Backup = 0x0008,
    /// The Kine adapter's published DTOs.
    KineAdapter = 0x0009,
}

impl Format {
    /// Every format, in identifier order.
    pub const ALL: [Format; 9] = [
        Format::Command,
        Format::Wire,
        Format::JournalRecord,
        Format::JournalMetadata,
        Format::StoreSchema,
        Format::SharedCheckpoint,
        Format::LocalCheckpoint,
        Format::Backup,
        Format::KineAdapter,
    ];

    /// The frozen identifier.
    pub const fn id(self) -> u16 {
        self as u16
    }

    /// A stable name for diagnostics and operator output.
    pub const fn name(self) -> &'static str {
        match self {
            Format::Command => "command",
            Format::Wire => "wire",
            Format::JournalRecord => "journal-record",
            Format::JournalMetadata => "journal-metadata",
            Format::StoreSchema => "store-schema",
            Format::SharedCheckpoint => "shared-checkpoint",
            Format::LocalCheckpoint => "local-checkpoint",
            Format::Backup => "backup",
            Format::KineAdapter => "kine-adapter",
        }
    }

    /// What this build reads and writes.
    ///
    /// Every window is currently one version wide, because nothing has
    /// had an incompatible change yet. Widening one is how a release
    /// keeps reading its predecessor's bytes, and it is a reviewed
    /// change to this table rather than a decoder that tries and sees.
    pub const fn window(self) -> Window {
        match self {
            Format::Command => Window {
                oldest: 1,
                current: 1,
            },
            Format::Wire => Window {
                oldest: 1,
                current: 1,
            },
            Format::JournalRecord => Window {
                oldest: 1,
                current: 1,
            },
            Format::JournalMetadata => Window {
                oldest: 1,
                current: 1,
            },
            Format::StoreSchema => Window {
                oldest: 1,
                current: 1,
            },
            Format::SharedCheckpoint => Window {
                oldest: 1,
                current: 1,
            },
            Format::LocalCheckpoint => Window {
                oldest: 1,
                current: 1,
            },
            Format::Backup => Window {
                oldest: 1,
                current: 1,
            },
            Format::KineAdapter => Window {
                oldest: 1,
                current: 1,
            },
        }
    }

    /// The version this build writes.
    pub const fn current(self) -> u32 {
        self.window().current
    }

    /// Whether this build can read `version`.
    ///
    /// Below the window is a format that has been retired; above it is
    /// one written by a newer binary. Both are refusals: guessing at
    /// either is how a store gets written by something that did not
    /// understand it.
    pub const fn supports(self, version: u32) -> bool {
        let window = self.window();
        version >= window.oldest && version <= window.current
    }

    /// Look up by frozen identifier.
    pub fn from_id(id: u16) -> Option<Format> {
        let mut i = 0;
        while i < Format::ALL.len() {
            if Format::ALL[i].id() == id {
                return Some(Format::ALL[i]);
            }
            i += 1;
        }
        None
    }
}

/// Why a version was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FormatError {
    /// Older than anything this build decodes.
    Retired {
        /// The format.
        format: Format,
        /// Version found.
        found: u32,
        /// Oldest decoded.
        oldest: u32,
    },
    /// Newer than anything this build writes: something else wrote it.
    Newer {
        /// The format.
        format: Format,
        /// Version found.
        found: u32,
        /// Newest understood.
        current: u32,
    },
}

impl core::fmt::Display for FormatError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            FormatError::Retired {
                format,
                found,
                oldest,
            } => write!(
                f,
                "{} format {found} is older than this build reads ({oldest})",
                format.name()
            ),
            FormatError::Newer {
                format,
                found,
                current,
            } => write!(
                f,
                "{} format {found} was written by a newer build than this one ({current})",
                format.name()
            ),
        }
    }
}

impl core::error::Error for FormatError {}

/// Check a version against a format's window.
pub const fn admit(format: Format, version: u32) -> Result<(), FormatError> {
    let window = format.window();
    if version < window.oldest {
        return Err(FormatError::Retired {
            format,
            found: version,
            oldest: window.oldest,
        });
    }
    if version > window.current {
        return Err(FormatError::Newer {
            format,
            found: version,
            current: window.current,
        });
    }
    Ok(())
}

/// A replicated behaviour that every voter must support before it is
/// used.
///
/// The discriminants are frozen: a voter reports them and an activation
/// record holds them. A feature is added here when a change alters
/// replicated behaviour or writes durable state an older voter could
/// not fully interpret -- not for a local optimization, and not for
/// anything a single node decides alone.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(u16)]
pub enum Feature {
    /// Quorum-certified checkpoint floors and the readiness promises
    /// they rest on (task-52/53): a voter that cannot read an activated
    /// floor cannot tell a retained command from a forgotten one.
    CheckpointFloor = 0x0001,
    /// Sealed membership handoff: seal rows, terminal certificates,
    /// installation records and activation (task-55/56/57). A voter
    /// that ignores a seal row would keep voting in a configuration the
    /// others have closed.
    SealedHandoff = 0x0002,
    /// Local recovery checkpoints and journal-prefix reclamation
    /// (task-j04). A voter that cannot load a published baseline cannot
    /// recover from a reclaimed prefix at all.
    LocalCheckpoint = 0x0003,
}

impl Feature {
    /// Every feature, in identifier order.
    pub const ALL: [Feature; 3] = [
        Feature::CheckpointFloor,
        Feature::SealedHandoff,
        Feature::LocalCheckpoint,
    ];

    /// The frozen identifier.
    pub const fn id(self) -> u16 {
        self as u16
    }

    /// A stable name for diagnostics and operator output.
    pub const fn name(self) -> &'static str {
        match self {
            Feature::CheckpointFloor => "checkpoint-floor",
            Feature::SealedHandoff => "sealed-handoff",
            Feature::LocalCheckpoint => "local-checkpoint",
        }
    }

    /// Look up by frozen identifier.
    pub fn from_id(id: u16) -> Option<Feature> {
        Feature::ALL.iter().copied().find(|f| f.id() == id)
    }
}

/// What one binary supports: the formats it writes and the features it
/// can take part in.
///
/// A build reports this; it never negotiates it down. Claiming support
/// for less than a binary has is harmless and claiming more is how a
/// cluster activates something a node cannot do, so this is derived
/// from the registry rather than configured.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Supported;

impl Supported {
    /// The features this build supports, in identifier order.
    pub const fn features() -> [Feature; 3] {
        Feature::ALL
    }

    /// Whether this build supports `feature`.
    ///
    /// Every feature in the registry is one this build implements: a
    /// discriminant exists because the code does. A build that dropped
    /// support for one would remove neither the discriminant nor the
    /// name, and would return `false` here.
    pub fn supports(feature: Feature) -> bool {
        Feature::ALL.contains(&feature)
    }
}
