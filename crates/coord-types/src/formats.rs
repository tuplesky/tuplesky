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
            // 2: no `kv_history_v1` rows; below the floor, each key's
            // newest put at or below it as its event row (task-d105).
            Format::SharedCheckpoint => Window {
                oldest: 2,
                current: 2,
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
    /// The leader's proposal sent before its batch is durable, to a
    /// follower that cannot make a fast quorum from it (task-d98, design
    /// Sections 4.3 and 4.8). A follower built before task-d74 hears the
    /// other followers' fast acknowledgements, and with the proposal they
    /// make a fast quorum it would count over a vote the leader may lose.
    EarlyProposal = 0x0004,
    /// `EarlyProposal`'s prepare marker (task-d99, design Section 13): a
    /// voter that wrote its durable support for `EarlyProposal` names this
    /// in its own activation record, in the same batch. It gates no
    /// behaviour. It is the rollback guard every build since `admit`
    /// already runs: a build that does not know this id cannot read the
    /// record and refuses to start, so a voter cannot report the feature,
    /// roll back, and leave unanimity counting a build it no longer runs.
    EarlyProposalPrepared = 0x0005,
    /// Every voter votes on durable payload rows and forgets only what its
    /// journal holds (task-d90's points 2 and 4, #195; point 3, task-d95).
    /// A build from before those can acknowledge a command whose payload
    /// row it never made durable, and come back holding the record alone.
    /// Two rules rest on this, and each is on only while it is active
    /// (design Section 4.5):
    /// - a leader's results are released as its commands execute in
    ///   memory, before their execution group is journaled; without the
    ///   premise a domain-wide crash could lose a result already
    ///   released;
    /// - a selection leaves out a re-proposal no reporter holds a payload
    ///   row for (`SelectionRules::leave_out_unheld`); without the premise
    ///   that could drop a command an older voter acknowledged, where
    ///   re-proposing it stalls until a supplier is back.
    ReleaseAtExecution = 0x0006,
    /// `ReleaseAtExecution`'s prepare marker, as `EarlyProposalPrepared`
    /// is `EarlyProposal`'s.
    ReleaseAtExecutionPrepared = 0x0007,
    /// A collector answers a command on its leader's final release alone,
    /// and the voters send its frontend no evidence for it (task-d100,
    /// step A; design Section 4.3). A collector marks its submissions so
    /// only while this is active, which implies that every configured
    /// voter runs a build that reads the mark and puts the decided
    /// payload's admission digest in its releases. Unmarked commands keep
    /// today's evidence, so the two mix while it activates.
    AnswerOnRelease = 0x0008,
    /// `AnswerOnRelease`'s prepare marker, as `EarlyProposalPrepared` is
    /// `EarlyProposal`'s.
    AnswerOnReleasePrepared = 0x0009,
    /// A contended round is a batch (task-d102, design Section 4.3), with
    /// task-d100's step B, the leader relay, as its first part. A
    /// collector may send a submission to its ballot's leader alone,
    /// marked as relayed, and the leader's proposal of it then carries
    /// the payload to every follower (`ProtocolMessage::RelayedProposal`).
    /// A voter of an older build could not decode that proposal, so a
    /// collector relays only while this is active, and it is activated
    /// only after `AnswerOnRelease`: a relayed command is marked, and
    /// answered on the leader's release alone. It gates a wire form and
    /// a routing, not an argument about votes: a relayed command is, to
    /// every voter, one whose submission to the followers was lost and
    /// whose payload arrived with the leader's proposal.
    BatchedRound = 0x000A,
    /// `BatchedRound`'s prepare marker, as `EarlyProposalPrepared` is
    /// `EarlyProposal`'s.
    BatchedRoundPrepared = 0x000B,
}

impl Feature {
    /// Every feature, in identifier order.
    pub const ALL: [Feature; 11] = [
        Feature::CheckpointFloor,
        Feature::SealedHandoff,
        Feature::LocalCheckpoint,
        Feature::EarlyProposal,
        Feature::EarlyProposalPrepared,
        Feature::ReleaseAtExecution,
        Feature::ReleaseAtExecutionPrepared,
        Feature::AnswerOnRelease,
        Feature::AnswerOnReleasePrepared,
        Feature::BatchedRound,
        Feature::BatchedRoundPrepared,
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
            Feature::EarlyProposal => "early-proposal",
            Feature::EarlyProposalPrepared => "early-proposal-prepared",
            Feature::ReleaseAtExecution => "release-at-execution",
            Feature::ReleaseAtExecutionPrepared => "release-at-execution-prepared",
            Feature::AnswerOnRelease => "answer-on-release",
            Feature::AnswerOnReleasePrepared => "answer-on-release-prepared",
            Feature::BatchedRound => "batched-round",
            Feature::BatchedRoundPrepared => "batched-round-prepared",
        }
    }

    /// The prepare marker of a feature a running domain can activate
    /// (task-d99), or `None` for one it cannot: a feature without a marker
    /// has no guard binding a voter's report from the moment it is
    /// durable, so it is not activated through the running path.
    pub const fn prepare_marker(self) -> Option<Feature> {
        match self {
            Feature::EarlyProposal => Some(Feature::EarlyProposalPrepared),
            Feature::ReleaseAtExecution => Some(Feature::ReleaseAtExecutionPrepared),
            Feature::AnswerOnRelease => Some(Feature::AnswerOnReleasePrepared),
            Feature::BatchedRound => Some(Feature::BatchedRoundPrepared),
            _ => None,
        }
    }

    /// The feature this is the prepare marker of, or `None` for a feature
    /// that is not a marker.
    pub const fn marks(self) -> Option<Feature> {
        match self {
            Feature::EarlyProposalPrepared => Some(Feature::EarlyProposal),
            Feature::ReleaseAtExecutionPrepared => Some(Feature::ReleaseAtExecution),
            Feature::AnswerOnReleasePrepared => Some(Feature::AnswerOnRelease),
            Feature::BatchedRoundPrepared => Some(Feature::BatchedRound),
            _ => None,
        }
    }

    /// Whether this is a prepare marker rather than a feature: it is never
    /// reported, never activated and gates nothing.
    pub const fn is_marker(self) -> bool {
        self.marks().is_some()
    }

    /// Look up by stable name (operator input).
    pub fn from_name(name: &str) -> Option<Feature> {
        Feature::ALL.iter().copied().find(|f| f.name() == name)
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
    /// The features this build supports, in identifier order, prepare
    /// markers included: a marker is supported exactly when the feature
    /// it marks is, and `admit` reads one as a requirement on this set.
    pub const fn features() -> [Feature; 11] {
        Feature::ALL
    }

    /// What this build reports it can take part in: its features without
    /// the prepare markers, which are never reported.
    pub fn reported() -> impl Iterator<Item = Feature> {
        Feature::ALL.into_iter().filter(|f| !f.is_marker())
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
