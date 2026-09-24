//! Conservative all-voter checkpoint trimming (task-51; design Sections
//! 5.3, 17.5–17.6, 17.16.5).
//!
//! This is the first correctness increment of semantic forgetting, and it
//! is deliberately the most pessimistic one: protocol state is trimmed
//! **only** after every configured voter of the epoch has durably
//! acknowledged the *same* shared checkpoint. There is no quorum
//! certificate, no activation and no recovery-floor discovery here; an
//! unavailable voter simply stops trimming, which is exactly the
//! availability cost Section 5.3 accepts for the reference increment and
//! task-52/task-53 later remove.
//!
//! The three durable pieces:
//!
//! * [`CheckpointAckV1`], one per voter in `checkpoint_v1`: this voter
//!   holds the checkpoint named by `(configuration, boundary, root)`. It is
//!   node-private, so it never enters the common digest, and it is
//!   evidence of possession, never of authority.
//! * [`establish_floor`] turns the acknowledgements into a [`TrimFloor`]
//!   only when every configured voter is present with a byte-identical
//!   `(configuration, boundary, root)` under this cluster and domain. An
//!   acknowledgement from a replica that is not a configured voter is
//!   refused outright: observers hold catch-up state, not obligations, and
//!   supply no trim votes.
//! * [`TrimmedFloorV1`], published in `checkpoint_v1` **before** the first
//!   deletion and never lowered. [`plan_trim`] enforces the first half: it
//!   refuses to plan any deletion unless the store it reads already holds a
//!   durable floor at or above the one it is given, so deletions can never
//!   be committed ahead of, or in the same batch as, their fence.
//!   [`publish_floor_in`] enforces the second: it re-reads the floor inside
//!   the write transaction that replaces it, so a publication planned from
//!   a stale snapshot cannot lower a floor committed meanwhile. It is the
//!   fence: once it is durable, a
//!   delayed message about state at or below it is answered from the
//!   retained common outcome ([`TrimFence`]) instead of re-creating the
//!   protocol rows that trimming removed, and a late acknowledgement of an
//!   older checkpoint cannot move the floor back down.
//!
//! What trimming deletes: rows of `protocol_v1`, in bounded batches, for
//! commands that are executed at or below the floor's execution position
//! (dependency and proposal rows). What it never deletes:
//!
//! * the **promise row** of any epoch. Forgetting a promise is exactly how
//!   a delayed lower ballot revives below-floor state; the row is small,
//!   bounded by the number of epochs, and stays.
//! * a bound **Sync row**. Section 4.9 requires a selection to be reused
//!   after a crash, never reselected.
//! * anything for a command that `executed_v1` does not place at or below
//!   the floor: an unresolved obligation is never evicted, under any
//!   pressure. The dependency row's own phase is not consulted, because
//!   execution is never written back into it: normal execution records the
//!   `executed_v1` identity and leaves the durable row at `Accept` or
//!   `Commit`, and recovery restores the executed phase from `executed_v1`
//!   too.
//! * anything a retained command still names as a direct dependency. The
//!   retained command's own record survives with its dependency identities,
//!   and the trimmed dependency's outcome stays in the common collections;
//!   pinning its record as well keeps the required closure whole rather
//!   than relying on that argument.
//! * any row this build does not recognize, and any row of any other
//!   collection.
//!
//! Retention stays separated (Section 17.16.5). Trimming reads the
//! replicated MVCC retention floor only as part of the boundary it compares
//! against; it never lowers it, never touches `kv_history_v1`, `events_v1`,
//! `executed_v1`, `retry_v1` or `payload_v1`, and emits no journal or
//! materialization progress. Because `protocol_v1` is node-private, a
//! trimmed replica re-exports (task-49) the same root as an untrimmed one.
//! Conversely, local redo compaction is not this: a complete local
//! checkpoint may replace old redo while the obligations inside it remain,
//! and only the floor here authorizes forgetting them.
//!
//! When the floor cannot be established, [`trim_backpressure`] reports the
//! retained protocol rows against the bound. Above the bound the caller
//! refuses new work; it does not evict anything.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::ops::Bound;

use coord_consensus::rows::{
    DEPENDENCY_TAG, PROPOSAL_TAG, SYNC_TAG, decode_dependency, decode_proposal,
};
use coord_core::effect::StoreUpdate;
use coord_storage::codecs;
use coord_store_api::engine::{
    Direction, EngineError, ErrorClass, OrderedRead, ScanRequest, WriteTxn,
};
use coord_store_api::envelope::StoreEnvelopeV1;
use coord_store_api::registry::Collection;
use coord_types::CommandId;
use coord_types::identity::Digest32;
use coord_types::ids::{
    Ballot, ClusterId, ConfigurationEpoch, DomainId, ExecutionPosition, ReplicaId,
};
use serde::{Deserialize, Serialize};

use crate::install::InstalledCheckpointV1;
use crate::manifest::{CheckpointBoundary, SharedManifestV1};

/// Record kind of a voter's checkpoint acknowledgement in `checkpoint_v1`.
pub const ACK_RECORD_KIND: u16 = 0x0002;
/// Record kind of the published trim floor in `checkpoint_v1`.
pub const FLOOR_RECORD_KIND: u16 = 0x0003;
/// Schema version of both records.
pub const TRIM_SCHEMA_VERSION: u16 = 1;
/// Key prefix of the acknowledgement rows; the voter identity follows.
pub const ACK_KEY_PREFIX: &[u8] = b"ack_shared_v1/";
/// Key of the published trim floor.
pub const FLOOR_KEY: &[u8] = b"trimmed_floor_v1";

/// One voter's durable statement that it holds a specific shared
/// checkpoint. Possession only: it authorizes nothing by itself, and it
/// becomes a trim vote only together with every other configured voter's
/// identical statement.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointAckV1 {
    /// Acknowledging voter.
    pub voter: ReplicaId,
    /// Cluster/restore identity of the checkpoint.
    pub cluster: ClusterId,
    /// Domain of the checkpoint.
    pub domain: DomainId,
    /// Configuration epoch the boundary was reached under.
    pub configuration: ConfigurationEpoch,
    /// Boundary the checkpoint closes.
    pub boundary: CheckpointBoundary,
    /// Verified root of the checkpoint the voter holds.
    pub root: Digest32,
}

impl CheckpointAckV1 {
    /// The acknowledgement a voter that exported or verified `manifest`
    /// writes. The manifest's own root is used, so a voter cannot
    /// acknowledge a root it did not compute.
    pub fn for_manifest(voter: ReplicaId, manifest: &SharedManifestV1) -> Self {
        CheckpointAckV1 {
            voter,
            cluster: manifest.cluster,
            domain: manifest.domain,
            configuration: manifest.configuration,
            boundary: manifest.boundary,
            root: manifest.root,
        }
    }

    /// The acknowledgement a node that completed an install (task-50)
    /// writes from its receipt.
    pub fn for_install(voter: ReplicaId, receipt: &InstalledCheckpointV1) -> Self {
        CheckpointAckV1 {
            voter,
            cluster: receipt.cluster,
            domain: receipt.domain,
            configuration: receipt.configuration,
            boundary: receipt.boundary,
            root: receipt.root,
        }
    }

    /// What must be identical across voters: the checkpoint itself.
    fn subject(&self) -> (ConfigurationEpoch, CheckpointBoundary, Digest32) {
        (self.configuration, self.boundary, self.root)
    }

    /// Encode as a `checkpoint_v1` row value.
    pub fn encode(&self) -> Result<Vec<u8>, EngineError> {
        encode_record(ACK_RECORD_KIND, self, "checkpoint acknowledgement encode")
    }

    /// Decode a `checkpoint_v1` row value.
    pub fn decode(bytes: &[u8]) -> Result<Self, EngineError> {
        decode_record(ACK_RECORD_KIND, bytes, "checkpoint acknowledgement")
    }
}

/// `checkpoint_v1` key of a voter's acknowledgement.
pub fn ack_key(voter: &ReplicaId) -> Vec<u8> {
    let mut key = Vec::with_capacity(ACK_KEY_PREFIX.len() + ReplicaId::LEN);
    key.extend_from_slice(ACK_KEY_PREFIX);
    key.extend_from_slice(voter.as_bytes());
    key
}

/// The update recording one voter's acknowledgement. The caller commits it
/// durably before the acknowledgement counts anywhere.
pub fn ack_update(ack: &CheckpointAckV1) -> Result<StoreUpdate, EngineError> {
    Ok(StoreUpdate {
        collection: Collection::CheckpointV1.id(),
        key: ack_key(&ack.voter),
        value: Some(ack.encode()?),
    })
}

/// The durably recorded acknowledgements, in voter order. A row whose key
/// does not carry a well-formed voter identity, or whose value does not
/// decode, or that disagrees with its own key, is corrupt: the floor is
/// never computed from a partially understood ledger.
pub fn read_acks<V: OrderedRead>(
    view: &V,
    limits: &TrimLimits,
) -> Result<Vec<CheckpointAckV1>, TrimError> {
    limits.validate()?;
    let mut out: Vec<CheckpointAckV1> = Vec::new();
    let mut resume: Option<Vec<u8>> = None;
    loop {
        let page = view.scan_page(
            Collection::CheckpointV1.id(),
            &ScanRequest {
                lower: Bound::Included(ACK_KEY_PREFIX.to_vec()),
                upper: Bound::Unbounded,
                direction: Direction::Forward,
                resume_after: resume.clone(),
                max_rows: limits.page_rows(),
                max_bytes: limits.page_bytes(),
            },
        )?;
        let mut past_prefix = false;
        for row in &page.rows {
            // The other `checkpoint_v1` records (the install receipt, the
            // published floor) sort after this prefix; reaching one ends the
            // ledger.
            let Some(suffix) = row.key.strip_prefix(ACK_KEY_PREFIX) else {
                past_prefix = true;
                break;
            };
            if out.len() as u32 >= limits.max_acknowledgements {
                return Err(TrimError::AcknowledgementBudget {
                    limit: limits.max_acknowledgements,
                });
            }
            let voter =
                ReplicaId::from_slice(suffix).map_err(|_| corrupt("acknowledgement key"))?;
            let ack = CheckpointAckV1::decode(&row.value)?;
            if ack.voter != voter {
                return Err(TrimError::Engine(corrupt(
                    "acknowledgement voter differs from its key",
                )));
            }
            out.push(ack);
        }
        match page.rows.last() {
            Some(last) if !past_prefix && !page.exhausted => resume = Some(last.key.clone()),
            _ => break,
        }
    }
    Ok(out)
}

/// The checkpoint every configured voter durably holds.
///
/// It exists only when the acknowledgements were unanimous and identical,
/// so it is safe to forget protocol state that produced everything at or
/// below its boundary: no permitted recovery can need that state to
/// reconstruct an outcome every voter already has.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrimFloor {
    /// Cluster/restore identity.
    pub cluster: ClusterId,
    /// Domain.
    pub domain: DomainId,
    /// Configuration epoch of the acknowledged boundary.
    pub configuration: ConfigurationEpoch,
    /// The acknowledged boundary.
    pub boundary: CheckpointBoundary,
    /// Root every voter acknowledged.
    pub root: Digest32,
    /// The exact voters that acknowledged it: the whole configured set.
    pub voters: BTreeSet<ReplicaId>,
}

impl TrimFloor {
    /// Execution position at or below which protocol state may be trimmed.
    pub fn execution_position(&self) -> ExecutionPosition {
        self.boundary.execution_position
    }

    /// The record published before any deletion.
    pub fn published(&self) -> TrimmedFloorV1 {
        TrimmedFloorV1 {
            cluster: self.cluster,
            domain: self.domain,
            configuration: self.configuration,
            boundary: self.boundary,
            root: self.root,
            voters: self.voters.len() as u32,
        }
    }
}

/// Establish the all-voter floor from durable acknowledgements.
///
/// `voters` is the exact configured voter set of the epoch, from committed
/// membership. Every voter must be present and every acknowledgement must
/// name the same cluster, domain, configuration, boundary and root; an
/// acknowledgement from a replica outside the set is refused rather than
/// ignored, so an observer or a departed voter cannot supply a trim vote
/// and a stale ledger cannot be mistaken for unanimity.
pub fn establish_floor(
    acks: &[CheckpointAckV1],
    voters: &BTreeSet<ReplicaId>,
    cluster: ClusterId,
    domain: DomainId,
) -> Result<TrimFloor, TrimError> {
    if voters.is_empty() {
        return Err(TrimError::NoVoters);
    }
    let mut held: BTreeMap<ReplicaId, &CheckpointAckV1> = BTreeMap::new();
    for ack in acks {
        if !voters.contains(&ack.voter) {
            return Err(TrimError::NonVoterAcknowledgement { replica: ack.voter });
        }
        if ack.cluster != cluster {
            return Err(TrimError::OriginMismatch {
                voter: ack.voter,
                field: "cluster",
            });
        }
        if ack.domain != domain {
            return Err(TrimError::OriginMismatch {
                voter: ack.voter,
                field: "domain",
            });
        }
        held.insert(ack.voter, ack);
    }
    let missing: Vec<ReplicaId> = voters
        .iter()
        .filter(|v| !held.contains_key(v))
        .copied()
        .collect();
    if !missing.is_empty() {
        return Err(TrimError::MissingAcknowledgements { missing });
    }
    // Voter order makes the reported divergence deterministic whatever
    // order the rows arrived in.
    let mut iter = held.values();
    let first = iter.next().expect("a non-empty voter set was acknowledged");
    let subject = first.subject();
    for ack in iter {
        if ack.subject() != subject {
            return Err(TrimError::DivergentAcknowledgement { voter: ack.voter });
        }
    }
    Ok(TrimFloor {
        cluster,
        domain,
        configuration: first.configuration,
        boundary: first.boundary,
        root: first.root,
        voters: voters.clone(),
    })
}

/// The floor durably published on this node: the fence that outlives a
/// crash in the middle of trimming, and the only thing that authorizes a
/// deletion.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrimmedFloorV1 {
    /// Cluster/restore identity.
    pub cluster: ClusterId,
    /// Domain.
    pub domain: DomainId,
    /// Configuration epoch of the acknowledged boundary.
    pub configuration: ConfigurationEpoch,
    /// The acknowledged boundary.
    pub boundary: CheckpointBoundary,
    /// Root every voter acknowledged.
    pub root: Digest32,
    /// Voters that acknowledged it (the whole configured set).
    pub voters: u32,
}

impl TrimmedFloorV1 {
    /// Encode as a `checkpoint_v1` row value.
    pub fn encode(&self) -> Result<Vec<u8>, EngineError> {
        encode_record(FLOOR_RECORD_KIND, self, "trim floor encode")
    }

    /// Decode a `checkpoint_v1` row value.
    pub fn decode(bytes: &[u8]) -> Result<Self, EngineError> {
        decode_record(FLOOR_RECORD_KIND, bytes, "trim floor")
    }
}

/// The floor this store has published, or `None` when nothing was ever
/// trimmed here. A corrupt record is an error, never an absence.
pub fn published_floor<V: OrderedRead>(view: &V) -> Result<Option<TrimmedFloorV1>, TrimError> {
    match view.get(Collection::CheckpointV1.id(), FLOOR_KEY)? {
        None => Ok(None),
        Some(bytes) => Ok(Some(TrimmedFloorV1::decode(&bytes)?)),
    }
}

/// The update publishing `floor`, checked against what is already
/// published.
///
/// The floor only ever moves forward. A late unanimous acknowledgement of
/// an older checkpoint, or of an older configuration, is refused
/// ([`TrimError::FloorRegressed`]) so delayed traffic cannot reopen state
/// this node already forgot, and a second floor at the same boundary with a
/// different root is a disagreement about history
/// ([`TrimError::FloorConflict`]), not a republication.
pub fn publish_floor(
    floor: &TrimFloor,
    published: Option<&TrimmedFloorV1>,
) -> Result<StoreUpdate, TrimError> {
    let next = floor.published();
    if let Some(current) = published {
        if current.cluster != next.cluster {
            return Err(TrimError::FloorOriginMismatch { field: "cluster" });
        }
        if current.domain != next.domain {
            return Err(TrimError::FloorOriginMismatch { field: "domain" });
        }
        if next.configuration < current.configuration
            || next.boundary.execution_position < current.boundary.execution_position
        {
            return Err(TrimError::FloorRegressed {
                published: current.boundary.execution_position,
                offered: next.boundary.execution_position,
            });
        }
        if next.configuration == current.configuration
            && next.boundary == current.boundary
            && next.root != current.root
        {
            return Err(TrimError::FloorConflict);
        }
    }
    Ok(StoreUpdate {
        collection: Collection::CheckpointV1.id(),
        key: FLOOR_KEY.to_vec(),
        value: Some(next.encode()?),
    })
}

/// Publish `floor` inside `txn`, checked against the floor the transaction
/// itself reads.
///
/// [`publish_floor`] compares against whatever `published` value the caller
/// read, and the engine applies a put unconditionally, so a caller that
/// planned from an older snapshot (a retry after an indeterminate commit is
/// the realistic one) could commit a floor below one that landed meanwhile,
/// and rows already deleted under the newer floor would lose their fence.
/// Reading `FLOOR_KEY` through the write transaction, which is the store's
/// only writer, makes the regression check hold at the moment of the write.
/// On an error nothing is written; the caller aborts or commits whatever
/// else the transaction holds.
pub fn publish_floor_in<T: WriteTxn>(txn: &mut T, floor: &TrimFloor) -> Result<(), TrimError> {
    let current = published_floor(txn)?;
    let update = publish_floor(floor, current.as_ref())?;
    let value = update
        .value
        .as_ref()
        .ok_or_else(|| corrupt("trim floor update without a value"))?;
    txn.put(update.collection, &update.key, value)?;
    Ok(())
}

/// What a delayed message may still do once a floor is published.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FenceDecision {
    /// Nothing about it lies below the floor; ordinary handling applies.
    Admit,
    /// It concerns state at or below the published floor. The protocol rows
    /// that would answer it may already be gone, so it is answered from the
    /// retained common outcome and never re-creates them.
    BelowFloor,
}

/// The fence a published floor puts in front of delayed traffic.
///
/// Trimming removes the bookkeeping of settled commands, so a message that
/// arrives afterwards must not be allowed to re-initialize them: without
/// this check a delayed PreAccept for a command executed long ago would
/// look like a brand-new command, and a message of a retired configuration
/// would look like live work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrimFence {
    floor: TrimmedFloorV1,
}

impl TrimFence {
    /// A fence over a published floor.
    pub fn new(floor: TrimmedFloorV1) -> Self {
        TrimFence { floor }
    }

    /// The floor being enforced.
    pub fn floor(&self) -> &TrimmedFloorV1 {
        &self.floor
    }

    /// A message of `epoch`: anything below the floor's configuration is
    /// below the floor, whatever it claims about commands.
    pub fn configuration(&self, epoch: ConfigurationEpoch) -> FenceDecision {
        if epoch < self.floor.configuration {
            FenceDecision::BelowFloor
        } else {
            FenceDecision::Admit
        }
    }

    /// A message under `ballot`.
    pub fn ballot(&self, ballot: &Ballot) -> FenceDecision {
        self.configuration(ballot.epoch)
    }

    /// A message about `command`: below the floor when the command is
    /// already executed at or below the floor's execution position. The
    /// answer comes from `executed_v1`, which trimming never touches, so it
    /// stays available after the command's protocol rows are gone.
    pub fn command<V: OrderedRead>(
        &self,
        view: &V,
        command: &CommandId,
    ) -> Result<FenceDecision, TrimError> {
        match view.get(Collection::ExecutedV1.id(), &codecs::executed_key(command))? {
            None => Ok(FenceDecision::Admit),
            Some(bytes) => {
                let record = codecs::decode_executed(&bytes)?;
                if record.position <= self.floor.boundary.execution_position {
                    Ok(FenceDecision::BelowFloor)
                } else {
                    Ok(FenceDecision::Admit)
                }
            }
        }
    }
}

/// Bounds of acknowledgement reading, backpressure accounting and one trim
/// step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrimLimits {
    /// Rows per engine page.
    pub page_rows: u32,
    /// Bytes per engine page.
    pub page_bytes: u32,
    /// Acknowledgement rows read before the ledger is refused.
    pub max_acknowledgements: u32,
    /// Protocol rows above which no new work is admitted while trimming is
    /// blocked. Strictly below `max_examined_rows`, so a store kept inside
    /// the backpressure bound can always be surveyed in one step.
    pub max_protocol_rows: u32,
    /// Protocol rows examined in one trim step.
    pub max_examined_rows: u32,
    /// Rows deleted in one trim step.
    pub max_deletions: u32,
}

impl Default for TrimLimits {
    fn default() -> Self {
        TrimLimits {
            page_rows: 1024,
            page_bytes: 1 << 20,
            max_acknowledgements: 1024,
            max_protocol_rows: 1 << 18,
            max_examined_rows: 1 << 20,
            max_deletions: 4096,
        }
    }
}

impl TrimLimits {
    /// Reject limits that would make a trim step unbounded, or that would
    /// let the store grow past what one step can survey.
    pub fn validate(&self) -> Result<(), TrimError> {
        if self.page_rows == 0
            || self.page_bytes == 0
            || self.max_acknowledgements == 0
            || self.max_protocol_rows == 0
            || self.max_deletions == 0
            || self.max_protocol_rows >= self.max_examined_rows
        {
            return Err(TrimError::InvalidLimits);
        }
        Ok(())
    }

    fn page_rows(&self) -> std::num::NonZeroU32 {
        std::num::NonZeroU32::new(self.page_rows.max(1)).expect("non-zero")
    }

    fn page_bytes(&self) -> std::num::NonZeroU32 {
        std::num::NonZeroU32::new(self.page_bytes.max(1)).expect("non-zero")
    }
}

/// Retained protocol state while trimming is blocked, and whether new work
/// may still be admitted.
///
/// Trimming that cannot run is backpressure, not a licence to evict: the
/// caller stops accepting new commands once the bound is reached and keeps
/// every obligation it already accepted (Section 5.3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrimBackpressure {
    /// Protocol rows counted, capped at `limit`.
    pub protocol_rows: u32,
    /// The bound.
    pub limit: u32,
    /// Voters whose acknowledgement is missing, in voter order.
    pub missing: Vec<ReplicaId>,
}

impl TrimBackpressure {
    /// Whether new work may still be accepted.
    pub fn admits_new_work(&self) -> bool {
        self.protocol_rows < self.limit
    }
}

/// Count the retained protocol rows against the bound.
///
/// The count stops at the bound, so the accounting itself stays bounded
/// however far behind the missing voter is.
pub fn trim_backpressure<V: OrderedRead>(
    view: &V,
    missing: &[ReplicaId],
    limits: &TrimLimits,
) -> Result<TrimBackpressure, TrimError> {
    limits.validate()?;
    let mut rows = 0u32;
    let mut resume: Option<Vec<u8>> = None;
    while rows < limits.max_protocol_rows {
        let page = view.scan_page(
            Collection::ProtocolV1.id(),
            &ScanRequest {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
                direction: Direction::Forward,
                resume_after: resume.clone(),
                max_rows: limits.page_rows(),
                max_bytes: limits.page_bytes(),
            },
        )?;
        rows = rows
            .saturating_add(page.rows.len() as u32)
            .min(limits.max_protocol_rows);
        match page.rows.last() {
            Some(last) if !page.exhausted => resume = Some(last.key.clone()),
            _ => break,
        }
    }
    let mut missing: Vec<ReplicaId> = missing.to_vec();
    missing.sort();
    missing.dedup();
    Ok(TrimBackpressure {
        protocol_rows: rows,
        limit: limits.max_protocol_rows,
        missing,
    })
}

/// One bounded trim step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrimPlan {
    /// Deletions to apply as one durable batch. Every entry targets
    /// `protocol_v1` and carries no value.
    pub updates: Vec<StoreUpdate>,
    /// Protocol rows examined.
    pub examined: u32,
    /// Rows eligible under the floor, including those beyond this batch.
    pub eligible: u32,
    /// Eligible rows held back because a retained command still names their
    /// command as a direct dependency.
    pub pinned: u32,
    /// Rows retained: promises, Sync selections, unresolved or
    /// above-boundary commands, pins and unrecognized rows.
    pub retained: u32,
    /// Whether every eligible row is in this batch.
    pub done: bool,
}

/// Compute one bounded trim step against a published floor.
///
/// The step surveys the protocol rows of every configuration epoch at or
/// below the floor's, decides each row locally, and only then emits
/// deletions, so a command pinned by a retained dependency is never deleted
/// because it happened to be scanned first. The survey must complete: a
/// range larger than `max_examined_rows` stops the step with
/// [`TrimError::ExaminationBudget`] and deletes nothing, which is why the
/// backpressure bound is required to be strictly smaller.
///
/// The floor must already be durable in `view`: a store whose published
/// floor is absent, or below `floor`, is refused with
/// [`TrimError::FloorNotDurable`] and nothing is planned. Deletions planned
/// here therefore always follow their fence into the store; a crash after
/// them finds the floor that rejects delayed traffic for what they removed.
///
/// Deleting the batch is idempotent and retryable: the rows are gone, so
/// the next step surveys what is left. A crash between the published floor
/// and the deletions leaves the floor in force and the rows still there.
pub fn plan_trim<V: OrderedRead>(
    view: &V,
    floor: &TrimmedFloorV1,
    limits: &TrimLimits,
) -> Result<TrimPlan, TrimError> {
    limits.validate()?;
    check_store_origin(view, floor)?;
    check_floor_durable(view, floor)?;
    let boundary = floor.boundary.execution_position;
    // Every epoch at or below the floor's, through its Sync rows; epochs
    // above the floor are outside the acknowledged history entirely.
    let mut upper = floor.configuration.to_be_bytes().to_vec();
    upper.push(SYNC_TAG + 1);

    let mut candidates: Vec<(Vec<u8>, CommandId)> = Vec::new();
    let mut pins: BTreeSet<CommandId> = BTreeSet::new();
    // Dependencies of every command seen, so a pin can be closed over
    // them. Pinning only the direct dependencies of retained commands
    // left a retained command U depending on executed B, whose own
    // dependency A was deleted: closure traversal after a restart stops
    // at the missing A.
    let mut deps_of: BTreeMap<CommandId, Vec<CommandId>> = BTreeMap::new();
    let mut examined = 0u32;
    let mut retained = 0u32;
    let mut resume: Option<Vec<u8>> = None;
    loop {
        let page = view.scan_page(
            Collection::ProtocolV1.id(),
            &ScanRequest {
                lower: Bound::Unbounded,
                upper: Bound::Excluded(upper.clone()),
                direction: Direction::Forward,
                resume_after: resume.clone(),
                max_rows: limits.page_rows(),
                max_bytes: limits.page_bytes(),
            },
        )?;
        for row in &page.rows {
            examined += 1;
            if examined > limits.max_examined_rows {
                return Err(TrimError::ExaminationBudget {
                    limit: limits.max_examined_rows,
                });
            }
            match classify(view, row.key.as_slice(), &row.value, boundary)? {
                Verdict::Retain => retained += 1,
                Verdict::RetainPinning(deps) => {
                    retained += 1;
                    pins.extend(deps);
                }
                Verdict::Eligible(command, deps) => {
                    // An eligible row's own dependencies are recorded even
                    // though it is a candidate: a command pinned later may
                    // depend on it, and closure has to continue through it.
                    deps_of.entry(command).or_insert(deps);
                    candidates.push((row.key.clone(), command));
                }
            }
        }
        match page.rows.last() {
            Some(last) if !page.exhausted => resume = Some(last.key.clone()),
            _ => break,
        }
    }

    // Close the pins over the dependency graph: whatever a pinned command
    // needs is needed too, however deep.
    let mut frontier: Vec<CommandId> = pins.iter().copied().collect();
    while let Some(command) = frontier.pop() {
        let Some(deps) = deps_of.get(&command) else {
            continue;
        };
        for dep in deps.clone() {
            if pins.insert(dep) {
                frontier.push(dep);
            }
        }
    }

    let mut updates = Vec::new();
    let mut eligible = 0u32;
    let mut pinned = 0u32;
    for (key, command) in candidates {
        if pins.contains(&command) {
            pinned += 1;
            retained += 1;
            continue;
        }
        eligible += 1;
        if updates.len() as u32 >= limits.max_deletions {
            continue;
        }
        updates.push(StoreUpdate {
            collection: Collection::ProtocolV1.id(),
            key,
            value: None,
        });
    }
    let done = eligible == updates.len() as u32;
    Ok(TrimPlan {
        updates,
        examined,
        eligible,
        pinned,
        retained,
        done,
    })
}

/// The floor must belong to this store. A floor of another cluster or
/// domain authorizes nothing here, whatever it says about unanimity.
fn check_store_origin<V: OrderedRead>(view: &V, floor: &TrimmedFloorV1) -> Result<(), TrimError> {
    let meta = Collection::MetaV1.id();
    let cluster = view
        .get(meta, coord_store_api::registry::meta_fields::CLUSTER_ID)?
        .ok_or(TrimError::FloorOriginMismatch { field: "cluster" })?;
    if cluster.as_slice() != floor.cluster.as_bytes() {
        return Err(TrimError::FloorOriginMismatch { field: "cluster" });
    }
    let domain = view
        .get(meta, coord_store_api::registry::meta_fields::DOMAIN_ID)?
        .ok_or(TrimError::FloorOriginMismatch { field: "domain" })?;
    if domain.as_slice() != floor.domain.as_bytes() {
        return Err(TrimError::FloorOriginMismatch { field: "domain" });
    }
    Ok(())
}

/// The store must already hold a published floor that covers `floor`: same
/// origin, a configuration and execution position at or above it, and the
/// same root when it names the same boundary. A durable floor above the
/// offered one is fine, since planning under the lower one deletes less.
fn check_floor_durable<V: OrderedRead>(view: &V, floor: &TrimmedFloorV1) -> Result<(), TrimError> {
    let Some(durable) = published_floor(view)? else {
        return Err(TrimError::FloorNotDurable);
    };
    if durable.cluster != floor.cluster {
        return Err(TrimError::FloorOriginMismatch { field: "cluster" });
    }
    if durable.domain != floor.domain {
        return Err(TrimError::FloorOriginMismatch { field: "domain" });
    }
    if durable.configuration < floor.configuration
        || durable.boundary.execution_position < floor.boundary.execution_position
    {
        return Err(TrimError::FloorNotDurable);
    }
    if durable.configuration == floor.configuration
        && durable.boundary == floor.boundary
        && durable.root != floor.root
    {
        return Err(TrimError::FloorConflict);
    }
    Ok(())
}

/// What one protocol row is for trimming.
enum Verdict {
    /// Keep it, and keep whatever it depends on.
    RetainPinning(Vec<CommandId>),
    /// Keep it.
    Retain,
    /// It belongs to a command executed at or below the floor, with the
    /// dependencies that command records: closure runs through it even
    /// though the row itself may go.
    Eligible(CommandId, Vec<CommandId>),
}

/// Decide one `protocol_v1` row. Anything whose tag or key shape this build
/// does not recognize is retained: an unreadable row is never a licence to
/// delete it.
fn classify<V: OrderedRead>(
    view: &V,
    key: &[u8],
    value: &[u8],
    boundary: ExecutionPosition,
) -> Result<Verdict, TrimError> {
    let Some(&tag) = key.get(8) else {
        return Ok(Verdict::Retain);
    };
    match tag {
        // A dependency row is eligible only when `executed_v1` places the
        // command's execution at or below the boundary. Anything else is an
        // obligation. The row's own phase says nothing either way: the
        // durable row stays at `Accept` or `Commit` after execution, which
        // is recorded only in `executed_v1`.
        DEPENDENCY_TAG if key.len() == 41 => {
            let command = command_of(key)?;
            let record = decode_dependency(value)?;
            if executed_within(view, &command, boundary)? {
                Ok(Verdict::Eligible(command, record.deps))
            } else {
                Ok(Verdict::RetainPinning(record.deps))
            }
        }
        // A proposal row follows its command: the leader must be able to
        // resume a proposal that is not yet settled.
        PROPOSAL_TAG if key.len() == 41 => {
            let command = command_of(key)?;
            let proposal_deps = decode_proposal(value)?.deps;
            if executed_within(view, &command, boundary)? {
                Ok(Verdict::Eligible(command, proposal_deps))
            } else {
                Ok(Verdict::RetainPinning(proposal_deps))
            }
        }
        // Promise rows (tag 0x00) and Sync rows: never trimmed. A forgotten
        // promise lets a delayed lower ballot be voted, and a bound Sync
        // must be reused after a crash, never reselected.
        _ => Ok(Verdict::Retain),
    }
}

fn command_of(key: &[u8]) -> Result<CommandId, TrimError> {
    let mut id = [0u8; 32];
    id.copy_from_slice(key.get(9..41).ok_or_else(|| corrupt("protocol row key"))?);
    Ok(CommandId(Digest32(id)))
}

/// Whether `command` is executed at or below the boundary. `executed_v1` is
/// common state the trim never touches, so this answer survives the trim.
fn executed_within<V: OrderedRead>(
    view: &V,
    command: &CommandId,
    boundary: ExecutionPosition,
) -> Result<bool, TrimError> {
    match view.get(Collection::ExecutedV1.id(), &codecs::executed_key(command))? {
        None => Ok(false),
        Some(bytes) => Ok(codecs::decode_executed(&bytes)?.position <= boundary),
    }
}

/// Why trimming was refused or stopped. Every variant leaves the store
/// exactly as it was: nothing is deleted on a doubt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TrimError {
    /// The engine failed, or a record did not decode.
    Engine(EngineError),
    /// The configuration names no voters, so unanimity is meaningless.
    NoVoters,
    /// A replica outside the configured voter set acknowledged the
    /// checkpoint. Observers hold catch-up state and supply no trim votes.
    NonVoterAcknowledgement {
        /// The acknowledging replica.
        replica: ReplicaId,
    },
    /// An acknowledgement names another cluster or domain.
    OriginMismatch {
        /// Acknowledging voter.
        voter: ReplicaId,
        /// Which field.
        field: &'static str,
    },
    /// Voters acknowledged different checkpoints; nothing is unanimous.
    DivergentAcknowledgement {
        /// The first voter, in voter order, disagreeing with the rest.
        voter: ReplicaId,
    },
    /// Some configured voter has not acknowledged. Trimming waits; the
    /// caller applies backpressure and evicts nothing.
    MissingAcknowledgements {
        /// The voters still owed, in voter order.
        missing: Vec<ReplicaId>,
    },
    /// The offered floor lies below the published one. Delayed traffic
    /// cannot move the floor back down.
    FloorRegressed {
        /// Published boundary.
        published: ExecutionPosition,
        /// Offered boundary.
        offered: ExecutionPosition,
    },
    /// Two different roots for the same boundary in the same configuration:
    /// a disagreement about history, not a republication.
    FloorConflict,
    /// The store holds no published floor at or above the one offered for
    /// trimming. Deletions are planned only behind a durable fence.
    FloorNotDurable,
    /// The floor names another cluster or domain than the published one, or
    /// than the store it would be applied to.
    FloorOriginMismatch {
        /// Which field.
        field: &'static str,
    },
    /// More acknowledgement rows than the ledger bound.
    AcknowledgementBudget {
        /// The bound.
        limit: u32,
    },
    /// More protocol rows than one step may survey, so no row can be shown
    /// to be unpinned. Nothing is deleted.
    ExaminationBudget {
        /// The bound.
        limit: u32,
    },
    /// Limits that would make a step unbounded, or that allow the store to
    /// outgrow one survey.
    InvalidLimits,
}

impl fmt::Display for TrimError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrimError::Engine(e) => write!(f, "engine: {e}"),
            other => write!(f, "{other:?}"),
        }
    }
}

impl std::error::Error for TrimError {}

impl From<EngineError> for TrimError {
    fn from(e: EngineError) -> Self {
        TrimError::Engine(e)
    }
}

fn corrupt(what: &'static str) -> EngineError {
    EngineError::new(ErrorClass::Corrupt, what)
}

fn encode_record<T: Serialize>(
    kind: u16,
    value: &T,
    what: &'static str,
) -> Result<Vec<u8>, EngineError> {
    let payload =
        postcard::to_allocvec(value).map_err(|_| EngineError::new(ErrorClass::Limit, what))?;
    StoreEnvelopeV1 {
        record_kind: kind,
        schema_version: TRIM_SCHEMA_VERSION,
        payload,
    }
    .encode()
}

fn decode_record<T: for<'de> Deserialize<'de>>(
    kind: u16,
    bytes: &[u8],
    what: &'static str,
) -> Result<T, EngineError> {
    let env = StoreEnvelopeV1::decode(bytes)?;
    if env.record_kind != kind || env.schema_version != TRIM_SCHEMA_VERSION {
        return Err(corrupt(what));
    }
    let (value, rest): (T, &[u8]) =
        postcard::take_from_bytes(&env.payload).map_err(|_| corrupt(what))?;
    if !rest.is_empty() {
        return Err(corrupt(what));
    }
    Ok(value)
}
