//! Quorum-certified checkpoint floors, durably (task-53; design Sections
//! 5.3, 17.6, 17.16.5).
//!
//! [`coord_consensus::floor`] holds the rules; this holds the rows, and
//! the order they become durable in. Task-51's all-voter floor stays
//! exactly as it was -- it is the conservative path and nothing here
//! weakens it -- and everything after the floor is established is shared
//! with it: the same [`TrimmedFloorV1`] fence, the same [`plan_trim`],
//! the same deletions. What changes is how the floor comes to exist.
//!
//! Three durable pieces, in this order:
//!
//! 1. [`CheckpointReadinessV1`], one per voter. A voter writes it only
//!    once it durably holds the checkpoint the record names, and writing
//!    it is a promise: this voter will never again vote from a baseline
//!    below that boundary. [`record_readiness`] is the only way to write
//!    one, and it applies the promise rules against the row already
//!    there -- a promise moves up, never down, and never holds two
//!    subjects at one position.
//! 2. [`ActivatedFloorV1`], the certificate: a majority of the
//!    configuration's voters, all ready for the same subject. It names
//!    its signers, because a floor's authority is the promises behind it
//!    and an operator reading one has to be able to see whose they were.
//! 3. The [`TrimmedFloorV1`] the certificate yields, published before
//!    the first deletion exactly as before.
//!
//! What a recovery does is read promises, not certificates.
//! [`recovery_obligation`] takes the readiness a majority of the voters
//! reported and says whether this replica may vote: a signer promised
//! before any certificate existed and keeps the promise whether or not
//! it ever saw one, so the promises are the evidence that is guaranteed
//! to be there. Refusing to answer from fewer than a majority of
//! distinct voters is not caution, it is the rule -- a narrower read can
//! miss the highest floor entirely, and the replica that missed it would
//! vote from a baseline the cluster has already forgotten below.
//!
//! [`plan_trim`]: crate::trim::plan_trim
//! [`TrimmedFloorV1`]: crate::trim::TrimmedFloorV1

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use coord_consensus::floor::{
    ActivationError, FloorCandidate, Readiness, ReadinessError, ReadinessLedger, activate, discover,
};
use coord_consensus::quorum::EpochVoters;
use coord_core::effect::StoreUpdate;
use coord_store_api::engine::{Direction, EngineError, OrderedRead, ScanRequest};
use coord_store_api::registry::Collection;
use coord_types::identity::{Digest32, HashDomain};
use coord_types::ids::{ClusterId, ConfigurationEpoch, DomainId, ExecutionPosition, ReplicaId};
use serde::{Deserialize, Serialize};

use crate::install::InstalledCheckpointV1;
use crate::manifest::{CheckpointBoundary, SharedManifestV1};
use crate::trim::{TrimError, TrimFloor, TrimLimits, corrupt, decode_record, encode_record};

/// Record kind of a voter's durable readiness in `checkpoint_v1`.
pub const READINESS_RECORD_KIND: u16 = 0x0004;
/// Record kind of the published activation certificate.
pub const ACTIVATION_RECORD_KIND: u16 = 0x0005;
/// Key prefix of the readiness rows; the voter identity follows.
pub const READINESS_KEY_PREFIX: &[u8] = b"ready_shared_v1/";
/// Key of the published activation certificate.
pub const ACTIVATION_KEY: &[u8] = b"activated_floor_v1";

/// Bytes a floor subject is derived from: the whole checkpoint identity,
/// at fixed widths.
const SUBJECT_LEN: usize = 16 + 16 + 8 + 8 + 8 + 8 + 8 + 32;

/// One voter's durable promise about a checkpoint it holds.
///
/// Strictly more than [`CheckpointAckV1`], and the difference is the
/// whole point. That record says "I have these bytes"; this one says "I
/// have these bytes, and I will never again vote from below this
/// boundary". A certificate built from possession binds nobody: the
/// holder crashes, comes back with its old baseline, and votes from
/// history the cluster has agreed to forget.
///
/// [`CheckpointAckV1`]: crate::trim::CheckpointAckV1
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointReadinessV1 {
    /// The promising voter.
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

impl CheckpointReadinessV1 {
    /// The promise a voter that exported or verified `manifest` makes.
    /// The manifest's own root is used, so a voter cannot promise about
    /// a root it did not compute.
    pub fn for_manifest(voter: ReplicaId, manifest: &SharedManifestV1) -> Self {
        CheckpointReadinessV1 {
            voter,
            cluster: manifest.cluster,
            domain: manifest.domain,
            configuration: manifest.configuration,
            boundary: manifest.boundary,
            root: manifest.root,
        }
    }

    /// The promise a node that completed an install (task-50) makes from
    /// its receipt.
    pub fn for_install(voter: ReplicaId, receipt: &InstalledCheckpointV1) -> Self {
        CheckpointReadinessV1 {
            voter,
            cluster: receipt.cluster,
            domain: receipt.domain,
            configuration: receipt.configuration,
            boundary: receipt.boundary,
            root: receipt.root,
        }
    }

    /// The subject every signer must agree on exactly.
    ///
    /// A fixed 104-byte encoding under its own hash domain, written out
    /// here rather than taken from an encoder: signers compute it
    /// independently and compare the results, so what it is derived from
    /// has to be a decision rather than a consequence of how a struct
    /// happens to serialize today.
    pub fn subject(&self) -> Digest32 {
        let mut bytes = [0u8; SUBJECT_LEN];
        bytes[0..16].copy_from_slice(self.cluster.as_bytes());
        bytes[16..32].copy_from_slice(self.domain.as_bytes());
        bytes[32..40].copy_from_slice(&self.configuration.to_be_bytes());
        bytes[40..48].copy_from_slice(&self.boundary.execution_position.to_be_bytes());
        bytes[48..56].copy_from_slice(&self.boundary.kv_revision.to_be_bytes());
        bytes[56..64].copy_from_slice(&self.boundary.retention_floor.to_be_bytes());
        bytes[64..72].copy_from_slice(&self.boundary.lease_authority.to_be_bytes());
        bytes[72..104].copy_from_slice(&self.root.0);
        HashDomain::CheckpointFloorSubject.digest(&[&bytes])
    }

    /// What this promise is, in the consensus vocabulary.
    pub fn candidate(&self) -> FloorCandidate {
        FloorCandidate {
            epoch: self.configuration,
            position: self.boundary.execution_position,
            subject: self.subject(),
        }
    }

    /// Encode as a `checkpoint_v1` row value.
    pub fn encode(&self) -> Result<Vec<u8>, EngineError> {
        encode_record(READINESS_RECORD_KIND, self, "checkpoint readiness encode")
    }

    /// Decode a `checkpoint_v1` row value.
    pub fn decode(bytes: &[u8]) -> Result<Self, EngineError> {
        decode_record(READINESS_RECORD_KIND, bytes, "checkpoint readiness")
    }
}

/// `checkpoint_v1` key of one voter's readiness.
pub fn readiness_key(voter: &ReplicaId) -> Vec<u8> {
    let mut key = Vec::with_capacity(READINESS_KEY_PREFIX.len() + ReplicaId::LEN);
    key.extend_from_slice(READINESS_KEY_PREFIX);
    key.extend_from_slice(voter.as_bytes());
    key
}

/// One voter's durable readiness, if it has promised.
pub fn own_readiness<V: OrderedRead>(
    view: &V,
    voter: &ReplicaId,
) -> Result<Option<CheckpointReadinessV1>, TrimError> {
    match view.get(Collection::CheckpointV1.id(), &readiness_key(voter))? {
        None => Ok(None),
        Some(bytes) => {
            let record = CheckpointReadinessV1::decode(&bytes)?;
            if &record.voter != voter {
                return Err(TrimError::Engine(corrupt(
                    "readiness voter differs from its key",
                )));
            }
            Ok(Some(record))
        }
    }
}

/// The update recording `offered` as this voter's promise.
///
/// The rules are applied against the row already there rather than
/// assumed: a voter that could lower its own promise, or hold two
/// subjects at one boundary, would undo the two properties the whole
/// protocol rests on. The caller commits the update durably *before* the
/// promise is told to anybody, because a promise that is not durable is
/// not a promise.
///
/// Re-recording the promise already held succeeds and rewrites the same
/// row, which is what makes a retry after a lost reply safe.
pub fn record_readiness<V: OrderedRead>(
    view: &V,
    voters: &EpochVoters,
    offered: &CheckpointReadinessV1,
) -> Result<StoreUpdate, TrimError> {
    let held = own_readiness(view, &offered.voter)?;
    let mut ledger =
        ReadinessLedger::recovered(offered.voter, voters.epoch(), held.map(|h| h.candidate()));
    ledger
        .record(voters, offered.candidate())
        .map_err(|e| readiness_error(offered.voter, offered.boundary.execution_position, e))?;
    Ok(StoreUpdate {
        collection: Collection::CheckpointV1.id(),
        key: readiness_key(&offered.voter),
        value: Some(offered.encode()?),
    })
}

fn readiness_error(voter: ReplicaId, offered: ExecutionPosition, e: ReadinessError) -> TrimError {
    match e {
        ReadinessError::EpochMismatch => TrimError::FloorOriginMismatch {
            field: "configuration",
        },
        ReadinessError::NotAVoter => TrimError::NonVoterReadiness { replica: voter },
        ReadinessError::Regression { held } => TrimError::ReadinessRegressed {
            held: held.position,
            offered,
        },
        ReadinessError::Competing { held } => TrimError::ReadinessCompeting {
            position: held.position,
        },
    }
}

/// The readiness rows this node durably holds, in voter order.
///
/// A row whose key does not carry a well-formed voter identity, whose
/// value does not decode, or that disagrees with its own key is corrupt:
/// a floor is never certified from a partially understood ledger.
pub fn read_readiness<V: OrderedRead>(
    view: &V,
    limits: &TrimLimits,
) -> Result<Vec<CheckpointReadinessV1>, TrimError> {
    limits.validate()?;
    let mut out: Vec<CheckpointReadinessV1> = Vec::new();
    let mut resume: Option<Vec<u8>> = None;
    loop {
        let page = view.scan_page(
            Collection::CheckpointV1.id(),
            &ScanRequest {
                lower: Bound::Included(READINESS_KEY_PREFIX.to_vec()),
                upper: Bound::Unbounded,
                direction: Direction::Forward,
                resume_after: resume.clone(),
                max_rows: limits.page_rows(),
                max_bytes: limits.page_bytes(),
            },
        )?;
        let mut past_prefix = false;
        for row in &page.rows {
            let Some(suffix) = row.key.strip_prefix(READINESS_KEY_PREFIX) else {
                past_prefix = true;
                break;
            };
            if out.len() as u32 >= limits.max_acknowledgements {
                return Err(TrimError::AcknowledgementBudget {
                    limit: limits.max_acknowledgements,
                });
            }
            let voter = ReplicaId::from_slice(suffix).map_err(|_| corrupt("readiness key"))?;
            let record = CheckpointReadinessV1::decode(&row.value)?;
            if record.voter != voter {
                return Err(TrimError::Engine(corrupt(
                    "readiness voter differs from its key",
                )));
            }
            out.push(record);
        }
        match page.rows.last() {
            Some(last) if !past_prefix && !page.exhausted => resume = Some(last.key.clone()),
            _ => break,
        }
    }
    Ok(out)
}

/// A floor a majority of the configuration's voters has certified.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivatedFloorV1 {
    /// Cluster/restore identity.
    pub cluster: ClusterId,
    /// Domain.
    pub domain: DomainId,
    /// Configuration epoch of the certified boundary.
    pub configuration: ConfigurationEpoch,
    /// The certified boundary.
    pub boundary: CheckpointBoundary,
    /// Root every signer promised about.
    pub root: Digest32,
    /// The voters that promised. Identities, not a count: a floor's
    /// authority is the promises behind it.
    pub signers: BTreeSet<ReplicaId>,
}

impl ActivatedFloorV1 {
    /// The floor this certificate authorizes, in the form task-51's
    /// publication, fence and trimming already take.
    pub fn trim_floor(&self) -> TrimFloor {
        TrimFloor {
            cluster: self.cluster,
            domain: self.domain,
            configuration: self.configuration,
            boundary: self.boundary,
            root: self.root,
            voters: self.signers.clone(),
        }
    }

    /// Encode as a `checkpoint_v1` row value.
    pub fn encode(&self) -> Result<Vec<u8>, EngineError> {
        encode_record(
            ACTIVATION_RECORD_KIND,
            self,
            "activation certificate encode",
        )
    }

    /// Decode a `checkpoint_v1` row value.
    pub fn decode(bytes: &[u8]) -> Result<Self, EngineError> {
        decode_record(ACTIVATION_RECORD_KIND, bytes, "activation certificate")
    }
}

/// Certify a floor from durable readiness.
///
/// `voters` is the exact configured voter set of the epoch, from
/// committed membership. Readiness from a replica outside it is refused
/// rather than ignored, so an observer or a departed voter supplies no
/// signature; readiness naming another cluster or domain is refused for
/// the same reason. A majority certifies, and the whole of what makes
/// that safe is that a recovery reads a majority too.
pub fn activate_floor(
    readiness: &[CheckpointReadinessV1],
    voters: &EpochVoters,
    cluster: ClusterId,
    domain: DomainId,
) -> Result<ActivatedFloorV1, TrimError> {
    if voters.voters().is_empty() {
        return Err(TrimError::NoVoters);
    }
    // One row per voter, in voter order, so a divergence is reported the
    // same way whatever order the rows arrived in.
    let mut held: BTreeMap<ReplicaId, &CheckpointReadinessV1> = BTreeMap::new();
    for record in readiness {
        if !voters.is_voter(&record.voter) {
            return Err(TrimError::NonVoterReadiness {
                replica: record.voter,
            });
        }
        if record.cluster != cluster {
            return Err(TrimError::OriginMismatch {
                voter: record.voter,
                field: "cluster",
            });
        }
        if record.domain != domain {
            return Err(TrimError::OriginMismatch {
                voter: record.voter,
                field: "domain",
            });
        }
        held.insert(record.voter, record);
    }
    // The candidate with the most promises behind it. A voter ready for
    // a later floor is not evidence for an earlier one, so the rows are
    // grouped rather than merged.
    let mut groups: BTreeMap<FloorCandidate, Vec<&CheckpointReadinessV1>> = BTreeMap::new();
    for record in held.values() {
        groups.entry(record.candidate()).or_default().push(record);
    }
    let mut best: Option<ActivatedFloorV1> = None;
    let mut shortfall: Option<TrimError> = None;
    for (candidate, records) in &groups {
        let offered: Vec<Readiness> = records
            .iter()
            .map(|r| Readiness {
                voter: r.voter,
                candidate: *candidate,
            })
            .collect();
        match activate(voters, &offered) {
            Ok(_) => {
                let first = records[0];
                let certified = ActivatedFloorV1 {
                    cluster,
                    domain,
                    configuration: first.configuration,
                    boundary: first.boundary,
                    root: first.root,
                    signers: records.iter().map(|r| r.voter).collect(),
                };
                // Certifying the highest is not a choice between
                // competing floors -- at most one subject per position
                // can ever be certified -- it is picking the furthest of
                // a chain that all hold.
                if best.as_ref().is_none_or(|b| {
                    b.boundary.execution_position < certified.boundary.execution_position
                }) {
                    best = Some(certified);
                }
            }
            Err(e) => {
                if shortfall.is_none() {
                    shortfall = Some(activation_error(e));
                }
            }
        }
    }
    best.ok_or_else(|| {
        shortfall.unwrap_or(TrimError::NoQuorum {
            have: 0,
            need: voters.majority(),
        })
    })
}

fn activation_error(e: ActivationError) -> TrimError {
    match e {
        ActivationError::Empty => TrimError::NoQuorum { have: 0, need: 1 },
        ActivationError::EpochMismatch => TrimError::FloorOriginMismatch {
            field: "configuration",
        },
        ActivationError::NotAVoter { replica } => TrimError::NonVoterReadiness { replica },
        ActivationError::Divided => TrimError::FloorConflict,
        ActivationError::NoQuorum { have, need } => TrimError::NoQuorum { have, need },
    }
}

/// The published certificate, if this node has one.
pub fn published_activation<V: OrderedRead>(
    view: &V,
) -> Result<Option<ActivatedFloorV1>, TrimError> {
    match view.get(Collection::CheckpointV1.id(), ACTIVATION_KEY)? {
        None => Ok(None),
        Some(bytes) => Ok(Some(ActivatedFloorV1::decode(&bytes)?)),
    }
}

/// The update publishing `next`, checked against what is already there.
///
/// The certificate moves forward only. A late certificate for an older
/// floor is ordinary traffic and is refused rather than applied, and a
/// second certificate at the same boundary with a different root is a
/// disagreement about history -- which the promise rules make
/// impossible, so finding one is a reason to stop rather than to choose.
pub fn publish_activation(
    next: &ActivatedFloorV1,
    published: Option<&ActivatedFloorV1>,
) -> Result<StoreUpdate, TrimError> {
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
        if next.boundary.execution_position == current.boundary.execution_position
            && next.root != current.root
        {
            return Err(TrimError::FloorConflict);
        }
    }
    Ok(StoreUpdate {
        collection: Collection::CheckpointV1.id(),
        key: ACTIVATION_KEY.to_vec(),
        value: Some(next.encode()?),
    })
}

/// What a recovering replica owes before it votes again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryObligation {
    /// Nothing. This replica's own executed prefix already covers the
    /// highest floor a majority reported -- or nobody has promised
    /// anything yet.
    None,
    /// Obtain the checkpoint `subject` names before voting. Below the
    /// floor this replica holds history the cluster has agreed to
    /// forget, so its votes would come from a discarded baseline.
    Install {
        /// The executed prefix it must reach.
        position: ExecutionPosition,
        /// Which checkpoint to obtain.
        subject: Digest32,
    },
    /// A majority reports promises at one position for checkpoints they
    /// do not agree on, and nothing here certifies which. At most one of
    /// them can ever be certified, so this is not a choice to make: the
    /// replica stops rather than guessing.
    Ambiguous {
        /// The contested position.
        position: ExecutionPosition,
    },
}

/// What `reports` -- the readiness of a majority of `voters` -- means for
/// a replica whose own executed prefix is `executed_through`.
///
/// Fewer than a majority of distinct voters is refused with
/// [`TrimError::NoQuorum`], and that refusal is the protocol rather than
/// caution: activation needed a majority, two majorities of one set
/// intersect, and a narrower read has no such guarantee. A replica that
/// answered from one report could miss the highest floor entirely and
/// then vote from below it.
pub fn recovery_obligation(
    voters: &EpochVoters,
    reports: &[CheckpointReadinessV1],
    executed_through: ExecutionPosition,
) -> Result<RecoveryObligation, TrimError> {
    let reporting: BTreeSet<ReplicaId> = reports
        .iter()
        .map(|r| r.voter)
        .filter(|v| voters.is_voter(v))
        .collect();
    let need = voters.majority();
    if reporting.len() < need {
        return Err(TrimError::NoQuorum {
            have: reporting.len(),
            need,
        });
    }
    let promises: Vec<Readiness> = reports
        .iter()
        .filter(|r| voters.is_voter(&r.voter))
        .map(|r| Readiness {
            voter: r.voter,
            candidate: r.candidate(),
        })
        .collect();
    let Some(discovered) = discover(&promises) else {
        return Ok(RecoveryObligation::None);
    };
    if executed_through >= discovered.position {
        return Ok(RecoveryObligation::None);
    }
    match discovered.subject() {
        Some(subject) => Ok(RecoveryObligation::Install {
            position: discovered.position,
            subject,
        }),
        None => Ok(RecoveryObligation::Ambiguous {
            position: discovered.position,
        }),
    }
}
