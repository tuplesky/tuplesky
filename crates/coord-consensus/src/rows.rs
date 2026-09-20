//! Source-required protocol rows of `protocol_v1` (design Sections 5.2,
//! 17.1): the promise record of an epoch. Values are `StoreEnvelopeV1`
//! payloads; keys are epoch-prefixed so an epoch's rows are contiguous.

use alloc::vec::Vec;

use coord_core::capability::{AdmissionFacts, admission_digest};
use coord_core::effect::StoreUpdate;
use coord_store_api::engine::{EngineError, ErrorClass};
use coord_store_api::envelope::StoreEnvelopeV1;
use coord_store_api::registry::Collection;
use coord_types::identity::Digest32;
use coord_types::ids::{Ballot, ConfigurationEpoch};
use coord_types::{CommandId, RetryKey};

use crate::commands::CommandRecord;
use crate::recovery::SyncDecision;
use serde::{Deserialize, Serialize};

/// Record kind of the promise row.
pub const PROMISE_KIND: u16 = 0x0001;
/// Record kind of a command dependency row.
pub const DEPENDENCY_KIND: u16 = 0x0002;
/// Key tag of the promise row within an epoch.
const PROMISE_TAG: u8 = 0x00;
/// Key tag of command dependency rows within an epoch.
pub const DEPENDENCY_TAG: u8 = 0x01;
/// Record kind of a leader proposal row.
pub const PROPOSAL_KIND: u16 = 0x0003;
/// Key tag of leader proposal rows within an epoch.
pub const PROPOSAL_TAG: u8 = 0x02;
/// Record kind of a payload row in `payload_v1`.
pub const PAYLOAD_KIND: u16 = 0x0001;
/// Record kind of a bound Sync selection row.
pub const SYNC_KIND: u16 = 0x0004;
/// Key tag of Sync rows within an epoch.
pub const SYNC_TAG: u8 = 0x03;
/// Record kind of the old-configuration seal row (task-55).
pub const SEAL_KIND: u16 = 0x0005;
/// Key tag of the seal row within an epoch.
///
/// After the Sync tag on purpose: trimming surveys an epoch's rows up to
/// and excluding this tag, so a seal is outside every trim step's range
/// by construction rather than by a rule someone has to remember.
pub const SEAL_TAG: u8 = 0x04;

/// The durable promise of one replica in one epoch: the highest ballot it
/// promised (no lower ballot is voted after it) and the ballot it last
/// synchronized (`cballot`, the source of state it reports in recovery).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PromiseRecordV1 {
    /// Highest promised ballot.
    pub promised: Ballot,
    /// Highest synchronized ballot.
    pub synced: Ballot,
}

/// `protocol_v1` key of the epoch's promise row.
pub fn promise_key(epoch: ConfigurationEpoch) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    out.extend_from_slice(&epoch.to_be_bytes());
    out.push(PROMISE_TAG);
    out
}

/// Encode the promise row.
pub fn encode_promise(record: &PromiseRecordV1) -> Result<Vec<u8>, EngineError> {
    let payload = postcard::to_allocvec(record)
        .map_err(|_| EngineError::new(ErrorClass::Limit, "promise encode"))?;
    StoreEnvelopeV1 {
        record_kind: PROMISE_KIND,
        schema_version: 1,
        payload,
    }
    .encode()
}

/// Decode the promise row.
pub fn decode_promise(bytes: &[u8]) -> Result<PromiseRecordV1, EngineError> {
    let env = StoreEnvelopeV1::decode(bytes)?;
    if env.record_kind != PROMISE_KIND || env.schema_version != 1 {
        return Err(EngineError::new(ErrorClass::Corrupt, "promise record"));
    }
    let (record, rest): (PromiseRecordV1, &[u8]) = postcard::take_from_bytes(&env.payload)
        .map_err(|_| EngineError::new(ErrorClass::Corrupt, "promise record"))?;
    if !rest.is_empty() {
        return Err(EngineError::new(ErrorClass::Corrupt, "promise record"));
    }
    Ok(record)
}

/// The update writing the epoch's promise row.
pub fn promise_update(
    epoch: ConfigurationEpoch,
    record: &PromiseRecordV1,
) -> Result<StoreUpdate, EngineError> {
    Ok(StoreUpdate {
        collection: Collection::ProtocolV1.id(),
        key: promise_key(epoch),
        value: Some(encode_promise(record)?),
    })
}

/// `protocol_v1` key of a command's dependency row in an epoch.
pub fn dependency_key(epoch: ConfigurationEpoch, command: &CommandId) -> Vec<u8> {
    let mut out = Vec::with_capacity(41);
    out.extend_from_slice(&epoch.to_be_bytes());
    out.push(DEPENDENCY_TAG);
    out.extend_from_slice(command.as_bytes());
    out
}

/// Encode a command's required dependency state (phase, dependencies,
/// payload binding and path evidence).
pub fn encode_dependency(record: &CommandRecord) -> Result<Vec<u8>, EngineError> {
    let payload = postcard::to_allocvec(record)
        .map_err(|_| EngineError::new(ErrorClass::Limit, "dependency encode"))?;
    StoreEnvelopeV1 {
        record_kind: DEPENDENCY_KIND,
        schema_version: 1,
        payload,
    }
    .encode()
}

/// Decode a dependency row.
pub fn decode_dependency(bytes: &[u8]) -> Result<CommandRecord, EngineError> {
    let env = StoreEnvelopeV1::decode(bytes)?;
    if env.record_kind != DEPENDENCY_KIND || env.schema_version != 1 {
        return Err(EngineError::new(ErrorClass::Corrupt, "dependency record"));
    }
    let (record, rest): (CommandRecord, &[u8]) = postcard::take_from_bytes(&env.payload)
        .map_err(|_| EngineError::new(ErrorClass::Corrupt, "dependency record"))?;
    if !rest.is_empty() {
        return Err(EngineError::new(ErrorClass::Corrupt, "dependency record"));
    }
    Ok(record)
}

/// The update persisting a command's dependency row.
pub fn dependency_update(
    epoch: ConfigurationEpoch,
    command: &CommandId,
    record: &CommandRecord,
) -> Result<StoreUpdate, EngineError> {
    Ok(StoreUpdate {
        collection: Collection::ProtocolV1.id(),
        key: dependency_key(epoch, command),
        value: Some(encode_dependency(record)?),
    })
}

/// The immutable canonical command in `payload_v1`, keyed by command
/// identity: retry key, canonical logical bytes (rehashed on read), and
/// the admission the command was accepted under.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PayloadRecordV1 {
    /// Stable invocation identity.
    pub retry_key: RetryKey,
    /// Canonical `LogicalRequest` encoding.
    pub logical: Vec<u8>,
    /// What the admitting verifier attested, as part of what this
    /// command durably *is*.
    ///
    /// Travelling beside the payload is not enough. Execution reads the
    /// principal, the trust rule and the ceiling from here, so a
    /// command whose admission were merely ambient would mean different
    /// things to a replica that executed it now, a replica that
    /// recovered its payload from a peer, and a replica that replayed
    /// it from the journal. Recording it with the payload is what makes
    /// those the same command.
    ///
    /// A fresh receipt presented on a retry does not replace it: the
    /// record is written once, when the command is first accepted, and
    /// a later presentation that disagrees is a conflict rather than an
    /// update.
    ///
    /// `None` only for a record written before this field existed, and
    /// for the protocol's own internal commands, which no verifier
    /// admitted.
    #[serde(default)]
    pub admission: Option<AdmissionFacts>,
}

impl PayloadRecordV1 {
    /// The digest that binds this command's admission into what the
    /// command is.
    ///
    /// Distinct from the command identity, deliberately. Identity is
    /// the retry key and the canonical request, so a retry keeps it and
    /// a credential rotation does not change it. This is what replicas
    /// compare to be sure they accepted the *same* command, and it is
    /// what a payload transfer is checked against.
    pub fn admission_digest(&self) -> Digest32 {
        admission_digest(self.admission.as_ref())
    }
}

/// `payload_v1` key: the command identity.
pub fn payload_key(command: &CommandId) -> Vec<u8> {
    command.as_bytes().to_vec()
}

fn envelope_at<T: Serialize>(
    kind: u16,
    schema_version: u16,
    value: &T,
    what: &'static str,
) -> Result<Vec<u8>, EngineError> {
    let payload =
        postcard::to_allocvec(value).map_err(|_| EngineError::new(ErrorClass::Limit, what))?;
    StoreEnvelopeV1 {
        record_kind: kind,
        schema_version,
        payload,
    }
    .encode()
}

fn envelope<T: Serialize>(
    kind: u16,
    value: &T,
    what: &'static str,
) -> Result<Vec<u8>, EngineError> {
    envelope_at(kind, 1, value, what)
}

/// Open an envelope of `kind`, returning its schema version and payload.
/// The payload layout of a schema version is fixed once written, so a
/// reader decides by version which decoder to run rather than guessing.
fn open(kind: u16, bytes: &[u8], what: &'static str) -> Result<(u16, Vec<u8>), EngineError> {
    let env = StoreEnvelopeV1::decode(bytes)?;
    if env.record_kind != kind {
        return Err(EngineError::new(ErrorClass::Corrupt, what));
    }
    Ok((env.schema_version, env.payload))
}

fn decode_exact<T: for<'de> Deserialize<'de>>(
    payload: &[u8],
    what: &'static str,
) -> Result<T, EngineError> {
    let (value, rest): (T, &[u8]) = postcard::take_from_bytes(payload)
        .map_err(|_| EngineError::new(ErrorClass::Corrupt, what))?;
    if !rest.is_empty() {
        return Err(EngineError::new(ErrorClass::Corrupt, what));
    }
    Ok(value)
}

fn unwrap<T: for<'de> Deserialize<'de>>(
    kind: u16,
    bytes: &[u8],
    what: &'static str,
) -> Result<T, EngineError> {
    let (version, payload) = open(kind, bytes, what)?;
    if version != 1 {
        return Err(EngineError::new(ErrorClass::Corrupt, what));
    }
    decode_exact(&payload, what)
}

/// Encode a payload row.
pub fn encode_payload(record: &PayloadRecordV1) -> Result<Vec<u8>, EngineError> {
    envelope(PAYLOAD_KIND, record, "payload encode")
}

/// Decode a payload row.
pub fn decode_payload(bytes: &[u8]) -> Result<PayloadRecordV1, EngineError> {
    unwrap(PAYLOAD_KIND, bytes, "payload record")
}

/// The update persisting a command's payload.
pub fn payload_update(
    command: &CommandId,
    record: &PayloadRecordV1,
) -> Result<StoreUpdate, EngineError> {
    Ok(StoreUpdate {
        collection: Collection::PayloadV1.id(),
        key: payload_key(command),
        value: Some(encode_payload(record)?),
    })
}

/// The leader's recoverable proposal state for a command.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProposalRecordV1 {
    /// Ballot the proposal was made under.
    pub ballot: Ballot,
    /// Leader sequence number.
    pub seqnum: u64,
    /// Ordered dependencies.
    pub deps: Vec<CommandId>,
    /// Dependency-path evidence.
    pub path: Digest32,
}

/// `protocol_v1` key of a command's proposal row in an epoch.
pub fn proposal_key(epoch: ConfigurationEpoch, command: &CommandId) -> Vec<u8> {
    let mut out = Vec::with_capacity(41);
    out.extend_from_slice(&epoch.to_be_bytes());
    out.push(PROPOSAL_TAG);
    out.extend_from_slice(command.as_bytes());
    out
}

/// Encode a proposal row.
pub fn encode_proposal(record: &ProposalRecordV1) -> Result<Vec<u8>, EngineError> {
    envelope(PROPOSAL_KIND, record, "proposal encode")
}

/// Decode a proposal row.
pub fn decode_proposal(bytes: &[u8]) -> Result<ProposalRecordV1, EngineError> {
    unwrap(PROPOSAL_KIND, bytes, "proposal record")
}

/// The update persisting a leader proposal.
pub fn proposal_update(
    epoch: ConfigurationEpoch,
    command: &CommandId,
    record: &ProposalRecordV1,
) -> Result<StoreUpdate, EngineError> {
    Ok(StoreUpdate {
        collection: Collection::ProtocolV1.id(),
        key: proposal_key(epoch, command),
        value: Some(encode_proposal(record)?),
    })
}

/// The Sync result a candidate selected for a ballot, bound durably before
/// publication (Section 4.9): after a crash it is reused, never reselected.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncRecordV1 {
    /// The selection.
    pub decision: SyncDecision,
}

/// `protocol_v1` key of the Sync row of a ballot in an epoch.
pub fn sync_key(epoch: ConfigurationEpoch, ballot: &Ballot) -> Vec<u8> {
    let mut out = Vec::with_capacity(33);
    out.extend_from_slice(&epoch.to_be_bytes());
    out.push(SYNC_TAG);
    out.extend_from_slice(&ballot.number.to_be_bytes());
    out.extend_from_slice(ballot.leader.as_bytes());
    out
}

/// Schema version of the Sync row. Version 1 carried no path evidence in
/// its entries; version 2 (task-28) carries the combined digest, the
/// per-key digests and the leader sequence number they were synchronized
/// at, so the layout changed and the version had to change with it.
pub const SYNC_SCHEMA_VERSION: u16 = 2;

/// A version 1 Sync entry: dependencies only, no path evidence.
#[derive(Clone, Debug, Deserialize)]
struct SyncEntryV1 {
    command: CommandId,
    phase: crate::phase::Phase,
    deps: Vec<CommandId>,
}

/// A version 1 Sync decision.
#[derive(Clone, Debug, Deserialize)]
struct SyncDecisionV1 {
    ballot: Ballot,
    source_ballot: Ballot,
    entries: alloc::collections::BTreeMap<CommandId, SyncEntryV1>,
    reproposed: alloc::collections::BTreeSet<CommandId>,
}

/// A version 1 Sync row.
#[derive(Clone, Debug, Deserialize)]
struct SyncRecordV1Legacy {
    decision: SyncDecisionV1,
}

/// The durable seal of one replica on one configuration (design
/// Section 10.3.2).
///
/// Written once, never rewritten and never removed. While it is there
/// this replica admits no ordinary voting transition of that
/// configuration under any ballot: not a promise for a higher ballot,
/// not a vote, not an adoption. Terminal recovery of what the
/// configuration already did stays possible -- that is the point of
/// sealing rather than shutting down -- and it is the only thing that
/// does.
///
/// `at` is the ballot this replica had promised when it sealed. It is
/// evidence for the report, never an authorization: a seal fences every
/// ballot of the configuration, including ones nobody has proposed yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SealRecordV1 {
    /// The transition this replica sealed for.
    pub transition: crate::handoff::Transition,
    /// The ballot promised when the seal was written.
    pub at: Ballot,
}

/// `protocol_v1` key of the seal row of an epoch.
pub fn seal_key(epoch: ConfigurationEpoch) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    out.extend_from_slice(&epoch.to_be_bytes());
    out.push(SEAL_TAG);
    out
}

/// Encode the seal row.
pub fn encode_seal(record: &SealRecordV1) -> Result<Vec<u8>, EngineError> {
    envelope(SEAL_KIND, record, "seal encode")
}

/// Decode the seal row.
pub fn decode_seal(bytes: &[u8]) -> Result<SealRecordV1, EngineError> {
    unwrap(SEAL_KIND, bytes, "seal record")
}

/// The update writing this replica's seal.
pub fn seal_update(
    epoch: ConfigurationEpoch,
    record: &SealRecordV1,
) -> Result<StoreUpdate, EngineError> {
    Ok(StoreUpdate {
        collection: Collection::ProtocolV1.id(),
        key: seal_key(epoch),
        value: Some(encode_seal(record)?),
    })
}

/// Encode a Sync row.
pub fn encode_sync(record: &SyncRecordV1) -> Result<Vec<u8>, EngineError> {
    envelope_at(SYNC_KIND, SYNC_SCHEMA_VERSION, record, "sync encode")
}

/// Decode a Sync row, including one written by a revision that stored the
/// version 1 layout. A version 1 selection is read back with empty path
/// evidence, which is what that revision knew: the dependencies it bound
/// are preserved exactly, and the replica realigns nothing it has no
/// evidence for. Any other version is refused rather than misread.
pub fn decode_sync(bytes: &[u8]) -> Result<SyncRecordV1, EngineError> {
    let (version, payload) = open(SYNC_KIND, bytes, "sync record")?;
    match version {
        SYNC_SCHEMA_VERSION => decode_exact(&payload, "sync record"),
        1 => {
            let legacy: SyncRecordV1Legacy = decode_exact(&payload, "sync record")?;
            Ok(SyncRecordV1 {
                decision: SyncDecision {
                    ballot: legacy.decision.ballot,
                    source_ballot: legacy.decision.source_ballot,
                    entries: legacy
                        .decision
                        .entries
                        .into_iter()
                        .map(|(c, e)| {
                            (
                                c,
                                crate::recovery::SyncEntry {
                                    command: e.command,
                                    phase: e.phase,
                                    deps: e.deps,
                                    path: crate::graph::empty_path(),
                                    paths: Vec::new(),
                                    seqnum: 0,
                                },
                            )
                        })
                        .collect(),
                    reproposed: legacy.decision.reproposed,
                },
            })
        }
        _ => Err(EngineError::new(
            ErrorClass::Corrupt,
            "sync record of an unsupported schema version",
        )),
    }
}

/// The update persisting a bound Sync selection.
pub fn sync_update(
    epoch: ConfigurationEpoch,
    record: &SyncRecordV1,
) -> Result<StoreUpdate, EngineError> {
    Ok(StoreUpdate {
        collection: Collection::ProtocolV1.id(),
        key: sync_key(epoch, &record.decision.ballot),
        value: Some(encode_sync(record)?),
    })
}
