//! The logical experiment workload (design Section 17.12).
//!
//! One workload is generated once from a seed by a pinned generator and is
//! then created independently per engine: the operations are canonical
//! `LogicalRequest`s driven through the common worker, planner, retry and
//! authorization paths, never engine-specific writes. The mix is
//! protocol-shaped rather than a bulk load: overwrites, deletes,
//! transactions that touch several indexes in one revision, lease grants
//! with attachments and revocations, retried invocations that must return a
//! retained result without executing, bounded reads and explicit retention
//! floors that give the maintenance path real work.

use coord_types::identity::Digest32;
use coord_types::ids::{KvRevision, LeaseId, NamespaceId};
use coord_types::logical_v1::*;
use rand_chacha::ChaCha12Rng;
use rand_core::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

/// Pinned generator identity; it is part of every experiment manifest.
pub const GENERATOR: &str = "store-workload-gen/v1 chacha12";

/// The namespace every generated request addresses.
pub const NAMESPACE: NamespaceId = NamespaceId([0x11; 16]);

/// Shape of one generated operation, reported in counters and manifests.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpKind {
    /// Overwrite one key.
    Put,
    /// Put attached to a native lease (writes the reverse index too).
    PutLeased,
    /// Delete one key.
    Delete,
    /// Compare-and-write touching several keys in one revision.
    Txn,
    /// Bounded range read.
    Range,
    /// Grant a lease.
    LeaseGrant,
    /// Revoke a lease, deleting its attachments atomically.
    LeaseRevoke,
    /// A repeated invocation of an earlier command identity.
    Retry,
    /// Advance the replicated retention floor.
    Compact,
}

/// One generated operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Op {
    /// A request submitted under its own invocation sequence.
    Request {
        /// Shape.
        kind: OpKind,
        /// Per-client request sequence (the retry identity).
        sequence: u64,
        /// The canonical request.
        request: LogicalRequest,
    },
    /// The same request and sequence as an earlier operation: the retry
    /// layer must answer it from the retained result.
    Repeat {
        /// Shape of the repeated operation.
        kind: OpKind,
        /// Sequence of the operation being repeated.
        sequence: u64,
        /// The identical canonical request.
        request: LogicalRequest,
    },
    /// Advance the retention floor to `revisions_back` below the current
    /// KV revision; the driver resolves the revision at execution time so
    /// the floor is real rather than generated.
    Compact {
        /// Per-client request sequence.
        sequence: u64,
        /// How far below the current revision the floor is placed.
        revisions_back: u64,
    },
}

impl Op {
    /// The invocation sequence of this operation.
    pub const fn sequence(&self) -> u64 {
        match self {
            Op::Request { sequence, .. }
            | Op::Repeat { sequence, .. }
            | Op::Compact { sequence, .. } => *sequence,
        }
    }

    /// The shape of this operation.
    pub const fn kind(&self) -> OpKind {
        match self {
            Op::Request { kind, .. } | Op::Repeat { kind, .. } => *kind,
            Op::Compact { .. } => OpKind::Compact,
        }
    }
}

/// The parameters of a workload. Two trials may be compared only when this
/// specification, its digest and the durability profile are identical.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadSpec {
    /// Generator seed.
    pub seed: [u8; 32],
    /// Operations applied before measurement starts.
    pub prefill_ops: u32,
    /// Operations applied to warm caches, measured separately.
    pub warmup_ops: u32,
    /// Measured operations.
    pub measured_ops: u32,
    /// Distinct keys in the churned key space.
    pub keys: u32,
    /// Value size in bytes.
    pub value_bytes: u32,
    /// Distinct leases.
    pub leases: u32,
    /// Scheduled inter-arrival time in nanoseconds; `0` offers the next
    /// operation as soon as the previous one returned (closed loop).
    pub arrival_interval_ns: u64,
    /// One operation in this many repeats an earlier invocation.
    pub retry_every: u32,
    /// One operation in this many advances the retention floor.
    pub compact_every: u32,
    /// How far below the current revision a compaction places the floor.
    pub retain_revisions: u64,
    /// One operation in this many checks a pinned snapshot for stability.
    pub pinned_read_every: u32,
    /// Operations a pinned snapshot is held across.
    pub pinned_hold_ops: u32,
    /// History rows one maintenance step may remove.
    pub gc_budget_rows: u32,
}

impl WorkloadSpec {
    /// The small in-suite workload: every shape, few operations.
    pub fn smoke() -> Self {
        WorkloadSpec {
            seed: [0x5a; 32],
            prefill_ops: 48,
            warmup_ops: 16,
            measured_ops: 96,
            keys: 24,
            value_bytes: 96,
            leases: 4,
            arrival_interval_ns: 0,
            retry_every: 11,
            compact_every: 17,
            retain_revisions: 8,
            pinned_read_every: 13,
            pinned_hold_ops: 4,
            gc_budget_rows: 16,
        }
    }

    /// Total operations of all three phases.
    pub const fn total_ops(&self) -> u32 {
        self.prefill_ops + self.warmup_ops + self.measured_ops
    }

    /// Digest of the specification. A comparison refuses paired trials
    /// whose digests differ, so the workload cannot change silently.
    pub fn digest(&self) -> Digest32 {
        let mut h = blake3::Hasher::new_derive_key("tuplesky store experiment workload v1");
        h.update(GENERATOR.as_bytes());
        h.update(&self.seed);
        for v in [
            self.prefill_ops as u64,
            self.warmup_ops as u64,
            self.measured_ops as u64,
            self.keys as u64,
            self.value_bytes as u64,
            self.leases as u64,
            self.arrival_interval_ns,
            self.retry_every as u64,
            self.compact_every as u64,
            self.retain_revisions,
            self.pinned_read_every as u64,
            self.pinned_hold_ops as u64,
            self.gc_budget_rows as u64,
        ] {
            h.update(&v.to_be_bytes());
        }
        Digest32(*h.finalize().as_bytes())
    }
}

fn req(op: CanonicalOperation) -> LogicalRequest {
    let mut r = LogicalRequest::new(NAMESPACE, op);
    r.canonicalize();
    r
}

fn key_of(index: u32) -> Vec<u8> {
    format!("kv/{index:06}").into_bytes()
}

fn lease_of(index: u32) -> LeaseId {
    let mut id = [0u8; 16];
    id[0] = 0xa0_u8.wrapping_add(index as u8);
    id[15] = index as u8 + 1;
    LeaseId(id)
}

/// The three phases of one trial.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Workload {
    /// Operations applied before measurement (setup, timed separately).
    pub prefill: Vec<Op>,
    /// Operations applied to warm caches (timed separately).
    pub warmup: Vec<Op>,
    /// Measured operations.
    pub measured: Vec<Op>,
}

/// Generate a workload. The same seed and specification produce the same
/// operations on every engine; nothing in generation depends on an engine.
pub fn generate(spec: &WorkloadSpec) -> Workload {
    let mut rng = ChaCha12Rng::from_seed(spec.seed);
    let mut sequence = 0u64;
    let mut ops = Vec::with_capacity(spec.total_ops() as usize);
    let mut granted: Vec<LeaseId> = Vec::new();
    let mut applied: Vec<(OpKind, u64, LogicalRequest)> = Vec::new();
    let value = |rng: &mut ChaCha12Rng, n: u32| -> Vec<u8> {
        (0..n as usize)
            .map(|_| (rng.next_u32() & 0xff) as u8)
            .collect()
    };
    for index in 0..spec.total_ops() {
        // A repeat reuses an earlier invocation identity unchanged.
        if spec.retry_every > 0
            && index > 0
            && index.is_multiple_of(spec.retry_every)
            && !applied.is_empty()
        {
            let (kind, seq, request) = applied[(rng.next_u32() as usize) % applied.len()].clone();
            ops.push(Op::Repeat {
                kind,
                sequence: seq,
                request,
            });
            continue;
        }
        sequence += 1;
        if spec.compact_every > 0 && index > 0 && index.is_multiple_of(spec.compact_every) {
            ops.push(Op::Compact {
                sequence,
                revisions_back: spec.retain_revisions,
            });
            continue;
        }
        let k = key_of(rng.next_u32() % spec.keys);
        let roll = rng.next_u32() % 100;
        let (kind, request) = match roll {
            0..=41 => (
                OpKind::Put,
                req(CanonicalOperation::Put(PutOp {
                    key: k,
                    value: value(&mut rng, spec.value_bytes),
                    lease: None,
                    prev_kv: roll.is_multiple_of(3),
                })),
            ),
            42..=51 => (
                OpKind::Delete,
                req(CanonicalOperation::DeleteRange(DeleteRangeOp {
                    range: KeyRange::exact(k),
                    prev_kv: false,
                })),
            ),
            52..=68 => {
                // One revision touching several keys and both KV indexes:
                // a comparison on one key, two writes and one delete. A
                // branch may not name a key twice, so the three keys are
                // drawn distinct.
                let mut chosen = vec![k.clone()];
                while chosen.len() < 3 {
                    let candidate = key_of(rng.next_u32() % spec.keys);
                    if !chosen.contains(&candidate) {
                        chosen.push(candidate);
                    }
                }
                let third = chosen.pop().expect("three keys");
                let other = chosen.pop().expect("three keys");
                (
                    OpKind::Txn,
                    req(CanonicalOperation::Txn(TxnOp {
                        compares: vec![Compare {
                            key: k.clone(),
                            target: CompareTarget::ModRevision,
                            result: CompareResult::Greater,
                            operand: CompareOperand::Counter(0),
                        }],
                        success: vec![
                            BranchOp::Put(PutOp {
                                key: k.clone(),
                                value: value(&mut rng, spec.value_bytes),
                                lease: None,
                                prev_kv: false,
                            }),
                            BranchOp::Put(PutOp {
                                key: other.clone(),
                                value: value(&mut rng, spec.value_bytes),
                                lease: None,
                                prev_kv: false,
                            }),
                            BranchOp::DeleteRange(DeleteRangeOp {
                                range: KeyRange::exact(third),
                                prev_kv: false,
                            }),
                        ],
                        failure: vec![BranchOp::Put(PutOp {
                            key: other,
                            value: value(&mut rng, spec.value_bytes),
                            lease: None,
                            prev_kv: false,
                        })],
                    })),
                )
            }
            69..=80 => {
                let lower = key_of(0);
                let upper = key_of(spec.keys);
                (
                    OpKind::Range,
                    req(CanonicalOperation::Range(RangeOp {
                        range: KeyRange::interval(lower, upper),
                        revision: None,
                        limit: 32,
                        keys_only: roll.is_multiple_of(2),
                        count_only: false,
                    })),
                )
            }
            81..=88 => {
                let lease = lease_of(rng.next_u32() % spec.leases);
                if granted.contains(&lease) {
                    (
                        OpKind::PutLeased,
                        req(CanonicalOperation::Put(PutOp {
                            key: k,
                            value: value(&mut rng, spec.value_bytes),
                            lease: Some(lease),
                            prev_kv: false,
                        })),
                    )
                } else {
                    granted.push(lease);
                    (
                        OpKind::LeaseGrant,
                        req(CanonicalOperation::LeaseGrant {
                            lease_id: lease,
                            ttl_seconds: 600,
                        }),
                    )
                }
            }
            89..=94 => {
                let lease = lease_of(rng.next_u32() % spec.leases);
                if granted.contains(&lease) {
                    (
                        OpKind::PutLeased,
                        req(CanonicalOperation::Put(PutOp {
                            key: k,
                            value: value(&mut rng, spec.value_bytes),
                            lease: Some(lease),
                            prev_kv: false,
                        })),
                    )
                } else {
                    granted.push(lease);
                    (
                        OpKind::LeaseGrant,
                        req(CanonicalOperation::LeaseGrant {
                            lease_id: lease,
                            ttl_seconds: 600,
                        }),
                    )
                }
            }
            _ => {
                if granted.len() > 1 {
                    let lease = granted.remove((rng.next_u32() as usize) % granted.len());
                    (
                        OpKind::LeaseRevoke,
                        req(CanonicalOperation::LeaseRevoke { lease_id: lease }),
                    )
                } else {
                    (
                        OpKind::Put,
                        req(CanonicalOperation::Put(PutOp {
                            key: k,
                            value: value(&mut rng, spec.value_bytes),
                            lease: None,
                            prev_kv: false,
                        })),
                    )
                }
            }
        };
        applied.push((kind, sequence, request.clone()));
        ops.push(Op::Request {
            kind,
            sequence,
            request,
        });
    }
    let mut rest = ops.split_off(spec.prefill_ops as usize);
    let measured = rest.split_off(spec.warmup_ops as usize);
    Workload {
        prefill: ops,
        warmup: rest,
        measured,
    }
}

/// A resolved compaction revision: the floor is `current - revisions_back`,
/// clamped to a valid revision, or `None` when there is not enough history
/// yet for the floor to move.
pub fn compaction_revision(current: KvRevision, revisions_back: u64) -> Option<KvRevision> {
    KvRevision::new(current.get().checked_sub(revisions_back)?).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn generation_is_deterministic_and_covers_every_shape() {
        let spec = WorkloadSpec::smoke();
        let a = generate(&spec);
        let b = generate(&spec);
        assert_eq!(
            a, b,
            "the same seed and specification generate the same ops"
        );
        let all: Vec<&Op> = a
            .prefill
            .iter()
            .chain(a.warmup.iter())
            .chain(a.measured.iter())
            .collect();
        assert_eq!(all.len(), spec.total_ops() as usize);
        let kinds: BTreeSet<&str> = all
            .iter()
            .map(|o| match (o, o.kind()) {
                (Op::Repeat { .. }, _) => "repeat",
                (_, OpKind::Put) => "put",
                (_, OpKind::PutLeased) => "put_leased",
                (_, OpKind::Delete) => "delete",
                (_, OpKind::Txn) => "txn",
                (_, OpKind::Range) => "range",
                (_, OpKind::LeaseGrant) => "lease_grant",
                (_, OpKind::LeaseRevoke) => "lease_revoke",
                (_, OpKind::Compact) => "compact",
                (_, OpKind::Retry) => "retry",
            })
            .collect();
        for expected in [
            "put",
            "put_leased",
            "delete",
            "txn",
            "range",
            "lease_grant",
            "compact",
            "repeat",
        ] {
            assert!(
                kinds.contains(expected),
                "{expected} missing from {kinds:?}"
            );
        }
    }

    #[test]
    fn a_changed_specification_changes_the_digest() {
        let spec = WorkloadSpec::smoke();
        let mut other = spec;
        other.value_bytes += 1;
        assert_ne!(spec.digest(), other.digest());
        let mut seeded = spec;
        seeded.seed[0] ^= 1;
        assert_ne!(spec.digest(), seeded.digest());
        assert_eq!(spec.digest(), WorkloadSpec::smoke().digest());
    }

    #[test]
    fn a_repeat_reuses_the_earlier_invocation_identity() {
        let a = generate(&WorkloadSpec::smoke());
        let all: Vec<Op> = a
            .prefill
            .iter()
            .chain(a.warmup.iter())
            .chain(a.measured.iter())
            .cloned()
            .collect();
        let mut seen: BTreeSet<(u64, Vec<u8>)> = BTreeSet::new();
        let mut repeats = 0;
        for op in &all {
            match op {
                Op::Request {
                    sequence, request, ..
                } => {
                    seen.insert((*sequence, postcard::to_allocvec(request).unwrap()));
                }
                Op::Repeat {
                    sequence, request, ..
                } => {
                    repeats += 1;
                    assert!(
                        seen.contains(&(*sequence, postcard::to_allocvec(request).unwrap())),
                        "a repeat must be byte-identical to the earlier invocation"
                    );
                }
                Op::Compact { .. } => {}
            }
        }
        assert!(repeats > 0);
    }
}
