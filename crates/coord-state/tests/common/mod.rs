//! Shared in-memory domain fixture for lease tests: current entries, lease
//! records and the reverse index derived from the entries (what common
//! storage builds for the planner), applied through the real planner.
#![allow(dead_code)]

use std::collections::BTreeMap;

use coord_core::effect::ApplyBase;
use coord_state::planner::{apply_leases, apply_to_map};
use coord_state::{
    ApplyPlan, InternalCommand, KvEntry, LeaseRecord, Mutation, PlanLimits, ReadView, plan,
    plan_internal,
};
use coord_types::ids::*;
use coord_types::logical_v1::*;

pub const NS: NamespaceId = NamespaceId([7; 16]);
pub const ALICE: PrincipalId = PrincipalId([0xa; 16]);
pub const BOB: PrincipalId = PrincipalId([0xb; 16]);
pub const L1: LeaseId = LeaseId([1; 16]);
pub const L2: LeaseId = LeaseId([2; 16]);

pub fn rev(n: u64) -> KvRevision {
    KvRevision::new(n).unwrap()
}

pub fn req(op: CanonicalOperation) -> LogicalRequest {
    let mut r = LogicalRequest::new(NS, op);
    r.canonicalize();
    r
}

pub fn put(key: &[u8], value: &[u8], lease: Option<LeaseId>) -> LogicalRequest {
    req(CanonicalOperation::Put(PutOp {
        key: key.to_vec(),
        value: value.to_vec(),
        lease,
        prev_kv: false,
    }))
}

pub fn grant(id: LeaseId, ttl: u32) -> LogicalRequest {
    req(CanonicalOperation::LeaseGrant {
        lease_id: id,
        ttl_seconds: ttl,
    })
}

pub fn revoke(id: LeaseId) -> LogicalRequest {
    req(CanonicalOperation::LeaseRevoke { lease_id: id })
}

pub fn keep_alive(id: LeaseId) -> LogicalRequest {
    req(CanonicalOperation::LeaseKeepAlive { lease_id: id })
}

pub fn ttl(id: LeaseId, keys: bool) -> LogicalRequest {
    req(CanonicalOperation::LeaseTimeToLive { lease_id: id, keys })
}

pub fn establish(epoch: u64) -> InternalCommand {
    InternalCommand::EstablishLeaseAuthority {
        namespace: NS,
        epoch: LeaseAuthorityEpoch::new(epoch).unwrap(),
    }
}

pub fn expire(id: LeaseId, generation: u64, seq: u64, epoch: u64) -> InternalCommand {
    InternalCommand::ExpireLease {
        namespace: NS,
        lease_id: id,
        generation: LeaseGeneration::new(generation).unwrap(),
        expected_renewal_sequence: seq,
        authority_epoch: LeaseAuthorityEpoch::new(epoch).unwrap(),
    }
}

pub struct Fixture {
    pub current: BTreeMap<Vec<u8>, KvEntry>,
    pub leases: BTreeMap<LeaseId, LeaseRecord>,
    pub lease_authority: LeaseAuthorityEpoch,
    pub revision: u64,
    pub position: u64,
    pub limits: PlanLimits,
}

impl Default for Fixture {
    fn default() -> Self {
        Self::new()
    }
}

impl Fixture {
    pub fn new() -> Self {
        Fixture {
            current: BTreeMap::new(),
            leases: BTreeMap::new(),
            lease_authority: LeaseAuthorityEpoch::ZERO,
            revision: 0,
            position: 0,
            limits: PlanLimits::default(),
        }
    }

    pub fn view(&self, principal: PrincipalId) -> ReadView {
        let base = ApplyBase {
            configuration: ConfigurationEpoch::ZERO,
            execution_position: ExecutionPosition::new(self.position).unwrap(),
        };
        let mut v = ReadView::empty(base, NS, principal, rev(self.revision));
        v.current = self.current.clone();
        v.leases = self.leases.clone();
        v.lease_authority = self.lease_authority;
        for (k, e) in &self.current {
            if let Some(l) = e.lease {
                v.lease_keys.entry(l).or_default().insert(k.clone());
            }
        }
        for l in self.leases.keys() {
            v.lease_keys.entry(*l).or_default();
        }
        v
    }

    fn apply(&mut self, p: ApplyPlan) -> ApplyPlan {
        assert_eq!(p.position.get(), self.position + 1);
        apply_to_map(&mut self.current, &p);
        apply_leases(&mut self.leases, &p);
        for m in &p.mutations {
            if let Mutation::LeaseAuthority { epoch } = m {
                self.lease_authority = *epoch;
            }
        }
        self.position += 1;
        if let Some(r) = p.revision {
            assert_eq!(r.get(), self.revision + 1);
            self.revision = r.get();
        }
        p
    }

    pub fn run_as(&mut self, principal: PrincipalId, r: &LogicalRequest) -> ApplyPlan {
        let v = self.view(principal);
        let p = plan(r, &v, &self.limits).unwrap();
        self.apply(p)
    }

    pub fn run(&mut self, r: &LogicalRequest) -> ApplyPlan {
        self.run_as(ALICE, r)
    }

    pub fn run_internal(&mut self, c: &InternalCommand) -> ApplyPlan {
        let v = self.view(PrincipalId([0; 16]));
        let p = plan_internal(c, &v, &self.limits).unwrap();
        self.apply(p)
    }

    pub fn record(&self, id: LeaseId) -> &LeaseRecord {
        &self.leases[&id]
    }
}
