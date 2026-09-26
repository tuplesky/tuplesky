//! The admission interface (design Sections 4.4, 9.3, 19.4): a request
//! enters the trusted boundary only through here. The caller's identity
//! was verified by the session binding of the connection (task-37 binds
//! it to the transport; here it is the [`Caller`] the runtime presents);
//! the request must be canonical, name this cluster and domain and the
//! caller's own session, and stay inside the caller's pending bound. The
//! receipt is minted with the verifier token because this *is* the
//! verifier code; state machines never mint one. Raw tokens never appear.

use std::collections::{BTreeMap, BTreeSet};

use coord_core::capability::{AdmissionReceipt, VerifierToken};
use coord_core::event::AdmittedRequest;
use coord_types::RetryKey;
use coord_types::identity::HashDomain;
use coord_types::ids::{ClusterId, DomainId, SessionId};
use coord_types::wire_v1::{MessageV1, PeerRole, RequestV1};

/// The verified caller of one API connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Caller {
    /// Role bound at negotiation.
    pub role: PeerRole,
    /// Replicated session the connection is bound to.
    pub session: SessionId,
    /// Rule/issuer generation the binding relied on.
    pub rule_generation: u64,
    /// Scope ceiling; execution policy can only narrow it.
    pub scope_ceiling: u32,
}

/// An admitted request and whether this presentation took the session's
/// slot.
///
/// A retry of a request the session still has outstanding is that
/// request again: it neither needs a slot nor takes one, and a refusal
/// of it must not free the slot the original holds.
pub struct Admitted {
    /// What goes to the collector.
    pub request: AdmittedRequest,
    /// Whether this presentation acquired the slot.
    pub reserved: bool,
}

/// Bounds of admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdmissionLimits {
    /// Requests one session may have pending at this frontend.
    pub max_pending_per_session: usize,
    /// The largest logical request this frontend admits, as the protocol
    /// counts one ([`coord_types::logical_v1::LogicalRequest::cost`]): the
    /// bytes of the keys, values and range ends it carries. At the
    /// default, the protocol's own bound, no request that validates is
    /// refused for its size.
    pub max_request_bytes: usize,
}

impl Default for AdmissionLimits {
    fn default() -> Self {
        AdmissionLimits {
            max_pending_per_session: 256,
            max_request_bytes: coord_types::logical_v1::limits::MAX_REQUEST_BYTES,
        }
    }
}

/// Why a request was not admitted. Nothing was submitted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionRefusal {
    /// Only native clients submit through the frontend; peer and
    /// collector roles have no request path here.
    RoleNotAdmitted(PeerRole),
    /// The request or its logical payload is not canonical.
    Malformed,
    /// The retry key names another cluster.
    WrongCluster,
    /// The retry key names another domain.
    WrongDomain,
    /// The retry key names a session other than the caller's.
    SessionMismatch,
    /// The session is at its pending bound.
    SessionBusy {
        /// Requests pending for the session.
        pending: usize,
    },
    /// The request is larger than this frontend admits.
    RequestTooLarge {
        /// What the request costs, as the protocol counts it.
        bytes: usize,
        /// What this frontend admits: `max_request_bytes`.
        limit: usize,
    },
}

/// The admission gate of one frontend for one domain.
#[derive(Debug)]
pub struct Admission {
    cluster: ClusterId,
    domain: DomainId,
    limits: AdmissionLimits,
    /// Unresolved requests per session, by identity. The bound counts
    /// distinct requests, not admissions: a retry of a request that is
    /// still unresolved is the same request, so it takes no second slot
    /// and, since a request is settled once, cannot leak one either.
    pending: BTreeMap<SessionId, BTreeSet<RetryKey>>,
    minted: u64,
}

impl Admission {
    /// A gate for `cluster`/`domain`.
    pub const fn new(cluster: ClusterId, domain: DomainId, limits: AdmissionLimits) -> Self {
        Admission {
            cluster,
            domain,
            limits,
            pending: BTreeMap::new(),
            minted: 0,
        }
    }

    /// Admit `request` from `caller` at `now_ticks`, minting its receipt.
    pub fn admit(
        &mut self,
        now_ticks: u64,
        caller: &Caller,
        request: &RequestV1,
    ) -> Result<Admitted, AdmissionRefusal> {
        if caller.role != PeerRole::Client {
            return Err(AdmissionRefusal::RoleNotAdmitted(caller.role));
        }
        let cost = request
            .logical()
            .ok()
            .and_then(|logical| logical.cost().ok())
            .ok_or(AdmissionRefusal::Malformed)?;
        // Before any slot is taken or any byte is budgeted: a request this
        // frontend will never admit costs it nothing, and is refused the
        // same way on every retry.
        if cost > self.limits.max_request_bytes {
            return Err(AdmissionRefusal::RequestTooLarge {
                bytes: cost,
                limit: self.limits.max_request_bytes,
            });
        }
        let key = &request.retry_key;
        if key.cluster_id != self.cluster {
            return Err(AdmissionRefusal::WrongCluster);
        }
        if key.domain_id != self.domain {
            return Err(AdmissionRefusal::WrongDomain);
        }
        if key.session_id != caller.session {
            return Err(AdmissionRefusal::SessionMismatch);
        }
        let pending = self.pending.entry(caller.session).or_default();
        // A retry of a request this session still has outstanding is that
        // request again, not another one: it neither needs a free slot
        // nor takes one. Counting every admission instead would refuse a
        // client's first retry under a bound of one, and leave a session
        // permanently busy once repeated retries outnumbered the single
        // release that settles them.
        let held = pending.contains(key);
        if !held && pending.len() >= self.limits.max_pending_per_session {
            return Err(AdmissionRefusal::SessionBusy {
                pending: pending.len(),
            });
        }
        pending.insert(*key);
        // Whether this presentation is what took the slot. A refusal of
        // a re-presentation must not release it: the request that holds
        // it is still outstanding, and freeing its reservation would let
        // the session exceed its bound.
        let reserved = !held;
        self.minted += 1;
        let receipt_id = HashDomain::AdmissionReceipt.digest(&[
            &key.canonical_bytes(),
            &caller.rule_generation.to_be_bytes(),
            &now_ticks.to_be_bytes(),
            &self.minted.to_be_bytes(),
        ]);
        // A submission under a session the cluster already agreed on.
        // Nothing here attests a principal or a trust rule: this
        // boundary verified a *binding*, not a credential, and a
        // receipt that could carry an identity would make every
        // frontend an identity issuer by accident.
        let receipt = AdmissionReceipt::submitting(
            VerifierToken::for_boundary(),
            coord_core::AttestedAdmission {
                cluster: self.cluster,
                domain: self.domain,
                session: caller.session,
                rule_generation: caller.rule_generation,
                scope_ceiling: caller.scope_ceiling,
                receipt_id,
                admitted_at_ticks: now_ticks,
            },
        );
        let frame = MessageV1::Request(request.clone())
            .encode()
            .map_err(|_| AdmissionRefusal::Malformed)?;
        Ok(Admitted {
            request: AdmittedRequest { receipt, frame },
            reserved,
        })
    }

    /// Release `key`'s slot, but only when `reserved` says this
    /// presentation is the one that took it.
    pub fn settled_reservation(&mut self, key: &RetryKey, reserved: bool) {
        if reserved {
            self.settled(key);
        }
    }

    /// A request settled (released, refused after admission or
    /// resolved): its slot is free again. Settling by identity is
    /// idempotent, so a duplicate settlement cannot free someone else's
    /// slot.
    pub fn settled(&mut self, key: &RetryKey) {
        if let Some(keys) = self.pending.get_mut(&key.session_id) {
            keys.remove(key);
            if keys.is_empty() {
                self.pending.remove(&key.session_id);
            }
        }
    }

    /// Distinct unresolved requests of `session`.
    pub fn pending(&self, session: &SessionId) -> usize {
        self.pending.get(session).map_or(0, BTreeSet::len)
    }
}
