//! The admission interface (design Sections 4.4, 9.3, 19.4): a request
//! enters the trusted boundary only through here. The caller's identity
//! was verified by the session binding of the connection (task-37 binds
//! it to the transport; here it is the [`Caller`] the runtime presents);
//! the request must be canonical, name this cluster and domain and the
//! caller's own session, and stay inside the caller's pending bound. The
//! receipt is minted with the verifier token because this *is* the
//! verifier code; state machines never mint one. Raw tokens never appear.

use std::collections::BTreeMap;

use coord_core::capability::{AdmissionReceipt, VerifierToken};
use coord_core::event::AdmittedRequest;
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

/// Bounds of admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdmissionLimits {
    /// Requests one session may have pending at this frontend.
    pub max_pending_per_session: usize,
}

impl Default for AdmissionLimits {
    fn default() -> Self {
        AdmissionLimits {
            max_pending_per_session: 256,
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
}

/// The admission gate of one frontend for one domain.
#[derive(Debug)]
pub struct Admission {
    cluster: ClusterId,
    domain: DomainId,
    limits: AdmissionLimits,
    pending: BTreeMap<SessionId, usize>,
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
    ) -> Result<AdmittedRequest, AdmissionRefusal> {
        if caller.role != PeerRole::Client {
            return Err(AdmissionRefusal::RoleNotAdmitted(caller.role));
        }
        request.logical().map_err(|_| AdmissionRefusal::Malformed)?;
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
        let pending = self.pending.entry(caller.session).or_insert(0);
        if *pending >= self.limits.max_pending_per_session {
            return Err(AdmissionRefusal::SessionBusy { pending: *pending });
        }
        *pending += 1;
        self.minted += 1;
        let receipt_id = HashDomain::AdmissionReceipt.digest(&[
            &key.canonical_bytes(),
            &caller.rule_generation.to_be_bytes(),
            &now_ticks.to_be_bytes(),
            &self.minted.to_be_bytes(),
        ]);
        let receipt = AdmissionReceipt::from_verifier(
            VerifierToken::for_boundary(),
            caller.session,
            caller.rule_generation,
            caller.scope_ceiling,
            receipt_id,
            now_ticks,
        );
        let frame = MessageV1::Request(request.clone())
            .encode()
            .map_err(|_| AdmissionRefusal::Malformed)?;
        Ok(AdmittedRequest { receipt, frame })
    }

    /// A request of `session` settled (released, refused after admission
    /// or resolved): its pending slot is free again.
    pub fn settled(&mut self, session: &SessionId) {
        if let Some(n) = self.pending.get_mut(session) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                self.pending.remove(session);
            }
        }
    }

    /// Requests pending for `session`.
    pub fn pending(&self, session: &SessionId) -> usize {
        self.pending.get(session).copied().unwrap_or(0)
    }
}
