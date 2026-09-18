//! Internal replicated commands: ordered like client commands, planned
//! against the same views, never issued by clients.

use coord_types::ids::{LeaseAuthorityEpoch, LeaseGeneration, LeaseId, NamespaceId};
use serde::{Deserialize, Serialize};

/// A command the service submits to its own ordered history.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum InternalCommand {
    /// Conditional lease expiration (Section 7.3): applies only when every
    /// field matches the replicated state, so a renewal ordered first makes
    /// it a no-op and an old leader's epoch is rejected outright.
    ExpireLease {
        /// Namespace of the lease (selects the reverse index to load).
        namespace: NamespaceId,
        /// Lease.
        lease_id: LeaseId,
        /// Generation the scheduler observed.
        generation: LeaseGeneration,
        /// Renewal sequence the scheduler observed.
        expected_renewal_sequence: u64,
        /// Authority epoch the scheduler runs under.
        authority_epoch: LeaseAuthorityEpoch,
    },
    /// Establish a new lease expiry authority epoch (Section 7.2), fencing
    /// every scheduler of an older epoch.
    EstablishLeaseAuthority {
        /// Namespace the command is planned in (leases are domain-wide; the
        /// epoch row is domain-wide too).
        namespace: NamespaceId,
        /// New epoch; must exceed the current one.
        epoch: LeaseAuthorityEpoch,
    },
}

impl InternalCommand {
    /// Namespace the command's view is built for.
    pub const fn namespace(&self) -> NamespaceId {
        match self {
            InternalCommand::ExpireLease { namespace, .. }
            | InternalCommand::EstablishLeaseAuthority { namespace, .. } => *namespace,
        }
    }

    /// Lease whose record and attachments the command needs, if any.
    pub const fn lease(&self) -> Option<LeaseId> {
        match self {
            InternalCommand::ExpireLease { lease_id, .. } => Some(*lease_id),
            InternalCommand::EstablishLeaseAuthority { .. } => None,
        }
    }
}
