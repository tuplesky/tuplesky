//! The fresh authorization barrier (design Sections 6.4, 6.9.3, 9.3): a
//! read of replicated policy at an ordered position, used for one
//! bounded selection of outputs and then dropped. It is deliberately not
//! `Clone`: a barrier cannot become a lease.

use coord_state::Response;
use coord_state::plan::Outcome;
use coord_state::policy::{Action, Authorization, KeyInterval};
use coord_storage::Persistence;
use coord_storage::WatchBatch;
use coord_storage::views::{ViewBudget, load_authorization};
use coord_types::ids::{ExecutionPosition, NamespaceId, SessionId};

/// Policy as of one ordered position, for one session in one namespace.
#[derive(Debug)]
pub struct AuthorizationBarrier {
    position: ExecutionPosition,
    namespace: NamespaceId,
    authorization: Authorization,
}

impl AuthorizationBarrier {
    /// Assemble from a loaded authorization context.
    pub const fn new(
        position: ExecutionPosition,
        namespace: NamespaceId,
        authorization: Authorization,
    ) -> Self {
        AuthorizationBarrier {
            position,
            namespace,
            authorization,
        }
    }

    /// The ordered position the policy was read at.
    pub const fn position(&self) -> ExecutionPosition {
        self.position
    }

    /// The replicated session record, if the cluster has one.
    ///
    /// Its absence is the answer a binding needs before it can serve:
    /// no row means no session, and a credential that names one is
    /// describing a session that has still to be established.
    pub const fn session_record(&self) -> Option<&coord_state::policy::SessionRecord> {
        self.authorization.session.as_ref()
    }

    /// Whether the session is still active under its rule generation.
    pub fn session_valid(&self) -> bool {
        self.authorization.valid_session().is_some()
    }

    /// Whether `key` may be read now.
    pub fn permits_read(&self, key: &[u8]) -> bool {
        self.authorization
            .permits(&self.namespace, Action::Read, &KeyInterval::exact(key))
    }

    /// Whether every key of `keys` may be read now (and the session is
    /// valid even when there is no key).
    pub fn permits_keys<'a>(&self, keys: impl IntoIterator<Item = &'a [u8]>) -> bool {
        self.session_valid() && keys.into_iter().all(|k| self.permits_read(k))
    }

    /// Whether the whole `interval` may be read now.
    ///
    /// A read result discloses more than the keys it happens to contain:
    /// its count, its truncation flag and the keys it does *not* contain
    /// are all statements about the interval that was asked for. Checking
    /// only the returned keys would let a response for a wide interval
    /// pass once policy had been narrowed to one key inside it.
    pub fn permits_interval(&self, interval: &KeyInterval) -> bool {
        self.session_valid()
            && self
                .authorization
                .permits(&self.namespace, Action::Read, interval)
    }

    /// Whether a selected watch batch may be delivered.
    pub fn permits_batch(&self, batch: &WatchBatch) -> bool {
        self.permits_keys(batch.events.iter().map(|e| e.key.as_slice()))
    }
}

/// The keys whose entries a response discloses: range items, transaction
/// branch results and deleted previous entries. A `Put` with a previous
/// value discloses the request key, which the response does not name;
/// callers add the request's keys when `carries_previous` says so.
pub fn protected_keys(response: &Response) -> Vec<Vec<u8>> {
    fn walk(outcome: &Outcome, out: &mut Vec<Vec<u8>>) {
        match outcome {
            Outcome::Range { items, .. } => out.extend(items.iter().map(|i| i.key.clone())),
            Outcome::Delete { prev, .. } => out.extend(prev.iter().map(|i| i.key.clone())),
            Outcome::Txn { results, .. } => results.iter().for_each(|r| walk(r, out)),
            // A Kine conditional operation that did not apply still hands
            // back the entry it saw, key and value: that is read output
            // and is protected like any other.
            Outcome::KineUpdated { current, .. } => {
                out.extend(current.iter().map(|kv| kv.key.clone()));
            }
            Outcome::KineDeleted { prev, .. } => {
                out.extend(prev.iter().map(|kv| kv.key.clone()));
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    walk(&response.outcome, &mut out);
    out.sort();
    out.dedup();
    out
}

/// Whether a response is read output: it discloses state rather than
/// only acknowledging a mutation.
///
/// An empty key list is not evidence of an unprotected acknowledgement. A
/// count-only range names no key and still reports how many there were;
/// an empty range discloses absence. Both have to be reauthorized, so
/// they are classified by what the outcome *is*, not by what it happens
/// to contain.
pub fn is_read_output(response: &Response) -> bool {
    fn walk(outcome: &Outcome) -> bool {
        match outcome {
            Outcome::Range { .. } | Outcome::KineUpdated { .. } | Outcome::KineDeleted { .. } => {
                true
            }
            Outcome::Delete { prev, .. } => !prev.is_empty(),
            Outcome::Put { prev } => prev.is_some(),
            Outcome::Txn { results, .. } => results.iter().any(walk),
            _ => false,
        }
    }
    walk(&response.outcome)
}

/// Whether a response carries previous values of keys it does not name.
pub fn carries_previous(response: &Response) -> bool {
    fn walk(outcome: &Outcome) -> bool {
        match outcome {
            Outcome::Put { prev } => prev.is_some(),
            Outcome::Txn { results, .. } => results.iter().any(walk),
            _ => false,
        }
    }
    walk(&response.outcome)
}

/// Why no barrier could be read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolicyError {
    /// Replicated policy cannot be read now: deny.
    Unavailable,
}

/// A source of fresh barriers.
pub trait PolicySource {
    /// Read current policy for `session` in `namespace`.
    fn barrier(
        &self,
        namespace: NamespaceId,
        session: &SessionId,
    ) -> Result<AuthorizationBarrier, PolicyError>;
}

/// Barriers read from a store's durable snapshot.
///
/// It reads and never writes, so it is over the persistence seam rather
/// than over one coordinator: the same gate has to hold whether this
/// node's record is its projection or a shared journal, and a second
/// copy of it for the other path would be a second place for an
/// authorization rule to go stale.
pub struct StorePolicySource<'a, P: Persistence> {
    /// Where this node's state is.
    pub store: &'a P,
    /// Row and byte budget of one read.
    pub budget: ViewBudget,
}

impl<P: Persistence> PolicySource for StorePolicySource<'_, P> {
    fn barrier(
        &self,
        namespace: NamespaceId,
        session: &SessionId,
    ) -> Result<AuthorizationBarrier, PolicyError> {
        let gated = self
            .store
            .reader()
            .snapshot()
            .map_err(|_| PolicyError::Unavailable)?;
        let authorization = load_authorization(gated.view(), namespace, session, self.budget)
            .map_err(|_| PolicyError::Unavailable)?;
        let position = gated.meta().frontier.as_base().execution_position;
        Ok(AuthorizationBarrier::new(
            position,
            namespace,
            authorization,
        ))
    }
}
