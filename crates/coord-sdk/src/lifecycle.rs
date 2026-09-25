//! The request lifecycle (design Sections 4.4, 6.5, 11.5, 19.4).
//!
//! ```text
//! Queued ──send──▶ InFlight ──response──▶ Done
//!   ▲                 │  reset/reconnect: same invocation, re-queued
//!   │                 └──deadline──▶ Unknown ──resolve──▶ Resolving ──▶ Done
//!   └──backpressure────────────────────────┘                 │ Pending: Unknown again
//! ```
//!
//! * A connection reset or reconnect re-queues the invocation with the
//!   same retry key and the same frame bytes; nothing allocates a fresh
//!   identity on the SDK's own initiative.
//! * A payload change under an allocated sequence is refused locally; a
//!   server `RequestIdentityConflict` is the same typed error.
//! * A deadline reports the outcome as *unknown* (a completion the
//!   application sees), keeps the invocation and resolves it by identity
//!   with `ResolveRequest`; `Pending` keeps it unknown, `Unknown` from the
//!   server is final.
//! * Outstanding requests (queued, in flight, unknown) are bounded; the
//!   pool bounds concurrent streams; a credential is presented once per
//!   connection binding.
//! * Only typed API responses are understood. Any other frame is a
//!   protocol violation: protocol evidence never reaches an application.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use coord_types::CommandId;
use coord_types::ids::KvRevision;
use coord_types::logical_v1::LogicalRequest;
use coord_types::wire_v1::{
    MessageV1, OutcomeV1, ResolveRequestV1, ResponseV1, codes, decode_stream,
};

use crate::credential::{CachedProvider, Credential, CredentialError, CredentialProvider};
use crate::identity::{ClientInstance, Invocation, InvocationError};
use crate::pool::{Pool, PoolError, PoolLimits, StreamPermit};

/// A connection the application opened.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConnectionId(pub u64);

/// A request handle: the invocation's request sequence.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RequestId(pub u64);

/// Client bounds and timing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientConfig {
    /// Pool bounds.
    pub pool: PoolLimits,
    /// Refresh the credential this many ticks before expiry.
    pub credential_margin: u64,
    /// Requests not done at once (queued, in flight, unknown).
    pub max_outstanding: usize,
    /// Wait between resolution attempts of an unknown outcome.
    pub resolve_interval: u64,
    /// Wait before re-sending after backpressure.
    pub backoff: u64,
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            pool: PoolLimits::default(),
            credential_margin: 30_000,
            max_outstanding: 256,
            resolve_interval: 1_000,
            backoff: 100,
        }
    }
}

/// A typed retry error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RetryError {
    /// The sequence is bound to another payload; the operation must not
    /// be retried under this identity.
    PayloadConflict {
        /// Sequence.
        sequence: u64,
    },
    /// The endpoint refused for lack of capacity; the same invocation is
    /// retried after the backoff.
    Backpressure,
    /// The binding was not accepted (credential or session): rebind.
    NotAdmitted,
    /// The request was rejected as malformed.
    Malformed,
    /// The result does not fit the response bound.
    ResultTooLarge,
    /// The request is larger than the frontend admits; retrying the same
    /// request is refused the same way.
    RequestTooLarge,
    /// Another frozen code.
    Other {
        /// Code.
        code: u16,
    },
}

/// What a request came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Established.
    Established {
        /// KV revision produced.
        revision: Option<KvRevision>,
        /// Encoded result.
        result: Vec<u8>,
    },
    /// Established error.
    Failed(RetryError),
    /// Unknown: the deadline passed or the endpoint lost the identity; the
    /// operation may or may not have happened. Resolve by identity.
    Unknown,
}

/// Where a request is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RequestState {
    /// Waiting for a stream (or for the backoff).
    Queued,
    /// Sent on a connection.
    InFlight(ConnectionId),
    /// Deadline passed; awaiting a resolution slot.
    Unknown,
    /// A `ResolveRequest` is in flight.
    Resolving(ConnectionId),
    /// Finished.
    Done(Outcome),
}

/// A finished (or unknown) request for the application.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Completion {
    /// Request.
    pub request: RequestId,
    /// Command identity.
    pub command_id: CommandId,
    /// Outcome.
    pub outcome: Outcome,
}

/// What the application must do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SdkAction {
    /// Present the credential on the connection once (the binding).
    Bind {
        /// Connection.
        connection: ConnectionId,
        /// Credential to present.
        credential: Credential,
    },
    /// Write the request frame on a new stream of the connection.
    Send {
        /// Connection.
        connection: ConnectionId,
        /// Request.
        request: RequestId,
        /// Frame bytes (identical on every retry).
        frame: Vec<u8>,
    },
    /// Write a `ResolveRequest` frame on a new stream of the connection.
    Resolve {
        /// Connection.
        connection: ConnectionId,
        /// Request.
        request: RequestId,
        /// Frame bytes.
        frame: Vec<u8>,
    },
    /// Reset the request's stream: its deadline passed and the client is
    /// about to reuse the stream credit it held. Nothing else closes that
    /// stream, so without this the original request and its resolution
    /// would both be open on a connection sized for one.
    Reset {
        /// Connection.
        connection: ConnectionId,
        /// Request whose stream is abandoned.
        request: RequestId,
    },
}

/// Why the client refused an application call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientError {
    /// Identity allocation or retry.
    Invocation(InvocationError),
    /// Pool.
    Pool(PoolError),
    /// Credential.
    Credential(CredentialError),
    /// The outstanding window is full.
    Window {
        /// Outstanding requests.
        outstanding: usize,
    },
    /// No such request.
    UnknownRequest,
    /// A frame that is not a typed API response (evidence, negotiation or
    /// watch frames on a request stream).
    ProtocolViolation,
    /// A response for a command this client does not have in flight.
    UnexpectedResponse,
}

#[derive(Debug)]
struct Entry {
    invocation: Invocation,
    state: RequestState,
    permit: Option<StreamPermit>,
    deadline: Option<u64>,
    deadline_ms: u32,
    not_before: u64,
    /// Sends and resolves performed.
    attempts: u32,
}

/// The SDK client of one session against one endpoint.
#[derive(Debug)]
pub struct Client<P> {
    config: ClientConfig,
    instance: ClientInstance,
    credentials: CachedProvider<P>,
    pool: Pool,
    requests: BTreeMap<RequestId, Entry>,
    actions: VecDeque<SdkAction>,
    completions: VecDeque<Completion>,
    /// Sequences below and including this are finished and their identity
    /// bindings have been retired. A client in service never gets to call
    /// `ClientInstance::retire` itself, so a workload that submits,
    /// completes and forgets for ever would otherwise grow the binding
    /// map and the serialized instance state without bound.
    retired_through: u64,
    /// Finished sequences above the floor, waiting for the gap below them
    /// to close. Bounded by the gap, not by the work done.
    finished: BTreeSet<u64>,
}

impl<P: CredentialProvider> Client<P> {
    /// A client over `instance` obtaining credentials from `provider`.
    pub fn new(config: ClientConfig, instance: ClientInstance, provider: P) -> Self {
        Client {
            config,
            pool: Pool::new(config.pool),
            credentials: CachedProvider::new(provider, config.credential_margin),
            instance,
            requests: BTreeMap::new(),
            retired_through: 0,
            finished: BTreeSet::new(),
            actions: VecDeque::new(),
            completions: VecDeque::new(),
        }
    }

    /// The instance (persist its state across restarts).
    pub const fn instance(&self) -> &ClientInstance {
        &self.instance
    }

    /// The pool.
    pub const fn pool(&self) -> &Pool {
        &self.pool
    }

    /// Credential exchanges performed so far.
    pub const fn exchanges(&self) -> u64 {
        self.credentials.exchanges
    }

    /// Whether the application should open another connection.
    pub fn wants_connection(&self) -> bool {
        self.pool.wants_connection()
    }

    /// Actions to perform, in order.
    pub fn take_actions(&mut self) -> Vec<SdkAction> {
        self.actions.drain(..).collect()
    }

    /// Completions for the application.
    pub fn take_completions(&mut self) -> Vec<Completion> {
        self.completions.drain(..).collect()
    }

    /// State of a request.
    pub fn state(&self, request: RequestId) -> Option<&RequestState> {
        self.requests.get(&request).map(|e| &e.state)
    }

    /// The invocation of a request.
    pub fn invocation(&self, request: RequestId) -> Option<&Invocation> {
        self.requests.get(&request).map(|e| &e.invocation)
    }

    /// Sends and resolves performed for a request.
    pub fn attempts(&self, request: RequestId) -> u32 {
        self.requests.get(&request).map_or(0, |e| e.attempts)
    }

    /// Requests not done.
    pub fn outstanding(&self) -> usize {
        self.requests
            .values()
            .filter(|e| !matches!(e.state, RequestState::Done(_)))
            .count()
    }

    /// The application opened `connection` at `now`: the credential is
    /// presented once, through the `Bind` action.
    pub fn connect(&mut self, now: u64, connection: ConnectionId) -> Result<(), ClientError> {
        self.pool.opened(connection).map_err(ClientError::Pool)?;
        let credential = match self.credentials.credential(now) {
            Ok(c) => c,
            Err(e) => {
                self.pool.closed(connection);
                return Err(ClientError::Credential(e));
            }
        };
        self.actions.push_back(SdkAction::Bind {
            connection,
            credential,
        });
        Ok(())
    }

    /// The binding on `connection` was accepted: queued work flows.
    pub fn bound(&mut self, now: u64, connection: ConnectionId) {
        self.pool.bound(connection);
        self.pump(now);
    }

    /// The binding was rejected (expired or revoked credential): the
    /// cached credential is dropped; the next connect exchanges again.
    pub fn binding_rejected(&mut self, connection: ConnectionId) {
        self.credentials.invalidate();
        self.pool.closed(connection);
    }

    /// Submit a new operation with `deadline_ms` (0: none).
    pub fn submit(
        &mut self,
        now: u64,
        request: &LogicalRequest,
        deadline_ms: u32,
    ) -> Result<RequestId, ClientError> {
        self.check_window()?;
        let invocation = self
            .instance
            .allocate(request, deadline_ms)
            .map_err(ClientError::Invocation)?;
        let id = RequestId(invocation.retry_key.request_sequence.get());
        self.insert(now, id, invocation, deadline_ms);
        self.pump(now);
        Ok(id)
    }

    /// Retry an allocated `sequence` (after a restart, or a request the
    /// application forgot) with the same payload; another payload is a
    /// typed conflict and nothing is sent.
    pub fn retry(
        &mut self,
        now: u64,
        sequence: u64,
        request: &LogicalRequest,
        deadline_ms: u32,
    ) -> Result<RequestId, ClientError> {
        let id = RequestId(sequence);
        // Validate before anything else, including for a sequence that is
        // still queued, in flight or resolving: the API promises that a
        // changed payload under an allocated sequence is refused locally,
        // and returning early for an active request skipped exactly that
        // check for the requests it matters most for.
        let invocation = self
            .instance
            .retry(sequence, request, deadline_ms)
            .map_err(ClientError::Invocation)?;
        if let Some(e) = self.requests.get(&id)
            && !matches!(e.state, RequestState::Done(_))
        {
            return Ok(id);
        }
        if let Some(e) = self.requests.get(&id)
            && let RequestState::Done(outcome) = &e.state
        {
            self.completions.push_back(Completion {
                request: id,
                command_id: invocation.command_id,
                outcome: outcome.clone(),
            });
            return Ok(id);
        }
        self.check_window()?;
        self.insert(now, id, invocation, deadline_ms);
        self.pump(now);
        Ok(id)
    }

    /// Ask for resolution of an unknown outcome now.
    pub fn resolve(&mut self, now: u64, request: RequestId) -> Result<(), ClientError> {
        let e = self
            .requests
            .get_mut(&request)
            .ok_or(ClientError::UnknownRequest)?;
        if !matches!(e.state, RequestState::Unknown) {
            return Ok(());
        }
        e.not_before = now;
        self.pump(now);
        Ok(())
    }

    /// Forget a finished or unknown request (frees its outstanding slot).
    pub fn forget(&mut self, request: RequestId) -> Option<Outcome> {
        let e = self.requests.remove(&request)?;
        if let Some(p) = e.permit {
            self.pool.release(p);
        }
        let outcome = match e.state {
            RequestState::Done(o) => Some(o),
            _ => Some(Outcome::Unknown),
        };
        // Only a request that reached a result is finished for good. One
        // forgotten while its outcome is unknown may still be retried
        // under its own identity, so its binding stays.
        if matches!(outcome, Some(Outcome::Unknown)) {
            return outcome;
        }
        self.finished.insert(request.0);
        self.retire_finished_prefix();
        outcome
    }

    /// Give up on a request for good, retiring its identity even if its
    /// outcome was never established.
    ///
    /// [`Self::forget`] keeps the binding of an unknown outcome so the
    /// caller may still ask again. That is the right default and it has
    /// a cost: the acknowledged floor is a contiguous prefix, so one
    /// request abandoned in silence holds the floor where it is, and the
    /// outstanding window closes around it a window later. A caller that
    /// has reported the failure upward and will never ask again says so
    /// here, and the prefix moves on.
    ///
    /// What is given up is the retained result: a sequence retired this
    /// way is `TooOld` if it is ever presented again, not a replay of
    /// whatever it did. So this is for a request the caller has finished
    /// with, never for one it merely stopped waiting on.
    pub fn abandon(&mut self, request: RequestId) -> Option<Outcome> {
        let e = self.requests.remove(&request)?;
        if let Some(p) = e.permit {
            self.pool.release(p);
        }
        let outcome = match e.state {
            RequestState::Done(o) => Some(o),
            _ => Some(Outcome::Unknown),
        };
        self.finished.insert(request.0);
        self.retire_finished_prefix();
        outcome
    }

    /// Retire the identity bindings of every finished sequence below the
    /// first gap. A gap is a sequence still tracked, or one allocated and
    /// never completed, and its binding has to survive for a retry.
    fn retire_finished_prefix(&mut self) {
        while self.finished.remove(&(self.retired_through + 1)) {
            self.retired_through += 1;
        }
        if self.retired_through > 0 {
            self.instance.retire(self.retired_through);
        }
    }

    /// Sequences whose identity bindings have been retired.
    pub const fn retired_through(&self) -> u64 {
        self.retired_through
    }

    /// A frame arrived on a request stream of `connection`.
    pub fn on_frame(
        &mut self,
        now: u64,
        connection: ConnectionId,
        frame: &[u8],
    ) -> Result<(), ClientError> {
        let response = match decode_stream(frame).as_deref() {
            Ok([MessageV1::Response(r)]) => r.clone(),
            _ => return Err(ClientError::ProtocolViolation),
        };
        let id = self
            .requests
            .iter()
            .find(|(_, e)| {
                e.invocation.command_id == response.command_id
                    && matches!(
                        e.state,
                        RequestState::InFlight(c) | RequestState::Resolving(c) if c == connection
                    )
            })
            .map(|(id, _)| *id)
            .ok_or(ClientError::UnexpectedResponse)?;
        self.apply(now, id, response);
        self.pump(now);
        Ok(())
    }

    fn apply(&mut self, now: u64, id: RequestId, response: ResponseV1) {
        let e = self.requests.get_mut(&id).expect("present");
        let resolving = matches!(e.state, RequestState::Resolving(_));
        if let Some(p) = e.permit.take() {
            self.pool.release(p);
        }
        let outcome = match response.outcome {
            OutcomeV1::Ok { revision, result } => Outcome::Established {
                revision,
                result: result.into_inner(),
            },
            OutcomeV1::Err { code, .. } => match code {
                codes::REQUEST_IDENTITY_CONFLICT => {
                    Outcome::Failed(RetryError::PayloadConflict { sequence: id.0 })
                }
                codes::BACKPRESSURE => {
                    // The same invocation, later; nothing is done.
                    e.state = RequestState::Queued;
                    e.not_before = now + self.config.backoff;
                    return;
                }
                codes::NOT_ADMITTED => {
                    self.credentials.invalidate();
                    Outcome::Failed(RetryError::NotAdmitted)
                }
                codes::MALFORMED_REQUEST => Outcome::Failed(RetryError::Malformed),
                codes::RESULT_TOO_LARGE => Outcome::Failed(RetryError::ResultTooLarge),
                codes::REQUEST_TOO_LARGE => Outcome::Failed(RetryError::RequestTooLarge),
                code => Outcome::Failed(RetryError::Other { code }),
            },
            OutcomeV1::Pending => {
                // Still collecting there: unknown here, resolve again later.
                e.state = RequestState::Unknown;
                e.not_before = now + self.config.resolve_interval;
                if !resolving {
                    self.completions.push_back(Completion {
                        request: id,
                        command_id: e.invocation.command_id,
                        outcome: Outcome::Unknown,
                    });
                }
                return;
            }
            OutcomeV1::Unknown => Outcome::Unknown,
        };
        e.state = RequestState::Done(outcome.clone());
        self.completions.push_back(Completion {
            request: id,
            command_id: e.invocation.command_id,
            outcome,
        });
    }

    /// `connection` was lost: its in-flight invocations are re-queued as
    /// they are (same identity, same bytes) for another connection.
    pub fn on_connection_lost(&mut self, now: u64, connection: ConnectionId) {
        self.pool.closed(connection);
        for e in self.requests.values_mut() {
            match e.state {
                RequestState::InFlight(c) if c == connection => {
                    e.state = RequestState::Queued;
                    e.permit = None;
                }
                RequestState::Resolving(c) if c == connection => {
                    e.state = RequestState::Unknown;
                    e.permit = None;
                }
                _ => {}
            }
        }
        self.pump(now);
    }

    /// Time passed: deadlines make outcomes unknown; backoffs and
    /// resolution intervals elapse.
    pub fn tick(&mut self, now: u64) {
        let mut expired = Vec::new();
        for (id, e) in &mut self.requests {
            if let RequestState::InFlight(_) | RequestState::Queued = e.state
                && let Some(deadline) = e.deadline
                && now >= deadline
            {
                // Nothing here closes the original stream, so reusing its
                // permit for the resolution would put both on a connection
                // sized for one: the reset is an action the application
                // performs before the permit is handed out again.
                let reset = match e.state {
                    RequestState::InFlight(connection) => Some(connection),
                    _ => None,
                };
                if let Some(p) = e.permit.take() {
                    self.pool.release(p);
                }
                e.state = RequestState::Unknown;
                e.not_before = now;
                expired.push((*id, e.invocation.command_id, reset));
            }
        }
        for (id, command_id, reset) in expired {
            if let Some(connection) = reset {
                self.actions.push_back(SdkAction::Reset {
                    connection,
                    request: id,
                });
            }
            self.completions.push_back(Completion {
                request: id,
                command_id,
                outcome: Outcome::Unknown,
            });
        }
        self.pump(now);
    }

    fn check_window(&self) -> Result<(), ClientError> {
        let outstanding = self.outstanding();
        if outstanding >= self.config.max_outstanding {
            return Err(ClientError::Window { outstanding });
        }
        Ok(())
    }

    fn insert(&mut self, now: u64, id: RequestId, invocation: Invocation, deadline_ms: u32) {
        // `RequestV1.deadline_ms` counts from admission, and a queued
        // request has not been admitted: starting the clock here turned a
        // request the pool never transmitted into an unknown outcome and
        // asked the endpoint to resolve an identity it had never seen.
        // `pump` starts it on the first send.
        let deadline = None;
        self.requests.insert(
            id,
            Entry {
                invocation,
                state: RequestState::Queued,
                permit: None,
                deadline,
                deadline_ms,
                not_before: now,
                attempts: 0,
            },
        );
    }

    /// Send what can be sent: queued requests and due resolutions, each
    /// on a stream of the pool; the pool's cap stops here, never by
    /// opening more.
    fn pump(&mut self, now: u64) {
        let ids: Vec<RequestId> = self.requests.keys().copied().collect();
        for id in ids {
            let e = self.requests.get_mut(&id).expect("present");
            if e.not_before > now {
                continue;
            }
            let (frame, resolving) = match e.state {
                RequestState::Queued => (e.invocation.frame.clone(), false),
                RequestState::Unknown => (
                    MessageV1::ResolveRequest(ResolveRequestV1 {
                        retry_key: e.invocation.retry_key,
                        command_id: e.invocation.command_id,
                    })
                    .encode()
                    .expect("bounded"),
                    true,
                ),
                _ => continue,
            };
            let permit = match self.pool.acquire() {
                Ok(p) => p,
                Err(_) => return,
            };
            let connection = permit.connection;
            e.permit = Some(permit);
            e.attempts += 1;
            if resolving {
                e.state = RequestState::Resolving(connection);
                self.actions.push_back(SdkAction::Resolve {
                    connection,
                    request: id,
                    frame,
                });
            } else {
                e.state = RequestState::InFlight(connection);
                if e.deadline.is_none() && e.deadline_ms > 0 {
                    e.deadline = Some(now + u64::from(e.deadline_ms));
                }
                self.actions.push_back(SdkAction::Send {
                    connection,
                    request: id,
                    frame,
                });
            }
        }
    }
}
