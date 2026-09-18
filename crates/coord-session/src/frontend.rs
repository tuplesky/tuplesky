//! The bound frontend: the collector's dispatcher behind per-connection
//! bindings and the output gate.
//!
//! * A frame on an unbound connection is `NotBound`; on an expired one,
//!   `Expired` (the transport closes it); a `Bind` verifies the token
//!   and answers `BindAck`, a rebind keeps the session.
//! * Requests, resolutions and watch opens go to the dispatcher with the
//!   binding's caller; the namespace and keys of each request are kept
//!   so later disclosures can be gated.
//! * Every response that discloses entries (unary result, retained
//!   result) is gated against one fresh barrier at delivery; each bounded
//!   watch pump loads one fresh barrier for the batches it selects. A
//!   denied disclosure becomes a `NOT_ADMITTED` error response; a denied
//!   batch closes the watch as unauthorized before any progress crosses
//!   it. An unreadable barrier denies (fail closed).

use std::collections::{BTreeMap, VecDeque};

use coord_authn::ClockHealth;
use coord_collector::{Action, Delivery, Dispatcher, codes};
use coord_state::Response;
use coord_storage::WatchHub;
use coord_types::RetryKey;
use coord_types::ids::NamespaceId;
use coord_types::logical_v1::{BranchOp, CanonicalOperation, LogicalRequest};
use coord_types::wire_v1::{Frame, MessageV1, OutcomeV1, decode, decode_stream};

use crate::binding::{BindError, Binding, BindingConfig, verify_bind};
use crate::gate::{PolicySource, carries_previous, protected_keys};
use crate::wire::{BindAckV1, KIND_BIND, bind_ack_frame, decode_bind};

/// What a frame produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Ingress {
    /// A binding was made or refreshed: write the acknowledgement.
    Bound(Vec<u8>),
    /// The binding was refused: close with negotiation code `2`.
    Rejected(BindError),
    /// No binding yet: close with code `2`.
    NotBound,
    /// The binding's validity ended: close with code `2`; admitted work
    /// stays resolvable under the session.
    Expired,
    /// The dispatcher's action (already gated where it discloses data).
    Action(Action),
}

#[derive(Clone, Debug)]
struct RequestInfo {
    namespace: NamespaceId,
    keys: Vec<Vec<u8>>,
}

/// The frontend of one domain with session binding and output gating.
pub struct BoundFrontend {
    dispatcher: Dispatcher,
    config: BindingConfig,
    bindings: BTreeMap<u64, Binding>,
    watches: BTreeMap<(u64, u64), NamespaceId>,
    requests: BTreeMap<RetryKey, RequestInfo>,
    order: VecDeque<RetryKey>,
    max_requests: usize,
    pump_bound: usize,
    /// Barriers read so far (one per gated delivery or bounded pump).
    pub barriers_read: u64,
    /// Disclosures denied by a barrier.
    pub denied: u64,
}

impl BoundFrontend {
    /// Wrap `dispatcher`; `pump_bound` items per watch pump share one
    /// barrier; `max_requests` request identities are remembered.
    pub fn new(
        dispatcher: Dispatcher,
        config: BindingConfig,
        pump_bound: usize,
        max_requests: usize,
    ) -> Self {
        BoundFrontend {
            dispatcher,
            config,
            bindings: BTreeMap::new(),
            watches: BTreeMap::new(),
            requests: BTreeMap::new(),
            order: VecDeque::new(),
            max_requests,
            pump_bound: pump_bound.max(1),
            barriers_read: 0,
            denied: 0,
        }
    }

    /// The dispatcher.
    pub const fn dispatcher(&self) -> &Dispatcher {
        &self.dispatcher
    }

    /// The dispatcher, mutably (evidence, releases).
    pub const fn dispatcher_mut(&mut self) -> &mut Dispatcher {
        &mut self.dispatcher
    }

    /// The binding of `connection`.
    pub fn binding(&self, connection: u64) -> Option<&Binding> {
        self.bindings.get(&connection)
    }

    /// Replace the STS keys (rotation).
    pub fn set_jwks(&mut self, jwks: serde_json::Value) {
        self.config.jwks = jwks;
    }

    /// One frame of `connection` at `clock`.
    pub fn on_frame(
        &mut self,
        clock: &ClockHealth,
        connection: u64,
        frame: &Frame,
        hub: &WatchHub,
        policy: &dyn PolicySource,
    ) -> Ingress {
        if frame.kind == KIND_BIND {
            return self.bind(clock, connection, frame);
        }
        let Some(binding) = self.bindings.get(&connection) else {
            return Ingress::NotBound;
        };
        if !binding.active(clock) {
            return Ingress::Expired;
        }
        let caller = binding.caller();
        let session = binding.session;
        // Bookkeeping for later disclosures.
        let decoded = decode(frame).ok();
        if let Some(MessageV1::Request(r)) = &decoded
            && let Ok(logical) = r.logical()
        {
            self.remember(r.retry_key, &logical);
        }
        let action = self
            .dispatcher
            .on_frame(clock.now, connection, &caller, frame, hub);
        let action = match action {
            Action::WatchOpened {
                connection,
                watch_id,
                registration,
            } => {
                if let Some(MessageV1::WatchOpen(o)) = &decoded {
                    self.watches.insert((connection, watch_id), o.namespace);
                }
                Action::WatchOpened {
                    connection,
                    watch_id,
                    registration,
                }
            }
            Action::Respond(delivery) => Action::Respond(self.gate(delivery, session, policy)),
            other => other,
        };
        Ingress::Action(action)
    }

    fn bind(&mut self, clock: &ClockHealth, connection: u64, frame: &Frame) -> Ingress {
        let bind = match decode_bind(frame) {
            Ok(b) => b,
            Err(_) => return Ingress::Rejected(BindError::Malformed),
        };
        let existing = self.bindings.get(&connection);
        match verify_bind(&self.config, bind.token.as_slice(), clock, existing) {
            Ok(binding) => {
                let ack = BindAckV1 {
                    session: binding.session,
                    expires_at: binding.expires_at,
                    scope: binding.scope_ceiling,
                    rule_generation: binding.rule_generation,
                };
                self.bindings.insert(connection, binding);
                Ingress::Bound(bind_ack_frame(&ack).expect("bounded"))
            }
            Err(e) => Ingress::Rejected(e),
        }
    }

    fn remember(&mut self, key: RetryKey, logical: &LogicalRequest) {
        let info = RequestInfo {
            namespace: logical.namespace,
            keys: request_keys(logical),
        };
        if self.requests.insert(key, info).is_none() {
            self.order.push_back(key);
            while self.order.len() > self.max_requests {
                if let Some(old) = self.order.pop_front() {
                    self.requests.remove(&old);
                }
            }
        }
    }

    /// Gate a delivery for `connection`'s session (a release, a retained
    /// result or a resolution) against a fresh barrier.
    pub fn deliver(&mut self, delivery: Delivery, policy: &dyn PolicySource) -> Delivery {
        let Some(session) = self.bindings.get(&delivery.connection).map(|b| b.session) else {
            return delivery;
        };
        self.gate(delivery, session, policy)
    }

    fn gate(
        &mut self,
        delivery: Delivery,
        session: coord_types::ids::SessionId,
        policy: &dyn PolicySource,
    ) -> Delivery {
        let response = match decode_stream(&delivery.frame).as_deref() {
            Ok([MessageV1::Response(r)]) => r.clone(),
            _ => return delivery,
        };
        let OutcomeV1::Ok { result, .. } = &response.outcome else {
            return delivery;
        };
        let Ok(decoded) = postcard::from_bytes::<Response>(result.as_slice()) else {
            return self.deny(delivery, response.command_id);
        };
        let mut keys = protected_keys(&decoded);
        let info = self.requests.get(&delivery.retry_key);
        if carries_previous(&decoded) {
            match info {
                Some(i) => keys.extend(i.keys.iter().cloned()),
                // The request is unknown here: its keys cannot be named,
                // so the previous values are not disclosed.
                None => return self.deny(delivery, response.command_id),
            }
        }
        if keys.is_empty() {
            return delivery;
        }
        let Some(namespace) = info.map(|i| i.namespace) else {
            return self.deny(delivery, response.command_id);
        };
        self.barriers_read += 1;
        match policy.barrier(namespace, &session) {
            Ok(barrier) if barrier.permits_keys(keys.iter().map(Vec::as_slice)) => delivery,
            _ => self.deny(delivery, response.command_id),
        }
    }

    fn deny(&mut self, delivery: Delivery, command: coord_types::CommandId) -> Delivery {
        self.denied += 1;
        Delivery {
            connection: delivery.connection,
            retry_key: delivery.retry_key,
            frame: MessageV1::Response(codes::error_response(
                command,
                codes::NOT_ADMITTED,
                "output not authorized by current policy",
            ))
            .encode()
            .expect("bounded"),
        }
    }

    /// Pump a watch: one fresh barrier for at most `pump_bound` selected
    /// items; a batch the barrier does not permit closes the watch.
    pub fn pump_watch(
        &mut self,
        clock: &ClockHealth,
        connection: u64,
        watch_id: u64,
        hub: &WatchHub,
        policy: &dyn PolicySource,
    ) -> Vec<Vec<u8>> {
        let bound = self.pump_bound;
        let binding = self.bindings.get(&connection);
        let namespace = self.watches.get(&(connection, watch_id)).copied();
        let barrier = match (binding, namespace) {
            (Some(b), Some(ns)) if b.active(clock) => {
                self.barriers_read += 1;
                policy.barrier(ns, &b.session).ok()
            }
            _ => None,
        };
        let mut denied = 0;
        let frames = self
            .dispatcher
            .pump_watch(hub, connection, watch_id, bound, |batch| {
                let ok = barrier.as_ref().is_some_and(|b| b.permits_batch(batch));
                if !ok {
                    denied += 1;
                }
                ok
            });
        self.denied += denied;
        if frames
            .iter()
            .any(|f| matches!(decode_stream(f).as_deref(), Ok([MessageV1::WatchClose(_)])))
        {
            self.watches.remove(&(connection, watch_id));
        }
        frames
    }

    /// Time passed: connections whose binding ended (to close) and the
    /// dispatcher's deadline deliveries.
    pub fn tick(&mut self, clock: &ClockHealth) -> (Vec<u64>, Vec<Delivery>) {
        let expired: Vec<u64> = self
            .bindings
            .iter()
            .filter(|(_, b)| !b.active(clock))
            .map(|(c, _)| *c)
            .collect();
        let deliveries = self.dispatcher.expire(clock.now);
        (expired, deliveries)
    }

    /// A connection closed: its binding, watches and attached requests go.
    pub fn on_connection_closed(&mut self, connection: u64, hub: &WatchHub) {
        self.bindings.remove(&connection);
        self.watches.retain(|(c, _), _| *c != connection);
        self.dispatcher.on_connection_closed(connection, hub);
    }
}

/// The keys a request names (whose previous values a response may
/// disclose).
pub fn request_keys(request: &LogicalRequest) -> Vec<Vec<u8>> {
    fn branch(ops: &[BranchOp], out: &mut Vec<Vec<u8>>) {
        for op in ops {
            match op {
                BranchOp::Put(p) => out.push(p.key.clone()),
                BranchOp::Range(_) | BranchOp::DeleteRange(_) => {}
                #[allow(unreachable_patterns)]
                _ => {}
            }
        }
    }
    let mut out = Vec::new();
    match &request.operation {
        CanonicalOperation::Put(p) => out.push(p.key.clone()),
        CanonicalOperation::Txn(t) => {
            branch(&t.success, &mut out);
            branch(&t.failure, &mut out);
        }
        _ => {}
    }
    out
}
