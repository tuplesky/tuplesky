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
use coord_state::policy::{Action as PolicyAction, KeyInterval};
use coord_storage::WatchHub;
use coord_types::RetryKey;
use coord_types::ids::NamespaceId;
use coord_types::logical_v1::{BranchOp, CanonicalOperation, LogicalRequest};
use coord_types::wire_v1::{Frame, MessageV1, OutcomeV1, decode, decode_stream};

use crate::binding::{BindError, Binding, BindingConfig, verify_bind};
use crate::gate::{PolicySource, carries_previous, is_read_output, protected_keys};
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
    /// The identity this metadata was accepted under. It is what makes
    /// the binding immutable: a later frame under the same retry key
    /// describes the same command or it is not this request at all.
    command: coord_types::CommandId,
    namespace: NamespaceId,
    keys: Vec<Vec<u8>>,
    /// Intervals the request asked to read, so a replayed result is
    /// reauthorized over what was asked for and not merely over what came
    /// back.
    intervals: Vec<KeyInterval>,
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
        let scope = binding.scope_ceiling;
        let decoded = decode(frame).ok();
        let presented = match &decoded {
            Some(MessageV1::Request(r)) => r.logical().ok().map(|l| (r.retry_key, l)),
            _ => None,
        };
        let action = self
            .dispatcher
            .on_frame(clock.now, connection, &caller, frame, hub);
        // Bookkeeping for later disclosures, recorded only for a request
        // the dispatcher accepted under its own identity. Recording it
        // before would let a conflicting payload under a pending retry
        // key replace the namespace and keys that authorize the original
        // command's result, and the rejection would not put them back.
        match (&action, &presented) {
            (Action::FanOut(f), Some((key, logical))) => {
                self.remember(*key, f.command, logical);
            }
            (Action::Pending { command }, Some((key, logical))) => {
                self.remember(*key, *command, logical);
            }
            _ => {}
        }
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
            Action::Respond(delivery) => {
                Action::Respond(self.gate(delivery, session, scope, policy))
            }
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

    /// Bind the disclosure metadata of `key` to the command it was
    /// accepted under. The first accepted request wins: a retry presents
    /// the same identity and therefore the same metadata, and anything
    /// else is not this request.
    fn remember(
        &mut self,
        key: RetryKey,
        command: coord_types::CommandId,
        logical: &LogicalRequest,
    ) {
        if let Some(existing) = self.requests.get(&key) {
            debug_assert_eq!(existing.command, command, "identity decides the metadata");
            return;
        }
        let info = RequestInfo {
            command,
            namespace: logical.namespace,
            keys: request_keys(logical),
            intervals: request_intervals(logical),
        };
        self.requests.insert(key, info);
        self.order.push_back(key);
        while self.order.len() > self.max_requests {
            if let Some(old) = self.order.pop_front() {
                self.requests.remove(&old);
            }
        }
    }

    /// Gate a delivery for `connection`'s session (a release, a retained
    /// result or a resolution) against a fresh barrier.
    pub fn deliver(&mut self, delivery: Delivery, policy: &dyn PolicySource) -> Delivery {
        let Some((session, scope)) = self
            .bindings
            .get(&delivery.connection)
            .map(|b| (b.session, b.scope_ceiling))
        else {
            return delivery;
        };
        self.gate(delivery, session, scope, policy)
    }

    fn gate(
        &mut self,
        delivery: Delivery,
        session: coord_types::ids::SessionId,
        scope: u32,
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
        // An empty key list is not evidence that nothing is disclosed: a
        // count-only range names no key and still reports how many there
        // were, and an empty range discloses absence. Read output is
        // reauthorized whether or not it carries a key; only a pure
        // mutation acknowledgement passes without a barrier.
        let reads = is_read_output(&decoded);
        if keys.is_empty() && !reads {
            return delivery;
        }
        let Some(info) = info else {
            return self.deny(delivery, response.command_id);
        };
        let namespace = info.namespace;
        let intervals = info.intervals.clone();
        // Read output also needs the bound token's own permission. The
        // replicated session's ceiling can be wider than the scope in the
        // token this connection presented, and a watch open bypasses
        // unary admission entirely, so without this a token without the
        // read bit would still receive read output.
        if reads && scope & PolicyAction::Read.bit() == 0 {
            return self.deny(delivery, response.command_id);
        }
        self.barriers_read += 1;
        let Ok(barrier) = policy.barrier(namespace, &session) else {
            return self.deny(delivery, response.command_id);
        };
        if !barrier.permits_keys(keys.iter().map(Vec::as_slice)) {
            return self.deny(delivery, response.command_id);
        }
        // What the request asked to read, not only what came back: the
        // count, the truncation flag and the keys that are absent all
        // describe the interval that was asked for.
        if !intervals.iter().all(|i| barrier.permits_interval(i)) {
            return self.deny(delivery, response.command_id);
        }
        delivery
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
            // Watch output is read output, and the bound token's scope
            // restricts it like any other. A watch open bypasses unary
            // admission, so without this a token without the read bit
            // would receive events whenever the session alone permitted
            // them; the replicated session's ceiling can be wider than
            // the scope of the token this connection actually presented.
            (Some(b), Some(ns))
                if b.active(clock) && b.scope_ceiling & PolicyAction::Read.bit() != 0 =>
            {
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
/// The intervals a request asks to read. A replayed result is
/// reauthorized over these, not only over the keys it returned.
pub fn request_intervals(request: &LogicalRequest) -> Vec<KeyInterval> {
    fn of_range(r: &coord_types::logical_v1::KeyRange) -> KeyInterval {
        match &r.range_end {
            None => KeyInterval::exact(&r.key),
            Some(end) => KeyInterval {
                lower: r.key.clone(),
                upper: Some(end.clone()),
            },
        }
    }
    fn branch(ops: &[BranchOp], out: &mut Vec<KeyInterval>) {
        for op in ops {
            if let BranchOp::Range(r) = op {
                out.push(of_range(&r.range));
            }
        }
    }
    let mut out = Vec::new();
    match &request.operation {
        CanonicalOperation::Range(r) => out.push(of_range(&r.range)),
        CanonicalOperation::Txn(t) => {
            for c in &t.compares {
                out.push(KeyInterval::exact(&c.key));
            }
            branch(&t.success, &mut out);
            branch(&t.failure, &mut out);
        }
        _ => {}
    }
    out
}

/// The keys a request names and whose previous values its response may
/// carry without naming them (a `Put` with `prev_kv`, and the puts of a
/// transaction branch).
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
