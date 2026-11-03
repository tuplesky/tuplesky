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

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use coord_authn::ClockHealth;
use coord_collector::MonotonicMillis;
use coord_collector::{Action, Delivery, Dispatcher, codes};
use coord_core::event::AdmittedRequest;
use coord_state::Response;
use coord_state::plan::Outcome;
use coord_state::policy::{Action as PolicyAction, KeyInterval};
use coord_storage::WatchHub;
use coord_types::RetryKey;
use coord_types::ids::NamespaceId;
use coord_types::logical_v1::{BranchOp, CanonicalOperation, LogicalRequest};
use coord_types::wire_v1::{Frame, MessageV1, OutcomeV1, RequestV1, decode, decode_stream};

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
    /// The credential was verified and the session it names does not
    /// exist yet: send this plan to the voters and hold the bind stream
    /// under its invocation. The acknowledgement is written when the
    /// establishment's outcome comes back, never before.
    Establishing(Box<coord_collector::FanOut>),
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
    /// The client's own deadline, where it named one: a disclosure held
    /// at the gate is answered `Pending` by then at the latest.
    deadline: Option<MonotonicMillis>,
}

/// The longest the output gate holds a disclosure whose session this
/// node has not projected yet, in milliseconds, when the client named no
/// earlier deadline (task-d93).
///
/// A node projects a session one leader turn and a hop after the leader
/// does (task-d74), so the hold is normally released within a few
/// milliseconds. The bound is for a node that is behind by more than
/// that: it answers `Pending` then, as it did before the hold, and the
/// caller resolves by identity.
pub const GATE_HOLD_MILLIS: u64 = 2_000;

/// A disclosure the output gate holds until this node has projected the
/// caller's session (task-d93).
#[derive(Clone, Debug)]
struct Held {
    delivery: Delivery,
    session: coord_types::ids::SessionId,
    scope: u32,
    /// When the hold ends with `Pending` if the projection has not
    /// caught up: the client's deadline, or `GATE_HOLD_MILLIS` after the
    /// hold began, whichever is earlier.
    until: MonotonicMillis,
    /// When the hold began, so a retry that presents another deadline
    /// moves `until` without moving the hold's own bound.
    began: MonotonicMillis,
}

/// When a hold that began at `began` ends: the client's deadline, or
/// `GATE_HOLD_MILLIS` after it began, whichever is earlier.
fn hold_until(began: MonotonicMillis, deadline: Option<MonotonicMillis>) -> MonotonicMillis {
    let bound = began.plus(GATE_HOLD_MILLIS);
    deadline.map_or(bound, |d| d.min(bound))
}

/// What the gate decided about one delivery.
enum Decision {
    /// Write this.
    Out(Delivery),
    /// This node has not projected the caller's session yet.
    NotYet(Delivery, coord_types::CommandId),
}

/// What the gate did with one delivery.
enum Gated {
    /// Write this.
    Out(Delivery),
    /// Held until the projection shows the session; `release_held`
    /// hands it out.
    Held(coord_types::CommandId),
}

/// A binding waiting for the session it named to be established.
#[derive(Clone, Debug)]
struct Establishing {
    /// Connection whose bind stream is held.
    connection: u64,
    /// What the verified credential said.
    binding: Binding,
    /// The acknowledgement to write once the session exists.
    ack: Vec<u8>,
}

/// What a delivery becomes for the caller waiting on it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Delivered {
    /// Write this frame on the caller's stream.
    Answer(Delivery),
    /// The establishment this connection was waiting for did not
    /// happen. Release its stream with no answer: an unbound caller is
    /// told that it has no session, and nothing further.
    Unbound {
        /// Connection whose bind stream was held.
        connection: u64,
        /// Invocation the stream was held under.
        retry_key: RetryKey,
        /// Why, for this node's own record.
        reason: BindError,
    },
    /// The output gate holds this delivery until this node has projected
    /// the caller's session (task-d93). Keep the caller's stream:
    /// `release_held` hands the delivery out, gated, once the
    /// projection shows the session or the hold's deadline passes.
    Held,
}

/// The frontend of one domain with session binding and output gating.
pub struct BoundFrontend {
    dispatcher: Dispatcher,
    config: BindingConfig,
    bindings: BTreeMap<u64, Binding>,
    establishing: BTreeMap<RetryKey, Establishing>,
    watches: BTreeMap<(u64, u64), NamespaceId>,
    requests: BTreeMap<RetryKey, RequestInfo>,
    order: VecDeque<RetryKey>,
    max_requests: usize,
    pump_bound: usize,
    /// Barriers read so far (one per gated delivery or bounded pump).
    pub barriers_read: u64,
    /// Replicated session states read while binding. Counted apart from
    /// `barriers_read`, which is the disclosure gate's: this one asks
    /// whether a session exists, not whether output may be shown.
    pub sessions_checked: u64,
    /// Disclosures denied by a barrier.
    pub denied: u64,
    /// Disclosures answered `Pending` because this node's replicated
    /// state had not caught up with the session by the end of the hold.
    /// Counted apart from `denied`, because they are not refusals: the
    /// caller is told to ask again.
    pub deferred: u64,
    /// Disclosures the gate held for the projection to catch up with the
    /// session (task-d93), however each hold ended.
    pub holds: u64,
    /// The disclosures held now, oldest first.
    held: Vec<Held>,
    /// Connections whose session a barrier on this node has shown at
    /// least once.
    ///
    /// The distinction this draws is between "not yet" and "no". A
    /// binding is established from a committed session-creation
    /// command, but the barrier reads this node's *materialized*
    /// projection, which lags the journal. Between the two, a barrier
    /// finds no session row for a session that certainly exists -- and
    /// refusing there turns a replica that is behind into a caller
    /// whose reads are unauthorized. Once a barrier has shown the row,
    /// its absence is a real answer again: policy has moved, and the
    /// projection only moves forward.
    seen: BTreeSet<u64>,
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
            establishing: BTreeMap::new(),
            watches: BTreeMap::new(),
            requests: BTreeMap::new(),
            order: VecDeque::new(),
            max_requests,
            pump_bound: pump_bound.max(1),
            barriers_read: 0,
            sessions_checked: 0,
            deferred: 0,
            holds: 0,
            held: Vec::new(),
            seen: BTreeSet::new(),
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

    /// The namespace a live watch of `connection` was opened on, or
    /// `None` when this frontend holds no such subscription.
    ///
    /// A runtime that owns the output stream needs both: the namespace
    /// to replay from a snapshot into the hub, and the absence of an
    /// entry to know the subscription has ended -- cancelled by the
    /// client, closed by the hub -- so the stream is finished rather
    /// than held open for events that will never come.
    pub fn watch_namespace(&self, connection: u64, watch_id: u64) -> Option<NamespaceId> {
        self.watches.get(&(connection, watch_id)).copied()
    }

    /// Replace the STS keys (rotation).
    pub fn set_jwks(&mut self, jwks: serde_json::Value) {
        self.config.jwks = jwks;
    }

    /// One frame of `connection` at `clock` (the authentication wall
    /// clock, which a binding's expiry and a receipt's stamp are judged
    /// by) and `now` (the monotonic reading a request's deadline is
    /// measured from).
    pub fn on_frame(
        &mut self,
        clock: &ClockHealth,
        now: MonotonicMillis,
        connection: u64,
        frame: &Frame,
        hub: &WatchHub,
        policy: &dyn PolicySource,
    ) -> Ingress {
        if frame.kind == KIND_BIND {
            return self.bind(clock, now, connection, frame, policy);
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
            Some(MessageV1::Request(r)) => r.logical().ok().map(|l| {
                let deadline = (r.deadline_ms > 0).then(|| now.plus(u64::from(r.deadline_ms)));
                (r.retry_key, l, deadline)
            }),
            _ => None,
        };
        let action = self
            .dispatcher
            .on_frame(clock.now, now, connection, &caller, frame, hub);
        // Bookkeeping for later disclosures, recorded only for a request
        // the dispatcher accepted under its own identity. Recording it
        // before would let a conflicting payload under a pending retry
        // key replace the namespace and keys that authorize the original
        // command's result, and the rejection would not put them back.
        match (&action, &presented) {
            (Action::FanOut(f), Some((key, logical, deadline))) => {
                self.remember(*key, f.command, logical, *deadline);
            }
            (Action::Pending { command }, Some((key, logical, deadline))) => {
                self.remember(*key, *command, logical, *deadline);
            }
            // A read sent to the leader barrier is disclosed through the
            // same gate, with the same metadata (task-d50).
            (Action::Read(r), Some((key, logical, deadline))) => {
                self.remember(*key, r.command, logical, *deadline);
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
                match self.gate(Some(now), delivery, session, scope, policy) {
                    Gated::Out(delivery) => Action::Respond(delivery),
                    // The stream is kept under the invocation, as for any
                    // outcome still to come, and `release_held` answers it.
                    Gated::Held(command) => Action::Pending { command },
                }
            }
            other => other,
        };
        Ingress::Action(action)
    }

    /// Bind `connection` to the session its credential names, first
    /// establishing that session if the cluster does not have it yet.
    ///
    /// Verifying the credential is not the same as the session
    /// existing. A caller's first command executes against a replicated
    /// session row, so the row has to be there -- written by an ordered
    /// command every replica agreed on, not by this node because this
    /// node happened to serve the bind. So:
    ///
    /// * the session exists and describes this credential: bind now;
    /// * the session exists and describes something else, or was
    ///   retired: refuse. A session's principal and ceiling are
    ///   immutable and its retirement permanent, so there is nothing a
    ///   credential could say that would change either;
    /// * no session yet: submit the establishment and hold this stream.
    ///   The acknowledgement is written when the command's outcome
    ///   comes back, so nothing the caller can do with the session
    ///   precedes the session.
    fn bind(
        &mut self,
        clock: &ClockHealth,
        now: MonotonicMillis,
        connection: u64,
        frame: &Frame,
        policy: &dyn PolicySource,
    ) -> Ingress {
        let bind = match decode_bind(frame) {
            Ok(b) => b,
            Err(_) => return Ingress::Rejected(BindError::Malformed),
        };
        let existing = self.bindings.get(&connection);
        let binding = match verify_bind(&self.config, bind.token.as_slice(), clock, existing) {
            Ok(b) => b,
            Err(e) => return Ingress::Rejected(e),
        };
        let ack = bind_ack_frame(&BindAckV1 {
            session: binding.session,
            expires_at: binding.expires_at,
            scope: binding.scope_ceiling,
            rule_generation: binding.rule_generation,
        })
        .expect("bounded");
        self.sessions_checked += 1;
        let barrier = match policy.barrier(self.config.namespace(), &binding.session) {
            Ok(b) => b,
            // Replicated policy cannot be read, so whether the session
            // exists is not known. Nothing is bound on a guess.
            Err(_) => return Ingress::Rejected(BindError::Unavailable),
        };
        match barrier.session_record() {
            Some(record) if binding.agrees_with(record) => {
                // This barrier showed the row, so the disclosure gate
                // must not read its later absence as "not yet": a
                // session retired before the connection's first gated
                // read is refused, not held for ever.
                self.seen.insert(connection);
                self.bindings.insert(connection, binding);
                Ingress::Bound(ack)
            }
            Some(_) => Ingress::Rejected(BindError::SessionDisagrees),
            None => self.establish(clock, now, connection, binding, ack, policy),
        }
    }

    /// Submit the command that creates `binding`'s session and hold the
    /// bind stream until it resolves.
    fn establish(
        &mut self,
        clock: &ClockHealth,
        now: MonotonicMillis,
        connection: u64,
        binding: Binding,
        ack: Vec<u8>,
        policy: &dyn PolicySource,
    ) -> Ingress {
        let retry_key = binding.establishment_key(&self.config);
        // The operation carries nothing. Everything about the session
        // is in the receipt, which travels beside the payload and is
        // what every voter binds into the command it accepts.
        let logical = LogicalRequest::new(
            self.config.namespace(),
            coord_types::logical_v1::CanonicalOperation::ConsumeAdmission,
        );
        // The establishment request acknowledges nothing: it is the
        // session's first invocation, so there is no earlier result of
        // this client instance for it to retire.
        let Ok(request) = RequestV1::new(retry_key, &logical, 0, 0) else {
            return Ingress::Rejected(BindError::Malformed);
        };
        let Ok(frame) = MessageV1::Request(request).encode() else {
            return Ingress::Rejected(BindError::Malformed);
        };
        let admitted = AdmittedRequest {
            receipt: binding.establishment(&self.config, clock.now),
            frame,
        };
        match self
            .dispatcher
            .establish(now, connection, &admitted, retry_key)
        {
            Ok(Action::FanOut(plan)) => {
                self.establishing.insert(
                    retry_key,
                    Establishing {
                        connection,
                        binding,
                        ack,
                    },
                );
                Ingress::Establishing(Box::new(plan))
            }
            // This very establishment already executed and its outcome
            // is kept: it is settled from that outcome, as the stream
            // that submitted it was. A released outcome can precede the
            // snapshot the barrier above reads (a response is released
            // at journal durability, task-d63), so a second binding
            // right after the first finds no row yet and would
            // otherwise be refused a session that exists.
            Ok(Action::Respond(delivery)) => {
                let waiting = Establishing {
                    connection,
                    binding,
                    ack,
                };
                match self.settle(waiting, delivery, policy) {
                    Delivered::Answer(answer) => Ingress::Bound(answer.frame),
                    Delivered::Unbound { reason, .. } => Ingress::Rejected(reason),
                    // An establishment's outcome is not gated, so it is
                    // never held.
                    Delivered::Held => Ingress::Rejected(BindError::Unavailable),
                }
            }
            // Another connection is already establishing this very
            // command. Its outcome is this one's too, but only one
            // stream is attached to an invocation, so this caller is
            // told to present its credential again rather than left
            // waiting for an answer addressed elsewhere.
            Ok(_) | Err(_) => Ingress::Rejected(BindError::Unavailable),
        }
    }

    /// Settle a held bind stream from the establishment's outcome.
    fn settle(
        &mut self,
        waiting: Establishing,
        delivery: Delivery,
        policy: &dyn PolicySource,
    ) -> Delivered {
        let Establishing {
            connection,
            binding,
            ack,
        } = waiting;
        let outcome = match decode_stream(&delivery.frame).as_deref() {
            Ok([MessageV1::Response(r)]) => match &r.outcome {
                OutcomeV1::Ok { result, .. } => postcard::from_bytes::<Response>(result.as_slice())
                    .ok()
                    .map(|r| r.outcome),
                _ => None,
            },
            _ => None,
        };
        let established = match outcome {
            // The command this caller submitted created the session.
            Some(Outcome::SessionCreated { session }) => session == binding.session,
            // Somebody else's command got there first: the receipt or
            // the session identity was already consumed. That is
            // convergence, not failure -- but only if the row that
            // exists is the session this credential describes, which
            // current replicated state answers and this node does not.
            Some(Outcome::ErrReceiptConsumed) => {
                let agrees = self.barrier_agrees(policy, &binding).unwrap_or(false);
                // That barrier showed the row; the outcome above did
                // not, since this node may not have projected the
                // command it answers yet.
                if agrees {
                    self.seen.insert(connection);
                }
                agrees
            }
            _ => false,
        };
        if !established {
            return Delivered::Unbound {
                connection,
                retry_key: delivery.retry_key,
                reason: BindError::Unavailable,
            };
        }
        self.bindings.insert(connection, binding);
        Delivered::Answer(Delivery {
            connection,
            retry_key: delivery.retry_key,
            frame: ack,
        })
    }

    /// Whether current replicated state holds a session this binding may
    /// be admitted under.
    fn barrier_agrees(&mut self, policy: &dyn PolicySource, binding: &Binding) -> Option<bool> {
        self.sessions_checked += 1;
        let barrier = policy
            .barrier(self.config.namespace(), &binding.session)
            .ok()?;
        Some(
            barrier
                .session_record()
                .is_some_and(|r| binding.agrees_with(r)),
        )
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
        deadline: Option<MonotonicMillis>,
    ) {
        if let Some(existing) = self.requests.get_mut(&key) {
            debug_assert_eq!(existing.command, command, "identity decides the metadata");
            // A retry the dispatcher accepted waits under the deadline it
            // presented, and so does its disclosure at the gate. What
            // authorizes the result stays the first request's.
            existing.deadline = deadline;
            for held in self.held.iter_mut() {
                if held.delivery.retry_key == key {
                    held.until = hold_until(held.began, deadline);
                }
            }
            return;
        }
        let info = RequestInfo {
            command,
            namespace: logical.namespace,
            keys: request_keys(logical),
            intervals: request_intervals(logical),
            deadline,
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
    /// result or a resolution) against a fresh barrier, answering now: a
    /// disclosure whose session this node has not projected yet is
    /// answered `Pending`. `deliver_at` holds it instead.
    pub fn deliver(&mut self, delivery: Delivery, policy: &dyn PolicySource) -> Delivered {
        self.deliver_with(None, delivery, policy)
    }

    /// Gate a delivery as `deliver` does, at `now` (the monotonic reading
    /// a request's deadline is measured on), holding a disclosure whose
    /// session this node has not projected yet until it has (task-d93).
    /// A held delivery is `Delivered::Held`, and `release_held` hands it
    /// out.
    pub fn deliver_at(
        &mut self,
        now: MonotonicMillis,
        delivery: Delivery,
        policy: &dyn PolicySource,
    ) -> Delivered {
        self.deliver_with(Some(now), delivery, policy)
    }

    fn deliver_with(
        &mut self,
        now: Option<MonotonicMillis>,
        delivery: Delivery,
        policy: &dyn PolicySource,
    ) -> Delivered {
        // An establishment's outcome is not output to gate: the caller
        // it answers has no session yet, and what it gets back is the
        // acknowledgement of the one this command created.
        if let Some(waiting) = self.establishing.remove(&delivery.retry_key) {
            return self.settle(waiting, delivery, policy);
        }
        let Some((session, scope)) = self
            .bindings
            .get(&delivery.connection)
            .map(|b| (b.session, b.scope_ceiling))
        else {
            return Delivered::Answer(delivery);
        };
        match self.gate(now, delivery, session, scope, policy) {
            Gated::Out(delivery) => Delivered::Answer(delivery),
            Gated::Held(_) => Delivered::Held,
        }
    }

    /// Gate a result the durable record retained for `delivery`'s retry
    /// key, answering a retry of `logical` (whose command is `command`).
    ///
    /// A retained result is gated exactly as a live one is, and the gate
    /// needs the request's namespace and keys to authorize read output
    /// and previous values. A live request's are recorded when the
    /// dispatcher accepts it; a retry answered from the record never
    /// reaches the dispatcher, and after a restart (or once the entry has
    /// been evicted) nothing else recorded them, so every read-bearing
    /// retry would be denied. They are recorded here from the request
    /// being answered, which the caller has already matched to the
    /// record's command. The record decides which command the key is
    /// bound to, so an entry this key holds for another command is
    /// replaced rather than used to authorize output it did not produce.
    ///
    /// A retained result is answered at once: one this node cannot
    /// disclose yet is answered `Pending` rather than held, since its
    /// caller is answered on the stream it retried on, within the call.
    pub fn deliver_retained(
        &mut self,
        delivery: Delivery,
        command: coord_types::CommandId,
        logical: &LogicalRequest,
        policy: &dyn PolicySource,
    ) -> Delivered {
        let key = delivery.retry_key;
        if self
            .requests
            .get(&key)
            .is_some_and(|i| i.command != command)
        {
            self.requests.remove(&key);
            self.order.retain(|k| *k != key);
        }
        self.remember(key, command, logical, None);
        self.deliver(delivery, policy)
    }

    /// The held disclosures that can go out at `now`: each is gated
    /// again, and goes out as that gate decides once this node has
    /// projected its caller's session, or as `Pending` once its hold's
    /// deadline has passed (task-d93). The rest stay held.
    ///
    /// Called whenever this node's projection may have moved, and at
    /// `next_hold_deadline`.
    pub fn release_held(
        &mut self,
        now: MonotonicMillis,
        policy: &dyn PolicySource,
    ) -> Vec<Delivery> {
        let mut out = Vec::new();
        for held in std::mem::take(&mut self.held) {
            match self.decide(held.delivery, held.session, held.scope, policy) {
                Decision::Out(delivery) => out.push(delivery),
                Decision::NotYet(delivery, command) if held.until <= now => {
                    out.push(self.defer(delivery, command));
                }
                Decision::NotYet(delivery, _) => self.held.push(Held { delivery, ..held }),
            }
        }
        out
    }

    /// Whether the gate holds any disclosure now.
    pub fn holds_any(&self) -> bool {
        !self.held.is_empty()
    }

    /// The earliest deadline of a held disclosure: nothing arrives to say
    /// it has passed.
    pub fn next_hold_deadline(&self) -> Option<MonotonicMillis> {
        self.held.iter().map(|h| h.until).min()
    }

    /// Gate a delivery, holding it if this node has not projected the
    /// caller's session yet and `now` says when the hold began
    /// (task-d93). Without `now` such a delivery is answered `Pending`.
    fn gate(
        &mut self,
        now: Option<MonotonicMillis>,
        delivery: Delivery,
        session: coord_types::ids::SessionId,
        scope: u32,
        policy: &dyn PolicySource,
    ) -> Gated {
        match self.decide(delivery, session, scope, policy) {
            Decision::Out(delivery) => Gated::Out(delivery),
            Decision::NotYet(delivery, command) => {
                let Some(now) = now else {
                    return Gated::Out(self.defer(delivery, command));
                };
                let deadline = self
                    .requests
                    .get(&delivery.retry_key)
                    .and_then(|i| i.deadline);
                let until = hold_until(now, deadline);
                // A deadline already passed, or no room: answered as
                // before the hold, `Pending`. The hold is bounded as the
                // request metadata it rests on is.
                if until <= now || self.held.len() >= self.max_requests {
                    return Gated::Out(self.defer(delivery, command));
                }
                self.holds += 1;
                self.held.push(Held {
                    delivery,
                    session,
                    scope,
                    until,
                    began: now,
                });
                Gated::Held(command)
            }
        }
    }

    /// The gate's decision about one delivery against a fresh barrier.
    fn decide(
        &mut self,
        delivery: Delivery,
        session: coord_types::ids::SessionId,
        scope: u32,
        policy: &dyn PolicySource,
    ) -> Decision {
        let response = match decode_stream(&delivery.frame).as_deref() {
            Ok([MessageV1::Response(r)]) => r.clone(),
            _ => return Decision::Out(delivery),
        };
        let OutcomeV1::Ok { result, .. } = &response.outcome else {
            return Decision::Out(delivery);
        };
        let Ok(decoded) = postcard::from_bytes::<Response>(result.as_slice()) else {
            return Decision::Out(self.deny(delivery, response.command_id));
        };
        let mut keys = protected_keys(&decoded);
        let info = self.requests.get(&delivery.retry_key);
        if carries_previous(&decoded) {
            match info {
                Some(i) => keys.extend(i.keys.iter().cloned()),
                // The request is unknown here: its keys cannot be named,
                // so the previous values are not disclosed.
                None => return Decision::Out(self.deny(delivery, response.command_id)),
            }
        }
        // An empty key list is not evidence that nothing is disclosed: a
        // count-only range names no key and still reports how many there
        // were, and an empty range discloses absence. Read output is
        // reauthorized whether or not it carries a key; only a pure
        // mutation acknowledgement passes without a barrier.
        let reads = is_read_output(&decoded);
        if keys.is_empty() && !reads {
            return Decision::Out(delivery);
        }
        let Some(info) = info else {
            return Decision::Out(self.deny(delivery, response.command_id));
        };
        let namespace = info.namespace;
        let intervals = info.intervals.clone();
        // Read output also needs the bound token's own permission. The
        // replicated session's ceiling can be wider than the scope in the
        // token this connection presented, and a watch open bypasses
        // unary admission entirely, so without this a token without the
        // read bit would still receive read output.
        if reads && scope & PolicyAction::Read.bit() == 0 {
            return Decision::Out(self.deny(delivery, response.command_id));
        }
        self.barriers_read += 1;
        let Ok(barrier) = policy.barrier(namespace, &session) else {
            return Decision::Out(self.deny(delivery, response.command_id));
        };
        // "Not yet" is not "no". This node's binding was established
        // from a committed session-creation command, but the barrier
        // reads this node's materialized projection, and a replica that
        // has fallen behind has not projected that command yet. A
        // refusal there is a false one, and an expensive one: the
        // client library treats it as a credential that is no longer
        // good, throws the credential away and binds again -- which
        // creates a newer session that this node has projected even
        // less of. Under load that is a caller whose reads never
        // succeed again, on a domain that is working.
        //
        // So a session this node has never seen means wait: the gate
        // holds the disclosure until this node's projection shows the
        // session (task-d93), and past the hold's deadline the caller
        // resolves as it does for any pending outcome. Once a barrier
        // has shown the row, its absence is a real answer: policy
        // moved, and the projection only moves forward.
        if barrier.session_record().is_none() && !self.seen.contains(&delivery.connection) {
            return Decision::NotYet(delivery, response.command_id);
        }
        self.seen.insert(delivery.connection);
        if !barrier.permits_keys(keys.iter().map(Vec::as_slice)) {
            return Decision::Out(self.deny(delivery, response.command_id));
        }
        // What the request asked to read, not only what came back: the
        // count, the truncation flag and the keys that are absent all
        // describe the interval that was asked for.
        if !intervals.iter().all(|i| barrier.permits_interval(i)) {
            return Decision::Out(self.deny(delivery, response.command_id));
        }
        Decision::Out(delivery)
    }

    /// Answer a disclosure this node cannot yet decide: nothing is shown,
    /// and the caller is told the outcome is still pending rather than
    /// that its credential was refused.
    fn defer(&mut self, delivery: Delivery, command: coord_types::CommandId) -> Delivery {
        self.deferred += 1;
        Delivery {
            connection: delivery.connection,
            retry_key: delivery.retry_key,
            frame: MessageV1::Response(codes::pending_response(command))
                .encode()
                .expect("bounded"),
        }
    }

    /// Withhold an executed command's result: `OUTPUT_WITHHELD`, never
    /// `NOT_ADMITTED` (task-d23). The command was admitted and executed;
    /// telling the caller it was not would have it take a write that
    /// happened for one that did not.
    fn deny(&mut self, delivery: Delivery, command: coord_types::CommandId) -> Delivery {
        self.denied += 1;
        Delivery {
            connection: delivery.connection,
            retry_key: delivery.retry_key,
            frame: MessageV1::Response(codes::error_response(
                command,
                codes::OUTPUT_WITHHELD,
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

    /// End a watch whose output stream is gone: the hub and the
    /// dispatcher both forget it. Answers whether there was one.
    pub fn cancel_watch(&mut self, connection: u64, watch_id: u64, hub: &WatchHub) -> bool {
        self.watches.remove(&(connection, watch_id));
        self.dispatcher.cancel_watch(connection, watch_id, hub)
    }

    /// Time passed: connections whose binding ended at `clock` (to
    /// close) and the requests whose deadline passed at `now` (the
    /// dispatcher's deliveries). A binding ends on the wall clock, a
    /// deadline on the monotonic one; a step in the first moves nothing
    /// in the second.
    pub fn tick(&mut self, clock: &ClockHealth, now: MonotonicMillis) -> (Vec<u64>, Vec<Delivery>) {
        let expired: Vec<u64> = self
            .bindings
            .iter()
            .filter(|(_, b)| !b.active(clock))
            .map(|(c, _)| *c)
            .collect();
        let deliveries = self.dispatcher.expire(now);
        (expired, deliveries)
    }

    /// A connection closed: its binding, watches and attached requests go.
    pub fn on_connection_closed(&mut self, connection: u64, hub: &WatchHub) {
        self.bindings.remove(&connection);
        self.seen.remove(&connection);
        self.held.retain(|h| h.delivery.connection != connection);
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
