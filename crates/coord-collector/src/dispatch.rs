//! Native dispatch at the frontend (design Sections 4.3, 6.4): unary
//! requests and resolution through admission and the collector, watches
//! through the storage watch hub, which publishes applied revisions only.
//!
//! The runtime hands every frame of an API-class connection to
//! [`Dispatcher::on_frame`] with the connection's verified caller and
//! takes back an [`Action`]: a fan-out to perform, a response frame for
//! that connection, a watch to replay, or a protocol violation to close
//! on. Evidence and releases from voters come through
//! [`Dispatcher::on_evidence`] and [`Dispatcher::on_release`] and yield
//! the delivery for the attached caller, if any. A closed connection
//! cancels its requests (identity and outcome resolution stay) and its
//! watches.

use std::collections::{BTreeMap, BTreeSet};

use coord_consensus::ProtocolMessage;
use coord_core::capability::ReleasedResult;
use coord_core::event::PeerProvenance;
use coord_state::plan::{KvEvent, KvEventKind};
use coord_storage::watch::{Registration, chunk};
use coord_storage::{CloseReason, WatchBatch, WatchHub, WatchId, WatchItem, WatchSpec};
use coord_types::ids::KvRevision;
use coord_types::wire_v1::{
    BoundedBytes, BoundedVec, EventKindV1, EventV1, Frame, MAX_EVENTS_PER_BATCH, MessageV1,
    WatchCloseReasonV1, WatchCloseV1, WatchEventsV1, WatchOpenV1, WatchProgressV1, decode,
};
use coord_types::{CommandId, RetryKey};

use crate::admission::{Admission, AdmissionRefusal, Caller};
use crate::codes;
use crate::collector::{
    Collector, EvidenceError, Progress, Release, Resolution, SubmitRefusal, Submitted,
};

/// A response for a connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delivery {
    /// Connection the caller is attached on.
    pub connection: u64,
    /// Retry key.
    pub retry_key: RetryKey,
    /// Complete response frame.
    pub frame: Vec<u8>,
}

/// What the runtime does next.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// Fan the submission out to every target in parallel.
    FanOut(crate::collector::FanOut),
    /// Write this frame back on the request's stream.
    Respond(Delivery),
    /// The request is pending; the stream stays open until a delivery.
    Pending {
        /// Command.
        command: CommandId,
    },
    /// A watch was registered: replay `registration.replay` from a
    /// durable view (the hub gates live delivery until
    /// `replay_complete`), then pump it.
    WatchOpened {
        /// Connection.
        connection: u64,
        /// Client watch identifier.
        watch_id: u64,
        /// Hub registration.
        registration: Registration,
    },
    /// A frame the connection may not send: close with a protocol code.
    Violation {
        /// Connection.
        connection: u64,
        /// Kind seen.
        kind: u16,
    },
}

/// The frontend dispatcher of one domain.
#[derive(Debug)]
pub struct Dispatcher {
    admission: Admission,
    collector: Collector,
    owner: BTreeMap<RetryKey, u64>,
    by_connection: BTreeMap<u64, BTreeSet<RetryKey>>,
    watches: BTreeMap<(u64, u64), WatchId>,
    watch_queue: usize,
}

impl Dispatcher {
    /// A dispatcher over an admission gate and a collector.
    pub const fn new(admission: Admission, collector: Collector, watch_queue: usize) -> Self {
        Dispatcher {
            admission,
            collector,
            owner: BTreeMap::new(),
            by_connection: BTreeMap::new(),
            watches: BTreeMap::new(),
            watch_queue,
        }
    }

    /// The collector.
    pub const fn collector(&self) -> &Collector {
        &self.collector
    }

    /// The collector, mutably (reconfiguration, traces).
    pub const fn collector_mut(&mut self) -> &mut Collector {
        &mut self.collector
    }

    /// The admission gate.
    pub const fn admission(&self) -> &Admission {
        &self.admission
    }

    /// Dispatch one frame of `connection` from `caller`.
    pub fn on_frame(
        &mut self,
        now_ticks: u64,
        connection: u64,
        caller: &Caller,
        frame: &Frame,
        hub: &WatchHub,
    ) -> Action {
        let message = match decode(frame) {
            Ok(m) => m,
            Err(_) => {
                return Action::Violation {
                    connection,
                    kind: frame.kind,
                };
            }
        };
        match message {
            MessageV1::Request(request) => {
                let key = request.retry_key;
                let admitted = match self.admission.admit(now_ticks, caller, &request) {
                    Ok(a) => a,
                    Err(refusal) => {
                        let (code, detail) = match refusal {
                            AdmissionRefusal::Malformed => (codes::MALFORMED_REQUEST, "malformed"),
                            AdmissionRefusal::SessionBusy { .. } => {
                                (codes::BACKPRESSURE, "session busy")
                            }
                            AdmissionRefusal::RoleNotAdmitted(_)
                            | AdmissionRefusal::WrongCluster
                            | AdmissionRefusal::WrongDomain
                            | AdmissionRefusal::SessionMismatch => {
                                (codes::NOT_ADMITTED, "not admitted")
                            }
                        };
                        return self.respond(connection, key, command_of(&key), code, detail);
                    }
                };
                match self.collector.submit(now_ticks, &admitted) {
                    Ok(Submitted::FanOut(fan_out)) => {
                        self.attach(connection, key);
                        Action::FanOut(fan_out)
                    }
                    Ok(Submitted::Attached { command }) => {
                        self.attach(connection, key);
                        Action::Pending { command }
                    }
                    Ok(Submitted::Resolved(response)) => {
                        self.admission.settled(&caller.session);
                        Action::Respond(Delivery {
                            connection,
                            retry_key: key,
                            frame: MessageV1::Response(response).encode().expect("bounded"),
                        })
                    }
                    Err(refusal) => {
                        self.admission.settled(&caller.session);
                        let (code, detail) = match refusal {
                            SubmitRefusal::Malformed => (codes::MALFORMED_REQUEST, "malformed"),
                            SubmitRefusal::RequestIdentityConflict { .. } => (
                                codes::REQUEST_IDENTITY_CONFLICT,
                                "retry key bound to another payload",
                            ),
                            SubmitRefusal::Backpressure { .. } => {
                                (codes::BACKPRESSURE, "domain at its collection bound")
                            }
                        };
                        self.respond(connection, key, command_of(&key), code, detail)
                    }
                }
            }
            MessageV1::ResolveRequest(resolve) => {
                if caller.role != coord_types::wire_v1::PeerRole::Client
                    || resolve.retry_key.session_id != caller.session
                {
                    return self.respond(
                        connection,
                        resolve.retry_key,
                        resolve.command_id,
                        codes::NOT_ADMITTED,
                        "not admitted",
                    );
                }
                let response = match self.collector.resolve(&resolve) {
                    Resolution::Outcome(r) => r,
                    Resolution::Pending => codes::pending_response(resolve.command_id),
                    Resolution::Conflict { .. } => codes::error_response(
                        resolve.command_id,
                        codes::REQUEST_IDENTITY_CONFLICT,
                        "retry key bound to another payload",
                    ),
                    Resolution::Unknown => codes::unknown_response(resolve.command_id),
                };
                Action::Respond(Delivery {
                    connection,
                    retry_key: resolve.retry_key,
                    frame: MessageV1::Response(response).encode().expect("bounded"),
                })
            }
            MessageV1::WatchOpen(open) => self.open_watch(connection, open, hub),
            MessageV1::WatchClose(close) => {
                if let Some(id) = self.watches.remove(&(connection, close.watch_id)) {
                    drop_watch(hub, id);
                }
                let frame = close_frame(close.watch_id, WatchCloseReasonV1::Cancelled, None);
                Action::Respond(Delivery {
                    connection,
                    retry_key: null_key(),
                    frame,
                })
            }
            MessageV1::Hello(_)
            | MessageV1::HelloAck(_)
            | MessageV1::Close(_)
            | MessageV1::Response(_)
            | MessageV1::WatchEvents(_)
            | MessageV1::WatchProgress(_) => Action::Violation {
                connection,
                kind: frame.kind,
            },
        }
    }

    fn open_watch(&mut self, connection: u64, open: WatchOpenV1, hub: &WatchHub) -> Action {
        if self.watches.contains_key(&(connection, open.watch_id)) {
            return Action::Violation {
                connection,
                kind: coord_types::wire_v1::MessageKind::WatchOpen.as_u16(),
            };
        }
        let spec = WatchSpec {
            namespace: open.namespace,
            key: open.key.as_slice().to_vec(),
            range_end: open.range_end.as_ref().map(|e| e.as_slice().to_vec()),
            start_revision: open.start_revision,
            prev_kv: open.prev_kv,
            progress_notify: open.progress_notify,
            queue_capacity: self.watch_queue,
        };
        match hub.register(spec) {
            Ok(registration) => {
                self.watches
                    .insert((connection, open.watch_id), registration.id);
                Action::WatchOpened {
                    connection,
                    watch_id: open.watch_id,
                    registration,
                }
            }
            Err(reason) => Action::Respond(Delivery {
                connection,
                retry_key: null_key(),
                frame: close_frame(open.watch_id, close_reason(reason), None),
            }),
        }
    }

    /// Take up to `max_items` items the hub has for a watch into frames:
    /// complete-revision batches chunked with `complete` on the last
    /// chunk, progress, and the close. `authorize` is the output
    /// authorization of each selected batch (task-37 binds it to one
    /// fresh policy barrier per bounded pump).
    pub fn pump_watch(
        &mut self,
        hub: &WatchHub,
        connection: u64,
        watch_id: u64,
        max_items: usize,
        mut authorize: impl FnMut(&WatchBatch) -> bool,
    ) -> Vec<Vec<u8>> {
        let Some(id) = self.watches.get(&(connection, watch_id)).copied() else {
            return Vec::new();
        };
        let mut out = Vec::new();
        let mut taken = 0;
        while taken < max_items
            && let Some(item) = hub.next(id, &mut authorize)
        {
            taken += 1;
            match item {
                WatchItem::Batch(batch) => {
                    for fragment in chunk(&batch, MAX_EVENTS_PER_BATCH) {
                        let events: Vec<EventV1> = fragment
                            .events
                            .iter()
                            .map(|e| event_v1(e, fragment.revision))
                            .collect();
                        out.push(
                            MessageV1::WatchEvents(WatchEventsV1 {
                                watch_id,
                                revision: fragment.revision,
                                events: BoundedVec::new(events).expect("chunked to the bound"),
                                complete: fragment.complete,
                            })
                            .encode()
                            .expect("bounded"),
                        );
                    }
                }
                WatchItem::Progress(revision) => out.push(
                    MessageV1::WatchProgress(WatchProgressV1 { watch_id, revision })
                        .encode()
                        .expect("bounded"),
                ),
                WatchItem::Closed {
                    reason,
                    last_complete_revision,
                } => {
                    self.watches.remove(&(connection, watch_id));
                    out.push(close_frame(
                        watch_id,
                        close_reason(reason),
                        (last_complete_revision.get() > 0).then_some(last_complete_revision),
                    ));
                    break;
                }
            }
        }
        out
    }

    /// Evidence from a voter identity.
    pub fn on_evidence(
        &mut self,
        from: PeerProvenance,
        message: ProtocolMessage,
    ) -> Result<Option<Delivery>, EvidenceError> {
        let progress = self.collector.on_evidence(from, message)?;
        Ok(self.deliver(progress))
    }

    /// A release from the leader.
    pub fn on_release(
        &mut self,
        from: PeerProvenance,
        released: ReleasedResult,
    ) -> Result<Option<Delivery>, EvidenceError> {
        let progress = self.collector.on_release(from, released)?;
        Ok(self.deliver(progress))
    }

    fn deliver(&mut self, progress: Progress) -> Option<Delivery> {
        let Progress::Released(release) = progress else {
            return None;
        };
        self.settle_release(&release)
    }

    /// Account a release the collector produced (when the runtime drove
    /// the collector directly): frees the session slot, detaches the
    /// owner and yields the delivery for the attached caller, if any.
    pub fn settle_release(&mut self, release: &Release) -> Option<Delivery> {
        self.admission.settled(&release.session);
        let connection = self.owner.remove(&release.retry_key);
        if let Some(c) = connection
            && let Some(set) = self.by_connection.get_mut(&c)
        {
            set.remove(&release.retry_key);
        }
        let connection = connection.filter(|_| release.attached)?;
        Some(Delivery {
            connection,
            retry_key: release.retry_key,
            frame: MessageV1::Response(release.response.clone())
                .encode()
                .expect("bounded"),
        })
    }

    /// Client deadlines that passed: the attached callers hear that the
    /// outcome is pending (never failed) and resolvable by identity; the
    /// commands keep collecting.
    pub fn expire(&mut self, now_ticks: u64) -> Vec<Delivery> {
        let mut out = Vec::new();
        for expired in self.collector.expire(now_ticks) {
            if let Some(connection) = self.owner.remove(&expired.retry_key) {
                if let Some(set) = self.by_connection.get_mut(&connection) {
                    set.remove(&expired.retry_key);
                }
                self.collector.cancel(&expired.retry_key);
                self.admission.settled(&expired.session);
                out.push(Delivery {
                    connection,
                    retry_key: expired.retry_key,
                    frame: MessageV1::Response(codes::pending_response(expired.command))
                        .encode()
                        .expect("bounded"),
                });
            }
        }
        out
    }

    /// A connection closed: its requests are cancelled (they keep
    /// collecting and stay resolvable), its watches are cancelled.
    pub fn on_connection_closed(&mut self, connection: u64, hub: &WatchHub) -> Vec<CommandId> {
        let mut cancelled = Vec::new();
        for key in self.by_connection.remove(&connection).unwrap_or_default() {
            self.owner.remove(&key);
            if let Some(command) = self.collector.cancel(&key) {
                cancelled.push(command);
            }
        }
        let watches: Vec<(u64, u64)> = self
            .watches
            .keys()
            .filter(|(c, _)| *c == connection)
            .copied()
            .collect();
        for k in watches {
            if let Some(id) = self.watches.remove(&k) {
                drop_watch(hub, id);
            }
        }
        cancelled
    }

    fn attach(&mut self, connection: u64, key: RetryKey) {
        self.owner.insert(key, connection);
        self.by_connection
            .entry(connection)
            .or_default()
            .insert(key);
    }

    fn respond(
        &mut self,
        connection: u64,
        key: RetryKey,
        command: CommandId,
        code: u16,
        detail: &str,
    ) -> Action {
        Action::Respond(Delivery {
            connection,
            retry_key: key,
            frame: MessageV1::Response(codes::error_response(command, code, detail))
                .encode()
                .expect("bounded"),
        })
    }
}

/// The identity a refusal names when no canonical command exists: the
/// retry key's own canonical digest under the command domain, so a
/// client can correlate the refusal with its invocation.
fn command_of(key: &RetryKey) -> CommandId {
    CommandId(coord_types::identity::HashDomain::CommandId.digest(&[&key.canonical_bytes()]))
}

/// Cancel a watch and drain it so the hub forgets it: queued batches are
/// discarded (the client cancelled) and the close is consumed here.
fn drop_watch(hub: &WatchHub, id: WatchId) {
    hub.cancel(id);
    while let Some(item) = hub.next(id, |_| true) {
        if matches!(item, WatchItem::Closed { .. }) {
            break;
        }
    }
}

fn null_key() -> RetryKey {
    RetryKey {
        cluster_id: coord_types::ids::ClusterId([0; 16]),
        domain_id: coord_types::ids::DomainId([0; 16]),
        session_id: coord_types::ids::SessionId([0; 16]),
        client_instance_id: coord_types::ids::ClientInstanceId([0; 16]),
        request_sequence: coord_types::ids::RequestSequence::new(1).expect("non-zero"),
    }
}

fn close_frame(watch_id: u64, reason: WatchCloseReasonV1, last: Option<KvRevision>) -> Vec<u8> {
    MessageV1::WatchClose(WatchCloseV1 {
        watch_id,
        reason,
        last_complete_revision: last,
    })
    .encode()
    .expect("bounded")
}

const fn close_reason(reason: CloseReason) -> WatchCloseReasonV1 {
    match reason {
        CloseReason::Cancelled => WatchCloseReasonV1::Cancelled,
        CloseReason::Compacted => WatchCloseReasonV1::Compacted,
        CloseReason::SlowConsumer => WatchCloseReasonV1::SlowConsumer,
        CloseReason::Unauthorized => WatchCloseReasonV1::Unauthorized,
        CloseReason::HubClosed => WatchCloseReasonV1::SourceLost,
    }
}

fn event_v1(event: &KvEvent, revision: KvRevision) -> EventV1 {
    let entry = event.entry.as_ref();
    EventV1 {
        kind: match event.kind {
            KvEventKind::Put => EventKindV1::Put,
            KvEventKind::Delete => EventKindV1::Delete,
        },
        key: BoundedBytes::new(event.key.clone()).expect("stored keys are bounded"),
        value: BoundedBytes::new(entry.map(|e| e.value.clone()).unwrap_or_default())
            .expect("stored values are bounded"),
        create_revision: entry.map(|e| e.create_revision).unwrap_or(KvRevision::ZERO),
        mod_revision: entry.map(|e| e.mod_revision).unwrap_or(revision),
        version: entry.map(|e| e.version).unwrap_or(0),
        prev_value: event
            .prev
            .as_ref()
            .map(|p| BoundedBytes::new(p.value.clone()).expect("stored values are bounded")),
    }
}
