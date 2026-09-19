//! The serving loop: transport in, frontend decision, storage out
//! (design Sections 3.2, 4.3, 22.1).
//!
//! Every piece this joins was built to hold none of the others.
//! `coord-transport` negotiates and frames and does not know what a
//! session is. `coord-session` decides what a frame of a bound
//! connection means and holds no sockets. `coord-collector` plans a
//! submission and does not send it. `coord-daemon` says what each
//! decision does to the stream it arrived on, and drives one voter's
//! effects, and owns neither the endpoint nor the store.
//!
//! This is where they meet, and it is deliberately the only place they
//! do: a join that happened in several places would be several
//! opportunities for one of them to answer the wrong caller.

use coord_authn::ClockHealth;
use coord_collector::{Admission, AdmissionLimits, Collector, CollectorConfig, Dispatcher};
use coord_daemon::pending::Pending;
use coord_daemon::serve::{Step, step};
use coord_daemon::{Config, fanout};
use coord_membership::membership::Membership;
use coord_session::{BindingConfig, BoundFrontend, StorePolicySource};
use coord_storage::views::ViewBudget;
use coord_storage::{Applier, Persistence};
use coord_transport::{Responder, Transport, TransportEvent};
use coord_types::RetryKey;
use coord_types::wire_v1::{MessageV1, decode};

/// Why the loop could not be built.
#[derive(Debug)]
pub enum ServeError {
    /// The configuration does not name the token service this process
    /// needs to verify its callers.
    NoTokenService,
    /// The issuer's published keys could not be read.
    Jwks {
        /// Where this node looked.
        path: String,
        /// Why.
        reason: String,
    },
    /// The committed configuration does not describe a quorum.
    Quorum(String),
}

impl core::fmt::Display for ServeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ServeError::NoTokenService => f.write_str(
                "this process serves clients but names no token service to verify them against",
            ),
            ServeError::Jwks { path, reason } => {
                write!(f, "cannot read the issuer's keys at {path}: {reason}")
            }
            ServeError::Quorum(why) => {
                write!(f, "the committed configuration is not a quorum: {why}")
            }
        }
    }
}

impl core::error::Error for ServeError {}

/// The frontend of one domain, ready to be driven by a transport.
pub struct Frontend<P: Persistence> {
    frontend: BoundFrontend,
    applier: Applier<P>,
    membership: Membership,
    pending: Pending<Responder>,
    /// What the loop has done, for the readiness report. Counts, not
    /// contents: a diagnostic that carried a caller's data would be a
    /// disclosure by another name.
    counts: Counts,
}

/// What a serving loop has seen.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    /// Voters a submission reached.
    pub reached: u64,
    /// Voters a submission could not reach. Not a failure of the
    /// submission: only the quorum rule decides that.
    pub unreachable: u64,
    /// Watches opened.
    pub watches: u64,
    /// Frames this build has no loop for yet, counted rather than
    /// discarded so a frame that arrives is visible.
    pub unserved: u64,
}

impl<P: Persistence> Frontend<P> {
    /// Build the frontend this configuration describes.
    pub fn new(
        config: &Config,
        membership: Membership,
        applier: Applier<P>,
    ) -> Result<Self, ServeError> {
        let sts = config.sts.as_ref().ok_or(ServeError::NoTokenService)?;
        let jwks = std::fs::read(&sts.jwks).map_err(|e| ServeError::Jwks {
            path: sts.jwks.clone(),
            reason: e.to_string(),
        })?;
        let jwks: serde_json::Value =
            serde_json::from_slice(&jwks).map_err(|e| ServeError::Jwks {
                path: sts.jwks.clone(),
                reason: e.to_string(),
            })?;

        // The quorum is the committed configuration's, never a setting:
        // a frontend that could be told its own quorum could be told a
        // smaller one, and would then release results on less evidence
        // than the cluster agreed on.
        let voters = membership.voters().map(|v| v.node).collect();
        let ballot = coord_types::ids::Ballot {
            epoch: membership.epoch(),
            number: 0,
            leader: membership
                .voters()
                .map(|v| v.node)
                .next()
                .ok_or_else(|| ServeError::Quorum("no voters".into()))?,
        };
        let quorum =
            coord_consensus::BallotConfiguration::c2_default(membership.epoch(), ballot, voters)
                .map_err(|e| ServeError::Quorum(format!("{e:?}")))?;

        let collector = Collector::new(CollectorConfig {
            quorum,
            max_pending: config.limits.max_outstanding_per_session,
            max_resolved: config.limits.max_outstanding_per_session,
        });
        let admission = Admission::new(
            membership.cluster(),
            membership.domain(),
            AdmissionLimits {
                max_pending_per_session: config.limits.max_outstanding_per_session,
            },
        );
        let dispatcher =
            Dispatcher::new(admission, collector, config.limits.max_live_subscriptions);
        let frontend = BoundFrontend::new(
            dispatcher,
            BindingConfig {
                issuer: sts.issuer.clone(),
                resource: sts.resource.clone(),
                jwks,
            },
            PUMP_BOUND,
            config.limits.max_outstanding_per_session,
        );
        Ok(Frontend {
            frontend,
            applier,
            membership,
            pending: Pending::new(),
            counts: Counts::default(),
        })
    }

    /// What this loop has seen.
    pub const fn counts(&self) -> Counts {
        self.counts
    }

    /// Streams held open for results that do not exist yet.
    pub fn waiting(&self) -> usize {
        self.pending.len()
    }

    /// Serve `transport` until it ends.
    ///
    /// One task, one domain: the frontend, the store and the held
    /// streams are all the same `&mut`, so there is no lock between a
    /// decision and the state it was made against, and no window in
    /// which a second task could answer the same caller.
    pub async fn run(&mut self, transport: &mut Transport, clock: impl Fn() -> u64) {
        while let Some(event) = transport.next_event().await {
            match event {
                TransportEvent::ApiRequest {
                    connection,
                    frame,
                    responder,
                    ..
                } => {
                    let health = ClockHealth::healthy(clock(), CLOCK_UNCERTAINTY_SECONDS);
                    let retry_key = invocation_of(&frame);
                    let policy = StorePolicySource {
                        store: self.applier.store(),
                        budget: ViewBudget::default(),
                    };
                    let ingress = self.frontend.on_frame(
                        &health,
                        connection.0,
                        &frame,
                        self.applier.hub(),
                        &policy,
                    );
                    let decided = step(ingress, retry_key);
                    self.carry_out(transport, connection, decided, responder)
                        .await;
                }
                TransportEvent::Closed { connection, .. } => {
                    // Both sides of the same closing: the frontend
                    // forgets the binding and the watches, and every
                    // stream this connection held is released rather
                    // than dropped, so nothing waits out a deadline for
                    // an answer that is no longer coming.
                    self.frontend
                        .on_connection_closed(connection.0, self.applier.hub());
                    for responder in self.pending.close(connection.0) {
                        drop(responder);
                    }
                }
                // A peer frame is a voter's business, and this build's
                // voter loop is not wired yet; it is counted rather than
                // acted on, so a frame that arrives is visible instead of
                // silently discarded.
                TransportEvent::PeerFrame { .. } | TransportEvent::ApiDelivery { .. } => {
                    self.counts.unserved += 1;
                }
                TransportEvent::Connected { .. } => {}
            }
        }
    }

    async fn carry_out(
        &mut self,
        transport: &Transport,
        connection: coord_transport::ConnectionId,
        decided: Step,
        responder: Responder,
    ) {
        match decided {
            Step::Answer(frame) => {
                let _ = responder.respond(frame).await;
            }
            Step::Hold(key) => {
                if let Some(displaced) = self.pending.hold(connection.0, key, responder) {
                    drop(displaced);
                }
            }
            Step::Submit(plan) => {
                // The stream is held before the frame goes out, not
                // after: evidence can come back faster than this task
                // returns, and a delivery that arrived first would find
                // nothing waiting and be dropped.
                if let Some(displaced) = self.pending.hold(connection.0, plan.retry_key, responder)
                {
                    drop(displaced);
                }
                let sent = fanout::dispatch(&self.membership, transport, &plan);
                self.counts.reached += sent.reached() as u64;
                self.counts.unreachable += sent.unreachable.len() as u64;
            }
            Step::Watch { .. } => {
                // The watch's own stream, held for its lifetime. Pumping
                // it is the next piece of this loop.
                self.counts.watches += 1;
                drop(responder);
            }
            Step::Close { code, reason } => {
                drop(responder);
                transport.disconnect(connection, code, reason);
            }
        }
    }
}

/// The invocation a frame names, where it names one.
fn invocation_of(frame: &coord_types::wire_v1::Frame) -> Option<RetryKey> {
    match decode(frame) {
        Ok(MessageV1::Request(request)) => Some(request.retry_key),
        Ok(MessageV1::ResolveRequest(resolve)) => Some(resolve.retry_key),
        _ => None,
    }
}

/// Items one watch pump moves under a single barrier read.
const PUMP_BOUND: usize = 64;

/// How far the local clock may be out before a binding is refused.
const CLOCK_UNCERTAINTY_SECONDS: u64 = 5;

/// Build the API-plane endpoint on the socket already bound.
///
/// The socket is taken rather than re-bound: binding again from the
/// address that was reported would either collide with the socket this
/// process is still holding, or leave a window in which the port was
/// free for another process to take.
///
/// The binder is the committed membership's, so what a peer may act as
/// is decided by the configuration the cluster agreed on and not by what
/// the peer says about itself.
pub fn api_endpoint(
    config: &Config,
    membership: &Membership,
    socket: std::net::UdpSocket,
) -> Result<Transport, TransportError> {
    let identity = coord_daemon::load_identity(
        &config.identity,
        membership.cluster(),
        membership.domain(),
        coord_transport::role_lanes(coord_types::wire_v1::PeerRole::Frontend)
            .iter()
            .map(|lane| lane.capability())
            .collect(),
    )
    .map_err(|e| TransportError::Identity(e.to_string()))?;
    let binder = coord_membership::binder::PeerBinder::new(membership.clone());
    // The transport's limits are its own (streams, windows, deadlines);
    // the semantic limits the configuration states are the frontend's,
    // and are enforced where the meaning is, not at the framing.
    let limits = coord_transport::Limits::default();
    Transport::with_socket(socket, identity, std::sync::Arc::new(binder), limits)
        .map_err(|e| TransportError::Endpoint(format!("{e:?}")))
}

/// Why the endpoint could not be built.
#[derive(Debug)]
pub enum TransportError {
    /// This node's credentials could not be loaded.
    Identity(String),
    /// The endpoint refused the socket or the configuration.
    Endpoint(String),
}

impl core::fmt::Display for TransportError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            TransportError::Identity(e) => write!(f, "cannot present an identity: {e}"),
            TransportError::Endpoint(e) => write!(f, "cannot serve on the bound socket: {e}"),
        }
    }
}

impl core::error::Error for TransportError {}
