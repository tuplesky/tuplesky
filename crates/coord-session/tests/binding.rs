//! task-37 acceptance: an expired warm connection cannot admit and a
//! rebind keeps the identity; post-revocation protected data is denied
//! even from cached and historical results; watch output is authorized
//! per selected batch against one fresh barrier per bounded pump and
//! progress cannot bypass it; previously authorized in-flight work
//! completes under the documented semantics.

use std::collections::BTreeSet;

use coord_authn::ClockHealth;
use coord_collector::{
    Action, Admission, AdmissionLimits, Collector, CollectorConfig, Delivery, Dispatcher, codes,
};
use coord_consensus::{BallotConfiguration, FastAck, ProtocolMessage};
use coord_core::capability::{EstablishedResult, EstablishmentEvidence, ReleasedResult};
use coord_core::effect::{BootId, PersistBatch};
use coord_core::event::PeerProvenance;
use coord_core::outbox::BarrierAllocator;
use coord_session::{
    BindError, BindingConfig, BoundFrontend, Ingress, StorePolicySource, bind_frame,
};
use coord_state::plan::{KvEvent, KvEventKind, Outcome, RangeItem};
use coord_state::policy::{Action as PolicyAction, KeyInterval, PolicyRule};
use coord_state::view::KvEntry;
use coord_state::{InternalCommand, PlanLimits, Response, plan_internal};
use coord_storage::materialize::{ApplyOutcome, apply_plan};
use coord_storage::policy::{bootstrap_session, rule_update};
use coord_storage::views::{ViewBudget, build_internal_view};
use coord_storage::{GroupLimits, StoreWorker, WatchHub};
use coord_store_testkit::model::ModelEngine;
use coord_sts::{KeyRing, ServiceClaims, SigningKey, TokenError};
use coord_types::identity::Digest32;
use coord_types::ids::*;
use coord_types::logical_v1::*;
use coord_types::wire_v1::{
    BoundedBytes, Frame, FrameReader, MessageV1, OutcomeV1, RequestV1, ResolveRequestV1,
    ResponseV1, WatchOpenV1, decode_stream,
};
use coord_types::{CommandId, RetryKey};

const NS: NamespaceId = NamespaceId([5; 16]);
const SESSION: SessionId = SessionId([3; 16]);
const CLIENT: ClientInstanceId = ClientInstanceId([4; 16]);
const ALICE: PrincipalId = PrincipalId([0xa; 16]);
const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);
const RESOURCE: &str = "tuplesky://cluster-1";
const ISSUER: &str = "https://sts.cluster-1";
const NOW: u64 = 1_700_000_000;

fn r(i: u8) -> ReplicaId {
    ReplicaId([i; 16])
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn clock(now: u64) -> ClockHealth {
    ClockHealth::healthy(now, 5)
}

fn frame_of(bytes: &[u8]) -> Frame {
    let mut reader = FrameReader::new();
    reader.push(bytes).expect("within the reader bound");
    reader.next_frame().unwrap().unwrap()
}

/// Replicated state stand-in with Alice's session and read/write rules
/// on keys below "z".
struct Domain {
    worker: StoreWorker<ModelEngine>,
    alloc: BarrierAllocator,
}

impl Domain {
    fn new() -> Self {
        let boot = BootId([1; 16]);
        let inc = ReplicaIncarnation::new(1).unwrap();
        let mut worker =
            StoreWorker::open(ModelEngine::new(), boot, inc, GroupLimits::default()).unwrap();
        let mut alloc = BarrierAllocator::new(inc, boot);
        let mut updates = bootstrap_session(&SESSION, ALICE, 64, true).unwrap();
        for (i, action) in [PolicyAction::Read, PolicyAction::Write].iter().enumerate() {
            updates.push(
                rule_update(
                    &PolicyRuleId([i as u8 + 1; 16]),
                    &PolicyRule {
                        principal: ALICE,
                        action: *action,
                        namespace: NS,
                        interval: KeyInterval {
                            lower: vec![],
                            upper: Some(b"z".to_vec()),
                        },
                    },
                )
                .unwrap(),
            );
        }
        // Session and policy rows are admission rows: they need an
        // ordered batch, so the bootstrap carries the application base.
        let base = worker.application_base();
        worker
            .submit(PersistBatch {
                barrier: alloc.allocate(),
                base: Some(base),
                updates,
            })
            .unwrap();
        worker.flush().unwrap();
        Domain { worker, alloc }
    }

    fn apply(&mut self, command: &InternalCommand) -> Response {
        let gated = self.worker.reader().snapshot().unwrap();
        let view = build_internal_view(&gated, command, ViewBudget::default()).unwrap();
        let planned = plan_internal(command, &view, &PlanLimits::default()).unwrap();
        drop(gated);
        match apply_plan(&mut self.worker, self.alloc.allocate(), NS, &planned, None).unwrap() {
            ApplyOutcome::Applied(_) => planned.response,
            other => panic!("{other:?}"),
        }
    }

    fn revoke(&mut self) {
        let response = self.apply(&InternalCommand::RetireSession {
            namespace: NS,
            session: SESSION,
        });
        assert_eq!(response.outcome, Outcome::SessionRetired);
    }

    /// Remove Alice's read permission entirely.
    fn remove_read(&mut self) {
        let response = self.apply(&InternalCommand::PutPolicyRule {
            namespace: NS,
            principal: ALICE,
            rule: PolicyRuleId([1; 16]),
            record: None,
        });
        assert!(!matches!(response.outcome, Outcome::ErrTrustRuleInvalid));
    }

    /// Narrow Alice's read permission to keys below "b".
    fn narrow_read(&mut self) {
        let response = self.apply(&InternalCommand::PutPolicyRule {
            namespace: NS,
            principal: ALICE,
            rule: PolicyRuleId([1; 16]),
            record: Some(PolicyRule {
                principal: ALICE,
                action: PolicyAction::Read,
                namespace: NS,
                interval: KeyInterval {
                    lower: vec![],
                    upper: Some(b"b".to_vec()),
                },
            }),
        });
        assert!(!matches!(response.outcome, Outcome::ErrTrustRuleInvalid));
    }

    fn policy(&self) -> StorePolicySource<'_, StoreWorker<ModelEngine>> {
        StorePolicySource {
            store: &self.worker,
            budget: ViewBudget::default(),
        }
    }
}

fn ring() -> KeyRing {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    KeyRing::new(SigningKey::from_pkcs8_der("sts-1", &key.serialize_der()).unwrap())
}

fn token(ring: &KeyRing, session: SessionId, exp: u64) -> Vec<u8> {
    let claims = ServiceClaims {
        iss: ISSUER.into(),
        sub: hex(&ALICE.0),
        aud: RESOURCE.into(),
        sid: hex(&session.0),
        scope: PolicyAction::FULL_CEILING,
        rule: hex(&session.0),
        generation: 1,
        jti: hex(&[9u8; 32]),
        iat: NOW,
        exp,
    };
    ring.sign(&claims).unwrap().into_bytes()
}

fn config(ring: &KeyRing) -> BindingConfig {
    BindingConfig {
        issuer: ISSUER.into(),
        resource: RESOURCE.into(),
        jwks: ring.jwks(),
        cluster: CLUSTER,
        domain: DOMAIN,
    }
}

fn frontend(ring: &KeyRing) -> BoundFrontend {
    let fast: BTreeSet<ReplicaId> = (0..2).map(r).collect();
    let quorum = BallotConfiguration::c2(
        ConfigurationEpoch::new(1).unwrap(),
        Ballot {
            epoch: ConfigurationEpoch::new(1).unwrap(),
            number: 0,
            leader: r(0),
        },
        (0..3).map(r).collect(),
        fast,
    )
    .unwrap();
    let dispatcher = Dispatcher::new(
        Admission::new(CLUSTER, DOMAIN, AdmissionLimits::default()),
        Collector::new(CollectorConfig {
            quorum,
            max_pending: 64,
            max_resolved: 16,
            max_undelivered_bytes: usize::MAX,
        }),
        16,
    );
    BoundFrontend::new(dispatcher, config(ring), 4, 64)
}

fn retry_key(seq: u64) -> RetryKey {
    RetryKey {
        cluster_id: CLUSTER,
        domain_id: DOMAIN,
        session_id: SESSION,
        client_instance_id: CLIENT,
        request_sequence: RequestSequence::new(seq).unwrap(),
    }
}

fn request(seq: u64, op: CanonicalOperation) -> (CommandId, Frame) {
    let mut logical = LogicalRequest::new(NS, op);
    logical.canonicalize();
    let key = retry_key(seq);
    let command = CommandId::derive(&key, &logical).unwrap();
    let frame = MessageV1::Request(RequestV1::new(key, &logical, 0, 0).unwrap())
        .encode()
        .unwrap();
    (command, frame_of(&frame))
}

fn range(k: &[u8]) -> CanonicalOperation {
    CanonicalOperation::Range(RangeOp {
        range: KeyRange::exact(k.to_vec()),
        revision: None,
        limit: 0,
        keys_only: false,
        count_only: false,
    })
}

fn put(k: &[u8], prev_kv: bool) -> CanonicalOperation {
    CanonicalOperation::Put(PutOp {
        key: k.to_vec(),
        value: b"v".to_vec(),
        lease: None,
        prev_kv,
    })
}

fn entry(v: &[u8]) -> KvEntry {
    KvEntry {
        value: v.to_vec(),
        create_revision: KvRevision::new(1).unwrap(),
        mod_revision: KvRevision::new(1).unwrap(),
        version: 1,
        lease: None,
        lease_generation: None,
    }
}

fn delivery(connection: u64, seq: u64, command: CommandId, outcome: Outcome) -> Delivery {
    let response = Response {
        revision: KvRevision::new(1).unwrap(),
        outcome,
    };
    Delivery {
        connection,
        retry_key: retry_key(seq),
        frame: MessageV1::Response(ResponseV1 {
            command_id: command,
            outcome: OutcomeV1::Ok {
                revision: Some(KvRevision::new(1).unwrap()),
                result: BoundedBytes::new(postcard::to_allocvec(&response).unwrap()).unwrap(),
            },
        })
        .encode()
        .unwrap(),
    }
}

fn outcome_of(d: &Delivery) -> OutcomeV1 {
    match decode_stream(&d.frame).unwrap().as_slice() {
        [MessageV1::Response(r)] => r.outcome.clone(),
        other => panic!("{other:?}"),
    }
}

/// The delivery a caller with a session gets back. Every delivery in
/// these tests answers a bound connection, so nothing here is an
/// establishment settling.
trait Answered {
    fn answered(self) -> Delivery;
}

impl Answered for coord_session::Delivered {
    fn answered(self) -> Delivery {
        match self {
            coord_session::Delivered::Answer(delivery) => delivery,
            other => panic!("{other:?}"),
        }
    }
}

fn is_denied(d: &Delivery) -> bool {
    matches!(outcome_of(d), OutcomeV1::Err { code, .. } if code == codes::NOT_ADMITTED)
}

fn range_outcome(k: &[u8]) -> Outcome {
    Outcome::Range {
        items: vec![RangeItem {
            key: k.to_vec(),
            entry: entry(b"secret"),
        }],
        count: 1,
        more: false,
    }
}

#[test]
fn an_expired_warm_connection_cannot_admit_and_a_rebind_keeps_the_identity() {
    let ring = ring();
    let domain = Domain::new();
    let hub = WatchHub::new(KvRevision::ZERO, KvRevision::ZERO);
    let mut f = frontend(&ring);
    let (_, req) = request(1, put(b"a", false));
    // Nothing before a binding.
    assert_eq!(
        f.on_frame(&clock(NOW), 1, &req, &hub, &domain.policy()),
        Ingress::NotBound
    );
    // A binding admits.
    let bind = frame_of(&bind_frame(&token(&ring, SESSION, NOW + 100)).unwrap());
    let Ingress::Bound(ack) = f.on_frame(&clock(NOW), 1, &bind, &hub, &domain.policy()) else {
        panic!()
    };
    let ack = coord_session::decode_bind_ack(&frame_of(&ack)).unwrap();
    assert_eq!(ack.session, SESSION);
    assert_eq!(ack.expires_at, NOW + 100);
    assert!(matches!(
        f.on_frame(&clock(NOW), 1, &req, &hub, &domain.policy()),
        Ingress::Action(Action::FanOut(_))
    ));
    // Validity ends conservatively: nothing is admitted, and the tick
    // names the connection to close.
    let (_, req2) = request(2, put(b"b", false));
    assert_eq!(
        f.on_frame(&clock(NOW + 96), 1, &req2, &hub, &domain.policy()),
        Ingress::Expired
    );
    assert_eq!(f.tick(&clock(NOW + 96)).0, vec![1]);
    // A rebind for the same session refreshes validity; another session
    // is refused and the binding stays.
    let other = frame_of(&bind_frame(&token(&ring, SessionId([8; 16]), NOW + 1000)).unwrap());
    assert_eq!(
        f.on_frame(&clock(NOW + 96), 1, &other, &hub, &domain.policy()),
        Ingress::Rejected(BindError::SessionMismatch { bound: SESSION })
    );
    let again = frame_of(&bind_frame(&token(&ring, SESSION, NOW + 1000)).unwrap());
    assert!(matches!(
        f.on_frame(&clock(NOW + 96), 1, &again, &hub, &domain.policy()),
        Ingress::Bound(_)
    ));
    assert_eq!(f.binding(1).unwrap().rebinds, 1);
    assert!(matches!(
        f.on_frame(&clock(NOW + 96), 1, &req2, &hub, &domain.policy()),
        Ingress::Action(Action::FanOut(_))
    ));
    // Expired, wrong-audience and foreign-key tokens never bind; an
    // unhealthy clock cannot establish validity.
    let expired = frame_of(&bind_frame(&token(&ring, SESSION, NOW - 1)).unwrap());
    assert!(matches!(
        f.on_frame(&clock(NOW), 2, &expired, &hub, &domain.policy()),
        Ingress::Rejected(BindError::Token(TokenError::Time(_)))
    ));
    let foreign = ring_other();
    let stranger = frame_of(&bind_frame(&token(&foreign, SESSION, NOW + 100)).unwrap());
    assert!(matches!(
        f.on_frame(&clock(NOW), 2, &stranger, &hub, &domain.policy()),
        Ingress::Rejected(BindError::Token(TokenError::UnknownKey))
    ));
    let sick = ClockHealth {
        now: NOW,
        uncertainty: 5,
        healthy: false,
    };
    assert!(matches!(
        f.on_frame(&sick, 2, &bind, &hub, &domain.policy()),
        Ingress::Rejected(BindError::Token(TokenError::Time(_)))
    ));
    assert!(f.binding(2).is_none());
    // A request whose retry key names another session is not admitted
    // under this binding.
    let mut foreign_key = retry_key(3);
    foreign_key.session_id = SessionId([8; 16]);
    let mut logical = LogicalRequest::new(NS, put(b"c", false));
    logical.canonicalize();
    let req3 = frame_of(
        &MessageV1::Request(RequestV1::new(foreign_key, &logical, 0, 0).unwrap())
            .encode()
            .unwrap(),
    );
    match f.on_frame(&clock(NOW + 96), 1, &req3, &hub, &domain.policy()) {
        Ingress::Action(Action::Respond(d)) => assert!(is_denied(&d)),
        other => panic!("{other:?}"),
    }
}

fn ring_other() -> KeyRing {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    KeyRing::new(SigningKey::from_pkcs8_der("foreign", &key.serialize_der()).unwrap())
}

#[test]
fn post_revocation_protected_data_is_denied_even_from_cached_results() {
    let ring = ring();
    let mut domain = Domain::new();
    let hub = WatchHub::new(KvRevision::ZERO, KvRevision::ZERO);
    let mut f = frontend(&ring);
    let bind = frame_of(&bind_frame(&token(&ring, SESSION, NOW + 1000)).unwrap());
    assert!(matches!(
        f.on_frame(&clock(NOW), 1, &bind, &hub, &domain.policy()),
        Ingress::Bound(_)
    ));
    // A read is admitted; its result passes the barrier while policy
    // permits, with one fresh barrier per delivery.
    let (c1, req) = request(1, range(b"a"));
    assert!(matches!(
        f.on_frame(&clock(NOW), 1, &req, &hub, &domain.policy()),
        Ingress::Action(Action::FanOut(_))
    ));
    let d = f
        .deliver(delivery(1, 1, c1, range_outcome(b"a")), &domain.policy())
        .answered();
    assert!(!is_denied(&d));
    assert_eq!(f.barriers_read, 1);
    // Narrowed permission denies the same historical data.
    domain.narrow_read();
    let d = f
        .deliver(delivery(1, 1, c1, range_outcome(b"c")), &domain.policy())
        .answered();
    assert!(is_denied(&d));
    let d = f
        .deliver(delivery(1, 1, c1, range_outcome(b"a")), &domain.policy())
        .answered();
    assert!(!is_denied(&d), "keys still permitted are still delivered");
    // Revocation denies everything protected, including cached results
    // answered to a retry or a resolution.
    domain.revoke();
    let d = f
        .deliver(delivery(1, 1, c1, range_outcome(b"a")), &domain.policy())
        .answered();
    assert!(is_denied(&d));
    assert_eq!(
        f.barriers_read, 4,
        "a fresh barrier per delivery, never cached"
    );
    // The collector establishes the read from voter evidence and the
    // leader's release; the delivery and the later resolution of the
    // retained outcome are both gated.
    let ballot = Ballot {
        epoch: ConfigurationEpoch::new(1).unwrap(),
        number: 0,
        leader: r(0),
    };
    let path = Digest32([1; 32]);
    let prov = |i: u8| PeerProvenance::from_transport(r(i), ReplicaIncarnation::new(1).unwrap(), 1);
    let admitted = f
        .dispatcher()
        .collector()
        .admission(&c1)
        .expect("outstanding");
    f.dispatcher_mut()
        .on_evidence(
            prov(0),
            ProtocolMessage::LeaderReply {
                ballot,
                command: c1,
                seqnum: 1,
                deps: vec![],
                path,
            },
        )
        .unwrap();
    f.dispatcher_mut()
        .on_evidence(
            prov(1),
            ProtocolMessage::FastAck(FastAck {
                replica: r(1),
                ballot,
                command: c1,
                deps: vec![],
                paths: vec![],
                path,
                // Evidence for a command is counted only under the
                // admission it was submitted with.
                admission: admitted,
                seqnum: None,
            }),
        )
        .unwrap();
    let response = Response {
        revision: KvRevision::new(1).unwrap(),
        outcome: range_outcome(b"a"),
    };
    let released = ReleasedResult::from_gate(
        EstablishedResult::establish(EstablishmentEvidence {
            command: c1,
            epoch: ConfigurationEpoch::new(1).unwrap(),
            ballot,
            position: ExecutionPosition::new(1).unwrap(),
            closed_predecessors: vec![],
            result_digest: Digest32([2; 32]),
            revision: Some(KvRevision::new(1).unwrap()),
            fast_path: true,
        })
        .unwrap(),
        postcard::to_allocvec(&response).unwrap(),
        false,
    );
    let delivered = f
        .dispatcher_mut()
        .on_release(prov(0), released)
        .unwrap()
        .expect("attached caller");
    assert!(is_denied(
        &f.deliver(delivered, &domain.policy()).answered()
    ));
    let resolve = frame_of(
        &MessageV1::ResolveRequest(ResolveRequestV1 {
            retry_key: retry_key(1),
            command_id: c1,
        })
        .encode()
        .unwrap(),
    );
    match f.on_frame(&clock(NOW), 1, &resolve, &hub, &domain.policy()) {
        Ingress::Action(Action::Respond(d)) => {
            assert!(is_denied(&d), "revoked: cached data denied")
        }
        other => panic!("{other:?}"),
    }
    // A mutation acknowledgement without previous values is not
    // protected data and is still delivered; one with a previous value
    // is gated by the request's key.
    let (c2, req2) = request(2, put(b"a", true));
    assert!(matches!(
        f.on_frame(&clock(NOW), 1, &req2, &hub, &domain.policy()),
        Ingress::Action(Action::FanOut(_))
    ));
    let ack = f
        .deliver(
            delivery(1, 2, c2, Outcome::Put { prev: None }),
            &domain.policy(),
        )
        .answered();
    assert!(!is_denied(&ack));
    let with_prev = f
        .deliver(
            delivery(
                1,
                2,
                c2,
                Outcome::Put {
                    prev: Some(entry(b"old")),
                },
            ),
            &domain.policy(),
        )
        .answered();
    assert!(is_denied(&with_prev));
    assert!(f.denied >= 3);
}

#[test]
fn watch_output_is_gated_per_selected_batch_and_progress_cannot_bypass_it() {
    let ring = ring();
    let mut domain = Domain::new();
    let hub = WatchHub::new(KvRevision::ZERO, KvRevision::ZERO);
    let mut f = frontend(&ring);
    let bind = frame_of(&bind_frame(&token(&ring, SESSION, NOW + 1000)).unwrap());
    assert!(matches!(
        f.on_frame(&clock(NOW), 1, &bind, &hub, &domain.policy()),
        Ingress::Bound(_)
    ));
    let open = frame_of(
        &MessageV1::WatchOpen(WatchOpenV1 {
            watch_id: 7,
            namespace: NS,
            key: BoundedBytes::new(vec![]).unwrap(),
            range_end: Some(BoundedBytes::new(b"~".to_vec()).unwrap()),
            start_revision: None,
            prev_kv: false,
            progress_notify: true,
        })
        .encode()
        .unwrap(),
    );
    let registration = match f.on_frame(&clock(NOW), 1, &open, &hub, &domain.policy()) {
        Ingress::Action(Action::WatchOpened { registration, .. }) => registration,
        other => panic!("{other:?}"),
    };
    hub.replay_complete(registration.id).unwrap();
    let event = |k: &[u8]| KvEvent {
        kind: KvEventKind::Put,
        key: k.to_vec(),
        entry: Some(entry(b"v")),
        prev: None,
    };
    for rev in 1..=6u64 {
        hub.publish(NS, KvRevision::new(rev).unwrap(), &[event(b"a")])
            .unwrap();
    }
    // One barrier per bounded pump (four items), shared only by the
    // batches selected in that pump.
    let frames = f.pump_watch(&clock(NOW), 1, 7, &hub, &domain.policy());
    assert_eq!(frames.len(), 4);
    assert_eq!(f.barriers_read, 1);
    let frames = f.pump_watch(&clock(NOW), 1, 7, &hub, &domain.policy());
    assert_eq!(f.barriers_read, 2);
    let kinds: Vec<&str> = frames
        .iter()
        .map(|fr| match decode_stream(fr).unwrap().remove(0) {
            MessageV1::WatchEvents(_) => "events",
            MessageV1::WatchProgress(_) => "progress",
            MessageV1::WatchClose(_) => "close",
            _ => "other",
        })
        .collect();
    assert_eq!(kinds, vec!["events", "events"]);
    // Narrowed policy: a batch with a key outside the permitted interval
    // closes the watch as unauthorized, and the progress a later
    // non-matching revision would announce never crosses it.
    domain.narrow_read();
    hub.publish(NS, KvRevision::new(7).unwrap(), &[event(b"a")])
        .unwrap();
    hub.publish(NS, KvRevision::new(8).unwrap(), &[event(b"c")])
        .unwrap();
    hub.publish(NS, KvRevision::new(9).unwrap(), &[event(b"\x7fout")])
        .unwrap();
    let frames = f.pump_watch(&clock(NOW), 1, 7, &hub, &domain.policy());
    let kinds: Vec<String> = frames
        .iter()
        .map(|fr| match decode_stream(fr).unwrap().remove(0) {
            MessageV1::WatchEvents(e) => format!("events@{}", e.revision.get()),
            MessageV1::WatchProgress(p) => format!("progress@{}", p.revision.get()),
            MessageV1::WatchClose(c) => format!("close:{:?}", c.reason),
            _ => "other".into(),
        })
        .collect();
    assert_eq!(kinds, vec!["events@7", "close:Unauthorized"]);
    assert!(f.denied >= 1);
    // After revocation a fresh watch delivers nothing: the first selected
    // batch is denied and the watch closes before any progress.
    domain.revoke();
    let open2 = frame_of(
        &MessageV1::WatchOpen(WatchOpenV1 {
            watch_id: 8,
            namespace: NS,
            key: BoundedBytes::new(b"a".to_vec()).unwrap(),
            range_end: None,
            start_revision: None,
            prev_kv: false,
            progress_notify: true,
        })
        .encode()
        .unwrap(),
    );
    let registration = match f.on_frame(&clock(NOW), 1, &open2, &hub, &domain.policy()) {
        Ingress::Action(Action::WatchOpened { registration, .. }) => registration,
        other => panic!("{other:?}"),
    };
    hub.replay_complete(registration.id).unwrap();
    hub.publish(NS, KvRevision::new(10).unwrap(), &[event(b"a")])
        .unwrap();
    let frames = f.pump_watch(&clock(NOW), 1, 8, &hub, &domain.policy());
    assert_eq!(frames.len(), 1);
    assert!(matches!(
        decode_stream(&frames[0]).unwrap().remove(0),
        MessageV1::WatchClose(c) if c.reason == coord_types::wire_v1::WatchCloseReasonV1::Unauthorized
    ));
    // An expired binding pumps nothing but the close.
    let mut g = frontend(&ring);
    let short = frame_of(&bind_frame(&token(&ring, SESSION, NOW + 10)).unwrap());
    assert!(matches!(
        g.on_frame(&clock(NOW), 3, &short, &hub, &Domain::new().policy()),
        Ingress::Bound(_)
    ));
    let fresh = Domain::new();
    let registration = match g.on_frame(&clock(NOW), 3, &open, &hub, &fresh.policy()) {
        Ingress::Action(Action::WatchOpened { registration, .. }) => registration,
        other => panic!("{other:?}"),
    };
    hub.replay_complete(registration.id).unwrap();
    hub.publish(NS, KvRevision::new(11).unwrap(), &[event(b"a")])
        .unwrap();
    let frames = g.pump_watch(&clock(NOW + 20), 3, 7, &hub, &fresh.policy());
    assert!(matches!(
        decode_stream(frames.last().unwrap()).unwrap().remove(0),
        MessageV1::WatchClose(_)
    ));
}

#[test]
fn previously_authorized_in_flight_work_follows_documented_semantics() {
    let ring = ring();
    let mut domain = Domain::new();
    let hub = WatchHub::new(KvRevision::ZERO, KvRevision::ZERO);
    let mut f = frontend(&ring);
    let bind = frame_of(&bind_frame(&token(&ring, SESSION, NOW + 50)).unwrap());
    assert!(matches!(
        f.on_frame(&clock(NOW), 1, &bind, &hub, &domain.policy()),
        Ingress::Bound(_)
    ));
    // Admitted before revocation: a write and a read.
    let (cw, write) = request(1, put(b"a", false));
    let (cr, read) = request(2, range(b"a"));
    for req in [&write, &read] {
        assert!(matches!(
            f.on_frame(&clock(NOW), 1, req, &hub, &domain.policy()),
            Ingress::Action(Action::FanOut(_))
        ));
    }
    domain.revoke();
    // The write's acknowledgement (execution decided at its position) is
    // delivered; the read's protected data is not.
    let ack = f
        .deliver(
            delivery(1, 1, cw, Outcome::Put { prev: None }),
            &domain.policy(),
        )
        .answered();
    assert!(matches!(outcome_of(&ack), OutcomeV1::Ok { .. }));
    let data = f
        .deliver(delivery(1, 2, cr, range_outcome(b"a")), &domain.policy())
        .answered();
    assert!(is_denied(&data));
    // The binding's own expiry does not cancel admitted work: it stays
    // pending and resolvable under the session, only new work is refused.
    let (deadline, _) = f.tick(&clock(NOW + 100));
    assert_eq!(deadline, vec![1]);
    assert!(f.dispatcher().collector().is_pending(&cw));
    let (_, req3) = request(3, put(b"b", false));
    assert_eq!(
        f.on_frame(&clock(NOW + 100), 1, &req3, &hub, &domain.policy()),
        Ingress::Expired
    );
    // A closed connection detaches its watches and requests; the
    // identities stay resolvable through the collector.
    f.on_connection_closed(1, &hub);
    assert!(f.binding(1).is_none());
    assert!(f.dispatcher().collector().is_pending(&cw));
    // Policy that cannot be read denies (fail closed).
    struct Down;
    impl coord_session::PolicySource for Down {
        fn barrier(
            &self,
            _: NamespaceId,
            _: &SessionId,
        ) -> Result<coord_session::AuthorizationBarrier, coord_session::PolicyError> {
            Err(coord_session::PolicyError::Unavailable)
        }
    }
    let mut g = frontend(&ring);
    let bind = frame_of(&bind_frame(&token(&ring, SESSION, NOW + 1000)).unwrap());
    let fresh = Domain::new();
    assert!(matches!(
        g.on_frame(&clock(NOW), 2, &bind, &hub, &fresh.policy()),
        Ingress::Bound(_)
    ));
    let (c4, read) = request(4, range(b"a"));
    assert!(matches!(
        g.on_frame(&clock(NOW), 2, &read, &hub, &fresh.policy()),
        Ingress::Action(Action::FanOut(_))
    ));
    assert!(is_denied(
        &g.deliver(delivery(2, 4, c4, range_outcome(b"a")), &Down)
            .answered()
    ));
    assert!(!is_denied(
        &g.deliver(delivery(2, 4, c4, range_outcome(b"a")), &fresh.policy())
            .answered()
    ));
}

/// A token like [`token`] but with chosen claims.
fn token_with(
    ring: &KeyRing,
    session: SessionId,
    exp: u64,
    scope: u32,
    generation: u64,
) -> Vec<u8> {
    let claims = ServiceClaims {
        iss: ISSUER.into(),
        sub: hex(&ALICE.0),
        aud: RESOURCE.into(),
        sid: hex(&session.0),
        scope,
        rule: hex(&session.0),
        generation,
        jti: hex(&[9u8; 32]),
        iat: NOW,
        exp,
    };
    ring.sign(&claims).unwrap().into_bytes()
}

#[test]
fn a_rejected_conflicting_request_never_replaces_the_accepted_metadata() {
    // A conflicting payload under a pending retry key used to overwrite
    // the namespace and keys that authorize the original command's
    // result, and the rejection did not put them back. The eventual
    // historical result was then authorized against a namespace the
    // attacker chose, where it still had read access.
    let ring = ring();
    let domain = Domain::new();
    let hub = WatchHub::new(KvRevision::ZERO, KvRevision::ZERO);
    let mut f = frontend(&ring);
    let bind = frame_of(&bind_frame(&token(&ring, SESSION, NOW + 100)).unwrap());
    let Ingress::Bound(_) = f.on_frame(&clock(NOW), 1, &bind, &hub, &domain.policy()) else {
        panic!()
    };
    // The accepted request reads the protected namespace.
    let (command, req) = request(1, range(b"secret"));
    assert!(matches!(
        f.on_frame(&clock(NOW), 1, &req, &hub, &domain.policy()),
        Ingress::Action(Action::FanOut(_))
    ));
    // A conflicting payload under the same retry key, naming another
    // namespace: the dispatcher refuses it.
    let mut other = LogicalRequest::new(NamespaceId([6; 16]), range(b"secret"));
    other.canonicalize();
    let key = retry_key(1);
    let conflicting = frame_of(
        &MessageV1::Request(RequestV1::new(key, &other, 0, 0).unwrap())
            .encode()
            .unwrap(),
    );
    match f.on_frame(&clock(NOW), 1, &conflicting, &hub, &domain.policy()) {
        Ingress::Action(Action::Respond(d)) => assert!(
            matches!(outcome_of(&d), OutcomeV1::Err { code, .. }
                if code == codes::REQUEST_IDENTITY_CONFLICT),
            "{:?}",
            outcome_of(&d)
        ),
        other => panic!("{other:?}"),
    }
    // The original result is still authorized in the original namespace,
    // where read access has since been removed.
    let mut domain = domain;
    domain.remove_read();
    let gated = f
        .deliver(
            delivery(1, 1, command, range_outcome(b"secret")),
            &domain.policy(),
        )
        .answered();
    assert!(
        is_denied(&gated),
        "the accepted request's namespace decides: {:?}",
        outcome_of(&gated)
    );
}

#[test]
fn read_output_that_names_no_key_is_still_reauthorized() {
    // An empty protected-key list is not an unprotected acknowledgement:
    // a count-only range names no key and still reports how many there
    // were, and an empty range discloses absence. Both used to be
    // delivered without a fresh barrier, from cache, after revocation.
    let ring = ring();
    let mut domain = Domain::new();
    let hub = WatchHub::new(KvRevision::ZERO, KvRevision::ZERO);
    let mut f = frontend(&ring);
    let bind = frame_of(&bind_frame(&token(&ring, SESSION, NOW + 100)).unwrap());
    let Ingress::Bound(_) = f.on_frame(&clock(NOW), 1, &bind, &hub, &domain.policy()) else {
        panic!()
    };
    let (command, req) = request(1, range(b"secret"));
    assert!(matches!(
        f.on_frame(&clock(NOW), 1, &req, &hub, &domain.policy()),
        Ingress::Action(Action::FanOut(_))
    ));
    let empty = Outcome::Range {
        items: Vec::new(),
        count: 7,
        more: false,
    };
    // Permitted now.
    let ok = f
        .deliver(delivery(1, 1, command, empty.clone()), &domain.policy())
        .answered();
    assert!(!is_denied(&ok), "{:?}", outcome_of(&ok));
    // Denied once read access is gone, although it names no key.
    domain.remove_read();
    let gated = f
        .deliver(delivery(1, 1, command, empty), &domain.policy())
        .answered();
    assert!(is_denied(&gated), "{:?}", outcome_of(&gated));
    // A pure mutation acknowledgement still needs no barrier.
    let (put_command, put_req) = request(2, put(b"a", false));
    assert!(matches!(
        f.on_frame(&clock(NOW), 1, &put_req, &hub, &domain.policy()),
        Ingress::Action(Action::FanOut(_))
    ));
    let ack = f
        .deliver(
            delivery(1, 2, put_command, Outcome::Put { prev: None }),
            &domain.policy(),
        )
        .answered();
    assert!(!is_denied(&ack), "{:?}", outcome_of(&ack));
}

#[test]
fn a_token_without_the_read_bit_receives_no_read_output() {
    // The replicated session's ceiling can be wider than the scope in the
    // token a connection presented, and a watch open bypasses unary
    // admission, so the bound scope has to restrict read output itself.
    let ring = ring();
    let domain = Domain::new();
    let hub = WatchHub::new(KvRevision::ZERO, KvRevision::ZERO);
    let mut f = frontend(&ring);
    let write_only = PolicyAction::Write.bit() | PolicyAction::Delete.bit();
    let bind =
        frame_of(&bind_frame(&token_with(&ring, SESSION, NOW + 100, write_only, 1)).unwrap());
    let Ingress::Bound(_) = f.on_frame(&clock(NOW), 1, &bind, &hub, &domain.policy()) else {
        panic!()
    };
    let (command, req) = request(1, range(b"secret"));
    assert!(matches!(
        f.on_frame(&clock(NOW), 1, &req, &hub, &domain.policy()),
        Ingress::Action(Action::FanOut(_))
    ));
    let gated = f
        .deliver(
            delivery(1, 1, command, range_outcome(b"secret")),
            &domain.policy(),
        )
        .answered();
    assert!(
        is_denied(&gated),
        "the session permits the read; the token does not: {:?}",
        outcome_of(&gated)
    );
}

#[test]
fn a_rebind_refreshes_validity_and_never_the_authorization_context() {
    // The rebind check compared only the session, after which the new
    // token replaced the principal, scope and rule generation: any valid
    // same-session token could change what the connection may do, and a
    // wider scope would widen every later admission.
    let ring = ring();
    let domain = Domain::new();
    let hub = WatchHub::new(KvRevision::ZERO, KvRevision::ZERO);
    let mut f = frontend(&ring);
    let narrow = PolicyAction::Read.bit();
    let bind = frame_of(&bind_frame(&token_with(&ring, SESSION, NOW + 100, narrow, 1)).unwrap());
    let Ingress::Bound(_) = f.on_frame(&clock(NOW), 1, &bind, &hub, &domain.policy()) else {
        panic!()
    };
    // The same session with a wider scope is refused.
    let wider = frame_of(
        &bind_frame(&token_with(
            &ring,
            SESSION,
            NOW + 200,
            PolicyAction::FULL_CEILING,
            1,
        ))
        .unwrap(),
    );
    assert!(
        matches!(
            f.on_frame(&clock(NOW), 1, &wider, &hub, &domain.policy()),
            Ingress::Rejected(_)
        ),
        "a rebind may not widen the scope"
    );
    // A later generation of the same session is refused too.
    let regenerated =
        frame_of(&bind_frame(&token_with(&ring, SESSION, NOW + 200, narrow, 2)).unwrap());
    assert!(matches!(
        f.on_frame(&clock(NOW), 1, &regenerated, &hub, &domain.policy()),
        Ingress::Rejected(_)
    ));
    // The same claims with a later expiry refresh validity.
    let refreshed =
        frame_of(&bind_frame(&token_with(&ring, SESSION, NOW + 200, narrow, 1)).unwrap());
    let Ingress::Bound(ack) = f.on_frame(&clock(NOW), 1, &refreshed, &hub, &domain.policy()) else {
        panic!("a pure refresh is accepted")
    };
    let ack = coord_session::decode_bind_ack(&frame_of(&ack)).unwrap();
    assert_eq!(ack.expires_at, NOW + 200);
}

/// A replica whose projection has not caught up with the session yet.
/// The row is not there; it is not *gone*.
struct Behind;

impl coord_session::PolicySource for Behind {
    fn barrier(
        &self,
        namespace: NamespaceId,
        _: &SessionId,
    ) -> Result<coord_session::AuthorizationBarrier, coord_session::PolicyError> {
        Ok(coord_session::AuthorizationBarrier::new(
            ExecutionPosition::ZERO,
            namespace,
            coord_state::policy::Authorization {
                session: None,
                trust_rule: None,
                rules: Vec::new(),
            },
        ))
    }
}

fn is_pending(d: &Delivery) -> bool {
    matches!(outcome_of(d), OutcomeV1::Pending)
}

/// A node that has not projected the session yet says "not yet", and a
/// node that has projected it and no longer finds it says "no".
///
/// The difference is the whole of it. A binding is established from a
/// committed session-creation command, but the disclosure barrier reads
/// this node's materialized projection, and a replica that has fallen
/// behind has not projected that command yet. Refusing there is a false
/// refusal, and an expensive one: the client library reads
/// `NOT_ADMITTED` as a credential that is no longer good, throws it
/// away and binds again -- making a newer session this node has
/// projected even less of. A benchmark caught that as about a third of
/// the reads of one caller in three failing, on a domain that was
/// working, and never recovering.
#[test]
fn a_projection_that_is_behind_holds_a_disclosure_instead_of_refusing_it() {
    let ring = ring();
    let hub = WatchHub::new(KvRevision::ZERO, KvRevision::ZERO);
    let domain = Domain::new();
    let mut f = frontend(&ring);
    // No barrier on this node has shown the row: the binding is
    // established from the session-creation command's outcome, which
    // is the one way a binding exists before the projection has it.
    let credential = token(&ring, SESSION, NOW + 1000);
    let bind = frame_of(&bind_frame(&credential).unwrap());
    assert!(matches!(
        f.on_frame(&clock(NOW), 7, &bind, &hub, &Behind),
        Ingress::Establishing(_)
    ));
    let key = coord_session::verify_bind(&config(&ring), &credential, &clock(NOW), None)
        .unwrap()
        .establishment_key(&config(&ring));
    let (command, read) = request(1, range(b"a"));
    let mut created = delivery(7, 1, command, Outcome::SessionCreated { session: SESSION });
    created.retry_key = key;
    assert!(matches!(
        f.deliver(created, &Behind),
        coord_session::Delivered::Answer(_)
    ));
    assert!(f.binding(7).is_some(), "the establishment did not bind");
    assert!(matches!(
        f.on_frame(&clock(NOW), 7, &read, &hub, &domain.policy()),
        Ingress::Action(Action::FanOut(_))
    ));

    // The answer comes back while this node's projection is behind:
    // held, not refused, and nothing is disclosed either way.
    let held = f
        .deliver(delivery(7, 1, command, range_outcome(b"a")), &Behind)
        .answered();
    assert!(
        is_pending(&held),
        "a projection that is behind refused instead of holding: {:?}",
        outcome_of(&held)
    );
    assert!(!is_denied(&held));
    assert_eq!(f.deferred, 1);
    assert_eq!(f.denied, 0);

    // It catches up: the same answer is now disclosed.
    let served = f
        .deliver(
            delivery(7, 1, command, range_outcome(b"a")),
            &domain.policy(),
        )
        .answered();
    assert!(!is_denied(&served) && !is_pending(&served));

    // And once the row has been seen, its absence is a real answer:
    // policy moved, and a projection only moves forward.
    let refused = f
        .deliver(delivery(7, 1, command, range_outcome(b"a")), &Behind)
        .answered();
    assert!(
        is_denied(&refused),
        "a session this node had seen and no longer finds was not refused"
    );
    assert_eq!(f.deferred, 1);
    assert_eq!(f.denied, 1);
}

/// The bind barrier counts as having seen the row. A connection that
/// bound against a present session and read nothing before that session
/// was retired gets the real refusal on its first gated read, not a
/// "not yet" that would hold every retry for ever.
#[test]
fn a_session_the_bind_barrier_showed_and_then_retired_is_refused_not_held() {
    let ring = ring();
    let hub = WatchHub::new(KvRevision::ZERO, KvRevision::ZERO);
    let domain = Domain::new();
    let mut f = frontend(&ring);
    // The row is present when the connection binds.
    let bind = frame_of(&bind_frame(&token(&ring, SESSION, NOW + 1000)).unwrap());
    assert!(matches!(
        f.on_frame(&clock(NOW), 7, &bind, &hub, &domain.policy()),
        Ingress::Bound(_)
    ));
    let (command, read) = request(1, range(b"a"));
    assert!(matches!(
        f.on_frame(&clock(NOW), 7, &read, &hub, &domain.policy()),
        Ingress::Action(Action::FanOut(_))
    ));

    // By the time the answer comes back the row is gone. This node had
    // shown it at the bind, so its absence is an answer, not a lag.
    let refused = f
        .deliver(delivery(7, 1, command, range_outcome(b"a")), &Behind)
        .answered();
    assert!(
        is_denied(&refused),
        "a session the bind barrier showed and no longer finds was held instead of refused: {:?}",
        outcome_of(&refused)
    );
    assert!(!is_pending(&refused));
    assert_eq!(f.deferred, 0);
    assert_eq!(f.denied, 1);
}
