//! task-34 acceptance: reconnect, reset and timeout preserve the
//! invocation identity and payload; a payload change conflicts; an
//! ambiguous timeout reports an unknown outcome and resolves by identity;
//! stream pressure is bounded; one credential exchange serves many
//! operations; protocol evidence never reaches the application; a
//! restored instance never reuses a sequence.

use coord_collector::evidence_frame_from_bytes;
use coord_sdk::identity::InvocationError;
use coord_sdk::{
    Client, ClientConfig, ClientError, ClientInstance, Completion, ConnectionId, Credential,
    Outcome, PoolError, PoolLimits, RequestId, RequestState, RetryError, SdkAction, StaticProvider,
};
use coord_types::CommandId;
use coord_types::ids::*;
use coord_types::logical_v1::*;
use coord_types::wire_v1::{
    BoundedBytes, MessageV1, OutcomeV1, ResponseV1, WatchProgressV1, codes, decode_stream,
};

const NS: NamespaceId = NamespaceId([5; 16]);

fn instance() -> ClientInstance {
    ClientInstance::new(
        ClusterId([1; 16]),
        DomainId([2; 16]),
        SessionId([3; 16]),
        ClientInstanceId([4; 16]),
    )
}

fn provider() -> StaticProvider {
    StaticProvider::new(Credential::new(b"secret-token".to_vec(), 1_000_000))
}

fn client(config: ClientConfig) -> Client<StaticProvider> {
    Client::new(config, instance(), provider())
}

fn put(k: &[u8], v: &[u8]) -> LogicalRequest {
    LogicalRequest::new(
        NS,
        CanonicalOperation::Put(PutOp {
            key: k.to_vec(),
            value: v.to_vec(),
            lease: None,
            prev_kv: false,
        }),
    )
}

fn ok_frame(command: CommandId, revision: u64) -> Vec<u8> {
    MessageV1::Response(ResponseV1 {
        command_id: command,
        outcome: OutcomeV1::Ok {
            revision: Some(KvRevision::new(revision).unwrap()),
            result: BoundedBytes::new(vec![0xab]).unwrap(),
        },
    })
    .encode()
    .unwrap()
}

fn err_frame(command: CommandId, code: u16) -> Vec<u8> {
    MessageV1::Response(ResponseV1 {
        command_id: command,
        outcome: OutcomeV1::Err {
            code,
            detail: BoundedBytes::new(b"x".to_vec()).unwrap(),
        },
    })
    .encode()
    .unwrap()
}

fn outcome_frame(command: CommandId, outcome: OutcomeV1) -> Vec<u8> {
    MessageV1::Response(ResponseV1 {
        command_id: command,
        outcome,
    })
    .encode()
    .unwrap()
}

fn sends(actions: &[SdkAction]) -> Vec<(ConnectionId, RequestId, Vec<u8>)> {
    actions
        .iter()
        .filter_map(|a| match a {
            SdkAction::Send {
                connection,
                request,
                frame,
            } => Some((*connection, *request, frame.clone())),
            _ => None,
        })
        .collect()
}

fn resolves(actions: &[SdkAction]) -> Vec<(ConnectionId, RequestId, Vec<u8>)> {
    actions
        .iter()
        .filter_map(|a| match a {
            SdkAction::Resolve {
                connection,
                request,
                frame,
            } => Some((*connection, *request, frame.clone())),
            _ => None,
        })
        .collect()
}

fn connected(c: &mut Client<StaticProvider>, now: u64, id: u64) -> ConnectionId {
    let connection = ConnectionId(id);
    c.connect(now, connection).unwrap();
    let actions = c.take_actions();
    assert!(
        matches!(actions.as_slice(), [SdkAction::Bind { connection: b, .. }] if *b == connection)
    );
    c.bound(now, connection);
    connection
}

#[test]
fn reconnect_and_reset_preserve_invocation_and_payload() {
    let mut c = client(ClientConfig::default());
    let c1 = connected(&mut c, 0, 1);
    let id = c.submit(0, &put(b"a", b"1"), 0).unwrap();
    let first = sends(&c.take_actions());
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].0, c1);
    let invocation = c.invocation(id).unwrap().clone();
    assert_eq!(first[0].2, invocation.frame);
    // The connection resets before any response: the request is queued
    // as it is, and goes out unchanged on the next bound connection.
    c.on_connection_lost(5, c1);
    assert_eq!(c.state(id), Some(&RequestState::Queued));
    assert!(
        c.take_actions().is_empty(),
        "nothing to send without a connection"
    );
    let c2 = connected(&mut c, 10, 2);
    let second = sends(&c.take_actions());
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].0, c2);
    assert_eq!(second[0].1, id);
    assert_eq!(second[0].2, first[0].2, "identical bytes on the retry");
    assert_eq!(c.invocation(id).unwrap(), &invocation, "identity unchanged");
    assert_eq!(c.attempts(id), 2);
    match decode_stream(&second[0].2).unwrap().as_slice() {
        [MessageV1::Request(r)] => {
            assert_eq!(r.retry_key.request_sequence.get(), 1);
            assert_eq!(r.retry_key.client_instance_id, ClientInstanceId([4; 16]));
        }
        other => panic!("{other:?}"),
    }
    // A response on the old connection is not for an in-flight stream.
    assert_eq!(
        c.on_frame(11, c1, &ok_frame(invocation.command_id, 1)),
        Err(ClientError::UnexpectedResponse)
    );
    c.on_frame(12, c2, &ok_frame(invocation.command_id, 1))
        .unwrap();
    assert_eq!(
        c.take_completions(),
        vec![Completion {
            request: id,
            command_id: invocation.command_id,
            outcome: Outcome::Established {
                revision: Some(KvRevision::new(1).unwrap()),
                result: vec![0xab],
            },
        }]
    );
    assert_eq!(c.outstanding(), 0);
    assert_eq!(c.pool().in_use(c2), 0, "the stream slot was released");
}

#[test]
fn a_payload_change_conflicts_locally_and_from_the_server() {
    let mut c = client(ClientConfig::default());
    let c1 = connected(&mut c, 0, 1);
    let id = c.submit(0, &put(b"a", b"1"), 0).unwrap();
    let command = c.invocation(id).unwrap().command_id;
    let _ = c.take_actions();
    c.on_frame(1, c1, &ok_frame(command, 1)).unwrap();
    let _ = c.take_completions();
    // The same sequence with another payload: refused here, nothing sent.
    assert_eq!(
        c.retry(2, 1, &put(b"a", b"2"), 0),
        Err(ClientError::Invocation(InvocationError::PayloadConflict {
            sequence: 1
        }))
    );
    assert!(c.take_actions().is_empty());
    // The same payload: the retained outcome, again nothing sent.
    assert_eq!(c.retry(2, 1, &put(b"a", b"1"), 0), Ok(id));
    assert!(c.take_actions().is_empty());
    assert!(matches!(
        c.take_completions().as_slice(),
        [Completion {
            outcome: Outcome::Established { .. },
            ..
        }]
    ));
    // The server's identity conflict is the same typed error.
    let id2 = c.submit(3, &put(b"b", b"1"), 0).unwrap();
    let command2 = c.invocation(id2).unwrap().command_id;
    let _ = c.take_actions();
    c.on_frame(
        4,
        c1,
        &err_frame(command2, codes::REQUEST_IDENTITY_CONFLICT),
    )
    .unwrap();
    assert_eq!(
        c.take_completions()[0].outcome,
        Outcome::Failed(RetryError::PayloadConflict { sequence: 2 })
    );
    // Other frozen codes map to typed errors; backpressure re-queues the
    // same invocation after the backoff.
    let id3 = c.submit(5, &put(b"c", b"1"), 0).unwrap();
    let command3 = c.invocation(id3).unwrap().command_id;
    let sent = sends(&c.take_actions());
    c.on_frame(6, c1, &err_frame(command3, codes::BACKPRESSURE))
        .unwrap();
    assert_eq!(c.state(id3), Some(&RequestState::Queued));
    assert!(c.take_completions().is_empty());
    c.tick(6 + ClientConfig::default().backoff);
    let again = sends(&c.take_actions());
    assert_eq!(again[0].2, sent[0].2, "same invocation after backpressure");
    c.on_frame(200, c1, &err_frame(command3, codes::NOT_ADMITTED))
        .unwrap();
    assert_eq!(
        c.take_completions()[0].outcome,
        Outcome::Failed(RetryError::NotAdmitted)
    );
    let id4 = c.submit(201, &put(b"d", b"1"), 0).unwrap();
    let command4 = c.invocation(id4).unwrap().command_id;
    let _ = c.take_actions();
    c.on_frame(202, c1, &err_frame(command4, 0x7777)).unwrap();
    assert_eq!(
        c.take_completions()[0].outcome,
        Outcome::Failed(RetryError::Other { code: 0x7777 })
    );
}

#[test]
fn an_ambiguous_timeout_reports_unknown_and_resolves_by_identity() {
    let config = ClientConfig {
        resolve_interval: 100,
        ..ClientConfig::default()
    };
    let mut c = client(config);
    let c1 = connected(&mut c, 0, 1);
    let id = c.submit(0, &put(b"a", b"1"), 50).unwrap();
    let invocation = c.invocation(id).unwrap().clone();
    let _ = c.take_actions();
    c.tick(49);
    assert!(c.take_completions().is_empty());
    c.tick(50);
    // The deadline: unknown, never failed; a resolution goes out by
    // identity on a fresh stream.
    assert_eq!(
        c.take_completions(),
        vec![Completion {
            request: id,
            command_id: invocation.command_id,
            outcome: Outcome::Unknown,
        }]
    );
    let r = resolves(&c.take_actions());
    assert_eq!(r.len(), 1);
    assert_eq!(c.state(id), Some(&RequestState::Resolving(c1)));
    match decode_stream(&r[0].2).unwrap().as_slice() {
        [MessageV1::ResolveRequest(rr)] => {
            assert_eq!(rr.retry_key, invocation.retry_key);
            assert_eq!(rr.command_id, invocation.command_id);
        }
        other => panic!("{other:?}"),
    }
    // Pending: still unknown, resolved again after the interval.
    c.on_frame(
        60,
        c1,
        &outcome_frame(invocation.command_id, OutcomeV1::Pending),
    )
    .unwrap();
    assert_eq!(c.state(id), Some(&RequestState::Unknown));
    assert!(c.take_completions().is_empty(), "no second unknown report");
    assert!(c.take_actions().is_empty());
    c.tick(160);
    assert_eq!(resolves(&c.take_actions()).len(), 1);
    // The identity resolves to the established outcome.
    c.on_frame(170, c1, &ok_frame(invocation.command_id, 7))
        .unwrap();
    assert_eq!(
        c.take_completions()[0].outcome,
        Outcome::Established {
            revision: Some(KvRevision::new(7).unwrap()),
            result: vec![0xab],
        }
    );
    // A server that lost the identity: unknown is final.
    let id2 = c.submit(200, &put(b"b", b"1"), 10).unwrap();
    let command2 = c.invocation(id2).unwrap().command_id;
    let _ = c.take_actions();
    c.tick(210);
    let _ = c.take_completions();
    let _ = c.take_actions();
    c.on_frame(211, c1, &outcome_frame(command2, OutcomeV1::Unknown))
        .unwrap();
    assert_eq!(c.take_completions()[0].outcome, Outcome::Unknown);
    assert_eq!(c.state(id2), Some(&RequestState::Done(Outcome::Unknown)));
    // A reset during resolution keeps resolving on the next connection.
    let id3 = c.submit(300, &put(b"c", b"1"), 10).unwrap();
    let _ = c.take_actions();
    c.tick(310);
    let _ = c.take_completions();
    let _ = c.take_actions();
    c.on_connection_lost(311, c1);
    assert_eq!(c.state(id3), Some(&RequestState::Unknown));
    let c2 = connected(&mut c, 312, 2);
    c.resolve(312, id3).unwrap();
    let r = resolves(&c.take_actions());
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].0, c2);
}

#[test]
fn stream_pressure_is_bounded_by_the_pool_and_the_window() {
    let config = ClientConfig {
        pool: PoolLimits {
            max_connections: 1,
            max_streams_per_connection: 2,
        },
        max_outstanding: 3,
        ..ClientConfig::default()
    };
    let mut c = client(config);
    let c1 = connected(&mut c, 0, 1);
    let a = c.submit(0, &put(b"a", b"1"), 0).unwrap();
    let b = c.submit(0, &put(b"b", b"1"), 0).unwrap();
    let d = c.submit(0, &put(b"d", b"1"), 0).unwrap();
    assert_eq!(
        sends(&c.take_actions()).len(),
        2,
        "two streams, the third waits"
    );
    assert_eq!(c.state(d), Some(&RequestState::Queued));
    assert!(c.pool().refused > 0);
    assert_eq!(
        c.submit(0, &put(b"e", b"1"), 0),
        Err(ClientError::Window { outstanding: 3 })
    );
    assert_eq!(
        c.connect(0, ConnectionId(2)),
        Err(ClientError::Pool(PoolError::TooManyConnections)),
        "the pool never grows past its cap"
    );
    let ca = c.invocation(a).unwrap().command_id;
    c.on_frame(1, c1, &ok_frame(ca, 1)).unwrap();
    assert_eq!(
        sends(&c.take_actions()).len(),
        1,
        "a freed stream carries the third"
    );
    assert_eq!(c.state(d), Some(&RequestState::InFlight(c1)));
    assert_eq!(c.pool().in_use(c1), 2);
    let _ = c.take_completions();
    let _ = b;
    // Forgetting a finished request frees its outstanding slot.
    assert!(matches!(c.forget(a), Some(Outcome::Established { .. })));
    assert!(c.submit(2, &put(b"e", b"1"), 0).is_ok());
    assert_eq!(c.outstanding(), 3);
}

#[test]
fn one_credential_exchange_serves_many_operations() {
    let config = ClientConfig {
        credential_margin: 0,
        pool: PoolLimits {
            max_connections: 3,
            max_streams_per_connection: 64,
        },
        ..ClientConfig::default()
    };
    let mut c = client(config);
    let c1 = ConnectionId(1);
    c.connect(0, c1).unwrap();
    let actions = c.take_actions();
    match actions.as_slice() {
        [SdkAction::Bind { credential, .. }] => {
            assert_eq!(credential.present(), b"secret-token");
            assert!(
                !format!("{credential:?}").contains("secret-token"),
                "redacted"
            );
        }
        other => panic!("{other:?}"),
    }
    c.bound(0, c1);
    assert_eq!(c.exchanges(), 1);
    for i in 1..=100u64 {
        let id = c.submit(i, &put(b"k", &i.to_be_bytes()), 0).unwrap();
        let command = c.invocation(id).unwrap().command_id;
        let sent = c.take_actions();
        assert!(
            matches!(sent.as_slice(), [SdkAction::Send { .. }]),
            "no bind per operation"
        );
        c.on_frame(i, c1, &ok_frame(command, i)).unwrap();
        c.forget(id);
    }
    assert_eq!(c.exchanges(), 1, "one exchange for a hundred operations");
    // A second warm connection reuses the cached credential.
    let c2 = ConnectionId(2);
    c.connect(500, c2).unwrap();
    assert!(matches!(
        c.take_actions().as_slice(),
        [SdkAction::Bind { .. }]
    ));
    assert_eq!(c.exchanges(), 1);
    // A rejected binding invalidates the cache: the next connect exchanges.
    c.binding_rejected(c2);
    c.connect(600, ConnectionId(3)).unwrap();
    assert_eq!(c.exchanges(), 2);
    assert_eq!(c.instance().state().next_sequence, 101);
}

#[test]
fn protocol_evidence_never_reaches_the_application() {
    let mut c = client(ClientConfig::default());
    let c1 = connected(&mut c, 0, 1);
    let id = c.submit(0, &put(b"a", b"1"), 0).unwrap();
    let _ = c.take_actions();
    let evidence = evidence_frame_from_bytes(&[0x01, 0x02, 0x03]).unwrap();
    assert_eq!(
        c.on_frame(1, c1, &evidence),
        Err(ClientError::ProtocolViolation)
    );
    let watch = MessageV1::WatchProgress(WatchProgressV1 {
        watch_id: 1,
        revision: KvRevision::new(1).unwrap(),
    })
    .encode()
    .unwrap();
    assert_eq!(
        c.on_frame(1, c1, &watch),
        Err(ClientError::ProtocolViolation)
    );
    assert_eq!(
        c.on_frame(
            1,
            c1,
            &ok_frame(CommandId(coord_types::identity::Digest32([9; 32])), 1)
        ),
        Err(ClientError::UnexpectedResponse)
    );
    assert_eq!(
        c.state(id),
        Some(&RequestState::InFlight(c1)),
        "nothing changed"
    );
    assert!(c.take_completions().is_empty());
}

#[test]
fn a_restored_instance_continues_its_sequence_and_never_reuses_one() {
    let mut inst = instance();
    let a = inst.allocate(&put(b"a", b"1"), 0).unwrap();
    let b = inst.allocate(&put(b"b", b"1"), 0).unwrap();
    let _ = inst.allocate(&put(b"c", b"1"), 0).unwrap();
    assert_eq!(a.retry_key.request_sequence.get(), 1);
    assert_eq!(b.retry_key.request_sequence.get(), 2);
    let persisted = postcard::to_allocvec(inst.state()).unwrap();
    let restored: coord_sdk::InstanceState = postcard::from_bytes(&persisted).unwrap();
    let mut inst = ClientInstance::restore(restored);
    let d = inst.allocate(&put(b"d", b"1"), 0).unwrap();
    assert_eq!(
        d.retry_key.request_sequence.get(),
        4,
        "continues, never reuses"
    );
    assert_eq!(inst.retry(2, &put(b"b", b"1"), 0).unwrap(), b);
    assert_eq!(
        inst.retry(2, &put(b"b", b"2"), 0),
        Err(InvocationError::PayloadConflict { sequence: 2 })
    );
    inst.retire(2);
    assert_eq!(
        inst.retry(2, &put(b"b", b"1"), 0),
        Err(InvocationError::UnknownSequence { sequence: 2 })
    );
    assert_eq!(
        inst.retry(9, &put(b"z", b"1"), 0),
        Err(InvocationError::UnknownSequence { sequence: 9 })
    );
    // A client over the restored instance submits sequence 5 next.
    let mut c = Client::new(ClientConfig::default(), inst, provider());
    let _ = connected(&mut c, 0, 1);
    let id = c.submit(0, &put(b"e", b"1"), 0).unwrap();
    assert_eq!(id, RequestId(5));
}

fn resets(actions: &[SdkAction]) -> Vec<(ConnectionId, RequestId)> {
    actions
        .iter()
        .filter_map(|a| match a {
            SdkAction::Reset {
                connection,
                request,
            } => Some((*connection, *request)),
            _ => None,
        })
        .collect()
}

#[test]
fn a_timed_out_stream_is_reset_before_its_credit_is_reused() {
    // Nothing closes the original stream when its deadline passes, so
    // handing its permit straight to the resolution put both on a
    // connection sized for one and repeated delayed responses defeated
    // the advertised stream bound.
    let mut c = client(ClientConfig {
        resolve_interval: 100,
        pool: PoolLimits {
            max_streams_per_connection: 1,
            ..PoolLimits::default()
        },
        ..ClientConfig::default()
    });
    let c1 = connected(&mut c, 0, 1);
    let id = c.submit(0, &put(b"a", b"1"), 50).unwrap();
    assert_eq!(sends(&c.take_actions()).len(), 1);
    c.tick(50);
    let actions = c.take_actions();
    // The reset comes with the resolution, naming the abandoned stream.
    assert_eq!(resets(&actions), vec![(c1, id)], "{actions:?}");
    assert_eq!(resolves(&actions).len(), 1);
    let order = actions
        .iter()
        .position(|a| matches!(a, SdkAction::Reset { .. }))
        .zip(
            actions
                .iter()
                .position(|a| matches!(a, SdkAction::Resolve { .. })),
        )
        .expect("both present");
    assert!(
        order.0 < order.1,
        "the reset precedes the reuse: {actions:?}"
    );
}

#[test]
fn a_queued_request_does_not_time_out_before_it_is_sent() {
    // The deadline counts from admission. Starting it while the request
    // was still queued turned one the pool never transmitted into an
    // unknown outcome and asked the endpoint to resolve an identity it
    // had never seen.
    let mut c = client(ClientConfig::default());
    // No connection: nothing can be sent.
    let id = c.submit(0, &put(b"a", b"1"), 50).unwrap();
    assert!(c.take_actions().is_empty(), "nothing was sent");
    c.tick(1_000);
    assert!(
        c.take_completions().is_empty(),
        "a queued request has no deadline yet"
    );
    assert!(c.take_actions().is_empty(), "and nothing to resolve");
    assert_eq!(c.state(id), Some(&RequestState::Queued));
    // Once it is admitted the clock starts, and only then.
    let _ = connected(&mut c, 1_000, 1);
    assert_eq!(sends(&c.take_actions()).len(), 1);
    c.tick(1_049);
    assert!(c.take_completions().is_empty());
    c.tick(1_050);
    assert_eq!(c.take_completions().len(), 1, "50ms after the send");
}

#[test]
fn a_retry_of_an_active_request_still_refuses_a_changed_payload() {
    // Returning early for a queued, in-flight or resolving sequence
    // skipped the local conflict check exactly where it matters most.
    let mut c = client(ClientConfig::default());
    let _ = connected(&mut c, 0, 1);
    let id = c.submit(0, &put(b"a", b"1"), 0).unwrap();
    assert_eq!(c.state(id), Some(&RequestState::InFlight(ConnectionId(1))));
    assert!(matches!(
        c.retry(1, id.0, &put(b"a", b"2"), 0),
        Err(ClientError::Invocation(InvocationError::PayloadConflict {
            sequence
        })) if sequence == id.0
    ));
    // The same payload is still accepted and changes nothing.
    assert_eq!(c.retry(1, id.0, &put(b"a", b"1"), 0).unwrap(), id);
    assert_eq!(c.state(id), Some(&RequestState::InFlight(ConnectionId(1))));
}

#[test]
fn a_retry_may_not_change_the_deadline_the_original_frame_carried() {
    // The deadline is in the frame but not in the command identity, so a
    // binding that recorded only the identity let a retry pass the
    // conflict check while sending different bytes and moving the
    // collector's deadline under the same invocation.
    let mut c = client(ClientConfig::default());
    let _ = connected(&mut c, 0, 1);
    let id = c.submit(0, &put(b"a", b"1"), 40).unwrap();
    let original = sends(&c.take_actions())[0].2.clone();
    assert!(matches!(
        c.retry(1, id.0, &put(b"a", b"1"), 90),
        Err(ClientError::Invocation(InvocationError::DeadlineConflict {
            sequence,
            bound: 40,
        })) if sequence == id.0
    ));
    // The original deadline is accepted, and a retry after a restart
    // rebuilds exactly the frame that was sent.
    let mut restored = Client::new(ClientConfig::default(), c.instance().clone(), provider());
    let _ = connected(&mut restored, 2, 1);
    let again = restored.retry(2, id.0, &put(b"a", b"1"), 40).unwrap();
    assert_eq!(again, id);
    assert_eq!(sends(&restored.take_actions())[0].2, original);
}

#[test]
fn finished_identity_bindings_do_not_accumulate_for_ever() {
    // A client in service cannot call `ClientInstance::retire` itself, so
    // a workload that submits, completes and forgets would grow the
    // binding map and the serialized instance state without bound while
    // keeping `outstanding()` small.
    let mut c = client(ClientConfig::default());
    let c1 = connected(&mut c, 0, 1);
    for i in 0..8u64 {
        let id = c.submit(i, &put(b"k", &[i as u8]), 0).unwrap();
        let command = c.invocation(id).unwrap().command_id;
        let _ = c.take_actions();
        c.on_frame(i, c1, &ok_frame(command, i + 1)).unwrap();
        let _ = c.take_completions();
        c.forget(id);
        assert_eq!(c.retired_through(), id.0, "retired as the prefix closes");
    }
    assert_eq!(c.outstanding(), 0);
    // A request forgotten while its outcome is unknown keeps its binding:
    // it may still be retried under its own identity.
    let pending = c.submit(100, &put(b"z", b"1"), 10).unwrap();
    let _ = c.take_actions();
    c.tick(110);
    let _ = c.take_completions();
    let _ = c.take_actions();
    c.forget(pending);
    assert_eq!(
        c.retired_through(),
        8,
        "the unknown request holds the floor where it is"
    );
}
