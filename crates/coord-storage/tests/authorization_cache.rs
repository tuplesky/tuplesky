//! The authorization contexts an applier keeps between commands (#185,
//! lead 15:27; `coord_storage::authorizations`).
//!
//! * A session's commands execute under one load of its context until a
//!   landing writes its session's record or a rule; another session's
//!   establishment leaves it (#279's review, debt 4).
//! * Within an execution group, a command reads a session the group's
//!   own earlier command established.
//! * A session executes in each namespace under that namespace's rules
//!   (#279's review, debt 1).
//! * A group the journal refuses, or a reconcile, empties the cache
//!   (#279's review, debt 2).
//! * Debug builds check every cached context against the view, so a
//!   write that goes around the applier is caught at the next command
//!   that uses the context, and `forget_authorizations` is what such a
//!   write calls.

mod support;

use coord_consensus::PayloadRecordV1;
use coord_core::capability::{
    AdmissionFacts, AttestedAdmission, AttestedEstablishment, CredentialDeadline,
};
use coord_core::effect::{BootId, PersistBatch, StoreUpdate};
use coord_core::outbox::BarrierAllocator;
use coord_journal_api::stream::ShardId;
use coord_state::policy::{Action, KeyInterval, PolicyRule};
use coord_state::{Outcome, Response};
use coord_storage::journaled::{JournalLimits, JournaledStore, TransitionKind};
use coord_storage::policy::{bootstrap_session, bootstrap_trust_rule, rule_update};
use coord_storage::{Applier, ApplyError, GroupLimits, JournaledDomain, Persistence, StoreWorker};
use coord_store_testkit::journal::{AppendScript, ModelJournal};
use coord_store_testkit::model::{CommitScript, ModelEngine};
use coord_types::identity::Digest32;
use coord_types::ids::*;
use coord_types::logical_v1::*;
use coord_types::{CommandId, RetryKey};
use support::Staged;

const NS: NamespaceId = NamespaceId([5; 16]);
const SESSION: SessionId = SessionId([3; 16]);
const OTHER: SessionId = SessionId([0x33; 16]);
const CLIENT: ClientInstanceId = ClientInstanceId([4; 16]);
const ALICE: PrincipalId = PrincipalId([0xa; 16]);
const RULE: TrustRuleId = TrustRuleId([0x7c; 16]);
const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);
const REPLICA: ReplicaId = ReplicaId([7; 16]);
const BOOT: BootId = BootId([1; 16]);

fn inc() -> ReplicaIncarnation {
    ReplicaIncarnation::new(1).unwrap()
}

fn key(session: SessionId, seq: u64) -> RetryKey {
    RetryKey {
        cluster_id: CLUSTER,
        domain_id: DOMAIN,
        session_id: session,
        client_instance_id: CLIENT,
        request_sequence: RequestSequence::new(seq).unwrap(),
    }
}

fn request(op: CanonicalOperation) -> LogicalRequest {
    request_in(NS, op)
}

fn request_in(namespace: NamespaceId, op: CanonicalOperation) -> LogicalRequest {
    let mut r = LogicalRequest::new(namespace, op);
    r.canonicalize();
    r
}

fn put_in(namespace: NamespaceId, k: &[u8]) -> LogicalRequest {
    request_in(
        namespace,
        CanonicalOperation::Put(PutOp {
            key: k.to_vec(),
            value: b"v".to_vec(),
            lease: None,
            prev_kv: false,
        }),
    )
}

/// An ordinary command under `session`.
fn command(session: SessionId, seq: u64, k: &[u8]) -> (CommandId, PayloadRecordV1) {
    command_in(NS, session, seq, k)
}

/// A put in `namespace` under `session`.
fn command_in(
    namespace: NamespaceId,
    session: SessionId,
    seq: u64,
    k: &[u8],
) -> (CommandId, PayloadRecordV1) {
    let request = put_in(namespace, k);
    (
        CommandId::derive(&key(session, seq), &request).unwrap(),
        PayloadRecordV1 {
            ack_through: 0,
            retry_key: key(session, seq),
            logical: postcard::to_allocvec(&request).unwrap(),
            admission: None,
        },
    )
}

/// The establishment of `session` for Alice under `RULE`: a landing
/// that writes a session row.
fn establishment(session: SessionId, receipt: u8) -> (CommandId, PayloadRecordV1) {
    let mut instance = [0u8; 16];
    instance.copy_from_slice(&[receipt; 16]);
    let key = RetryKey {
        cluster_id: CLUSTER,
        domain_id: DOMAIN,
        session_id: session,
        client_instance_id: ClientInstanceId(instance),
        request_sequence: RequestSequence::new(1).unwrap(),
    };
    let request = request(CanonicalOperation::ConsumeAdmission);
    let facts = AdmissionFacts {
        attested: AttestedAdmission {
            cluster: CLUSTER,
            domain: DOMAIN,
            session,
            rule_generation: 1,
            scope_ceiling: Action::FULL_CEILING,
            receipt_id: Digest32([receipt; 32]),
            admitted_at_ticks: 17,
        },
        establishing: Some(AttestedEstablishment {
            principal: ALICE,
            trust_rule: RULE,
            credential_valid_until: CredentialDeadline(1_000),
        }),
    };
    (
        CommandId::derive(&key, &request).unwrap(),
        PayloadRecordV1 {
            ack_through: 0,
            retry_key: key,
            logical: postcard::to_allocvec(&request).unwrap(),
            admission: Some(facts),
        },
    )
}

fn rule(id: u8, action: Action) -> StoreUpdate {
    rule_in(NS, id, action)
}

fn rule_in(namespace: NamespaceId, id: u8, action: Action) -> StoreUpdate {
    rule_update(
        &PolicyRuleId([id; 16]),
        &PolicyRule {
            principal: ALICE,
            action,
            namespace,
            interval: KeyInterval::all(),
        },
    )
    .unwrap()
}

/// Alice's session with every permission, and the trust rule her other
/// sessions are established under.
fn bootstrap() -> Vec<StoreUpdate> {
    let mut updates = bootstrap_session(&SESSION, ALICE, 1 << 20, true).unwrap();
    for (i, action) in Action::ALL.iter().enumerate() {
        updates.push(rule(i as u8 + 1, *action));
    }
    updates.push(bootstrap_trust_rule(&RULE).unwrap());
    updates
}

/// One worker; each command its own batch.
fn worker() -> Applier<StoreWorker<ModelEngine>> {
    let mut worker =
        StoreWorker::open(ModelEngine::new(), BOOT, inc(), GroupLimits::default()).unwrap();
    let mut alloc = BarrierAllocator::new(inc(), BOOT);
    worker
        .submit(PersistBatch {
            barrier: alloc.allocate(),
            base: Some(worker.application_base()),
            updates: bootstrap(),
        })
        .unwrap();
    worker.flush().unwrap();
    Applier::new(worker, alloc).unwrap()
}

/// One journal and one projection, which can group commands.
fn journaled() -> Applier<JournaledDomain<ModelJournal, ModelEngine>> {
    let mut store = JournaledStore::open(
        ModelJournal::new(),
        CLUSTER,
        REPLICA,
        inc(),
        BOOT,
        JournalLimits::default(),
    )
    .unwrap();
    store
        .attach(DOMAIN, ShardId::new(0).unwrap(), ModelEngine::new())
        .unwrap();
    let base = store.application_base(DOMAIN).unwrap();
    let mut domain = JournaledDomain::new(
        store,
        DOMAIN,
        Ballot {
            epoch: base.configuration,
            number: 0,
            leader: REPLICA,
        },
    )
    .expect("attached");
    let mut alloc = BarrierAllocator::new(inc(), BOOT);
    domain
        .submit(
            PersistBatch {
                barrier: alloc.allocate(),
                base: Some(base),
                updates: bootstrap(),
            },
            TransitionKind::Application {
                position: base.execution_position.checked_next().unwrap(),
                revision: None,
                result_digest: Digest32([0; 32]),
            },
        )
        .unwrap();
    domain.lower().unwrap();
    Applier::new(domain, alloc).unwrap()
}

fn outcome(response: &[u8]) -> Outcome {
    postcard::from_bytes::<Response>(response).unwrap().outcome
}

fn written(applied: &coord_consensus::AppliedOutcome) {
    assert!(
        matches!(outcome(&applied.response), Outcome::Put { .. }),
        "{:?}",
        outcome(&applied.response)
    );
}

#[test]
fn a_session_executes_under_one_load_until_a_landing_writes_its_rows() {
    let mut applier = worker();
    for seq in 1..=4 {
        let (id, payload) = command(SESSION, seq, &[seq as u8]);
        written(&applier.apply_staged(id, &payload).unwrap());
    }
    let stats = applier.authorization_cache();
    assert_eq!((stats.loads, stats.hits, stats.cleared), (1, 3, 0));

    // Establishing another session writes that session's row, which no
    // context of SESSION reads: SESSION's context stays, and OTHER's
    // first command loads its own (#279's review, debt 4).
    let (id, payload) = establishment(OTHER, 9);
    assert_eq!(
        outcome(&applier.apply_staged(id, &payload).unwrap().response),
        Outcome::SessionCreated { session: OTHER }
    );
    let stats = applier.authorization_cache();
    assert_eq!((stats.cleared, stats.forgotten), (0, 0));
    let (id, payload) = command(SESSION, 5, b"after");
    written(&applier.apply_staged(id, &payload).unwrap());
    let (id, payload) = command(OTHER, 1, b"other");
    written(&applier.apply_staged(id, &payload).unwrap());
    let (id, payload) = command(OTHER, 2, b"other again");
    written(&applier.apply_staged(id, &payload).unwrap());
    let stats = applier.authorization_cache();
    assert_eq!((stats.loads, stats.hits), (2, 5));

    // A rule written empties the cache: the next command loads again.
    write_around(&mut applier, rule(0x40, Action::Read));
    applier.forget_authorizations();
    assert_eq!(applier.authorization_cache().cleared, 1);
    let (id, payload) = command(SESSION, 6, b"after the rule");
    written(&applier.apply_staged(id, &payload).unwrap());
    assert_eq!(applier.authorization_cache().loads, 3);
}

#[test]
fn a_group_reads_the_session_its_own_earlier_command_established() {
    let mut applier = journaled();
    let items = [
        command(SESSION, 1, b"a"),
        establishment(OTHER, 9),
        command(SESSION, 2, b"b"),
        command(OTHER, 1, b"c"),
        command(OTHER, 2, b"d"),
    ];
    support::stage_all(applier.store_mut(), &items);
    assert!(applier.begin_group());
    let applied: Vec<_> = items
        .iter()
        .map(|(id, payload)| applier.apply(*id, payload).expect("applied"))
        .collect();
    applier.finish_group().expect("the group materializes");
    written(&applied[0]);
    assert_eq!(
        outcome(&applied[1].response),
        Outcome::SessionCreated { session: OTHER }
    );
    written(&applied[2]);
    written(&applied[3]);
    written(&applied[4]);
    // SESSION loaded and kept through OTHER's establishment, OTHER
    // loaded once over the group's own row and then kept.
    let stats = applier.authorization_cache();
    assert_eq!(
        (stats.loads, stats.hits, stats.cleared, stats.forgotten),
        (2, 2, 0, 0)
    );
}

/// One session in two namespaces whose rules differ: each command
/// executes under its own namespace's rules, whichever was loaded first
/// (#279's review, debt 1). The key holds the namespace.
#[test]
fn a_session_executes_in_each_namespace_under_that_namespaces_rules() {
    const ELSEWHERE: NamespaceId = NamespaceId([6; 16]);
    let mut applier = worker();
    // Alice may only read in ELSEWHERE.
    write_around(&mut applier, rule_in(ELSEWHERE, 0x50, Action::Read));
    applier.forget_authorizations();
    for round in 0..2u64 {
        let (id, payload) = command_in(NS, SESSION, 2 * round + 1, b"here");
        written(&applier.apply_staged(id, &payload).unwrap());
        let (id, payload) = command_in(ELSEWHERE, SESSION, 2 * round + 2, b"there");
        assert_eq!(
            outcome(&applier.apply_staged(id, &payload).unwrap().response),
            Outcome::ErrPermissionDenied,
            "round {round}"
        );
    }
    let stats = applier.authorization_cache();
    assert_eq!((stats.loads, stats.hits), (2, 2));
}

/// A group the journal refuses is lost, and the contexts its commands
/// loaded go with it (#279's review, debt 2): the cache does not rest on
/// the voter stopping after the loss.
#[test]
fn a_lost_group_empties_the_cache() {
    let mut applier = journaled();
    let items = [establishment(OTHER, 9), command(OTHER, 1, b"a")];
    support::stage_all(applier.store_mut(), &items);
    applier
        .store_mut()
        .store_mut()
        .journal_mut()
        .unwrap()
        .script_append(AppendScript::DefinitelyNotCommitted);
    assert!(applier.begin_group());
    for (id, payload) in &items {
        applier.apply(*id, payload).expect("applied");
    }
    let before = applier.authorization_cache();
    assert_eq!((before.loads, before.cleared), (1, 0));
    assert!(matches!(applier.finish_group(), Err(ApplyError::GroupLost)));
    assert_eq!(applier.authorization_cache().cleared, 1);
}

/// A commit whose outcome is unknown is settled by a reconcile, which
/// empties the cache: what the store settles may hold rows a context was
/// read over (#279's review, debt 2).
#[test]
fn a_reconcile_empties_the_cache() {
    let mut applier = worker();
    let (id, payload) = command(SESSION, 1, b"a");
    written(&applier.apply_staged(id, &payload).unwrap());
    assert_eq!(applier.authorization_cache().loads, 1);
    applier
        .store_mut()
        .engine_mut()
        .script_commit(CommitScript::Indeterminate { applied: true });
    let (id, payload) = command(SESSION, 2, b"b");
    written(&applier.apply_staged(id, &payload).unwrap());
    let stats = applier.authorization_cache();
    assert_eq!((stats.hits, stats.cleared), (1, 1));
    let (id, payload) = command(SESSION, 3, b"c");
    written(&applier.apply_staged(id, &payload).unwrap());
    assert_eq!(applier.authorization_cache().loads, 2);
}

/// A rule written around the applier, as a test's bootstrap or a tool
/// would, without saying so.
fn write_around(applier: &mut Applier<StoreWorker<ModelEngine>>, update: StoreUpdate) {
    let barrier = applier.alloc().allocate();
    let base = applier.store().application_base();
    let store = applier.store_mut();
    store
        .submit(PersistBatch {
            barrier,
            base: Some(base),
            updates: vec![update],
        })
        .unwrap();
    store.flush().unwrap();
}

#[test]
fn a_write_around_the_applier_that_says_so_is_read_by_the_next_command() {
    let mut applier = worker();
    let (id, payload) = command(SESSION, 1, b"a");
    written(&applier.apply_staged(id, &payload).unwrap());
    write_around(&mut applier, rule(0x40, Action::Read));
    applier.forget_authorizations();
    let (id, payload) = command(SESSION, 2, b"b");
    written(&applier.apply_staged(id, &payload).unwrap());
    assert_eq!(applier.authorization_cache().loads, 2);
}

/// The control for the rule above: without `forget_authorizations`, the
/// next command's context is the cached one, and the debug check says
/// it is not the view's.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "a cached authorization context differs from the view's")]
fn a_write_around_the_applier_that_does_not_say_so_is_caught() {
    let mut applier = worker();
    let (id, payload) = command(SESSION, 1, b"a");
    written(&applier.apply_staged(id, &payload).unwrap());
    write_around(&mut applier, rule(0x40, Action::Read));
    let (id, payload) = command(SESSION, 2, b"b");
    let _ = applier.apply_staged(id, &payload);
}

/// A session whose trust rule was disabled, regenerated or removed is
/// refused at execution, from the cached context as from its rows. The
/// applier takes the session's standing from the context its command
/// executes under, so the context has to judge the rule as
/// `retry::record_executable` does: a standing taken from the session
/// row alone admits the command, and the debug check says the context's
/// admission is not the rows'.
#[test]
fn a_session_its_trust_rule_no_longer_stands_behind_is_refused_from_its_context() {
    let trust_rule = |rule: Option<coord_state::policy::TrustRule>| StoreUpdate {
        collection: coord_store_api::registry::Collection::PolicyV1.id(),
        key: coord_storage::codecs::trust_rule_key(&RULE),
        value: rule.map(|rule| coord_storage::codecs::encode_trust_rule(&rule).unwrap()),
    };
    let changes = [
        (
            "disabled",
            trust_rule(Some(coord_state::policy::TrustRule {
                enabled: false,
                generation: 1,
            })),
        ),
        (
            "regenerated",
            trust_rule(Some(coord_state::policy::TrustRule {
                enabled: true,
                generation: 2,
            })),
        ),
        ("removed", trust_rule(None)),
    ];
    for (change, update) in changes {
        let mut applier = worker();
        let (id, payload) = establishment(OTHER, 9);
        assert_eq!(
            outcome(&applier.apply_staged(id, &payload).unwrap().response),
            Outcome::SessionCreated { session: OTHER }
        );
        let (id, payload) = command(OTHER, 1, b"a");
        written(&applier.apply_staged(id, &payload).unwrap());
        write_around(&mut applier, update);
        applier.forget_authorizations();
        let at = applier.authorization_cache();
        let before = applier.kv_revision().unwrap();
        for seq in [2, 3] {
            let (id, payload) = command(OTHER, seq, b"b");
            let applied = applier.apply_staged(id, &payload).unwrap();
            assert_eq!(applied.revision, None, "{change}: a refusal writes nothing");
        }
        assert_eq!(applier.kv_revision().unwrap(), before, "{change}");
        // The first refusal loads the context; the second is answered
        // from it.
        let stats = applier.authorization_cache();
        assert_eq!(
            (stats.loads - at.loads, stats.hits - at.hits),
            (1, 1),
            "{change}: {stats:?}"
        );
    }
}
