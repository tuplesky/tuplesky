//! task-43 acceptance for the reference preview composition: strict
//! configuration (unknown fields, capability covers limits, 0-RTT and
//! test bypasses refused), role wiring and listener requirements, the
//! lifecycle where cached leadership is not fresh-quorum readiness,
//! restart budgets that quarantine rather than spin, disk quarantine,
//! and secret-safe diagnostics.

use coord_daemon::config::CONFIG_VERSION;
use coord_daemon::lifecycle::QuarantineReason;
use coord_daemon::supervise::{Decision, WorkerError, WorkerId};
use coord_daemon::{
    Config, ConfigError, Diagnostics, Lifecycle, Phase, Readiness, ReadyGate, Redacted,
    RestartBudget, Role, RoleSet, Supervisor,
};
use coord_daemon::{ListenConfig, bind_listeners};

fn base_config(extra: &str) -> String {
    format!(
        r#"config_version = {CONFIG_VERSION}
role = "voter-frontend-observer"
cluster_manifest = "/etc/coord/genesis.json"
genesis_admin_key = "/etc/coord/genesis-admin.pem"
domain = "control-plane-a"
state_directory = "/var/lib/coord/a"
{extra}
[listen]
api_quic = "[::]:7443"
peer_quic = "[::]:7444"
admin_http = "127.0.0.1:7446"

[capability]
writer_queue_bytes = 16777216
buffer_bytes_per_subscription = 8388608
max_live_subscriptions = 4096

[state]
root = "state"

[journal]
root = "journal"
shards = 1

[identity]
trust_bundle = "/etc/coord/roots.pem"
node_certificate = "/etc/coord/node.pem"
node_key = "/etc/coord/node.key"

[sts]
issuer = "https://sts.example"
resource = "control-plane-a"
jwks = "/etc/coord/sts-jwks.json"
trust_rule = "7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c"
"#
    )
}

#[test]
fn strict_configuration_is_validated() {
    // A well-formed configuration.
    let config = Config::parse(&base_config("")).unwrap();
    assert_eq!(config.domain, "control-plane-a");
    let roles = config.role_set().unwrap();
    assert!(roles.votes());
    assert!(roles.needs_peer_listener() && roles.needs_api_listener());
    // An unknown field is rejected.
    assert!(matches!(
        Config::parse(&base_config("mystery_field = true\n")),
        Err(ConfigError::Parse(_))
    ));
    // The wrong schema version.
    let wrong = base_config("").replace(
        &format!("config_version = {CONFIG_VERSION}"),
        "config_version = 1",
    );
    assert!(matches!(
        Config::parse(&wrong),
        Err(ConfigError::UnsupportedVersion { version: 1 })
    ));
    // 0-RTT and test bypasses are refused (production graph).
    assert_eq!(
        Config::parse(&base_config("allow_application_0rtt = true\n")),
        Err(ConfigError::ZeroRttEnabled)
    );
    assert_eq!(
        Config::parse(&base_config("allow_test_bypasses = true\n")),
        Err(ConfigError::TestBypassEnabled)
    );
    // A missing listener for a role.
    let no_peer = base_config("").replace("peer_quic = \"[::]:7444\"\n", "");
    assert_eq!(
        Config::parse(&no_peer),
        Err(ConfigError::MissingListener("peer_quic"))
    );
    // The capability must cover the limits: shrink the writer queue below
    // one largest request.
    let small = base_config(
        "\n[limits]\nmax_request_bytes = 33554432\nmax_response_bytes = 8388608\nmax_outstanding_per_session = 256\nmax_live_subscriptions = 4096\n",
    );
    assert_eq!(
        Config::parse(&small),
        Err(ConfigError::CapabilityTooSmall("writer_queue_bytes"))
    );
    // An auth-broker-only process needs an HTTPS listener.
    let auth_only = r#"config_version = 2
role = "auth"
cluster_manifest = "/g.json"
genesis_admin_key = "/g.pem"
domain = "d"
state_directory = "/s"

[listen]
admin_http = "127.0.0.1:1"

[capability]
writer_queue_bytes = 16777216
buffer_bytes_per_subscription = 8388608
max_live_subscriptions = 4096

[state]
root = "state"

[journal]
root = "journal"

[identity]
trust_bundle = "/r.pem"
node_certificate = "/n.pem"
node_key = "/n.key"

[sts]
issuer = "https://sts.example"
resource = "d"
jwks = "/jwks.json"
trust_rule = "7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c"
"#;
    assert_eq!(
        Config::parse(auth_only),
        Err(ConfigError::MissingListener("https"))
    );
}

#[test]
fn role_parsing_and_listener_requirements() {
    assert!(RoleSet::parse("").is_err());
    assert!(RoleSet::parse("voter-voter").is_err());
    assert!(RoleSet::parse("wizard").is_err());
    let frontend = RoleSet::parse("frontend").unwrap();
    assert!(!frontend.votes());
    assert!(frontend.needs_api_listener() && !frontend.needs_peer_listener());
    let issuer = RoleSet::parse("issuer").unwrap();
    assert!(issuer.needs_https() && !issuer.needs_api_listener());
    assert!(Role::Voter.votes() && !Role::Observer.votes());
}

#[test]
fn cached_leadership_is_not_fresh_quorum_readiness() {
    let roles = RoleSet::parse("voter-frontend").unwrap();
    let gate = ReadyGate::new(roles.clone());
    // Everything up except fresh quorum: a voter is not ready even though
    // it believes it is the leader (cached leadership).
    let cached = Readiness {
        listeners_up: true,
        storage_ready: true,
        identity_ready: true,
        fresh_quorum: false,
        auth_ready: true,
    };
    assert!(!gate.ready(&cached), "cached leadership is not readiness");
    let fresh = Readiness {
        fresh_quorum: true,
        ..cached
    };
    assert!(gate.ready(&fresh));
    // A frontend-and-observer process (no vote) is ready on storage
    // alone, without fresh quorum of its own.
    let fo = ReadyGate::new(RoleSet::parse("frontend-observer").unwrap());
    assert!(fo.ready(&cached));

    // The lifecycle reflects it and never un-quarantines.
    let mut lifecycle = Lifecycle::new(roles);
    assert_eq!(lifecycle.phase(), Phase::Starting);
    lifecycle.observe(cached);
    assert_eq!(lifecycle.phase(), Phase::Live);
    assert!(!lifecycle.serving());
    lifecycle.observe(fresh);
    assert_eq!(lifecycle.phase(), Phase::Ready);
    assert!(lifecycle.serving());
    // A quorum loss drops readiness back to Live.
    lifecycle.observe(cached);
    assert_eq!(lifecycle.phase(), Phase::Live);
    // Drain then stop.
    lifecycle.observe(fresh);
    lifecycle.drain();
    assert_eq!(lifecycle.phase(), Phase::Draining);
    lifecycle.observe(fresh);
    assert_eq!(
        lifecycle.phase(),
        Phase::Draining,
        "drain does not resume serving"
    );
    lifecycle.stopped();
    assert_eq!(lifecycle.phase(), Phase::Stopped);
}

#[test]
fn disk_quarantine_stops_serving_and_never_recovers_in_place() {
    let roles = RoleSet::parse("voter").unwrap();
    let mut lifecycle = Lifecycle::new(roles);
    let fresh = Readiness {
        listeners_up: true,
        storage_ready: true,
        identity_ready: true,
        fresh_quorum: true,
        auth_ready: true,
    };
    lifecycle.observe(fresh);
    assert!(lifecycle.serving());
    // A disk fault quarantines; readiness observations no longer serve.
    lifecycle.quarantine(QuarantineReason::Disk);
    assert_eq!(
        lifecycle.phase(),
        Phase::Quarantined(QuarantineReason::Disk)
    );
    lifecycle.observe(fresh);
    assert_eq!(
        lifecycle.phase(),
        Phase::Quarantined(QuarantineReason::Disk),
        "a quarantined node does not resume in place"
    );
    assert!(!lifecycle.serving());
}

#[test]
fn worker_restarts_are_bounded_then_quarantine() {
    let mut sup = Supervisor::new(
        RestartBudget {
            max_restarts: 3,
            window_ticks: 1000,
        },
        10,
    );
    sup.register(WorkerId(1));
    // Transient failures restart with growing backoff until the budget is
    // spent, then quarantine.
    let mut backoffs = Vec::new();
    let mut now = 0;
    for _ in 0..3 {
        match sup.on_stop(WorkerId(1), now, &WorkerError::Transient("blip".into())) {
            Decision::Restart { after_ticks } => backoffs.push(after_ticks),
            other => panic!("{other:?}"),
        }
        now += 1;
    }
    assert_eq!(backoffs, vec![10, 20, 40], "growing backoff");
    assert!(matches!(
        sup.on_stop(WorkerId(1), now, &WorkerError::Transient("blip".into())),
        Decision::Quarantine { .. }
    ));
    // A fatal failure quarantines immediately, whatever the budget.
    sup.register(WorkerId(2));
    assert!(matches!(
        sup.on_stop(WorkerId(2), 0, &WorkerError::Fatal("corrupt".into())),
        Decision::Quarantine { .. }
    ));
    // Restarts outside the window are forgotten; a stable run resets.
    let mut sup = Supervisor::new(
        RestartBudget {
            max_restarts: 2,
            window_ticks: 100,
        },
        5,
    );
    sup.register(WorkerId(3));
    assert!(matches!(
        sup.on_stop(WorkerId(3), 0, &WorkerError::Transient("a".into())),
        Decision::Restart { .. }
    ));
    assert!(matches!(
        sup.on_stop(WorkerId(3), 500, &WorkerError::Transient("b".into())),
        Decision::Restart { .. }
    ));
    assert_eq!(
        sup.restarts(WorkerId(3)),
        1,
        "the first is outside the window"
    );
    sup.on_stable(WorkerId(3));
    assert_eq!(sup.restarts(WorkerId(3)), 0);
}

#[test]
fn diagnostics_are_secret_safe() {
    let secret = Redacted("super-secret-token".to_string());
    assert_eq!(format!("{secret:?}"), "<redacted>");
    assert_eq!(format!("{secret}"), "<redacted>");
    assert_eq!(secret.expose(), "super-secret-token");
    let roles = RoleSet::parse("voter-frontend").unwrap();
    let mut lifecycle = Lifecycle::new(roles.clone());
    lifecycle.observe(Readiness {
        listeners_up: true,
        storage_ready: true,
        identity_ready: true,
        fresh_quorum: true,
        auth_ready: true,
    });
    let diag = Diagnostics::snapshot(&roles, &lifecycle, 2);
    assert_eq!(diag.phase, "ready");
    assert!(diag.serving && diag.fresh_quorum);
    assert_eq!(diag.roles, vec![Role::Voter, Role::Frontend]);
    assert_eq!(diag.worker_restarts, 2);
    let rendered = format!("{diag:?}");
    assert!(!rendered.contains("secret"));
}

#[test]
fn a_required_listener_must_be_a_usable_address() {
    // Presence was all that was checked, so an empty or unparseable
    // address passed validation and failed later at bind, after the
    // process had reported its configuration good.
    for (address, ok) in [
        ("127.0.0.1:7000", true),
        ("[::]:7444", true),
        ("", false),
        ("   ", false),
        ("not-an-address", false),
        ("127.0.0.1", false),
        ("127.0.0.1:99999", false),
    ] {
        let text = base_config("").replace(
            "peer_quic = \"[::]:7444\"",
            &format!("peer_quic = \"{address}\""),
        );
        let parsed = Config::parse(&text);
        assert_eq!(
            parsed.is_ok(),
            ok,
            "{address:?} should {}validate: {parsed:?}",
            if ok { "" } else { "not " }
        );
        if !ok {
            assert!(matches!(
                parsed.unwrap_err(),
                ConfigError::InvalidListener("peer_quic")
            ));
        }
    }
}

#[test]
fn a_process_is_live_only_once_its_listeners_are_bound() {
    // Live means the listeners are up but the process is not serving.
    // Reporting it before they were up said the opposite of the truth.
    let roles = RoleSet::parse("voter").unwrap();
    let mut lifecycle = Lifecycle::new(roles);
    assert_eq!(lifecycle.phase(), Phase::Starting);
    lifecycle.observe(Readiness {
        storage_ready: true,
        identity_ready: true,
        ..Readiness::default()
    });
    assert_eq!(
        lifecycle.phase(),
        Phase::Starting,
        "storage open, nothing bound: still starting"
    );
    lifecycle.observe(Readiness {
        listeners_up: true,
        storage_ready: true,
        identity_ready: true,
        ..Readiness::default()
    });
    assert_eq!(lifecycle.phase(), Phase::Live);

    // Binding is real: a node holds the sockets it names, and what it
    // reports is what it actually bound.
    let listeners = bind_listeners(&ListenConfig {
        api_quic: Some("127.0.0.1:0".into()),
        peer_quic: Some("127.0.0.1:0".into()),
        admin_http: Some("127.0.0.1:0".into()),
        https: None,
    })
    .expect("loopback binds");
    let bound = listeners.addresses();
    assert_eq!(bound.len(), 3);
    assert!(bound.iter().all(|(_, a)| a.port() != 0), "{bound:?}");
    // A second bind of the same TCP address fails rather than being
    // reported as up.
    let (_, admin) = bound
        .iter()
        .find(|(name, _)| *name == "admin_http")
        .expect("admin bound");
    let clash = bind_listeners(&ListenConfig {
        api_quic: None,
        peer_quic: None,
        admin_http: Some(admin.to_string()),
        https: None,
    });
    assert!(clash.is_err(), "the address is already held");
}

/// The socket a process bound is the socket it serves on.
///
/// Binding is what decides whether a node can serve, and the `Live`
/// phase is defined by it, so it has to happen once. Handing the bound
/// socket to the QUIC endpoint is what makes the address an operator was
/// told about the address that answers: re-binding from that address
/// would either collide with the socket still held here, or leave the
/// port free for another process in between.
#[tokio::test(flavor = "multi_thread")]
async fn the_socket_a_process_bound_is_the_socket_it_serves_on() {
    use coord_transport::{Limits as TransportLimits, Transport};
    use coord_transport_testkit::{TestBinder, TestCa};
    use coord_types::ids::{ClusterId, DomainId, ReplicaId, ReplicaIncarnation};
    use coord_types::wire_v1::PeerRole;

    const CLUSTER: ClusterId = ClusterId([0x11; 16]);
    const DOMAIN: DomainId = DomainId([0x22; 16]);

    // Port zero, so the bound address is only knowable after binding.
    let mut bound = bind_listeners(&ListenConfig {
        api_quic: Some("127.0.0.1:0".into()),
        peer_quic: None,
        admin_http: None,
        https: None,
    })
    .expect("bind");
    let reported = bound
        .addresses()
        .into_iter()
        .find(|(name, _)| *name == "api_quic")
        .expect("an api listener was bound")
        .1;

    let socket = bound.take_api().expect("the api socket is handed over");
    assert!(
        bound.take_api().is_none(),
        "a socket is handed over once, not cloned"
    );

    let ca = TestCa::new();
    let frontend = ca.issue(
        "frontend.local",
        ReplicaId([1; 16]),
        ReplicaIncarnation::new(1).expect("positive"),
        PeerRole::Frontend,
    );
    let mut binder = TestBinder::new(CLUSTER, DOMAIN);
    binder.register(&frontend);
    let transport = Transport::with_socket(
        socket,
        frontend.local(&ca, CLUSTER, DOMAIN, vec![]),
        std::sync::Arc::new(binder),
        TransportLimits::default(),
    )
    .expect("serve on the handed-over socket");

    assert_eq!(
        transport.local_addr().expect("bound"),
        reported,
        "the endpoint serves on the address the process reported"
    );
    // And the port is not free in between: binding it again fails while
    // the endpoint holds it.
    assert!(
        std::net::UdpSocket::bind(reported).is_err(),
        "the port was released between binding and serving"
    );
}

/// A node opens durable state only under the engine and profile this
/// build implements. The name in the configuration is the name the
/// generation's own manifest must carry, so a mismatch is a different
/// store rather than a compatible one, and it is refused before anything
/// has been opened.
#[test]
fn durable_state_is_only_opened_under_a_name_this_build_serves() {
    use coord_daemon::config::{
        EXPERIMENTAL_ENGINE, JOURNAL_ENGINE, JOURNAL_PROFILE, STATE_ENGINE, STATE_PROFILE,
    };

    // Omitted engine and profile mean this build's own: a configuration
    // need not repeat what the binary can only do one way.
    let config = Config::parse(&base_config("")).unwrap();
    assert_eq!(config.state.engine, STATE_ENGINE);
    assert_eq!(config.state.profile, STATE_PROFILE);
    assert_eq!(config.journal.engine, JOURNAL_ENGINE);
    assert_eq!(config.journal.profile, JOURNAL_PROFILE);
    // Naming them explicitly is equally fine, and equally binding.
    let spelled = base_config("").replace(
        "[state]\nroot = \"state\"",
        &format!(
            "[state]\nengine = \"{STATE_ENGINE}\"\nprofile = \"{STATE_PROFILE}\"\nroot = \"state\""
        ),
    );
    assert_eq!(Config::parse(&spelled).unwrap().state, config.state);

    // Each of the four names is checked, and the refusal says which one
    // and what this build does serve -- an operator who mis-set a profile
    // should not have to guess which section was refused.
    for (find, replace, section, supported) in [
        (
            "[state]\nroot",
            "[state]\nengine = \"sled\"\nroot",
            "state",
            STATE_ENGINE,
        ),
        (
            "[state]\nroot",
            "[state]\nprofile = \"loose-v0\"\nroot",
            "state.profile",
            STATE_PROFILE,
        ),
        (
            "[journal]\nroot",
            "[journal]\nengine = \"append-file\"\nroot",
            "journal",
            JOURNAL_ENGINE,
        ),
        (
            "[journal]\nroot",
            "[journal]\nprofile = \"journaled-loose-v1\"\nroot",
            "journal.profile",
            JOURNAL_PROFILE,
        ),
    ] {
        let text = base_config("").replace(find, replace);
        let error = Config::parse(&text).unwrap_err();
        let ConfigError::UnsupportedEngine {
            section: refused,
            supported: serves,
            ..
        } = &error
        else {
            panic!("{section} should be refused as unsupported: {error:?}");
        };
        assert_eq!(*refused, section);
        assert_eq!(*serves, supported);
    }

    // The experimental engine is built and tested, so naming it is a
    // deliberate act rather than a typo. It is still refused, and it is
    // refused for the reason it actually is.
    let fjall = base_config("").replace(
        "[state]\nroot",
        &format!("[state]\nengine = \"{EXPERIMENTAL_ENGINE}\"\nroot"),
    );
    assert_eq!(
        Config::parse(&fjall),
        Err(ConfigError::ExperimentalEngine {
            named: EXPERIMENTAL_ENGINE.to_owned()
        })
    );
}

/// A node that journals nothing has no authoritative transition to apply
/// from, and a path that is empty is not a default: it resolves to the
/// working directory, which is where a process would quietly create a
/// second, empty generation beside the real one.
#[test]
fn a_configuration_that_could_not_find_its_own_state_is_refused() {
    let no_shards = base_config("").replace("shards = 1", "shards = 0");
    assert_eq!(Config::parse(&no_shards), Err(ConfigError::NoJournalShards));
    // Omitting the count entirely is the single-shard node, not zero.
    let default_shards = base_config("").replace("shards = 1\n", "");
    assert_eq!(Config::parse(&default_shards).unwrap().journal.shards, 1);

    for (find, replace, name) in [
        (
            "state_directory = \"/var/lib/coord/a\"",
            "state_directory = \"\"",
            "state_directory",
        ),
        (
            "[state]\nroot = \"state\"",
            "[state]\nroot = \"  \"",
            "state.root",
        ),
        (
            "[journal]\nroot = \"journal\"",
            "[journal]\nroot = \"\"",
            "journal.root",
        ),
        (
            "cluster_manifest = \"/etc/coord/genesis.json\"",
            "cluster_manifest = \"\"",
            "cluster_manifest",
        ),
        (
            "genesis_admin_key = \"/etc/coord/genesis-admin.pem\"",
            "genesis_admin_key = \"\"",
            "genesis_admin_key",
        ),
        (
            "trust_bundle = \"/etc/coord/roots.pem\"",
            "trust_bundle = \"\"",
            "identity.trust_bundle",
        ),
        (
            "node_certificate = \"/etc/coord/node.pem\"",
            "node_certificate = \"\"",
            "identity.node_certificate",
        ),
        (
            "node_key = \"/etc/coord/node.key\"",
            "node_key = \"\"",
            "identity.node_key",
        ),
    ] {
        let text = base_config("").replace(find, replace);
        assert_ne!(text, base_config(""), "{name}: the fixture did not change");
        assert_eq!(
            Config::parse(&text),
            Err(ConfigError::EmptyPath(name)),
            "{name}"
        );
    }

    // Credentials are paths, not material: a configuration that names
    // them is not itself a secret, and nothing here reads the files.
    let config = Config::parse(&base_config("")).unwrap();
    assert_eq!(config.identity.node_key, "/etc/coord/node.key");
}

/// The names in the configuration are the engine's own names.
///
/// A configuration says `engine = "redb"` and the generation's manifest
/// says `engine = "redb"`, and validation here is only worth anything if
/// those are the same string. They live in different crates, so nothing
/// but this makes them stay so: a rename in the engine that left this
/// constant behind would refuse every real store while still passing its
/// own tests.
#[test]
fn the_configured_state_engine_names_are_the_engine_crates_own() {
    use coord_daemon::config::{STATE_ENGINE, STATE_PROFILE};
    use coord_storage_redb::manifest::{ENGINE_NAME, PROFILE_NAME};

    assert_eq!(STATE_ENGINE, ENGINE_NAME);
    assert_eq!(STATE_PROFILE, PROFILE_NAME);
}

/// A relative projection root is relative to the node's state
/// directory; an absolute one is taken as given.
///
/// One setting moves a whole node, and a projection can still be put on
/// its own device without moving anything else. Resolving it in one
/// place is what stops two call sites disagreeing about which kind of
/// path a given string was -- and a disagreement there means two
/// generations of the same node's state, in two directories, both
/// looking correct.
#[test]
fn where_the_projection_lives_is_resolved_in_one_place() {
    let config = Config::parse(&base_config("")).unwrap();
    assert_eq!(
        config.state.root_path(&config.state_directory),
        std::path::Path::new("/var/lib/coord/a/state")
    );

    let absolute = base_config("").replace(
        "[state]\nroot = \"state\"",
        "[state]\nroot = \"/mnt/fast/projection\"",
    );
    let absolute = Config::parse(&absolute).unwrap();
    assert_eq!(
        absolute.state.root_path(&absolute.state_directory),
        std::path::Path::new("/mnt/fast/projection"),
        "an absolute root is not joined onto the state directory"
    );
    // A nested relative root stays under the state directory rather than
    // escaping it by accident.
    let nested = base_config("").replace("root = \"state\"", "root = \"projection/current\"");
    let nested = Config::parse(&nested).unwrap();
    assert!(
        nested
            .state
            .root_path(&nested.state_directory)
            .starts_with("/var/lib/coord/a")
    );
}

/// A process that serves clients verifies their tokens, so it is
/// configured with the keys to verify them against or it does not start.
///
/// Starting without them would mean a frontend that binds its listener
/// and then refuses every caller -- which reads as a client problem, and
/// is the most expensive kind of misconfiguration to diagnose because
/// the node looks healthy.
///
/// A process with no API listener needs none of it: the auth broker and
/// the node issuer establish credentials rather than consume them, so
/// requiring an issuer of them would be circular.
/// An identity in the configuration is an identity, or the node does not
/// start.
///
/// The trust rule the issuer signs under and the principals a domain's
/// genesis grants are row keys of replicated state, written by every
/// replica from this file. A spelling that differed between nodes --
/// upper case here, short there -- would be a different row on each,
/// and the divergence would show up as a session that exists on some
/// replicas and not others rather than as a bad configuration.
#[test]
fn an_identity_in_the_configuration_is_exactly_one_identity() {
    let with = |field: &str, value: &str| {
        base_config("").replace(
            "trust_rule = \"7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c\"",
            &format!("trust_rule = \"7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c\"\n\n[[grant]]\nprincipal = \"0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a\"\nnamespace = \"{value}\"\n{field}"),
        )
    };
    // A well-formed grant.
    assert!(Config::parse(&with("", "5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e")).is_ok());
    // Too short.
    assert_eq!(
        Config::parse(&with("", "5e5e")),
        Err(ConfigError::NotAnIdentity("grant.namespace"))
    );
    // Upper case is a second spelling of the same bytes, and a second
    // spelling is what this refuses.
    assert_eq!(
        Config::parse(&with("", "5E5E5E5E5E5E5E5E5E5E5E5E5E5E5E5E")),
        Err(ConfigError::NotAnIdentity("grant.namespace"))
    );
    // And the issuer's own rule.
    let wrong = base_config("").replace(
        "trust_rule = \"7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c\"",
        "trust_rule = \"not-an-identity\"",
    );
    assert_eq!(
        Config::parse(&wrong),
        Err(ConfigError::NotAnIdentity("sts.trust_rule"))
    );
}

#[test]
fn a_process_that_serves_clients_is_configured_to_verify_them() {
    let config = Config::parse(&base_config("")).unwrap();
    let sts = config.sts.as_ref().expect("the fixture serves clients");
    assert_eq!(sts.issuer, "https://sts.example");

    let without = base_config("").replace(
        "\n[sts]\nissuer = \"https://sts.example\"\nresource = \"control-plane-a\"\njwks = \"/etc/coord/sts-jwks.json\"\ntrust_rule = \"7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c\"\n",
        "",
    );
    assert_ne!(without, base_config(""), "the fixture did not change");
    assert_eq!(
        Config::parse(&without),
        Err(ConfigError::MissingSection {
            section: "sts",
            needed_by: "a process that serves clients",
        })
    );

    // The auth broker issues the tokens; it does not verify them against
    // an issuer of its own.
    let broker = r#"config_version = 2
role = "auth"
cluster_manifest = "/g.json"
genesis_admin_key = "/g.pem"
domain = "d"
state_directory = "/s"

[listen]
https = "127.0.0.1:8443"

[capability]
writer_queue_bytes = 16777216
buffer_bytes_per_subscription = 8388608
max_live_subscriptions = 4096

[state]
root = "state"

[journal]
root = "journal"

[identity]
trust_bundle = "/r.pem"
node_certificate = "/n.pem"
node_key = "/n.key"
"#;
    assert!(
        Config::parse(broker).is_ok(),
        "a process that issues credentials was made to consume them: {:?}",
        Config::parse(broker)
    );

    // The paths it names are paths like any other: an empty one is the
    // working directory, not a default.
    for (find, replace, name) in [
        (
            "issuer = \"https://sts.example\"",
            "issuer = \"\"",
            "sts.issuer",
        ),
        (
            "resource = \"control-plane-a\"",
            "resource = \"  \"",
            "sts.resource",
        ),
        (
            "jwks = \"/etc/coord/sts-jwks.json\"",
            "jwks = \"\"",
            "sts.jwks",
        ),
    ] {
        let text = base_config("").replace(find, replace);
        assert_eq!(
            Config::parse(&text),
            Err(ConfigError::EmptyPath(name)),
            "{name}"
        );
    }
}

/// Renewal is optional, and where it is configured the issuer it names
/// is one this node may take its credential from (task-d02).
#[test]
fn a_renewal_issuer_is_https_or_an_allowed_loopback_address() {
    let renewal = |body: &str| base_config(&format!("\n[renewal]\n{body}\n"));
    // Absent: nothing renews, and nothing is required.
    assert_eq!(Config::parse(&base_config("")).unwrap().renewal, None);
    let config = Config::parse(&renewal(
        "issuer = \"https://issuer.example:8443\"\nassertion = \"/var/run/secrets/token\"\nlifetime_secs = 86400",
    ))
    .unwrap();
    let section = config.renewal.expect("configured");
    assert_eq!(section.jitter_secs, 3600, "the default spread");
    assert_eq!(section.lifetime_secs, 86400);
    // Plain HTTP is refused, loopback included, unless explicitly allowed
    // -- and then only on loopback, decided by parsing the host rather
    // than by a prefix of the string.
    for (url, allow, ok) in [
        ("http://127.0.0.1:9000", false, false),
        ("http://127.0.0.1:9000", true, true),
        ("http://[::1]:9000", true, true),
        ("http://localhost:9000", true, true),
        ("http://127.0.0.1.evil.example", true, false),
        ("http://issuer.example", true, false),
        ("https://user:secret@issuer.example", false, false),
        ("https://issuer.example/?next=x", false, false),
        ("ftp://issuer.example", true, false),
        // A host and port the HTTP client would refuse, for either scheme.
        ("https://issuer.example:not-a-port", false, false),
        ("https://issuer.example:", false, false),
        ("https://issuer.example:0", false, false),
        ("https://issuer.example:65536", false, false),
        ("https://issuer.example:8443:1", false, false),
        ("https://:8443", false, false),
        ("https://[::1", false, false),
        ("https://[not-v6]:8443", false, false),
        ("https://[::1]x", false, false),
        ("https://issuer_example", false, false),
        ("https://-issuer.example", false, false),
        ("https://999.0.0.1", false, false),
        ("http://127.0.0.1:x", true, false),
        ("https://[2001:db8::1]:8443", false, true),
        ("https://192.0.2.1", false, true),
        ("https://issuer.example.:8443/base/", false, true),
        ("http://LOCALHOST:9000", true, true),
    ] {
        let parsed = Config::parse(&renewal(&format!(
            "issuer = \"{url}\"\nassertion = \"/t\"\nlifetime_secs = 60\nallow_insecure_loopback = {allow}"
        )));
        // The switch itself is test-only: a release build refuses it set,
        // before the URL is looked at (the unit test in `config.rs` asks
        // both builds).
        if allow && !cfg!(debug_assertions) {
            assert_eq!(
                parsed,
                Err(ConfigError::TestOnlySwitch(
                    "renewal.allow_insecure_loopback"
                )),
                "{url}"
            );
            continue;
        }
        assert_eq!(
            parsed.is_ok(),
            ok,
            "{url} allow={allow}: {:?}",
            parsed.err()
        );
        if !ok {
            assert_eq!(parsed, Err(ConfigError::InsecureIssuer), "{url}");
        }
    }
    // A lifetime of nothing, none at all, an empty path, and an unknown
    // key are refused.
    assert!(matches!(
        Config::parse(&renewal(
            "issuer = \"https://i.example\"\nassertion = \"/t\""
        )),
        Err(ConfigError::Parse(_))
    ));
    assert_eq!(
        Config::parse(&renewal(
            "issuer = \"https://i.example\"\nassertion = \"/t\"\nlifetime_secs = 0"
        )),
        Err(ConfigError::ZeroLifetime)
    );
    assert_eq!(
        Config::parse(&renewal(
            "issuer = \"https://i.example\"\nassertion = \" \"\nlifetime_secs = 60"
        )),
        Err(ConfigError::EmptyPath("renewal.assertion"))
    );
    assert!(matches!(
        Config::parse(&renewal(
            "issuer = \"https://i.example\"\nassertion = \"/t\"\nlifetime_secs = 60\nextend_deadline = true"
        )),
        Err(ConfigError::Parse(_))
    ));
}
