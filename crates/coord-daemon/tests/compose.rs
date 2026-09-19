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
domain = "d"
state_directory = "/s"

[listen]
admin_http = "127.0.0.1:1"

[capability]
writer_queue_bytes = 16777216
buffer_bytes_per_subscription = 8388608
max_live_subscriptions = 4096
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
