//! task-36 acceptance (sans-I/O): the exchange creates the session
//! atomically before signing, execution rechecks policy changed after
//! verification, outages and stale keys fail closed, lifetime and scope
//! ceilings hold, receipts are single use, keys rotate with overlap and
//! neither tokens nor private keys reach storage or logs.

mod common;

use common::*;
use coord_authn::{ClockHealth, KubernetesMode, TimeError};
use coord_state::plan::Outcome;
use coord_state::policy::Action;
use coord_sts::{
    ExchangeError, ExchangeForm, GRANT_TYPE, KeyError, TOKEN_TYPE_ACCESS, TOKEN_TYPE_JWT,
    TokenError, scope_string, verify_service_token,
};
use coord_types::ids::SessionId;
use serde_json::Value;

const NOW: u64 = 1_700_000_000;

fn form(token: &str) -> ExchangeForm {
    ExchangeForm {
        grant_type: GRANT_TYPE.into(),
        subject_token: token.into(),
        subject_token_type: TOKEN_TYPE_JWT.into(),
        audience: None,
        resource: Some(RESOURCE.into()),
        scope: None,
        requested_token_type: Some(TOKEN_TYPE_ACCESS.into()),
    }
}

fn clock() -> ClockHealth {
    ClockHealth::healthy(NOW, 5)
}

#[test]
fn the_exchange_creates_the_session_atomically_before_signing() {
    let k8s = k8s_issuer();
    let mut sts = sts(&k8s, "https://k8s/keys", KubernetesMode::Offline, 3600);
    sts.verifier_mut()
        .registry_mut()
        .install_keys("k8s", &k8s.jwks, NOW)
        .unwrap();
    let mut domain = Domain::new();
    let token = assertion(&k8s, NOW, NOW + 600);
    let entropy = [0x11u8; 32];
    let response = sts
        .exchange(&form(&token), &clock(), &entropy, None, &mut domain)
        .unwrap();
    assert_eq!(response.token_type, "Bearer");
    assert_eq!(response.issued_token_type, TOKEN_TYPE_ACCESS);
    assert_eq!(
        response.expires_in, 300,
        "the rule's lifetime caps the assertion's"
    );
    assert_eq!(response.scope, "read write delete");
    // The service token verifies against the published keys and is bound
    // to the created session and its principal.
    let claims = verify_service_token(
        &response.access_token,
        &sts.ring().jwks(),
        "https://sts.cluster-1",
        RESOURCE,
        &clock(),
    )
    .unwrap();
    assert_eq!(claims.sid, "11".repeat(16));
    assert_eq!(claims.sub, "07".repeat(16));
    assert_eq!(
        claims.scope,
        Action::Read.bit() | Action::Write.bit() | Action::Delete.bit()
    );
    assert_eq!(claims.exp, NOW + 300);
    assert_eq!(claims.generation, 3);
    // Exactly one replicated command carried the receipt, never the
    // assertion or the token.
    assert_eq!(domain.applied.len(), 1);
    let rendered = format!("{:?}", domain.applied[0]);
    assert!(!rendered.contains(&token));
    assert!(!rendered.contains(&response.access_token));
    assert!(rendered.contains("ConsumeAdmission"));
    // The session exists in replicated state.
    let retired = domain.apply(&coord_state::InternalCommand::RetireSession {
        namespace: NS,
        session: SessionId([0x11; 16]),
    });
    assert_eq!(retired.outcome, Outcome::SessionRetired);
    // The receipt is single use: replaying the same command is refused.
    let replay = domain.applied[0].clone();
    assert_eq!(domain.apply(&replay).outcome, Outcome::ErrReceiptConsumed);
    // The log records the receipt, not the token.
    let log = serde_json::to_string(&sts.log().records().collect::<Vec<_>>()).unwrap();
    assert!(log.contains("system:serviceaccount:prod:kine"));
    assert!(!log.contains(&token) && !log.contains(&response.access_token));
    assert_eq!(sts.issued, 1);
    // A second exchange with fresh entropy creates a second session; the
    // same entropy (a replayed session identity) is refused by state.
    let response2 = sts
        .exchange(&form(&token), &clock(), &[0x22u8; 32], None, &mut domain)
        .unwrap();
    assert_ne!(response2.access_token, response.access_token);
    assert_eq!(
        sts.exchange(&form(&token), &clock(), &[0x22u8; 32], None, &mut domain),
        Err(ExchangeError::InvalidGrant("receipt already consumed"))
    );
    assert_eq!(sts.issued, 2);
}

#[test]
fn execution_rechecks_policy_changed_after_verification() {
    let k8s = k8s_issuer();
    let mut sts = sts(&k8s, "https://k8s/keys", KubernetesMode::Offline, 3600);
    sts.verifier_mut()
        .registry_mut()
        .install_keys("k8s", &k8s.jwks, NOW)
        .unwrap();
    let mut domain = Domain::new();
    domain.disable_rule_before_consume = true;
    let token = assertion(&k8s, NOW, NOW + 600);
    assert_eq!(
        sts.exchange(&form(&token), &clock(), &[1u8; 32], None, &mut domain),
        Err(ExchangeError::InvalidGrant(
            "policy changed since verification"
        ))
    );
    assert_eq!(sts.issued, 0);
    assert!(sts.log().records().next().is_none(), "nothing admitted");
    // A rule generation the verifier does not know is equally refused.
    let mut domain = Domain::new();
    sts.set_rules(vec![rule(4, 300)]);
    assert_eq!(
        sts.exchange(&form(&token), &clock(), &[1u8; 32], None, &mut domain),
        Err(ExchangeError::InvalidGrant(
            "policy changed since verification"
        ))
    );
    assert_eq!(domain.applied.len(), 1, "state decided, with the receipt");
}

#[test]
fn outages_and_stale_keys_fail_closed() {
    let k8s = k8s_issuer();
    let token = assertion(&k8s, NOW, NOW + 600);
    // No keys yet: the HTTP layer fetches the configured endpoint once;
    // here it is an explicit, typed condition.
    let mut sts = sts(&k8s, "https://k8s/keys", KubernetesMode::Offline, 3600);
    let mut domain = Domain::new();
    assert_eq!(
        sts.exchange(&form(&token), &clock(), &[1u8; 32], None, &mut domain),
        Err(ExchangeError::KeysUnavailable {
            name: "k8s".into(),
            jwks_url: "https://k8s/keys".into()
        })
    );
    // Stale keys deny.
    sts.verifier_mut()
        .registry_mut()
        .install_keys("k8s", &k8s.jwks, NOW - 100_000)
        .unwrap();
    let e = sts.exchange(&form(&token), &clock(), &[1u8; 32], None, &mut domain);
    assert!(
        matches!(
            e,
            Err(ExchangeError::KeysUnavailable { .. })
                | Err(ExchangeError::Unavailable("issuer keys stale"))
        ),
        "{e:?}"
    );
    // Replicated state down: nothing is issued.
    sts.verifier_mut()
        .registry_mut()
        .install_keys("k8s", &k8s.jwks, NOW)
        .unwrap();
    domain.down = true;
    assert_eq!(
        sts.exchange(&form(&token), &clock(), &[1u8; 32], None, &mut domain),
        Err(ExchangeError::Unavailable("replicated state"))
    );
    domain.down = false;
    // An unhealthy clock cannot establish validity.
    let sick = ClockHealth {
        now: NOW,
        uncertainty: 5,
        healthy: false,
    };
    assert_eq!(
        sts.exchange(&form(&token), &sick, &[1u8; 32], None, &mut domain),
        Err(ExchangeError::Unavailable("clock health"))
    );
    // TokenReview mode without a review result denies.
    let mut reviewed = common::sts(&k8s, "https://k8s/keys", KubernetesMode::TokenReview, 3600);
    reviewed
        .verifier_mut()
        .registry_mut()
        .install_keys("k8s", &k8s.jwks, NOW)
        .unwrap();
    assert_eq!(
        reviewed.exchange(&form(&token), &clock(), &[1u8; 32], None, &mut domain),
        Err(ExchangeError::Unavailable("token review"))
    );
    // An expired assertion is a grant failure, not an outage.
    let old = assertion(&k8s, NOW - 1000, NOW - 1);
    assert_eq!(
        sts.exchange(&form(&old), &clock(), &[1u8; 32], None, &mut domain),
        Err(ExchangeError::InvalidGrant("assertion rejected"))
    );
    assert_eq!(sts.issued, 0);
    let _ = TimeError::Expired;
}

#[test]
fn lifetime_and_scope_ceilings_hold() {
    let k8s = k8s_issuer();
    let mut domain = Domain::new();
    // The STS bound is tighter than the rule's.
    let mut sts = sts(&k8s, "https://k8s/keys", KubernetesMode::Offline, 100);
    sts.verifier_mut()
        .registry_mut()
        .install_keys("k8s", &k8s.jwks, NOW)
        .unwrap();
    let token = assertion(&k8s, NOW, NOW + 600);
    let r = sts
        .exchange(&form(&token), &clock(), &[1u8; 32], None, &mut domain)
        .unwrap();
    assert_eq!(r.expires_in, 100);
    // The assertion's remaining validity (under uncertainty) is tighter
    // than both.
    let short = assertion(&k8s, NOW, NOW + 40);
    let r = sts
        .exchange(&form(&short), &clock(), &[2u8; 32], None, &mut domain)
        .unwrap();
    assert_eq!(r.expires_in, 35);
    // Scope narrows within the ceiling; outside it is refused.
    let mut f = form(&token);
    f.scope = Some("read".into());
    let r = sts
        .exchange(&f, &clock(), &[3u8; 32], None, &mut domain)
        .unwrap();
    assert_eq!(r.scope, "read");
    let claims = verify_service_token(
        &r.access_token,
        &sts.ring().jwks(),
        "https://sts.cluster-1",
        RESOURCE,
        &clock(),
    )
    .unwrap();
    assert_eq!(claims.scope, Action::Read.bit());
    f.scope = Some("read compact".into());
    assert_eq!(
        sts.exchange(&f, &clock(), &[4u8; 32], None, &mut domain),
        Err(ExchangeError::InvalidScope)
    );
    f.scope = Some("admin".into());
    assert_eq!(
        sts.exchange(&f, &clock(), &[4u8; 32], None, &mut domain),
        Err(ExchangeError::InvalidScope)
    );
    assert_eq!(
        scope_string(Action::Compact.bit() | Action::Read.bit()),
        "read compact"
    );
    // Targets and request shape.
    let mut f = form(&token);
    f.resource = Some("tuplesky://other".into());
    assert_eq!(
        sts.exchange(&f, &clock(), &[5u8; 32], None, &mut domain),
        Err(ExchangeError::InvalidTarget)
    );
    f.resource = None;
    f.audience = Some(RESOURCE.into());
    assert!(
        sts.exchange(&f, &clock(), &[6u8; 32], None, &mut domain)
            .is_ok()
    );
    let mut f = form(&token);
    f.grant_type = "client_credentials".into();
    assert_eq!(
        sts.exchange(&f, &clock(), &[7u8; 32], None, &mut domain),
        Err(ExchangeError::InvalidRequest("unsupported grant_type"))
    );
    let mut f = form(&token);
    f.subject_token = "x".repeat(9000);
    assert_eq!(
        sts.exchange(&f, &clock(), &[8u8; 32], None, &mut domain),
        Err(ExchangeError::InvalidRequest("subject_token size"))
    );
    // Verification of the issued token is exact about issuer, audience
    // and time.
    assert_eq!(
        verify_service_token(
            &r.access_token,
            &sts.ring().jwks(),
            "https://other",
            RESOURCE,
            &clock()
        ),
        Err(TokenError::Invalid)
    );
    assert_eq!(
        verify_service_token(
            &r.access_token,
            &sts.ring().jwks(),
            "https://sts.cluster-1",
            "x",
            &clock()
        ),
        Err(TokenError::Invalid)
    );
    assert_eq!(
        verify_service_token(
            &r.access_token,
            &sts.ring().jwks(),
            "https://sts.cluster-1",
            RESOURCE,
            &ClockHealth::healthy(NOW + 100, 5)
        ),
        Err(TokenError::Time(TimeError::Expired))
    );
}

#[test]
fn keys_rotate_with_overlap_and_never_leave_the_process() {
    let k8s = k8s_issuer();
    let mut domain = Domain::new();
    let mut sts = sts(&k8s, "https://k8s/keys", KubernetesMode::Offline, 3600);
    sts.verifier_mut()
        .registry_mut()
        .install_keys("k8s", &k8s.jwks, NOW)
        .unwrap();
    let token = assertion(&k8s, NOW, NOW + 600);
    let first = sts
        .exchange(&form(&token), &clock(), &[1u8; 32], None, &mut domain)
        .unwrap();
    sts.ring_mut().rotate(signing_key("sts-2")).unwrap();
    assert_eq!(sts.ring().active(), "sts-2");
    assert_eq!(sts.ring().kids(), vec!["sts-1", "sts-2"]);
    let second = sts
        .exchange(&form(&token), &clock(), &[2u8; 32], None, &mut domain)
        .unwrap();
    let jwks = sts.ring().jwks();
    for t in [&first.access_token, &second.access_token] {
        verify_service_token(t, &jwks, "https://sts.cluster-1", RESOURCE, &clock()).unwrap();
    }
    // The JWKS is public material only.
    let rendered = serde_json::to_string(&jwks).unwrap();
    assert!(!rendered.contains("\"d\""));
    assert_eq!(jwks["keys"].as_array().unwrap().len(), 2);
    for k in jwks["keys"].as_array().unwrap() {
        assert!(k.get("d").is_none());
        assert_eq!(k["kty"], Value::from("EC"));
    }
    // Debug output redacts private material and the form redacts tokens.
    let dbg = format!("{:?}", sts.ring());
    assert!(dbg.contains("<redacted>") && !dbg.contains("\"d\""));
    let f = format!("{:?}", form(&token));
    assert!(f.contains("<redacted>") && !f.contains(&token));
    // Retiring the old key ends the overlap; the active key cannot be
    // retired; duplicate identifiers are refused.
    assert_eq!(sts.ring_mut().retire("sts-2"), Err(KeyError::ActiveKey));
    sts.ring_mut().retire("sts-1").unwrap();
    assert_eq!(
        verify_service_token(
            &first.access_token,
            &sts.ring().jwks(),
            "https://sts.cluster-1",
            RESOURCE,
            &clock()
        ),
        Err(TokenError::UnknownKey)
    );
    verify_service_token(
        &second.access_token,
        &sts.ring().jwks(),
        "https://sts.cluster-1",
        RESOURCE,
        &clock(),
    )
    .unwrap();
    assert_eq!(
        sts.ring_mut().rotate(signing_key("sts-2")),
        Err(KeyError::DuplicateKid)
    );
    assert_eq!(sts.ring_mut().retire("nope"), Err(KeyError::UnknownKid));
    // A key that is not P-256 PKCS#8 is refused.
    assert_eq!(
        coord_sts::SigningKey::from_pkcs8_der("bad", b"not a key").err(),
        Some(KeyError::InvalidKey)
    );
}

#[test]
fn a_downscoped_token_creates_a_session_at_the_scope_it_was_granted() {
    // The narrower scope was signed into the token while the replicated
    // session was created at the rule's ceiling. Execution reconstructs
    // authorization from the session, so an operation presented through a
    // read-only token would have run under a read/write session and the
    // narrowing would have existed only in the token.
    let k8s = k8s_issuer();
    let mut domain = Domain::new();
    let mut sts = sts(&k8s, "https://k8s/keys", KubernetesMode::Offline, 3600);
    sts.verifier_mut()
        .registry_mut()
        .install_keys("k8s", &k8s.jwks, NOW)
        .unwrap();
    let token = assertion(&k8s, NOW, NOW + 600);
    let mut f = form(&token);
    f.scope = Some("read".into());
    let r = sts
        .exchange(&f, &clock(), &[9u8; 32], None, &mut domain)
        .unwrap();
    assert_eq!(r.scope, "read");
    let ceiling = match domain.applied.last().expect("one command") {
        coord_state::InternalCommand::ConsumeAdmission { receipt, .. } => receipt.scope_ceiling,
        other => panic!("{other:?}"),
    };
    assert_eq!(
        ceiling,
        Action::Read.bit(),
        "the session is created at the granted scope, not the rule's ceiling"
    );
    // Asking for nothing still gets the rule's ceiling, in both places.
    let mut wide = form(&token);
    wide.scope = None;
    let r = sts
        .exchange(&wide, &clock(), &[10u8; 32], None, &mut domain)
        .unwrap();
    assert_eq!(r.scope, "read write delete");
    let ceiling = match domain.applied.last().expect("a second command") {
        coord_state::InternalCommand::ConsumeAdmission { receipt, .. } => receipt.scope_ceiling,
        other => panic!("{other:?}"),
    };
    assert_eq!(
        ceiling,
        Action::Read.bit() | Action::Write.bit() | Action::Delete.bit()
    );
}

#[test]
fn a_renewal_never_outlives_the_session_it_renews() {
    // A renewal signs a fresh token inside an existing session. Taking
    // only the STS lifetime let a session admitted on a short-lived
    // credential be renewed indefinitely, outliving that credential by
    // any amount.
    let k8s = k8s_issuer();
    let mut sts = sts(&k8s, "https://k8s/keys", KubernetesMode::Offline, 300);
    sts.verifier_mut()
        .registry_mut()
        .install_keys("k8s", &k8s.jwks, NOW)
        .unwrap();
    // A session whose admission ends well inside one STS lifetime.
    let record = coord_state::policy::SessionRecord {
        principal: PRINCIPAL,
        scope_ceiling: Action::Read.bit(),
        trust_rule: RULE,
        rule_generation: 3,
        active: true,
        window: 8,
        receipt_id: coord_types::identity::Digest32([1; 32]),
        expires_at: NOW + 40,
    };
    let session = coord_types::ids::SessionId([7; 16]);
    let r = sts.renew(session, &record, &clock()).unwrap();
    assert_eq!(
        r.expires_in, 40,
        "the token ends with the session, not one STS lifetime later"
    );
    // Once the session's own deadline has passed there is nothing to
    // renew, however healthy the clock and however active the record.
    let late = ClockHealth::healthy(NOW + 41, 0);
    assert_eq!(
        sts.renew(session, &record, &late),
        Err(ExchangeError::InvalidGrant("session expired"))
    );
    // A session with room left still gets the STS bound.
    let long = coord_state::policy::SessionRecord {
        expires_at: NOW + 10_000,
        ..record
    };
    assert_eq!(sts.renew(session, &long, &clock()).unwrap().expires_in, 300);
}
