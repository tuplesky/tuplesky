//! task-35 acceptance (sans-I/O): mix-up, algorithm confusion, wrong
//! audience, time and claim checks, token-directed key locations,
//! unknown-kid floods, cache staleness, clock-health failure, issuer and
//! TokenReview outage, and receipts that never carry the token.

use std::collections::{BTreeMap, BTreeSet};

use coord_authn::{
    AdmissionLog, ClockHealth, ConfigError, Decision, IssuerConfig, JwksLimits, KubernetesMode,
    MintError, OidcVerifier, Registry, SubjectKind, TimeError, TokenReview, TrustRuleConfig,
    VerifyError, WifVerifier, Workload, WorkloadKind, mint,
};
use coord_state::policy::Action;
use coord_types::ids::{PrincipalId, TrustRuleId};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use rcgen::KeyPair;
use serde_json::{Value, json};

const AUD: &str = "tuplesky-exchange";

fn b64url(bytes: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(T[(n >> 18) as usize & 63] as char);
        out.push(T[(n >> 12) as usize & 63] as char);
        if chunk.len() > 1 {
            out.push(T[(n >> 6) as usize & 63] as char);
        }
        if chunk.len() > 2 {
            out.push(T[n as usize & 63] as char);
        }
    }
    out
}

struct Issuer {
    name: String,
    iss: String,
    kid: String,
    enc: EncodingKey,
    jwks: Vec<u8>,
}

fn issuer(name: &str, iss: &str, kid: &str) -> Issuer {
    let key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let point = key.public_key_raw();
    assert_eq!(point.len(), 65, "uncompressed P-256 point");
    let jwks = json!({
        "keys": [{
            "kty": "EC", "crv": "P-256", "kid": kid, "alg": "ES256", "use": "sig",
            "x": b64url(&point[1..33]), "y": b64url(&point[33..65]),
        }]
    });
    Issuer {
        name: name.into(),
        iss: iss.into(),
        kid: kid.into(),
        enc: EncodingKey::from_ec_der(&key.serialize_der()),
        jwks: serde_json::to_vec(&jwks).unwrap(),
    }
}

fn config(i: &Issuer, audiences: &[&str]) -> IssuerConfig {
    IssuerConfig {
        name: i.name.clone(),
        issuer: i.iss.clone(),
        jwks_url: format!("https://{}/keys", i.name),
        algorithms: vec![Algorithm::ES256, Algorithm::RS256],
        audiences: audiences.iter().map(|a| a.to_string()).collect(),
        max_age_secs: Some(3600),
        allow_insecure_loopback: false,
    }
}

fn registry(issuers: &[&Issuer], limits: JwksLimits) -> Registry {
    Registry::new(
        issuers
            .iter()
            .map(|i| config(i, &[AUD, "client-1"]))
            .collect(),
        limits,
    )
    .unwrap()
}

fn claims(iss: &str, sub: &str, aud: Value, now: u64) -> serde_json::Map<String, Value> {
    let mut m = serde_json::Map::new();
    m.insert("iss".into(), json!(iss));
    m.insert("sub".into(), json!(sub));
    m.insert("aud".into(), aud);
    m.insert("exp".into(), json!(now + 600));
    m.insert("iat".into(), json!(now));
    m
}

fn sign(i: &Issuer, header: Header, claims: &serde_json::Map<String, Value>) -> String {
    encode(&header, claims, &i.enc).unwrap()
}

fn header(kid: &str) -> Header {
    let mut h = Header::new(Algorithm::ES256);
    h.kid = Some(kid.into());
    h
}

fn token(i: &Issuer, claims: &serde_json::Map<String, Value>) -> String {
    sign(i, header(&i.kid), claims)
}

fn clock(now: u64) -> ClockHealth {
    ClockHealth::healthy(now, 5)
}

/// Fetch and install the named issuer's keys whenever the registry asks,
/// then decide.
fn settle(
    registry: &mut Registry,
    token: &str,
    clock: &ClockHealth,
    issuers: &[&Issuer],
) -> Decision {
    loop {
        match registry.verify(token, clock) {
            Decision::NeedKeys { name, jwks_url } => {
                assert!(
                    jwks_url.starts_with("https://"),
                    "only configured endpoints"
                );
                let issuer = issuers.iter().find(|i| i.name == name).expect("configured");
                registry
                    .install_keys(&name, &issuer.jwks, clock.now)
                    .unwrap();
            }
            other => return other,
        }
    }
}

fn verified(d: Decision) -> coord_authn::VerifiedIdentity {
    match d {
        Decision::Verified(i) => *i,
        other => panic!("not verified: {other:?}"),
    }
}

fn denied(d: Decision) -> VerifyError {
    match d {
        Decision::Denied(e) => e,
        other => panic!("not denied: {other:?}"),
    }
}

fn rule(id: u8, issuer: &str, subject: SubjectKind, required: &[(&str, &str)]) -> TrustRuleConfig {
    TrustRuleConfig {
        id: TrustRuleId([id; 16]),
        generation: 3,
        enabled: true,
        issuer: issuer.into(),
        subject,
        audience: AUD.into(),
        required: required
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        principal: PrincipalId([id; 16]),
        scope_ceiling: Action::Read.bit() | Action::Write.bit(),
        max_lifetime_secs: 300,
    }
}

#[test]
fn oidc_id_tokens_verify_and_mint_receipts_without_the_raw_jwt() {
    let idp = issuer("idp", "https://idp.example", "k1");
    let mut v = OidcVerifier::new(registry(&[&idp], JwksLimits::default()), "client-1".into());
    let now = 1_700_000_000;
    let mut c = claims(&idp.iss, "user-42", json!("client-1"), now);
    c.insert("nonce".into(), json!("n-1"));
    c.insert("email".into(), json!("someone@example"));
    let t = token(&idp, &c);
    // The first attempt needs the configured key set.
    assert!(matches!(
        v.verify(&t, &clock(now), Some("n-1")),
        Decision::NeedKeys { ref name, ref jwks_url } if name == "idp" && jwks_url == "https://idp/keys"
    ));
    v.registry_mut()
        .install_keys("idp", &idp.jwks, now)
        .unwrap();
    let identity = verified(v.verify(&t, &clock(now), Some("n-1")));
    assert_eq!(identity.subject, "user-42");
    assert_eq!(identity.audiences, vec!["client-1"]);
    assert!(identity.workload.is_none());
    // The human identity is (issuer, subject); email is data, never the
    // identity.
    let attrs = coord_authn::receipt::attributes(&identity);
    assert_eq!(attrs.get("subject").unwrap(), "user-42");
    assert!(!attrs.contains_key("email"));
    let rules = vec![
        rule(1, "idp", SubjectKind::Human, &[("subject", "someone-else")]),
        TrustRuleConfig {
            audience: "client-1".into(),
            ..rule(2, "idp", SubjectKind::Human, &[("subject", "user-42")])
        },
    ];
    let entropy = [7u8; 32];
    let admitted = mint(&identity, &rules, &clock(now), &entropy).unwrap();
    assert_eq!(admitted.rule, TrustRuleId([2; 16]));
    assert_eq!(admitted.receipt.principal, PrincipalId([2; 16]));
    assert_eq!(admitted.receipt.rule_generation, 3);
    assert_eq!(
        admitted.receipt.scope_ceiling,
        Action::Read.bit() | Action::Write.bit()
    );
    assert_eq!(
        admitted.valid_until,
        now + 300,
        "the rule's lifetime caps the token's"
    );
    let mut log = AdmissionLog::new(2);
    log.record(&identity, &admitted, now);
    let rendered = serde_json::to_string(&log.records().collect::<Vec<_>>()).unwrap();
    assert!(rendered.contains("user-42"));
    assert!(!rendered.contains(&t), "no raw token in the log");
    assert!(!format!("{admitted:?}").contains(&t));
    // A disabled rule denies; no rule denies by default; a wrong ceiling
    // is refused.
    let mut disabled = rules.clone();
    disabled[1].enabled = false;
    assert_eq!(
        mint(&identity, &disabled, &clock(now), &entropy),
        Err(MintError::RuleDisabled(TrustRuleId([2; 16])))
    );
    assert_eq!(
        mint(&identity, &rules[..1], &clock(now), &entropy),
        Err(MintError::NoRule)
    );
    let mut wide = rules.clone();
    wide[1].scope_ceiling = 1 << 20;
    assert_eq!(
        mint(&identity, &wide, &clock(now), &entropy),
        Err(MintError::CeilingTooWide)
    );
}

#[test]
fn issuer_mix_up_and_unknown_issuers_are_rejected() {
    let a = issuer("a", "https://a.example", "ka");
    let b = issuer("b", "https://b.example", "kb");
    let mut r = registry(&[&a, &b], JwksLimits::default());
    let now = 1_700_000_000;
    r.install_keys("a", &a.jwks, now).unwrap();
    r.install_keys("b", &b.jwks, now).unwrap();
    // Signed by A, claiming to be B: B's namespace has no such key, and
    // asking B's configured endpoint later does not produce one.
    let t = sign(&a, header("ka"), &claims(&b.iss, "s", json!(AUD), now));
    assert_eq!(
        denied(settle(&mut r, &t, &clock(now + 10), &[&a, &b])),
        VerifyError::UnknownKey
    );
    assert_eq!(r.cache("b").unwrap().refresh_requests, 1);
    assert_eq!(
        r.cache("a").unwrap().refresh_requests,
        0,
        "A's keys were never consulted"
    );
    // Signed by A with B's kid, claiming B: B's key does not verify it.
    let t = sign(&a, header("kb"), &claims(&b.iss, "s", json!(AUD), now));
    assert_eq!(
        denied(r.verify(&t, &clock(now))),
        VerifyError::InvalidSignature
    );
    // An issuer nobody configured never selects keys.
    let t = sign(
        &a,
        header("ka"),
        &claims("https://evil.example", "s", json!(AUD), now),
    );
    assert_eq!(
        denied(r.verify(&t, &clock(now))),
        VerifyError::UnknownIssuer
    );
    assert_eq!(
        r.cache("a").unwrap().refresh_requests,
        0,
        "no fetch was triggered"
    );
    // Not a JWT at all.
    assert_eq!(
        denied(r.verify("nope", &clock(now))),
        VerifyError::Malformed
    );
}

#[test]
fn algorithm_confusion_is_rejected_before_any_key_is_used() {
    let a = issuer("a", "https://a.example", "ka");
    let mut r = registry(&[&a], JwksLimits::default());
    let now = 1_700_000_000;
    r.install_keys("a", &a.jwks, now).unwrap();
    // HS256 with the public key as the secret: the algorithm is not in the
    // configured set, so no key is even looked up.
    let mut h = Header::new(Algorithm::HS256);
    h.kid = Some("ka".into());
    let secret = EncodingKey::from_secret(&a.jwks);
    let t = encode(&h, &claims(&a.iss, "s", json!(AUD), now), &secret).unwrap();
    assert_eq!(
        denied(r.verify(&t, &clock(now))),
        VerifyError::AlgorithmNotAllowed(Algorithm::HS256)
    );
    // RS256 in the header over an EC key: family mismatch.
    let good = token(&a, &claims(&a.iss, "s", json!(AUD), now));
    let parts: Vec<&str> = good.split('.').collect();
    let rs = b64url(br#"{"alg":"RS256","kid":"ka","typ":"JWT"}"#);
    let forged = format!("{rs}.{}.{}", parts[1], parts[2]);
    assert_eq!(
        denied(r.verify(&forged, &clock(now))),
        VerifyError::KeyAlgorithmMismatch
    );
    // A tampered payload does not verify.
    let tampered = format!(
        "{}.{}.{}",
        parts[0],
        b64url(
            br#"{"iss":"https://a.example","sub":"x","aud":"tuplesky-exchange","exp":9999999999}"#
        ),
        parts[2]
    );
    assert_eq!(
        denied(r.verify(&tampered, &clock(now))),
        VerifyError::InvalidSignature
    );
    // Configuring a symmetric algorithm is refused.
    let mut bad = config(&a, &[AUD]);
    bad.algorithms.push(Algorithm::HS256);
    assert_eq!(
        Registry::new(vec![bad], JwksLimits::default()).err(),
        Some(coord_authn::ConfigError::AlgorithmNotPermitted(
            Algorithm::HS256
        ))
    );
    // A key set with a symmetric key is refused.
    let oct =
        serde_json::to_vec(&json!({"keys": [{"kty": "oct", "kid": "k", "k": "AAAA"}]})).unwrap();
    assert_eq!(
        r.install_keys("a", &oct, now),
        Err(coord_authn::JwksError::UnusableKey)
    );
}

#[test]
fn audience_azp_nonce_and_time_claims_are_checked_conservatively() {
    let idp = issuer("idp", "https://idp.example", "k1");
    let now = 1_700_000_000;
    let mut v = OidcVerifier::new(registry(&[&idp], JwksLimits::default()), "client-1".into());
    v.registry_mut()
        .install_keys("idp", &idp.jwks, now)
        .unwrap();
    let mut verify =
        |c: &serde_json::Map<String, Value>, clock: ClockHealth, nonce: Option<&str>| {
            let t = token(&idp, c);
            loop {
                match v.verify(&t, &clock, nonce) {
                    Decision::NeedKeys { name, .. } => {
                        v.registry_mut()
                            .install_keys(&name, &idp.jwks, clock.now)
                            .unwrap();
                    }
                    other => return other,
                }
            }
        };
    // Wrong audience.
    let c = claims(&idp.iss, "u", json!("other-client"), now);
    assert_eq!(
        denied(verify(&c, clock(now), None)),
        VerifyError::InvalidAudience
    );
    // A configured audience that is not this client.
    let c = claims(&idp.iss, "u", json!(AUD), now);
    assert_eq!(
        denied(verify(&c, clock(now), None)),
        VerifyError::InvalidAudience
    );
    // Several audiences without azp; with a wrong azp; with the right one.
    let c = claims(&idp.iss, "u", json!(["client-1", "client-2"]), now);
    assert_eq!(
        denied(verify(&c, clock(now), None)),
        VerifyError::AzpMismatch
    );
    let mut c2 = c.clone();
    c2.insert("azp".into(), json!("client-2"));
    assert_eq!(
        denied(verify(&c2, clock(now), None)),
        VerifyError::AzpMismatch
    );
    c2.insert("azp".into(), json!("client-1"));
    verified(verify(&c2, clock(now), None));
    // Nonce.
    let mut c = claims(&idp.iss, "u", json!("client-1"), now);
    assert_eq!(
        denied(verify(&c, clock(now), Some("n"))),
        VerifyError::NonceMismatch
    );
    c.insert("nonce".into(), json!("n"));
    verified(verify(&c, clock(now), Some("n")));
    // Time, conservatively: expiry inside the uncertainty is expired.
    let c = claims(&idp.iss, "u", json!("client-1"), now);
    assert_eq!(
        denied(verify(&c, clock(now + 596), None)),
        VerifyError::Time(TimeError::Expired)
    );
    verified(verify(&c, clock(now + 594), None));
    let mut c = claims(&idp.iss, "u", json!("client-1"), now);
    c.insert("nbf".into(), json!(now + 10));
    assert_eq!(
        denied(verify(&c, clock(now + 12), None)),
        VerifyError::Time(TimeError::NotYetValid)
    );
    verified(verify(&c, clock(now + 15), None));
    let mut c = claims(&idp.iss, "u", json!("client-1"), now);
    c.insert("exp".into(), json!(now + 100_000));
    assert_eq!(
        denied(verify(&c, clock(now + 3700), None)),
        VerifyError::Time(TimeError::TooOld)
    );
    let mut c = claims(&idp.iss, "u", json!("client-1"), now);
    c.insert("iat".into(), json!(now + 100));
    assert_eq!(
        denied(verify(&c, clock(now), None)),
        VerifyError::Time(TimeError::IssuedInFuture)
    );
    // An unhealthy clock cannot establish validity.
    let c = claims(&idp.iss, "u", json!("client-1"), now);
    let sick = ClockHealth {
        now,
        uncertainty: 5,
        healthy: false,
    };
    assert_eq!(
        denied(verify(&c, sick, None)),
        VerifyError::Time(TimeError::ClockUnhealthy)
    );
    // Missing required claims.
    let mut c = claims(&idp.iss, "u", json!("client-1"), now);
    c.remove("exp");
    assert_eq!(
        denied(verify(&c, clock(now), None)),
        VerifyError::MissingClaim("exp".into())
    );
}

#[test]
fn token_directed_key_locations_are_never_dereferenced() {
    let a = issuer("a", "https://a.example", "ka");
    let mut r = registry(&[&a], JwksLimits::default());
    let now = 1_700_000_000;
    let c = claims(&a.iss, "s", json!(AUD), now);
    for directed in ["jku", "x5u"] {
        let mut h = header("ka");
        if directed == "jku" {
            h.jku = Some("https://attacker.example/keys".into());
        } else {
            h.x5u = Some("https://attacker.example/cert".into());
        }
        let t = sign(&a, h, &c);
        assert_eq!(
            denied(r.verify(&t, &clock(now))),
            VerifyError::TokenDirectedKeys
        );
    }
    assert_eq!(r.cache("a").unwrap().refresh_requests, 0);
    // Without such headers the only fetch ever requested is the
    // configured endpoint.
    match r.verify(&token(&a, &c), &clock(now)) {
        Decision::NeedKeys { jwks_url, .. } => assert_eq!(jwks_url, "https://a/keys"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn unknown_kid_floods_are_bounded_by_the_refresh_budget() {
    let a = issuer("a", "https://a.example", "ka");
    let limits = JwksLimits {
        refreshes_per_window: 2,
        window_secs: 60,
        ..JwksLimits::default()
    };
    let mut r = registry(&[&a], limits);
    let now = 1_700_000_000;
    r.install_keys("a", &a.jwks, now).unwrap();
    let c = claims(&a.iss, "s", json!(AUD), now);
    let mut refreshes = 0;
    for i in 0..50 {
        let t = sign(&a, header(&format!("unknown-{i}")), &c);
        match r.verify(&t, &clock(now + i)) {
            Decision::NeedKeys { name, .. } => {
                refreshes += 1;
                // The issuer still publishes only `ka`; it is not asked
                // again for the same flood.
                r.install_keys(&name, &a.jwks, now + i).unwrap();
                assert_eq!(
                    denied(r.verify(&t, &clock(now + i))),
                    VerifyError::UnknownKey
                );
            }
            Decision::Denied(VerifyError::UnknownKey) => {}
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(refreshes, 2, "two refreshes per window, whatever the flood");
    // Refreshes at ticks 5 and 10 (the first flood tick is inside the
    // minimum interval of the initial fetch); everything after the second
    // refresh's minimum interval is refused until the window resets.
    assert_eq!(r.cache("a").unwrap().refused, 35);
    // The known key keeps verifying throughout.
    verified(r.verify(&token(&a, &c), &clock(now + 50)));
    // The next window allows refreshes again.
    let t = sign(&a, header("unknown-x"), &c);
    assert!(matches!(
        r.verify(&t, &clock(now + 120)),
        Decision::NeedKeys { .. }
    ));
}

#[test]
fn cache_staleness_and_issuer_outage_fail_closed_after_the_limit() {
    let a = issuer("a", "https://a.example", "ka");
    let limits = JwksLimits {
        ttl_secs: 100,
        stale_limit_secs: 1000,
        refreshes_per_window: 1,
        window_secs: 60,
        ..JwksLimits::default()
    };
    let mut r = registry(&[&a], limits);
    let t0 = 1_700_000_000;
    r.install_keys("a", &a.jwks, t0).unwrap();
    let c = claims(&a.iss, "s", json!(AUD), t0);
    let fresh = |now: u64| {
        let mut cl = claims(&a.iss, "s", json!(AUD), now);
        cl.insert("exp".into(), json!(now + 600));
        (token(&a, &cl), clock(now))
    };
    let _ = &c;
    // Fresh: verified without a fetch.
    let (t, cl) = fresh(t0 + 50);
    verified(r.verify(&t, &cl));
    // Past the freshness limit: a refresh is requested; the issuer is
    // down, so the cached key is still used (within the staleness limit).
    let (t, cl) = fresh(t0 + 200);
    assert!(matches!(r.verify(&t, &cl), Decision::NeedKeys { .. }));
    r.refresh_failed("a");
    verified(r.verify(&t, &cl));
    // Beyond the staleness limit: nothing verifies until a fetch succeeds.
    let (t, cl) = fresh(t0 + 1500);
    assert!(matches!(r.verify(&t, &cl), Decision::NeedKeys { .. }));
    r.refresh_failed("a");
    assert_eq!(denied(r.verify(&t, &cl)), VerifyError::KeysStale);
    // The issuer returns: keys refresh and verification resumes.
    let (t, cl) = fresh(t0 + 1600);
    match r.verify(&t, &cl) {
        Decision::NeedKeys { name, .. } => r.install_keys(&name, &a.jwks, t0 + 1600).unwrap(),
        other => panic!("{other:?}"),
    };
    verified(r.verify(&t, &cl));
    // Oversized and over-populated documents are refused.
    let big = vec![b' '; limits.max_document_bytes + 1];
    assert!(matches!(
        r.install_keys("a", &big, t0),
        Err(coord_authn::JwksError::TooLarge { .. })
    ));
    let many: Vec<Value> = (0..limits.max_keys + 1)
        .map(|i| json!({"kty": "EC", "crv": "P-256", "kid": format!("k{i}"), "x": "AA", "y": "AA"}))
        .collect();
    let doc = serde_json::to_vec(&json!({ "keys": many })).unwrap();
    assert!(matches!(
        r.install_keys("a", &doc, t0),
        Err(coord_authn::JwksError::TooManyKeys { .. })
    ));
}

#[test]
fn workload_claims_map_through_rules_and_token_review_is_explicit() {
    let k8s = issuer("k8s", "https://kubernetes.default.svc", "kk");
    let gh = issuer("gh", "https://token.actions.githubusercontent.com", "kg");
    let now = 1_700_000_000;
    let mut kinds = BTreeMap::new();
    kinds.insert(
        "k8s".to_string(),
        WorkloadKind::Kubernetes(KubernetesMode::Offline),
    );
    kinds.insert("gh".to_string(), WorkloadKind::GithubActions);
    let mut v = WifVerifier::new(registry(&[&k8s, &gh], JwksLimits::default()), kinds);
    v.registry_mut()
        .install_keys("k8s", &k8s.jwks, now)
        .unwrap();
    v.registry_mut().install_keys("gh", &gh.jwks, now).unwrap();

    // Kubernetes, offline: the subject and the kubernetes.io claims agree.
    let mut c = claims(&k8s.iss, "system:serviceaccount:prod:kine", json!(AUD), now);
    c.insert(
        "kubernetes.io".into(),
        json!({"namespace": "prod", "serviceaccount": {"name": "kine", "uid": "u-1"}, "pod": {"name": "kine-0"}}),
    );
    let identity = verified(v.verify(&token(&k8s, &c), &clock(now), None));
    assert_eq!(
        identity.workload,
        Some(Workload::KubernetesServiceAccount {
            namespace: "prod".into(),
            name: "kine".into(),
            uid: Some("u-1".into()),
            pod: Some("kine-0".into()),
        })
    );
    // Rules bind the intended issuer, namespace and account; a claim
    // alone grants nothing without a rule.
    let rules = vec![rule(
        1,
        "k8s",
        SubjectKind::KubernetesServiceAccount,
        &[("namespace", "prod"), ("serviceaccount", "kine")],
    )];
    let admitted = mint(&identity, &rules, &clock(now), &[1u8; 32]).unwrap();
    assert_eq!(admitted.receipt.principal, PrincipalId([1; 16]));
    let other_ns = vec![rule(
        1,
        "k8s",
        SubjectKind::KubernetesServiceAccount,
        &[("namespace", "dev"), ("serviceaccount", "kine")],
    )];
    assert_eq!(
        mint(&identity, &other_ns, &clock(now), &[1u8; 32]),
        Err(MintError::NoRule)
    );
    let human_rule = vec![rule(1, "k8s", SubjectKind::Human, &[])];
    assert_eq!(
        mint(&identity, &human_rule, &clock(now), &[1u8; 32]),
        Err(MintError::NoRule)
    );
    // Inconsistent or malformed subjects are refused.
    let mut bad = c.clone();
    bad.insert("kubernetes.io".into(), json!({"namespace": "dev"}));
    assert_eq!(
        denied(v.verify(&token(&k8s, &bad), &clock(now), None)),
        VerifyError::ClaimMismatch("kubernetes.io.namespace".into())
    );
    let odd = claims(&k8s.iss, "not-a-service-account", json!(AUD), now);
    assert_eq!(
        denied(v.verify(&token(&k8s, &odd), &clock(now), None)),
        VerifyError::ClaimMismatch("sub".into())
    );

    // GitHub Actions: immutable identifiers are required.
    let mut g = claims(&gh.iss, "repo:org/app:ref:refs/heads/main", json!(AUD), now);
    g.insert("repository".into(), json!("org/app"));
    g.insert("repository_id".into(), json!("1234"));
    g.insert("repository_owner_id".into(), json!("99"));
    g.insert(
        "workflow_ref".into(),
        json!("org/app/.github/workflows/deploy.yml@refs/heads/main"),
    );
    g.insert("environment".into(), json!("production"));
    let identity = verified(v.verify(&token(&gh, &g), &clock(now), None));
    assert!(matches!(
        identity.workload,
        Some(Workload::GithubActions { ref repository_id, .. }) if repository_id == "1234"
    ));
    let gh_rule = vec![rule(
        2,
        "gh",
        SubjectKind::GithubActions,
        &[
            ("repository_id", "1234"),
            ("repository_owner_id", "99"),
            ("environment", "production"),
        ],
    )];
    mint(&identity, &gh_rule, &clock(now), &[2u8; 32]).unwrap();
    let renamed_repo = vec![rule(
        2,
        "gh",
        SubjectKind::GithubActions,
        &[("repository_id", "5678"), ("repository", "org/app")],
    )];
    assert_eq!(
        mint(&identity, &renamed_repo, &clock(now), &[2u8; 32]),
        Err(MintError::NoRule),
        "the mutable name does not stand in for the immutable id"
    );
    g.remove("repository_id");
    assert_eq!(
        denied(v.verify(&token(&gh, &g), &clock(now), None)),
        VerifyError::ClaimMismatch("repository_id".into())
    );

    // TokenReview mode: explicit, and unavailable means denied.
    let mut kinds = BTreeMap::new();
    kinds.insert(
        "k8s".to_string(),
        WorkloadKind::Kubernetes(KubernetesMode::TokenReview),
    );
    let mut v = WifVerifier::new(registry(&[&k8s], JwksLimits::default()), kinds);
    v.registry_mut()
        .install_keys("k8s", &k8s.jwks, now)
        .unwrap();
    let t = token(&k8s, &c);
    assert_eq!(
        denied(v.verify(&t, &clock(now), None)),
        VerifyError::TokenReviewUnavailable
    );
    assert_eq!(
        denied(v.verify(&t, &clock(now), Some(&TokenReview::Unavailable))),
        VerifyError::TokenReviewUnavailable
    );
    let reviewed = |authenticated: bool, username: &str| TokenReview::Reviewed {
        authenticated,
        username: Some(username.into()),
        audiences: vec![AUD.into()],
    };
    assert_eq!(
        denied(v.verify(
            &t,
            &clock(now),
            Some(&reviewed(false, "system:serviceaccount:prod:kine"))
        )),
        VerifyError::TokenReviewDenied
    );
    assert_eq!(
        denied(v.verify(
            &t,
            &clock(now),
            Some(&reviewed(true, "system:serviceaccount:prod:other"))
        )),
        VerifyError::TokenReviewDenied
    );
    verified(v.verify(
        &t,
        &clock(now),
        Some(&reviewed(true, "system:serviceaccount:prod:kine")),
    ));
    // An issuer without a workload kind admits no workload.
    let mut v = WifVerifier::new(registry(&[&k8s], JwksLimits::default()), BTreeMap::new());
    v.registry_mut()
        .install_keys("k8s", &k8s.jwks, now)
        .unwrap();
    assert_eq!(
        denied(v.verify(&t, &clock(now), None)),
        VerifyError::NoWorkloadKind
    );
    let _: BTreeSet<String> = BTreeSet::new();
}

#[test]
fn a_configuration_name_or_issuer_is_never_shared_by_two_issuers() {
    // The name selects the key cache. Two configurations sharing one
    // would share its keys however far apart their issuer strings and
    // JWKS endpoints are, so a token naming issuer A would verify under
    // B's key whenever the kid and algorithm matched.
    let a = issuer("shared", "https://a.example", "k1");
    let b = issuer("shared", "https://b.example", "k2");
    let clash = Registry::new(
        vec![config(&a, &[AUD]), config(&b, &[AUD])],
        JwksLimits::default(),
    );
    assert!(matches!(clash.err(), Some(ConfigError::DuplicateName(n)) if n == "shared"));
    // The same issuer string twice is refused too: the second would
    // silently replace the first.
    let c = issuer("other", "https://a.example", "k3");
    let clash = Registry::new(
        vec![config(&a, &[AUD]), config(&c, &[AUD])],
        JwksLimits::default(),
    );
    assert!(
        matches!(clash.err(), Some(ConfigError::DuplicateIssuer(i)) if i == "https://a.example")
    );
    // Distinct names and issuers are accepted.
    let d = issuer("distinct", "https://d.example", "k4");
    assert!(
        Registry::new(
            vec![config(&a, &[AUD]), config(&d, &[AUD])],
            JwksLimits::default()
        )
        .is_ok()
    );
}

#[test]
fn plaintext_key_endpoints_are_admitted_only_for_a_real_loopback_host() {
    // A prefix test is not a host check: the authority of
    // `http://localhost@evil.example/keys` is `evil.example`, and
    // `http://localhost.evil.example/keys` is a different host again, so
    // both used to pass and let an attacker serve JWKS over plaintext.
    for url in [
        "http://localhost@evil.example/keys",
        "http://localhost.evil.example/keys",
        "http://127.0.0.1.evil.example/keys",
        "http://user@127.0.0.1.evil.example/keys",
        "http://evil.example/keys",
        "http://[::1]@evil.example/keys",
    ] {
        assert!(
            !coord_authn::secure_endpoint(url, true),
            "{url} is not loopback"
        );
    }
    for url in [
        "http://localhost/keys",
        "http://localhost:8080/keys",
        "http://127.0.0.1:9000/keys",
        "http://127.2.3.4/keys",
        "http://[::1]:9000/keys",
        "http://LocalHost/keys",
    ] {
        assert!(coord_authn::secure_endpoint(url, true), "{url} is loopback");
    }
    // Plaintext is refused outright unless the configuration asks for it,
    // and HTTPS never needs the exception.
    assert!(!coord_authn::secure_endpoint(
        "http://localhost/keys",
        false
    ));
    assert!(coord_authn::secure_endpoint(
        "https://evil.example/keys",
        false
    ));
}

#[test]
fn a_freshness_ceiling_is_not_satisfied_by_a_token_without_an_issue_time() {
    // A maximum age cannot be enforced against a token that does not say
    // when it was issued, so skipping the check for it let such a token
    // bypass the ceiling entirely.
    let clock = ClockHealth::healthy(10_000, 5);
    assert_eq!(
        clock.check(20_000, None, None, Some(3_600)),
        Err(TimeError::IssuedAtMissing)
    );
    // Without a ceiling, a missing issue time is not by itself a denial.
    assert_eq!(clock.check(20_000, None, None, None), Ok(()));
    // The age is measured from the latest instant the reading may denote,
    // so a token that may already be past the ceiling is refused.
    assert_eq!(
        clock.check(20_000, None, Some(10_000 - 3_600), Some(3_600)),
        Err(TimeError::TooOld),
        "now + uncertainty - iat is over the ceiling"
    );
    assert_eq!(
        clock.check(20_000, None, Some(10_000 - 3_590), Some(3_600)),
        Ok(())
    );
    // A freshly minted token is still accepted under an uncertain clock.
    assert_eq!(clock.check(20_000, None, Some(10_000), Some(3_600)), Ok(()));
    // An issue time beyond what the reading could denote is refused.
    assert_eq!(
        clock.check(20_000, None, Some(10_006), Some(3_600)),
        Err(TimeError::IssuedInFuture)
    );
}
