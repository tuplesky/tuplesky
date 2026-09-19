//! task-38 acceptance (sans-I/O): the service code flow refuses wrong or
//! missing `azp` and unchecked multi-audience tokens, CSRF and issuer
//! mix-up, code and redirect substitution, concurrent tabs interfering
//! with each other and callbacks after a broker restart; one code
//! creates at most one session in replicated state.

use std::collections::BTreeMap;

use coord_authn::{ClockHealth, SubjectKind, TrustRuleConfig};
use coord_core::effect::BootId;
use coord_core::outbox::BarrierAllocator;
use coord_login::{
    Approved, LoginError, LoginLimits, RedeemRequest, Registration, ServiceLogin, StartRequest,
    UpstreamIdentity,
};
use coord_state::plan::Outcome;
use coord_state::policy::{Action, AdmissionReceiptV1, GrantKind, TrustRule};
use coord_state::{InternalCommand, PlanLimits, Response, plan_internal};
use coord_storage::materialize::{ApplyOutcome, apply_plan};
use coord_storage::views::{ViewBudget, build_internal_view};
use coord_storage::{GroupLimits, StoreWorker};
use coord_store_testkit::model::ModelEngine;
use coord_sts::{CreatorError, ExchangeError, KeyRing, SessionCreator, SigningKey, Sts, StsConfig};
use coord_types::identity::Digest32;
use coord_types::ids::*;
use oauth2::{PkceCodeChallenge, PkceCodeVerifier};

const NS: NamespaceId = NamespaceId([5; 16]);
const IDP: &str = "https://idp.example";
const NOW: u64 = 1_700_000_000;

fn verifier(seed: u8) -> String {
    std::iter::repeat_n((b'a' + seed % 26) as char, 64).collect()
}

fn challenge(v: &str) -> String {
    PkceCodeChallenge::from_code_verifier_sha256(&PkceCodeVerifier::new(v.to_string()))
        .as_str()
        .to_string()
}

fn login(limits: LoginLimits) -> ServiceLogin {
    let mut upstream_clients = BTreeMap::new();
    upstream_clients.insert("idp".to_string(), "broker-at-idp".to_string());
    ServiceLogin::new(
        limits,
        vec![Registration {
            client_id: "coordctl".into(),
            redirect_uris: vec![
                "http://127.0.0.1:4100/callback".into(),
                "http://127.0.0.1:4101/callback".into(),
            ],
            upstream: "idp".into(),
        }],
        upstream_clients,
    )
}

fn start(seed: u8, redirect: &str) -> StartRequest {
    StartRequest {
        client_id: "coordctl".into(),
        redirect_uri: redirect.into(),
        code_challenge: challenge(&verifier(seed)),
        code_challenge_method: "S256".into(),
        state: format!("tab-{seed}"),
    }
}

fn identity(aud: &[&str], azp: Option<&str>) -> UpstreamIdentity {
    UpstreamIdentity {
        issuer: IDP.into(),
        subject: "user-42".into(),
        audiences: aud.iter().map(|a| a.to_string()).collect(),
        authorized_party: azp.map(str::to_string),
        expires_at: NOW + 600,
    }
}

fn entropy(n: u8) -> [u8; 32] {
    let mut e = [0u8; 32];
    for (i, b) in e.iter_mut().enumerate() {
        *b = n.wrapping_mul(7).wrapping_add(i as u8);
    }
    e
}

fn code_of(approved: &Approved) -> String {
    let query = approved.redirect.split('?').nth(1).unwrap();
    query
        .split('&')
        .find_map(|kv| kv.strip_prefix("code="))
        .unwrap()
        .to_string()
}

fn redeem(code: &str, seed: u8, client: &str, redirect: &str) -> RedeemRequest {
    RedeemRequest {
        code: code.into(),
        code_verifier: verifier(seed),
        client_id: client.into(),
        redirect_uri: redirect.into(),
    }
}

#[test]
fn the_service_code_flow_refuses_every_substitution() {
    let mut l = login(LoginLimits {
        max_pending: 3,
        pending_ttl_secs: 600,
        code_ttl_secs: 120,
    });
    let r1 = "http://127.0.0.1:4100/callback";
    // Registration and PKCE checks at start.
    let mut bad = start(1, r1);
    bad.client_id = "other".into();
    assert_eq!(
        l.start(NOW, &bad, &entropy(1)),
        Err(LoginError::UnknownClient)
    );
    assert_eq!(
        l.start(NOW, &start(1, "http://127.0.0.1:9/callback"), &entropy(1)),
        Err(LoginError::RedirectNotRegistered)
    );
    let mut bad = start(1, r1);
    bad.code_challenge_method = "plain".into();
    assert_eq!(
        l.start(NOW, &bad, &entropy(1)),
        Err(LoginError::UnsupportedChallengeMethod)
    );
    let mut bad = start(1, r1);
    bad.code_challenge = "short".into();
    assert_eq!(
        l.start(NOW, &bad, &entropy(1)),
        Err(LoginError::MalformedPkce)
    );
    // Two tabs: distinct transactions, upstream states and nonces.
    let a = l.start(NOW, &start(1, r1), &entropy(1)).unwrap();
    let b = l
        .start(
            NOW,
            &start(2, "http://127.0.0.1:4101/callback"),
            &entropy(2),
        )
        .unwrap();
    assert_ne!(a.txn, b.txn);
    assert_ne!(a.upstream_state, b.upstream_state);
    assert_ne!(a.upstream_nonce, b.upstream_nonce);
    assert_ne!(a.upstream_state, a.upstream_nonce);
    assert!(!format!("{a:?}").contains(&a.upstream_state), "redacted");
    assert_eq!(l.pending(), 2);
    let _ = l.start(NOW, &start(3, r1), &entropy(3)).unwrap();
    assert_eq!(
        l.start(NOW, &start(4, r1), &entropy(4)),
        Err(LoginError::TooManyPending)
    );
    // The upstream leg for tab A: the broker's own verifier is what the
    // exchange uses, never the CLI's.
    let (txn, upstream_verifier, upstream) = l.upstream_exchange(&a.upstream_state).unwrap();
    assert_eq!(txn, a.txn);
    assert_eq!(upstream, "idp");
    assert_ne!(upstream_verifier, verifier(1));
    // CSRF: a callback with a state nobody started.
    assert_eq!(
        l.approve(
            NOW,
            "forged",
            identity(&["broker-at-idp"], None),
            IDP,
            &entropy(9)
        ),
        Err(LoginError::UnknownTransaction)
    );
    // Mix-up: the identity comes from another issuer.
    let mut foreign = identity(&["broker-at-idp"], None);
    foreign.issuer = "https://other.example".into();
    assert_eq!(
        l.approve(NOW, &a.upstream_state, foreign, IDP, &entropy(9)),
        Err(LoginError::IssuerMismatch)
    );
    // Audience and authorized party.
    assert_eq!(
        l.approve(
            NOW,
            &a.upstream_state,
            identity(&["someone-else"], None),
            IDP,
            &entropy(9)
        ),
        Err(LoginError::AudienceMismatch)
    );
    assert_eq!(
        l.approve(
            NOW,
            &a.upstream_state,
            identity(&["broker-at-idp", "someone-else"], None),
            IDP,
            &entropy(9)
        ),
        Err(LoginError::AzpMismatch),
        "several audiences need azp"
    );
    assert_eq!(
        l.approve(
            NOW,
            &a.upstream_state,
            identity(&["broker-at-idp"], Some("someone-else")),
            IDP,
            &entropy(9)
        ),
        Err(LoginError::AzpMismatch)
    );
    let approved_a = l
        .approve(
            NOW,
            &a.upstream_state,
            identity(&["broker-at-idp", "someone-else"], Some("broker-at-idp")),
            IDP,
            &entropy(0x11),
        )
        .unwrap();
    assert!(approved_a.redirect.starts_with(&format!("{r1}?code=")));
    assert!(approved_a.redirect.ends_with("&state=tab-1"));
    assert!(!format!("{approved_a:?}").contains("code="), "redacted");
    // The upstream state is single use.
    assert_eq!(
        l.approve(
            NOW,
            &a.upstream_state,
            identity(&["broker-at-idp"], None),
            IDP,
            &entropy(9)
        ),
        Err(LoginError::UnknownTransaction)
    );
    let approved_b = l
        .approve(
            NOW,
            &b.upstream_state,
            identity(&["broker-at-idp"], None),
            IDP,
            &entropy(0x22),
        )
        .unwrap();
    let code_a = code_of(&approved_a);
    let code_b = code_of(&approved_b);
    assert_ne!(code_a, code_b);
    // Redemption binds code, verifier, client and redirect together.
    assert_eq!(
        l.redeem(NOW, &redeem("nope", 1, "coordctl", r1)),
        Err(LoginError::UnknownCode)
    );
    assert_eq!(
        l.redeem(NOW, &redeem(&code_a, 2, "coordctl", r1)),
        Err(LoginError::VerifierMismatch),
        "another tab's verifier"
    );
    assert_eq!(
        l.redeem(NOW, &redeem(&code_b, 1, "coordctl", r1)),
        Err(LoginError::VerifierMismatch),
        "another tab's code"
    );
    assert_eq!(
        l.redeem(NOW, &redeem(&code_a, 1, "other", r1)),
        Err(LoginError::ClientMismatch)
    );
    assert_eq!(
        l.redeem(
            NOW,
            &redeem(&code_a, 1, "coordctl", "http://127.0.0.1:4101/callback")
        ),
        Err(LoginError::RedirectMismatch)
    );
    let mut short = redeem(&code_a, 1, "coordctl", r1);
    short.code_verifier = "short".into();
    assert_eq!(l.redeem(NOW, &short), Err(LoginError::MalformedPkce));
    assert!(!format!("{short:?}").contains("short"), "verifier redacted");
    // The right redemption, once.
    let redeemed = l.redeem(NOW, &redeem(&code_a, 1, "coordctl", r1)).unwrap();
    assert_eq!(redeemed.identity.subject, "user-42");
    assert_eq!(redeemed.upstream, "idp");
    assert_eq!(redeemed.commitment, approved_a.commitment);
    assert_eq!(
        l.redeem(NOW, &redeem(&code_a, 1, "coordctl", r1)),
        Err(LoginError::UnknownCode),
        "a code redeems once"
    );
    // Tab B's code expires unredeemed; a pending login expires before
    // its callback.
    assert_eq!(
        l.redeem(
            NOW + 121,
            &redeem(&code_b, 2, "coordctl", "http://127.0.0.1:4101/callback")
        ),
        Err(LoginError::CodeExpired)
    );
    let c = l.start(NOW + 121, &start(5, r1), &entropy(5)).unwrap();
    assert_eq!(
        l.approve(
            NOW + 1000,
            &c.upstream_state,
            identity(&["broker-at-idp"], None),
            IDP,
            &entropy(9)
        ),
        Err(LoginError::UnknownTransaction)
    );
    assert_eq!(l.pending(), 0);
    // A broker restart loses pending logins: the callback fails cleanly
    // and the user logs in again.
    let mut before = login(LoginLimits::default());
    let s = before.start(NOW, &start(6, r1), &entropy(6)).unwrap();
    let mut after = login(LoginLimits::default());
    assert_eq!(
        after.approve(
            NOW,
            &s.upstream_state,
            identity(&["broker-at-idp"], None),
            IDP,
            &entropy(9)
        ),
        Err(LoginError::UnknownTransaction)
    );
    assert_eq!(after.pending(), 0);
}

/// Replicated state stand-in.
struct Domain {
    worker: StoreWorker<ModelEngine>,
    alloc: BarrierAllocator,
}

impl Domain {
    fn new() -> Self {
        let boot = BootId([1; 16]);
        let inc = ReplicaIncarnation::new(1).unwrap();
        let mut d = Domain {
            worker: StoreWorker::open(ModelEngine::new(), boot, inc, GroupLimits::default())
                .unwrap(),
            alloc: BarrierAllocator::new(inc, boot),
        };
        d.apply(&InternalCommand::PutTrustRule {
            namespace: NS,
            rule: TrustRuleId([9; 16]),
            record: TrustRule {
                enabled: true,
                generation: 3,
            },
        });
        d
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
}

impl SessionCreator for Domain {
    fn create(&mut self, command: InternalCommand) -> Result<Response, CreatorError> {
        Ok(self.apply(&command))
    }
}

fn receipt(n: u8) -> AdmissionReceiptV1 {
    AdmissionReceiptV1 {
        receipt_id: Digest32([n; 32]),
        session: SessionId([n; 16]),
        principal: PrincipalId([7; 16]),
        scope_ceiling: Action::Read.bit(),
        trust_rule: TrustRuleId([9; 16]),
        rule_generation: 3,
        expires_at: NOW + 3600,
    }
}

#[test]
fn one_code_creates_at_most_one_session() {
    let mut d = Domain::new();
    let commitment = Digest32([0xc0; 32]);
    let committed = d.apply(&InternalCommand::CommitGrant {
        namespace: NS,
        commitment,
        kind: GrantKind::Code,
    });
    assert_eq!(committed.outcome, Outcome::GrantCommitted);
    // Consuming an unknown code creates nothing.
    let consume = |receipt: AdmissionReceiptV1, code: Digest32| InternalCommand::ConsumeAdmission {
        namespace: NS,
        receipt,
        code: Some(code),
        refresh_family: None,
        window: 64,
    };
    assert_eq!(
        d.apply(&consume(receipt(1), Digest32([0xd0; 32]))).outcome,
        Outcome::ErrGrantUnavailable
    );
    // The first consumption creates the session; a second one, even with
    // a fresh receipt and session identity, finds the code consumed.
    assert_eq!(
        d.apply(&consume(receipt(1), commitment)).outcome,
        Outcome::SessionCreated {
            session: SessionId([1; 16])
        }
    );
    assert_eq!(
        d.apply(&consume(receipt(2), commitment)).outcome,
        Outcome::ErrGrantUnavailable
    );
    // Through the STS issue path: the second redemption of the same
    // commitment is a grant failure and no token is signed.
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let ring = KeyRing::new(SigningKey::from_pkcs8_der("sts-1", &key.serialize_der()).unwrap());
    let rules = vec![TrustRuleConfig {
        id: TrustRuleId([9; 16]),
        generation: 3,
        enabled: true,
        issuer: "idp".into(),
        subject: SubjectKind::Human,
        audience: "broker-at-idp".into(),
        required: [("subject".to_string(), "user-42".to_string())]
            .into_iter()
            .collect(),
        principal: PrincipalId([7; 16]),
        scope_ceiling: Action::Read.bit(),
        max_lifetime_secs: 300,
    }];
    let mut upstream_clients = BTreeMap::new();
    upstream_clients.insert("idp".to_string(), "broker-at-idp".to_string());
    let registry = coord_authn::Registry::new(vec![], coord_authn::JwksLimits::default()).unwrap();
    let verifier = coord_authn::WifVerifier::new(registry, BTreeMap::new());
    let mut sts = Sts::new(
        StsConfig {
            issuer: "https://sts".into(),
            resource: "tuplesky://c".into(),
            namespace: NS,
            max_token_lifetime_secs: 3600,
            session_window: 64,
            max_subject_token_bytes: 8192,
        },
        verifier,
        rules,
        ring,
    );
    let identity = coord_login::http::verified(&identity(&["broker-at-idp"], None), "idp");
    let second = Digest32([0xc1; 32]);
    d.apply(&InternalCommand::CommitGrant {
        namespace: NS,
        commitment: second,
        kind: GrantKind::Code,
    });
    let clock = ClockHealth::healthy(NOW, 5);
    let first = sts
        .issue(
            &identity,
            Some(second),
            None,
            None,
            &clock,
            &[0x31; 32],
            &mut d,
        )
        .unwrap();
    assert_eq!(first.expires_in, 300);
    assert_eq!(
        sts.issue(
            &identity,
            Some(second),
            None,
            None,
            &clock,
            &[0x32; 32],
            &mut d
        ),
        Err(ExchangeError::InvalidGrant("grant already consumed"))
    );
    assert_eq!(sts.issued, 1);
}

#[test]
fn a_parameterized_callback_keeps_its_own_query_and_a_denial_reaches_the_client() {
    let mut l = ServiceLogin::new(
        LoginLimits::default(),
        vec![Registration {
            client_id: "coordctl".into(),
            // A client whose loopback callback already carries a
            // parameter of its own.
            redirect_uris: vec!["http://127.0.0.1:4100/callback?tab=7".into()],
            upstream: "idp".into(),
        }],
        [("idp".to_string(), "broker-at-idp".to_string())]
            .into_iter()
            .collect(),
    );
    let mut request = start(1, "http://127.0.0.1:4100/callback?tab=7");
    request.state = "st".into();
    let started = l.start(NOW, &request, &[1; 32]).unwrap();
    let approved = l
        .approve(
            NOW,
            &started.upstream_state,
            identity(&["broker-at-idp"], None),
            IDP,
            &[2; 32],
        )
        .unwrap();
    // The client's own parameter survives, and the response is appended.
    assert!(
        approved
            .redirect
            .starts_with("http://127.0.0.1:4100/callback?tab=7&"),
        "{}",
        approved.redirect
    );
    assert!(approved.redirect.contains("code="));
    assert!(approved.redirect.contains("state=st"));

    // An upstream refusal reaches the waiting client rather than only
    // the browser.
    let started = l
        .start(
            NOW,
            &start(2, "http://127.0.0.1:4100/callback?tab=7"),
            &[3; 32],
        )
        .unwrap();
    let redirect = l
        .upstream_denied(&started.upstream_state, "consent_required")
        .unwrap();
    assert!(redirect.starts_with("http://127.0.0.1:4100/callback?tab=7&"));
    assert!(redirect.contains("error=access_denied"));
    assert!(redirect.contains("error_description=consent_required"));
    assert!(redirect.contains("state=tab-2"));
    assert_eq!(l.denied, 1);
    // The state is spent: a second callback for it decides nothing.
    assert!(
        l.upstream_denied(&started.upstream_state, "consent_required")
            .is_err()
    );
    assert!(
        l.approve(
            NOW,
            &started.upstream_state,
            identity(&["broker-at-idp"], None),
            IDP,
            &[4; 32]
        )
        .is_err(),
        "a refused login cannot then be approved"
    );
}
