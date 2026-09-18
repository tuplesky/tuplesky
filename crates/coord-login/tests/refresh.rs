//! task-40 acceptance on the broker: a login binds a refresh family; a
//! refresh rotates the secret and signs a new token; presenting a
//! retired secret (reuse, or a retry after a lost rotation response)
//! revokes the family and retires the session, so only a fresh login
//! recovers; concurrent refreshes with one secret let exactly one
//! through; logout retires the session.

use std::collections::BTreeMap;

use coord_authn::{ClockHealth, SubjectKind, TrustRuleConfig};
use coord_core::effect::BootId;
use coord_core::outbox::BarrierAllocator;
use coord_login::{
    RefreshError, SessionReader, UpstreamIdentity, logout, new_family, parse_refresh_token,
    refresh, refresh_token,
};
use coord_state::plan::Outcome;
use coord_state::policy::{Action, GrantKind, GrantRecord, SessionRecord, TrustRule};
use coord_state::{InternalCommand, PlanLimits, Response, plan_internal};
use coord_storage::materialize::{ApplyOutcome, apply_plan};
use coord_storage::views::{ViewBudget, build_internal_view};
use coord_storage::{GroupLimits, StoreWorker, codecs};
use coord_store_api::engine::OrderedRead;
use coord_store_api::registry::Collection;
use coord_store_testkit::model::ModelEngine;
use coord_sts::{
    CreatorError, KeyRing, SessionCreator, SigningKey, Sts, StsConfig, verify_service_token,
};
use coord_types::identity::Digest32;
use coord_types::ids::*;

const NS: NamespaceId = NamespaceId([5; 16]);
const NOW: u64 = 1_700_000_000;

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

impl SessionReader for Domain {
    fn session(&self, session: &SessionId) -> Result<Option<SessionRecord>, CreatorError> {
        let gated = self.worker.reader().snapshot().unwrap();
        Ok(gated
            .view()
            .get(Collection::SessionV1.id(), &codecs::session_key(session))
            .unwrap()
            .map(|b| codecs::decode_session(&b).unwrap()))
    }
    fn grant(&self, commitment: &Digest32) -> Result<Option<GrantRecord>, CreatorError> {
        let gated = self.worker.reader().snapshot().unwrap();
        Ok(gated
            .view()
            .get(Collection::AuthGrantV1.id(), &codecs::grant_key(commitment))
            .unwrap()
            .map(|b| codecs::decode_grant(&b).unwrap()))
    }
}

fn sts() -> Sts {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let ring = KeyRing::new(SigningKey::from_pkcs8_der("sts-1", &key.serialize_der()).unwrap());
    let rules = vec![TrustRuleConfig {
        id: TrustRuleId([9; 16]),
        generation: 3,
        enabled: true,
        issuer: "idp".into(),
        subject: SubjectKind::Human,
        audience: "broker".into(),
        required: BTreeMap::new(),
        principal: PrincipalId([7; 16]),
        scope_ceiling: Action::Read.bit() | Action::Write.bit(),
        max_lifetime_secs: 300,
    }];
    let registry = coord_authn::Registry::new(vec![], coord_authn::JwksLimits::default()).unwrap();
    Sts::new(
        StsConfig {
            issuer: "https://sts".into(),
            resource: "tuplesky://c".into(),
            namespace: NS,
            max_token_lifetime_secs: 120,
            session_window: 64,
            max_subject_token_bytes: 8192,
        },
        coord_authn::WifVerifier::new(registry, BTreeMap::new()),
        rules,
        ring,
    )
}

fn identity() -> coord_authn::VerifiedIdentity {
    coord_login::http::verified(
        &UpstreamIdentity {
            issuer: "https://idp.example".into(),
            subject: "user-42".into(),
            audiences: vec!["broker".into()],
            authorized_party: None,
            expires_at: NOW + 600,
        },
        "idp",
    )
}

/// A login that binds a family, as the handlers do.
fn login(sts: &mut Sts, d: &mut Domain, entropy: u8) -> (String, SessionId) {
    let (secret, family) = new_family(&[entropy; 32]);
    assert_eq!(
        d.apply(&InternalCommand::CommitGrant {
            namespace: NS,
            commitment: family,
            kind: GrantKind::RefreshFamily,
        })
        .outcome,
        Outcome::GrantCommitted
    );
    let code = Digest32([entropy.wrapping_add(100); 32]);
    d.apply(&InternalCommand::CommitGrant {
        namespace: NS,
        commitment: code,
        kind: GrantKind::Code,
    });
    let clock = ClockHealth::healthy(NOW, 5);
    let issued = sts
        .issue(
            &identity(),
            Some(code),
            Some(family),
            None,
            &clock,
            &[entropy.wrapping_add(50); 32],
            d,
        )
        .unwrap();
    let session = SessionId([entropy.wrapping_add(50); 16]);
    assert_eq!(d.grant(&family).unwrap().unwrap().session, Some(session));
    let _ = issued;
    (refresh_token(&family, &secret), session)
}

#[test]
fn refresh_rotates_and_reuse_revokes_the_family_and_session() {
    let mut sts = sts();
    let mut d = Domain::new();
    let (token, session) = login(&mut sts, &mut d, 1);
    let (family, first_secret) = parse_refresh_token(&token).unwrap();
    // A refresh rotates the secret and signs a new token for the session.
    let clock = ClockHealth::healthy(NOW + 60, 5);
    let r = refresh(&token, NS, &clock, &[0x77; 32], &mut sts, &mut d).unwrap();
    let next = r.refresh_token.clone().unwrap();
    let (family2, second_secret) = parse_refresh_token(&next).unwrap();
    assert_eq!(family2, family, "same family");
    assert_ne!(second_secret, first_secret, "rotated secret");
    let claims = verify_service_token(
        &r.access_token,
        &sts.ring().jwks(),
        "https://sts",
        "tuplesky://c",
        &clock,
    )
    .unwrap();
    assert_eq!(
        claims.sid,
        session
            .0
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );
    assert_eq!(
        claims.exp,
        NOW + 60 + 120,
        "the STS lifetime bounds a renewed token"
    );
    assert_eq!(d.grant(&family).unwrap().unwrap().generation, 1);
    // The session is still active.
    assert!(d.session(&session).unwrap().unwrap().active);
    // Reuse of the retired secret (the lost rotation response case: the
    // client retries with what it has) revokes the family and retires
    // the session at that position: only a fresh login recovers.
    assert_eq!(
        refresh(&token, NS, &clock, &[0x78; 32], &mut sts, &mut d),
        Err(RefreshError::FamilyRevoked)
    );
    assert!(
        !d.session(&session).unwrap().unwrap().active,
        "session retired"
    );
    assert_eq!(
        refresh(&next, NS, &clock, &[0x79; 32], &mut sts, &mut d),
        Err(RefreshError::UnknownFamily),
        "the current secret is worthless once the family is revoked"
    );
    // Malformed and unknown tokens.
    assert_eq!(
        refresh("nope", NS, &clock, &[0; 32], &mut sts, &mut d),
        Err(RefreshError::Malformed)
    );
    let stranger = refresh_token(&Digest32([0xee; 32]), &"ab".repeat(32));
    assert_eq!(
        refresh(&stranger, NS, &clock, &[0; 32], &mut sts, &mut d),
        Err(RefreshError::UnknownFamily)
    );
}

#[test]
fn concurrent_refreshes_let_exactly_one_through_and_logout_retires() {
    let mut sts = sts();
    let mut d = Domain::new();
    let (token, session) = login(&mut sts, &mut d, 2);
    let clock = ClockHealth::healthy(NOW + 10, 5);
    // Two clients sharing one secret refresh at once: the first rotates,
    // the second presents a retired secret and revokes everything.
    let first = refresh(&token, NS, &clock, &[0x11; 32], &mut sts, &mut d);
    let second = refresh(&token, NS, &clock, &[0x12; 32], &mut sts, &mut d);
    assert!(first.is_ok());
    assert_eq!(second, Err(RefreshError::FamilyRevoked));
    assert!(!d.session(&session).unwrap().unwrap().active);
    // A fresh login and an explicit logout.
    let (token, session) = login(&mut sts, &mut d, 3);
    logout(&token, NS, &mut d).unwrap();
    assert!(!d.session(&session).unwrap().unwrap().active);
    assert_eq!(
        logout(&token, NS, &mut d),
        Err(RefreshError::SessionRetired)
    );
    assert_eq!(
        refresh(&token, NS, &clock, &[0x13; 32], &mut sts, &mut d),
        Err(RefreshError::Exchange(
            coord_sts::ExchangeError::InvalidGrant("session retired")
        ))
    );
    // Secrets never enter replicated rows: only commitments.
    let grant = d
        .grant(&parse_refresh_token(&token).unwrap().0)
        .unwrap()
        .unwrap();
    let rendered = format!("{grant:?}");
    assert!(!rendered.contains(&parse_refresh_token(&token).unwrap().1));
}
