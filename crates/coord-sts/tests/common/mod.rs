//! Shared fixtures: an issuer that signs Kubernetes assertions, the STS
//! signing key, trust rules and a session creator over the model engine.
#![allow(dead_code)]

use std::collections::BTreeMap;

use coord_authn::{
    IssuerConfig, JwksLimits, KubernetesMode, Registry, SubjectKind, TrustRuleConfig, WifVerifier,
    WorkloadKind,
};
use coord_core::effect::BootId;
use coord_core::outbox::BarrierAllocator;
use coord_state::policy::{Action, TrustRule};
use coord_state::{InternalCommand, PlanLimits, Response, plan_internal};
use coord_storage::materialize::{ApplyOutcome, apply_plan};
use coord_storage::views::{ViewBudget, build_internal_view};
use coord_storage::{GroupLimits, StoreWorker};
use coord_store_testkit::model::ModelEngine;
use coord_sts::{CreatorError, KeyRing, SessionCreator, SigningKey, Sts, StsConfig};
use coord_types::ids::*;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use rcgen::KeyPair;
use serde_json::json;

pub const NS: NamespaceId = NamespaceId([5; 16]);
pub const AUD: &str = "tuplesky-exchange";
pub const RESOURCE: &str = "tuplesky://cluster-1";
pub const RULE: TrustRuleId = TrustRuleId([9; 16]);
pub const PRINCIPAL: PrincipalId = PrincipalId([7; 16]);

pub struct Issuer {
    pub name: String,
    pub iss: String,
    pub kid: String,
    pub enc: EncodingKey,
    pub jwks: Vec<u8>,
}

pub fn issuer(name: &str, iss: &str, kid: &str) -> Issuer {
    let key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let point = key.public_key_raw();
    let jwks = json!({"keys": [{
        "kty": "EC", "crv": "P-256", "kid": kid, "alg": "ES256", "use": "sig",
        "x": coord_sts::keys::b64url(&point[1..33]), "y": coord_sts::keys::b64url(&point[33..65]),
    }]});
    Issuer {
        name: name.into(),
        iss: iss.into(),
        kid: kid.into(),
        enc: EncodingKey::from_ec_der(&key.serialize_der()),
        jwks: serde_json::to_vec(&jwks).unwrap(),
    }
}

pub fn k8s_issuer() -> Issuer {
    issuer("k8s", "https://kubernetes.default.svc", "kk")
}

/// A Kubernetes service-account assertion for `prod/kine`.
pub fn assertion(i: &Issuer, now: u64, exp: u64) -> String {
    let mut h = Header::new(Algorithm::ES256);
    h.kid = Some(i.kid.clone());
    let claims = json!({
        "iss": i.iss, "sub": "system:serviceaccount:prod:kine", "aud": AUD,
        "exp": exp, "iat": now,
        "kubernetes.io": {"namespace": "prod", "serviceaccount": {"name": "kine"}},
    });
    encode(&h, &claims, &i.enc).unwrap()
}

pub fn verifier(i: &Issuer, jwks_url: &str, mode: KubernetesMode) -> WifVerifier {
    let config = IssuerConfig {
        name: i.name.clone(),
        issuer: i.iss.clone(),
        jwks_url: jwks_url.into(),
        algorithms: vec![Algorithm::ES256],
        audiences: vec![AUD.into()],
        max_age_secs: Some(3600),
        allow_insecure_loopback: true,
    };
    let registry = Registry::new(vec![config], JwksLimits::default()).unwrap();
    let mut kinds = BTreeMap::new();
    kinds.insert(i.name.clone(), WorkloadKind::Kubernetes(mode));
    WifVerifier::new(registry, kinds)
}

pub fn rule(generation: u64, max_lifetime_secs: u64) -> TrustRuleConfig {
    TrustRuleConfig {
        id: RULE,
        generation,
        enabled: true,
        issuer: "k8s".into(),
        subject: SubjectKind::KubernetesServiceAccount,
        audience: AUD.into(),
        required: [("namespace", "prod"), ("serviceaccount", "kine")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        principal: PRINCIPAL,
        scope_ceiling: Action::Read.bit() | Action::Write.bit() | Action::Delete.bit(),
        max_lifetime_secs,
    }
}

pub fn signing_key(kid: &str) -> SigningKey {
    let key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    SigningKey::from_pkcs8_der(kid, &key.serialize_der()).unwrap()
}

pub fn config(max_token_lifetime_secs: u64) -> StsConfig {
    StsConfig {
        issuer: "https://sts.cluster-1".into(),
        resource: RESOURCE.into(),
        namespace: NS,
        max_token_lifetime_secs,
        session_window: 64,
        max_subject_token_bytes: 8192,
    }
}

pub fn sts(i: &Issuer, jwks_url: &str, mode: KubernetesMode, max_lifetime: u64) -> Sts {
    Sts::new(
        config(max_lifetime),
        verifier(i, jwks_url, mode),
        vec![rule(3, 300)],
        KeyRing::new(signing_key("sts-1")),
    )
}

/// Replicated state stand-in: the model engine with the internal-command
/// planner, as the storage tests drive it.
pub struct Domain {
    pub worker: StoreWorker<ModelEngine>,
    pub alloc: BarrierAllocator,
    /// Commands applied, for inspection (receipts only, never tokens).
    pub applied: Vec<InternalCommand>,
    /// Fail every command with unavailability.
    pub down: bool,
    /// Disable the trust rule before the next consume (policy changed
    /// between verification and execution).
    pub disable_rule_before_consume: bool,
}

impl Domain {
    pub fn new() -> Self {
        let boot = BootId([1; 16]);
        let inc = ReplicaIncarnation::new(1).unwrap();
        let mut d = Domain {
            worker: StoreWorker::open(ModelEngine::new(), boot, inc, GroupLimits::default())
                .unwrap(),
            alloc: BarrierAllocator::new(inc, boot),
            applied: Vec::new(),
            down: false,
            disable_rule_before_consume: false,
        };
        d.apply(&InternalCommand::PutTrustRule {
            namespace: NS,
            rule: RULE,
            record: TrustRule {
                enabled: true,
                generation: 3,
            },
        });
        d
    }

    pub fn apply(&mut self, command: &InternalCommand) -> Response {
        let gated = self.worker.reader().snapshot().unwrap();
        let view = build_internal_view(&gated, command, ViewBudget::default()).unwrap();
        let planned = plan_internal(command, &view, &PlanLimits::default()).unwrap();
        drop(gated);
        match apply_plan(&mut self.worker, self.alloc.allocate(), NS, &planned, None).unwrap() {
            ApplyOutcome::Applied(_) => planned.response,
            other => panic!("{other:?}"),
        }
    }

    pub fn disable_rule(&mut self) {
        self.apply(&InternalCommand::PutTrustRule {
            namespace: NS,
            rule: RULE,
            record: TrustRule {
                enabled: false,
                generation: 3,
            },
        });
    }
}

impl Default for Domain {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionCreator for Domain {
    fn create(&mut self, command: InternalCommand) -> Result<Response, CreatorError> {
        if self.down {
            return Err(CreatorError::Unavailable);
        }
        if self.disable_rule_before_consume {
            self.disable_rule_before_consume = false;
            self.disable_rule();
        }
        let response = self.apply(&command);
        self.applied.push(command);
        Ok(response)
    }
}
