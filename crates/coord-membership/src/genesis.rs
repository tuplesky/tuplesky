//! The signed, immutable genesis manifest (design Sections 10.2, 20.4).
//! The manifest travels as an ES256 JWT so it is verified against a
//! pinned public key delivered through deployment trust; a larger epoch
//! or a discovery-node signature alone is never sufficient.

use std::collections::BTreeSet;

use coord_types::formats::Feature;
use coord_types::identity::{Digest32, HashDomain};
use coord_types::ids::{
    ClusterId, ConfigurationEpoch, DomainId, PrincipalId, ReplicaId, ReplicaIncarnation,
};
use jsonwebtoken::{
    Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, decode_header, encode,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One initial voter's committed identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoterSeed {
    /// Node identity (lowercase hex of the replica id).
    pub node: String,
    /// Committed key generation / incarnation.
    pub incarnation: u64,
    /// The voter's committed public key: base64url of the certificate's
    /// SubjectPublicKeyInfo DER.
    ///
    /// Without this, genesis names only an identity, and any
    /// certificate the issuer signs for that node at that incarnation is
    /// accepted as the voter, so an issuer that is compromised or merely
    /// tricked mints a peer of an existing cluster.
    pub public_key: String,
}

/// The genesis manifest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenesisManifest {
    /// Cluster identity (lowercase hex).
    pub cluster: String,
    /// Domain identity (lowercase hex).
    pub domain: String,
    /// Configuration epoch of the initial membership.
    pub epoch: u64,
    /// Exact initial voters.
    pub voters: Vec<VoterSeed>,
    /// Issuer trust anchors (base64url CA certificate DERs).
    pub issuer_roots: Vec<String>,
    /// The workload-identity trust rules the cluster starts with, as the
    /// admission policy serializes them.
    ///
    /// The rules decide which external credentials become sessions, so
    /// leaving them out of genesis left the founding admission policy
    /// outside the manifest's commitment: it could be set differently on
    /// each node, and nothing in the pinned digest would notice.
    pub wif_rules: Vec<Value>,
    /// Admin principal (lowercase hex).
    pub admin: String,
    /// Protocol/version policy identifier.
    pub protocol_version: u32,
    /// The features active from genesis, by stable name
    /// (`Feature::name`; task-d121, design Section 10.2).
    ///
    /// A feature that keeps a stalled domain going can't be activated
    /// once it has stalled: activation needs a leader. So a domain names
    /// such a feature here, under the manifest's signature, and every
    /// voter's `init` records it active with no activation round. Left
    /// out of the serialized manifest when empty, so a manifest naming
    /// none keeps the digest it had before this field existed.
    ///
    /// A manifest that names one carries [`FEATURES_PROTOCOL_VERSION`],
    /// and one naming none carries [`PROTOCOL_VERSION`]
    /// ([`GenesisManifest::check_protocol`]). A build from before this
    /// field drops it when it parses the manifest, and would verify the
    /// rest and pin another digest. That build accepts only
    /// [`PROTOCOL_VERSION`], so it refuses such a manifest at `init` and
    /// at start instead.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub features: Vec<String>,
}

/// Why a manifest was rejected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GenesisError {
    /// The token did not verify against the pinned key.
    Signature,
    /// Not ES256, or a key-location header.
    Algorithm,
    /// Malformed manifest.
    Malformed,
    /// A field did not parse (bad identity, empty voters, and so on).
    Invalid,
    /// The protocol version is not supported.
    UnsupportedProtocol {
        /// The manifest's version.
        version: u32,
    },
    /// The admin key is not a PEM-encoded P-256 public key.
    AdminKey,
    /// A manifest at [`FEATURES_PROTOCOL_VERSION`] that names no
    /// feature. Each version has one meaning: a manifest naming none
    /// carries [`PROTOCOL_VERSION`] (#329 review).
    VersionWithoutFeatures {
        /// The manifest's version.
        version: u32,
    },
    /// A feature the manifest names can't be active from genesis.
    Feature {
        /// The name as the manifest gives it.
        name: String,
        /// Why.
        reason: String,
    },
}

/// The protocol version this build's genesis manifests carry.
pub const PROTOCOL_VERSION: u32 = 1;

/// The protocol version a genesis manifest naming features carries
/// (task-d121). A build that predates `features` accepts only
/// [`PROTOCOL_VERSION`], so it refuses such a manifest rather than drop
/// the features and pin another digest; a manifest naming none keeps
/// [`PROTOCOL_VERSION`], with its encoding and pin unchanged.
pub const FEATURES_PROTOCOL_VERSION: u32 = 2;

/// The genesis protocol versions this build reads: what a `coordd`
/// built from this source passes to [`verify_genesis`], and what
/// [`sign_genesis`] checks a manifest against before it signs. A caller
/// modelling another build passes that build's versions instead.
pub const SUPPORTED_PROTOCOL_VERSIONS: &[u32] = &[PROTOCOL_VERSION, FEATURES_PROTOCOL_VERSION];

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex<const N: usize>(s: &str) -> Option<[u8; N]> {
    if s.len() != 2 * N {
        return None;
    }
    let mut out = [0u8; N];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        out[i] = u8::from_str_radix(std::str::from_utf8(chunk).ok()?, 16).ok()?;
    }
    Some(out)
}

impl GenesisManifest {
    /// Cluster.
    pub fn cluster_id(&self) -> Option<ClusterId> {
        Some(ClusterId(unhex(&self.cluster)?))
    }
    /// Domain.
    pub fn domain_id(&self) -> Option<DomainId> {
        Some(DomainId(unhex(&self.domain)?))
    }
    /// Epoch.
    pub fn config_epoch(&self) -> Option<ConfigurationEpoch> {
        ConfigurationEpoch::new(self.epoch).ok()
    }
    /// Admin principal.
    pub fn admin_principal(&self) -> Option<PrincipalId> {
        Some(PrincipalId(unhex(&self.admin)?))
    }
    /// A voter seed's replica identity and incarnation.
    pub fn voter(seed: &VoterSeed) -> Option<(ReplicaId, ReplicaIncarnation, Vec<u8>)> {
        Some((
            ReplicaId(unhex(&seed.node)?),
            ReplicaIncarnation::new(seed.incarnation).ok()?,
            b64url_decode(&seed.public_key)?,
        ))
    }

    /// The features active from genesis, or why one named can't be.
    ///
    /// Each name is a feature this build supports and activates through
    /// a prepare marker, named once, with every feature it rests on
    /// (`Feature::requires`) named too. A marker is never named: the
    /// record `init` writes carries each feature's marker with it.
    pub fn genesis_features(&self) -> Result<BTreeSet<Feature>, GenesisError> {
        let refused = |name: &str, reason: String| GenesisError::Feature {
            name: name.to_owned(),
            reason,
        };
        let mut out = BTreeSet::new();
        for name in &self.features {
            let Some(feature) = Feature::from_name(name) else {
                return Err(refused(name, "this build knows no such feature".into()));
            };
            if feature.is_marker() {
                return Err(refused(
                    name,
                    "a prepare marker is written with its feature, never named".into(),
                ));
            }
            if feature.prepare_marker().is_none() {
                return Err(refused(
                    name,
                    "it has no prepare marker, so it is not activated from genesis".into(),
                ));
            }
            if !coord_types::formats::Supported::supports(feature) {
                return Err(refused(name, "this build does not support it".into()));
            }
            if !out.insert(feature) {
                return Err(refused(name, "named twice".into()));
            }
        }
        for feature in &out {
            if let Some(required) = feature.requires()
                && !out.contains(&required)
            {
                return Err(refused(
                    feature.name(),
                    format!("it rests on {}, which is not named", required.name()),
                ));
            }
        }
        Ok(out)
    }

    /// A stable digest of the canonical manifest, used to pin it durably.
    pub fn digest(&self) -> Digest32 {
        let canonical = serde_json::to_vec(self).expect("serializable");
        HashDomain::AuthGrantCommitment.digest(&[b"genesis-manifest", &canonical])
    }

    /// The manifest's protocol version, checked against the versions the
    /// caller reads and against its features: the one rule the signer
    /// and every verifier apply (#329 review).
    ///
    /// - A version the caller doesn't read is
    ///   [`GenesisError::UnsupportedProtocol`].
    /// - A manifest naming a feature carries
    ///   [`FEATURES_PROTOCOL_VERSION`], which a build without features
    ///   refuses ([`GenesisError::Feature`] otherwise).
    /// - A manifest naming none carries [`PROTOCOL_VERSION`]
    ///   ([`GenesisError::VersionWithoutFeatures`] otherwise).
    pub fn check_protocol(&self, supported: &[u32]) -> Result<(), GenesisError> {
        if !supported.contains(&self.protocol_version) {
            return Err(GenesisError::UnsupportedProtocol {
                version: self.protocol_version,
            });
        }
        match self.features.first() {
            Some(first) if self.protocol_version != FEATURES_PROTOCOL_VERSION => {
                Err(GenesisError::Feature {
                    name: first.clone(),
                    reason: format!(
                        "a manifest naming a feature carries protocol version \
                         {FEATURES_PROTOCOL_VERSION}, which a build without features \
                         refuses; this one carries {}",
                        self.protocol_version
                    ),
                })
            }
            None if self.protocol_version != PROTOCOL_VERSION => {
                Err(GenesisError::VersionWithoutFeatures {
                    version: self.protocol_version,
                })
            }
            _ => Ok(()),
        }
    }

    fn validate(&self, supported: &[u32]) -> Result<(), GenesisError> {
        if self.cluster_id().is_none()
            || self.domain_id().is_none()
            || self.config_epoch().is_none()
            || self.admin_principal().is_none()
            || self.voters.is_empty()
            || self.issuer_roots.is_empty()
            || self.wif_rules.is_empty()
        {
            return Err(GenesisError::Invalid);
        }
        for seed in &self.voters {
            if Self::voter(seed).is_none() {
                return Err(GenesisError::Invalid);
            }
        }
        self.check_protocol(supported)?;
        self.genesis_features()?;
        Ok(())
    }
}

/// A signed manifest as delivered (an ES256 JWT).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedGenesis(pub String);

/// Sign a manifest with the admin ES256 key (provisioning trust).
///
/// A manifest whose version and features break
/// [`GenesisManifest::check_protocol`] for this build is refused before
/// it is signed. Signed, a feature-bearing manifest at
/// [`PROTOCOL_VERSION`] would be refused by a build with features and
/// read without them by one from before: a majority of older voters could
/// serve with the features off (#329 review).
pub fn sign_genesis(
    manifest: &GenesisManifest,
    key: &EncodingKey,
) -> Result<SignedGenesis, GenesisError> {
    manifest.check_protocol(SUPPORTED_PROTOCOL_VERSIONS)?;
    let mut header = Header::new(Algorithm::ES256);
    header.typ = Some("genesis+jwt".into());
    let mut claims = serde_json::to_value(manifest).map_err(|_| GenesisError::Malformed)?;
    // A JWT needs registered claims for the verifier's required set.
    if let Value::Object(map) = &mut claims {
        map.insert("iss".into(), Value::from("tuplesky-genesis"));
        map.insert("sub".into(), Value::from(manifest.cluster.clone()));
        map.insert("aud".into(), Value::from(manifest.domain.clone()));
        map.insert("exp".into(), Value::from(u64::MAX / 2));
    }
    encode(&header, &claims, key)
        .map(SignedGenesis)
        .map_err(|_| GenesisError::Signature)
}

/// Sign a manifest with the admin key given as PEM (PKCS#8 `PRIVATE
/// KEY`, P-256): what provisioning tools hold.
pub fn sign_genesis_pem(
    manifest: &GenesisManifest,
    private_pem: &[u8],
) -> Result<SignedGenesis, GenesisError> {
    use rustls_pki_types::PrivatePkcs8KeyDer;
    use rustls_pki_types::pem::PemObject;
    let der =
        PrivatePkcs8KeyDer::from_pem_slice(private_pem).map_err(|_| GenesisError::AdminKey)?;
    sign_genesis(manifest, &EncodingKey::from_ec_der(der.secret_pkcs8_der()))
}

/// The admin public key a manifest is verified against, from its PEM
/// encoding (`PUBLIC KEY`, P-256).
///
/// Exactly one PEM section, and it is the key: a file that carries
/// another section beside it, or bytes after the key's own encoding, is
/// not taken to mean whichever part a parser happens to read first.
pub fn admin_key_from_pem(pem: &[u8]) -> Result<DecodingKey, GenesisError> {
    use rustls_pki_types::SubjectPublicKeyInfoDer;
    use rustls_pki_types::pem::PemObject;
    use x509_parser::prelude::FromDer;
    // id-ecPublicKey over prime256v1: the only key ES256 verifies with.
    const EC_PUBLIC_KEY: &str = "1.2.840.10045.2.1";
    const P256: &str = "1.2.840.10045.3.1.7";
    let text = core::str::from_utf8(pem).map_err(|_| GenesisError::AdminKey)?;
    let sections: Vec<&str> = text
        .lines()
        .filter_map(|line| line.trim().strip_prefix("-----BEGIN "))
        .collect();
    if sections != ["PUBLIC KEY-----"] {
        return Err(GenesisError::AdminKey);
    }
    let der = SubjectPublicKeyInfoDer::from_pem_slice(pem).map_err(|_| GenesisError::AdminKey)?;
    let (rest, spki) = x509_parser::x509::SubjectPublicKeyInfo::from_der(&der)
        .map_err(|_| GenesisError::AdminKey)?;
    if !rest.is_empty() {
        return Err(GenesisError::AdminKey);
    }
    let curve = spki
        .algorithm
        .parameters
        .as_ref()
        .and_then(|p| p.as_oid().ok())
        .map(|oid| oid.to_id_string());
    let point = &spki.subject_public_key.data;
    if spki.algorithm.algorithm.to_id_string() != EC_PUBLIC_KEY
        || curve.as_deref() != Some(P256)
        || point.len() != 65
        || point[0] != 0x04
    {
        return Err(GenesisError::AdminKey);
    }
    DecodingKey::from_ec_components(&b64url(&point[1..33]), &b64url(&point[33..65]))
        .map_err(|_| GenesisError::AdminKey)
}

/// Verify a delivered manifest against the pinned public key.
///
/// `supported` is every protocol version the caller reads: a node built
/// from this source passes [`SUPPORTED_PROTOCOL_VERSIONS`]. The manifest
/// must satisfy [`GenesisManifest::check_protocol`] against it.
pub fn verify_genesis(
    signed: &SignedGenesis,
    pinned: &DecodingKey,
    supported: &[u32],
) -> Result<GenesisManifest, GenesisError> {
    let header = decode_header(&signed.0).map_err(|_| GenesisError::Malformed)?;
    if header.alg != Algorithm::ES256
        || header.jku.is_some()
        || header.x5u.is_some()
        || header.jwk.is_some()
    {
        return Err(GenesisError::Algorithm);
    }
    let mut validation = Validation::new(Algorithm::ES256);
    validation.validate_exp = false;
    validation.validate_aud = false;
    validation.required_spec_claims.clear();
    let data = decode::<GenesisManifest>(&signed.0, pinned, &validation)
        .map_err(|_| GenesisError::Signature)?;
    data.claims.validate(supported)?;
    Ok(data.claims)
}

/// The base64url encoding of bytes (issuer roots).
pub fn b64url(bytes: &[u8]) -> String {
    coord_node_issuer_b64::encode(bytes)
}

/// Decode a base64url issuer root.
pub fn b64url_decode(s: &str) -> Option<Vec<u8>> {
    coord_node_issuer_b64::decode(s)
}

mod coord_node_issuer_b64 {
    pub fn encode(bytes: &[u8]) -> String {
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
    pub fn decode(s: &str) -> Option<Vec<u8>> {
        let rev = |c: u8| match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'-' => Some(62),
            b'_' => Some(63),
            _ => None,
        };
        let mut out = Vec::new();
        let mut acc = 0u32;
        let mut bits = 0;
        for &c in s.as_bytes() {
            let v = rev(c)?;
            acc = (acc << 6) | u32::from(v);
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                out.push((acc >> bits) as u8);
            }
        }
        Some(out)
    }
}

/// A helper for hex identities.
pub fn hex_id(bytes: &[u8]) -> String {
    hex(bytes)
}
