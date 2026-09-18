//! The process-local signing key ring (design Sections 9.3, 20.2, 20.4):
//! ES256 keys loaded from mounted PKCS#8 DER, never from replicated
//! state; rotation keeps the previous key published for verification
//! until it is retired; the JWKS carries public coordinates only.

use std::collections::BTreeMap;
use std::fmt;

use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use rustls_pki_types::PrivatePkcs8KeyDer;
use serde::Serialize;
use serde_json::{Value, json};

/// Lowercase base64url without padding.
pub fn b64url(bytes: &[u8]) -> String {
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

/// Why a key was not accepted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyError {
    /// Not a P-256 PKCS#8 key.
    InvalidKey,
    /// A key with this identifier is already loaded.
    DuplicateKid,
    /// No such key.
    UnknownKid,
    /// The active key cannot be retired.
    ActiveKey,
    /// Signing failed.
    Signing,
}

/// One ES256 signing key.
pub struct SigningKey {
    kid: String,
    encoding: EncodingKey,
    x: String,
    y: String,
}

impl SigningKey {
    /// Load a P-256 key from PKCS#8 DER under `kid`.
    pub fn from_pkcs8_der(kid: &str, der: &[u8]) -> Result<Self, KeyError> {
        let pair = rcgen::KeyPair::from_pkcs8_der_and_sign_algo(
            &PrivatePkcs8KeyDer::from(der.to_vec()),
            &rcgen::PKCS_ECDSA_P256_SHA256,
        )
        .map_err(|_| KeyError::InvalidKey)?;
        let point = pair.public_key_raw();
        if point.len() != 65 || point[0] != 4 {
            return Err(KeyError::InvalidKey);
        }
        Ok(SigningKey {
            kid: kid.to_string(),
            encoding: EncodingKey::from_ec_der(der),
            x: b64url(&point[1..33]),
            y: b64url(&point[33..65]),
        })
    }

    /// Key identifier.
    pub fn kid(&self) -> &str {
        &self.kid
    }

    /// The public JWK.
    pub fn jwk(&self) -> Value {
        json!({
            "kty": "EC", "crv": "P-256", "alg": "ES256", "use": "sig",
            "kid": self.kid, "x": self.x, "y": self.y,
        })
    }
}

impl fmt::Debug for SigningKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SigningKey")
            .field("kid", &self.kid)
            .field("private", &"<redacted>")
            .finish()
    }
}

/// The ring: one active signing key and the keys still published.
#[derive(Debug)]
pub struct KeyRing {
    active: String,
    keys: BTreeMap<String, SigningKey>,
}

impl KeyRing {
    /// A ring whose only key is `key`.
    pub fn new(key: SigningKey) -> Self {
        let active = key.kid.clone();
        let mut keys = BTreeMap::new();
        keys.insert(active.clone(), key);
        KeyRing { active, keys }
    }

    /// The active key identifier.
    pub fn active(&self) -> &str {
        &self.active
    }

    /// Published key identifiers.
    pub fn kids(&self) -> Vec<&str> {
        self.keys.keys().map(String::as_str).collect()
    }

    /// Rotate: `key` becomes active; the previous key stays published
    /// for verification until retired.
    pub fn rotate(&mut self, key: SigningKey) -> Result<(), KeyError> {
        if self.keys.contains_key(&key.kid) {
            return Err(KeyError::DuplicateKid);
        }
        self.active = key.kid.clone();
        self.keys.insert(key.kid.clone(), key);
        Ok(())
    }

    /// Retire a published key (never the active one).
    pub fn retire(&mut self, kid: &str) -> Result<(), KeyError> {
        if kid == self.active {
            return Err(KeyError::ActiveKey);
        }
        self.keys
            .remove(kid)
            .map(|_| ())
            .ok_or(KeyError::UnknownKid)
    }

    /// The public JWKS document.
    pub fn jwks(&self) -> Value {
        json!({ "keys": self.keys.values().map(SigningKey::jwk).collect::<Vec<_>>() })
    }

    /// Sign `claims` with the active key.
    pub fn sign<T: Serialize>(&self, claims: &T) -> Result<String, KeyError> {
        let key = &self.keys[&self.active];
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(key.kid.clone());
        header.typ = Some("at+jwt".into());
        encode(&header, claims, &key.encoding).map_err(|_| KeyError::Signing)
    }
}
