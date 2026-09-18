//! The protected reference CA (design Sections 10.2, 20.4): a PKCS#8
//! signing key and its certificate, loaded from protected mounts and
//! validated at startup. Signing alone does not prove the key matches
//! the certificate or that the certificate is a usable CA; both are
//! checked here.

use rcgen::{Issuer, KeyPair, PublicKeyData};
use rustls_pki_types::{CertificateDer, PrivatePkcs8KeyDer};

/// Why the CA could not be loaded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CaError {
    /// The signing key is not a supported PKCS#8 key.
    BadKey,
    /// The certificate did not parse.
    BadCertificate,
    /// The certificate is not a CA (no basic-constraints CA bit).
    NotCa,
    /// The certificate cannot sign certificates (no keyCertSign usage).
    CannotSign,
    /// The certificate has expired or is not yet valid.
    Expired,
    /// The signing key does not match the certificate's public key.
    KeyMismatch,
}

/// The loaded, validated CA.
pub struct Ca {
    issuer: Issuer<'static, KeyPair>,
    cert_der: CertificateDer<'static>,
}

impl Ca {
    /// Load and validate an ECDSA P-256 CA from its certificate DER and
    /// PKCS#8 key DER, at `now` (unix seconds).
    pub fn load(cert_der: &[u8], key_pkcs8_der: &[u8], now: u64) -> Result<Self, CaError> {
        let key = KeyPair::from_pkcs8_der_and_sign_algo(
            &PrivatePkcs8KeyDer::from(key_pkcs8_der.to_vec()),
            &rcgen::PKCS_ECDSA_P256_SHA256,
        )
        .map_err(|_| CaError::BadKey)?;
        let (_, x509) =
            x509_parser::parse_x509_certificate(cert_der).map_err(|_| CaError::BadCertificate)?;
        if !x509.is_ca() {
            return Err(CaError::NotCa);
        }
        match x509.key_usage() {
            Ok(Some(ku)) if ku.value.key_cert_sign() => {}
            _ => return Err(CaError::CannotSign),
        }
        let validity = x509.validity();
        let now_asn1 = x509_parser::time::ASN1Time::from_timestamp(now as i64)
            .map_err(|_| CaError::Expired)?;
        if !validity.is_valid_at(now_asn1) {
            return Err(CaError::Expired);
        }
        // The signing key must match the certificate: sign a probe and
        // check the certificate's public key verifies it, cheaply by
        // comparing the SubjectPublicKeyInfo bytes.
        if x509.public_key().raw != key.subject_public_key_info() {
            return Err(CaError::KeyMismatch);
        }
        let owned = CertificateDer::from(cert_der.to_vec());
        let issuer = Issuer::from_ca_cert_der(&owned, key).map_err(|_| CaError::BadCertificate)?;
        Ok(Ca {
            issuer,
            cert_der: owned,
        })
    }

    /// The issuer for signing.
    pub const fn issuer(&self) -> &Issuer<'static, KeyPair> {
        &self.issuer
    }

    /// The CA certificate DER (the trust anchor peers pin).
    pub fn certificate(&self) -> &CertificateDer<'static> {
        &self.cert_der
    }
}
