//! CSR possession, request validation and policy-built issuance (design
//! Sections 10.1, 20.4).

use coord_authn::{ClockHealth, Decision, VerifyError, WifVerifier};
use rcgen::string::Ia5String;
use rcgen::{
    CertificateParams, CertificateSigningRequestParams, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyUsagePurpose, SanType,
};
use rustls_pki_types::{CertificateDer, CertificateSigningRequestDer};
use time::OffsetDateTime;

use crate::ca::Ca;
use crate::identity::node_uri;
use crate::policy::{RolePolicy, authorize};

/// A signed node certificate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Issued {
    /// The certificate DER.
    pub certificate: Vec<u8>,
    /// The node identity URI SAN.
    pub node_uri: String,
    /// Expiry (unix seconds).
    pub expires_at: u64,
}

/// A node enrollment request.
#[derive(Clone, Debug)]
pub struct NodeRequest {
    /// The WIF assertion proving the workload's platform identity.
    pub assertion: String,
    /// The CSR proving possession of the node key (DER).
    pub csr_der: Vec<u8>,
    /// Requested node identity (raw bytes).
    pub node: [u8; 16],
    /// Requested key generation / incarnation.
    pub incarnation: u64,
    /// Requested lifetime in seconds.
    pub lifetime_secs: u64,
}

/// Why enrollment failed. Bounded; never key material.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IssueError {
    /// The WIF assertion did not verify (or keys unavailable: fail closed).
    Assertion,
    /// The issuer's keys must be fetched first.
    KeysUnavailable {
        /// Issuer configuration name.
        name: String,
        /// Configured endpoint.
        jwks_url: String,
    },
    /// The clock is unhealthy: validity cannot be established.
    ClockUnhealthy,
    /// The CSR did not parse or its signature (proof of possession) failed.
    Csr,
    /// The CSR requests a CA certificate.
    CaRequested,
    /// The CSR requests key usages a node may not hold.
    ForbiddenUsage,
    /// No policy authorizes this node identity.
    Policy(crate::policy::PolicyError),
    /// The requested incarnation is not representable.
    BadIncarnation,
    /// Signing failed.
    Signing,
}

/// The node issuer.
pub struct NodeIssuer {
    verifier: WifVerifier,
    ca: Ca,
    rules: Vec<RolePolicy>,
    /// Certificates issued.
    pub issued: u64,
}

impl NodeIssuer {
    /// An issuer over a WIF verifier, a validated CA and role policies.
    pub const fn new(verifier: WifVerifier, ca: Ca, rules: Vec<RolePolicy>) -> Self {
        NodeIssuer {
            verifier,
            ca,
            rules,
            issued: 0,
        }
    }

    /// The verifier (key installation).
    pub const fn verifier_mut(&mut self) -> &mut WifVerifier {
        &mut self.verifier
    }

    /// The CA (trust anchor).
    pub const fn ca(&self) -> &Ca {
        &self.ca
    }

    /// Enroll a node at `clock`.
    pub fn enroll(
        &mut self,
        request: &NodeRequest,
        clock: &ClockHealth,
    ) -> Result<Issued, IssueError> {
        // Platform identity first.
        let identity = match self.verifier.verify(&request.assertion, clock, None) {
            Decision::Verified(i) => *i,
            Decision::NeedKeys { name, jwks_url } => {
                return Err(IssueError::KeysUnavailable { name, jwks_url });
            }
            Decision::Denied(VerifyError::Time(coord_authn::TimeError::ClockUnhealthy)) => {
                return Err(IssueError::ClockUnhealthy);
            }
            Decision::Denied(_) => return Err(IssueError::Assertion),
        };
        // Possession of the node key.
        let csr = CertificateSigningRequestParams::from_der(&CertificateSigningRequestDer::from(
            request.csr_der.clone(),
        ))
        .map_err(|_| IssueError::Csr)?;
        // Never a CA, never certificate-signing usage.
        if !matches!(csr.params.is_ca, IsCa::NoCa) {
            return Err(IssueError::CaRequested);
        }
        if csr
            .params
            .key_usages
            .iter()
            .any(|u| matches!(u, KeyUsagePurpose::KeyCertSign | KeyUsagePurpose::CrlSign))
        {
            return Err(IssueError::ForbiddenUsage);
        }
        let incarnation = coord_types::ids::ReplicaIncarnation::new(request.incarnation)
            .map_err(|_| IssueError::BadIncarnation)?;
        let node = coord_types::ids::ReplicaId(request.node);
        let policy = authorize(
            &self.rules,
            &identity,
            node,
            incarnation,
            request.lifetime_secs,
        )
        .map_err(IssueError::Policy)?;
        let uri = node_uri(&policy.identity);
        // Build the certificate from policy: node URI SAN plus endpoint
        // DNS, client-and-server EKU, digital-signature usage, not a CA.
        let mut params = CertificateParams::default();
        params
            .distinguished_name
            .push(DnType::CommonName, uri.clone());
        params.is_ca = IsCa::NoCa;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        params.use_authority_key_identifier_extension = true;
        let mut sans = vec![SanType::URI(
            Ia5String::try_from(uri.clone()).map_err(|_| IssueError::Signing)?,
        )];
        for dns in &policy.dns_names {
            sans.push(SanType::DnsName(
                Ia5String::try_from(dns.clone()).map_err(|_| IssueError::Signing)?,
            ));
        }
        params.subject_alt_names = sans;
        let expires_at = clock.now.saturating_add(policy.lifetime_secs);
        params.not_before = OffsetDateTime::from_unix_timestamp(clock.now as i64)
            .map_err(|_| IssueError::Signing)?;
        params.not_after = OffsetDateTime::from_unix_timestamp(expires_at as i64)
            .map_err(|_| IssueError::Signing)?;
        let signed = params
            .signed_by(&csr.public_key, self.ca.issuer())
            .map_err(|_| IssueError::Signing)?;
        self.issued += 1;
        Ok(Issued {
            certificate: signed.der().to_vec(),
            node_uri: uri,
            expires_at,
        })
    }
}

/// The DER of a certificate, for callers that keep the typed form.
pub fn certificate_der(issued: &Issued) -> CertificateDer<'static> {
    CertificateDer::from(issued.certificate.clone())
}
