//! The harness credential endpoint (design Sections 9.1-9.4, 19.4).
//!
//! It answers `POST /token` with a service token the provisioned
//! domain's frontends verify, and publishes the matching JWKS. That is
//! deliberately *less* than `coord-sts` does: the real exchange verifies
//! an external assertion with the hardened verifiers of `coord-authn`,
//! maps it through trust rules, and creates the session as a replicated
//! command before it signs anything. This one signs on presentation of
//! any non-empty assertion.
//!
//! The distinction matters and is not papered over. What the Kubernetes
//! certification measures is the storage edge: the API server through
//! Kine through the native frontend into consensus and the store. The
//! token exchange is task-35 and task-36's to qualify, and it has its
//! own tests. Putting a real identity provider in the certification job
//! would make the job's failures ambiguous without making the edge any
//! better tested. What the harness does *not* do is weaken the verifying
//! side: the token it mints is a real ES256 service credential with real
//! claims, and the frontend that accepts it runs the same verification
//! it runs in production.
//!
//! Consequently this endpoint binds loopback, and it is in a `test-only`
//! crate that no production artifact can reach. The one exception is a
//! domain provisioned for several hosts (task-d04), whose Kine build may
//! run on a host other than the endpoint's: there it may bind the host
//! it was provisioned for, and only that. The opt-in is made when the
//! domain is provisioned, because that is when the endpoint's
//! certificate is issued -- a bind address given later could not change
//! what the certificate names, and the check below is against the
//! certificate, not against a setting anyone can edit.

use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rustls::ServerConnection;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

use crate::domain::Provisioned;

/// How long a minted service token is valid.
const LIFETIME_SECS: u64 = 3600;

/// The signing half, without a listener in front of it.
///
/// A benchmark driving the native path needs a credential and has no
/// reason to make an HTTP request for it; the endpoint below is the same
/// minting, reached over the wire a Kine build uses.
pub struct Minter {
    ring: coord_sts::KeyRing,
    claims: Claims,
    minted: AtomicU32,
}

/// Everything the endpoint needs, loaded once.
pub struct Endpoint {
    minter: Minter,
    tls: Arc<rustls::ServerConfig>,
    listener: TcpListener,
}

struct Claims {
    issuer: String,
    resource: String,
    principal: String,
    rule: String,
}

/// What went wrong before the endpoint was listening.
#[derive(Debug)]
pub enum IssuerError {
    /// A file the endpoint needs is missing or unreadable.
    Io(std::io::Error),
    /// The signing key or its published form is not usable.
    Key(String),
    /// The TLS material is not usable.
    Tls(String),
    /// The endpoint was asked to bind something other than loopback or
    /// the host its certificate was provisioned for.
    NotLoopback(String),
}

impl std::fmt::Display for IssuerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IssuerError::Io(e) => write!(f, "{e}"),
            IssuerError::Key(e) => write!(f, "signing key: {e}"),
            IssuerError::Tls(e) => write!(f, "tls: {e}"),
            IssuerError::NotLoopback(a) => write!(
                f,
                "the harness issuer binds loopback, or the host it was provisioned \
                 for with --issuer-listen, not {a}"
            ),
        }
    }
}

impl std::error::Error for IssuerError {}

impl From<std::io::Error> for IssuerError {
    fn from(e: std::io::Error) -> Self {
        IssuerError::Io(e)
    }
}

fn b64url_decode(text: &str) -> Option<Vec<u8>> {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut bits = 0u32;
    let mut have = 0u32;
    let mut out = Vec::new();
    for byte in text.trim().bytes() {
        let value = A.iter().position(|c| *c == byte)? as u32;
        bits = (bits << 6) | value;
        have += 6;
        if have >= 8 {
            have -= 8;
            out.push((bits >> have) as u8);
        }
    }
    Some(out)
}

impl Minter {
    /// Load the provisioned signing key and the claims it signs.
    pub fn load(dir: &Path) -> Result<Self, IssuerError> {
        Minter::of(&Provisioned::read(dir)?)
    }

    /// The same, for a caller that already read the description.
    pub fn of(provisioned: &Provisioned) -> Result<Self, IssuerError> {
        let encoded = std::fs::read_to_string(&provisioned.issuer.signing_key)?;
        let der = b64url_decode(&encoded)
            .ok_or_else(|| IssuerError::Key("the stored key is not base64url".into()))?;
        let ring = coord_sts::KeyRing::new(
            coord_sts::SigningKey::from_pkcs8_der("tuplesky-harness-1", &der)
                .map_err(|e| IssuerError::Key(format!("{e:?}")))?,
        );
        Ok(Minter {
            ring,
            claims: Claims {
                issuer: provisioned.issuer_claim.clone(),
                resource: provisioned.resource.clone(),
                principal: provisioned.principal.clone(),
                rule: provisioned.trust_rule.clone(),
            },
            minted: AtomicU32::new(0),
        })
    }

    /// The published verification keys.
    pub fn jwks(&self) -> serde_json::Value {
        self.ring.jwks()
    }

    /// Mint one service token, and say which session it names.
    ///
    /// The session identifier is part of the answer because a caller
    /// that has to construct its own client instance needs it, and
    /// reading it back out of the credential would mean parsing a token
    /// this process just signed.
    pub fn mint_token(&self) -> Option<(String, [u8; 16])> {
        let issued = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
        let ordinal = self.minted.fetch_add(1, Ordering::Relaxed);
        let session = session_id(ordinal);
        let token = self
            .ring
            .sign(&coord_sts::ServiceClaims {
                iss: self.claims.issuer.clone(),
                sub: self.claims.principal.clone(),
                aud: self.claims.resource.clone(),
                sid: hex(&session),
                scope: 0xffff,
                rule: self.claims.rule.clone(),
                generation: 1,
                jti: hex(&jti(ordinal)),
                iat: issued,
                exp: issued + LIFETIME_SECS,
            })
            .ok()?;
        Some((token, session))
    }
}

impl Endpoint {
    /// Load the provisioned material and bind the endpoint where it was
    /// provisioned to listen.
    pub fn bind(dir: &Path) -> Result<Self, IssuerError> {
        Endpoint::bind_on(dir, None)
    }

    /// The same, binding `listen` instead of the provisioned listener
    /// when one is given: `coord-harness issuer --listen`.
    ///
    /// Whatever is asked, the bind is refused unless [`permitted`] allows
    /// it: loopback, or the host the certificate was provisioned for.
    pub fn bind_on(dir: &Path, listen: Option<SocketAddr>) -> Result<Self, IssuerError> {
        let provisioned = Provisioned::read(dir)?;
        let address = match listen {
            Some(address) => address,
            None => provisioned
                .issuer
                .listen
                .parse()
                .map_err(|_| IssuerError::NotLoopback(provisioned.issuer.listen.clone()))?,
        };

        let chain: Vec<CertificateDer<'static>> =
            CertificateDer::pem_file_iter(&provisioned.issuer.certificate)
                .map_err(|e| IssuerError::Tls(format!("{e}")))?
                .collect::<Result<_, _>>()
                .map_err(|e| IssuerError::Tls(format!("{e}")))?;
        let leaf = chain
            .first()
            .ok_or_else(|| IssuerError::Tls("the endpoint has no certificate".into()))?;
        permitted(address, &provisioned.issuer.url, leaf)?;
        let minter = Minter::of(&provisioned)?;
        let key = PrivateKeyDer::from_pem_file(&provisioned.issuer.key)
            .map_err(|e| IssuerError::Tls(format!("{e}")))?;
        let mut tls = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .map_err(|e| IssuerError::Tls(format!("{e}")))?;
        // HTTP/1.1 only, and said so: a client that would otherwise
        // negotiate h2 gets a protocol this endpoint actually speaks
        // instead of a hang.
        tls.alpn_protocols = vec![b"http/1.1".to_vec()];

        let listener = TcpListener::bind(address)?;
        Ok(Endpoint {
            minter,
            tls: Arc::new(tls),
            listener,
        })
    }

    /// The address it bound.
    pub fn address(&self) -> std::io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    /// Serve until the process is stopped.
    pub fn serve(self: Arc<Self>) -> std::io::Result<()> {
        for stream in self.listener.incoming() {
            let stream = stream?;
            let endpoint = Arc::clone(&self);
            std::thread::spawn(move || {
                if let Err(err) = endpoint.answer(stream) {
                    // A client that hangs up mid-handshake is not an
                    // event; it is what a probe looks like.
                    eprintln!("coord-harness issuer: {err}");
                }
            });
        }
        Ok(())
    }

    fn answer(&self, stream: TcpStream) -> std::io::Result<()> {
        stream.set_nodelay(true)?;
        let connection =
            ServerConnection::new(Arc::clone(&self.tls)).map_err(std::io::Error::other)?;
        let mut tls = rustls::StreamOwned::new(connection, stream);
        let (method, path, body) = read_request(&mut tls)?;
        let (status, content_type, payload) = self.route(&method, &path, &body);
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            payload.len()
        );
        tls.write_all(response.as_bytes())?;
        tls.write_all(payload.as_bytes())?;
        tls.flush()?;
        tls.conn.send_close_notify();
        let _ = tls.flush();
        Ok(())
    }

    fn route(&self, method: &str, path: &str, body: &str) -> (&'static str, &'static str, String) {
        match (method, path) {
            ("GET", "/.well-known/jwks.json") => {
                ("200 OK", "application/json", self.minter.jwks().to_string())
            }
            ("POST", "/token") => match self.mint(body) {
                Some(token) => (
                    "200 OK",
                    "application/json",
                    format!(
                        "{{\"access_token\":\"{token}\",\"token_type\":\"Bearer\",\"issued_token_type\":\"urn:ietf:params:oauth:token-type:access_token\",\"expires_in\":{LIFETIME_SECS}}}"
                    ),
                ),
                None => (
                    "400 Bad Request",
                    "application/json",
                    "{\"error\":\"invalid_request\"}".to_owned(),
                ),
            },
            _ => (
                "404 Not Found",
                "application/json",
                "{\"error\":\"invalid_request\"}".to_owned(),
            ),
        }
    }

    /// Check the presented form, then mint. Every exchange gets its own
    /// session identifier: the binding establishes the session as a
    /// replicated command, so two callers that exchanged separately are
    /// two sessions, as they would be against a real issuer.
    fn mint(&self, body: &str) -> Option<String> {
        let mut subject = None;
        let mut resource = None;
        for pair in body.split('&') {
            let (name, value) = pair.split_once('=')?;
            match name {
                "subject_token" => subject = Some(value),
                "resource" | "audience" => resource = Some(form_decode(value)),
                _ => {}
            }
        }
        if subject.is_none_or(str::is_empty) {
            return None;
        }
        if let Some(asked) = resource
            && asked != self.minter.claims.resource
        {
            return None;
        }
        self.minter.mint_token().map(|(token, _)| token)
    }
}

/// Whether the endpoint may bind `address`.
///
/// Loopback always: that is the endpoint this harness has always run.
/// Anything else only for a domain provisioned with `--issuer-listen`
/// for a host that is not loopback, and only on the port its URL names,
/// either at that host's own address or at the unspecified address (for
/// a host reached at an address that is not on any of its interfaces, or
/// by name). Which host that is comes from the URL, and the certificate
/// has to name it: editing the description to point the endpoint
/// somewhere else does not produce a certificate for somewhere else, so
/// a loopback-provisioned domain cannot be talked into listening on the
/// network after the fact.
fn permitted(
    address: SocketAddr,
    url: &str,
    certificate: &CertificateDer<'_>,
) -> Result<(), IssuerError> {
    if address.ip().is_loopback() {
        return Ok(());
    }
    let refused = || IssuerError::NotLoopback(address.to_string());
    let (host, port) = url
        .strip_prefix("https://")
        .and_then(|rest| rest.rsplit_once(':'))
        .ok_or_else(refused)?;
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    let port: u16 = port.parse().map_err(|_| refused())?;
    let named_ip = host.parse::<IpAddr>().ok();
    if named_ip.is_some_and(|ip| ip.is_loopback()) || port != address.port() {
        return Err(refused());
    }
    if !(address.ip().is_unspecified() || named_ip == Some(address.ip())) {
        return Err(refused());
    }
    if !certificate_names(certificate, host, named_ip) {
        return Err(refused());
    }
    Ok(())
}

/// Whether `certificate` carries `host` as a subject alternative name.
fn certificate_names(certificate: &CertificateDer<'_>, host: &str, ip: Option<IpAddr>) -> bool {
    use x509_parser::extensions::GeneralName;
    use x509_parser::prelude::FromDer;

    let Ok((_, parsed)) = x509_parser::certificate::X509Certificate::from_der(certificate) else {
        return false;
    };
    let Ok(Some(names)) = parsed.subject_alternative_name() else {
        return false;
    };
    names
        .value
        .general_names
        .iter()
        .any(|name| match (name, ip) {
            (GeneralName::IPAddress(bytes), Some(IpAddr::V4(v4))) => *bytes == v4.octets(),
            (GeneralName::IPAddress(bytes), Some(IpAddr::V6(v6))) => *bytes == v6.octets(),
            (GeneralName::DNSName(dns), None) => dns.eq_ignore_ascii_case(host),
            _ => false,
        })
}

/// A session identifier no other exchange of this process reuses.
fn session_id(ordinal: u32) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&start_nanos().to_be_bytes());
    out[8..12].copy_from_slice(&std::process::id().to_be_bytes());
    out[12..].copy_from_slice(&ordinal.to_be_bytes());
    out
}

/// A distinct identifier per minted token, so no two exchanges replay.
fn jti(ordinal: u32) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[..8].copy_from_slice(&start_nanos().to_be_bytes());
    out[8..12].copy_from_slice(&std::process::id().to_be_bytes());
    out[12..16].copy_from_slice(&ordinal.to_be_bytes());
    out
}

/// The process's start instant, to the nanosecond. Two harness runs on
/// one machine do not collide, and one run's ordinals do not repeat.
fn start_nanos() -> u64 {
    use std::sync::OnceLock;
    static START: OnceLock<u64> = OnceLock::new();
    *START.get_or_init(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or_default()
    })
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn form_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let decode = |c: u8| match c {
                    b'0'..=b'9' => Some(c - b'0'),
                    b'a'..=b'f' => Some(c - b'a' + 10),
                    b'A'..=b'F' => Some(c - b'A' + 10),
                    _ => None,
                };
                match (decode(bytes[i + 1]), decode(bytes[i + 2])) {
                    (Some(hi), Some(lo)) => {
                        out.push(hi << 4 | lo);
                        i += 3;
                    }
                    _ => {
                        out.push(bytes[i]);
                        i += 1;
                    }
                }
            }
            other => {
                out.push(other);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The largest request this endpoint reads. A token exchange is a small
/// form; anything larger is not one.
const MAX_REQUEST_BYTES: usize = 64 * 1024;

fn read_request(stream: &mut impl Read) -> std::io::Result<(String, String, String)> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    let headers_end = loop {
        if let Some(at) = find(&buffer, b"\r\n\r\n") {
            break at;
        }
        if buffer.len() > MAX_REQUEST_BYTES {
            return Err(std::io::Error::other("request headers are too large"));
        }
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            return Err(std::io::Error::other(
                "the request ended before its headers",
            ));
        }
        buffer.extend_from_slice(&chunk[..read]);
    };
    let headers = String::from_utf8_lossy(&buffer[..headers_end]).into_owned();
    let mut lines = headers.split("\r\n");
    let mut request = lines.next().unwrap_or_default().split(' ');
    let method = request.next().unwrap_or_default().to_owned();
    let path = request.next().unwrap_or_default().to_owned();
    let length: usize = lines
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().ok())?
        })
        .unwrap_or(0);
    if length > MAX_REQUEST_BYTES {
        return Err(std::io::Error::other("request body is too large"));
    }
    let mut body = buffer[headers_end + 4..].to_vec();
    while body.len() < length {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..read]);
    }
    body.truncate(length);
    Ok((method, path, String::from_utf8_lossy(&body).into_owned()))
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
