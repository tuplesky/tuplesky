//! One native caller: a QUIC connection to a frontend, a bound session,
//! and the SDK deciding what an answer means (design Sections 19.4,
//! 22.3).
//!
//! Nothing here is a shortcut for the benchmark's convenience. The
//! credential is a real ES256 service token the daemon verifies against
//! the keys it read at startup; the connection is admitted by the
//! committed membership's binder; the request is the SDK's own frame and
//! the completion is the SDK's own verdict. A harness that framed its
//! own requests would measure a path no client has.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use coord_harness::domain::Provisioned;
use coord_harness::issuer::Minter;
use coord_types::ids::{
    ClientInstanceId, ClusterId, DomainId, ReplicaId, ReplicaIncarnation, SessionId,
};
use coord_types::logical_v1::LogicalRequest;
use coord_types::wire_v1::PeerRole;

/// What stopped a caller from getting to a bound session.
#[derive(Debug)]
pub enum CallerError {
    /// The provisioned material could not be read.
    Material(String),
    /// The endpoint could not be built or the frontend could not be
    /// reached.
    Transport(String),
    /// The credential could not be minted or was refused.
    Credential(String),
    /// The daemon refused the binding.
    Binding(String),
}

impl std::fmt::Display for CallerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallerError::Material(e) => write!(f, "provisioned material: {e}"),
            CallerError::Transport(e) => write!(f, "transport: {e}"),
            CallerError::Credential(e) => write!(f, "credential: {e}"),
            CallerError::Binding(e) => write!(f, "binding: {e}"),
        }
    }
}

impl std::error::Error for CallerError {}

/// How one operation ended, as the SDK decided it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Answer {
    /// The domain established the command.
    Established,
    /// The domain refused it, with a bounded reason.
    Refused(String),
    /// This caller never learned the outcome. It is not a failure: the
    /// invocation stays resolvable by its identity, and a run that
    /// counted it as one would be reporting a client's ignorance as a
    /// domain's error.
    Unknown,
}

impl Answer {
    /// The name a report groups refusals under.
    pub fn reason(&self) -> String {
        match self {
            Answer::Established => "established".into(),
            Answer::Refused(reason) => reason.clone(),
            Answer::Unknown => "unknown".into(),
        }
    }
}

/// A connected, bound caller.
pub struct Caller {
    transport: coord_transport::Transport,
    connection: coord_transport::ConnectionId,
    client: coord_sdk::Client<coord_sdk::StaticProvider>,
    ticks: u64,
    /// The session this caller's credential named.
    pub session: SessionId,
    /// The frontend it reached.
    pub frontend: String,
}

fn parse_id(hex: &str) -> Result<[u8; 16], CallerError> {
    if hex.len() != 32 {
        return Err(CallerError::Material(format!(
            "`{hex}` is not a 16-byte identifier"
        )));
    }
    let mut out = [0u8; 16];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|_| CallerError::Material(format!("`{hex}` is not hexadecimal")))?;
    }
    Ok(out)
}

impl Caller {
    /// Dial `voter`'s api plane and bind a session on it.
    ///
    /// `instance` distinguishes this caller's client instance from the
    /// others in the same run; each caller exchanges its own credential
    /// and therefore holds its own session, as separate processes would.
    pub async fn connect(
        dir: &Path,
        provisioned: &Provisioned,
        minter: &Minter,
        voter: usize,
        instance: u16,
    ) -> Result<Self, CallerError> {
        let node = provisioned
            .voters
            .get(voter)
            .ok_or_else(|| CallerError::Material("no such voter".into()))?;
        let cluster = ClusterId(parse_id(&provisioned.cluster)?);
        let domain = DomainId(parse_id(&provisioned.domain)?);
        let replica = ReplicaId(parse_id(&node.node)?);

        // This caller's own credential, issued here rather than shared.
        // An api link is keyed by the identity on the certificate, so
        // callers that presented one credential would queue behind each
        // other and the run would be measuring the harness.
        let issued =
            coord_harness::domain::issue_caller(&authority(provisioned)?, cluster, instance);
        let chain = vec![rustls_pki_types::CertificateDer::from(
            issued.certificate.der().to_vec(),
        )];
        let key = rustls_pki_types::PrivateKeyDer::Pkcs8(issued.key.serialize_der().into());
        let roots = load_roots(&provisioned.trust_bundle)?;

        let manifest_bytes = std::fs::read(&provisioned.manifest)
            .map_err(|e| CallerError::Material(format!("genesis: {e}")))?;
        let manifest: coord_membership::genesis::GenesisManifest =
            serde_json::from_slice(&manifest_bytes)
                .map_err(|e| CallerError::Material(format!("genesis: {e}")))?;
        let membership = coord_membership::membership::Membership::from_genesis(&manifest)
            .map_err(|e| CallerError::Material(format!("membership: {e:?}")))?;

        let identity = coord_transport::LocalIdentity {
            cluster,
            domain,
            chain,
            key,
            roots: Arc::new(roots),
            capabilities: coord_transport::role_lanes(PeerRole::Client)
                .iter()
                .map(|lane| lane.capability())
                .collect(),
            api_client: None,
            serves: Some(coord_transport::Class::Api),
            replica: None,
        };
        let transport = coord_transport::Transport::bind(
            "127.0.0.1:0".parse().expect("loopback"),
            identity,
            Arc::new(coord_membership::binder::PeerBinder::new(membership)),
            coord_transport::Limits::default(),
        )
        .map_err(|e| CallerError::Transport(format!("{e:?}")))?;

        let address = node
            .api
            .parse()
            .map_err(|_| CallerError::Material(format!("`{}` is not an address", node.api)))?;
        let connection = transport
            .connect(
                address,
                &provisioned.server_name,
                PeerRole::Client,
                None,
                coord_transport::Lane::Unary,
                coord_transport::BoundIdentity {
                    role: PeerRole::Voter,
                    replica: Some(replica),
                    incarnation: Some(ReplicaIncarnation::new(1).expect("one is positive")),
                    capabilities: Vec::new(),
                },
            )
            .await
            .map_err(|e| CallerError::Transport(format!("{e:?}")))?;

        let (token, session) = minter
            .mint_token()
            .ok_or_else(|| CallerError::Credential("the harness could not sign".into()))?;
        let mut instance_id = [0u8; 16];
        instance_id[..2].copy_from_slice(&instance.to_be_bytes());
        instance_id[2..].copy_from_slice(&session[..14]);
        let mut client = coord_sdk::Client::new(
            coord_sdk::ClientConfig::default(),
            coord_sdk::ClientInstance::new(
                cluster,
                domain,
                SessionId(session),
                ClientInstanceId(instance_id),
            ),
            coord_sdk::StaticProvider::new(coord_sdk::Credential::new(
                token.into_bytes(),
                u64::MAX,
            )),
        );
        let sdk_connection = coord_sdk::ConnectionId(connection.0);
        client
            .connect(0, sdk_connection)
            .map_err(|e| CallerError::Binding(format!("{e:?}")))?;
        let actions = client.take_actions();
        let [coord_sdk::SdkAction::Bind { credential, .. }] = actions.as_slice() else {
            return Err(CallerError::Binding(format!(
                "the client did not ask to bind: {actions:?}"
            )));
        };
        let frame = coord_session::bind_frame(credential.present())
            .map_err(|e| CallerError::Binding(format!("{e:?}")))?;
        let answer = transport
            .request(connection, frame, Duration::from_secs(30))
            .await
            .map_err(|e| CallerError::Binding(format!("{e:?}")))?;
        let ack = coord_session::decode_bind_ack(&answer)
            .map_err(|e| CallerError::Binding(format!("{e:?}")))?;
        client.bound(0, sdk_connection);

        let _ = dir;
        Ok(Caller {
            transport,
            connection,
            client,
            ticks: 0,
            session: ack.session,
            frontend: node.api.clone(),
        })
    }

    /// Submit one request and carry it to a completion.
    ///
    /// The SDK's clock is in milliseconds and only has to move forward,
    /// so it is advanced once per operation rather than sampled: what is
    /// being measured is wall time on this side, not the client's notion
    /// of it.
    pub async fn ask(&mut self, request: &LogicalRequest, deadline: Duration) -> Answer {
        self.ticks += 1;
        let now = self.ticks;
        let Ok(id) = self.client.submit(now, request, 0) else {
            return Answer::Refused("not-submitted".into());
        };
        let actions = self.client.take_actions();
        let [coord_sdk::SdkAction::Send { frame, .. }] = actions.as_slice() else {
            return Answer::Refused("not-sent".into());
        };
        let Ok(answer) = self
            .transport
            .request(self.connection, frame.clone(), deadline)
            .await
        else {
            // An unknown outcome leaves the invocation resolvable by
            // identity, which is right for a client and wrong for a load
            // generator: holding every unanswered invocation would fill
            // the client's own bound and turn one deadline into a run
            // full of refusals that came from this process. The run
            // reports the unknown and lets the invocation go.
            self.client.forget(id);
            return Answer::Unknown;
        };
        let Ok(bytes) =
            coord_types::wire_v1::encode_frame(answer.kind, answer.version, &answer.payload)
        else {
            self.client.forget(id);
            return Answer::Refused("unframeable".into());
        };
        if self
            .client
            .on_frame(now, coord_sdk::ConnectionId(self.connection.0), &bytes)
            .is_err()
        {
            self.client.forget(id);
            return Answer::Refused("undecodable".into());
        }
        let completions = self.client.take_completions();
        let Some(completion) = completions.iter().find(|c| c.request == id) else {
            self.client.forget(id);
            return Answer::Unknown;
        };
        // Release the invocation. The SDK retains a completed request so
        // that a caller which never saw the answer can still resolve it
        // by identity; a driver that never released one would hold every
        // invocation of the run and stop being a measurement of the
        // domain.
        let outcome = completion.outcome.clone();
        self.client.forget(id);
        match &outcome {
            coord_sdk::Outcome::Established { .. } => Answer::Established,
            coord_sdk::Outcome::Unknown => Answer::Unknown,
            other => Answer::Refused(refusal(other)),
        }
    }
}

/// A bounded name for a refusal. Never the detail bytes: a report is
/// published and a detail is not a label.
fn refusal(outcome: &coord_sdk::Outcome) -> String {
    match outcome {
        coord_sdk::Outcome::Established { .. } => "established".into(),
        coord_sdk::Outcome::Unknown => "unknown".into(),
        coord_sdk::Outcome::Failed(error) => format!("{error:?}")
            .split(['(', ' ', '{'])
            .next()
            .unwrap_or("failed")
            .to_lowercase(),
    }
}

/// Reopen the run directory's fixture authority.
fn authority(provisioned: &Provisioned) -> Result<coord_harness::pki::Ca, CallerError> {
    let encoded = std::fs::read_to_string(&provisioned.authority_key)
        .map_err(|e| CallerError::Material(format!("authority key: {e}")))?;
    let der = b64url_decode(&encoded)
        .ok_or_else(|| CallerError::Material("the authority key is not base64url".into()))?;
    coord_harness::pki::Ca::reopen(&der)
        .ok_or_else(|| CallerError::Material("the authority key is not usable".into()))
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

fn load_roots(path: &Path) -> Result<rustls::RootCertStore, CallerError> {
    use rustls_pki_types::pem::PemObject;
    let mut roots = rustls::RootCertStore::empty();
    for certificate in rustls_pki_types::CertificateDer::pem_file_iter(path)
        .map_err(|e| CallerError::Material(format!("trust bundle: {e}")))?
    {
        let certificate =
            certificate.map_err(|e| CallerError::Material(format!("trust bundle: {e}")))?;
        roots
            .add(certificate)
            .map_err(|e| CallerError::Material(format!("trust bundle: {e}")))?;
    }
    Ok(roots)
}
