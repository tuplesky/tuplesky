//! One bound session on one voter's frontend, and the SDK deciding what
//! each answer means (design Sections 6.5, 19.4).
//!
//! The shape is the benchmark caller's (`coord-wan-bench`), with the
//! three things a Jepsen client needs and a load generator does not:
//!
//! * **It is reachable across hosts.** The endpoint binds the
//!   unspecified address of the frontend's family, and the frontend may
//!   be a DNS name; the caller binds loopback and parses IP literals.
//! * **It resolves.** An answer that did not arrive is not given up: the
//!   invocation is asked about by identity (`ResolveRequest`), on a new
//!   connection bound to the same session when the old one is gone,
//!   until the domain says what it came to or the operation's budget is
//!   spent. Only then is the outcome unknown. A client that gave up at
//!   the first lost answer would hand a checker an `info` for every
//!   operation that was merely slow.
//! * **It returns results.** The established response is decoded, so a
//!   read has a value and a transaction a branch.
//!
//! Credentials are the harness fixture's, as the caller's are: a service
//! token minted from the run directory's signing key and a client
//! certificate issued from its test authority. A session is one token,
//! and a rebind presents the same token, so it keeps the session and
//! every invocation of it stays resolvable.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use coord_harness::domain::Provisioned;
use coord_harness::issuer::Minter;
use coord_sdk::{Client, Completion, Outcome, RetryError, SdkAction, StaticProvider};
use coord_state::Response;
use coord_types::ids::{
    ClientInstanceId, ClusterId, DomainId, NamespaceId, ReplicaId, ReplicaIncarnation, SessionId,
};
use coord_types::logical_v1::LogicalRequest;
use coord_types::wire_v1::PeerRole;

use crate::ops::Answer;

/// Timing of one session.
#[derive(Clone, Copy, Debug)]
pub struct Timing {
    /// How long one attempt (a send or a resolution) waits for its answer.
    pub attempt: Duration,
    /// How long one operation may take in all, attempts and reconnects
    /// included, before its outcome is reported unknown.
    pub budget: Duration,
    /// How long a dial and bind may take.
    pub connect: Duration,
}

impl Default for Timing {
    fn default() -> Self {
        Timing {
            attempt: Duration::from_secs(2),
            budget: Duration::from_secs(10),
            connect: Duration::from_secs(10),
        }
    }
}

/// Why a session could not be opened.
#[derive(Debug)]
pub struct OpenError(pub String);

impl std::fmt::Display for OpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for OpenError {}

fn material(what: &str, e: impl std::fmt::Debug) -> OpenError {
    OpenError(format!("{what}: {e:?}"))
}

/// Everything needed to dial and bind, kept for reconnects.
struct Target {
    address: SocketAddr,
    server_name: String,
    replica: ReplicaId,
}

/// A bound session.
pub struct Session {
    transport: coord_transport::Transport,
    connection: Option<coord_transport::ConnectionId>,
    client: Client<StaticProvider>,
    target: Target,
    timing: Timing,
    started: Instant,
    /// The namespace the provisioned grant authorizes.
    pub namespace: NamespaceId,
    /// The session this client's credential names.
    pub session: SessionId,
    /// Connections opened, the first included.
    pub connects: u64,
}

pub(crate) fn parse_id(hex: &str) -> Result<[u8; 16], OpenError> {
    if hex.len() != 32 {
        return Err(OpenError(format!("`{hex}` is not a 16-byte identifier")));
    }
    let mut out = [0u8; 16];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|_| OpenError(format!("`{hex}` is not hexadecimal")))?;
    }
    Ok(out)
}

/// Resolve `host:port`, IP literal or DNS name.
async fn resolve(address: &str) -> Result<SocketAddr, OpenError> {
    tokio::net::lookup_host(address)
        .await
        .map_err(|e| material("frontend address", e))?
        .next()
        .ok_or_else(|| OpenError(format!("`{address}` resolves to nothing")))
}

impl Session {
    /// Dial voter `voter` (zero-based) of the domain provisioned in `dir`
    /// and bind a fresh session on it. `instance` distinguishes this
    /// client's certificate and instance from others of the same run.
    ///
    /// One process opens one session this way. The harness minter names
    /// its sessions by process and by its own count, so two minters in
    /// one process would mint the same session twice; a process that
    /// opens several sessions shares one minter through
    /// [`Session::open_minted`].
    pub async fn open(
        dir: &Path,
        voter: usize,
        instance: u16,
        timing: Timing,
    ) -> Result<Session, OpenError> {
        let provisioned = Provisioned::read(dir).map_err(|e| material("harness.json", e))?;
        let minter = Minter::of(&provisioned).map_err(|e| material("issuer", e))?;
        Session::open_minted(&provisioned, &minter, voter, instance, timing).await
    }

    /// [`Session::open`], with the provisioned domain already read and a
    /// minter shared by every session of this process.
    pub async fn open_minted(
        provisioned: &Provisioned,
        minter: &Minter,
        voter: usize,
        instance: u16,
        timing: Timing,
    ) -> Result<Session, OpenError> {
        let node = provisioned
            .voters
            .get(voter)
            .ok_or_else(|| OpenError(format!("the domain has no voter {}", voter + 1)))?;
        let cluster = ClusterId(parse_id(&provisioned.cluster)?);
        let domain = DomainId(parse_id(&provisioned.domain)?);
        let namespace = NamespaceId(parse_id(&provisioned.namespace)?);
        let target = Target {
            address: resolve(&node.api).await?,
            server_name: provisioned.server_name.clone(),
            replica: ReplicaId(parse_id(&node.node)?),
        };

        let issued =
            coord_harness::domain::issue_caller(&authority(provisioned)?, cluster, instance);
        let chain = vec![rustls_pki_types::CertificateDer::from(
            issued.certificate.der().to_vec(),
        )];
        let key = rustls_pki_types::PrivateKeyDer::Pkcs8(issued.key.serialize_der().into());
        let roots = load_roots(&provisioned.trust_bundle)?;
        let manifest = coord_harness::domain::verified_genesis(&provisioned.manifest)
            .map_err(|e| material("genesis", e))?;
        let membership = coord_membership::membership::Membership::from_genesis(&manifest)
            .map_err(|e| material("membership", e))?;
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
        let local: SocketAddr = if target.address.is_ipv4() {
            "0.0.0.0:0".parse().expect("unspecified")
        } else {
            "[::]:0".parse().expect("unspecified")
        };
        let transport = coord_transport::Transport::bind(
            local,
            identity,
            Arc::new(coord_membership::binder::PeerBinder::new(membership)),
            coord_transport::Limits::default(),
        )
        .map_err(|e| material("endpoint", e))?;

        let (token, session) = minter
            .mint_token()
            .ok_or_else(|| OpenError("the harness could not sign a token".into()))?;
        let mut instance_id = [0u8; 16];
        instance_id[..2].copy_from_slice(&instance.to_be_bytes());
        instance_id[2..].copy_from_slice(&session[..14]);
        let client = Client::new(
            coord_sdk::ClientConfig::default(),
            coord_sdk::ClientInstance::new(
                cluster,
                domain,
                SessionId(session),
                ClientInstanceId(instance_id),
            ),
            StaticProvider::new(coord_sdk::Credential::new(token.into_bytes(), u64::MAX)),
        );
        let mut this = Session {
            transport,
            connection: None,
            client,
            target,
            timing,
            started: Instant::now(),
            namespace,
            session: SessionId(session),
            connects: 0,
        };
        this.connect().await.map_err(OpenError)?;
        Ok(this)
    }

    /// The SDK's clock: milliseconds since the session opened.
    fn now(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// Dial the frontend and bind this session's credential on it. The
    /// SDK re-sends whatever it had in flight once the binding holds.
    async fn connect(&mut self) -> Result<(), String> {
        let connection = tokio::time::timeout(
            self.timing.connect,
            self.transport.connect(
                self.target.address,
                &self.target.server_name,
                PeerRole::Client,
                None,
                coord_transport::Lane::Unary,
                coord_transport::BoundIdentity {
                    role: PeerRole::Voter,
                    replica: Some(self.target.replica),
                    incarnation: Some(ReplicaIncarnation::new(1).expect("one is positive")),
                    capabilities: Vec::new(),
                },
            ),
        )
        .await
        .map_err(|_| "dial timed out".to_string())?
        .map_err(|e| format!("dial: {e:?}"))?;
        self.connects += 1;
        let sdk = coord_sdk::ConnectionId(connection.0);
        let now = self.now();
        self.client
            .connect(now, sdk)
            .map_err(|e| format!("connect: {e:?}"))?;
        let actions = self.client.take_actions();
        let Some(credential) = actions.iter().find_map(|a| match a {
            SdkAction::Bind { credential, .. } => Some(credential.clone()),
            _ => None,
        }) else {
            return Err(format!("the client did not ask to bind: {actions:?}"));
        };
        let bound = async {
            let frame = coord_session::bind_frame(credential.present())
                .map_err(|e| format!("bind frame: {e:?}"))?;
            let answer = self
                .transport
                .request(connection, frame, self.timing.connect)
                .await
                .map_err(|e| format!("bind: {e:?}"))?;
            coord_session::decode_bind_ack(&answer).map_err(|e| format!("bind ack: {e:?}"))
        }
        .await;
        match bound {
            Ok(_) => {
                self.connection = Some(connection);
                self.client.bound(self.now(), sdk);
                Ok(())
            }
            Err(e) => {
                self.client.binding_rejected(sdk);
                self.transport.disconnect(
                    connection,
                    coord_transport::CloseCode::Shutdown,
                    "unbound",
                );
                Err(e)
            }
        }
    }

    /// Drop the current connection: the SDK re-queues what it had on it.
    fn lost(&mut self) {
        if let Some(connection) = self.connection.take() {
            self.transport
                .disconnect(connection, coord_transport::CloseCode::Shutdown, "lost");
            let now = self.now();
            self.client
                .on_connection_lost(now, coord_sdk::ConnectionId(connection.0));
        }
    }

    /// Carry `request` to an answer: established, refused before
    /// submission, or unknown once the budget is spent.
    pub async fn execute(&mut self, request: &LogicalRequest) -> Answer {
        let deadline = Instant::now() + self.timing.budget;
        let attempt_ms = u32::try_from(self.timing.attempt.as_millis()).unwrap_or(u32::MAX);
        let id = match self.client.submit(self.now(), request, attempt_ms) {
            Ok(id) => id,
            Err(e) => return Answer::NotSubmitted(format!("not-submitted: {e:?}")),
        };
        let mut last = String::from("no-answer");
        loop {
            if let Some(answer) = self.finished(id) {
                return answer;
            }
            if Instant::now() >= deadline {
                // Keep the identity bound: the invocation may still be
                // resolved by a later session of the same instance, and
                // nothing about it is claimed here beyond "not known".
                self.client.forget(id);
                return Answer::Unknown(last);
            }
            if self.connection.is_none() {
                if let Err(e) = self.connect().await {
                    last = e;
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                continue;
            }
            let actions = self.client.take_actions();
            let mut sent = false;
            for action in actions {
                let (connection, frame) = match action {
                    SdkAction::Send {
                        connection, frame, ..
                    }
                    | SdkAction::Resolve {
                        connection, frame, ..
                    } => (connection, frame),
                    // A reset stream is released when its request future
                    // is dropped, which has already happened.
                    SdkAction::Reset { .. } | SdkAction::Bind { .. } => continue,
                };
                sent = true;
                let left = deadline.saturating_duration_since(Instant::now());
                let wait = self.timing.attempt.min(left).max(Duration::from_millis(1));
                let transport = coord_transport::ConnectionId(connection.0);
                match self.transport.request(transport, frame, wait).await {
                    Ok(answer) => {
                        let bytes = match coord_types::wire_v1::encode_frame(
                            answer.kind,
                            answer.version,
                            &answer.payload,
                        ) {
                            Ok(bytes) => bytes,
                            Err(e) => {
                                last = format!("unframeable: {e:?}");
                                self.lost();
                                break;
                            }
                        };
                        if let Err(e) = self.client.on_frame(self.now(), connection, &bytes) {
                            last = format!("undecodable: {e:?}");
                            self.lost();
                            break;
                        }
                        if matches!(
                            self.client.state(id),
                            Some(coord_sdk::RequestState::Unknown)
                        ) {
                            // The frontend is still collecting it.
                            last = "pending".into();
                        }
                    }
                    Err(coord_transport::RequestError::Timeout) => {
                        last = "timeout".into();
                        // Past the SDK's own deadline the invocation
                        // becomes unknown and is resolved by identity.
                        self.client.tick(self.now());
                    }
                    Err(e) => {
                        last = format!("{e:?}");
                        self.lost();
                        break;
                    }
                }
            }
            if !sent {
                // Waiting on a backoff or a resolution interval.
                tokio::time::sleep(Duration::from_millis(50)).await;
                self.client.tick(self.now());
            }
        }
    }

    /// The final answer for `id`, if the SDK has one. An unknown
    /// completion is not final: the SDK resolves it.
    fn finished(&mut self, id: coord_sdk::RequestId) -> Option<Answer> {
        let completions: Vec<Completion> = self.client.take_completions();
        let done = completions
            .into_iter()
            .filter(|c| c.request == id)
            .find(|c| !matches!(c.outcome, Outcome::Unknown));
        let done = match done {
            Some(done) => done,
            // The endpoint answered a resolution with `Unknown`: it has
            // lost the identity, and asking again cannot learn more.
            None if matches!(
                self.client.state(id),
                Some(coord_sdk::RequestState::Done(Outcome::Unknown))
            ) =>
            {
                self.client.forget(id);
                return Some(Answer::Unknown("resolved-unknown".into()));
            }
            None => return None,
        };
        // A completion is final. Retire the identity, so the session's
        // acknowledged floor keeps moving.
        self.client.forget(id);
        Some(match done.outcome {
            Outcome::Established { result, .. } => {
                match postcard::from_bytes::<Response>(&result) {
                    Ok(response) => Answer::Established(response),
                    Err(e) => Answer::Unknown(format!("undecodable result: {e}")),
                }
            }
            Outcome::Failed(error) => match error {
                RetryError::Malformed => Answer::NotSubmitted("malformed".into()),
                RetryError::RequestTooLarge => Answer::NotSubmitted("request-too-large".into()),
                RetryError::PayloadConflict { .. } => {
                    Answer::NotSubmitted("payload-conflict".into())
                }
                // Refused at the frontend, but a refusal of a disclosure
                // is also `NotAdmitted` and says nothing about whether the
                // command ran; the result bound says it did run.
                other => Answer::Unknown(format!("{other:?}").to_lowercase()),
            },
            Outcome::Unknown => unreachable!("filtered above"),
        })
    }
}

/// Reopen the run directory's fixture authority.
fn authority(provisioned: &Provisioned) -> Result<coord_harness::pki::Ca, OpenError> {
    let encoded = std::fs::read_to_string(&provisioned.authority_key)
        .map_err(|e| material("authority key", e))?;
    let der = b64url_decode(&encoded)
        .ok_or_else(|| OpenError("the authority key is not base64url".into()))?;
    coord_harness::pki::Ca::reopen(&der)
        .ok_or_else(|| OpenError("the authority key is not usable".into()))
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

fn load_roots(path: &Path) -> Result<rustls::RootCertStore, OpenError> {
    use rustls_pki_types::pem::PemObject;
    let mut roots = rustls::RootCertStore::empty();
    for certificate in rustls_pki_types::CertificateDer::pem_file_iter(path)
        .map_err(|e| material("trust bundle", e))?
    {
        let certificate = certificate.map_err(|e| material("trust bundle", e))?;
        roots
            .add(certificate)
            .map_err(|e| material("trust bundle", e))?;
    }
    Ok(roots)
}
