//! Strict typed configuration (design Section 22.1): unknown fields are
//! rejected, the local capability must cover the active semantic limits,
//! and the production dependency graph must exclude test keys and
//! bypasses.

use serde::Deserialize;

use crate::role::RoleSet;

/// Listener addresses.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListenConfig {
    /// Native API QUIC listener.
    pub api_quic: Option<String>,
    /// Peer plane QUIC listener.
    pub peer_quic: Option<String>,
    /// Loopback admin/metrics HTTP.
    pub admin_http: Option<String>,
    /// HTTPS credential-establishment listener (auth broker, issuer).
    pub https: Option<String>,
}

/// Semantic limits the local capability must cover.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    /// Largest logical request accepted.
    pub max_request_bytes: usize,
    /// Largest response / complete watch revision.
    pub max_response_bytes: usize,
    /// Outstanding requests per session.
    pub max_outstanding_per_session: usize,
    /// Concurrent watch subscriptions per process.
    pub max_live_subscriptions: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_request_bytes: 2 * 1024 * 1024,
            max_response_bytes: 8 * 1024 * 1024,
            max_outstanding_per_session: 256,
            max_live_subscriptions: 4096,
        }
    }
}

/// The local capability: what this process's storage and buffers can hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capability {
    /// Bytes the writer queue can hold.
    pub writer_queue_bytes: usize,
    /// Bytes buffered per watch subscription.
    pub buffer_bytes_per_subscription: usize,
    /// Subscriptions the process can hold live.
    pub max_live_subscriptions: usize,
}

/// The state engine this build serves in production, and the durability
/// profile it must have been created under (design Section 19.3).
pub const STATE_ENGINE: &str = "redb";
/// Profile of [`STATE_ENGINE`].
pub const STATE_PROFILE: &str = "strict-single-store-v1";
/// The journal engine this build serves, and its profile: journal the
/// authoritative transition first, then apply atomically (Section 17.3).
pub const JOURNAL_ENGINE: &str = "raft-engine";
/// Profile of [`JOURNAL_ENGINE`].
pub const JOURNAL_PROFILE: &str = "journaled-strict-v1";
/// An engine that is built and tested but carries no production support:
/// it reports its own platforms, and task-s03's review boundary keeps it
/// out of a production graph. Named here so configuring it is refused for
/// the reason it actually is, rather than as an unknown name.
pub const EXPERIMENTAL_ENGINE: &str = "fjall";

/// The projection this node keeps its logical state in.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateConfig {
    /// Engine name, which must match the generation's own manifest.
    #[serde(default = "state_engine")]
    pub engine: String,
    /// Durability profile, likewise.
    #[serde(default = "state_profile")]
    pub profile: String,
    /// Root directory, relative to `state_directory` unless absolute.
    pub root: String,
    /// Engine page cache.
    #[serde(default = "default_cache_bytes")]
    pub cache_bytes: usize,
}

fn state_engine() -> String {
    STATE_ENGINE.to_owned()
}

fn state_profile() -> String {
    STATE_PROFILE.to_owned()
}

fn default_cache_bytes() -> usize {
    64 * 1024 * 1024
}

impl StateConfig {
    /// Where this node's projection actually lives.
    ///
    /// A relative root is relative to `state_directory`, so one setting
    /// moves a whole node; an absolute one is taken as given, so a
    /// projection can be put on its own device without moving anything
    /// else. Resolving it here rather than at each use is what stops two
    /// call sites disagreeing about which of those a given string was.
    pub fn root_path(&self, state_directory: &str) -> std::path::PathBuf {
        let root = std::path::Path::new(&self.root);
        if root.is_absolute() {
            return root.to_path_buf();
        }
        std::path::Path::new(state_directory).join(root)
    }
}

/// The journal this node makes transitions durable in.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalConfig {
    /// Engine name, which must match the journal's own metadata.
    #[serde(default = "journal_engine")]
    pub engine: String,
    /// Durability profile.
    #[serde(default = "journal_profile")]
    pub profile: String,
    /// Root directory, relative to `state_directory` unless absolute.
    pub root: String,
    /// Shards this node writes.
    #[serde(default = "one_shard")]
    pub shards: u16,
}

fn journal_engine() -> String {
    JOURNAL_ENGINE.to_owned()
}

fn journal_profile() -> String {
    JOURNAL_PROFILE.to_owned()
}

fn one_shard() -> u16 {
    1
}

/// The security token service this domain's sessions are bound against.
///
/// A frontend verifies a client's token against these keys before the
/// connection carries any work, so a node that had no answer for them
/// could not admit anyone. They are named here rather than discovered,
/// because discovery is itself something a caller could influence: a
/// node that learned its issuer from the network could be told to trust
/// one.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StsConfig {
    /// The issuer identifier tokens must name.
    pub issuer: String,
    /// This cluster's resource, which a token's audience must name.
    pub resource: String,
    /// Path to the issuer's published JWKS, read at startup.
    ///
    /// A file, not a URL: fetching keys over the network at startup
    /// makes a node's ability to serve depend on a service being
    /// reachable, and makes what it trusts depend on what answered.
    /// Rotation replaces the file and is picked up by
    /// `BoundFrontend::set_jwks`, out of band.
    pub jwks: String,
    /// The trust rule this issuer's service tokens name, as 32 lowercase
    /// hex characters.
    ///
    /// A session is established under a trust rule that replicated
    /// policy holds enabled at the generation the credential names, and
    /// a domain that trusted nothing could establish no session -- the
    /// first session cannot be the one that authorizes writing the rule
    /// that would admit it. So the rule is written when the domain's
    /// store is initialized, from this, and every replica writes the
    /// same row. Disabling or regenerating it afterwards is ordinary
    /// replicated administration, and invalidates the sessions admitted
    /// under the old generation.
    pub trust_rule: String,
}

/// A permission this domain's genesis grants.
///
/// Permission is allow-only and a fresh domain allows nothing, so
/// without an initial grant the first administrative command would
/// itself be unauthorized. Each grant gives one principal every action
/// over the whole of one namespace; narrowing it, and granting anyone
/// else, is replicated administration afterwards.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrantConfig {
    /// Principal, as 32 lowercase hex characters.
    pub principal: String,
    /// Namespace, as 32 lowercase hex characters.
    pub namespace: String,
}

/// Where this node's own credentials and trust anchors are.
///
/// They are paths, not material: nothing here is a secret, and a
/// diagnostics snapshot may name them. The files behind them are read
/// once at startup, under the process's own account (Section 22.2).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityConfig {
    /// PEM bundle of the roots this node validates peers against.
    pub trust_bundle: String,
    /// This node's certificate chain, issued by the node issuer.
    pub node_certificate: String,
    /// Its private key.
    pub node_key: String,
    /// The certificate this process presents when it acts as this
    /// domain's trusted collector toward another voter's API plane.
    ///
    /// A node certificate binds exactly one role. A process that runs a
    /// voter *and* that domain's frontend is two principals, and the
    /// submission it makes on a client's behalf is the collector's, not
    /// the voter's -- so it presents the collector's credential for it.
    /// Without one such a process can still serve callers and still
    /// deliver to a voter in its own process; it simply cannot submit
    /// to a voter anywhere else, and is refused at startup for saying
    /// so, rather than at the first request.
    ///
    /// Optional: a process with no frontend never submits, and a
    /// single-voter domain has nobody to submit to.
    #[serde(default)]
    pub collector_certificate: Option<String>,
    /// Its private key.
    #[serde(default)]
    pub collector_key: Option<String>,
}

/// The daemon configuration.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Configuration schema version.
    pub config_version: u32,
    /// Role specification (e.g. `voter-frontend-observer`).
    pub role: String,
    /// Genesis manifest path.
    pub cluster_manifest: String,
    /// A signed endpoint catalog naming where this epoch's voters are.
    ///
    /// Addresses are not committed configuration: the manifest says who
    /// the voters are and what key each proves with, and deliberately
    /// not where any of them is. A catalog carries the where, attested
    /// by a voter, and cannot introduce a voter or re-incarnate one.
    ///
    /// Optional because a process that does not vote never dials a peer,
    /// and because a single-voter domain has none to dial. A voter with
    /// peers and no catalog is refused at startup rather than left to
    /// discover it has nobody to talk to.
    #[serde(default)]
    pub cluster_endpoints: Option<String>,
    /// Domain name.
    pub domain: String,
    /// State directory.
    pub state_directory: String,
    /// Listeners.
    pub listen: ListenConfig,
    /// The logical-state projection.
    pub state: StateConfig,
    /// The durable journal.
    pub journal: JournalConfig,
    /// Credentials and trust anchors.
    pub identity: IdentityConfig,
    /// The token service sessions are bound against. A process that
    /// serves clients needs one; a voter that serves only its peers does
    /// not, and saying so is how a peer-only node avoids carrying a
    /// dependency it never uses.
    #[serde(default)]
    pub sts: Option<StsConfig>,
    /// Permissions this domain's genesis grants (see [`GrantConfig`]).
    #[serde(default)]
    pub grant: Vec<GrantConfig>,
    /// Semantic limits.
    #[serde(default)]
    pub limits: Limits,
    /// Local capability.
    pub capability: Capability,
    /// Whether application 0-RTT is disabled (must be true).
    #[serde(default)]
    pub allow_application_0rtt: bool,
    /// Whether test keys or verification bypasses are permitted (must be
    /// false in a production graph).
    #[serde(default)]
    pub allow_test_bypasses: bool,
}

/// Why a configuration is refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConfigError {
    /// The TOML did not parse, or an unknown field was present.
    Parse(String),
    /// The schema version is not supported.
    UnsupportedVersion {
        /// The version seen.
        version: u32,
    },
    /// The role specification is invalid.
    Role(String),
    /// A role needs a listener the configuration does not provide.
    MissingListener(&'static str),
    /// A named listener is not a usable socket address.
    InvalidListener(&'static str),
    /// The local capability does not cover an active semantic limit.
    CapabilityTooSmall(&'static str),
    /// Application 0-RTT is enabled; it must be disabled.
    ZeroRttEnabled,
    /// The production graph would include test keys or bypasses.
    TestBypassEnabled,
    /// A field that must be an identity is not 32 lowercase hex
    /// characters.
    NotAnIdentity(&'static str),
    /// An engine or profile this build does not serve. A node never opens
    /// durable state under a name it does not implement: the manifest is
    /// what says what the bytes are, and a mismatch is a different store,
    /// not a compatible one.
    UnsupportedEngine {
        /// Which section.
        section: &'static str,
        /// What the configuration asked for.
        named: String,
        /// What this build serves.
        supported: &'static str,
    },
    /// An experimental engine in a production configuration. It reports
    /// its own platforms and carries no production support, so naming it
    /// here is refused rather than quietly honoured.
    ExperimentalEngine {
        /// The engine named.
        named: String,
    },
    /// A path a required file or directory would be read from is empty.
    EmptyPath(&'static str),
    /// A node must write at least one journal shard.
    NoJournalShards,
    /// A role needs a section the configuration does not provide.
    MissingSection {
        /// Which section.
        section: &'static str,
        /// What needs it.
        needed_by: &'static str,
    },
}

/// Supported configuration schema version.
pub const CONFIG_VERSION: u32 = 2;

/// Whether `capability` covers `limits`.
pub fn capability_covers(capability: &Capability, limits: &Limits) -> Result<(), ConfigError> {
    // The writer queue must hold at least one largest request.
    if capability.writer_queue_bytes < limits.max_request_bytes {
        return Err(ConfigError::CapabilityTooSmall("writer_queue_bytes"));
    }
    // A subscription buffer must hold at least one complete revision.
    if capability.buffer_bytes_per_subscription < limits.max_response_bytes {
        return Err(ConfigError::CapabilityTooSmall(
            "buffer_bytes_per_subscription",
        ));
    }
    if capability.max_live_subscriptions < limits.max_live_subscriptions {
        return Err(ConfigError::CapabilityTooSmall("max_live_subscriptions"));
    }
    Ok(())
}

/// The sixteen bytes `hex` names, or `None` if it is not exactly 32
/// lowercase hex characters.
///
/// Lowercase only, and exactly the full width: an identity written two
/// ways is two spellings of one row key waiting to be compared as
/// strings somewhere.
pub fn identity_bytes(hex: &str) -> Option<[u8; 16]> {
    if hex.len() != 32
        || hex
            .chars()
            .any(|c| !c.is_ascii_hexdigit() || c.is_ascii_uppercase())
    {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        out[i] = u8::from_str_radix(core::str::from_utf8(chunk).ok()?, 16).ok()?;
    }
    Some(out)
}

impl Config {
    /// Parse and validate a configuration from TOML text.
    pub fn parse(text: &str) -> Result<Self, ConfigError> {
        let config: Config = toml::from_str(text).map_err(|e| ConfigError::Parse(e.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    /// The parsed role set.
    pub fn role_set(&self) -> Result<RoleSet, ConfigError> {
        RoleSet::parse(&self.role).map_err(ConfigError::Role)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.config_version != CONFIG_VERSION {
            return Err(ConfigError::UnsupportedVersion {
                version: self.config_version,
            });
        }
        if self.allow_application_0rtt {
            return Err(ConfigError::ZeroRttEnabled);
        }
        if self.allow_test_bypasses {
            return Err(ConfigError::TestBypassEnabled);
        }
        if let Some(sts) = &self.sts
            && identity_bytes(&sts.trust_rule).is_none()
        {
            return Err(ConfigError::NotAnIdentity("sts.trust_rule"));
        }
        for grant in &self.grant {
            if identity_bytes(&grant.principal).is_none() {
                return Err(ConfigError::NotAnIdentity("grant.principal"));
            }
            if identity_bytes(&grant.namespace).is_none() {
                return Err(ConfigError::NotAnIdentity("grant.namespace"));
            }
        }
        let roles = self.role_set()?;
        // A required listener has to be usable, not merely present: an
        // empty or unparseable address passed validation and then failed
        // at bind, after the process had reported its configuration good.
        let required = [
            (
                roles.needs_peer_listener(),
                "peer_quic",
                &self.listen.peer_quic,
            ),
            (
                roles.needs_api_listener(),
                "api_quic",
                &self.listen.api_quic,
            ),
            (roles.needs_https(), "https", &self.listen.https),
        ];
        for (needed, name, value) in required {
            if !needed {
                continue;
            }
            let Some(address) = value.as_deref() else {
                return Err(ConfigError::MissingListener(name));
            };
            if address.trim().is_empty() || address.parse::<std::net::SocketAddr>().is_err() {
                return Err(ConfigError::InvalidListener(name));
            }
        }
        // A process that serves clients verifies their tokens, so it
        // needs the keys to verify them against. Starting without them
        // would mean a frontend that binds its listener and then refuses
        // every caller, which reads as a client problem.
        if roles.needs_api_listener() && self.sts.is_none() {
            return Err(ConfigError::MissingSection {
                section: "sts",
                needed_by: "a process that serves clients",
            });
        }
        // An optional listener that is configured must still be usable.
        if let Some(address) = self.listen.admin_http.as_deref()
            && (address.trim().is_empty() || address.parse::<std::net::SocketAddr>().is_err())
        {
            return Err(ConfigError::InvalidListener("admin_http"));
        }
        capability_covers(&self.capability, &self.limits)?;
        // Durable state is opened under the name this build implements or
        // it is not opened at all. A name this build does not serve is a
        // different store, not a compatible one, and the manifest is what
        // says which -- so the refusal happens here, before anything has
        // been read, rather than as a surprise inside an engine.
        engine_named("state", &self.state.engine, STATE_ENGINE)?;
        engine_named("state.profile", &self.state.profile, STATE_PROFILE)?;
        engine_named("journal", &self.journal.engine, JOURNAL_ENGINE)?;
        engine_named("journal.profile", &self.journal.profile, JOURNAL_PROFILE)?;
        // A node that journals nothing has no authoritative transition to
        // apply from, so zero shards is a configuration that cannot serve.
        if self.journal.shards == 0 {
            return Err(ConfigError::NoJournalShards);
        }
        // An empty path is not a default: it resolves to the working
        // directory, which is where a process would silently create a
        // second, empty generation beside the real one.
        for (name, path) in [
            ("state_directory", &self.state_directory),
            ("state.root", &self.state.root),
            ("journal.root", &self.journal.root),
            ("cluster_manifest", &self.cluster_manifest),
            ("identity.trust_bundle", &self.identity.trust_bundle),
            ("identity.node_certificate", &self.identity.node_certificate),
            ("identity.node_key", &self.identity.node_key),
        ]
        .into_iter()
        .chain(self.sts.iter().flat_map(|sts| {
            [
                ("sts.issuer", &sts.issuer),
                ("sts.resource", &sts.resource),
                ("sts.jwks", &sts.jwks),
            ]
        })) {
            if path.trim().is_empty() {
                return Err(ConfigError::EmptyPath(name));
            }
        }
        Ok(())
    }
}

/// Refuse an engine or profile this build does not serve.
fn engine_named(
    section: &'static str,
    named: &str,
    supported: &'static str,
) -> Result<(), ConfigError> {
    if named == supported {
        return Ok(());
    }
    // An experimental engine gets its own refusal rather than being
    // counted with typos: it exists, it is built, and naming it is a
    // deliberate act that is nevertheless not supported in production.
    if named == EXPERIMENTAL_ENGINE {
        return Err(ConfigError::ExperimentalEngine {
            named: named.to_owned(),
        });
    }
    Err(ConfigError::UnsupportedEngine {
        section,
        named: named.to_owned(),
        supported,
    })
}
