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
    /// Domain name.
    pub domain: String,
    /// State directory.
    pub state_directory: String,
    /// Listeners.
    pub listen: ListenConfig,
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
        // An optional listener that is configured must still be usable.
        if let Some(address) = self.listen.admin_http.as_deref()
            && (address.trim().is_empty() || address.parse::<std::net::SocketAddr>().is_err())
        {
            return Err(ConfigError::InvalidListener("admin_http"));
        }
        capability_covers(&self.capability, &self.limits)?;
        Ok(())
    }
}
