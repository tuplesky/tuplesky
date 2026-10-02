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
    /// Largest logical request accepted, as the protocol counts one: the
    /// bytes of the keys, values and range ends it carries
    /// (`LogicalRequest::cost`). A frontend refuses a larger one at
    /// admission with `REQUEST_TOO_LARGE`.
    pub max_request_bytes: usize,
    /// Largest response / complete watch revision.
    pub max_response_bytes: usize,
    /// Outstanding requests per session.
    pub max_outstanding_per_session: usize,
    /// Concurrent watch subscriptions per process.
    pub max_live_subscriptions: usize,
    /// Journal records a domain may hold beyond its published
    /// checkpoint before the next one is published and the prefix
    /// reclaimed. Zero never publishes.
    ///
    /// A local setting, and only a local one: what it decides is when
    /// this node spends I/O on its own redo, and no replicated result
    /// depends on the answer. Publishing more often costs exports and
    /// keeps the journal small; publishing less often costs journal and
    /// makes a restart replay further.
    #[serde(default = "default_checkpoint_after")]
    pub checkpoint_after_records: u64,
    /// Commands a voter's command table holds at once (task-d05): the
    /// ones it still owes work on, and the executed ones it has not
    /// retired yet.
    ///
    /// A local memory bound and an operational setting, not a safety
    /// switch: no replicated result depends on it, and voters of one
    /// domain may differ. It is also, until recovery is bounded by what
    /// the voters executed, how much history an election can carry: a
    /// candidate must hold the whole selection, so a domain whose history
    /// is well past this many commands cannot elect a leader. Raising it
    /// moves that cliff; it does not remove it.
    #[serde(default = "default_command_table_capacity")]
    pub command_table_capacity: usize,
    /// New requests each frontend admits a second, with a second's worth
    /// as a burst (task-d26). A retry of a request still outstanding is
    /// not a new one.
    ///
    /// Bounded below the rate at which a voter catching up executes: a
    /// page of up to 64 commands installed in one batch (task-d25). A
    /// voter returning behind gains on the domain by the difference, so
    /// the time it takes to close a gap is bounded by the gap; admitted
    /// faster than it can execute, it never closes it. The bound is per
    /// frontend: a domain served by several admits their sum.
    #[serde(default = "default_max_admitted_per_second")]
    pub max_admitted_per_second: u32,
}

const fn default_checkpoint_after() -> u64 {
    4096
}

/// The admission rate a configuration that names none gets (task-d26): a
/// design target and a ceiling, not a proved bound. It is under a sixth
/// of what catch-up would execute at one 64-command window per durable
/// batch of 10 ms, which nothing has measured; task-d33's budget oracle
/// is its check. At the 35 to 100 commands a second a domain sustains
/// today it never binds. It gates client requests only: resolves,
/// watches and held retries bypass it, and every session of a frontend
/// shares one bucket.
pub const DEFAULT_MAX_ADMITTED_PER_SECOND: u32 = 1000;

const fn default_max_admitted_per_second() -> u32 {
    DEFAULT_MAX_ADMITTED_PER_SECOND
}

/// The command table capacity a configuration that names none gets: the
/// largest (task-d20).
pub const DEFAULT_COMMAND_TABLE_CAPACITY: usize = MAX_COMMAND_TABLE_CAPACITY;
/// The smallest command table capacity a voter is configured with. A
/// table must hold a proposal's worth of in-flight commands and still
/// reclaim, and the follower's held-proposal bound is a multiple of it.
pub const MIN_COMMAND_TABLE_CAPACITY: usize = 32;
/// The largest. A report carries at most twice the table (its live
/// records and its retirement window, `coord_consensus::max_report_entries`;
/// a larger one is set aside), and a Sync is capped to the same, 2,000
/// commands at this capacity, except kept entries, which are never cut
/// (task-d24). The Sync is written as one row and sent as one frame, and
/// the row binds first. An entry with its admission digest, one
/// dependency and the largest sequence number is 208 bytes, and each
/// further dependency 32 more; 2,000 such entries are 416,000 bytes, a
/// fifth of the row, so at the cap an entry has about 1,058 bytes, some
/// 27 dependencies on average, before the Sync is refused by name rather
/// than written (task-d20, measured by
/// `the_largest_table_gives_a_sync_that_fits_a_row_and_a_frame`).
pub const MAX_COMMAND_TABLE_CAPACITY: usize = coord_consensus::MAX_TABLE_CAPACITY;

const fn default_command_table_capacity() -> usize {
    DEFAULT_COMMAND_TABLE_CAPACITY
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_request_bytes: 2 * 1024 * 1024,
            max_response_bytes: 8 * 1024 * 1024,
            max_outstanding_per_session: 256,
            max_live_subscriptions: 4096,
            checkpoint_after_records: default_checkpoint_after(),
            command_table_capacity: default_command_table_capacity(),
            max_admitted_per_second: default_max_admitted_per_second(),
        }
    }
}

/// Reporting this node's metrics while it serves (task-d45).
///
/// A snapshot printed only at start and at a clean end says nothing
/// about a daemon that was killed, which is the daemon a fault run and
/// a stuck one leave behind. One every interval leaves the last
/// interval's counters in the log however the process ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
    /// Seconds between the snapshots a serving node prints. Zero prints
    /// only the ones at start and at the end.
    #[serde(default = "default_metrics_interval")]
    pub interval_seconds: u64,
}

/// How this node's frontend serves a current read (task-d50; design
/// Section 6.3).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadsConfig {
    /// Which path a current read takes.
    #[serde(default)]
    pub path: ReadPath,
}

/// The path a current read takes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReadPath {
    /// The leader read barrier, falling back to an ordered command. A
    /// frontend with no voter beside it orders every read: it follows no
    /// ballot, so it does not know who leads.
    #[default]
    Leader,
    /// Every read is an ordered command, as before task-d50.
    Ordered,
}

/// The interval a configuration that names none gets: often enough that
/// a killed daemon's last line is recent, rarely enough that the log is
/// not the snapshot.
pub const DEFAULT_METRICS_INTERVAL_SECONDS: u64 = 10;

const fn default_metrics_interval() -> u64 {
    DEFAULT_METRICS_INTERVAL_SECONDS
}

impl Default for MetricsConfig {
    fn default() -> Self {
        MetricsConfig {
            interval_seconds: default_metrics_interval(),
        }
    }
}

/// Agreeing a forgetting floor (task-d27; design Section 5.3).
///
/// A voter that takes part exports the shared checkpoint at every floor
/// boundary ([`crate::floor::FLOOR_INTERVAL`] executed positions), keeps
/// the image, and promises its peers it holds it. A majority promising
/// one checkpoint activates the floor. Nothing is forgotten below it
/// yet: this is the agreement, and trimming comes after it.
///
/// Off by default. The interval is the schema's rather than this file's,
/// so voters that take part agree on the boundaries without having to
/// agree on a setting; one that does not take part simply promises
/// nothing, and a floor then needs a majority of the others.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FloorConfig {
    /// Whether this voter exports and promises at floor boundaries.
    #[serde(default)]
    pub enabled: bool,
    /// Directory the promised images are kept in, relative to
    /// `state_directory` unless absolute. Not the local checkpoints'
    /// directory: that one reclaims every image but the selected one.
    #[serde(default = "floor_images")]
    pub images: String,
    /// Free bytes the images' filesystem must keep beyond the image an
    /// export would add. Short of it, the boundary is refused and said
    /// so, and no promise is made.
    #[serde(default = "floor_headroom_bytes")]
    pub headroom_bytes: u64,
}

fn floor_images() -> String {
    "floor-images".to_owned()
}

const fn floor_headroom_bytes() -> u64 {
    1024 * 1024 * 1024
}

impl Default for FloorConfig {
    fn default() -> Self {
        FloorConfig {
            enabled: false,
            images: floor_images(),
            headroom_bytes: floor_headroom_bytes(),
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
/// The replay-backed profile (task-j06, Section 17.3.4): projection
/// commits are working commits, made durable on
/// [`JournalConfig::projection_durable_commits`],
/// [`JournalConfig::projection_durable_records`] and
/// [`JournalConfig::projection_durable_ms`], before every checkpoint
/// publication and at a clean stop, and a crash's loss is replayed from
/// the journal before the node answers. Named, never defaulted: the
/// strict profile stays the default until task-j05 qualifies the journal
/// this one leans on for more.
pub const JOURNAL_REPLAY_PROFILE: &str = "journaled-replay-v1";
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
    /// Directory local recovery checkpoints are published into,
    /// relative to `state_directory` unless absolute.
    ///
    /// Its own directory, not the projection's: a checkpoint is an
    /// inactive image that must survive whatever happens to the
    /// generation it was read from, and keeping the two together would
    /// make losing one a way of losing both.
    #[serde(default = "state_checkpoints")]
    pub checkpoints: String,
}

fn state_checkpoints() -> String {
    "checkpoints".to_owned()
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
    /// Under [`JOURNAL_REPLAY_PROFILE`], working projection commits at
    /// most between durable ones. Ignored under the strict profile.
    #[serde(default = "projection_durable_commits")]
    pub projection_durable_commits: u32,
    /// Under [`JOURNAL_REPLAY_PROFILE`], journal records at most applied
    /// by working commits between durable ones: what a crash makes the
    /// next start replay.
    #[serde(default = "projection_durable_records")]
    pub projection_durable_records: u32,
    /// Under [`JOURNAL_REPLAY_PROFILE`], milliseconds at most a working
    /// commit waits for a durable one while the node is busy.
    #[serde(default = "projection_durable_ms")]
    pub projection_durable_ms: u64,
}

impl JournalConfig {
    /// Whether the projection runs under [`JOURNAL_REPLAY_PROFILE`].
    pub fn replays_projection(&self) -> bool {
        self.profile == JOURNAL_REPLAY_PROFILE
    }
}

fn projection_durable_commits() -> u32 {
    64
}

fn projection_durable_records() -> u32 {
    4096
}

fn projection_durable_ms() -> u64 {
    100
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

/// Renewing this node's leaf while it serves (task-d02; design Sections
/// 10.4, 20.4).
///
/// A node certificate is short-lived on purpose, so a node that serves
/// longer than one lifetime has to renew it in place. It does so the way
/// it was first enrolled: at the node issuer, presenting a workload
/// assertion and a request signed with the key it already holds -- the
/// committed key, so what comes back is a renewal and not a replacement.
///
/// Optional. Without it the node serves on the leaf it started with until
/// that leaf's `notAfter`, and is put back by restarting it on a renewed
/// one; the startup report says renewal is not configured.
///
/// Paths and a URL, nothing secret: the assertion is read from its file
/// at each attempt and never held in the configuration or printed.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenewalConfig {
    /// The node issuer's base URL. Enrollment is posted to
    /// `{issuer}/enroll`.
    ///
    /// `https://` always, except a loopback `http://` URL where
    /// [`RenewalConfig::allow_insecure_loopback`] says so. The issuer
    /// hands this node the credential its peers authenticate it by, and
    /// a response nothing authenticated could only be refused here
    /// afterwards, if it could be refused at all.
    pub issuer: String,
    /// A file holding the workload assertion the issuer verifies (a
    /// projected service-account token, for instance).
    ///
    /// Read at every attempt, because the platform rotates it in place;
    /// a copy taken at startup would expire long before the leaf does.
    pub assertion: String,
    /// PEM roots for the issuer's own TLS certificate. Absent, the public
    /// web roots are used; present, only these are.
    #[serde(default)]
    pub issuer_roots: Option<String>,
    /// The lifetime to ask for, in seconds.
    ///
    /// Stated rather than copied from the leaf being renewed. The issuer
    /// back-dates each leaf by its clock uncertainty, so a lifetime read
    /// off one leaf is a little longer than the one asked for it, and a
    /// node that asked for "the same again" would ask for a little more
    /// each time -- until the issuer's policy refused it, at a due point,
    /// on a node that had renewed without trouble for weeks. The issuer's
    /// policy bounds it either way, and a lifetime past the policy's is a
    /// refusal the node reports at every attempt.
    pub lifetime_secs: u64,
    /// The window renewal is spread over after the leaf turns due, in
    /// seconds, so a fleet issued together does not renew together. Each
    /// node's point in it is derived from its own identity, so it is the
    /// same across restarts.
    #[serde(default = "default_renewal_jitter")]
    pub jitter_secs: u64,
    /// Accept an `http://` issuer on a loopback address. Test-only.
    ///
    /// For the tests, which serve an issuer in-process: a loopback URL
    /// never leaves the machine, and anything else is refused whatever
    /// this says. It stays in the schema so one configuration parses in
    /// every build, but only a build with debug assertions accepts it
    /// set; a release build refuses it at validation
    /// ([`ConfigError::TestOnlySwitch`]), because no insecure switch
    /// belongs in a production artifact (design Section 20.5). A
    /// production node renews at an `https://` issuer, with
    /// [`RenewalConfig::issuer_roots`] where it is not publicly rooted.
    #[serde(default)]
    pub allow_insecure_loopback: bool,
}

const fn default_renewal_jitter() -> u64 {
    3600
}

/// Whether `url` is an issuer this node may enroll at: `https://`, or a
/// loopback `http://` where `insecure_loopback` allows it.
///
/// The scheme and host are decided by parsing the authority, never by a
/// prefix of the string: `http://127.0.0.1.evil.example` starts with the
/// loopback prefix and is not loopback at all. Userinfo is refused
/// outright, since a URL that carries a credential would carry it into
/// every report that names the issuer. The host and port are checked
/// here too, for either scheme: a URL the HTTP client would refuse is a
/// configuration error to report at startup (and under `--check`), not
/// one discovered at the first due attempt and retried until the leaf
/// runs out.
pub fn issuer_url_permitted(url: &str, insecure_loopback: bool) -> bool {
    let (secure, rest) = if let Some(rest) = url.strip_prefix("https://") {
        (true, rest)
    } else if let Some(rest) = url.strip_prefix("http://") {
        (false, rest)
    } else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    if authority.is_empty() || authority.contains('@') || rest.contains(['?', '#']) {
        return false;
    }
    let Some(host) = authority_host(authority) else {
        return false;
    };
    if secure {
        return true;
    }
    if !insecure_loopback {
        return false;
    }
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// The host of `authority` (`host`, `host:port`, `[v6]` or `[v6]:port`),
/// if both it and the port are well formed. A bracketed host is an IPv6
/// address; any other is an IPv4 address or a DNS name. A port is one
/// to five digits naming a nonzero `u16`.
fn authority_host(authority: &str) -> Option<&str> {
    let (host, port) = if let Some(inside) = authority.strip_prefix('[') {
        let (host, after) = inside.split_once(']')?;
        host.parse::<std::net::Ipv6Addr>().ok()?;
        match after {
            "" => (host, None),
            _ => (host, Some(after.strip_prefix(':')?)),
        }
    } else {
        match authority.split_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (authority, None),
        }
    };
    if let Some(port) = port
        && !(port.bytes().all(|b| b.is_ascii_digit()) && port.parse::<u16>().is_ok_and(|p| p != 0))
    {
        return None;
    }
    if authority.starts_with('[') {
        return Some(host);
    }
    let labels: Vec<&str> = host.split('.').collect();
    if labels
        .iter()
        .all(|l| !l.is_empty() && l.bytes().all(|b| b.is_ascii_digit()))
    {
        // All numeric: an IPv4 address, or nothing.
        return host.parse::<std::net::Ipv4Addr>().ok().map(|_| host);
    }
    let name = host.strip_suffix('.').unwrap_or(host);
    let well_formed = !name.is_empty()
        && name.len() <= 253
        && name.split('.').all(|l| {
            !l.is_empty()
                && l.len() <= 63
                && !l.starts_with('-')
                && !l.ends_with('-')
                && l.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
        });
    well_formed.then_some(host)
}

/// The daemon configuration.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Configuration schema version.
    pub config_version: u32,
    /// Role specification (e.g. `voter-frontend-observer`).
    pub role: String,
    /// Genesis manifest path: the manifest as the admin signed it (an
    /// ES256 token), never as plain JSON.
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

    /// The admin public key (PEM, P-256) the manifest is verified against
    /// at `init` and at every start.
    pub genesis_admin_key: String,
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
    /// Renewing this node's leaf while it serves (see
    /// [`RenewalConfig`]). Absent, the node never renews in place.
    #[serde(default)]
    pub renewal: Option<RenewalConfig>,
    /// Semantic limits.
    #[serde(default)]
    pub limits: Limits,
    /// Agreeing a forgetting floor with the other voters (task-d27).
    /// Absent, it is off.
    #[serde(default)]
    pub floor: FloorConfig,
    /// Printing this node's metrics while it serves (task-d45).
    #[serde(default)]
    pub metrics: MetricsConfig,
    /// How current reads are served (task-d50).
    #[serde(default)]
    pub reads: ReadsConfig,
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
    /// The issuer a node renews at is not one it may trust with that:
    /// not `https://`, and not a loopback URL explicitly allowed.
    InsecureIssuer,
    /// A renewal that asks for no lifetime at all.
    ZeroLifetime,
    /// A setting outside the range this build accepts.
    OutOfRange {
        /// Which setting.
        field: &'static str,
        /// The smallest value accepted.
        min: u64,
        /// The largest value accepted.
        max: u64,
    },
    /// A test-only switch set in a build without debug assertions. The
    /// field is named.
    TestOnlySwitch(&'static str),
    /// Two stores that each reclaim what they do not keep given the same
    /// directory, or one inside the other: each would take the other's
    /// images for its own leftovers and remove them.
    SharedDirectory {
        /// The one setting.
        first: &'static str,
        /// The other.
        second: &'static str,
    },
}

/// Which build is validating: whether test-only switches may be set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Build {
    /// Debug assertions on: the tests' build.
    Test,
    /// Debug assertions off: what ships.
    Release,
}

impl Build {
    const fn current() -> Build {
        if cfg!(debug_assertions) {
            Build::Test
        } else {
            Build::Release
        }
    }
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
        self.validate_as(Build::current())
    }

    fn validate_as(&self, build: Build) -> Result<(), ConfigError> {
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
        if !(MIN_COMMAND_TABLE_CAPACITY..=MAX_COMMAND_TABLE_CAPACITY)
            .contains(&self.limits.command_table_capacity)
        {
            return Err(ConfigError::OutOfRange {
                field: "limits.command_table_capacity",
                min: MIN_COMMAND_TABLE_CAPACITY as u64,
                max: MAX_COMMAND_TABLE_CAPACITY as u64,
            });
        }
        if self.limits.max_admitted_per_second == 0 {
            return Err(ConfigError::OutOfRange {
                field: "limits.max_admitted_per_second",
                min: 1,
                max: u64::from(u32::MAX),
            });
        }
        // Durable state is opened under the name this build implements or
        // it is not opened at all. A name this build does not serve is a
        // different store, not a compatible one, and the manifest is what
        // says which -- so the refusal happens here, before anything has
        // been read, rather than as a surprise inside an engine.
        engine_named("state", &self.state.engine, STATE_ENGINE)?;
        engine_named("state.profile", &self.state.profile, STATE_PROFILE)?;
        engine_named("journal", &self.journal.engine, JOURNAL_ENGINE)?;
        if !self.journal.replays_projection() {
            engine_named("journal.profile", &self.journal.profile, JOURNAL_PROFILE)?;
        }
        for (field, value) in [
            (
                "journal.projection_durable_commits",
                u64::from(self.journal.projection_durable_commits),
            ),
            (
                "journal.projection_durable_records",
                u64::from(self.journal.projection_durable_records),
            ),
            (
                "journal.projection_durable_ms",
                self.journal.projection_durable_ms,
            ),
        ] {
            if value == 0 {
                return Err(ConfigError::OutOfRange {
                    field,
                    min: 1,
                    max: u64::from(u32::MAX),
                });
            }
        }
        // A node that journals nothing has no authoritative transition to
        // apply from, so zero shards is a configuration that cannot serve.
        if self.journal.shards == 0 {
            return Err(ConfigError::NoJournalShards);
        }
        if let Some(renewal) = &self.renewal {
            // Refused before the URL is looked at, so a release build
            // says which switch it will not honour rather than calling
            // the loopback issuer insecure.
            if renewal.allow_insecure_loopback && build == Build::Release {
                return Err(ConfigError::TestOnlySwitch(
                    "renewal.allow_insecure_loopback",
                ));
            }
            if !issuer_url_permitted(&renewal.issuer, renewal.allow_insecure_loopback) {
                return Err(ConfigError::InsecureIssuer);
            }
            if renewal.lifetime_secs == 0 {
                return Err(ConfigError::ZeroLifetime);
            }
        }
        // An empty path is not a default: it resolves to the working
        // directory, which is where a process would silently create a
        // second, empty generation beside the real one.
        for (name, path) in [
            ("state_directory", &self.state_directory),
            ("state.root", &self.state.root),
            ("journal.root", &self.journal.root),
            ("cluster_manifest", &self.cluster_manifest),
            ("genesis_admin_key", &self.genesis_admin_key),
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
        }))
        .chain(self.renewal.iter().flat_map(|renewal| {
            [("renewal.assertion", &renewal.assertion)]
                .into_iter()
                .chain(
                    renewal
                        .issuer_roots
                        .iter()
                        .map(|roots| ("renewal.issuer_roots", roots)),
                )
        })) {
            if path.trim().is_empty() {
                return Err(ConfigError::EmptyPath(name));
            }
        }
        // The floor's images and the local checkpoints are each reclaimed
        // down to what their own store keeps, and both name an image by a
        // 64-hex directory. Given one directory, or one inside the other,
        // the local store would remove the images a floor promise names
        // and the floor would remove the selected recovery checkpoint.
        if self.floor.enabled {
            if self.floor.images.trim().is_empty() {
                return Err(ConfigError::EmptyPath("floor.images"));
            }
            let images = resolved(&self.state_directory, &self.floor.images);
            let checkpoints = resolved(&self.state_directory, &self.state.checkpoints);
            if images.starts_with(&checkpoints) || checkpoints.starts_with(&images) {
                return Err(ConfigError::SharedDirectory {
                    first: "floor.images",
                    second: "state.checkpoints",
                });
            }
        }
        Ok(())
    }
}

/// `path` under `state_directory` unless absolute, with `.` and `..`
/// taken out lexically, so two spellings of one directory compare equal.
/// Links are not followed here: the store compares the directories it
/// opened as well (`coordd`'s `keep_floor`).
pub fn resolved(state_directory: &str, path: &str) -> std::path::PathBuf {
    use std::path::{Component, Path, PathBuf};
    let joined = Path::new(state_directory).join(path);
    let mut out = PathBuf::new();
    for component in joined.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other),
        }
    }
    out
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

#[cfg(test)]
mod tests {
    use super::{Build, CONFIG_VERSION, Config, ConfigError};

    fn with_renewal(allow: bool) -> Config {
        let text = format!(
            r#"config_version = {CONFIG_VERSION}
role = "voter-frontend-observer"
cluster_manifest = "/etc/coord/genesis.json"
genesis_admin_key = "/etc/coord/genesis-admin.pem"
domain = "control-plane-a"
state_directory = "/var/lib/coord/a"

[listen]
api_quic = "[::]:7443"
peer_quic = "[::]:7444"

[capability]
writer_queue_bytes = 16777216
buffer_bytes_per_subscription = 8388608
max_live_subscriptions = 4096

[state]
root = "state"

[journal]
root = "journal"
shards = 1

[identity]
trust_bundle = "/etc/coord/roots.pem"
node_certificate = "/etc/coord/node.pem"
node_key = "/etc/coord/node.key"

[sts]
issuer = "https://sts.example"
resource = "control-plane-a"
jwks = "/etc/coord/sts-jwks.json"
trust_rule = "7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c"

[renewal]
issuer = "http://127.0.0.1:9000"
assertion = "/var/run/secrets/token"
lifetime_secs = 3600
allow_insecure_loopback = {allow}
"#
        );
        // Deserialized without validating, so each build's answer can be
        // asked for from the one build the tests run in.
        toml::from_str(&text).expect("the fixture parses")
    }

    #[test]
    fn the_insecure_loopback_switch_is_honoured_only_by_a_test_build() {
        let set = with_renewal(true);
        assert_eq!(set.validate_as(Build::Test), Ok(()));
        assert_eq!(
            set.validate_as(Build::Release),
            Err(ConfigError::TestOnlySwitch(
                "renewal.allow_insecure_loopback"
            ))
        );
        // Unset, the loopback http issuer is refused in either build, as
        // it always was: the switch is what a release build will not
        // honour, not a second way to reach the same refusal.
        let unset = with_renewal(false);
        for build in [Build::Test, Build::Release] {
            assert_eq!(unset.validate_as(build), Err(ConfigError::InsecureIssuer));
        }
    }

    /// The floor is off unless a configuration turns it on, and turned on
    /// it keeps its images apart from the local checkpoints (task-d27).
    #[test]
    fn the_forgetting_floor_is_off_unless_configured() {
        use super::{FloorConfig, state_checkpoints};
        assert_eq!(with_renewal(true).floor, FloorConfig::default());
        assert!(!FloorConfig::default().enabled);
        let on: FloorConfig = toml::from_str("enabled = true").expect("parses");
        assert!(on.enabled);
        assert_ne!(on.images, state_checkpoints());
        assert!(toml::from_str::<FloorConfig>("interval = 8").is_err());
    }

    /// Turned on, the floor refuses a directory the local checkpoints
    /// reclaim, however it is spelled: each store would remove the
    /// other's images.
    #[test]
    fn the_floor_keeps_its_images_apart_from_the_local_checkpoints() {
        let on = |images: &str, checkpoints: &str| {
            let mut config = with_renewal(false);
            config.renewal = None;
            config.floor.enabled = true;
            config.floor.images = images.to_owned();
            config.state.checkpoints = checkpoints.to_owned();
            config.validate_as(Build::Test)
        };
        let shared = Err(ConfigError::SharedDirectory {
            first: "floor.images",
            second: "state.checkpoints",
        });
        assert_eq!(on("floor-images", "checkpoints"), Ok(()));
        assert_eq!(on("checkpoints", "checkpoints"), shared);
        assert_eq!(on("./checkpoints/", "checkpoints"), shared);
        assert_eq!(on("/var/lib/coord/a/checkpoints", "checkpoints"), shared);
        assert_eq!(on("x/../checkpoints", "checkpoints"), shared);
        assert_eq!(on("checkpoints/floor", "checkpoints"), shared);
        assert_eq!(on(".", "checkpoints"), shared);
        assert_eq!(
            on("", "checkpoints"),
            Err(ConfigError::EmptyPath("floor.images"))
        );
        // Off, the setting is not used and not checked.
        let mut off = with_renewal(false);
        off.renewal = None;
        off.floor.images = "checkpoints".to_owned();
        assert_eq!(off.validate_as(Build::Test), Ok(()));
    }
}
