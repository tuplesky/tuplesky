//! Dependency policy (task-01, design Sections 16.1-16.3):
//!
//! * `Cargo.lock` exists and every workspace dependency is an exact pin, a path
//!   or a Git revision. Every workspace member manifest inherits its external
//!   dependencies from the workspace table (`workspace = true`) or pins them
//!   the same way, so no member can add a floating requirement that a later
//!   lockfile update would silently move.
//! * Every workspace member declares `package.metadata.tuplesky.role` as one of
//!   `core`, `production`, `tool` or `test-only`.
//! * No `core`, `production` or `tool` crate reaches a test-only crate (a
//!   workspace member with role `test-only` or a listed external test crate)
//!   through normal or build dependencies. Insecure test entropy, simulators
//!   and model engines therefore cannot enter production artifacts.
//! * Boundary constructors that turn plain data into a sealed capability
//!   (`VerifierToken::for_boundary`, `PeerProvenance::from_transport`) appear
//!   only in the reviewed boundary crates or in test-only crates. Library
//!   sources are scanned; integration tests are exempt.
//! * No crate in the resolved graph is a forbidden alternative TLS stack.
//! * Git sources are limited to the reviewed raft-engine pin.
//! * `cargo deny check` enforces licenses, advisories, bans and sources.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

/// External crates that are development/test aids and must never appear in a
/// production dependency edge.
const TEST_ONLY_EXTERNAL: &[&str] = &[
    "proptest",
    "loom",
    "arbitrary",
    "libfuzzer-sys",
    "criterion",
    "quickcheck",
    // The experimental state engine (task-s03): production remains redb-only.
    "fjall",
];

/// Crates a `core` (pure, deterministic) crate may never reach through normal
/// or build dependencies: runtimes, engines, sockets, ambient entropy and
/// seeded generators (design Sections 11, 16.3 and 18.2).
const CORE_FORBIDDEN: &[&str] = &[
    "tokio",
    "redb",
    "fjall",
    "raft-engine",
    "quinn",
    "quinn-proto",
    "rustls",
    "getrandom",
    "rand",
    "rand_chacha",
    "rand_core",
    "reqwest",
    "axum",
    "hyper",
    "mio",
];

/// Expected resolved feature sets for audited dependencies. The pinned
/// raft-engine must build with no optional feature (no scripting, internals,
/// nightly allocator, failpoints or optional codecs); the experimental fjall
/// pin resolves exactly the explicit `lz4` feature (no bytes, metrics or
/// whitebox features).
const FEATURE_AUDIT: &[(&str, &[&str])] = &[("raft-engine", &[]), ("fjall", &["lz4"])];

/// Constructors that cast unverified data into a sealed capability, and the
/// non-test crates allowed to contain them (design Sections 3, 9.3, 18.1).
/// Test-only crates are always allowed: the dependency policy keeps them out
/// of production artifacts.
const BOUNDARY_CONSTRUCTORS: &[(&str, &[&str])] = &[
    // Two boundaries mint receipts, and they mint different ones.
    // `coord-collector` reconstructs a receipt from facts that reached
    // it over an ingress whose authority it has just established.
    // `coord-session` is the authentication boundary itself: it
    // verifies a service credential against the configured issuer,
    // audience and keys, and what it attests -- the principal, the
    // trust rule and its generation, the ceiling, the credential's
    // deadline -- is that credential's, never a caller's.
    (
        "VerifierToken::for_boundary",
        &["coord-collector", "coord-session"],
    ),
    ("PeerProvenance::from_transport", &["coord-transport"]),
    // Evidence from a voter running in the same process never crosses a
    // connection, so the transport cannot be the one to prove where it
    // came from. The voter runtime is, and it is the only crate that
    // holds a voter instance and its committed identity together.
    ("PeerProvenance::from_local_voter", &["coord-daemon"]),
];

/// Crates that must not appear anywhere in the resolved graph.
const FORBIDDEN: &[&str] = &["openssl", "openssl-sys", "native-tls", "hyper-tls"];

/// Allowed Git sources (repository URL and exact revision).
const ALLOWED_GIT: &[(&str, &str)] = &[(
    "https://github.com/tikv/raft-engine",
    "097c499a19fbb38754c73aa2f31532329df7c0c6",
)];

const ROLES: &[&str] = &["core", "production", "tool", "test-only"];

#[derive(Deserialize)]
struct Metadata {
    packages: Vec<Package>,
    workspace_members: Vec<String>,
    resolve: Resolve,
}

#[derive(Deserialize)]
struct Package {
    name: String,
    id: String,
    source: Option<String>,
    #[serde(default)]
    manifest_path: String,
    #[serde(default)]
    metadata: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct Resolve {
    nodes: Vec<Node>,
}

#[derive(Deserialize)]
struct Node {
    id: String,
    deps: Vec<Dep>,
    #[serde(default)]
    features: Vec<String>,
}

#[derive(Deserialize)]
struct Dep {
    pkg: String,
    dep_kinds: Vec<DepKind>,
}

#[derive(Deserialize)]
struct DepKind {
    kind: Option<String>,
}

pub(crate) fn check(root: &Path, offline: bool) -> Result<()> {
    if !root.join("Cargo.lock").is_file() {
        bail!("Cargo.lock is missing; commit a locked resolution");
    }
    check_exact_pins(root)?;
    // Filter by each supported platform so `cfg(loom)`-only dependencies
    // (never set in production builds) do not appear in the normal graph.
    // The member-manifest and boundary-constructor scans are platform
    // independent and run once.
    for (i, platform) in ["x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu"]
        .iter()
        .enumerate()
    {
        let json = crate::output(
            root,
            "cargo",
            &[
                "metadata",
                "--format-version",
                "1",
                "--locked",
                "--filter-platform",
                platform,
            ],
        )?;
        let metadata: Metadata = serde_json::from_str(&json).context("parsing cargo metadata")?;
        if i == 0 {
            check_member_pins(&metadata)?;
            check_boundary_constructors(&metadata)?;
        }
        check_graph(&metadata)?;
    }
    let go_sum = root.join("adapters/kine/go.sum");
    if !go_sum.is_file() {
        bail!("adapters/kine/go.sum is missing");
    }
    crate::run(&root.join("adapters/kine"), "go", &["mod", "verify"])?;
    if crate::has_program("cargo", &["deny", "--version"]) {
        let checks: &[&str] = if offline {
            &["deny", "--locked", "check", "bans", "licenses", "sources"]
        } else {
            &["deny", "--locked", "check"]
        };
        crate::run(root, "cargo", checks)?;
    } else {
        bail!("cargo-deny is not installed; run `cargo xtask check-tools --install`");
    }
    eprintln!("dependency policy: ok");
    Ok(())
}

fn check_exact_pins(root: &Path) -> Result<()> {
    let manifest: toml::Value = toml::from_str(&std::fs::read_to_string(root.join("Cargo.toml"))?)?;
    let deps = manifest
        .get("workspace")
        .and_then(|w| w.get("dependencies"))
        .and_then(|d| d.as_table())
        .context("workspace.dependencies missing")?;
    let mut bad = Vec::new();
    for (name, spec) in deps {
        let ok = match spec {
            toml::Value::String(v) => v.starts_with('='),
            toml::Value::Table(t) => {
                t.contains_key("path")
                    || (t.contains_key("git") && t.contains_key("rev"))
                    || t.get("version")
                        .and_then(|v| v.as_str())
                        .is_some_and(|v| v.starts_with('='))
            }
            _ => false,
        };
        if !ok {
            bad.push(name.clone());
        }
    }
    if !bad.is_empty() {
        bail!(
            "workspace dependencies without exact `=` pin, path or git rev: {}",
            bad.join(", ")
        );
    }
    Ok(())
}

/// Dependency tables a member manifest may declare, directly or under
/// `[target.<cfg>.*]`.
const DEPENDENCY_TABLES: &[&str] = &["dependencies", "dev-dependencies", "build-dependencies"];

/// Every external dependency declared by a workspace member must be inherited
/// from the workspace table or pinned exactly, so `cargo metadata --locked`
/// and cargo-deny never see a floating requirement that the root check does
/// not cover.
fn check_member_pins(metadata: &Metadata) -> Result<()> {
    let mut bad = Vec::new();
    for package in metadata
        .packages
        .iter()
        .filter(|p| metadata.workspace_members.contains(&p.id))
    {
        let text = std::fs::read_to_string(&package.manifest_path)
            .with_context(|| format!("reading {}", package.manifest_path))?;
        let manifest: toml::Value =
            toml::from_str(&text).with_context(|| format!("parsing {}", package.manifest_path))?;
        for entry in unpinned_member_dependencies(&manifest) {
            bad.push(format!("{}: {entry}", package.name));
        }
    }
    if !bad.is_empty() {
        bail!(
            "member dependencies must use `workspace = true`, an exact `=` pin, a path or a git rev: {}",
            bad.join(", ")
        );
    }
    Ok(())
}

/// Names (`table.name`) of dependencies in one member manifest that neither
/// inherit from the workspace nor pin exactly.
fn unpinned_member_dependencies(manifest: &toml::Value) -> Vec<String> {
    let mut bad = Vec::new();
    let mut tables: Vec<(String, &toml::Value)> = Vec::new();
    for table in DEPENDENCY_TABLES {
        if let Some(v) = manifest.get(table) {
            tables.push(((*table).to_string(), v));
        }
    }
    if let Some(targets) = manifest.get("target").and_then(|t| t.as_table()) {
        for (cfg, spec) in targets {
            for table in DEPENDENCY_TABLES {
                if let Some(v) = spec.get(table) {
                    tables.push((format!("target.{cfg}.{table}"), v));
                }
            }
        }
    }
    for (table, deps) in tables {
        let Some(deps) = deps.as_table() else {
            bad.push(format!("{table} (not a table)"));
            continue;
        };
        for (name, spec) in deps {
            if !member_spec_is_pinned(spec) {
                bad.push(format!("{table}.{name}"));
            }
        }
    }
    bad
}

fn member_spec_is_pinned(spec: &toml::Value) -> bool {
    match spec {
        toml::Value::String(v) => v.starts_with('='),
        toml::Value::Table(t) => {
            t.get("workspace").and_then(|w| w.as_bool()) == Some(true)
                || t.contains_key("path")
                || (t.contains_key("git") && t.contains_key("rev"))
                || t.get("version")
                    .and_then(|v| v.as_str())
                    .is_some_and(|v| v.starts_with('='))
        }
        _ => false,
    }
}

/// Scan the library sources (`src/`) of every non-test workspace member for
/// boundary constructors outside their allow list.
fn check_boundary_constructors(metadata: &Metadata) -> Result<()> {
    let mut violations = Vec::new();
    for package in metadata
        .packages
        .iter()
        .filter(|p| metadata.workspace_members.contains(&p.id))
    {
        // Test-only crates never reach production artifacts, and this crate
        // holds the policy text itself.
        if role_of(package) == Some("test-only") || package.name == "xtask" {
            continue;
        }
        let src = Path::new(&package.manifest_path)
            .parent()
            .context("manifest without a directory")?
            .join("src");
        for file in rust_files(&src)? {
            let text = std::fs::read_to_string(&file)
                .with_context(|| format!("reading {}", file.display()))?;
            for hit in boundary_violations(&package.name, &text) {
                violations.push(format!("{} ({hit})", file.display()));
            }
        }
    }
    if !violations.is_empty() {
        bail!(
            "boundary constructors outside their allowed crates: {}",
            violations.join(", ")
        );
    }
    Ok(())
}

/// Every `.rs` file under `dir`, recursively; empty when `dir` is missing.
fn rust_files(dir: &Path) -> Result<Vec<std::path::PathBuf>> {
    let mut out = Vec::new();
    if !dir.is_dir() {
        return Ok(out);
    }
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).with_context(|| format!("reading {}", d.display()))? {
            let path = entry?.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }
    out.sort();
    Ok(out)
}

/// The boundary constructors `text` mentions that `crate_name` may not use.
/// Doc comments and line comments are ignored; `#[cfg(test)]` modules are
/// not, so an in-crate unit test that needs one belongs in `tests/`.
fn boundary_violations(crate_name: &str, text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for (symbol, allowed) in BOUNDARY_CONSTRUCTORS {
        if allowed.contains(&crate_name) {
            continue;
        }
        let mentioned = text.lines().any(|line| {
            let code = line.trim_start();
            !code.starts_with("//") && code.contains(symbol)
        });
        if mentioned {
            out.push((*symbol).to_string());
        }
    }
    out
}

fn role_of(package: &Package) -> Option<&str> {
    package
        .metadata
        .as_ref()?
        .get("tuplesky")?
        .get("role")?
        .as_str()
}

fn check_graph(metadata: &Metadata) -> Result<()> {
    let by_id: BTreeMap<&str, &Package> = metadata
        .packages
        .iter()
        .map(|p| (p.id.as_str(), p))
        .collect();
    let members: BTreeSet<&str> = metadata
        .workspace_members
        .iter()
        .map(String::as_str)
        .collect();

    let mut roles: BTreeMap<&str, &str> = BTreeMap::new();
    for id in &members {
        let package = by_id
            .get(id)
            .context("workspace member missing from packages")?;
        match role_of(package) {
            Some(role) if ROLES.contains(&role) => {
                roles.insert(id, role);
            }
            Some(role) => bail!(
                "{}: unknown package.metadata.tuplesky.role {:?}",
                package.name,
                role
            ),
            None => bail!(
                "{}: missing package.metadata.tuplesky.role (one of {})",
                package.name,
                ROLES.join(", ")
            ),
        }
    }

    let mut test_only: BTreeSet<&str> = BTreeSet::new();
    for package in &metadata.packages {
        if members.contains(package.id.as_str()) {
            if roles.get(package.id.as_str()) == Some(&"test-only") {
                test_only.insert(&package.id);
            }
        } else if TEST_ONLY_EXTERNAL.contains(&package.name.as_str()) {
            test_only.insert(&package.id);
        }
    }

    for package in &metadata.packages {
        if FORBIDDEN.contains(&package.name.as_str()) {
            bail!("forbidden crate {} is in the resolved graph", package.name);
        }
        if let Some(source) = &package.source
            && source.starts_with("git+")
        {
            let allowed = ALLOWED_GIT.iter().any(|(repo, rev)| {
                source.starts_with(&format!("git+{repo}")) && source.ends_with(&format!("#{rev}"))
            });
            if !allowed {
                bail!("{}: unreviewed git source {}", package.name, source);
            }
        }
    }

    // Normal/build adjacency; dev-dependencies are excluded on purpose.
    let mut adjacency: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for node in &metadata.resolve.nodes {
        let edges = node
            .deps
            .iter()
            .filter(|d| {
                d.dep_kinds
                    .iter()
                    .any(|k| k.kind.is_none() || k.kind.as_deref() == Some("build"))
            })
            .map(|d| d.pkg.as_str())
            .collect();
        adjacency.insert(&node.id, edges);
    }

    let core_forbidden: BTreeSet<&str> = metadata
        .packages
        .iter()
        .filter(|p| CORE_FORBIDDEN.contains(&p.name.as_str()))
        .map(|p| p.id.as_str())
        .collect();
    for (id, role) in &roles {
        if *role == "test-only" {
            continue;
        }
        if let Some(path) = reach(&adjacency, id, &test_only) {
            let names: Vec<&str> = path.iter().map(|p| by_id[p].name.as_str()).collect();
            bail!(
                "{} crate {} reaches test-only crate through normal/build dependencies: {}",
                role,
                by_id[id].name,
                names.join(" -> ")
            );
        }
        if *role == "core"
            && let Some(path) = reach(&adjacency, id, &core_forbidden)
        {
            let names: Vec<&str> = path.iter().map(|p| by_id[p].name.as_str()).collect();
            bail!(
                "core crate {} reaches a runtime/engine/entropy crate: {}",
                by_id[id].name,
                names.join(" -> ")
            );
        }
    }

    for node in &metadata.resolve.nodes {
        let Some(package) = by_id.get(node.id.as_str()) else {
            continue;
        };
        if let Some((_, expected)) = FEATURE_AUDIT.iter().find(|(n, _)| *n == package.name) {
            let mut actual: Vec<&str> = node.features.iter().map(String::as_str).collect();
            actual.sort_unstable();
            let mut expected: Vec<&str> = expected.to_vec();
            expected.sort_unstable();
            if actual != expected {
                bail!(
                    "{}: resolved features {:?} differ from audited {:?}",
                    package.name,
                    actual,
                    expected
                );
            }
        }
    }
    eprintln!(
        "dependency graph: {} workspace members, {} packages, {} test-only crates isolated",
        members.len(),
        metadata.packages.len(),
        test_only.len()
    );
    Ok(())
}

/// Breadth-first search returning the path from `start` to the first
/// forbidden node, if any.
fn reach<'a>(
    adjacency: &BTreeMap<&'a str, Vec<&'a str>>,
    start: &'a str,
    forbidden: &BTreeSet<&'a str>,
) -> Option<Vec<&'a str>> {
    let mut parent: BTreeMap<&str, &str> = BTreeMap::new();
    let mut queue = VecDeque::from([start]);
    let mut seen = BTreeSet::from([start]);
    while let Some(node) = queue.pop_front() {
        for &next in adjacency.get(node).map(Vec::as_slice).unwrap_or(&[]) {
            if !seen.insert(next) {
                continue;
            }
            parent.insert(next, node);
            if forbidden.contains(next) {
                let mut path = vec![next];
                let mut cur = next;
                while let Some(&p) = parent.get(cur) {
                    path.push(p);
                    cur = p;
                }
                path.reverse();
                return Some(path);
            }
            queue.push_back(next);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn adj<'a>(edges: &[(&'a str, &'a str)]) -> BTreeMap<&'a str, Vec<&'a str>> {
        let mut m: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for (a, b) in edges {
            m.entry(a).or_default().push(b);
        }
        m
    }

    #[test]
    fn member_manifest_requires_inherited_or_exact_dependencies() {
        let manifest: toml::Value = toml::from_str(
            r#"
            [package]
            name = "m"
            [dependencies]
            a = { workspace = true }
            b = "=1.2.3"
            c = { version = "=0.1.0", features = ["x"] }
            d = { path = "../d" }
            e = { git = "https://example.invalid/e", rev = "abc" }
            floating = "1.2"
            caret = { version = "^1", optional = true }
            [dev-dependencies]
            f = { workspace = true }
            loose = { version = "0.5" }
            [build-dependencies]
            g = "2"
            [target.'cfg(unix)'.dependencies]
            h = { git = "https://example.invalid/h", branch = "main" }
            "#,
        )
        .unwrap();
        assert_eq!(
            unpinned_member_dependencies(&manifest),
            vec![
                "dependencies.caret",
                "dependencies.floating",
                "dev-dependencies.loose",
                "build-dependencies.g",
                "target.cfg(unix).dependencies.h",
            ]
        );
        let clean: toml::Value =
            toml::from_str("[package]\nname = \"m\"\n[dependencies]\na = { workspace = true }")
                .unwrap();
        assert!(unpinned_member_dependencies(&clean).is_empty());
    }

    #[test]
    fn boundary_constructors_are_confined_to_their_crates() {
        let text = "// PeerProvenance::from_transport in a comment is fine\n\
                    /// and VerifierToken::for_boundary in docs too\n\
                    let p = PeerProvenance::from_transport(a, b, c);\n";
        assert_eq!(
            boundary_violations("coord-storage", text),
            vec!["PeerProvenance::from_transport"]
        );
        assert!(boundary_violations("coord-transport", text).is_empty());
        let both = "VerifierToken::for_boundary(); PeerProvenance::from_transport(x, y, z);";
        assert_eq!(
            boundary_violations("coord-consensus", both),
            vec![
                "VerifierToken::for_boundary",
                "PeerProvenance::from_transport"
            ]
        );
        assert_eq!(
            boundary_violations("coord-collector", both),
            vec!["PeerProvenance::from_transport"]
        );
    }

    #[test]
    fn reach_finds_transitive_path() {
        let a = adj(&[("prod", "lib"), ("lib", "sim")]);
        let forbidden = BTreeSet::from(["sim"]);
        assert_eq!(
            reach(&a, "prod", &forbidden),
            Some(vec!["prod", "lib", "sim"])
        );
        assert_eq!(reach(&a, "lib", &BTreeSet::from(["other"])), None);
    }

    #[test]
    fn synthetic_metadata_rejects_production_to_test_only_edge() {
        let json = r#"{
          "packages": [
            {"name":"prod","id":"prod","source":null,"metadata":{"tuplesky":{"role":"production"}}},
            {"name":"sim","id":"sim","source":null,"metadata":{"tuplesky":{"role":"test-only"}}},
            {"name":"proptest","id":"proptest","source":"registry+x","metadata":null}
          ],
          "workspace_members": ["prod","sim"],
          "resolve": {"nodes": [
            {"id":"prod","deps":[{"pkg":"sim","dep_kinds":[{"kind":null}]}]},
            {"id":"sim","deps":[{"pkg":"proptest","dep_kinds":[{"kind":null}]}]},
            {"id":"proptest","deps":[]}
          ]}
        }"#;
        let m: Metadata = serde_json::from_str(json).unwrap();
        let err = check_graph(&m).unwrap_err().to_string();
        assert!(err.contains("prod -> sim"), "{err}");
    }

    #[test]
    fn synthetic_metadata_allows_dev_dependency_edge() {
        let json = r#"{
          "packages": [
            {"name":"prod","id":"prod","source":null,"metadata":{"tuplesky":{"role":"core"}}},
            {"name":"proptest","id":"proptest","source":"registry+x","metadata":null}
          ],
          "workspace_members": ["prod"],
          "resolve": {"nodes": [
            {"id":"prod","deps":[{"pkg":"proptest","dep_kinds":[{"kind":"dev"}]}]},
            {"id":"proptest","deps":[]}
          ]}
        }"#;
        let m: Metadata = serde_json::from_str(json).unwrap();
        check_graph(&m).unwrap();
    }

    #[test]
    fn core_crate_cannot_reach_runtime_or_entropy() {
        let json = r#"{
          "packages": [
            {"name":"core","id":"core","source":null,"metadata":{"tuplesky":{"role":"core"}}},
            {"name":"tool","id":"tool","source":null,"metadata":{"tuplesky":{"role":"tool"}}},
            {"name":"tokio","id":"tokio","source":"registry+x","metadata":null}
          ],
          "workspace_members": ["core","tool"],
          "resolve": {"nodes": [
            {"id":"core","deps":[{"pkg":"tokio","dep_kinds":[{"kind":null}]}]},
            {"id":"tool","deps":[{"pkg":"tokio","dep_kinds":[{"kind":null}]}]},
            {"id":"tokio","deps":[]}
          ]}
        }"#;
        let m: Metadata = serde_json::from_str(json).unwrap();
        let err = check_graph(&m).unwrap_err().to_string();
        assert!(err.contains("core crate core reaches"), "{err}");
        // A tool crate may use a runtime.
        let json = json.replace(
            r#"{"id":"core","deps":[{"pkg":"tokio","dep_kinds":[{"kind":null}]}]}"#,
            r#"{"id":"core","deps":[]}"#,
        );
        let m: Metadata = serde_json::from_str(&json).unwrap();
        check_graph(&m).unwrap();
    }

    #[test]
    fn feature_audit_rejects_unexpected_features() {
        let json = r#"{
          "packages": [
            {"name":"tool","id":"tool","source":null,"metadata":{"tuplesky":{"role":"tool"}}},
            {"name":"raft-engine","id":"re","source":"git+https://github.com/tikv/raft-engine?rev=097c499a19fbb38754c73aa2f31532329df7c0c6#097c499a19fbb38754c73aa2f31532329df7c0c6","metadata":null}
          ],
          "workspace_members": ["tool"],
          "resolve": {"nodes": [
            {"id":"tool","deps":[{"pkg":"re","dep_kinds":[{"kind":null}]}]},
            {"id":"re","deps":[],"features":["scripting"]}
          ]}
        }"#;
        let m: Metadata = serde_json::from_str(json).unwrap();
        let err = check_graph(&m).unwrap_err().to_string();
        assert!(err.contains("resolved features"), "{err}");
        let m: Metadata =
            serde_json::from_str(&json.replace(r#""features":["scripting"]"#, r#""features":[]"#))
                .unwrap();
        check_graph(&m).unwrap();
    }

    #[test]
    fn synthetic_metadata_requires_role_and_rejects_forbidden_and_git() {
        let no_role = r#"{"packages":[{"name":"x","id":"x","source":null,"metadata":null}],
          "workspace_members":["x"],"resolve":{"nodes":[{"id":"x","deps":[]}]}}"#;
        let m: Metadata = serde_json::from_str(no_role).unwrap();
        assert!(
            check_graph(&m)
                .unwrap_err()
                .to_string()
                .contains("missing package.metadata")
        );

        let forbidden = r#"{"packages":[
            {"name":"x","id":"x","source":null,"metadata":{"tuplesky":{"role":"core"}}},
            {"name":"openssl","id":"o","source":"registry+x","metadata":null}],
          "workspace_members":["x"],"resolve":{"nodes":[{"id":"x","deps":[]},{"id":"o","deps":[]}]}}"#;
        let m: Metadata = serde_json::from_str(forbidden).unwrap();
        assert!(
            check_graph(&m)
                .unwrap_err()
                .to_string()
                .contains("forbidden crate openssl")
        );

        let git = r#"{"packages":[
            {"name":"x","id":"x","source":null,"metadata":{"tuplesky":{"role":"core"}}},
            {"name":"y","id":"y","source":"git+https://example.invalid/y?rev=1#1","metadata":null}],
          "workspace_members":["x"],"resolve":{"nodes":[{"id":"x","deps":[]},{"id":"y","deps":[]}]}}"#;
        let m: Metadata = serde_json::from_str(git).unwrap();
        assert!(
            check_graph(&m)
                .unwrap_err()
                .to_string()
                .contains("unreviewed git source")
        );
    }
}
