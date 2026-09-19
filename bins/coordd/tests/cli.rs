//! Acceptance for the daemon's own command surface (task-43).
//!
//! These drive the built binary, because what they hold is true of the
//! process and not of a function: which command creates a store, which
//! refuses to, and what a process does before it accepts a connection.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

fn binary() -> PathBuf {
    // The integration test's own executable sits beside the binary under
    // test, whichever profile built it.
    let mut path = std::env::current_exe().expect("test binary");
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    path.join("coordd")
}

fn workspace(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("coordd-cli-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("workspace");
    path
}

/// A configuration whose listeners are ephemeral loopback ports, so the
/// test never depends on a fixed port or on IPv6 being available.
fn config(dir: &Path) -> PathBuf {
    let text = format!(
        r#"config_version = 2
role = "voter-frontend-observer"
cluster_manifest = "{root}/genesis.json"
domain = "control-plane-test"
state_directory = "{root}"

[listen]
api_quic = "127.0.0.1:0"
peer_quic = "127.0.0.1:0"

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
trust_bundle = "{root}/roots.pem"
node_certificate = "{root}/node.pem"
node_key = "{root}/node.key"
"#,
        root = dir.display()
    );
    let path = dir.join("coordd.toml");
    std::fs::write(&path, text).expect("write config");
    path
}

struct Run {
    code: Option<i32>,
    out: String,
    err: String,
}

/// Run `coordd` and wait for it to finish, within a bound.
///
/// The bound is not a convenience. Every case here is one where the
/// daemon must *stop*, and the way each of them regresses is that the
/// daemon starts instead -- at which point it holds its listeners and
/// parks, for ever. Without a bound the regression is a test suite that
/// hangs rather than one that fails, and a hang says nothing about
/// which case broke.
fn run(config: &Path, args: &[&str]) -> Run {
    let mut child = Command::new(binary())
        .arg("--config")
        .arg(config)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("coordd started");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        match child.try_wait().expect("wait") {
            Some(_) => break,
            None if std::time::Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("coordd {args:?} did not stop within 30s: it started instead of refusing");
            }
            None => std::thread::sleep(Duration::from_millis(20)),
        }
    }
    let output = child.wait_with_output().expect("output");
    Run {
        code: output.status.code(),
        out: String::from_utf8_lossy(&output.stdout).into_owned(),
        err: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

/// A node serves the store it already has. Finding none is reported --
/// never repaired.
///
/// This is the guard every other one is downstream of. A daemon that
/// created a store when it failed to find one would turn a lost disk, an
/// unmounted volume or a mistyped path into a fresh, empty, *valid*
/// node, which would then vote, having forgotten everything it had ever
/// promised.
#[test]
fn a_missing_store_stops_the_daemon_rather_than_being_created() {
    let dir = workspace("missing");
    let config = config(&dir);

    let refused = run(&config, &[]);
    assert_eq!(refused.code, Some(2), "{}{}", refused.out, refused.err);
    assert!(
        refused.err.contains("no store at"),
        "the refusal did not say what was missing: {}",
        refused.err
    );
    // And it says what to do, because the right answer differs: a
    // genuinely new node is initialized, an existing one is found.
    assert!(refused.err.contains("coordd init"));
    assert!(
        !dir.join("state").exists(),
        "a failed start created a store anyway"
    );
}

/// Initialization is its own command, it works once, and a second one is
/// refused: running it against a node that already has a store would
/// give that node an empty history under its own identity.
#[test]
fn initializing_is_deliberate_and_happens_exactly_once() {
    let dir = workspace("init");
    let config = config(&dir);

    let first = run(&config, &["init"]);
    assert_eq!(first.code, Some(0), "{}{}", first.out, first.err);
    assert!(
        first.out.contains("gen-000001"),
        "initialization did not say what it made: {}",
        first.out
    );
    assert!(dir.join("state").join("gen-000001").is_dir());

    let second = run(&config, &["init"]);
    assert_eq!(second.code, Some(2));
    assert!(
        second.err.contains("already exists"),
        "a second initialization was not refused: {}",
        second.err
    );
    // The first generation is untouched: the refusal is a refusal, not a
    // partial replacement.
    assert!(dir.join("state").join("gen-000001").is_dir());
}

/// A configuration a node cannot serve is refused under `--check` --
/// which is where an operator would want to find out, rather than at
/// the first start in production.
#[test]
fn a_configuration_this_build_cannot_serve_is_refused_under_check() {
    let dir = workspace("profile");
    let path = config(&dir);
    let text = std::fs::read_to_string(&path).expect("read");

    // The experimental engine is built and tested, so naming it is a
    // deliberate act rather than a typo. It carries no production
    // support, so it is refused all the same.
    std::fs::write(
        &path,
        text.replace("[state]\nroot", "[state]\nengine = \"fjall\"\nroot"),
    )
    .expect("write");
    let checked = run(&path, &["--check"]);
    assert_eq!(checked.code, Some(2), "{}{}", checked.out, checked.err);
    assert!(
        checked.err.contains("ExperimentalEngine"),
        "the refusal did not name the engine: {}",
        checked.err
    );

    // Without it, the same configuration checks out.
    std::fs::write(&path, text).expect("restore");
    assert_eq!(run(&path, &["--check"]).code, Some(0));
}

/// `--check` validates and does nothing else: it does not create a
/// store, and it does not bind a listener. An operator runs it on a
/// machine that is not meant to be serving.
#[test]
fn checking_a_configuration_touches_nothing() {
    let dir = workspace("check");
    let config = config(&dir);

    let checked = run(&config, &["--check"]);
    assert_eq!(checked.code, Some(0), "{}{}", checked.out, checked.err);
    assert!(
        !dir.join("state").exists(),
        "--check created a store: {}",
        checked.out
    );
    assert!(
        !checked.out.contains("listening"),
        "--check bound a listener: {}",
        checked.out
    );
}
