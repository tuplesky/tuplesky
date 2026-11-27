//! The replacement test spawns a child process. It runs in its own test
//! binary, so its fork never overlaps another test's write of an
//! executable script in the same process: a child forked while that
//! script is open for writing holds the descriptor until it execs, and the
//! script's own exec then fails with `ETXTBSY` ("Text file busy").

#![cfg(target_os = "linux")]

use coord_harness::run::{REPLACEMENT_FILE, replacement};

/// A replacement counts while it runs and names the voter's
/// configuration; an identifier that names something else, an exited
/// process, and no file at all do not.
#[test]
fn a_replacement_counts_only_while_it_runs_as_that_voter() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = dir.path().join("coordd.toml");
    assert_eq!(replacement(dir.path(), &config), None);

    // `sh -c SCRIPT NAME` keeps NAME in its command line, and a
    // script of two commands keeps the shell from exec'ing `sleep`.
    let mut child = std::process::Command::new("sh")
        .arg("-c")
        .arg("sleep 30; :")
        .arg(&config)
        .spawn()
        .expect("sh");
    std::fs::write(
        dir.path().join(REPLACEMENT_FILE),
        format!("{}\n", child.id()),
    )
    .expect("write");
    // Until the child has exec'd `sh`, its command line is still this
    // test's: wait for the exec, not for a guess at how long it takes.
    let by = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while replacement(dir.path(), &config).is_none() && std::time::Instant::now() < by {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert_eq!(replacement(dir.path(), &config), Some(child.id()));
    assert_eq!(
        replacement(dir.path(), &dir.path().join("other.toml")),
        None
    );

    child.kill().expect("kill");
    child.wait().expect("wait");
    assert_eq!(replacement(dir.path(), &config), None);

    std::fs::write(
        dir.path().join(REPLACEMENT_FILE),
        std::process::id().to_string(),
    )
    .expect("write");
    assert_eq!(replacement(dir.path(), &config), None);
}
