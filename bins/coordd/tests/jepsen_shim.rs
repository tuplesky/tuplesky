//! The Jepsen client shim against a running domain.
//!
//! `coord-jepsen` is what a Jepsen test's clients run: one bound session
//! per client, JSON lines in and out, every operation reported as `ok`,
//! `fail` or `info`. A checker takes `fail` as a promise that the
//! operation had no effect, so the two things shown here are that each
//! operation does what its line says against a real domain, and that a
//! write whose frontend is gone is `info` -- never `fail` -- while a read
//! that could not complete is `fail`.

use std::path::PathBuf;
use std::time::Duration;

use coord_harness::domain::{Plan, provision};
use coord_jepsen::{Codec, Session, Timing, protocol};
use serde_json::{Value, json};

fn binary() -> PathBuf {
    let mut path = std::env::current_exe().expect("test binary");
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    path.join("coordd")
}

fn workspace(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("coordd-jepsen-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("workspace");
    path
}

fn timing() -> Timing {
    Timing {
        attempt: Duration::from_secs(2),
        budget: Duration::from_secs(10),
        connect: Duration::from_secs(10),
    }
}

/// Wait until every voter's last reports show both planes linked to the
/// other two, as `multi_host.rs` does: a daemon is live before its
/// links are, and a request before then waits on them.
fn meshed(provisioned: &coord_harness::domain::Provisioned) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let last = |said: &str, prefix: &str| {
        said.lines()
            .filter_map(|line| line.strip_prefix(prefix))
            .filter_map(|rest| rest.split(' ').next()?.parse::<usize>().ok())
            .next_back()
    };
    for node in &provisioned.voters {
        loop {
            let said =
                std::fs::read_to_string(node.directory.join("coordd.log")).unwrap_or_default();
            if last(&said, "peers connected=") == Some(2)
                && last(&said, "voters submittable=") == Some(2)
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "voter {} never meshed:\n{said}",
                node.node
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

async fn ask(session: &mut Session, codec: &Codec, line: Value) -> Value {
    protocol::handle(session, codec, &line.to_string()).await
}

/// Every function of the line protocol, through a voter that is not the
/// genesis leader.
#[tokio::test(flavor = "multi_thread")]
async fn each_operation_does_what_its_line_says() {
    let dir = workspace("operations");
    let provisioned = provision(&Plan::loopback(dir.clone(), 3, 0)).expect("provisioned");
    coord_harness::run::initialize(&binary(), &provisioned).expect("initialized");
    let _daemons = coord_harness::run::start_all(&binary(), &provisioned).expect("started");
    meshed(&provisioned);

    // One minter for the process: two would name the same session.
    let minter = coord_harness::issuer::Minter::of(&provisioned).expect("minter");
    let mut session = Session::open_minted(&provisioned, &minter, 1, 1, timing())
        .await
        .expect("bound");
    let codec = Codec {
        namespace: session.namespace,
        prefix: "jepsen-test/".into(),
    };
    let ok = |value: Value| json!({"type": "ok", "value": value});

    assert_eq!(
        ask(&mut session, &codec, json!({"f": "read", "key": "r"})).await,
        ok(Value::Null)
    );
    // A compare against what that read reported for the absent key holds.
    assert_eq!(
        ask(
            &mut session,
            &codec,
            json!({"f": "cas", "key": "r", "value": [null, 2]})
        )
        .await,
        ok(json!([null, 2]))
    );
    assert_eq!(
        ask(
            &mut session,
            &codec,
            json!({"f": "write", "key": "r", "value": 3})
        )
        .await,
        ok(json!(3))
    );
    assert_eq!(
        ask(&mut session, &codec, json!({"f": "read", "key": "r"})).await,
        ok(json!(3))
    );
    assert_eq!(
        ask(
            &mut session,
            &codec,
            json!({"f": "cas", "key": "r", "value": [3, 4]})
        )
        .await,
        ok(json!([3, 4]))
    );
    // The value is 4 now, so a compare against 3 does not hold: an
    // established result in which nothing was written.
    assert_eq!(
        ask(
            &mut session,
            &codec,
            json!({"id": 9, "f": "cas", "key": "r", "value": [3, 5]})
        )
        .await,
        json!({"type": "fail", "error": "guard-failed", "id": 9})
    );
    assert_eq!(
        ask(&mut session, &codec, json!({"f": "read", "key": "r"})).await,
        ok(json!(4))
    );

    // A transaction sees its own appends, and the next one sees them all.
    assert_eq!(
        ask(
            &mut session,
            &codec,
            json!({"f": "txn", "value": [["r", 1, null], ["append", 1, 7], ["r", 1, null], ["append", 2, 8]]})
        )
        .await,
        ok(json!([["r", 1, null], ["append", 1, 7], ["r", 1, [7]], ["append", 2, 8]]))
    );
    assert_eq!(
        ask(
            &mut session,
            &codec,
            json!({"f": "txn", "value": [["append", 1, 9], ["r", 1, null], ["r", 2, null], ["w", 3, 1], ["r", 3, null]]})
        )
        .await,
        ok(json!([["append", 1, 9], ["r", 1, [7, 9]], ["r", 2, [8]], ["w", 3, 1], ["r", 3, 1]]))
    );
    // A second session on another voter reads what the first committed.
    let mut other = Session::open_minted(&provisioned, &minter, 2, 2, timing())
        .await
        .expect("bound");
    assert_eq!(
        ask(
            &mut other,
            &codec,
            json!({"f": "txn", "value": [["r", 1, null], ["r", 2, null]]})
        )
        .await,
        ok(json!([["r", 1, [7, 9]], ["r", 2, [8]]]))
    );

    let refused = ask(&mut session, &codec, json!({"f": "increment", "key": "r"})).await;
    assert_eq!(refused["type"], "fail");
    let refused = ask(
        &mut session,
        &codec,
        json!({"f": "txn", "value": [["x", 1, 2]]}),
    )
    .await;
    assert_eq!(refused["type"], "fail");
}

/// With the session's frontend gone, a write is `info` and a read is
/// `fail`: nothing the shim cannot vouch for is reported as not having
/// happened, and nothing that cannot have happened is reported as
/// unknown.
#[tokio::test(flavor = "multi_thread")]
async fn a_write_whose_frontend_is_gone_is_info_and_a_read_is_fail() {
    let dir = workspace("gone");
    let provisioned = provision(&Plan::loopback(dir.clone(), 3, 0)).expect("provisioned");
    coord_harness::run::initialize(&binary(), &provisioned).expect("initialized");
    let mut daemons = coord_harness::run::start_all(&binary(), &provisioned).expect("started");
    meshed(&provisioned);

    let short = Timing {
        attempt: Duration::from_millis(500),
        budget: Duration::from_secs(3),
        connect: Duration::from_secs(1),
    };
    let mut session = Session::open(&dir, 2, 3, short).await.expect("bound");
    let codec = Codec {
        namespace: session.namespace,
        prefix: "jepsen-test/".into(),
    };
    assert_eq!(
        ask(
            &mut session,
            &codec,
            json!({"f": "write", "key": "g", "value": 1})
        )
        .await["type"],
        "ok"
    );

    // Voter 3's frontend is this session's; stopping it loses every
    // answer from here on.
    drop(daemons.remove(2));

    // A read that is never answered is `fail`, and its identity is
    // retired with it: kept bound, it would hold the session's
    // acknowledged floor, and a window later every request of the
    // session would be refused as out of window.
    let floor = session.retired_through();
    let answer = ask(&mut session, &codec, json!({"f": "read", "key": "g"})).await;
    assert_eq!(answer["type"], "fail", "{answer}");
    assert_eq!(session.retired_through(), floor + 1);

    for line in [
        json!({"f": "write", "key": "g", "value": 2}),
        json!({"f": "cas", "key": "g", "value": [1, 2]}),
        json!({"f": "txn", "value": [["w", "h", 1]]}),
    ] {
        let answer = ask(&mut session, &codec, line.clone()).await;
        // The transaction's snapshot read comes first and cannot be
        // answered either, so it ends before anything is written: `fail`.
        let expected = if line["f"] == "txn" { "fail" } else { "info" };
        assert_eq!(answer["type"], expected, "{line} came to {answer}");
    }
    let answer = ask(&mut session, &codec, json!({"f": "read", "key": "g"})).await;
    assert_eq!(answer["type"], "fail", "{answer}");
}
