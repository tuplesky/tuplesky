//! The line protocol a Jepsen client speaks to the shim.
//!
//! One JSON object per line in, one per line out, strictly in turn:
//!
//! ```text
//! {"f": "read",  "key": K}                      -> value, or null when absent
//! {"f": "write", "key": K, "value": V}          -> V
//! {"f": "cas",   "key": K, "value": [OLD, NEW]} -> [OLD, NEW], or fail
//! {"f": "txn",   "value": [["r", K, null], ["append", K, V], ["w", K, V], ...]}
//!                                               -> the micro-operations, reads filled in
//! ```
//!
//! and every answer is `{"type": "ok", "value": ...}`,
//! `{"type": "fail", "error": ...}` or `{"type": "info", "error": ...}`,
//! with the request's `"id"` echoed when it had one. Keys and values are
//! any JSON; the shim stores a value as its JSON text.
//!
//! A transaction that only reads is one snapshot read. One that writes
//! is optimistic, as the etcd test's is: a snapshot read of every key it
//! touches, then one transaction guarded on each of those keys'
//! modification revisions that writes the final value of every key it
//! wrote. The guard holding means nothing changed between the two, so
//! the reads of the first are the state the second committed against,
//! and the transaction is atomic at the second's execution point. The
//! guard failing is an established result in which nothing was written:
//! `fail`.

use serde_json::{Value, json};

use crate::ops::{self, Answer, Codec, Micro, Verdict};
use crate::session::Session;

/// Handle one request line.
pub async fn handle(session: &mut Session, codec: &Codec, line: &str) -> Value {
    let request: Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => return render(None, Verdict::Fail(format!("unparseable request: {e}"))),
    };
    let id = request.get("id").cloned();
    let verdict = match run(session, codec, &request).await {
        Ok(verdict) => verdict,
        Err(reason) => Verdict::Fail(reason),
    };
    render(id, verdict)
}

/// The answer line for `verdict`.
pub fn render(id: Option<Value>, verdict: Verdict) -> Value {
    let mut out = match verdict {
        Verdict::Ok(value) => json!({"type": "ok", "value": value}),
        Verdict::Fail(error) => json!({"type": "fail", "error": error}),
        Verdict::Info(error) => json!({"type": "info", "error": error}),
    };
    if let Some(id) = id {
        out["id"] = id;
    }
    out
}

fn field<'a>(request: &'a Value, name: &str) -> Result<&'a Value, String> {
    request
        .get(name)
        .ok_or_else(|| format!("the request has no `{name}`"))
}

async fn run(session: &mut Session, codec: &Codec, request: &Value) -> Result<Verdict, String> {
    match field(request, "f")?.as_str() {
        Some("read") => {
            let key = field(request, "key")?;
            let answer = session
                .execute_read(&codec.read(std::slice::from_ref(key)))
                .await;
            Ok(ops::read_verdict(answer, |r| {
                Ok(ops::snapshot(r, 1)?.remove(0).value)
            }))
        }
        Some("write") => {
            let key = field(request, "key")?;
            let value = field(request, "value")?;
            let answer = session.execute(&codec.write(key, value)).await;
            Ok(ops::write_verdict(answer, |r| {
                ops::put_applied(r).map(|()| Some(value.clone()))
            }))
        }
        Some("cas") => {
            let key = field(request, "key")?;
            let value = field(request, "value")?;
            let [old, new] = value.as_array().map(Vec::as_slice).unwrap_or_default() else {
                return Err(format!("a cas value is [old, new], not {value}"));
            };
            let answer = session.execute(&codec.cas(key, old, new)).await;
            Ok(ops::write_verdict(answer, |r| {
                ops::txn_succeeded(r).map(|ok| ok.then(|| value.clone()))
            }))
        }
        Some("txn") => {
            let mops = field(request, "value")?
                .as_array()
                .ok_or("a transaction is a list of micro-operations")?
                .iter()
                .map(Micro::parse)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(txn(session, codec, &mops).await)
        }
        _ => Err(format!("unknown function {}", request["f"])),
    }
}

async fn txn(session: &mut Session, codec: &Codec, mops: &[Micro]) -> Verdict {
    let keys = ops::keys_of(mops);
    let answer = session.execute_read(&codec.read(&keys)).await;
    let seen = match answer {
        Answer::Established(response) => match ops::snapshot(&response, keys.len()) {
            Ok(seen) => seen,
            Err(reason) => return Verdict::Fail(reason),
        },
        // Nothing has been written yet, whatever became of the read.
        Answer::NotSubmitted(reason) | Answer::Unknown(reason) => return Verdict::Fail(reason),
    };
    let applied = match ops::apply(codec, mops, &seen) {
        Ok(applied) => applied,
        Err(reason) => return Verdict::Fail(reason),
    };
    if applied.writes.is_empty() {
        return Verdict::Ok(Value::Array(applied.completed));
    }
    let answer = session
        .execute(&codec.guarded_write(&applied.observed, &applied.writes))
        .await;
    let completed = Value::Array(applied.completed);
    ops::write_verdict(answer, |r| {
        ops::txn_succeeded(r).map(|ok| ok.then_some(completed))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn answers_carry_the_request_id() {
        assert_eq!(
            render(Some(json!(7)), Verdict::Ok(json!([1]))),
            json!({"type": "ok", "value": [1], "id": 7})
        );
        assert_eq!(
            render(None, Verdict::Info("timeout".into())),
            json!({"type": "info", "error": "timeout"})
        );
        assert_eq!(
            render(None, Verdict::Fail("guard-failed".into())),
            json!({"type": "fail", "error": "guard-failed"})
        );
    }
}
