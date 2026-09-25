//! What a Jepsen operation asks of the domain, and what the domain's
//! answer means for it.
//!
//! Everything here is pure: a request is built from the operation, and an
//! answer is read back as `ok`, `fail` or `info`. The session in
//! [`crate::session`] only carries requests to the domain and answers
//! back.
//!
//! The one rule every verdict follows is Jepsen's: `fail` is a promise
//! that the operation had no effect, and a checker takes it at its word.
//! An operation that may have written is therefore `fail` only when the
//! domain said so in an established result -- a transaction whose guard
//! did not hold, or a command the planner refused -- or when the request
//! was refused before anything could be submitted. Everything else that
//! is not `ok` is `info`. A read has no effect, so a read that did not
//! complete is simply `fail`.

use std::collections::BTreeMap;

use coord_state::Response;
use coord_state::plan::{Outcome, RejectionReason};
use coord_types::ids::{KvRevision, NamespaceId};
use coord_types::logical_v1::{
    BranchOp, CanonicalOperation, Compare, CompareOperand, CompareResult, CompareTarget, KeyRange,
    LogicalRequest, PutOp, RangeOp, TxnOp,
};
use serde_json::Value;

/// How an operation ended, in Jepsen's terms.
#[derive(Clone, Debug, PartialEq)]
pub enum Verdict {
    /// It happened; `value` is the completed operation's value.
    Ok(Value),
    /// It did not happen, and never will.
    Fail(String),
    /// It may or may not have happened.
    Info(String),
}

/// What the session brought back for one request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Answer {
    /// An established result: the decoded response.
    Established(Response),
    /// A refusal that guarantees nothing was submitted: the request was
    /// malformed, too large, or its identity was bound to another
    /// payload.
    NotSubmitted(String),
    /// Anything else: a deadline, a lost connection that could not be
    /// resolved, or a refusal that does not rule out execution.
    Unknown(String),
}

/// Maps Jepsen keys and values onto domain keys and values.
#[derive(Clone, Debug)]
pub struct Codec {
    /// Namespace every request names.
    pub namespace: NamespaceId,
    /// Prefix of every key, so a test's keys never collide with anything
    /// else stored in the namespace.
    pub prefix: String,
}

impl Codec {
    /// The domain key of a Jepsen key: the prefix and the key's JSON text.
    pub fn key(&self, key: &Value) -> Vec<u8> {
        let mut out = self.prefix.clone().into_bytes();
        out.extend_from_slice(key.to_string().as_bytes());
        out
    }

    fn request(&self, operation: CanonicalOperation) -> LogicalRequest {
        let mut request = LogicalRequest::new(self.namespace, operation);
        request.canonicalize();
        request
    }

    /// Read the latest committed value of each key in one transaction:
    /// one snapshot, whatever the number of keys.
    pub fn read(&self, keys: &[Value]) -> LogicalRequest {
        self.request(CanonicalOperation::Txn(TxnOp {
            compares: Vec::new(),
            success: keys
                .iter()
                .map(|k| BranchOp::Range(exact(self.key(k))))
                .collect(),
            failure: Vec::new(),
        }))
    }

    /// Put `value` at `key`, unconditionally.
    pub fn write(&self, key: &Value, value: &Value) -> LogicalRequest {
        self.request(CanonicalOperation::Put(put(self.key(key), value)))
    }

    /// Put `new` at `key` if and only if its current value is `old`.
    ///
    /// `old` is compared as the stored bytes. Values are written as JSON
    /// text by this codec only, so one value has one encoding. A `null`
    /// `old` is the value a read reports for an absent key, so it is
    /// compared as absence -- version zero -- and not as the bytes `null`,
    /// which an absent key never holds.
    pub fn cas(&self, key: &Value, old: &Value, new: &Value) -> LogicalRequest {
        let key = self.key(key);
        let compare = if old.is_null() {
            Compare {
                key: key.clone(),
                target: CompareTarget::Version,
                result: CompareResult::Equal,
                operand: CompareOperand::Counter(0),
            }
        } else {
            Compare {
                key: key.clone(),
                target: CompareTarget::Value,
                result: CompareResult::Equal,
                operand: CompareOperand::Bytes(encode(old)),
            }
        };
        self.request(CanonicalOperation::Txn(TxnOp {
            compares: vec![compare],
            success: vec![BranchOp::Put(put(key, new))],
            failure: Vec::new(),
        }))
    }

    /// Write `writes` if and only if no key in `observed` has been
    /// modified since it was read at the given modification revision
    /// (zero: absent).
    pub fn guarded_write(
        &self,
        observed: &BTreeMap<Vec<u8>, KvRevision>,
        writes: &BTreeMap<Vec<u8>, Value>,
    ) -> LogicalRequest {
        self.request(CanonicalOperation::Txn(TxnOp {
            compares: observed
                .iter()
                .map(|(key, revision)| Compare {
                    key: key.clone(),
                    target: CompareTarget::ModRevision,
                    result: CompareResult::Equal,
                    operand: CompareOperand::Counter(revision.get()),
                })
                .collect(),
            success: writes
                .iter()
                .map(|(key, value)| BranchOp::Put(put(key.clone(), value)))
                .collect(),
            failure: Vec::new(),
        }))
    }
}

fn exact(key: Vec<u8>) -> RangeOp {
    RangeOp {
        range: KeyRange::exact(key),
        revision: None,
        limit: 1,
        keys_only: false,
        count_only: false,
    }
}

fn put(key: Vec<u8>, value: &Value) -> PutOp {
    PutOp {
        key,
        value: encode(value),
        lease: None,
        prev_kv: false,
    }
}

/// The stored bytes of a value.
pub fn encode(value: &Value) -> Vec<u8> {
    value.to_string().into_bytes()
}

/// A stored value read back. Bytes this codec did not write are a
/// string of what they held, so a checker sees them rather than a
/// silent null.
pub fn decode(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(bytes).into_owned()))
}

/// One key as a snapshot read saw it.
#[derive(Clone, Debug, PartialEq)]
pub struct Seen {
    /// The value (`Null`: absent).
    pub value: Value,
    /// Its modification revision (zero: absent).
    pub mod_revision: KvRevision,
}

/// Read an established snapshot read of `keys`, in the order asked.
pub fn snapshot(response: &Response, keys: usize) -> Result<Vec<Seen>, String> {
    let Outcome::Txn { succeeded, results } = &response.outcome else {
        return Err(refused(&response.outcome));
    };
    if !succeeded || results.len() != keys {
        return Err(format!(
            "a read of {keys} keys came back with {} results",
            results.len()
        ));
    }
    results
        .iter()
        .map(|result| match result {
            Outcome::Range { items, .. } => Ok(match items.first() {
                Some(item) => Seen {
                    value: decode(&item.entry.value),
                    mod_revision: item.entry.mod_revision,
                },
                None => Seen {
                    value: Value::Null,
                    mod_revision: KvRevision::ZERO,
                },
            }),
            other => Err(refused(other)),
        })
        .collect()
}

/// The name of an established outcome that is not the one asked for.
fn refused(outcome: &Outcome) -> String {
    match outcome {
        Outcome::ErrRejected { reason } => format!("rejected-{reason:?}").to_lowercase(),
        Outcome::ErrSessionInvalid => "session-invalid".into(),
        Outcome::ErrPermissionDenied => "permission-denied".into(),
        other => {
            let name = format!("{other:?}");
            name.split([' ', '{', '('])
                .next()
                .unwrap_or("unexpected")
                .to_lowercase()
        }
    }
}

/// The verdict on an operation with no effect: a read.
pub fn read_verdict(
    answer: Answer,
    complete: impl FnOnce(&Response) -> Result<Value, String>,
) -> Verdict {
    match answer {
        Answer::Established(response) => match complete(&response) {
            Ok(value) => Verdict::Ok(value),
            Err(reason) => Verdict::Fail(reason),
        },
        Answer::NotSubmitted(reason) | Answer::Unknown(reason) => Verdict::Fail(reason),
    }
}

/// Whether an established refusal leaves open that the request ran.
///
/// Two rejections name a retry key rather than the request: a sequence
/// at or below the session's retired floor (`RetryTooOld`), which is not
/// a replay of whatever it did, and a retained result that current
/// authorization no longer hands out (`RetryUnauthorized`), which was
/// executed. Neither says the operation had no effect.
fn may_have_run(outcome: &Outcome) -> bool {
    matches!(
        outcome,
        Outcome::ErrRejected {
            reason: RejectionReason::RetryTooOld | RejectionReason::RetryUnauthorized
        }
    )
}

/// The verdict on an operation that may write.
///
/// `complete` reads an established response: `Ok(Some(value))` is the
/// operation done, `Ok(None)` is an established result in which it did
/// not happen (a guard that did not hold). An established outcome of
/// another kind is a planner refusal, which changed nothing -- except the
/// two refusals of a retry key that may name a request that ran, which
/// are `info`.
pub fn write_verdict(
    answer: Answer,
    complete: impl FnOnce(&Response) -> Result<Option<Value>, String>,
) -> Verdict {
    match answer {
        Answer::Established(response) if may_have_run(&response.outcome) => {
            Verdict::Info(refused(&response.outcome))
        }
        Answer::Established(response) => match complete(&response) {
            Ok(Some(value)) => Verdict::Ok(value),
            Ok(None) => Verdict::Fail("guard-failed".into()),
            Err(reason) => Verdict::Fail(reason),
        },
        Answer::NotSubmitted(reason) => Verdict::Fail(reason),
        Answer::Unknown(reason) => Verdict::Info(reason),
    }
}

/// Whether an established transaction ran its success branch.
pub fn txn_succeeded(response: &Response) -> Result<bool, String> {
    match &response.outcome {
        Outcome::Txn { succeeded, .. } => Ok(*succeeded),
        other => Err(refused(other)),
    }
}

/// Whether an established put applied.
pub fn put_applied(response: &Response) -> Result<(), String> {
    match &response.outcome {
        Outcome::Put { .. } => Ok(()),
        other => Err(refused(other)),
    }
}

/// One micro-operation of an Elle transaction.
#[derive(Clone, Debug, PartialEq)]
pub enum Micro {
    /// Read `key`.
    Read(Value),
    /// Append `value` to the list at `key`.
    Append(Value, Value),
    /// Write `value` at `key`.
    Write(Value, Value),
}

impl Micro {
    /// Parse `["r", k, _]`, `["append", k, v]` or `["w", k, v]`.
    pub fn parse(mop: &Value) -> Result<Micro, String> {
        let parts = mop
            .as_array()
            .filter(|p| p.len() == 3)
            .ok_or_else(|| format!("a micro-operation is [f, k, v], not {mop}"))?;
        let key = parts[1].clone();
        match parts[0].as_str() {
            Some("r") => Ok(Micro::Read(key)),
            Some("append") => Ok(Micro::Append(key, parts[2].clone())),
            Some("w") => Ok(Micro::Write(key, parts[2].clone())),
            _ => Err(format!("unknown micro-operation {}", parts[0])),
        }
    }

    /// The key it touches.
    pub fn key(&self) -> &Value {
        match self {
            Micro::Read(k) | Micro::Append(k, _) | Micro::Write(k, _) => k,
        }
    }

    /// Whether it writes.
    pub fn writes(&self) -> bool {
        !matches!(self, Micro::Read(_))
    }
}

/// The distinct keys of a transaction, in first-touched order.
pub fn keys_of(mops: &[Micro]) -> Vec<Value> {
    let mut keys: Vec<Value> = Vec::new();
    for mop in mops {
        if !keys.contains(mop.key()) {
            keys.push(mop.key().clone());
        }
    }
    keys
}

/// A transaction carried out against a snapshot: the completed
/// micro-operations, and the final value of each key it wrote.
#[derive(Clone, Debug, PartialEq)]
pub struct Applied {
    /// The micro-operations with every read filled in.
    pub completed: Vec<Value>,
    /// Domain key to final value, for each key written.
    pub writes: BTreeMap<Vec<u8>, Value>,
    /// Domain key to the modification revision the snapshot saw, for
    /// each key touched.
    pub observed: BTreeMap<Vec<u8>, KvRevision>,
}

/// Carry out `mops` in order against `seen` (indexed as [`keys_of`]).
///
/// A read after a write in the same transaction sees that write, as the
/// transaction's own effects are visible to it. An append to a key that
/// holds something other than a list is an error: the test never writes
/// both kinds to one key.
pub fn apply(codec: &Codec, mops: &[Micro], seen: &[Seen]) -> Result<Applied, String> {
    let keys = keys_of(mops);
    if keys.len() != seen.len() {
        return Err(format!(
            "{} keys touched and {} read",
            keys.len(),
            seen.len()
        ));
    }
    // Keyed by domain key: a JSON value has no order of its own.
    let mut state: BTreeMap<Vec<u8>, Value> = BTreeMap::new();
    let mut observed = BTreeMap::new();
    for (key, seen) in keys.iter().zip(seen) {
        state.insert(codec.key(key), seen.value.clone());
        observed.insert(codec.key(key), seen.mod_revision);
    }
    let mut written: Vec<Vec<u8>> = Vec::new();
    let mut completed = Vec::with_capacity(mops.len());
    for mop in mops {
        let domain_key = codec.key(mop.key());
        match mop {
            Micro::Read(key) => {
                let value = state.get(&domain_key).cloned().unwrap_or(Value::Null);
                completed.push(Value::Array(vec!["r".into(), key.clone(), value]));
            }
            Micro::Append(key, element) => {
                let current = state.entry(domain_key.clone()).or_insert(Value::Null);
                let mut list = match current.take() {
                    Value::Null => Vec::new(),
                    Value::Array(list) => list,
                    other => return Err(format!("key {key} holds {other}, not a list")),
                };
                list.push(element.clone());
                *current = Value::Array(list);
                written.push(domain_key);
                completed.push(Value::Array(vec![
                    "append".into(),
                    key.clone(),
                    element.clone(),
                ]));
            }
            Micro::Write(key, value) => {
                state.insert(domain_key.clone(), value.clone());
                written.push(domain_key);
                completed.push(Value::Array(vec!["w".into(), key.clone(), value.clone()]));
            }
        }
    }
    let writes = written
        .into_iter()
        .map(|key| {
            let value = state[&key].clone();
            (key, value)
        })
        .collect();
    Ok(Applied {
        completed,
        writes,
        observed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use coord_state::plan::RangeItem;
    use coord_state::view::KvEntry;
    use serde_json::json;

    fn codec() -> Codec {
        Codec {
            namespace: NamespaceId([7; 16]),
            prefix: "jepsen/".into(),
        }
    }

    fn rev(n: u64) -> KvRevision {
        KvRevision::new(n).unwrap()
    }

    fn range(value: Option<&Value>, mod_revision: u64) -> Outcome {
        Outcome::Range {
            items: value
                .map(|v| RangeItem {
                    key: b"k".to_vec(),
                    entry: KvEntry {
                        value: encode(v),
                        create_revision: rev(1),
                        mod_revision: rev(mod_revision),
                        version: 1,
                        lease: None,
                        lease_generation: None,
                    },
                })
                .into_iter()
                .collect(),
            count: u64::from(value.is_some()),
            more: false,
        }
    }

    fn established(outcome: Outcome) -> Answer {
        Answer::Established(Response {
            revision: rev(9),
            outcome,
        })
    }

    #[test]
    fn every_request_is_valid_and_canonical() {
        let c = codec();
        let mut observed = BTreeMap::new();
        observed.insert(c.key(&json!(2)), rev(4));
        observed.insert(c.key(&json!(1)), KvRevision::ZERO);
        let mut writes = BTreeMap::new();
        writes.insert(c.key(&json!(1)), json!([1, 2]));
        for request in [
            c.read(&[json!(1), json!(2)]),
            c.write(&json!(1), &json!(3)),
            c.cas(&json!(1), &json!(3), &json!(4)),
            c.guarded_write(&observed, &writes),
        ] {
            request.validate().expect("valid");
            assert!(request.operation.is_canonical());
        }
    }

    #[test]
    fn a_cas_from_null_compares_absence() {
        let c = codec();
        let target = |request: LogicalRequest| match request.operation {
            CanonicalOperation::Txn(txn) => {
                (txn.compares[0].target, txn.compares[0].operand.clone())
            }
            other => panic!("a cas is a transaction, not {other:?}"),
        };
        assert_eq!(
            target(c.cas(&json!("k"), &Value::Null, &json!(1))),
            (CompareTarget::Version, CompareOperand::Counter(0))
        );
        assert_eq!(
            target(c.cas(&json!("k"), &json!(1), &json!(2))),
            (CompareTarget::Value, CompareOperand::Bytes(b"1".to_vec()))
        );
    }

    #[test]
    fn keys_are_prefixed_json_and_values_round_trip() {
        let c = codec();
        assert_eq!(c.key(&json!(5)), b"jepsen/5");
        assert_eq!(c.key(&json!("x")), b"jepsen/\"x\"");
        for value in [json!(3), json!([1, 2, 3]), json!(null)] {
            assert_eq!(decode(&encode(&value)), value);
        }
        assert_eq!(decode(b"not json"), json!("not json"));
    }

    #[test]
    fn a_transaction_sees_its_own_writes_and_writes_each_key_once() {
        let c = codec();
        let mops: Vec<Micro> = [
            json!(["r", 1, null]),
            json!(["append", 1, 5]),
            json!(["r", 1, null]),
            json!(["append", 1, 6]),
            json!(["w", 2, 9]),
            json!(["r", 2, null]),
        ]
        .iter()
        .map(|m| Micro::parse(m).unwrap())
        .collect();
        let seen = [
            Seen {
                value: json!([4]),
                mod_revision: rev(3),
            },
            Seen {
                value: Value::Null,
                mod_revision: KvRevision::ZERO,
            },
        ];
        let applied = apply(&c, &mops, &seen).unwrap();
        assert_eq!(
            applied.completed,
            vec![
                json!(["r", 1, [4]]),
                json!(["append", 1, 5]),
                json!(["r", 1, [4, 5]]),
                json!(["append", 1, 6]),
                json!(["w", 2, 9]),
                json!(["r", 2, 9]),
            ]
        );
        assert_eq!(applied.writes.len(), 2);
        assert_eq!(applied.writes[&c.key(&json!(1))], json!([4, 5, 6]));
        assert_eq!(applied.observed[&c.key(&json!(1))], rev(3));
        assert_eq!(applied.observed[&c.key(&json!(2))], KvRevision::ZERO);
        c.guarded_write(&applied.observed, &applied.writes)
            .validate()
            .expect("one put per key");
    }

    #[test]
    fn a_snapshot_reads_absent_and_present_keys() {
        let Answer::Established(response) = established(Outcome::Txn {
            succeeded: true,
            results: vec![range(Some(&json!([1])), 5), range(None, 0)],
        }) else {
            unreachable!()
        };
        let seen = snapshot(&response, 2).unwrap();
        assert_eq!(seen[0].value, json!([1]));
        assert_eq!(seen[0].mod_revision, rev(5));
        assert_eq!(seen[1].value, Value::Null);
        assert_eq!(seen[1].mod_revision, KvRevision::ZERO);
        assert!(snapshot(&response, 3).is_err());
    }

    #[test]
    fn only_an_established_no_is_a_write_failure() {
        let applied = |r: &Response| txn_succeeded(r).map(|ok| ok.then(|| json!("done")));
        assert_eq!(
            write_verdict(
                established(Outcome::Txn {
                    succeeded: true,
                    results: vec![]
                }),
                applied
            ),
            Verdict::Ok(json!("done"))
        );
        assert!(matches!(
            write_verdict(
                established(Outcome::Txn {
                    succeeded: false,
                    results: vec![]
                }),
                applied
            ),
            Verdict::Fail(_)
        ));
        assert_eq!(
            write_verdict(
                established(Outcome::ErrRejected {
                    reason: RejectionReason::Unsupported
                }),
                applied
            ),
            Verdict::Fail("rejected-unsupported".into())
        );
        // A refusal of the retry key, not of the request: it may have run.
        for (reason, name) in [
            (RejectionReason::RetryTooOld, "rejected-retrytooold"),
            (
                RejectionReason::RetryUnauthorized,
                "rejected-retryunauthorized",
            ),
        ] {
            assert_eq!(
                write_verdict(established(Outcome::ErrRejected { reason }), applied),
                Verdict::Info(name.into())
            );
        }
        assert!(matches!(
            write_verdict(Answer::NotSubmitted("malformed".into()), applied),
            Verdict::Fail(_)
        ));
        assert!(matches!(
            write_verdict(Answer::Unknown("timeout".into()), applied),
            Verdict::Info(_)
        ));
    }

    #[test]
    fn a_read_that_did_not_complete_fails() {
        let v = read_verdict(Answer::Unknown("timeout".into()), |_| Ok(json!(1)));
        assert!(matches!(v, Verdict::Fail(_)));
    }

    #[test]
    fn malformed_micro_operations_are_refused() {
        assert!(Micro::parse(&json!(["x", 1, 2])).is_err());
        assert!(Micro::parse(&json!(["r", 1])).is_err());
        assert!(Micro::parse(&json!("r")).is_err());
    }
}
