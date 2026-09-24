//! The offered work: what each scheduled arrival actually asks the
//! domain to do (design Sections 14.1, 22.3).
//!
//! It is generated from a seed, so the same run can be offered again to
//! a differently configured domain and the comparison is between the
//! domains rather than between two workloads. The mix names the shapes a
//! Kubernetes-like control plane produces: plain writes, point reads,
//! contended transactions on hot keys, multi-key transactions and range
//! reads.
//!
//! Hot writers are a parameter rather than a separate workload: a small
//! `hot_keys` concentrates every contended transaction onto a handful of
//! keys, which is the contention a leader election or a node-lease
//! renewal produces.
//!
//! There is deliberately no compare-and-swap kind. A real one compares
//! against a revision the caller read and does nothing when it lost the
//! race, which needs the caller to carry what it read from one answer
//! into the next request; the arrivals here are generated ahead of any
//! answer. What the contended kind measures instead is the conditional
//! transaction path under contention, and it is named for that so no
//! report calls it optimistic concurrency.

use coord_types::ids::NamespaceId;
use coord_types::logical_v1::{
    CanonicalOperation, Compare, CompareOperand, CompareResult, CompareTarget, KeyRange,
    LogicalRequest, PutOp, RangeOp, TxnOp,
};
use rand_core::Rng;
use serde::{Deserialize, Serialize};

/// The kinds a run reports separately. Mixing their latencies would hide
/// the thing a WAN matrix is for: a read and a replicated write do not
/// cost the same and must never be averaged together.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Kind {
    /// Unconditional write.
    Put,
    /// Point read.
    Get,
    /// A transaction on a hot key whose compare holds after the key's
    /// first write and whose both branches write: the conditional path
    /// under contention, not an optimistic compare-and-swap.
    ContendedTransaction,
    /// Multi-key transaction.
    Transaction,
    /// Bounded range read.
    Scan,
}

impl Kind {
    /// The name this kind is reported under.
    pub const fn name(self) -> &'static str {
        match self {
            Kind::Put => "put",
            Kind::Get => "get",
            Kind::ContendedTransaction => "contended-transaction",
            Kind::Transaction => "transaction",
            Kind::Scan => "scan",
        }
    }
}

/// Relative weights of the kinds. A weight of zero removes a kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mix {
    /// Unconditional writes.
    pub put: u32,
    /// Point reads.
    pub get: u32,
    /// Contended transactions on the hot keys.
    pub contended: u32,
    /// Transactions.
    pub transaction: u32,
    /// Range reads.
    pub scan: u32,
}

impl Mix {
    /// A control-plane-shaped mix: mostly reads, a steady stream of
    /// contended writes, occasional transactions and lists.
    pub const CONTROL_PLANE: Mix = Mix {
        put: 15,
        get: 55,
        contended: 20,
        transaction: 5,
        scan: 5,
    };

    /// Writes only, for the write-path matrix.
    pub const WRITE_ONLY: Mix = Mix {
        put: 50,
        get: 0,
        contended: 50,
        transaction: 0,
        scan: 0,
    };

    /// Parse `put=15,get=55,contended=20,txn=5,scan=5`.
    ///
    /// `cas` is still read, as an older spelling of `contended`, so an
    /// existing command line runs; it has only ever meant the contended
    /// transaction, and the report names it that.
    pub fn parse(text: &str) -> Result<Mix, String> {
        let mut mix = Mix {
            put: 0,
            get: 0,
            contended: 0,
            transaction: 0,
            scan: 0,
        };
        for part in text.split(',') {
            let (name, value) = part
                .split_once('=')
                .ok_or_else(|| format!("`{part}` is not name=weight"))?;
            let weight: u32 = value
                .parse()
                .map_err(|_| format!("`{value}` is not a weight"))?;
            match name.trim() {
                "put" => mix.put = weight,
                "get" => mix.get = weight,
                "contended" | "contended-transaction" | "cas" => mix.contended = weight,
                "txn" | "transaction" => mix.transaction = weight,
                "scan" => mix.scan = weight,
                other => return Err(format!("unknown operation `{other}`")),
            }
        }
        if mix.total() == 0 {
            return Err("a mix with no weight offers no work".into());
        }
        Ok(mix)
    }

    /// Sum of the weights.
    pub const fn total(&self) -> u32 {
        self.put + self.get + self.contended + self.transaction + self.scan
    }

    fn pick(&self, draw: u32) -> Kind {
        let mut at = draw % self.total();
        for (kind, weight) in [
            (Kind::Put, self.put),
            (Kind::Get, self.get),
            (Kind::ContendedTransaction, self.contended),
            (Kind::Transaction, self.transaction),
            (Kind::Scan, self.scan),
        ] {
            if at < weight {
                return kind;
            }
            at -= weight;
        }
        Kind::Get
    }
}

/// The prefix every object key shares; a scan runs to its end.
const OBJECTS: &[u8] = b"/registry/bench/objects/";

/// How the offered work is shaped.
#[derive(Clone, Copy, Debug)]
pub struct Workload {
    /// The namespace every request names.
    pub namespace: NamespaceId,
    /// How many distinct keys the reads and unconditional writes touch.
    pub keyspace: u32,
    /// How many keys the contended transactions share. A small number
    /// is the hot-writer case.
    pub hot_keys: u32,
    /// Value size in bytes.
    pub value_bytes: usize,
    /// Keys per transaction.
    pub transaction_keys: u32,
    /// Rows a scan asks for.
    pub scan_limit: u32,
    /// The mix.
    pub mix: Mix,
}

impl Workload {
    /// The next operation, and the request that carries it.
    pub fn next(&self, rng: &mut impl Rng) -> (Kind, LogicalRequest) {
        let kind = self.mix.pick(rng.next_u32());
        let request = match kind {
            Kind::Put => self.put(rng),
            Kind::Get => self.get(rng),
            Kind::ContendedTransaction => self.contended_transaction(rng),
            Kind::Transaction => self.transaction(rng),
            Kind::Scan => self.scan(rng),
        };
        (kind, request)
    }

    fn finish(&self, operation: CanonicalOperation) -> LogicalRequest {
        let mut request = LogicalRequest::new(self.namespace, operation);
        request.canonicalize();
        request
    }

    fn key(&self, index: u32) -> Vec<u8> {
        let mut key = OBJECTS.to_vec();
        key.extend_from_slice(format!("{index:08}").as_bytes());
        key
    }

    fn hot(&self, index: u32) -> Vec<u8> {
        format!("/registry/bench/hot/{index:04}").into_bytes()
    }

    fn value(&self, rng: &mut impl Rng) -> Vec<u8> {
        // Incompressible enough not to be free, generated from the same
        // stream so the same seed offers the same bytes.
        let mut value = vec![0u8; self.value_bytes];
        rng.fill_bytes(&mut value);
        value
    }

    fn put(&self, rng: &mut impl Rng) -> LogicalRequest {
        let key = self.key(rng.next_u32() % self.keyspace.max(1));
        self.finish(CanonicalOperation::Put(PutOp {
            key,
            value: self.value(rng),
            lease: None,
            prev_kv: false,
        }))
    }

    fn get(&self, rng: &mut impl Rng) -> LogicalRequest {
        let key = self.key(rng.next_u32() % self.keyspace.max(1));
        self.finish(CanonicalOperation::Range(RangeOp {
            range: KeyRange::exact(key),
            revision: None,
            limit: 1,
            keys_only: false,
            count_only: false,
        }))
    }

    /// A conditional transaction on a contended key.
    ///
    /// The comparison is against "some modification revision", not one
    /// this caller read, so it holds for any key written before, and
    /// both branches write. What is measured is the cost of the
    /// conditional path and of contention on one key; it never detects a
    /// stale read and is not a compare-and-swap, which is why it is not
    /// called one. A read-then-write would also measure two operations
    /// under one sample.
    fn contended_transaction(&self, rng: &mut impl Rng) -> LogicalRequest {
        let key = self.hot(rng.next_u32() % self.hot_keys.max(1));
        self.finish(CanonicalOperation::Txn(TxnOp {
            compares: vec![Compare {
                key: key.clone(),
                target: CompareTarget::ModRevision,
                result: CompareResult::Greater,
                operand: CompareOperand::Counter(0),
            }],
            success: vec![coord_types::logical_v1::BranchOp::Put(PutOp {
                key: key.clone(),
                value: self.value(rng),
                lease: None,
                prev_kv: false,
            })],
            failure: vec![coord_types::logical_v1::BranchOp::Put(PutOp {
                key,
                value: self.value(rng),
                lease: None,
                prev_kv: false,
            })],
        }))
    }

    fn transaction(&self, rng: &mut impl Rng) -> LogicalRequest {
        let mut success = Vec::new();
        for _ in 0..self.transaction_keys.max(1) {
            success.push(coord_types::logical_v1::BranchOp::Put(PutOp {
                key: self.key(rng.next_u32() % self.keyspace.max(1)),
                value: self.value(rng),
                lease: None,
                prev_kv: false,
            }));
        }
        self.finish(CanonicalOperation::Txn(TxnOp {
            compares: Vec::new(),
            success,
            failure: Vec::new(),
        }))
    }

    /// A bounded range read from a random object key to the end of the
    /// object prefix. The end is the prefix's successor rather than the
    /// start key's: a range that ended just past the start key would
    /// hold that key and nothing else, and `scan_limit` would never be
    /// what bounded the read.
    fn scan(&self, rng: &mut impl Rng) -> LogicalRequest {
        let from = rng.next_u32() % self.keyspace.max(1);
        self.finish(CanonicalOperation::Range(RangeOp {
            range: KeyRange::interval(self.key(from), prefix_end(OBJECTS)),
            revision: None,
            limit: self.scan_limit.max(1),
            keys_only: false,
            count_only: false,
        }))
    }
}

/// The first key that sorts after every key with `prefix`: the last byte
/// that can be incremented is, and everything after it is dropped. A
/// prefix of nothing but `0xff` has no such key; none of the prefixes
/// here is one, and the empty answer is a range no request validates.
fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last != 0xff {
            end.push(last + 1);
            break;
        }
    }
    end
}

#[cfg(test)]
mod tests {
    use super::prefix_end;

    #[test]
    fn a_prefix_end_carries_past_a_full_byte() {
        assert_eq!(prefix_end(b"/objects/"), b"/objects0");
        assert_eq!(prefix_end(b"ab\xff\xff"), b"ac");
        assert_eq!(prefix_end(b"\xff"), b"");
    }
}
