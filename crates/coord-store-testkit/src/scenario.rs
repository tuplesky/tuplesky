//! Versioned logical fixtures (`StoreScenarioV1`, design Section 17.12).
//!
//! A scenario is generated from a seed by a pinned generator and replayed
//! against any engine through the common contract. The replay compares the
//! engine's final rows with an independent map oracle and returns a digest
//! that a committed fixture freezes. Physical layout may differ between
//! engines; the logical digest may not.

use std::collections::BTreeMap;

use coord_core::effect::CollectionId;
use coord_store_api::engine::{LocalEngine, OrderedRead, ScanRequest, SnapshotSource, WriteTxn};
use coord_store_api::registry::Collection;
use coord_types::identity::Digest32;
use rand_chacha::ChaCha12Rng;
use rand_core::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

/// One replay step.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Step {
    /// Put inside the current transaction.
    Put {
        /// Collection id.
        collection: u16,
        /// Key.
        key: Vec<u8>,
        /// Value.
        value: Vec<u8>,
    },
    /// Delete inside the current transaction.
    Delete {
        /// Collection id.
        collection: u16,
        /// Key.
        key: Vec<u8>,
    },
    /// Commit the current transaction durably.
    Commit,
    /// Abort the current transaction.
    Abort,
    /// Crash and reopen (only durable state survives).
    CrashReopen,
}

/// A versioned logical scenario.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreScenarioV1 {
    /// Schema name.
    pub schema: String,
    /// Generator seed.
    pub seed: [u8; 32],
    /// Generator description.
    pub generator: String,
    /// Steps.
    pub steps: Vec<Step>,
    /// Expected digest of the final durable rows (set when frozen).
    pub expected_digest: Option<Digest32>,
}

/// Pinned generator description.
pub const GENERATOR: &str = "store-scenario-gen/v1 chacha12";

impl StoreScenarioV1 {
    /// Generate a scenario with `commits` transactions of a few steps each,
    /// mixing overwrites, deletes, aborts and crashes.
    pub fn generate(seed: [u8; 32], commits: u32) -> Self {
        let mut rng = ChaCha12Rng::from_seed(seed);
        let collections = [
            Collection::KvCurrentV1.id().0,
            Collection::KvHistoryV1.id().0,
            Collection::LeaseV1.id().0,
        ];
        let mut steps = Vec::new();
        for _ in 0..commits {
            let n = 1 + (rng.next_u32() % 5) as usize;
            for _ in 0..n {
                let collection = collections[(rng.next_u32() % 3) as usize];
                let key = format!("k{:02}", rng.next_u32() % 24).into_bytes();
                if rng.next_u32().is_multiple_of(4) {
                    steps.push(Step::Delete { collection, key });
                } else {
                    let len = (rng.next_u32() % 40) as usize;
                    let value: Vec<u8> = (0..len).map(|_| (rng.next_u32() & 0xff) as u8).collect();
                    steps.push(Step::Put {
                        collection,
                        key,
                        value,
                    });
                }
            }
            match rng.next_u32() % 10 {
                0 => steps.push(Step::Abort),
                1 => {
                    steps.push(Step::Commit);
                    steps.push(Step::CrashReopen);
                }
                _ => steps.push(Step::Commit),
            }
        }
        StoreScenarioV1 {
            schema: "store_scenario_v1".to_owned(),
            seed,
            generator: GENERATOR.to_owned(),
            steps,
            expected_digest: None,
        }
    }
}

/// Rows as `(collection, key, value)`.
pub type FlatRows = Vec<(u16, Vec<u8>, Vec<u8>)>;

/// Digest of rows `(collection, key, value)` in canonical order.
pub fn digest_rows(rows: &[(u16, Vec<u8>, Vec<u8>)]) -> Digest32 {
    let mut h = blake3::Hasher::new_derive_key("tuplesky store scenario digest v1");
    for (c, k, v) in rows {
        h.update(&c.to_be_bytes());
        h.update(&(k.len() as u64).to_be_bytes());
        h.update(k);
        h.update(&(v.len() as u64).to_be_bytes());
        h.update(v);
    }
    Digest32(*h.finalize().as_bytes())
}

/// Outcome of a replay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayOutcome {
    /// Digest of the engine's final rows.
    pub digest: Digest32,
    /// Whether the engine's rows equal the independent oracle's rows.
    pub matches_oracle: bool,
    /// Whether the digest equals the scenario's frozen expectation (or
    /// `None` when the scenario has none).
    pub matches_expected: Option<bool>,
    /// Number of committed transactions.
    pub commits: u32,
}

/// Upper bound on scan pages per collection during replay; a conforming
/// engine holding at most a few thousand rows never approaches it.
const MAX_REPLAY_PAGES: u32 = 1 << 16;

fn read_all<E: LocalEngine>(engine: &E) -> Result<FlatRows, String> {
    let view = engine.reader().snapshot().map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for c in Collection::ALL {
        let mut request = ScanRequest::all(64, 1 << 20);
        let mut pages = 0u32;
        let mut last_key: Option<Vec<u8>> = None;
        loop {
            pages += 1;
            if pages > MAX_REPLAY_PAGES {
                return Err(format!(
                    "collection {} did not exhaust within {MAX_REPLAY_PAGES} pages",
                    c.name()
                ));
            }
            let page = view
                .scan_page(c.id(), &request)
                .map_err(|e| e.to_string())?;
            // Every page must advance strictly past the previous cursor; an
            // adapter that ignores `resume_after` is nonconformant, not a
            // reason to loop forever.
            for r in &page.rows {
                if last_key.as_ref().is_some_and(|k| r.key <= *k) {
                    return Err(format!(
                        "collection {} scan did not advance past the resume key",
                        c.name()
                    ));
                }
                last_key = Some(r.key.clone());
                out.push((c.id().0, r.key.clone(), r.value.clone()));
            }
            if page.exhausted {
                break;
            }
            request.resume_after = Some(
                page.rows
                    .last()
                    .ok_or("empty non-exhausted page")?
                    .key
                    .clone(),
            );
        }
    }
    Ok(out)
}

/// Replay a scenario against `engine`, using `crash` to crash and reopen.
pub fn replay<E: LocalEngine>(
    engine: &mut E,
    scenario: &StoreScenarioV1,
    mut crash: impl FnMut(&mut E),
) -> Result<ReplayOutcome, String> {
    let mut oracle: BTreeMap<(u16, Vec<u8>), Vec<u8>> = BTreeMap::new();
    let mut pending: Vec<Step> = Vec::new();
    let mut commits = 0u32;
    let mut i = 0;
    while i < scenario.steps.len() {
        match &scenario.steps[i] {
            Step::Put { .. } | Step::Delete { .. } => {
                pending.push(scenario.steps[i].clone());
                i += 1;
            }
            Step::Commit => {
                {
                    let mut tx = engine.begin_write().map_err(|e| e.to_string())?;
                    for step in &pending {
                        match step {
                            Step::Put {
                                collection,
                                key,
                                value,
                            } => tx
                                .put(CollectionId(*collection), key, value)
                                .map_err(|e| e.to_string())?,
                            Step::Delete { collection, key } => tx
                                .delete(CollectionId(*collection), key)
                                .map_err(|e| e.to_string())?,
                            _ => unreachable!(),
                        }
                    }
                    tx.commit_durable().map_err(|e| e.to_string())?;
                }
                for step in pending.drain(..) {
                    match step {
                        Step::Put {
                            collection,
                            key,
                            value,
                        } => {
                            oracle.insert((collection, key), value);
                        }
                        Step::Delete { collection, key } => {
                            oracle.remove(&(collection, key));
                        }
                        _ => unreachable!(),
                    }
                }
                commits += 1;
                i += 1;
            }
            Step::Abort => {
                pending.clear();
                i += 1;
            }
            Step::CrashReopen => {
                // A crash discards the uncommitted transaction: the pending
                // steps are neither applied to the engine afterwards nor to
                // the oracle. (The pinned generator only crashes after a
                // commit, so frozen fixtures are unaffected.)
                pending.clear();
                crash(engine);
                i += 1;
            }
        }
    }
    let rows = read_all(engine)?;
    let expected: FlatRows = oracle
        .iter()
        .map(|((c, k), v)| (*c, k.clone(), v.clone()))
        .collect();
    let digest = digest_rows(&rows);
    Ok(ReplayOutcome {
        digest,
        matches_oracle: rows == expected,
        matches_expected: scenario.expected_digest.map(|d| d == digest),
        commits,
    })
}
