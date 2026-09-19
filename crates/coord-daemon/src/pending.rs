//! Responses a request stream is still waiting for (design Sections 3.2,
//! 4.4).
//!
//! The collector answers most requests later than it receives them: a
//! submission is fanned out, evidence is collected, and only when the
//! release rule holds does a response exist. The stream the caller opened
//! stays open until then, so something has to hold the means of writing
//! to it and match it up with the delivery when it arrives. That is this
//! module, and it is the daemon's own: `coord-collector` deliberately
//! holds no sockets, and `coord-session` routes frames without owning the
//! streams they came from.
//!
//! Two things make it worth its own type rather than a map inline in the
//! event loop. A response must reach the connection that asked for it and
//! no other -- a retry key is not a capability, and writing a result onto
//! whichever stream happens to hold that key now would disclose one
//! caller's data to another. And a responder that is dropped without
//! being answered leaves the caller's stream open until a timeout, so
//! every path that forgets one hands it back instead of losing it.

use std::collections::{BTreeMap, BTreeSet};

use coord_types::RetryKey;

/// Why a delivery could not be matched to a waiting stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Undeliverable {
    /// Nothing is waiting under this key: the caller went away, the
    /// stream was already answered, or the delivery is for an invocation
    /// this process never held.
    Unknown,
    /// Another connection holds the stream for this key. The result is
    /// not written: the caller that asked is the caller that is told.
    OtherConnection {
        /// The connection actually waiting.
        held_by: u64,
    },
}

impl core::fmt::Display for Undeliverable {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Undeliverable::Unknown => f.write_str("no stream is waiting for this invocation"),
            Undeliverable::OtherConnection { held_by } => {
                write!(f, "connection {held_by} holds this invocation's stream")
            }
        }
    }
}

impl core::error::Error for Undeliverable {}

struct Waiting<R> {
    connection: u64,
    responder: R,
}

/// The request streams this process is holding open, by invocation.
///
/// `R` is the means of writing one response -- `coord_transport::Responder`
/// in the daemon, anything in a test.
pub struct Pending<R> {
    by_key: BTreeMap<RetryKey, Waiting<R>>,
    by_connection: BTreeMap<u64, BTreeSet<RetryKey>>,
}

impl<R> Default for Pending<R> {
    fn default() -> Self {
        Pending::new()
    }
}

impl<R> Pending<R> {
    /// Nothing waiting.
    pub const fn new() -> Self {
        Pending {
            by_key: BTreeMap::new(),
            by_connection: BTreeMap::new(),
        }
    }

    /// How many streams are held open.
    pub fn len(&self) -> usize {
        self.by_key.len()
    }

    /// Whether nothing is waiting.
    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    /// Hold `responder` for `retry_key` on `connection` until a delivery
    /// answers it.
    ///
    /// A retry of the same invocation on a new connection displaces the
    /// old stream, which is returned rather than dropped: the caller that
    /// opened it is owed a close, not a stream that hangs until its
    /// deadline. The collector's own identity rules decide whether the
    /// retry is the same command; this only decides where its answer
    /// goes, which is always the newest stream that asked.
    pub fn hold(&mut self, connection: u64, retry_key: RetryKey, responder: R) -> Option<R> {
        let displaced = self.by_key.insert(
            retry_key,
            Waiting {
                connection,
                responder,
            },
        );
        if let Some(previous) = &displaced {
            // The displaced stream's connection no longer waits for this
            // invocation, whether or not it is the same connection: the
            // insert below re-adds it when it is.
            let connection = previous.connection;
            self.forget(connection, &retry_key);
        }
        self.by_connection
            .entry(connection)
            .or_default()
            .insert(retry_key);
        displaced.map(|w| w.responder)
    }

    /// Take the stream waiting for `retry_key` on `connection`.
    ///
    /// The connection is checked, not trusted: a delivery that names a
    /// connection other than the one holding the key is refused with the
    /// holder's identity and nothing is written. Taking removes the
    /// entry, so a second delivery for the same invocation finds nothing
    /// and cannot answer the same stream twice.
    pub fn take(&mut self, connection: u64, retry_key: &RetryKey) -> Result<R, Undeliverable> {
        let Some(waiting) = self.by_key.get(retry_key) else {
            return Err(Undeliverable::Unknown);
        };
        if waiting.connection != connection {
            return Err(Undeliverable::OtherConnection {
                held_by: waiting.connection,
            });
        }
        let waiting = self.by_key.remove(retry_key).expect("just found");
        self.forget(waiting.connection, retry_key);
        Ok(waiting.responder)
    }

    /// Release every stream `connection` was holding, because it closed.
    ///
    /// The responders come back so the caller can drop them deliberately;
    /// the keys are forgotten, so a delivery that arrives afterwards is
    /// `Unknown` rather than a write to a stream that is already gone.
    /// The invocations themselves are not cancelled by this: work already
    /// admitted stays resolvable under its session, which is what makes a
    /// reconnecting client able to ask for its outcome again.
    pub fn close(&mut self, connection: u64) -> Vec<R> {
        let Some(keys) = self.by_connection.remove(&connection) else {
            return Vec::new();
        };
        keys.into_iter()
            .filter_map(|key| self.by_key.remove(&key))
            .map(|w| w.responder)
            .collect()
    }

    fn forget(&mut self, connection: u64, retry_key: &RetryKey) {
        if let Some(keys) = self.by_connection.get_mut(&connection) {
            keys.remove(retry_key);
            if keys.is_empty() {
                self.by_connection.remove(&connection);
            }
        }
    }
}

impl<R> core::fmt::Debug for Pending<R> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Pending")
            .field("waiting", &self.by_key.len())
            .field("connections", &self.by_connection.len())
            .finish()
    }
}
