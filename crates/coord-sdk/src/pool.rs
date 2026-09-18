//! Bounded warm pool (design Sections 3.3, 11.5): connections to one
//! endpoint and streams per connection are capped. A request beyond the
//! cap is refused with a typed error; the SDK never opens more to evade
//! the bound. Connections are bound once (one credential presentation)
//! and reused.

use std::collections::BTreeMap;

use crate::lifecycle::ConnectionId;

/// Pool bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolLimits {
    /// Warm connections at most.
    pub max_connections: usize,
    /// Concurrent request streams per connection at most.
    pub max_streams_per_connection: usize,
}

impl Default for PoolLimits {
    fn default() -> Self {
        PoolLimits {
            max_connections: 2,
            max_streams_per_connection: 64,
        }
    }
}

/// Why a stream could not be taken.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PoolError {
    /// Every bound connection is at its stream cap.
    Exhausted,
    /// No connection is bound yet; the application must open and bind
    /// one (the pool has room when `wants_connection` says so).
    NotConnected,
    /// The pool is at its connection cap.
    TooManyConnections,
}

/// A stream slot on a bound connection; give it back with
/// [`Pool::release`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamPermit {
    /// Connection.
    pub connection: ConnectionId,
}

#[derive(Debug)]
struct Slot {
    bound: bool,
    in_use: usize,
}

/// The pool of one endpoint.
#[derive(Debug)]
pub struct Pool {
    limits: PoolLimits,
    slots: BTreeMap<ConnectionId, Slot>,
    /// Streams refused at the cap.
    pub refused: u64,
}

impl Pool {
    /// An empty pool.
    pub const fn new(limits: PoolLimits) -> Self {
        Pool {
            limits,
            slots: BTreeMap::new(),
            refused: 0,
        }
    }

    /// Whether the application should open another connection.
    pub fn wants_connection(&self) -> bool {
        self.slots.len() < self.limits.max_connections
    }

    /// A connection was opened (not yet bound).
    pub fn opened(&mut self, connection: ConnectionId) -> Result<(), PoolError> {
        if !self.wants_connection() {
            return Err(PoolError::TooManyConnections);
        }
        self.slots.insert(
            connection,
            Slot {
                bound: false,
                in_use: 0,
            },
        );
        Ok(())
    }

    /// A connection completed its binding.
    pub fn bound(&mut self, connection: ConnectionId) {
        if let Some(s) = self.slots.get_mut(&connection) {
            s.bound = true;
        }
    }

    /// A connection closed; its streams are gone.
    pub fn closed(&mut self, connection: ConnectionId) {
        self.slots.remove(&connection);
    }

    /// Take a stream on the least loaded bound connection.
    pub fn acquire(&mut self) -> Result<StreamPermit, PoolError> {
        let cap = self.limits.max_streams_per_connection;
        let best = self
            .slots
            .iter()
            .filter(|(_, s)| s.bound && s.in_use < cap)
            .min_by_key(|(_, s)| s.in_use)
            .map(|(c, _)| *c);
        match best {
            Some(connection) => {
                self.slots.get_mut(&connection).expect("present").in_use += 1;
                Ok(StreamPermit { connection })
            }
            None if self.slots.values().any(|s| s.bound) => {
                self.refused += 1;
                Err(PoolError::Exhausted)
            }
            None => Err(PoolError::NotConnected),
        }
    }

    /// Give a stream back.
    pub fn release(&mut self, permit: StreamPermit) {
        if let Some(s) = self.slots.get_mut(&permit.connection) {
            s.in_use = s.in_use.saturating_sub(1);
        }
    }

    /// Streams in use on `connection`.
    pub fn in_use(&self, connection: ConnectionId) -> usize {
        self.slots.get(&connection).map_or(0, |s| s.in_use)
    }

    /// Bound connections.
    pub fn bound_connections(&self) -> usize {
        self.slots.values().filter(|s| s.bound).count()
    }
}
