//! Observations of one domain.

use coord_types::logical_v1::CanonicalOperation;
use serde::{Deserialize, Serialize};

use crate::model::ModelResponse;

/// Identity of one invocation within a history.
pub type OpId = u32;

/// Watch event kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum WatchEventKind {
    /// Put.
    Put,
    /// Delete.
    Delete,
}

/// One observed or modeled watch event.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct WatchEvent {
    /// Kind.
    pub kind: WatchEventKind,
    /// Key.
    pub key: Vec<u8>,
    /// Value (empty for deletes).
    pub value: Vec<u8>,
}

/// One observation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Observation {
    /// A client invoked an operation.
    Invoke {
        /// Identity.
        id: OpId,
        /// Client (diagnostic).
        client: u32,
        /// Real time of invocation.
        tick: u64,
        /// Stable retry identity, when the client used one.
        retry: Option<u64>,
        /// Operation.
        op: CanonicalOperation,
    },
    /// The client received a response.
    Respond {
        /// Identity of the invocation.
        id: OpId,
        /// Real time of response.
        tick: u64,
        /// Response as observed.
        response: ModelResponse,
    },
    /// A watch delivered a complete revision batch.
    WatchBatch {
        /// Real time of delivery.
        tick: u64,
        /// Revision.
        revision: u64,
        /// Events, in delivery order.
        events: Vec<WatchEvent>,
    },
}

/// A history of observations.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct History {
    observations: Vec<Observation>,
}

impl History {
    /// Empty history.
    pub fn new() -> Self {
        History::default()
    }

    /// Append an observation.
    pub fn push(&mut self, observation: Observation) {
        self.observations.push(observation);
    }

    /// Record an invocation.
    pub fn invoke(
        &mut self,
        id: OpId,
        client: u32,
        tick: u64,
        retry: Option<u64>,
        op: CanonicalOperation,
    ) -> &mut Self {
        self.push(Observation::Invoke {
            id,
            client,
            tick,
            retry,
            op,
        });
        self
    }

    /// Record a response.
    pub fn respond(&mut self, id: OpId, tick: u64, response: ModelResponse) -> &mut Self {
        self.push(Observation::Respond { id, tick, response });
        self
    }

    /// Record a watch batch.
    pub fn watch(&mut self, tick: u64, revision: u64, events: Vec<WatchEvent>) -> &mut Self {
        self.push(Observation::WatchBatch {
            tick,
            revision,
            events,
        });
        self
    }

    /// All observations.
    pub fn observations(&self) -> &[Observation] {
        &self.observations
    }
}
