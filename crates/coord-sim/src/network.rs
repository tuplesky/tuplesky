//! Link model: delay ranges, loss and duplication probabilities, and
//! directed partitions (design Section 12.2, message-level fidelity).

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::rng::NamedStreams;
use crate::world::NodeId;

/// Network parameters of a scenario.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// Minimum one-way delay in ticks.
    pub min_delay: u64,
    /// Maximum one-way delay in ticks.
    pub max_delay: u64,
    /// Loss probability per message, in parts per million.
    pub loss_ppm: u32,
    /// Duplication probability per message, in parts per million.
    pub duplicate_ppm: u32,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        NetworkConfig {
            min_delay: 1,
            max_delay: 5,
            loss_ppm: 0,
            duplicate_ppm: 0,
        }
    }
}

/// Delivery decision for one send.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delivery {
    /// Delays of each copy to deliver (empty when lost or partitioned).
    pub copies: Vec<u64>,
}

/// Link state.
#[derive(Debug)]
pub struct Network {
    config: NetworkConfig,
    /// Directed cuts `(from, to)`.
    cuts: BTreeSet<(NodeId, NodeId)>,
}

impl Network {
    /// New network.
    pub fn new(config: NetworkConfig) -> Self {
        Network {
            config,
            cuts: BTreeSet::new(),
        }
    }

    /// Cut the directed link.
    pub fn cut(&mut self, from: NodeId, to: NodeId) {
        self.cuts.insert((from, to));
    }

    /// Heal the directed link.
    pub fn heal(&mut self, from: NodeId, to: NodeId) {
        self.cuts.remove(&(from, to));
    }

    /// Decide how a message from `from` to `to` is delivered, drawing from
    /// the `net` substream.
    pub fn decide(&self, rng: &mut NamedStreams, from: NodeId, to: NodeId) -> Delivery {
        if self.cuts.contains(&(from, to)) {
            return Delivery { copies: Vec::new() };
        }
        if rng.chance("net.loss", self.config.loss_ppm) {
            return Delivery { copies: Vec::new() };
        }
        let mut copies = vec![rng.range("net.delay", self.config.min_delay, self.config.max_delay)];
        if rng.chance("net.dup", self.config.duplicate_ppm) {
            copies.push(rng.range("net.delay", self.config.min_delay, self.config.max_delay));
        }
        Delivery { copies }
    }
}
