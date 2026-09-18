//! Independent checker for the echo actors: every acknowledgement the
//! client received must be present in the acknowledging node's durable
//! image whenever that node is inspected after a crash, and at the end.
//! The checker reads the storage model directly; it never trusts the actor.

use std::collections::BTreeSet;

use crate::actors::ECHO_COLLECTION;
use crate::world::{NodeId, World};

/// A detected violation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    /// Node whose durable image lacks the value.
    pub node: NodeId,
    /// Acknowledged value that is not durable.
    pub value: Vec<u8>,
    /// Virtual tick of detection.
    pub tick: u64,
}

/// Durable-acknowledgement invariant.
#[derive(Debug, Default)]
pub struct DurableAckOracle {
    violations: Vec<Violation>,
}

impl DurableAckOracle {
    /// Check `node` now (after a crash or at the end of the run).
    pub fn check(&mut self, world: &World, node: NodeId) {
        let durable: BTreeSet<Vec<u8>> = world
            .storage(node)
            .durable_rows()
            .into_iter()
            .filter(|(c, _, _)| *c == ECHO_COLLECTION.0)
            .map(|(_, _, v)| v)
            .collect();
        for (from, value) in world.client_acks() {
            if *from == node && !durable.contains(value) {
                let v = Violation {
                    node,
                    value: value.clone(),
                    tick: world.now(),
                };
                if !self.violations.contains(&v) {
                    self.violations.push(v);
                }
            }
        }
    }

    /// Violations found so far.
    pub fn violations(&self) -> &[Violation] {
        &self.violations
    }
}
