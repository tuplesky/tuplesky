//! Maps and sets keyed by a command identity (task-d60, step 3).
//!
//! A command identity is a BLAKE3 digest, uniform already, so it needs no
//! hashing of its own: [`DigestHasher`] takes the first eight bytes of
//! what it is given. A lookup is then one probe and one comparison, where
//! an ordered map compares 32-byte keys at every level of its tree.
//!
//! Only a collection that is looked up and never iterated to produce an
//! effect may be one of these: the machines are deterministic by the
//! order of the ordered maps they iterate, and an iteration here would
//! follow the hasher's buckets.
//!
//! The bucket a key lands in is not left to the digest alone: a caller
//! who picks request contents could grind identities whose first bytes
//! agree and crowd one probe sequence, and the executed history is not
//! bounded by the command table. So the hasher is keyed. Each map takes
//! the process's key ([`seed`]) when it is built and keeps it, and a key
//! word is mixed into every write by a folded multiply (wyhash's and
//! foldhash's core). Without the key a caller cannot tell which
//! identities share a bucket. The key changes no output: nothing iterates
//! these maps. The protocol simulator and the tests run on the fixed
//! default key.

use core::hash::{BuildHasher, Hasher};
use core::sync::atomic::{AtomicU64, Ordering};

/// The key a map built from now on takes ([`seed`]); a fixed default
/// until the process sets one.
static KEY: [AtomicU64; 2] = [
    AtomicU64::new(0x243f_6a88_85a3_08d3),
    AtomicU64::new(0x1319_8a2e_0370_7344),
];

/// Set the key every digest map built after this call hashes with. A
/// daemon calls it once at start, with a key drawn from the operating
/// system, before it builds a machine. A map already built keeps the key
/// it was built with, so a later call cannot move its entries.
pub fn seed(k0: u64, k1: u64) {
    KEY[0].store(k0, Ordering::Relaxed);
    KEY[1].store(k1 | 1, Ordering::Relaxed);
}

/// A hasher for keys that are digests: the first sixteen bytes of each
/// write, mixed with the map's key (task-d60).
#[derive(Clone, Copy, Debug)]
pub struct DigestHasher {
    state: u64,
    key: u64,
}

const fn folded(x: u64, y: u64) -> u64 {
    let p = (x as u128) * (y as u128);
    (p as u64) ^ ((p >> 64) as u64)
}

impl Hasher for DigestHasher {
    fn finish(&self) -> u64 {
        self.state
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut words = [0u8; 16];
        let n = bytes.len().min(16);
        words[..n].copy_from_slice(&bytes[..n]);
        let (a, b) = words.split_at(8);
        let a = u64::from_le_bytes(a.try_into().expect("eight bytes"));
        let b = u64::from_le_bytes(b.try_into().expect("eight bytes"));
        self.state = folded(self.state ^ a, self.key ^ b);
    }
}

/// A map's key, taken from [`seed`] when the map is built.
#[derive(Clone, Copy, Debug)]
pub struct DigestState {
    k0: u64,
    k1: u64,
}

impl DigestState {
    /// The process's current key.
    pub fn new() -> Self {
        DigestState {
            k0: KEY[0].load(Ordering::Relaxed),
            k1: KEY[1].load(Ordering::Relaxed),
        }
    }
}

impl Default for DigestState {
    fn default() -> Self {
        DigestState::new()
    }
}

impl BuildHasher for DigestState {
    type Hasher = DigestHasher;

    fn build_hasher(&self) -> DigestHasher {
        DigestHasher {
            state: self.k0,
            key: self.k1,
        }
    }
}

/// A map keyed by a command identity, or by one and a small value.
pub type DigestMap<K, V> = hashbrown::HashMap<K, V, DigestState>;

/// A set of command identities.
pub type DigestSet<K> = hashbrown::HashSet<K, DigestState>;

/// An empty map, with the process's key.
pub fn map<K, V>() -> DigestMap<K, V> {
    DigestMap::with_hasher(DigestState::new())
}

/// An empty set, with the process's key.
pub fn set<K>() -> DigestSet<K> {
    DigestSet::with_hasher(DigestState::new())
}

#[cfg(test)]
mod tests {
    use core::hash::BuildHasher;

    use coord_types::CommandId;
    use coord_types::identity::Digest32;

    use super::*;

    fn id(head: u8, tail: u8) -> CommandId {
        let mut bytes = [head; 32];
        bytes[31] = tail;
        CommandId(Digest32(bytes))
    }

    #[test]
    fn a_digest_hashes_by_its_keyed_first_bytes() {
        let state = DigestState::new();
        assert_ne!(state.hash_one(id(1, 0)), state.hash_one(id(2, 0)));
        // Past the sixteenth byte nothing is read: two identities that
        // differ only there land together, and the map still tells them
        // apart.
        assert_eq!(state.hash_one(id(1, 0)), state.hash_one(id(1, 1)));
        let mut m = map();
        for tail in 0..=255u8 {
            m.insert(id(1, tail), tail);
        }
        assert_eq!(m.len(), 256);
        assert!((0..=255u8).all(|tail| m.get(&id(1, tail)) == Some(&tail)));
        // Another key places the same identity elsewhere.
        let other = DigestState { k0: 7, k1: 9 };
        assert_ne!(state.hash_one(id(1, 0)), other.hash_one(id(1, 0)));
    }

    #[test]
    fn a_map_keeps_the_key_it_was_built_with() {
        let mut m = DigestMap::with_hasher(DigestState { k0: 3, k1: 5 });
        for head in 0..=255u8 {
            m.insert(id(head, 0), head);
        }
        let copy = m.clone();
        assert!((0..=255u8).all(|head| copy.get(&id(head, 0)) == Some(&head)));
    }
}
