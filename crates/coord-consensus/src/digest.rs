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
//! The bucket a key lands in is a function of its first bytes, which a
//! caller who picks retry keys could grind to crowd one bucket. Every map
//! here is bounded by the command table but the executed history, so the
//! worst a ground set does is lengthen the probes of the commands it
//! crowds; it cannot make a lookup wrong.

use core::hash::{BuildHasherDefault, Hasher};

/// A hasher for keys that are digests: the first eight bytes of each
/// write, folded into the state (task-d60).
#[derive(Clone, Copy, Debug, Default)]
pub struct DigestHasher(u64);

impl Hasher for DigestHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut word = [0u8; 8];
        let n = bytes.len().min(8);
        word[..n].copy_from_slice(&bytes[..n]);
        self.0 = self.0.rotate_left(29) ^ u64::from_le_bytes(word);
    }
}

/// The hasher's builder.
pub type DigestState = BuildHasherDefault<DigestHasher>;

/// A map keyed by a command identity, or by one and a small value.
pub type DigestMap<K, V> = hashbrown::HashMap<K, V, DigestState>;

/// A set of command identities.
pub type DigestSet<K> = hashbrown::HashSet<K, DigestState>;

/// An empty map.
pub const fn map<K, V>() -> DigestMap<K, V> {
    DigestMap::with_hasher(DigestState::new())
}

/// An empty set.
pub const fn set<K>() -> DigestSet<K> {
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
    fn a_digest_hashes_by_its_first_bytes() {
        let state = DigestState::new();
        assert_ne!(state.hash_one(id(1, 0)), state.hash_one(id(2, 0)));
        // Past the eighth byte nothing is read: two identities that differ
        // only there land together, and the map still tells them apart.
        assert_eq!(state.hash_one(id(1, 0)), state.hash_one(id(1, 1)));
        let mut m = map();
        for tail in 0..=255u8 {
            m.insert(id(1, tail), tail);
        }
        assert_eq!(m.len(), 256);
        assert!((0..=255u8).all(|tail| m.get(&id(1, tail)) == Some(&tail)));
    }
}
