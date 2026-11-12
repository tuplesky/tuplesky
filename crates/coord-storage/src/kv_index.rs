//! The key index held in memory (task-d105, design Section 17.4 as
//! amended by its design step: one version row a revision).
//!
//! A put or a delete writes one event row a mutated key, keyed by revision
//! and ordinal (`EventsV1`). Those rows sit in revision order, so a read at
//! a revision needs, for each key it reads, that key's versions in key
//! order: this index, from each (namespace, key) to its retained
//! (revision, ordinal)s, oldest first, as etcd's `treeIndex` does.
//!
//! - **Built** at open from the retained event rows ([`KvIndex::build`]),
//!   and from a local image's at install.
//! - **Kept** by the materializer: each commit's event rows go through
//!   [`KvDelta::observe`] as they are lowered, and the delta is published
//!   with the commit, before the gate moves, as the intake index's is
//!   (task-d113). So a snapshot the gate hands out is never ahead of it.
//! - **Through.** The index says up to which KV revision it holds every
//!   event row ([`KvIndex::through`]). A view can be ahead of it: a
//!   recovery cut's overlay, or an execution group's application rows laid
//!   over the projection for planning. A reader takes the index's versions
//!   up to that revision and reads the view's own event rows above it
//!   ([`versions_at`]).
//!
//! A version removed from the index (an event row compaction deleted)
//! is superseded below the floor, so no read at or above the floor wants it.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::ops::Bound;

use coord_core::effect::StoreUpdate;
use coord_state::plan::KvEventKind;
use coord_store_api::engine::{Direction, EngineError, ErrorClass, OrderedRead, ScanRequest};
use coord_store_api::registry::{Collection, meta_fields};
use coord_types::ids::{KvRevision, NamespaceId};

use crate::codecs;

/// One retained version of a key: the event row that holds it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Version {
    /// The revision that wrote it.
    pub revision: KvRevision,
    /// The event's ordinal in its revision.
    pub ordinal: u32,
}

impl Version {
    /// The event row's key.
    pub fn event_key(&self) -> Vec<u8> {
        codecs::event_key(self.revision, self.ordinal)
    }
}

/// Bytes an entry is charged on top of its key: a version's revision and
/// ordinal and the map's own share, as the design step sizes it (about
/// 16 bytes a retained version), not measured.
pub const VERSION_BYTES: usize = 16;

/// The index by key of the retained event rows.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KvIndex {
    keys: BTreeMap<(NamespaceId, Vec<u8>), Vec<Version>>,
    /// Every event row at or below this revision is in the index.
    through: Option<KvRevision>,
    /// The retention floor of the newest commit published. Compaction
    /// removes a version only once a newer one at or below the floor it
    /// ran under exists, so the index answers every read at or above this
    /// floor as any view would; below it, a view pinned before compaction
    /// may hold versions the index has dropped.
    floor: Option<KvRevision>,
    versions: usize,
    key_bytes: usize,
}

impl KvIndex {
    /// Build the index from `view`'s event rows, through its KV revision.
    ///
    /// A store of format 3 or before, whose versions are history rows this index
    /// does not see, is refused before this by the recorded format
    /// ([`crate::intake::IntakeIndex::build`]); `kv_history_v1` is not
    /// read.
    pub fn build<V: OrderedRead>(view: &V) -> Result<Self, EngineError> {
        let mut index = KvIndex::default();
        let mut delta = KvDelta::default();
        scan_events(view, Bound::Unbounded, Bound::Unbounded, |key, value| {
            delta.add_row(key, value)
        })?;
        index.publish(delta);
        index.through = through_of(view)?;
        index.floor = Some(codecs::read_retention_floor(view)?);
        Ok(index)
    }

    /// Build the index of `view`'s event rows at or below `floor` only:
    /// what [`KvIndex::keeps_below_floor`] reads, without decoding a row
    /// above the floor (an export checks those against its boundary).
    pub fn build_at_or_below<V: OrderedRead>(
        view: &V,
        floor: KvRevision,
    ) -> Result<Self, EngineError> {
        let mut index = KvIndex::default();
        let mut delta = KvDelta::default();
        scan_events(
            view,
            Bound::Unbounded,
            Bound::Included(codecs::event_key(floor, u32::MAX)),
            |key, value| delta.add_row(key, value),
        )?;
        index.publish(delta);
        index.through = through_of(view)?.map(|t| t.min(floor));
        index.floor = Some(codecs::read_retention_floor(view)?);
        Ok(index)
    }

    /// Apply a commit's changes.
    pub fn publish(&mut self, delta: KvDelta) {
        for (namespace, key, version) in delta.removed {
            let slot = (namespace, key);
            if let Some(versions) = self.keys.get_mut(&slot)
                && let Ok(at) = versions.binary_search(&version)
            {
                versions.remove(at);
                self.versions -= 1;
                if versions.is_empty() {
                    self.key_bytes -= slot.1.len();
                    self.keys.remove(&slot);
                }
            }
        }
        for (namespace, key, version) in delta.added {
            let len = key.len();
            let versions = self.keys.entry((namespace, key)).or_insert_with(|| {
                self.key_bytes += len;
                Vec::new()
            });
            if let Err(at) = versions.binary_search(&version) {
                versions.insert(at, version);
                self.versions += 1;
            }
        }
        if let Some(through) = delta.through {
            self.through = Some(self.through.map_or(through, |t| t.max(through)));
        }
        if let Some(floor) = delta.floor {
            self.floor = Some(self.floor.map_or(floor, |f| f.max(floor)));
        }
    }

    /// Drop everything (a quarantined store; the next open rebuilds it).
    pub fn clear(&mut self) {
        *self = KvIndex::default();
    }

    /// The revision through which every event row is in the index.
    pub fn through(&self) -> Option<KvRevision> {
        self.through
    }

    /// The retention floor of the newest commit published.
    pub fn floor(&self) -> KvRevision {
        self.floor.unwrap_or(KvRevision::ZERO)
    }

    /// Retained versions held.
    pub fn versions(&self) -> usize {
        self.versions
    }

    /// Distinct keys held.
    pub fn keys(&self) -> usize {
        self.keys.len()
    }

    /// The keys' bytes held.
    pub fn key_bytes(&self) -> usize {
        self.key_bytes
    }

    /// Bytes the index is charged: its keys and [`VERSION_BYTES`] a
    /// version.
    pub fn bytes(&self) -> usize {
        self.key_bytes + self.versions * VERSION_BYTES
    }

    /// Each key of `namespace` in `[lower, upper)` with its newest version
    /// at or below `at`, in key order. A key whose versions are all above
    /// `at` is left out.
    pub fn newest_at<'a>(
        &'a self,
        namespace: NamespaceId,
        lower: &[u8],
        upper: Bound<&[u8]>,
        at: KvRevision,
    ) -> impl Iterator<Item = (&'a [u8], Version)> + 'a {
        let lower = Bound::Included((namespace, lower.to_vec()));
        let upper = match upper {
            Bound::Included(k) => Bound::Included((namespace, k.to_vec())),
            Bound::Excluded(k) => Bound::Excluded((namespace, k.to_vec())),
            // The rest of the namespace.
            Bound::Unbounded => match next_namespace(namespace) {
                Some(next) => Bound::Excluded((next, Vec::new())),
                None => Bound::Unbounded,
            },
        };
        self.keys
            .range((lower, upper))
            .filter_map(move |((_, key), versions)| {
                let n = versions.partition_point(|v| v.revision <= at);
                (n > 0).then(|| (key.as_slice(), versions[n - 1]))
            })
    }

    /// Whether the event row (`key`, `value`), below `floor`, stays when
    /// compaction collects below it: it is its key's newest version at or
    /// below the floor, and a put. That is the version a read at or above
    /// the floor finds; any other row below the floor answers no read.
    pub fn keeps_below_floor(
        &self,
        key: &[u8],
        value: &[u8],
        floor: KvRevision,
    ) -> Result<bool, EngineError> {
        let (revision, ordinal) = codecs::decode_event_key(key)?;
        let record = codecs::decode_event(value)?;
        if record.kind != KvEventKind::Put {
            return Ok(false);
        }
        let this = Version { revision, ordinal };
        Ok(self
            .newest_at(
                record.namespace,
                &record.key,
                Bound::Included(record.key.as_slice()),
                floor,
            )
            .next()
            .is_some_and(|(_, newest)| newest == this))
    }

    /// The versions of one key, oldest first (tests and diagnostics).
    pub fn versions_of(&self, namespace: NamespaceId, key: &[u8]) -> &[Version] {
        self.keys
            .get(&(namespace, key.to_vec()))
            .map_or(&[], Vec::as_slice)
    }
}

/// The namespace after `namespace` in key order, if any.
fn next_namespace(namespace: NamespaceId) -> Option<NamespaceId> {
    let mut bytes = *namespace.as_bytes();
    for byte in bytes.iter_mut().rev() {
        if *byte == u8::MAX {
            *byte = 0;
        } else {
            *byte += 1;
            return Some(NamespaceId(bytes));
        }
    }
    None
}

/// A commit's changes to the index, collected as its rows are lowered.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KvDelta {
    added: Vec<(NamespaceId, Vec<u8>, Version)>,
    removed: Vec<(NamespaceId, Vec<u8>, Version)>,
    through: Option<KvRevision>,
    floor: Option<KvRevision>,
}

impl KvDelta {
    /// Whether the commit changed nothing the index holds.
    pub fn is_empty(&self) -> bool {
        self.added.is_empty()
            && self.removed.is_empty()
            && self.through.is_none()
            && self.floor.is_none()
    }

    /// Take `update` into the delta, before it is written. A deleted event
    /// row names its key only in its value, which `old` reads from the rows
    /// as they stand before the delete.
    pub fn observe(
        &mut self,
        update: &StoreUpdate,
        old: impl FnOnce(&[u8]) -> Result<Option<Vec<u8>>, EngineError>,
    ) -> Result<(), EngineError> {
        if update.collection == Collection::EventsV1.id() {
            match &update.value {
                Some(value) => self.add_row(&update.key, value)?,
                None => {
                    if let Some(value) = old(&update.key)? {
                        let (namespace, key, version) = decode_row(&update.key, &value)?;
                        self.added
                            .retain(|(n, k, v)| !(*n == namespace && *k == key && *v == version));
                        self.removed.push((namespace, key, version));
                    }
                }
            }
        } else if update.collection == Collection::MetaV1.id()
            && update.key == meta_fields::KV_REVISION
            && let Some(value) = &update.value
        {
            let revision = KvRevision::new(codecs::decode_counter(value)?)
                .map_err(|_| EngineError::new(ErrorClass::Corrupt, "kv revision"))?;
            self.through = Some(self.through.map_or(revision, |t| t.max(revision)));
        } else if update.collection == Collection::MetaV1.id()
            && update.key == meta_fields::RETENTION_FLOOR
            && let Some(value) = &update.value
        {
            let floor = KvRevision::new(codecs::decode_counter(value)?)
                .map_err(|_| EngineError::new(ErrorClass::Corrupt, "retention floor"))?;
            self.floor = Some(self.floor.map_or(floor, |f| f.max(floor)));
        }
        Ok(())
    }

    fn add_row(&mut self, key: &[u8], value: &[u8]) -> Result<(), EngineError> {
        let (namespace, key, version) = decode_row(key, value)?;
        self.added.push((namespace, key, version));
        Ok(())
    }

    /// Lay `later` over this delta, as the two commits apply in order.
    pub fn absorb(&mut self, later: KvDelta) {
        for removed in later.removed {
            self.added.retain(|a| *a != removed);
            self.removed.push(removed);
        }
        self.added.extend(later.added);
        if let Some(t) = later.through {
            self.through = Some(self.through.map_or(t, |s| s.max(t)));
        }
        if let Some(f) = later.floor {
            self.floor = Some(self.floor.map_or(f, |s| s.max(f)));
        }
    }
}

fn decode_row(key: &[u8], value: &[u8]) -> Result<(NamespaceId, Vec<u8>, Version), EngineError> {
    let (revision, ordinal) = codecs::decode_event_key(key)?;
    let record = codecs::decode_event(value)?;
    Ok((record.namespace, record.key, Version { revision, ordinal }))
}

fn through_of<V: OrderedRead>(view: &V) -> Result<Option<KvRevision>, EngineError> {
    match view.get(Collection::MetaV1.id(), meta_fields::KV_REVISION)? {
        Some(_) => Ok(Some(codecs::read_kv_revision(view)?)),
        None => Ok(None),
    }
}

/// Hand each event row of `view` from `lower` on to `each`, page by page,
/// keeping no page past it.
fn scan_events<V: OrderedRead>(
    view: &V,
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
    mut each: impl FnMut(&[u8], &[u8]) -> Result<(), EngineError>,
) -> Result<(), EngineError> {
    const PAGE_BYTES: u32 = 16 * 1024 * 1024;
    let mut request = ScanRequest {
        lower,
        upper,
        direction: Direction::Forward,
        resume_after: None,
        max_rows: NonZeroU32::new(1024).expect("nonzero"),
        max_bytes: NonZeroU32::new(PAGE_BYTES).expect("nonzero"),
    };
    loop {
        let page = view.scan_page(Collection::EventsV1.id(), &request)?;
        let last = page.rows.last().map(|r| r.key.clone());
        for row in &page.rows {
            each(&row.key, &row.value)?;
        }
        if page.exhausted {
            return Ok(());
        }
        match last {
            Some(key) => request.resume_after = Some(key),
            None => {
                return Err(EngineError::new(
                    ErrorClass::Corrupt,
                    "event scan made no progress",
                ));
            }
        }
    }
}

/// What a key holds at a revision: its entry, or nothing (never written,
/// or deleted).
pub type Chosen = Option<coord_state::KvEntry>;

/// Hand each key of `namespace` in `[lower, upper)` with what it holds at
/// `at` to `each`, in key order, read through `index` and `view`'s event
/// rows: the index's versions up to its [`KvIndex::through`], the view's
/// own event rows above it (an overlay, or commits the index has not been
/// handed yet). Each chosen version's event row is read as it is handed
/// on, so `each` returning `false` (or an error) stops the read there: a
/// caller's budget bounds the rows read, not only the rows kept.
///
/// The index serves a read at `at` only at or above its
/// [`KvIndex::floor`]. Below it, compaction may have removed a version
/// that `view`, pinned before it, still holds; such a read takes every
/// event row of `view` at or below `at` instead.
///
/// A key whose newest version at or below `at` is a delete is reported
/// with `None`; the caller decides whether to keep it.
pub fn versions_at<V: OrderedRead, E: From<EngineError>>(
    index: &KvIndex,
    view: &V,
    namespace: NamespaceId,
    lower: &[u8],
    upper: Bound<&[u8]>,
    at: KvRevision,
    mut each: impl FnMut(Vec<u8>, Chosen) -> Result<bool, E>,
) -> Result<(), E> {
    let usable = index.through().is_some() && at >= index.floor();
    // The view's event rows the index doesn't cover, up to `at`: each
    // key's newest version among them. They are above anything the index
    // holds for this read, so they take its place.
    let from = match index.through() {
        _ if !usable => Some(Bound::Unbounded),
        Some(t) if t >= at => None,
        Some(t) => Some(Bound::Excluded(codecs::event_key(t, u32::MAX))),
        None => Some(Bound::Unbounded),
    };
    let in_range = |key: &[u8]| {
        key >= lower
            && match upper {
                Bound::Included(u) => key <= u,
                Bound::Excluded(u) => key < u,
                Bound::Unbounded => true,
            }
    };
    let mut overlay: BTreeMap<Vec<u8>, Version> = BTreeMap::new();
    if let Some(from) = from {
        let to = Bound::Included(codecs::event_key(at, u32::MAX));
        scan_events(view, from, to, |row_key, value| {
            let (revision, ordinal) = codecs::decode_event_key(row_key)?;
            let record = codecs::decode_event(value)?;
            if record.namespace == namespace && in_range(&record.key) {
                let version = Version { revision, ordinal };
                let slot = overlay.entry(record.key).or_insert(version);
                *slot = (*slot).max(version);
            }
            Ok(())
        })?;
    }
    let covered = index.through().map_or(at, |t| t.min(at));
    let mut indexed = usable
        .then(|| index.newest_at(namespace, lower, upper, covered))
        .into_iter()
        .flatten()
        .peekable();
    let mut overlay = overlay.into_iter().peekable();
    loop {
        let (key, version) = match (indexed.peek(), overlay.peek()) {
            (None, None) => return Ok(()),
            (Some((k, _)), Some((o, _))) if *k == o.as_slice() => {
                indexed.next();
                overlay.next().expect("peeked")
            }
            (Some((k, _)), Some((o, _))) if *k > o.as_slice() => overlay.next().expect("peeked"),
            (Some(_), _) => {
                let (k, v) = indexed.next().expect("peeked");
                (k.to_vec(), v)
            }
            (None, Some(_)) => overlay.next().expect("peeked"),
        };
        let chosen = read_version(view, namespace, &key, version)?;
        if !each(key, chosen)? {
            return Ok(());
        }
    }
}

/// What the event row of `version` says `key` holds.
fn read_version<V: OrderedRead>(
    view: &V,
    namespace: NamespaceId,
    key: &[u8],
    version: Version,
) -> Result<Chosen, EngineError> {
    let row = view
        .get(Collection::EventsV1.id(), &version.event_key())?
        .ok_or_else(|| {
            EngineError::new(ErrorClass::Corrupt, "an indexed version has no event row")
        })?;
    let record = codecs::decode_event(&row)?;
    if record.namespace != namespace || record.key != key {
        return Err(EngineError::new(
            ErrorClass::Corrupt,
            "an indexed version's event row names another key",
        ));
    }
    Ok(entry_of(record.kind, record.entry))
}

fn entry_of(kind: KvEventKind, entry: Option<coord_state::KvEntry>) -> Chosen {
    match kind {
        KvEventKind::Put => entry,
        KvEventKind::Delete => None,
    }
}
