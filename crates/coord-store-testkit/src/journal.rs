//! The deterministic model journal (task-j01; design Sections 17.3.1-17.3.3,
//! 17.15, 17.16.3).
//!
//! `ModelJournal` implements the `coord-journal-api` contract in memory: it
//! refuses a stream whose mapping is not durable, validates every entry
//! against the accepted durable head (index, origin, predecessor chain and
//! digest) before anything is appended, applies a whole group or nothing,
//! and reports scripted outcomes (durable, definitely not appended,
//! indeterminate with the group present or absent). Its events are distinct
//! kinds: a durable append is `Durable`, materialization is the caller's
//! `Materialized`, a checkpoint pointer is a record like any other, and
//! there is no establishment event at all. Nothing here is real-engine
//! crash qualification (task-j05).

use std::collections::{BTreeMap, VecDeque};

use coord_core::effect::BarrierId;
use coord_journal_api::engine::{JournalEngine, ReadBudget, RecordPage};
use coord_journal_api::failure::{JournalError, JournalErrorClass, JournalFailure};
use coord_journal_api::frontier::CheckpointPointerV1;
use coord_journal_api::group::{GroupReceipt, GroupWrite};
use coord_journal_api::head::WrittenBytes;
use coord_journal_api::record::{JournalRecordV1, RecordBody, RecordExpectation, RecordOrigin};
use coord_journal_api::stream::{StorageStreamId, StreamHighWater, StreamMappingV1};
use coord_types::ids::LocalJournalSeq;

/// Scripted outcome of the next group append.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppendScript {
    /// Append and sync.
    Durable,
    /// Fail after validation with definite noncommit evidence.
    DefinitelyNotCommitted,
    /// Fail after submission; the group is durable when `applied`.
    Indeterminate {
        /// Whether the group reached the log before the failure.
        applied: bool,
    },
}

/// Distinct model events. None of them is protocol establishment, and
/// materialization is reported by the materializer, not the journal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JournalEvent {
    /// A stream mapping became durable.
    MappingPersisted {
        /// Stream.
        stream: StorageStreamId,
    },
    /// One entry of a group is durable.
    Durable {
        /// Barrier.
        barrier: BarrierId,
        /// Stream.
        stream: StorageStreamId,
        /// Last durable sequence of the entry.
        last: LocalJournalSeq,
    },
    /// A group append failed.
    Failed {
        /// Whether the failure was definite.
        definite: bool,
    },
    /// Entries through `through` were retired under a durable pointer.
    Retired {
        /// Stream.
        stream: StorageStreamId,
        /// Represented sequence of the pointer.
        through: LocalJournalSeq,
    },
}

#[derive(Clone, Debug)]
struct Stream {
    records: Vec<JournalRecordV1>,
    /// Sequences at or below this were retired.
    retired: LocalJournalSeq,
}

impl Default for Stream {
    fn default() -> Self {
        Stream {
            records: Vec::new(),
            retired: LocalJournalSeq::ZERO,
        }
    }
}

impl Stream {
    fn head(&self) -> LocalJournalSeq {
        self.records
            .last()
            .map_or(self.retired, JournalRecordV1::seq)
    }
}

/// The model journal of one shard set.
#[derive(Clone, Debug, Default)]
pub struct ModelJournal {
    streams: BTreeMap<StorageStreamId, Stream>,
    mappings: BTreeMap<StorageStreamId, StreamMappingV1>,
    high_water: StreamHighWater,
    scripts: VecDeque<AppendScript>,
    events: Vec<JournalEvent>,
    appends: u64,
}

fn definite(diagnostic: &'static str) -> JournalFailure {
    JournalFailure::rejected_before_append(diagnostic)
}

impl ModelJournal {
    /// Empty journal with no mappings.
    pub fn new() -> Self {
        ModelJournal::default()
    }

    /// Script the outcome of upcoming appends (consumed in order; the
    /// default is durable).
    pub fn script_append(&mut self, script: AppendScript) {
        self.scripts.push_back(script);
    }

    /// Recorded events.
    pub fn events(&self) -> &[JournalEvent] {
        &self.events
    }

    /// Number of appends attempted (validated groups only).
    pub const fn appends(&self) -> u64 {
        self.appends
    }

    /// Every durable record of a stream in sequence order (retired ones
    /// excluded).
    pub fn records(&self, stream: StorageStreamId) -> &[JournalRecordV1] {
        self.streams.get(&stream).map_or(&[], |s| &s.records)
    }

    /// What the next record of a stream must be. A stream with durable
    /// records continues their chain; an empty one expects genesis under
    /// the identity its durable mapping was allocated for, so the cluster,
    /// domain and incarnation checks compare against the mapping rather
    /// than against the record that is being validated. The mapping does
    /// not persist a replica, so that field is the record's own.
    fn expectation(&self, mapping: &StreamMappingV1, first: &JournalRecordV1) -> RecordExpectation {
        match self
            .streams
            .get(&mapping.stream)
            .and_then(|s| s.records.last())
        {
            Some(last) => RecordExpectation {
                origin: *last.origin(),
                seq: last.seq().checked_next().expect("model head below maximum"),
                predecessor: last.digest(),
            },
            None => RecordExpectation::genesis(RecordOrigin {
                cluster: mapping.key.cluster,
                domain: mapping.key.domain,
                replica: first.origin().replica,
                incarnation: mapping.key.incarnation,
                stream: mapping.stream,
            }),
        }
    }

    fn validate(&self, group: &GroupWrite) -> Result<(), JournalFailure> {
        if group.is_empty() {
            return Err(definite("empty group"));
        }
        for entry in group.entries() {
            let Some(mapping) = self.mappings.get(&entry.stream()) else {
                return Err(definite("stream mapping not durable"));
            };
            // A retired mapping stays durable so the identifier is never
            // recycled, but the stream it names is closed to appends.
            if mapping.retired {
                return Err(definite("stream retired"));
            }
            let mut expect = self.expectation(mapping, &entry.records()[0]);
            if let Some(s) = self.streams.get(&entry.stream())
                && s.records.is_empty()
                && s.retired != LocalJournalSeq::ZERO
            {
                // A fully retired stream continues after its retired
                // prefix; its chain is anchored by the checkpoint.
                expect.seq = s.retired.checked_next().expect("below maximum");
                expect.predecessor = entry.records()[0].predecessor();
            }
            for record in entry.records() {
                record
                    .verify(&expect)
                    .map_err(|_| definite("record guard rejected"))?;
                expect = expect.after(record).expect("below maximum");
            }
        }
        Ok(())
    }

    fn apply(&mut self, group: &GroupWrite) -> GroupReceipt {
        let mut bytes = 0u64;
        for entry in group.entries() {
            let stream = self.streams.entry(entry.stream()).or_default();
            stream.records.extend(entry.records().iter().cloned());
            bytes += entry.bytes() as u64;
        }
        group
            .receipt(WrittenBytes(bytes))
            .expect("validated nonempty group")
    }
}

impl JournalEngine for ModelJournal {
    fn append_group(&mut self, group: &GroupWrite) -> Result<GroupReceipt, JournalFailure> {
        self.validate(group)?;
        self.appends += 1;
        let script = self.scripts.pop_front().unwrap_or(AppendScript::Durable);
        match script {
            AppendScript::Durable => {
                let receipt = self.apply(group);
                for c in &receipt.completions {
                    self.events.push(JournalEvent::Durable {
                        barrier: c.barrier,
                        stream: c.stream,
                        last: c.last,
                    });
                }
                Ok(receipt)
            }
            AppendScript::DefinitelyNotCommitted => {
                self.events.push(JournalEvent::Failed { definite: true });
                Err(JournalFailure::Definite(JournalError::new(
                    JournalErrorClass::Io,
                    "scripted noncommit",
                )))
            }
            AppendScript::Indeterminate { applied } => {
                if applied {
                    let _ = self.apply(group);
                }
                self.events.push(JournalEvent::Failed { definite: false });
                Err(JournalFailure::after_submission(JournalError::new(
                    JournalErrorClass::Io,
                    "scripted sync failure",
                )))
            }
        }
    }

    fn durable_head(&self, stream: StorageStreamId) -> Result<LocalJournalSeq, JournalError> {
        Ok(self
            .streams
            .get(&stream)
            .map_or(LocalJournalSeq::ZERO, Stream::head))
    }

    fn read_suffix(
        &self,
        stream: StorageStreamId,
        after: LocalJournalSeq,
        budget: ReadBudget,
    ) -> Result<RecordPage, JournalError> {
        let Some(s) = self.streams.get(&stream) else {
            return Ok(RecordPage {
                records: Vec::new(),
                exhausted: true,
            });
        };
        if after < s.retired {
            return Err(JournalError::new(
                JournalErrorClass::Corrupt,
                "required suffix starts inside the retired prefix",
            ));
        }
        let mut records = Vec::new();
        let mut bytes = 0usize;
        for r in s.records.iter().filter(|r| r.seq() > after) {
            let len = r
                .encoded_len()
                .map_err(|_| JournalError::new(JournalErrorClass::Corrupt, "record encode"))?;
            if records.len() as u32 >= budget.max_records.get()
                || (!records.is_empty() && bytes + len > budget.max_bytes.get() as usize)
            {
                return Ok(RecordPage {
                    records,
                    exhausted: false,
                });
            }
            bytes += len;
            records.push(r.clone());
        }
        Ok(RecordPage {
            records,
            exhausted: true,
        })
    }

    fn retire_prefix(
        &mut self,
        stream: StorageStreamId,
        pointer: &CheckpointPointerV1,
    ) -> Result<(), JournalFailure> {
        let Some(s) = self.streams.get_mut(&stream) else {
            return Err(definite("unknown stream"));
        };
        let published = s.records.iter().any(|r| {
            matches!(r.body(), RecordBody::PublishLocalCheckpoint(p) if p == pointer)
                && r.seq() > pointer.represented
        });
        if !published {
            return Err(definite("pointer is not durable in the stream"));
        }
        s.records.retain(|r| r.seq() > pointer.represented);
        s.retired = pointer.represented;
        self.events.push(JournalEvent::Retired {
            stream,
            through: pointer.represented,
        });
        Ok(())
    }

    fn mappings(&self) -> Result<(StreamHighWater, Vec<StreamMappingV1>), JournalError> {
        Ok((self.high_water, self.mappings.values().copied().collect()))
    }

    fn persist_mapping(
        &mut self,
        high_water: StreamHighWater,
        mapping: &StreamMappingV1,
    ) -> Result<(), JournalFailure> {
        if high_water < self.high_water || mapping.stream.get() > high_water.get() {
            return Err(definite("high-water mark must cover the mapping"));
        }
        if let Some(existing) = self.mappings.get(&mapping.stream) {
            if existing.key != mapping.key || existing.shard != mapping.shard {
                return Err(definite(
                    "mapping identity of an allocated stream cannot change",
                ));
            }
            // Retirement is permanent, here as in the real journal:
            // persisting a stream's own older, active mapping must not
            // bring it back.
            if existing.retired && !mapping.retired {
                return Err(definite("a retired stream cannot become active again"));
            }
        }
        self.high_water = high_water;
        self.mappings.insert(mapping.stream, *mapping);
        self.events.push(JournalEvent::MappingPersisted {
            stream: mapping.stream,
        });
        Ok(())
    }
}
