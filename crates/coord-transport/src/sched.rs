//! Fair group scheduling and wait accounting (task-31; design Sections
//! 3.3, 11.6, 11.7): frames of every domain group queued on a lane are
//! served round-robin across groups, each group's queue is bounded, and
//! the time a frame waits in the queue is measured apart from the time it
//! waits for budget and stream credit, both apart from the path RTT.

use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

use coord_types::ids::DomainId;

/// A frame waiting to be sent.
#[derive(Debug)]
pub struct Queued {
    /// Group (domain) the frame belongs to.
    pub group: DomainId,
    /// Complete encoded frame.
    pub frame: Vec<u8>,
    /// When it was queued.
    pub enqueued: Instant,
}

/// Why a frame was refused at the queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueueError {
    /// The group's queue is at its depth.
    GroupFull,
    /// Too many groups have queued frames.
    TooManyGroups,
}

/// Round-robin queues, one per group.
#[derive(Debug)]
pub struct FairQueue {
    groups: BTreeMap<DomainId, VecDeque<Queued>>,
    ring: VecDeque<DomainId>,
    depth: usize,
    max_groups: usize,
    len: usize,
}

impl FairQueue {
    /// A queue with `depth` frames per group and at most `max_groups`.
    pub fn new(depth: usize, max_groups: usize) -> Self {
        FairQueue {
            groups: BTreeMap::new(),
            ring: VecDeque::new(),
            depth: depth.max(1),
            max_groups: max_groups.max(1),
            len: 0,
        }
    }

    /// Frames queued.
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Whether nothing is queued.
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Queue a frame for its group.
    pub fn push(&mut self, queued: Queued) -> Result<(), QueueError> {
        match self.groups.get_mut(&queued.group) {
            Some(q) => {
                if q.len() >= self.depth {
                    return Err(QueueError::GroupFull);
                }
                q.push_back(queued);
            }
            None => {
                if self.groups.len() >= self.max_groups {
                    return Err(QueueError::TooManyGroups);
                }
                self.ring.push_back(queued.group);
                self.groups
                    .entry(queued.group)
                    .or_default()
                    .push_back(queued);
            }
        }
        self.len += 1;
        Ok(())
    }

    /// The length of the frame [`Self::pop`] would return next.
    pub fn next_len(&self) -> Option<usize> {
        self.ring
            .iter()
            .find_map(|group| self.groups.get(group)?.front())
            .map(|q| q.frame.len())
    }

    /// The next frame, taking groups in turn.
    pub fn pop(&mut self) -> Option<Queued> {
        while let Some(group) = self.ring.pop_front() {
            let Some(q) = self.groups.get_mut(&group) else {
                continue;
            };
            let next = q.pop_front();
            if q.is_empty() {
                self.groups.remove(&group);
            } else {
                self.ring.push_back(group);
            }
            if let Some(n) = next {
                self.len -= 1;
                return Some(n);
            }
        }
        None
    }
}

/// Count, total and maximum of a wait.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WaitStats {
    /// Samples.
    pub count: u64,
    /// Sum of the waits.
    pub total: Duration,
    /// Longest wait.
    pub max: Duration,
}

impl WaitStats {
    /// Record one wait.
    pub fn record(&mut self, wait: Duration) {
        self.count += 1;
        self.total += wait;
        if wait > self.max {
            self.max = wait;
        }
    }
}

/// Per-lane accounting of one link.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LaneStats {
    /// Time from enqueue to being picked by the sender.
    pub queue_wait: WaitStats,
    /// Time from being picked to holding budget and an open stream.
    pub credit_wait: WaitStats,
    /// Frames handed to QUIC.
    pub frames: u64,
    /// Bytes handed to QUIC.
    pub bytes: u64,
    /// Frames refused at the queue.
    pub refused: u64,
    /// Frames queued now.
    pub queued: usize,
    /// Path RTT as QUIC estimates it (a separate quantity).
    pub rtt: Duration,
}

/// What a node's transport carried between voters (task-d62), across
/// every peer link and lane, cumulative.
///
/// A link whose two ends both offer
/// [`crate::CAPABILITY_FRAMES_PER_STREAM`] carries what was queued for
/// it together on one stream (task-d61), so frames per stream is the
/// batching factor; elsewhere each frame has a stream of its own.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PeerTraffic {
    /// Frames handed to QUIC for a peer.
    pub sent_frames: u64,
    /// Their bytes.
    pub sent_bytes: u64,
    /// Streams opened to peers.
    pub sent_streams: u64,
    /// Frames to a peer lost before they were written: no stream could be
    /// opened, or the write did not complete.
    pub sent_lost: u64,
    /// Streams those frames were lost on: a refused batch loses every
    /// frame in it, so `sent_lost` over this is the frames lost per
    /// refused stream (task-d61).
    pub sent_lost_streams: u64,
    /// Frames read from peers.
    pub received_frames: u64,
    /// Their bytes.
    pub received_bytes: u64,
    /// Streams peers opened to this node.
    pub received_streams: u64,
    /// UDP datagrams QUIC sent on peer connections, acknowledgements and
    /// retransmissions included (task-d61): what a frame costs is mostly
    /// what its packet costs.
    pub datagrams_sent: u64,
    /// UDP datagrams QUIC received on peer connections.
    pub datagrams_received: u64,
    /// The system calls that sent those datagrams: fewer than the
    /// datagrams where segmentation offload carries several in one.
    pub send_calls: u64,
    /// ACK frames QUIC sent on peer connections.
    pub acks_sent: u64,
    /// ACK frames QUIC received on peer connections.
    pub acks_received: u64,
    /// The same QUIC counts on api connections, callers' and collectors'
    /// (task-d70), so a node's datagrams split into its two planes.
    pub api: Datagrams,
}

/// What QUIC sent and received on one class of connections (task-d70).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Datagrams {
    /// UDP datagrams sent, acknowledgements and retransmissions included.
    pub datagrams_sent: u64,
    /// UDP datagrams received.
    pub datagrams_received: u64,
    /// The system calls that sent the datagrams.
    pub send_calls: u64,
    /// ACK frames sent.
    pub acks_sent: u64,
    /// ACK frames received.
    pub acks_received: u64,
}
