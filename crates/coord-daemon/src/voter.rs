//! A voter and the two ways work reaches it (design Sections 3.2, 3.3,
//! 22.1).
//!
//! [`Node`] drives the protocol machine and its store. This is the layer
//! above: what turns an arriving frame into an event that machine may
//! consume, and what a voter's own evidence looks like to the collector
//! that counts it.
//!
//! # Two routes, one door
//!
//! A submission arrives either on the peer plane from a collector in
//! another process, or -- when the collector is in *this* process -- out
//! of the voter's own [`Ingress`]. The two differ in how the bytes got
//! here and in nothing else. Both are parsed by the same bounded frame
//! reader, with the same class limits; both are admitted by
//! [`coord_collector::admitted_from_submit`], which mints the receipt
//! only after checking that the submitting role is one that may act on a
//! client's behalf; both then step the same machine, against the same
//! ballot, through the same store.
//!
//! The local route is not permitted to be shorter than that. Skipping
//! the network is a transport optimization; skipping the admission
//! boundary would be a second, weaker way into the protocol, and a
//! cluster with two ways in has the properties of the weaker one.
//!
//! # A local vote is still one vote
//!
//! A voter running beside the collector does not get to acknowledge its
//! own submission. Its evidence is produced by the machine, held by the
//! outbox until the record behind it is durable, and handed to the
//! collector as a frame with this replica's committed identity on it --
//! whereupon it is validated and deduplicated by voter identity exactly
//! as a frame off the wire is. There is no local-success flag and no
//! pre-counted acknowledgement, which is what keeps one co-located voter
//! from looking like a quorum.

use coord_collector::{IngressError, admitted_from_submit};
use coord_core::effect::{BootId, PeerId};
use coord_core::event::{AuthenticatedPeerMessage, Event, PeerProvenance};
use coord_membership::membership::Membership;
use coord_storage::Persistence;
use coord_types::ids::{Ballot, ReplicaId, ReplicaIncarnation};
use coord_types::wire_v1::{FrameReader, WireError};

use crate::mailbox::Ingress;
use crate::node::{DriveError, Node, Outbound};

/// The replica identity reserved for a domain's trusted collector.
///
/// A collector is not a node. It holds no replica identity, votes on
/// nothing and appears in no configuration; the protocol machines need
/// a [`PeerId`] only to say "this send is the collector's" rather than
/// a peer's. This is that label.
///
/// It is reserved rather than derived because the property that matters
/// is simply that no voter has it, and [`collector_peer`] checks
/// exactly that against the committed configuration rather than
/// trusting an identity space to stay disjoint.
pub const COLLECTOR_LABEL: ReplicaId = ReplicaId([0xff; 16]);

/// The peer identity this domain's voters address their collector as.
///
/// `None` when the committed configuration names a voter holding the
/// reserved label, which would make a voter's evidence and a peer's
/// send indistinguishable to the driver.
pub fn collector_peer(membership: &Membership) -> Option<PeerId> {
    if membership.voters().any(|v| v.node == COLLECTOR_LABEL) {
        return None;
    }
    Some(PeerId {
        replica: COLLECTOR_LABEL,
        incarnation: ReplicaIncarnation::ZERO,
    })
}

/// Why a frame offered to a voter was not admitted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refused {
    /// The bytes are not one well-formed frame of a permitted class.
    Malformed(WireError),
    /// The frame is not a submission this submitter may make.
    NotAdmissible(IngressError),
}

/// One voter of one domain, with the ingress a collector in this process
/// delivers through.
pub struct Voter<P: Persistence> {
    node: Node<P>,
    ingress: Ingress,
    ballot: Ballot,
    provenance: PeerProvenance,
    /// Local submissions admitted since boot (diagnostic).
    pub admitted: u64,
    /// Local submissions refused at the door since boot (diagnostic).
    pub refused: u64,
}

impl<P: Persistence> Voter<P> {
    /// A voter over `node`, taking local submissions from `ingress` and
    /// voting at `ballot`.
    ///
    /// The provenance this voter's evidence carries is built here, once,
    /// from the ingress's committed identity. It is not derived from
    /// anything a frame says, which is why the collector can count it
    /// the same way it counts a peer's.
    pub fn new(node: Node<P>, ingress: Ingress, ballot: Ballot) -> Self {
        let provenance = PeerProvenance::from_local_voter(ingress.replica(), ingress.incarnation());
        Voter {
            node,
            ingress,
            ballot,
            provenance,
            admitted: 0,
            refused: 0,
        }
    }

    /// The identity this voter's evidence reaches a collector under.
    pub const fn provenance(&self) -> PeerProvenance {
        self.provenance
    }

    /// A handle for a collector in this process to deliver through.
    pub fn route(&self) -> crate::mailbox::LocalRoute {
        self.ingress.route()
    }

    /// The node.
    pub const fn node(&self) -> &Node<P> {
        &self.node
    }

    /// The node, mutably.
    pub const fn node_mut(&mut self) -> &mut Node<P> {
        &mut self.node
    }

    /// The ballot this voter is at.
    pub const fn ballot(&self) -> Ballot {
        self.ballot
    }

    /// Move to `ballot` (a campaign, or an adopted higher one).
    pub const fn set_ballot(&mut self, ballot: Ballot) {
        self.ballot = ballot;
    }

    /// Local submissions waiting for a turn.
    pub fn waiting(&self) -> usize {
        self.ingress.depth()
    }

    /// Whether there is work the runtime should give this voter a turn
    /// for before it goes back to waiting on its sockets.
    pub fn has_work(&self) -> bool {
        self.ingress.depth() > 0 || self.node.machine().next_executable().is_some()
    }

    /// The first event of this incarnation.
    pub fn boot(
        &mut self,
        boot: BootId,
        incarnation: coord_types::ids::ReplicaIncarnation,
    ) -> Result<Outbound, DriveError> {
        self.node.on_event(
            Event::Boot {
                boot_id: boot,
                incarnation,
            },
            &self.ballot,
        )
    }

    /// An authenticated frame from a peer.
    pub fn on_peer(
        &mut self,
        provenance: PeerProvenance,
        frame: Vec<u8>,
    ) -> Result<Outbound, DriveError> {
        self.node.on_event(
            Event::Peer(AuthenticatedPeerMessage::new(provenance, frame)),
            &self.ballot,
        )
    }

    /// A submission a collector on the peer plane made.
    ///
    /// The local route below uses this same function: past the door
    /// there is one path.
    pub fn on_submission(
        &mut self,
        submitter: coord_types::wire_v1::PeerRole,
        frame: &coord_types::wire_v1::Frame,
    ) -> Result<Result<Outbound, Refused>, DriveError> {
        let admitted = match admitted_from_submit(submitter, frame) {
            Ok(a) => a,
            Err(e) => return Ok(Err(Refused::NotAdmissible(e))),
        };
        self.node
            .on_event(Event::Admitted(admitted), &self.ballot)
            .map(Ok)
    }

    /// Take up to `budget` local submissions and give the voter its turn
    /// on each.
    ///
    /// Bounded on purpose. The runtime interleaves this with the peer
    /// plane, its timers and its recovery, so a collector that can fill
    /// the ingress faster than the voter empties it slows the voter down
    /// rather than deciding what else the voter does not get to do.
    ///
    /// A refused frame is counted and dropped. It never reached a
    /// machine, so there is nothing to undo and nothing to answer: the
    /// caller that sent it is waiting on evidence that will not come,
    /// which is the same thing that happens to a submission a remote
    /// voter refuses.
    pub fn serve_local(&mut self, budget: usize) -> Result<(Outbound, Vec<Refused>), DriveError> {
        let mut out = Outbound::default();
        let mut refused = Vec::new();
        for bytes in self.ingress.take(budget) {
            match parse(&bytes) {
                Err(e) => {
                    self.refused += 1;
                    refused.push(Refused::Malformed(e));
                }
                Ok(frame) => match self.on_submission(self.ingress.submitter(), &frame)? {
                    Ok(round) => {
                        self.admitted += 1;
                        out.absorb(round);
                    }
                    Err(why) => {
                        self.refused += 1;
                        refused.push(why);
                    }
                },
            }
        }
        Ok((out, refused))
    }

    /// Apply every command whose turn has come.
    pub fn execute(&mut self) -> Result<Outbound, DriveError> {
        self.node.execute(&self.ballot)
    }
}

/// One frame, through the reader the transport uses.
///
/// The same bounded reader and the same class limits, so a local
/// submission cannot be a frame a peer would have been refused for
/// sending. Trailing bytes are an error: one offer is one frame.
fn parse(bytes: &[u8]) -> Result<coord_types::wire_v1::Frame, WireError> {
    let mut reader = FrameReader::new();
    reader.push(bytes)?;
    let frame = reader.next_frame()?.ok_or(WireError::LengthBelowMinimum {
        length: bytes.len() as u32,
    })?;
    reader.finish()?;
    Ok(frame)
}
