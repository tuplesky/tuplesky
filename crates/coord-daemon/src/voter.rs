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
use coord_types::CommandId;
use coord_types::ids::{Ballot, ClusterId, DomainId, ReplicaId, ReplicaIncarnation};
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

/// Where a submission came from, and therefore where its evidence goes.
///
/// A voter's evidence belongs to the collector that submitted the
/// command, not to whichever collector happens to share its process. In
/// a deployment where every node runs a frontend, sending it to the
/// local one would give the caller's collector nothing to count and
/// would hand a second collector evidence for a request it never made.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    /// A collector in this process, through the voter's own ingress.
    Local,
    /// A collector on the other end of an API-class connection.
    Connection(u64),
}

/// How many commands a voter remembers a collector for.
///
/// Bounded, because a collector that submitted and vanished must not
/// cost this replica memory for ever. Falling out of the bound costs a
/// caller its evidence and nothing else: the command is committed and
/// durable either way, and the caller resolves it by identity.
const ORIGINS: usize = 4096;

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
    /// This domain's committed origin, which a submission's claims must
    /// have been attested for.
    origin: (ClusterId, DomainId),
    ballot: Ballot,
    provenance: PeerProvenance,
    origins: alloc_map::Origins,
    /// Local submissions admitted since boot (diagnostic).
    pub admitted: u64,
    /// Local submissions refused at the door since boot (diagnostic).
    pub refused: u64,
    /// Queued transitions refused because a higher ballot was promised
    /// (diagnostic, task-d01).
    pub fenced: u64,
}

impl<P: Persistence> Voter<P> {
    /// A voter over `node`, taking local submissions from `ingress` and
    /// voting at `ballot`.
    ///
    /// The provenance this voter's evidence carries is built here, once,
    /// from the ingress's committed identity. It is not derived from
    /// anything a frame says, which is why the collector can count it
    /// the same way it counts a peer's.
    ///
    /// The store is told the same ballot here, whatever it was opened
    /// with: it stamps every transition it records, and the ballot the
    /// machine steps at is this one ([`Persistence::follow_ballot`]).
    pub fn new(
        mut node: Node<P>,
        ingress: Ingress,
        origin: (ClusterId, DomainId),
        ballot: Ballot,
    ) -> Self {
        let provenance = PeerProvenance::from_local_voter(ingress.replica(), ingress.incarnation());
        node.applier_mut().store_mut().follow_ballot(ballot);
        Voter {
            node,
            ingress,
            origin,
            ballot,
            provenance,
            origins: alloc_map::Origins::new(ORIGINS),
            admitted: 0,
            refused: 0,
            fenced: 0,
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
    ///
    /// The store moves with it. The machine is stepped at this voter's
    /// ballot, and the store stamps what it records with its own, so the
    /// two are one value kept in two places: moving only this one would
    /// record every later promise and vote under the ballot before.
    ///
    /// Nothing in production calls this yet. The points where a voter
    /// adopts a higher ballot are inside `coord-consensus` -- a
    /// follower's `NewLeader`, and a promise row turning durable
    /// (`PromiseOutcome::Promised`) -- and neither is surfaced to the
    /// voter, so a promise for a higher ballot would still be stamped
    /// with the one this voter was built at. Since task-d01 every ballot
    /// the voter adopts comes through here: its own campaign
    /// ([`Voter::campaign`]), a candidate's `NewLeader` (before the promise
    /// row is written, see [`Voter::on_peer`]), and any promise the
    /// machine made by another path ([`Voter::follow_machine`]).
    pub fn set_ballot(&mut self, ballot: Ballot) {
        self.ballot = ballot;
        self.node.applier_mut().store_mut().follow_ballot(ballot);
    }

    /// Campaign for the next ballot, led by this voter (task-d01).
    ///
    /// The order is the whole of the wiring. The ballot moves first, so
    /// the promise this voter makes itself is stamped with the ballot it
    /// promises; the campaign then queues that promise and asks the other
    /// voters for theirs; and the store is fenced at the new ballot last,
    /// which refuses what was still queued under the old one -- work the
    /// old leader's ballot can no longer make durable -- and keeps the
    /// promise row. A leader does not campaign, and nor does a voter whose
    /// ballot number cannot be advanced.
    ///
    /// Returns the ballot campaigned for, or `None` when there was nothing
    /// to do.
    pub fn campaign(&mut self) -> Result<Option<(Ballot, Outbound)>, DriveError> {
        if self.leads() {
            return Ok(None);
        }
        let highest = highest(self.ballot, self.node.machine().promised());
        let Ok(ballot) = highest.successor(self.ingress.replica()) else {
            return Ok(None);
        };
        let before = self.ballot;
        self.set_ballot(ballot);
        let mut out = self.node.campaign(ballot, &self.ballot)?;
        if self.node.machine().promised() != ballot {
            // The machine refused to campaign (it has not booted, or the
            // ballot is not one it can lead): nothing was promised, so
            // nothing moves.
            self.set_ballot(before);
            return Ok(None);
        }
        out.absorb(self.fence()?);
        Ok(Some((ballot, out)))
    }

    /// Publish a selection this voter bound durably before it last
    /// stopped (see [`Node::resume_campaign`]); the voter is already at
    /// the decision's ballot.
    pub fn resume_campaign(
        &mut self,
        decision: coord_consensus::SyncDecision,
    ) -> Result<Outbound, DriveError> {
        self.node.resume_campaign(decision, &self.ballot)
    }

    /// Bring a voter whose link has come back onto the ballot this voter
    /// leads (task-d01): a `NewLeader` for it, which the voter promises if
    /// it has not already, and the Sync follows when its promise arrives
    /// (see [`Voter::on_peer`]).
    ///
    /// A campaign asks every voter once, and a voter that was away then
    /// -- down, or cut off -- never heard it: it comes back still leading,
    /// or still following, a ballot the domain has left, and nothing
    /// else would tell it. Nothing to say for the genesis ballot, which
    /// every voter starts at.
    pub fn welcome(&self, replica: ReplicaId) -> Outbound {
        let mut out = Outbound::default();
        if self.leads() && self.node.won().is_some() && replica != self.ingress.replica() {
            out.peer.push((
                PeerId {
                    replica,
                    incarnation: ReplicaIncarnation::ZERO,
                },
                coord_consensus::ProtocolMessage::NewLeader {
                    ballot: self.ballot,
                }
                .encode(),
            ));
        }
        out
    }

    /// Follow the machine after a step: adopt any higher ballot it has
    /// promised, and change role when it has won a campaign or been
    /// deposed (task-d01).
    ///
    /// Every entry point that steps the machine ends here, so a ballot
    /// the machine moved to by any path reaches the store, and the store
    /// is fenced at it.
    pub fn follow_machine(&mut self) -> Result<Outbound, DriveError> {
        let mut out = Outbound::default();
        let promised = self.node.machine().promised();
        if higher(&promised, &self.ballot) {
            self.set_ballot(promised);
            out.absorb(self.fence()?);
        }
        if let Some(changed) = self.node.change_role(&self.ballot)? {
            out.absorb(changed);
        }
        Ok(out)
    }

    /// Fence the store at this voter's ballot and feed back the queued
    /// transitions it refused, so no barrier waits for ever on work the
    /// old ballot can no longer make durable.
    fn fence(&mut self) -> Result<Outbound, DriveError> {
        let refused = self
            .node
            .applier_mut()
            .store_mut()
            .fence(self.ballot)
            .map_err(|e| DriveError::Engine(format!("{e:?}")))?;
        let mut out = Outbound::default();
        for event in refused {
            self.fenced += 1;
            out.absorb(self.node.on_storage(event, &self.ballot)?);
        }
        Ok(out)
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
    ///
    /// A candidate's `NewLeader` for a ballot above this voter's is
    /// adopted *before* the machine sees it (task-d01): the promise row
    /// the machine queues in reply is stamped with the ballot the store
    /// follows, and stamped with the old one it would be a promise
    /// recorded under a ballot this voter was leaving, which the fence at
    /// the new ballot refuses. If the machine does not promise after all
    /// -- a guard it applies and this does not -- the ballot goes back.
    pub fn on_peer(
        &mut self,
        provenance: PeerProvenance,
        frame: Vec<u8>,
    ) -> Result<Outbound, DriveError> {
        let before = self.ballot;
        let message = coord_consensus::ProtocolMessage::decode(&frame).ok();
        if let Some(coord_consensus::ProtocolMessage::NewLeader { ballot }) = &message
            && higher(ballot, &self.ballot)
        {
            self.set_ballot(*ballot);
        }
        // A voter this leader welcomed back (see [`Voter::welcome`]) has
        // promised its ballot: it is sent the selection the ballot was won
        // with, which it missed, and follows from there. A promise that
        // arrives after the campaign is won and from a voter that did get
        // the Sync is answered too; a second copy of the active ballot's
        // Sync changes nothing.
        let mut out = Outbound::default();
        if let Some(coord_consensus::ProtocolMessage::Promise {
            ballot, replica, ..
        }) = &message
            && *replica == provenance.from()
            && *ballot == self.ballot
            && self.leads()
            && let Some(decision) = self.node.won()
        {
            out.peer.push((
                PeerId {
                    replica: *replica,
                    incarnation: ReplicaIncarnation::ZERO,
                },
                coord_consensus::ProtocolMessage::Sync(decision.clone()).encode(),
            ));
        }
        out.absorb(self.node.on_event(
            Event::Peer(AuthenticatedPeerMessage::new(provenance, frame)),
            &self.ballot,
        )?);
        if self.ballot != before {
            if self.node.machine().promised() == self.ballot {
                // Promised: what is still queued under the ballot this
                // voter left is refused now, as for its own campaign.
                out.absorb(self.fence()?);
            } else {
                self.set_ballot(before);
            }
        }
        out.absorb(self.follow_machine()?);
        Ok(out)
    }

    /// A submission a collector made, from `origin`.
    ///
    /// The local route below uses this same function: past the door
    /// there is one path. `origin` is not part of that path -- it never
    /// reaches a machine and cannot change what a command is -- it only
    /// says where this command's evidence is owed.
    pub fn on_submission(
        &mut self,
        submitter: coord_types::wire_v1::PeerRole,
        frame: &coord_types::wire_v1::Frame,
        origin: Origin,
    ) -> Result<Result<Outbound, Refused>, DriveError> {
        let admitted = match admitted_from_submit(submitter, frame, self.origin.0, self.origin.1) {
            Ok(a) => a,
            Err(e) => return Ok(Err(Refused::NotAdmissible(e))),
        };
        // The identity the machine will derive, derived the same way. A
        // retry rebinds it: the same command submitted again through
        // another collector is owed to that one, and a collector that
        // went away is not owed anything.
        if let Some(command) = command_of(&admitted.frame) {
            self.origins.remember(command, origin);
        }
        self.node
            .on_event(Event::Admitted(admitted), &self.ballot)
            .map(Ok)
    }

    /// Where this command's evidence is owed, if this voter admitted it.
    pub fn origin_of(&self, command: &CommandId) -> Option<Origin> {
        self.origins.get(command)
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
                Ok(frame) => {
                    match self.on_submission(self.ingress.submitter(), &frame, Origin::Local)? {
                        Ok(round) => {
                            self.admitted += 1;
                            out.absorb(round);
                        }
                        Err(why) => {
                            self.refused += 1;
                            refused.push(why);
                        }
                    }
                }
            }
        }
        Ok((out, refused))
    }

    /// Apply every command whose turn has come.
    /// Take what this voter's machine refused since the last call.
    pub fn take_rejections(&mut self) -> Vec<String> {
        self.node.take_rejections()
    }

    /// Propose one of the service's own commands (an expiry candidate,
    /// a lease authority epoch). Nothing happens unless this replica
    /// leads, and nothing but those two operations may travel this way.
    pub fn propose_service(&mut self, frame: &[u8]) -> Result<Outbound, DriveError> {
        self.node.propose_service(frame, &self.ballot)
    }

    /// Whether this replica is holding a command it knows by identity
    /// and not by content.
    pub fn wants_payloads(&self) -> bool {
        self.node.wants_payloads()
    }

    /// How many commands this replica knows by identity and not by
    /// content.
    pub fn missing_payloads(&self) -> usize {
        self.node.missing_payloads()
    }

    /// How many payload transfers a peer has answered this replica
    /// with.
    pub fn payloads_answered(&self) -> u64 {
        self.node.payloads_answered()
    }

    /// Ask this ballot's leader for the payloads this replica lacks.
    pub fn request_payloads(&mut self) -> Result<Outbound, DriveError> {
        let leader = self.ballot.leader;
        self.node.request_payloads(leader, &self.ballot)
    }

    /// The command execution is waiting for a payload for, if any.
    pub fn awaiting(&self) -> Option<coord_types::CommandId> {
        self.node.awaiting()
    }

    /// Whether this replica currently leads its ballot.
    pub fn leads(&self) -> bool {
        self.node.machine().leads()
    }

    /// Apply every command whose turn has come.
    pub fn execute(&mut self) -> Result<Outbound, DriveError> {
        self.node.execute(&self.ballot)
    }
}

/// The command a request frame is the submission of.
///
/// Derived from the request's own identity, exactly as the machine
/// derives it, so the two cannot disagree about which command a frame
/// became.
fn command_of(frame: &[u8]) -> Option<CommandId> {
    let coord_types::wire_v1::MessageV1::Request(request) =
        coord_types::wire_v1::decode(&parse(frame).ok()?).ok()?
    else {
        return None;
    };
    let logical = request.logical().ok()?;
    CommandId::derive(&request.retry_key, &logical).ok()
}

/// A bounded map of command to collector.
mod alloc_map {
    use std::collections::{BTreeMap, VecDeque};

    use coord_types::CommandId;

    use super::Origin;

    /// Commands this voter admitted, and where each one's evidence is
    /// owed, oldest forgotten first.
    #[derive(Debug)]
    pub struct Origins {
        by_command: BTreeMap<CommandId, Origin>,
        order: VecDeque<CommandId>,
        bound: usize,
    }

    impl Origins {
        pub fn new(bound: usize) -> Self {
            Origins {
                by_command: BTreeMap::new(),
                order: VecDeque::new(),
                bound: bound.max(1),
            }
        }

        /// Bind `command` to `origin`, replacing any earlier binding.
        pub fn remember(&mut self, command: CommandId, origin: Origin) {
            if self.by_command.insert(command, origin).is_none() {
                self.order.push_back(command);
            }
            while self.order.len() > self.bound {
                if let Some(old) = self.order.pop_front() {
                    self.by_command.remove(&old);
                }
            }
        }

        pub fn get(&self, command: &CommandId) -> Option<Origin> {
            self.by_command.get(command).copied()
        }
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

/// Whether `a` is a higher ballot than `b` of the same epoch.
fn higher(a: &Ballot, b: &Ballot) -> bool {
    a.compare_same_epoch(b) == Some(core::cmp::Ordering::Greater)
}

/// The higher of two ballots of the same epoch; `a` if they are not
/// comparable.
fn highest(a: Ballot, b: Ballot) -> Ballot {
    if higher(&b, &a) { b } else { a }
}
