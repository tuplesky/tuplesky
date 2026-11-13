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

use coord_collector::{
    IngressError, MonotonicMillis, ReadAnswerV1, ReadOutcomeV1, ReadRefusal, admitted_from_submit,
    decode_read, read_answer_frame,
};
use coord_core::effect::{BootId, PeerId};
use coord_core::event::{AuthenticatedPeerMessage, Event, PeerProvenance};
use coord_membership::membership::Membership;
use coord_storage::Persistence;
use coord_types::CommandId;
use coord_types::ids::{Ballot, ClusterId, DomainId, ReplicaId, ReplicaIncarnation};
use coord_types::wire_v1::{FrameReader, WireError};

use crate::mailbox::Ingress;
use crate::node::{DriveError, Machine, Node, Outbound};
use crate::reads::{Evaluated, Leading, ReadBarrier, evaluate};

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
    /// The highest ballot the store was fenced at. A fence never moves
    /// back, so a voter whose promise did not become durable is below it,
    /// and a campaign for anything at or under it would be refused by its
    /// own store. Compared within an epoch only (`highest`): a fence
    /// from another epoch is not a bound on this one's ballots.
    fenced_at: Option<Ballot>,
    /// The reads this voter holds as a leader (task-d50).
    reads: ReadBarrier,
    /// The projection's completed frontier and the journaled position
    /// read just before the last view taken for due reads, when that view
    /// was behind one of them (task-d58; #185, lead 17:43). While both
    /// still read exactly this, a new view would show what that one did.
    reads_behind_at: Option<(u64, Option<coord_types::ids::ExecutionPosition>)>,
    /// Marked commands whose release this voter, as their leader,
    /// publishes again once its projection holds their retry record
    /// (task-d100, step A). Bounded by [`ORIGINS`], oldest first: a
    /// collector that is still waiting asks again.
    release_again: std::collections::BTreeMap<CommandId, ReleaseAgain>,
    /// The projection's completed frontier when the waiting releases
    /// were last looked up and some still waited: until a commit moves
    /// it, a new snapshot would show the same (#204 review).
    release_again_seen_at: Option<u64>,
    /// How many snapshots the waiting releases were looked up in
    /// (diagnostic).
    release_again_lookups: u64,
}

/// A marked command's release waiting to be published again (task-d100,
/// step A).
#[derive(Clone, Copy, Debug)]
struct ReleaseAgain {
    /// The command's retry key, where its record is.
    key: coord_types::RetryKey,
    /// The decided payload's admission digest.
    admission: coord_types::identity::Digest32,
    /// What this leader had executed through when it was asked: the
    /// command executed at or below it. A projection applied that far
    /// with no record for the key never will hold one: the command was
    /// refused at admission, which records nothing under the key
    /// (#204 review).
    executed_by: coord_types::ids::ExecutionPosition,
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
            fenced_at: None,
            reads: ReadBarrier::new(),
            reads_behind_at: None,
            release_again: std::collections::BTreeMap::new(),
            release_again_seen_at: None,
            release_again_lookups: 0,
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
        // Above the fence as well: after a promise that did not become
        // durable the voter is back below the ballot its store is fenced
        // at, and the successor of what it holds and has promised can be
        // under that fence (a candidate with a higher replica id at the
        // same number), which its own store refuses every time.
        let held = highest(self.ballot, self.node.machine().promised());
        // Above any ballot this voter refused as behind (task-d10): that
        // candidate promised itself the ballot and hears nothing below
        // it, and it has to follow this campaign's leader to catch up.
        let held = self
            .node
            .machine()
            .outranked()
            .map_or(held, |refused| highest(held, refused));
        let highest = self.fenced_at.map_or(held, |fence| highest(held, fence));
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
    ///
    /// The Sync goes with it. A voter that had promised this ballot and
    /// not received its Sync before it stopped refuses a `NewLeader` for
    /// the ballot it has already promised, so it sends no promise and
    /// would never be sent the Sync on one. It takes the Sync directly. A
    /// voter that has not promised the ballot refuses the Sync, harmlessly,
    /// and promises on the `NewLeader`. One that already has it ignores
    /// the copy.
    pub fn welcome(&self, replica: ReplicaId) -> Outbound {
        let mut out = Outbound::default();
        if self.leads()
            && replica != self.ingress.replica()
            && let Some(decision) = self.node.won()
        {
            let to = PeerId {
                replica,
                incarnation: ReplicaIncarnation::ZERO,
            };
            out.peer.push((
                to,
                coord_consensus::ProtocolMessage::NewLeader {
                    ballot: self.ballot,
                    executed: self.node.machine().executed_through(),
                }
                .encode(),
            ));
            out.peer.push((
                to,
                coord_consensus::ProtocolMessage::Sync(decision.clone()).encode(),
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
        } else if higher(&self.ballot, &promised) {
            // The promise this voter fenced for did not become durable, and
            // the machine is back at the ballot it had. The voter follows
            // it, so what the machine does is stamped with the ballot it
            // does it under. The store's fence does not move back. Work
            // stamped below it is refused at submit, and counted, until
            // the voter promises that ballot or a higher one again. For a
            // follower the refusal reaches the machine as a failed barrier
            // (`Node::one_round`): the transition is definitely not
            // committed, and the voter serves on. A leader stops instead,
            // and so does a refused apply (`DriveError::Fenced`); the fence
            // is in memory only, so a restart resumes at the promised
            // ballot. A voter that does not vote is always safe, and its
            // own next campaign goes above the fence (`Voter::campaign`).
            self.set_ballot(promised);
        }
        if let Some(changed) = self.node.change_role(&self.ballot)? {
            out.absorb(changed);
        }
        // A leader that refused a higher ballot as behind leads again above
        // it, at once (task-d10). The refused voter promised itself that
        // ballot and hears nothing below it, so under this leader's ballot
        // it would sit deaf, one voter short, until an election came
        // along -- and a restarted voter's timer firing before it hears
        // the leader is exactly how the refused campaign starts. Stepping
        // down and campaigning through [`Voter::campaign`] goes above the
        // refused ballot; the refused voter promises and follows. It costs
        // an election per refused campaign, bounded because the refused
        // voter does not campaign again until it has caught up.
        if self.leads()
            && self
                .node
                .machine()
                .outranked()
                .is_some_and(|refused| higher(&refused, &self.ballot))
        {
            out.absorb(self.node.step_down(&self.ballot)?);
            if let Some((_, campaigned)) = self.campaign()? {
                out.absorb(campaigned);
            }
        }
        Ok(out)
    }

    /// Fence the store at this voter's ballot and feed back the queued
    /// transitions it refused, so no barrier waits for ever on work the
    /// old ballot can no longer make durable.
    fn fence(&mut self) -> Result<Outbound, DriveError> {
        // What is queued is lowered first, under the ballot it was made
        // under, as a node lowering each round would have (task-d47):
        // the fence refuses only the work of a ballot left behind, and
        // work this voter made before it promised was not. It is
        // journaled before the fence, not lent: the fence tests what is
        // still queued against the frontier the journal has reached, and
        // a group out on the appender's thread is in none (task-d54).
        let mut out = self.node.flush_journaled(&self.ballot)?;
        let refused = self
            .node
            .applier_mut()
            .store_mut()
            .fence(self.ballot)
            .map_err(|e| DriveError::Engine(format!("{e:?}")))?;
        self.fenced_at = Some(
            self.fenced_at
                .map_or(self.ballot, |fence| highest(fence, self.ballot)),
        );
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
    ///
    /// Only a voter's frames get that far (design Section 10.1). An
    /// observer's or learner's link is peer access, never a vote, and its
    /// provenance still names a replica: everything below -- the ballot
    /// raised for a higher `NewLeader`, a promise answered with the won
    /// selection, a catch-up ask served, a floor or feature promise
    /// heard, the machines' own counting -- would take it for the voter
    /// that replica is. So a frame on any other role's provenance is
    /// dropped first, before it is even decoded.
    pub fn on_peer(
        &mut self,
        provenance: PeerProvenance,
        frame: Vec<u8>,
    ) -> Result<Outbound, DriveError> {
        if !provenance.is_voter() {
            return Ok(Outbound::default());
        }
        let before = self.ballot;
        let message = coord_consensus::ProtocolMessage::decode(&frame).ok();
        if let Some(coord_consensus::ProtocolMessage::NewLeader { ballot, .. }) = &message
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
        // A lagging voter's ask for this node's executed history is
        // answered from its durable rows, which the machine does not
        // hold (task-d08).
        if let Some(coord_consensus::ProtocolMessage::CatchUpRequest { ballot, after }) = &message {
            out.absorb(self.node.serve_catch_up(provenance.from(), *ballot, *after));
        }
        // A leader asking whether its ballot is still this voter's promise
        // is answered from the promise this voter holds, not by the
        // machine (task-d50). Only a promise of exactly that ballot, with
        // nothing higher in flight, is a yes; anything else is silence.
        if let Some(coord_consensus::ProtocolMessage::ReadConfirm { ballot, round }) = &message
            && provenance.from() == ballot.leader
            && let Machine::Follower(f) = self.node.machine()
            && f.ballots().promised() == *ballot
            && f.ballots().in_flight().is_none()
            && f.quorum().ballot() == *ballot
        {
            out.peer.push((
                PeerId {
                    replica: ballot.leader,
                    incarnation: ReplicaIncarnation::ZERO,
                },
                coord_consensus::ProtocolMessage::ReadConfirmed {
                    ballot: *ballot,
                    round: *round,
                    replica: self.provenance.from(),
                }
                .encode(),
            ));
        }
        if let Some(coord_consensus::ProtocolMessage::ReadConfirmed {
            ballot,
            round,
            replica,
        }) = &message
            && *replica == provenance.from()
            && let Some(leading) = self.leading()
            && let Machine::Leader(leader) = self.node.machine()
            && leader.config_quorum().is_voter(replica)
        {
            self.reads
                .on_confirmed(*replica, *ballot, *round, leading.slow_size);
        }
        // A peer's promise about a floor boundary is recorded from its own
        // link: the runtime's, like the checkpoint it names (task-d27).
        if let Some(coord_consensus::ProtocolMessage::FloorReadiness { readiness }) = &message {
            out.absorb(
                self.node
                    .hear_floor(provenance.from(), readiness, &self.ballot)?,
            );
        }
        // A peer's feature advertisement, prepare or activation is the
        // runtime's, like a floor promise (task-d99): the machines ignore
        // it, and it is taken only from the voter whose link it came on.
        if let Some(
            message @ (coord_consensus::ProtocolMessage::FeatureSupport { .. }
            | coord_consensus::ProtocolMessage::FeaturePrepare { .. }
            | coord_consensus::ProtocolMessage::FeaturePrepared { .. }
            | coord_consensus::ProtocolMessage::FeatureActivated { .. }
            | coord_consensus::ProtocolMessage::FeatureHeld { .. }),
        ) = &message
        {
            out.absorb(
                self.node
                    .hear_feature(provenance.from(), message, &self.ballot)?,
            );
            return Ok(out);
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

    /// What this voter says about features at start (task-d99): its
    /// advertisement to every peer, and what it holds to its leader.
    pub fn feature_start(&mut self) -> Result<Outbound, DriveError> {
        self.node.feature_start(&self.ballot)
    }

    /// A new peer-plane link with `peer` came up (task-d99): what it said
    /// about features before is forgotten, and it is told this voter's.
    pub fn peer_linked(&mut self, peer: ReplicaId) -> Result<Outbound, DriveError> {
        self.node.feature_linked(peer, &self.ballot)
    }

    /// The leader's paced re-send of feature prepares and activations
    /// (task-d99).
    pub fn resend_features(&mut self) -> Result<Outbound, DriveError> {
        self.node.feature_resend(&self.ballot)
    }

    /// An operator's request to activate `feature` (task-d99).
    pub fn activate_feature(
        &mut self,
        feature: coord_types::formats::Feature,
    ) -> Result<
        Result<(crate::feature::Activation, Outbound), crate::feature::ActivateRefusal>,
        DriveError,
    > {
        self.node.activate_feature(feature, &self.ballot)
    }

    /// The features as this voter sees them, if it takes part (task-d99).
    pub fn feature_status(&self) -> Option<crate::feature::FeatureStatus> {
        self.node.features().map(crate::feature::Features::status)
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
        // The identity the machine derives, taken from it (task-d60, step
        // 3), or derived the same way here where the machine refuses the
        // request before deriving it. A retry rebinds it: the same command
        // submitted again through another collector is owed to that one,
        // and a collector that went away is not owed anything.
        #[cfg(debug_assertions)]
        let expected = command_of(&admitted.frame);
        let early = if self.node.machine().takes_admission() {
            None
        } else {
            command_of(&admitted.frame)
        };
        // A submission sent as answered on the release alone (task-d100,
        // step A) marks its command as the machine derives it, so the
        // proposal or acknowledgement this step leads to already knows
        // not to send the frontend anything, and the command is derived
        // once, by the machine (#204 review). One refused before its
        // command is held leaves no mark.
        if frame.version == coord_collector::VERSION_ANSWER_ON_RELEASE {
            self.node.mark_next_admitted();
        }
        // Relayed to this voter alone as the leader (task-d102, step B):
        // marked the same way, proposed with its payload if this voter
        // leads, and refused as `NotLeading` if it doesn't.
        if frame.version == coord_collector::VERSION_RELAYED {
            self.node.mark_next_relayed();
        }
        let out = self.node.on_event(Event::Admitted(admitted), &self.ballot);
        let command = early.or_else(|| self.node.machine().admitted());
        #[cfg(debug_assertions)]
        debug_assert_eq!(command, expected, "the machine's command is the frame's");
        if let Some(command) = command {
            self.origins.remember(command, origin);
            self.node
                .stamp_submitted(command, std::time::Instant::now());
        }
        let mut out = out?;
        let asked = self.node.take_release_again();
        if !asked.is_empty() {
            let executed_by = self.node.machine().executed_through();
            for (command, key, admission) in asked {
                if self.release_again.len() >= ORIGINS {
                    self.release_again.pop_first();
                }
                self.release_again.insert(
                    command,
                    ReleaseAgain {
                        key,
                        admission,
                        executed_by,
                    },
                );
            }
            // A new entry may already be answerable.
            self.release_again_seen_at = None;
        }
        out.absorb(self.publish_releases_again());
        Ok(Ok(out))
    }

    /// Publish again the releases of marked commands a collector sent
    /// again (task-d100, step A), rebuilt from this node's retry record
    /// of each: the response, position, revision and result digest from
    /// the record, the epoch and ballot this voter leads at, and the
    /// admission digest the leader's table held. One whose record the
    /// projection does not hold yet waits for it, as a retry there does,
    /// until the projection has applied past where the command executed:
    /// a command refused at admission records nothing under its key, so
    /// its entry goes then (#204 review). None goes out once this voter
    /// no longer leads, and the collector then settles from its own
    /// node's record (task-d22).
    ///
    /// Called after every step that can move the projection; it pins a
    /// snapshot only when something waits and a projection commit has
    /// completed since it last looked. The
    /// fast-path label of a rebuilt release is `false`: nothing kept
    /// says which path decided it, and the collector compares only the
    /// position, revision, digest and bytes.
    pub fn publish_releases_again(&mut self) -> Outbound {
        let mut out = Outbound::default();
        if self.release_again.is_empty() {
            return out;
        }
        let Some(leading) = self.leading() else {
            self.release_again.clear();
            self.release_again_seen_at = None;
            return out;
        };
        // Only a projection commit changes what a snapshot shows, so one
        // is pinned again only once the completed frontier has moved
        // (#204 review), as the read barrier does (task-d58).
        let reader = self.node.applier().store().reader();
        let completed = reader.completed();
        if self.release_again_seen_at == Some(completed) {
            return out;
        }
        let Ok(gated) = reader.snapshot() else {
            return out;
        };
        self.release_again_lookups += 1;
        let applied = coord_storage::lowering::DurableMeta::read(gated.view())
            .map(|meta| meta.frontier.execution_position)
            .ok();
        let mut found = Vec::new();
        self.release_again.retain(|&command, waiting| {
            match coord_storage::retry::lookup(gated.view(), &waiting.key) {
                Ok(Some(record)) if record.command_id == command => {
                    found.push((command, waiting.admission, record));
                    false
                }
                // Applied past where it executed with no record of it:
                // refused at admission, and nothing here will release it
                // (#204 review). Its collector settles from its own
                // node, or is answered by its solicit's next step.
                Ok(None) => applied.is_none_or(|applied| applied < waiting.executed_by),
                // The key bound to another command: the domain executed
                // that one under it, and this one has nothing to release.
                Ok(Some(_)) => false,
                Err(_) => true,
            }
        });
        drop(gated);
        for (command, admission, record) in found {
            let Ok(established) = coord_core::capability::EstablishedResult::restore(
                coord_core::capability::EstablishedRecord {
                    command,
                    epoch: leading.ballot.epoch,
                    ballot: leading.ballot,
                    position: record.position,
                    result_digest: record.result_digest,
                    revision: record.revision,
                    fast_path: false,
                },
            ) else {
                continue;
            };
            let released = coord_core::capability::ReleasedResult::from_gate(
                established,
                record.response,
                false,
            )
            .with_admission(admission);
            if let Ok(frame) = coord_collector::release_frame(&released) {
                out.frontend.push(frame);
            }
        }
        self.release_again_seen_at = (!self.release_again.is_empty()).then_some(completed);
        // A release still waiting looks for its record in the next
        // commit, which is not held for a larger one (task-d86).
        if !self.release_again.is_empty() {
            self.node
                .applier_mut()
                .store_mut()
                .request_projection_commit();
        }
        out
    }

    /// What this voter leads, for the read barrier: `None` unless its
    /// machine is a leader that is leading (task-d50).
    fn leading(&self) -> Option<Leading> {
        match self.node.machine() {
            Machine::Leader(l) if l.is_leading() => {
                let quorum = l.config_quorum();
                Some(Leading {
                    ballot: quorum.ballot(),
                    index: l.read_index(),
                    slow_size: quorum.slow_size(),
                })
            }
            _ => None,
        }
    }

    /// A current read a collector sent this voter as the leader it
    /// follows, from `origin` (task-d50; design Section 6.3).
    ///
    /// Held, or refused at once; either way the answer goes back in
    /// [`Outbound::reads`]. A frame that does not decode as a read is
    /// dropped: the collector that sent it orders the read when its
    /// deadline passes.
    pub fn on_read(
        &mut self,
        frame: &coord_types::wire_v1::Frame,
        origin: Origin,
        now: MonotonicMillis,
    ) -> Outbound {
        let mut out = Outbound::default();
        let Ok(read) = decode_read(frame) else {
            return out;
        };
        let leading = self.leading();
        if let Err(refused) = self.reads.admit(origin, read, leading, now) {
            let (origin, answer) = *refused;
            push_answer(&mut out, origin, &answer);
            return out;
        }
        out.absorb(self.pump_reads(now));
        out
    }

    /// Move the reads this voter holds along (task-d50): start the
    /// confirmation round a newly arrived read needs, answer every read
    /// whose round confirmed, whose index executed and whose snapshot
    /// reached it, and refuse what can no longer be served.
    ///
    /// Called after anything that can move one of those: a read, a
    /// confirmation, an execution, a materialization, a turn.
    pub fn pump_reads(&mut self, now: MonotonicMillis) -> Outbound {
        // A materialization may also be what a release published again
        // waited for (task-d100, step A).
        let mut out = self.publish_releases_again();
        if self.reads.held() == 0 {
            self.reads_behind_at = None;
            return out;
        }
        let leading = self.leading();
        if let Some(l) = leading
            && let Some(round) = self.reads.round_to_start(l.ballot, l.slow_size, now)
        {
            let frame = coord_consensus::ProtocolMessage::ReadConfirm {
                ballot: l.ballot,
                round,
            }
            .encode();
            if let Machine::Leader(leader) = self.node.machine() {
                for voter in leader.config_quorum().voters() {
                    if *voter == l.ballot.leader {
                        continue;
                    }
                    out.peer.push((
                        PeerId {
                            replica: *voter,
                            incarnation: ReplicaIncarnation::ZERO,
                        },
                        frame.clone(),
                    ));
                }
            }
        }
        let (due, refused) = match self.node.machine() {
            Machine::Leader(leader) => self.reads.take_due(
                leading,
                now,
                |index| leader.executed_below(index),
                leader.executed_through(),
            ),
            Machine::Follower(_) => self.reads.take_due(
                None,
                now,
                |_| false,
                coord_types::ids::ExecutionPosition::ZERO,
            ),
        };
        for (origin, answer) in refused {
            push_answer(&mut out, origin, &answer);
        }
        // One view for every read due in this pump (task-d58): each is
        // due because everything below its index executed, so a view
        // taken now covers all of them, and pinning one per read was a
        // read transaction and a metadata read each. No view (the
        // projection is mid-commit, or quarantined) holds them all again,
        // as a view behind their position does.
        //
        // The view is the projection's snapshot with the journaled groups
        // in flight laid over it (`Applier::read_view`; #185, lead 17:43):
        // what a command planned there reads, and what a release has
        // already disclosed. So a read is answerable once its index is in
        // the journal, not once the projection has committed that far,
        // and it asks for no commit: the reads' asks were what cut the
        // leader's held commits short, 71% of them at 120 callers and 98%
        // at 30.
        //
        // A read can still be due before it is answerable: its index in
        // the group still open, or in one whose append is still out.
        // Between the two every pump would take a view only to find it
        // behind. So once one was behind, none is taken again until the
        // projection's completed frontier or the journaled position has
        // moved from what it read before that one: nothing else changes
        // what the view shows. Moved either way, as a reattached
        // projection's could, a new view is taken.
        let reaches = (
            self.node.applier().store().reader().completed(),
            self.node.applier().journaled_through(),
        );
        let gated = if due.is_empty() || self.reads_behind_at == Some(reaches) {
            None
        } else {
            self.node.applier().read_view().ok()
        };
        let mut again = Vec::new();
        for held in due {
            let retry_key = held.read.request.retry_key;
            let ballot = held.read.ballot;
            let evaluated = gated
                .as_ref()
                .map_or(Evaluated::NotYet, |gated| evaluate(gated, &held));
            match evaluated {
                Evaluated::Served(response) => {
                    self.reads.served(&held, now);
                    push_answer(
                        &mut out,
                        held.origin,
                        &ReadAnswerV1 {
                            retry_key,
                            ballot,
                            outcome: ReadOutcomeV1::Served { response },
                        },
                    );
                }
                Evaluated::NotYet => again.push(held),
                Evaluated::NotServable => {
                    self.reads.refused();
                    push_answer(
                        &mut out,
                        held.origin,
                        &crate::reads::refusal(retry_key, ballot, ReadRefusal::NotServable),
                    );
                }
            }
        }
        if gated.is_some() {
            self.reads.pinned(again.len());
            self.reads_behind_at = (!again.is_empty()).then_some(reaches);
        }
        for held in again.into_iter().rev() {
            self.reads.hold_again(held);
        }
        out
    }

    /// What the read barrier has done since boot (task-d50).
    pub fn read_counts(&self) -> crate::reads::ReadCounts {
        self.reads.counts
    }

    /// Marked commands whose release waits to be published again
    /// (task-d100, step A; diagnostic).
    pub fn releases_waiting(&self) -> usize {
        self.release_again.len()
    }

    /// How many projection snapshots the waiting releases have been
    /// looked up in (diagnostic): one per completed projection commit
    /// at most, while anything waits.
    pub fn release_lookups(&self) -> u64 {
        self.release_again_lookups
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
    /// content, counted to the most one ask is for (task-d109).
    pub fn missing_payloads(&self) -> usize {
        self.node.missing_payloads()
    }

    /// How many payload transfers a peer has answered this replica
    /// with.
    pub fn payloads_answered(&self) -> u64 {
        self.node.payloads_answered()
    }

    /// How many payload requests this voter's machine sent, the ones it
    /// made by itself included (a campaign's next batch).
    pub fn payload_asks(&self) -> u64 {
        self.node.payload_asks()
    }

    /// What this voter's re-sends did since boot (task-d49).
    pub fn resend_counts(&self) -> coord_consensus::ResendCounts {
        self.node.resend_counts()
    }

    /// Send this ballot's voters, again, the proposals they have not voted
    /// on. Only a leader sends anything (task-d07).
    pub fn resend_proposals(&mut self) -> Result<Outbound, DriveError> {
        self.node.resend_proposals(&self.ballot)
    }

    /// Ask this ballot's leader for the payloads this replica lacks.
    pub fn request_payloads(&mut self) -> Result<Outbound, DriveError> {
        let leader = self.ballot.leader;
        self.node.request_payloads(leader, &self.ballot)
    }

    /// Ask the voters that promised this voter's campaign for the report
    /// pages that have not arrived (task-d28).
    pub fn request_report_pages(&mut self) -> Result<Outbound, DriveError> {
        self.node.request_report_pages(&self.ballot)
    }

    /// Ask a peer for the commands it executed after this voter's
    /// frontier, when `pacer` says it is time (task-d08): the leader
    /// first, then the other voters in turn.
    pub fn catch_up(
        &mut self,
        pacer: &mut crate::catch_up::Pacer,
        now: std::time::Instant,
    ) -> Result<Outbound, DriveError> {
        let machine = self.node.machine();
        let standing = crate::catch_up::Standing {
            executed: machine.executed_through(),
            holds: machine.holds_unexecuted(),
            backlog: || machine.decided_unexecuted_held(),
            busy: machine.catching_up(),
            chain: machine.catch_up_chain_open(),
            pages: machine.catch_up_counts().0,
            leads: machine.leads(),
        };
        let voters: Vec<ReplicaId> = machine.identity().voters.iter().copied().collect();
        let me = self.ingress.replica();
        match pacer.due(now, standing, me, self.ballot.leader, &voters) {
            Some(donor) => self.node.request_catch_up(donor, &self.ballot),
            None => Ok(Outbound::default()),
        }
    }

    /// The command execution is waiting for a payload for, if any.
    pub fn awaiting(&self) -> Option<coord_types::CommandId> {
        self.node.awaiting()
    }

    /// Whether this replica currently leads its ballot.
    pub fn leads(&self) -> bool {
        self.node.machine().leads()
    }

    /// Apply every command whose turn has come as a staged group, journal
    /// it with what this voter's rounds left queued, and hand out what
    /// that released (task-d47, [`Node::stage`], [`Node::flush`]). A
    /// runtime lowering in groups calls this, then [`Voter::finish`],
    /// rather than [`Voter::execute`], once it has no more events ready.
    pub fn flush(&mut self) -> Result<Outbound, DriveError> {
        // The ready commands are staged first, so their batches and the
        // protocol's share one journal append; what that releases is
        // handed out here, before the group's projection commit, and the
        // group's results by [`Voter::finish`] after it.
        let mut out = self.node.stage(&self.ballot)?;
        out.absorb(self.node.flush(&self.ballot)?);
        Ok(out)
    }

    /// Materialize the execution group a [`Voter::flush`] staged and hand
    /// out its results (task-d47).
    pub fn finish(&mut self) -> Result<Outbound, DriveError> {
        self.node.finish(&self.ballot)
    }

    /// Take back what a pipelined store's materializer has committed and
    /// hand out what it released (task-d52, [`Node::settle`]). The runtime
    /// calls this when the materializer's waker fires.
    pub fn settle(&mut self) -> Result<Outbound, DriveError> {
        self.node.settle(&self.ballot)
    }

    /// Whether [`Voter::flush`] has anything to do: batches queued, a
    /// command to apply, a leader's proposals waiting for the turn's
    /// frame (task-d102), or a leader's commit frontier waiting for the
    /// turn's end (#242 follow-up).
    ///
    /// Batches queued behind a journal append that is out are not owed a
    /// flush (task-d54): nothing can be journaled until the append is
    /// back, and the appender's wake ([`Voter::settle`]) journals them
    /// then. Owed one, the runtime would flush in a loop that moves
    /// nothing.
    pub fn owes_flush(&self) -> bool {
        (self.queued() > 0 && !self.appending())
            || self.node.can_execute()
            || self.node.machine().frames_queued()
            || self.node.machine().frontier_owed()
    }

    /// Whether a journal append is out on the appender's thread, its
    /// outcome not yet taken back (task-d54).
    pub fn appending(&self) -> bool {
        self.node.applier().store().appending()
    }

    /// Batches this voter submitted and has not lowered yet.
    pub fn queued(&self) -> usize {
        self.node.applier().store().queued()
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
    request.command().ok().map(|(_, command)| command)
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

/// Queue `answer` for `origin`'s collector.
fn push_answer(out: &mut Outbound, origin: Origin, answer: &ReadAnswerV1) {
    if let Ok(frame) = read_answer_frame(answer) {
        out.reads.push((origin, frame));
    }
}
