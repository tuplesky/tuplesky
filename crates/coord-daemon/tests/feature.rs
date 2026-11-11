//! task-d99 acceptance for the runtime's feature driver: voters over model
//! stores, a network that delivers or drops, and batches that are durable
//! only when the test says so.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use coord_checkpoint::feature::{FeatureSupportV1, admit, published_activation};
use coord_consensus::ProtocolMessage;
use coord_consensus::quorum::EpochVoters;
use coord_core::effect::{BarrierId, BootId, StoreUpdate};
use coord_daemon::feature::{ActivateRefusal, Activation, FeatureStep, Features, Standing};
use coord_store_api::engine::{LocalEngine, SnapshotSource, WriteTxn};
use coord_store_testkit::model::ModelEngine;
use coord_types::formats::Feature;
use coord_types::ids::{
    Ballot, ClusterId, ConfigurationEpoch, DomainId, ReplicaId, ReplicaIncarnation,
};

const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);
const EP: Feature = Feature::EarlyProposal;

fn replica(n: u8) -> ReplicaId {
    ReplicaId([n; 16])
}

fn epoch(n: u64) -> ConfigurationEpoch {
    ConfigurationEpoch::new(n).unwrap()
}

fn ballot(number: u64, leader: u8) -> Ballot {
    Ballot {
        epoch: epoch(3),
        number,
        leader: replica(leader),
    }
}

/// One voter: its driver, if its build has one -- a build from before
/// task-d99 drops every feature message -- and its store, with the batches
/// journaled and not yet durable.
struct Voter {
    features: Option<Features>,
    engine: ModelEngine,
    pending: VecDeque<(BarrierId, Vec<StoreUpdate>)>,
    durable: BTreeSet<BarrierId>,
    failed: BTreeSet<BarrierId>,
    standing: Standing,
    boot: u8,
    /// A build from before task-d99.
    old: bool,
}

struct Domain {
    voters: BTreeMap<ReplicaId, Voter>,
    epoch: ConfigurationEpoch,
    members: BTreeSet<ReplicaId>,
    net: VecDeque<(ReplicaId, ReplicaId, ProtocolMessage)>,
    /// Links that drop everything, as (from, to).
    cut: BTreeSet<(ReplicaId, ReplicaId)>,
    /// Links that drop only activations.
    mute_activated: BTreeSet<(ReplicaId, ReplicaId)>,
    /// Voters whose batches are not made durable.
    hold: BTreeSet<ReplicaId>,
    /// Voters whose next batch fails, as a store fenced at a higher
    /// promise refuses one.
    fail_next: BTreeSet<ReplicaId>,
}

fn open(
    me: ReplicaId,
    members: &BTreeSet<ReplicaId>,
    e: ConfigurationEpoch,
    engine: &ModelEngine,
    boot: u8,
) -> Features {
    let voters = EpochVoters::new(e, members.clone()).unwrap();
    let view = engine.reader().snapshot().unwrap();
    Features::open(
        CLUSTER,
        DOMAIN,
        me,
        voters,
        &view,
        ReplicaIncarnation::new(1).unwrap(),
        BootId([boot; 16]),
    )
    .unwrap()
}

impl Domain {
    /// `count` voters at epoch 3 led by voter 1; `old` are builds from
    /// before task-d99.
    fn new(count: u8, old: &[u8]) -> Self {
        let members: BTreeSet<ReplicaId> = (1..=count).map(replica).collect();
        let mut voters = BTreeMap::new();
        for n in 1..=count {
            let engine = ModelEngine::new();
            let features =
                (!old.contains(&n)).then(|| open(replica(n), &members, epoch(3), &engine, 1));
            voters.insert(
                replica(n),
                Voter {
                    features,
                    engine,
                    pending: VecDeque::new(),
                    durable: BTreeSet::new(),
                    failed: BTreeSet::new(),
                    standing: Standing {
                        ballot: ballot(1, 1),
                        leading: n == 1,
                    },
                    boot: 1,
                    old: old.contains(&n),
                },
            );
        }
        let mut domain = Domain {
            voters,
            epoch: epoch(3),
            members,
            net: VecDeque::new(),
            cut: BTreeSet::new(),
            mute_activated: BTreeSet::new(),
            hold: BTreeSet::new(),
            fail_next: BTreeSet::new(),
        };
        for n in 1..=count {
            domain.start(replica(n));
        }
        domain.run();
        domain
    }

    fn voter(&mut self, n: u8) -> &mut Voter {
        self.voters.get_mut(&replica(n)).unwrap()
    }

    fn features(&self, n: u8) -> &Features {
        self.voters[&replica(n)].features.as_ref().unwrap()
    }

    fn take(&mut self, from: ReplicaId, step: FeatureStep) {
        let voter = self.voters.get_mut(&from).unwrap();
        voter.pending.extend(step.batches);
        for (to, message) in step.sends {
            self.net.push_back((from, to, message));
        }
    }

    fn start(&mut self, n: ReplicaId) {
        let voter = self.voters.get_mut(&n).unwrap();
        let standing = voter.standing;
        if let Some(features) = voter.features.as_mut() {
            let step = features.start(standing);
            self.take(n, step);
        }
    }

    /// Make every journaled batch of `n` durable and readable.
    fn sync(&mut self, n: ReplicaId) {
        if self.hold.contains(&n) {
            return;
        }
        let voter = self.voters.get_mut(&n).unwrap();
        while let Some((barrier, updates)) = voter.pending.pop_front() {
            if self.fail_next.remove(&n) {
                voter.failed.insert(barrier);
                continue;
            }
            let mut tx = voter.engine.begin_write().unwrap();
            for update in &updates {
                match &update.value {
                    Some(v) => tx.put(update.collection, &update.key, v).unwrap(),
                    None => tx.delete(update.collection, &update.key).unwrap(),
                }
            }
            tx.commit_durable().unwrap();
            voter.durable.insert(barrier);
        }
        let durable = voter.durable.clone();
        let failed = voter.failed.clone();
        let standing = voter.standing;
        if let Some(features) = voter.features.as_mut() {
            let mut step = features.observe(standing);
            step.absorb(features.settle(|b| durable.contains(b), |b| failed.contains(b)));
            self.take(n, step);
        }
    }

    /// Deliver and sync until nothing moves.
    fn run(&mut self) {
        for _ in 0..10_000 {
            let ids: Vec<ReplicaId> = self.voters.keys().copied().collect();
            for id in &ids {
                self.sync(*id);
            }
            let Some((from, to, message)) = self.net.pop_front() else {
                if self
                    .voters
                    .iter()
                    .all(|(id, v)| v.pending.is_empty() || self.hold.contains(id))
                {
                    return;
                }
                continue;
            };
            if self.cut.contains(&(from, to))
                || (self.mute_activated.contains(&(from, to))
                    && matches!(message, ProtocolMessage::FeatureActivated { .. }))
            {
                continue;
            }
            let Some(voter) = self.voters.get_mut(&to) else {
                continue;
            };
            let standing = voter.standing;
            if let Some(features) = voter.features.as_mut() {
                let mut step = features.observe(standing);
                step.absorb(features.hear(from, &message));
                self.take(to, step);
            }
        }
        panic!("the domain did not settle");
    }

    fn activate(&mut self, n: u8, feature: Feature) -> Result<Activation, ActivateRefusal> {
        let id = replica(n);
        let voter = self.voters.get_mut(&id).unwrap();
        let standing = voter.standing;
        let features = voter.features.as_mut().unwrap();
        let observed = features.observe(standing);
        let result = features.activate(feature);
        self.take(id, observed);
        let (activation, step) = result?;
        self.take(id, step);
        self.run();
        Ok(activation)
    }

    fn resend(&mut self, n: u8) {
        let id = replica(n);
        let voter = self.voters.get_mut(&id).unwrap();
        let standing = voter.standing;
        let features = voter.features.as_mut().unwrap();
        let mut step = features.observe(standing);
        step.absorb(features.resend());
        self.take(id, step);
        self.run();
    }

    /// A new link between `a` and `b`: each side forgets what the other
    /// said and tells it its own.
    fn link(&mut self, a: u8, b: u8) {
        for (me, peer) in [(a, b), (b, a)] {
            let id = replica(me);
            let voter = self.voters.get_mut(&id).unwrap();
            let standing = voter.standing;
            if let Some(features) = voter.features.as_mut() {
                let mut step = features.observe(standing);
                step.absorb(features.linked(replica(peer)));
                self.take(id, step);
            }
        }
        self.run();
    }

    /// Restart `n` from its store: a new boot, new links to every peer.
    fn restart(&mut self, n: u8) {
        let id = replica(n);
        let members = self.members.clone();
        let e = self.epoch;
        let voter = self.voters.get_mut(&id).unwrap();
        voter.pending.clear();
        voter.boot += 1;
        if voter.features.is_some() || !voter.old {
            voter.features = Some(open(id, &members, e, &voter.engine, voter.boot));
        }
        self.start(id);
        let peers: Vec<u8> = self
            .members
            .iter()
            .map(|r| r.0[0])
            .filter(|p| *p != n)
            .collect();
        for peer in peers {
            self.link(n, peer);
        }
    }

    /// Every voter moves to `ballot`, led by `leader`.
    fn elect(&mut self, number: u64, leader: u8) {
        for (id, voter) in &mut self.voters {
            voter.standing = Standing {
                ballot: ballot(number, leader),
                leading: *id == replica(leader),
            };
        }
        let ids: Vec<ReplicaId> = self.voters.keys().copied().collect();
        for id in ids {
            let voter = self.voters.get_mut(&id).unwrap();
            let standing = voter.standing;
            if let Some(features) = voter.features.as_mut() {
                let step = features.observe(standing);
                self.take(id, step);
            }
        }
        self.run();
    }

    fn admitted(&self, n: u8) -> BTreeSet<Feature> {
        let view = self.voters[&replica(n)].engine.reader().snapshot().unwrap();
        admit(&view).unwrap()
    }

    fn record(&self, n: u8) -> Option<coord_checkpoint::feature::ActiveFeaturesV1> {
        let view = self.voters[&replica(n)].engine.reader().snapshot().unwrap();
        published_activation(&view).unwrap()
    }
}

fn set(features: &[Feature]) -> BTreeSet<Feature> {
    features.iter().copied().collect()
}

/// The whole path at three voters: advertise, prepare, activate, install,
/// hold. Every store ends with one record and the reporters of the voters
/// that prepared, and every follower is in the leader's window.
#[test]
fn three_voters_activate_and_every_follower_holds() {
    let mut d = Domain::new(3, &[]);
    assert_eq!(d.activate(1, EP), Ok(Activation::Started));
    let record = d.record(1).expect("the leader holds the activation");
    for n in 1..=3 {
        assert_eq!(d.admitted(n), set(&[EP]), "voter {n} does not hold it");
        assert_eq!(
            d.record(n).as_ref(),
            Some(&record),
            "voter {n}'s record differs"
        );
        assert_eq!(
            d.features(n).active(),
            set(&[EP]),
            "voter {n}'s switch is off"
        );
    }
    assert_eq!(record.reporters, (1..=3).map(replica).collect::<Vec<_>>());
    assert_eq!(
        d.features(1).holders(EP),
        [replica(2), replica(3)].into_iter().collect()
    );
    // Again: idempotent, and says so.
    assert_eq!(d.activate(1, EP), Ok(Activation::AlreadyActive));
    let status = d.features(1).status();
    assert!(status.leading);
    assert_eq!(status.active, set(&[EP]));
    assert!(status.voters.iter().all(|v| v.held == Some(set(&[EP]))));
}

/// Activation is refused without every configured voter's current
/// advertisement, and names the voters missing; a follower refuses it and
/// names the leader.
#[test]
fn activation_is_refused_without_every_voter_and_by_a_follower() {
    let mut d = Domain::new(3, &[3]);
    assert_eq!(
        d.activate(1, EP),
        Err(ActivateRefusal::Missing {
            feature: EP,
            voters: vec![replica(3)],
        })
    );
    assert_eq!(d.record(1), None, "a refused activation wrote nothing");
    assert_eq!(
        d.activate(2, EP),
        Err(ActivateRefusal::NotLeader { leader: replica(1) })
    );
    assert_eq!(
        d.activate(1, Feature::EarlyProposalPrepared),
        Err(ActivateRefusal::NotActivatable(
            Feature::EarlyProposalPrepared
        ))
    );
    assert_eq!(
        d.activate(1, Feature::CheckpointFloor),
        Err(ActivateRefusal::NotActivatable(Feature::CheckpointFloor))
    );
}

/// A voter from before task-d99 drops every feature message: it never
/// advertises, so nothing is activated over it, and it is never in the
/// window, even once a later build replaces it and the domain activates.
#[test]
fn a_voter_from_before_the_driver_never_enters_the_window() {
    let mut d = Domain::new(5, &[4]);
    assert!(matches!(
        d.activate(1, EP),
        Err(ActivateRefusal::Missing { .. })
    ));
    assert!(!d.features(1).holders(EP).contains(&replica(4)));
    for n in 1..=5 {
        assert!(d.admitted(n).is_empty());
    }
}

/// A restarted follower is out of the window until it says it holds the
/// activation again, and it does say it again, at start.
#[test]
fn a_restarted_follower_is_out_of_the_window_until_it_says_it_again() {
    let mut d = Domain::new(3, &[]);
    d.activate(1, EP).unwrap();
    assert!(d.features(1).holders(EP).contains(&replica(3)));
    // The new links come up first; its `FeatureHeld` has not arrived.
    d.cut.insert((replica(3), replica(1)));
    d.restart(3);
    assert!(
        !d.features(1).holders(EP).contains(&replica(3)),
        "a restarted follower stayed in the window on its old link's word"
    );
    // The activation it holds survives the restart.
    assert_eq!(d.admitted(3), set(&[EP]));
    assert_eq!(d.features(3).active(), set(&[EP]));
    // Once its word arrives, it is back.
    d.cut.clear();
    d.link(3, 1);
    assert!(d.features(1).holders(EP).contains(&replica(3)));
}

/// A node's switch turns on once its own activation row is durable, with
/// no restart; until then it is off.
#[test]
fn a_nodes_switch_turns_on_when_its_own_row_is_durable() {
    let mut d = Domain::new(3, &[]);
    d.mute_activated.insert((replica(1), replica(2)));
    d.activate(1, EP).unwrap();
    assert_eq!(d.features(1).active(), set(&[EP]));
    assert!(d.features(2).active().is_empty());
    // Voter 2 installs the record, and its batch is not durable yet.
    d.mute_activated.clear();
    d.hold.insert(replica(2));
    d.resend(1);
    assert!(!d.voter(2).pending.is_empty(), "the install is journaled");
    assert!(
        d.features(2).active().is_empty(),
        "the switch turned on before its row was durable"
    );
    assert!(!d.features(1).holders(EP).contains(&replica(2)));
    // Durable: on, and the leader is told.
    d.hold.clear();
    d.run();
    assert_eq!(
        d.features(2).active(),
        set(&[EP]),
        "the switch waited for a restart"
    );
    assert!(d.features(1).holders(EP).contains(&replica(2)));
}

/// A leader change between the activation and the last follower's install
/// still finishes it, with the old leader's record and reporters: the new
/// leader never heard the activation, installs it from a follower's
/// `FeatureHeld`, and re-sends it.
#[test]
fn a_new_leader_finishes_an_activation_with_the_old_leaders_record() {
    let mut d = Domain::new(3, &[]);
    // Voter 3 hears the prepare, and never the activation.
    d.mute_activated.insert((replica(1), replica(3)));
    d.activate(1, EP).unwrap();
    let record = d.record(1).expect("the old leader activated");
    assert_eq!(d.record(2).as_ref(), Some(&record));
    assert!(d.admitted(3).is_empty(), "voter 3 heard the activation");
    // Voter 3 is elected. Voter 2 tells it what it holds.
    d.mute_activated.clear();
    d.elect(2, 3);
    assert_eq!(
        d.record(3).as_ref(),
        Some(&record),
        "the new leader did not install the old leader's record unchanged"
    );
    assert_eq!(d.features(3).active(), set(&[EP]));
    // Its re-send reaches the old leader, which says it holds it.
    d.resend(3);
    assert_eq!(
        d.features(3).holders(EP),
        [replica(1), replica(2)].into_iter().collect()
    );
    for n in 1..=3 {
        assert_eq!(d.record(n).as_ref(), Some(&record), "voter {n}");
    }
    assert_eq!(record.reporters, (1..=3).map(replica).collect::<Vec<_>>());
}

/// A voter that joins after an activation holds no feature rows. Its
/// promotion waits for its advertisement of every active feature; on a
/// build with it, the leader's re-send prepares and activates it alone,
/// with the domain's record unchanged.
#[test]
fn a_voter_added_after_an_activation_is_prepared_and_activated_alone() {
    let mut d = Domain::new(3, &[]);
    d.activate(1, EP).unwrap();
    let record = d.record(1).unwrap();
    // Voter 4 joins at epoch 4, on a fresh store: an image carries no
    // feature rows.
    let members: BTreeSet<ReplicaId> = (1..=4).map(replica).collect();
    d.members = members.clone();
    d.epoch = epoch(4);
    d.voters.insert(
        replica(4),
        Voter {
            features: None,
            engine: ModelEngine::new(),
            pending: VecDeque::new(),
            durable: BTreeSet::new(),
            failed: BTreeSet::new(),
            standing: Standing {
                ballot: ballot(1, 1),
                leading: false,
            },
            boot: 1,
            old: false,
        },
    );
    for n in 1..=4 {
        d.restart(n);
    }
    // Its promotion waits for an advertisement of every active feature,
    // which this build makes (a build without it is refused by
    // `promotion_missing`, coord-checkpoint's tests).
    let active = d.features(1).active();
    let advertised = d.features(1).status().voters[3].advertised.clone();
    assert_eq!(
        coord_checkpoint::feature::promotion_missing(&active, &advertised.unwrap_or_default()),
        Vec::<Feature>::new(),
        "this build advertises the feature"
    );
    // On this build: the re-send prepares and activates it alone.
    d.resend(1);
    let joined = d.record(4).expect("the joiner holds the activation");
    assert_eq!(joined.features, record.features);
    assert_eq!(joined.reporters, record.reporters, "the reporters changed");
    assert_eq!(
        d.record(1).as_ref(),
        Some(&record),
        "the leader's record changed"
    );
    assert!(d.features(1).holders(EP).contains(&replica(4)));
    assert_eq!(d.admitted(4), set(&[EP]));
    let view = d.voters[&replica(4)].engine.reader().snapshot().unwrap();
    let own = coord_checkpoint::feature::own_support(&view, &replica(4))
        .unwrap()
        .unwrap();
    assert_eq!(own.configuration, epoch(4));
    let _: FeatureSupportV1 = own;
}

/// A failed feature batch -- a store fenced at a higher promise refuses
/// one around an election -- rolls the driver back to its durable rows, not
/// to a view that may lag them: the follower's prepare stands, and the
/// leader's next re-send gets it holding the activation, with no restart.
#[test]
fn a_failed_install_is_installed_again_from_the_durable_rows() {
    let mut d = Domain::new(3, &[]);
    d.mute_activated.insert((replica(1), replica(2)));
    d.activate(1, EP).unwrap();
    d.mute_activated.clear();
    // Voter 2's install fails.
    d.fail_next.insert(replica(2));
    d.resend(1);
    assert!(d.fail_next.is_empty(), "no batch failed");
    assert!(d.features(2).active().is_empty());
    assert!(!d.features(1).holders(EP).contains(&replica(2)));
    // The next re-send installs it.
    d.resend(1);
    assert_eq!(d.features(2).active(), set(&[EP]));
    assert_eq!(d.record(2), d.record(1));
    assert!(d.features(1).holders(EP).contains(&replica(2)));
}

/// A voter whose store holds the activation answers the leader's re-send
/// with `FeatureHeld` even with no support row of its own, as a store
/// activated before this driver existed is.
#[test]
fn a_voter_holding_the_record_answers_even_without_its_support_row() {
    let mut d = Domain::new(3, &[]);
    d.activate(1, EP).unwrap();
    let record = d.record(1).unwrap();
    // Voter 3's store: the record alone.
    let mut engine = ModelEngine::new();
    let mut tx = engine.begin_write().unwrap();
    let update = record.update().unwrap();
    tx.put(
        update.collection,
        &update.key,
        update.value.as_ref().unwrap(),
    )
    .unwrap();
    tx.commit_durable().unwrap();
    d.voter(3).engine = engine;
    d.restart(3);
    d.resend(1);
    assert!(d.features(1).holders(EP).contains(&replica(3)));
}

/// task-d102: the relay (`BatchedRound`) is activated only once
/// `AnswerOnRelease` is active, since a relayed command is answered on
/// the leader's release alone. Asked first, it is refused and nothing is
/// written; asked after, it activates as any feature does.
#[test]
fn the_relay_is_activated_only_after_answer_on_release() {
    let mut d = Domain::new(3, &[]);
    assert_eq!(
        d.activate(1, Feature::BatchedRound),
        Err(ActivateRefusal::Requires {
            feature: Feature::BatchedRound,
            before: Feature::AnswerOnRelease,
        })
    );
    assert_eq!(d.record(1), None, "a refused activation wrote nothing");
    assert_eq!(
        d.activate(1, Feature::AnswerOnRelease),
        Ok(Activation::Started)
    );
    assert_eq!(
        d.activate(1, Feature::BatchedRound),
        Ok(Activation::Started)
    );
    for n in 1..=3 {
        assert_eq!(
            d.features(n).active(),
            set(&[Feature::AnswerOnRelease, Feature::BatchedRound]),
            "voter {n}'s switches"
        );
    }
}

/// A store whose activation record names a feature this build has never
/// heard of is refused at open (Codex on #98), not read as nothing active:
/// `Features::active` turns a record whose features do not parse into an
/// empty set, so a voter opened over it would run without what its cluster
/// activated. The same record with only features this build supports opens.
#[test]
fn an_activation_record_this_build_cannot_run_is_refused_at_open() {
    let members: BTreeSet<ReplicaId> = (1..=3).map(replica).collect();
    let voters = EpochVoters::new(epoch(3), members).unwrap();
    for (features, opens) in [(vec![0x7fff], false), (vec![EP.id()], true)] {
        let record = coord_checkpoint::feature::ActiveFeaturesV1 {
            cluster: CLUSTER,
            domain: DOMAIN,
            configuration: epoch(3),
            features: features.clone(),
            reporters: vec![replica(1), replica(2), replica(3)],
        };
        let mut engine = ModelEngine::new();
        let mut tx = engine.begin_write().unwrap();
        let update = record.update().unwrap();
        tx.put(
            update.collection,
            &update.key,
            update.value.as_ref().unwrap(),
        )
        .unwrap();
        tx.commit_durable().unwrap();
        let view = engine.reader().snapshot().unwrap();
        let opened = Features::open(
            CLUSTER,
            DOMAIN,
            replica(1),
            voters.clone(),
            &view,
            ReplicaIncarnation::new(1).unwrap(),
            BootId([1; 16]),
        );
        match (opened, opens) {
            (Ok(features), true) => assert_eq!(features.active(), set(&[EP])),
            (Err(e), false) => assert!(e.contains("unknown active feature"), "{e}"),
            (Ok(features), false) => panic!(
                "{features:?}: opened over a record it cannot run, active {:?}",
                features.active()
            ),
            (Err(e), true) => panic!("{features:?}: refused a record it can run: {e}"),
        }
    }
}
