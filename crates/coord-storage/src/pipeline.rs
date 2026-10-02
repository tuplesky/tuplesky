//! Where a pipelined store's projection commits run (task-d52).
//!
//! [`JournaledStore::hand_off`] lends a domain's engine to a
//! [`Materializer`] for one projection commit and takes it back with the
//! outcome. Two materializers are here: [`ThreadMaterializer`] runs the
//! commits on a thread of its own, which is what takes the projection's
//! transaction and its sync off the domain thread; [`ManualMaterializer`]
//! runs them only when a test says so, so a test can stop the pipeline at
//! each of its points -- a job handed over and not started, a commit
//! returned and not taken back -- and end the boot there.
//!
//! [`JournaledStore::hand_off`]: crate::journaled::JournaledStore::hand_off

use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use coord_store_api::engine::LocalEngine;
use coord_types::ids::DomainId;

use crate::journaled::{MaterializeDone, MaterializeJob, Materializer};

/// Called on the materializer's thread each time a commit finishes, so
/// the domain thread wakes and takes it back.
pub type Waker = Arc<dyn Fn() + Send + Sync>;

/// Projection commits on a thread of their own (task-d52).
///
/// The thread runs the jobs in the order they were submitted, one at a
/// time, and calls the waker after each. Dropping it ends the thread once
/// the jobs already submitted have run; an engine still out is dropped
/// there.
pub struct ThreadMaterializer<E: LocalEngine> {
    jobs: Option<Sender<MaterializeJob<E>>>,
    done: Receiver<MaterializeDone<E>>,
    out: usize,
    thread: Option<JoinHandle<()>>,
}

impl<E: LocalEngine> ThreadMaterializer<E> {
    /// Start the thread. `waker` is called after each commit.
    pub fn new(waker: Waker) -> std::io::Result<Self> {
        let (jobs, inbox) = channel::<MaterializeJob<E>>();
        let (outbox, done) = channel::<MaterializeDone<E>>();
        let thread = std::thread::Builder::new()
            .name("materializer".into())
            .spawn(move || {
                while let Ok(job) = inbox.recv() {
                    if outbox.send(job.run()).is_err() {
                        return;
                    }
                    waker();
                }
            })?;
        Ok(ThreadMaterializer {
            jobs: Some(jobs),
            done,
            out: 0,
            thread: Some(thread),
        })
    }
}

impl<E: LocalEngine> Materializer<E> for ThreadMaterializer<E> {
    fn submit(&mut self, job: MaterializeJob<E>) {
        let jobs = self.jobs.as_ref().expect("open until dropped");
        // The thread ends only when this sender is dropped or the done
        // channel is, and neither has happened while `self` lives.
        jobs.send(job).expect("the materializer thread is running");
        self.out += 1;
    }

    fn try_take(&mut self) -> Option<MaterializeDone<E>> {
        if self.out == 0 {
            return None;
        }
        match self.done.try_recv() {
            Ok(done) => {
                self.out -= 1;
                Some(done)
            }
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => {
                panic!("the materializer thread ended with a commit out")
            }
        }
    }

    fn take(&mut self) -> Option<MaterializeDone<E>> {
        if self.out == 0 {
            return None;
        }
        let done = self
            .done
            .recv()
            .expect("the materializer thread ended with a commit out");
        self.out -= 1;
        Some(done)
    }

    fn reclaim(&mut self) -> Vec<(DomainId, E)> {
        let mut engines = Vec::new();
        while let Some(done) = self.take() {
            engines.push(done.into_engine());
        }
        engines
    }
}

impl<E: LocalEngine> Drop for ThreadMaterializer<E> {
    fn drop(&mut self) {
        self.jobs = None;
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Projection commits run only when a test says so (task-d52).
///
/// The store owns the materializer and the test keeps the
/// [`ManualHandle`]: jobs wait, handed over and not started, until
/// [`ManualHandle::run`]; finished ones wait until the store takes them
/// back. A store that must wait ([`Materializer::take`]) runs the next job
/// itself, so draining never hangs.
pub struct ManualMaterializer<E: LocalEngine> {
    shared: Arc<Mutex<Manual<E>>>,
}

/// The test's side of a [`ManualMaterializer`].
pub struct ManualHandle<E: LocalEngine> {
    shared: Arc<Mutex<Manual<E>>>,
}

struct Manual<E: LocalEngine> {
    waiting: VecDeque<MaterializeJob<E>>,
    finished: VecDeque<MaterializeDone<E>>,
}

impl<E: LocalEngine> ManualMaterializer<E> {
    /// A materializer and the handle that runs its jobs.
    pub fn new() -> (Self, ManualHandle<E>) {
        let shared = Arc::new(Mutex::new(Manual {
            waiting: VecDeque::new(),
            finished: VecDeque::new(),
        }));
        (
            ManualMaterializer {
                shared: shared.clone(),
            },
            ManualHandle { shared },
        )
    }
}

impl<E: LocalEngine> ManualHandle<E> {
    /// Jobs handed over and not started.
    pub fn waiting(&self) -> usize {
        self.shared.lock().expect("not poisoned").waiting.len()
    }

    /// Jobs run and not taken back.
    pub fn finished(&self) -> usize {
        self.shared.lock().expect("not poisoned").finished.len()
    }

    /// Run the next waiting job; whether there was one. Its outcome waits
    /// for the store to take it back.
    pub fn run(&self) -> bool {
        let job = self
            .shared
            .lock()
            .expect("not poisoned")
            .waiting
            .pop_front();
        let Some(job) = job else {
            return false;
        };
        let done = job.run();
        self.shared
            .lock()
            .expect("not poisoned")
            .finished
            .push_back(done);
        true
    }
}

impl<E: LocalEngine> Materializer<E> for ManualMaterializer<E> {
    fn submit(&mut self, job: MaterializeJob<E>) {
        self.shared
            .lock()
            .expect("not poisoned")
            .waiting
            .push_back(job);
    }

    fn try_take(&mut self) -> Option<MaterializeDone<E>> {
        self.shared
            .lock()
            .expect("not poisoned")
            .finished
            .pop_front()
    }

    fn take(&mut self) -> Option<MaterializeDone<E>> {
        if let Some(done) = self.try_take() {
            return Some(done);
        }
        let job = self
            .shared
            .lock()
            .expect("not poisoned")
            .waiting
            .pop_front()?;
        Some(job.run())
    }

    /// What has run gives its engine back, outcome unseen; what has not
    /// gives it back unrun. Either is where a crash could end the boot.
    fn reclaim(&mut self) -> Vec<(DomainId, E)> {
        let mut shared = self.shared.lock().expect("not poisoned");
        let mut engines: Vec<(DomainId, E)> = shared
            .finished
            .drain(..)
            .map(MaterializeDone::into_engine)
            .collect();
        engines.extend(shared.waiting.drain(..).map(MaterializeJob::abandon));
        engines
    }
}
