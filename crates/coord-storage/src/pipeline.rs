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
//! Journal appends leave the domain thread the same way (task-d54):
//! [`JournaledStore::append`] lends the shared journal to an [`Appender`]
//! for one synced group append. [`ThreadAppender`] runs it on a thread of
//! its own; [`ManualAppender`] runs it when a test says so.
//!
//! [`JournaledStore::hand_off`]: crate::journaled::JournaledStore::hand_off
//! [`JournaledStore::append`]: crate::journaled::JournaledStore::append

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use coord_journal_api::engine::JournalEngine;
use coord_store_api::engine::LocalEngine;
use coord_types::ids::DomainId;

use crate::journaled::{
    AppendDone, AppendJob, Appender, MaterializeDone, MaterializeJob, Materializer,
};

/// Called on the materializer's or the appender's thread each time a job
/// finishes, so the domain thread wakes and takes it back.
pub type Waker = Arc<dyn Fn() + Send + Sync>;

/// How often, and for how long, the domain thread blocked taking a job
/// back from a pipeline thread (task-d54): a take that found the job
/// still running, and the time until it finished. A take that found it
/// done is not a wait.
///
/// Cumulative and shared, so the runtime reads it while the store owns
/// the pipeline thread. Only the threaded pipelines count; the manual
/// ones a test drives do not wait.
#[derive(Debug, Default)]
pub struct Waits {
    count: AtomicU64,
    nanos: AtomicU64,
}

impl Waits {
    /// The waits so far, and their total time.
    pub fn read(&self) -> (u64, Duration) {
        (
            self.count.load(Ordering::Relaxed),
            Duration::from_nanos(self.nanos.load(Ordering::Relaxed)),
        )
    }

    fn record(&self, waited: Duration) {
        self.count.fetch_add(1, Ordering::Relaxed);
        let nanos = u64::try_from(waited.as_nanos()).unwrap_or(u64::MAX);
        self.nanos.fetch_add(nanos, Ordering::Relaxed);
    }
}

/// Take the next job back from `done`, blocking if it is still running,
/// and record the block in `waits`.
fn take_counting<T>(done: &Receiver<T>, waits: &Waits) -> Result<T, TryRecvError> {
    match done.try_recv() {
        Ok(done) => Ok(done),
        Err(TryRecvError::Empty) => {
            let started = Instant::now();
            let taken = done.recv().map_err(|_| TryRecvError::Disconnected);
            waits.record(started.elapsed());
            taken
        }
        Err(e) => Err(e),
    }
}

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
    waits: Arc<Waits>,
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
            waits: Arc::default(),
        })
    }

    /// Where the domain thread's waits for a commit are counted.
    pub fn waits(&self) -> Arc<Waits> {
        Arc::clone(&self.waits)
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
        let done = take_counting(&self.done, &self.waits)
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

/// Journal appends on a thread of their own (task-d54).
///
/// The store has at most one append out, and the thread calls the waker
/// after each. Dropping it ends the thread once a job already submitted
/// has run; a journal still out is dropped there.
pub struct ThreadAppender<J: JournalEngine + Send + 'static> {
    jobs: Option<Sender<AppendJob<J>>>,
    done: Receiver<AppendDone<J>>,
    out: usize,
    thread: Option<JoinHandle<()>>,
    waits: Arc<Waits>,
}

impl<J: JournalEngine + Send + 'static> ThreadAppender<J> {
    /// Start the thread. `waker` is called after each append.
    pub fn new(waker: Waker) -> std::io::Result<Self> {
        let (jobs, inbox) = channel::<AppendJob<J>>();
        let (outbox, done) = channel::<AppendDone<J>>();
        let thread = std::thread::Builder::new()
            .name("appender".into())
            .spawn(move || {
                while let Ok(job) = inbox.recv() {
                    if outbox.send(job.run()).is_err() {
                        return;
                    }
                    waker();
                }
            })?;
        Ok(ThreadAppender {
            jobs: Some(jobs),
            done,
            out: 0,
            thread: Some(thread),
            waits: Arc::default(),
        })
    }

    /// Where the domain thread's waits for an append are counted.
    pub fn waits(&self) -> Arc<Waits> {
        Arc::clone(&self.waits)
    }
}

impl<J: JournalEngine + Send + 'static> Appender<J> for ThreadAppender<J> {
    fn submit(&mut self, job: AppendJob<J>) {
        let jobs = self.jobs.as_ref().expect("open until dropped");
        // The thread ends only when this sender is dropped or the done
        // channel is, and neither has happened while `self` lives.
        jobs.send(job).expect("the appender thread is running");
        self.out += 1;
    }

    fn try_take(&mut self) -> Option<AppendDone<J>> {
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
                panic!("the appender thread ended with an append out")
            }
        }
    }

    fn take(&mut self) -> Option<AppendDone<J>> {
        if self.out == 0 {
            return None;
        }
        let done = take_counting(&self.done, &self.waits)
            .expect("the appender thread ended with an append out");
        self.out -= 1;
        Some(done)
    }

    fn reclaim(&mut self) -> Option<J> {
        self.take().map(AppendDone::into_journal)
    }
}

impl<J: JournalEngine + Send + 'static> Drop for ThreadAppender<J> {
    fn drop(&mut self) {
        self.jobs = None;
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Journal appends run only when a test says so (task-d54).
///
/// The store owns the appender and the test keeps the
/// [`ManualAppendHandle`]: an append waits, lent and not started, until
/// [`ManualAppendHandle::run`]; a finished one waits until the store takes
/// it back. A store that must wait ([`Appender::take`]) runs it itself, so
/// waiting never hangs.
pub struct ManualAppender<J: JournalEngine> {
    shared: Arc<Mutex<ManualAppends<J>>>,
}

/// The test's side of a [`ManualAppender`].
pub struct ManualAppendHandle<J: JournalEngine> {
    shared: Arc<Mutex<ManualAppends<J>>>,
}

struct ManualAppends<J: JournalEngine> {
    waiting: Option<AppendJob<J>>,
    finished: Option<AppendDone<J>>,
}

impl<J: JournalEngine + Send> ManualAppender<J> {
    /// An appender and the handle that runs its appends.
    pub fn new() -> (Self, ManualAppendHandle<J>) {
        let shared = Arc::new(Mutex::new(ManualAppends {
            waiting: None,
            finished: None,
        }));
        (
            ManualAppender {
                shared: shared.clone(),
            },
            ManualAppendHandle { shared },
        )
    }
}

impl<J: JournalEngine> ManualAppendHandle<J> {
    /// Whether an append is lent and not started.
    pub fn waiting(&self) -> bool {
        self.shared.lock().expect("not poisoned").waiting.is_some()
    }

    /// Whether an append has run and not been taken back.
    pub fn finished(&self) -> bool {
        self.shared.lock().expect("not poisoned").finished.is_some()
    }

    /// Run the waiting append; whether there was one. Its outcome waits
    /// for the store to take it back.
    pub fn run(&self) -> bool {
        let job = self.shared.lock().expect("not poisoned").waiting.take();
        let Some(job) = job else {
            return false;
        };
        let done = job.run();
        self.shared.lock().expect("not poisoned").finished = Some(done);
        true
    }
}

impl<J: JournalEngine + Send> Appender<J> for ManualAppender<J> {
    fn submit(&mut self, job: AppendJob<J>) {
        let mut shared = self.shared.lock().expect("not poisoned");
        assert!(
            shared.waiting.is_none() && shared.finished.is_none(),
            "a store has one append out at a time"
        );
        shared.waiting = Some(job);
    }

    fn try_take(&mut self) -> Option<AppendDone<J>> {
        self.shared.lock().expect("not poisoned").finished.take()
    }

    fn take(&mut self) -> Option<AppendDone<J>> {
        if let Some(done) = self.try_take() {
            return Some(done);
        }
        let job = self.shared.lock().expect("not poisoned").waiting.take()?;
        Some(job.run())
    }

    /// What has run gives the journal back, outcome unseen; what has not
    /// gives it back unrun. Either is where a crash could end the boot.
    fn reclaim(&mut self) -> Option<J> {
        let mut shared = self.shared.lock().expect("not poisoned");
        if let Some(done) = shared.finished.take() {
            return Some(done.into_journal());
        }
        shared.waiting.take().map(AppendJob::abandon)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_take_that_blocks_is_counted_and_one_that_finds_the_job_done_is_not() {
        let waits = Waits::default();
        let (outbox, done) = channel::<u8>();
        outbox.send(1).unwrap();
        assert_eq!(take_counting(&done, &waits), Ok(1));
        assert_eq!(waits.read(), (0, Duration::ZERO));

        let late = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            outbox.send(2).unwrap();
        });
        assert_eq!(take_counting(&done, &waits), Ok(2));
        let (count, time) = waits.read();
        assert_eq!(count, 1);
        assert!(time >= Duration::from_millis(15), "waited {time:?}");
        late.join().unwrap();
        assert_eq!(
            take_counting(&done, &waits),
            Err(TryRecvError::Disconnected)
        );
    }
}
