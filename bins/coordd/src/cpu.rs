//! CPU time, for the cost a snapshot reports (task-d54), and how the
//! domain loop's thread was scheduled (task-d55).
//!
//! The domain loop's busy time counts a sync it waits for as work. CPU
//! time does not, so the two side by side say how much of a command's
//! cost is the loop computing and how much is it waiting on the disk;
//! with appends off the loop's thread the difference is what moved.
//!
//! Read from `/proc`, which needs no unsafe code: a thread's
//! `schedstat` gives its time on a CPU in nanoseconds, and the process's
//! `stat` its user and system time in clock ticks, which the kernel
//! reports to userspace at a fixed 100 a second. A publication's steps
//! (task-d66) are timed by the thread's CPU clock instead, which `rustix`
//! reads without unsafe code either. Elsewhere there is no reading, and
//! the snapshot says so rather than reporting zero.

use std::time::Duration;

/// Clock ticks a second in `/proc/<pid>/stat` (`USER_HZ`, fixed by the
/// kernel's userspace ABI).
const USER_HZ: u64 = 100;

/// CPU time the calling thread has used since it started.
pub fn this_thread() -> Option<Duration> {
    this_thread_schedstat().map(|(on_cpu, _)| on_cpu)
}

/// The calling thread's time on a CPU and waiting for one since it
/// started: the clock a publication's steps are timed by (task-d66).
///
/// The time on a CPU is the thread's CPU clock, not `schedstat`'s:
/// that is the scheduler's account as of its last update, which for a
/// running thread trails by up to a tick (about 4 ms on a runner),
/// against steps the 10 ms line is drawn under. The run-queue time is
/// `schedstat`'s, settled each time the thread is put back on a CPU, so
/// a reading the thread takes itself is never short of a wait.
pub fn this_thread_time() -> Option<coord_storage::ThreadTime> {
    let (_, run_queue) = this_thread_schedstat()?;
    let on_cpu = rustix::time::clock_gettime(rustix::time::ClockId::ThreadCPUTime);
    let on_cpu = Duration::new(
        u64::try_from(on_cpu.tv_sec).ok()?,
        u32::try_from(on_cpu.tv_nsec).ok()?,
    );
    Some(coord_storage::ThreadTime { on_cpu, run_queue })
}

/// How the calling thread was scheduled since it started (task-d55).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Scheduling {
    /// Time it was runnable and waiting for a CPU.
    pub run_queue: Duration,
    /// Times it gave up the CPU itself: a sleep, a sync, a lock.
    pub voluntary: u64,
    /// Times the scheduler took the CPU from it.
    pub involuntary: u64,
}

/// The calling thread's scheduling, from `schedstat`'s second field and
/// `status`'s two context-switch counts.
///
/// Busy time less CPU time is the loop blocked or waiting to run. With
/// the pipeline's waits counted, what is left is either a syscall on the
/// loop's thread or the scheduler running something else: the run-queue
/// time says which, and a runner with more runnable threads than cores
/// shows it here before anywhere else.
pub fn this_thread_scheduling() -> Option<Scheduling> {
    let (_, run_queue) = this_thread_schedstat()?;
    let status = std::fs::read_to_string("/proc/thread-self/status").ok()?;
    let (voluntary, involuntary) = switches(&status)?;
    Some(Scheduling {
        run_queue,
        voluntary,
        involuntary,
    })
}

/// The calling thread's time waiting on a run queue since it started,
/// from `schedstat` alone: what the domain loop reads on a sampled pass
/// (task-d97), one read rather than [`this_thread_scheduling`]'s two.
pub fn this_thread_run_queue() -> Option<Duration> {
    this_thread_schedstat().map(|(_, run_queue)| run_queue)
}

/// The calling thread's `schedstat` line, read through a descriptor the
/// thread opened once (#185, lead 15:27).
///
/// Opening `/proc/thread-self/schedstat` walks the path to the thread's
/// directory and allocates the file's buffer, and closing frees both, on
/// every read: about 1% of a loop's time even once a sampled pass
/// (task-d115). The file prints its line again from offset 0, so a held
/// descriptor costs one `pread` a read. It names the thread that opened
/// it, so each thread keeps its own. A failed open is not kept, and the
/// next read tries again.
#[cfg(unix)]
fn this_thread_schedstat() -> Option<(Duration, Duration)> {
    use std::os::unix::fs::FileExt;
    thread_local! {
        static SCHEDSTAT: std::cell::RefCell<Option<std::fs::File>> =
            const { std::cell::RefCell::new(None) };
    }
    SCHEDSTAT.with_borrow_mut(|held| {
        if held.is_none() {
            *held = std::fs::File::open("/proc/thread-self/schedstat").ok();
        }
        // The line is three decimal numbers and a newline, at most 63 bytes.
        let mut line = [0u8; 128];
        let read = held.as_ref()?.read_at(&mut line, 0).ok()?;
        // A read that fills the buffer may have cut the line short.
        if read == line.len() {
            return None;
        }
        schedstat_times(std::str::from_utf8(&line[..read]).ok()?)
    })
}

/// No `/proc` elsewhere, so no reading.
#[cfg(not(unix))]
fn this_thread_schedstat() -> Option<(Duration, Duration)> {
    None
}

/// Time on a CPU and time waiting on a run queue, the first two fields of
/// a `schedstat` line, in nanoseconds.
fn schedstat_times(schedstat: &str) -> Option<(Duration, Duration)> {
    let mut fields = schedstat.split_whitespace();
    let on_cpu = fields.next()?.parse::<u64>().ok()?;
    let waiting = fields.next()?.parse::<u64>().ok()?;
    Some((Duration::from_nanos(on_cpu), Duration::from_nanos(waiting)))
}

/// Voluntary and involuntary context switches from a `status` file.
fn switches(status: &str) -> Option<(u64, u64)> {
    let count = |name: &str| {
        status
            .lines()
            .find_map(|line| line.strip_prefix(name)?.trim().parse::<u64>().ok())
    };
    Some((
        count("voluntary_ctxt_switches:")?,
        count("nonvoluntary_ctxt_switches:")?,
    ))
}

/// CPU time the whole process has used since it started, every thread,
/// exited ones included.
pub fn process() -> Option<Duration> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    ticks(&stat).map(|t| Duration::from_millis(t.saturating_mul(1000 / USER_HZ)))
}

/// User plus system ticks from a `stat` line. The command name is in
/// parentheses and may hold spaces, so fields are counted after its
/// closing one: `utime` and `stime` are the 12th and 13th there.
fn ticks(stat: &str) -> Option<u64> {
    let after = &stat[stat.rfind(')')? + 1..];
    let mut fields = after.split_whitespace().skip(11);
    let user = fields.next()?.parse::<u64>().ok()?;
    let system = fields.next()?.parse::<u64>().ok()?;
    Some(user.saturating_add(system))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticks_are_read_after_a_command_name_with_spaces() {
        let stat = "12607 (a b) c) R 12603 12607 12603 0 -1 4194304 85 0 0 0 7 5 0 0 20 0";
        assert_eq!(ticks(stat), Some(12));
    }

    #[test]
    fn schedstat_and_status_are_read_by_field() {
        assert_eq!(
            schedstat_times("1500 250 7\n"),
            Some((Duration::from_nanos(1500), Duration::from_nanos(250)))
        );
        assert_eq!(schedstat_times("1500\n"), None);
        let status = "Name:\tcoordd\nvoluntary_ctxt_switches:\t41\n\
                      nonvoluntary_ctxt_switches:\t3\n";
        assert_eq!(switches(status), Some((41, 3)));
        assert_eq!(switches("Name:\tcoordd\n"), None);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn this_threads_switches_count_its_sleeps() {
        let before = this_thread_scheduling().expect("linux has schedstat and status");
        for _ in 0..3 {
            std::thread::sleep(Duration::from_millis(1));
        }
        let after = this_thread_scheduling().expect("linux has schedstat and status");
        assert!(after.voluntary >= before.voluntary + 3);
        assert!(after.run_queue >= before.run_queue);
    }

    /// The descriptor a thread holds reads the line as it is now, not
    /// as it was when the thread first read it, and it is the thread's
    /// own: another thread's reading is of that thread.
    #[test]
    #[cfg(target_os = "linux")]
    fn a_held_schedstat_reads_the_calling_thread_now() {
        let before = this_thread().expect("linux has schedstat");
        let mut x = 0u64;
        for i in 0..20_000_000u64 {
            x = x.wrapping_mul(31).wrapping_add(i);
        }
        std::hint::black_box(x);
        let after = this_thread().expect("linux has schedstat");
        assert!(after > before);
        let fresh = std::fs::read_to_string("/proc/thread-self/schedstat").unwrap();
        let (on_cpu, _) = schedstat_times(&fresh).unwrap();
        assert!(on_cpu >= after);
        // A new thread starts from nothing, not from this one's time.
        let other = std::thread::spawn(this_thread)
            .join()
            .unwrap()
            .expect("linux has schedstat");
        assert!(other < after);
    }

    /// The CPU clock moves with work that `schedstat` may not have
    /// accounted yet: a millisecond of it reads as a millisecond.
    #[test]
    #[cfg(target_os = "linux")]
    fn the_steps_clock_reads_a_short_step() {
        let before = this_thread_time().expect("linux has schedstat");
        let spun = std::time::Instant::now();
        let mut x = 0u64;
        while spun.elapsed() < Duration::from_millis(2) {
            x = std::hint::black_box(x.wrapping_add(1));
        }
        let after = this_thread_time().expect("linux has schedstat");
        let on_cpu = after.on_cpu - before.on_cpu;
        assert!(on_cpu > Duration::ZERO && on_cpu <= spun.elapsed() + Duration::from_millis(1));
        assert!(after.run_queue >= before.run_queue);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn this_threads_cpu_time_grows_with_work() {
        let before = this_thread().expect("linux has schedstat");
        let mut x = 0u64;
        for i in 0..20_000_000u64 {
            x = x.wrapping_mul(31).wrapping_add(i);
        }
        std::hint::black_box(x);
        let after = this_thread().expect("linux has schedstat");
        assert!(after > before);
        let time = this_thread_time().expect("linux has schedstat");
        assert!(time.on_cpu > Duration::ZERO);
        assert!(process().expect("linux has stat") >= Duration::ZERO);
    }
}
