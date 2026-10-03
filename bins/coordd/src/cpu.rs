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
//! reports to userspace at a fixed 100 a second. Elsewhere there is no
//! reading, and the snapshot says so rather than reporting zero.

use std::time::Duration;

/// Clock ticks a second in `/proc/<pid>/stat` (`USER_HZ`, fixed by the
/// kernel's userspace ABI).
const USER_HZ: u64 = 100;

/// CPU time the calling thread has used since it started.
pub fn this_thread() -> Option<Duration> {
    let schedstat = std::fs::read_to_string("/proc/thread-self/schedstat").ok()?;
    schedstat_times(&schedstat).map(|(on_cpu, _)| on_cpu)
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
    let schedstat = std::fs::read_to_string("/proc/thread-self/schedstat").ok()?;
    let (_, run_queue) = schedstat_times(&schedstat)?;
    let status = std::fs::read_to_string("/proc/thread-self/status").ok()?;
    let (voluntary, involuntary) = switches(&status)?;
    Some(Scheduling {
        run_queue,
        voluntary,
        involuntary,
    })
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
        assert!(process().expect("linux has stat") >= Duration::ZERO);
    }
}
