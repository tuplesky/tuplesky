//! CPU time, for the cost a snapshot reports (task-d54).
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
    let nanos = schedstat.split_whitespace().next()?.parse::<u64>().ok()?;
    Some(Duration::from_nanos(nanos))
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
