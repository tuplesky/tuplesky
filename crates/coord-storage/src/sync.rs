//! Synchronization primitives, replaced by loom's under `cfg(loom)` so the
//! watch handoff boundary can be model-checked.

#[cfg(loom)]
pub use loom::sync::{Arc, Mutex, MutexGuard};
#[cfg(not(loom))]
pub use std::sync::{Arc, Mutex, MutexGuard};

/// Lock a mutex, tolerating poisoning (a panicked holder already failed).
pub fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    }
}
