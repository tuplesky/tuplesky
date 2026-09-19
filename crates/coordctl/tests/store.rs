//! task-40 acceptance for credential stores: explicit stores only,
//! locked and missing stores are typed failures, no plaintext fallback,
//! redaction, and serialized shared-credential updates.

use std::sync::Arc;
use std::thread;

use coordctl::CredentialStore;
use coordctl::{
    Credentials, KeyringStore, MemoryStore, StoreError, StoreKind, UpdateLock, open_store, update,
};

/// `keyring_core`'s default store is process-global, so the tests that
/// install or assert the absence of one cannot run at the same time.
static KEYRING_DEFAULT: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn credentials(n: u64) -> Credentials {
    Credentials {
        broker: "http://127.0.0.1:1".into(),
        client_id: "coordctl".into(),
        access_token: format!("access-secret-{n}"),
        expires_at: 1_700_000_000 + n,
        scope: "read".into(),
        refresh_token: Some(format!("family.refresh-secret-{n}")),
    }
}

#[test]
fn explicit_stores_only_and_redaction() {
    let _global = KEYRING_DEFAULT.lock().unwrap_or_else(|e| e.into_inner());
    // Memory: works, not persistent.
    let store = open_store(StoreKind::Memory, "coordctl", "broker").unwrap();
    assert!(!store.persistent());
    assert_eq!(store.load().unwrap(), None);
    store.save(&credentials(1)).unwrap();
    assert_eq!(store.load().unwrap(), Some(credentials(1)));
    store.clear().unwrap();
    assert_eq!(store.load().unwrap(), None);
    // Platform stores are explicit features: absent, they are refused
    // rather than replaced by a file.
    assert_eq!(
        open_store(StoreKind::Keychain, "coordctl", "broker").err(),
        Some(StoreError::NoSecureStore)
    );
    assert_eq!(
        open_store(StoreKind::SecretService, "coordctl", "broker").err(),
        Some(StoreError::NoSecureStore)
    );
    assert_eq!(
        open_store(StoreKind::Installed, "coordctl", "broker").err(),
        Some(StoreError::NoSecureStore),
        "no installed store, no fallback"
    );
    // Nothing secret in diagnostics or status.
    let c = credentials(2);
    let dbg = format!("{c:?}");
    assert!(!dbg.contains("secret") && dbg.contains("<redacted>"));
    assert!(!c.summary().contains("secret"));
    assert!(c.summary().contains("refresh=yes"));
}

#[test]
fn the_keyring_store_maps_locked_and_missing_and_round_trips() {
    let _global = KEYRING_DEFAULT.lock().unwrap_or_else(|e| e.into_inner());
    let mock = keyring_core::mock::Store::new().unwrap();
    keyring_core::set_default_store(mock.clone());
    let store = KeyringStore::open("coordctl", "broker-a", "mock").unwrap();
    assert!(store.persistent());
    assert_eq!(store.load().unwrap(), None);
    store.save(&credentials(3)).unwrap();
    assert_eq!(store.load().unwrap(), Some(credentials(3)));
    store.clear().unwrap();
    assert_eq!(store.load().unwrap(), None);
    store.clear().unwrap();
    // A locked store is a typed failure, never a plaintext detour.
    let cred: &keyring_core::mock::Cred = store.entry().as_any().downcast_ref().unwrap();
    cred.set_error(keyring_core::Error::NoStorageAccess(Box::new(
        std::io::Error::other("locked"),
    )));
    assert_eq!(store.load(), Err(StoreError::Locked));
    cred.set_error(keyring_core::Error::PlatformFailure(Box::new(
        std::io::Error::other("gone"),
    )));
    assert!(matches!(
        store.save(&credentials(4)),
        Err(StoreError::Unavailable(_))
    ));
    let _ = &mock;
    keyring_core::unset_default_store();
}

#[test]
fn shared_credential_updates_are_serialized() {
    let dir = tempfile::tempdir().unwrap();
    let lock = Arc::new(UpdateLock::new(&dir.path().join("coordctl.lock")));
    let store: Arc<MemoryStore> = Arc::new(MemoryStore::new());
    store.save(&credentials(0)).unwrap();
    let mut handles = Vec::new();
    for _ in 0..8 {
        let store = store.clone();
        let lock = lock.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..25 {
                update(store.as_ref(), &lock, |current| {
                    let c = current.expect("present");
                    Ok(Some(Credentials {
                        expires_at: c.expires_at + 1,
                        ..c
                    }))
                })
                .unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(
        store.load().unwrap().unwrap().expires_at,
        1_700_000_000 + 200,
        "every update applied in order, none lost"
    );
    // Clearing under the lock.
    assert_eq!(update(store.as_ref(), &lock, |_| Ok(None)).unwrap(), None);
    assert_eq!(store.load().unwrap(), None);
}

#[test]
fn a_rotation_holds_the_lock_across_the_exchange() {
    // The refresh secret is single use. Loading it outside the lock let
    // two invocations read the same secret and spend it twice, and the
    // family's reuse detection then revoked it for both.
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn CredentialStore> = Arc::new(MemoryStore::new());
    store.save(&credentials(1)).unwrap();

    let spent = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let mut handles = Vec::new();
    for n in 2..6u64 {
        let store = Arc::clone(&store);
        let lock = UpdateLock::new(&dir.path().join("lock"));
        let spent = Arc::clone(&spent);
        handles.push(thread::spawn(move || {
            let guard = coordctl::begin_update(store.as_ref(), &lock).unwrap();
            let current = guard.load().unwrap().expect("credentials");
            // Stand in for the network round trip that spends the
            // secret: whatever each rotation reads must be distinct.
            spent
                .lock()
                .unwrap()
                .push(current.refresh_token.clone().expect("a refresh token"));
            thread::sleep(std::time::Duration::from_millis(5));
            guard.save(&credentials(n)).unwrap();
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let spent = spent.lock().unwrap().clone();
    let distinct: std::collections::BTreeSet<_> = spent.iter().collect();
    assert_eq!(
        distinct.len(),
        spent.len(),
        "each rotation spent a secret of its own: {spent:?}"
    );
}
