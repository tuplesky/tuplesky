//! Explicit secure credential stores (design Sections 8.2, 20.3).

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use keyring_core::{Entry, Error as KeyringError};

use crate::credentials::Credentials;

/// Why the store could not serve.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreError {
    /// The store is locked or access was denied.
    Locked,
    /// The store is unavailable (bounded reason).
    Unavailable(String),
    /// The stored item is not credentials.
    Corrupt,
    /// No secure store is available: memory only, or workload identity.
    NoSecureStore,
}

/// A credential store.
pub trait CredentialStore: Send + Sync {
    /// Load the credentials, if any.
    fn load(&self) -> Result<Option<Credentials>, StoreError>;
    /// Store the credentials.
    fn save(&self, credentials: &Credentials) -> Result<(), StoreError>;
    /// Remove the credentials.
    fn clear(&self) -> Result<(), StoreError>;
    /// Whether the credentials survive this process.
    fn persistent(&self) -> bool;
    /// Store name for status output.
    fn name(&self) -> &'static str;
}

/// Memory for one process: never written anywhere.
#[derive(Default)]
pub struct MemoryStore {
    inner: Mutex<Option<Credentials>>,
}

impl MemoryStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

impl CredentialStore for MemoryStore {
    fn load(&self) -> Result<Option<Credentials>, StoreError> {
        Ok(self.inner.lock().expect("store lock").clone())
    }
    fn save(&self, credentials: &Credentials) -> Result<(), StoreError> {
        *self.inner.lock().expect("store lock") = Some(credentials.clone());
        Ok(())
    }
    fn clear(&self) -> Result<(), StoreError> {
        *self.inner.lock().expect("store lock") = None;
        Ok(())
    }
    fn persistent(&self) -> bool {
        false
    }
    fn name(&self) -> &'static str {
        "memory"
    }
}

fn map_keyring(e: KeyringError) -> StoreError {
    match e {
        KeyringError::NoStorageAccess(_) => StoreError::Locked,
        KeyringError::BadEncoding(_) | KeyringError::BadDataFormat(..) => StoreError::Corrupt,
        other => StoreError::Unavailable(format!("{other:?}")),
    }
}

/// The platform keyring through `keyring-core` (whatever default store
/// was installed: Apple keychain, Secret Service, or a test store).
pub struct KeyringStore {
    entry: Entry,
    name: &'static str,
}

impl KeyringStore {
    /// Open the entry for `service`/`user` in the installed store.
    pub fn open(service: &str, user: &str, name: &'static str) -> Result<Self, StoreError> {
        if keyring_core::get_default_store().is_none() {
            return Err(StoreError::NoSecureStore);
        }
        let entry = Entry::new(service, user).map_err(map_keyring)?;
        Ok(KeyringStore { entry, name })
    }
}

impl KeyringStore {
    /// The underlying entry (tests inject store failures through it).
    pub const fn entry(&self) -> &Entry {
        &self.entry
    }
}

impl CredentialStore for KeyringStore {
    fn load(&self) -> Result<Option<Credentials>, StoreError> {
        match self.entry.get_secret() {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|_| StoreError::Corrupt),
            Err(KeyringError::NoEntry) => Ok(None),
            Err(e) => Err(map_keyring(e)),
        }
    }
    fn save(&self, credentials: &Credentials) -> Result<(), StoreError> {
        let bytes = serde_json::to_vec(credentials).map_err(|_| StoreError::Corrupt)?;
        self.entry.set_secret(&bytes).map_err(map_keyring)
    }
    fn clear(&self) -> Result<(), StoreError> {
        match self.entry.delete_credential() {
            Ok(()) | Err(KeyringError::NoEntry) => Ok(()),
            Err(e) => Err(map_keyring(e)),
        }
    }
    fn persistent(&self) -> bool {
        true
    }
    fn name(&self) -> &'static str {
        self.name
    }
}

/// Which store the operator selected. There is no file store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreKind {
    /// Memory for this process only.
    Memory,
    /// The Apple keychain (needs the `keychain` feature on macOS).
    Keychain,
    /// The Linux Secret Service (needs the `secret-service` feature).
    SecretService,
    /// Whatever default store the process installed (tests).
    Installed,
}

/// Open the selected store; `NoSecureStore` when it is not available.
pub fn open_store(
    kind: StoreKind,
    service: &str,
    user: &str,
) -> Result<Box<dyn CredentialStore>, StoreError> {
    match kind {
        StoreKind::Memory => Ok(Box::new(MemoryStore::new())),
        StoreKind::Installed => Ok(Box::new(KeyringStore::open(service, user, "installed")?)),
        StoreKind::Keychain => {
            #[cfg(all(feature = "keychain", target_os = "macos"))]
            {
                let store = apple_native_keyring_store::keychain::Store::new()
                    .map_err(|e| StoreError::Unavailable(format!("{e:?}")))?;
                keyring_core::set_default_store(store);
                return Ok(Box::new(KeyringStore::open(service, user, "keychain")?));
            }
            #[cfg(not(all(feature = "keychain", target_os = "macos")))]
            {
                Err(StoreError::NoSecureStore)
            }
        }
        StoreKind::SecretService => {
            #[cfg(feature = "secret-service")]
            {
                let store = zbus_secret_service_keyring_store::Store::new()
                    .map_err(|e| StoreError::Unavailable(format!("{e:?}")))?;
                keyring_core::set_default_store(store);
                return Ok(Box::new(KeyringStore::open(
                    service,
                    user,
                    "secret-service",
                )?));
            }
            #[cfg(not(feature = "secret-service"))]
            {
                Err(StoreError::NoSecureStore)
            }
        }
    }
}

static PROCESS_LOCK: Mutex<()> = Mutex::new(());

/// Serializes shared-credential updates across threads and processes.
pub struct UpdateLock {
    path: PathBuf,
}

impl UpdateLock {
    /// A lock at `path` (an empty file; never holds a secret).
    pub fn new(path: &Path) -> Self {
        UpdateLock {
            path: path.to_path_buf(),
        }
    }

    fn acquire(&self) -> Result<File, StoreError> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&self.path)
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        file.lock()
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        Ok(file)
    }
}

/// The update lock, held: load and store while nobody else can.
///
/// A refresh secret is single use, so reading it, spending it on the
/// network and writing the replacement have to be one critical section.
/// Loading outside the lock let two invocations spend the same secret,
/// and the family's reuse detection then revoked it for both.
pub struct UpdateGuard<'a> {
    store: &'a dyn CredentialStore,
    file: Option<File>,
    _process: std::sync::MutexGuard<'static, ()>,
}

impl UpdateGuard<'_> {
    /// The credentials as they are now.
    pub fn load(&self) -> Result<Option<Credentials>, StoreError> {
        self.store.load()
    }

    /// Replace the stored credentials.
    pub fn save(&self, credentials: &Credentials) -> Result<(), StoreError> {
        self.store.save(credentials)
    }

    /// Remove the stored credentials.
    pub fn clear(&self) -> Result<(), StoreError> {
        self.store.clear()
    }
}

impl Drop for UpdateGuard<'_> {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            let _ = file.unlock();
        }
    }
}

/// Take the update lock for a sequence that has to be atomic across an
/// await, such as spending a refresh secret.
pub fn begin_update<'a>(
    store: &'a dyn CredentialStore,
    lock: &UpdateLock,
) -> Result<UpdateGuard<'a>, StoreError> {
    let process = PROCESS_LOCK.lock().expect("process lock");
    let file = lock.acquire()?;
    Ok(UpdateGuard {
        store,
        file: Some(file),
        _process: process,
    })
}

/// Load, transform and store under the lock: `f` sees the current
/// credentials and returns what to keep (`None` clears).
pub fn update(
    store: &dyn CredentialStore,
    lock: &UpdateLock,
    f: impl FnOnce(Option<Credentials>) -> Result<Option<Credentials>, StoreError>,
) -> Result<Option<Credentials>, StoreError> {
    let _process = PROCESS_LOCK.lock().expect("process lock");
    let file = lock.acquire()?;
    let current = store.load()?;
    let next = f(current)?;
    match &next {
        Some(c) => store.save(c)?,
        None => store.clear()?,
    }
    let _ = file.unlock();
    Ok(next)
}
