//! `coordctl`: login, refresh, logout and status.

use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand, ValueEnum};
use coordctl::{
    BrokerClient, CliError, Credentials, StoreKind, UpdateLock, begin_update, open_store, update,
};

#[derive(Parser)]
#[command(name = "coordctl", about = "TupleSky operator CLI")]
struct Cli {
    /// Broker base URL.
    #[arg(long)]
    broker: String,
    /// Registered client identifier.
    #[arg(long, default_value = "coordctl")]
    client_id: String,
    /// Credential store. The default is the platform's secret store, so
    /// a login survives the process; `memory` keeps the credentials for
    /// this process only and is an explicit choice.
    #[arg(long, value_enum, default_value_t = Store::Platform)]
    store: Store,
    /// Lock file serializing shared-credential updates.
    #[arg(long, default_value = "/tmp/coordctl.lock")]
    lock: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, ValueEnum)]
enum Store {
    /// The platform's own secret store.
    Platform,
    Memory,
    Keychain,
    SecretService,
}

#[derive(Subcommand)]
enum Command {
    /// Log in through the browser (default) or a device code.
    Login {
        #[arg(long)]
        device: bool,
    },
    /// Rotate the refresh family and obtain a new token.
    Refresh,
    /// Retire the session and clear stored credentials.
    Logout,
    /// Show the stored credentials' metadata (never secrets).
    Status,
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let kind = match cli.store {
        // Defaulting to memory meant a successful login was discarded
        // when the process exited and every later command found nothing.
        Store::Platform => {
            if cfg!(target_os = "macos") {
                StoreKind::Keychain
            } else {
                StoreKind::SecretService
            }
        }
        Store::Memory => StoreKind::Memory,
        Store::Keychain => StoreKind::Keychain,
        Store::SecretService => StoreKind::SecretService,
    };
    if matches!(cli.store, Store::Memory) {
        eprintln!("note: --store memory keeps credentials for this process only");
    }
    let store = match open_store(kind, "coordctl", &cli.broker) {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "credential store unavailable: {e:?}; use --store memory or workload identity"
            );
            std::process::exit(2);
        }
    };
    let lock = UpdateLock::new(&cli.lock);
    let client = match BrokerClient::new(&cli.broker, &cli.client_id, Duration::from_secs(20)) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e:?}");
            std::process::exit(2);
        }
    };
    let mut ui = std::io::stderr();
    let result: Result<(), CliError> = match cli.command {
        Command::Login { device } => {
            let response = if device {
                client.device_login(&mut ui).await
            } else {
                client
                    .browser_login(&mut ui, Duration::from_secs(300))
                    .await
            };
            response.and_then(|r| {
                let credentials =
                    Credentials::from_response(&cli.broker, &cli.client_id, now(), &r);
                update(store.as_ref(), &lock, |_| Ok(Some(credentials)))
                    .map(|_| ())
                    .map_err(CliError::Store)
            })
        }
        Command::Refresh => {
            // The whole rotation is one critical section: the secret is
            // single use, so reading it, spending it and writing its
            // replacement cannot be interleaved with another invocation.
            let guard = begin_update(store.as_ref(), &lock).map_err(CliError::Store);
            match guard {
                Err(e) => Err(e),
                Ok(guard) => {
                    let current = guard.load().map_err(CliError::Store);
                    match current {
                        Ok(Some(c)) => match c.refresh_token.as_deref() {
                            Some(t) => match client.refresh(t).await {
                                Ok(r) => {
                                    let next = Credentials::from_response(
                                        &cli.broker,
                                        &cli.client_id,
                                        now(),
                                        &r,
                                    );
                                    guard.save(&next).map_err(CliError::Store)
                                }
                                Err(CliError::FreshLoginRequired) => {
                                    let _ = guard.clear();
                                    Err(CliError::FreshLoginRequired)
                                }
                                Err(e) => Err(e),
                            },
                            None => Err(CliError::NoRefreshToken),
                        },
                        Ok(None) => Err(CliError::NoRefreshToken),
                        Err(e) => Err(e),
                    }
                }
            }
        }
        Command::Logout => {
            let current = store.load().map_err(CliError::Store);
            let result = match current {
                Ok(Some(c)) => match c.refresh_token.as_deref() {
                    Some(t) => client.logout(t).await,
                    None => Ok(()),
                },
                Ok(None) => Ok(()),
                Err(e) => Err(e),
            };
            update(store.as_ref(), &lock, |_| Ok(None))
                .map_err(CliError::Store)
                .and(result)
        }
        Command::Status => match store.load() {
            Ok(Some(c)) => {
                println!("{} store={}", c.summary(), store.name());
                Ok(())
            }
            Ok(None) => {
                println!("not logged in (store={})", store.name());
                Ok(())
            }
            Err(e) => Err(CliError::Store(e)),
        },
    };
    if let Err(e) = result {
        match e {
            CliError::FreshLoginRequired => {
                eprintln!(
                    "the refresh family was revoked or the session retired: run `coordctl login` again"
                )
            }
            other => eprintln!("{other:?}"),
        }
        std::process::exit(1);
    }
}
