//! Binding the configured listeners (design Section 19.2).
//!
//! The preview has no serving runtime yet, but binding is the step that
//! decides whether a node can serve at all, and the lifecycle's `Live`
//! phase is defined by it. Doing it for real means a configuration that
//! cannot bind fails where an operator sees it, rather than passing
//! validation and failing later.

use std::io;
use std::net::{SocketAddr, TcpListener, UdpSocket};

use crate::config::ListenConfig;

/// The sockets a node holds while it runs.
#[derive(Debug)]
pub struct BoundListeners {
    /// Native API QUIC socket.
    pub api_quic: Option<UdpSocket>,
    /// Peer plane QUIC socket.
    pub peer_quic: Option<UdpSocket>,
    /// Credential-establishment listener.
    pub https: Option<TcpListener>,
    /// Loopback admin/metrics listener.
    pub admin_http: Option<TcpListener>,
}

impl BoundListeners {
    /// The addresses actually bound, for diagnostics. A configured port
    /// of zero resolves here, so what is reported is what is listening.
    pub fn addresses(&self) -> Vec<(&'static str, SocketAddr)> {
        let mut out = Vec::new();
        if let Some(s) = &self.api_quic
            && let Ok(a) = s.local_addr()
        {
            out.push(("api_quic", a));
        }
        if let Some(s) = &self.peer_quic
            && let Ok(a) = s.local_addr()
        {
            out.push(("peer_quic", a));
        }
        if let Some(s) = &self.https
            && let Ok(a) = s.local_addr()
        {
            out.push(("https", a));
        }
        if let Some(s) = &self.admin_http
            && let Ok(a) = s.local_addr()
        {
            out.push(("admin_http", a));
        }
        out
    }
}

/// Why a listener could not be bound.
#[derive(Debug)]
pub struct BindFailure {
    /// Which listener.
    pub listener: &'static str,
    /// The operating system's reason.
    pub reason: io::Error,
}

/// Bind every listener `config` names.
///
/// Validation has already established that each address parses, so a
/// failure here is the environment refusing the socket, which is a
/// reason not to start rather than something to report as ready.
pub fn bind_listeners(config: &ListenConfig) -> Result<BoundListeners, BindFailure> {
    fn udp(name: &'static str, address: Option<&str>) -> Result<Option<UdpSocket>, BindFailure> {
        match address {
            None => Ok(None),
            Some(a) => UdpSocket::bind(a).map(Some).map_err(|reason| BindFailure {
                listener: name,
                reason,
            }),
        }
    }
    fn tcp(name: &'static str, address: Option<&str>) -> Result<Option<TcpListener>, BindFailure> {
        match address {
            None => Ok(None),
            Some(a) => TcpListener::bind(a)
                .map(Some)
                .map_err(|reason| BindFailure {
                    listener: name,
                    reason,
                }),
        }
    }
    Ok(BoundListeners {
        api_quic: udp("api_quic", config.api_quic.as_deref())?,
        peer_quic: udp("peer_quic", config.peer_quic.as_deref())?,
        https: tcp("https", config.https.as_deref())?,
        admin_http: tcp("admin_http", config.admin_http.as_deref())?,
    })
}
