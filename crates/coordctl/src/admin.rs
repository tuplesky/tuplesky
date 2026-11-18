//! A node's admin socket (task-d99): `coordctl feature activate` and
//! `coordctl feature status` reach a running voter through the Unix socket
//! beside its state, `<state_directory>/admin.sock`. Local only, and only
//! for whoever its permissions let in, which the node sets to its owner's.
//!
//! One request line, one answer: its first line is `ok` or `refused:` with
//! the reason, and the rest is what the node said.

use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::time::Duration;

/// The longest answer read.
const MAX_ANSWER_BYTES: u64 = 1 << 20;

/// What the node answered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Answer {
    /// Whether it did what was asked.
    pub ok: bool,
    /// What it said: the reason it refused, or what it reports.
    pub text: String,
}

/// Send `line` to the node at `socket` and read its answer.
#[cfg(unix)]
pub fn request(socket: &Path, line: &str, timeout: Duration) -> std::io::Result<Answer> {
    let mut stream = std::os::unix::net::UnixStream::connect(socket)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    stream.write_all(line.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.shutdown(std::net::Shutdown::Write)?;
    let mut reader = BufReader::new(stream.take(MAX_ANSWER_BYTES));
    let mut first = String::new();
    reader.read_line(&mut first)?;
    let mut rest = String::new();
    reader.read_to_string(&mut rest)?;
    let first = first.trim_end();
    match first.strip_prefix("refused:") {
        Some(reason) => Ok(Answer {
            ok: false,
            text: format!("{}\n{rest}", reason.trim()).trim_end().to_owned(),
        }),
        None if first == "ok" => Ok(Answer {
            ok: true,
            text: rest.trim_end().to_owned(),
        }),
        None => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("the node answered neither ok nor refused: {first}"),
        )),
    }
}
