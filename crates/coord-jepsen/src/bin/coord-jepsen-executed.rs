//! `coord-jepsen-executed`: a voter store's execution order, for a test's
//! log.
//!
//! Prints a store's `executed_v1` rows in position order, one line each:
//! `position revision digest command`, the digest being the result
//! digest's first 4 bytes. With `--around PREFIX`, only the rows within
//! `--span` positions of each command whose id starts with that hex
//! prefix, the match marked `*`. A voter that stops on a release that
//! contradicts its own execution names the command by such a prefix, and
//! the rows around it on every voter show where their orders split, or,
//! when the orders agree, where one command's results differ.
//!
//! It writes nothing itself, but opening a store that was not closed (a
//! killed voter's, a crash image) lets `redb` repair it in place, so give
//! it a scratch copy when the file must keep its bytes. One that does not
//! open says why.
#![forbid(unsafe_code)]

use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

#[derive(Parser)]
#[command(
    name = "coord-jepsen-executed",
    about = "Print a voter store's executed_v1 rows in position order, with result digests"
)]
struct Cli {
    /// The store: a voter's `domain.redb`.
    store: PathBuf,
    /// Only the rows around each command whose id starts with this hex.
    #[arg(long)]
    around: Option<String>,
    /// How many positions either side of a match to print.
    #[arg(long, default_value_t = 5)]
    span: u64,
}

const EXECUTED: TableDefinition<&[u8], &[u8]> = TableDefinition::new("executed_v1");

/// One executed command: its position, its revision (if it wrote), the
/// first 4 bytes of its result digest and its id, both as hex.
struct Row {
    position: u64,
    revision: Option<u64>,
    digest: String,
    command: String,
}

fn rows(store: &PathBuf) -> Result<Vec<Row>, String> {
    let db = Database::open(store).map_err(|e| format!("cannot open {}: {e}", store.display()))?;
    let tx = db.begin_read().map_err(|e| format!("cannot read: {e}"))?;
    let table = tx
        .open_table(EXECUTED)
        .map_err(|e| format!("no executed_v1 table: {e}"))?;
    let mut rows = Vec::new();
    for row in table.iter().map_err(|e| format!("cannot iterate: {e}"))? {
        let (key, value) = row.map_err(|e| format!("cannot read a row: {e}"))?;
        let record = coord_storage::codecs::decode_executed(value.value())
            .map_err(|e| format!("cannot decode a row: {e:?}"))?;
        rows.push(Row {
            position: record.position.get(),
            revision: record.revision.map(|r| r.get()),
            digest: hex(&record.result_digest.0[..4]),
            command: hex(key.value()),
        });
    }
    rows.sort_by_key(|r| r.position);
    Ok(rows)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The rows to print: all of them, or those within `span` positions of a
/// command starting with `prefix`, each with whether it is a match.
fn select<'a>(rows: &'a [Row], around: Option<&str>, span: u64) -> Vec<(&'a Row, bool)> {
    let Some(prefix) = around else {
        return rows.iter().map(|r| (r, false)).collect();
    };
    let prefix = prefix.to_ascii_lowercase();
    let hits: Vec<u64> = rows
        .iter()
        .filter(|r| r.command.starts_with(&prefix))
        .map(|r| r.position)
        .collect();
    rows.iter()
        .filter(|r| hits.iter().any(|&h| r.position.abs_diff(h) <= span))
        .map(|r| (r, r.command.starts_with(&prefix)))
        .collect()
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let rows = match rows(&cli.store) {
        Ok(rows) => rows,
        Err(why) => {
            println!("{why}");
            return ExitCode::FAILURE;
        }
    };
    let selected = select(&rows, cli.around.as_deref(), cli.span);
    if let (Some(prefix), true) = (&cli.around, selected.is_empty()) {
        println!(
            "no executed command starts with {prefix} ({} rows)",
            rows.len()
        );
    }
    // A reader that stops early (`| head`) ends the output, not a panic.
    let mut out = std::io::stdout().lock();
    for (row, hit) in selected {
        let revision = row
            .revision
            .map_or_else(|| "-".to_owned(), |r| r.to_string());
        let mark = if hit { " *" } else { "" };
        let line = format!(
            "{} {} {} {}",
            row.position, revision, row.digest, row.command
        );
        if writeln!(out, "{line}{mark}").is_err() {
            break;
        }
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(position: u64, command: &str) -> Row {
        Row {
            position,
            revision: None,
            digest: "00000000".to_owned(),
            command: command.to_owned(),
        }
    }

    #[test]
    fn around_keeps_the_span_on_either_side_of_each_match() {
        let rows: Vec<Row> = (1..=20).map(|p| row(p, &format!("{p:02x}00"))).collect();
        let picked: Vec<(u64, bool)> = select(&rows, Some("0A"), 2)
            .into_iter()
            .map(|(r, hit)| (r.position, hit))
            .collect();
        assert_eq!(
            picked,
            vec![(8, false), (9, false), (10, true), (11, false), (12, false)]
        );
    }

    #[test]
    fn without_around_every_row_is_kept() {
        let rows = vec![row(1, "aa"), row(2, "bb")];
        assert_eq!(select(&rows, None, 0).len(), 2);
        assert!(select(&rows, Some("cc"), 5).is_empty());
    }
}
