//! `coord-jepsen-executed`: a voter store's execution order, for a test's
//! log.
//!
//! Prints a store's executed rows in position order, one line each:
//! `position revision digest command`, the digest being the result
//! digest's first 4 bytes. A store of format 3 (task-d113, step 2) keeps
//! them in `execution_v1` by position; an older one in `executed_v1` by
//! command, which is read when it holds any. With `--around PREFIX`, only the rows within
//! `--span` positions of each command whose id starts with that hex
//! prefix, the match marked `*`. A voter that stops on a release that
//! contradicts its own execution names the command by such a prefix, and
//! the rows around it on every voter show where their orders split, or,
//! when the orders agree, where one command's results differ.
//!
//! With `--last`, only the highest executed position, for a test's
//! summary of how far each voter got.
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
    about = "Print a voter store's executed rows in position order, with result digests"
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
    /// Print only the highest executed position (0 when nothing executed).
    #[arg(long, conflicts_with = "around")]
    last: bool,
}

/// Executed rows by command, in a store of format 2 or older.
const EXECUTED: TableDefinition<&[u8], &[u8]> = TableDefinition::new("executed_v1");
/// Executed rows by position, in a store of format 3 (task-d113, step 2).
const EXECUTION: TableDefinition<&[u8], &[u8]> = TableDefinition::new("execution_v1");

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
    let mut rows = Vec::new();
    // Format 2 or older: `executed_v1`, keyed by command. A store of format
    // 3 may hold the table, empty, or not at all.
    if let Ok(table) = tx.open_table(EXECUTED) {
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
    }
    if rows.is_empty() {
        // Format 3: `execution_v1`, keyed by position, each row naming its
        // command.
        let table = tx
            .open_table(EXECUTION)
            .map_err(|e| format!("no executed_v1 rows and no execution_v1 table: {e}"))?;
        for row in table.iter().map_err(|e| format!("cannot iterate: {e}"))? {
            let (_, value) = row.map_err(|e| format!("cannot read a row: {e}"))?;
            let record = coord_storage::codecs::decode_execution(value.value())
                .map_err(|e| format!("cannot decode a row: {e:?}"))?;
            rows.push(Row {
                position: record.executed.position.get(),
                revision: record.executed.revision.map(|r| r.get()),
                digest: hex(&record.executed.result_digest.0[..4]),
                command: hex(&record.command.0.0),
            });
        }
    }
    rows.sort_by_key(|r| r.position);
    Ok(rows)
}

/// The highest executed position, or 0 when nothing executed.
fn last_position(rows: &[Row]) -> u64 {
    rows.iter().map(|r| r.position).max().unwrap_or(0)
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
    if cli.last {
        println!("{}", last_position(&rows));
        return ExitCode::SUCCESS;
    }
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

    fn record(position: u64) -> coord_storage::codecs::ExecutedRecordV1 {
        coord_storage::codecs::ExecutedRecordV1 {
            position: coord_types::ids::ExecutionPosition::new(position).unwrap(),
            revision: None,
            result_digest: coord_types::identity::Digest32([position as u8; 32]),
        }
    }

    fn command(byte: u8) -> coord_types::CommandId {
        coord_types::CommandId(coord_types::identity::Digest32([byte; 32]))
    }

    /// A store of format 3 keeps its executed rows in `execution_v1` by
    /// position (task-d113, step 2), and one of format 2 in `executed_v1`
    /// by command: both read the same.
    #[test]
    fn both_store_formats_give_the_rows_by_position() {
        let dir =
            std::env::temp_dir().join(format!("coord-jepsen-executed-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        for format in [2, 3] {
            let path = dir.join(format!("format-{format}.redb"));
            let _ = std::fs::remove_file(&path);
            let db = Database::create(&path).unwrap();
            let tx = db.begin_write().unwrap();
            {
                // A format-3 store may hold `executed_v1`, empty.
                let mut executed = tx.open_table(EXECUTED).unwrap();
                let mut execution = tx.open_table(EXECUTION).unwrap();
                for (position, byte) in [(2u64, 0xbb), (1, 0xaa)] {
                    let c = command(byte);
                    if format == 2 {
                        let value =
                            coord_storage::codecs::encode_executed(&record(position)).unwrap();
                        executed
                            .insert(
                                coord_storage::codecs::executed_key(&c).as_slice(),
                                value.as_slice(),
                            )
                            .unwrap();
                    } else {
                        let value = coord_storage::codecs::encode_execution(
                            &coord_storage::codecs::ExecutionRecordV1 {
                                command: c,
                                executed: record(position),
                            },
                        )
                        .unwrap();
                        let key = coord_storage::codecs::position_key(record(position).position);
                        execution.insert(key.as_slice(), value.as_slice()).unwrap();
                    }
                }
            }
            tx.commit().unwrap();
            drop(db);
            let read = rows(&path).unwrap();
            let got: Vec<(u64, String)> = read
                .iter()
                .map(|r| (r.position, r.command.clone()))
                .collect();
            assert_eq!(
                got,
                vec![(1, "aa".repeat(32)), (2, "bb".repeat(32))],
                "format {format}"
            );
            assert_eq!(read[0].digest, "01010101", "format {format}");
            assert_eq!(last_position(&read), 2, "format {format}");
        }
        let _ = std::fs::remove_dir_all(&dir);
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
    fn last_is_the_highest_position_or_zero() {
        assert_eq!(last_position(&[]), 0);
        assert_eq!(
            last_position(&[row(3, "aa"), row(9, "bb"), row(4, "cc")]),
            9
        );
    }

    #[test]
    fn without_around_every_row_is_kept() {
        let rows = vec![row(1, "aa"), row(2, "bb")];
        assert_eq!(select(&rows, None, 0).len(), 2);
        assert!(select(&rows, Some("cc"), 5).is_empty());
    }
}
