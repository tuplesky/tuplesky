//! `cargo xtask`: the single entry point for format, lint, test and policy
//! checks (task-01). CI runs the same commands; see `docs/build/ci.md`.
#![forbid(unsafe_code)]

mod deps;
mod tools;

use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "xtask",
    about = "TupleSky workspace automation",
    disable_help_subcommand = true
)]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Format Rust (rustfmt) and Go (gofmt) sources; `--check` only verifies.
    Fmt {
        #[arg(long)]
        check: bool,
    },
    /// Clippy with warnings denied, plus `go vet`.
    Lint,
    /// Run the Rust test suites (nextest when installed) and Go tests.
    Test {
        /// Also build release artifacts; used by scheduled/manual full runs.
        #[arg(long)]
        extended: bool,
    },
    /// Dependency policy: exact pins, roles, test-only isolation, sources, cargo-deny.
    CheckDeps {
        /// Skip `cargo deny check advisories` (needs network access).
        #[arg(long)]
        offline: bool,
    },
    /// Verify pinned tool versions from tools/manifest.toml; `--install` first installs them.
    CheckTools {
        #[arg(long)]
        install: bool,
        /// Ignore npm tools (mermaid-cli), which only the documentation job needs.
        #[arg(long)]
        skip_npm: bool,
    },
    /// Markdown link, task-graph and Mermaid checks; `--render` renders every diagram.
    CheckDocs {
        #[arg(long)]
        render: bool,
    },
    /// Unit tests for the CI classifier, gate and documentation checker.
    CheckCi,
    /// `cargo check` the workspace with the declared minimum supported Rust version.
    Msrv,
    /// Everything a pull request runs: fmt --check, lint, check-deps, test, check-docs, check-ci.
    Ci,
}

fn main() {
    if let Err(err) = run_cli() {
        eprintln!("error: {err:#}");
        std::process::exit(1);
    }
}

fn run_cli() -> Result<()> {
    let cli = Cli::parse();
    let root = workspace_root()?;
    match cli.command {
        Cmd::Fmt { check } => fmt(&root, check),
        Cmd::Lint => lint(&root),
        Cmd::Test { extended } => test(&root, extended),
        Cmd::CheckDeps { offline } => deps::check(&root, offline),
        Cmd::CheckTools { install, skip_npm } => tools::check(&root, install, skip_npm),
        Cmd::CheckDocs { render } => check_docs(&root, render),
        Cmd::CheckCi => check_ci(&root),
        Cmd::Msrv => msrv(&root),
        Cmd::Ci => {
            fmt(&root, true)?;
            lint(&root)?;
            deps::check(&root, true)?;
            test(&root, false)?;
            check_docs(&root, false)?;
            check_ci(&root)
        }
    }
}

fn workspace_root() -> Result<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let root = manifest_dir
        .parent()
        .context("xtask has no parent directory")?
        .to_path_buf();
    if !root.join("Cargo.toml").is_file() {
        bail!("workspace root {} has no Cargo.toml", root.display());
    }
    Ok(root)
}

pub(crate) fn run(cwd: &Path, program: &str, args: &[&str]) -> Result<()> {
    eprintln!("$ {program} {}", args.join(" "));
    let status = Command::new(program)
        .args(args)
        .current_dir(cwd)
        .status()
        .with_context(|| format!("failed to start {program}"))?;
    ensure_success(program, status)
}

pub(crate) fn output(cwd: &Path, program: &str, args: &[&str]) -> Result<String> {
    let out = Command::new(program)
        .args(args)
        .current_dir(cwd)
        .output()
        .with_context(|| format!("failed to start {program}"))?;
    if !out.status.success() {
        bail!(
            "{program} {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    String::from_utf8(out.stdout).context("non-UTF-8 output")
}

fn ensure_success(program: &str, status: ExitStatus) -> Result<()> {
    if status.success() {
        Ok(())
    } else {
        bail!("{program} exited with {status}")
    }
}

pub(crate) fn has_program(program: &str, args: &[&str]) -> bool {
    Command::new(program)
        .args(args)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn go_dir(root: &Path) -> PathBuf {
    root.join("adapters").join("kine")
}

fn fmt(root: &Path, check: bool) -> Result<()> {
    let mut args = vec!["fmt", "--all"];
    if check {
        args.extend(["--", "--check"]);
    }
    run(root, "cargo", &args)?;
    let unformatted = output(&go_dir(root), "gofmt", &["-l", "."])?;
    if check {
        if !unformatted.trim().is_empty() {
            bail!("gofmt: unformatted Go files:\n{unformatted}");
        }
    } else {
        run(&go_dir(root), "gofmt", &["-w", "."])?;
    }
    Ok(())
}

fn lint(root: &Path) -> Result<()> {
    run(
        root,
        "cargo",
        &[
            "clippy",
            "--workspace",
            "--all-targets",
            "--locked",
            "--",
            "-D",
            "warnings",
        ],
    )?;
    run(
        root,
        "cargo",
        &["doc", "--workspace", "--no-deps", "--locked"],
    )?;
    run(&go_dir(root), "go", &["vet", "./..."])
}

fn test(root: &Path, extended: bool) -> Result<()> {
    if has_program("cargo", &["nextest", "--version"]) {
        run(
            root,
            "cargo",
            &[
                "nextest",
                "run",
                "--workspace",
                "--locked",
                "--no-fail-fast",
            ],
        )?;
        // nextest does not run doctests.
        run(root, "cargo", &["test", "--workspace", "--locked", "--doc"])?;
    } else {
        eprintln!("cargo-nextest not installed (see tools/manifest.toml); using cargo test");
        run(
            root,
            "cargo",
            &["test", "--workspace", "--locked", "--no-fail-fast"],
        )?;
    }
    run(&go_dir(root), "go", &["test", "-count=1", "./..."])?;
    if extended {
        // Extended runs build the release profile as well. No long-running
        // qualification campaigns are registered yet; this command does not
        // pretend otherwise (design Section 12.4.1).
        run(
            root,
            "cargo",
            &["build", "--workspace", "--release", "--locked"],
        )?;
        eprintln!("extended: release build verified; no extended campaigns registered yet");
    }
    Ok(())
}

fn check_docs(root: &Path, render: bool) -> Result<()> {
    let script = root.join("scripts/ci/check_docs.py");
    let mut args = vec![script.to_str().context("non-UTF-8 path")?.to_owned()];
    if render {
        args.push("--render".to_owned());
        let mmdc = root.join("tools/mermaid/node_modules/.bin/mmdc");
        if mmdc.is_file() {
            args.push("--mmdc".to_owned());
            args.push(mmdc.to_str().context("non-UTF-8 path")?.to_owned());
        }
        let cfg = root.join("tools/mermaid/puppeteer-config.json");
        args.push("--puppeteer-config".to_owned());
        args.push(cfg.to_str().context("non-UTF-8 path")?.to_owned());
    }
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    run(root, "python3", &args)
}

fn check_ci(root: &Path) -> Result<()> {
    run(
        &root.join("scripts/ci"),
        "python3",
        &["-m", "unittest", "discover", "-s", ".", "-p", "test_*.py"],
    )
}

fn msrv(root: &Path) -> Result<()> {
    let manifest: toml::Value = toml::from_str(&std::fs::read_to_string(root.join("Cargo.toml"))?)?;
    let msrv = manifest
        .get("workspace")
        .and_then(|w| w.get("package"))
        .and_then(|p| p.get("rust-version"))
        .and_then(|v| v.as_str())
        .context("workspace.package.rust-version missing")?;
    let toolchain = if msrv.matches('.').count() == 1 {
        format!("{msrv}.0")
    } else {
        msrv.to_owned()
    };
    let plus = format!("+{toolchain}");
    if !has_program("cargo", &[&plus, "--version"]) {
        bail!(
            "toolchain {toolchain} is not installed; run `rustup toolchain install {toolchain} --profile minimal`"
        );
    }
    run(
        root,
        "cargo",
        &[&plus, "check", "--workspace", "--all-targets", "--locked"],
    )
}
