//! Verification of the checksummed tool manifest (`tools/manifest.toml`).

use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Deserialize)]
struct Manifest {
    version: u32,
    tool: Vec<Tool>,
}

#[derive(Deserialize)]
struct Tool {
    name: String,
    version: String,
    version_command: Vec<String>,
    version_pattern: String,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    download: Vec<Download>,
}

#[derive(Deserialize)]
struct Download {
    platform: String,
    url: String,
    sha256: String,
    member: String,
}

pub(crate) fn check(root: &Path, install: bool, skip_npm: bool) -> Result<()> {
    let manifest_path = root.join("tools/manifest.toml");
    let manifest: Manifest = toml::from_str(&std::fs::read_to_string(&manifest_path)?)
        .context("parsing tools/manifest.toml")?;
    if manifest.version != 1 {
        bail!("unsupported tool manifest version {}", manifest.version);
    }
    validate(&manifest)?;
    if install {
        let mut args = vec!["scripts/ci/install_tools.py"];
        if skip_npm {
            args.push("--skip-npm");
        }
        crate::run(root, "python3", &args)?;
    }
    let mut missing = Vec::new();
    for tool in &manifest.tool {
        if skip_npm && tool.kind.as_deref() == Some("npm") {
            continue;
        }
        let (program, args) = tool
            .version_command
            .split_first()
            .context("empty version_command")?;
        let program = if program.contains('/') {
            root.join(program)
        } else {
            Path::new(program).to_path_buf()
        };
        let out = Command::new(&program).args(args).current_dir(root).output();
        match out {
            Ok(o) if o.status.success() => {
                let text = format!(
                    "{}{}",
                    String::from_utf8_lossy(&o.stdout),
                    String::from_utf8_lossy(&o.stderr)
                );
                if text.contains(&tool.version_pattern) {
                    eprintln!("ok: {} {}", tool.name, tool.version);
                } else {
                    missing.push(format!(
                        "{}: expected version {} but found: {}",
                        tool.name,
                        tool.version,
                        text.lines().next().unwrap_or("").trim()
                    ));
                }
            }
            _ => missing.push(format!(
                "{}: not installed (expected {})",
                tool.name, tool.version
            )),
        }
    }
    if !missing.is_empty() {
        bail!(
            "tool manifest check failed:\n  {}\nInstall with `cargo xtask check-tools --install`",
            missing.join("\n  ")
        );
    }
    Ok(())
}

fn validate(manifest: &Manifest) -> Result<()> {
    for tool in &manifest.tool {
        if tool.kind.as_deref() == Some("npm") {
            continue;
        }
        if tool.download.is_empty() {
            bail!("{}: binary tool without downloads", tool.name);
        }
        for d in &tool.download {
            if !d.url.starts_with("https://") || d.url.contains("latest") {
                bail!(
                    "{}: download URL must be a pinned https release URL: {}",
                    tool.name,
                    d.url
                );
            }
            if d.sha256.len() != 64 || !d.sha256.chars().all(|c| c.is_ascii_hexdigit()) {
                bail!("{}: malformed sha256 for {}", tool.name, d.platform);
            }
            if d.member.is_empty() || d.member.starts_with('/') || d.member.contains("..") {
                bail!("{}: unsafe archive member {}", tool.name, d.member);
            }
            if !d.url.contains(&tool.version) {
                bail!(
                    "{}: download {} does not name version {}",
                    tool.name,
                    d.url,
                    tool.version
                );
            }
        }
    }
    Ok(())
}
