# Toolchain, locks and dependency audit (task-01)

This page records what `task-01` actually resolved and built. It is the
reviewed source of truth for pins; the design's Section 16 lists the starting
candidates. Every deviation from a candidate is listed under
[Resolved incompatibilities](#resolved-incompatibilities).

## Exact toolchains

| Component | Pin | Where |
|---|---|---|
| Rust toolchain | `1.94.1` (rustfmt, clippy; targets `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`) | `rust-toolchain.toml` |
| Declared MSRV | `rust-version = "1.90"`; verified by `cargo xtask msrv` and the `msrv` CI job with `1.90.0` | `Cargo.toml` |
| Rust edition | 2024 | `Cargo.toml` |
| Cargo resolver | 3 | `Cargo.toml` |
| Go | `go 1.26.0` directive; CI installs it from `go.mod` | `adapters/kine/go.mod` |
| cargo-deny | 0.18.9 (sha256 in manifest) | `tools/manifest.toml` |
| cargo-nextest | 0.9.99 (sha256 in manifest) | `tools/manifest.toml` |
| mermaid-cli | 11.17.0 (npm lockfile integrity) | `tools/mermaid/package-lock.json` |

Builds use `--locked` everywhere. `Cargo.lock` and `adapters/kine/go.sum` are
committed; `cargo xtask check-deps` fails when either is missing, when a
workspace dependency is not an exact `=` pin, path or Git revision, or when
`go mod verify` fails.

## Recorded compiler and platform results

Verified locally on Linux x86_64 with rustc 1.94.1 and 1.90.0 (`cargo check`
of the whole workspace with the MSRV succeeds). aarch64 is built by the
`rust (aarch64)` CI job on `ubuntu-24.04-arm`; cross-compilation is not
claimed as qualification (Section 22.2).

## Pinned Rust dependencies in the workspace graph

| Crate | Pin | Features | Notes |
|---|---|---|---|
| serde | =1.0.229 | derive, alloc (no default) | |
| postcard | =1.1.3 | alloc (no default) | wire and journal codecs |
| bytes | =1.12.1 | default | |
| blake3 | =1.8.7 | default | domain-separated identity hashing |
| redb | =4.2.0 | std (no default) | production state engine; not yet linked by a crate |
| raft-engine | git `097c499a19fbb38754c73aa2f31532329df7c0c6` | none (no default) | shared journal candidate; see audit below |
| thiserror | =2.0.20 | no default | |
| anyhow | =1.0.104 | default | binaries/tooling only |
| clap | =4.6.6 | std, derive, help, usage, error-context | tooling and binaries |
| toml | =1.1.6 | parse, serde | strict configuration |
| serde_json | =1.0.151 | default | tooling, replay bundles |
| rand_chacha / rand_core | =0.10.0 | no default | deterministic tests only; forbidden in `core` crates |
| proptest | =1.11.0 | std | dev-dependency only |
| tempfile | =3.27.0 | default | dev-dependency only |

Candidates not yet consumed by any crate (quinn, rustls, tokio, fjall, the
OIDC/JWT/HTTP stack, keyring stores, telemetry, loom, criterion, fuzzing) are
added by the tasks that first use them, with the same exact-pin rule.

## Dependency policy enforced by `cargo xtask check-deps`

* Every workspace member declares `package.metadata.tuplesky.role` as
  `core`, `production`, `tool` or `test-only`.
* Crates with role `core` cannot reach tokio, redb, fjall, raft-engine,
  quinn, rustls, getrandom, rand, rand_chacha, rand_core, reqwest, axum,
  hyper or mio through normal or build dependencies (Sections 11, 16.3, 18.2).
* No `core`, `production` or `tool` crate reaches a `test-only` workspace
  crate (coord-sim, later coord-store-testkit) or an external test aid
  (proptest, loom, arbitrary, libfuzzer-sys, criterion, quickcheck) through
  normal or build dependencies. Simulator entropy and model engines therefore
  cannot be linked into production artifacts.
* openssl, openssl-sys, native-tls and hyper-tls are forbidden anywhere in
  the resolved graph.
* Git sources are limited to the reviewed raft-engine revision.
* Feature audit: raft-engine must resolve with an empty feature set.
* `cargo deny check` applies `deny.toml` (advisories, licenses, bans,
  sources). `--offline` skips only the advisory database fetch.

## raft-engine candidate audit

Revision `097c499a19fbb38754c73aa2f31532329df7c0c6` (2026-09-10, "feat: add
optional serde codec for log entries (#411)") builds with
`default-features = false` on the pinned toolchain in about 25 seconds. Its
resolved feature set in this workspace is `[]`, so scripting
(`rhai`), `internals`, `nightly`, `swap`, `failpoints`, `serde-bincode` and
`serde-json` are all off.

The pinned commit exposes `ValueCodec`, `MessageExt<C>`,
`LogBatch::add_entries_with`, `Engine::get_entry_with` and
`Engine::fetch_entries_to_with`. `tools/raft-engine-smoke` implements a bounded
postcard `ValueCodec` against them and its tests show: appended (never
clobbered) encoding, rejection of trailing bytes and oversized payloads,
multi-group synced writes surviving reopen, `Engine::write` returning a byte
count (not a sequence), and a usable batch after a codec failure. This is a
smoke test of the engine API, not the task-j02 journal adapter.

Observed transitive facts to carry into task-j02:

* `fail` 0.5 (non-optional) pulls `rand` 0.8 and `rand_chacha` 0.3 into the
  normal graph of anything linking raft-engine. Production entropy must still
  come from OS sources; the policy forbids these crates only in `core` crates.
* `protobuf` 2.28 is present for the legacy codec; it is not used on the
  native wire (Section 16.4).
* `lz4-sys` is pinned by the engine at `=1.9.5`; `prometheus` 0.13 and
  `thiserror` 1 coexist with the workspace's newer pins (duplicate versions are
  a warning, not an error, in `deny.toml`).
* The engine manifest carries a `[patch.crates-io]` for `raft-proto`; patches
  in a dependency's manifest do not apply to this workspace and are not needed
  because `raft`/`kvproto` are dev-dependencies of the engine.

## Resolved incompatibilities

| Candidate (Section 16) | Resolution | Reason |
|---|---|---|
| Go toolchain unspecified | `go 1.26.0` | quic-go v0.62.0 requires Go 1.26; `go mod tidy` raised the directive |
| Rust minimum 1.90 | MSRV stays 1.90; exact toolchain 1.94.1 | raft-engine's own `rust-version` is 1.85; all workspace crates check with 1.90.0 |
| tokio 1.53.1, quinn 0.11.11, rustls 0.23.44, fjall 3.1.10, openidconnect 4.0.1, reqwest 0.12.28 and the other unused candidates | Not added yet | Added by first consuming task with exact pins; presence in the index was confirmed on 2026-09-18 |
| `toml` requirement `=1.1.6` | Kept | Resolves to `1.1.6+spec-1.1.0` |
| Workspace license `FSL-1.1-ALv2` | `deny.toml` ignores private (`publish = false`) crates | Not an SPDX identifier; not a third-party dependency |

## Accepted advisories

`cargo deny check` runs with the live RustSec database in CI. Advisories with
no upgrade path are ignored in `deny.toml` only after review, with the reason
recorded here. Every entry is accepted for development and test builds and is
to be resolved or re-justified before a production release.

| Advisory | Crate | Nature | Why no upgrade | Why unreachable here |
|---|---|---|---|---|
| RUSTSEC-2024-0437 | protobuf 2.28.0 | stack overflow (denial of service) when skipping unknown group fields in untrusted input; patched in 3.7.2+ | the pinned raft-engine revision requires protobuf 2 directly and through prometheus 0.13; the 2.x line is unmaintained | raft-engine is a dependency of `tools/raft-engine-smoke` only, no daemon or library crate links it, and the engine decodes only its own on-disk log entries |

## Reproducing

```text
cargo xtask check-tools --install   # downloads and verifies pinned tools
cargo xtask ci                      # fmt --check, lint, check-deps --offline, test, check-docs, check-ci
cargo xtask msrv                    # cargo +1.90.0 check --workspace --locked
cargo xtask check-deps              # with advisory database (network)
cargo xtask check-docs --render     # renders every Mermaid block
```
