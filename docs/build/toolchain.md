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
| Go | `go 1.26.5` directive; CI installs it from `go.mod` | `adapters/kine/go.mod` |
| cargo-deny | 0.18.9 (sha256 in manifest) | `tools/manifest.toml` |
| cargo-nextest | 0.9.99 (sha256 in manifest) | `tools/manifest.toml` |
| mermaid-cli | 11.17.0 (npm lockfile integrity) | `tools/mermaid/package-lock.json` |

Builds use `--locked` everywhere. `Cargo.lock` and `adapters/kine/go.sum` are
committed; `cargo xtask check-deps` fails when either is missing, when a
workspace dependency is not an exact `=` pin, path or Git revision, when a
member manifest declares a dependency that neither inherits from the workspace
table (`workspace = true`) nor pins it the same way, or when `go mod verify`
fails.

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
| redb | =4.2.0 | std (no default) | production state engine (coord-storage-redb) |
| fjall | =3.1.10 | lz4 (no default; audited) | experimental state engine (task-s03); linked only by the test-only coord-storage-fjall crate, listed as an external test-only crate in the dependency policy so no production, core or tool crate can reach it |
| raft-engine | git `097c499a19fbb38754c73aa2f31532329df7c0c6` | none (no default) | shared journal candidate; see audit below |
| thiserror | =2.0.20 | no default | |
| anyhow | =1.0.104 | default | binaries/tooling only |
| clap | =4.6.6 | std, derive, help, usage, error-context | tooling and binaries |
| toml | =1.1.6 | parse, serde | strict configuration |
| serde_json | =1.0.151 | default | tooling, replay bundles |
| rand_chacha / rand_core | =0.10.0 | no default | deterministic tests only; forbidden in `core` crates |
| proptest | =1.11.0 | std | dev-dependency only |
| loom | =0.7.2 | default | linked only under `cfg(loom)` (never set in production builds); the policy filters metadata by platform |
| tempfile | =3.27.0 | default | dev-dependency only |

Candidates not yet consumed by any crate (quinn, rustls, tokio, fjall, the
OIDC/JWT/HTTP stack, keyring stores, telemetry, loom, criterion, fuzzing) are
added by the tasks that first use them, with the same exact-pin rule.

## Pinned Go modules (`adapters/kine`)

Go requirements are exact by construction (`go.mod` names one version per
module and `go.sum` records its checksum; `go mod verify` runs in
`cargo xtask check-deps` and CI). Direct requirements:

| Module | Pin | Role | Notes |
|---|---|---|---|
| github.com/quic-go/quic-go | v0.62.0 | native QUIC client (task-45) | requires Go 1.26 |
| github.com/k3s-io/kine | v0.17.1-0.20260909185625-746ef418669e | frozen Kine server bridge and driver registry (task-46) | the pseudo-version of the design's compatibility reference commit `746ef418669e2131e1d4447024ac7489ee2bb5d0` (Section 6.6); its `go.mod` declares `go 1.26.5`, which raised the adapter's directive; only `pkg/server`, `pkg/drivers` and `pkg/tls` are linked, so no SQL driver, TTL worker, NATS, etcd server or Kubernetes module reaches the build graph |
| go.etcd.io/etcd/api/v3, go.etcd.io/etcd/client/v3 | v3.7.1 | etcd error metadata (`rpctypes`) and the bridge tests' client | the versions Kine's pin resolves |
| google.golang.org/grpc | v1.83.2 | the edge's gRPC server and credentials | the version Kine's pin resolves |
| lukechampine.com/blake3 | v1.4.1 | BLAKE3 derive-key for command and binding identities | pure Go, no assembly requirement; verified against the Rust fixtures |

Interface drift in Kine is resolved at this one explicit pin (plan gate
checklist): `server.Backend` at the pin carries `Watch(ctx, key, end,
revision)`, `Compact` and `WaitForSyncTo(revision)`, and `Get`/`List` take
`keysOnly`; task-47 selects the production Kine/Kubernetes pin for watch
and progress plumbing.

## Dependency policy enforced by `cargo xtask check-deps`

* Every workspace member declares `package.metadata.tuplesky.role` as
  `core`, `production`, `tool` or `test-only`.
* Crates with role `core` cannot reach tokio, redb, fjall, raft-engine,
  quinn, rustls, getrandom, rand, rand_chacha, rand_core, reqwest, axum,
  hyper or mio through normal or build dependencies (Sections 11, 16.3, 18.2).
* No `core`, `production` or `tool` crate reaches a `test-only` workspace
  crate (coord-sim, coord-store-testkit, coord-storage-fjall) or an external
  test aid (proptest, loom, arbitrary, libfuzzer-sys, criterion, quickcheck,
  fjall) through normal or build dependencies. Simulator entropy, model
  engines and the experimental state engine therefore cannot be linked into
  production artifacts.
* openssl, openssl-sys, native-tls and hyper-tls are forbidden anywhere in
  the resolved graph.
* Boundary constructors that cast plain data into a sealed capability
  (`VerifierToken::for_boundary`, `PeerProvenance::from_transport`) may appear
  in library sources only of the reviewed boundary crates (`coord-collector`,
  `coord-transport`) or of test-only crates; the scan ignores comments and
  integration tests.
* Git sources are limited to the reviewed raft-engine revision.
* Feature audit: raft-engine must resolve with an empty feature set; fjall
  must resolve exactly `lz4`.
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
| Go toolchain unspecified | `go 1.26.5` | quic-go v0.62.0 requires Go 1.26 and the pinned Kine revision declares `go 1.26.5`; `go mod tidy` raised the directive (task-45, task-46) |
| Rust minimum 1.90 | MSRV stays 1.90; exact toolchain 1.94.1 | raft-engine's own `rust-version` is 1.85; all workspace crates check with 1.90.0 |
| openidconnect 4.0.1, reqwest 0.12.28 and the other unused candidates | Not added yet | Added by first consuming task with exact pins; presence in the index was confirmed on 2026-09-18 |
| fjall 3.1.10 | Pinned at `=3.1.10` with `default-features = false, features = ["lz4"]` (task-s03) | The candidate resolves as pinned; its graph adds xxhash-rust (BSL-1.0) and varint-rs (0BSD), both permissive and now allowed in `deny.toml` with the reason recorded there. Fjall 3 has no `open_existing`: `Database::open` creates a missing database, so the adapter's lifecycle checks the directory and version marker itself before opening (opening never initializes) |
| rustls 0.23.44 | Pinned at `=0.23.45` | RUSTSEC-2026-0285 (TLS 1.3 handshake messages accepted across encryption-level boundaries) is patched in 0.23.45; quinn 0.11.11 and quinn-proto 0.11.17 accept it, and no other selection changes |
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
| RUSTSEC-2023-0071 | rsa 0.9.10 | Marvin attack: timing side channel in RSA private-key operations that can leak the key to a network observer | no patched release exists (tracked upstream since 2023); openidconnect 4.0.1 requires the crate unconditionally | only openidconnect (coord-login) uses it, for public-key verification of upstream identity-provider signatures; no RSA private key is created, held or used in the workspace, and the broker signs with ES256 through aws-lc-rs

## Reproducing

```text
cargo xtask check-tools --install   # downloads and verifies pinned tools
cargo xtask ci                      # fmt --check, lint, check-deps --offline, test, check-docs, check-ci
cargo xtask msrv                    # cargo +1.90.0 check --workspace --locked
cargo xtask check-deps              # with advisory database (network)
cargo xtask check-docs --render     # renders every Mermaid block
```
