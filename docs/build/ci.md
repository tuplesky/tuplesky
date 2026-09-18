# Continuous integration and change routing (task-01)

Implements design Section 12.4.1. The workflows under `.github/workflows/`
and the scripts under `scripts/ci/` are maintained repository CI, not the
local authoring utilities excluded from the design package.

## Workflows

| Workflow | Trigger | Purpose |
|---|---|---|
| `ci.yml` | `pull_request`, `push` to `main`, `merge_group: checks_requested`, `workflow_dispatch`, weekly `schedule` | Entry point: classify, documentation checks, conditional heavy jobs, stable `CI` gate |
| `docs.yml` | `workflow_call` | Markdown links, task identifiers/anchors, plan graph consistency, Mermaid rendering with the pinned mermaid-cli |
| `build-test.yml` | `workflow_call` with `full` | Rust (x86_64 and aarch64): format, clippy, dependency policy, tests; MSRV check; Go vet/test with race detector |

All actions are pinned to commit SHAs. Pull requests run with `contents: read`,
no secrets and `persist-credentials: false`; `pull_request_target` is not
used. Scheduled and manual runs pass `full: true`, which additionally builds
release artifacts. No extended qualification campaign is registered yet and
none is advertised as passing.

## Classification

`scripts/ci/classify_changes.py` computes the complete change set with
`git diff --name-status -z --find-renames`:

* pull requests and merge groups compare the merge-base of base and head to
  the head;
* pushes compare `before` to `after`; a push without a `before` revision
  (new branch) selects full CI;
* schedule and manual runs always select full CI.

A change set is documentation-only only when every entry has status A, M, D
or R and every path, including both sides of a rename, matches the reviewed
allowlist in `scripts/ci/docs-only-policy.json`. Type changes, copies,
unmerged entries, unknown paths, spec/fixture/model/lockfile/toolchain/
workflow/filter/policy changes, an empty change set, missing history and
unsupported events all require the heavy jobs. The script attempts to recover
history (`fetch --unshallow`, fetching the exact commits) and otherwise
selects full CI; it never fails toward a documentation-only decision. Event
data reaches the script as arguments and environment data, never as
interpolated shell.

Fixtures: `scripts/ci/fixtures/classify-cases.json` (pure decisions, including
350-file change sets) and `scripts/ci/test_classify_changes.py` (real Git
repositories: merge-base versus a moved base branch, rename of code to
documentation, deletions/additions, symlink type change, more than 300 files,
empty commits, unknown commits, shallow clones).

## Required gate

Branch protection requires the single job named `CI`. It runs with
`if: always()` and `scripts/ci/evaluate_gate.py` decides:

* `classify` and `docs` must have succeeded;
* `build-test` must have succeeded, or have been skipped only when the
  classifier output `docs_only=true`.

Failure, cancellation or a missing result never becomes green. Workflow-level
`paths-ignore` is not used, so a filtered-out required check can never stay
pending.

## Developer entry points

`cargo xtask` wraps everything CI runs:

| Command | Effect |
|---|---|
| `fmt [--check]` | rustfmt and gofmt |
| `lint` | clippy with `-D warnings`, `cargo doc`, `go vet` |
| `test [--extended]` | nextest (or cargo test), doctests, Go tests; extended adds a release build |
| `check-deps [--offline]` | exact pins, roles, test-only isolation, core boundary, forbidden crates, Git sources, feature audit, `go mod verify`, cargo-deny |
| `check-tools [--install] [--skip-npm]` | verify (and optionally install) the checksummed tool manifest |
| `check-docs [--render]` | run `scripts/ci/check_docs.py` |
| `check-ci` | unit tests of the classifier, gate and documentation checker |
| `msrv` | `cargo +1.90.0 check --workspace --locked` |
| `ci` | the pull-request sequence |

The Python scripts are the CI-owned implementations so the lightweight jobs
never need the Rust toolchain; `xtask` delegates to them rather than
duplicating the checks.
