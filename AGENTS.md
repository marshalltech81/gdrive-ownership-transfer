# AGENTS.md

## Mission

This repository exists to make a tedious Google Drive ownership-transfer task safe, repeatable, and auditable for personal Google accounts.

The tool should help a collaborator:

- find items they own inside a shared-folder tree
- initiate bulk ownership-transfer requests
- optionally help the recipient accept those requests with their own login

Favor predictable behavior, clear reporting, and conservative guardrails over clever shortcuts.

## Repository Principles

- Keep the toolchain Python-first and `uv`-native.
- Preserve a dry-run-first workflow for any operation that changes Drive permissions.
- Treat Google Drive API limitations as product constraints, not bugs to paper over.
- Prefer small, testable functions around planning, filtering, and reporting logic.
- Keep dependencies lean and avoid adding heavy frameworks for a small CLI utility.
- Optional features (e.g. `rich`) are soft dependencies gated behind `try/except ImportError` — the tool must work without them installed.
- Prefer well-established libraries and standard patterns over custom implementations — do not reinvent the wheel. If the standard library or a well-known package already solves a problem correctly, use it.

## Current Architecture

### Source

- `src/gdrive_ownership_transfer/cli.py`: CLI, Drive traversal, ownership-transfer actions, and all subcommand logic
- `src/gdrive_ownership_transfer/__main__.py`: module entrypoint for `python -m gdrive_ownership_transfer`
- `src/gdrive_ownership_transfer/conventional_commits.py`: Conventional Commit validation helpers

### Scripts and Tests

- `scripts/check_conventional_commit.py`: local and CI entrypoint for commit-title validation
- `tests/test_cli_helpers.py`: unit tests for transfer-planning, filtering, reporting, and policy helpers

### Configuration and Tooling

- `.editorconfig`: editor defaults for Python, YAML, TOML, JSON, and Markdown
- `.pre-commit-config.yaml`: local contributor quality gates
- `noxfile.py`: nox sessions for lint, format, typecheck, tests (Python 3.11–3.13), and bandit

### GitHub Automation

- `.github/workflows/ci.yml`: lint, type-check, tests, build, and Conventional Commit validation
- `.github/workflows/security.yml`: Bandit and dependency audit checks
- `.github/workflows/codex-gate.yml`: writes the required check `codex-review` from Codex's review comment (`scripts/codex-gate.sh`)
- `.github/workflows/automerge.yml`: turns on squash auto-merge for eligible Dependabot pull requests (`scripts/automerge.sh`); GitHub merges once the required checks pass and threads are resolved. Label `no-automerge` holds a pull request
- `.github/workflows/conflicts.yml`: labels pull requests that conflict with `main` as `has-conflicts` (`scripts/flag-conflicts.sh`)
- `.github/dependabot.yml`: automated dependency and GitHub Actions update policy

`CONTRIBUTING.md`, `SECURITY.md`, and other community-health files are inherited
from the account-level [`.github`](https://github.com/marshalltech81/.github) repository.

## CLI Subcommands

| Subcommand | Description |
|---|---|
| `scan` | Walk the folder tree and show items owned by the authenticated user |
| `request` | Initiate ownership-transfer requests for owned items |
| `accept` | Accept pending ownership-transfer requests as the recipient |
| `diff` | Compare two CSV report files and show items missing from the second file plus status changes |
| `revoke` | Revoke the stored OAuth token and remove the token file |
| `doctor` | Run diagnostic checks: credentials, token, Drive API, and folder access |

## Key Internal Components

- `TokenBucket`: thread-safe rate limiter used to stay within Drive API quota
- `_run_loop`: central traversal engine; handles concurrency, checkpointing, filtering, idempotency, interactive confirmation, and reporting
- `load_checkpoint` / `save_checkpoint`: JSON-backed resume state stored as a set of completed item IDs
- `plan_request` / `plan_accept`: pure functions that decide the action for one item without side effects
- `_apply_filters`: applies `--filter-mime-type`, `--filter-path`, `--exclude-mime-type`, `--exclude-path` before planning
- `_ensure_token_fresh`: mid-run OAuth refresh when the token is near expiry
- `_check_credential_permissions`: POSIX mode check; warns when credential files are world-readable
- `_notify_webhook`: POST JSON run summary to a caller-supplied URL after a run completes
- `_print_diff_table`: ASCII table of planned mutations shown by `--dry-run-diff`

## Behavioral Contracts

These should not change casually:

- The default command behavior remains dry-run unless `--apply` is passed.
- Consumer-account ownership transfers remain modeled as a two-step flow:
  - current owner sends pending-owner requests (`request`)
  - recipient accepts ownership (`accept`)
- Shared-drive items must be rejected clearly because ownership transfer is not supported there.
- The authenticated account should always be shown before actionable work begins.
- Bulk folder handling must continue to process nested items individually because folder ownership alone is not sufficient.
- `diff` is read-only and never modifies Drive or token state.
- `revoke` removes the local token file after revoking at the provider; it does not touch credentials files.
- `doctor` is read-only and always exits non-zero if any check fails.
- Conflict detection in `plan_request` must surface other pending-owner conflicts rather than silently overwriting them.
- Idempotency checks must re-fetch the item from the API before applying to prevent duplicate mutations.

## Workflow Expectations

Use these commands unless the task specifically requires something else:

```bash
uv sync --dev
uv run pre-commit run --all-files
uv run ruff check .
uv run ruff format --check .
uv run pyright src
uv run pytest
uv run bandit -q -r src
uv build
uv lock
```

Install local hooks when working on the repo:

```bash
uv run pre-commit install --install-hooks --hook-type pre-commit --hook-type pre-push --hook-type commit-msg
```

Run the nox session matrix locally:

```bash
uv run nox
```

## Conventional Commits

This repository uses Conventional Commits for commit subjects and pull request titles.

Preferred forms:

- `feat: ...`
- `fix: ...`
- `docs: ...`
- `test: ...`
- `chore(ci): ...`
- `refactor: ...`

Allowed types are:

- `build`
- `chore`
- `ci`
- `docs`
- `feat`
- `fix`
- `perf`
- `refactor`
- `revert`
- `style`
- `test`

When in doubt:

- use `feat` for user-visible capability changes
- use `fix` for behavior corrections
- use `chore(ci)` for workflow and automation updates
- use `docs` for README, AGENTS, or SECURITY content

## Quality Bar

Changes are expected to keep these checks green:

- `pre-commit run --all-files`
- `ruff check .`
- `ruff format --check .`
- `pyright src`
- `pytest` with coverage floor `>= 90%`
- `bandit -q -r src`
- `uv build`

`pip-audit` runs in the GitHub security workflow and may need network access that is not always available locally.

## Security and GitHub Automation

- Keep GitHub Actions workflow permissions minimal and explicit.
- The codex-gate, automerge and conflicts workflows run from the default branch through `workflow_run`, so a pull request's own code never runs with their write token. Do not switch them to `pull_request`. The one `pull_request_target` use is the `relabel` job in `automerge.yml`, which must keep checking out only the default branch and running nothing from the pull request.
- To hold a pull request, open it as a draft or add the `no-automerge` label before its first push; converting an open pull request to a draft also stops the merge. Adding or removing `no-automerge` later also takes effect: the `relabel` job in `automerge.yml` runs `scripts/automerge.sh` on that label change and disables or re-enables auto-merge.
- `main` does not require branches to be up to date (strict off); auto-merge is on. Dependabot pull requests need a Codex review too (`CODEX_GATE_DEPENDABOT_POLICY=require`).
- Merge (squash) a pull request by a person only on the owner's explicit go-ahead (owner, 2026-11-12; this repository is public). Dependabot is the one exception: a Dependabot pull request that updates only patch or minor versions, with Dependabot's own head commit, turns on auto-merge by itself. `scripts/automerge.sh` leaves a person's pull request alone, except that `no-automerge` turns off an auto-merge the owner enabled.
- Keep Dependabot enabled for both Python dependencies and GitHub Actions.
- Prefer pinned major versions for GitHub Actions and keep them updated through Dependabot.
- Preserve private vulnerability reporting support (shared `SECURITY.md` is inherited from the account-level `.github` repository).
- If GitHub-side security settings are changed, keep the repository files aligned with those settings.

## Editing Guidance

- Keep CLI output concise and actionable.
- Add tests when changing transfer-planning logic or policy checks.
- Avoid hard-coding account-specific values beyond examples and placeholders in docs.
- Document any new CLI flag in `README.md`.
- Do not silently skip unsupported Drive cases.
- When adding optional features that require third-party packages, gate them behind a `try/except ImportError` and add the package to the `dev` dependency group in `pyproject.toml` so contributors can install it locally.
