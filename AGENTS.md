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

## Current Architecture

- `src/gdrive_ownership_transfer/cli.py`: CLI, Drive traversal, and ownership-transfer actions
- `src/gdrive_ownership_transfer/__main__.py`: module entrypoint for `python -m gdrive_ownership_transfer`
- `src/gdrive_ownership_transfer/conventional_commits.py`: Conventional Commit validation helpers
- `scripts/check_conventional_commit.py`: local and CI entrypoint for commit-title validation
- `tests/`: unit tests for transfer-planning and repository-policy helpers
- `.editorconfig`: editor defaults for Python, YAML, TOML, JSON, and Markdown
- `.pre-commit-config.yaml`: local contributor quality gates
- `.github/workflows/ci.yml`: lint, type-check, tests, build, and Conventional Commit validation
- `.github/workflows/security.yml`: Bandit and dependency audit checks
- `.github/dependabot.yml`: automated dependency and GitHub Actions update policy
- `SECURITY.md`: coordinated disclosure guidance
- `CONTRIBUTING.md`: contributor workflow and recommended local checks

## Behavioral Contracts

These should not change casually:

- The default command behavior remains dry-run unless `--apply` is passed.
- Consumer-account ownership transfers remain modeled as a two-step flow:
  - current owner sends pending-owner requests
  - recipient accepts ownership
- Shared-drive items must be rejected clearly because ownership transfer is not supported there.
- The authenticated account should always be shown before actionable work begins.
- Bulk folder handling must continue to process nested items individually because folder ownership alone is not sufficient.

## Workflow Expectations

Use these commands unless the task specifically requires something else:

```bash
uv sync --dev
uv run pre-commit run --all-files
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
uv run bandit -q -r src
uv build
uv lock
```

Install local hooks when working on the repo:

```bash
uv run pre-commit install --install-hooks --hook-type pre-commit --hook-type pre-push --hook-type commit-msg
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
- `mypy src`
- `pytest` with coverage floor `>= 90%`
- `bandit -q -r src`
- `uv build`

`pip-audit` runs in the GitHub security workflow and may need network access that is not always available locally.

## Security and GitHub Automation

- Keep GitHub Actions workflow permissions minimal and explicit.
- Keep Dependabot enabled for both Python dependencies and GitHub Actions.
- Prefer pinned major versions for GitHub Actions and keep them updated through Dependabot.
- Preserve `SECURITY.md` and private vulnerability reporting support.
- If GitHub-side security settings are changed, keep the repository files aligned with those settings.

## Editing Guidance

- Keep CLI output concise and actionable.
- Add tests when changing transfer-planning logic or policy checks.
- Avoid hard-coding account-specific values beyond examples and placeholders in docs.
- Document any new CLI flag in `README.md`.
- Do not silently skip unsupported Drive cases.
