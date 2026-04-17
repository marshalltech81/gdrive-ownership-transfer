# Contributing

Thanks for contributing to `gdrive-ownership-transfer`.

## Before you start

- Read [AGENTS.md](AGENTS.md) for repository guardrails and coding expectations.
- Read [SECURITY.md](SECURITY.md) for how to report vulnerabilities privately.
- Keep the tool focused on safe, auditable bulk ownership-transfer workflows for personal Google accounts.

## Local workflow

1. Install `uv`.
2. Run `uv sync --dev`.
3. Make focused changes with clear commit boundaries.
4. Run the smallest relevant checks before opening a pull request.

## Recommended checks

- `uv run pre-commit run --all-files`
- `uv run ruff check .`
- `uv run ruff format --check .`
- `uv run mypy src`
- `uv run pytest`
- `uv run bandit -q -r src`
- `uv build`

If a change affects only one area, run the smallest relevant subset and explain what you ran in the pull request.

## Pull requests

- Keep pull requests narrow and explain why the change is needed.
- Update `README.md` when CLI behavior or setup changes.
- Preserve dry-run-first behavior for mutating Drive operations.
- Do not weaken the shared-drive guardrails or the authenticated-user confirmation flow without explicit approval.

## Commit messages

Follow the repository's lowercase Conventional Commit style:

- `feat: add recursive ownership scan`
- `fix(cli): reject unsupported shared-drive roots`
- `chore(ci): add workflow linting`
- `docs: clarify recipient acceptance flow`

## Reporting security issues

Do not open public issues for vulnerabilities. Use the private reporting flow in [SECURITY.md](SECURITY.md).

