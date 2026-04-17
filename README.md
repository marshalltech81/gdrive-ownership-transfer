# Google Drive Ownership Transfer Helper

This repo contains a small CLI for bulk ownership-transfer workflows inside a Google Drive shared-folder tree.

## Security

See [SECURITY.md](/Users/marshall/src/github.com/marshalltech81/gdrive-ownership-transfer/SECURITY.md:1) for how to report vulnerabilities privately.

It is designed for the personal-account case where Google does **not** provide an admin bulk-transfer feature. The script helps with the work Google does allow through the Drive API:

- As the current owner, recursively find the items you own inside a shared folder and send ownership-transfer requests.
- As the recipient, recursively find pending ownership requests in that same folder tree and accept them with a second authenticated run.

## Important Google limitations

- This only works for files and folders in **My Drive**, not shared drives.
- For consumer Google accounts, ownership transfer requires the recipient to explicitly accept the transfer request.
- Transferring a folder does **not** transfer the ownership of the files inside it, so the script walks the tree recursively and processes every owned item individually.
- Ownership transfers to service accounts fail.

Google references:

- https://developers.google.com/workspace/drive/api/guides/transfer-file
- https://support.google.com/drive/answer/2494892

## Setup

1. Create a Google Cloud project.
2. Enable the Google Drive API.
3. Configure the OAuth consent screen.
4. Create an **OAuth client ID** of type **Desktop app**.
5. Download the client credentials JSON file.
6. If your app is in testing mode, add every Google account that will authorize it as a test user.

## Install

```bash
uv sync
```

For contributor tooling:

```bash
uv sync --dev
```

This repository is `uv`-first and includes a committed `uv.lock`. The local Python version is pinned in [.python-version](/Users/marshall/src/github.com/marshalltech81/gdrive-ownership-transfer/.python-version:1).

## Usage

The CLI has three subcommands:

- `scan`: walk the folder tree and show which items are owned by the authenticated user
- `request`: initiate ownership-transfer requests for the items the authenticated user owns
- `accept`: accept pending ownership-transfer requests as the recipient

### 1. Scan the folder tree

```bash
uv run gdrive-ownership-transfer scan \
  --folder-id YOUR_SHARED_FOLDER_ID \
  --credentials-file credentials.json
```

You can also invoke the package module directly:

```bash
uv run python -m gdrive_ownership_transfer scan \
  --folder-id YOUR_SHARED_FOLDER_ID \
  --credentials-file credentials.json
```

### 2. Send transfer requests as the current owner

Dry run first:

```bash
uv run gdrive-ownership-transfer request \
  --folder-id YOUR_SHARED_FOLDER_ID \
  --target-email new-owner@gmail.com \
  --credentials-file credentials.json
```

Apply the changes:

```bash
uv run gdrive-ownership-transfer request \
  --folder-id YOUR_SHARED_FOLDER_ID \
  --target-email new-owner@gmail.com \
  --credentials-file credentials.json \
  --apply \
  --report-file request-report.csv
```

### 3. Accept pending requests as the recipient

Use a separate token file so the recipient's login is stored independently:

```bash
uv run gdrive-ownership-transfer accept \
  --folder-id YOUR_SHARED_FOLDER_ID \
  --credentials-file credentials.json \
  --token-file .tokens/recipient.json \
  --apply \
  --report-file accept-report.csv
```

## Notes

- If the recipient does not want to run the CLI, they can still accept requests in Google Drive by searching for `pendingowner:me`.
- If you omit `--target-email` for `request`, the CLI tries to infer it from the shared folder's owner.
- The authenticated user for each run is printed at startup so you can confirm you are using the right account.
- The default behavior is a dry run. You must pass `--apply` to make changes.
- Reports are optional CSV files with one row per visited item.

## Troubleshooting

- `ModuleNotFoundError` when running local helper scripts:
  Run commands through `uv run ...` after `uv sync --dev`, or use the checked-in scripts exactly as documented in this repo.
- OAuth consent screen blocks sign-in:
  If your Google Cloud app is still in testing mode, make sure both Google accounts are added as test users.
- Recipient does not see ownership requests:
  The recipient must have a direct user permission on the item and may still need to search Google Drive for `pendingowner:me`.
- Shared-drive folders are rejected:
  This tool only supports items in My Drive. Google does not support ownership transfer for shared-drive items.
- A transferred folder did not transfer its contents:
  That is expected Google behavior. Ownership must be transferred item by item, which is why the tool walks the full tree recursively.

## Development

Common local commands:

```bash
uv sync --dev
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
uv run bandit -q -r src
uv build
```

If you use pre-commit locally:

```bash
uv run pre-commit install --install-hooks --hook-type pre-commit --hook-type pre-push --hook-type commit-msg
```

Run all hooks manually:

```bash
uv run pre-commit run --all-files
```

## Conventional Commits

This repository uses Conventional Commits for commit subjects and pull request titles.

Examples:

- `feat: add recursive pending-owner scan`
- `fix: handle shared-drive folders explicitly`
- `chore(ci): add GitHub Actions quality gates`
- `docs: clarify consumer-account ownership transfer limits`

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

## GitHub Automation

The repository includes:

- `CI`: workflow linting, Conventional Commit validation, linting, formatting, typing, tests, CLI smoke checks, and package builds
- `Security`: `bandit` and `pip-audit` on `main`, on a weekly schedule, and on manual dispatch
- `Dependabot`: weekly grouped updates for `uv` dependencies and GitHub Actions

## Project Layout

- `src/gdrive_ownership_transfer/`: package source
- `scripts/`: local repository automation helpers
- `tests/`: unit tests
- `AGENTS.md`: repository guidance for contributors and coding agents
- `CONTRIBUTING.md`: contributor workflow and commit conventions

## License

MIT
