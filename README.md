# Google Drive Ownership Transfer Helper

This repo contains a small CLI for bulk ownership-transfer workflows inside a Google Drive shared-folder tree.

## Security

See [SECURITY.md](SECURITY.md) for how to report vulnerabilities privately.

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
git clone https://github.com/marshalltech81/gdrive-ownership-transfer
cd gdrive-ownership-transfer
uv sync
```

For contributor tooling:

```bash
uv sync --dev
```

This repository is `uv`-first and includes a committed `uv.lock`. The local Python version is pinned in [.python-version](.python-version).

## Usage

The CLI has six subcommands:

- `scan`: walk the folder tree and show which items are owned by the authenticated user
- `request`: initiate ownership-transfer requests for the items the authenticated user owns
- `accept`: accept pending ownership-transfer requests as the recipient
- `diff`: compare two CSV report files and show what changed
- `revoke`: revoke the stored OAuth token and delete the token file
- `doctor`: run diagnostic checks against credentials, token, Drive API, and folder access

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

### 4. Compare two report CSVs

```bash
uv run gdrive-ownership-transfer diff before.csv after.csv --key-field item_id
```

The two positional arguments are the paths to the CSV reports. Pass `--key-field name` to diff on a different column (default: `item_id`).

### 5. Revoke the stored OAuth token

```bash
uv run gdrive-ownership-transfer revoke \
  --credentials-file credentials.json \
  --token-file .tokens/token.json
```

### 6. Run diagnostics

```bash
uv run gdrive-ownership-transfer doctor \
  --folder-id YOUR_SHARED_FOLDER_ID \
  --credentials-file credentials.json
```

`doctor` exits non-zero if any check fails. Use it to confirm your setup before a bulk run.

## Common flags

`scan`, `request`, `accept`, and `doctor` accept these optional flags:

| Flag | Description |
|------|-------------|
| `--filter-mime-type MIME_TYPE` | Only process items of this MIME type. Repeat to allow multiple types. |
| `--filter-path PREFIX` | Only process items whose path starts with `PREFIX`. |
| `--exclude-mime-type MIME_TYPE` | Skip items of this MIME type. Repeat to exclude multiple types. |
| `--exclude-path PREFIX` | Skip items whose path starts with `PREFIX`. |
| `--output-format {text,json}` | Output format. `json` prints a JSON array to stdout; metadata goes to stderr. Default: `text`. |
| `--log-file PATH` | Write a structured JSON audit log to this path in addition to the normal report. |
| `--quiet` | Suppress skipped-item lines; dry-run, applied, and error lines still print. |
| `--page-size N` | Number of Drive API results per page (default: 100). |
| `--token-file PATH` | Token file for OAuth credentials (default: `.tokens/default.json`). |
| `--rate-limit N` | Maximum Drive API calls per 100 seconds (default: 100). |
| `--otlp-endpoint URL` | OpenTelemetry OTLP endpoint for distributed tracing (requires `opentelemetry-*` packages). |
| `--notify-webhook URL` | POST a JSON run summary to this URL after the run completes. |

`request` and `accept` also accept:

| Flag | Description |
|------|-------------|
| `--confirm` | Prompt for confirmation before applying any changes. |
| `--concurrency N` | Number of parallel Drive API calls (default: 1). |
| `--checkpoint-file PATH` | JSON file to store completed item IDs for resuming an interrupted run. |
| `--dry-run-diff` | Print a table of planned changes before applying. |
| `--interactive` | Prompt for confirmation on each item individually. |
| `--idempotency-check` | Re-fetch each item from the API before applying to avoid duplicate mutations. |

`diff` also accepts:

| Flag | Description |
|------|-------------|
| `--key-field FIELD` | CSV column to use as the join key (default: `item_id`). |

Global flag:

| Flag | Description |
|------|-------------|
| `--version` | Print the package version and exit. |

## Notes

- If the recipient does not want to run the CLI, they can still accept requests in Google Drive by searching for `pendingowner:me`.
- If you omit `--target-email` for `request`, the CLI tries to infer it from the shared folder's owner.
- The authenticated user for each run is printed at startup so you can confirm you are using the right account.
- The default behavior is a dry run. You must pass `--apply` to make changes.
- Reports are optional CSV files with one row per visited item.
- Pass `--quiet` to suppress skipped-item output and keep the terminal focused on applied changes and errors.
- Pass `--confirm` to require interactive confirmation before any changes are applied.
- Use `--output-format json` to pipe structured output into other tools; progress messages go to stderr.
- Use `--log-file` to write a timestamped JSON audit log in addition to a CSV report.
- Use `--checkpoint-file` to resume an interrupted bulk run; completed item IDs are saved after each successful apply.
- Use `--concurrency N` to parallelize Drive API calls when processing large folder trees.
- Use `--dry-run-diff` to preview every planned change as an ASCII table before committing.
- Use `--notify-webhook` to receive a JSON summary POST at the end of a run (for CI or alerting pipelines).
- Use `--rate-limit` to stay within Drive API quota on large runs.

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
- `doctor` reports a credential permission warning:
  Your credentials file is world-readable. Run `chmod 600 credentials.json` to restrict access.

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

Run the nox session matrix (lint, format, typecheck, tests, bandit):

```bash
uv run nox
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
- `noxfile.py`: nox session matrix
- `AGENTS.md`: repository guidance for contributors and coding agents
- `CONTRIBUTING.md`: contributor workflow and commit conventions

## License

MIT
