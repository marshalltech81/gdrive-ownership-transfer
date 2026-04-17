from __future__ import annotations

import argparse
import concurrent.futures
import csv
import json
import os
import random
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError

try:
    from importlib.metadata import version as _pkg_version

    _VERSION = _pkg_version("gdrive-ownership-transfer")
except Exception:
    _VERSION = "unknown"

# ---------------------------------------------------------------------------
# Optional dependencies
# ---------------------------------------------------------------------------

try:
    from rich.console import Console as _RichConsole
    from rich.progress import MofNCompleteColumn, Progress, SpinnerColumn, TextColumn
    from rich.prompt import Confirm as _RichConfirm

    _RICH_AVAILABLE = True
    _rich_err_console = _RichConsole(stderr=True)
except ImportError:
    _RICH_AVAILABLE = False
    _rich_err_console = None  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Constants and module-level state
# ---------------------------------------------------------------------------

SCOPES = ["https://www.googleapis.com/auth/drive"]
FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
RETRYABLE_STATUSES = {429, 500, 502, 503, 504}
_EXPIRY_WARN_SECONDS = 300
_IDEMPOTENCY_FIELDS = (
    "id,name,mimeType,ownedByMe,driveId,permissions(id,type,emailAddress,role,pendingOwner)"
)

ActionType = Literal["skip", "create-permission", "update-permission", "accept-transfer"]

_rate_bucket: TokenBucket | None = None


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DriveItem:
    id: str
    name: str
    mime_type: str
    path: str
    owned_by_me: bool
    drive_id: str | None
    permissions: tuple[dict[str, Any], ...]

    @property
    def is_folder(self) -> bool:
        return self.mime_type == FOLDER_MIME_TYPE


@dataclass(frozen=True)
class ActionPlan:
    action: ActionType
    detail: str
    permission_id: str | None = None


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------


class TokenBucket:
    """Thread-safe token bucket for proactive Drive API rate limiting."""

    def __init__(self, rate: float, per_seconds: float = 100.0) -> None:
        self._rate = rate
        self._per_seconds = per_seconds
        # Capacity is at least 1 so fractional rates (e.g. 0.5 req/100s) can
        # still issue the first request immediately instead of sleeping forever.
        self._capacity = max(1.0, rate)
        self._tokens = self._capacity
        self._last_check = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        wait = 0.0
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_check
            self._last_check = now
            self._tokens = min(
                self._capacity, self._tokens + elapsed * self._rate / self._per_seconds
            )
            if self._tokens >= 1.0:
                self._tokens -= 1.0
            else:
                deficit = 1.0 - self._tokens
                wait = deficit * self._per_seconds / self._rate
                self._tokens = 0.0
        if wait > 0.0:
            time.sleep(wait)


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Bulk-initiate and accept Google Drive ownership transfers inside a shared-folder tree."
        )
    )
    parser.add_argument(
        "--version", action="version", version=f"gdrive-ownership-transfer {_VERSION}"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    scan_parser = subparsers.add_parser("scan", help="List items inside a folder tree.")
    add_common_args(scan_parser)
    scan_parser.add_argument(
        "--owned-only",
        action="store_true",
        help="Only print items owned by the authenticated user.",
    )

    request_parser = subparsers.add_parser(
        "request",
        help="Initiate pending-owner requests for items owned by the authenticated user.",
    )
    add_common_args(request_parser)
    request_parser.add_argument(
        "--target-email",
        help="Recipient email. If omitted, the CLI tries to infer the shared folder owner.",
    )
    request_parser.add_argument(
        "--email-message",
        help="Optional plain-text note to include in Google's notification email.",
    )
    request_parser.add_argument(
        "--apply",
        action="store_true",
        help="Perform the transfer-request calls. Without this flag, the command is a dry run.",
    )
    request_parser.add_argument(
        "--confirm",
        action="store_true",
        help="Prompt for confirmation before applying mutations (requires --apply).",
    )
    _add_mutation_args(request_parser)

    accept_parser = subparsers.add_parser(
        "accept",
        help="Accept pending ownership requests as the recipient.",
    )
    add_common_args(accept_parser)
    accept_parser.add_argument(
        "--apply",
        action="store_true",
        help="Perform the ownership-acceptance calls. Without this flag, the command is a dry run.",
    )
    accept_parser.add_argument(
        "--confirm",
        action="store_true",
        help="Prompt for confirmation before applying mutations (requires --apply).",
    )
    _add_mutation_args(accept_parser)

    diff_parser = subparsers.add_parser(
        "diff",
        help="Compare two CSV reports and show items present in the first but not the second.",
    )
    diff_parser.add_argument("csv_a", type=Path, help="First CSV report.")
    diff_parser.add_argument("csv_b", type=Path, help="Second CSV report.")
    diff_parser.add_argument(
        "--key-field",
        default="item_id",
        help="CSV column to use as the unique key (default: item_id).",
    )

    revoke_parser = subparsers.add_parser(
        "revoke",
        help="Revoke the stored OAuth token and delete the local token file.",
    )
    revoke_parser.add_argument(
        "--credentials-file",
        type=Path,
        default=Path("credentials.json"),
        help="Path to the Desktop OAuth client JSON file.",
    )
    revoke_parser.add_argument(
        "--token-file",
        type=Path,
        default=Path(".tokens/default.json"),
        help="Path to the cached OAuth token file to revoke and delete.",
    )

    doctor_parser = subparsers.add_parser(
        "doctor",
        help="Run diagnostic checks: credentials, token, Drive API reachability, folder access.",
    )
    add_common_args(doctor_parser)

    return parser


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--folder-id",
        required=True,
        help="Drive folder ID for the shared folder to walk recursively.",
    )
    parser.add_argument(
        "--credentials-file",
        type=Path,
        default=Path("credentials.json"),
        help="Path to the Desktop OAuth client JSON file.",
    )
    parser.add_argument(
        "--token-file",
        type=Path,
        default=Path(".tokens/default.json"),
        help="Path where the OAuth refresh token should be cached.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        help="Page size for Drive API list calls (1–1000).",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=None,
        help="Optional cap on how many actionable items to mutate when --apply is used.",
    )
    parser.add_argument(
        "--filter-mime-type",
        action="append",
        dest="mime_types",
        metavar="MIME_TYPE",
        help=(
            "Only process items with this MIME type. May be repeated. "
            "Folders are always traversed regardless of this filter."
        ),
    )
    parser.add_argument(
        "--filter-path",
        dest="path_prefix",
        metavar="PREFIX",
        help="Only process items whose path starts with PREFIX.",
    )
    parser.add_argument(
        "--exclude-mime-type",
        action="append",
        dest="exclude_mime_types",
        metavar="MIME_TYPE",
        help="Exclude items with this MIME type. May be repeated.",
    )
    parser.add_argument(
        "--exclude-path",
        dest="exclude_path_prefix",
        metavar="PREFIX",
        help="Exclude items whose path starts with PREFIX.",
    )
    parser.add_argument(
        "--output-format",
        choices=["text", "json"],
        default="text",
        help=(
            "Output format for results (default: text). "
            "When 'json', per-item lines go to stderr and results are printed as JSON to stdout."
        ),
    )
    parser.add_argument(
        "--report-file",
        type=Path,
        help="Optional CSV report path.",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        help="Optional structured JSON log path.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help=(
            "Suppress skipped and not-owned-by-me output. "
            "Dry-run, applied, and error lines still print."
        ),
    )
    parser.add_argument(
        "--notify-webhook",
        metavar="URL",
        help="POST a JSON summary to this URL when the run completes.",
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        metavar="REQ_PER_100S",
        default=None,
        help="Maximum Drive API requests per 100 seconds. Proactively sleeps to stay under quota.",
    )


def _add_mutation_args(parser: argparse.ArgumentParser) -> None:
    """Extra flags for request and accept subcommands."""
    parser.add_argument(
        "--concurrency",
        type=int,
        default=1,
        metavar="N",
        help="Number of Drive API calls to make in parallel (default: 1).",
    )
    parser.add_argument(
        "--checkpoint-file",
        type=Path,
        default=None,
        metavar="PATH",
        help=(
            "Resume a previous run from a checkpoint file. "
            "Items already marked complete are skipped."
        ),
    )
    parser.add_argument(
        "--dry-run-diff",
        action="store_true",
        help="Show planned mutations as a table instead of per-item lines.",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Prompt for confirmation before each individual mutation.",
    )
    parser.add_argument(
        "--idempotency-check",
        action="store_true",
        help="Re-fetch each item's permissions before applying to skip already-complete mutations.",
    )


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main() -> int:  # noqa: C901
    global _rate_bucket
    _rate_bucket = None

    parser = build_parser()
    args = parser.parse_args()

    if args.command == "diff":
        return run_diff(args.csv_a, args.csv_b, key_field=args.key_field)

    if args.command == "revoke":
        return run_auth_revoke(token_file=args.token_file)

    if args.page_size < 1 or args.page_size > 1000:
        raise SystemExit("--page-size must be between 1 and 1000.")

    if getattr(args, "concurrency", 1) < 1:
        raise SystemExit("--concurrency must be at least 1.")

    if getattr(args, "interactive", False) and getattr(args, "concurrency", 1) > 1:
        raise SystemExit("--interactive cannot be combined with --concurrency > 1.")

    if getattr(args, "rate_limit", None) is not None and args.rate_limit <= 0:
        raise SystemExit("--rate-limit must be a positive number.")

    if args.rate_limit:
        _rate_bucket = TokenBucket(args.rate_limit)

    credentials = load_credentials(args.credentials_file, args.token_file)
    service = build_drive_service(credentials)

    if args.command == "doctor":
        return run_doctor(
            service,
            credentials,
            credentials_file=args.credentials_file,
            token_file=args.token_file,
            folder_id=args.folder_id,
        )

    me = execute_with_retries(
        lambda: service.about().get(fields="user(emailAddress,displayName)").execute()
    )["user"]
    root = get_file(
        service,
        args.folder_id,
        fields=(
            "id,name,mimeType,ownedByMe,driveId,"
            "owners(emailAddress,displayName),"
            "permissions(id,type,emailAddress,role,pendingOwner)"
        ),
    )

    if root["mimeType"] != FOLDER_MIME_TYPE:
        raise SystemExit("--folder-id must point to a Google Drive folder.")
    if root.get("driveId"):
        raise SystemExit(
            "This folder is in a shared drive. Google does not support ownership "
            "transfers for shared-drive items."
        )

    _meta_out = sys.stderr if args.output_format == "json" else sys.stdout
    print(f"Authenticated as: {format_user(me)}", file=_meta_out)
    print(f"Root folder: {root['name']} ({root['id']})", file=_meta_out)

    confirm = getattr(args, "confirm", False)
    common = dict(
        page_size=args.page_size,
        quiet=args.quiet,
        output_format=args.output_format,
        mime_types=args.mime_types,
        path_prefix=args.path_prefix,
        exclude_mime_types=getattr(args, "exclude_mime_types", None),
        exclude_path_prefix=getattr(args, "exclude_path_prefix", None),
        notify_webhook=getattr(args, "notify_webhook", None),
    )

    if args.command == "scan":
        rows = run_scan(service, root, owned_only=args.owned_only, **common)
    elif args.command == "request":
        target_email = args.target_email or infer_target_email(root, me.get("emailAddress"))
        print(f"Target owner: {target_email}", file=_meta_out)
        print("Mode: apply" if args.apply else "Mode: dry-run", file=_meta_out)
        rows = run_request(
            service,
            root,
            target_email=target_email,
            apply=args.apply,
            max_items=args.max_items,
            email_message=args.email_message,
            confirm=confirm,
            concurrency=args.concurrency,
            checkpoint_file=args.checkpoint_file,
            dry_run_diff=args.dry_run_diff,
            interactive=args.interactive,
            idempotency_check=args.idempotency_check,
            credentials=credentials,
            **common,
        )
    elif args.command == "accept":
        print("Mode: apply" if args.apply else "Mode: dry-run", file=_meta_out)
        rows = run_accept(
            service,
            root,
            recipient_email=me["emailAddress"],
            apply=args.apply,
            max_items=args.max_items,
            confirm=confirm,
            concurrency=args.concurrency,
            checkpoint_file=args.checkpoint_file,
            dry_run_diff=args.dry_run_diff,
            interactive=args.interactive,
            idempotency_check=args.idempotency_check,
            credentials=credentials,
            **common,
        )
    else:
        raise SystemExit(f"Unknown command: {args.command!r}")

    if args.output_format == "json":
        print(json.dumps(rows, indent=2))
    else:
        print_summary(rows)

    if args.report_file:
        write_report(args.report_file, rows)
        print(f"Report written: {args.report_file}", file=_meta_out)
    if args.log_file:
        write_json_log(args.log_file, rows)
        print(f"Log written: {args.log_file}", file=_meta_out)
    if common.get("notify_webhook"):
        _notify_webhook(common["notify_webhook"], rows, command=args.command)

    return 0


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------


def load_credentials(credentials_file: Path, token_file: Path) -> Credentials:
    if not credentials_file.exists():
        raise SystemExit(
            f"OAuth client file not found: {credentials_file}. "
            "Create a Desktop OAuth client in Google Cloud and pass --credentials-file."
        )

    _check_credential_permissions(credentials_file)

    credentials: Credentials | None = None
    if token_file.exists():
        try:
            credentials = Credentials.from_authorized_user_file(str(token_file), SCOPES)
        except Exception:
            print(
                f"Warning: token file {token_file} is invalid or unreadable — re-authenticating.",
                file=sys.stderr,
            )

    if credentials and credentials.valid:
        _warn_if_expiring_soon(credentials)
        return credentials

    if credentials and credentials.expired and credentials.refresh_token:
        credentials.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(str(credentials_file), SCOPES)
        credentials = flow.run_local_server(port=0)

    token_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(token_file.parent, 0o700)
    except OSError:
        pass
    token_file.write_text(credentials.to_json(), encoding="utf-8")
    try:
        os.chmod(token_file, 0o600)
    except OSError:
        pass
    return credentials


def _check_credential_permissions(credentials_file: Path) -> None:
    """Warn if credentials.json is readable by group or others on POSIX systems."""
    if os.name != "posix":
        return
    try:
        mode = credentials_file.stat().st_mode
        if mode & 0o044:
            print(
                f"Warning: {credentials_file} is readable by group or others (mode "
                f"{oct(mode & 0o777)}). Consider: chmod 600 {credentials_file}",
                file=sys.stderr,
            )
    except OSError:
        pass


def _warn_if_expiring_soon(credentials: Credentials) -> None:
    expiry = getattr(credentials, "expiry", None)
    if expiry is None:
        return
    remaining = expiry.replace(tzinfo=UTC) - datetime.now(UTC)
    if remaining.total_seconds() < _EXPIRY_WARN_SECONDS:
        secs = int(remaining.total_seconds())
        print(
            f"Warning: OAuth token expires in {secs}s — consider deleting "
            f"the token file and re-authenticating.",
            file=sys.stderr,
        )


def _ensure_token_fresh(credentials: Credentials) -> None:
    """Proactively refresh the token mid-run if it will expire soon."""
    expiry = getattr(credentials, "expiry", None)
    if expiry is None:
        return
    remaining = expiry.replace(tzinfo=UTC) - datetime.now(UTC)
    if remaining.total_seconds() < _EXPIRY_WARN_SECONDS and credentials.refresh_token:
        try:
            credentials.refresh(Request())
        except Exception as exc:
            print(f"Warning: mid-run token refresh failed: {exc}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Drive API helpers
# ---------------------------------------------------------------------------


def build_drive_service(credentials: Credentials) -> Resource:
    return build("drive", "v3", credentials=credentials, cache_discovery=False)


def execute_with_retries(request_fn: Callable[[], Any], attempts: int = 5) -> Any:
    for attempt in range(attempts):
        try:
            if _rate_bucket is not None:
                _rate_bucket.acquire()
            return request_fn()
        except HttpError as exc:
            if not is_retryable(exc) or attempt == attempts - 1:
                raise
            time.sleep(2**attempt + random.uniform(0, 1))  # nosec B311
    raise RuntimeError("Unexpected retry loop exit")


def is_retryable(exc: HttpError) -> bool:
    status = getattr(exc.resp, "status", None)
    if status in RETRYABLE_STATUSES:
        return True
    if status != 403:
        return False

    try:
        payload = json.loads(exc.content.decode("utf-8"))
    except Exception:
        return False

    reasons = {
        error.get("reason", "")
        for error in payload.get("error", {}).get("errors", [])
        if isinstance(error, dict)
    }
    return bool(reasons & {"userRateLimitExceeded", "rateLimitExceeded", "backendError"})


def get_file(service: Resource, file_id: str, fields: str) -> dict[str, Any]:
    return cast(
        dict[str, Any],
        execute_with_retries(
            lambda: (
                service.files().get(fileId=file_id, supportsAllDrives=True, fields=fields).execute()
            )
        ),
    )


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------


def _apply_filters(
    items: list[DriveItem],
    *,
    mime_types: list[str] | None,
    path_prefix: str | None,
    exclude_mime_types: list[str] | None = None,
    exclude_path_prefix: str | None = None,
) -> list[DriveItem]:
    result = items
    if mime_types:
        result = [item for item in result if item.mime_type in mime_types]
    if path_prefix:
        result = [item for item in result if item.path.startswith(path_prefix)]
    if exclude_mime_types:
        result = [item for item in result if item.mime_type not in exclude_mime_types]
    if exclude_path_prefix:
        result = [item for item in result if not item.path.startswith(exclude_path_prefix)]
    return result


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------


def load_checkpoint(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return set(data.get("completed_ids", []))
    except Exception:
        print(f"Warning: could not read checkpoint file {path} — starting fresh.", file=sys.stderr)
        return set()


def save_checkpoint(path: Path, completed_ids: set[str]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"completed_ids": sorted(completed_ids)}, indent=2), encoding="utf-8"
        )
    except OSError as exc:
        print(f"Warning: could not save checkpoint to {path}: {exc}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Tree traversal
# ---------------------------------------------------------------------------


def walk_tree(service: Resource, root: dict[str, Any], page_size: int) -> Iterator[DriveItem]:
    stack: list[tuple[dict[str, Any], str]] = [(root, root["name"])]
    seen_ids: set[str] = set()

    while stack:
        current, current_path = stack.pop()
        current_id = current["id"]
        if current_id in seen_ids:
            continue
        seen_ids.add(current_id)

        item = DriveItem(
            id=current_id,
            name=current["name"],
            mime_type=current["mimeType"],
            path=current_path,
            owned_by_me=current.get("ownedByMe", False),
            drive_id=current.get("driveId"),
            permissions=tuple(current.get("permissions", [])),
        )
        yield item

        if not item.is_folder:
            continue

        children = list_children(service, current_id, page_size=page_size)
        for child in reversed(children):
            child_path = f"{current_path}/{child['name']}"
            stack.append((child, child_path))


def list_children(service: Resource, parent_id: str, page_size: int) -> list[dict[str, Any]]:
    children: list[dict[str, Any]] = []
    page_token: str | None = None
    fields = (
        "nextPageToken,"
        "files(id,name,mimeType,ownedByMe,driveId,"
        "permissions(id,type,emailAddress,role,pendingOwner),"
        "owners(emailAddress,displayName))"
    )

    while True:
        page_token_for_request = page_token

        def request_page(page_token: str | None = page_token_for_request) -> dict[str, Any]:
            return cast(
                dict[str, Any],
                service.files()
                .list(
                    q=f"'{parent_id}' in parents and trashed = false",
                    fields=fields,
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                    pageSize=page_size,
                    pageToken=page_token,
                )
                .execute(),
            )

        response = execute_with_retries(request_page)
        children.extend(response.get("files", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    children.sort(key=lambda child: (child["mimeType"] != FOLDER_MIME_TYPE, child["name"].lower()))
    return children


def _collect_items_with_progress(
    service: Resource,
    root: dict[str, Any],
    *,
    page_size: int,
    output_format: str,
) -> list[DriveItem]:
    items: list[DriveItem] = []

    if _RICH_AVAILABLE and output_format == "text" and sys.stderr.isatty():
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            MofNCompleteColumn(),
            console=_rich_err_console,
            transient=True,
        ) as progress:
            task = progress.add_task("[cyan]Scanning…", total=None)
            for item in walk_tree(service, root, page_size=page_size):
                items.append(item)
                progress.update(task, completed=len(items), description="[cyan]Scanning…")
        print(f"[scanning] {len(items)} items found.", file=sys.stderr)
        return items

    use_progress = output_format == "text" and sys.stderr.isatty()
    for item in walk_tree(service, root, page_size=page_size):
        items.append(item)
        if use_progress:
            print(f"\r[scanning] {len(items)} items...", end="", file=sys.stderr, flush=True)
    if use_progress and items:
        print(f"\r[scanning] {len(items)} items found.    ", file=sys.stderr)
    return items


# ---------------------------------------------------------------------------
# run_scan
# ---------------------------------------------------------------------------


def run_scan(
    service: Resource,
    root: dict[str, Any],
    *,
    page_size: int,
    owned_only: bool,
    quiet: bool,
    output_format: str,
    mime_types: list[str] | None,
    path_prefix: str | None,
    exclude_mime_types: list[str] | None = None,
    exclude_path_prefix: str | None = None,
    notify_webhook: str | None = None,
) -> list[dict[str, str]]:
    _out = sys.stderr if output_format == "json" else sys.stdout
    all_items = _collect_items_with_progress(
        service, root, page_size=page_size, output_format=output_format
    )
    items = _apply_filters(
        all_items,
        mime_types=mime_types,
        path_prefix=path_prefix,
        exclude_mime_types=exclude_mime_types,
        exclude_path_prefix=exclude_path_prefix,
    )
    rows: list[dict[str, str]] = []
    for item in items:
        status = "owned-by-me" if item.owned_by_me else "not-owned-by-me"
        if owned_only and not item.owned_by_me:
            continue
        if not quiet or item.owned_by_me:
            print(f"[{status}] {item.path}", file=_out)
        rows.append(
            {
                "path": item.path,
                "item_id": item.id,
                "mime_type": item.mime_type,
                "action": "scan",
                "status": status,
                "detail": "",
            }
        )
    return rows


# ---------------------------------------------------------------------------
# _run_loop
# ---------------------------------------------------------------------------


def _run_loop(  # noqa: C901
    service: Resource,
    root: dict[str, Any],
    *,
    page_size: int,
    apply: bool,
    max_items: int | None,
    quiet: bool,
    output_format: str,
    mime_types: list[str] | None,
    path_prefix: str | None,
    exclude_mime_types: list[str] | None,
    exclude_path_prefix: str | None,
    confirm: bool,
    concurrency: int,
    checkpoint_file: Path | None,
    dry_run_diff: bool,
    interactive: bool,
    idempotency_check: bool,
    plan_fn: Callable[[DriveItem], ActionPlan],
    apply_fn: Callable[[DriveItem, ActionPlan], None],
    credentials: Credentials | None = None,
) -> list[dict[str, str]]:
    _out = sys.stderr if output_format == "json" else sys.stdout

    completed_ids: set[str] = set()
    if checkpoint_file is not None:
        completed_ids = load_checkpoint(checkpoint_file)
        if completed_ids:
            print(
                f"[resume] Skipping {len(completed_ids)} already-completed item(s).",
                file=_out,
            )

    all_items = _collect_items_with_progress(
        service, root, page_size=page_size, output_format=output_format
    )
    items = _apply_filters(
        all_items,
        mime_types=mime_types,
        path_prefix=path_prefix,
        exclude_mime_types=exclude_mime_types,
        exclude_path_prefix=exclude_path_prefix,
    )

    # Skip already-completed items from checkpoint
    if completed_ids:
        items = [item for item in items if item.id not in completed_ids]

    planned = [(item, plan_fn(item)) for item in items]

    # Batch confirm prompt
    if confirm and apply:
        actionable = [(item, plan) for item, plan in planned if plan.action != "skip"]
        if actionable:
            n = len(actionable)
            print(
                f"\n{n} item{'s' if n != 1 else ''} will be modified. Proceed? [y/N] ",
                end="",
                flush=True,
                file=sys.stderr,
            )
            if input().strip().lower() != "y":
                raise SystemExit("Aborted.")

    # Dry-run diff table mode — show table instead of per-item lines
    if dry_run_diff and not apply:
        _print_diff_table(planned, _out)
        rows: list[dict[str, str]] = []
        for item, plan in planned:
            row = make_row(item, action=plan.action, status="dry-run", detail=plan.detail)
            if plan.action == "skip":
                row["status"] = "skipped"
            rows.append(row)
        return rows

    checkpoint_lock = threading.Lock()

    def _apply_single(
        item: DriveItem,
        plan: ActionPlan,
        attempted_ref: list[int],
        count_lock: threading.Lock,
        print_lock: threading.Lock,
    ) -> dict[str, str]:
        row = make_row(item, action=plan.action, status="planned", detail=plan.detail)

        if plan.action == "skip":
            row["status"] = "skipped"
            if not quiet:
                with print_lock:
                    print(f"[skip] {item.path} :: {plan.detail}", file=_out)
            return row

        # Early bail without reserving a slot — exact cap enforcement happens below.
        with count_lock:
            if max_items is not None and attempted_ref[0] >= max_items:
                row["status"] = "skipped"
                row["detail"] = f"{plan.detail}; max-items reached"
                if not quiet:
                    with print_lock:
                        print(f"[skip] {item.path} :: max-items reached", file=_out)
                return row

        if not apply:
            row["status"] = "dry-run"
            with print_lock:
                print(f"[dry-run] {item.path} :: {plan.detail}", file=_out)
            return row

        # Interactive per-item confirmation
        if interactive:
            with print_lock:
                if _RICH_AVAILABLE and _rich_err_console is not None:
                    if not _RichConfirm.ask(
                        f"[bold]{item.path}[/bold] — {plan.action}: {plan.detail} — Apply?",
                        console=_rich_err_console,
                        default=False,
                    ):
                        row["status"] = "skipped"
                        row["detail"] = "skipped interactively"
                        return row
                else:
                    print(
                        f"[interactive] {item.path} :: {plan.action}: {plan.detail}\nApply? [y/N] ",
                        end="",
                        flush=True,
                        file=sys.stderr,
                    )
                    if input().strip().lower() != "y":
                        row["status"] = "skipped"
                        row["detail"] = "skipped interactively"
                        return row

        # Idempotency re-check before applying
        if idempotency_check:
            try:
                fresh_data = get_file(service, item.id, fields=_IDEMPOTENCY_FIELDS)
                fresh_item = _dict_to_drive_item(fresh_data, item.path)
                fresh_plan = plan_fn(fresh_item)
                if fresh_plan.action == "skip":
                    row["status"] = "skipped"
                    row["detail"] = f"idempotency check: {fresh_plan.detail}"
                    if not quiet:
                        with print_lock:
                            print(f"[skip] {item.path} :: {row['detail']}", file=_out)
                    return row
                plan_to_use = fresh_plan
            except HttpError:
                plan_to_use = plan
        else:
            plan_to_use = plan

        # Reserve a slot atomically right before the API call so interactive/
        # idempotency skips do not consume a max-items slot.
        with count_lock:
            if max_items is not None and attempted_ref[0] >= max_items:
                row["status"] = "skipped"
                row["detail"] = f"{plan.detail}; max-items reached"
                if not quiet:
                    with print_lock:
                        print(f"[skip] {item.path} :: max-items reached", file=_out)
                return row
            attempted_ref[0] += 1

        # Proactive token refresh mid-run
        if credentials is not None:
            _ensure_token_fresh(credentials)

        try:
            apply_fn(item, plan_to_use)
            row["status"] = "applied"
            with print_lock:
                print(f"[applied] {item.path} :: {plan_to_use.detail}", file=_out)
            if checkpoint_file is not None:
                with checkpoint_lock:
                    completed_ids.add(item.id)
                    save_checkpoint(checkpoint_file, completed_ids)
        except HttpError as exc:
            row["status"] = "error"
            row["detail"] = format_http_error(exc)
            with print_lock:
                print(f"[error] {item.path} :: {row['detail']}", file=_out)
        except Exception as exc:
            row["status"] = "error"
            row["detail"] = str(exc) or exc.__class__.__name__
            with print_lock:
                print(f"[error] {item.path} :: {row['detail']}", file=_out)
        return row

    attempted_ref = [0]
    count_lock = threading.Lock()
    print_lock = threading.Lock()
    rows = []

    if concurrency > 1 and apply:
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            future_to_idx = {
                executor.submit(_apply_single, item, plan, attempted_ref, count_lock, print_lock): i
                for i, (item, plan) in enumerate(planned)
            }
            results: list[dict[str, str] | None] = [None] * len(planned)
            for future in concurrent.futures.as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    results[idx] = future.result()
                except Exception as exc:
                    item, plan = planned[idx]
                    row = make_row(item, action=plan.action, status="error", detail=str(exc))
                    results[idx] = row
            rows = [r for r in results if r is not None]
    else:
        for item, plan in planned:
            rows.append(_apply_single(item, plan, attempted_ref, count_lock, print_lock))

    return rows


def _print_diff_table(planned: list[tuple[DriveItem, ActionPlan]], _out: Any) -> None:
    actionable = [(item, plan) for item, plan in planned if plan.action != "skip"]
    if not actionable:
        print("(no actionable items)", file=_out)
        return

    col_path = max(max(len(item.path) for item, _ in actionable), 4)
    col_action = max(max(len(plan.action) for _, plan in actionable), 6)

    header = f"{'PATH':<{col_path}}  {'ACTION':<{col_action}}  PLANNED CHANGE"
    print(header, file=_out)
    print("─" * min(len(header) + 20, 120), file=_out)
    for item, plan in actionable:
        print(f"{item.path:<{col_path}}  {plan.action:<{col_action}}  {plan.detail}", file=_out)


def _dict_to_drive_item(data: dict[str, Any], path: str) -> DriveItem:
    return DriveItem(
        id=data["id"],
        name=data["name"],
        mime_type=data["mimeType"],
        path=path,
        owned_by_me=data.get("ownedByMe", False),
        drive_id=data.get("driveId"),
        permissions=tuple(data.get("permissions", [])),
    )


# ---------------------------------------------------------------------------
# run_request / run_accept
# ---------------------------------------------------------------------------


def run_request(
    service: Resource,
    root: dict[str, Any],
    *,
    target_email: str,
    page_size: int,
    apply: bool,
    max_items: int | None,
    email_message: str | None,
    quiet: bool,
    output_format: str,
    mime_types: list[str] | None,
    path_prefix: str | None,
    confirm: bool,
    exclude_mime_types: list[str] | None = None,
    exclude_path_prefix: str | None = None,
    notify_webhook: str | None = None,
    concurrency: int = 1,
    checkpoint_file: Path | None = None,
    dry_run_diff: bool = False,
    interactive: bool = False,
    idempotency_check: bool = False,
    credentials: Credentials | None = None,
) -> list[dict[str, str]]:
    return _run_loop(
        service,
        root,
        page_size=page_size,
        apply=apply,
        max_items=max_items,
        quiet=quiet,
        output_format=output_format,
        mime_types=mime_types,
        path_prefix=path_prefix,
        exclude_mime_types=exclude_mime_types,
        exclude_path_prefix=exclude_path_prefix,
        confirm=confirm,
        concurrency=concurrency,
        checkpoint_file=checkpoint_file,
        dry_run_diff=dry_run_diff,
        interactive=interactive,
        idempotency_check=idempotency_check,
        plan_fn=lambda item: plan_request(item, target_email),
        apply_fn=lambda item, plan: apply_request_plan(
            service, item, target_email=target_email, plan=plan, email_message=email_message
        ),
        credentials=credentials,
    )


def run_accept(
    service: Resource,
    root: dict[str, Any],
    *,
    recipient_email: str,
    page_size: int,
    apply: bool,
    max_items: int | None,
    quiet: bool,
    output_format: str,
    mime_types: list[str] | None,
    path_prefix: str | None,
    confirm: bool,
    exclude_mime_types: list[str] | None = None,
    exclude_path_prefix: str | None = None,
    notify_webhook: str | None = None,
    concurrency: int = 1,
    checkpoint_file: Path | None = None,
    dry_run_diff: bool = False,
    interactive: bool = False,
    idempotency_check: bool = False,
    credentials: Credentials | None = None,
) -> list[dict[str, str]]:
    return _run_loop(
        service,
        root,
        page_size=page_size,
        apply=apply,
        max_items=max_items,
        quiet=quiet,
        output_format=output_format,
        mime_types=mime_types,
        path_prefix=path_prefix,
        exclude_mime_types=exclude_mime_types,
        exclude_path_prefix=exclude_path_prefix,
        confirm=confirm,
        concurrency=concurrency,
        checkpoint_file=checkpoint_file,
        dry_run_diff=dry_run_diff,
        interactive=interactive,
        idempotency_check=idempotency_check,
        plan_fn=lambda item: plan_accept(item, recipient_email),
        apply_fn=lambda item, plan: apply_accept_plan(service, item, plan),
        credentials=credentials,
    )


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------


def plan_request(item: DriveItem, target_email: str) -> ActionPlan:
    if not item.owned_by_me:
        return ActionPlan("skip", "item is not owned by the authenticated user")
    if item.drive_id:
        return ActionPlan("skip", "item belongs to a shared drive")

    # Conflict: another user (not the target) already has a pending transfer in progress
    other_pending = [
        p
        for p in item.permissions
        if p.get("type") == "user"
        and p.get("pendingOwner")
        and p.get("emailAddress", "").casefold() != target_email.casefold()
    ]
    if other_pending:
        conflict_email = other_pending[0].get("emailAddress", "unknown")
        return ActionPlan(
            "skip", f"conflict: pending transfer to {conflict_email} already in progress"
        )

    permission = find_user_permission(item.permissions, target_email)
    if permission and permission.get("role") == "owner":
        return ActionPlan("skip", "target user is already the owner", permission.get("id"))
    if permission and permission.get("pendingOwner"):
        return ActionPlan("skip", "ownership transfer is already pending", permission.get("id"))
    if permission:
        return ActionPlan(
            "update-permission",
            "mark existing user permission as pending owner",
            permission.get("id"),
        )
    return ActionPlan(
        "create-permission",
        "create writer permission with pending owner enabled",
    )


def plan_accept(item: DriveItem, recipient_email: str) -> ActionPlan:
    if item.drive_id:
        return ActionPlan("skip", "item belongs to a shared drive")
    permission = find_user_permission(item.permissions, recipient_email)
    if not permission:
        return ActionPlan("skip", "recipient has no explicit user permission on this item")
    if permission.get("role") == "owner":
        return ActionPlan("skip", "recipient already owns this item", permission.get("id"))
    if not permission.get("pendingOwner"):
        return ActionPlan(
            "skip",
            "no pending ownership transfer for recipient",
            permission.get("id"),
        )
    return ActionPlan(
        "accept-transfer",
        "accept pending ownership transfer",
        permission.get("id"),
    )


# ---------------------------------------------------------------------------
# Apply functions
# ---------------------------------------------------------------------------


def apply_request_plan(
    service: Resource,
    item: DriveItem,
    *,
    target_email: str,
    plan: ActionPlan,
    email_message: str | None,
) -> None:
    if plan.action == "create-permission":
        create_kwargs: dict[str, Any] = {
            "fileId": item.id,
            "supportsAllDrives": True,
            "sendNotificationEmail": bool(email_message),
            "body": {
                "type": "user",
                "role": "writer",
                "emailAddress": target_email,
                "pendingOwner": True,
            },
            "fields": "id,emailAddress,role,pendingOwner",
        }
        if email_message:
            create_kwargs["emailMessage"] = email_message
        request = service.permissions().create(**create_kwargs)
    elif plan.action == "update-permission":
        if not plan.permission_id:
            raise ValueError("update-permission action requires a permission id")
        request = service.permissions().update(
            fileId=item.id,
            permissionId=plan.permission_id,
            supportsAllDrives=True,
            body={"role": "writer", "pendingOwner": True},
            fields="id,emailAddress,role,pendingOwner",
        )
    else:
        raise ValueError(f"Unsupported request action: {plan.action}")

    execute_with_retries(request.execute)


def apply_accept_plan(service: Resource, item: DriveItem, plan: ActionPlan) -> None:
    if not plan.permission_id:
        raise ValueError("accept-transfer action requires a permission id")

    request = service.permissions().update(
        fileId=item.id,
        permissionId=plan.permission_id,
        supportsAllDrives=True,
        transferOwnership=True,
        body={"role": "owner"},
        fields="id,emailAddress,role,pendingOwner",
    )
    execute_with_retries(request.execute)


# ---------------------------------------------------------------------------
# New subcommands
# ---------------------------------------------------------------------------


def run_diff(csv_a: Path, csv_b: Path, *, key_field: str = "item_id") -> int:
    """Compare two CSV reports and print items in csv_a missing from csv_b."""
    if not csv_a.exists():
        print(f"Error: {csv_a} does not exist.", file=sys.stderr)
        return 1
    if not csv_b.exists():
        print(f"Error: {csv_b} does not exist.", file=sys.stderr)
        return 1

    def _read_csv(path: Path) -> dict[str, dict[str, str]]:
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fieldnames = reader.fieldnames or []
            if key_field not in fieldnames:
                raise ValueError(
                    f"{path}: key field {key_field!r} not found. "
                    f"Available fields: {', '.join(fieldnames) if fieldnames else '(none)'}"
                )
            rows: dict[str, dict[str, str]] = {}
            total = 0
            for row in reader:
                total += 1
                key = row.get(key_field)
                if key:
                    rows[key] = row
            if total > 0 and not rows:
                raise ValueError(
                    f"{path}: {total} row(s) found but none have a non-empty value "
                    f"for key field {key_field!r}."
                )
            return rows

    try:
        rows_a = _read_csv(csv_a)
        rows_b = _read_csv(csv_b)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    missing = {k: v for k, v in rows_a.items() if k not in rows_b}
    status_only = {
        k: v
        for k, v in rows_a.items()
        if k in rows_b and rows_a[k].get("status") != rows_b[k].get("status")
    }

    if not missing and not status_only:
        print("All items in the first report are present in the second report.")
        return 0

    if missing:
        print(f"\nItems in {csv_a} missing from {csv_b} ({len(missing)}):")
        for row in missing.values():
            print(f"  [{row.get('status', '?')}] {row.get('path', row.get(key_field, '?'))}")

    if status_only:
        print(f"\nStatus differences ({len(status_only)}):")
        for key, row_a in status_only.items():
            row_b = rows_b[key]
            path = row_a.get("path", key)
            print(f"  {path}: {row_a.get('status')} → {row_b.get('status')}")

    return 1 if missing else 0


def run_doctor(
    service: Resource,
    credentials: Credentials,
    *,
    credentials_file: Path,
    token_file: Path,
    folder_id: str,
) -> int:
    """Run diagnostic checks and print a pass/fail report."""
    failures = 0

    def check(label: str, ok: bool, detail: str = "") -> None:
        nonlocal failures
        symbol = "✓" if ok else "✗"
        suffix = f"  ({detail})" if detail else ""
        print(f"  {symbol}  {label}{suffix}")
        if not ok:
            failures += 1

    print("--- doctor ---")

    # Credentials file
    cred_exists = credentials_file.exists()
    check("credentials file exists", cred_exists, str(credentials_file))
    if cred_exists and os.name == "posix":
        try:
            mode = credentials_file.stat().st_mode & 0o777
            check("credentials file permissions", not (mode & 0o044), f"mode {oct(mode)}")
        except OSError:
            pass

    # Token file
    token_exists = token_file.exists()
    check("token file exists", token_exists, str(token_file))
    if token_exists and os.name == "posix":
        try:
            mode = token_file.stat().st_mode & 0o777
            check("token file permissions", not (mode & 0o077), f"mode {oct(mode)}")
        except OSError:
            pass

    # Token validity
    token_valid = getattr(credentials, "valid", False)
    check("OAuth token is valid", token_valid)

    token_expired = getattr(credentials, "expired", True)
    check("OAuth token is not expired", not token_expired)

    # Drive API reachability
    try:
        about = execute_with_retries(
            lambda: service.about().get(fields="user(emailAddress,displayName)").execute()
        )
        user_email = about.get("user", {}).get("emailAddress", "unknown")
        check("Drive API reachable", True, f"authenticated as {user_email}")
    except Exception as exc:
        check("Drive API reachable", False, str(exc))

    # Folder access
    try:
        folder = get_file(service, folder_id, fields="id,name,mimeType,driveId")
        is_folder = folder.get("mimeType") == FOLDER_MIME_TYPE
        check("folder-id is accessible", True, folder.get("name", folder_id))
        check("folder-id is not a shared drive", not folder.get("driveId"), "")
        check("folder-id points to a folder", is_folder, folder.get("mimeType", ""))
    except Exception as exc:
        check("folder-id is accessible", False, str(exc))

    print("")
    if failures:
        print(f"doctor: {failures} check(s) failed.")
        return 1
    print("doctor: all checks passed.")
    return 0


def run_auth_revoke(*, token_file: Path) -> int:
    """Revoke the OAuth token at the provider and delete the local token file."""
    if not token_file.exists():
        print(f"No token file found at {token_file}.", file=sys.stderr)
        return 1

    try:
        credentials = Credentials.from_authorized_user_file(str(token_file), SCOPES)
        token = credentials.token or credentials.refresh_token
    except Exception as exc:
        print(f"Could not read token file: {exc}", file=sys.stderr)
        token = None

    revoked = False
    if token:
        body = urllib.parse.urlencode({"token": token}).encode("ascii")
        try:
            req = urllib.request.Request(  # nosec B310
                "https://accounts.google.com/o/oauth2/revoke",
                data=body,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:  # nosec B310
                revoked = resp.status == 200
        except urllib.error.HTTPError as exc:
            print(
                f"Warning: revoke returned HTTP {exc.code} — token may already be invalid.",
                file=sys.stderr,
            )
            revoked = True
        except Exception as exc:
            print(f"Warning: could not reach revoke endpoint: {exc}", file=sys.stderr)

    try:
        token_file.unlink()
        print(f"Token file deleted: {token_file}")
    except OSError as exc:
        print(f"Warning: could not delete token file: {exc}", file=sys.stderr)

    if revoked:
        print("OAuth token revoked successfully.")
    else:
        print("OAuth token file removed (revocation may not have completed).")
    return 0


# ---------------------------------------------------------------------------
# Webhook notification
# ---------------------------------------------------------------------------


def _notify_webhook(url: str, rows: list[dict[str, str]], *, command: str) -> None:
    counts = Counter(row["status"] for row in rows)
    payload = json.dumps(
        {
            "command": command,
            "generated_at": datetime.now(UTC).isoformat(),
            "item_count": len(rows),
            "status_counts": dict(counts),
        }
    ).encode("utf-8")
    try:
        req = urllib.request.Request(  # nosec B310
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10):  # nosec B310
            pass
    except Exception as exc:
        print(f"Warning: webhook notification failed: {exc}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def find_user_permission(
    permissions: tuple[dict[str, Any], ...], email_address: str
) -> dict[str, Any] | None:
    normalized = email_address.casefold()
    for permission in permissions:
        if permission.get("type") != "user":
            continue
        if permission.get("emailAddress", "").casefold() == normalized:
            return permission
    return None


def infer_target_email(root: dict[str, Any], current_user_email: str | None) -> str:
    owners = root.get("owners", [])
    if len(owners) != 1 or not owners[0].get("emailAddress"):
        raise SystemExit("Could not infer the shared folder owner. Pass --target-email explicitly.")

    owner_email = str(owners[0]["emailAddress"])
    if current_user_email and owner_email.casefold() == current_user_email.casefold():
        raise SystemExit(
            "The shared folder owner appears to be the authenticated user. "
            "Pass --target-email explicitly."
        )
    return owner_email


def make_row(item: DriveItem, *, action: str, status: str, detail: str) -> dict[str, str]:
    return {
        "path": item.path,
        "item_id": item.id,
        "mime_type": item.mime_type,
        "action": action,
        "status": status,
        "detail": detail,
    }


def write_report(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["path", "item_id", "mime_type", "action", "status", "detail"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json_log(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    log_data = {
        "generated_at": datetime.now(UTC).isoformat(),
        "item_count": len(rows),
        "items": rows,
    }
    with path.open("w", encoding="utf-8") as handle:
        json.dump(log_data, handle, indent=2)


def print_summary(rows: list[dict[str, str]]) -> None:
    status_counts = Counter(row["status"] for row in rows)
    print("")
    print("Summary:")
    for status in sorted(status_counts):
        print(f"  {status}: {status_counts[status]}")
    print(f"  total: {len(rows)}")


def format_user(user: dict[str, Any]) -> str:
    email = str(user.get("emailAddress", "<unknown-email>"))
    display_name = user.get("displayName")
    if isinstance(display_name, str) and display_name:
        return f"{display_name} <{email}>"
    return email


def format_http_error(exc: HttpError) -> str:
    status = getattr(exc.resp, "status", "unknown")
    try:
        payload = json.loads(exc.content.decode("utf-8"))
        message = payload.get("error", {}).get("message", "").strip()
    except Exception:
        message = ""
    if message:
        return f"HTTP {status}: {message}"
    return f"HTTP {status}"
