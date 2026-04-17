from __future__ import annotations

import argparse
import csv
import json
import os
import time
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/drive"]
FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
RETRYABLE_STATUSES = {429, 500, 502, 503, 504}

ActionType = Literal["skip", "create-permission", "update-permission", "accept-transfer"]


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Bulk-initiate and accept Google Drive ownership transfers inside a shared-folder tree."
        )
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
        "--report-file",
        type=Path,
        help="Optional CSV report path.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress skipped-item output. Errors and the summary are always shown.",
    )


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.page_size < 1 or args.page_size > 1000:
        raise SystemExit("--page-size must be between 1 and 1000.")

    credentials = load_credentials(args.credentials_file, args.token_file)
    service = build_drive_service(credentials)
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

    print(f"Authenticated as: {format_user(me)}")
    print(f"Root folder: {root['name']} ({root['id']})")

    if args.command == "scan":
        rows = run_scan(
            service,
            root,
            page_size=args.page_size,
            owned_only=args.owned_only,
            quiet=args.quiet,
        )
    elif args.command == "request":
        target_email = args.target_email or infer_target_email(root, me.get("emailAddress"))
        print(f"Target owner: {target_email}")
        print("Mode: apply" if args.apply else "Mode: dry-run")
        rows = run_request(
            service,
            root,
            target_email=target_email,
            page_size=args.page_size,
            apply=args.apply,
            max_items=args.max_items,
            email_message=args.email_message,
            quiet=args.quiet,
        )
    elif args.command == "accept":
        print("Mode: apply" if args.apply else "Mode: dry-run")
        rows = run_accept(
            service,
            root,
            recipient_email=me["emailAddress"],
            page_size=args.page_size,
            apply=args.apply,
            max_items=args.max_items,
            quiet=args.quiet,
        )
    else:
        raise SystemExit(f"Unknown command: {args.command!r}")

    print_summary(rows)
    if args.report_file:
        write_report(args.report_file, rows)
        print(f"Report written: {args.report_file}")
    return 0


def load_credentials(credentials_file: Path, token_file: Path) -> Credentials:
    if not credentials_file.exists():
        raise SystemExit(
            f"OAuth client file not found: {credentials_file}. "
            "Create a Desktop OAuth client in Google Cloud and pass --credentials-file."
        )

    credentials: Credentials | None = None
    if token_file.exists():
        try:
            credentials = Credentials.from_authorized_user_file(str(token_file), SCOPES)
        except Exception:
            print(
                f"Warning: token file {token_file} is invalid or unreadable — re-authenticating."
            )

    if credentials and credentials.valid:
        return credentials

    if credentials and credentials.expired and credentials.refresh_token:
        credentials.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(str(credentials_file), SCOPES)
        credentials = flow.run_local_server(port=0)

    token_file.parent.mkdir(parents=True, exist_ok=True)
    token_file.write_text(credentials.to_json(), encoding="utf-8")
    os.chmod(token_file, 0o600)
    return credentials


def build_drive_service(credentials: Credentials) -> Resource:
    return build("drive", "v3", credentials=credentials, cache_discovery=False)


def execute_with_retries(request_fn: Callable[[], Any], attempts: int = 5) -> Any:
    for attempt in range(attempts):
        try:
            return request_fn()
        except HttpError as exc:
            if not is_retryable(exc) or attempt == attempts - 1:
                raise
            time.sleep(2**attempt)
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


def run_scan(
    service: Resource,
    root: dict[str, Any],
    *,
    page_size: int,
    owned_only: bool,
    quiet: bool,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for item in walk_tree(service, root, page_size=page_size):
        status = "owned-by-me" if item.owned_by_me else "not-owned-by-me"
        if owned_only and not item.owned_by_me:
            continue
        if not quiet or item.owned_by_me:
            print(f"[{status}] {item.path}")
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


def _run_loop(
    service: Resource,
    root: dict[str, Any],
    *,
    page_size: int,
    apply: bool,
    max_items: int | None,
    quiet: bool,
    plan_fn: Callable[[DriveItem], ActionPlan],
    apply_fn: Callable[[DriveItem, ActionPlan], None],
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    attempted_count = 0

    for item in walk_tree(service, root, page_size=page_size):
        plan = plan_fn(item)
        row = make_row(item, action=plan.action, status="planned", detail=plan.detail)

        if plan.action == "skip":
            row["status"] = "skipped"
            if not quiet:
                print(f"[skip] {item.path} :: {plan.detail}")
            rows.append(row)
            continue

        if max_items is not None and attempted_count >= max_items:
            row["status"] = "skipped"
            row["detail"] = f"{plan.detail}; max-items reached"
            if not quiet:
                print(f"[skip] {item.path} :: max-items reached")
            rows.append(row)
            continue

        if not apply:
            row["status"] = "dry-run"
            print(f"[dry-run] {item.path} :: {plan.detail}")
            rows.append(row)
            continue

        # Count every attempted mutation so max-items remains a hard cap on side effects.
        attempted_count += 1
        try:
            apply_fn(item, plan)
            row["status"] = "applied"
            print(f"[applied] {item.path} :: {plan.detail}")
        except HttpError as exc:
            row["status"] = "error"
            row["detail"] = format_http_error(exc)
            print(f"[error] {item.path} :: {row['detail']}")
        rows.append(row)

    return rows


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
) -> list[dict[str, str]]:
    return _run_loop(
        service,
        root,
        page_size=page_size,
        apply=apply,
        max_items=max_items,
        quiet=quiet,
        plan_fn=lambda item: plan_request(item, target_email),
        apply_fn=lambda item, plan: apply_request_plan(
            service, item, target_email=target_email, plan=plan, email_message=email_message
        ),
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
) -> list[dict[str, str]]:
    return _run_loop(
        service,
        root,
        page_size=page_size,
        apply=apply,
        max_items=max_items,
        quiet=quiet,
        plan_fn=lambda item: plan_accept(item, recipient_email),
        apply_fn=lambda item, plan: apply_accept_plan(service, item, plan),
    )


def plan_request(item: DriveItem, target_email: str) -> ActionPlan:
    if not item.owned_by_me:
        return ActionPlan("skip", "item is not owned by the authenticated user")
    if item.drive_id:
        return ActionPlan("skip", "item belongs to a shared drive")

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
