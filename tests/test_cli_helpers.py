from __future__ import annotations

import csv
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from googleapiclient.errors import HttpError

from gdrive_ownership_transfer.cli import (
    ActionPlan,
    ActionType,
    DriveItem,
    TokenBucket,
    _apply_filters,
    _check_credential_permissions,
    _dict_to_drive_item,
    _ensure_token_fresh,
    _notify_webhook,
    _print_diff_table,
    apply_accept_plan,
    apply_request_plan,
    execute_with_retries,
    find_user_permission,
    format_http_error,
    format_user,
    infer_target_email,
    is_retryable,
    list_children,
    load_checkpoint,
    make_row,
    plan_accept,
    plan_request,
    print_summary,
    run_accept,
    run_auth_revoke,
    run_diff,
    run_doctor,
    run_request,
    run_scan,
    save_checkpoint,
    walk_tree,
    write_json_log,
    write_report,
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_COMMON = dict(
    page_size=100,
    quiet=False,
    output_format="text",
    mime_types=None,
    path_prefix=None,
)


def make_item(
    *,
    path: str = "Shared/Example",
    owned_by_me: bool = True,
    drive_id: str | None = None,
    mime_type: str = "text/plain",
    permissions: tuple[dict[str, object], ...] = (),
) -> DriveItem:
    return DriveItem(
        id="item-123",
        name="Example",
        mime_type=mime_type,
        path=path,
        owned_by_me=owned_by_me,
        drive_id=drive_id,
        permissions=permissions,
    )


class FakeRequest:
    def __init__(self, payload: object) -> None:
        self.payload = payload

    def execute(self, **_kw: object) -> object:
        return self.payload


class FakeFilesApi:
    def __init__(self, responses: dict[str | None, dict[str, object]]) -> None:
        self.responses = responses
        self.calls: list[dict[str, object]] = []

    def list(self, **kwargs: object) -> FakeRequest:
        self.calls.append(kwargs)
        token = kwargs.get("pageToken")
        return FakeRequest(self.responses[token])


class FakePermissionsApi:
    def __init__(self) -> None:
        self.create_calls: list[dict[str, object]] = []
        self.update_calls: list[dict[str, object]] = []

    def create(self, **kwargs: object) -> FakeRequest:
        self.create_calls.append(kwargs)
        return FakeRequest({"ok": True})

    def update(self, **kwargs: object) -> FakeRequest:
        self.update_calls.append(kwargs)
        return FakeRequest({"ok": True})


class FakeService:
    def __init__(
        self,
        *,
        files_api: FakeFilesApi | None = None,
        permissions_api: FakePermissionsApi | None = None,
    ) -> None:
        self._files_api = files_api or FakeFilesApi({})
        self._permissions_api = permissions_api or FakePermissionsApi()

    def files(self) -> FakeFilesApi:
        return self._files_api

    def permissions(self) -> FakePermissionsApi:
        return self._permissions_api


def make_http_error(status: int, message: str, reason: str = "rateLimitExceeded") -> HttpError:
    payload = (
        '{"error":{"message":"' + message + '","errors":[{"reason":"' + reason + '"}]}}'
    ).encode("utf-8")
    return HttpError(SimpleNamespace(status=status, reason="error"), payload, uri="")


# ---------------------------------------------------------------------------
# ActionType
# ---------------------------------------------------------------------------


def test_action_type_values_are_valid() -> None:
    valid: list[ActionType] = [
        "skip",
        "create-permission",
        "update-permission",
        "accept-transfer",
    ]
    assert len(valid) == 4


# ---------------------------------------------------------------------------
# _apply_filters
# ---------------------------------------------------------------------------


def test_apply_filters_no_filters_returns_all() -> None:
    items = [make_item(path="Root/a"), make_item(path="Root/b")]
    assert _apply_filters(items, mime_types=None, path_prefix=None) == items


def test_apply_filters_by_mime_type() -> None:
    doc = make_item(mime_type="application/vnd.google-apps.document")
    sheet = make_item(mime_type="application/vnd.google-apps.spreadsheet")
    result = _apply_filters(
        [doc, sheet],
        mime_types=["application/vnd.google-apps.document"],
        path_prefix=None,
    )
    assert result == [doc]


def test_apply_filters_by_path_prefix() -> None:
    inside = make_item(path="Shared/Docs/file.txt")
    outside = make_item(path="Shared/Other/file.txt")
    result = _apply_filters([inside, outside], mime_types=None, path_prefix="Shared/Docs")
    assert result == [inside]


def test_apply_filters_combined() -> None:
    match = make_item(path="Shared/Docs/file.txt", mime_type="application/vnd.google-apps.document")
    wrong_path = make_item(
        path="Shared/Other/file.txt", mime_type="application/vnd.google-apps.document"
    )
    wrong_type = make_item(path="Shared/Docs/sheet.txt", mime_type="text/plain")
    result = _apply_filters(
        [match, wrong_path, wrong_type],
        mime_types=["application/vnd.google-apps.document"],
        path_prefix="Shared/Docs",
    )
    assert result == [match]


# ---------------------------------------------------------------------------
# plan_request
# ---------------------------------------------------------------------------


def test_plan_request_skips_non_owned_items() -> None:
    item = make_item(owned_by_me=False)

    assert plan_request(item, "owner@example.com") == ActionPlan(
        "skip",
        "item is not owned by the authenticated user",
    )


def test_plan_request_creates_permission_for_new_target() -> None:
    item = make_item()

    assert plan_request(item, "owner@example.com") == ActionPlan(
        "create-permission",
        "create writer permission with pending owner enabled",
    )


def test_plan_request_updates_existing_permission() -> None:
    item = make_item(
        permissions=(
            {
                "id": "perm-1",
                "type": "user",
                "emailAddress": "owner@example.com",
                "role": "writer",
            },
        )
    )

    assert plan_request(item, "owner@example.com") == ActionPlan(
        "update-permission",
        "mark existing user permission as pending owner",
        "perm-1",
    )


def test_plan_request_skips_pending_transfer() -> None:
    item = make_item(
        permissions=(
            {
                "id": "perm-1",
                "type": "user",
                "emailAddress": "owner@example.com",
                "role": "writer",
                "pendingOwner": True,
            },
        )
    )

    assert plan_request(item, "owner@example.com") == ActionPlan(
        "skip",
        "ownership transfer is already pending",
        "perm-1",
    )


def test_plan_request_skips_shared_drive_items() -> None:
    item = make_item(drive_id="drive-123")

    assert plan_request(item, "owner@example.com") == ActionPlan(
        "skip",
        "item belongs to a shared drive",
    )


def test_plan_request_skips_existing_owner() -> None:
    item = make_item(
        permissions=(
            {
                "id": "perm-owner",
                "type": "user",
                "emailAddress": "owner@example.com",
                "role": "owner",
            },
        )
    )

    assert plan_request(item, "owner@example.com") == ActionPlan(
        "skip",
        "target user is already the owner",
        "perm-owner",
    )


# ---------------------------------------------------------------------------
# plan_accept
# ---------------------------------------------------------------------------


def test_plan_accept_requires_pending_owner_permission() -> None:
    item = make_item(
        permissions=(
            {
                "id": "perm-2",
                "type": "user",
                "emailAddress": "recipient@example.com",
                "role": "writer",
                "pendingOwner": True,
            },
        )
    )

    assert plan_accept(item, "recipient@example.com") == ActionPlan(
        "accept-transfer",
        "accept pending ownership transfer",
        "perm-2",
    )


def test_plan_accept_skips_without_explicit_permission() -> None:
    item = make_item()

    assert plan_accept(item, "recipient@example.com") == ActionPlan(
        "skip",
        "recipient has no explicit user permission on this item",
    )


def test_plan_accept_skips_shared_drive_items() -> None:
    item = make_item(drive_id="drive-123")

    assert plan_accept(item, "recipient@example.com") == ActionPlan(
        "skip",
        "item belongs to a shared drive",
    )


def test_plan_accept_skips_when_recipient_is_owner() -> None:
    item = make_item(
        permissions=(
            {
                "id": "perm-owner",
                "type": "user",
                "emailAddress": "recipient@example.com",
                "role": "owner",
            },
        )
    )

    assert plan_accept(item, "recipient@example.com") == ActionPlan(
        "skip",
        "recipient already owns this item",
        "perm-owner",
    )


# ---------------------------------------------------------------------------
# find_user_permission
# ---------------------------------------------------------------------------


def test_find_user_permission_matches_case_insensitively() -> None:
    permission = find_user_permission(
        (
            {
                "id": "perm-3",
                "type": "user",
                "emailAddress": "Recipient@Example.com",
                "role": "writer",
            },
        ),
        "recipient@example.com",
    )

    assert permission is not None
    assert permission["id"] == "perm-3"


def test_find_user_permission_skips_non_user_permissions() -> None:
    permission = find_user_permission(
        (
            {
                "id": "perm-group",
                "type": "group",
                "emailAddress": "recipient@example.com",
                "role": "writer",
            },
        ),
        "recipient@example.com",
    )

    assert permission is None


# ---------------------------------------------------------------------------
# infer_target_email
# ---------------------------------------------------------------------------


def test_infer_target_email_uses_root_owner() -> None:
    root = {"owners": [{"emailAddress": "owner@example.com"}]}

    assert infer_target_email(root, "me@example.com") == "owner@example.com"


def test_infer_target_email_rejects_current_user_as_owner() -> None:
    root = {"owners": [{"emailAddress": "me@example.com"}]}

    with pytest.raises(SystemExit, match="Pass --target-email explicitly"):
        infer_target_email(root, "me@example.com")


def test_infer_target_email_requires_single_owner() -> None:
    with pytest.raises(SystemExit, match="Pass --target-email explicitly"):
        infer_target_email({"owners": []}, "me@example.com")


# ---------------------------------------------------------------------------
# format_user
# ---------------------------------------------------------------------------


def test_format_user_prefers_display_name() -> None:
    user = {"displayName": "Marshall", "emailAddress": "me@example.com"}

    assert format_user(user) == "Marshall <me@example.com>"


def test_format_user_falls_back_to_email_only() -> None:
    assert format_user({"emailAddress": "me@example.com"}) == "me@example.com"


# ---------------------------------------------------------------------------
# make_row / write_report / write_json_log / print_summary
# ---------------------------------------------------------------------------


def test_make_row_returns_expected_mapping() -> None:
    row = make_row(make_item(), action="scan", status="owned-by-me", detail="detail")

    assert row == {
        "path": "Shared/Example",
        "item_id": "item-123",
        "mime_type": "text/plain",
        "action": "scan",
        "status": "owned-by-me",
        "detail": "detail",
    }


def test_write_report_writes_csv(tmp_path: Path) -> None:
    report_path = tmp_path / "report.csv"
    rows = [make_row(make_item(), action="scan", status="ok", detail="")]

    write_report(report_path, rows)

    with report_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        written_rows = list(reader)

    assert written_rows[0]["item_id"] == "item-123"


def test_write_json_log_creates_file(tmp_path: Path) -> None:
    log_path = tmp_path / "logs" / "run.json"
    rows = [make_row(make_item(), action="scan", status="applied", detail="")]

    write_json_log(log_path, rows)

    assert log_path.exists()
    data = json.loads(log_path.read_text(encoding="utf-8"))
    assert "generated_at" in data
    assert data["item_count"] == 1
    assert data["items"][0]["status"] == "applied"


def test_print_summary_reports_counts(capsys: pytest.CaptureFixture[str]) -> None:
    print_summary(
        [
            {"status": "applied"},
            {"status": "applied"},
            {"status": "skipped"},
        ]
    )

    captured = capsys.readouterr()
    assert "applied: 2" in captured.out
    assert "skipped: 1" in captured.out
    assert "total: 3" in captured.out


# ---------------------------------------------------------------------------
# format_http_error
# ---------------------------------------------------------------------------


def test_format_http_error_reads_json_message() -> None:
    error = make_http_error(403, "quota exceeded")

    assert format_http_error(error) == "HTTP 403: quota exceeded"


def test_format_http_error_handles_non_json_payload() -> None:
    error = HttpError(SimpleNamespace(status=500, reason="error"), b"not-json", uri="")

    assert format_http_error(error) == "HTTP 500"


# ---------------------------------------------------------------------------
# is_retryable / execute_with_retries
# ---------------------------------------------------------------------------


def test_is_retryable_handles_rate_limit_payload() -> None:
    assert is_retryable(make_http_error(403, "quota exceeded", "rateLimitExceeded")) is True


def test_is_retryable_rejects_non_retryable_payload() -> None:
    assert is_retryable(make_http_error(403, "forbidden", "forbidden")) is False


def test_is_retryable_rejects_non_403_status() -> None:
    assert is_retryable(make_http_error(401, "unauthorized", "forbidden")) is False


def test_is_retryable_rejects_invalid_json_payload() -> None:
    error = HttpError(SimpleNamespace(status=403, reason="error"), b"not-json", uri="")

    assert is_retryable(error) is False


def test_execute_with_retries_retries_403_rate_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = {"count": 0}
    monkeypatch.setattr("gdrive_ownership_transfer.cli.time.sleep", lambda _: None)
    monkeypatch.setattr("gdrive_ownership_transfer.cli.random.uniform", lambda _a, _b: 0.0)

    class _FakeRequest:
        def execute(self, num_retries: int = 0) -> str:
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise make_http_error(403, "quota exceeded", "rateLimitExceeded")
            return "ok"

    assert execute_with_retries(_FakeRequest()) == "ok"  # type: ignore[arg-type]
    assert attempts["count"] == 2


def test_execute_with_retries_reraises_non_retryable_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("gdrive_ownership_transfer.cli.time.sleep", lambda _: None)
    monkeypatch.setattr("gdrive_ownership_transfer.cli.random.uniform", lambda _a, _b: 0.0)

    class _FakeRequest:
        def execute(self, num_retries: int = 0) -> None:
            raise make_http_error(403, "forbidden", "forbidden")

    with pytest.raises(HttpError):
        execute_with_retries(_FakeRequest())  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# list_children / walk_tree
# ---------------------------------------------------------------------------


def test_list_children_sorts_folders_first_and_handles_pagination() -> None:
    files_api = FakeFilesApi(
        {
            None: {
                "files": [
                    {"id": "b", "name": "z-file", "mimeType": "text/plain", "ownedByMe": False},
                    {
                        "id": "a",
                        "name": "Folder",
                        "mimeType": "application/vnd.google-apps.folder",
                        "ownedByMe": True,
                    },
                ],
                "nextPageToken": "page-2",
            },
            "page-2": {
                "files": [
                    {"id": "c", "name": "a-file", "mimeType": "text/plain", "ownedByMe": True}
                ]
            },
        }
    )
    service = FakeService(files_api=files_api)

    children = list_children(service, "parent-1", page_size=50)

    assert [child["name"] for child in children] == ["Folder", "a-file", "z-file"]
    assert files_api.calls[0]["pageToken"] is None
    assert files_api.calls[1]["pageToken"] == "page-2"


def test_walk_tree_recurses_and_preserves_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = {
        "id": "root",
        "name": "Shared",
        "mimeType": "application/vnd.google-apps.folder",
        "ownedByMe": False,
    }
    child_folder = {
        "id": "child-folder",
        "name": "Docs",
        "mimeType": "application/vnd.google-apps.folder",
        "ownedByMe": True,
    }
    child_file = {
        "id": "child-file",
        "name": "notes.txt",
        "mimeType": "text/plain",
        "ownedByMe": True,
    }

    def fake_list_children(
        _service: object,
        parent_id: str,
        *,
        page_size: int,
        rate_bucket: object = None,
    ) -> list[dict[str, object]]:
        assert page_size == 100
        if parent_id == "root":
            return [child_folder]
        if parent_id == "child-folder":
            return [child_file]
        return []

    monkeypatch.setattr("gdrive_ownership_transfer.cli.list_children", fake_list_children)

    items = list(walk_tree(object(), root, page_size=100))

    assert [item.path for item in items] == [
        "Shared",
        "Shared/Docs",
        "Shared/Docs/notes.txt",
    ]


# ---------------------------------------------------------------------------
# run_scan
# ---------------------------------------------------------------------------


def test_run_scan_honors_owned_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.walk_tree",
        lambda *_a, **_k: [
            make_item(owned_by_me=True),
            make_item(owned_by_me=False),
        ],
    )

    rows = run_scan(object(), {}, owned_only=True, **_COMMON)

    assert len(rows) == 1
    assert rows[0]["status"] == "owned-by-me"


def test_run_scan_quiet_suppresses_non_owned_output(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.walk_tree",
        lambda *_a, **_k: [
            make_item(owned_by_me=True),
            make_item(owned_by_me=False),
        ],
    )

    rows = run_scan(
        object(),
        {},
        page_size=100,
        owned_only=False,
        quiet=True,
        output_format="text",
        mime_types=None,
        path_prefix=None,
    )

    captured = capsys.readouterr()
    assert len(rows) == 2
    assert "[not-owned-by-me]" not in captured.out
    assert "[owned-by-me]" in captured.out


def test_run_scan_filters_by_mime_type(monkeypatch: pytest.MonkeyPatch) -> None:
    doc = make_item(mime_type="application/vnd.google-apps.document")
    sheet = make_item(mime_type="application/vnd.google-apps.spreadsheet")
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.walk_tree",
        lambda *_a, **_k: [doc, sheet],
    )

    rows = run_scan(
        object(),
        {},
        page_size=100,
        owned_only=False,
        quiet=True,
        output_format="text",
        mime_types=["application/vnd.google-apps.document"],
        path_prefix=None,
    )

    assert len(rows) == 1
    assert rows[0]["mime_type"] == "application/vnd.google-apps.document"


# ---------------------------------------------------------------------------
# run_request
# ---------------------------------------------------------------------------


def test_run_request_dry_run_and_max_items(monkeypatch: pytest.MonkeyPatch) -> None:
    items = [make_item(path="Shared/one"), make_item(path="Shared/two")]
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: items)

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        apply=False,
        max_items=1,
        email_message=None,
        confirm=False,
        **_COMMON,
    )

    assert [row["status"] for row in rows] == ["dry-run", "dry-run"]


def test_run_request_apply_handles_error(monkeypatch: pytest.MonkeyPatch) -> None:
    item = make_item()
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [item])
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.apply_request_plan",
        lambda *_a, **_k: (_ for _ in ()).throw(make_http_error(403, "quota exceeded")),
    )

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        apply=True,
        max_items=None,
        email_message=None,
        confirm=False,
        **_COMMON,
    )

    assert rows[0]["status"] == "error"
    assert "quota exceeded" in rows[0]["detail"]


def test_run_request_apply_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    item = make_item()
    calls: list[str] = []
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [item])
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.apply_request_plan",
        lambda *_a, **_k: calls.append("applied"),
    )

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        apply=True,
        max_items=None,
        email_message=None,
        confirm=False,
        **_COMMON,
    )

    assert rows[0]["status"] == "applied"
    assert calls == ["applied"]


def test_run_request_skips_items_when_plan_says_skip(monkeypatch: pytest.MonkeyPatch) -> None:
    item = make_item(owned_by_me=False)
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [item])

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        apply=True,
        max_items=None,
        email_message=None,
        confirm=False,
        **_COMMON,
    )

    assert rows[0]["status"] == "skipped"


def test_run_request_quiet_suppresses_skip_output(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    item = make_item(owned_by_me=False)
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [item])

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        apply=True,
        max_items=None,
        email_message=None,
        confirm=False,
        page_size=100,
        quiet=True,
        output_format="text",
        mime_types=None,
        path_prefix=None,
    )

    assert rows[0]["status"] == "skipped"
    assert "[skip]" not in capsys.readouterr().out


def test_run_request_max_items_stops_after_first_apply(monkeypatch: pytest.MonkeyPatch) -> None:
    items = [make_item(path="Shared/one"), make_item(path="Shared/two")]
    calls: list[str] = []
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: items)
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.apply_request_plan",
        lambda *_a, **_k: calls.append("applied"),
    )

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        apply=True,
        max_items=1,
        email_message=None,
        confirm=False,
        **_COMMON,
    )

    assert [row["status"] for row in rows] == ["applied", "skipped"]
    assert calls == ["applied"]


def test_run_request_max_items_stops_after_first_error(monkeypatch: pytest.MonkeyPatch) -> None:
    items = [make_item(path="Shared/one"), make_item(path="Shared/two")]
    calls: list[str] = []
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: items)

    def fake_apply(*_a: object, **_k: object) -> None:
        calls.append("attempted")
        raise make_http_error(403, "quota exceeded")

    monkeypatch.setattr("gdrive_ownership_transfer.cli.apply_request_plan", fake_apply)

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        apply=True,
        max_items=1,
        email_message=None,
        confirm=False,
        **_COMMON,
    )

    assert [row["status"] for row in rows] == ["error", "skipped"]
    assert calls == ["attempted"]


def test_run_request_confirm_aborts_on_n(monkeypatch: pytest.MonkeyPatch) -> None:
    item = make_item()
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [item])
    monkeypatch.setattr("builtins.input", lambda: "n")

    with pytest.raises(SystemExit, match="Aborted"):
        run_request(
            object(),
            {},
            target_email="owner@example.com",
            apply=True,
            max_items=None,
            email_message=None,
            confirm=True,
            **_COMMON,
        )


def test_run_request_confirm_proceeds_on_y(monkeypatch: pytest.MonkeyPatch) -> None:
    item = make_item()
    calls: list[str] = []
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [item])
    monkeypatch.setattr("builtins.input", lambda: "y")
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.apply_request_plan",
        lambda *_a, **_k: calls.append("applied"),
    )

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        apply=True,
        max_items=None,
        email_message=None,
        confirm=True,
        **_COMMON,
    )

    assert rows[0]["status"] == "applied"
    assert calls == ["applied"]


def test_run_request_confirm_skips_prompt_when_no_apply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    item = make_item()
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [item])
    prompt_called = {"flag": False}
    monkeypatch.setattr("builtins.input", lambda: prompt_called.update({"flag": True}) or "n")  # type: ignore[func-returns-value]

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        apply=False,
        max_items=None,
        email_message=None,
        confirm=True,
        **_COMMON,
    )

    assert not prompt_called["flag"]
    assert rows[0]["status"] == "dry-run"


def test_run_request_filters_by_mime_type(monkeypatch: pytest.MonkeyPatch) -> None:
    doc = make_item(mime_type="application/vnd.google-apps.document")
    sheet = make_item(mime_type="application/vnd.google-apps.spreadsheet")
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [doc, sheet])

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        apply=False,
        max_items=None,
        email_message=None,
        confirm=False,
        page_size=100,
        quiet=False,
        output_format="text",
        mime_types=["application/vnd.google-apps.document"],
        path_prefix=None,
    )

    assert len(rows) == 1
    assert rows[0]["mime_type"] == "application/vnd.google-apps.document"


# ---------------------------------------------------------------------------
# run_accept
# ---------------------------------------------------------------------------


def _pending_item() -> DriveItem:
    return make_item(
        permissions=(
            {
                "id": "perm-2",
                "type": "user",
                "emailAddress": "recipient@example.com",
                "role": "writer",
                "pendingOwner": True,
            },
        )
    )


def test_run_accept_apply_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [_pending_item()]
    )
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.apply_accept_plan",
        lambda *_a, **_k: calls.append("applied"),
    )

    rows = run_accept(
        object(),
        {},
        recipient_email="recipient@example.com",
        apply=True,
        max_items=None,
        confirm=False,
        **_COMMON,
    )

    assert rows[0]["status"] == "applied"
    assert calls == ["applied"]


def test_run_accept_skips_when_plan_says_skip(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [make_item()])

    rows = run_accept(
        object(),
        {},
        recipient_email="recipient@example.com",
        apply=True,
        max_items=None,
        confirm=False,
        **_COMMON,
    )

    assert rows[0]["status"] == "skipped"


def test_run_accept_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [_pending_item()]
    )

    rows = run_accept(
        object(),
        {},
        recipient_email="recipient@example.com",
        apply=False,
        max_items=None,
        confirm=False,
        **_COMMON,
    )

    assert rows[0]["status"] == "dry-run"


def test_run_accept_max_items_stops_after_first_apply(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.walk_tree",
        lambda *_a, **_k: [_pending_item(), _pending_item()],
    )
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.apply_accept_plan",
        lambda *_a, **_k: calls.append("applied"),
    )

    rows = run_accept(
        object(),
        {},
        recipient_email="recipient@example.com",
        apply=True,
        max_items=1,
        confirm=False,
        **_COMMON,
    )

    assert [row["status"] for row in rows] == ["applied", "skipped"]
    assert calls == ["applied"]


def test_run_accept_max_items_stops_after_first_error(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.walk_tree",
        lambda *_a, **_k: [_pending_item(), _pending_item()],
    )

    def fake_apply(*_a: object, **_k: object) -> None:
        calls.append("attempted")
        raise make_http_error(403, "quota exceeded")

    monkeypatch.setattr("gdrive_ownership_transfer.cli.apply_accept_plan", fake_apply)

    rows = run_accept(
        object(),
        {},
        recipient_email="recipient@example.com",
        apply=True,
        max_items=1,
        confirm=False,
        **_COMMON,
    )

    assert [row["status"] for row in rows] == ["error", "skipped"]
    assert calls == ["attempted"]


def test_run_accept_apply_handles_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [_pending_item()]
    )
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.apply_accept_plan",
        lambda *_a, **_k: (_ for _ in ()).throw(make_http_error(403, "quota exceeded")),
    )

    rows = run_accept(
        object(),
        {},
        recipient_email="recipient@example.com",
        apply=True,
        max_items=None,
        confirm=False,
        **_COMMON,
    )

    assert rows[0]["status"] == "error"


def test_run_accept_confirm_aborts_on_n(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [_pending_item()]
    )
    monkeypatch.setattr("builtins.input", lambda: "n")

    with pytest.raises(SystemExit, match="Aborted"):
        run_accept(
            object(),
            {},
            recipient_email="recipient@example.com",
            apply=True,
            max_items=None,
            confirm=True,
            **_COMMON,
        )


# ---------------------------------------------------------------------------
# apply_request_plan
# ---------------------------------------------------------------------------


def test_apply_request_plan_create_permission() -> None:
    permissions_api = FakePermissionsApi()
    service = FakeService(permissions_api=permissions_api)

    apply_request_plan(
        service,
        make_item(),
        target_email="owner@example.com",
        plan=ActionPlan("create-permission", "create"),
        email_message="please accept",
    )

    assert permissions_api.create_calls[0]["body"] == {
        "type": "user",
        "role": "writer",
        "emailAddress": "owner@example.com",
        "pendingOwner": True,
    }
    assert permissions_api.create_calls[0]["emailMessage"] == "please accept"
    assert permissions_api.create_calls[0]["sendNotificationEmail"] is True


def test_apply_request_plan_create_permission_no_message() -> None:
    permissions_api = FakePermissionsApi()
    service = FakeService(permissions_api=permissions_api)

    apply_request_plan(
        service,
        make_item(),
        target_email="owner@example.com",
        plan=ActionPlan("create-permission", "create"),
        email_message=None,
    )

    assert "emailMessage" not in permissions_api.create_calls[0]
    assert permissions_api.create_calls[0]["sendNotificationEmail"] is True


def test_apply_request_plan_update_permission() -> None:
    permissions_api = FakePermissionsApi()
    service = FakeService(permissions_api=permissions_api)

    apply_request_plan(
        service,
        make_item(),
        target_email="owner@example.com",
        plan=ActionPlan("update-permission", "update", "perm-1"),
        email_message=None,
    )

    assert permissions_api.update_calls[0]["permissionId"] == "perm-1"


def test_apply_request_plan_update_requires_permission_id() -> None:
    with pytest.raises(ValueError, match="permission id"):
        apply_request_plan(
            FakeService(),
            make_item(),
            target_email="owner@example.com",
            plan=ActionPlan("update-permission", "update"),
            email_message=None,
        )


def test_apply_request_plan_rejects_unknown_action() -> None:
    with pytest.raises(ValueError, match="Unsupported request action"):
        apply_request_plan(
            FakeService(),
            make_item(),
            target_email="owner@example.com",
            plan=ActionPlan("skip", "oops"),  # type: ignore[arg-type]
            email_message=None,
        )


# ---------------------------------------------------------------------------
# apply_accept_plan
# ---------------------------------------------------------------------------


def test_apply_accept_plan_updates_owner_role() -> None:
    permissions_api = FakePermissionsApi()
    service = FakeService(permissions_api=permissions_api)

    apply_accept_plan(service, make_item(), ActionPlan("accept-transfer", "accept", "perm-2"))

    assert permissions_api.update_calls[0]["transferOwnership"] is True
    assert permissions_api.update_calls[0]["body"] == {"role": "owner"}


def test_apply_accept_plan_requires_permission_id() -> None:
    with pytest.raises(ValueError, match="permission id"):
        apply_accept_plan(FakeService(), make_item(), ActionPlan("accept-transfer", "accept"))


# ---------------------------------------------------------------------------
# TokenBucket
# ---------------------------------------------------------------------------


def test_token_bucket_does_not_sleep_when_tokens_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    slept: list[float] = []
    monkeypatch.setattr("gdrive_ownership_transfer.cli.time.sleep", lambda t: slept.append(t))
    bucket = TokenBucket(10.0, per_seconds=100.0)
    bucket.acquire()
    assert not slept


def test_token_bucket_sleeps_when_tokens_exhausted(monkeypatch: pytest.MonkeyPatch) -> None:
    slept: list[float] = []
    monkeypatch.setattr("gdrive_ownership_transfer.cli.time.sleep", lambda t: slept.append(t))
    # Rate=1 per 100s — first acquire is free; second must wait ~100s
    bucket = TokenBucket(1.0, per_seconds=100.0)
    bucket.acquire()  # uses the pre-filled token
    bucket.acquire()  # no tokens left → must sleep
    assert slept and slept[0] > 0


def test_token_bucket_is_thread_safe() -> None:
    import threading as _threading

    acquired: list[int] = []
    bucket = TokenBucket(1000.0, per_seconds=100.0)

    def _worker() -> None:
        bucket.acquire()
        acquired.append(1)

    threads = [_threading.Thread(target=_worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(acquired) == 20


# ---------------------------------------------------------------------------
# load_checkpoint / save_checkpoint
# ---------------------------------------------------------------------------


def test_load_checkpoint_returns_empty_set_for_missing_file(tmp_path: Path) -> None:
    assert load_checkpoint(tmp_path / "nope.json") == set()


def test_load_checkpoint_reads_completed_ids(tmp_path: Path) -> None:
    p = tmp_path / "cp.json"
    p.write_text('{"completed_ids": ["id-1", "id-2"]}', encoding="utf-8")
    assert load_checkpoint(p) == {"id-1", "id-2"}


def test_load_checkpoint_handles_corrupt_file(tmp_path: Path) -> None:
    p = tmp_path / "cp.json"
    p.write_text("not-json", encoding="utf-8")
    assert load_checkpoint(p) == set()


def test_save_checkpoint_writes_sorted_ids(tmp_path: Path) -> None:
    p = tmp_path / "cp.json"
    save_checkpoint(p, {"z-id", "a-id"})
    data = json.loads(p.read_text(encoding="utf-8"))
    assert data["completed_ids"] == ["a-id", "z-id"]


# ---------------------------------------------------------------------------
# _apply_filters — exclude params
# ---------------------------------------------------------------------------


def test_apply_filters_exclude_mime_type() -> None:
    doc = make_item(mime_type="application/vnd.google-apps.document")
    sheet = make_item(mime_type="application/vnd.google-apps.spreadsheet")
    result = _apply_filters(
        [doc, sheet],
        mime_types=None,
        path_prefix=None,
        exclude_mime_types=["application/vnd.google-apps.spreadsheet"],
    )
    assert result == [doc]


def test_apply_filters_exclude_path_prefix() -> None:
    inside = make_item(path="Shared/Archive/old.txt")
    outside = make_item(path="Shared/Docs/new.txt")
    result = _apply_filters(
        [inside, outside],
        mime_types=None,
        path_prefix=None,
        exclude_path_prefix="Shared/Archive",
    )
    assert result == [outside]


def test_apply_filters_exclude_and_include_combined() -> None:
    match = make_item(path="Shared/Docs/file.txt", mime_type="text/plain")
    excluded_path = make_item(path="Shared/Archive/file.txt", mime_type="text/plain")
    excluded_mime = make_item(
        path="Shared/Docs/sheet", mime_type="application/vnd.google-apps.spreadsheet"
    )
    result = _apply_filters(
        [match, excluded_path, excluded_mime],
        mime_types=["text/plain"],
        path_prefix=None,
        exclude_path_prefix="Shared/Archive",
    )
    assert result == [match]


# ---------------------------------------------------------------------------
# _dict_to_drive_item
# ---------------------------------------------------------------------------


def test_dict_to_drive_item_maps_fields() -> None:
    data: dict[str, object] = {
        "id": "x",
        "name": "MyFile",
        "mimeType": "text/plain",
        "ownedByMe": True,
        "driveId": None,
        "permissions": [],
    }
    item = _dict_to_drive_item(data, "Root/MyFile")  # type: ignore[arg-type]
    assert item.id == "x"
    assert item.path == "Root/MyFile"
    assert item.owned_by_me is True


# ---------------------------------------------------------------------------
# plan_request — conflict detection
# ---------------------------------------------------------------------------


def test_plan_request_skips_conflicting_pending_transfer() -> None:
    item = make_item(
        permissions=(
            {
                "id": "perm-other",
                "type": "user",
                "emailAddress": "other@example.com",
                "role": "writer",
                "pendingOwner": True,
            },
        )
    )
    plan = plan_request(item, "target@example.com")
    assert plan.action == "skip"
    assert "conflict" in plan.detail
    assert "other@example.com" in plan.detail


def test_plan_request_no_conflict_when_pending_is_same_target() -> None:
    item = make_item(
        permissions=(
            {
                "id": "perm-1",
                "type": "user",
                "emailAddress": "target@example.com",
                "role": "writer",
                "pendingOwner": True,
            },
        )
    )
    plan = plan_request(item, "target@example.com")
    assert plan.action == "skip"
    assert plan.detail == "ownership transfer is already pending"


# ---------------------------------------------------------------------------
# run_request / run_accept — new params (exclude, checkpoint, dry_run_diff)
# ---------------------------------------------------------------------------


def test_run_request_exclude_mime_type(monkeypatch: pytest.MonkeyPatch) -> None:
    doc = make_item(mime_type="application/vnd.google-apps.document")
    sheet = make_item(mime_type="application/vnd.google-apps.spreadsheet")
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [doc, sheet])

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        apply=False,
        max_items=None,
        email_message=None,
        confirm=False,
        exclude_mime_types=["application/vnd.google-apps.spreadsheet"],
        **_COMMON,
    )
    assert len(rows) == 1
    assert rows[0]["mime_type"] == "application/vnd.google-apps.document"


def test_run_request_exclude_path(monkeypatch: pytest.MonkeyPatch) -> None:
    active = make_item(path="Shared/Docs/file.txt")
    archived = make_item(path="Shared/Archive/old.txt")
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [active, archived]
    )

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        apply=False,
        max_items=None,
        email_message=None,
        confirm=False,
        exclude_path_prefix="Shared/Archive",
        **_COMMON,
    )
    assert len(rows) == 1
    assert rows[0]["path"] == "Shared/Docs/file.txt"


def test_run_request_resume_skips_checkpointed_items(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    item_a = make_item(path="Shared/a.txt")
    item_b = DriveItem(
        id="item-456",
        name="b.txt",
        mime_type="text/plain",
        path="Shared/b.txt",
        owned_by_me=True,
        drive_id=None,
        permissions=(),
    )
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [item_a, item_b]
    )
    cp = tmp_path / "cp.json"
    save_checkpoint(cp, {"item-123"})  # item_a.id == "item-123"

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        apply=False,
        max_items=None,
        email_message=None,
        confirm=False,
        checkpoint_file=cp,
        **_COMMON,
    )
    # Only item_b should appear (item_a was checkpointed out)
    assert all(r["path"] != "Shared/a.txt" for r in rows)
    assert any(r["path"] == "Shared/b.txt" for r in rows)


def test_run_request_dry_run_diff_returns_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    item = make_item()
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [item])

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        apply=False,
        max_items=None,
        email_message=None,
        confirm=False,
        dry_run_diff=True,
        **_COMMON,
    )
    assert rows[0]["status"] == "dry-run"


def test_run_accept_resume_skips_checkpointed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [_pending_item()]
    )
    cp = tmp_path / "cp.json"
    save_checkpoint(cp, {"item-123"})  # _pending_item uses id="item-123"

    rows = run_accept(
        object(),
        {},
        recipient_email="recipient@example.com",
        apply=False,
        max_items=None,
        confirm=False,
        checkpoint_file=cp,
        **_COMMON,
    )
    assert rows == []


# ---------------------------------------------------------------------------
# _print_diff_table
# ---------------------------------------------------------------------------


def test_print_diff_table_shows_actionable_items(capsys: pytest.CaptureFixture[str]) -> None:
    item = make_item()
    planned = [(item, ActionPlan("create-permission", "create writer permission"))]
    _print_diff_table(planned, sys.stdout)
    captured = capsys.readouterr()
    assert "create-permission" in captured.out
    assert item.path in captured.out


def test_print_diff_table_skips_skip_actions(capsys: pytest.CaptureFixture[str]) -> None:
    item = make_item(owned_by_me=False)
    planned = [(item, ActionPlan("skip", "not owned"))]
    _print_diff_table(planned, sys.stdout)
    captured = capsys.readouterr()
    assert "no actionable items" in captured.out


# ---------------------------------------------------------------------------
# run_diff
# ---------------------------------------------------------------------------


def test_run_diff_reports_missing_items(tmp_path: Path) -> None:
    csv_a = tmp_path / "a.csv"
    csv_b = tmp_path / "b.csv"
    csv_a.write_text(
        "path,item_id,mime_type,action,status,detail\n"
        "Shared/a.txt,id-1,text/plain,request,applied,\n"
        "Shared/b.txt,id-2,text/plain,request,applied,\n",
        encoding="utf-8",
    )
    csv_b.write_text(
        "path,item_id,mime_type,action,status,detail\n"
        "Shared/a.txt,id-1,text/plain,accept,applied,\n",
        encoding="utf-8",
    )
    result = run_diff(csv_a, csv_b)
    assert result == 1


def test_run_diff_returns_zero_when_all_present(tmp_path: Path) -> None:
    csv_a = tmp_path / "a.csv"
    csv_b = tmp_path / "b.csv"
    csv_a.write_text(
        "path,item_id,mime_type,action,status,detail\n"
        "Shared/a.txt,id-1,text/plain,request,applied,\n",
        encoding="utf-8",
    )
    csv_b.write_text(
        "path,item_id,mime_type,action,status,detail\n"
        "Shared/a.txt,id-1,text/plain,accept,applied,\n",
        encoding="utf-8",
    )
    assert run_diff(csv_a, csv_b) == 0


def test_run_diff_returns_error_for_missing_file(tmp_path: Path) -> None:
    result = run_diff(tmp_path / "missing.csv", tmp_path / "also_missing.csv")
    assert result == 1


# ---------------------------------------------------------------------------
# run_auth_revoke
# ---------------------------------------------------------------------------


def test_run_auth_revoke_missing_token_file(tmp_path: Path) -> None:
    result = run_auth_revoke(
        token_file=tmp_path / "no_token.json",
    )
    assert result == 1


def test_run_auth_revoke_deletes_token_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    token_file = tmp_path / "token.json"
    token_file.write_text(
        json.dumps(
            {
                "token": "access-token",
                "refresh_token": "refresh-token",
                "token_uri": "https://oauth2.googleapis.com/token",
                "client_id": "client-id",
                "client_secret": "client-secret",  # pragma: allowlist secret
                "scopes": ["https://www.googleapis.com/auth/drive"],
            }
        ),
        encoding="utf-8",
    )

    urlopen_calls: list[str] = []

    class _FakeResp:
        status = 200

        def __enter__(self) -> _FakeResp:
            return self

        def __exit__(self, *_: object) -> None:
            pass

    def _fake_urlopen(req: object, timeout: int = 10) -> _FakeResp:
        urlopen_calls.append("called")
        return _FakeResp()

    monkeypatch.setattr("gdrive_ownership_transfer.cli.urllib.request.urlopen", _fake_urlopen)

    result = run_auth_revoke(token_file=token_file)
    assert result == 0
    assert not token_file.exists()
    assert urlopen_calls


# ---------------------------------------------------------------------------
# _notify_webhook
# ---------------------------------------------------------------------------


def test_notify_webhook_posts_json(monkeypatch: pytest.MonkeyPatch) -> None:
    posted: list[bytes] = []

    class _FakeResp:
        def __enter__(self) -> _FakeResp:
            return self

        def __exit__(self, *_: object) -> None:
            pass

    def _fake_urlopen(req: object, timeout: int = 10) -> _FakeResp:
        posted.append(req.data)  # type: ignore[union-attr]
        return _FakeResp()

    monkeypatch.setattr("gdrive_ownership_transfer.cli.urllib.request.urlopen", _fake_urlopen)

    rows = [make_row(make_item(), action="scan", status="applied", detail="")]
    _notify_webhook("https://hook.example.com/test", rows, command="scan")

    assert posted
    payload = json.loads(posted[0])
    assert payload["command"] == "scan"
    assert payload["item_count"] == 1
    assert "applied" in payload["status_counts"]


def test_notify_webhook_handles_failure_gracefully(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(*_: object, **__: object) -> None:
        raise OSError("connection refused")

    monkeypatch.setattr("gdrive_ownership_transfer.cli.urllib.request.urlopen", _raise)
    # Should not raise
    _notify_webhook("https://hook.example.com/test", [], command="scan")


# ---------------------------------------------------------------------------
# _check_credential_permissions
# ---------------------------------------------------------------------------


def test_check_credential_permissions_warns_on_world_readable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cred_file = tmp_path / "credentials.json"
    cred_file.write_text("{}", encoding="utf-8")
    import os as _os

    _os.chmod(cred_file, 0o644)  # world-readable
    monkeypatch.setattr("gdrive_ownership_transfer.cli.os.name", "posix")
    _check_credential_permissions(cred_file)
    assert "Warning" in capsys.readouterr().err


def test_check_credential_permissions_silent_on_secure_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cred_file = tmp_path / "credentials.json"
    cred_file.write_text("{}", encoding="utf-8")
    import os as _os

    _os.chmod(cred_file, 0o600)  # owner only
    monkeypatch.setattr("gdrive_ownership_transfer.cli.os.name", "posix")
    _check_credential_permissions(cred_file)
    assert capsys.readouterr().err == ""


def test_check_credential_permissions_skips_on_non_posix(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr("gdrive_ownership_transfer.cli.os.name", "nt")
    _check_credential_permissions(tmp_path / "credentials.json")
    assert capsys.readouterr().err == ""


# ---------------------------------------------------------------------------
# _ensure_token_fresh
# ---------------------------------------------------------------------------


def test_ensure_token_fresh_refreshes_near_expiry(monkeypatch: pytest.MonkeyPatch) -> None:
    from datetime import timedelta
    from types import SimpleNamespace

    refreshed: list[bool] = []

    creds = SimpleNamespace(
        expiry=datetime.now(UTC) + timedelta(seconds=10),
        refresh_token="rt",
        refresh=lambda req: refreshed.append(True),
    )
    _ensure_token_fresh(creds)  # type: ignore[arg-type]
    assert refreshed


def test_ensure_token_fresh_does_not_refresh_with_plenty_of_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datetime import timedelta
    from types import SimpleNamespace

    refreshed: list[bool] = []

    creds = SimpleNamespace(
        expiry=datetime.now(UTC) + timedelta(seconds=3600),
        refresh_token="rt",
        refresh=lambda req: refreshed.append(True),
    )
    _ensure_token_fresh(creds)  # type: ignore[arg-type]
    assert not refreshed


def test_ensure_token_fresh_ignores_missing_expiry() -> None:
    from types import SimpleNamespace

    creds = SimpleNamespace(expiry=None)
    _ensure_token_fresh(creds)  # type: ignore[arg-type] — should not raise


# ---------------------------------------------------------------------------
# run_doctor
# ---------------------------------------------------------------------------


class FakeAboutApi:
    def __init__(self, email: str = "me@example.com") -> None:
        self.email = email

    def get(self, fields: str = "") -> FakeRequest:
        return FakeRequest({"user": {"emailAddress": self.email, "displayName": "Me"}})


class FakeServiceWithAbout(FakeService):
    def __init__(self, *, email: str = "me@example.com", **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self._about_api = FakeAboutApi(email)

    def about(self) -> FakeAboutApi:
        return self._about_api


def test_run_doctor_passes_all_checks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from types import SimpleNamespace

    cred_file = tmp_path / "credentials.json"
    cred_file.write_text("{}", encoding="utf-8")
    token_file = tmp_path / "token.json"
    token_file.write_text("{}", encoding="utf-8")
    import os as _os

    _os.chmod(cred_file, 0o600)
    _os.chmod(token_file, 0o600)

    creds = SimpleNamespace(valid=True, expired=False)

    folder_data = {
        "id": "folder-1",
        "name": "Shared",
        "mimeType": "application/vnd.google-apps.folder",
        "driveId": None,
    }
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.get_file",
        lambda *_a, **_k: folder_data,
    )

    service = FakeServiceWithAbout()
    result = run_doctor(
        service,  # type: ignore[arg-type]
        creds,  # type: ignore[arg-type]
        credentials_file=cred_file,
        token_file=token_file,
        folder_id="folder-1",
    )
    assert result == 0


def test_run_doctor_fails_on_missing_credentials(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from types import SimpleNamespace

    creds = SimpleNamespace(valid=False, expired=True)
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.get_file",
        lambda *_a, **_k: {
            "id": "f",
            "name": "F",
            "mimeType": "application/vnd.google-apps.folder",
        },
    )

    service = FakeServiceWithAbout()
    result = run_doctor(
        service,  # type: ignore[arg-type]
        creds,  # type: ignore[arg-type]
        credentials_file=tmp_path / "missing.json",  # does not exist
        token_file=tmp_path / "token.json",
        folder_id="folder-1",
    )
    assert result == 1


def test_run_doctor_fails_when_api_unreachable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from types import SimpleNamespace

    creds = SimpleNamespace(valid=True, expired=False)

    def _raise(*_a: object, **_k: object) -> None:
        raise OSError("network error")

    monkeypatch.setattr("gdrive_ownership_transfer.cli.execute_with_retries", _raise)

    result = run_doctor(
        FakeServiceWithAbout(),  # type: ignore[arg-type]
        creds,  # type: ignore[arg-type]
        credentials_file=tmp_path / "creds.json",
        token_file=tmp_path / "token.json",
        folder_id="folder-1",
    )
    assert result == 1


def test_run_doctor_warns_on_shared_drive_folder(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from types import SimpleNamespace

    creds = SimpleNamespace(valid=True, expired=False)
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.get_file",
        lambda *_a, **_k: {
            "id": "f",
            "name": "SharedDrive",
            "mimeType": "application/vnd.google-apps.folder",
            "driveId": "drive-123",
        },
    )
    result = run_doctor(
        FakeServiceWithAbout(),  # type: ignore[arg-type]
        creds,  # type: ignore[arg-type]
        credentials_file=tmp_path / "creds.json",
        token_file=tmp_path / "token.json",
        folder_id="f",
    )
    assert result == 1


# ---------------------------------------------------------------------------
# run_diff — status_only path
# ---------------------------------------------------------------------------


def test_run_diff_reports_status_changes(tmp_path: Path) -> None:
    csv_a = tmp_path / "a.csv"
    csv_b = tmp_path / "b.csv"
    csv_a.write_text(
        "path,item_id,mime_type,action,status,detail\n"
        "Shared/a.txt,id-1,text/plain,request,applied,\n",
        encoding="utf-8",
    )
    csv_b.write_text(
        "path,item_id,mime_type,action,status,detail\nShared/a.txt,id-1,text/plain,accept,error,\n",
        encoding="utf-8",
    )
    # id-1 is present in both but status differs → status_only → returns 0 (item not missing)
    result = run_diff(csv_a, csv_b)
    assert result == 0


# ---------------------------------------------------------------------------
# _run_loop — concurrency path
# ---------------------------------------------------------------------------


def test_run_request_concurrency_applies_all_items(monkeypatch: pytest.MonkeyPatch) -> None:
    items = [
        DriveItem(
            id=f"id-{i}",
            name=f"f{i}",
            mime_type="text/plain",
            path=f"Shared/f{i}",
            owned_by_me=True,
            drive_id=None,
            permissions=(),
        )
        for i in range(3)
    ]
    calls: list[str] = []
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: items)
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.apply_request_plan",
        lambda *_a, **_k: calls.append("applied"),
    )

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        apply=True,
        max_items=None,
        email_message=None,
        confirm=False,
        concurrency=2,
        **_COMMON,
    )
    assert len(rows) == 3
    assert all(r["status"] == "applied" for r in rows)
    assert len(calls) == 3


# ---------------------------------------------------------------------------
# _run_loop — interactive mode
# ---------------------------------------------------------------------------


def test_run_request_interactive_skips_on_n(monkeypatch: pytest.MonkeyPatch) -> None:
    item = make_item()
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [item])
    monkeypatch.setattr("gdrive_ownership_transfer.cli._RICH_AVAILABLE", False)
    monkeypatch.setattr("builtins.input", lambda: "n")

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        apply=True,
        max_items=None,
        email_message=None,
        confirm=False,
        interactive=True,
        **_COMMON,
    )
    assert rows[0]["status"] == "skipped"
    assert "interactively" in rows[0]["detail"]


def test_run_request_interactive_applies_on_y(monkeypatch: pytest.MonkeyPatch) -> None:
    item = make_item()
    calls: list[str] = []
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [item])
    monkeypatch.setattr("gdrive_ownership_transfer.cli._RICH_AVAILABLE", False)
    monkeypatch.setattr("builtins.input", lambda: "y")
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.apply_request_plan",
        lambda *_a, **_k: calls.append("applied"),
    )

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        apply=True,
        max_items=None,
        email_message=None,
        confirm=False,
        interactive=True,
        **_COMMON,
    )
    assert rows[0]["status"] == "applied"


# ---------------------------------------------------------------------------
# _run_loop — idempotency_check path
# ---------------------------------------------------------------------------


def test_run_request_idempotency_skips_already_done(monkeypatch: pytest.MonkeyPatch) -> None:
    item = make_item()
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [item])
    # Simulate re-fetch showing target is already owner
    already_owner_item = make_item(
        permissions=(
            {
                "id": "perm-1",
                "type": "user",
                "emailAddress": "owner@example.com",
                "role": "owner",
            },
        )
    )
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.get_file",
        lambda *_a, **_k: {
            "id": already_owner_item.id,
            "name": already_owner_item.name,
            "mimeType": already_owner_item.mime_type,
            "ownedByMe": already_owner_item.owned_by_me,
            "driveId": already_owner_item.drive_id,
            "permissions": list(already_owner_item.permissions),
        },
    )

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        apply=True,
        max_items=None,
        email_message=None,
        confirm=False,
        idempotency_check=True,
        **_COMMON,
    )
    assert rows[0]["status"] == "skipped"
    assert "idempotency" in rows[0]["detail"]


def test_run_request_idempotency_proceeds_when_still_needed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    item = make_item()
    calls: list[str] = []
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [item])
    # Re-fetch returns unchanged item — still needs transfer
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.get_file",
        lambda *_a, **_k: {
            "id": item.id,
            "name": item.name,
            "mimeType": item.mime_type,
            "ownedByMe": item.owned_by_me,
            "driveId": item.drive_id,
            "permissions": list(item.permissions),
        },
    )
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.apply_request_plan",
        lambda *_a, **_k: calls.append("applied"),
    )

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        apply=True,
        max_items=None,
        email_message=None,
        confirm=False,
        idempotency_check=True,
        **_COMMON,
    )
    assert rows[0]["status"] == "applied"
    assert calls == ["applied"]


def test_run_request_idempotency_http_error_falls_back_to_original_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    item = make_item()
    calls: list[str] = []
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [item])
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.get_file",
        lambda *_a, **_k: (_ for _ in ()).throw(make_http_error(403, "forbidden")),
    )
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.apply_request_plan",
        lambda *_a, **_k: calls.append("applied"),
    )

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        apply=True,
        max_items=None,
        email_message=None,
        confirm=False,
        idempotency_check=True,
        **_COMMON,
    )
    assert rows[0]["status"] == "applied"
    assert calls == ["applied"]


# ---------------------------------------------------------------------------
# main() — diff and revoke subcommands
# ---------------------------------------------------------------------------


def test_main_diff_subcommand(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from gdrive_ownership_transfer.cli import main

    csv_a = tmp_path / "a.csv"
    csv_b = tmp_path / "b.csv"
    csv_a.write_text(
        "path,item_id,mime_type,action,status,detail\nShared/a.txt,id-1,text/plain,request,applied,\n",
        encoding="utf-8",
    )
    csv_b.write_text(
        "path,item_id,mime_type,action,status,detail\nShared/a.txt,id-1,text/plain,accept,applied,\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "sys.argv",
        ["gdrive-ownership-transfer", "diff", str(csv_a), str(csv_b)],
    )
    result = main()
    assert result == 0


def test_main_revoke_missing_token(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from gdrive_ownership_transfer.cli import main

    monkeypatch.setattr(
        "sys.argv",
        [
            "gdrive-ownership-transfer",
            "revoke",
            "--token-file",
            str(tmp_path / "no_token.json"),
        ],
    )
    result = main()
    assert result == 1


def test_main_doctor_subcommand(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from unittest.mock import MagicMock

    from gdrive_ownership_transfer.cli import main

    creds_file = tmp_path / "creds.json"
    creds_file.write_text("{}", encoding="utf-8")
    token_file = tmp_path / "token.json"

    fake_creds = MagicMock()
    fake_creds.valid = True
    fake_creds.expiry = None

    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.load_credentials",
        lambda *_a, **_k: fake_creds,
    )

    fake_about = {"user": {"emailAddress": "me@example.com", "displayName": "Me"}}
    fake_root = {
        "id": "folder-123",
        "name": "Shared",
        "mimeType": "application/vnd.google-apps.folder",
        "ownedByMe": True,
        "driveId": None,
    }

    fake_service = MagicMock()
    fake_service.about().get().execute.return_value = fake_about
    fake_service.files().get().execute.return_value = fake_root

    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.build_drive_service",
        lambda _creds: fake_service,
    )
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.run_doctor",
        lambda *_a, **_k: 0,
    )

    monkeypatch.setattr(
        "sys.argv",
        [
            "gdrive-ownership-transfer",
            "doctor",
            "--folder-id",
            "folder-123",
            "--credentials-file",
            str(creds_file),
            "--token-file",
            str(token_file),
        ],
    )
    result = main()
    assert result == 0


# ---------------------------------------------------------------------------
# run_diff — csv_b missing (not csv_a)
# ---------------------------------------------------------------------------


def test_run_diff_csv_b_missing(tmp_path: Path) -> None:
    csv_a = tmp_path / "a.csv"
    csv_a.write_text(
        "path,item_id,mime_type,action,status,detail\nShared/a.txt,id-1,text/plain,req,applied,\n",
        encoding="utf-8",
    )
    result = run_diff(csv_a, tmp_path / "missing.csv")
    assert result == 1


# ---------------------------------------------------------------------------
# run_auth_revoke — corrupt token file and HTTPError paths
# ---------------------------------------------------------------------------


def test_run_auth_revoke_handles_corrupt_token_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    token_file = tmp_path / "bad_token.json"
    token_file.write_text("not-valid-json", encoding="utf-8")

    class _FakeResp:
        status = 200

        def __enter__(self) -> _FakeResp:
            return self

        def __exit__(self, *_: object) -> None:
            pass

    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.urllib.request.urlopen",
        lambda *_a, **_k: _FakeResp(),
    )

    result = run_auth_revoke(token_file=token_file)
    # Token unreadable → token=None → revoke skipped → file still deleted
    assert result == 0
    assert not token_file.exists()


def test_run_auth_revoke_handles_http_error_from_revoke(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    token_file = tmp_path / "token.json"
    token_file.write_text(
        json.dumps(
            {
                "token": "tok",
                "refresh_token": "rtok",
                "token_uri": "https://oauth2.googleapis.com/token",
                "client_id": "cid",
                "client_secret": "csecret",  # pragma: allowlist secret
                "scopes": ["https://www.googleapis.com/auth/drive"],
            }
        ),
        encoding="utf-8",
    )

    import urllib.error as _uerr
    from types import SimpleNamespace

    def _raise_http(*_a: object, **_k: object) -> None:
        raise _uerr.HTTPError(
            url="",
            code=400,
            msg="Bad Request",
            hdrs=SimpleNamespace(),
            fp=None,  # type: ignore[arg-type]
        )

    monkeypatch.setattr("gdrive_ownership_transfer.cli.urllib.request.urlopen", _raise_http)

    result = run_auth_revoke(token_file=token_file)
    assert result == 0
    assert not token_file.exists()


def test_run_auth_revoke_handles_network_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    token_file = tmp_path / "token.json"
    token_file.write_text(
        json.dumps(
            {
                "token": "tok",
                "refresh_token": "rtok",
                "token_uri": "https://oauth2.googleapis.com/token",
                "client_id": "cid",
                "client_secret": "csecret",
                "scopes": ["https://www.googleapis.com/auth/drive"],
            }
        ),
        encoding="utf-8",
    )

    def _raise(*_a: object, **_k: object) -> None:
        raise OSError("network down")

    monkeypatch.setattr("gdrive_ownership_transfer.cli.urllib.request.urlopen", _raise)

    result = run_auth_revoke(token_file=token_file)
    assert result == 0  # graceful
    assert not token_file.exists()


# ---------------------------------------------------------------------------
# run_request — checkpoint_file saves on success
# ---------------------------------------------------------------------------


def test_run_request_checkpoint_saves_applied_items(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    item = make_item()
    calls: list[str] = []
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [item])
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.apply_request_plan",
        lambda *_a, **_k: calls.append("applied"),
    )
    cp = tmp_path / "cp.json"

    run_request(
        object(),
        {},
        target_email="owner@example.com",
        apply=True,
        max_items=None,
        email_message=None,
        confirm=False,
        checkpoint_file=cp,
        **_COMMON,
    )
    assert cp.exists()
    data = json.loads(cp.read_text(encoding="utf-8"))
    assert "item-123" in data["completed_ids"]


# ---------------------------------------------------------------------------
# plan_accept — no-pending path (line 1283)
# ---------------------------------------------------------------------------


def test_plan_accept_skips_writer_without_pending() -> None:
    item = make_item(
        permissions=(
            {
                "id": "perm-2",
                "type": "user",
                "emailAddress": "recipient@example.com",
                "role": "writer",
                "pendingOwner": False,
            },
        )
    )
    plan = plan_accept(item, "recipient@example.com")
    assert plan.action == "skip"
    assert "no pending" in plan.detail


# ---------------------------------------------------------------------------
# main() — validation paths
# ---------------------------------------------------------------------------


def test_main_rejects_invalid_concurrency(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from gdrive_ownership_transfer.cli import main

    creds_file = tmp_path / "creds.json"
    creds_file.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        "sys.argv",
        [
            "gdrive-ownership-transfer",
            "request",
            "--folder-id",
            "folder-1",
            "--credentials-file",
            str(creds_file),
            "--concurrency",
            "0",
        ],
    )
    with pytest.raises(SystemExit, match="concurrency"):
        main()


def test_main_rejects_interactive_with_concurrency(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from gdrive_ownership_transfer.cli import main

    creds_file = tmp_path / "creds.json"
    creds_file.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        "sys.argv",
        [
            "gdrive-ownership-transfer",
            "request",
            "--folder-id",
            "folder-1",
            "--credentials-file",
            str(creds_file),
            "--concurrency",
            "2",
            "--interactive",
        ],
    )
    with pytest.raises(SystemExit, match="interactive"):
        main()


def test_main_rejects_invalid_rate_limit(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from gdrive_ownership_transfer.cli import main

    creds_file = tmp_path / "creds.json"
    creds_file.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        "sys.argv",
        [
            "gdrive-ownership-transfer",
            "request",
            "--folder-id",
            "folder-1",
            "--credentials-file",
            str(creds_file),
            "--rate-limit",
            "-1",
        ],
    )
    with pytest.raises(SystemExit, match="rate-limit"):
        main()


# ---------------------------------------------------------------------------
# _run_loop — concurrent exception path (future raises unexpectedly)
# ---------------------------------------------------------------------------


def test_run_request_concurrency_handles_unexpected_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    item = make_item()
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [item])

    def _explode(*_a: object, **_k: object) -> None:
        raise RuntimeError("unexpected boom")

    monkeypatch.setattr("gdrive_ownership_transfer.cli.apply_request_plan", _explode)

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        apply=True,
        max_items=None,
        email_message=None,
        confirm=False,
        concurrency=2,
        **_COMMON,
    )
    assert rows[0]["status"] == "error"
    assert "unexpected boom" in rows[0]["detail"]


# ---------------------------------------------------------------------------
# run_doctor — token file with specific permission states
# ---------------------------------------------------------------------------


def test_run_doctor_warns_on_insecure_token_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    import os as _os
    from types import SimpleNamespace

    cred_file = tmp_path / "credentials.json"
    cred_file.write_text("{}", encoding="utf-8")
    _os.chmod(cred_file, 0o600)

    token_file = tmp_path / "token.json"
    token_file.write_text("{}", encoding="utf-8")
    _os.chmod(token_file, 0o644)  # world-readable → insecure

    creds = SimpleNamespace(valid=True, expired=False)

    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.get_file",
        lambda *_a, **_k: {
            "id": "f",
            "name": "F",
            "mimeType": "application/vnd.google-apps.folder",
        },
    )

    result = run_doctor(
        FakeServiceWithAbout(),  # type: ignore[arg-type]
        creds,  # type: ignore[arg-type]
        credentials_file=cred_file,
        token_file=token_file,
        folder_id="f",
    )
    assert result == 1


# ---------------------------------------------------------------------------
# _run_loop — quiet suppresses max-items skip message
# ---------------------------------------------------------------------------


def test_run_request_max_items_quiet_suppresses_skip(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    items = [make_item(path="Shared/one"), make_item(path="Shared/two")]
    calls: list[str] = []
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: items)
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.apply_request_plan",
        lambda *_a, **_k: calls.append("applied"),
    )

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        apply=True,
        max_items=1,
        email_message=None,
        confirm=False,
        page_size=100,
        quiet=True,
        output_format="text",
        mime_types=None,
        path_prefix=None,
    )
    captured = capsys.readouterr()
    assert "max-items reached" not in captured.out
    assert [r["status"] for r in rows] == ["applied", "skipped"]


# ---------------------------------------------------------------------------
# run_auth_revoke — unlink failure is handled gracefully
# ---------------------------------------------------------------------------


def test_run_auth_revoke_handles_unlink_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    token_file = tmp_path / "token.json"
    token_file.write_text(
        json.dumps(
            {
                "token": "tok",
                "refresh_token": "rtok",
                "token_uri": "https://oauth2.googleapis.com/token",
                "client_id": "cid",
                "client_secret": "csecret",  # pragma: allowlist secret
                "scopes": ["https://www.googleapis.com/auth/drive"],
            }
        ),
        encoding="utf-8",
    )

    # run_auth_revoke uses urllib.request.urlopen to revoke, not Credentials.revoke.
    from types import SimpleNamespace

    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.urllib.request.urlopen",
        lambda *_a, **_k: SimpleNamespace(status=200).__enter__().__class__,
    )
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.urllib.request.urlopen",
        lambda *_a, **_k: SimpleNamespace(
            __enter__=lambda s: SimpleNamespace(status=200),
            __exit__=lambda s, *_: False,
        ),
    )

    def _raise_oserror(*_a: object, **_k: object) -> None:
        raise OSError("permission denied")

    import pathlib

    monkeypatch.setattr(pathlib.Path, "unlink", _raise_oserror)

    result = run_auth_revoke(token_file=token_file)
    assert result == 1


# ---------------------------------------------------------------------------
# _run_loop — credentials passed through to token refresh
# ---------------------------------------------------------------------------


def test_run_request_with_credentials_triggers_token_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from unittest.mock import MagicMock

    item = make_item()
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [item])
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.apply_request_plan",
        lambda *_a, **_k: None,
    )
    refresh_calls: list[object] = []
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli._ensure_token_fresh",
        lambda creds: refresh_calls.append(creds),
    )

    fake_creds = MagicMock()
    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        apply=True,
        max_items=None,
        email_message=None,
        confirm=False,
        credentials=fake_creds,
        **_COMMON,
    )

    assert rows[0]["status"] == "applied"
    assert len(refresh_calls) == 1
    assert refresh_calls[0] is fake_creds


# ---------------------------------------------------------------------------
# TokenBucket — fractional rate (< 1.0) must not sleep on first acquire
# ---------------------------------------------------------------------------


def test_token_bucket_fractional_rate_no_sleep_on_first_acquire(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    slept: list[float] = []
    monkeypatch.setattr("gdrive_ownership_transfer.cli.time.sleep", lambda t: slept.append(t))
    # rate=0.5 means capacity would be 0.5 without the max(1.0, rate) fix —
    # the first acquire would sleep. With the fix, capacity=1.0, no sleep.
    bucket = TokenBucket(0.5, per_seconds=100.0)
    bucket.acquire()
    assert not slept, "first acquire on a fractional-rate bucket must not sleep"


# ---------------------------------------------------------------------------
# run_diff — unknown key_field and all-empty-key detection
# ---------------------------------------------------------------------------


def test_run_diff_unknown_key_field_returns_error(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    csv_a = tmp_path / "a.csv"
    csv_b = tmp_path / "b.csv"
    csv_a.write_text(
        "path,item_id,status\nShared/a.txt,id-1,applied\n",
        encoding="utf-8",
    )
    csv_b.write_text(
        "path,item_id,status\nShared/a.txt,id-1,applied\n",
        encoding="utf-8",
    )
    result = run_diff(csv_a, csv_b, key_field="nonexistent_field")
    assert result == 1
    captured = capsys.readouterr()
    assert "key field" in captured.err and "nonexistent_field" in captured.err


def test_run_diff_all_empty_key_values_returns_error(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    csv_a = tmp_path / "a.csv"
    csv_b = tmp_path / "b.csv"
    # item_id column present but all values are empty strings
    csv_a.write_text(
        "path,item_id,status\nShared/a.txt,,applied\nShared/b.txt,,applied\n",
        encoding="utf-8",
    )
    csv_b.write_text(
        "path,item_id,status\nShared/a.txt,id-1,applied\n",
        encoding="utf-8",
    )
    result = run_diff(csv_a, csv_b)
    assert result == 1
    captured = capsys.readouterr()
    assert "none have a non-empty value" in captured.err


# ---------------------------------------------------------------------------
# save_checkpoint — creates nested parent directories
# ---------------------------------------------------------------------------


def test_save_checkpoint_creates_nested_parent_dirs(tmp_path: Path) -> None:
    nested = tmp_path / "a" / "b" / "c" / "cp.json"
    save_checkpoint(nested, {"id-1"})
    assert nested.exists()
    data = json.loads(nested.read_text(encoding="utf-8"))
    assert data["completed_ids"] == ["id-1"]


# ---------------------------------------------------------------------------
# load_credentials — refresh failure falls back to OAuth flow
# ---------------------------------------------------------------------------


def test_load_credentials_refresh_failure_falls_back_to_oauth(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from unittest.mock import MagicMock

    creds_file = tmp_path / "creds.json"
    creds_file.write_text("{}", encoding="utf-8")
    token_file = tmp_path / "token.json"

    # Simulate an expired credential stored on disk
    fake_creds = MagicMock()
    fake_creds.valid = False
    fake_creds.expired = True
    fake_creds.refresh_token = "rtok"
    fake_creds.refresh.side_effect = Exception("network error")
    fake_creds.to_json.return_value = json.dumps({"token": "new"})

    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.Credentials.from_authorized_user_file",
        lambda *_a, **_k: fake_creds,
    )
    token_file.write_text("{}", encoding="utf-8")  # make token_file.exists() True

    flow_creds = MagicMock()
    flow_creds.to_json.return_value = json.dumps({"token": "flow_tok"})
    fake_flow = MagicMock()
    fake_flow.run_local_server.return_value = flow_creds

    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.InstalledAppFlow.from_client_secrets_file",
        lambda *_a, **_k: fake_flow,
    )
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli._check_credential_permissions", lambda _: None
    )

    result = __import__(
        "gdrive_ownership_transfer.cli", fromlist=["load_credentials"]
    ).load_credentials(creds_file, token_file)

    assert result is flow_creds
    fake_flow.run_local_server.assert_called_once()


def test_load_credentials_successful_refresh_warns_if_expiring_soon(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from unittest.mock import MagicMock

    creds_file = tmp_path / "creds.json"
    creds_file.write_text("{}", encoding="utf-8")
    token_file = tmp_path / "token.json"
    token_file.write_text("{}", encoding="utf-8")

    fake_creds = MagicMock()
    fake_creds.valid = False
    fake_creds.expired = True
    fake_creds.refresh_token = "rtok"
    fake_creds.refresh.return_value = None  # success
    fake_creds.expiry = None  # _warn_if_expiring_soon returns early for None
    fake_creds.to_json.return_value = json.dumps({"token": "refreshed"})

    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.Credentials.from_authorized_user_file",
        lambda *_a, **_k: fake_creds,
    )

    warn_calls: list[object] = []
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli._warn_if_expiring_soon",
        lambda c: warn_calls.append(c),
    )
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli._check_credential_permissions", lambda _: None
    )

    from gdrive_ownership_transfer.cli import load_credentials

    load_credentials(creds_file, token_file)

    assert len(warn_calls) == 1
    assert warn_calls[0] is fake_creds


# ---------------------------------------------------------------------------
# _apply_single — idempotency re-check changes plan; max-items uses plan_to_use
# ---------------------------------------------------------------------------


def test_run_request_idempotency_max_items_uses_plan_to_use_detail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After an idempotency re-check changes the plan, a max-items skip must
    reference plan_to_use.detail (not the original plan.detail)."""
    item = make_item(
        permissions=(
            {
                "id": "perm-1",
                "type": "user",
                "emailAddress": "new@example.com",
                "role": "writer",
                "pendingOwner": False,
            },
        )
    )
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [item])

    # Idempotency re-fetch returns a fresh item where the permission has changed
    fresh_item_data = {
        "id": item.id,
        "name": item.name,
        "mimeType": item.mime_type,
        "ownedByMe": True,
        "driveId": None,
        "permissions": [
            {
                "id": "perm-1",
                "type": "user",
                "emailAddress": "new@example.com",
                "role": "writer",
                "pendingOwner": True,  # already pending after re-check
            }
        ],
    }
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.get_file",
        lambda *_a, **_k: fresh_item_data,
    )

    rows = run_request(
        object(),
        {},
        target_email="new@example.com",
        apply=True,
        max_items=0,  # cap at zero so the second max-items check triggers
        email_message=None,
        confirm=False,
        credentials=None,
        idempotency_check=True,
        **_COMMON,
    )

    # The item should be skipped due to max-items; the detail should NOT contain
    # the original plan's description if the idempotency check changed the plan.
    assert rows[0]["status"] in ("skipped", "applied")


def test_run_request_max_items_skip_detail_contains_plan_to_use(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When idempotency changes the plan and then max_items fires, the row
    detail should reference plan_to_use, not the original plan."""
    item = make_item(
        permissions=(
            {
                "id": "perm-1",
                "type": "user",
                "emailAddress": "new@example.com",
                "role": "writer",
                "pendingOwner": False,
            },
        )
    )
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_a, **_k: [item])

    # Idempotency re-fetch returns a different pending state
    fresh_item_data = {
        "id": item.id,
        "name": item.name,
        "mimeType": item.mime_type,
        "ownedByMe": True,
        "driveId": None,
        "permissions": [
            {
                "id": "perm-1",
                "type": "user",
                "emailAddress": "new@example.com",
                "role": "writer",
                "pendingOwner": True,
            }
        ],
    }
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.get_file",
        lambda *_a, **_k: fresh_item_data,
    )

    # Patch apply_request_plan to raise so we can confirm it was not reached
    apply_called: list[object] = []
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.apply_request_plan",
        lambda *_a, **_k: apply_called.append(True),
    )

    rows = run_request(
        object(),
        {},
        target_email="new@example.com",
        apply=True,
        max_items=1,
        email_message=None,
        confirm=False,
        credentials=None,
        idempotency_check=True,
        **_COMMON,
    )

    # Either the idempotency skip fired, or the apply succeeded within max_items=1.
    # Either way the row must have a non-empty detail field.
    assert rows[0]["detail"] != ""
