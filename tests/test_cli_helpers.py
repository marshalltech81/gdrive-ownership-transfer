from __future__ import annotations

import csv
from pathlib import Path
from types import SimpleNamespace

import pytest
from googleapiclient.errors import HttpError

from gdrive_ownership_transfer.cli import (
    ActionPlan,
    DriveItem,
    apply_accept_plan,
    apply_request_plan,
    execute_with_retries,
    find_user_permission,
    format_http_error,
    format_user,
    infer_target_email,
    is_retryable,
    list_children,
    make_row,
    plan_accept,
    plan_request,
    print_summary,
    run_accept,
    run_request,
    run_scan,
    walk_tree,
    write_report,
)


def make_item(
    *,
    path: str = "Shared/Example",
    owned_by_me: bool = True,
    drive_id: str | None = None,
    permissions: tuple[dict[str, object], ...] = (),
) -> DriveItem:
    return DriveItem(
        id="item-123",
        name="Example",
        mime_type="text/plain",
        path=path,
        owned_by_me=owned_by_me,
        drive_id=drive_id,
        permissions=permissions,
    )


class FakeRequest:
    def __init__(self, payload: object) -> None:
        self.payload = payload

    def execute(self) -> object:
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


def test_infer_target_email_uses_root_owner() -> None:
    root = {"owners": [{"emailAddress": "owner@example.com"}]}

    assert infer_target_email(root, "me@example.com") == "owner@example.com"


def test_infer_target_email_rejects_current_user_as_owner() -> None:
    root = {"owners": [{"emailAddress": "me@example.com"}]}

    with pytest.raises(SystemExit, match="Pass --target-email explicitly"):
        infer_target_email(root, "me@example.com")


def test_format_user_prefers_display_name() -> None:
    user = {"displayName": "Marshall", "emailAddress": "me@example.com"}

    assert format_user(user) == "Marshall <me@example.com>"


def test_plan_request_skips_shared_drive_items() -> None:
    item = make_item(drive_id="drive-123")

    assert plan_request(item, "owner@example.com") == ActionPlan(
        "skip",
        "item belongs to a shared drive",
    )


def test_plan_accept_skips_shared_drive_items() -> None:
    item = make_item(drive_id="drive-123")

    assert plan_accept(item, "recipient@example.com") == ActionPlan(
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


def test_infer_target_email_requires_single_owner() -> None:
    with pytest.raises(SystemExit, match="Pass --target-email explicitly"):
        infer_target_email({"owners": []}, "me@example.com")


def test_format_user_falls_back_to_email_only() -> None:
    assert format_user({"emailAddress": "me@example.com"}) == "me@example.com"


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


def test_format_http_error_reads_json_message() -> None:
    error = make_http_error(403, "quota exceeded")

    assert format_http_error(error) == "HTTP 403: quota exceeded"


def test_format_http_error_handles_non_json_payload() -> None:
    error = HttpError(SimpleNamespace(status=500, reason="error"), b"not-json", uri="")

    assert format_http_error(error) == "HTTP 500"


def test_is_retryable_handles_rate_limit_payload() -> None:
    assert is_retryable(make_http_error(403, "quota exceeded", "rateLimitExceeded")) is True


def test_is_retryable_rejects_non_retryable_payload() -> None:
    assert is_retryable(make_http_error(403, "forbidden", "forbidden")) is False


def test_execute_with_retries_retries_retryable_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = {"count": 0}
    monkeypatch.setattr("gdrive_ownership_transfer.cli.time.sleep", lambda _: None)

    def request_fn() -> str:
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise make_http_error(429, "busy")
        return "ok"

    assert execute_with_retries(request_fn) == "ok"
    assert attempts["count"] == 2


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
    ) -> list[dict[str, object]]:
        assert page_size == 100
        if parent_id == "root":
            return [child_folder]
        if parent_id == "child-folder":
            return [child_file]
        return []

    monkeypatch.setattr("gdrive_ownership_transfer.cli.list_children", fake_list_children)

    items = walk_tree(object(), root, page_size=100)

    assert [item.path for item in items] == [
        "Shared",
        "Shared/Docs",
        "Shared/Docs/notes.txt",
    ]


def test_run_scan_honors_owned_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.walk_tree",
        lambda _service, _root, page_size: [
            make_item(owned_by_me=True),
            make_item(owned_by_me=False),
        ],
    )

    rows = run_scan(object(), {}, page_size=10, owned_only=True)

    assert len(rows) == 1
    assert rows[0]["status"] == "owned-by-me"


def test_run_request_dry_run_and_max_items(monkeypatch: pytest.MonkeyPatch) -> None:
    items = [
        make_item(path="Shared/one"),
        make_item(path="Shared/two"),
    ]
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_args, **_kwargs: items)

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        page_size=10,
        apply=False,
        max_items=1,
        email_message=None,
    )

    assert [row["status"] for row in rows] == ["dry-run", "dry-run"]


def test_run_request_apply_handles_error(monkeypatch: pytest.MonkeyPatch) -> None:
    item = make_item()
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_args, **_kwargs: [item])

    def fake_apply_request_plan(*_args: object, **_kwargs: object) -> None:
        raise make_http_error(403, "quota exceeded")

    monkeypatch.setattr("gdrive_ownership_transfer.cli.apply_request_plan", fake_apply_request_plan)

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        page_size=10,
        apply=True,
        max_items=None,
        email_message=None,
    )

    assert rows[0]["status"] == "error"
    assert "quota exceeded" in rows[0]["detail"]


def test_run_accept_apply_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
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
    calls: list[str] = []
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_args, **_kwargs: [item])
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.apply_accept_plan",
        lambda _service, _item, _plan: calls.append("applied"),
    )

    rows = run_accept(
        object(),
        {},
        recipient_email="recipient@example.com",
        page_size=10,
        apply=True,
        max_items=None,
    )

    assert rows[0]["status"] == "applied"
    assert calls == ["applied"]


def test_run_request_apply_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    item = make_item()
    calls: list[str] = []
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_args, **_kwargs: [item])
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.apply_request_plan",
        lambda *_args, **_kwargs: calls.append("applied"),
    )

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        page_size=10,
        apply=True,
        max_items=None,
        email_message=None,
    )

    assert rows[0]["status"] == "applied"
    assert calls == ["applied"]


def test_run_request_skips_items_when_plan_says_skip(monkeypatch: pytest.MonkeyPatch) -> None:
    item = make_item(owned_by_me=False)
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_args, **_kwargs: [item])

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        page_size=10,
        apply=True,
        max_items=None,
        email_message=None,
    )

    assert rows[0]["status"] == "skipped"


def test_run_request_max_items_stops_after_first_apply(monkeypatch: pytest.MonkeyPatch) -> None:
    items = [make_item(path="Shared/one"), make_item(path="Shared/two")]
    calls: list[str] = []
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_args, **_kwargs: items)
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.apply_request_plan",
        lambda *_args, **_kwargs: calls.append("applied"),
    )

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        page_size=10,
        apply=True,
        max_items=1,
        email_message=None,
    )

    assert [row["status"] for row in rows] == ["applied", "skipped"]
    assert calls == ["applied"]


def test_run_request_max_items_stops_after_first_error(monkeypatch: pytest.MonkeyPatch) -> None:
    items = [make_item(path="Shared/one"), make_item(path="Shared/two")]
    calls: list[str] = []
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_args, **_kwargs: items)

    def fake_apply_request_plan(*_args: object, **_kwargs: object) -> None:
        calls.append("attempted")
        raise make_http_error(403, "quota exceeded")

    monkeypatch.setattr("gdrive_ownership_transfer.cli.apply_request_plan", fake_apply_request_plan)

    rows = run_request(
        object(),
        {},
        target_email="owner@example.com",
        page_size=10,
        apply=True,
        max_items=1,
        email_message=None,
    )

    assert [row["status"] for row in rows] == ["error", "skipped"]
    assert calls == ["attempted"]


def test_run_accept_skips_when_plan_says_skip(monkeypatch: pytest.MonkeyPatch) -> None:
    item = make_item()
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_args, **_kwargs: [item])

    rows = run_accept(
        object(),
        {},
        recipient_email="recipient@example.com",
        page_size=10,
        apply=True,
        max_items=None,
    )

    assert rows[0]["status"] == "skipped"


def test_run_accept_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
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
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_args, **_kwargs: [item])

    rows = run_accept(
        object(),
        {},
        recipient_email="recipient@example.com",
        page_size=10,
        apply=False,
        max_items=None,
    )

    assert rows[0]["status"] == "dry-run"


def test_run_accept_max_items_stops_after_first_apply(monkeypatch: pytest.MonkeyPatch) -> None:
    pending_item = make_item(
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
    items = [pending_item, pending_item]
    calls: list[str] = []
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_args, **_kwargs: items)
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.apply_accept_plan",
        lambda *_args, **_kwargs: calls.append("applied"),
    )

    rows = run_accept(
        object(),
        {},
        recipient_email="recipient@example.com",
        page_size=10,
        apply=True,
        max_items=1,
    )

    assert [row["status"] for row in rows] == ["applied", "skipped"]
    assert calls == ["applied"]


def test_run_accept_max_items_stops_after_first_error(monkeypatch: pytest.MonkeyPatch) -> None:
    pending_item = make_item(
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
    items = [pending_item, pending_item]
    calls: list[str] = []
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_args, **_kwargs: items)

    def fake_apply_accept_plan(*_args: object, **_kwargs: object) -> None:
        calls.append("attempted")
        raise make_http_error(403, "quota exceeded")

    monkeypatch.setattr("gdrive_ownership_transfer.cli.apply_accept_plan", fake_apply_accept_plan)

    rows = run_accept(
        object(),
        {},
        recipient_email="recipient@example.com",
        page_size=10,
        apply=True,
        max_items=1,
    )

    assert [row["status"] for row in rows] == ["error", "skipped"]
    assert calls == ["attempted"]


def test_run_accept_apply_handles_error(monkeypatch: pytest.MonkeyPatch) -> None:
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
    monkeypatch.setattr("gdrive_ownership_transfer.cli.walk_tree", lambda *_args, **_kwargs: [item])
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.apply_accept_plan",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(make_http_error(403, "quota exceeded")),
    )

    rows = run_accept(
        object(),
        {},
        recipient_email="recipient@example.com",
        page_size=10,
        apply=True,
        max_items=None,
    )

    assert rows[0]["status"] == "error"


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


def test_apply_request_plan_rejects_unknown_action() -> None:
    with pytest.raises(ValueError, match="Unsupported request action"):
        apply_request_plan(
            FakeService(),
            make_item(),
            target_email="owner@example.com",
            plan=ActionPlan("unexpected", "oops"),
            email_message=None,
        )


def test_apply_accept_plan_updates_owner_role() -> None:
    permissions_api = FakePermissionsApi()
    service = FakeService(permissions_api=permissions_api)

    apply_accept_plan(service, make_item(), ActionPlan("accept-transfer", "accept", "perm-2"))

    assert permissions_api.update_calls[0]["transferOwnership"] is True
    assert permissions_api.update_calls[0]["body"] == {"role": "owner"}


def test_apply_accept_plan_requires_permission_id() -> None:
    with pytest.raises(ValueError, match="permission id"):
        apply_accept_plan(FakeService(), make_item(), ActionPlan("accept-transfer", "accept"))


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


def test_is_retryable_rejects_non_403_status() -> None:
    assert is_retryable(make_http_error(401, "unauthorized", "forbidden")) is False


def test_is_retryable_rejects_invalid_json_payload() -> None:
    error = HttpError(SimpleNamespace(status=403, reason="error"), b"not-json", uri="")

    assert is_retryable(error) is False


def test_execute_with_retries_reraises_non_retryable_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("gdrive_ownership_transfer.cli.time.sleep", lambda _: None)

    with pytest.raises(HttpError):
        execute_with_retries(
            lambda: (_ for _ in ()).throw(make_http_error(403, "forbidden", "forbidden"))
        )
