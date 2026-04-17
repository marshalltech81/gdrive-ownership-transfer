from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from gdrive_ownership_transfer import cli


class DummyRequest:
    def __init__(self, payload: object) -> None:
        self.payload = payload

    def execute(self) -> object:
        return self.payload


class DummyFilesApi:
    def __init__(self, payload: object) -> None:
        self.payload = payload
        self.calls: list[dict[str, object]] = []

    def get(self, **kwargs: object) -> DummyRequest:
        self.calls.append(kwargs)
        return DummyRequest(self.payload)


class DummyService:
    def __init__(self, payload: object) -> None:
        self.files_api = DummyFilesApi(payload)

    def files(self) -> DummyFilesApi:
        return self.files_api


class FakeCredentials:
    def __init__(
        self,
        *,
        valid: bool,
        expired: bool = False,
        refresh_token: str | None = None,
        serialized: str = '{"token":"abc"}',
    ) -> None:
        self.valid = valid
        self.expired = expired
        self.refresh_token = refresh_token
        self.serialized = serialized
        self.refreshed = False

    def refresh(self, _request: object) -> None:
        self.refreshed = True
        self.valid = True

    def to_json(self) -> str:
        return self.serialized


def test_build_parser_parses_scan_defaults() -> None:
    parser = cli.build_parser()

    args = parser.parse_args(["scan", "--folder-id", "folder-123"])

    assert args.command == "scan"
    assert args.folder_id == "folder-123"
    assert args.page_size == 100
    assert args.credentials_file == Path("credentials.json")
    assert args.token_file == Path(".tokens/default.json")


def test_build_parser_parses_request_flags() -> None:
    parser = cli.build_parser()

    args = parser.parse_args(
        [
            "request",
            "--folder-id",
            "folder-123",
            "--target-email",
            "owner@example.com",
            "--apply",
            "--email-message",
            "Please accept",
        ]
    )

    assert args.command == "request"
    assert args.target_email == "owner@example.com"
    assert args.apply is True
    assert args.email_message == "Please accept"


def test_build_parser_parses_accept_flags() -> None:
    parser = cli.build_parser()

    args = parser.parse_args(["accept", "--folder-id", "folder-123", "--apply"])

    assert args.command == "accept"
    assert args.apply is True


def test_load_credentials_requires_credentials_file(tmp_path: Path) -> None:
    with pytest.raises(SystemExit, match="OAuth client file not found"):
        cli.load_credentials(tmp_path / "missing.json", tmp_path / "token.json")


def test_load_credentials_uses_valid_cached_token(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    credentials_file = tmp_path / "credentials.json"
    token_file = tmp_path / "token.json"
    credentials_file.write_text("{}", encoding="utf-8")
    token_file.write_text("{}", encoding="utf-8")
    fake_credentials = FakeCredentials(valid=True)

    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.Credentials.from_authorized_user_file",
        lambda *_args, **_kwargs: fake_credentials,
    )

    assert cli.load_credentials(credentials_file, token_file) is fake_credentials


def test_load_credentials_refreshes_expired_token(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    credentials_file = tmp_path / "credentials.json"
    token_file = tmp_path / "token.json"
    credentials_file.write_text("{}", encoding="utf-8")
    token_file.write_text("{}", encoding="utf-8")
    fake_credentials = FakeCredentials(valid=False, expired=True, refresh_token="refresh-token")

    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.Credentials.from_authorized_user_file",
        lambda *_args, **_kwargs: fake_credentials,
    )

    result = cli.load_credentials(credentials_file, token_file)

    assert result is fake_credentials
    assert fake_credentials.refreshed is True
    assert token_file.read_text(encoding="utf-8") == fake_credentials.serialized


def test_load_credentials_runs_oauth_flow_when_needed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    credentials_file = tmp_path / "credentials.json"
    token_file = tmp_path / "token.json"
    credentials_file.write_text("{}", encoding="utf-8")
    fake_credentials = FakeCredentials(valid=True, serialized='{"token":"from-flow"}')
    fake_flow = SimpleNamespace(run_local_server=lambda port: fake_credentials)

    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.InstalledAppFlow.from_client_secrets_file",
        lambda *_args, **_kwargs: fake_flow,
    )

    result = cli.load_credentials(credentials_file, token_file)

    assert result is fake_credentials
    assert token_file.read_text(encoding="utf-8") == '{"token":"from-flow"}'


def test_build_drive_service_uses_expected_arguments(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}
    credentials = SimpleNamespace()

    def fake_build(*args: object, **kwargs: object) -> str:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return "service"

    monkeypatch.setattr("gdrive_ownership_transfer.cli.build", fake_build)

    result = cli.build_drive_service(credentials)

    assert result == "service"
    assert captured["args"] == ("drive", "v3")
    assert captured["kwargs"] == {
        "credentials": credentials,
        "cache_discovery": False,
    }


def test_get_file_uses_files_get() -> None:
    service = DummyService({"id": "file-123"})

    result = cli.get_file(service, "file-123", "id")

    assert result == {"id": "file-123"}
    assert service.files_api.calls == [
        {"fileId": "file-123", "supportsAllDrives": True, "fields": "id"}
    ]


def test_main_scan_branch_writes_report(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "scan-report.csv"
    writes: list[Path] = []

    monkeypatch.setattr(
        "sys.argv",
        [
            "prog",
            "scan",
            "--folder-id",
            "folder-123",
            "--report-file",
            str(report_path),
        ],
    )
    monkeypatch.setattr("gdrive_ownership_transfer.cli.load_credentials", lambda *_args: object())
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.build_drive_service", lambda _creds: object()
    )
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.execute_with_retries",
        lambda _fn: {"user": {"emailAddress": "me@example.com", "displayName": "Me"}},
    )
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.get_file",
        lambda *_args, **_kwargs: {
            "id": "folder-123",
            "name": "Shared",
            "mimeType": cli.FOLDER_MIME_TYPE,
        },
    )
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.run_scan",
        lambda *_args, **_kwargs: [{"status": "owned-by-me"}],
    )
    monkeypatch.setattr("gdrive_ownership_transfer.cli.print_summary", lambda rows: None)
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.write_report",
        lambda path, rows: writes.append(path),
    )

    assert cli.main() == 0
    assert writes == [report_path]


def test_main_request_branch_infers_target_email(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr("sys.argv", ["prog", "request", "--folder-id", "folder-123"])
    monkeypatch.setattr("gdrive_ownership_transfer.cli.load_credentials", lambda *_args: object())
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.build_drive_service", lambda _creds: object()
    )
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.execute_with_retries",
        lambda _fn: {"user": {"emailAddress": "me@example.com", "displayName": "Me"}},
    )
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.get_file",
        lambda *_args, **_kwargs: {
            "id": "folder-123",
            "name": "Shared",
            "mimeType": cli.FOLDER_MIME_TYPE,
            "owners": [{"emailAddress": "owner@example.com"}],
        },
    )

    def fake_run_request(*_args: object, **kwargs: object) -> list[dict[str, str]]:
        captured.update(kwargs)
        return [{"status": "dry-run"}]

    monkeypatch.setattr("gdrive_ownership_transfer.cli.run_request", fake_run_request)
    monkeypatch.setattr("gdrive_ownership_transfer.cli.print_summary", lambda rows: None)

    assert cli.main() == 0
    assert captured["target_email"] == "owner@example.com"


def test_main_accept_branch_uses_authenticated_user_email(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr("sys.argv", ["prog", "accept", "--folder-id", "folder-123"])
    monkeypatch.setattr("gdrive_ownership_transfer.cli.load_credentials", lambda *_args: object())
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.build_drive_service", lambda _creds: object()
    )
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.execute_with_retries",
        lambda _fn: {"user": {"emailAddress": "me@example.com", "displayName": "Me"}},
    )
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.get_file",
        lambda *_args, **_kwargs: {
            "id": "folder-123",
            "name": "Shared",
            "mimeType": cli.FOLDER_MIME_TYPE,
        },
    )

    def fake_run_accept(*_args: object, **kwargs: object) -> list[dict[str, str]]:
        captured.update(kwargs)
        return [{"status": "dry-run"}]

    monkeypatch.setattr("gdrive_ownership_transfer.cli.run_accept", fake_run_accept)
    monkeypatch.setattr("gdrive_ownership_transfer.cli.print_summary", lambda rows: None)

    assert cli.main() == 0
    assert captured["recipient_email"] == "me@example.com"


def test_main_rejects_non_folder_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.argv", ["prog", "scan", "--folder-id", "file-123"])
    monkeypatch.setattr("gdrive_ownership_transfer.cli.load_credentials", lambda *_args: object())
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.build_drive_service", lambda _creds: object()
    )
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.execute_with_retries",
        lambda _fn: {"user": {"emailAddress": "me@example.com"}},
    )
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.get_file",
        lambda *_args, **_kwargs: {
            "id": "file-123",
            "name": "File",
            "mimeType": "text/plain",
        },
    )

    with pytest.raises(SystemExit, match="must point to a Google Drive folder"):
        cli.main()


def test_main_rejects_shared_drive_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.argv", ["prog", "scan", "--folder-id", "folder-123"])
    monkeypatch.setattr("gdrive_ownership_transfer.cli.load_credentials", lambda *_args: object())
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.build_drive_service", lambda _creds: object()
    )
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.execute_with_retries",
        lambda _fn: {"user": {"emailAddress": "me@example.com"}},
    )
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.get_file",
        lambda *_args, **_kwargs: {
            "id": "folder-123",
            "name": "Shared",
            "mimeType": cli.FOLDER_MIME_TYPE,
            "driveId": "drive-123",
        },
    )

    with pytest.raises(SystemExit, match="shared drive"):
        cli.main()


def test_main_rejects_invalid_page_size(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        ["prog", "scan", "--folder-id", "folder-123", "--page-size", "0"],
    )
    monkeypatch.setattr("gdrive_ownership_transfer.cli.load_credentials", lambda *_args: object())
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.build_drive_service", lambda _creds: object()
    )
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.execute_with_retries",
        lambda _fn: {"user": {"emailAddress": "me@example.com"}},
    )
    monkeypatch.setattr(
        "gdrive_ownership_transfer.cli.get_file",
        lambda *_args, **_kwargs: {
            "id": "folder-123",
            "name": "Shared",
            "mimeType": cli.FOLDER_MIME_TYPE,
        },
    )

    with pytest.raises(SystemExit, match="page-size must be between 1 and 1000"):
        cli.main()
