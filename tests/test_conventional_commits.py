from __future__ import annotations

from types import SimpleNamespace

import pytest

from gdrive_ownership_transfer.conventional_commits import (
    format_errors,
    is_conventional_commit,
    read_commit_subjects_from_range,
    validate_messages,
)


@pytest.mark.parametrize(
    "message",
    [
        "feat: add recursive ownership scan",
        "fix(cli): reject shared-drive roots",
        "chore(ci): add security workflow",
        "refactor!: simplify traversal planning",
    ],
)
def test_is_conventional_commit_accepts_valid_subjects(message: str) -> None:
    assert is_conventional_commit(message)


@pytest.mark.parametrize(
    "message",
    [
        "",
        "update readme",
        "feature: wrong type name",
        "fix missing colon",
        "docs(scope) missing colon",
    ],
)
def test_validate_messages_rejects_invalid_subjects(message: str) -> None:
    assert validate_messages([message])


def test_format_errors_is_readable() -> None:
    rendered = format_errors(["Invalid Conventional Commit subject: 'oops'."])

    assert rendered.startswith("Conventional Commit validation failed:")
    assert "- Invalid Conventional Commit subject: 'oops'." in rendered


def test_read_commit_subjects_from_range_reads_git_log(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "gdrive_ownership_transfer.conventional_commits.shutil.which",
        lambda name: f"/usr/bin/{name}",
    )
    monkeypatch.setattr(
        "gdrive_ownership_transfer.conventional_commits.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(stdout="feat: one\nfix: two\n"),
    )

    subjects = read_commit_subjects_from_range("abc123..def456")

    assert subjects == ["feat: one", "fix: two"]


def test_read_commit_subjects_from_range_rejects_unsafe_input() -> None:
    with pytest.raises(ValueError, match="Unsafe git revision range"):
        read_commit_subjects_from_range("abc123; rm -rf")


def test_read_commit_subjects_from_range_requires_git(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "gdrive_ownership_transfer.conventional_commits.shutil.which",
        lambda _: None,
    )

    with pytest.raises(RuntimeError, match="git executable not found"):
        read_commit_subjects_from_range("abc123..def456")
