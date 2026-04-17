from __future__ import annotations

import re
import shutil
import subprocess  # nosec B404
from collections.abc import Iterable

ALLOWED_TYPES = (
    "build",
    "chore",
    "ci",
    "docs",
    "feat",
    "fix",
    "perf",
    "refactor",
    "revert",
    "style",
    "test",
)

CONVENTIONAL_COMMIT_PATTERN = re.compile(
    r"^(?P<type>" + "|".join(ALLOWED_TYPES) + r")"
    r"(\([a-z0-9._/-]+\))?"
    r"(!)?"
    r": "
    r"(?P<description>\S.*)$"
)


def is_conventional_commit(message: str) -> bool:
    return CONVENTIONAL_COMMIT_PATTERN.match(message.strip()) is not None


def validate_messages(messages: Iterable[str]) -> list[str]:
    errors: list[str] = []
    for message in messages:
        subject = message.strip()
        if not subject:
            errors.append("Encountered an empty commit subject.")
            continue
        if not is_conventional_commit(subject):
            errors.append(
                f"Invalid Conventional Commit subject: {subject!r}. "
                f"Expected '<type>[(optional-scope)][!]: description' "
                f"where type is one of: {', '.join(ALLOWED_TYPES)}."
            )
    return errors


def format_errors(errors: Iterable[str]) -> str:
    lines = ["Conventional Commit validation failed:"]
    lines.extend(f"- {error}" for error in errors)
    return "\n".join(lines)


def read_commit_subjects_from_range(revision_range: str) -> list[str]:
    _rev = r"[A-Za-z0-9][A-Za-z0-9._/\-^]*"
    if not re.fullmatch(rf"{_rev}\.\.{_rev}", revision_range):
        raise ValueError(f"Unsafe git revision range: {revision_range!r}")

    git_executable = shutil.which("git")
    if not git_executable:
        raise RuntimeError("git executable not found on PATH")

    completed = subprocess.run(
        [git_executable, "log", "--format=%s", revision_range],
        check=True,
        capture_output=True,
        text=True,
    )  # nosec B603
    return [line.strip() for line in completed.stdout.splitlines() if line.strip()]
