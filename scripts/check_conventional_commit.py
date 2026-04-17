from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"


def load_validation_helpers():
    if str(SRC_DIR) not in sys.path:
        sys.path.insert(0, str(SRC_DIR))

    from gdrive_ownership_transfer.conventional_commits import (
        format_errors,
        read_commit_subjects_from_range,
        validate_messages,
    )

    return format_errors, read_commit_subjects_from_range, validate_messages


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate Conventional Commit subjects from messages, files, or a git range."
    )
    parser.add_argument(
        "paths",
        nargs="*",
        help=(
            "Optional commit message files, such as the path passed by pre-commit's "
            "commit-msg hook."
        ),
    )
    parser.add_argument(
        "--message",
        action="append",
        default=[],
        help="Explicit message to validate. May be passed multiple times.",
    )
    parser.add_argument(
        "--range",
        dest="revision_range",
        help="Git revision range to inspect, for example abc123..def456.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    format_errors, read_commit_subjects_from_range, validate_messages = load_validation_helpers()
    parser = build_parser()
    args = parser.parse_args(argv)

    messages = list(args.message)
    for path_str in args.paths:
        path = Path(path_str)
        messages.append(path.read_text(encoding="utf-8").splitlines()[0].strip())

    if args.revision_range:
        messages.extend(read_commit_subjects_from_range(args.revision_range))

    if not messages:
        parser.error("provide at least one commit message source")

    errors = validate_messages(messages)
    if errors:
        print(format_errors(errors), file=sys.stderr)
        return 1

    print(f"Validated {len(messages)} conventional commit message(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
