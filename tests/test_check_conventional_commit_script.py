from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "check_conventional_commit.py"


def load_script_module():
    spec = importlib.util.spec_from_file_location("check_conventional_commit_script", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("content", ["", "\n\n"])
def test_main_handles_empty_commit_message_file(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    content: str,
) -> None:
    message_file = tmp_path / "COMMIT_EDITMSG"
    message_file.write_text(content, encoding="utf-8")
    module = load_script_module()

    assert module.main([str(message_file)]) == 1
    assert "Conventional Commit validation failed:" in capsys.readouterr().err
