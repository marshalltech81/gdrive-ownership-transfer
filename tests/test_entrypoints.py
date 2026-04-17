from __future__ import annotations

import importlib
import runpy
import sys

import pytest


def test_module_entrypoint_is_importable() -> None:
    module = importlib.import_module("gdrive_ownership_transfer.__main__")

    assert hasattr(module, "main")


def test_module_entrypoint_runs_main(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("gdrive_ownership_transfer.cli.main", lambda: 0)
    monkeypatch.delitem(sys.modules, "gdrive_ownership_transfer.__main__", raising=False)

    with pytest.raises(SystemExit, match="0"):
        runpy.run_module("gdrive_ownership_transfer", run_name="__main__")
