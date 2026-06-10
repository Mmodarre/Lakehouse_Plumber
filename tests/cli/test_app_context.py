"""Tests for the cross-command stateless wiring in ``lhp.cli._app_context``."""

import pytest

from lhp.cli._app_context import require_substitution_file, resolve_project_root
from lhp.errors import LHPError


def test_resolve_project_root_raises_cfg_011_when_no_lhp_yaml(tmp_path, monkeypatch):
    """Empty cwd with no lhp.yaml anywhere up the tree -> LHP-CFG-011."""
    monkeypatch.chdir(tmp_path)
    with pytest.raises(LHPError) as excinfo:
        resolve_project_root()
    assert excinfo.value.code == "LHP-CFG-011"


def test_resolve_project_root_returns_dir_with_lhp_yaml(tmp_path, monkeypatch):
    """lhp.yaml present in cwd -> that directory is returned."""
    (tmp_path / "lhp.yaml").write_text("name: demo\n")
    monkeypatch.chdir(tmp_path)
    assert resolve_project_root() == tmp_path.resolve()


def test_require_substitution_file_raises_io_006_when_missing(tmp_path):
    """Missing substitution file for the requested env -> LHP-IO-006."""
    with pytest.raises(LHPError) as excinfo:
        require_substitution_file(tmp_path, "no_such_env")
    assert excinfo.value.code == "LHP-IO-006"
