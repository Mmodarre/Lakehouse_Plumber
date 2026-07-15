"""Tests for GET /api/operational-metadata.

The endpoint resolves the operational-metadata columns/presets available to
the current project using the SAME ``_get_available_columns`` REPLACE
semantics the generator uses: when ``lhp.yaml`` declares
``operational_metadata.columns`` those project columns SUPPRESS the five
built-ins; otherwise the five built-ins are returned. Each test builds an
isolated deep copy of the shared E2E fixture project (never edited in place)
and overwrites only its ``lhp.yaml``.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from .conftest import _client_for

pytestmark = pytest.mark.webapp

# The five built-in operational-metadata columns and their ``applies_to``
# target lists (mirrors OperationalMetadataCatalog.default_columns).
BUILTIN_APPLIES_TO = {
    "_ingestion_timestamp": ["streaming_table", "materialized_view", "view"],
    "_source_file": ["view"],
    "_pipeline_run_id": ["streaming_table", "materialized_view", "view"],
    "_pipeline_name": ["streaming_table", "materialized_view", "view"],
    "_flowgroup_name": ["streaming_table", "materialized_view", "view"],
}

_LHP_YAML_NO_COLUMNS = """\
name: acme_edw
version: "1.0"
"""

_LHP_YAML_WITH_COLUMNS = """\
name: acme_edw
version: "1.0"

operational_metadata:
  columns:
    _custom_col:
      expression: "F.lit('x')"
      description: "A custom project column"
      applies_to: ["streaming_table", "materialized_view", "view"]
    _another_col:
      expression: "F.current_timestamp()"
      applies_to: ["view"]
  presets:
    my_preset:
      columns: ["_custom_col"]
      description: "A demo preset"
"""


def _project_copy(tmp_path: Path, e2e_project_path: Path, lhp_yaml: str) -> Path:
    """Deep-copy the shared fixture project and overwrite its ``lhp.yaml``."""
    dest = tmp_path / "testing_project"
    shutil.copytree(e2e_project_path, dest)
    (dest / "lhp.yaml").write_text(lhp_yaml, encoding="utf-8")
    return dest


class TestBuiltinColumns:
    """Case A — no project columns: the five built-ins are returned."""

    def test_returns_five_builtins(
        self,
        tmp_path: Path,
        e2e_project_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        project = _project_copy(tmp_path, e2e_project_path, _LHP_YAML_NO_COLUMNS)
        with _client_for(monkeypatch, project) as client:
            resp = client.get("/api/operational-metadata")

        assert resp.status_code == 200, resp.text
        data = resp.json()
        names = {col["name"] for col in data["columns"]}
        assert names == set(BUILTIN_APPLIES_TO)

    def test_builtin_source_and_applies_to(
        self,
        tmp_path: Path,
        e2e_project_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        project = _project_copy(tmp_path, e2e_project_path, _LHP_YAML_NO_COLUMNS)
        with _client_for(monkeypatch, project) as client:
            data = client.get("/api/operational-metadata").json()

        by_name = {col["name"]: col for col in data["columns"]}
        for name, expected_applies_to in BUILTIN_APPLIES_TO.items():
            assert by_name[name]["source"] == "builtin"
            assert by_name[name]["applies_to"] == expected_applies_to
        # `_source_file` is view-only; `_ingestion_timestamp` spans all three.
        assert by_name["_source_file"]["applies_to"] == ["view"]
        assert by_name["_ingestion_timestamp"]["applies_to"] == [
            "streaming_table",
            "materialized_view",
            "view",
        ]

    def test_no_presets_when_none_declared(
        self,
        tmp_path: Path,
        e2e_project_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        project = _project_copy(tmp_path, e2e_project_path, _LHP_YAML_NO_COLUMNS)
        with _client_for(monkeypatch, project) as client:
            data = client.get("/api/operational-metadata").json()

        assert data["presets"] == []


class TestProjectColumns:
    """Case B — project columns: built-ins suppressed (REPLACE), presets returned."""

    def test_builtins_suppressed_only_project_columns(
        self,
        tmp_path: Path,
        e2e_project_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        project = _project_copy(tmp_path, e2e_project_path, _LHP_YAML_WITH_COLUMNS)
        with _client_for(monkeypatch, project) as client:
            data = client.get("/api/operational-metadata").json()

        names = {col["name"] for col in data["columns"]}
        assert names == {"_custom_col", "_another_col"}
        # None of the five built-ins leak through when project columns exist.
        assert names.isdisjoint(BUILTIN_APPLIES_TO)

    def test_project_columns_source_and_fields(
        self,
        tmp_path: Path,
        e2e_project_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        project = _project_copy(tmp_path, e2e_project_path, _LHP_YAML_WITH_COLUMNS)
        with _client_for(monkeypatch, project) as client:
            data = client.get("/api/operational-metadata").json()

        by_name = {col["name"]: col for col in data["columns"]}
        assert all(col["source"] == "project" for col in data["columns"])
        assert by_name["_custom_col"]["expression"] == "F.lit('x')"
        assert by_name["_custom_col"]["description"] == "A custom project column"
        assert by_name["_custom_col"]["applies_to"] == [
            "streaming_table",
            "materialized_view",
            "view",
        ]
        # A column without a description reports null, not the empty string.
        assert by_name["_another_col"]["description"] is None
        assert by_name["_another_col"]["applies_to"] == ["view"]

    def test_presets_returned(
        self,
        tmp_path: Path,
        e2e_project_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        project = _project_copy(tmp_path, e2e_project_path, _LHP_YAML_WITH_COLUMNS)
        with _client_for(monkeypatch, project) as client:
            data = client.get("/api/operational-metadata").json()

        presets = {p["name"]: p for p in data["presets"]}
        assert set(presets) == {"my_preset"}
        assert presets["my_preset"]["columns"] == ["_custom_col"]
        assert presets["my_preset"]["description"] == "A demo preset"
