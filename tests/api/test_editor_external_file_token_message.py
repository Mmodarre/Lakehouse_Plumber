"""Editor diagnostics name the ``sql_path`` / tags file holding an unresolved token (#298).

The editor shows an error's title as the diagnostic message. A plain
``${name}`` left in a ``sql_path`` file or a UC ``tags_file`` is raised while
the flowgroup resolves, so the diagnostic sits on the flowgroup file; its
message must therefore say which file holds which token.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lhp.api import EditorDiagnosticView, inspect_editor_project

pytestmark = pytest.mark.unit

_FLOWGROUP = """\
pipeline: p1
flowgroup: fg
actions:
  - name: load
    type: load
    source:
      type: sql
      sql_path: sql/q.sql
    target: v_t
  - name: write
    type: write
    source: v_t
    write_target:
      type: streaming_table
      catalog: cat
      schema: bronze
      table: out
"""


def _project(tmp_path: Path, sql: str, target_extra: str = "") -> Path:
    root = tmp_path / "project"
    for sub in ("pipelines/p1", "substitutions", "sql", "schemas"):
        (root / sub).mkdir(parents=True)
    (root / "lhp.yaml").write_text('name: tokens\nversion: "1.0"\n', encoding="utf-8")
    (root / "substitutions" / "dev.yaml").write_text(
        "dev:\n  catalog: cat\n", encoding="utf-8"
    )
    (root / "sql" / "q.sql").write_text(sql, encoding="utf-8")
    (root / "pipelines" / "p1" / "fg.yaml").write_text(
        _FLOWGROUP + target_extra, encoding="utf-8"
    )
    return root


def _only_error(root: Path) -> EditorDiagnosticView:
    errors = [
        diagnostic
        for diagnostic in inspect_editor_project(root).diagnostics
        if diagnostic.severity == "error"
    ]
    assert len(errors) == 1, errors
    return errors[0]


def test_sql_path_token_message_names_the_file_and_the_token(tmp_path):
    root = _project(tmp_path, "SELECT * FROM ${catalog}.raw.t${order_suffix}")

    error = _only_error(root)

    assert error.code == "LHP-CFG-010"
    assert error.message == (
        "Unresolved substitution token(s) ${order_suffix} in sql/q.sql"
    )


def test_tags_file_token_message_names_the_file_and_the_token(tmp_path):
    root = _project(
        tmp_path,
        "SELECT 1 AS id",
        target_extra="      tags_file: schemas/out.yaml\n",
    )
    (root / "schemas" / "out.yaml").write_text(
        'table: out\ntags:\n  team: "${missing_team}"\n', encoding="utf-8"
    )

    error = _only_error(root)

    assert error.code == "LHP-CFG-010"
    assert error.message == (
        "Unresolved substitution token(s) ${missing_team} in schemas/out.yaml"
    )
