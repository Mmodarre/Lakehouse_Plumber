"""Editor diagnostics point operational-metadata errors at lhp.yaml (issue #299).

An unresolved token (``LHP-CFG-010``) or a secret (``LHP-CFG-070``) in an
``lhp.yaml`` operational-metadata ``expression`` is raised while a flowgroup
resolves, so the editor used to attribute it to the flowgroup file, line 0.
The defect lives in ``lhp.yaml``; the diagnostic now points at the column's
``expression`` there. A flowgroup's own unresolved token still points at the
flowgroup file.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lhp.api import EditorDiagnosticView, inspect_editor_project

pytestmark = pytest.mark.unit

_FLOWGROUP = """\
pipeline: p1
flowgroup: fg
operational_metadata: ["_tag"]
actions:
  - name: load
    type: load
    source:
      type: delta
      catalog: cat
      schema: raw
      table: {table}
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

_EXPRESSION_PATH = ("operational_metadata", "columns", "_tag", "expression")


def _project(tmp_path: Path, expression: str, table: str = "t") -> Path:
    root = tmp_path / "project"
    (root / "pipelines" / "p1").mkdir(parents=True)
    (root / "substitutions").mkdir()
    (root / "lhp.yaml").write_text(
        'name: attribution\nversion: "1.0"\n'
        "operational_metadata:\n"
        "  columns:\n"
        "    _tag:\n"
        f'      expression: "{expression}"\n'
        '      applies_to: ["view"]\n',
        encoding="utf-8",
    )
    (root / "substitutions" / "dev.yaml").write_text(
        # Placeholder secret reference, not a real secret.
        'dev:\n  catalog: cat\n  wrapped_secret: "${secret:placeholder_scope/placeholder_key}"\n',
        encoding="utf-8",
    )
    (root / "pipelines" / "p1" / "fg.yaml").write_text(
        _FLOWGROUP.format(table=table), encoding="utf-8"
    )
    return root


def _expression_line(root: Path) -> int:
    lines = (root / "lhp.yaml").read_text(encoding="utf-8").splitlines()
    return next(i for i, line in enumerate(lines) if "expression:" in line)


def _only_error(root: Path) -> EditorDiagnosticView:
    errors = [
        diagnostic
        for diagnostic in inspect_editor_project(root).diagnostics
        if diagnostic.severity == "error"
    ]
    assert len(errors) == 1, errors
    return errors[0]


@pytest.mark.parametrize(
    ("expression", "code"),
    [
        ("F.lit('${missing_token}')", "LHP-CFG-010"),
        ("F.lit('${secret:placeholder_scope/placeholder_key}')", "LHP-CFG-070"),
        ("F.lit('${wrapped_secret}')", "LHP-CFG-070"),
    ],
    ids=["unresolved_token", "direct_secret", "secret_through_token"],
)
def test_metadata_expression_error_points_at_lhp_yaml(tmp_path, expression, code):
    root = _project(tmp_path, expression)

    error = _only_error(root)

    assert error.code == code
    assert error.source is not None
    assert error.source.path == "lhp.yaml"
    assert error.source.yaml_path == _EXPRESSION_PATH
    assert error.source.line == _expression_line(root)


def test_flowgroup_unresolved_token_still_points_at_the_flowgroup(tmp_path):
    root = _project(tmp_path, "F.current_timestamp()", table="${missing_table}")

    error = _only_error(root)

    assert error.code == "LHP-CFG-010"
    assert error.source is not None
    assert error.source.path == "pipelines/p1/fg.yaml"
    assert error.source.yaml_path == ()
