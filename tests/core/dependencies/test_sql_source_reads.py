"""``sql_source_reads``: a SQL action's read of each table its SQL names.

A SQL body that reads a ``STREAM`` has no single read (``effective_read_mode``
is ``None``), so a check of each view's read takes it per name: ``stream`` for
``STREAM(name)``, ``batch`` for a plain reference. The SQL is read the way
``producer_kind`` reads it; anything it cannot read is ``None``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from lhp.core.dependencies import sql_source_reads
from lhp.core.processing import EnhancedSubstitutionManager
from lhp.models import Action, ActionType, TransformType

pytestmark = pytest.mark.unit


def _transform(**fields: Any) -> Action:
    return Action(
        name="t",
        type=ActionType.TRANSFORM,
        transform_type=TransformType.SQL,
        target="v_out",
        **fields,
    )


def test_stream_static_join_reads_each_name_its_way():
    action = _transform(sql="SELECT * FROM STREAM(v_s) s JOIN v_b b ON s.id = b.id")

    assert sql_source_reads(action) == {("v_s", "stream"), ("v_b", "batch")}


def test_sql_load_string_and_mapping_sources():
    string_load = Action(
        name="l", type=ActionType.LOAD, source="SELECT * FROM v", target="v_l"
    )
    mapping_load = Action(
        name="l",
        type=ActionType.LOAD,
        source={"type": "sql", "sql": "SELECT * FROM STREAM v"},
        target="v_l",
    )

    assert sql_source_reads(string_load) == {("v", "batch")}
    assert sql_source_reads(mapping_load) == {("v", "stream")}


def test_sql_file_under_the_project_root_is_substituted(tmp_path: Path):
    (tmp_path / "q.sql").write_text("SELECT * FROM STREAM(${view})")
    substitutions = tmp_path / "dev.yaml"
    substitutions.write_text("dev:\n  view: v_sub\n")
    manager = EnhancedSubstitutionManager(substitutions, "dev")
    action = _transform(sql_path="q.sql")

    assert sql_source_reads(action) is None
    assert sql_source_reads(
        action, project_root=tmp_path, substitution_mgr=manager
    ) == {("v_sub", "stream")}


@pytest.mark.parametrize(
    "action",
    [
        pytest.param(_transform(sql="SELECT * FROM $source"), id="source-placeholder"),
        pytest.param(_transform(sql="SELECT * FROM v WHERE ("), id="unparseable"),
        pytest.param(_transform(sql_path="missing.sql"), id="unreadable-file"),
        pytest.param(
            Action(
                name="w",
                type=ActionType.WRITE,
                source="v",
                write_target={"type": "materialized_view", "table": "t"},
            ),
            id="not-sql",
        ),
        pytest.param(
            Action(
                name="p",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.PYTHON,
                source="v",
                target="v_p",
            ),
            id="python-transform",
        ),
    ],
)
def test_unreadable_or_non_sql_is_none(action: Action):
    assert sql_source_reads(action, project_root=Path("/nonexistent")) is None
