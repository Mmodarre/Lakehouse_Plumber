"""``rendered_sql``: the SQL text a SQL load or SQL transform renders (#346).

:func:`lhp.core.dependencies.rendered_sql` reads a body as the SQL generators
render it: inline SQL substituted, else the ``sql_path`` file under the project
root with its ``${token}`` references substituted; ``None`` when the SQL cannot
be read.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from lhp.core.dependencies import rendered_sql
from lhp.core.processing import EnhancedSubstitutionManager
from lhp.models import Action, ActionType, TransformType

pytestmark = pytest.mark.unit


def _mgr(tmp_path: Path) -> EnhancedSubstitutionManager:
    sub_file = tmp_path / "dev.yaml"
    sub_file.write_text("dev:\n  catalog: acme_dev\n")
    return EnhancedSubstitutionManager(substitution_file=sub_file, env="dev")


def _load(source: Any) -> Action:
    return Action(name="load", type=ActionType.LOAD, source=source, target="v")


def _transform(**fields: Any) -> Action:
    return Action(
        name="transform",
        type=ActionType.TRANSFORM,
        transform_type=TransformType.SQL,
        source="v_in",
        target="v_out",
        **fields,
    )


@pytest.mark.parametrize(
    "action",
    [
        _load({"type": "sql", "sql": "SELECT * FROM STREAM(${catalog}.s.t)"}),
        _transform(sql="  SELECT * FROM STREAM(${catalog}.s.t)\n"),
    ],
    ids=["load-source-sql", "transform-sql-stripped"],
)
def test_inline_sql_is_substituted(tmp_path: Path, action: Action) -> None:
    assert (
        rendered_sql(action, project_root=tmp_path, substitution_mgr=_mgr(tmp_path))
        == "SELECT * FROM STREAM(acme_dev.s.t)"
    )


@pytest.mark.parametrize(
    "action",
    [
        _load({"type": "sql", "sql_path": "sql/orders.sql"}),
        _transform(sql_path="sql/orders.sql"),
    ],
    ids=["load", "transform"],
)
def test_sql_path_file_is_read_with_tokens_substituted(
    tmp_path: Path, action: Action
) -> None:
    (tmp_path / "sql").mkdir()
    (tmp_path / "sql" / "orders.sql").write_text(
        "SELECT * FROM STREAM(${catalog}.silver.orders)\n"
    )

    assert (
        rendered_sql(action, project_root=tmp_path, substitution_mgr=_mgr(tmp_path))
        == "SELECT * FROM STREAM(acme_dev.silver.orders)"
    )


@pytest.mark.parametrize(
    "action",
    [
        _load({"type": "sql", "sql_path": "sql/missing.sql"}),
        _transform(sql_path="sql/missing.sql"),
    ],
    ids=["load", "transform"],
)
def test_unreadable_sql_path_is_none(tmp_path: Path, action: Action) -> None:
    assert (
        rendered_sql(action, project_root=tmp_path, substitution_mgr=_mgr(tmp_path))
        is None
    )
