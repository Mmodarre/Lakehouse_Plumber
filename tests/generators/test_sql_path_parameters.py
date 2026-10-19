"""SQL generators render a ``sql_path`` file with the action's ``sql_parameters``.

The SQL load, SQL transform and materialized-view generators read their file
through ``load_sql_file``, which renders it as a Jinja template with the
action's ``sql_parameters`` before ``${token}`` substitution. An undefined name
raises LHP-CFG-071 naming the file and the action. Without ``sql_parameters``
the file is read as before. Real substitution managers.
"""

from pathlib import Path

import pytest

from lhp.core.processing import EnhancedSubstitutionManager
from lhp.errors import LHPError
from lhp.generators.load.sql import SQLLoadGenerator
from lhp.generators.transform.sql import SQLTransformGenerator
from lhp.generators.write.materialized_view import MaterializedViewWriteGenerator
from lhp.models import Action, ActionType, FlowGroup

pytestmark = pytest.mark.unit

_SQL_FILE = "sql/q.sql"


def _load_action(**extra):
    return Action(
        name="load_q",
        type=ActionType.LOAD,
        source={"type": "sql", "sql_path": _SQL_FILE},
        target="v_q",
        **extra,
    )


def _transform_action(**extra):
    return Action(
        name="transform_q",
        type=ActionType.TRANSFORM,
        transform_type="sql",
        source="v_in",
        target="v_q",
        sql_path=_SQL_FILE,
        **extra,
    )


def _mv_action(**extra):
    return Action(
        name="write_q",
        type=ActionType.WRITE,
        write_target={
            "type": "materialized_view",
            "catalog": "c",
            "schema": "s",
            "table": "q",
            "sql_path": _SQL_FILE,
        },
        **extra,
    )


_GENERATORS = {
    "load": (SQLLoadGenerator, _load_action),
    "transform": (SQLTransformGenerator, _transform_action),
    "materialized_view": (MaterializedViewWriteGenerator, _mv_action),
}


def _context(tmp_path: Path) -> dict:
    sub_file = tmp_path / "dev.yaml"
    sub_file.write_text("dev:\n  catalog: main\n")
    return {
        "project_root": tmp_path,
        "substitution_manager": EnhancedSubstitutionManager(
            substitution_file=sub_file, env="dev"
        ),
        "secret_references": set(),
        "flowgroup": FlowGroup(pipeline="gold", flowgroup="q_fg", actions=[]),
    }


def _write_sql(tmp_path: Path, text: str) -> None:
    (tmp_path / "sql").mkdir(exist_ok=True)
    (tmp_path / _SQL_FILE).write_text(text)


@pytest.mark.parametrize("kind", sorted(_GENERATORS))
def test_sql_parameters_render_into_the_generated_sql(tmp_path, kind):
    generator_cls, make_action = _GENERATORS[kind]
    _write_sql(tmp_path, "SELECT * FROM ${catalog}.{{ silver_schema }}.orders")

    code = generator_cls().generate(
        make_action(sql_parameters={"silver_schema": "silver"}), _context(tmp_path)
    )

    assert "main.silver.orders" in code
    assert "{{" not in code


@pytest.mark.parametrize("kind", sorted(_GENERATORS))
def test_without_sql_parameters_the_file_is_not_rendered(tmp_path, kind):
    generator_cls, make_action = _GENERATORS[kind]
    _write_sql(tmp_path, "SELECT '{{ literal }}' AS x FROM ${catalog}.s.t")

    code = generator_cls().generate(make_action(), _context(tmp_path))

    assert "{{ literal }}" in code


@pytest.mark.parametrize("kind", sorted(_GENERATORS))
def test_undefined_name_raises_cfg_071_naming_file_and_action(tmp_path, kind):
    generator_cls, make_action = _GENERATORS[kind]
    _write_sql(tmp_path, "SELECT * FROM {{ gold_schema }}.orders")
    action = make_action(sql_parameters={"silver_schema": "silver"})

    with pytest.raises(LHPError) as excinfo:
        generator_cls().generate(action, _context(tmp_path))

    error = excinfo.value
    assert error.code == "LHP-CFG-071"
    assert error.context["File"] == _SQL_FILE
    assert error.context["Pipeline"] == "gold"
    assert error.context["FlowGroup"] == "q_fg"
    assert error.context["Action"] == action.name
