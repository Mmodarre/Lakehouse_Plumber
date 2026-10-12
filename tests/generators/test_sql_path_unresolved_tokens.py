"""SQL generators reject a ``sql_path`` file that keeps an undefined ``${name}`` (#298).

The SQL load, SQL transform and materialized-view generators read their
``sql_path`` file through ``load_sql_file``. A plain ``${name}`` left after
substitution raises LHP-CFG-010 naming the file, the token and the action;
defined tokens are substituted; dotted / prefixed Spark variables pass; a
manager built with ``skip_validation=True`` keeps the old leave-as-is
behaviour. Inline ``sql`` is checked during flowgroup resolution, not here, so
the generators keep rendering it as before. Real substitution managers.
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


def _load_action(source_extra):
    return Action(
        name="load_q",
        type=ActionType.LOAD,
        source={"type": "sql", **source_extra},
        target="v_q",
    )


def _transform_action(**extra):
    return Action(
        name="transform_q",
        type=ActionType.TRANSFORM,
        transform_type="sql",
        source="v_in",
        target="v_q",
        **extra,
    )


def _mv_action(target_extra):
    return Action(
        name="write_q",
        type=ActionType.WRITE,
        write_target={
            "type": "materialized_view",
            "catalog": "c",
            "schema": "s",
            "table": "q",
            **target_extra,
        },
    )


_GENERATORS = {
    "load": (SQLLoadGenerator, lambda: _load_action({"sql_path": _SQL_FILE})),
    "transform": (SQLTransformGenerator, lambda: _transform_action(sql_path=_SQL_FILE)),
    "materialized_view": (
        MaterializedViewWriteGenerator,
        lambda: _mv_action({"sql_path": _SQL_FILE}),
    ),
}


def _context(tmp_path: Path, *, skip_validation: bool = False) -> dict:
    sub_file = tmp_path / "dev.yaml"
    sub_file.write_text("dev:\n  catalog: main\n")
    mgr = EnhancedSubstitutionManager(
        substitution_file=sub_file, env="dev", skip_validation=skip_validation
    )
    return {
        "project_root": tmp_path,
        "substitution_manager": mgr,
        "secret_references": set(),
        "flowgroup": FlowGroup(pipeline="gold", flowgroup="q_fg", actions=[]),
    }


def _write_sql(tmp_path: Path, text: str) -> None:
    (tmp_path / "sql").mkdir(exist_ok=True)
    (tmp_path / _SQL_FILE).write_text(text)


@pytest.mark.parametrize("kind", sorted(_GENERATORS))
def test_undefined_plain_token_raises_cfg_010(tmp_path, kind):
    generator_cls, make_action = _GENERATORS[kind]
    _write_sql(tmp_path, "SELECT * FROM ${catalog}.s.orders${order_suffix}")
    action = make_action()

    with pytest.raises(LHPError) as excinfo:
        generator_cls().generate(action, _context(tmp_path))

    error = excinfo.value
    assert error.code == "LHP-CFG-010"
    assert _SQL_FILE in error.details
    assert "${order_suffix}" in error.details
    assert error.context["File"] == _SQL_FILE
    assert error.context["Pipeline"] == "gold"
    assert error.context["FlowGroup"] == "q_fg"
    assert error.context["Action"] == action.name


@pytest.mark.parametrize("kind", sorted(_GENERATORS))
def test_defined_token_is_substituted(tmp_path, kind):
    generator_cls, make_action = _GENERATORS[kind]
    _write_sql(tmp_path, "SELECT * FROM ${catalog}.s.orders")

    code = generator_cls().generate(make_action(), _context(tmp_path))

    assert "main.s.orders" in code
    assert "${catalog}" not in code


@pytest.mark.parametrize("kind", sorted(_GENERATORS))
def test_dotted_or_prefixed_variables_pass(tmp_path, kind):
    generator_cls, make_action = _GENERATORS[kind]
    _write_sql(
        tmp_path,
        "SELECT * FROM t WHERE d >= '${mypipeline.start}' AND e = '${hiveconf:x}'",
    )

    code = generator_cls().generate(make_action(), _context(tmp_path))

    assert "${mypipeline.start}" in code
    assert "${hiveconf:x}" in code


@pytest.mark.parametrize("kind", sorted(_GENERATORS))
def test_skip_validation_leaves_undefined_token(tmp_path, kind):
    generator_cls, make_action = _GENERATORS[kind]
    _write_sql(tmp_path, "SELECT * FROM t${order_suffix}")

    code = generator_cls().generate(
        make_action(), _context(tmp_path, skip_validation=True)
    )

    assert "t${order_suffix}" in code


@pytest.mark.parametrize(
    "action",
    [
        _load_action({"sql": "SELECT '${undefined_inline}' AS x"}),
        _transform_action(sql="SELECT '${undefined_inline}' AS x"),
        _mv_action({"sql": "SELECT '${undefined_inline}' AS x"}),
    ],
    ids=["load", "transform", "materialized_view"],
)
def test_inline_sql_rendering_is_unchanged(tmp_path, action):
    generator_cls = {
        ActionType.LOAD: SQLLoadGenerator,
        ActionType.TRANSFORM: SQLTransformGenerator,
        ActionType.WRITE: MaterializedViewWriteGenerator,
    }[action.type]

    code = generator_cls().generate(action, _context(tmp_path))

    assert "${undefined_inline}" in code


@pytest.mark.parametrize("kind", sorted(_GENERATORS))
def test_secret_in_sql_file_is_recorded_on_the_context(tmp_path, kind):
    generator_cls, make_action = _GENERATORS[kind]
    _write_sql(tmp_path, "SELECT '${secret:scope/key}' AS s")
    context = _context(tmp_path)

    generator_cls().generate(make_action(), context)

    assert {(r.scope, r.key) for r in context["secret_references"]} == {
        ("scope", "key")
    }
