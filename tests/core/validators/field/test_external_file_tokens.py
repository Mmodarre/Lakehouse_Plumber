"""``ExternalFileTokenValidator``: unresolved ``${name}`` in referenced files (#298).

Checks the files code generation renders into the pipeline — the ``sql_path``
of a SQL load, a SQL transform and a materialized view, and the UC
``tags_file`` the tagging hook reads — through the same loader and tags check
generation uses (injected by the composition root). Python files are not
checked. Real loader, real tags check, real substitution managers.
"""

import functools
import pickle
from pathlib import Path

import pytest

from lhp.core.codegen import check_tags_file_tokens
from lhp.core.loaders import load_sql_file
from lhp.core.processing import EnhancedSubstitutionManager
from lhp.core.validators import ExternalFileTokenValidator
from lhp.errors import LHPError
from lhp.models import Action, ActionType, FlowGroup, ProjectConfig

pytestmark = pytest.mark.unit

_UNDEFINED_SQL = "SELECT * FROM ${catalog}.s.orders${order_suffix}"


def _validator(root: Path) -> ExternalFileTokenValidator:
    return ExternalFileTokenValidator(
        root,
        load_sql_file,
        functools.partial(
            check_tags_file_tokens,
            project_config=ProjectConfig(name="p"),
            project_root=root,
        ),
    )


def _mgr(root: Path, *, skip_validation: bool = False):
    sub_file = root / "dev.yaml"
    sub_file.write_text("dev:\n  catalog: main\n")
    return EnhancedSubstitutionManager(
        substitution_file=sub_file, env="dev", skip_validation=skip_validation
    )


def _file(root: Path, rel: str, text: str) -> str:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return rel


def _fg(*actions: Action) -> FlowGroup:
    return FlowGroup(pipeline="gold", flowgroup="orders_fg", actions=list(actions))


def _load(**source) -> Action:
    return Action(
        name="load_orders",
        type=ActionType.LOAD,
        source={"type": "sql", **source},
        target="v_orders",
    )


def _transform(**extra) -> Action:
    return Action(
        name="transform_orders",
        type=ActionType.TRANSFORM,
        transform_type="sql",
        source="v_in",
        target="v_orders",
        **extra,
    )


def _mv(**target) -> Action:
    return Action(
        name="write_orders",
        type=ActionType.WRITE,
        write_target={
            "type": "materialized_view",
            "catalog": "c",
            "schema": "s",
            "table": "orders",
            **target,
        },
    )


_SQL_PATH_ACTIONS = {
    "load": lambda path: _load(sql_path=path),
    "transform": lambda path: _transform(sql_path=path),
    "materialized_view": lambda path: _mv(sql_path=path),
}


@pytest.mark.parametrize("kind", sorted(_SQL_PATH_ACTIONS))
def test_undefined_token_in_sql_path_raises_cfg_010(tmp_path, kind):
    path = _file(tmp_path, "sql/q.sql", _UNDEFINED_SQL)
    action = _SQL_PATH_ACTIONS[kind](path)

    with pytest.raises(LHPError) as excinfo:
        _validator(tmp_path).validate(_fg(action), _mgr(tmp_path))

    error = excinfo.value
    assert error.code == "LHP-CFG-010"
    assert error.context["File"] == "sql/q.sql"
    assert error.context["Unresolved"] == "${order_suffix}"
    assert error.context["Pipeline"] == "gold"
    assert error.context["FlowGroup"] == "orders_fg"
    assert error.context["Action"] == action.name


@pytest.mark.parametrize("kind", sorted(_SQL_PATH_ACTIONS))
def test_defined_and_dotted_tokens_pass(tmp_path, kind):
    path = _file(
        tmp_path, "sql/q.sql", "SELECT * FROM ${catalog}.s.t WHERE d >= '${p.start}'"
    )

    _validator(tmp_path).validate(_fg(_SQL_PATH_ACTIONS[kind](path)), _mgr(tmp_path))


@pytest.mark.parametrize(
    "action",
    [
        _load(sql="SELECT 1", sql_path="sql/absent.sql"),
        _transform(sql="SELECT 1", sql_path="sql/absent.sql"),
        _mv(sql="SELECT 1", sql_path="sql/absent.sql"),
    ],
    ids=["load", "transform", "materialized_view"],
)
def test_sql_path_shadowed_by_inline_sql_is_not_read(tmp_path, action):
    """Generation renders the inline ``sql`` and never reads the file."""
    _validator(tmp_path).validate(_fg(action), _mgr(tmp_path))


def test_missing_sql_path_file_raises_io_001(tmp_path):
    with pytest.raises(LHPError) as excinfo:
        _validator(tmp_path).validate(
            _fg(_transform(sql_path="sql/absent.sql")), _mgr(tmp_path)
        )

    assert excinfo.value.code == "LHP-IO-001"


def test_python_module_is_not_checked(tmp_path):
    """Owner decision: Python files keep leaving undefined tokens as-is."""
    module = _file(tmp_path, "py/t.py", "def f(df):\n    return '${missing}'\n")
    action = Action(
        name="py_transform",
        type=ActionType.TRANSFORM,
        transform_type="python",
        source="v_in",
        target="v_out",
        module_path=module,
        function_name="f",
    )

    _validator(tmp_path).validate(_fg(action), _mgr(tmp_path))


def test_undefined_token_in_tags_file_raises_cfg_010(tmp_path):
    tags = _file(
        tmp_path, "schemas/orders.yaml", 'table: orders\ntags:\n  team: "${missing}"\n'
    )
    action = Action(
        name="write_orders",
        type=ActionType.WRITE,
        source="v_orders",
        write_target={
            "type": "streaming_table",
            "catalog": "c",
            "schema": "s",
            "table": "orders",
            "tags_file": tags,
        },
    )

    with pytest.raises(LHPError) as excinfo:
        _validator(tmp_path).validate(_fg(action), _mgr(tmp_path))

    assert excinfo.value.code == "LHP-CFG-010"
    assert excinfo.value.context["File"] == "schemas/orders.yaml"
    assert excinfo.value.context["Action"] == "write_orders"


def test_skip_validation_reads_nothing(tmp_path):
    action = _transform(sql_path="sql/absent.sql")

    _validator(tmp_path).validate(_fg(action), _mgr(tmp_path, skip_validation=True))


def test_secret_references_are_not_recorded_on_the_shared_manager(tmp_path):
    """Generation records a sql_path file's secrets; the check must not add
    them early (they would reach the resolver's secret validation)."""
    path = _file(tmp_path, "sql/q.sql", "SELECT '${secret:scope/key}' AS s")
    mgr = _mgr(tmp_path)

    _validator(tmp_path).validate(_fg(_transform(sql_path=path)), mgr)

    assert mgr.secret_references == set()


def test_validator_pickles(tmp_path):
    """It crosses the worker ``spawn`` boundary with the resolver."""
    restored = pickle.loads(pickle.dumps(_validator(tmp_path)))
    path = _file(tmp_path, "sql/q.sql", _UNDEFINED_SQL)

    with pytest.raises(LHPError):
        restored.validate(_fg(_transform(sql_path=path)), _mgr(tmp_path))
