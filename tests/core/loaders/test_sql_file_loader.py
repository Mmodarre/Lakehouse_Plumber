"""``load_sql_file``: read a ``sql_path`` file, substitute it, reject leftovers (#298).

The SQL load, SQL transform and materialized-view generators read their
``sql_path`` file through this loader, and the flowgroup-resolution validator
reuses it, so ``lhp validate`` and ``lhp generate`` raise the same error.
Real substitution managers over on-disk files — no mocks.
"""

from pathlib import Path

import pytest

from lhp.core.loaders import load_sql_file
from lhp.core.processing import EnhancedSubstitutionManager
from lhp.errors import LHPError

pytestmark = pytest.mark.unit


def _mgr(tmp_path: Path, *, skip_validation: bool = False):
    sub_file = tmp_path / "dev.yaml"
    sub_file.write_text("dev:\n  catalog: main\n  bronze_schema: raw\n")
    return EnhancedSubstitutionManager(
        substitution_file=sub_file, env="dev", skip_validation=skip_validation
    )


def _sql(tmp_path: Path, text: str) -> str:
    sql_dir = tmp_path / "sql"
    sql_dir.mkdir(exist_ok=True)
    (sql_dir / "q.sql").write_text(text)
    return "sql/q.sql"


def test_defined_tokens_are_substituted_and_text_stripped(tmp_path):
    path = _sql(tmp_path, "\n  SELECT * FROM ${catalog}.${bronze_schema}.t  \n")

    assert load_sql_file(path, tmp_path, _mgr(tmp_path)) == "SELECT * FROM main.raw.t"


def test_undefined_plain_token_raises_cfg_010_naming_file_and_token(tmp_path):
    path = _sql(tmp_path, "SELECT * FROM ${catalog}.s.orders${order_suffix}")

    with pytest.raises(LHPError) as excinfo:
        load_sql_file(
            path,
            tmp_path,
            _mgr(tmp_path),
            pipeline="gold",
            flowgroup="orders_fg",
            action="write_orders",
        )

    error = excinfo.value
    assert error.code == "LHP-CFG-010"
    assert "sql/q.sql" in error.details
    assert "${order_suffix}" in error.details
    assert error.context["File"] == "sql/q.sql"
    assert error.context["Unresolved"] == "${order_suffix}"
    assert error.context["Environment"] == "dev"
    assert error.context["Pipeline"] == "gold"
    assert error.context["FlowGroup"] == "orders_fg"
    assert error.context["Action"] == "write_orders"


@pytest.mark.parametrize(
    "reference", ["${mypipeline.start}", "${hiveconf:x}", "${env:X}", "${var.x}"]
)
def test_dotted_or_prefixed_variables_pass_unchanged(tmp_path, reference):
    path = _sql(tmp_path, f"SELECT * FROM t WHERE d >= '{reference}'")

    assert (
        load_sql_file(path, tmp_path, _mgr(tmp_path))
        == f"SELECT * FROM t WHERE d >= '{reference}'"
    )


def test_skip_validation_leaves_undefined_tokens_without_error(tmp_path):
    path = _sql(tmp_path, "SELECT * FROM ${catalog}.s.t${missing}")

    text = load_sql_file(path, tmp_path, _mgr(tmp_path, skip_validation=True))

    assert text == "SELECT * FROM main.s.t${missing}"


def test_without_a_manager_the_file_is_returned_unsubstituted(tmp_path):
    path = _sql(tmp_path, " SELECT ${missing} ")

    assert load_sql_file(path, tmp_path) == "SELECT ${missing}"


def test_secret_reference_becomes_placeholder_and_is_recorded(tmp_path):
    path = _sql(tmp_path, "SELECT '${secret:scope/key}' AS s")
    mgr = _mgr(tmp_path)

    text = load_sql_file(path, tmp_path, mgr)

    assert text == "SELECT '__SECRET_scope_key__' AS s"
    assert {(r.scope, r.key) for r in mgr.secret_references} == {("scope", "key")}


def test_missing_file_raises_io_001(tmp_path):
    with pytest.raises(LHPError) as excinfo:
        load_sql_file("sql/absent.sql", tmp_path, _mgr(tmp_path))

    assert excinfo.value.code == "LHP-IO-001"


def test_unresolved_token_error_title_names_the_file_and_the_token(tmp_path):
    """``lhp validate`` prints a flowgroup failure as ``<code> <title>`` and the
    editor shows only the title, at the flowgroup file: the title must say
    which ``sql_path`` file holds which token."""
    path = _sql(tmp_path, "SELECT * FROM t${order_suffix} WHERE d = '${start_date}'")

    with pytest.raises(LHPError) as excinfo:
        load_sql_file(path, tmp_path, _mgr(tmp_path))

    error = excinfo.value
    assert error.title == (
        "Unresolved substitution token(s) ${order_suffix}, ${start_date} in sql/q.sql"
    )
    assert "if nothing defines it at runtime" in error.details
    assert "Spark substitutes an empty string" in error.details
    assert any("${mypipeline.start_date}" in s for s in error.suggestions)


def test_parameters_are_rendered_before_tokens_are_substituted(tmp_path):
    path = _sql(tmp_path, "\nSELECT * FROM ${catalog}.{{ schema }}.{{ table }}\n")

    text = load_sql_file(
        path, tmp_path, _mgr(tmp_path), parameters={"schema": "s", "table": "t"}
    )

    assert text == "SELECT * FROM main.s.t"


def test_a_parameter_value_may_hold_a_token(tmp_path):
    path = _sql(tmp_path, "SELECT * FROM {{ source }}.t")

    text = load_sql_file(
        path, tmp_path, _mgr(tmp_path), parameters={"source": "${catalog}.raw"}
    )

    assert text == "SELECT * FROM main.raw.t"


def test_without_parameters_braces_are_left_as_written(tmp_path):
    path = _sql(tmp_path, "SELECT '{{ x }}' AS literal")

    assert (
        load_sql_file(path, tmp_path, _mgr(tmp_path)) == "SELECT '{{ x }}' AS literal"
    )


def test_undefined_parameter_raises_cfg_071_with_project_relative_file(tmp_path):
    path = tmp_path / _sql(tmp_path, "SELECT * FROM {{ missing }}")

    with pytest.raises(LHPError) as excinfo:
        load_sql_file(
            path, tmp_path, _mgr(tmp_path), action="write_q", parameters={"t": "x"}
        )

    error = excinfo.value
    assert error.code == "LHP-CFG-071"
    assert error.context["File"] == "sql/q.sql"
    assert error.context["Action"] == "write_q"


def test_unresolved_token_title_is_project_relative_for_an_absolute_path(tmp_path):
    path = tmp_path / _sql(tmp_path, "SELECT ${missing}")

    with pytest.raises(LHPError) as excinfo:
        load_sql_file(path, tmp_path, _mgr(tmp_path))

    assert (
        excinfo.value.title
        == "Unresolved substitution token(s) ${missing} in sql/q.sql"
    )
