"""CLI acceptance tests for #298: unresolved ``${tokens}`` in referenced files.

A plain ``${name}`` left in a ``sql_path`` file (SQL load, SQL transform,
materialized view) or a UC ``tags_file`` after substitution fails BOTH
``lhp validate --env X`` and ``lhp generate --env X`` with the same
LHP-CFG-010 error, naming the file and the token. Defined tokens resolve;
dotted / prefixed Spark variables pass; ``lhp dag`` (which skips validation)
still runs; inline ``sql:`` keeps its existing error; Python modules are not
checked (owner decision).

Thin ``CliRunner`` tests over real on-disk projects; the check itself is
unit-tested under ``tests/core/``.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner
from conftest import strip_ansi

from lhp.cli.commands.generate_command import generate
from lhp.cli.commands.validate_command import validate_command
from lhp.cli.main import cli

pytestmark = pytest.mark.unit

_FLAGS = ["--no-bundle", "--no-progress", "--show-details"]

_LOAD_SQL = """pipeline: repro
flowgroup: repro_fg
actions:
  - name: load_orders
    type: load
    source:
      type: sql
      sql_path: sql/q.sql
    target: v_orders
  - name: write_orders
    type: write
    source: v_orders
    write_target:
      type: streaming_table
      catalog: c
      schema: s
      table: orders
"""

_TRANSFORM_SQL = """pipeline: repro
flowgroup: repro_fg
actions:
  - name: load_raw
    type: load
    source:
      type: sql
      sql: "SELECT 1 AS id"
    target: v_raw
  - name: transform_orders
    type: transform
    transform_type: sql
    source: v_raw
    sql_path: sql/q.sql
    target: v_orders
  - name: write_orders
    type: write
    source: v_orders
    write_target:
      type: streaming_table
      catalog: c
      schema: s
      table: orders
"""

_MATERIALIZED_VIEW = """pipeline: repro
flowgroup: repro_fg
actions:
  - name: write_orders
    type: write
    write_target:
      type: materialized_view
      catalog: c
      schema: s
      table: orders
      sql_path: sql/q.sql
"""

_FLOWGROUPS = {
    "load_sql": _LOAD_SQL,
    "transform_sql": _TRANSFORM_SQL,
    "materialized_view": _MATERIALIZED_VIEW,
}


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def _project(root: Path, flowgroup: str, sql: str = "SELECT 1 AS id") -> None:
    _write(root / "lhp.yaml", 'name: tok_repro\nversion: "1.0"\n')
    _write(root / "substitutions" / "dev.yaml", "dev:\n  catalog: main\n")
    for sub in ("presets", "templates"):
        (root / sub).mkdir(exist_ok=True)
    _write(root / "sql" / "q.sql", sql)
    _write(root / "pipelines" / "repro" / "p.yaml", flowgroup)


def _flat(text: str) -> str:
    return " ".join(strip_ansi(text).replace("│", " ").split())


def _error_panel(stderr: str) -> str:
    """The rendered ``LHP-...`` error panel (first one), flattened."""
    lines = strip_ansi(stderr).splitlines()
    start = next(i for i, line in enumerate(lines) if line.startswith("╭─ LHP-"))
    end = next(i for i in range(start, len(lines)) if lines[i].startswith("╰"))
    return _flat("\n".join(lines[start : end + 1]))


def _validate():
    return CliRunner().invoke(validate_command, ["--env", "dev", *_FLAGS])


def _generate():
    return CliRunner().invoke(generate, ["-e", "dev", *_FLAGS])


def _generated(root: Path) -> str:
    return (root / "generated" / "dev" / "repro" / "repro_fg.py").read_text()


def _assert_same_cfg_010(validated, generated, *tokens: str, file: str) -> None:
    for result in (validated, generated):
        assert result.exit_code == 1, result.stderr
        flat = _flat(result.stderr)
        assert "LHP-CFG-010" in flat
        assert file in flat
        for token in tokens:
            assert token in flat
        assert "substitutions/dev.yaml" in flat
    assert _error_panel(validated.stderr) == _error_panel(generated.stderr)


@pytest.mark.parametrize("kind", sorted(_FLOWGROUPS))
def test_undefined_token_in_sql_path_fails_validate_and_generate(
    tmp_path, monkeypatch, kind
):
    _project(tmp_path, _FLOWGROUPS[kind], "SELECT * FROM ${catalog}.s.t${order_suffix}")
    monkeypatch.chdir(tmp_path)

    _assert_same_cfg_010(_validate(), _generate(), "${order_suffix}", file="sql/q.sql")
    assert not (tmp_path / "generated" / "dev" / "repro" / "repro_fg.py").exists()


@pytest.mark.parametrize("kind", sorted(_FLOWGROUPS))
def test_defined_and_dotted_tokens_pass(tmp_path, monkeypatch, kind):
    _project(
        tmp_path,
        _FLOWGROUPS[kind],
        "SELECT * FROM ${catalog}.s.t WHERE d >= '${mypipeline.start}'",
    )
    monkeypatch.chdir(tmp_path)

    validated = _validate()
    assert validated.exit_code == 0, validated.stderr
    generated = _generate()
    assert generated.exit_code == 0, generated.stderr
    code = _generated(tmp_path)
    assert "main.s.t" in code
    assert "${mypipeline.start}" in code


def test_undefined_token_in_tags_file_fails_validate_and_generate(
    tmp_path, monkeypatch
):
    _project(
        tmp_path,
        _LOAD_SQL.replace(
            "      table: orders\n",
            "      table: orders\n      tags_file: schemas/orders.yaml\n",
        ),
    )
    _write(
        tmp_path / "schemas" / "orders.yaml",
        'table: orders\ntags:\n  team: "${missing_team}"\n',
    )
    monkeypatch.chdir(tmp_path)

    _assert_same_cfg_010(
        _validate(), _generate(), "${missing_team}", file="schemas/orders.yaml"
    )


def test_dag_still_runs_with_an_undefined_sql_path_token(tmp_path, monkeypatch):
    """``lhp dag`` skips validation; dependency analysis must keep working."""
    _project(tmp_path, _MATERIALIZED_VIEW, "SELECT * FROM c.s.orders${order_suffix}")
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(cli, ["dag", "--format", "json"])

    assert result.exit_code == 0, result.output


def test_inline_sql_keeps_its_existing_error(tmp_path, monkeypatch):
    inline = _LOAD_SQL.replace(
        "      sql_path: sql/q.sql\n", '      sql: "SELECT * FROM t${order_suffix}"\n'
    )
    _project(tmp_path, inline)
    monkeypatch.chdir(tmp_path)

    for result in (_validate(), _generate()):
        assert result.exit_code == 1, result.stderr
        flat = _flat(result.stderr)
        assert "LHP-CFG-010" in flat
        assert "Total Unresolved" in flat


def test_python_module_with_undefined_token_is_not_checked(tmp_path, monkeypatch):
    """Owner decision: Python files keep leaving undefined tokens as written."""
    python_fg = _TRANSFORM_SQL.replace(
        "    transform_type: sql\n    source: v_raw\n    sql_path: sql/q.sql\n",
        "    transform_type: python\n    source: v_raw\n"
        "    module_path: py/t.py\n    function_name: shape\n",
    )
    _project(tmp_path, python_fg)
    _write(
        tmp_path / "py" / "t.py",
        "def shape(df, spark, parameters):\n    return df.filter(\"x = '${missing}'\")\n",
    )
    monkeypatch.chdir(tmp_path)

    validated = _validate()
    assert validated.exit_code == 0, validated.stderr
    generated = _generate()
    assert generated.exit_code == 0, generated.stderr
    copied = [
        p.read_text()
        for p in (tmp_path / "generated" / "dev" / "repro").rglob("*.py")
        if "${missing}" in p.read_text()
    ]
    assert copied, "the Python module must be copied with the token as written"


def _summary(*args: str):
    """``lhp validate`` without ``--show-details``: one summary row per failure."""
    return CliRunner().invoke(
        validate_command, ["--env", "dev", "--no-bundle", "--no-progress", *args]
    )


@pytest.mark.parametrize("kind", sorted(_FLOWGROUPS))
def test_validate_summary_row_names_the_sql_file_and_the_token(
    tmp_path, monkeypatch, kind
):
    """The summary row shows the flowgroup file, the code and the error title;
    the title must say which ``sql_path`` file holds which token."""
    _project(tmp_path, _FLOWGROUPS[kind], "SELECT * FROM ${catalog}.s.t${order_suffix}")
    monkeypatch.chdir(tmp_path)

    result = _summary()

    assert result.exit_code == 1, result.stderr
    flat = _flat(result.output + result.stderr)
    assert (
        "LHP-CFG-010 Unresolved substitution token(s) ${order_suffix} in sql/q.sql"
        in flat
    )
    assert "Unresolved substitution tokens detected" not in flat


def test_validate_summary_row_names_the_tags_file_and_the_token(tmp_path, monkeypatch):
    _project(
        tmp_path,
        _LOAD_SQL.replace(
            "      table: orders\n",
            "      table: orders\n      tags_file: schemas/orders.yaml\n",
        ),
    )
    _write(
        tmp_path / "schemas" / "orders.yaml",
        'table: orders\ntags:\n  team: "${missing_team}"\n',
    )
    monkeypatch.chdir(tmp_path)

    result = _summary()

    assert result.exit_code == 1, result.stderr
    flat = _flat(result.output + result.stderr)
    assert (
        "LHP-CFG-010 Unresolved substitution token(s) ${missing_team} in "
        "schemas/orders.yaml" in flat
    )


def test_pipeline_parameter_in_sql_path_gets_accurate_runtime_guidance(
    tmp_path, monkeypatch
):
    """A plain ``${start_date}`` meant as a pipeline ``configuration:`` parameter
    still fails (LHP cannot tell it from a missing key), but the panel must not
    claim Spark always blanks it, and must offer the dotted-key form besides
    ``substitutions/<env>.yaml``."""
    _project(tmp_path, _LOAD_SQL, "SELECT * FROM t WHERE d >= '${start_date}'")
    _write(
        tmp_path / "config" / "pipeline_config.yaml",
        "pipeline: repro\nconfiguration:\n  start_date: '2024-01-01'\n",
    )
    monkeypatch.chdir(tmp_path)

    result = _validate()

    assert result.exit_code == 1, result.stderr
    panel = _error_panel(result.stderr)
    assert "LHP-CFG-010" in panel
    assert "if nothing defines it at runtime" in panel
    assert "substitutions/dev.yaml" in panel
    assert "${mypipeline.start_date}" in panel
