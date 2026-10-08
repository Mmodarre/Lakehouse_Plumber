"""CLI acceptance tests for #289: ``${tokens}`` in operational-metadata expressions.

``lhp generate --env X`` resolves environment tokens in ``lhp.yaml``
operational-metadata expressions, and ``lhp validate --env X`` reports exactly
the errors ``lhp generate --env X`` raises for the same project:

- undefined ``${token}`` / ``%{var}`` / ``{{ param }}`` -> LHP-CFG-010
- ``${secret:...}`` -> LHP-CFG-070 (secrets would be written into table data)

Thin ``CliRunner`` tests over real on-disk projects; the expression logic is
unit-tested in ``tests/core/codegen/operational_metadata/test_expression.py``.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner
from conftest import strip_ansi

from lhp.cli.commands.generate_command import generate
from lhp.cli.commands.validate_command import validate_command

pytestmark = pytest.mark.unit

_FLOWGROUP = """pipeline: repro
flowgroup: repro_fg
actions:
  - name: load_raw
    type: load
    operational_metadata: [_source, _pipeline]
    source:
      type: sql
      sql: "SELECT 1 AS id"
    target: v_raw
  - name: write_raw
    type: write
    source: v_raw
    write_target:
      type: streaming_table
      catalog: c
      schema: s
      table: t
"""


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def _project(root: Path, source_expression: str) -> None:
    """One flowgroup selecting ``_source`` (parameterised) and ``_pipeline``."""
    _write(
        root / "lhp.yaml",
        "name: om_repro\n"
        'version: "1.0"\n'
        "operational_metadata:\n"
        "  columns:\n"
        "    _source:\n"
        f"      expression: {source_expression}\n"
        '      applies_to: ["view"]\n'
        "    _pipeline:\n"
        "      expression: \"F.lit('${pipeline_name}')\"\n"
        '      applies_to: ["view"]\n',
    )
    _write(
        root / "substitutions" / "dev.yaml",
        "dev:\n  fd_source: STXX\n  pipeline_name: FROM_SUBS\n",
    )
    _write(root / "substitutions" / "prod.yaml", "prod:\n  fd_source: STPROD\n")
    for sub in ("presets", "templates"):
        (root / sub).mkdir(exist_ok=True)
    _write(root / "pipelines" / "repro" / "p.yaml", _FLOWGROUP)


def _flat(text: str) -> str:
    return " ".join(strip_ansi(text).replace("│", " ").split())


def _error_panel(stderr: str) -> str:
    """The rendered ``LHP-...`` error panel (first one), flattened."""
    lines = strip_ansi(stderr).splitlines()
    start = next(i for i, line in enumerate(lines) if line.startswith("╭─ LHP-"))
    end = next(i for i in range(start, len(lines)) if lines[i].startswith("╰"))
    return _flat("\n".join(lines[start : end + 1]))


# ``--show-details`` renders the full error panel (details + context), so the
# column name and the unresolved token are asserted on, not just the code.
_FLAGS = ["--no-bundle", "--no-progress", "--show-details"]


def _validate(env: str = "dev", *flags: str):
    return CliRunner().invoke(validate_command, ["--env", env, *_FLAGS, *flags])


def _generate(env: str = "dev", *flags: str):
    return CliRunner().invoke(generate, ["-e", env, *_FLAGS, *flags])


def _generated(root: Path, env: str = "dev") -> str:
    return (root / "generated" / env / "repro" / "repro_fg.py").read_text()


@pytest.mark.parametrize(("env", "value"), [("dev", "STXX"), ("prod", "STPROD")])
def test_env_token_resolves_per_environment(tmp_path, monkeypatch, env, value):
    _project(tmp_path, "\"F.lit('${fd_source}')\"")
    monkeypatch.chdir(tmp_path)

    validated = _validate(env)
    assert validated.exit_code == 0, validated.stderr

    generated = _generate(env)
    assert generated.exit_code == 0, generated.stderr
    code = _generated(tmp_path, env)
    assert f'F.lit("{value}")' in code
    assert "${fd_source}" not in code


def test_context_tokens_win_over_substitution_keys(tmp_path, monkeypatch):
    """``substitutions/dev.yaml`` defines ``pipeline_name: FROM_SUBS``; the
    real pipeline name still wins inside the metadata expression."""
    _project(tmp_path, "\"F.lit('${fd_source}')\"")
    monkeypatch.chdir(tmp_path)

    generated = _generate()
    assert generated.exit_code == 0, generated.stderr
    code = _generated(tmp_path)
    assert 'F.lit("repro")' in code
    assert "FROM_SUBS" not in code


@pytest.mark.parametrize(
    ("expression", "token"),
    [
        ("\"F.lit('${fd_missing}')\"", "${fd_missing}"),
        ("\"F.lit('%{local_var}')\"", "%{local_var}"),
        ("\"F.lit('{{ table_name }}')\"", "{{ table_name }}"),
    ],
)
def test_unresolved_token_fails_validate_and_generate_with_cfg_010(
    tmp_path, monkeypatch, expression, token
):
    _project(tmp_path, expression)
    monkeypatch.chdir(tmp_path)

    validated = _validate()
    generated = _generate()

    for result in (validated, generated):
        assert result.exit_code == 1, result.stderr
        flat = _flat(result.stderr)
        assert "LHP-CFG-010" in flat
        assert "_source" in flat
        assert token in flat
        assert "substitutions/dev.yaml" in flat
    # Same error, word for word, from both commands.
    assert _error_panel(validated.stderr) == _error_panel(generated.stderr)
    assert not (tmp_path / "generated" / "dev" / "repro" / "repro_fg.py").exists()


def test_secret_fails_validate_and_generate_with_cfg_070(tmp_path, monkeypatch):
    _project(tmp_path, "\"F.lit('${secret:scope/key}')\"")
    monkeypatch.chdir(tmp_path)

    validated = _validate()
    generated = _generate()

    for result in (validated, generated):
        assert result.exit_code == 1, result.stderr
        flat = _flat(result.stderr)
        assert "LHP-CFG-070" in flat
        assert "_source" in flat
        assert "table data" in flat
    assert _error_panel(validated.stderr) == _error_panel(generated.stderr)
    assert not (tmp_path / "generated" / "dev" / "repro" / "repro_fg.py").exists()


_WRITE_ONLY_SELECTION = """pipeline: repro
flowgroup: repro_fg
actions:
  - name: load_raw
    type: load
    source:
      type: sql
      sql: "SELECT 1 AS id"
    target: v_raw
  - name: write_raw
    type: write
    source: v_raw
    operational_metadata: [_source]
    write_target:
      type: streaming_table
      catalog: c
      schema: s
      table: t
"""

_TEST_ONLY_SELECTION = """pipeline: repro
flowgroup: repro_fg
operational_metadata: [_source]
actions:
  - name: check_fact_dim
    type: test
    test_type: referential_integrity
    source: c.s.fact
    reference: c.s.dim
    source_columns: [id]
    reference_columns: [id]
    on_violation: warn
"""


@pytest.mark.parametrize(
    ("flowgroup", "flags"),
    [(_WRITE_ONLY_SELECTION, ()), (_TEST_ONLY_SELECTION, ("--include-tests",))],
    ids=["streaming_table_write", "test_action"],
)
def test_selection_on_an_action_that_renders_no_metadata_is_not_checked(
    tmp_path, monkeypatch, flowgroup, flags
):
    """Streaming-table writes and test actions never render operational-metadata
    columns, so a column only they select reaches no generated code: an
    unresolved token in it fails neither command (as before #289)."""
    _project(tmp_path, "\"F.lit('${fd_missing}')\"")
    _write(tmp_path / "pipelines" / "repro" / "p.yaml", flowgroup)
    monkeypatch.chdir(tmp_path)

    validated = _validate("dev", *flags)
    assert validated.exit_code == 0, validated.stderr

    generated = _generate("dev", *flags)
    assert generated.exit_code == 0, generated.stderr
    assert "${fd_missing}" not in _generated(tmp_path)


def test_regex_quantifier_is_not_flagged_and_is_emitted_unchanged(
    tmp_path, monkeypatch
):
    _project(tmp_path, '\'F.col("a").rlike("\\\\d{8}")\'')
    monkeypatch.chdir(tmp_path)

    validated = _validate()
    assert validated.exit_code == 0, validated.stderr

    generated = _generate()
    assert generated.exit_code == 0, generated.stderr
    assert 'F.col("a").rlike("\\\\d{8}")' in _generated(tmp_path)
