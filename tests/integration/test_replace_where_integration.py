"""``mode: replace_where`` through ``lhp validate`` and ``lhp generate`` (#346).

Each test deep-copies ``tests/e2e/fixtures/testing_project`` into a temp dir and
injects one flowgroup at runtime (batch delta loads feeding replace_where
writes), and sometimes a preset, so the tracked fixture is read but never
modified.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner
from conftest import strip_ansi

from lhp.cli.commands.generate_command import generate
from lhp.cli.commands.validate_command import validate_command

pytestmark = pytest.mark.integration

_FIXTURE = Path(__file__).resolve().parents[1] / "e2e" / "fixtures" / "testing_project"
_PIPELINE = "replace_where_it"
_FLOWGROUP = "orders_recent_rw"

_FLOWGROUP_YAML = """\
pipeline: replace_where_it
flowgroup: orders_recent_rw
{presets}
actions:
  - name: load_orders_batch
    type: load
    readMode: batch
    source:
      type: delta
      catalog: "${{catalog}}"
      schema: "${{bronze_schema}}"
      table: customer
    target: v_orders_source
    description: "Batch read of the source rows"

  - name: write_orders_recent
    type: write
    source: v_orders_source
    write_target:
      type: streaming_table
      mode: replace_where
      catalog: "${{catalog}}"
      schema: "${{silver_schema}}"
      table: orders_recent
      comment: "Recent orders, recomputed for the last 7 days"
      replace_where_config:
        replace_where: "{predicate}"
    description: "Recompute the last 7 days of orders"
"""

# The ${env} token resolves to "dev" before validation and generation.
_PREDICATE = "order_date >= date_add(current_date(), -7) AND region <> '${env}'"

_EXPECTED_TARGET_BLOCK = '''\
@dp.table(
    name="acme_edw_dev.edw_silver.orders_recent",
    comment="Recent orders, recomputed for the last 7 days",
    table_properties={"pipelines.reset.allowed": "false"},
    replace_where="order_date >= date_add(current_date(), -7) AND region <> 'dev'",
)
def orders_recent():
    """Recompute the last 7 days of orders"""
    df = spark.read.table("v_orders_source")

    return df
'''


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "testing_project"
    shutil.copytree(_FIXTURE, root)
    monkeypatch.chdir(root)
    return root


def _inject(project: Path, predicate: str, *, preset: str = "") -> None:
    """Write the flowgroup, applying ``preset`` (a preset name) when given."""
    path = project / "pipelines" / "98_replace_where_it" / f"{_FLOWGROUP}.yaml"
    path.parent.mkdir(parents=True)
    presets = f"presets:\n  - {preset}\n" if preset else ""
    path.write_text(
        _FLOWGROUP_YAML.format(predicate=predicate, presets=presets),
        encoding="utf-8",
    )


def _generated_code(project: Path) -> str:
    return (project / "generated" / "dev" / _PIPELINE / f"{_FLOWGROUP}.py").read_text(
        encoding="utf-8"
    )


def test_replace_where_validates_and_generates_the_exact_shape(project: Path):
    _inject(project, _PREDICATE)

    validated = CliRunner().invoke(validate_command, ["--env", "dev", "--no-bundle"])
    assert validated.exit_code == 0, validated.stderr

    generated = CliRunner().invoke(
        generate, ["-e", "dev", "--no-bundle", "--no-progress", "-p", _PIPELINE]
    )
    assert generated.exit_code == 0, generated.stderr

    code = _generated_code(project)
    assert code.count("@dp.table(") == 1
    assert code[code.index("@dp.table(") :] == _EXPECTED_TARGET_BLOCK
    for absent in ("create_streaming_table", "append_flow", "replace_flow"):
        assert absent not in code
    # The batch load view is the only other dataset; nothing reads a stream.
    assert "readStream" not in code


def test_secret_in_predicate_is_rejected_after_substitution(project: Path):
    """Write-action config validation runs on the substituted flowgroup, so the
    ``__SECRET_`` placeholder that ``${secret:...}`` resolves to is what the
    replace_where rule sees."""
    _inject(project, "region = '${secret:dev_secrets/region_key}'")

    result = CliRunner().invoke(
        validate_command,
        ["--env", "dev", "--no-bundle", "--show-details", "-p", _PIPELINE],
    )

    assert result.exit_code == 1, result.stderr
    # The console may wrap long lines; compare on collapsed whitespace.
    stderr = " ".join(result.stderr.split())
    assert "LHP-VAL-007" in stderr
    assert "'replace_where' cannot reference a secret" in stderr


# A preset supplying the reset guard for every streaming_table write; ``{key}``
# is the property name as the preset spells it.
_RESET_PRESET_YAML = """\
name: rw_reset_allowed
version: "1.0"
defaults:
  write_actions:
    streaming_table:
      table_properties:
        {key}: "true"
"""


@pytest.mark.parametrize("key", ["pipelines.reset.allowed", "Pipelines.Reset.Allowed"])
def test_preset_reset_guard_wins_over_the_default(project: Path, key: str):
    """The flowgroup sets no ``table_properties``; the preset's
    ``pipelines.reset.allowed: "true"`` is kept as written, and a key in another
    case gets no duplicate ``"false"`` default injected next to it."""
    (project / "presets" / "rw_reset_allowed.yaml").write_text(
        _RESET_PRESET_YAML.format(key=key), encoding="utf-8"
    )
    _inject(project, _PREDICATE, preset="rw_reset_allowed")

    generated = CliRunner().invoke(
        generate, ["-e", "dev", "--no-bundle", "--no-progress", "-p", _PIPELINE]
    )
    assert generated.exit_code == 0, generated.stderr

    code = _generated_code(project)
    assert code.count(f'"{key}": "true"') == 1, code
    assert code.lower().count("pipelines.reset.allowed") == 1, code
    assert '"false"' not in code


# Two replace_where writes, each fed by its own batch load. ``{metadata}`` is the
# loads' ``operational_metadata`` selection; the fixture's lhp.yaml defines
# ``_processing_timestamp`` as ``F.current_timestamp()``. catalog/schema (not
# the deprecated ``database``) keep the pipeline free of other warnings, so a
# ``--strict`` failure can only come from LHP-VAL-073.
_METADATA_PIPELINE = "replace_where_metadata_it"
_METADATA_SCOPE = ("--env", "dev", "--no-bundle", "-p", _METADATA_PIPELINE)
_METADATA_FLOWGROUP_YAML = """\
pipeline: replace_where_metadata_it
flowgroup: orders_rw_metadata

actions:
{loads}
{writes}
"""
_METADATA_LOAD = """\
  - name: load_{suffix}
    type: load
    readMode: batch
    operational_metadata: {metadata}
    source:
      type: delta
      catalog: "${{catalog}}"
      schema: "${{bronze_schema}}"
      table: orders_{suffix}
    target: v_orders_{suffix}
"""
_METADATA_WRITE = """\
  - name: write_orders_{suffix}
    type: write
    source: v_orders_{suffix}
    write_target:
      type: streaming_table
      mode: replace_where
      catalog: "${{catalog}}"
      schema: "${{silver_schema}}"
      table: orders_recent_{suffix}
      replace_where_config:
        replace_where: "order_date >= date_add(current_date(), -7)"
"""


def _inject_two_writers(project: Path, metadata: str) -> None:
    suffixes = ("a", "b")
    yaml_text = _METADATA_FLOWGROUP_YAML.format(
        loads="".join(
            _METADATA_LOAD.format(suffix=s, metadata=metadata) for s in suffixes
        ),
        writes="".join(_METADATA_WRITE.format(suffix=s) for s in suffixes),
    )
    path = project / "pipelines" / "98_replace_where_metadata_it" / "fg.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(yaml_text, encoding="utf-8")


def _validate(*extra: str):
    return CliRunner().invoke(
        validate_command,
        [*_METADATA_SCOPE, *extra],
    )


def test_deterministic_metadata_upstream_of_replace_where_is_warning_free(
    project: Path,
):
    """Control for the test below: the same flowgroup with only a deterministic
    metadata column (``F.col('_metadata.file_path')``) passes ``--strict``, so
    the strict failure there is VAL-073's."""
    _inject_two_writers(project, metadata='["_source_file_path"]')

    strict = _validate("--strict")

    assert strict.exit_code == 0, strict.stderr


def test_timestamp_metadata_upstream_of_two_replace_where_writes_warns_once(
    project: Path,
):
    """Real producer, real pipeline: the spawn workers record LHP-VAL-073 and
    the merged warning the user sees names BOTH writes (one record per
    flowgroup, so the ``(code, file)`` dedup cannot drop the second write)."""
    _inject_two_writers(project, metadata='["_processing_timestamp"]')

    lenient = _validate("--show-details")
    strict = _validate("--strict")
    lenient_generate = CliRunner().invoke(generate, [*_METADATA_SCOPE, "--no-progress"])
    strict_generate = CliRunner().invoke(
        generate, [*_METADATA_SCOPE, "--no-progress", "--strict"]
    )

    assert lenient.exit_code == 0, lenient.stderr
    # The console may wrap long lines; compare on collapsed whitespace.
    shown = " ".join(strip_ansi(lenient.stderr).split())
    assert shown.count("LHP-VAL-073") == 1, shown
    assert "write 'write_orders_a' <- action 'load_a'" in shown
    assert "write 'write_orders_b' <- action 'load_b'" in shown
    assert strict.exit_code == 1, strict.stderr
    # generate surfaces the warning too, and fails only under --strict.
    assert lenient_generate.exit_code == 0, lenient_generate.stderr
    assert "LHP-VAL-073" in strip_ansi(lenient_generate.stderr)
    assert strict_generate.exit_code == 1, strict_generate.stderr
