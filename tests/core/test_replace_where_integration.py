"""``mode: replace_where`` through ``lhp validate`` and ``lhp generate`` (#346).

Each test deep-copies ``tests/e2e/fixtures/testing_project`` into a temp dir and
injects one flowgroup at runtime (a batch delta load feeding a replace_where
write), so the tracked fixture is read but never modified.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.commands.generate_command import generate
from lhp.cli.commands.validate_command import validate_command

pytestmark = pytest.mark.integration

_FIXTURE = Path(__file__).resolve().parents[1] / "e2e" / "fixtures" / "testing_project"
_PIPELINE = "replace_where_it"
_FLOWGROUP = "orders_recent_rw"

_FLOWGROUP_YAML = """\
pipeline: replace_where_it
flowgroup: orders_recent_rw

actions:
  - name: load_orders_batch
    type: load
    readMode: batch
    source:
      type: delta
      database: "${{catalog}}.${{bronze_schema}}"
      table: customer
    target: v_orders_source
    description: "Batch read of the source rows"

  - name: write_orders_recent
    type: write
    source: v_orders_source
    write_target:
      type: streaming_table
      mode: replace_where
      database: "${{catalog}}.${{silver_schema}}"
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


def _inject(project: Path, predicate: str) -> None:
    path = project / "pipelines" / "98_replace_where_it" / f"{_FLOWGROUP}.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(_FLOWGROUP_YAML.format(predicate=predicate), encoding="utf-8")


def test_replace_where_validates_and_generates_the_exact_shape(project: Path):
    _inject(project, _PREDICATE)

    validated = CliRunner().invoke(validate_command, ["--env", "dev", "--no-bundle"])
    assert validated.exit_code == 0, validated.stderr

    generated = CliRunner().invoke(
        generate, ["-e", "dev", "--no-bundle", "--no-progress", "-p", _PIPELINE]
    )
    assert generated.exit_code == 0, generated.stderr

    code = (project / "generated" / "dev" / _PIPELINE / f"{_FLOWGROUP}.py").read_text(
        encoding="utf-8"
    )
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
