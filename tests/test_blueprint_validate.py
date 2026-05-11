"""Spec-driven unit tests for `lhp validate` blueprint integration (Phase 13/A).

Covers:
  (a) silent pass when no blueprints/instances present
  (b) malformed blueprint → code 050
  (c) instance referencing nonexistent blueprint → code 041
  (d) duplicate (pipeline, flowgroup) tuple after expansion → code 045
  (e) ${env_token} in blueprint pipeline → code 044
"""

from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli

pytestmark = pytest.mark.unit


def _write(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


def _bootstrap(root: Path) -> None:
    _write(root / "lhp.yaml", 'name: bp_validate_test\nversion: "1.0"\n')
    _write(root / "substitutions" / "dev.yaml", "catalog: c\n")
    for d in ("presets", "templates", "pipelines"):
        (root / d).mkdir(exist_ok=True)


def test_validate_silent_pass_when_no_blueprints(tmp_path):
    """No blueprints/instances → validate must not fail with a blueprint error."""
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as fs:
        _bootstrap(Path(fs))
        result = runner.invoke(cli, ["validate", "--env", "dev"])
    # Validate may succeed (no flowgroups → vacuously valid) or report no
    # pipelines, but it must not raise a blueprint-range error code.
    for blueprint_code in (
        "LHP-CFG-040",
        "LHP-VAL-041",
        "LHP-VAL-042",
        "LHP-VAL-043",
        "LHP-VAL-044",
        "LHP-VAL-045",
        "LHP-VAL-046",
    ):
        assert blueprint_code not in result.output


def test_validate_malformed_blueprint_fails(tmp_path):
    """A blueprint with broken Pydantic shape must fail validate with one of
    the documented blueprint error codes (e.g. 050, 049)."""
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as fs:
        root = Path(fs)
        _bootstrap(root)
        # `flowgroups` declared as an int — Pydantic must reject.
        _write(
            root / "blueprints" / "broken.yaml",
            """
name: broken
parameters: []
flowgroups: 42
""",
        )
        result = runner.invoke(cli, ["validate", "--env", "dev"])
    assert result.exit_code != 0
    # The blueprint error must surface; spec table documents 050 for Pydantic
    # failure, 049 for non-blueprint shape. Either is acceptable.
    assert ("LHP-CFG-050" in result.output) or ("LHP-CFG-049" in result.output)


def test_validate_instance_unknown_blueprint_raises_041(tmp_path):
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as fs:
        root = Path(fs)
        _bootstrap(root)
        # No blueprint files; instance references a nonexistent blueprint.
        _write(
            root / "pipelines" / "erp" / "bronze" / "sg.yaml",
            "blueprint: nonexistent_blueprint\nsite_name: apac_sg\n",
        )
        result = runner.invoke(cli, ["validate", "--env", "dev"])
    assert result.exit_code != 0
    assert "LHP-VAL-041" in result.output


def test_validate_duplicate_tuple_raises_045(tmp_path):
    """Two instances yielding the same (pipeline, flowgroup) → 045 with both
    paths in the message."""
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as fs:
        root = Path(fs)
        _bootstrap(root)
        _write(
            root / "blueprints" / "erp.yaml",
            """
name: erp
parameters:
  - name: site_name
    required: true
flowgroups:
  - pipeline: "%{site_name}_raw"
    flowgroup: "%{site_name}_orders"
    actions: []
""",
        )
        _write(
            root / "pipelines" / "erp" / "bronze" / "sg_a.yaml",
            "blueprint: erp\nsite_name: apac_sg\n",
        )
        _write(
            root / "pipelines" / "erp" / "bronze" / "sg_b.yaml",
            "blueprint: erp\nsite_name: apac_sg\n",
        )
        result = runner.invoke(cli, ["validate", "--env", "dev"])
    assert result.exit_code != 0
    assert "LHP-VAL-045" in result.output
    assert "sg_a.yaml" in result.output
    assert "sg_b.yaml" in result.output


def test_validate_env_token_in_pipeline_raises_044(tmp_path):
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as fs:
        root = Path(fs)
        _bootstrap(root)
        _write(
            root / "blueprints" / "erp.yaml",
            """
name: erp
parameters:
  - name: site_name
    required: true
flowgroups:
  - pipeline: "${env}_%{site_name}_raw"
    flowgroup: "%{site_name}_orders"
    actions: []
""",
        )
        _write(
            root / "pipelines" / "erp" / "bronze" / "sg.yaml",
            "blueprint: erp\nsite_name: apac_sg\n",
        )
        result = runner.invoke(cli, ["validate", "--env", "dev"])
    assert result.exit_code != 0
    assert "LHP-VAL-044" in result.output


def test_validate_with_valid_blueprint_passes(tmp_path):
    """A well-formed blueprint + instance project must complete validate without
    raising any blueprint error code. Exercises the success path of
    _validate_blueprints_and_instances + downstream pipeline discovery."""
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as fs:
        root = Path(fs)
        _bootstrap(root)
        _write(
            root / "blueprints" / "erp.yaml",
            """
name: erp
parameters:
  - name: site_name
    required: true
flowgroups:
  - pipeline: "%{site_name}_raw"
    flowgroup: "%{site_name}_orders"
    actions:
      - name: load_raw
        type: load
        source:
          type: sql
          sql: "SELECT 1"
        target: v_raw
      - name: write_raw
        type: write
        source: v_raw
        write_target:
          type: streaming_table
          database: dev_cat.bronze
          table: orders
""",
        )
        _write(
            root / "pipelines" / "erp" / "bronze" / "sg.yaml",
            "blueprint: erp\nsite_name: apac_sg\n",
        )
        result = runner.invoke(cli, ["validate", "--env", "dev"])
    # Must not surface a 040–059 blueprint error.
    for code in (
        "LHP-CFG-040",
        "LHP-VAL-041",
        "LHP-VAL-042",
        "LHP-VAL-043",
        "LHP-VAL-044",
        "LHP-VAL-045",
        "LHP-VAL-046",
    ):
        assert code not in result.output


def test_validate_with_specific_pipeline_filter(tmp_path):
    """Exercises the --pipeline filter path through _determine_pipelines_to_validate."""
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as fs:
        root = Path(fs)
        _bootstrap(root)
        _write(
            root / "pipelines" / "p.yaml",
            """
pipeline: bronze_pipe
flowgroup: bronze_fg
actions:
  - name: load_x
    type: load
    source:
      type: sql
      sql: "SELECT 1"
    target: v_x
  - name: write_x
    type: write
    source: v_x
    write_target:
      type: streaming_table
      database: dev_cat.bronze
      table: x
""",
        )
        result = runner.invoke(
            cli, ["validate", "--env", "dev", "--pipeline", "bronze_pipe"]
        )
    # A nonexistent pipeline filter would raise; this name exists.
    assert "bronze_pipe" in result.output or result.exit_code == 0


def test_validate_verbose_flag_emits_extra_output(tmp_path):
    """Exercises the verbose-flag branch."""
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as fs:
        _bootstrap(Path(fs))
        result = runner.invoke(cli, ["validate", "--env", "dev", "--verbose"])
    # Verbose mode should at least include the "Validating" header.
    assert "Validating" in result.output


def test_validate_no_flowgroups_raises_014(tmp_path):
    """Empty project (no pipelines, no blueprints) → code 014 'No flowgroups
    found' on the pipeline-discovery path."""
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as fs:
        _bootstrap(Path(fs))
        result = runner.invoke(cli, ["validate", "--env", "dev"])
    assert result.exit_code != 0
    # Code 014 is the existing "no flowgroups" config error.
    assert "LHP-CFG-014" in result.output


def test_validate_pipeline_filter_not_found_raises_015(tmp_path):
    """An unknown --pipeline filter must surface code 015."""
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as fs:
        root = Path(fs)
        _bootstrap(root)
        _write(
            root / "pipelines" / "p.yaml",
            """
pipeline: real_pipeline
flowgroup: fg
actions:
  - name: load
    type: load
    source:
      type: sql
      sql: "SELECT 1"
    target: v_x
  - name: write
    type: write
    source: v_x
    write_target:
      type: streaming_table
      database: dev_cat.bronze
      table: x
""",
        )
        result = runner.invoke(
            cli, ["validate", "--env", "dev", "--pipeline", "missing_pipeline"]
        )
    assert result.exit_code != 0
    assert "LHP-CFG-015" in result.output


def test_validate_include_tests_runs_test_reporting(tmp_path):
    """--include-tests must exercise _validate_test_reporting."""
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as fs:
        root = Path(fs)
        _bootstrap(root)
        _write(
            root / "pipelines" / "p.yaml",
            """
pipeline: bronze_pipe
flowgroup: bronze_fg
actions:
  - name: load_x
    type: load
    source:
      type: sql
      sql: "SELECT 1"
    target: v_x
  - name: write_x
    type: write
    source: v_x
    write_target:
      type: streaming_table
      database: dev_cat.bronze
      table: x
""",
        )
        result = runner.invoke(cli, ["validate", "--env", "dev", "--include-tests"])
    # Must complete (success or fail) without raising NotImplementedError; the
    # test-reporting branch should at least be reachable.
    assert "Validating" in result.output
