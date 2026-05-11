"""Spec-driven unit tests for blueprint CLI surface (Phase 12).

Covers ``lhp list_blueprints`` (with/without --verbose), ``lhp show --instance``
(M4), and the mutual-exclusion error codes 057 / 058 raised by the show
command's argument parsing.
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


def _bootstrap_blueprint_project(root: Path) -> None:
    _write(root / "lhp.yaml", 'name: bp_cli_test\nversion: "1.0"\n')
    _write(root / "substitutions" / "dev.yaml", "catalog: c\n")
    for d in ("presets", "templates", "pipelines"):
        (root / d).mkdir(exist_ok=True)
    _write(
        root / "blueprints" / "erp.yaml",
        """
name: erp_ingestion
description: "ERP standard"
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
        "blueprint: erp_ingestion\nsite_name: apac_sg\n",
    )
    _write(
        root / "pipelines" / "erp" / "bronze" / "uk.yaml",
        "blueprint: erp_ingestion\nsite_name: emea_uk\n",
    )


def test_list_blueprints_shows_blueprint_summary(tmp_path):
    _bootstrap_blueprint_project(tmp_path)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as fs:
        # Materialize project files in the isolated fs root.
        _bootstrap_blueprint_project(Path(fs))
        result = runner.invoke(cli, ["list-blueprints"])
    assert result.exit_code == 0, result.output
    assert "erp_ingestion" in result.output


def test_list_blueprints_verbose_shows_instances(tmp_path):
    _bootstrap_blueprint_project(tmp_path)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as fs:
        _bootstrap_blueprint_project(Path(fs))
        result = runner.invoke(cli, ["list-blueprints", "--verbose"])
    assert result.exit_code == 0, result.output
    out = result.output
    # Verbose mode lists each instance + its resolved pipelines.
    assert "apac_sg" in out or "sg.yaml" in out
    assert "emea_uk" in out or "uk.yaml" in out


def test_show_instance_resolves_flowgroups(tmp_path):
    _bootstrap_blueprint_project(tmp_path)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as fs:
        _bootstrap_blueprint_project(Path(fs))
        result = runner.invoke(
            cli,
            ["show", "--env", "dev", "--instance", "pipelines/erp/bronze/sg.yaml"],
        )
    assert result.exit_code == 0, result.output
    # The resolved pipeline / flowgroup names should appear in the output.
    assert "apac_sg_raw" in result.output or "apac_sg_orders" in result.output


def test_show_with_both_flowgroup_and_instance_raises_057(tmp_path):
    _bootstrap_blueprint_project(tmp_path)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as fs:
        _bootstrap_blueprint_project(Path(fs))
        result = runner.invoke(
            cli,
            ["show", "some_fg", "--instance", "pipelines/erp/bronze/sg.yaml"],
        )
    assert result.exit_code != 0
    assert "LHP-CFG-057" in result.output


def test_show_with_neither_flowgroup_nor_instance_raises_058(tmp_path):
    _bootstrap_blueprint_project(tmp_path)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as fs:
        _bootstrap_blueprint_project(Path(fs))
        result = runner.invoke(cli, ["show"])
    assert result.exit_code != 0
    assert "LHP-CFG-058" in result.output
