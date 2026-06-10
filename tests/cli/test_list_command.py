"""Acceptance tests for the ``lhp list`` command group (C2).

Invokes the ``list_group`` Click object directly (not through ``main.py``,
which is import-red until the CLI assembly task). The autouse
``_isolate_lhp_console`` fixture in ``tests/conftest.py`` swaps the module-level
Rich consoles for deterministic plain-text sinks, so the presenter's stdout
output lands in ``result.stdout`` verbatim (tab-separated, no ANSI).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.commands.list_command import list_group

pytestmark = pytest.mark.unit

_BLUEPRINT_YAML = """
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
"""


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def _bare_project(root: Path) -> None:
    """Minimal on-disk project: just ``lhp.yaml`` so ``resolve_project_root``
    succeeds. No ``presets/`` / ``templates/`` / ``blueprints/`` directories.
    """
    _write(root / "lhp.yaml", 'name: list_cmd_test\nversion: "1.0"\n')


def _blueprint_project(root: Path) -> None:
    """A project with one blueprint and two instance files that resolve to
    distinct pipeline names (``apac_sg_raw`` / ``emea_uk_raw``).
    """
    _bare_project(root)
    _write(root / "substitutions" / "dev.yaml", "catalog: c\n")
    for sub in ("presets", "templates", "pipelines"):
        (root / sub).mkdir(exist_ok=True)
    _write(root / "blueprints" / "erp.yaml", _BLUEPRINT_YAML)
    _write(
        root / "pipelines" / "erp" / "bronze" / "sg.yaml",
        "blueprint: erp_ingestion\nsite_name: apac_sg\n",
    )
    _write(
        root / "pipelines" / "erp" / "bronze" / "uk.yaml",
        "blueprint: erp_ingestion\nsite_name: emea_uk\n",
    )


def test_presets_missing_dir_exits_zero_with_empty_notice():
    """No ``presets/`` directory -> exit 0 and a 'No presets found' notice."""
    runner = CliRunner()
    with runner.isolated_filesystem() as fs:
        _bare_project(Path(fs))
        result = runner.invoke(list_group, ["presets"])

    assert result.exit_code == 0, result.output
    assert "No presets found" in result.stdout


def test_templates_missing_dir_exits_zero_with_empty_notice():
    """No ``templates/`` directory -> exit 0 and a 'No templates found' notice."""
    runner = CliRunner()
    with runner.isolated_filesystem() as fs:
        _bare_project(Path(fs))
        result = runner.invoke(list_group, ["templates"])

    assert result.exit_code == 0, result.output
    assert "No templates found" in result.stdout


def test_presets_with_file_renders_a_row():
    """A preset file under ``presets/`` -> a table row naming the preset."""
    runner = CliRunner()
    with runner.isolated_filesystem() as fs:
        root = Path(fs)
        _bare_project(root)
        _write(
            root / "presets" / "bronze_layer.yaml",
            'name: bronze_layer\nversion: "1.0"\ndescription: Bronze defaults\n',
        )
        result = runner.invoke(list_group, ["presets"])

    assert result.exit_code == 0, result.output
    out = result.stdout
    assert "Available presets" in out
    assert "bronze_layer" in out
    assert "bronze_layer.yaml" in out
    # Description block is rendered (the old presenter carried it).
    assert "Bronze defaults" in out


def test_blueprints_instances_shows_per_instance_pipelines():
    """``list blueprints --instances`` -> the blueprint summary row plus a
    per-instance block naming the pipelines each instance file produces.
    """
    runner = CliRunner()
    with runner.isolated_filesystem() as fs:
        _blueprint_project(Path(fs))
        result = runner.invoke(list_group, ["blueprints", "--instances"])

    assert result.exit_code == 0, result.output
    out = result.stdout
    assert "erp_ingestion" in out
    # Blueprint description is now rendered (dropped by the old command).
    assert "ERP standard" in out
    # Per-instance pipelines surfaced under --instances.
    assert "sg.yaml" in out
    assert "uk.yaml" in out
    assert "apac_sg_raw" in out
    assert "emea_uk_raw" in out


def test_blueprints_without_instances_omits_per_instance_block():
    """Without ``--instances`` the per-instance pipelines block is not shown."""
    runner = CliRunner()
    with runner.isolated_filesystem() as fs:
        _blueprint_project(Path(fs))
        result = runner.invoke(list_group, ["blueprints"])

    assert result.exit_code == 0, result.output
    out = result.stdout
    assert "erp_ingestion" in out
    assert "apac_sg_raw" not in out
    assert "emea_uk_raw" not in out
