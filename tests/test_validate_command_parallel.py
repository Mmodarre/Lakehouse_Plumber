"""End-to-end tests for the parallel ``lhp validate`` flow.

Covers the path from the Click CLI down through
:meth:`ValidateCommand._validate_all_pipelines` to
:meth:`ActionOrchestrator.validate_pipelines_by_fields`.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from lhp.cli.main import cli


def _build_multipipeline_project(project_root: Path, pipeline_names) -> None:
    """Create a project with one flowgroup per pipeline.

    Mirrors the helper in ``test_generate_command_parallel`` so both tests
    cover the same fixture shape.
    """
    (project_root / "presets").mkdir(parents=True, exist_ok=True)
    (project_root / "templates").mkdir(parents=True, exist_ok=True)
    (project_root / "substitutions").mkdir(parents=True, exist_ok=True)
    for name in pipeline_names:
        (project_root / "pipelines" / name).mkdir(parents=True, exist_ok=True)

    (project_root / "lhp.yaml").write_text(
        "name: test_parallel_validate_project\nversion: '1.0'\n"
    )

    subs = {
        "dev": {
            "catalog": "dev_catalog",
            "bronze_schema": "bronze",
            "landing_path": "/mnt/dev/landing",
        }
    }
    with open(project_root / "substitutions" / "dev.yaml", "w") as f:
        yaml.dump(subs, f)

    for name in pipeline_names:
        flowgroup = {
            "pipeline": name,
            "flowgroup": f"{name}_fg",
            "actions": [
                {
                    "name": f"load_{name}",
                    "type": "load",
                    "target": f"v_{name}_raw",
                    "source": {
                        "type": "cloudfiles",
                        "path": "${landing_path}/" + name,
                        "format": "json",
                    },
                },
                {
                    "name": f"clean_{name}",
                    "type": "transform",
                    "transform_type": "sql",
                    "source": f"v_{name}_raw",
                    "target": f"v_{name}_clean",
                    "sql": f"SELECT * FROM v_{name}_raw",
                },
                {
                    "name": f"write_{name}",
                    "type": "write",
                    "source": f"v_{name}_clean",
                    "write_target": {
                        "type": "streaming_table",
                        "catalog": "${catalog}",
                        "schema": "${bronze_schema}",
                        "table": name,
                        "create_table": True,
                    },
                },
            ],
        }
        with open(project_root / "pipelines" / name / f"{name}_fg.yaml", "w") as f:
            yaml.dump(flowgroup, f)


class TestValidateCommandParallel:
    """Integration tests for ``lhp validate --max-workers N``."""

    PIPELINES = ["pv_alpha", "pv_beta", "pv_gamma"]

    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_max_workers_flag_accepted_with_4_workers(self, runner):
        """``lhp validate --max-workers 4`` runs cleanly through the
        flat-pool engine and exits 0 on a clean project."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _build_multipipeline_project(project_root, self.PIPELINES)

            cwd = os.getcwd()
            try:
                os.chdir(project_root)
                # ``--show-all`` opts into the full per-pipeline summary
                # table; the failures-only default would suppress the
                # table on a clean run, but this test asserts on
                # per-pipeline row visibility.
                result = runner.invoke(
                    cli,
                    [
                        "validate",
                        "--env",
                        "dev",
                        "--max-workers",
                        "4",
                        "--show-all",
                    ],
                )
                assert result.exit_code == 0, (
                    f"CLI exited {result.exit_code}: {result.output}"
                )
                # Each pipeline name should appear in the per-pipeline
                # display section.
                for name in self.PIPELINES:
                    assert name in result.output, (
                        f"Pipeline {name} missing from validate output:\n"
                        f"{result.output}"
                    )
            finally:
                os.chdir(cwd)

    def test_parallel_validate_matches_sequential(self, runner):
        """The set of pipeline-level errors is the same for
        ``--max-workers 1`` and ``--max-workers 4``."""
        with (
            tempfile.TemporaryDirectory() as par_tmp,
            tempfile.TemporaryDirectory() as seq_tmp,
        ):
            par_root = Path(par_tmp)
            seq_root = Path(seq_tmp)
            _build_multipipeline_project(par_root, self.PIPELINES)
            _build_multipipeline_project(seq_root, self.PIPELINES)

            cwd = os.getcwd()
            try:
                os.chdir(par_root)
                # ``--show-all`` opts into the full per-pipeline summary
                # table on both runs; the failures-only default would
                # suppress the table on a clean run, but this test
                # asserts on per-pipeline row visibility across both
                # worker configurations.
                par_result = runner.invoke(
                    cli,
                    ["validate", "--env", "dev", "--max-workers", "4", "--show-all"],
                )
                assert par_result.exit_code == 0

                os.chdir(seq_root)
                seq_result = runner.invoke(
                    cli,
                    ["validate", "--env", "dev", "--max-workers", "1", "--show-all"],
                )
                assert seq_result.exit_code == 0

                # Both runs should report the same per-pipeline names.
                for name in self.PIPELINES:
                    assert name in par_result.output
                    assert name in seq_result.output
            finally:
                os.chdir(cwd)
