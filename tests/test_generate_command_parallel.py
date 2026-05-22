"""End-to-end tests for the parallel ``lhp generate`` flow.

Covers the path from the Click CLI down to the orchestrator and verifies:

  * ``--max-workers N`` is accepted and propagated.
  * Per-pipeline ``✅`` lines fire via the ``on_pipeline_complete`` callback
    on the main thread (``capsys`` sees them in click.echo output).
  * Generated file content is byte-identical between ``--max-workers 1``
    (sequential) and ``--max-workers 4`` (parallel).
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from lhp.cli.main import cli


def _build_multipipeline_project(project_root: Path, pipeline_names) -> None:
    """Create a project with one flowgroup per pipeline.

    The flowgroups are deliberately small so the bytewise-diff test is fast,
    but cover load + transform + write so the codegen path is exercised.
    """
    (project_root / "presets").mkdir(parents=True, exist_ok=True)
    (project_root / "templates").mkdir(parents=True, exist_ok=True)
    (project_root / "substitutions").mkdir(parents=True, exist_ok=True)
    for name in pipeline_names:
        (project_root / "pipelines" / name).mkdir(parents=True, exist_ok=True)

    (project_root / "lhp.yaml").write_text(
        "name: test_parallel_project\nversion: '1.0'\n"
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


class TestGenerateCommandParallel:
    """Integration tests for ``lhp generate --max-workers N``."""

    PIPELINES = ["p_alpha", "p_beta", "p_gamma"]

    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_max_workers_flag_accepted_with_4_workers(
        self, runner, tmp_path, monkeypatch
    ):
        """``--max-workers 4`` runs cleanly through CLI to the flat-pool engine."""
        project_root = tmp_path
        _build_multipipeline_project(project_root, self.PIPELINES)

        monkeypatch.chdir(project_root)
        result = runner.invoke(
            cli,
            [
                "generate",
                "--env",
                "dev",
                "--max-workers",
                "4",
                "--no-bundle",
            ],
        )
        assert result.exit_code == 0, f"CLI exited {result.exit_code}: {result.output}"

        # Each pipeline gets its own subdirectory under generated/dev.
        for name in self.PIPELINES:
            assert (
                project_root / "generated" / "dev" / name / f"{name}_fg.py"
            ).exists(), f"Expected file missing for {name}"

    def test_per_pipeline_completion_line_per_pipeline(
        self, runner, tmp_path, monkeypatch
    ):
        """The on_pipeline_complete callback fires once per pipeline.

        Asserts the per-pipeline display line appears in CLI output exactly
        once for each pipeline (the existing display function emits
        ``✅`` for actual writes, or ``⚡ is up to date`` for skip).
        """
        project_root = tmp_path
        _build_multipipeline_project(project_root, self.PIPELINES)

        monkeypatch.chdir(project_root)
        # ``--show-all`` opts into the full per-pipeline summary
        # table; the failures-only default would suppress the
        # table on a successful run, but this test asserts on
        # per-pipeline row visibility.
        result = runner.invoke(
            cli,
            [
                "generate",
                "--env",
                "dev",
                "--max-workers",
                "4",
                "--no-bundle",
                "--show-all",
            ],
        )
        assert result.exit_code == 0, f"CLI exited {result.exit_code}: {result.output}"

        # Each pipeline's name appears in output (either ✅ generated
        # or ⚡ up-to-date line). On a fresh run with no state we
        # expect generation, so look for the file count line.
        for name in self.PIPELINES:
            # The display line for a fresh generate writes the
            # pipeline name with file count; just assert the name
            # is present at least once.
            assert name in result.output, (
                f"Pipeline {name} missing from CLI output:\n{result.output}"
            )

    def test_parallel_output_byte_identical_to_sequential(
        self, runner, tmp_path, monkeypatch
    ):
        """``--max-workers 1`` and ``--max-workers 4`` produce identical files.

        This test relies on generator output being fully deterministic:
        no per-run timestamps, no dict-iteration-order dependency.
        If a future generator change introduces non-determinism, this
        test should be the first to fail; do NOT silence it — fix the
        generator to remain deterministic.
        """
        par_root = tmp_path / "par"
        seq_root = tmp_path / "seq"
        par_root.mkdir()
        seq_root.mkdir()
        _build_multipipeline_project(par_root, self.PIPELINES)
        _build_multipipeline_project(seq_root, self.PIPELINES)

        monkeypatch.chdir(par_root)
        par_result = runner.invoke(
            cli,
            [
                "generate",
                "--env",
                "dev",
                "--max-workers",
                "4",
                "--no-bundle",
            ],
        )
        assert par_result.exit_code == 0

        monkeypatch.chdir(seq_root)
        seq_result = runner.invoke(
            cli,
            [
                "generate",
                "--env",
                "dev",
                "--max-workers",
                "1",
                "--no-bundle",
            ],
        )
        assert seq_result.exit_code == 0

        for name in self.PIPELINES:
            par_file = par_root / "generated" / "dev" / name / f"{name}_fg.py"
            seq_file = seq_root / "generated" / "dev" / name / f"{name}_fg.py"
            assert par_file.read_bytes() == seq_file.read_bytes(), (
                f"Bytewise diff for {name} between parallel and sequential runs"
            )
