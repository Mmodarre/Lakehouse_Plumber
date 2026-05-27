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
    """Project with one small flowgroup per pipeline covering load + transform + write."""
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
    PIPELINES = ["p_alpha", "p_beta", "p_gamma"]

    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_max_workers_flag_accepted_with_4_workers(
        self, runner, tmp_path, monkeypatch
    ):
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

        for name in self.PIPELINES:
            assert (
                project_root / "generated" / "dev" / name / f"{name}_fg.py"
            ).exists(), f"Expected file missing for {name}"

    def test_per_pipeline_completion_line_per_pipeline(
        self, runner, tmp_path, monkeypatch
    ):
        project_root = tmp_path
        _build_multipipeline_project(project_root, self.PIPELINES)

        monkeypatch.chdir(project_root)
        # ``--show-all`` opts into the full per-pipeline summary table;
        # the failures-only default would suppress the table on a
        # successful run, but this test asserts on per-pipeline row
        # visibility.
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

        for name in self.PIPELINES:
            assert name in result.output, (
                f"Pipeline {name} missing from CLI output:\n{result.output}"
            )

    def test_parallel_output_byte_identical_to_sequential(
        self, runner, tmp_path, monkeypatch
    ):
        """``--max-workers 1`` and ``--max-workers 4`` produce identical files.

        Relies on generator output being fully deterministic. If a future
        generator change introduces non-determinism, fix the generator —
        do not silence this test.
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


# Module-level stub so the spawn-pool can pickle it (local functions can't).
def _stub_pipeline_worker(pipeline_name, contexts):
    from lhp.models.processing import PipelineDelta

    return PipelineDelta.success_(pipeline_name)


def test_run_generate_pool_fires_on_pipeline_start_in_submission_order():
    """on_pipeline_start fires on main thread in submission order before each submit."""
    import threading

    from lhp.core.coordination.executor import run_generate_pool

    main_thread_id = threading.get_ident()

    starts: list[str] = []
    start_threads: list[int] = []

    def on_start(name: str) -> None:
        starts.append(name)
        start_threads.append(threading.get_ident())

    # Mix of empty (main-thread fast path) and non-empty (submission path)
    # pipelines exercises both call sites. Non-empty context lists use
    # picklable sentinels (plain strings) since the spawn-pool pickles
    # ``ctxs`` when submitting each future.
    flowgroups_by_pipeline = {
        "pipe_empty": [],
        "pipe_a": ["ctx"],
        "pipe_b": ["ctx"],
        "pipe_c": ["ctx"],
    }

    successful, failed = run_generate_pool(
        flowgroups_by_pipeline=flowgroups_by_pipeline,
        max_workers=2,
        process_one=_stub_pipeline_worker,
        on_pipeline_start=on_start,
    )

    assert starts == ["pipe_empty", "pipe_a", "pipe_b", "pipe_c"]
    assert all(tid == main_thread_id for tid in start_threads)
    assert len(failed) == 0
    assert {d.pipeline_name for d in successful} == {
        "pipe_empty",
        "pipe_a",
        "pipe_b",
        "pipe_c",
    }
