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
    """Project with one flowgroup per pipeline (mirrors ``test_generate_command_parallel`` fixture shape)."""
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
    PIPELINES = ["pv_alpha", "pv_beta", "pv_gamma"]

    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_max_workers_flag_accepted_with_4_workers(self, runner):
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
                assert (
                    result.exit_code == 0
                ), f"CLI exited {result.exit_code}: {result.output}"
                for name in self.PIPELINES:
                    assert name in result.output, (
                        f"Pipeline {name} missing from validate output:\n"
                        f"{result.output}"
                    )
            finally:
                os.chdir(cwd)

    def test_parallel_validate_matches_sequential(self, runner):
        """Pipeline-level errors are identical for ``--max-workers 1`` and ``--max-workers 4``."""
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

                for name in self.PIPELINES:
                    assert name in par_result.output
                    assert name in seq_result.output
            finally:
                os.chdir(cwd)


class _FakeValidateFlowGroup:
    """Picklable FlowGroup stand-in for the validate-pool unit test."""

    def __init__(self, pipeline: str, flowgroup: str):
        self.pipeline = pipeline
        self.flowgroup = flowgroup


def _ctx_validate(fg: "_FakeValidateFlowGroup"):
    from lhp.models.config import FlowGroupContext

    return FlowGroupContext(flowgroup=fg, source_yaml=None)


@pytest.mark.unit
def test_run_validate_pool_guards_executor_submit_raises(monkeypatch):
    """If ``executor.submit`` raises mid-loop, every pipeline's ``on_pipeline_complete`` still fires exactly once and the submit exception is surfaced as a flowgroup-level error.

    Monkeypatches ``ProcessPoolExecutor`` with a fake that fails its second
    ``submit`` and short-circuits successful submits with a pre-completed
    Future, so the worker function is never invoked and no subprocess spawn
    is required.
    """
    from concurrent.futures import Future

    from lhp.core.coordination import executor as pe
    from lhp.core.coordination.executor import (
        FlowgroupValidationResult,
        PipelineValidationOutcome,
        _ValidateWorkerState,
        run_validate_pool,
    )

    flowgroups_by_pipeline = {
        "p_alpha": [
            _ctx_validate(_FakeValidateFlowGroup("p_alpha", "alpha_fg1")),
            _ctx_validate(_FakeValidateFlowGroup("p_alpha", "alpha_fg2")),
        ],
        "p_beta": [
            _ctx_validate(_FakeValidateFlowGroup("p_beta", "beta_fg1")),
            _ctx_validate(_FakeValidateFlowGroup("p_beta", "beta_fg2")),
        ],
    }

    submit_call_count = {"n": 0}

    class _FakeExecutor:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def submit(self, fn, fg_ctx):
            submit_call_count["n"] += 1
            if submit_call_count["n"] == 2:
                raise RuntimeError("simulated BrokenProcessPool on submit")
            fut: Future = Future()
            fut.set_result(
                FlowgroupValidationResult(
                    pipeline=fg_ctx.flowgroup.pipeline,
                    flowgroup_name=fg_ctx.flowgroup.flowgroup,
                    errors=(),
                )
            )
            return fut

    monkeypatch.setattr(pe, "ProcessPoolExecutor", _FakeExecutor)

    def _assemble(pipeline: str, results):
        errs = tuple(e for r in results for e in r.errors)
        return PipelineValidationOutcome(
            pipeline=pipeline,
            errors=errs,
            warnings=(),
            success=not errs,
        )

    completions: list[PipelineValidationOutcome] = []

    worker_state = _ValidateWorkerState(
        processor=None,  # never invoked: _FakeExecutor short-circuits results.
        substitution_managers={},
        include_tests=False,
    )

    outcomes = run_validate_pool(
        flowgroups_by_pipeline=flowgroups_by_pipeline,
        worker_state=worker_state,
        assemble_pipeline=_assemble,
        max_workers=2,
        on_pipeline_complete=completions.append,
    )

    completed_names = [o.pipeline for o in completions]
    assert sorted(completed_names) == ["p_alpha", "p_beta"]
    assert len(completions) == 2

    assert [o.pipeline for o in outcomes] == ["p_alpha", "p_beta"]

    by_pipeline = {o.pipeline: o for o in outcomes}

    assert by_pipeline["p_alpha"].success is False
    assert any(
        "alpha_fg2" in e and "simulated BrokenProcessPool" in e
        for e in by_pipeline["p_alpha"].errors
    ), by_pipeline["p_alpha"].errors

    assert by_pipeline["p_beta"].success is True
    assert by_pipeline["p_beta"].errors == ()
