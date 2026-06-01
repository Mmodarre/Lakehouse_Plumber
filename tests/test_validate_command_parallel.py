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
    """FlowGroup stand-in for the engine submit-guard unit test."""

    def __init__(self, pipeline: str, flowgroup: str):
        self.pipeline = pipeline
        self.flowgroup = flowgroup


def _ctx_validate(fg: "_FakeValidateFlowGroup"):
    from lhp.models import FlowGroupContext

    return FlowGroupContext(flowgroup=fg, source_yaml=None)


@pytest.mark.unit
def test_flat_engine_guards_executor_submit_raises(monkeypatch):
    """A raising ``executor.submit`` becomes a per-flowgroup failure DTO, never an escape.

    Migrated from the deleted pipeline-batched validate-pool runner's
    submit-guard test. The consolidated engine
    :func:`._pool._run_flowgroup_pool_core` carries the SAME
    submit-guard: when ``executor.submit`` raises (a pickle / post-shutdown /
    ``BrokenProcessPool`` failure), the engine synthesizes a
    :class:`FlowgroupOutcome.failure` carrying the submit exception on the
    string channel, buckets it, and finalizes that pipeline — the exception
    never crosses back to the caller and the other pipeline still completes.

    The fan-out is now per-flowgroup (not pipeline-batched), so this drives
    the engine directly with a fake executor that fails its SECOND ``submit``
    and short-circuits successful submits with a pre-completed Future (the
    real worker is never invoked, no subprocess spawn). A barrier spy returns
    an empty result so the focus stays on the submit-guard, not issue-building.
    """
    from concurrent.futures import Future
    from typing import Optional, Sequence

    from lhp.core._interfaces import CrossFlowgroupCheckResult
    from lhp.core.coordination import _pool as fe
    from lhp.core.coordination._flowgroup_pool import _FlowgroupWorkerState
    from lhp.core.coordination._pool import _run_flowgroup_pool_core
    from lhp.models.processing import FlowgroupOutcome

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
        """Fails its 2nd ``submit``; pre-completes the rest with an ok outcome."""

        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def submit(self, fn, /, *args, **kwargs):
            submit_call_count["n"] += 1
            if submit_call_count["n"] == 2:
                raise RuntimeError("simulated BrokenProcessPool on submit")
            # args[0] is the FlowGroupContext the engine submits; mode is kwarg.
            fg_ctx = args[0]
            fut: Future = Future()
            fut.set_result(
                FlowgroupOutcome.ok(
                    fg_ctx.flowgroup.pipeline,
                    fg_ctx.flowgroup.flowgroup,
                    resolved_flowgroup=fg_ctx.flowgroup,
                )
            )
            return fut

    # The faked worker fn is never called (the executor short-circuits every
    # successful submit with a pre-completed Future); patch it to a sentinel
    # that would fail loudly if it ever ran, to keep the guard the only path.
    def _never_called_worker(*a, **k):  # pragma: no cover - must never run
        raise AssertionError("worker fn must not run; executor short-circuits")

    monkeypatch.setattr(fe, "ProcessPoolExecutor", _FakeExecutor)
    monkeypatch.setattr(fe, "_process_one_flowgroup", _never_called_worker)

    class _BarrierSpy:
        def validate_cross_flowgroup(
            self,
            flowgroups: Sequence[object],
            *,
            pipeline_filter: Optional[str] = None,
        ) -> CrossFlowgroupCheckResult:
            return CrossFlowgroupCheckResult()

    worker_state = _FlowgroupWorkerState(
        processor=object(),  # never invoked: executor short-circuits results.
        substitution_managers={},
        include_tests=False,
        code_generator=object(),
        formatter=object(),
        pipeline_output_dirs={},
        environment="dev",
    )

    results = _run_flowgroup_pool_core(
        flowgroups_by_pipeline=flowgroups_by_pipeline,
        worker_state=worker_state,
        validation_service=_BarrierSpy(),
        max_workers=2,
        mode="validate",
    )

    # One result per pipeline, in input order — the submit failure
    # on one flowgroup does not drop its pipeline from the output.
    assert [r.pipeline for r in results] == ["p_alpha", "p_beta"]
    by_pipeline = {r.pipeline: r for r in results}

    # p_alpha's 2nd flowgroup (alpha_fg2) saw the submit raise -> failure DTO
    # carrying the submit exception on the string channel; the other ok.
    alpha_outcomes = by_pipeline["p_alpha"].outcomes_in_order
    assert {o.flowgroup_name for o in alpha_outcomes} == {"alpha_fg1", "alpha_fg2"}
    alpha_fg2 = next(o for o in alpha_outcomes if o.flowgroup_name == "alpha_fg2")
    assert alpha_fg2.success is False
    assert any(
        "alpha_fg2" in e and "simulated BrokenProcessPool" in e
        for e in alpha_fg2.errors
    ), alpha_fg2.errors
    alpha_fg1 = next(o for o in alpha_outcomes if o.flowgroup_name == "alpha_fg1")
    assert alpha_fg1.success is True

    # p_beta is untouched by the alpha submit failure: both flowgroups ok.
    beta_outcomes = by_pipeline["p_beta"].outcomes_in_order
    assert {o.flowgroup_name for o in beta_outcomes} == {"beta_fg1", "beta_fg2"}
    assert all(o.success for o in beta_outcomes)


# N3 — §9.24 closure: cross-flowgroup duplicate-table that is INVISIBLE in the
# raw flowgroups but PRESENT after template resolution.
def _build_template_duplicate_table_project(project_root: Path) -> None:
    """One pipeline; two flowgroups that BOTH instantiate the same template.

    The template ``dup_table`` expands into a ``write`` action that creates
    ``${catalog}.${schema}.<table_name>`` (``create_table`` omitted, so it
    defaults to ``true``). Both flowgroups pass the SAME ``table_name`` to the
    template, so AFTER resolution two ``create_table: true`` actions target the
    one table — the cross-flowgroup ``LHP-CFG-004`` ("Multiple table creators")
    conflict.

    The conflict is INVISIBLE in the raw flowgroups: a ``use_template``
    flowgroup carries an EMPTY ``actions`` list until the template is expanded
    during resolution (proven in the test below), so a cross-flowgroup barrier
    run on the RAW flowgroups — the pre-§9.24 behaviour — sees zero table
    creators and reports nothing. It only materialises on the RESOLVED set,
    which is exactly where the consolidated engine now runs the barrier
    (§9.24). The two flowgroups carry DIFFERENT ``view_suffix`` values
    so the load/view names are distinct — the ONLY conflict is the shared table,
    keeping the failure unambiguous.
    """
    for d in ("presets", "templates", "substitutions", "pipelines/dup_pipeline"):
        (project_root / d).mkdir(parents=True, exist_ok=True)

    (project_root / "lhp.yaml").write_text(
        "name: test_n3_template_duplicate_project\nversion: '1.0'\n"
    )
    with open(project_root / "substitutions" / "dev.yaml", "w") as f:
        yaml.dump({"dev": {"catalog": "dev_catalog", "schema": "shared_schema"}}, f)

    template = {
        "name": "dup_table",
        "version": "1.0",
        "parameters": [
            {"name": "table_name", "type": "string", "required": True},
            {"name": "view_suffix", "type": "string", "required": True},
        ],
        "actions": [
            {
                "name": "load_{{ view_suffix }}",
                "type": "load",
                "source": {"type": "sql", "sql": "SELECT 1 AS id"},
                "target": "v_{{ view_suffix }}_raw",
            },
            {
                "name": "write_{{ view_suffix }}",
                "type": "write",
                "source": "v_{{ view_suffix }}_raw",
                "write_target": {
                    "type": "streaming_table",
                    "catalog": "${catalog}",
                    "schema": "${schema}",
                    "table": "{{ table_name }}",
                    # create_table omitted -> defaults to True (a creator).
                },
            },
        ],
    }
    with open(project_root / "templates" / "dup_table.yaml", "w") as f:
        yaml.dump(template, f)

    for fg_name, suffix in (("fg_alpha", "alpha"), ("fg_beta", "beta")):
        flowgroup = {
            "pipeline": "dup_pipeline",
            "flowgroup": fg_name,
            "use_template": "dup_table",
            "template_parameters": {
                "table_name": "shared_tbl",  # SAME table in both -> the conflict
                "view_suffix": suffix,  # DIFFERENT view -> no spurious conflict
            },
        }
        with open(
            project_root / "pipelines" / "dup_pipeline" / f"{fg_name}.yaml", "w"
        ) as f:
            yaml.dump(flowgroup, f)


@pytest.mark.integration
def test_validate_catches_template_resolved_duplicate_table_invisible_in_raw():
    """§9.24 closure: a template-resolved duplicate-table conflict fails validate.

    Demonstrates the "raw-invisible, resolved-visible" contrast the refactor
    fixes:

    * **Raw-invisible** — the two ``use_template`` flowgroups discovered from
      disk carry EMPTY action lists (the template is not yet expanded), so the
      cross-flowgroup ``TableCreationValidator`` run on the RAW set finds zero
      table creators and reports no error. This is what the pre-§9.24 barrier
      (which ran on raw flowgroups) saw — validate would have PASSED.
    * **Resolved-visible** — ``lhp validate`` now runs the barrier on the
      RESOLVED flowgroups inside the consolidated engine (§9.24).
      After resolution both flowgroups create
      ``dev_catalog.shared_schema.shared_tbl``, so validate FAILS with the
      structured ``LHP-CFG-004`` ("Multiple table creators") and a non-zero
      exit code.
    """
    from lhp.api.facade import LakehousePlumberApplicationFacade
    from lhp.cli.exit_codes import ExitCode

    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir)
        _build_template_duplicate_table_project(project_root)

        cwd = os.getcwd()
        try:
            os.chdir(project_root)

            # ---- RAW-INVISIBLE: the duplicate does NOT exist pre-resolution ----
            # Discover the disk flowgroups (un-resolved) and run the SAME
            # cross-flowgroup barrier on them that the pre-§9.24 path used.
            facade = LakehousePlumberApplicationFacade.for_project(
                project_root, enforce_version=False
            )
            orchestrator = facade._orchestrator
            raw_flowgroups = list(orchestrator.bootstrap.discover_all_flowgroups())

            # Both flowgroups are present but their template is un-expanded:
            # zero actions, hence zero write/create_table actions to conflict.
            assert {fg.flowgroup for fg in raw_flowgroups} == {"fg_alpha", "fg_beta"}
            assert all(
                fg.actions == [] for fg in raw_flowgroups
            ), "raw use_template flowgroups must carry no expanded actions"

            raw_cross = orchestrator.validation.validate_cross_flowgroup(
                raw_flowgroups, pipeline_filter="dup_pipeline"
            )
            assert raw_cross.has_errors is False, (
                "the duplicate-table conflict must be INVISIBLE in the raw "
                f"flowgroups (got {raw_cross.table_creation_errors})"
            )
            assert raw_cross.table_creation_errors == []

            # ---- RESOLVED-VISIBLE: lhp validate now catches it ----
            result = runner.invoke(cli, ["validate", "--env", "dev", "--show-all"])

            # Validate is REJECTED (it would have passed under the raw barrier).
            assert result.exit_code == ExitCode.DATA_ERROR, (
                f"expected validate to FAIL (exit {ExitCode.DATA_ERROR}); got "
                f"{result.exit_code}:\n{result.output}"
            )
            # The structured cross-flowgroup duplicate-table error surfaces, on
            # the fully-resolved table name.
            assert "LHP-CFG-004" in result.output, result.output
            assert "Multiple table creators" in result.output, result.output
            assert (
                "dev_catalog.shared_schema.shared_tbl" in result.output
            ), result.output
        finally:
            os.chdir(cwd)
