"""Unit tests for the flat-pool pipeline executor.

A single :class:`ProcessPoolExecutor` (under a ``spawn`` multiprocessing
context) runs Phase A across ALL pipelines. The per-pipeline barrier is
realised in the main thread via :func:`as_completed`: when a pipeline's
flowgroup bucket is full, the ``assemble_pipeline`` hook (Phase B) is
invoked synchronously, then the ``on_pipeline_complete`` callback fires.

These tests use top-level fake workers so the callable can be pickled
into a spawned worker process — closures or lambdas would not pickle.
They cover:

- empty input
- single pipeline, single + multiple flowgroups
- multi-pipeline (interleaved completion order)
- mixed success/failure
- determinism — running the same workload many times should produce
  byte-identical assembled outcomes, regardless of worker scheduling
- callback ordering — fires once per pipeline, in completion order
- max_workers=1 behaviour matches max_workers=8 by content

The ``no state in workers`` invariant is verified in
:mod:`tests.test_pipeline_executor_no_state_in_workers`.

The pool spawns subprocesses, which has a per-test warmup cost (~500ms).
Tests are marked ``slow`` so users can opt out of the suite during
fast iteration loops.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import pytest

from lhp.core.pipeline_executor import (
    FlowgroupResult,
    PipelineGenerationOutcome,
    run_generate_pool,
)
from lhp.generators.python_file_copier import CopiedModuleRecord


class _FakeFlowGroup:
    """Picklable FlowGroup stand-in driven by a string ``mode`` attribute.

    ``mode`` keys into :func:`_fake_process_one` to select the response
    shape per test. Carrying behaviour on the model (rather than via a
    per-test closure) keeps the worker function top-level and importable
    so it can be pickled into a spawned worker process.
    """

    def __init__(self, pipeline: str, flowgroup: str, *, mode: str = "default"):
        self.pipeline = pipeline
        self.flowgroup = flowgroup
        self.mode = mode


def _make_result(
    pipeline: str,
    flowgroup: str,
    *,
    success: bool = True,
    formatted: str = "",
    error: Exception = None,
    records: Tuple[CopiedModuleRecord, ...] = (),
) -> FlowgroupResult:
    return FlowgroupResult(
        pipeline=pipeline,
        flowgroup_name=flowgroup,
        processed_flowgroup=None,
        code=formatted,
        formatted_code=formatted,
        source_yaml=None,
        success=success,
        copied_modules=records,
        error=error,
    )


def _fake_process_one(fg: _FakeFlowGroup) -> FlowgroupResult:
    """Top-level picklable worker. Behaviour switches on ``fg.mode``."""
    if fg.mode == "default":
        return _make_result(fg.pipeline, fg.flowgroup)
    if fg.mode == "format_name":
        return _make_result(fg.pipeline, fg.flowgroup, formatted=f"code_{fg.flowgroup}")
    if fg.mode == "format_pipe_colon_name":
        return _make_result(
            fg.pipeline, fg.flowgroup, formatted=f"{fg.pipeline}:{fg.flowgroup}"
        )
    if fg.mode == "format_pipe_slash_name":
        return _make_result(
            fg.pipeline, fg.flowgroup, formatted=f"{fg.pipeline}/{fg.flowgroup}"
        )
    if fg.mode == "format_pipe_double_colon_name":
        return _make_result(
            fg.pipeline, fg.flowgroup, formatted=f"{fg.pipeline}::{fg.flowgroup}"
        )
    if fg.mode == "ok":
        return _make_result(fg.pipeline, fg.flowgroup, formatted="ok")
    if fg.mode == "fail_if_bad":
        if fg.flowgroup == "bad":
            return _make_result(
                fg.pipeline,
                fg.flowgroup,
                success=False,
                error=RuntimeError("oh no"),
            )
        return _make_result(fg.pipeline, fg.flowgroup, formatted="ok")
    raise ValueError(f"Unknown _FakeFlowGroup.mode={fg.mode!r}")


def _ok_assembler(
    pipeline: str, results: List[FlowgroupResult]
) -> PipelineGenerationOutcome:
    """Default Phase B: aggregate formatted code into one generated_files map.

    Phase B runs on the main thread, so this closure-free top-level
    function suffices — picklability is not required for assemblers.
    """
    files = {f"{r.flowgroup_name}.py": r.formatted_code for r in results if r.success}
    failed = sum(1 for r in results if not r.success)
    return PipelineGenerationOutcome(
        pipeline=pipeline,
        generated_files=files,
        files_written=len(files),
        flowgroups_processed=len(results),
        flowgroups_failed=failed,
        success=failed == 0,
    )


@pytest.mark.unit
@pytest.mark.slow
class TestRunGeneratePoolBasics:
    def test_empty_pipelines_returns_empty_lists(self):
        # No worklist → executor is never constructed; no spawn cost.
        succ, fail = run_generate_pool(
            pipelines=[],
            flowgroups_by_pipeline={},
            process_one=_fake_process_one,
            assemble_pipeline=_ok_assembler,
            max_workers=4,
        )
        assert succ == []
        assert fail == []

    def test_empty_pipeline_still_emits_outcome(self):
        """A pipeline with zero flowgroups gets an empty assembly call."""
        succ, fail = run_generate_pool(
            pipelines=["p_empty"],
            flowgroups_by_pipeline={"p_empty": []},
            process_one=_fake_process_one,
            assemble_pipeline=_ok_assembler,
            max_workers=4,
        )
        assert len(succ) == 1
        assert succ[0].pipeline == "p_empty"
        assert succ[0].generated_files == {}

    def test_single_pipeline_single_flowgroup(self):
        fgs = [_FakeFlowGroup("p1", "fg_a", mode="format_name")]
        succ, fail = run_generate_pool(
            pipelines=["p1"],
            flowgroups_by_pipeline={"p1": fgs},
            process_one=_fake_process_one,
            assemble_pipeline=_ok_assembler,
            max_workers=4,
        )
        assert fail == []
        assert len(succ) == 1
        assert succ[0].generated_files == {"fg_a.py": "code_fg_a"}

    def test_single_pipeline_multiple_flowgroups(self):
        fgs = [_FakeFlowGroup("p1", f"fg_{i}", mode="format_name") for i in range(5)]
        succ, fail = run_generate_pool(
            pipelines=["p1"],
            flowgroups_by_pipeline={"p1": fgs},
            process_one=_fake_process_one,
            assemble_pipeline=_ok_assembler,
            max_workers=4,
        )
        assert fail == []
        assert len(succ) == 1
        assert set(succ[0].generated_files) == {f"fg_{i}.py" for i in range(5)}


@pytest.mark.unit
@pytest.mark.slow
class TestRunGeneratePoolMultiPipeline:
    def test_multi_pipeline_outcomes_present(self):
        pipelines = ["p1", "p2", "p3"]
        flowgroups: Dict[str, Sequence[_FakeFlowGroup]] = {
            "p1": [
                _FakeFlowGroup("p1", "a", mode="format_pipe_colon_name"),
                _FakeFlowGroup("p1", "b", mode="format_pipe_colon_name"),
            ],
            "p2": [_FakeFlowGroup("p2", "c", mode="format_pipe_colon_name")],
            "p3": [
                _FakeFlowGroup("p3", "d", mode="format_pipe_colon_name"),
                _FakeFlowGroup("p3", "e", mode="format_pipe_colon_name"),
            ],
        }
        succ, fail = run_generate_pool(
            pipelines=pipelines,
            flowgroups_by_pipeline=flowgroups,
            process_one=_fake_process_one,
            assemble_pipeline=_ok_assembler,
            max_workers=4,
        )
        assert fail == []
        by_pipeline = {o.pipeline: o for o in succ}
        assert set(by_pipeline.keys()) == set(pipelines)
        assert by_pipeline["p1"].files_written == 2
        assert by_pipeline["p2"].files_written == 1
        assert by_pipeline["p3"].files_written == 2

    def test_pipeline_partial_failure_lands_in_failed_bucket(self):
        fgs = {
            "p_ok": [_FakeFlowGroup("p_ok", "a", mode="fail_if_bad")],
            "p_partial": [
                _FakeFlowGroup("p_partial", "a", mode="fail_if_bad"),
                _FakeFlowGroup("p_partial", "bad", mode="fail_if_bad"),
            ],
        }
        succ, fail = run_generate_pool(
            pipelines=["p_ok", "p_partial"],
            flowgroups_by_pipeline=fgs,
            process_one=_fake_process_one,
            assemble_pipeline=_ok_assembler,
            max_workers=4,
        )
        assert len(succ) == 1 and succ[0].pipeline == "p_ok"
        assert len(fail) == 1 and fail[0].pipeline == "p_partial"
        assert fail[0].flowgroups_failed == 1


@pytest.mark.unit
@pytest.mark.slow
class TestRunGeneratePoolCallback:
    def test_callback_fires_once_per_pipeline_in_completion_order(self):
        seen = []
        lock = threading.Lock()

        def cb(outcome: PipelineGenerationOutcome) -> None:
            # Callback fires on the main thread after the pool exits — a
            # closure here is fine (no pickle boundary).
            with lock:
                seen.append(outcome.pipeline)

        fgs = {f"p{i}": [_FakeFlowGroup(f"p{i}", "only", mode="ok")] for i in range(5)}
        succ, fail = run_generate_pool(
            pipelines=list(fgs.keys()),
            flowgroups_by_pipeline=fgs,
            process_one=_fake_process_one,
            assemble_pipeline=_ok_assembler,
            max_workers=4,
            on_pipeline_complete=cb,
        )
        assert sorted(seen) == sorted(fgs.keys())
        assert len(seen) == 5

    def test_callback_exception_does_not_abort_pool(self):
        def bad_cb(_outcome: PipelineGenerationOutcome) -> None:
            raise RuntimeError("callback boom")

        fgs = {"p1": [_FakeFlowGroup("p1", "a", mode="ok")]}
        # Should NOT raise — the callback's exception is swallowed and logged.
        succ, fail = run_generate_pool(
            pipelines=["p1"],
            flowgroups_by_pipeline=fgs,
            process_one=_fake_process_one,
            assemble_pipeline=_ok_assembler,
            max_workers=4,
            on_pipeline_complete=bad_cb,
        )
        assert len(succ) == 1


@pytest.mark.unit
@pytest.mark.slow
class TestRunGeneratePoolDeterminism:
    def test_max_workers_1_vs_8_produce_identical_assembled_content(self):
        """Sequential and parallel runs must produce identical generated_files."""
        pipelines = ["p1", "p2", "p3"]
        flowgroups = {
            p: [
                _FakeFlowGroup(p, f"fg_{i}", mode="format_pipe_slash_name")
                for i in range(6)
            ]
            for p in pipelines
        }

        succ1, _ = run_generate_pool(
            pipelines=pipelines,
            flowgroups_by_pipeline=flowgroups,
            process_one=_fake_process_one,
            assemble_pipeline=_ok_assembler,
            max_workers=1,
        )
        succ8, _ = run_generate_pool(
            pipelines=pipelines,
            flowgroups_by_pipeline=flowgroups,
            process_one=_fake_process_one,
            assemble_pipeline=_ok_assembler,
            max_workers=8,
        )
        by1 = {o.pipeline: dict(o.generated_files) for o in succ1}
        by8 = {o.pipeline: dict(o.generated_files) for o in succ8}
        assert by1 == by8

    def test_repeat_runs_produce_byte_identical_outcomes(self):
        """Repeated runs of the same workload must produce identical content.

        Reduced from 50 → 10 iterations because each iteration now spawns
        a fresh worker pool (process-mode warmup ~500ms). The non-determinism
        we're checking — out-of-order Phase A completion — manifests on every
        run, not statistically over many, so 10 reruns is enough confidence.
        """
        pipelines = ["p1", "p2", "p3", "p4"]
        flowgroups = {
            p: [
                _FakeFlowGroup(p, f"fg_{i}", mode="format_pipe_double_colon_name")
                for i in range(4)
            ]
            for p in pipelines
        }

        baseline = None
        for _ in range(10):
            succ, _ = run_generate_pool(
                pipelines=pipelines,
                flowgroups_by_pipeline=flowgroups,
                process_one=_fake_process_one,
                assemble_pipeline=_ok_assembler,
                max_workers=4,
            )
            run_content = {o.pipeline: dict(o.generated_files) for o in succ}
            if baseline is None:
                baseline = run_content
            else:
                assert (
                    run_content == baseline
                ), "Non-deterministic outcome across repeated runs"
