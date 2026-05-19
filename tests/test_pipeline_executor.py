"""Unit tests for the per-pipeline-batched pool dispatch.

A single :class:`ProcessPoolExecutor` (under a ``spawn`` multiprocessing
context) submits ONE task per pipeline. Each task runs the full worker-
side pass and returns a :class:`PipelineDelta`. The main thread acts as
a pure aggregator — Phase B no longer runs there.

These tests use top-level fake workers so the callable can be pickled
into a spawned worker process — closures or lambdas would not pickle.
They cover:

- empty input
- single pipeline, single + multiple flowgroups
- multi-pipeline (interleaved completion order)
- mixed success/failure (whole-pipeline failure model)
- determinism — running the same workload many times should produce
  byte-identical deltas, regardless of worker completion order
- callback ordering — fires once per pipeline, in completion order
- max_workers=1 behaviour matches max_workers=8 by content

The ``no state in workers`` invariant is verified in
:mod:`tests.test_pipeline_executor_no_state_in_workers`.

The pool spawns subprocesses, which has a per-test warmup cost (~500ms).
Tests are marked ``slow`` so users can opt out of the suite during
fast iteration loops.
"""

import threading
from typing import Dict, Sequence

import pytest

from lhp.core.pipeline_executor import group_by_pipeline, run_generate_pool
from lhp.core.state_models import PipelineDelta
from lhp.models.config import FlowGroupContext


class _FakeFlowGroup:
    """Picklable FlowGroup stand-in driven by a string ``mode`` attribute.

    ``mode`` keys into :func:`_fake_process_one_pipeline` to select the
    response shape per test. Carrying behaviour on the model (rather than
    via a per-test closure) keeps the worker function top-level and
    importable so it can be pickled into a spawned worker process.
    """

    def __init__(self, pipeline: str, flowgroup: str, *, mode: str = "default"):
        self.pipeline = pipeline
        self.flowgroup = flowgroup
        self.mode = mode


def _ctx(fg: "_FakeFlowGroup") -> FlowGroupContext:
    """Wrap a fake FlowGroup in a real FlowGroupContext envelope.

    ``FlowGroupContext`` is a frozen dataclass with no runtime validation
    on its ``flowgroup`` field, so the picklable fake passes through.
    """
    return FlowGroupContext(flowgroup=fg, source_yaml=None)


def _format_content(fg: "_FakeFlowGroup") -> str:
    """Render the formatted-code payload a worker would compute for ``fg``."""
    if fg.mode == "default":
        return ""
    if fg.mode == "format_name":
        return f"code_{fg.flowgroup}"
    if fg.mode == "format_pipe_colon_name":
        return f"{fg.pipeline}:{fg.flowgroup}"
    if fg.mode == "format_pipe_slash_name":
        return f"{fg.pipeline}/{fg.flowgroup}"
    if fg.mode == "format_pipe_double_colon_name":
        return f"{fg.pipeline}::{fg.flowgroup}"
    if fg.mode == "ok":
        return "ok"
    if fg.mode == "fail_if_bad":
        if fg.flowgroup == "bad":
            raise RuntimeError("oh no")
        return "ok"
    raise ValueError(f"Unknown _FakeFlowGroup.mode={fg.mode!r}")


def _fake_process_one_pipeline(
    pipeline_name: str, contexts: Sequence[FlowGroupContext]
) -> PipelineDelta:
    """Top-level picklable per-pipeline worker.

    Mirrors the legacy per-flowgroup helper's mode dispatch but produces
    a :class:`PipelineDelta` instead of a list of per-flowgroup results.
    A single flowgroup raising mid-iteration fails the whole pipeline —
    that's the worker-atomicity contract that motivated the dispatch
    refactor in the first place.
    """
    generated_filenames: list[str] = []
    try:
        for ctx in contexts:
            fg = ctx.flowgroup
            content = _format_content(fg)
            if content:
                generated_filenames.append(f"{fg.flowgroup}.py")
    except BaseException as exc:
        return PipelineDelta.failure(pipeline_name, exc)
    return PipelineDelta.success_(
        pipeline_name,
        files_written=len(generated_filenames),
        generated_filenames=tuple(generated_filenames),
    )


@pytest.mark.unit
@pytest.mark.slow
class TestRunGeneratePoolBasics:
    def test_empty_pipelines_returns_empty_lists(self):
        # No worklist → executor is never constructed; no spawn cost.
        succ, fail = run_generate_pool(
            flowgroups_by_pipeline={},
            process_one=_fake_process_one_pipeline,
            max_workers=4,
        )
        assert succ == []
        assert fail == []

    def test_empty_pipeline_emits_success_delta(self):
        """A pipeline with zero flowgroups still produces a success delta."""
        succ, fail = run_generate_pool(
            flowgroups_by_pipeline={"p_empty": []},
            process_one=_fake_process_one_pipeline,
            max_workers=4,
        )
        assert len(succ) == 1
        assert succ[0].pipeline_name == "p_empty"
        assert succ[0].generated_filenames == ()

    def test_single_pipeline_single_flowgroup(self):
        fgs = [_ctx(_FakeFlowGroup("p1", "fg_a", mode="format_name"))]
        succ, fail = run_generate_pool(
            flowgroups_by_pipeline={"p1": fgs},
            process_one=_fake_process_one_pipeline,
            max_workers=4,
        )
        assert fail == []
        assert len(succ) == 1
        assert succ[0].generated_filenames == ("fg_a.py",)

    def test_single_pipeline_multiple_flowgroups(self):
        fgs = [
            _ctx(_FakeFlowGroup("p1", f"fg_{i}", mode="format_name"))
            for i in range(5)
        ]
        succ, fail = run_generate_pool(
            flowgroups_by_pipeline={"p1": fgs},
            process_one=_fake_process_one_pipeline,
            max_workers=4,
        )
        assert fail == []
        assert len(succ) == 1
        assert set(succ[0].generated_filenames) == {f"fg_{i}.py" for i in range(5)}


@pytest.mark.unit
@pytest.mark.slow
class TestRunGeneratePoolMultiPipeline:
    def test_multi_pipeline_deltas_present(self):
        flowgroups: Dict[str, Sequence[FlowGroupContext]] = {
            "p1": [
                _ctx(_FakeFlowGroup("p1", "a", mode="format_pipe_colon_name")),
                _ctx(_FakeFlowGroup("p1", "b", mode="format_pipe_colon_name")),
            ],
            "p2": [_ctx(_FakeFlowGroup("p2", "c", mode="format_pipe_colon_name"))],
            "p3": [
                _ctx(_FakeFlowGroup("p3", "d", mode="format_pipe_colon_name")),
                _ctx(_FakeFlowGroup("p3", "e", mode="format_pipe_colon_name")),
            ],
        }
        succ, fail = run_generate_pool(
            flowgroups_by_pipeline=flowgroups,
            process_one=_fake_process_one_pipeline,
            max_workers=4,
        )
        assert fail == []
        by_pipeline = {d.pipeline_name: d for d in succ}
        assert set(by_pipeline.keys()) == set(flowgroups)
        assert by_pipeline["p1"].files_written == 2
        assert by_pipeline["p2"].files_written == 1
        assert by_pipeline["p3"].files_written == 2

    def test_pipeline_failure_lands_in_failed_bucket(self):
        """A flowgroup raising inside a worker fails the whole pipeline.

        With per-pipeline dispatch, partial-pipeline success isn't a
        thing — the worker either returns a success delta or a failure
        delta for the WHOLE pipeline. This matches the atomicity model
        of :class:`PipelineProcessor`.
        """
        fgs = {
            "p_ok": [_ctx(_FakeFlowGroup("p_ok", "a", mode="fail_if_bad"))],
            "p_partial": [
                _ctx(_FakeFlowGroup("p_partial", "a", mode="fail_if_bad")),
                _ctx(_FakeFlowGroup("p_partial", "bad", mode="fail_if_bad")),
            ],
        }
        succ, fail = run_generate_pool(
            flowgroups_by_pipeline=fgs,
            process_one=_fake_process_one_pipeline,
            max_workers=4,
        )
        assert len(succ) == 1 and succ[0].pipeline_name == "p_ok"
        assert len(fail) == 1 and fail[0].pipeline_name == "p_partial"
        assert fail[0].error_type == "RuntimeError"
        assert fail[0].error_message == "oh no"


@pytest.mark.unit
@pytest.mark.slow
class TestRunGeneratePoolCallback:
    def test_callback_fires_once_per_pipeline_in_completion_order(self):
        seen = []
        lock = threading.Lock()

        def cb(delta: PipelineDelta) -> None:
            # Callback fires on the main thread after each pipeline
            # completes — a closure here is fine (no pickle boundary).
            with lock:
                seen.append(delta.pipeline_name)

        fgs = {
            f"p{i}": [_ctx(_FakeFlowGroup(f"p{i}", "only", mode="ok"))]
            for i in range(5)
        }
        succ, fail = run_generate_pool(
            flowgroups_by_pipeline=fgs,
            process_one=_fake_process_one_pipeline,
            max_workers=4,
            on_pipeline_complete=cb,
        )
        assert sorted(seen) == sorted(fgs.keys())
        assert len(seen) == 5

    def test_callback_exception_does_not_abort_pool(self):
        def bad_cb(_delta: PipelineDelta) -> None:
            raise RuntimeError("callback boom")

        fgs = {"p1": [_ctx(_FakeFlowGroup("p1", "a", mode="ok"))]}
        # Should NOT raise — the callback's exception is swallowed and logged.
        succ, fail = run_generate_pool(
            flowgroups_by_pipeline=fgs,
            process_one=_fake_process_one_pipeline,
            max_workers=4,
            on_pipeline_complete=bad_cb,
        )
        assert len(succ) == 1


@pytest.mark.unit
@pytest.mark.slow
class TestRunGeneratePoolDeterminism:
    def test_max_workers_1_vs_8_produce_identical_deltas(self):
        """Sequential and parallel runs must produce identical generated_filenames."""
        flowgroups = {
            p: [
                _ctx(_FakeFlowGroup(p, f"fg_{i}", mode="format_pipe_slash_name"))
                for i in range(6)
            ]
            for p in ("p1", "p2", "p3")
        }

        succ1, _ = run_generate_pool(
            flowgroups_by_pipeline=flowgroups,
            process_one=_fake_process_one_pipeline,
            max_workers=1,
        )
        succ8, _ = run_generate_pool(
            flowgroups_by_pipeline=flowgroups,
            process_one=_fake_process_one_pipeline,
            max_workers=8,
        )
        by1 = {d.pipeline_name: d.generated_filenames for d in succ1}
        by8 = {d.pipeline_name: d.generated_filenames for d in succ8}
        assert by1 == by8

    def test_repeat_runs_produce_byte_identical_deltas(self):
        """Repeated runs of the same workload must produce identical content.

        Each iteration spawns a fresh worker pool (process-mode warmup
        ~500ms). The non-determinism being checked — out-of-order worker
        completion — manifests on every run, not statistically over many,
        so 10 reruns is enough confidence.
        """
        flowgroups = {
            p: [
                _ctx(_FakeFlowGroup(p, f"fg_{i}", mode="format_pipe_double_colon_name"))
                for i in range(4)
            ]
            for p in ("p1", "p2", "p3", "p4")
        }

        baseline = None
        for _ in range(10):
            succ, _ = run_generate_pool(
                flowgroups_by_pipeline=flowgroups,
                process_one=_fake_process_one_pipeline,
                max_workers=4,
            )
            run_content = {
                d.pipeline_name: d.generated_filenames for d in succ
            }
            if baseline is None:
                baseline = run_content
            else:
                assert (
                    run_content == baseline
                ), "Non-deterministic outcome across repeated runs"


@pytest.mark.unit
class TestGroupByPipeline:
    """:func:`group_by_pipeline` is a pure helper — no pool involved."""

    def test_groups_by_pipeline_field(self):
        contexts = [
            _ctx(_FakeFlowGroup("p1", "a")),
            _ctx(_FakeFlowGroup("p2", "b")),
            _ctx(_FakeFlowGroup("p1", "c")),
        ]
        result = group_by_pipeline(contexts)
        assert set(result) == {"p1", "p2"}
        assert [ctx.flowgroup.flowgroup for ctx in result["p1"]] == ["a", "c"]
        assert [ctx.flowgroup.flowgroup for ctx in result["p2"]] == ["b"]

    def test_preserves_first_occurrence_order(self):
        # Insertion order: pipelines first appear in this order: p2, p1, p3
        contexts = [
            _ctx(_FakeFlowGroup("p2", "x")),
            _ctx(_FakeFlowGroup("p1", "y")),
            _ctx(_FakeFlowGroup("p2", "z")),
            _ctx(_FakeFlowGroup("p3", "w")),
        ]
        result = group_by_pipeline(contexts)
        assert list(result.keys()) == ["p2", "p1", "p3"]

    def test_empty_input_returns_empty_dict(self):
        assert group_by_pipeline([]) == {}
