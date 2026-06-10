"""Unit tests for the generate delta-STREAM source of truth.

:meth:`PipelineExecutionService._iter_generate_deltas` is the generator that
the generate path produces deltas through; ``run_generate`` is itself a thin
``yield from`` over it (E3 removed the prior callback-firing drain adapter
``_run_generate_engine_and_gate``, so the facade/orchestrator consume the delta
stream directly). These tests pin the stream's three load-bearing properties:

1. deltas iterate in DETERMINISTIC **input-pipeline order** — the order
   :func:`._pool._run_flowgroup_pool_core` returns its per-pipeline buckets
   (``list(flowgroups_by_pipeline.keys())``), NOT flowgroup-completion order
   and NOT alphabetical;
2. on a gate FAILURE, EVERY failure delta is yielded BEFORE the gate raises
   (constitution §1.4: the raise closes the stream only after the failures
   are observed);
3. on a gate PASS, a success delta per committed pipeline appears, in input
   order.

The spawn pool is removed the same way ``test_executor_validate`` /
``test_flowgroup_pool`` do it: a synchronous in-process executor swapped onto
the engine module (``_pool``) plus an in-process fake worker returning
pre-canned generate-mode :class:`FlowgroupOutcome`s. Everything between the
engine and the stream — the gate, the failure-delta synthesis, and the commit
generator — is the real production driver, unmodified.
"""

from __future__ import annotations

from concurrent.futures import Future
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

import pytest

from lhp.core.coordination import _pool as fe
from lhp.core.coordination._flowgroup_pool import _FlowgroupWorkerState
from lhp.core.coordination.executor import PipelineExecutionService
from lhp.errors import ErrorCategory, LHPError, LHPValidationError
from lhp.models import FlowGroupContext
from lhp.models.processing import FlowgroupOutcome, PipelineDelta


class _FakeFlowGroup:
    """Minimal FlowGroup stand-in (the worker/barrier read only these two)."""

    def __init__(self, pipeline: str, flowgroup: str) -> None:
        self.pipeline = pipeline
        self.flowgroup = flowgroup


def _ctx(pipeline: str, flowgroup: str) -> FlowGroupContext:
    return FlowGroupContext(
        flowgroup=_FakeFlowGroup(pipeline, flowgroup), source_yaml=None
    )


class _SyncExecutor:
    """Synchronous in-process stand-in for ``ProcessPoolExecutor``.

    Runs each submitted callable eagerly on ``submit`` and returns an
    already-completed :class:`Future`, preserving the engine's
    ``submit`` / ``as_completed`` / ``result`` call shape while removing the
    spawn boundary. Ignores the pool kwargs the engine passes.
    """

    def __init__(self, *args, **kwargs) -> None:
        self.submitted: List[tuple] = []

    def __enter__(self) -> "_SyncExecutor":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def submit(self, fn, /, *args, **kwargs) -> Future:
        self.submitted.append((args, kwargs))
        fut: Future = Future()
        try:
            fut.set_result(fn(*args, **kwargs))
        except Exception as exc:  # pragma: no cover - fake worker never raises
            fut.set_exception(exc)
        return fut


class _BarrierSpy:
    """Returns an empty cross-flowgroup result → no cross-fg issues.

    Isolates the stream's ordering / gate / commit wiring from the
    issue-building path (covered elsewhere).
    """

    def validate_cross_flowgroup(
        self,
        flowgroups: Sequence[object],
        *,
        pipeline_filter: Optional[str] = None,
    ):
        from lhp.core._interfaces import CrossFlowgroupCheckResult

        return CrossFlowgroupCheckResult()


def _val_error() -> LHPValidationError:
    return LHPValidationError(
        category=ErrorCategory.VALIDATION,
        code_number="007",
        title="FlowGroup validation failed",
        details="bad flowgroup",
    )


def _gate_worker_factory(outcomes_by_key: Mapping[Tuple[str, str], FlowgroupOutcome]):
    """Build a fake generate-mode worker returning a pre-canned outcome per fg."""

    def _fake_worker(fg_ctx: FlowGroupContext, *, mode: str) -> FlowgroupOutcome:
        assert mode == "generate"
        key = (fg_ctx.flowgroup.pipeline, fg_ctx.flowgroup.flowgroup)
        return outcomes_by_key[key]

    return _fake_worker


def _service_for(
    monkeypatch,
    *,
    flowgroups_by_pipeline: Mapping[str, Sequence[FlowGroupContext]],
    outcomes_by_key: Mapping[Tuple[str, str], FlowgroupOutcome],
    pipeline_dirs: Mapping[str, Optional[Path]],
) -> Tuple[PipelineExecutionService, _FlowgroupWorkerState]:
    """Wire the sync-executor seam + fake worker; return (service, worker_state)."""
    monkeypatch.setattr(fe, "ProcessPoolExecutor", _SyncExecutor)
    monkeypatch.setattr(
        fe, "_process_one_flowgroup", _gate_worker_factory(outcomes_by_key)
    )
    worker_state = _FlowgroupWorkerState(
        processor=_BarrierSpy(),  # never called by the fake worker
        substitution_managers=dict.fromkeys(flowgroups_by_pipeline, object()),
        include_tests=False,
        code_generator=_BarrierSpy(),  # never called by the fake worker
        pipeline_output_dirs=dict(pipeline_dirs),
        environment="dev",
    )
    return PipelineExecutionService(max_workers=4), worker_state


def _ok(pipeline: str, flowgroup: str, code: str) -> FlowgroupOutcome:
    return FlowgroupOutcome.ok(
        pipeline,
        flowgroup,
        resolved_flowgroup=_FakeFlowGroup(pipeline, flowgroup),
        formatted_code=code,
    )


@pytest.mark.unit
def test_iter_generate_deltas_success_in_input_pipeline_order(monkeypatch, tmp_path):
    """All-clean 3-pipeline run → success deltas in INPUT order, not alphabetical.

    The insertion order ``["gamma", "alpha", "beta"]`` is deliberately NOT
    sorted: a stream that yielded in completion order (the sync executor drains
    in submit order, which IS the worklist/insertion order here) or in
    alphabetical order would diverge from this. The assertion that the delta
    sequence equals the dict's key order proves the stream crystallizes in
    input-pipeline order (matching ``_run_flowgroup_pool_core``'s
    ``list(flowgroups_by_pipeline.keys())``), the same order the legacy
    callback path observed.
    """
    flowgroups_by_pipeline = {
        "gamma": [_ctx("gamma", "fg_g")],
        "alpha": [_ctx("alpha", "fg_a")],
        "beta": [_ctx("beta", "fg_b")],
    }
    outcomes_by_key = {
        ("gamma", "fg_g"): _ok("gamma", "fg_g", "g = 1\n"),
        ("alpha", "fg_a"): _ok("alpha", "fg_a", "a = 1\n"),
        ("beta", "fg_b"): _ok("beta", "fg_b", "b = 1\n"),
    }
    env_dir = tmp_path / "generated"
    pipeline_dirs = {p: env_dir / p for p in flowgroups_by_pipeline}

    service, worker_state = _service_for(
        monkeypatch,
        flowgroups_by_pipeline=flowgroups_by_pipeline,
        outcomes_by_key=outcomes_by_key,
        pipeline_dirs=pipeline_dirs,
    )

    deltas = list(
        service._iter_generate_deltas(
            flowgroups_by_pipeline=flowgroups_by_pipeline,
            worker_state=worker_state,
            validation_service=_BarrierSpy(),
            output_dir=env_dir,
            project_config=None,
            project_root=tmp_path,
            max_workers=4,
        )
    )

    assert [d.pipeline_name for d in deltas] == ["gamma", "alpha", "beta"]
    assert all(d.success for d in deltas)
    assert [d.generated_filenames for d in deltas] == [
        ("fg_g.py",),
        ("fg_a.py",),
        ("fg_b.py",),
    ]
    assert (pipeline_dirs["gamma"] / "fg_g.py").read_text(encoding="utf-8") == "g = 1\n"
    assert (pipeline_dirs["alpha"] / "fg_a.py").read_text(encoding="utf-8") == "a = 1\n"
    assert (pipeline_dirs["beta"] / "fg_b.py").read_text(encoding="utf-8") == "b = 1\n"


@pytest.mark.unit
def test_iter_generate_deltas_all_failures_yielded_before_raise(monkeypatch, tmp_path):
    """Two failing pipelines → BOTH failure deltas yielded, THEN the gate raises.

    Drains the generator manually, collecting each yielded delta until it
    raises. The §1.4 contract: every failure delta is emitted BEFORE
    :func:`._generate_gate.gate_or_raise` raises (the raise closes the stream),
    and the raise is the multi-failure aggregate ``LHP-VAL-902``. The deltas
    are in INPUT pipeline order, and NO files are written (the gate precedes
    commit).
    """
    flowgroups_by_pipeline = {
        "pipe_bad1": [_ctx("pipe_bad1", "fg_b1")],
        "pipe_bad2": [_ctx("pipe_bad2", "fg_b2")],
    }
    outcomes_by_key = {
        ("pipe_bad1", "fg_b1"): FlowgroupOutcome.failure(
            "pipe_bad1", "fg_b1", lhp_error=_val_error()
        ),
        ("pipe_bad2", "fg_b2"): FlowgroupOutcome.failure(
            "pipe_bad2", "fg_b2", lhp_error=_val_error()
        ),
    }
    env_dir = tmp_path / "generated"
    pipeline_dirs = {p: env_dir / p for p in flowgroups_by_pipeline}

    service, worker_state = _service_for(
        monkeypatch,
        flowgroups_by_pipeline=flowgroups_by_pipeline,
        outcomes_by_key=outcomes_by_key,
        pipeline_dirs=pipeline_dirs,
    )

    stream = service._iter_generate_deltas(
        flowgroups_by_pipeline=flowgroups_by_pipeline,
        worker_state=worker_state,
        validation_service=_BarrierSpy(),
        output_dir=env_dir,
        project_config=None,
        project_root=tmp_path,
        max_workers=4,
    )

    seen: List[PipelineDelta] = []
    with pytest.raises(LHPError) as excinfo:
        for delta in stream:
            seen.append(delta)

    assert [d.pipeline_name for d in seen] == ["pipe_bad1", "pipe_bad2"]
    assert all(not d.success for d in seen)
    assert excinfo.value.code == "LHP-VAL-902"
    assert not env_dir.exists()


@pytest.mark.unit
def test_iter_generate_deltas_failures_precede_gate_then_no_success(
    monkeypatch, tmp_path
):
    """One failing + one clean pipeline → the failure delta is yielded, then raise.

    With exactly ONE failing unit the gate re-raises THAT error verbatim
    (LHP-VAL-007, not 902). Critically, the clean pipeline's SUCCESS delta is
    NEVER yielded: success deltas come from the commit step, which runs only
    after the gate passes — so a failure delta is observed, then the raise
    closes the stream before any success delta or write.
    """
    err = _val_error()  # LHP-VAL-007
    flowgroups_by_pipeline = {
        "pipe_bad": [_ctx("pipe_bad", "fg_bad")],
        "pipe_ok": [_ctx("pipe_ok", "fg_ok")],
    }
    outcomes_by_key = {
        ("pipe_bad", "fg_bad"): FlowgroupOutcome.failure(
            "pipe_bad", "fg_bad", lhp_error=err
        ),
        ("pipe_ok", "fg_ok"): _ok("pipe_ok", "fg_ok", "x = 1\n"),
    }
    env_dir = tmp_path / "generated"
    pipeline_dirs = {p: env_dir / p for p in flowgroups_by_pipeline}

    service, worker_state = _service_for(
        monkeypatch,
        flowgroups_by_pipeline=flowgroups_by_pipeline,
        outcomes_by_key=outcomes_by_key,
        pipeline_dirs=pipeline_dirs,
    )

    stream = service._iter_generate_deltas(
        flowgroups_by_pipeline=flowgroups_by_pipeline,
        worker_state=worker_state,
        validation_service=_BarrierSpy(),
        output_dir=env_dir,
        project_config=None,
        project_root=tmp_path,
        max_workers=4,
    )

    seen: List[PipelineDelta] = []
    with pytest.raises(LHPError) as excinfo:
        for delta in stream:
            seen.append(delta)

    assert [d.pipeline_name for d in seen] == ["pipe_bad"]
    assert excinfo.value is err
    assert excinfo.value.code == "LHP-VAL-007"
    assert not env_dir.exists()


@pytest.mark.unit
def test_run_generate_yields_success_deltas_in_input_order(monkeypatch, tmp_path):
    """``run_generate`` is itself the generator and yields deltas in input order.

    E3 removed the transitional callback-firing drain adapter
    (``_run_generate_engine_and_gate``); ``run_generate`` now ``yield from`` the
    source-of-truth :meth:`_iter_generate_deltas` directly. On an all-clean
    multi-pipeline run, draining ``run_generate`` observes the SUCCESS deltas in
    INPUT pipeline order — the stream the facade/orchestrator consume to render
    the §5.7 per-pipeline events.
    """
    flowgroups_by_pipeline = {
        "gamma": [_ctx("gamma", "fg_g")],
        "alpha": [_ctx("alpha", "fg_a")],
    }
    outcomes_by_key = {
        ("gamma", "fg_g"): _ok("gamma", "fg_g", "g = 1\n"),
        ("alpha", "fg_a"): _ok("alpha", "fg_a", "a = 1\n"),
    }
    env_dir = tmp_path / "generated"
    pipeline_dirs = {p: env_dir / p for p in flowgroups_by_pipeline}

    monkeypatch.setattr(fe, "ProcessPoolExecutor", _SyncExecutor)
    monkeypatch.setattr(
        fe, "_process_one_flowgroup", _gate_worker_factory(outcomes_by_key)
    )
    worker_state = _FlowgroupWorkerState(
        processor=_BarrierSpy(),
        substitution_managers=dict.fromkeys(flowgroups_by_pipeline, object()),
        include_tests=False,
        code_generator=_BarrierSpy(),
        pipeline_output_dirs=dict(pipeline_dirs),
        environment="dev",
    )
    service = PipelineExecutionService(
        max_workers=4,
        generate_worker_state=worker_state,
        validation_service=_BarrierSpy(),
    )

    deltas = list(
        service.run_generate(
            flowgroups_by_pipeline=flowgroups_by_pipeline,
            substitution_managers=dict.fromkeys(flowgroups_by_pipeline, object()),
            output_dirs=dict(pipeline_dirs),
            discovery_errors={},
            output_dir=env_dir,
            project_config=None,
            project_root=tmp_path,
            max_workers=4,
        )
    )

    assert [d.pipeline_name for d in deltas] == ["gamma", "alpha"]
    assert all(d.success for d in deltas)
