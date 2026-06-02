"""Unit tests for the validate outcome-STREAM source of truth.

:meth:`PipelineExecutionService._iter_validate_outcomes` is the generator that
the validate path produces outcomes through, and (after E4)
:meth:`PipelineExecutionService.run_validate` IS that generator's thin wrapper:
it ``yield from`` ``_iter_validate_outcomes`` directly, mirroring what E3 did
for ``run_generate``. E4 removed the prior callback-firing drain adapter. These
tests pin the stream's two load-bearing properties:

1. outcomes iterate in DETERMINISTIC **input-pipeline order** — the order
   :func:`._pool._run_flowgroup_pool_core` returns its per-pipeline buckets
   (``list(flowgroups_by_pipeline.keys())``), preserved by
   :func:`._pool.assemble_validate_outcomes`, NOT flowgroup-completion order
   and NOT alphabetical;
2. ``run_validate`` is itself that generator — draining it yields one outcome
   per pipeline IN INPUT ORDER (no completion callback), the stream the
   facade/orchestrator now consume to render the §5.7 progress events.

Validate REPORTS findings and never raises on them, so — unlike the generate
stream — there is no gate/raise to interleave; the generator simply runs to
completion.

The spawn pool is removed the same way ``test_executor_validate`` /
``test_executor_generate_stream`` do it: a synchronous in-process executor
swapped onto the engine module (``_pool``) plus an in-process fake worker
returning pre-canned validate-mode :class:`FlowgroupOutcome`s. Everything
between the engine and the stream — the per-pipeline bucketing, the cross-fg
barrier, and the :func:`assemble_validate_outcomes` fold — is the real
production code, unmodified.
"""

from __future__ import annotations

from collections.abc import Iterator
from concurrent.futures import Future
from typing import List, Mapping, Optional, Sequence, Tuple

import pytest

from lhp.core._interfaces import CrossFlowgroupCheckResult
from lhp.core.coordination import _pool as fe
from lhp.core.coordination._flowgroup_pool import _FlowgroupWorkerState
from lhp.core.coordination.executor import PipelineExecutionService
from lhp.models import FlowGroupContext
from lhp.models.processing import FlowgroupOutcome


# Fakes
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

    Isolates the stream's ordering / fold wiring from the issue-building path
    (covered by ``test_executor_validate``), so every per-pipeline outcome is
    clean and successful and the assertions key purely on ORDER.
    """

    def validate_cross_flowgroup(
        self,
        flowgroups: Sequence[object],
        *,
        pipeline_filter: Optional[str] = None,
    ) -> CrossFlowgroupCheckResult:
        return CrossFlowgroupCheckResult()


def _validate_worker_factory(
    outcomes_by_key: Mapping[Tuple[str, str], FlowgroupOutcome],
):
    """Build a fake validate-mode worker returning a pre-canned outcome per fg."""

    def _fake_worker(fg_ctx: FlowGroupContext, *, mode: str) -> FlowgroupOutcome:
        assert mode == "validate"
        key = (fg_ctx.flowgroup.pipeline, fg_ctx.flowgroup.flowgroup)
        return outcomes_by_key[key]

    return _fake_worker


def _ok(pipeline: str, flowgroup: str) -> FlowgroupOutcome:
    """A clean validate-mode outcome carrying a resolved flowgroup (no code)."""
    return FlowgroupOutcome.ok(
        pipeline,
        flowgroup,
        resolved_flowgroup=_FakeFlowGroup(pipeline, flowgroup),
    )


def _service_for(
    monkeypatch,
    *,
    flowgroups_by_pipeline: Mapping[str, Sequence[FlowGroupContext]],
    outcomes_by_key: Mapping[Tuple[str, str], FlowgroupOutcome],
) -> Tuple[PipelineExecutionService, _FlowgroupWorkerState]:
    """Wire the sync-executor seam + fake worker; return (service, worker_state)."""
    monkeypatch.setattr(fe, "ProcessPoolExecutor", _SyncExecutor)
    monkeypatch.setattr(
        fe, "_process_one_flowgroup", _validate_worker_factory(outcomes_by_key)
    )
    worker_state = _FlowgroupWorkerState(
        processor=_BarrierSpy(),  # never called by the fake worker
        substitution_managers=dict.fromkeys(flowgroups_by_pipeline, object()),
        include_tests=False,
        code_generator=_BarrierSpy(),  # never called by the fake worker
        pipeline_output_dirs=dict.fromkeys(flowgroups_by_pipeline, None),
        environment="dev",
    )
    service = PipelineExecutionService(
        max_workers=4,
        validate_worker_state=worker_state,
        validation_service=_BarrierSpy(),
    )
    return service, worker_state


@pytest.mark.unit
def test_iter_validate_outcomes_in_input_pipeline_order(monkeypatch):
    """3-pipeline validate run → outcomes in INPUT order, not alphabetical.

    The insertion order ``["gamma", "alpha", "beta"]`` is deliberately NOT
    sorted: a stream that yielded in alphabetical order would diverge from
    this. The assertion that the outcome sequence equals the dict's key order
    proves the stream crystallizes in input-pipeline order (matching
    ``_run_flowgroup_pool_core``'s ``list(flowgroups_by_pipeline.keys())``,
    preserved by ``assemble_validate_outcomes``), the same order the legacy
    callback path observed.
    """
    flowgroups_by_pipeline = {
        "gamma": [_ctx("gamma", "fg_g")],
        "alpha": [_ctx("alpha", "fg_a")],
        "beta": [_ctx("beta", "fg_b")],
    }
    outcomes_by_key = {
        ("gamma", "fg_g"): _ok("gamma", "fg_g"),
        ("alpha", "fg_a"): _ok("alpha", "fg_a"),
        ("beta", "fg_b"): _ok("beta", "fg_b"),
    }

    service, worker_state = _service_for(
        monkeypatch,
        flowgroups_by_pipeline=flowgroups_by_pipeline,
        outcomes_by_key=outcomes_by_key,
    )

    outcomes = list(
        service._iter_validate_outcomes(
            flowgroups_by_pipeline=flowgroups_by_pipeline,
            worker_state=worker_state,
            validation_service=_BarrierSpy(),
            discovery_errors={},
        )
    )

    # Outcomes crystallize in INPUT pipeline order (NOT alphabetical/sorted).
    assert [o.pipeline for o in outcomes] == ["gamma", "alpha", "beta"]
    # The empty barrier + clean worker outcomes ⇒ every pipeline succeeds.
    assert all(o.success for o in outcomes)
    # A clean outcome carries no findings.
    assert all(o.issues == () for o in outcomes)


@pytest.mark.unit
def test_run_validate_is_the_outcome_generator_in_input_order(monkeypatch):
    """``run_validate`` IS the outcome generator and yields in INPUT order.

    E4 removed the transitional callback-firing drain adapter: ``run_validate``
    now ``yield from`` the source-of-truth ``_iter_validate_outcomes`` directly
    (mirroring what E3 did for ``run_generate``). On a multi-pipeline run it
    yields one outcome per pipeline IN INPUT pipeline order — the stream the
    facade/orchestrator now consume to render the §5.7 progress events. No
    completion callback is involved any more.

    The insertion order ``["gamma", "alpha"]`` is again deliberately unsorted
    so a coincidental alphabetical ordering cannot make this pass.
    """
    flowgroups_by_pipeline = {
        "gamma": [_ctx("gamma", "fg_g")],
        "alpha": [_ctx("alpha", "fg_a")],
    }
    outcomes_by_key = {
        ("gamma", "fg_g"): _ok("gamma", "fg_g"),
        ("alpha", "fg_a"): _ok("alpha", "fg_a"),
    }

    service, _worker_state = _service_for(
        monkeypatch,
        flowgroups_by_pipeline=flowgroups_by_pipeline,
        outcomes_by_key=outcomes_by_key,
    )

    # ``run_validate`` is a generator (lazy) — draining it yields outcomes in
    # INPUT pipeline order, the prior contract the facade consumes.
    outcomes_iter = service.run_validate(
        flowgroups_by_pipeline=flowgroups_by_pipeline,
        substitution_managers=dict.fromkeys(flowgroups_by_pipeline, object()),
        output_dirs=dict.fromkeys(flowgroups_by_pipeline, None),
        discovery_errors={},
    )
    assert isinstance(outcomes_iter, Iterator)
    outcomes = list(outcomes_iter)

    assert [o.pipeline for o in outcomes] == ["gamma", "alpha"]
    assert all(o.success for o in outcomes)


@pytest.mark.unit
def test_iter_validate_outcomes_reports_discovery_errors_in_order(monkeypatch):
    """A discovery-failed pipeline is REPORTED (not raised), still in input order.

    Validate never raises on findings: a pipeline in ``discovery_errors`` folds
    to a single-error, unsuccessful outcome, while the clean pipeline yields a
    successful one — and both appear in input-pipeline order. This pins that the
    stream runs to completion (no gate/raise) even with a failing pipeline,
    distinguishing validate from the generate stream's failures-then-raise.
    """
    flowgroups_by_pipeline = {
        "pipe_ok": [_ctx("pipe_ok", "fg_ok")],
        "pipe_missing": [],
    }
    outcomes_by_key = {
        ("pipe_ok", "fg_ok"): _ok("pipe_ok", "fg_ok"),
    }

    service, worker_state = _service_for(
        monkeypatch,
        flowgroups_by_pipeline=flowgroups_by_pipeline,
        outcomes_by_key=outcomes_by_key,
    )

    outcomes = list(
        service._iter_validate_outcomes(
            flowgroups_by_pipeline=flowgroups_by_pipeline,
            worker_state=worker_state,
            validation_service=_BarrierSpy(),
            discovery_errors={"pipe_missing": "discovery blew up for pipe_missing"},
        )
    )

    # Both pipelines reported, in INPUT order — no raise short-circuits this.
    assert [o.pipeline for o in outcomes] == ["pipe_ok", "pipe_missing"]
    assert outcomes[0].success is True
    # The discovery failure folds to a single-issue, unsuccessful outcome — a
    # string finding with no owning flowgroup (discovery has no flowgroup).
    assert outcomes[1].success is False
    assert len(outcomes[1].issues) == 1
    record = outcomes[1].issues[0]
    assert record.issue == "discovery blew up for pipe_missing"
    assert record.severity == "error"
    assert record.flowgroup_name is None
    assert record.source_file is None
