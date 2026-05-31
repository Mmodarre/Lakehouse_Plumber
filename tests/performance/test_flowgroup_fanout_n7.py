"""N7: flowgroup-granular fan-out
spreads a single large pipeline's flowgroups across worker processes.

Before Stage 2, ``lhp generate`` parallelized at PIPELINE granularity (one
``ProcessPoolExecutor`` future per pipeline; a worker drained that pipeline's
flowgroups sequentially). On an asymmetric project — one big pipeline plus many
tiny ones — the big pipeline's flowgroups were pinned to a single core (a
straggler), so worker utilization was bounded by *pipeline* count.

Stage 2's :func:`lhp.core.coordination._pool._run_flowgroup_pool_core`
makes the FLOWGROUP the unit of parallelism (one future per flowgroup, pool
capped at ``min(max_workers, flowgroup_count)``). The claim this test pins:
**worker utilization tracks flowgroup count, not pipeline count** — so one
large pipeline's flowgroups fan out across all available workers.

Method (robust under ``mp_context="spawn"``):
  - Drive the REAL engine (real cap logic, real spawn boundary) with a
    synthetic asymmetric worklist: 1 big pipeline (``_BIG_FG`` flowgroups) +
    N small pipelines (1 flowgroup each).
  - Inject a picklable fake ``processor`` whose ``process_flowgroup`` (the
    first thing the real worker calls — see ``_flowgroup_pool._process_one_flowgroup``)
    records ``os.getpid()`` to a per-flowgroup file. ``mode="validate"`` stops
    the worker right after that call, so no codegen collaborators are needed.
  - Assert the BIG pipeline was processed by >1 distinct PID under
    ``max_workers >= 2`` — i.e. flowgroup count, not pipeline count (=1 for the
    big pipeline), is the bound. At ``max_workers=1`` it must be exactly 1 PID
    (the sequential control).

This is a LOCAL, non-blocking perf check; it carries the ``performance`` marker
so it is skipped on normal ``pytest`` / CI runs (see ``conftest.py``). Run with::

    pytest tests/performance/test_flowgroup_fanout_n7.py -m performance -v
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

from lhp.core._interfaces import CrossFlowgroupCheckResult
from lhp.core.coordination._flowgroup_pool import _FlowgroupWorkerState
from lhp.core.coordination._pool import _run_flowgroup_pool_core
from lhp.models import Action, ActionType, FlowGroup, FlowGroupContext

# Sink dir is passed to spawned workers via the environment (the worker-state
# is picklable, but a plain env var is the simplest cross-spawn channel for a
# scratch path). One file per flowgroup => single writer, no append contention.
_PID_DIR_ENV = "LHP_N7_PID_DIR"

_BIG_PIPELINE = "big_pipeline"
_BIG_FG = 200  # flowgroups in the single large pipeline
_SMALL_N = 8  # number of 1-flowgroup pipelines alongside it


class _PidRecordingProcessor:
    """Picklable stand-in for the leaf ``FlowgroupResolutionService``.

    The real worker (``_process_one_flowgroup``) calls
    ``state.processor.process_flowgroup(ctx, sub_mgr, include_tests=...)``
    first; we record the worker PID per flowgroup and return ``ctx``
    unchanged. In ``validate`` mode the worker returns right after this call,
    so the (unused) codegen/formatter collaborators may be ``None``.
    """

    def process_flowgroup(self, ctx, substitution_mgr, include_tests=True):
        fg = ctx.flowgroup
        pid_dir = Path(os.environ[_PID_DIR_ENV])
        (pid_dir / f"{fg.pipeline}__{fg.flowgroup}.pid").write_text(str(os.getpid()))
        return ctx

    def resolve(self, ctx, substitution_mgr, *, include_tests=True):
        # Mirror the ABC method for completeness; the worker uses
        # process_flowgroup, but resolve delegates to it on the real class.
        return self.process_flowgroup(ctx, substitution_mgr, include_tests)


class _BenignValidationService:
    """Cross-flowgroup barrier runs on the COORDINATOR (parent process), not in
    workers, so this never needs to be picklable into a worker. Returns no
    cross-flowgroup issues."""

    def validate_cross_flowgroup(self, resolved, *, pipeline_filter=None):
        return CrossFlowgroupCheckResult()


def _fg(pipeline: str, name: str) -> FlowGroupContext:
    fg = FlowGroup(
        pipeline=pipeline,
        flowgroup=name,
        actions=[Action(name=f"load_{name}", type=ActionType.LOAD, target=f"v_{name}")],
    )
    return FlowGroupContext(flowgroup=fg, source_yaml=None)


def _run_engine(max_workers: int) -> tuple[set[int], set[int], int]:
    """Drive the real flat engine over the asymmetric worklist; return
    ``(distinct PIDs on the big pipeline, distinct PIDs overall, #pipeline
    results)``."""
    flowgroups_by_pipeline: dict = {
        _BIG_PIPELINE: [_fg(_BIG_PIPELINE, f"big_fg_{i:03d}") for i in range(_BIG_FG)],
    }
    for j in range(_SMALL_N):
        p = f"small_pipeline_{j:02d}"
        flowgroups_by_pipeline[p] = [_fg(p, f"small_fg_{j:02d}")]

    with tempfile.TemporaryDirectory() as td:
        prev = os.environ.get(_PID_DIR_ENV)
        os.environ[_PID_DIR_ENV] = td
        try:
            state = _FlowgroupWorkerState(
                processor=_PidRecordingProcessor(),
                # Looked up by pipeline name inside the real worker, but our
                # fake processor ignores it, so None values are fine.
                substitution_managers={p: None for p in flowgroups_by_pipeline},
                include_tests=False,
                code_generator=None,  # unused in validate mode
                formatter=None,  # unused in validate mode
                pipeline_output_dirs={p: None for p in flowgroups_by_pipeline},
                environment="dev",
            )
            results = _run_flowgroup_pool_core(
                flowgroups_by_pipeline=flowgroups_by_pipeline,
                worker_state=state,
                validation_service=_BenignValidationService(),
                max_workers=max_workers,
                mode="validate",
            )
            big_pids: set[int] = set()
            all_pids: set[int] = set()
            for f in Path(td).glob("*.pid"):
                pid = int(f.read_text())
                all_pids.add(pid)
                if f.name.startswith(f"{_BIG_PIPELINE}__"):
                    big_pids.add(pid)
            return big_pids, all_pids, len(results)
        finally:
            if prev is None:
                os.environ.pop(_PID_DIR_ENV, None)
            else:
                os.environ[_PID_DIR_ENV] = prev


@pytest.mark.performance
def test_single_worker_is_the_sequential_control() -> None:
    """max_workers=1 ⇒ every flowgroup runs in the one worker process."""
    if (os.cpu_count() or 1) < 2:
        pytest.skip("needs >=2 logical CPUs to be meaningful")
    big_pids, all_pids, n_results = _run_engine(max_workers=1)
    assert n_results == 1 + _SMALL_N
    assert len(big_pids) == 1, f"mw=1 control: expected 1 PID, got {len(big_pids)}"
    assert len(all_pids) == 1


@pytest.mark.performance
def test_big_pipeline_fans_out_across_workers() -> None:
    """N7: the single large pipeline's flowgroups spread across >1 worker PID.

    The pre-Stage-2 pipeline-granular pool would pin all ``_BIG_FG`` flowgroups
    of ``_BIG_PIPELINE`` to one worker (1 future for the pipeline). Stage-2's
    flowgroup-granular pool fans them out; the bound is flowgroup count
    (``_BIG_FG + _SMALL_N``), not pipeline count (``1 + _SMALL_N``).
    """
    cpus = os.cpu_count() or 1
    if cpus < 2:
        pytest.skip("needs >=2 logical CPUs to demonstrate fan-out")
    workers = min(8, cpus)
    big_pids, all_pids, n_results = _run_engine(max_workers=workers)
    assert n_results == 1 + _SMALL_N
    # The core N7 claim: ONE pipeline's flowgroups used MORE THAN ONE worker.
    assert len(big_pids) > 1, (
        f"N7 violated: big pipeline's {_BIG_FG} flowgroups ran on "
        f"{len(big_pids)} PID(s) at max_workers={workers}; expected >1 "
        "(flowgroup-granular fan-out should spread one pipeline across cores)."
    )
    # Utilization tracks the worker cap (= min(max_workers, flowgroup_count)),
    # not pipeline count. With far more flowgroups than workers, all workers
    # should see the big pipeline.
    assert len(big_pids) <= workers
