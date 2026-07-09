"""Cold-discovery parallel parse pool (package-private).

Fans out the per-file YAML parse+validate of persistent-parse-cache MISSES
to a ``ProcessPoolExecutor`` so a large cold discovery does not pay the
serial parse cost. Consumed only by
:meth:`~lhp.core.discovery.flowgroup_discoverer.FlowgroupDiscoveryService.discover_all_flowgroups_with_paths`;
warm runs (no misses / below threshold) and ``max_workers == 1`` never
construct a pool.

Fatal-error semantics: the parent does NOT transport or re-raise worker
errors. A file whose parse fails yields a ``failed``
:class:`ParsedFileOutcome`, which the discoverer simply does NOT seed. The
unchanged serial loop then re-parses that file at its deterministic sorted
position and raises the ORIGINAL :class:`~lhp.errors.LHPError` in-process —
byte-identical behavior to a fully serial run. ``failure_summary`` is
diagnostic-only (a debug log line), never surfaced to the user.

Pickle / spawn invariants (same contract as
:mod:`lhp.core.coordination._flowgroup_pool`):

- Worker entry points are module-level functions referenced by import path
  across the ``spawn`` boundary (pickle-by-name; no closures cross).
  Renaming :func:`_init_parse_worker` / :func:`_parse_file_batch` breaks
  workers in flight.
- Workers NEVER raise (constitution §5.6 / §9.6): every exception is wrapped
  into a failed :class:`ParsedFileOutcome` on the DTO channel.

:stability: internal
"""

from __future__ import annotations

import logging
import math
import multiprocessing
import sys
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from lhp.models import FlowGroup

from ...parsers.yaml_loader import load_yaml_documents_all
from ...parsers.yaml_parser import YAMLParser
from ...utils.performance_timer import perf_timer

logger = logging.getLogger(__name__)

#: Minimum number of cache-miss files before a pool is worth its spawn cost.
#: Module-level (not a function default) so tests can monkeypatch it.
MIN_FILES_FOR_PARALLEL_PARSE = 500

#: Sizing input: one worker per this many miss files, before the
#: ``max_workers`` cap. Module-level so tests can monkeypatch it.
FILES_PER_WORKER = 250


@dataclass(frozen=True)
class ParsedFileOutcome:
    """Result of parsing ONE YAML file in a spawn worker (internal DTO).

    Must pickle round-trip across the spawn boundary. ``documents`` /
    ``flowgroups`` are meaningful only when ``failed`` is ``False``. A
    blueprint-instance file yields an EMPTY ``flowgroups`` tuple with
    non-empty ``documents`` — still a success, and still seedable (the
    instance pass reads the cached documents).
    """

    path: Path
    documents: Tuple[Dict[str, Any], ...]
    flowgroups: Tuple[FlowGroup, ...]
    failed: bool = False
    failure_summary: Optional[str] = None


def _failed_outcome(path: Path, exc: BaseException) -> ParsedFileOutcome:
    """Total constructor for the failure shape (never raises)."""
    return ParsedFileOutcome(
        path=path,
        documents=(),
        flowgroups=(),
        failed=True,
        failure_summary=f"{type(exc).__name__}: {exc}",
    )


def _init_parse_worker(level: int) -> None:
    """Pool initializer: silence worker stdlib loggers entirely.

    Mirrors :func:`lhp.core.coordination._flowgroup_pool._init_worker_logger`
    (duplicated inline rather than imported so ``core/discovery`` does not
    grow an upward edge into ``core/coordination``). Workers MUST NOT write
    to OS stderr; all diagnostics ride back on the DTO.
    """
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    root.addHandler(logging.NullHandler())
    root.setLevel(level)


def _parse_file_batch(paths: Sequence[Path]) -> Tuple[ParsedFileOutcome, ...]:
    """Worker entry: parse each path exactly as a fresh serial parse would.

    Mirrors ``CachingYAMLParser._parse_and_persist``'s fresh-parse pair —
    :func:`load_yaml_documents_all` (same ``error_context`` string) then
    ``YAMLParser._flowgroups_from_documents`` — so a seeded result is
    indistinguishable from a serially parsed one. NEVER raises: any
    per-file exception becomes a failed outcome; the batch always returns
    one outcome per input path.
    """
    parser = YAMLParser()
    outcomes: List[ParsedFileOutcome] = []
    for path in paths:
        try:
            documents = load_yaml_documents_all(
                path, error_context=f"flowgroup file {path}"
            )
            flowgroups = parser._flowgroups_from_documents(documents, path)
        except Exception as exc:
            outcomes.append(_failed_outcome(path, exc))
        else:
            outcomes.append(
                ParsedFileOutcome(
                    path=path,
                    documents=tuple(documents),
                    flowgroups=tuple(flowgroups),
                )
            )
    return tuple(outcomes)


def run_parse_pool(paths: Sequence[Path], max_workers: int) -> List[ParsedFileOutcome]:
    """Fan out parsing of ``paths`` across a spawn ``ProcessPoolExecutor``.

    Sizing: ``min(max(1, max_workers), max(1, len(paths) // FILES_PER_WORKER))``
    — one worker per :data:`FILES_PER_WORKER` files, capped by the caller's
    budget. Paths are chunked into batches of ``~len(paths) / (workers * 4)``
    so stragglers rebalance across workers.

    Returns one outcome per path, in NO guaranteed order (the discoverer's
    serial loop, not this pool, owns result ordering). Never raises: a
    ``submit`` failure or a pool-level result failure (worker death, result
    unpickling) degrades that batch to failed outcomes on the DTO channel.
    """
    n_files = len(paths)
    if n_files == 0:
        return []
    workers = min(max(1, max_workers), max(1, n_files // FILES_PER_WORKER))
    if sys.platform == "win32":
        # ProcessPoolExecutor raises ValueError above 61 workers on Windows
        # (WaitForMultipleObjects handle limit) — and this constructor sits
        # outside the never-raise guards. Same clamp in
        # core/coordination/_pool._run_flowgroup_pool_core.
        workers = min(workers, 61)
    batch_size = max(1, math.ceil(n_files / (workers * 4)))
    batches: List[List[Path]] = [
        list(paths[i : i + batch_size]) for i in range(0, n_files, batch_size)
    ]

    ctx_mp = multiprocessing.get_context("spawn")
    parent_level = logging.getLogger().level
    outcomes: List[ParsedFileOutcome] = []
    with (
        perf_timer(f"parse_pool [{n_files} files, {workers} workers]"),
        ProcessPoolExecutor(
            max_workers=workers,
            mp_context=ctx_mp,
            initializer=_init_parse_worker,
            initargs=(parent_level,),
        ) as executor,
    ):
        future_to_batch: Dict[Future, List[Path]] = {}
        for batch in batches:
            # Mirror the coordination pools' submit guard: a pickle /
            # post-shutdown failure becomes failed outcomes, never an
            # escaped exception.
            try:
                fut: Future = executor.submit(_parse_file_batch, batch)
            except Exception as submit_exc:
                logger.warning(
                    f"executor.submit raised for a parse batch of "
                    f"{len(batch)} file(s): {submit_exc}"
                )
                outcomes.extend(_failed_outcome(p, submit_exc) for p in batch)
                continue
            future_to_batch[fut] = batch

        for fut in as_completed(future_to_batch):
            batch = future_to_batch[fut]
            try:
                outcomes.extend(fut.result())
            except Exception as exc:
                # _parse_file_batch never raises; this guards a pool-level
                # failure (worker death, result unpickling). Stay on the
                # DTO channel — the serial loop re-parses these files.
                logger.warning(
                    f"Parse pool future failed for a batch of "
                    f"{len(batch)} file(s): {exc}"
                )
                outcomes.extend(_failed_outcome(p, exc) for p in batch)
    return outcomes
