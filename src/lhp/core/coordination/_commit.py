"""Commit one pipeline's generate outputs to disk (Phase-B write step).

Single responsibility: **write one pipeline's committed outputs to disk**.

This module is the post-gate write half of the consolidated generate path.
The all-or-nothing
gate (:func:`._generate_gate.gate_or_raise`) has already proven that EVERY
pipeline's flowgroups resolved, codegen'd, and formatted cleanly and that no
cross-source copy collides; only then does the driver in
:class:`~lhp.core.coordination.executor.PipelineExecutionService` call
:func:`commit_pipeline` once per pipeline to flush the already-computed
artifacts.

The write ORDER, filename scheme (``{flowgroup_name}.py``), encoding (UTF-8
via :func:`~lhp.utils.file_header.write_normalized`), empty-flowgroup unlink
rule, auxiliary-file handling, one-:class:`PythonFileCopier`-per-pipeline
dedup, and the synthesized :class:`~lhp.models.processing.PipelineDelta` field
values must be byte-for-byte identical to the legacy
:class:`~lhp.core.coordination.processor.PipelineProcessor` path — the E2E
baselines must not move.

It is deliberately NOT a second processor: there is NO
resolve / codegen / validate / cross-flowgroup logic in here. It only WRITES
what the worker pool already produced. The output-tree WIPE is the DRIVER's
job (a single whole-env wipe before the per-pipeline loop), not this
function's — and runs AFTER the gate passes, so a failed run leaves prior
output untouched.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Callable, List, Mapping, Optional, Sequence

from ...models.processing import FlowgroupOutcome, PipelineDelta
from ...utils.file_header import write_normalized
from ...utils.performance_timer import perf_timer
from ..codegen.python_file_copier import PythonFileCopier
from ..codegen.test_reporting import generate_test_reporting_hook

if TYPE_CHECKING:
    from lhp.models import FlowGroup, ProjectConfig

    from ..processing.substitution import EnhancedSubstitutionManager
    from ._pool import _PipelinePoolResult

logger = logging.getLogger(__name__)


def _collect_generated_filenames(
    outcomes: Sequence[FlowgroupOutcome],
) -> tuple[str, ...]:
    """Build the ordered ``aggregate_generated_filenames`` tuple.

    Filenames are bare flowgroup-derived (``{flowgroup_name}.py``). Empty
    flowgroups (whose ``formatted_code`` is blank) are skipped. Dry-run
    (``output_dir is None``) still populates the tuple for "would generate"
    listings.

    Insertion order is preserved — ``outcomes`` is the engine's
    ``outcomes_in_order`` (sorted by flowgroup name) — so dry-run
    output is deterministic.
    """
    out: List[str] = []
    for outcome in outcomes:
        if not outcome.success:
            continue
        if not (outcome.formatted_code or "").strip():
            continue
        out.append(f"{outcome.flowgroup_name}.py")
    return tuple(out)


def _apply_copy_records(
    outcomes: Sequence[FlowgroupOutcome], copier: PythonFileCopier
) -> None:
    """Replay each successful outcome's copy records through one copier.

    The lock inside ``copier`` deduplicates intra-pipeline copies (an
    identical source re-claiming the same destination is a no-op).
    Cross-source conflicts cannot reach here: the gate's dry pass
    (:meth:`PythonFileCopier.plan`) already raised ``LHP-VAL-019`` across all
    pipelines before commit.
    """
    for outcome in outcomes:
        if not outcome.success or not outcome.copy_records:
            continue
        for record in outcome.copy_records:
            copier.apply_copy_record(record)


def _write_python_files(
    outcomes: Sequence[FlowgroupOutcome], output_dir: Optional[Path]
) -> None:
    """Write the per-flowgroup ``.py`` files and any auxiliary files.

    Empty flowgroups (blank ``formatted_code``) have their output file
    removed (idempotent ``unlink(missing_ok=True)``). Dry-run
    (``output_dir is None``) skips disk writes and only logs intent.

    The empty-flowgroup log line reads ``outcome.flowgroup_name`` (always
    present) instead of the resolved FlowGroup's ``.flowgroup`` so the
    resolved-FlowGroup release cannot affect it.
    """
    if output_dir is None:
        for outcome in outcomes:
            if outcome.success:
                logger.info(f"Would generate: {outcome.flowgroup_name}.py")
        return

    for outcome in outcomes:
        if not outcome.success:
            continue
        filename = f"{outcome.flowgroup_name}.py"
        output_file = output_dir / filename
        formatted_code = outcome.formatted_code or ""

        if not formatted_code.strip():
            try:
                output_file.unlink(missing_ok=True)
            except OSError as exc:
                logger.exception(
                    "Failed to delete empty flowgroup file %s: %s",
                    output_file,
                    type(exc).__name__,
                )
                raise
            logger.info(
                f"Skipping empty flowgroup: {outcome.flowgroup_name} (no content)"
            )
            continue

        with perf_timer(
            f"write_file [{outcome.flowgroup_name}]", category="write_file"
        ):
            write_normalized(output_file, formatted_code)

        if outcome.auxiliary_files:
            for aux_name, aux_content in outcome.auxiliary_files:
                aux_file = output_dir / aux_name
                write_normalized(aux_file, aux_content)
                logger.info(f"Generated auxiliary: {aux_file}")


def _generate_test_hook(
    pipeline: str,
    resolved_flowgroups: List["FlowGroup"],
    *,
    output_dir: Optional[Path],
    project_config: Optional["ProjectConfig"],
    project_root: Path,
    include_tests: bool,
    substitution_mgr: Optional["EnhancedSubstitutionManager"],
) -> int:
    """Emit the per-pipeline test-reporting hook when configured.

    Returns the artifact count the hook wrote (0 when not configured or no
    flowgroup carried a ``test_id``). Dry-run (``output_dir is None``) emits
    nothing.

    ``resolved_flowgroups`` is the list of non-``None``
    ``FlowgroupOutcome.resolved_flowgroup``s. Those are
    RETAINED only when ``include_tests AND project_config.test_reporting`` —
    exactly the condition under which this hook does real work — so when the
    hook is active the list is fully populated, and when the resolved
    flowgroups were released the hook's own guards short-circuit before
    walking the (then-empty) list.
    """
    if output_dir is None:
        return 0
    return generate_test_reporting_hook(
        pipeline_name=pipeline,
        flowgroups=resolved_flowgroups,
        output_dir=output_dir,
        project_config=project_config,
        project_root=project_root,
        include_tests=include_tests,
        substitution_mgr=substitution_mgr,
    )


def commit_pipeline(
    pipeline: str,
    outcomes: Sequence[FlowgroupOutcome],
    *,
    output_dir: Optional[Path],
    project_config: Optional["ProjectConfig"],
    project_root: Path,
    include_tests: bool,
    substitution_mgr: Optional["EnhancedSubstitutionManager"] = None,
    copier_factory: Callable[[], PythonFileCopier] = PythonFileCopier,
) -> PipelineDelta:
    """Write ONE pipeline's gate-approved outputs to disk; synthesize a delta.

    Called once per pipeline by the generate driver
    (:meth:`PipelineExecutionService._run_generate_engine_and_gate`) ONLY on
    the clean (gate-passed) path. The whole-env output wipe is the driver's
    responsibility and has already run once before this loop; this function
    re-creates the per-pipeline directory (``mkdir(parents=True,
    exist_ok=True)``) and then writes:

    1. Apply each successful outcome's copy records through ONE
       :class:`PythonFileCopier` (intra-pipeline dedup).
    2. Write ``{flowgroup_name}.py`` from ``formatted_code`` (empty → unlink),
       then any auxiliary files.
    3. Emit the test-reporting hook (when ``include_tests`` and
       ``project_config.test_reporting`` are set).
    4. Return a :class:`PipelineDelta.success_` whose ``files_written`` /
       ``artifacts_count`` / ``generated_filenames`` must match the legacy
       path byte-for-byte. ``duration_s`` stays ``0.0``.

    Args:
        pipeline: Pipeline name being committed.
        outcomes: This pipeline's ``outcomes_in_order`` (sorted by flowgroup
            name). ALL are successful on the gate-passed path; the
            success guard is defensive only.
        output_dir: Per-pipeline output directory (``generated/<env>/<pipeline>``)
            or ``None`` for dry-run (no writes, no hook).
        project_config: Project config; ``project_config.test_reporting``
            decides whether the hook is emitted.
        project_root: Project root (forwarded to the hook generator).
        include_tests: Whether test actions were emitted; gates the hook.
        substitution_mgr: Per-pipeline substitution manager (hook provider copy).
        copier_factory: Builds the per-pipeline :class:`PythonFileCopier`.
            Injectable for tests; defaults to the real class.

    Returns:
        A success :class:`PipelineDelta` for this pipeline.
    """
    with perf_timer(f"commit_pipeline [{pipeline}]"):
        if output_dir is not None:
            output_dir.mkdir(parents=True, exist_ok=True)

        copier = copier_factory()
        _apply_copy_records(outcomes, copier)
        _write_python_files(outcomes, output_dir)

        resolved_flowgroups: List["FlowGroup"] = [
            o.resolved_flowgroup
            for o in outcomes
            if o.success and o.resolved_flowgroup is not None
        ]
        artifacts_count = _generate_test_hook(
            pipeline,
            resolved_flowgroups,
            output_dir=output_dir,
            project_config=project_config,
            project_root=project_root,
            include_tests=include_tests,
            substitution_mgr=substitution_mgr,
        )

    generated_filenames = _collect_generated_filenames(outcomes)
    return PipelineDelta.success_(
        pipeline,
        files_written=len(generated_filenames),
        artifacts_count=artifacts_count,
        generated_filenames=generated_filenames,
    )


def _wipe_env_output_dir(output_dir: Optional[Path]) -> None:
    """Wipe and recreate the env-specific generated output directory.

    Scope is strictly ``generated/<env>``: removed if it exists and recreated
    empty (the full-regenerate contract). Runs AFTER the all-or-nothing gate
    passes, so a failed run leaves prior output untouched. No-op when
    ``output_dir`` is ``None`` (dry run: neither wipe nor write). Runs ONCE
    before the per-pipeline commit loop; each pipeline's own ``mkdir`` in
    :func:`commit_pipeline` then re-creates its subdir.
    """
    import shutil

    if output_dir is None:
        return
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)


def commit_generate_results(
    pool_results: Sequence["_PipelinePoolResult"],
    *,
    pipeline_output_dirs: Mapping[str, Optional[Path]],
    substitution_managers: Mapping[str, Optional["EnhancedSubstitutionManager"]],
    include_tests: bool,
    output_dir: Optional[Path],
    project_config: Optional["ProjectConfig"],
    project_root: Path,
    on_pipeline_complete: Optional[Callable[[PipelineDelta], None]] = None,
) -> tuple[PipelineDelta, ...]:
    """Commit every gate-approved pipeline to disk, in INPUT pipeline order.

    The per-pipeline commit ORCHESTRATION half of the generate write step
    (the single-pipeline mechanics are :func:`commit_pipeline`). Lives here,
    not on the executor, so the driver
    (:meth:`PipelineExecutionService._run_generate_engine_and_gate`) stays
    thin and the commit mechanics are single-sourced in this module
    (constitution §3.3 size).

    Runs ONLY after :func:`._generate_gate.gate_or_raise` confirmed every
    pipeline is clean. Wipes the whole env output tree ONCE up front
    (:func:`_wipe_env_output_dir` — writes happen only on the gate-passed
    path), then delegates each pipeline to :func:`commit_pipeline` in input
    order, firing ``on_pipeline_complete`` (when supplied) with each
    synthesized success delta.

    Args:
        pool_results: The clean per-pipeline results, in input pipeline order.
        pipeline_output_dirs: Per-pipeline output dir map (``generated/<env>/
            <pipeline>``), keyed by pipeline name. Missing / ``None`` → dry-run
            for that pipeline.
        substitution_managers: Per-pipeline substitution manager map.
        include_tests: Whether test actions were emitted (gates the hook).
        output_dir: Env-level output dir (``generated/<env>``) for the single
            whole-env wipe. ``None`` for dry-run (no wipe).
        project_config: Project config; ``test_reporting`` gates the hook.
        project_root: Project root (forwarded to the hook generator).
        on_pipeline_complete: Optional per-pipeline success-delta callback.

    Returns:
        One success :class:`PipelineDelta` per pipeline, in input order.
    """
    _wipe_env_output_dir(output_dir)
    deltas: List[PipelineDelta] = []
    for result in pool_results:
        pipeline = result.pipeline
        delta = commit_pipeline(
            pipeline,
            result.outcomes_in_order,
            output_dir=pipeline_output_dirs.get(pipeline),
            project_config=project_config,
            project_root=project_root,
            include_tests=include_tests,
            substitution_mgr=substitution_managers.get(pipeline),
        )
        deltas.append(delta)
        if on_pipeline_complete is not None:
            on_pipeline_complete(delta)
    return tuple(deltas)
