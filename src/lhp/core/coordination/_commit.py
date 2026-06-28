"""Post-gate write step: flush worker-produced artifacts to disk.

No resolve / codegen / validate / cross-flowgroup logic lives here — only
WRITES. The output-tree WIPE is the DRIVER's job (a single whole-env wipe
before the per-pipeline loop), running AFTER the gate passes so a failed
run leaves prior output untouched.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
)

from ...models.processing import FlowgroupOutcome, PipelineDelta
from ...utils.file_header import write_normalized
from ...utils.performance_timer import perf_timer
from ...utils.version import get_version
from ..codegen.python_file_copier import PythonFileCopier
from ..codegen.test_reporting import (
    build_test_reporting_hook_files,
    generate_test_reporting_hook,
)
from ..codegen.uc_tagging import (
    build_uc_tagging_hook_files,
    generate_uc_tagging_hook,
)
from ..packaging import PipelinePackager

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


def _generate_uc_tagging_hook(
    pipeline: str,
    resolved_flowgroups: List["FlowGroup"],
    *,
    output_dir: Optional[Path],
    project_config: Optional["ProjectConfig"],
    project_root: Path,
    substitution_mgr: Optional["EnhancedSubstitutionManager"],
) -> int:
    """Emit the per-pipeline UC tagging hook when tags are declared.

    Returns the artifact count written (0 when ``uc_tagging`` is disabled or no
    UC tags are declared). Dry-run (``output_dir is None``) emits nothing.
    """
    if output_dir is None:
        return 0
    return generate_uc_tagging_hook(
        pipeline_name=pipeline,
        flowgroups=resolved_flowgroups,
        output_dir=output_dir,
        project_config=project_config,
        project_root=project_root,
        substitution_mgr=substitution_mgr,
    )


def _split_hook_files(
    hook_files: Optional[Dict[str, str]],
) -> tuple[Dict[str, str], Dict[str, str]]:
    """Split the in-memory test-reporting hook files into packager buckets.

    The hook module (``_test_reporting_hook.py``) ships UNDER the flowgroup
    package (``extra_package_modules``); every ``test_reporting_providers/...``
    file ships at the wheel TOP-LEVEL (``extra_toplevel_files``) because the
    hook imports the providers package by its absolute top-level name. ``None``
    (hook not configured / no ``test_id``) yields two empty maps.
    """
    package_modules: Dict[str, str] = {}
    toplevel_files: Dict[str, str] = {}
    for relpath, content in (hook_files or {}).items():
        if relpath.startswith("test_reporting_providers/"):
            toplevel_files[relpath] = content
        else:
            package_modules[relpath] = content
    return package_modules, toplevel_files


def _commit_wheel_pipeline(
    pipeline: str,
    outcomes: Sequence[FlowgroupOutcome],
    *,
    output_dir: Path,
    project_config: Optional["ProjectConfig"],
    project_root: Path,
    include_tests: bool,
    env: str,
    substitution_mgr: Optional["EnhancedSubstitutionManager"],
) -> PipelineDelta:
    """Package ONE pipeline as a deterministic wheel; synthesize its delta.

    The wheel-mode counterpart to the source-mode body of
    :func:`commit_pipeline`. Reached only when ``packaging_mode == "wheel"``
    AND ``output_dir`` is not ``None``. The wheel carries the flowgroup
    modules, the copied ``custom_python_functions``, and the test-reporting
    hook, so the source-mode disk steps (``_write_python_files``,
    ``_apply_copy_records``, the disk ``_generate_test_hook``) are ALL skipped
    here — only the runner is written under the pipeline dir, and the ``.whl``
    under the env-root ``_wheels/<pipeline>/dist/`` staging tree.

    The test-reporting hook is built IN MEMORY (mirroring the exact arguments
    :func:`_generate_test_hook` passes to ``generate_test_reporting_hook``) and
    split into the packager's two member buckets.
    """
    resolved_flowgroups: List["FlowGroup"] = [
        o.resolved_flowgroup
        for o in outcomes
        if o.success and o.resolved_flowgroup is not None
    ]
    hook_files = build_test_reporting_hook_files(
        pipeline_name=pipeline,
        flowgroups=resolved_flowgroups,
        project_config=project_config,
        project_root=project_root,
        include_tests=include_tests,
        substitution_mgr=substitution_mgr,
    )
    extra_package_modules, extra_toplevel_files = _split_hook_files(hook_files)

    # The UC tagging hook is a single top-level module shipped under the
    # flowgroup package (no provider split), discovered by DLT like the
    # test-reporting hook.
    uc_tagging_hook_files = build_uc_tagging_hook_files(
        pipeline_name=pipeline,
        flowgroups=resolved_flowgroups,
        project_config=project_config,
        project_root=project_root,
        substitution_mgr=substitution_mgr,
    )
    if uc_tagging_hook_files:
        extra_package_modules.update(uc_tagging_hook_files)

    result = PipelinePackager().package(
        outcomes,
        output_dir=output_dir,
        pipeline=pipeline,
        env=env,
        version=get_version(),
        extra_package_modules=extra_package_modules,
        extra_toplevel_files=extra_toplevel_files,
    )

    wheel_path = (
        output_dir.parent / "_wheels" / pipeline / "dist" / result.wheel_filename
    )
    wheel_path.parent.mkdir(parents=True, exist_ok=True)
    wheel_path.write_bytes(result.wheel_bytes)

    write_normalized(output_dir / result.runner_filename, result.runner_code)

    logger.debug(
        f"Packaged wheel {result.wheel_filename} (hash {result.content_hash}) "
        f"for pipeline {pipeline}"
    )

    return PipelineDelta.success_(
        pipeline,
        files_written=1,
        generated_filenames=(result.runner_filename,),
        wheel_filename=result.wheel_filename,
    )


def commit_pipeline(
    pipeline: str,
    outcomes: Sequence[FlowgroupOutcome],
    *,
    output_dir: Optional[Path],
    project_config: Optional["ProjectConfig"],
    project_root: Path,
    include_tests: bool,
    env: str,
    packaging_mode: str = "source",
    substitution_mgr: Optional["EnhancedSubstitutionManager"] = None,
    copier_factory: Callable[[], PythonFileCopier] = PythonFileCopier,
) -> PipelineDelta:
    """Write ONE pipeline's gate-approved outputs to disk; synthesize a delta.

    Called once per pipeline on the clean (gate-passed) path only. The
    whole-env output wipe is the driver's responsibility and has already run
    once before this loop; this function re-creates the per-pipeline directory.
    ``copier_factory`` is injectable for tests; defaults to the real class.

    ``packaging_mode == "wheel"`` (with a real ``output_dir``) routes to
    :func:`_commit_wheel_pipeline`: the pipeline is packaged into a single
    deterministic ``.whl`` plus its runner instead of loose flowgroup files.
    ``packaging_mode == "source"`` (the default) and dry-run
    (``output_dir is None``) reproduce today's byte-identical source-mode output.
    """
    with perf_timer(f"commit_pipeline [{pipeline}]"):
        if output_dir is not None:
            output_dir.mkdir(parents=True, exist_ok=True)

        if packaging_mode == "wheel" and output_dir is not None:
            return _commit_wheel_pipeline(
                pipeline,
                outcomes,
                output_dir=output_dir,
                project_config=project_config,
                project_root=project_root,
                include_tests=include_tests,
                env=env,
                substitution_mgr=substitution_mgr,
            )

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
        artifacts_count += _generate_uc_tagging_hook(
            pipeline,
            resolved_flowgroups,
            output_dir=output_dir,
            project_config=project_config,
            project_root=project_root,
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
    env: str,
    packaging_modes: Optional[Mapping[str, str]] = None,
) -> Iterator[PipelineDelta]:
    """Commit every gate-approved pipeline to disk, YIELDING deltas in order.

    Lives here so the driver stays thin (constitution §3.3). Wipes the whole
    env output tree ONCE up front (lazily, when the first delta is pulled),
    then delegates each pipeline to :func:`commit_pipeline` in INPUT pipeline
    order. MUST be fully drained — the terminal ``ruff`` pass is the driver's
    responsibility and runs after this generator is exhausted. Runs ONLY after
    :func:`._generate_gate.gate_or_raise` confirmed every pipeline is clean.

    ``packaging_modes`` maps pipeline name -> ``"wheel"`` / ``"source"`` for
    the per-pipeline wheel packaging branch (a later task); ``None`` is
    normalized to an empty map so every pipeline resolves to ``"source"`` and
    output is byte-identical to today.
    """
    modes = packaging_modes or {}
    _wipe_env_output_dir(output_dir)
    for result in pool_results:
        pipeline = result.pipeline
        yield commit_pipeline(
            pipeline,
            result.outcomes_in_order,
            output_dir=pipeline_output_dirs.get(pipeline),
            project_config=project_config,
            project_root=project_root,
            include_tests=include_tests,
            env=env,
            packaging_mode=modes.get(pipeline, "source"),
            substitution_mgr=substitution_managers.get(pipeline),
        )
