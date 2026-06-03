"""Build an in-memory generation plan that is byte-for-byte a real generate.

Single responsibility: **produce the exact file tree a real ``generate`` would
write to ``generated/<env>``, without touching the real output directory.**

The parity-guaranteed primitive behind the public generation *plan* surface
(``lhp.api.GenerationPlan`` — assembled by the API layer from the core
structure this module returns; ``core`` must not import ``lhp.api``, §5.x
layering). It works by driving the *real* generate flow against a throwaway
:class:`tempfile.TemporaryDirectory` instead of ``generated/<env>``:

    drive ``ActionOrchestrator.generate_pipelines(output_dir=<temp>, ...)``
      → the engine pool codegens every flowgroup (mode=``generate``)
      → the all-or-nothing gate raises on any failure (no writes)
      → :func:`~lhp.core.coordination._commit.commit_generate_results` writes
        every committed pipeline's files into ``<temp>/<pipeline>/``
      → the single terminal :func:`~lhp.core.codegen.formatter
        .format_generated_tree` ruff pass formats the whole ``<temp>`` tree

then READ the formatted ``<temp>`` tree back into an ordered collection of
:class:`PlannedArtifact`\\ s (path *relative to the temp root*, so it matches
the real ``generated/<env>`` layout) and DISCARD the temp dir.

Why drive the real orchestrator rather than re-run the pool here: reusing the
unmodified generate driver — same pool, same gate, same
``commit_generate_results`` writer, same terminal ``format_generated_tree``
pass — is exactly what guarantees the plan is byte-for-byte identical to a real
generate. Re-implementing the commit/gate/pool drive in this module would both
duplicate the coordination logic and reach the engine internals
(``_run_flowgroup_pool_core`` / ``gate_or_raise``) the coordination package
keeps private (§4.6). The only difference between a plan and a real generate is
the ``output_dir`` (a temp dir) and that monitoring *finalization* (notebook +
job-resource writes, which land in the project root, NOT under
``generated/<env>``) is deliberately NOT run — a plan must not mutate the real
project tree. The synthetic monitoring *pipeline* file
(``<monitoring_pipeline>/monitoring.py``) IS produced, because it is a normal
flowgroup discovered by ``discover_all_flowgroups`` and committed under
``output_dir`` like any other flowgroup.

Artifact-kind classification (``PlannedArtifact.kind``) is derived from where
each file lands in the per-pipeline output tree — the deterministic landing
scheme of the commit step — plus the per-pipeline flowgroup filename set the
drive reports through its ``PipelineDelta`` stream. See
:func:`_classify_artifact`.
"""

from __future__ import annotations

import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
)

if TYPE_CHECKING:
    from lhp.models import FlowGroup
    from lhp.models.processing import PipelineDelta

    from ..coordination.orchestrator import ActionOrchestrator

logger = logging.getLogger(__name__)

# The kind discriminator mirrors ``lhp.api.responses.PlannedFileView.kind``
# (§4.8 Literal). Kept as a core-local Literal so this module owns no
# dependency on the API layer; the API converter maps these strings 1:1.
ArtifactKind = Literal["flowgroup", "aux", "helper", "test_hook", "monitoring"]

# Fixed landing sub-paths of the per-pipeline output dir, from the commit step:
#   * copied helper modules  → ``<pipeline>/custom_python_functions/...``
#     (``PythonFileCopier`` / ``compute_copy_records``; the import-rewrite
#     prefix constant ``custom_python_functions`` pins the dir name).
#   * test-reporting hook     → ``<pipeline>/_test_reporting_hook.py`` and
#     ``<pipeline>/test_reporting_providers/...`` (``generate_test_reporting_hook``).
#   * synthetic monitoring    → ``<pipeline>/monitoring.py`` carrying the
#     ``FLOWGROUP_ID = "monitoring"`` marker the monitoring builder emits.
_HELPER_DIR = "custom_python_functions"
_TEST_PROVIDERS_DIR = "test_reporting_providers"
_TEST_HOOK_FILE = "_test_reporting_hook.py"
_MONITORING_FILE = "monitoring.py"
_MONITORING_MARKER = 'FLOWGROUP_ID = "monitoring"'


@dataclass(frozen=True)
class PlannedArtifact:
    """One file a generate run would write, read back from the plan temp tree.

    The core-level analogue of ``lhp.api.responses.PlannedFileView``: the API
    layer converts a tuple of these into ``PlannedFileView``s 1:1 (``path`` →
    ``path``, ``content`` → ``content``, ``pipeline`` → ``pipeline``, ``kind``
    → ``kind``). Lives in ``core`` so ``core`` need not import ``lhp.api``.

    Attributes:
        path: File path RELATIVE to the (temp) output root, i.e. exactly the
            path under ``generated/<env>`` a real generate would write
            (``<pipeline>/<...>``). Forward-slash semantics via ``Path``.
        content: The file's formatted bytes decoded as UTF-8 text — identical
            to what a real generate writes to ``generated/<env>``.
        pipeline: The pipeline (output sub-directory) the file belongs to.
        kind: Role discriminator; see :func:`_classify_artifact`.
    """

    path: Path
    content: str
    pipeline: str
    kind: ArtifactKind


@dataclass(frozen=True)
class GenerationPlanResult:
    """The complete in-memory generation plan (core structure for the API).

    The API layer maps this onto ``lhp.api.GenerationPlan``:
    ``artifacts`` → ``files`` (each converted to ``PlannedFileView``),
    ``pipeline_count`` → ``pipeline_count``, ``len(artifacts)`` → ``file_count``,
    and the caller-supplied real output dir → ``output_location`` (this core
    builder only knows the temp dir, so it surfaces temp-relative ``path``\\ s
    and leaves the real location to the API layer).

    Attributes:
        artifacts: Every planned file, ordered by pipeline (drive/input order)
            then by sorted relative path within the pipeline — deterministic.
        pipeline_count: Number of pipelines the drive committed (the number of
            distinct output sub-directories that produced at least one file).
    """

    artifacts: Tuple[PlannedArtifact, ...]
    pipeline_count: int


def _classify_artifact(
    *,
    tail: Tuple[str, ...],
    content: str,
    flowgroup_filenames: frozenset[str],
) -> ArtifactKind:
    """Map one committed file to its :class:`PlannedArtifact` kind.

    ``tail`` is the file's path components WITHIN its pipeline output dir
    (``rel.parts[1:]``). Classification is by landing location — the
    deterministic commit-step scheme — refined by the per-pipeline flowgroup
    filename set (``flowgroup_filenames``, from the drive's ``PipelineDelta``
    stream) to separate top-level flowgroup modules from top-level auxiliary
    modules (both are bare ``<pipeline>/X.py``):

    1. under ``custom_python_functions/`` → ``helper`` (copied user module or
       its synthesized package ``__init__.py``).
    2. ``_test_reporting_hook.py`` at the top level, or anything under
       ``test_reporting_providers/`` → ``test_hook``.
    3. top-level ``monitoring.py`` carrying the ``FLOWGROUP_ID = "monitoring"``
       marker → ``monitoring`` (the synthetic monitoring pipeline file; the
       marker distinguishes it from a user flowgroup that happens to be named
       ``monitoring``). Checked BEFORE the flowgroup rule because the synthetic
       monitoring file is itself reported as a generated flowgroup filename.
    4. a top-level file whose name is in ``flowgroup_filenames`` → ``flowgroup``.
    5. any other top-level file (a generated flowgroup's inline auxiliary
       module, e.g. the monitoring ``jobs_stats_loader.py``) → ``aux``.
    """
    if tail and tail[0] == _HELPER_DIR:
        return "helper"
    if tail and tail[0] == _TEST_PROVIDERS_DIR:
        return "test_hook"
    if tail == (_TEST_HOOK_FILE,):
        return "test_hook"
    if tail == (_MONITORING_FILE,) and _MONITORING_MARKER in content:
        return "monitoring"
    if len(tail) == 1 and tail[0] in flowgroup_filenames:
        return "flowgroup"
    return "aux"


def _read_plan_tree(
    temp_root: Path,
    flowgroup_filenames_by_pipeline: Dict[str, frozenset[str]],
) -> List[PlannedArtifact]:
    """Read every file under the formatted temp tree into ordered artifacts.

    Walks ``temp_root`` (which contains one sub-directory per committed
    pipeline) and classifies each file via :func:`_classify_artifact`. Order:
    pipelines in the drive's commit order (``flowgroup_filenames_by_pipeline``
    preserves it — a ``dict`` populated per ``PipelineDelta`` in input order),
    then files sorted by relative path within each pipeline. Files are read as
    plain UTF-8 (``read_text(encoding="utf-8")``) — the literal on-disk bytes
    the commit step's ``write_normalized`` produced — so a real-generate disk
    read with the same decoding compares byte-for-byte.
    """
    artifacts: List[PlannedArtifact] = []
    # Pipelines that committed files but never produced a flowgroup delta
    # (defensive — every committed pipeline yields a delta) still get walked.
    pipeline_dirs = {
        child.name: child for child in temp_root.iterdir() if child.is_dir()
    }
    # Drive/input order first (delta order), then any extra dirs not in the map.
    ordered_names = list(flowgroup_filenames_by_pipeline.keys())
    ordered_names += [
        n for n in sorted(pipeline_dirs) if n not in flowgroup_filenames_by_pipeline
    ]

    for pipeline in ordered_names:
        pipeline_dir = pipeline_dirs.get(pipeline)
        if pipeline_dir is None:
            continue
        flowgroup_filenames = flowgroup_filenames_by_pipeline.get(pipeline, frozenset())
        files = sorted(p for p in pipeline_dir.rglob("*") if p.is_file())
        for file_path in files:
            rel = file_path.relative_to(temp_root)
            content = file_path.read_text(encoding="utf-8")
            kind = _classify_artifact(
                tail=rel.parts[1:],
                content=content,
                flowgroup_filenames=flowgroup_filenames,
            )
            artifacts.append(
                PlannedArtifact(path=rel, content=content, pipeline=pipeline, kind=kind)
            )
    return artifacts


def build_generation_plan(
    orchestrator: "ActionOrchestrator",
    *,
    env: str,
    pipeline_filter: Optional[str] = None,
    pipeline_fields: Optional[Sequence[str]] = None,
    specific_flowgroups: Optional[List[str]] = None,
    include_tests: bool = False,
    apply_formatting: Optional[bool] = None,
    pre_discovered_all_flowgroups: Optional[Sequence["FlowGroup"]] = None,
    max_workers: Optional[int] = None,
    on_pipeline_complete: Optional[Callable[["PipelineDelta"], None]] = None,
    on_total: Optional[Callable[[int], None]] = None,
    on_flowgroup_done: Optional[Callable[[], None]] = None,
) -> GenerationPlanResult:
    """Generate to a temp dir, format, read back; return the in-memory plan.

    Drives the UNMODIFIED real generate flow
    (:meth:`~lhp.core.coordination.orchestrator.ActionOrchestrator.generate_pipelines`)
    against a throwaway temp directory, so the resulting plan is byte-for-byte
    identical to a real ``generate`` over the same inputs (same pool, gate,
    ``commit_generate_results`` writer, and terminal ``format_generated_tree``
    pass). The temp dir is discarded before returning; the real
    ``generated/<env>`` tree is never touched. Monitoring *finalization*
    (notebook + ``*.job.yml`` writes to the project root) is intentionally not
    run — a plan is read-only with respect to the project — but the synthetic
    monitoring *pipeline* file is produced like any flowgroup.

    Streaming: ``generate_pipelines`` is a generator yielding one
    :class:`~lhp.models.processing.PipelineDelta` per pipeline; each delta is
    forwarded to ``on_pipeline_complete`` as it is yielded (the same per-pipeline
    stream the real generate exposes, which A2 renders as §5.7 progress events).
    The plan files are read back only AFTER the generator is fully drained,
    because the terminal ruff pass formats the entire tree in one shot —
    formatted file content does not exist until then.

    Args:
        orchestrator: The constructed orchestrator for the project. Its
            ``generate_pipelines`` is driven with a temp ``output_dir``.
        env: Target environment (selects ``substitutions/<env>.yaml``).
        pipeline_filter: Single pipeline field to plan, OR
        pipeline_fields: Batch of pipeline fields to plan (mutually exclusive
            with ``pipeline_filter``; passing neither plans nothing).
        specific_flowgroups: Forwarded for signature parity with the real
            generate (the orchestrator does not narrow by it at this layer).
        include_tests: Emit test actions + the per-pipeline test-reporting hook.
        apply_formatting: Tri-state terminal-format override forwarded verbatim
            (``None`` → use the project's ``lhp.yaml`` ``apply_formatting``;
            ``True`` / ``False`` override it) — the plan formats exactly as the
            equivalent real generate would.
        pre_discovered_all_flowgroups: Optional pre-discovered flowgroup list
            (single-parse reuse), forwarded so the plan's discovery — including
            the synthetic monitoring flowgroup — matches the real generate's.
        max_workers: Pool fan-out override.
        on_pipeline_complete: Optional per-pipeline ``PipelineDelta`` sink, fired
            in commit order during the drive.
        on_total: Optional per-FLOWGROUP total hook, called once with the flat
            worklist length before the pool runs. Forwarded verbatim to
            ``generate_pipelines`` (plain ``Callable``; ``core`` owns no
            ``lhp.api`` dependency) so a plan drives the same flowgroup-grained
            progress the real generate does.
        on_flowgroup_done: Optional per-FLOWGROUP completion hook, called once
            per finished flowgroup. Forwarded verbatim to ``generate_pipelines``.

    Returns:
        A :class:`GenerationPlanResult` whose ``artifacts`` carry temp-relative
        paths (== the ``generated/<env>`` layout) and byte-faithful content.

    Raises:
        lhp.errors.LHPError: Propagated unchanged from the underlying
            all-or-nothing gate (a failing plan raises exactly as a failing
            real generate does, before any temp write).
    """
    flowgroup_filenames_by_pipeline: Dict[str, frozenset[str]] = {}

    with tempfile.TemporaryDirectory(prefix="lhp-plan-") as temp_name:
        temp_root = Path(temp_name)
        # Drive the real generate flow at the temp dir. ``generate_pipelines``
        # is a GENERATOR yielding one ``PipelineDelta`` per pipeline (§5.7 / E3);
        # iterating it runs the pool, the all-or-nothing gate (raises on failure
        # before any write), the ``commit_generate_results`` writer, and — once
        # the generator is fully drained — the terminal ``format_generated_tree``
        # ruff pass, all unmodified. The stream MUST be exhausted for the format
        # pass to run, so this loop drains it completely.
        for delta in orchestrator.generate_pipelines(
            pipeline_filter=pipeline_filter,
            pipeline_fields=(
                list(pipeline_fields) if pipeline_fields is not None else None
            ),
            env=env,
            output_dir=temp_root,
            specific_flowgroups=specific_flowgroups,
            include_tests=include_tests,
            apply_formatting=apply_formatting,
            pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
            max_workers=max_workers,
            on_total=on_total,
            on_flowgroup_done=on_flowgroup_done,
        ):
            # On the clean path every delta is a committed success carrying its
            # flowgroup filenames; a gate failure raises mid-stream (per §1.4 the
            # failure deltas are yielded first, then the raise). Forward each
            # delta to the caller's sink so A2 can render the §5.7 stream.
            if delta.success:
                flowgroup_filenames_by_pipeline[delta.pipeline_name] = frozenset(
                    delta.generated_filenames
                )
            if on_pipeline_complete is not None:
                on_pipeline_complete(delta)
        # Defensive: ``generate_pipelines`` with neither filter nor fields
        # yields nothing and never touches ``output_dir``; rglob a missing tree
        # would raise.
        if not temp_root.exists():
            return GenerationPlanResult(artifacts=(), pipeline_count=0)
        artifacts = _read_plan_tree(temp_root, flowgroup_filenames_by_pipeline)

    committed_pipelines = {a.pipeline for a in artifacts}
    return GenerationPlanResult(
        artifacts=tuple(artifacts),
        pipeline_count=len(committed_pipelines),
    )
