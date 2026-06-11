"""Sandbox rewrite-plan pre-pass: scope partition + rewrite decisions.

Runs ONCE on the main thread, after the discover phase and before the gated
flowgroup pool, turning the discovered flowgroup set into one
:class:`~lhp.core.sandbox.SandboxRewritePlan`:

1. Partition the discovered flowgroups by sandbox scope
   (``flowgroup.pipeline`` membership in ``run.pipelines``).
2. Build ONE real-environment substitution manager — the *same* construction
   the flat-engine worklist builder performs per pipeline
   (:func:`.flowgroup_worklist_builder.build_flowgroup_worklist`:
   ``substitutions/<env>.yaml`` resolved against ``project_root``, handed to
   :meth:`~lhp.core.registry.OrchestrationDependencies.create_substitution_manager`)
   — so pre-pass-resolved table names match worker-resolved names
   byte-for-byte.
3. Resolve BOTH partitions through the same
   :meth:`~lhp.core._interfaces.BaseFlowgroupResolutionService.process_flowgroup`
   path the pool workers use, tolerantly (see :func:`_resolve_tolerantly`).
4. Rename set: exactly the tables produced by the IN-scope partition, via
   :func:`~lhp.core.sandbox.build_sandbox_table_renames`.
5. Mixed-producer sinks: every rename key the OUT-of-scope partition ALSO
   produces is still rewritten, and ALL such findings fold
   into ONE ``LHP-VAL-065`` :class:`~lhp.models.processing.SandboxWarningRecord`
   (a WARNING naming the out-of-scope producers — never an error).

**Double resolution is accepted**: the pre-pass and each pool worker both
resolve the in-scope flowgroups (in-memory, milliseconds each); shipping
pre-resolved models to the pool would break the resolve-in-worker contract.

**DependencyAnalysisService is deliberately NOT reused**: its resolution
hardcodes a skip-validation ``env="dev"`` substitution manager
(``dependencies/service.py``), so its keys can carry unresolved tokens and
would be unmatchable against worker-resolved names.

Not part of the public API; callers above ``core`` go through the
:meth:`~lhp.core.coordination.orchestrator.ActionOrchestrator.build_sandbox_rewrite_plan`
seam, which supplies the orchestrator-owned services this function composes
(§5.5: the pre-pass is HANDED its collaborators, it never constructs them).

:stability: internal
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Sequence, Tuple

from lhp.models import FlowGroup
from lhp.models.processing import SandboxRunConfig, SandboxWarningRecord

from ...errors import codes
from .._interfaces import (
    BaseFlowgroupBootstrapService,
    BaseFlowgroupResolutionService,
)
from ..dependencies import build_producer_indexes, match_produced_table
from ..registry import OrchestrationDependencies
from ..sandbox import (
    SandboxRewritePlan,
    SandboxTableRenames,
    TableRenameStrategy,
    build_sandbox_table_renames,
)

if TYPE_CHECKING:
    from ..processing.substitution import EnhancedSubstitutionManager

logger = logging.getLogger(__name__)


def build_sandbox_rewrite_plan(
    *,
    processing: BaseFlowgroupResolutionService,
    bootstrap: BaseFlowgroupBootstrapService,
    orchestration_dependencies: OrchestrationDependencies,
    project_root: Path,
    env: str,
    run: SandboxRunConfig,
    flowgroups: Sequence[FlowGroup],
) -> SandboxRewritePlan:
    """Build one sandbox run's rewrite plan from the discovered flowgroups.

    Args:
        processing: The orchestrator's flowgroup-resolution service — the
            SAME service instance the pool workers re-resolve through, so
            pre-pass keys carry the worker-resolved spelling.
        bootstrap: The orchestrator's bootstrap service; supplies
            ``make_context`` (synthetic provenance + source-YAML lookup,
            main-process only — which is where this pre-pass runs).
        orchestration_dependencies: The orchestrator's dependency container;
            supplies ``create_substitution_manager`` exactly as the worklist
            builder uses it.
        project_root: Project root; anchors ``substitutions/<env>.yaml``.
        env: Target environment name (the REAL env of this generate run).
        run: The frozen per-run sandbox parameters. ``run.pipelines`` is the
            already-resolved concrete pipeline-field set (scope/glob
            expansion happened upstream; VAL_064 guards empty matches there).
        flowgroups: The discovered (blueprint-expanded) flowgroup set.

    Returns:
        The :class:`SandboxRewritePlan`: renames over the in-scope partition
        plus zero-or-one folded mixed-producer warning.
    """
    scope = set(run.pipelines)
    in_scope_raw = [fg for fg in flowgroups if fg.pipeline in scope]
    out_scope_raw = [fg for fg in flowgroups if fg.pipeline not in scope]

    # Same construction as the worklist builder
    # (flowgroup_worklist_builder.py: substitution_file + the
    # orchestration-dependencies factory) so resolved names match the
    # workers' byte-for-byte.
    substitution_file = project_root / "substitutions" / f"{env}.yaml"
    substitution_mgr: Optional[EnhancedSubstitutionManager]
    try:
        substitution_mgr = orchestration_dependencies.create_substitution_manager(
            substitution_file, env
        )
    except Exception:
        # Documented swallow (§9.17): the worklist builder performs this
        # identical construction for the gated pool — if it fails here it
        # fails identically there, and THAT path owns the user-facing
        # error. The pre-pass degrades to raw-name indexes instead of
        # pre-empting it with a less actionable failure.
        logger.debug(
            f"Sandbox pre-pass: substitution manager construction failed "
            f"for env '{env}'; indexing raw flowgroups.",
            exc_info=True,
        )
        substitution_mgr = None

    in_scope = _resolve_tolerantly(
        in_scope_raw,
        processing=processing,
        bootstrap=bootstrap,
        substitution_mgr=substitution_mgr,
    )
    out_scope = _resolve_tolerantly(
        out_scope_raw,
        processing=processing,
        bootstrap=bootstrap,
        substitution_mgr=substitution_mgr,
    )

    strategy = TableRenameStrategy(
        namespace=run.namespace, table_pattern=run.table_pattern
    )
    renames = build_sandbox_table_renames(in_scope, strategy)
    warnings = _fold_mixed_producer_warnings(renames, out_scope)
    return SandboxRewritePlan(renames=renames, warnings=warnings)


def _resolve_tolerantly(
    flowgroups: Sequence[FlowGroup],
    *,
    processing: BaseFlowgroupResolutionService,
    bootstrap: BaseFlowgroupBootstrapService,
    substitution_mgr: Optional[EnhancedSubstitutionManager],
) -> List[FlowGroup]:
    """Resolve each flowgroup via the worker path; fall back to raw on failure.

    Mirrors the pool worker's resolution call
    (``_flowgroup_pool._process_one_flowgroup_impl``:
    ``processor.process_flowgroup(ctx, substitution_mgr, include_tests=...)``).
    ``include_tests=False`` is safe for index purposes: the flag only filters
    TEST actions, and the producer indexes read WRITE actions exclusively
    (:func:`~lhp.core.dependencies.build_producer_indexes`), so it cannot
    change a rename key.
    """
    if substitution_mgr is None:
        return list(flowgroups)
    resolved: List[FlowGroup] = []
    for fg in flowgroups:
        try:
            ctx_out = processing.process_flowgroup(
                bootstrap.make_context(fg),
                substitution_mgr,
                include_tests=False,
            )
            resolved.append(ctx_out.flowgroup)
        except Exception:
            # Documented swallow (§9.17): resolution failures are NOT this
            # pre-pass's to report — the gated pool resolves the same
            # flowgroup again and surfaces the real error with full context;
            # raising here would pre-empt it. The RAW flowgroup still feeds
            # the producer indexes so the plan stays total over the scope.
            logger.debug(
                f"Sandbox pre-pass: tolerated resolution failure for "
                f"flowgroup '{fg.flowgroup}' (pipeline '{fg.pipeline}'); "
                f"using the raw flowgroup for index purposes.",
                exc_info=True,
            )
            resolved.append(fg)
    return resolved


def _fold_mixed_producer_warnings(
    renames: SandboxTableRenames,
    out_of_scope: List[FlowGroup],
) -> Tuple[SandboxWarningRecord, ...]:
    """Mixed-producer sinks are rewritten + ONE folded WARNING.

    Every in-scope rename key is matched against the out-of-scope producer
    indexes through :func:`~lhp.core.dependencies.match_produced_table` (the
    2-part<->3-part reconciliation choke point — exact canonical equality for
    3-part keys, uniqueness-guarded suffix match across scopes).
    ALL findings fold into ONE ``LHP-VAL-065`` record with ``file=None`` —
    the downstream warning dedup layers key on ``(code, file)``, so exactly
    one record survives every merge.
    """
    out_tp, out_ts = build_producer_indexes(out_of_scope)
    if not out_tp:
        return ()
    findings: List[str] = []
    for key in sorted(renames.table_producers):
        hit = match_produced_table(key, out_tp, out_ts)
        if hit is None:
            continue
        producers = ", ".join(sorted(out_tp[hit]))
        findings.append(f"'{key}' is also produced by {producers}")
    if not findings:
        return ()
    message = (
        f"Sandbox rewrite: {len(findings)} sink table(s) are also produced "
        f"by out-of-scope flowgroup(s); they are rewritten anyway "
        f"(mixed-producer sinks): " + "; ".join(findings)
    )
    return (
        SandboxWarningRecord(
            code=codes.VAL_065.code,
            message=message,
            file=None,
            flowgroup=None,
        ),
    )
