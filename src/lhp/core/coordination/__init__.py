"""Cross-pipeline coordination: the consolidated flat per-flowgroup engine.

This package drives a single :class:`concurrent.futures.ProcessPoolExecutor`
fan-out engine (:func:`._pool._run_flowgroup_pool_core`, ``mode`` the only
fork) for both generation and validation, with the single-flowgroup worker
seam in :mod:`._flowgroup_pool` shipping across the ``spawn`` boundary. The
orchestrator-facing facade is :class:`PipelineExecutionService`
(:mod:`.executor`). Also hosts:

- :class:`ActionOrchestrator` — top-level orchestration entrypoint
  (deferred behind ``__getattr__`` to break the
  :mod:`lhp.core.dependencies` import cycle).

The generate write-step generator ``commit_generate_results`` lives in the
private :mod:`._commit` module and is consumed only by :mod:`.executor`
(same-package private import); it is intentionally NOT re-exported on this
package surface.

Public response DTOs (``GenerationResponse``, ``BatchGenerationResponse``,
``ValidationResponse``, ``BatchValidationResponse``) and view records
(``ValidationIssueView``) live in :mod:`lhp.api.responses` /
:mod:`lhp.api.views`; this package no longer re-exports them.

The engine internals (``_run_flowgroup_pool_core`` and its ``mode`` fork)
are NOT exported from this package surface (constitution §4.6);
the worker-seam symbols re-exported from :mod:`.executor` form the
pickle-by-name contract and must not be renamed without updating workers.
"""

from typing import TYPE_CHECKING, Any

from lhp.core.coordination.bootstrap_service import FlowgroupBootstrapService
from lhp.core.coordination.executor import (
    PipelineExecutionService,
    PipelineValidationOutcome,
)
from lhp.core.coordination.monitoring_pipeline_builder import (
    MonitoringBuildResult,
    MonitoringPipelineBuilder,
    resolve_monitoring_pipeline_name,
)
from lhp.core.coordination.monitoring_service import MonitoringFinalizerService
from lhp.core.coordination.validation_service import ValidationService

if TYPE_CHECKING:
    from lhp.core.coordination.orchestrator import ActionOrchestrator


# ``ActionOrchestrator`` is deferred behind ``__getattr__`` to break the
# import cycle with ``core/dependencies/``: the orchestrator's module-level
# imports pull in ``core.dependencies``, whose ``service.py`` imports back
# from ``core.coordination`` for ``ValidationService``. Eager import here
# would partially initialize the package and break the chain.
def __getattr__(name: str) -> Any:
    if name == "ActionOrchestrator":
        from lhp.core.coordination.orchestrator import ActionOrchestrator

        return ActionOrchestrator
    raise AttributeError(f"module 'lhp.core.coordination' has no attribute {name!r}")


__all__ = [
    "ActionOrchestrator",
    "FlowgroupBootstrapService",
    "MonitoringBuildResult",
    "MonitoringFinalizerService",
    "MonitoringPipelineBuilder",
    "PipelineExecutionService",
    "PipelineValidationOutcome",
    "ValidationService",
    "resolve_monitoring_pipeline_name",
]
