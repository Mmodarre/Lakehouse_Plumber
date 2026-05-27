"""Cross-pipeline coordination: parallel execution and Phase-A/B workers.

This package wraps :class:`concurrent.futures.ProcessPoolExecutor` for
both generation and validation, plus the worker entrypoints that ship
across the ``spawn`` boundary. Also hosts:

- :class:`ActionOrchestrator` — top-level orchestration entrypoint

:class:`LakehousePlumberApplicationFacade` lives in :mod:`lhp.api.facade`
now; it is re-exported below via ``__getattr__`` as a transitional
alias for internal callers that still import from this package.
Phase D removes the alias once the CLI cutover lands (week5_plan §D11).

Response DTOs (``GenerationResponse``, ``BatchGenerationResponse``,
``ValidationResponse``, ``BatchValidationResponse``) and view records
(``ValidationIssueView``) now live in :mod:`lhp.api.responses` /
:mod:`lhp.api.views`. They are temporarily re-exported here so the
intermediate-state CLI keeps importing; new code should import from
:mod:`lhp.api`. Phase D removes the re-exports together with the CLI
cutover.

The free functions exported here are entrypoints for multiprocessing
workers. Workers reference them by import path; renaming them would
break pickle-by-name across the process boundary.
"""

from typing import TYPE_CHECKING, Any

# TRANSITIONAL SHIM — DELETE IN PHASE D (week5_plan §B2 + D1).
# Re-exports of DTOs moved to ``lhp.api`` so the intermediate-state CLI and
# the older unit tests keep resolving these names. ``ValidationIssue`` is
# the pre-Week-5 class name kept as an alias on :class:`ValidationIssueView`
# so existing imports survive the rename until Phase D rewrites callers.
from lhp.api.responses import (
    BatchGenerationResponse,
    BatchValidationResponse,
    GenerationResponse,
    ValidationResponse,
)
from lhp.api.views import ValidationIssueView

ValidationIssue = ValidationIssueView
from lhp.core.coordination.executor import (
    FlowgroupValidationResult,
    OnValidationComplete,
    PipelineExecutionService,
    PipelineValidationOutcome,
    ValidationAssembler,
    _GenerateWorkerState,
    _ValidateWorkerState,
    run_generate_pool,
    run_validate_pool,
)
from lhp.core.coordination.monitoring_pipeline_builder import (
    MonitoringBuildResult,
    MonitoringPipelineBuilder,
)
from lhp.core.coordination.monitoring_service import MonitoringFinalizerService
from lhp.core.coordination.processor import PipelineProcessor
from lhp.core.coordination.validation_service import ValidationService
from lhp.models.processing import PipelineWorkUnit

if TYPE_CHECKING:
    from lhp.api.facade import LakehousePlumberApplicationFacade
    from lhp.core.coordination.orchestrator import ActionOrchestrator


# ``ActionOrchestrator`` and ``LakehousePlumberApplicationFacade`` are deferred
# behind ``__getattr__`` to break the import cycle with ``core/dependencies/``:
# the orchestrator's module-level imports pull in ``core.dependencies``, whose
# ``service.py`` imports back from ``core.coordination`` for
# ``ValidationService``. Eager import here would partially initialize the
# package and break the chain. Deferring keeps the cycle latent — both symbols
# are still importable as ``from lhp.core.coordination import …`` at call time.
#
# The ``LakehousePlumberApplicationFacade`` arm of this shim is transitional:
# the canonical home of the class is :mod:`lhp.api.facade` and Phase D
# (week5_plan §D11) deletes the alias here together with the CLI cutover.
def __getattr__(name: str) -> Any:
    if name == "ActionOrchestrator":
        from lhp.core.coordination.orchestrator import ActionOrchestrator

        return ActionOrchestrator
    if name == "LakehousePlumberApplicationFacade":
        from lhp.api.facade import LakehousePlumberApplicationFacade

        return LakehousePlumberApplicationFacade
    raise AttributeError(f"module 'lhp.core.coordination' has no attribute {name!r}")


__all__ = [
    "ActionOrchestrator",
    "BatchGenerationResponse",
    "BatchValidationResponse",
    "FlowgroupValidationResult",
    "GenerationResponse",
    "LakehousePlumberApplicationFacade",
    "MonitoringBuildResult",
    "MonitoringFinalizerService",
    "MonitoringPipelineBuilder",
    "OnValidationComplete",
    "PipelineExecutionService",
    "PipelineProcessor",
    "PipelineValidationOutcome",
    "PipelineWorkUnit",
    "ValidationAssembler",
    "ValidationIssue",
    "ValidationIssueView",
    "ValidationResponse",
    "ValidationService",
    "_GenerateWorkerState",
    "_ValidateWorkerState",
    "run_generate_pool",
    "run_validate_pool",
]
