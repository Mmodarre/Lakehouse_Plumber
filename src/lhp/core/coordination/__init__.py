"""Cross-pipeline coordination: parallel execution and Phase-A/B workers.

This package wraps :class:`concurrent.futures.ProcessPoolExecutor` for
both generation and validation, plus the worker entrypoints that ship
across the ``spawn`` boundary. Also hosts:

- :class:`ActionOrchestrator` — top-level orchestration entrypoint
  (deferred behind ``__getattr__`` to break the
  :mod:`lhp.core.dependencies` import cycle).

Public response DTOs (``GenerationResponse``, ``BatchGenerationResponse``,
``ValidationResponse``, ``BatchValidationResponse``) and view records
(``ValidationIssueView``) live in :mod:`lhp.api.responses` /
:mod:`lhp.api.views`; this package no longer re-exports them.

The free functions exported here are entrypoints for multiprocessing
workers. Workers reference them by import path; renaming them would
break pickle-by-name across the process boundary.
"""

from typing import TYPE_CHECKING, Any

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
    "FlowgroupValidationResult",
    "MonitoringBuildResult",
    "MonitoringFinalizerService",
    "MonitoringPipelineBuilder",
    "OnValidationComplete",
    "PipelineExecutionService",
    "PipelineProcessor",
    "PipelineValidationOutcome",
    "PipelineWorkUnit",
    "ValidationAssembler",
    "ValidationService",
    "_GenerateWorkerState",
    "_ValidateWorkerState",
    "run_generate_pool",
    "run_validate_pool",
]
