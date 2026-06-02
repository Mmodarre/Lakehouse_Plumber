"""LHP public API surface.

CLI and external consumers MUST import from this package only. Internal
domain modules (``lhp.core``, ``lhp.bundle``, ``lhp.parsers``,
``lhp.models``, ``lhp.generators``) are not part of the public contract
and may change without notice.

The canonical entry points are:

- :class:`LakehousePlumberApplicationFacade` for runtime operations.
- :class:`LakehousePlumberBootstrap` for new-project scaffolding.

Long-running operations (``generate_pipelines``, ``validate_pipelines``,
``BundleFacade.sync_resources``) return an ``Iterator[LHPEvent]`` per
constitution §1.2 / §5.7. Callers that don't need event-level
visibility use :func:`collect_response` to walk the stream and return
the terminal response DTO.

:stability: provisional
"""

from __future__ import annotations

from typing import Iterator

from lhp.api._serialization import to_dict
from lhp.api.bootstrap import LakehousePlumberBootstrap
from lhp.api.callbacks import WarningCollector
from lhp.api.events import (
    BundleSyncCompleted,
    ErrorEmitted,
    GenerationCompleted,
    LHPEvent,
    OperationCompleted,
    OperationStarted,
    ValidationCompleted,
)
from lhp.api.facade import (
    BundleFacade,
    GenerationFacade,
    InspectionFacade,
    LakehousePlumberApplicationFacade,
    ValidationFacade,
)
from lhp.api.responses import (
    BatchGenerationResponse,
    BatchValidationResponse,
    BundleEnableResult,
    BundleSyncResult,
    BundleValidationResult,
    DependencyAnalysisResult,
    DependencyOutputEntry,
    DependencyOutputsResult,
    FinalizeMonitoringResult,
    GenerationResponse,
    InitProjectResult,
    JSONValue,
    StatsResult,
    ValidationResponse,
)
from lhp.api.views import (
    ActionView,
    BlueprintInstanceView,
    BlueprintView,
    FlowgroupView,
    GeneratedCodeView,
    PipelineStats,
    PresetView,
    ProcessedFlowgroupView,
    ProjectConfigView,
    SecretReferenceView,
    SubstitutionView,
    TemplateParameterView,
    TemplateView,
    ValidationIssueView,
)
from lhp.bundle.detection import should_enable_bundle_support


def collect_response(events: Iterator[LHPEvent]) -> object:
    """Walk an LHPEvent stream and return the terminal ``response``.

    Callers who don't need event-level visibility use this helper::

        response = collect_response(facade.generate_pipelines(...))

    The iterator is exhausted. If the stream emits :class:`ErrorEmitted`
    and the underlying generator raises, the raise propagates from this
    function — callers handle the raise.

    The return type is ``object`` here for typing-flexibility; the
    actual returned value is the response DTO of whichever stream was
    consumed (:class:`BatchGenerationResponse` /
    :class:`BatchValidationResponse` / :class:`BundleSyncResult`).
    Annotate at the call site if you need a precise type.

    Terminal events are discovered via the :class:`OperationCompleted`
    base, so this helper does not need to enumerate concrete subclasses.

    :stability: stable
    """
    final_response: object = None
    for event in events:
        if isinstance(event, OperationCompleted):
            final_response = event.response
    if final_response is None:
        # Stream ended without a terminal Completed event — should not
        # happen on the success path; investigate the producer.
        raise RuntimeError("LHPEvent stream ended without a terminal Completed event")
    return final_response


__all__: list[str] = [
    "ActionView",
    "BatchGenerationResponse",
    "BatchValidationResponse",
    "BlueprintInstanceView",
    "BlueprintView",
    "BundleEnableResult",
    "BundleFacade",
    "BundleSyncCompleted",
    "BundleSyncResult",
    "BundleValidationResult",
    "DependencyAnalysisResult",
    "DependencyOutputEntry",
    "DependencyOutputsResult",
    "ErrorEmitted",
    "FinalizeMonitoringResult",
    "FlowgroupView",
    "GeneratedCodeView",
    "GenerationCompleted",
    "GenerationFacade",
    "GenerationResponse",
    "InitProjectResult",
    "InspectionFacade",
    "JSONValue",
    "LHPEvent",
    "LakehousePlumberApplicationFacade",
    "LakehousePlumberBootstrap",
    "OperationCompleted",
    "OperationStarted",
    "PipelineStats",
    "PresetView",
    "ProcessedFlowgroupView",
    "ProjectConfigView",
    "SecretReferenceView",
    "StatsResult",
    "SubstitutionView",
    "TemplateParameterView",
    "TemplateView",
    "ValidationCompleted",
    "ValidationFacade",
    "ValidationIssueView",
    "ValidationResponse",
    "WarningCollector",
    "collect_response",
    "should_enable_bundle_support",
    "to_dict",
]
