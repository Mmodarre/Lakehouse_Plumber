"""Stream events yielded by long-running facade operations.

The base ``LHPEvent`` is a marker class; concrete subclasses
(``OperationStarted``, ``GenerationCompleted``, ``ValidationCompleted``,
``BundleSyncCompleted``, ``ErrorEmitted``) are frozen dataclasses.

The stream protocol (constitution §5.7) guarantees:
- Exactly one ``OperationStarted`` is yielded first.
- On success: exactly one terminal ``*Completed`` is yielded last.
- On failure: ``ErrorEmitted`` is yielded, then the underlying
  ``LHPError`` is re-raised.

:stability: provisional
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from lhp.api.responses import (
        BatchGenerationResponse,
        BatchValidationResponse,
        BundleSyncResult,
        GenerationPlan,
    )
    from lhp.errors.types import LHPError


@dataclass(frozen=True)
class LHPEvent:
    """Base marker for stream-protocol events.

    Subclasses are frozen dataclasses with no methods beyond what
    ``dataclass(frozen=True)`` provides. The protocol invariants
    (constitution §5.7):

    - Exactly one ``OperationStarted`` is yielded as the first event.
    - On success: exactly one terminal ``*Completed`` is yielded last.
    - On failure: ``ErrorEmitted`` is yielded, then the underlying
      ``LHPError`` propagates via ``raise``.

    Constitution §9.21: ``LHPEvent`` stays a marker; do NOT add
    methods or attributes here. Subclasses carry the payload.

    :stability: stable
    """


@dataclass(frozen=True)
class OperationStarted(LHPEvent):
    """First event yielded by any long-running facade operation.

    :stability: stable
    """

    operation_name: str
    env: Optional[str] = None


@dataclass(frozen=True)
class OperationCompleted(LHPEvent):
    """Marker for terminal success events.

    Carries the response DTO of a long-running operation. The three
    concrete subclasses (``GenerationCompleted``,
    ``ValidationCompleted``, ``BundleSyncCompleted``) each pin a
    precise response type via a typed ``response`` field that
    narrows this base annotation. This base lets generic consumers
    (e.g., :func:`lhp.api.collect_response`) discover the terminal
    event without per-type dispatch (§9.22).

    Constitution §9.21 keeps :class:`LHPEvent` a marker. Adding an
    intermediate base BELOW the marker is allowed; it does not turn
    ``LHPEvent`` itself into anything other than a marker.

    :stability: stable
    """

    response: object


@dataclass(frozen=True)
class GenerationCompleted(OperationCompleted):
    """Terminal success event for ``GenerationFacade.generate_pipelines``.

    :stability: stable
    """

    response: "BatchGenerationResponse"


@dataclass(frozen=True)
class ValidationCompleted(OperationCompleted):
    """Terminal success event for ``ValidationFacade.validate_pipelines``.

    :stability: stable
    """

    response: "BatchValidationResponse"


@dataclass(frozen=True)
class BundleSyncCompleted(OperationCompleted):
    """Terminal success event for ``BundleFacade.sync_resources``.

    :stability: provisional
    """

    response: "BundleSyncResult"


@dataclass(frozen=True)
class GenerationPlanCompleted(OperationCompleted):
    """Terminal success event for the plan-only generation path.

    Carries a :class:`GenerationPlan` describing every file the run
    would write, without anything having been written to disk. Like the
    other terminal events it subclasses :class:`OperationCompleted`, so
    :func:`lhp.api.collect_response` discovers it as the stream terminal
    without per-type dispatch (§9.22).

    :stability: provisional
    """

    response: "GenerationPlan"


@dataclass(frozen=True)
class ErrorEmitted(LHPEvent):
    """Yielded immediately before an :class:`LHPError` propagates from a stream op.

    Carries the live exception instance — this is the one exception
    to the §4.8 "no Exception in DTO fields" rule (§9.21 permits it
    for the failure-rendezvous protocol). The CLI uses
    ``lhp_error.code`` to map to an exit code; the rich panel
    payload renders from ``lhp_error.suggestions`` / ``context`` /
    ``doc_link``.

    :stability: stable
    """

    lhp_error: "LHPError"


@dataclass(frozen=True)
class PhaseStarted(LHPEvent):
    """A named phase of a long-running operation has begun.

    Non-terminal progress event. ``phase`` is a human-facing label
    (e.g. ``"discovery"``, ``"generation"``, ``"bundle-sync"``) the CLI
    renders as a live progress marker. Paired with a later
    :class:`PhaseCompleted` carrying the same ``phase``.

    :stability: provisional
    """

    phase: str


@dataclass(frozen=True)
class PhaseCompleted(LHPEvent):
    """A named phase of a long-running operation has finished.

    Non-terminal progress event paired with an earlier
    :class:`PhaseStarted` of the same ``phase``. ``duration_s`` is the
    wall-clock seconds the phase took; ``success`` is ``False`` when the
    phase finished in a degraded / failed state (the operation may still
    continue or raise downstream).

    :stability: provisional
    """

    phase: str
    duration_s: float
    success: bool


@dataclass(frozen=True)
class PipelineStarted(LHPEvent):
    """Per-pipeline progress event: work on ``pipeline`` has begun.

    Non-terminal. Emitted once per pipeline in a multi-pipeline run so
    consumers can render per-pipeline progress. Paired with a later
    :class:`PipelineCompleted` or :class:`PipelineFailed`.

    :stability: provisional
    """

    pipeline: str


@dataclass(frozen=True)
class PipelineCompleted(LHPEvent):
    """Per-pipeline progress event: ``pipeline`` finished successfully.

    Non-terminal. ``duration_s`` is the per-pipeline wall-clock seconds;
    ``files_written`` is the number of files persisted for this pipeline.

    :stability: provisional
    """

    pipeline: str
    duration_s: float
    files_written: int


@dataclass(frozen=True)
class PipelineFailed(LHPEvent):
    """Per-pipeline progress event: ``pipeline`` failed.

    Non-terminal — distinct from the stream's failure-rendezvous
    :class:`ErrorEmitted` (which precedes a ``raise``). This event
    records that one pipeline failed while a multi-pipeline run
    continues. ``code`` is the LHP error code (e.g. ``"LHP-VAL-021"``)
    and ``message`` a human-readable summary; no live exception is
    carried, so the event stays §4.8-compliant and picklable.

    :stability: provisional
    """

    pipeline: str
    code: str
    message: str


@dataclass(frozen=True)
class WarningEmitted(LHPEvent):
    """A non-fatal warning surfaced during a long-running operation.

    Non-terminal and data-only — distinct from :class:`ErrorEmitted`,
    which precedes a ``raise``; a :class:`WarningEmitted` never halts the
    stream. ``message`` is the positional, always-present human-readable
    text; ``code`` / ``category`` / ``file`` / ``flowgroup`` are optional
    structured context. The locked soft-cap emission
    ``WarningEmitted("event buffer near limit", code="LHP-EVT-SOFT-CAP")``
    (constitution §13 item 4) relies on this message-first field order.

    :stability: provisional
    """

    message: str
    code: str = ""
    category: str = ""
    file: Optional[Path] = None
    flowgroup: Optional[str] = None
