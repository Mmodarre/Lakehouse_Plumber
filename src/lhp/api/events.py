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
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from lhp.api.responses import (
        BatchGenerationResponse,
        BatchValidationResponse,
        BundleSyncResult,
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
