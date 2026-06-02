"""The per-pipeline validate outcome DTO, isolated from the executor module.

Holds :class:`PipelineValidationOutcome` — the aggregate result the validate
path yields from the engine up to the orchestrator/facade. It lives in this
tiny leaf module (rather than in :mod:`.executor`) so the engine
(:mod:`._pool`) and the executor can BOTH import it WITHOUT an
``executor`` ↔ ``_pool`` cycle and without a lazy-import workaround: this
module imports only from :mod:`lhp.models`, so nothing in the package depends
back on it. :mod:`.executor` re-exports it (and the package ``__init__``
surfaces it), preserving the public import path ``from
lhp.core.coordination.executor import PipelineValidationOutcome`` that the
orchestrator, the facade, and tests already use.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from ...models.processing import ValidationIssueRecord


@dataclass(frozen=True, slots=True)
class PipelineValidationOutcome:
    """Aggregate outcome for a single pipeline's validation.

    ``issues`` is the tuple of per-finding
    :class:`~lhp.models.processing.ValidationIssueRecord`s aggregated
    across the pipeline's flowgroups plus the cross-flowgroup CDC
    fan-in check. Each record carries the finding itself (a live
    :class:`~lhp.errors.LHPError` OR a plain string) together with the
    flowgroup it came from and that flowgroup's source YAML path — the
    per-issue attribution that the prior flat ``errors`` / ``warnings``
    / ``lhp_errors`` tuples discarded. A finding with no single owning
    flowgroup (a cross-flowgroup fan-in issue, a discovery failure, the
    empty-pipeline message) carries ``flowgroup_name=None`` /
    ``source_file=None``.

    ``success`` is ``True`` iff there are no error-severity issues.
    """

    pipeline: str
    issues: Tuple[ValidationIssueRecord, ...]
    success: bool
