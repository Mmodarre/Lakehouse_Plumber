"""Composition of the bounded project shape carried by telemetry events.

Underscore-prefixed module: not part of :mod:`lhp.api`'s public surface,
not re-exported from :mod:`lhp.api`. The CLI telemetry hook reaches it
directly.

The shape describes a project by size and feature usage only. Composing
it takes six facade reads plus the project-identity reader, which is
exactly the kind of cross-service assembly that belongs in the
composition root rather than in a consumer (the ``_preflight`` /
``_dataset_index`` precedent), so the CLI hands over a facade and
receives a finished mapping.

Two properties make this safe to call on the exit path of every command:
the composition never raises, and it never outlives its wall-clock
budget. ``compute_stats()`` rides the discovery memo, but
``list_presets()`` / ``list_templates()`` glob and parse on every call,
so reads are ordered cheapest-first and the budget is checked between
them: whichever read runs long is the one that loses, and the caller
omits the ``project`` key rather than delaying the command.

:stability: internal
"""

from __future__ import annotations

import logging
import time
from dataclasses import asdict
from typing import TYPE_CHECKING, Callable, Dict, Iterator, Optional, Tuple, Union

from lhp.telemetry import fold_project_shape, read_project_identity

if TYPE_CHECKING:
    from pathlib import Path

    from lhp.api.facade import LakehousePlumberApplicationFacade
    from lhp.api.responses import StatsResult
    from lhp.api.views import BlueprintView, ProjectConfigView

logger = logging.getLogger(__name__)

ShapeValues = Dict[str, Union[int, bool]]

# Wall-clock ceiling for the whole composition: no command may be made
# slower by the telemetry it emits.
_DEFAULT_BUDGET_S = 0.25


def _from_stats(stats: "StatsResult") -> ShapeValues:
    """Size counters: the action breakdown plus the three project totals.

    The breakdown is passed through whole. Folding drops the keys the
    allowlist does not name — including the bare ``load`` / ``transform``
    / ``write`` / ``test`` totals, which the allowlist deliberately omits.
    """
    values: ShapeValues = dict(stats.action_counts_by_type)
    values["pipelines"] = stats.pipeline_count
    values["flowgroups"] = stats.flowgroup_count
    values["actions"] = stats.total_actions
    return values


def _from_config(config: "ProjectConfigView") -> ShapeValues:
    """Feature flags, as booleans — never the configured values."""
    return {
        "has_operational_metadata": config.has_operational_metadata,
        "has_event_log": config.has_event_log,
        "has_monitoring": config.has_monitoring,
        "has_uc_tagging": config.has_uc_tagging,
        "has_test_reporting": config.has_test_reporting,
        "has_wheel": config.has_wheel,
        "has_sandbox": config.has_sandbox,
        "has_required_lhp_version": config.required_lhp_version is not None,
        "apply_formatting": config.apply_formatting,
    }


def _from_blueprints(blueprints: Tuple["BlueprintView", ...]) -> ShapeValues:
    """Blueprint and instance counts.

    ``instance_count`` is populated by the non-verbose listing, so the
    instance files are counted without being expanded.
    """
    return {
        "blueprints": len(blueprints),
        "blueprint_instances": sum(view.instance_count for view in blueprints),
    }


def _project_reads(
    facade: "LakehousePlumberApplicationFacade", project_root: "Path"
) -> Iterator[Tuple[str, ShapeValues]]:
    """Yield ``(read name, values)`` cheapest read first, one read at a time.

    Lazy by construction: each read runs only when the consumer asks for
    the next pair, which is what lets the budget be enforced BETWEEN
    reads. The name is a fixed label for the debug log, never project data.
    """
    inspection = facade.inspection
    yield "stats", _from_stats(inspection.compute_stats())
    yield "project config", _from_config(inspection.get_project_config())
    yield (
        "flowgroups",
        {
            "flowgroups_using_templates": sum(
                1 for view in inspection.list_flowgroups() if view.template
            )
        },
    )
    yield (
        "blueprints",
        _from_blueprints(inspection.list_blueprints(include_instances=False)),
    )
    yield "presets", {"presets": len(inspection.list_presets())}
    yield "templates", {"templates": len(inspection.list_templates())}
    yield (
        "project identity",
        {"environments": read_project_identity(project_root).environment_count},
    )


def build_project_shape(
    facade: "LakehousePlumberApplicationFacade",
    project_root: "Path",
    *,
    budget_s: float = _DEFAULT_BUDGET_S,
    clock: Callable[[], float] = time.perf_counter,
) -> Optional[ShapeValues]:
    """Compose the allowlisted project shape, or ``None`` when it cannot be.

    ``facade`` is a :class:`~lhp.api.facade.LakehousePlumberApplicationFacade`;
    only its inspection surface is read, and nothing is mutated. The
    result is the folded shape as a plain mapping of allowlisted key to
    integer or boolean — never a name, a path or free text.

    ``None`` means "send no ``project`` key", and is returned for the two
    reasons a caller must not care about: the composition ran past
    ``budget_s``, or a read failed. Both are logged at DEBUG.

    :stability: experimental
    """
    try:
        raw: ShapeValues = {}
        started = clock()
        for name, values in _project_reads(facade, project_root):
            raw.update(values)
            if clock() - started > budget_s:
                logger.debug(
                    f"Project shape abandoned: the {name} read passed the "
                    f"{budget_s}s telemetry budget"
                )
                return None
        return asdict(fold_project_shape(raw))
    except Exception:  # telemetry must never affect the command
        logger.debug("Could not compose the project shape", exc_info=True)
        return None
