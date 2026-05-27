"""Private inspection-listing helpers for ``InspectionFacade``.

Underscore-prefixed module: not part of :mod:`lhp.api`'s public surface.
External callers MUST NOT import from here.

Hosts the verbose blueprint-expansion logic for
:meth:`lhp.api.facade.InspectionFacade.list_blueprints` — the
``BlueprintExpander`` reach lives here so the facade stays read-only
and ``_converters.py`` stays focused on per-type DTO projections.

:stability: internal
"""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional

from lhp.api._converters import _blueprint_to_view
from lhp.api.views import BlueprintInstanceView, BlueprintView

if TYPE_CHECKING:
    from lhp.models.config import Blueprint


def _build_blueprint_views(
    discoverer: object,
    *,
    include_instances: bool,
) -> tuple[BlueprintView, ...]:
    """Discover blueprints, count instances, and project to view-tuples.

    The expander is only instantiated when ``include_instances`` is
    ``True`` — the non-verbose listing avoids the cost of expanding
    every instance.
    """
    blueprints: Dict[str, tuple["Blueprint", Path]] = (
        discoverer.discover_blueprints()  # type: ignore[attr-defined]
    )
    if not blueprints:
        return ()
    instance_counts: Dict[str, int] = {name: 0 for name in blueprints}
    instances_by_blueprint: Dict[str, List[BlueprintInstanceView]] = {
        name: [] for name in blueprints
    }
    expander: Optional[object] = None
    if include_instances:
        from lhp.core.processing.blueprint_expander import BlueprintExpander

        expander = BlueprintExpander()
    for instance, instance_path in discoverer.discover_instances(blueprints):  # type: ignore[attr-defined]
        instance_counts[instance.blueprint_name] = (
            instance_counts.get(instance.blueprint_name, 0) + 1
        )
        if expander is None:
            continue
        contexts, _ = expander.expand_single_instance(  # type: ignore[attr-defined]
            instance, instance_path, blueprints
        )
        pipelines = tuple(sorted({ctx.flowgroup.pipeline for ctx in contexts}))
        instances_by_blueprint.setdefault(instance.blueprint_name, []).append(
            BlueprintInstanceView(
                instance_file_path=instance_path,
                flowgroup_count=len(contexts),
                pipelines=pipelines,
            )
        )
    return tuple(
        _blueprint_to_view(
            name,
            blueprint,
            path,
            instance_count=instance_counts.get(name, 0),
            instances=tuple(instances_by_blueprint.get(name, ())),
        )
        for name, (blueprint, path) in sorted(blueprints.items())
    )
