"""Private converter: operational-metadata catalog → public view.

Underscore-prefixed module: not part of :mod:`lhp.api`'s public surface.
External callers MUST NOT import from here.

Kept out of :mod:`lhp.api._inspection_converters` because this is the only
inspection converter that reaches into ``lhp.core.codegen`` (it resolves the
generator's operational-metadata catalog); it also keeps that registry under
the constitution §3.3 soft cap.

:stability: internal
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from lhp.api.views import (
    OperationalMetadataColumnView,
    OperationalMetadataPresetView,
    OperationalMetadataView,
)

if TYPE_CHECKING:
    from lhp.models import ProjectConfig


def _operational_metadata_to_view(
    project_config: Optional["ProjectConfig"],
) -> OperationalMetadataView:
    """Project the operational-metadata catalog onto a public view.

    Resolves available columns through the generator's
    :meth:`OperationalMetadataCatalog._get_available_columns` (REPLACE
    semantics: project columns suppress the five built-ins wholesale, else
    the built-ins), so the view matches generate-time exactly. All columns
    are exposed regardless of their ``enabled`` flag — ``_get_available_columns``
    does not filter on ``enabled``, so honouring it here would DIVERGE from
    generation. Expressions carry any substitution tokens verbatim.
    """
    from lhp.core.codegen.operational_metadata.metadata import (
        OperationalMetadataCatalog,
    )

    project_meta = (
        project_config.operational_metadata if project_config is not None else None
    )
    project_column_names = (
        set(project_meta.columns)
        if project_meta is not None and project_meta.columns
        else set()
    )

    catalog = OperationalMetadataCatalog(project_meta)
    columns = tuple(
        OperationalMetadataColumnView(
            name=name,
            expression=cfg.expression,
            description=cfg.description,
            applies_to=tuple(cfg.applies_to),
            source="project" if name in project_column_names else "builtin",
        )
        for name, cfg in catalog._get_available_columns().items()
    )

    presets: tuple[OperationalMetadataPresetView, ...] = ()
    if project_meta is not None and project_meta.presets:
        presets = tuple(
            OperationalMetadataPresetView(
                name=name,
                columns=tuple(preset.columns),
                description=preset.description,
            )
            for name, preset in project_meta.presets.items()
        )

    return OperationalMetadataView(columns=columns, presets=presets)
