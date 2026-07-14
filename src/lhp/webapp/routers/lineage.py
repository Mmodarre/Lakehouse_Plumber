"""Dataset table→table lineage endpoint for the ``lhp web`` local IDE backend.

``GET /api/lineage?env=<required>&fqn=<required>`` returns one produced
dataset's upstream lineage chain (external source → load → transform → write)
plus its downstream consumers, out of the public
:meth:`lhp.api.DependencyFacade.build_dataset_index` result cached per
environment by :class:`lhp.webapp.services.dataset_index.DatasetIndexCache`.

* ``env`` / ``fqn`` are QUERY params (not path params) because sink FQNs
  contain ``/`` (``sink:<type>/<id>``), which would break path routing.
* ``400`` when ``env`` has no ``substitutions/<env>.yaml`` (mirrors
  ``GET /api/tables``). A missing ``env`` / ``fqn`` query param is the
  FastAPI-standard ``422`` (both are declared required, matching
  ``routers/tables.py``).
* ``404`` when no produced dataset matches ``fqn`` in the (serve-stale) index:
  a genuinely-unproduced dataset, a dataset added since the last graph build
  (the ``stale`` flag is then set — a Refresh rebuilds the index and it
  resolves), or a delta sink's underlying ``options.tableName`` (a documented v1
  limitation: the sink is indexed under its ``sink:<type>/<id>`` id, never that
  table FQN). The index is never rebuilt against a stale graph, so a dataset
  present at the last build never false-404s.

Multi-writer collision policy: two write actions may target one FQN. The
endpoint serves the FIRST writer in the index's deterministic order and appends
a warning naming the other writer(s); the fact of a second writer is never
dropped.

Per the ``webapp-uses-public-api`` import contract, this module imports only
:mod:`lhp.api` from the ``lhp`` package, plus the webapp's own DI / schema /
service modules.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from lhp.api import DatasetView, DependencyFacade
from lhp.webapp.dependencies import get_dependency, get_project_root
from lhp.webapp.schemas.lineage import (
    DatasetConsumer,
    DatasetLineageResponse,
    LineageEdge,
    LineageNode,
)
from lhp.webapp.services.dataset_index import DatasetIndexCache

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/lineage", tags=["lineage"])


def _to_response(
    dataset: DatasetView, *, warnings: list[str], stale: bool
) -> DatasetLineageResponse:
    """Map a public :class:`DatasetView` DTO onto the HTTP response model."""
    return DatasetLineageResponse(
        fqn=dataset.fqn,
        kind=dataset.kind,
        pipeline=dataset.pipeline,
        flowgroup=dataset.flowgroup,
        action_name=dataset.action_name,
        write_mode=dataset.write_mode,
        scd_type=dataset.scd_type,
        source_file=dataset.source_file,
        nodes=[
            LineageNode(
                id=node.id,
                kind=node.kind,
                label=node.label,
                pipeline=node.pipeline,
                flowgroup=node.flowgroup,
                dataset_fqn=node.dataset_fqn,
            )
            for node in dataset.nodes
        ],
        edges=[
            LineageEdge(source=edge.source, target=edge.target)
            for edge in dataset.edges
        ],
        consumers=[
            DatasetConsumer(
                dataset_fqn=consumer.dataset_fqn,
                pipeline=consumer.pipeline,
                flowgroup=consumer.flowgroup,
                action_name=consumer.action_name,
            )
            for consumer in dataset.consumers
        ],
        warnings=warnings,
        stale=stale,
    )


@router.get("", response_model=DatasetLineageResponse)
async def get_lineage(
    request: Request,
    env: str = Query(
        ..., description="Environment for substitution resolution (required)"
    ),
    fqn: str = Query(
        ..., description="Fully-qualified dataset name or sink id (required)"
    ),
    dependency: DependencyFacade = Depends(get_dependency),
    project_root: Path = Depends(get_project_root),
) -> DatasetLineageResponse:
    """Return the lineage of the produced dataset ``fqn`` under ``env``."""
    substitution_file = project_root / "substitutions" / f"{env}.yaml"
    if not substitution_file.exists():
        raise HTTPException(
            status_code=400,
            detail=f"Environment '{env}' not found (no substitutions/{env}.yaml)",
        )

    cache: DatasetIndexCache = request.app.state.dataset_index_cache
    # The first request per env pays the build (process_flowgroup × N, off the
    # event loop); subsequent requests are O(1) dict hits until an invalidation.
    cached = await asyncio.to_thread(cache.get, dependency, env=env)

    found = cached.lookup(fqn)
    if found is None:
        raise HTTPException(
            status_code=404,
            detail=f"Dataset '{fqn}' not found in lineage index for env '{env}'",
        )

    dataset, collision_warnings = found
    stale = bool(getattr(request.app.state, "graph_stale", False))
    warnings = list(cached.result.warnings) + list(collision_warnings)
    return _to_response(dataset, warnings=warnings, stale=stale)
