"""Dependency-analysis read endpoints for the ``lhp web`` local IDE backend.

* No auth dependencies (the local IDE is same-origin, single-user).
* Every endpoint is backed by ONE
  :meth:`lhp.api.InspectionFacade.analyze_dependencies` call, whose public
  result is the networkx-free :class:`lhp.api.DependencyAnalysisResult`. The
  public DTO carries no live graph objects, so order/cycle/external state is
  read straight from the flat result.
* All three graph levels are exposed: ``/graph/pipeline`` from the flattened
  summary, ``/graph/flowgroup`` and ``/graph/action`` from the opt-in
  ``include_graphs=True`` snapshots (frozen, networkx-free level projections
  on the same public result).
* There is no ``/export/{fmt}`` endpoint in v1.

Per the ``webapp-uses-public-api`` import contract, this module may import only
:mod:`lhp.api` / :mod:`lhp.errors` from the ``lhp`` package (plus FastAPI and
the standard library).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query

from lhp.api import DependencyAnalysisResult, InspectionFacade
from lhp.webapp.dependencies import get_inspection
from lhp.webapp.schemas.dependency import (
    CircularDependencyResponse,
    DependencyResponse,
    ExecutionOrderResponse,
    ExternalSourcesResponse,
    GraphResponse,
)
from lhp.webapp.services.graph_serializer import (
    serialize_action_graph,
    serialize_flowgroup_graph,
    serialize_pipeline_graph,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/dependencies", tags=["dependencies"])


async def _analyze(
    inspection: InspectionFacade,
    pipeline: Optional[str] = None,
    *,
    include_graphs: bool = False,
) -> DependencyAnalysisResult:
    """Run dependency analysis off the event loop and return the public result."""
    return await asyncio.to_thread(
        inspection.analyze_dependencies,
        pipeline_filter=pipeline,
        include_graphs=include_graphs,
    )


@router.get("", response_model=DependencyResponse)
async def get_dependencies(
    pipeline: Optional[str] = Query(None, description="Filter by pipeline"),
    inspection: InspectionFacade = Depends(get_inspection),
) -> DependencyResponse:
    """Full dependency-analysis summary (counts, stages, cycles, sources)."""
    result = await _analyze(inspection, pipeline)

    # The public DTO carries a flat pipeline->dependency mapping; serialize each
    # dependency tuple to a list so it lands as plain JSON.
    pipeline_deps = {
        name: list(deps) for name, deps in result.pipeline_dependencies.items()
    }

    return DependencyResponse(
        total_pipelines=result.total_pipelines,
        total_external_sources=result.total_external_sources,
        execution_stages=[list(stage) for stage in result.execution_stages],
        circular_dependencies=[list(cycle) for cycle in result.circular_dependencies],
        external_sources=list(result.external_sources),
        pipeline_dependencies=pipeline_deps,
    )


@router.get("/graph/pipeline", response_model=GraphResponse)
async def get_pipeline_graph(
    pipeline: Optional[str] = Query(None, description="Filter by pipeline"),
    inspection: InspectionFacade = Depends(get_inspection),
) -> GraphResponse:
    """Pipeline-level dependency graph for frontend visualization."""
    result = await _analyze(inspection, pipeline)
    return serialize_pipeline_graph(result)


@router.get("/graph/flowgroup", response_model=GraphResponse)
async def get_flowgroup_graph(
    pipeline: Optional[str] = Query(None, description="Filter by pipeline"),
    inspection: InspectionFacade = Depends(get_inspection),
) -> GraphResponse:
    """Flowgroup-level dependency graph (the pipeline drill-down modal)."""
    result = await _analyze(inspection, pipeline, include_graphs=True)
    return serialize_flowgroup_graph(result)


@router.get("/graph/action", response_model=GraphResponse)
async def get_action_graph(
    pipeline: Optional[str] = Query(None, description="Filter by pipeline"),
    inspection: InspectionFacade = Depends(get_inspection),
) -> GraphResponse:
    """Action-level dependency graph (the flowgroup drill-down modal).

    Nodes are keyed ``{flowgroup}.{action}``; the frontend narrows to one
    flowgroup client-side via each node's ``flowgroup`` field.
    """
    result = await _analyze(inspection, pipeline, include_graphs=True)
    return serialize_action_graph(result)


@router.get("/execution-order", response_model=ExecutionOrderResponse)
async def get_execution_order(
    pipeline: Optional[str] = Query(None, description="Filter by pipeline"),
    inspection: InspectionFacade = Depends(get_inspection),
) -> ExecutionOrderResponse:
    """Execution stages — which pipelines can run in parallel.

    ``flat_order`` is the stage list flattened in generation order (the public
    DTO no longer exposes a precomputed flat order).
    """
    result = await _analyze(inspection, pipeline)

    stages = [list(stage) for stage in result.execution_stages]
    flat_order = [pipeline_name for stage in stages for pipeline_name in stage]

    return ExecutionOrderResponse(
        stages=stages,
        total_stages=len(stages),
        flat_order=flat_order,
    )


@router.get("/circular", response_model=CircularDependencyResponse)
async def get_circular_dependencies(
    inspection: InspectionFacade = Depends(get_inspection),
) -> CircularDependencyResponse:
    """Detect circular dependencies across the project's pipelines."""
    result = await _analyze(inspection)

    cycles = [list(cycle) for cycle in result.circular_dependencies]
    return CircularDependencyResponse(
        has_circular=result.has_cycles,
        cycles=cycles,
        total_cycles=len(cycles),
    )


@router.get("/external-sources", response_model=ExternalSourcesResponse)
async def get_external_sources(
    inspection: InspectionFacade = Depends(get_inspection),
) -> ExternalSourcesResponse:
    """Catalog of external (project-unowned) data sources referenced."""
    result = await _analyze(inspection)

    return ExternalSourcesResponse(
        sources=list(result.external_sources),
        total=result.total_external_sources,
    )
