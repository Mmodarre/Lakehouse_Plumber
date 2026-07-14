"""Dependency-analysis read endpoints for the ``lhp web`` local IDE backend.

* No auth dependencies (the local IDE is same-origin, single-user).
* Every endpoint is backed by ONE
  :meth:`lhp.api.DependencyFacade.analyze_dependencies` call, whose public
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

from fastapi import APIRouter, Depends, Query, Request

from lhp.api import DependencyAnalysisResult, DependencyFacade
from lhp.webapp.dependencies import get_dependency
from lhp.webapp.schemas.dependency import (
    CircularDependencyResponse,
    CrossPipelineSummary,
    DependencyResponse,
    ExecutionOrderResponse,
    ExternalSourcesResponse,
    GraphResponse,
    StalenessResponse,
)
from lhp.webapp.services.dataset_index import invalidate as invalidate_dataset_index
from lhp.webapp.services.graph_serializer import (
    serialize_action_graph,
    serialize_flowgroup_graph,
    serialize_pipeline_graph,
    summarize_cross_pipeline,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/dependencies", tags=["dependencies"])


async def _analyze(
    dependency: DependencyFacade,
    pipeline: Optional[str] = None,
    *,
    include_graphs: bool = False,
) -> DependencyAnalysisResult:
    """Run dependency analysis off the event loop and return the public result."""
    return await asyncio.to_thread(
        dependency.analyze_dependencies,
        pipeline_filter=pipeline,
        include_graphs=include_graphs,
    )


@router.get("", response_model=DependencyResponse)
async def get_dependencies(
    pipeline: Optional[str] = Query(None, description="Filter by pipeline"),
    dependency: DependencyFacade = Depends(get_dependency),
) -> DependencyResponse:
    """Full dependency-analysis summary (counts, stages, cycles, sources)."""
    result = await _analyze(dependency, pipeline)

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
    dependency: DependencyFacade = Depends(get_dependency),
) -> GraphResponse:
    """Pipeline-level dependency graph for frontend visualization."""
    result = await _analyze(dependency, pipeline)
    return serialize_pipeline_graph(result)


@router.get("/graph/flowgroup", response_model=GraphResponse)
async def get_flowgroup_graph(
    pipeline: Optional[str] = Query(None, description="Filter by pipeline"),
    dependency: DependencyFacade = Depends(get_dependency),
) -> GraphResponse:
    """Flowgroup-level dependency graph (the pipeline drill-down modal)."""
    result = await _analyze(dependency, pipeline, include_graphs=True)
    return serialize_flowgroup_graph(result)


@router.get("/graph/action", response_model=GraphResponse)
async def get_action_graph(
    pipeline: Optional[str] = Query(None, description="Filter by pipeline"),
    dependency: DependencyFacade = Depends(get_dependency),
) -> GraphResponse:
    """Action-level dependency graph (the flowgroup drill-down modal).

    Nodes are keyed ``{flowgroup}.{action}``; the frontend narrows to one
    flowgroup client-side via each node's ``flowgroup`` field.
    """
    result = await _analyze(dependency, pipeline, include_graphs=True)
    return serialize_action_graph(result)


@router.get("/cross-pipeline", response_model=CrossPipelineSummary)
async def get_cross_pipeline_summary(
    pipeline: str = Query(
        ..., description="Pipeline whose cross-pipeline badges are wanted"
    ),
    dependency: DependencyFacade = Depends(get_dependency),
) -> CrossPipelineSummary:
    """Cross-pipeline / external badge data for ONE pipeline (cheap).

    Backed by ONE FULL (unscoped) ``analyze_dependencies(include_graphs=True)``
    call — cheap thanks to the persisted + memoized graph cache — from which the
    small per-flowgroup cross-pipeline summary is derived server-side. Lets the
    drill-down badge layer skip transferring and recomputing the whole project
    flowgroup graph client-side; the target pipelines it needs survive only in
    the full (unscoped) analysis.
    """
    result = await _analyze(dependency, None, include_graphs=True)
    return summarize_cross_pipeline(result, pipeline)


@router.get("/execution-order", response_model=ExecutionOrderResponse)
async def get_execution_order(
    pipeline: Optional[str] = Query(None, description="Filter by pipeline"),
    dependency: DependencyFacade = Depends(get_dependency),
) -> ExecutionOrderResponse:
    """Execution stages — which pipelines can run in parallel.

    ``flat_order`` is the stage list flattened in generation order (the public
    DTO no longer exposes a precomputed flat order).
    """
    result = await _analyze(dependency, pipeline)

    stages = [list(stage) for stage in result.execution_stages]
    flat_order = [pipeline_name for stage in stages for pipeline_name in stage]

    return ExecutionOrderResponse(
        stages=stages,
        total_stages=len(stages),
        flat_order=flat_order,
    )


@router.get("/circular", response_model=CircularDependencyResponse)
async def get_circular_dependencies(
    dependency: DependencyFacade = Depends(get_dependency),
) -> CircularDependencyResponse:
    """Detect circular dependencies across the project's pipelines."""
    result = await _analyze(dependency)

    cycles = [list(cycle) for cycle in result.circular_dependencies]
    return CircularDependencyResponse(
        has_circular=result.has_cycles,
        cycles=cycles,
        total_cycles=len(cycles),
    )


@router.get("/external-sources", response_model=ExternalSourcesResponse)
async def get_external_sources(
    dependency: DependencyFacade = Depends(get_dependency),
) -> ExternalSourcesResponse:
    """Catalog of external (project-unowned) data sources referenced."""
    result = await _analyze(dependency)

    return ExternalSourcesResponse(
        sources=list(result.external_sources),
        total=result.total_external_sources,
    )


@router.get("/staleness", response_model=StalenessResponse)
async def get_staleness(
    request: Request,
    pipeline: Optional[str] = Query(None, description="Filter by pipeline"),
    dependency: DependencyFacade = Depends(get_dependency),
) -> StalenessResponse:
    """Report whether the served dependency graph is stale (O(1)).

    Reads the per-app ``graph_stale`` flag (set by the file watcher on a
    graph-relevant edit, cleared by ``POST /refresh``); when that flag is
    unset it falls back to the facade's cheap persisted-shard metadata. Never
    re-hashes or rebuilds the project — the graph GET endpoints keep serving
    the last-good result regardless of this answer.
    """
    staleness = await asyncio.to_thread(
        dependency.describe_graph_staleness, pipeline_filter=pipeline
    )
    flag = getattr(request.app.state, "graph_stale", None)
    stale = flag if flag is not None else staleness.stale
    return StalenessResponse(
        stale=stale,
        fingerprint=staleness.fingerprint,
        built_at=staleness.built_at,
    )


@router.post("/refresh", response_model=StalenessResponse)
async def refresh_dependencies(
    request: Request,
    pipeline: Optional[str] = Query(None, description="Filter by pipeline"),
    dependency: DependencyFacade = Depends(get_dependency),
) -> StalenessResponse:
    """Force a fresh dependency-graph build and clear the stale flag.

    Bypasses the serve-stale caches (in-process memo + on-disk graph cache),
    re-reads the project from disk, rebuilds, and re-persists — then clears
    ``app.state.graph_stale`` and returns the new build's metadata. This is the
    only path that rebuilds the graph after an edit; every graph GET keeps
    serving the last-good result until it completes.
    """
    await asyncio.to_thread(
        dependency.analyze_dependencies,
        pipeline_filter=pipeline,
        force_rebuild=True,
    )
    request.app.state.graph_stale = False
    # The dataset lineage index (GET /api/lineage) shares this serve-stale
    # model: drop its per-env cache so the next lineage request rebuilds against
    # the freshly-rebuilt graph.
    invalidate_dataset_index(request.app)
    staleness = await asyncio.to_thread(
        dependency.describe_graph_staleness, pipeline_filter=pipeline
    )
    return StalenessResponse(
        stale=False,
        fingerprint=staleness.fingerprint,
        built_at=staleness.built_at,
    )
