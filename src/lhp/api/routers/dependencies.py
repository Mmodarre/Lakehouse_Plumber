import asyncio
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from lhp.api.dependencies import (
    get_dependency_analyzer,
    get_dependency_output_manager,
)
from lhp.api.schemas.dependency import (
    CircularDependencyResponse,
    DependencyResponse,
    ExecutionOrderResponse,
    ExportResponse,
    ExternalSourcesResponse,
    GraphResponse,
)
from lhp.api.services.graph_serializer import serialize_graph
from lhp.core.services.dependency_analyzer import DependencyAnalyzer
from lhp.core.services.dependency_output_manager import DependencyOutputManager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/dependencies", tags=["dependencies"])


@router.get("", response_model=DependencyResponse)
async def get_dependencies(
    pipeline: Optional[str] = Query(None, description="Filter by pipeline"),
    analyzer: DependencyAnalyzer = Depends(get_dependency_analyzer),
) -> DependencyResponse:
    """#67: Full dependency analysis result."""
    result = await asyncio.to_thread(
        analyzer.analyze_dependencies, pipeline_filter=pipeline
    )

    # Convert pipeline_dependencies to serializable dict
    pipeline_deps = {}
    for name, dep in result.pipeline_dependencies.items():
        pipeline_deps[name] = {
            "flowgroups": [
                {
                    "name": fg.name,
                    "action_count": fg.action_count,
                }
                for fg in dep.flowgroups
            ]
            if hasattr(dep, "flowgroups")
            else {},
        }

    return DependencyResponse(
        total_pipelines=result.total_pipelines,
        total_external_sources=result.total_external_sources,
        execution_stages=result.execution_stages,
        circular_dependencies=result.circular_dependencies,
        external_sources=result.external_sources,
        pipeline_dependencies=pipeline_deps,
    )


@router.get("/graph/{level}", response_model=GraphResponse)
async def get_dependency_graph(
    level: str,
    pipeline: Optional[str] = Query(None, description="Filter by pipeline"),
    analyzer: DependencyAnalyzer = Depends(get_dependency_analyzer),
) -> GraphResponse:
    """#68: Graph JSON for frontend visualization (Cytoscape.js / D3).

    Level must be one of: action, flowgroup, pipeline.
    Returns nodes, edges, and metadata in the format specified by the graph schema.
    """
    if level not in ("action", "flowgroup", "pipeline"):
        raise HTTPException(400, f"Invalid level '{level}'. Use: action, flowgroup, pipeline")

    result = await asyncio.to_thread(
        analyzer.analyze_dependencies, pipeline_filter=pipeline
    )

    return serialize_graph(result.graphs, level)


@router.get("/execution-order", response_model=ExecutionOrderResponse)
async def get_execution_order(
    pipeline: Optional[str] = Query(None),
    analyzer: DependencyAnalyzer = Depends(get_dependency_analyzer),
) -> ExecutionOrderResponse:
    """#69: Execution stages — which pipelines/flowgroups can run in parallel."""
    result = await asyncio.to_thread(
        analyzer.analyze_dependencies, pipeline_filter=pipeline
    )

    return ExecutionOrderResponse(
        stages=result.execution_stages,
        total_stages=len(result.execution_stages),
        flat_order=result.get_pipeline_execution_order(),
    )


@router.get("/circular", response_model=CircularDependencyResponse)
async def get_circular_dependencies(
    analyzer: DependencyAnalyzer = Depends(get_dependency_analyzer),
) -> CircularDependencyResponse:
    """#70: Detect circular dependencies."""
    result = await asyncio.to_thread(analyzer.analyze_dependencies)

    return CircularDependencyResponse(
        has_circular=len(result.circular_dependencies) > 0,
        cycles=result.circular_dependencies,
        total_cycles=len(result.circular_dependencies),
    )


@router.get("/external-sources", response_model=ExternalSourcesResponse)
async def get_external_sources(
    analyzer: DependencyAnalyzer = Depends(get_dependency_analyzer),
) -> ExternalSourcesResponse:
    """#71: Catalog of external data sources referenced by the pipeline."""
    result = await asyncio.to_thread(analyzer.analyze_dependencies)

    return ExternalSourcesResponse(
        sources=result.external_sources,
        total=result.total_external_sources,
    )


@router.get("/export/{fmt}", response_model=ExportResponse)
async def export_dependencies(
    fmt: str,
    pipeline: Optional[str] = Query(None),
    analyzer: DependencyAnalyzer = Depends(get_dependency_analyzer),
    output_mgr: DependencyOutputManager = Depends(get_dependency_output_manager),
) -> ExportResponse:
    """#72: Export dependency data in DOT, JSON, or text format.

    Returns the content as a string rather than writing to disk.

    Actual signatures:
      - analyzer.export_to_dot(graphs: DependencyGraphs, level: str = "pipeline") -> str
      - analyzer.export_to_json(result: DependencyAnalysisResult) -> Dict[str, Any]
      - output_mgr.generate_text_representation(result: DependencyAnalysisResult) -> str
        (rename from _generate_text_representation — see Prerequisites)

    Note: Path parameter renamed from 'format' to 'fmt' to avoid shadowing
    Python's builtin format() function.
    """
    if fmt not in ("dot", "json", "text"):
        raise HTTPException(400, f"Invalid format '{fmt}'. Use: dot, json, text")

    result = await asyncio.to_thread(
        analyzer.analyze_dependencies, pipeline_filter=pipeline
    )

    # Generate content in the requested format
    if fmt == "dot":
        content = await asyncio.to_thread(
            analyzer.export_to_dot, result.graphs, "pipeline"
        )
    elif fmt == "json":
        # export_to_json takes the full DependencyAnalysisResult, returns dict
        json_dict = await asyncio.to_thread(analyzer.export_to_json, result)
        import json
        content = json.dumps(json_dict, indent=2)
    elif fmt == "text":
        content = await asyncio.to_thread(
            output_mgr.generate_text_representation, result
        )
    else:
        raise HTTPException(400, f"Unsupported format: {fmt}")

    return ExportResponse(format=fmt, content=content)
