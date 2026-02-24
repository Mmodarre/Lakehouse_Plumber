import asyncio
import logging
from pathlib import Path
from typing import Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query

from lhp.api.dependencies import (
    get_discoverer,
    get_orchestrator,
    get_project_root_adaptive,
)
from lhp.api.schemas.table import (
    TableListResponse,
    build_table_summary,
)
from lhp.core.orchestrator import ActionOrchestrator
from lhp.core.services.flowgroup_discoverer import FlowgroupDiscoverer
from lhp.models.config import ActionType

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/tables", tags=["tables"])


def _extract_scd_type(wt, is_dict: bool) -> Optional[int]:
    """Extract SCD type from cdc_config or snapshot_cdc_config."""
    for cfg_key in ("cdc_config", "snapshot_cdc_config"):
        cfg = wt.get(cfg_key) if is_dict else getattr(wt, cfg_key, None)
        if isinstance(cfg, dict):
            scd = cfg.get("scd_type") or cfg.get("stored_as_scd_type")
            if scd is not None:
                return scd
    return None


def _build_full_name(
    wt, write_type: Optional[str], action_name: str, is_dict: bool
) -> str:
    """Build the display name for a write target (table or sink)."""
    if write_type == "sink":
        if is_dict:
            sink_type = wt.get("sink_type", "unknown")
            sink_id = wt.get("topic") or wt.get("sink_name") or "unnamed"
        else:
            sink_type = getattr(wt, "sink_type", None) or "unknown"
            sink_id = (
                getattr(wt, "topic", None)
                or getattr(wt, "sink_name", None)
                or "unnamed"
            )
        return f"sink:{sink_type}/{sink_id}"

    if is_dict:
        database = wt.get("database", "")
        table = wt.get("table", "")
    else:
        database = getattr(wt, "database", "") or ""
        table = getattr(wt, "table", "") or ""
    if database and table:
        return f"{database}.{table}"
    return database or table or action_name


def _extract_write_metadata(action) -> Tuple[
    Optional[str],  # write_type
    Optional[str],  # write_mode
    Optional[int],  # scd_type
    str,  # full_name
]:
    """Extract write target metadata from a resolved action.

    Handles both dict and WriteTarget Pydantic model forms, replicating
    the canonical pattern from dependency_analyzer._build_action_graph().
    """
    wt = action.write_target
    if wt is None:
        return None, None, None, ""

    is_dict = isinstance(wt, dict)
    if is_dict:
        write_type = wt.get("type")
        write_mode = wt.get("mode")
    else:
        write_type = wt.type.value if hasattr(wt.type, "value") else wt.type
        write_mode = getattr(wt, "mode", None)

    scd_type = _extract_scd_type(wt, is_dict)
    full_name = _build_full_name(wt, write_type, action.name, is_dict)

    return write_type, write_mode, scd_type, full_name


@router.get("", response_model=TableListResponse)
async def list_tables(
    env: str = Query(
        ..., description="Environment for substitution resolution (required)"
    ),
    pipeline: Optional[str] = Query(None, description="Filter by pipeline name"),
    discoverer: FlowgroupDiscoverer = Depends(get_discoverer),
    orchestrator: ActionOrchestrator = Depends(get_orchestrator),
    project_root: Path = Depends(get_project_root_adaptive),
) -> TableListResponse:
    """List all write targets (tables/sinks) across the project.

    Resolves each flowgroup through the orchestrator to expand templates,
    merge presets, and apply substitutions, then extracts write action
    metadata into a flat table-centric view.
    """
    # Validate environment
    substitution_file = project_root / "substitutions" / f"{env}.yaml"
    if not substitution_file.exists():
        raise HTTPException(
            400,
            f"Environment '{env}' not found " f"(no substitutions/{env}.yaml)",
        )

    # Discover flowgroups
    if pipeline:
        flowgroups = await asyncio.to_thread(
            discoverer.discover_flowgroups_by_pipeline_field, pipeline
        )
    else:
        flowgroups = await asyncio.to_thread(discoverer.discover_all_flowgroups)

    # Build path map (best-effort)
    try:
        fg_with_paths = await asyncio.to_thread(
            discoverer.discover_all_flowgroups_with_paths
        )
        path_map = {
            fg.flowgroup: str(path.relative_to(project_root))
            for fg, path in fg_with_paths
        }
    except Exception:
        path_map = {}

    # Create substitution manager
    substitution_mgr = orchestrator.dependencies.create_substitution_manager(
        substitution_file, env
    )

    tables = []
    warnings = []

    for fg in flowgroups:
        try:
            processed = await asyncio.to_thread(
                orchestrator.process_flowgroup, fg, substitution_mgr
            )
        except Exception as exc:
            msg = f"Failed to resolve flowgroup '{fg.flowgroup}': {exc}"
            logger.warning(msg)
            warnings.append(msg)
            continue

        source_file = path_map.get(fg.flowgroup, "")

        for action in processed.actions:
            if action.type != ActionType.WRITE:
                continue

            write_type, write_mode, scd_type, full_name = _extract_write_metadata(
                action
            )

            target_type = write_type or "streaming_table"

            tables.append(
                build_table_summary(
                    full_name=full_name,
                    target_type=target_type,
                    pipeline=fg.pipeline,
                    flowgroup=fg.flowgroup,
                    write_mode=write_mode,
                    scd_type=scd_type,
                    source_file=source_file,
                )
            )

    return TableListResponse(tables=tables, total=len(tables), warnings=warnings)
