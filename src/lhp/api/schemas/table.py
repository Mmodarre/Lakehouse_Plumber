from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel


class TableSummary(BaseModel):
    full_name: str  # "catalog.schema.table" or "sink:kafka/topic_name"
    target_type: str  # "streaming_table" | "materialized_view" | "sink"
    pipeline: str
    flowgroup: str
    write_mode: Optional[str] = None  # "cdc" | "snapshot_cdc" | None
    scd_type: Optional[int] = None  # 1 | 2 | None
    source_file: str  # Relative path to YAML source


def build_table_summary(
    *,
    full_name: str,
    target_type: str,
    pipeline: str,
    flowgroup: str,
    write_mode: Optional[str],
    scd_type: Optional[int],
    source_file: str,
) -> TableSummary:
    """Build a TableSummary from extracted write action metadata."""
    return TableSummary(
        full_name=full_name,
        target_type=target_type,
        pipeline=pipeline,
        flowgroup=flowgroup,
        write_mode=write_mode,
        scd_type=scd_type,
        source_file=source_file,
    )


class TableListResponse(BaseModel):
    tables: List[TableSummary]
    total: int
    warnings: List[str] = []  # Per-flowgroup resolution failures
