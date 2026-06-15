"""Table-summary response schemas for the write-target catalog view."""

from __future__ import annotations

from pydantic import BaseModel, Field


class TableSummary(BaseModel):
    full_name: str  # "catalog.schema.table" or "sink:kafka/topic_name"
    target_type: str  # "streaming_table" | "materialized_view" | "sink"
    pipeline: str
    flowgroup: str
    write_mode: str | None = None  # "cdc" | "snapshot_cdc" | None
    scd_type: int | None = None  # 1 | 2 | None
    source_file: str  # Relative path to YAML source


def build_table_summary(
    *,
    full_name: str,
    target_type: str,
    pipeline: str,
    flowgroup: str,
    write_mode: str | None,
    scd_type: int | None,
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
    tables: list[TableSummary]
    total: int
    warnings: list[str] = Field(
        default_factory=list
    )  # Per-flowgroup resolution failures
