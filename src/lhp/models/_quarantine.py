"""Quarantine configuration for data-quality transforms."""

from pydantic import BaseModel, Field


class QuarantineConfig(BaseModel):
    """Configuration for quarantine mode in data quality transforms."""

    dlq_table: str = Field(..., description="Fully qualified DLQ table name")
    source_table: str = Field(
        ..., description="Fully qualified source table name for DLQ tagging"
    )
